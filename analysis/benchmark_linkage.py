# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Tuning harness for ``powerplantmatching.linkage`` record linkage.

Ground truth: the GEO<->GPD pairs encoded in the ``projectID`` column of the
production ``powerplants.csv``. Per-field similarity matrices are computed once
per country and cached, so a (low, high, threshold) configuration is scored by
pure arithmetic on the cached matrices.
"""

import ast
import pickle
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

import powerplantmatching as pm
from powerplantmatching.cleaning import clean_name
from powerplantmatching.linkage import (
    GEO_MAX_DISTANCE_M,
    LINKAGE_FIELDS,
    LINKAGE_THRESHOLD,
)
from powerplantmatching.linkage import _name_matrix as engine_name_matrix

REPO = Path(__file__).resolve().parent.parent
POWERPLANTS_CSV = REPO / "powerplants.csv"
CACHE_DIR = Path(tempfile.gettempdir()) / "powerplantmatching_benchmark"
CACHE_FILE = CACHE_DIR / "benchmark_linkage_cache.pkl"

NAME_SCORERS = {
    "token_set_ratio": fuzz.token_set_ratio,
    "token_sort_ratio": fuzz.token_sort_ratio,
    "WRatio": fuzz.WRatio,
}
DEFAULT_NAME_MATRIX = "jw_tokenized"
QGRAM_FIELDS = ["Fueltype", "Country"]
STR_COLUMNS = ["Name", "Fueltype", "Technology", "Set", "Country"]

Fields = list[tuple[str, float, float]]
BASELINE_FIELDS: Fields = [(f.column, f.low, f.high) for f in LINKAGE_FIELDS]


@dataclass
class CountryCache:
    country: str
    left_ids: np.ndarray
    right_ids: np.ndarray
    names: dict[str, np.ndarray]
    sims: dict[str, np.ndarray]
    present: dict[str, np.ndarray]


def load_ground_truth(path: Path = POWERPLANTS_CSV) -> set[tuple[str, str]]:
    """GEO<->GPD projectID pairs that ended in the same production cluster."""
    ids = pd.read_csv(path, usecols=["projectID"]).projectID.map(ast.literal_eval)
    pairs = set()
    for entry in ids:
        for left in entry.get("GEO", ()):
            for right in entry.get("GPD", ()):
                pairs.add((left, right))
    return pairs


def _preprocess(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    str_cols = [c for c in STR_COLUMNS if c in df.columns]
    out = df.assign(
        lat=df.lat.astype(float),
        lon=df.lon.astype(float),
        **df[str_cols].fillna("").astype(str),
    )
    out = clean_name(out, config=config)
    with pd.option_context("future.no_silent_downcasting", True):
        out[str_cols] = out[str_cols].replace("", pd.NA).infer_objects(copy=False)
    return out.reset_index(drop=True)


def load_frames(config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """GEO and GPD with their ``matching_sources`` queries and production preprocessing."""
    queries = {
        k: v
        for source in config["matching_sources"]
        if isinstance(source, dict)
        for k, v in source.items()
    }
    geo = pm.data.GEO(update=False, config=config).query(queries["GEO"])
    gpd = pm.data.GPD(update=False, config=config).query(queries["GPD"])
    return _preprocess(geo, config), _preprocess(gpd, config)


def _qgram_matrix(a: pd.Series, b: pd.Series) -> np.ndarray:
    """Dice coefficient over character bigrams, computed on unique values only."""
    codes_a, uniq_a = pd.factorize(a.fillna("").astype(str).str.lower())
    codes_b, uniq_b = pd.factorize(b.fillna("").astype(str).str.lower())

    def grams(s: str) -> set[str]:
        return {s[i : i + 2] for i in range(len(s) - 1)} if len(s) > 1 else {s}

    bg_a, bg_b = [grams(x) for x in uniq_a], [grams(x) for x in uniq_b]
    table = np.empty((len(uniq_a), len(uniq_b)), dtype=np.float32)
    for i, ga in enumerate(bg_a):
        for j, gb in enumerate(bg_b):
            table[i, j] = 2 * len(ga & gb) / (len(ga) + len(gb)) if (ga or gb) else 0.0
    return table[codes_a[:, None], codes_b[None, :]]


def _numeric_matrix(a: pd.Series, b: pd.Series) -> np.ndarray:
    av = a.to_numpy(dtype=float)[:, None]
    bv = b.to_numpy(dtype=float)[None, :]
    lo, hi = np.minimum(av, bv), np.maximum(av, bv)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(hi > 0, lo / hi, 1.0).astype(np.float32)


def _geo_matrix(
    left: pd.DataFrame, right: pd.DataFrame
) -> tuple[np.ndarray, np.ndarray]:
    r = 6371000.0
    la1 = np.radians(left["lat"].to_numpy(dtype=float))[:, None]
    lo1 = np.radians(left["lon"].to_numpy(dtype=float))[:, None]
    la2 = np.radians(right["lat"].to_numpy(dtype=float))[None, :]
    lo2 = np.radians(right["lon"].to_numpy(dtype=float))[None, :]
    h = (
        np.sin((la2 - la1) / 2) ** 2
        + np.cos(la1) * np.cos(la2) * np.sin((lo2 - lo1) / 2) ** 2
    )
    dist = 2 * r * np.arcsin(np.sqrt(np.clip(h, 0, 1)))
    sim = np.clip(1 - dist / GEO_MAX_DISTANCE_M, 0.0, None)
    return sim.astype(np.float32), ~np.isnan(dist)


def _name_matrix(a: pd.Series, b: pd.Series, scorer) -> np.ndarray:
    av = a.fillna("").astype(str).str.lower().to_numpy()
    bv = b.fillna("").astype(str).str.lower().to_numpy()
    return (process.cdist(av, bv, scorer=scorer, workers=-1) / 100.0).astype(np.float32)


def _jw_tokenized_matrix(a: pd.Series, b: pd.Series) -> np.ndarray:
    """The production comparator, so the harness tunes what the engine runs."""
    sim, _ = engine_name_matrix(a.to_frame("Name"), b.to_frame("Name"), "Name", -1)
    return sim.astype(np.float32)


def _present(a: pd.Series, b: pd.Series) -> np.ndarray:
    return a.notna().to_numpy()[:, None] & b.notna().to_numpy()[None, :]


def _build_country_cache(
    country: str, left: pd.DataFrame, right: pd.DataFrame
) -> CountryCache:
    names = {
        key: _name_matrix(left.Name, right.Name, scorer)
        for key, scorer in NAME_SCORERS.items()
    }
    names[DEFAULT_NAME_MATRIX] = _jw_tokenized_matrix(left.Name, right.Name)
    sims = {col: _qgram_matrix(left[col], right[col]) for col in QGRAM_FIELDS}
    present = {col: _present(left[col], right[col]) for col in QGRAM_FIELDS}
    sims["Name"], present["Name"] = (
        names[DEFAULT_NAME_MATRIX],
        _present(left.Name, right.Name),
    )
    sims["Capacity"], present["Capacity"] = (
        _numeric_matrix(left.Capacity, right.Capacity),
        _present(left.Capacity, right.Capacity),
    )
    sims["geo"], present["geo"] = _geo_matrix(left, right)
    return CountryCache(
        country,
        left.projectID.to_numpy(),
        right.projectID.to_numpy(),
        names,
        sims,
        present,
    )


@dataclass
class Harness:
    caches: list[CountryCache]
    ground_truth: set[tuple[str, str]]

    def scores(
        self,
        cache: CountryCache,
        fields: Fields,
        name_scorer: str,
        geo_curve: tuple[float, float],
    ) -> np.ndarray:
        log_odds = np.zeros(cache.sims["geo"].shape, dtype=np.float32)
        for column, low, high in fields:
            sim = cache.names[name_scorer] if column == "Name" else cache.sims[column]
            if column == "geo" and geo_curve != (1.0, 0.0):
                sim = np.clip(geo_curve[0] * sim + geo_curve[1], 0.0, 1.0)
            p = np.clip(low + sim * (high - low), 1e-12, 1 - 1e-12)
            log_odds += np.where(
                cache.present[column], np.log(p / (1 - p)), 0.0
            ).astype(np.float32)
        return log_odds

    def predict(
        self,
        fields: Fields = BASELINE_FIELDS,
        threshold: float = LINKAGE_THRESHOLD,
        name_scorer: str = DEFAULT_NAME_MATRIX,
        geo_curve: tuple[float, float] = (1.0, 0.0),
    ) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        """Return the (raw, 1:1 reduced) predicted projectID pairs."""
        cut = np.log(threshold / (1 - threshold))
        raw, reduced = set(), set()
        for cache in self.caches:
            log_odds = self.scores(cache, fields, name_scorer, geo_curve)
            li, ri = np.nonzero(log_odds >= cut)
            if not len(li):
                continue
            raw.update(zip(cache.left_ids[li], cache.right_ids[ri]))
            best_r = log_odds.argmax(axis=1)
            best_s = log_odds[np.arange(len(best_r)), best_r]
            kept_l = np.nonzero(best_s >= cut)[0]
            kept_r, kept_s = best_r[kept_l], best_s[kept_l]
            order = np.lexsort((-kept_s, kept_r))
            first = np.concatenate([[True], np.diff(kept_r[order]) != 0])
            winners = order[first]
            reduced.update(
                zip(cache.left_ids[kept_l[winners]], cache.right_ids[kept_r[winners]])
            )
        return raw, reduced

    def evaluate(
        self,
        fields: Fields = BASELINE_FIELDS,
        threshold: float = LINKAGE_THRESHOLD,
        **kwargs,
    ) -> dict[str, float]:
        raw, reduced = self.predict(fields, threshold, **kwargs)
        truth = self.ground_truth
        hits = len(reduced & truth)
        precision = hits / len(reduced) if reduced else 0.0
        recall = hits / len(truth)
        f1 = 2 * precision * recall / (precision + recall) if hits else 0.0
        return {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "pre_reduction_recall": len(raw & truth) / len(truth),
            "n_predicted": len(reduced),
            "n_raw": len(raw),
        }


def prepare(config: dict | None = None, use_disk_cache: bool = True) -> Harness:
    """Load data, build (or restore) the per-country similarity caches."""
    if use_disk_cache and CACHE_FILE.exists():
        with CACHE_FILE.open("rb") as f:
            caches = [CountryCache(**entry) for entry in pickle.load(f)]
        return Harness(caches, load_ground_truth())

    config = config or pm.get_config()
    left, right = load_frames(config)
    caches = []
    for country in config["target_countries"]:
        sub_left, sub_right = (
            left[left.Country == country],
            right[right.Country == country],
        )
        if sub_left.empty or sub_right.empty:
            continue
        caches.append(_build_country_cache(country, sub_left, sub_right))
    if use_disk_cache:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with CACHE_FILE.open("wb") as f:
            pickle.dump([asdict(cache) for cache in caches], f)
    return Harness(caches, load_ground_truth())


def main() -> None:
    t0 = time.perf_counter()
    harness = prepare()
    print(f"prepared in {time.perf_counter() - t0:.1f} s")
    print(f"countries: {len(harness.caches)} {[c.country for c in harness.caches]}")
    print(f"candidate pairs: {sum(c.sims['geo'].size for c in harness.caches):,}")
    print(f"ground truth pairs: {len(harness.ground_truth)}")

    harness.evaluate()
    t0 = time.perf_counter()
    repeats = 5
    for _ in range(repeats):
        result = harness.evaluate()
    print(f"eval cost: {(time.perf_counter() - t0) / repeats * 1000:.0f} ms/config")
    print("baseline:", {k: round(v, 3) for k, v in result.items()})

    for scorer in [DEFAULT_NAME_MATRIX, *NAME_SCORERS]:
        res = harness.evaluate(name_scorer=scorer)
        print(
            f"  name scorer {scorer:17s} F1 {res['f1']:.3f} (P {res['precision']:.3f} / R {res['recall']:.3f})"
        )


if __name__ == "__main__":
    main()

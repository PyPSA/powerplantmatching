# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Pure-Python record-linkage backend mirroring the DUKE configs without a JVM.

Drop-in compatible with ``duke.duke``: same signature and return shapes, so it
feeds ``matching.compare_two_datasets`` and ``cleaning.aggregate_units``
unchanged. Pass a list of two frames for record linkage (Comparison.xml) or a
single frame for deduplication (Deleteduplicates.xml).

Scoring follows DUKE's Fellegi-Sunter belief update (0.5 prior, per-field
low/high probability bounds taken verbatim from the XML configs). Comparators
are vectorised: rapidfuzz token-set ratio for names (≈ JaroWinklerTokenized), a
factorised q-gram Dice for categorical fields, a min/max ratio for capacity
(NumericComparator) and a haversine linear falloff for position
(GeopositionComparator, 5 km cutoff).
"""

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)

GEO_MAX_DISTANCE_M = 5000.0


@dataclass
class FieldSpec:
    column: str
    kind: str  # "name" | "qgram" | "numeric" | "geo"
    low: float
    high: float


# Verbatim from package_data/Comparison.xml
LINKAGE_FIELDS = [
    FieldSpec("Name", "name", 0.09, 0.99),
    FieldSpec("Fueltype", "qgram", 0.09, 0.7),
    FieldSpec("Country", "qgram", 0.0, 0.53),
    FieldSpec("Capacity", "numeric", 0.3, 0.75),
    FieldSpec("Geoposition", "geo", 0.1, 0.8),
]
LINKAGE_THRESHOLD = 0.965

# Verbatim from package_data/Deleteduplicates.xml
DEDUP_FIELDS = [
    FieldSpec("Name", "name", 0.09, 0.99),
    FieldSpec("Fueltype", "qgram", 0.05, 0.65),
    FieldSpec("Technology", "qgram", 0.25, 0.51),
    FieldSpec("Country", "qgram", 0.05, 0.51),
    FieldSpec("Capacity", "numeric", 0.49, 0.51),
    FieldSpec("Geoposition", "geo", 0.05, 0.75),
]
DEDUP_THRESHOLD = 0.96


def _bigrams(s: str) -> set:
    return {s[i : i + 2] for i in range(len(s) - 1)} if len(s) > 1 else {s}


def _qgram_matrix(a: pd.Series, b: pd.Series) -> np.ndarray:
    """Dice coefficient over character bigrams, computed on unique values only."""
    ca, ua = pd.factorize(a.fillna("").astype(str).str.lower())
    cb, ub = pd.factorize(b.fillna("").astype(str).str.lower())
    bga, bgb = [_bigrams(x) for x in ua], [_bigrams(x) for x in ub]
    table = np.empty((len(ua), len(ub)))
    for i, ga in enumerate(bga):
        for j, gb in enumerate(bgb):
            table[i, j] = 2 * len(ga & gb) / (len(ga) + len(gb)) if (ga or gb) else 0.0
    return table[ca[:, None], cb[None, :]]


def _name_matrix(a: pd.Series, b: pd.Series) -> np.ndarray:
    av = a.fillna("").astype(str).str.lower().to_numpy()
    bv = b.fillna("").astype(str).str.lower().to_numpy()
    return process.cdist(av, bv, scorer=fuzz.token_set_ratio, workers=-1) / 100.0


def _numeric_matrix(a: pd.Series, b: pd.Series) -> np.ndarray:
    av = a.to_numpy(dtype=float)[:, None]
    bv = b.to_numpy(dtype=float)[None, :]
    lo, hi = np.minimum(av, bv), np.maximum(av, bv)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(hi > 0, lo / hi, 0.0)


def _geo_matrix(left: pd.DataFrame, right: pd.DataFrame):
    r = 6371000.0
    la1 = np.radians(left["lat"].to_numpy(dtype=float))[:, None]
    lo1 = np.radians(left["lon"].to_numpy(dtype=float))[:, None]
    la2 = np.radians(right["lat"].to_numpy(dtype=float))[None, :]
    lo2 = np.radians(right["lon"].to_numpy(dtype=float))[None, :]
    h = np.sin((la2 - la1) / 2) ** 2 + np.cos(la1) * np.cos(la2) * np.sin((lo2 - lo1) / 2) ** 2
    dist = 2 * r * np.arcsin(np.sqrt(np.clip(h, 0, 1)))
    sim = np.clip(1 - dist / GEO_MAX_DISTANCE_M, 0.0, None)
    return sim, ~np.isnan(dist)


def _present(a: pd.Series, b: pd.Series) -> np.ndarray:
    return a.notna().to_numpy()[:, None] & b.notna().to_numpy()[None, :]


def _field_contribution(spec: FieldSpec, left: pd.DataFrame, right: pd.DataFrame):
    if spec.kind == "name":
        return _name_matrix(left[spec.column], right[spec.column]), _present(left[spec.column], right[spec.column])
    if spec.kind == "qgram":
        return _qgram_matrix(left[spec.column], right[spec.column]), _present(left[spec.column], right[spec.column])
    if spec.kind == "numeric":
        return _numeric_matrix(left[spec.column], right[spec.column]), _present(left[spec.column], right[spec.column])
    if spec.kind == "geo":
        return _geo_matrix(left, right)
    raise ValueError(f"Unknown field kind {spec.kind}")


def _score_matrix(left: pd.DataFrame, right: pd.DataFrame, fields) -> np.ndarray:
    """DUKE-style Bayesian belief update over all candidate pairs, 0.5 prior."""
    prob = np.full((len(left), len(right)), 0.5)
    for spec in fields:
        sim, present = _field_contribution(spec, left, right)
        p = np.where(present, spec.low + sim * (spec.high - spec.low), 0.5)
        prob = (prob * p) / (prob * p + (1 - prob) * (1 - p))
    return prob


def _deduplicate(df: pd.DataFrame, labels, threshold: float) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=labels)
    scores = _score_matrix(df, df, DEDUP_FIELDS)
    iu, ju = np.triu_indices(len(df), k=1)
    keep = scores[iu, ju] >= threshold
    idx = df.index.to_numpy()
    a, b = idx[iu[keep]], idx[ju[keep]]
    return pd.DataFrame({labels[0]: np.concatenate([a, b]), labels[1]: np.concatenate([b, a])})


def duke(datasets, labels=["one", "two"], singlematch=False, threshold=None, **_):
    """recordlinkage equivalent of ``duke.duke`` (record linkage and dedup)."""
    if isinstance(datasets, pd.DataFrame):
        return _deduplicate(datasets, labels, DEDUP_THRESHOLD if threshold is None else threshold)

    left, right = datasets
    if left.empty or right.empty:
        return pd.DataFrame(columns=[*labels, "scores"])

    th = LINKAGE_THRESHOLD if threshold is None else threshold
    scores = _score_matrix(left, right, LINKAGE_FIELDS)
    li, ri = np.nonzero(scores >= th)
    res = pd.DataFrame(
        {
            labels[0]: left.index.to_numpy()[li],
            labels[1]: right.index.to_numpy()[ri],
            "scores": scores[li, ri],
        }
    )
    if singlematch and not res.empty:
        res = res.loc[res.groupby(labels[0])["scores"].idxmax()].reset_index(drop=True)
    return res.reset_index(drop=True)

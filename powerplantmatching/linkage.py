# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Vectorised record-linkage and deduplication engine.

``match`` takes a list of two frames for record linkage or a single frame for
deduplication and returns the matched index pairs. Scoring is a Fellegi-Sunter
belief update over a 0.5 prior: every field maps its similarity linearly onto
its ``[low, high]`` probability bounds and updates the running belief. The
comparators are vectorised: mean best-token Jaro-Winkler for names, a factorised
q-gram Dice for categorical fields, a min/max ratio for capacity and a haversine
linear falloff for position (5 km cutoff).

The bounds and thresholds are tuned against the GEO/GPD ground truth in
``analysis/benchmark_linkage.py``; they are properties of this scoring curve and
carry no meaning outside it.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd
from rapidfuzz import process
from rapidfuzz.distance import JaroWinkler

GEO_MAX_DISTANCE_M = 5000.0
BLOCK_CELLS = 2_000_000

Comparison = tuple[np.ndarray, np.ndarray]
Comparator = Callable[[pd.DataFrame, pd.DataFrame, str, int], Comparison]


def _strings(frame: pd.DataFrame, column: str) -> tuple[np.ndarray, np.ndarray]:
    values = frame[column].fillna("").astype(str).str.lower().to_numpy()
    return values, values != ""


def _bigrams(s: str) -> set[str]:
    return {s[i : i + 2] for i in range(len(s) - 1)} if len(s) > 1 else {s}


def _qgram_matrix(
    left: pd.DataFrame, right: pd.DataFrame, column: str, threads: int
) -> Comparison:
    """Dice coefficient over character bigrams, computed on unique values only."""
    av, present_a = _strings(left, column)
    bv, present_b = _strings(right, column)
    codes_a, uniq_a = pd.factorize(av)
    codes_b, uniq_b = pd.factorize(bv)
    grams_a, grams_b = [_bigrams(x) for x in uniq_a], [_bigrams(x) for x in uniq_b]
    table = np.empty((len(uniq_a), len(uniq_b)))
    for i, ga in enumerate(grams_a):
        for j, gb in enumerate(grams_b):
            table[i, j] = 2 * len(ga & gb) / (len(ga) + len(gb))
    return table[codes_a[:, None], codes_b[None, :]], present_a[:, None] & present_b[
        None, :
    ]


def _token_codes(values: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Pad the per-record token lists into a (records, width) vocabulary index."""
    tokens = [list(dict.fromkeys(v.split())) for v in values]
    vocabulary = pd.unique(np.array([t for ts in tokens for t in ts] or [""]))
    position = {token: i for i, token in enumerate(vocabulary)}
    width = max(max((len(ts) for ts in tokens), default=1), 1)
    codes = np.full((len(tokens), width), len(vocabulary), dtype=np.intp)
    counts = np.zeros(len(tokens), dtype=np.intp)
    for i, ts in enumerate(tokens):
        codes[i, : len(ts)] = [position[t] for t in ts]
        counts[i] = len(ts)
    return codes, counts, np.append(vocabulary, "")


def _name_matrix(
    left: pd.DataFrame, right: pd.DataFrame, column: str, threads: int
) -> Comparison:
    """Mean best-token Jaro-Winkler similarity over the longer token list.

    Character-level ratios cannot resolve unit designators -- ``token_set_ratio``
    scores "Doel 1" against "Doel 4" at 0.83 and "Neurath" against "Neurath F" at
    1.0 -- which merges the units of a station into a single record. Aligning
    token by token scores the mismatched designator at 0 instead.
    """
    av, present_a = _strings(left, column)
    bv, present_b = _strings(right, column)
    codes_a, counts_a, vocabulary_a = _token_codes(av)
    codes_b, counts_b, vocabulary_b = _token_codes(bv)
    tokens = process.cdist(
        vocabulary_a, vocabulary_b, scorer=JaroWinkler.similarity, workers=threads
    )
    tokens[-1, :] = tokens[:, -1] = 0.0
    best = np.zeros((len(vocabulary_a), len(bv)))
    for j in range(codes_b.shape[1]):
        np.maximum(best, tokens[:, codes_b[:, j]], out=best)
    total = np.zeros((len(av), len(bv)))
    for k in range(codes_a.shape[1]):
        total += np.where(counts_a[:, None] > k, best[codes_a[:, k]], 0.0)
    width = np.maximum(counts_a[:, None], counts_b[None, :])
    sim = np.divide(total, width, out=np.zeros_like(total), where=width > 0)
    return sim, present_a[:, None] & present_b[None, :]


def _numeric_matrix(
    left: pd.DataFrame, right: pd.DataFrame, column: str, threads: int
) -> Comparison:
    av = left[column].to_numpy(dtype=float)[:, None]
    bv = right[column].to_numpy(dtype=float)[None, :]
    lo, hi = np.minimum(av, bv), np.maximum(av, bv)
    with np.errstate(invalid="ignore", divide="ignore"):
        sim = np.where(hi > 0, lo / hi, 1.0)
    return sim, ~np.isnan(av) & ~np.isnan(bv)


def _geo_matrix(
    left: pd.DataFrame, right: pd.DataFrame, column: str = "geo", threads: int = -1
) -> Comparison:
    """Haversine falloff on ``lat``/``lon``; ``column`` is a label, not a column."""
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
    return np.clip(1 - dist / GEO_MAX_DISTANCE_M, 0.0, None), ~np.isnan(dist)


@dataclass(frozen=True)
class FieldSpec:
    column: str
    compare: Comparator
    low: float
    high: float


LINKAGE_FIELDS = [
    FieldSpec("Name", _name_matrix, 0.09, 0.99),
    FieldSpec("Fueltype", _qgram_matrix, 0.09, 0.7),
    FieldSpec("Country", _qgram_matrix, 0.0, 0.53),
    FieldSpec("Capacity", _numeric_matrix, 0.3, 0.75),
    FieldSpec("geo", _geo_matrix, 0.1, 0.8),
]
LINKAGE_THRESHOLD = 0.85

DEDUP_FIELDS = [
    FieldSpec("Name", _name_matrix, 0.09, 0.99),
    FieldSpec("Fueltype", _qgram_matrix, 0.05, 0.65),
    FieldSpec("Technology", _qgram_matrix, 0.25, 0.51),
    FieldSpec("Country", _qgram_matrix, 0.05, 0.51),
    FieldSpec("Capacity", _numeric_matrix, 0.49, 0.51),
    FieldSpec("geo", _geo_matrix, 0.05, 0.75),
]
DEDUP_THRESHOLD = 0.96


def _accepted_pairs(
    left: pd.DataFrame,
    right: pd.DataFrame,
    fields: Sequence[FieldSpec],
    threshold: float,
    threads: int,
    upper_triangle: bool,
) -> list[tuple[np.ndarray, np.ndarray, np.ndarray]]:
    """Fellegi-Sunter belief update per row block, kept below ``BLOCK_CELLS`` cells."""
    rows_per_block = max(1, BLOCK_CELLS // max(len(right), 1))
    column_index = np.arange(len(right))
    blocks = []
    for start in range(0, len(left), rows_per_block):
        block = left.iloc[start : start + rows_per_block]
        prob = np.full((len(block), len(right)), 0.5)
        for spec in fields:
            sim, present = spec.compare(block, right, spec.column, threads)
            p = np.where(present, spec.low + sim * (spec.high - spec.low), 0.5)
            prob = (prob * p) / (prob * p + (1 - prob) * (1 - p))
        keep = prob >= threshold
        if upper_triangle:
            keep &= column_index[None, :] > (start + np.arange(len(block)))[:, None]
        li, ri = np.nonzero(keep)
        blocks.append((li + start, ri, prob[li, ri]))
    return blocks


def _stack(parts: list[np.ndarray], dtype: type) -> np.ndarray:
    return np.concatenate(parts) if parts else np.empty(0, dtype=dtype)


def _deduplicate(
    df: pd.DataFrame, labels: Sequence[str], threshold: float, threads: int
) -> pd.DataFrame:
    blocks = _accepted_pairs(
        df, df, DEDUP_FIELDS, threshold, threads, upper_triangle=True
    )
    idx = df.index.to_numpy()
    a = idx[_stack([li for li, _, _ in blocks], int)]
    b = idx[_stack([ri for _, ri, _ in blocks], int)]
    return pd.DataFrame(
        {labels[0]: np.concatenate([a, b]), labels[1]: np.concatenate([b, a])}
    )


def match(
    datasets: pd.DataFrame | Sequence[pd.DataFrame],
    labels: Sequence[str] = ("one", "two"),
    singlematch: bool = False,
    threshold: float | None = None,
    threads: int = -1,
) -> pd.DataFrame:
    """
    Link two datasets or deduplicate one.

    A single frame is deduplicated (returns reciprocal index pairs as
    ``cliques()`` requires); a list of two frames is linked and additionally
    carries a ``scores`` column. In record linkage pass ``singlematch=True`` and
    reduce with ``best_matches()`` afterwards. ``threshold`` overrides the tuned
    acceptance probability (``DEDUP_THRESHOLD`` / ``LINKAGE_THRESHOLD``);
    ``threads`` is the rapidfuzz worker count, ``-1`` meaning all cores.
    """
    if isinstance(datasets, pd.DataFrame):
        cut = DEDUP_THRESHOLD if threshold is None else threshold
        return _deduplicate(datasets, labels, cut, threads)

    left, right = datasets
    cut = LINKAGE_THRESHOLD if threshold is None else threshold
    empty = left.empty or right.empty
    blocks = (
        []
        if empty
        else _accepted_pairs(
            left, right, LINKAGE_FIELDS, cut, threads, upper_triangle=False
        )
    )
    res = pd.DataFrame(
        {
            labels[0]: left.index.to_numpy()[_stack([li for li, _, _ in blocks], int)],
            labels[1]: right.index.to_numpy()[_stack([ri for _, ri, _ in blocks], int)],
            "scores": _stack([s for _, _, s in blocks], float),
        }
    )
    if singlematch and not res.empty:
        res = res.loc[res.groupby(labels[0])["scores"].idxmax()]
    return res.reset_index(drop=True)

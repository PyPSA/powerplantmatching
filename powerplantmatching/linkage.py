# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Vectorised record-linkage and deduplication engine.

``match`` takes a list of two frames for record linkage or a single frame for
deduplication and returns the matched index pairs. Scoring is a Fellegi-Sunter
belief update over a 0.5 prior: every field maps its similarity linearly onto
its ``[low, high]`` probability bounds and updates the running belief. The
comparators are vectorised: symmetric best-token Jaro-Winkler for names, a factorised
q-gram Dice for categorical fields, a min/max ratio for capacity and a haversine
linear falloff for position (5 km cutoff).

The bounds and thresholds originate in PR #301's GEO/GPD benchmark. Symmetric
name scoring and one-to-one assignment change its results and require separate
dataset validation before claiming the benchmark's precision or recall.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd
from rapidfuzz import process
from rapidfuzz.distance import JaroWinkler
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import min_weight_full_bipartite_matching

GEO_MAX_DISTANCE_M = 5000.0
BLOCK_CELLS = 2_000_000
EARTH_RADIUS_M = 6_371_000.0
UNMATCHED_COST = 2.0

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
    tokens = pd.Series(values).str.split().explode().dropna()
    records = tokens.rename_axis("record").reset_index(name="token").drop_duplicates()
    flat_codes, vocabulary = pd.factorize(records.token)
    row_indices = records.record.to_numpy(dtype=np.intp)
    positions = records.groupby("record").cumcount().to_numpy(dtype=np.intp)
    counts = np.bincount(row_indices, minlength=len(values))
    width = max(int(counts.max(initial=0)), 1)
    codes = np.full((len(values), width), len(vocabulary), dtype=np.intp)
    codes[row_indices, positions] = flat_codes
    return codes, counts, np.append(vocabulary, "")


def _name_matrix(
    left: pd.DataFrame, right: pd.DataFrame, column: str, threads: int
) -> Comparison:
    """Use the lower directional token total over the longer token list.

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
    forward = _token_totals(tokens, codes_a, counts_a, codes_b)
    reverse = _token_totals(tokens.T, codes_b, counts_b, codes_a).T
    total = np.minimum(forward, reverse)
    width = np.maximum(counts_a[:, None], counts_b[None, :])
    sim = np.divide(total, width, out=np.zeros_like(total), where=width > 0)
    return sim, present_a[:, None] & present_b[None, :]


def _token_totals(
    similarities: np.ndarray,
    left_codes: np.ndarray,
    left_counts: np.ndarray,
    right_codes: np.ndarray,
) -> np.ndarray:
    """Sum best token matches in one direction without iterating record pairs."""
    best = np.zeros((similarities.shape[0], len(right_codes)))
    for token in range(right_codes.shape[1]):
        np.maximum(best, similarities[:, right_codes[:, token]], out=best)
    total = np.zeros((len(left_codes), len(right_codes)))
    for token in range(left_codes.shape[1]):
        total += np.where(left_counts[:, None] > token, best[left_codes[:, token]], 0.0)
    return total


def _numeric_matrix(
    left: pd.DataFrame, right: pd.DataFrame, column: str, threads: int
) -> Comparison:
    av = left[column].to_numpy(dtype=float)[:, None]
    bv = right[column].to_numpy(dtype=float)[None, :]
    lo, hi = np.minimum(av, bv), np.maximum(av, bv)
    with np.errstate(invalid="ignore", divide="ignore"):
        sim = np.where(hi > 0, lo / hi, 1.0)
    return sim, np.isfinite(av) & np.isfinite(bv) & (av >= 0) & (bv >= 0)


def _geo_matrix(
    left: pd.DataFrame, right: pd.DataFrame, column: str = "geo", threads: int = -1
) -> Comparison:
    """Haversine falloff on ``lat``/``lon``; ``column`` is a label, not a column."""
    la1 = np.radians(left["lat"].to_numpy(dtype=float))[:, None]
    lo1 = np.radians(left["lon"].to_numpy(dtype=float))[:, None]
    la2 = np.radians(right["lat"].to_numpy(dtype=float))[None, :]
    lo2 = np.radians(right["lon"].to_numpy(dtype=float))[None, :]
    h = (
        np.sin((la2 - la1) / 2) ** 2
        + np.cos(la1) * np.cos(la2) * np.sin((lo2 - lo1) / 2) ** 2
    )
    dist = 2 * EARTH_RADIUS_M * np.arcsin(np.sqrt(np.clip(h, 0, 1)))
    return np.clip(1 - dist / GEO_MAX_DISTANCE_M, 0.0, None), np.isfinite(dist)


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


def select_one_to_one(links: pd.DataFrame) -> pd.DataFrame:
    """Maximize total accepted score with sparse assignment and unmatched rows.

    Sort source labels and identifiers before assignment so source reversal and
    input row order cannot change tie resolution. Dummy columns allow every row
    to remain unmatched without adding absent candidate edges.
    """
    if links.empty:
        return links.copy()
    labels = sorted(links.columns.difference(["scores"]))
    if len(labels) != 2:
        raise ValueError("One-to-one selection requires two source columns")
    candidates = links.groupby(labels, as_index=False, sort=True).scores.max()
    scores = candidates.scores.to_numpy(dtype=float)
    if not (np.isfinite(scores) & (scores >= 0) & (scores <= 1)).all():
        raise ValueError("Matching scores must be finite and between zero and one")
    rows, left_ids = pd.factorize(candidates[labels[0]], sort=True)
    cols, right_ids = pd.factorize(candidates[labels[1]], sort=True)
    unmatched = np.arange(len(left_ids))
    graph = coo_matrix(
        (
            np.concatenate(
                [UNMATCHED_COST - scores, np.full(len(left_ids), UNMATCHED_COST)]
            ),
            (
                np.concatenate([rows, unmatched]),
                np.concatenate([cols, len(right_ids) + unmatched]),
            ),
        ),
        shape=(len(left_ids), len(right_ids) + len(left_ids)),
    ).tocsr()
    chosen_rows, chosen_cols = min_weight_full_bipartite_matching(graph)
    matched = chosen_cols < len(right_ids)
    pairs = pd.DataFrame(
        {
            labels[0]: left_ids[chosen_rows[matched]],
            labels[1]: right_ids[chosen_cols[matched]],
        }
    )
    return pairs.merge(candidates, on=labels, validate="one_to_one")[links.columns]


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
    carries a ``scores`` column. ``singlematch=True`` selects a maximum-weight
    one-to-one assignment. ``threshold`` overrides the inherited
    acceptance probability (``DEDUP_THRESHOLD`` / ``LINKAGE_THRESHOLD``);
    ``threads`` is the rapidfuzz worker count, ``-1`` meaning all cores.
    """
    if len(labels) != 2 or labels[0] == labels[1] or "scores" in labels:
        raise ValueError(
            "Matching requires two distinct source labels other than scores"
        )
    frames = [datasets] if isinstance(datasets, pd.DataFrame) else list(datasets)
    if any(not frame.index.is_unique for frame in frames):
        raise ValueError("Matching requires a unique index in each source")
    if threshold is not None and not 0 <= threshold <= 1:
        raise ValueError("Matching threshold must be between zero and one")
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
    if singlematch:
        res = select_one_to_one(res)
    return res.reset_index(drop=True)

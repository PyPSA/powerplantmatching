# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Prototype record-linkage backend that mirrors the DUKE Comparison.xml model
without a JVM dependency. Drop-in compatible with ``duke.duke`` in terms of the
returned link DataFrame, so it can feed ``matching.compare_two_datasets``.
"""

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
import recordlinkage as rl

logger = logging.getLogger(__name__)


@dataclass
class Field:
    column: str
    method: str
    low: float
    high: float


# Mirrors powerplantmatching/package_data/Comparison.xml
FIELDS = [
    Field("Name", "jarowinkler", 0.09, 0.99),
    Field("Fueltype", "qgram", 0.09, 0.7),
    Field("Country", "qgram", 0.0, 0.53),
    Field("Capacity", "numeric", 0.3, 0.75),
    Field("Geoposition", "geo", 0.1, 0.8),
]
THRESHOLD = 0.965
GEO_MAX_DISTANCE_M = 5000.0


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Replicate DUKE's LowerCaseNormalizeCleaner (lowercase, collapse whitespace)."""
    df = df.copy()
    for col in ("Name", "Fueltype"):
        s = df[col].astype("string").str.lower().str.replace(r"\s+", " ", regex=True)
        df[col] = s.str.strip()
    return df


def _capacity_similarity(a: pd.Series, b: pd.Series) -> pd.Series:
    """DUKE NumericComparator: ratio of the smaller to the larger value."""
    lo = np.minimum(a, b)
    hi = np.maximum(a, b)
    sim = lo / hi
    return sim.where(hi > 0, 0.0)


def _geo_similarity(
    lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series
) -> pd.Series:
    """DUKE GeopositionComparator: linear falloff to 0 at max-distance."""
    r = 6371000.0
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlmb = np.radians(lon2 - lon1)
    h = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlmb / 2) ** 2
    dist = 2 * r * np.arcsin(np.sqrt(h))
    return (1 - dist / GEO_MAX_DISTANCE_M).clip(lower=0.0)


def _field_similarity(field: Field, left: pd.DataFrame, right: pd.DataFrame, pairs):
    if field.method in ("jarowinkler", "qgram"):
        a = left[field.column].reindex(pairs.get_level_values(0)).reset_index(drop=True)
        b = right[field.column].reindex(pairs.get_level_values(1)).reset_index(drop=True)
        comparer = rl.Compare()
        comparer.string(field.column, field.column, method=field.method, label="s")
        sim = comparer.compute(pairs, left, right)["s"].reset_index(drop=True)
        present = a.notna() & b.notna()
        return sim, present
    if field.method == "numeric":
        a = left[field.column].reindex(pairs.get_level_values(0)).reset_index(drop=True)
        b = right[field.column].reindex(pairs.get_level_values(1)).reset_index(drop=True)
        sim = _capacity_similarity(a, b)
        return sim, (a.notna() & b.notna())
    if field.method == "geo":
        la = left["lat"].reindex(pairs.get_level_values(0)).reset_index(drop=True)
        lo = left["lon"].reindex(pairs.get_level_values(0)).reset_index(drop=True)
        ra = right["lat"].reindex(pairs.get_level_values(1)).reset_index(drop=True)
        ro = right["lon"].reindex(pairs.get_level_values(1)).reset_index(drop=True)
        sim = _geo_similarity(la, lo, ra, ro)
        present = la.notna() & lo.notna() & ra.notna() & ro.notna()
        return sim, present
    raise ValueError(f"Unknown method {field.method}")


def _fellegi_sunter(left: pd.DataFrame, right: pd.DataFrame, pairs) -> pd.Series:
    """DUKE-style Bayesian belief update, starting from a 0.5 prior."""
    prob = pd.Series(0.5, index=range(len(pairs)))
    for field in FIELDS:
        sim, present = _field_similarity(field, left, right, pairs)
        p = field.low + sim * (field.high - field.low)
        p = p.where(present, 0.5)
        prob = (prob * p) / (prob * p + (1 - prob) * (1 - p))
    return prob


def duke(
    datasets,
    labels=["one", "two"],
    singlematch=False,
    threshold=THRESHOLD,
    **_,
):
    """recordlinkage equivalent of ``duke.duke`` (record-linkage mode only)."""
    if isinstance(datasets, pd.DataFrame):
        raise NotImplementedError("Dedup mode not implemented in the prototype")

    left, right = datasets
    if left.empty or right.empty:
        return pd.DataFrame(columns=[*labels, "scores"])

    left, right = _normalize(left), _normalize(right)

    pairs = rl.index.Full().index(left, right)
    scores = _fellegi_sunter(left, right, pairs)

    keep = scores >= threshold
    res = pd.DataFrame(
        {
            labels[0]: pairs.get_level_values(0)[keep.values],
            labels[1]: pairs.get_level_values(1)[keep.values],
            "scores": scores[keep].values,
        }
    )

    if singlematch:
        res = res.loc[res.groupby(labels[0])["scores"].idxmax()].reset_index(drop=True)
    return res.reset_index(drop=True)

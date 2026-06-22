# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Splink-backed record-linkage and deduplication engine.

``match`` takes a list of two frames for record linkage or a single frame for
deduplication and returns the matched index pairs. It runs a fixed-parameter
`Splink <https://moj-analytical-services.github.io/splink>`_ (Fellegi-Sunter)
model on DuckDB: the per-field ``m``/``u`` probabilities are set directly from
the former DUKE field weights (``Comparison.xml`` / ``Deleteduplicates.xml``),
so the model needs no per-call EM training and is fully deterministic.

Comparators: Jaro-Winkler thresholds for names, exact match for categorical
fields, a min/max ratio for capacity and a haversine distance ladder for
position. A 0.5 prior reproduces DUKE's belief update; only the relative
Bayes factor between levels matters for ranking, so the absolute match
probability is calibrated through ``LINKAGE_THRESHOLD`` / ``DEDUP_THRESHOLD``.
"""

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from splink import DuckDBAPI, Linker

logger = logging.getLogger(__name__)
logging.getLogger("splink").setLevel(logging.ERROR)

_HAVERSINE_KM = (
    '2*6371*asin(sqrt(pow(sin(radians(("lat_r"-"lat_l"))/2),2)'
    '+cos(radians("lat_l"))*cos(radians("lat_r"))'
    '*pow(sin(radians(("lon_r"-"lon_l"))/2),2)))'
)


@dataclass
class FieldSpec:
    """A field comparison expressed as similarity levels with DUKE-derived probabilities.

    ``levels`` maps each SQL agreement condition to the DUKE agreement
    probability ``p`` for records that reach it (high for strong agreement, low
    for the catch-all ``ELSE`` level).
    """

    output: str
    null_sql: str
    levels: list  # [(sql_condition, p)], last entry is the ELSE level

    def comparison(self) -> dict:
        sqls = [sql for sql, _ in self.levels]
        ps = np.array([p for _, p in self.levels], dtype=float)
        m = ps / ps.sum()
        u = (1 - ps) / (1 - ps).sum()
        graded = [
            {
                "sql_condition": sql,
                "label_for_charts": sql,
                "m_probability": float(mi),
                "u_probability": float(ui),
            }
            for sql, mi, ui in zip(sqls, m, u)
        ]
        null_level = {
            "sql_condition": self.null_sql,
            "label_for_charts": "null",
            "is_null_level": True,
        }
        return {"output_column_name": self.output, "comparison_levels": [null_level, *graded]}


def _name(low: float, high: float) -> FieldSpec:
    return FieldSpec(
        "Name",
        '"Name_l" IS NULL OR "Name_r" IS NULL',
        [
            ('jaro_winkler_similarity("Name_l","Name_r") >= 0.95', high),
            ('jaro_winkler_similarity("Name_l","Name_r") >= 0.85', low + 0.7 * (high - low)),
            ('jaro_winkler_similarity("Name_l","Name_r") >= 0.7', low + 0.35 * (high - low)),
            ("ELSE", low),
        ],
    )


def _exact(col: str, low: float, high: float) -> FieldSpec:
    return FieldSpec(
        col,
        f'"{col}_l" IS NULL OR "{col}_r" IS NULL',
        [(f'"{col}_l" = "{col}_r"', high), ("ELSE", low)],
    )


def _capacity(low: float, high: float) -> FieldSpec:
    ratio = 'least("Capacity_l","Capacity_r")/nullif(greatest("Capacity_l","Capacity_r"),0)'
    return FieldSpec(
        "Capacity",
        '"Capacity_l" IS NULL OR "Capacity_r" IS NULL',
        [
            (f"{ratio} >= 0.95", high),
            (f"{ratio} >= 0.8", low + 0.5 * (high - low)),
            ("ELSE", low),
        ],
    )


def _geo(low: float, high: float) -> FieldSpec:
    return FieldSpec(
        "geo",
        '"lat_l" IS NULL OR "lat_r" IS NULL OR "lon_l" IS NULL OR "lon_r" IS NULL',
        [
            (f"{_HAVERSINE_KM} <= 1", high),
            (f"{_HAVERSINE_KM} <= 5", low + 0.7 * (high - low)),
            ("ELSE", low),
        ],
    )


LINKAGE_FIELDS = [
    _name(0.09, 0.99),
    _exact("Fueltype", 0.09, 0.7),
    _exact("Country", 0.01, 0.53),
    _capacity(0.3, 0.75),
    _geo(0.1, 0.8),
]
LINKAGE_THRESHOLD = 0.7

DEDUP_FIELDS = [
    _name(0.09, 0.99),
    _exact("Fueltype", 0.05, 0.65),
    _exact("Technology", 0.25, 0.51),
    _exact("Country", 0.05, 0.51),
    _capacity(0.49, 0.51),
    _geo(0.05, 0.75),
]
DEDUP_THRESHOLD = 0.95


def _settings(link_type: str, fields) -> dict:
    return {
        "link_type": link_type,
        "probability_two_random_records_match": 0.5,
        "unique_id_column_name": "id",
        "blocking_rules_to_generate_predictions": [{"blocking_rule": "1=1"}],
        "comparisons": [f.comparison() for f in fields],
        "retain_matching_columns": False,
        "retain_intermediate_calculation_columns": False,
    }


def _predict(tables, link_type, fields, threshold) -> pd.DataFrame:
    aliases = ["df_left", "df_right"] if isinstance(tables, list) else None
    linker = Linker(tables, _settings(link_type, fields), DuckDBAPI(), input_table_aliases=aliases)
    return linker.inference.predict(threshold_match_probability=threshold).as_pandas_dataframe()


def _deduplicate(df: pd.DataFrame, labels, threshold: float) -> pd.DataFrame:
    if len(df) < 2:
        return pd.DataFrame(columns=labels)
    table = df.assign(id=df.index)
    pred = _predict(table, "dedupe_only", DEDUP_FIELDS, threshold)
    if pred.empty:
        return pd.DataFrame(columns=labels)
    a, b = pred["id_l"].to_numpy(), pred["id_r"].to_numpy()
    return pd.DataFrame(
        {labels[0]: np.concatenate([a, b]), labels[1]: np.concatenate([b, a])}
    )


def match(datasets, labels=["one", "two"], singlematch=False, threshold=None, **_):
    """
    Link two datasets or deduplicate one.

    A single frame is deduplicated (returns reciprocal index pairs as
    ``cliques()`` requires); a list of two frames is linked. In record linkage
    pass ``singlematch=True`` and reduce with ``best_matches()`` afterwards.
    """
    if isinstance(datasets, pd.DataFrame):
        th = DEDUP_THRESHOLD if threshold is None else threshold
        return _deduplicate(datasets, labels, th)

    left, right = datasets
    if left.empty or right.empty:
        return pd.DataFrame(columns=[*labels, "scores"])

    th = LINKAGE_THRESHOLD if threshold is None else threshold
    tables = [left.assign(id=left.index), right.assign(id=right.index)]
    pred = _predict(tables, "link_only", LINKAGE_FIELDS, th)
    if pred.empty:
        return pd.DataFrame(columns=[*labels, "scores"])

    from_left = pred["source_dataset_l"].to_numpy() == "df_left"
    res = pd.DataFrame(
        {
            labels[0]: np.where(from_left, pred["id_l"], pred["id_r"]),
            labels[1]: np.where(from_left, pred["id_r"], pred["id_l"]),
            "scores": pred["match_probability"].to_numpy(),
        }
    )
    if singlematch and not res.empty:
        res = res.loc[res.groupby(labels[0])["scores"].idxmax()].reset_index(drop=True)
    return res.reset_index(drop=True)

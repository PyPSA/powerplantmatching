# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

from io import StringIO

import numpy as np
import pandas as pd
import pytest

from powerplantmatching.cleaning import aggregate_units
from powerplantmatching.core import get_config
from powerplantmatching.matching import _match_by_eic, reduce_matched_dataframe
from powerplantmatching.utils import parse_string_to_dict


@pytest.mark.parametrize(
    "left, right, expected",
    [
        # partially overlapping collections, a record without codes
        ([{"EMSA"}, {"8xG", "8xK"}, {"MVSQ"}], [{"EMSA"}, {"8xG"}, set()], [0, 1]),
        (["C1", {"C2"}], [["C1"], {"C2"}], [0, 1]),
        ([{np.nan}, {"C1"}], [{np.nan}, {"C1"}], [1]),
        ([None], [None], []),
        # a code shared by several stations must not pick one arbitrarily
        ([{"C1", "C2"}], [{"C1"}, {"C2"}], []),
        ([["C1", "C2"], ["SCHEME"]], [["C1"], ["SCHEME"], ["SCHEME"]], [0]),
    ],
)
def test_eic_matching(left, right, expected):
    left = pd.DataFrame({"EIC": left}, index=np.arange(len(left)) + 10)
    right = pd.DataFrame({"EIC": right}, index=np.arange(len(right)) + 20)
    matches, idx0, idx1 = _match_by_eic(left, right, ["A", "B"])
    pairs = [(i + 10, i + 20) for i in expected]
    assert list(matches.itertuples(index=False, name=None)) == pairs
    assert idx0 == {a for a, _ in pairs} and idx1 == {b for _, b in pairs}


def test_eic_matching_without_eic_column():
    left = pd.DataFrame({"Name": ["Plant A"]})
    right = pd.DataFrame({"EIC": [{"C1"}]})
    matches, idx0, idx1 = _match_by_eic(left, right, ["A", "B"])
    assert matches.empty and not idx0 and not idx1


def test_eic_matching_rejects_duplicate_index():
    left = pd.DataFrame({"EIC": [["C1"], ["C2"]]}, index=[0, 0])
    right = pd.DataFrame({"EIC": [["C1"]]})
    with pytest.raises(ValueError, match="unique index"):
        _match_by_eic(left, right, ["A", "B"])


def test_eic_matching_through_linkage_table():
    """Translated links must be one to one, share the fuel and be within 20%."""
    left = pd.DataFrame(
        {
            "EIC": ["E1", "E2", "E3", "E4", "E5"],
            "Fueltype": ["Hard Coal"] * 4 + ["Natural Gas"],
            "Capacity": 100,
        },
        index=[10, 20, 30, 40, 50],
    )
    right = pd.DataFrame(
        {
            "projectID": [{"WRI1"}, {"WRI2"}, {"WRI4"}, {"WRI5"}],
            "EIC": None,
            "Fueltype": ["Hard Coal", "Hard Coal", "Hard Coal", "Hard Coal"],
            "Capacity": [110, 100, 300, 100],
        },
        index=[60, 70, 80, 90],
    )
    linkages = pd.DataFrame(
        [
            ("E1", "GPD", "WRI1"),
            ("E2", "GPD", "WRI2"),
            ("E3", "GPD", "WRI2"),
            ("E4", "GPD", "WRI4"),
            ("E5", "GPD", "WRI5"),
        ],
        columns=["EIC", "Source", "projectID"],
    )
    matches, idx0, idx1 = _match_by_eic(left, right, ["ENTSOE", "GPD"], linkages)
    assert matches.to_dict("records") == [{"ENTSOE": 10, "GPD": 60}]
    assert idx0 == {10} and idx1 == {60}


def test_eic_linkage_never_links_two_listed_sources():
    left = pd.DataFrame({"projectID": [{"WRI1"}]})
    right = pd.DataFrame({"projectID": [{"GEO-1"}]})
    linkages = pd.DataFrame(
        [("E1", "GPD", "WRI1"), ("E1", "GEO", "GEO-1")],
        columns=["EIC", "Source", "projectID"],
    )
    matches, _, _ = _match_by_eic(left, right, ["GPD", "GEO"], linkages)
    assert matches.empty


@pytest.mark.parametrize("codes", [["C2", "C1", "C1"], {"C1", "C2"}, "C1"])
def test_reduce_preserves_identifiers_across_sources(codes):
    """A higher-priority source without EICs cannot erase known identifiers."""
    columns = ["Name", "Fueltype", "Technology", "Set", "EIC"]
    values = ["Plant", "Natural Gas", "CCGT", "PP"]
    frame = pd.DataFrame(
        [[v for v in values for _ in range(2)] + [codes, None]],
        columns=pd.MultiIndex.from_product([columns, ["ENTSOE", "GEM"]]),
    )
    config = {
        "target_columns": columns,
        "ENTSOE": {"reliability_score": 5},
        "GEM": {"reliability_score": 6},
    }
    result = reduce_matched_dataframe(frame, config=config)
    assert result.EIC.iloc[0] == (["C1"] if isinstance(codes, str) else ["C1", "C2"])


@pytest.mark.parametrize(
    "codes, expected",
    [(["C2", "C1", "C1", None, np.nan, "", 42], ["C1", "C2"]), (None, [])],
)
def test_aggregate_identifiers_survive_cache_roundtrip(codes, expected):
    """Unit aggregation and cached reload retain the same usable identifiers."""
    config = get_config()
    unit = {
        "Name": "Alpha Plant",
        "Fueltype": "Natural Gas",
        "Technology": "CCGT",
        "Set": "PP",
        "Country": "Netherlands",
        "Capacity": 100.0,
        "lat": 53.0,
        "lon": 6.0,
        "EIC": codes,
        "projectID": "unit-1",
    }
    units = pd.DataFrame([unit]).reindex(columns=config["target_columns"])
    aggregated = aggregate_units(units, dataset_name="test", config=config)
    assert aggregated.EIC.iloc[0] == expected
    cached = pd.read_csv(StringIO(aggregated.to_csv(index=False)))
    assert parse_string_to_dict(cached, ["EIC"]).EIC.iloc[0] == expected

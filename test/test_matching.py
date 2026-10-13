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


@pytest.fixture
def df_entsoe():
    """ENTSOE-like dataset with EIC codes as sets."""
    return pd.DataFrame(
        {
            "Name": ["Eemshavencentrale", "Eemscentrale", "Maasvlakte"],
            "Fueltype": ["Hard Coal", "Natural Gas", "Hard Coal"],
            "Country": ["Netherlands", "Netherlands", "Netherlands"],
            "Capacity": [1560.0, 2200.0, 1040.0],
            "EIC": [
                {"49W000000000EMSA"},
                {"49W00000000008xG", "49W00000000008xK"},
                {"49W000000000MVSQ"},
            ],
            "lat": [53.44, 53.44, 51.95],
            "lon": [6.83, 6.84, 4.03],
        }
    )


@pytest.fixture
def df_opsd():
    """OPSD-like dataset with EIC codes as sets."""
    return pd.DataFrame(
        {
            "Name": ["Eemshaven coal", "Eems gas", "Rijnmond"],
            "Fueltype": ["Hard Coal", "Natural Gas", "Natural Gas"],
            "Country": ["Netherlands", "Netherlands", "Netherlands"],
            "Capacity": [1560.0, 2200.0, 800.0],
            "EIC": [
                {"49W000000000EMSA"},
                {"49W00000000008xG"},
                set(),  # Rijnmond has no EIC
            ],
            "lat": [53.44, 53.44, 51.88],
            "lon": [6.83, 6.84, 4.50],
        }
    )


def test_eic_matching_basic(df_entsoe, df_opsd):
    """EIC matching correctly pairs plants sharing EIC codes."""
    labels = ["ENTSOE", "OPSD"]
    matches, idx0, idx1 = _match_by_eic(df_entsoe, df_opsd, labels)

    # Eemshavencentrale (0) ↔ Eemshaven coal (0) via EMSA
    # Eemscentrale (1) ↔ Eems gas (1) via 008xG
    assert len(matches) == 2
    assert set(idx0) == {0, 1}
    assert set(idx1) == {0, 1}

    # Maasvlakte (2) and Rijnmond (2) should NOT match (no shared EIC)
    assert 2 not in idx0
    assert 2 not in idx1


def test_eic_matching_no_eic_column():
    """Gracefully handles datasets without EIC column."""
    df0 = pd.DataFrame({"Name": ["Plant A"], "Capacity": [100]})
    df1 = pd.DataFrame({"Name": ["Plant B"], "Capacity": [200], "EIC": [{"CODE1"}]})

    matches, idx0, idx1 = _match_by_eic(df0, df1, ["A", "B"])
    assert matches.empty
    assert len(idx0) == 0


def test_eic_matching_empty_sets():
    """No matches when all EIC sets are empty."""
    df0 = pd.DataFrame({"Name": ["A"], "EIC": [set()]})
    df1 = pd.DataFrame({"Name": ["B"], "EIC": [set()]})

    matches, _, _ = _match_by_eic(df0, df1, ["X", "Y"])
    assert matches.empty


def test_eic_matching_nan_values():
    """Float nan inside EIC sets does not produce false matches."""
    df0 = pd.DataFrame({"Name": ["A", "B"], "EIC": [{np.nan}, {"CODE1"}]})
    df1 = pd.DataFrame({"Name": ["X", "Y"], "EIC": [{np.nan}, {"CODE1"}]})

    matches, idx0, idx1 = _match_by_eic(df0, df1, ["L", "R"])
    # Only CODE1 should match, not nan
    assert len(matches) == 1
    assert 0 not in idx0  # row with {nan} not matched


def test_eic_matching_nan_only():
    """All-NaN EIC column produces no matches."""
    df0 = pd.DataFrame({"Name": ["A"], "EIC": [None]})
    df1 = pd.DataFrame({"Name": ["B"], "EIC": [None]})

    matches, _, _ = _match_by_eic(df0, df1, ["X", "Y"])
    assert matches.empty


def test_eic_matching_one_to_one():
    """A shared scheme identifier cannot select a station arbitrarily."""
    # Plant A has {C1, C2}; Plant X has {C1}, Plant Y has {C2}
    df0 = pd.DataFrame({"Name": ["Plant A"], "EIC": [{"C1", "C2"}]})
    df1 = pd.DataFrame({"Name": ["Plant X", "Plant Y"], "EIC": [{"C1"}, {"C2"}]})

    matches, idx0, idx1 = _match_by_eic(df0, df1, ["src0", "src1"])

    assert matches.empty
    assert not idx0 and not idx1


def test_eic_matching_non_set_values():
    """Scalar identifiers participate in the same matching as collections."""
    df0 = pd.DataFrame({"Name": ["A", "B"], "EIC": ["CODE1", {"CODE2"}]})
    df1 = pd.DataFrame({"Name": ["X", "Y"], "EIC": [{"CODE1"}, {"CODE2"}]})

    matches, idx0, idx1 = _match_by_eic(df0, df1, ["L", "R"])
    assert len(matches) == 2
    assert idx0 == idx1 == {0, 1}


@pytest.mark.parametrize("codes", [["C2", "C1", "C1"], {"C1", "C2"}, "C1"])
def test_reduce_preserves_identifiers_across_sources(codes: object) -> None:
    """A higher-priority source without EICs cannot erase known identifiers."""
    columns = pd.MultiIndex.from_product(
        [["Name", "Fueltype", "Technology", "Set", "EIC"], ["ENTSOE", "GEM"]]
    )
    frame = pd.DataFrame(
        [
            [
                "Plant",
                "Plant",
                "Natural Gas",
                "Natural Gas",
                "CCGT",
                "CCGT",
                "PP",
                "PP",
                codes,
                None,
            ]
        ],
        columns=columns,
    )
    config = {
        "target_columns": ["Name", "Fueltype", "Technology", "Set", "EIC"],
        "ENTSOE": {"reliability_score": 5},
        "GEM": {"reliability_score": 6},
    }
    result = reduce_matched_dataframe(frame, config=config)
    assert result.EIC.iloc[0] == (["C1"] if isinstance(codes, str) else ["C1", "C2"])


def test_eic_matching_lists_and_ambiguous_scheme() -> None:
    left = pd.DataFrame({"EIC": [["C1", "C2"], ["SCHEME"]]}, index=[10, 20])
    right = pd.DataFrame({"EIC": [["C1"], ["SCHEME"], ["SCHEME"]]}, index=[30, 40, 50])
    matches, idx0, idx1 = _match_by_eic(left, right, ["A", "B"])
    assert matches.to_dict("records") == [{"A": 10, "B": 30}]
    assert idx0 == {10} and idx1 == {30}


def test_eic_matching_rejects_duplicate_index() -> None:
    left = pd.DataFrame({"EIC": [["C1"], ["C2"]]}, index=[0, 0])
    right = pd.DataFrame({"EIC": [["C1"]]})
    with pytest.raises(ValueError, match="unique index"):
        _match_by_eic(left, right, ["A", "B"])


def test_eic_matching_through_linkage_table() -> None:
    """Linked project IDs match only when the translated EIC is one to one."""
    plant = {"Fueltype": "Hard Coal", "Capacity": 100}
    left = pd.DataFrame({"EIC": ["E1", "E2", "E3"], **plant}, index=[10, 20, 30])
    right = pd.DataFrame(
        {
            "projectID": [{"WRI1"}, {"WRI2"}, {"WRI3"}],
            "EIC": [None, None, None],
            **plant,
        },
        index=[40, 50, 60],
    )
    linkages = pd.DataFrame(
        [("E1", "GPD", "WRI1"), ("E2", "GPD", "WRI2"), ("E3", "GPD", "WRI2")],
        columns=["EIC", "Source", "projectID"],
    )
    matches, idx0, idx1 = _match_by_eic(left, right, ["ENTSOE", "GPD"], linkages)
    assert matches.to_dict("records") == [{"ENTSOE": 10, "GPD": 40}]
    assert idx0 == {10} and idx1 == {40}


def test_eic_linkage_requires_plausible_pair() -> None:
    """Translated links need the same fuel type and a capacity within 20%."""
    left = pd.DataFrame(
        {
            "EIC": ["E1", "E2"],
            "Fueltype": ["Hard Coal", "Natural Gas"],
            "Capacity": [100, 100],
        }
    )
    right = pd.DataFrame(
        {
            "projectID": [{"WRI1"}, {"WRI2"}],
            "EIC": [None, None],
            "Fueltype": ["Hard Coal", "Natural Gas"],
            "Capacity": [110, 300],
        }
    )
    linkages = pd.DataFrame(
        [("E1", "GPD", "WRI1"), ("E2", "GPD", "WRI2")],
        columns=["EIC", "Source", "projectID"],
    )
    matches, idx0, idx1 = _match_by_eic(left, right, ["ENTSOE", "GPD"], linkages)
    assert matches.to_dict("records") == [{"ENTSOE": 0, "GPD": 0}]
    assert idx0 == {0} and idx1 == {0}


def test_eic_linkage_never_links_two_listed_sources() -> None:
    left = pd.DataFrame({"projectID": [{"WRI1"}]})
    right = pd.DataFrame({"projectID": [{"GEO-1"}]})
    linkages = pd.DataFrame(
        [("E1", "GPD", "WRI1"), ("E1", "GEO", "GEO-1")],
        columns=["EIC", "Source", "projectID"],
    )
    matches, idx0, idx1 = _match_by_eic(left, right, ["GPD", "GEO"], linkages)
    assert matches.empty
    assert not idx0 and not idx1


@pytest.mark.parametrize(
    "codes, expected",
    [(["C2", "C1", "C1", None, np.nan, "", 42], ["C1", "C2"]), (None, [])],
)
def test_aggregate_identifiers_survive_cache_roundtrip(
    codes: object, expected: list[str]
) -> None:
    """Unit aggregation and cached reload retain the same usable identifiers."""
    config = get_config()
    units = pd.DataFrame(
        [
            {
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
        ]
    ).reindex(columns=config["target_columns"])
    aggregated = aggregate_units(units, dataset_name="test", config=config)
    assert aggregated.EIC.iloc[0] == expected
    cached = pd.read_csv(StringIO(aggregated.to_csv(index=False)))
    restored = parse_string_to_dict(cached, ["EIC"])
    assert restored.EIC.iloc[0] == expected

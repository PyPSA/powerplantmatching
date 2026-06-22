# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import pytest

from powerplantmatching.matching import DirectMatcher, _match_by_eic


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
    matches = _match_by_eic(df_entsoe, df_opsd, "ENTSOE", "OPSD")

    # Eemshavencentrale (0) <-> Eemshaven coal (0) via EMSA
    # Eemscentrale (1) <-> Eems gas (1) via 008xG (008xK is ENTSOE-only)
    assert len(matches) == 2
    assert set(matches["ENTSOE"]) == {0, 1}
    assert set(matches["OPSD"]) == {0, 1}

    # Maasvlakte (2) and Rijnmond (2) must NOT match (no shared EIC)
    assert 2 not in set(matches["ENTSOE"])
    assert 2 not in set(matches["OPSD"])


def test_eic_matching_no_eic_column():
    """Gracefully handles datasets without an EIC column."""
    df0 = pd.DataFrame({"Name": ["Plant A"], "Capacity": [100]})
    df1 = pd.DataFrame({"Name": ["Plant B"], "Capacity": [200], "EIC": [{"CODE1"}]})

    matches = _match_by_eic(df0, df1, "A", "B")
    assert matches.empty
    assert list(matches.columns) == ["A", "B"]


def test_eic_matching_empty_sets():
    """No matches when all EIC sets are empty."""
    df0 = pd.DataFrame({"Name": ["A"], "EIC": [set()]})
    df1 = pd.DataFrame({"Name": ["B"], "EIC": [set()]})

    matches = _match_by_eic(df0, df1, "X", "Y")
    assert matches.empty


def test_eic_matching_nan_values():
    """Float nan inside EIC sets does not produce false matches."""
    df0 = pd.DataFrame({"Name": ["A", "B"], "EIC": [{np.nan}, {"CODE1"}]})
    df1 = pd.DataFrame({"Name": ["X", "Y"], "EIC": [{np.nan}, {"CODE1"}]})

    matches = _match_by_eic(df0, df1, "L", "R")
    # Only CODE1 should match, never nan
    assert len(matches) == 1
    assert set(matches["L"]) == {1}
    assert set(matches["R"]) == {1}


def test_eic_matching_nan_only():
    """All-None EIC column produces no matches."""
    df0 = pd.DataFrame({"Name": ["A"], "EIC": [None]})
    df1 = pd.DataFrame({"Name": ["B"], "EIC": [None]})

    matches = _match_by_eic(df0, df1, "X", "Y")
    assert matches.empty


def test_eic_matching_one_to_many_is_ambiguous():
    """A code shared with several rows on the other side is left to Duke.

    df0 Plant A carries {C1, C2}; df1 splits these across Plant X {C1} and
    Plant Y {C2}. Sharing a single code does not prove identity here, so the
    deterministic phase makes no match and defers to fuzzy matching.
    """
    df0 = pd.DataFrame({"Name": ["Plant A"], "EIC": [{"C1", "C2"}]})
    df1 = pd.DataFrame({"Name": ["Plant X", "Plant Y"], "EIC": [{"C1"}, {"C2"}]})

    matches = _match_by_eic(df0, df1, "src0", "src1")
    assert matches.empty


def test_eic_matching_hydro_scheme_ambiguous():
    """Regression for the Alpine-hydro aggregation mismatch (PR #289 review).

    ENTSOE reports one aggregated scheme (1307 MW) under a single scheme-level
    EIC; OPSD splits it into three stations all tagged with that same code.
    'One shared code' would pick an arbitrary station, so none is matched.
    """
    entsoe = pd.DataFrame(
        {
            "Name": ["Oberhasli scheme"],
            "Capacity": [1307.0],
            "EIC": [{"12W-0000000031-O"}],
        }
    )
    opsd = pd.DataFrame(
        {
            "Name": ["Gental", "Grimsel", "Handeck"],
            "Capacity": [10.0, 389.0, 316.0],
            "EIC": [{"12W-0000000031-O"}] * 3,
        }
    )
    matches = _match_by_eic(entsoe, opsd, "ENTSOE", "OPSD")
    assert matches.empty


def test_eic_matching_multi_code_one_to_one():
    """Two rows sharing several codes (and nothing else) are a single match."""
    df0 = pd.DataFrame(
        {"Name": ["Eems gas"], "EIC": [{"C1", "C2", "C3", "C4", "C5", "C6"}]}
    )
    df1 = pd.DataFrame(
        {"Name": ["Eemscentrale"], "EIC": [{"C1", "C2", "C3", "C4", "C5", "C6"}]}
    )

    matches = _match_by_eic(df0, df1, "L", "R")
    assert len(matches) == 1
    assert matches.iloc[0]["L"] == 0
    assert matches.iloc[0]["R"] == 0


def test_eic_matching_subset_superset_one_to_one():
    """Partial code coverage still matches when the link is unambiguous 1-to-1.

    One source lists a subset of the other's codes (e.g. OPSD has one unit
    code, ENTSOE has two). With no competing rows, this is a confident match.
    """
    df0 = pd.DataFrame({"Name": ["Ballylumford"], "EIC": [{"C1"}]})
    df1 = pd.DataFrame({"Name": ["Ballylumford"], "EIC": [{"C1", "C2"}]})

    matches = _match_by_eic(df0, df1, "OPSD", "ENTSOE")
    assert len(matches) == 1


def test_eic_matching_raw_string_treated_as_single_code():
    """A raw string EIC (not wrapped in a set) is treated as one code."""
    df0 = pd.DataFrame({"Name": ["A", "B"], "EIC": ["CODE1", {"CODE2"}]})
    df1 = pd.DataFrame({"Name": ["X", "Y"], "EIC": [{"CODE1"}, {"CODE2"}]})

    matches = _match_by_eic(df0, df1, "L", "R")
    # CODE1 (raw string) and CODE2 (set) both yield a clean 1-to-1 match
    assert len(matches) == 2
    assert set(matches["L"]) == {0, 1}


def test_direct_matcher_run_returns_matches_and_residual(df_entsoe, df_opsd):
    """DirectMatcher.run pairs via EIC and returns the unmatched residual."""
    matches, remaining = DirectMatcher().run(df_entsoe, df_opsd, "ENTSOE", "OPSD")

    assert len(matches) == 2
    assert list(matches.columns) == ["ENTSOE", "OPSD"]

    rem0, rem1 = remaining
    # the two matched rows (idx 0, 1) are removed; only the unmatched stay
    assert set(rem0.index) == {2}  # Maasvlakte
    assert set(rem1.index) == {2}  # Rijnmond


def test_direct_matcher_no_matches_passes_everything_through():
    """With no shared identifiers, residual equals the inputs and matches is empty."""
    df0 = pd.DataFrame({"Name": ["A"], "EIC": [{"C1"}]})
    df1 = pd.DataFrame({"Name": ["B"], "EIC": [{"C2"}]})

    matches, (rem0, rem1) = DirectMatcher().run(df0, df1, "L", "R")
    assert matches.empty
    assert list(matches.columns) == ["L", "R"]
    assert len(rem0) == 1 and len(rem1) == 1


def test_direct_matcher_custom_matcher_list():
    """Matchers are pluggable; an empty list yields no matches and full residual."""
    df0 = pd.DataFrame({"Name": ["A"], "EIC": [{"C1"}]})
    df1 = pd.DataFrame({"Name": ["B"], "EIC": [{"C1"}]})

    matches, (rem0, rem1) = DirectMatcher(matchers=[]).run(df0, df1, "L", "R")
    assert matches.empty
    assert len(rem0) == 1 and len(rem1) == 1

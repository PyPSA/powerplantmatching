# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import pytest

from powerplantmatching.matching import _match_by_eic


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
    """Enforces 1-to-1: each row matches at most once."""
    # Plant A has {C1, C2}; Plant X has {C1}, Plant Y has {C2}
    df0 = pd.DataFrame({"Name": ["Plant A"], "EIC": [{"C1", "C2"}]})
    df1 = pd.DataFrame({"Name": ["Plant X", "Plant Y"], "EIC": [{"C1"}, {"C2"}]})

    matches, idx0, idx1 = _match_by_eic(df0, df1, ["src0", "src1"])

    # Plant A should match exactly one of X or Y (1-to-1 constraint)
    assert len(matches) == 1
    assert matches["src0"].iloc[0] == 0
    assert matches["src1"].iloc[0] in {0, 1}


def test_eic_matching_non_set_values():
    """Non-set EIC values (e.g. raw strings from CSV) are skipped."""
    df0 = pd.DataFrame({"Name": ["A", "B"], "EIC": ["CODE1", {"CODE2"}]})
    df1 = pd.DataFrame({"Name": ["X", "Y"], "EIC": [{"CODE1"}, {"CODE2"}]})

    matches, idx0, idx1 = _match_by_eic(df0, df1, ["L", "R"])
    # Only CODE2 matches (CODE1 in df0 is a raw string, not a set)
    assert len(matches) == 1
    assert 1 in idx0

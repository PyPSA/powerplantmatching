# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import pandas as pd
import pytest

from powerplantmatching import linkage as lk

TEST_DATA = {
    "Name": ["Powerplant", "Hydro Station", "Gas Turbine A", "Coal Block 1"],
    "Fueltype": ["Hard Coal", "Hydro", "Natural Gas", "Hard Coal"],
    "Technology": ["Steam Turbine", "Run-Of-River", "CCGT", "Steam Turbine"],
    "Country": ["Germany", "Germany", "Germany", "Germany"],
    "Capacity": [120.0, 40.0, 80.0, 300.0],
    "lat": [51.5, 48.1, 50.9, 52.5],
    "lon": [7.0, 11.6, 6.9, 13.4],
}


@pytest.fixture
def left():
    return pd.DataFrame(TEST_DATA)


@pytest.fixture
def right(left):
    df = left.copy()
    df.loc[0, "Name"] = "Power Plant"  # near-duplicate of "Powerplant"
    df.loc[2, "Capacity"] = 82.0
    return df


def test_record_linkage_format(left, right):
    out = lk.match([left, right], labels=["one", "two"], singlematch=True)
    assert list(out.columns) == ["one", "two", "scores"]
    assert out["scores"].between(0, 1).all()
    assert (out["scores"] >= lk.LINKAGE_THRESHOLD).all()


def test_record_linkage_matches_identical_rows(left, right):
    out = lk.match([left, right], labels=["one", "two"], singlematch=True)
    pairs = set(zip(out["one"], out["two"]))
    assert {(i, i) for i in left.index}.issubset(pairs)


def test_singlematch_is_unique_per_left(left, right):
    out = lk.match([left, right], labels=["one", "two"], singlematch=True)
    assert out["one"].is_unique


def test_empty_input_returns_empty_links(left):
    empty = left.iloc[0:0]
    out = lk.match([left, empty], labels=["one", "two"])
    assert out.empty
    assert list(out.columns) == ["one", "two", "scores"]


def test_dedup_returns_symmetric_pairs(left):
    dup = left.copy()
    dup.loc[len(dup)] = dup.loc[0]  # exact duplicate of row 0
    out = lk.match(dup, labels=["one", "two"])
    assert list(out.columns) == ["one", "two"]
    forward = set(zip(out["one"], out["two"]))
    backward = set(zip(out["two"], out["one"]))
    assert forward == backward  # reciprocal, as cliques() requires
    assert (0, len(dup) - 1) in forward


def test_geo_falloff_zero_beyond_cutoff():
    a = pd.DataFrame({"lat": [0.0], "lon": [0.0]})
    b = pd.DataFrame({"lat": [1.0], "lon": [1.0]})  # ~157 km apart
    sim, present = lk._geo_matrix(a, b)
    assert present.all()
    assert sim[0, 0] == 0.0

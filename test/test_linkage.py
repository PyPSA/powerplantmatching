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
    out = lk.match([left, left.iloc[0:0]], labels=["one", "two"])
    assert out.empty
    assert list(out.columns) == ["one", "two", "scores"]


def test_dedup_returns_symmetric_pairs(left):
    dup = left.copy()
    dup.loc[len(dup)] = dup.loc[0]  # exact duplicate of row 0
    out = lk.match(dup, labels=["one", "two"])
    assert list(out.columns) == ["one", "two"]
    forward = set(zip(out["one"], out["two"]))
    assert forward == set(zip(out["two"], out["one"]))  # reciprocal, as cliques() requires
    assert (0, len(dup) - 1) in forward


def test_geo_proximity_increases_score(left):
    one = left.iloc[[0]].reset_index(drop=True)
    other = one.copy()
    other.loc[0, "Name"] = "Power Station"  # only loosely similar, so geo matters
    near, far = other.copy(), other.copy()
    near.loc[0, ["lat", "lon"]] = left.loc[0, ["lat", "lon"]].to_numpy()
    far.loc[0, ["lat", "lon"]] = [40.0, -3.0]  # far from row 0

    score = lambda r: lk.match([one, r], threshold=0.0)["scores"].iloc[0]
    assert score(near) > score(far)

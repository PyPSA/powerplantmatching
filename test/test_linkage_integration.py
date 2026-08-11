# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""Offline coverage of the two production callers of ``linkage.match``."""

import pandas as pd
import pytest

import powerplantmatching as pm
from powerplantmatching.cleaning import aggregate_units
from powerplantmatching.matching import compare_two_datasets

UNITS = [
    ("Alpha Power Station", "Hard Coal", "Steam Turbine", "Germany", 100.0, 51.0, 7.0),
    ("Alpha Power Stn", "Hard Coal", "Steam Turbine", "Germany", 150.0, 51.0, 7.0),
    ("Beta Hydro Plant", "Hydro", "Run-Of-River", "Germany", 40.0, 48.0, 11.0),
    ("Alpha Power Station", "Hard Coal", "Steam Turbine", "France", 100.0, 45.0, 2.0),
]
COLUMNS = ["Name", "Fueltype", "Technology", "Country", "Capacity", "lat", "lon"]


@pytest.fixture(scope="module")
def config() -> dict:
    return pm.get_config()


@pytest.fixture
def units(config) -> pd.DataFrame:
    df = pd.DataFrame(UNITS, columns=COLUMNS)
    extras = dict(Set="PP", DateIn=2000, Efficiency=0.4, Duration=0.0, EIC=None)
    df = df.assign(projectID=[f"p{i}" for i in df.index], **extras)
    return df.reindex(columns=df.columns.union(config["target_columns"], sort=False))


def test_aggregate_units_merges_only_true_duplicates(units, config):
    out = aggregate_units(units, dataset_name="test", config=config).sort_values(
        "Capacity"
    )

    assert len(out) == 3
    assert out.Capacity.to_list() == [40.0, 100.0, 250.0]


def test_aggregate_units_does_not_merge_across_countries(units, config):
    """The country-wise fan-out blocks on Country before matching."""
    single = aggregate_units(
        units.query("Country == 'Germany'"), dataset_name="test", config=config
    )
    both = aggregate_units(units, dataset_name="test", config=config)

    assert len(single) == 2
    assert len(both) == len(single) + 1


def test_compare_two_datasets_returns_index_labels(units, config):
    left = units.head(3)
    right = units.head(3).iloc[::-1].set_axis([17, 18, 19])

    matches = compare_two_datasets([left, right], ["A", "B"], config=config)

    assert set(map(tuple, matches[["A", "B"]].to_numpy())) == {
        (0, 19),
        (1, 18),
        (2, 17),
    }

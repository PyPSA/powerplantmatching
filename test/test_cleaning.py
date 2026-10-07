# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import pytest

from powerplantmatching.cleaning import (
    aggregate_units,
    clean_name,
    gather_and_replace,
    gather_specifications,
    map_status,
)
from powerplantmatching.core import get_config
from powerplantmatching.utils import config_filter, set_column_name

TEST_DATA = {
    "Name": [
        "Powerplant",
        "an hydro powerplant",
        " another    powerplant with whitespaces",
        " Power II coalition",
        " Kraftwerk Besonders besonders '2' CHP",
    ],
    "Fueltype": [
        "",
        "Run of-River",
        "OCGT",
        "Nuclear Power",
        "",
    ],
    "Technology": [
        "Natural Gas",
        "Run of-River",
        "",
        " Nuclear",
        "",
    ],
    "Set": [
        np.nan,
        "",
        "",
        "Powerplant",
        "",
    ],
}


@pytest.fixture
def data():
    data = pd.DataFrame(TEST_DATA)
    return data


def test_gather_and_replace(data):
    mapping = {
        "Nuclear": ["nuclear"],
        "Natural Gas": ["natural gas", "ocgt"],
        "Hydro": "",
    }
    res = gather_and_replace(data, mapping)
    assert res[0] == "Natural Gas"
    assert res[1] == "Hydro"
    assert res[2] == "Natural Gas"
    assert res[3] == "Nuclear"

    # test overwrite
    mapping = {"Nuclear": "", "Coal": "(?i)Coalition"}
    res = gather_and_replace(data, mapping)
    assert res[3] == "Coal"


def test_gather_specifications(data):
    res = gather_specifications(data)
    assert res.Fueltype[0] == "Natural Gas"
    assert res.Fueltype[1] == "Hydro"
    assert res.Fueltype[2] == "Natural Gas"
    assert res.Fueltype[3] == "Nuclear"
    assert res.Technology[0] == "CCGT"
    assert res.Technology[2] == "OCGT"
    assert np.isnan(res.Technology[4])
    assert res.Set[4] == "CHP"


def test_clean_name(data):
    res = clean_name(data)
    assert res.Name[0] == "Powerplant"
    assert res.Name[1] == "An Powerplant"
    assert res.Name[2] == "Another Powerplant With Whitespaces"
    assert res.Name[3] == "Coalition"
    assert res.Name[4] == "Besonders Chp"


def test_map_status():
    status = ["Operating", "pre-permit", "Vorübergehend stillgelegt", "cancelled", None]
    res = map_status(pd.DataFrame({"Status": status}), config=get_config())
    assert res.Status.to_list() == ["operating", "announced", "mothballed"]


@pytest.fixture
def status_data():
    return pd.DataFrame(
        {
            "Name": "Kraftwerk Musterstadt",
            "Fueltype": "Hard Coal",
            "Country": "Germany",
            "Capacity": 400.0,
            "Status": ["operating", "retired", np.nan],
            "lat": 51.0,
            "lon": 10.0,
            "projectID": ["A", "B", "C"],
        }
    ).pipe(set_column_name, "GCPT")


@pytest.mark.parametrize(
    "overall, source, expected",
    [
        (None, None, ["operating", "retired", np.nan]),
        (["operating"], None, ["operating", np.nan]),
        (["operating"], ["retired"], ["retired", np.nan]),
    ],
)
def test_config_filter_status(status_data, overall, source, expected):
    config = get_config()
    if overall is not None:
        config["status"] = overall
    if source is not None:
        config["GCPT"]["status"] = source
    res = config_filter(status_data, config)
    pd.testing.assert_series_equal(
        res.Status, pd.Series(expected, name="Status"), check_dtype=False
    )


def test_config_filter_unknown_status(status_data):
    config = get_config()
    config["status"] = ["Operational"]
    with pytest.raises(ValueError, match="Unknown status"):
        config_filter(status_data, config)


@pytest.mark.parametrize(
    "fueltype, status, n_plants",
    [
        ("Hard Coal", ["operating", "operating"], 1),
        ("Hard Coal", ["operating", "retired"], 2),
        ("Nuclear", ["operating", "operating"], 2),
    ],
)
def test_aggregate_units(fueltype, status, n_plants):
    config = get_config()
    df = pd.DataFrame(
        {
            "Name": "Kraftwerk Musterstadt",
            "Fueltype": fueltype,
            "Country": "Germany",
            "Capacity": 400.0,
            "Status": status,
            "lat": 51.0,
            "lon": 10.0,
        }
    ).reindex(columns=config["target_columns"])
    res = aggregate_units(df, dataset_name="GCPT", config=config)
    assert len(res) == n_plants

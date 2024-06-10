import numpy as np
import pandas as pd
import pytest

from powerplantmatching.cleaning import (
    clean_name,
    gather_and_replace,
    gather_specifications,
)

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
    assert res.Name[1] == "An Hydro Powerplant"
    assert res.Name[2] == "Another Powerplant With Whitespaces"
    assert res.Name[3] == "Coalition"
    assert res.Name[4] == "Besonders Chp"

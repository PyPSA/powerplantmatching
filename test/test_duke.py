import numpy as np
import pandas as pd

from powerplantmatching.duke import add_geoposition_for_duke, duke

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
    "Country": ["", "", "", "", ""],
    "Capacity": [120, 40, 80.008, 90.122, 400.2],
    "lat": [51.5074, np.nan, 40.7128, 42.8765, 48.2031],
    "lon": [-0.1278, np.nan, -74.0060, -60.9807, -1.0221],
}


def test_add_geoposition_for_duke():
    df = pd.DataFrame(TEST_DATA)

    expected = pd.DataFrame(TEST_DATA)
    expected["Geoposition"] = [
        "51.5074,-0.1278",
        np.nan,
        "40.7128,-74.006",
        "42.8765,-60.9807",
        "48.2031,-1.0221",
    ]

    output = add_geoposition_for_duke(df)

    pd.testing.assert_frame_equal(output, expected)


def test_duke_deduplication_mode():
    df = pd.DataFrame(TEST_DATA)

    expected = pd.DataFrame({"one": [0, 1], "two": [1, 0]})

    output = duke(df)

    pd.testing.assert_frame_equal(output, expected)


def test_duke_record_linkage_mode():
    df1 = pd.DataFrame(TEST_DATA)

    df2 = pd.DataFrame(TEST_DATA)
    df2.loc[0, "Name"] = "Plant"
    df2.loc[0, "Technology"] = "Gas"
    df2.loc[2, "Capacity"] = 44.5

    expected = pd.DataFrame(
        {
            "one": {0: 0, 1: 1, 2: 2, 3: 3, 4: 4},
            "two": {0: 1, 1: 1, 2: 2, 3: 3, 4: 4},
            "scores": {
                0: 0.9769736842105264,
                1: 0.9985590778097984,
                2: 0.9992083247849796,
                3: 0.999639379733141,
                4: 0.9991589571068124,
            },
        }
    )

    output = duke([df1, df2], labels=["one", "two"], singlematch=False)

    pd.testing.assert_frame_equal(output, expected)

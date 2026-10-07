# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import pytest

import powerplantmatching as pm
from powerplantmatching.matching import reduce_matched_dataframe


@pytest.mark.parametrize("gpd_lat, expected", [(50.0, 50.0), (np.nan, 49.0)])
def test_entsoe_coordinates_are_fallback(gpd_lat, expected):
    plant = {"Name": "Plant", "Fueltype": "Hydro", "Country": "Austria", "lon": 10.0}
    df = pd.concat(
        {
            "ENTSOE": pd.DataFrame([{**plant, "Capacity": 100.0, "lat": 49.0}]),
            "GPD": pd.DataFrame([{**plant, "Capacity": 90.0, "lat": gpd_lat}]),
        },
        axis=1,
    ).swaplevel(axis=1)
    cols = pd.MultiIndex.from_product(
        [pm.get_config()["target_columns"], ["ENTSOE", "GPD"]]
    )
    df = df.reindex(columns=cols)
    reduced = reduce_matched_dataframe(df)
    assert reduced.loc[0, "lat"] == expected
    assert reduced.loc[0, "Capacity"] == 100.0

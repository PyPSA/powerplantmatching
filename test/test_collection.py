# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import pandas as pd

import powerplantmatching as pm
from powerplantmatching import collection


def test_powerplants_keeps_units_with_same_name(monkeypatch):
    turbines = pd.DataFrame(
        {
            "Name": ["Windpark Nord", "Windpark Nord"],
            "Fueltype": "Wind",
            "Country": "Germany",
            "Capacity": [3.0, 4.0],
            "lat": [54.0, 54.1],
            "lon": [9.0, 9.1],
            "projectID": [{"MASTR": {"SEE1"}}, {"MASTR": {"SEE2"}}],
        }
    )
    monkeypatch.setattr(collection, "collect", lambda *args, **kwargs: turbines)
    config = pm.get_config(fully_included_sources=[], variant="test-same-name")

    df = pm.powerplants(config=config, update=True, fill_geopositions=False)

    assert df.Capacity.sum() == turbines.Capacity.sum()

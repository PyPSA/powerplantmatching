import pytest

import powerplantmatching as pm
from powerplantmatching import data

config = pm.get_config()
sources = config["matching_sources"]


def test_aggregate():
    for source in sources:
        df = getattr(data, source)().sort_values("Name").head(100)
        aggregated = df.powerplant.aggregate_units()
        if not config[source].get("aggregated_units", False):
            assert len(aggregated) < len(df)

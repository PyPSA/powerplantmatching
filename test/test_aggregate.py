import pytest

import powerplantmatching as pm
from powerplantmatching import data

config = pm.get_config()
sources = config["matching_sources"]

if not config["entsoe_token"] and "ENTSOE" in sources:
    sources.remove("ENTSOE")


@pytest.mark.parametrize("source", sources)
def test_aggregate(source):
    df = getattr(data, source)().sort_values("Name").head(200)
    aggregated = df.powerplant.aggregate_units()
    if not config[source].get("aggregated_units", False):
        assert len(aggregated) < len(df)

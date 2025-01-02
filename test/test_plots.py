import pytest

import powerplantmatching as pm
from powerplantmatching import data

config = pm.get_config()
sources = [s if isinstance(s, str) else list(s)[0] for s in config["matching_sources"]]

if not config["entsoe_token"] and "ENTSOE" in sources:
    sources.remove("ENTSOE")


def test_powerplant_map():
    pm.plot.powerplant_map(pm.powerplants())


@pytest.mark.parametrize("source", sources)
def test_source_plots(source):
    func = getattr(data, source)
    df = func(update=True)
    df.powerplant.plot_map(figsize=(11, 8))

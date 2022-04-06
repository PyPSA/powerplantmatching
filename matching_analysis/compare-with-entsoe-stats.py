import warnings

import country_converter as cc
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from entsoe import EntsoePandasClient

import powerplantmatching as pm
from powerplantmatching.cleaning import gather_fueltype_info

warnings.simplefilter(action="ignore", category=FutureWarning)

config = pm.get_config()
powerplants = pm.powerplants()
powerplants = powerplants.powerplant.convert_country_to_alpha2()

client = EntsoePandasClient(api_key=config["entsoe_token"])

start = pd.Timestamp("20190101", tz="Europe/Berlin")
end = pd.Timestamp("20200101", tz="Europe/Berlin")

kwargs = dict(start=start, end=end, psr_type=None)


def parse(c):
    try:
        return client.query_installed_generation_capacity(c, **kwargs).iloc[0]
    except:
        print(f"Country {c} failed")
        return np.nan


stats = pd.DataFrame({c: parse(c) for c in powerplants.Country.unique()})
fueltypes = gather_fueltype_info(pd.DataFrame({"Fueltype": stats.index}), ["Fueltype"])
stats = stats.groupby(fueltypes.Fueltype.values).sum().unstack()

totals = powerplants.powerplant.lookup().fillna(0)

sources = config["matching_sources"]
dbs = {
    s.title(): getattr(pm.data, s)()
    .powerplant.convert_country_to_alpha2()
    .powerplant.lookup()
    .fillna(0)
    for s in sources
}

compare = (
    pd.concat({"Statistics": stats, "Totals": totals, "NONE": 0, **dbs}, axis=1).fillna(
        0
    )
    / 1000
)


for c in compare.index.unique(0):
    df = compare.loc[c]
    fig, ax = plt.subplots(figsize=(15, 5))
    df.plot.bar(ax=ax)
    ax.set_ylabel("Capacity [GW]")
    ax.set_title(cc.convert(c, to="name"))
    fig.tight_layout()
    fig.savefig("figures/country-comparison/" + c + ".png")

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
powerplants = pm.powerplants(update=True)
powerplants = powerplants.powerplant.convert_country_to_alpha2()
powerplants = powerplants[powerplants.lat.notnull()]

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

# Manual correction on the statistics

# https://de.wikipedia.org/wiki/Liste_von_Wasserkraftwerken_in_der_Schweiz?oldformat=true
stats.loc["CH", "Hydro"] = 18000
# https://www.bmk.gv.at/dam/jcr:f0bdbaa4-59f2-4bde-9af9-e139f9568769/Energie_in_OE_2020_ua.pdf
stats.loc["AT", "Hydro"] = 14600

totals = powerplants.powerplant.lookup().fillna(0)

sources = [
    s if isinstance(s, str) else list(s).pop() for s in config["matching_sources"]
]
dbs = {
    s.title(): getattr(pm.data, s)()
    .powerplant.convert_country_to_alpha2()
    .powerplant.lookup()
    .fillna(0)
    for s in sources
}

empty = pd.Series(0, index=totals.index)  # only for cosmetics
compare = (
    pd.concat({"Statistics": stats, "Totals": totals, "": empty, **dbs}, axis=1).fillna(
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
    fig.savefig("figures/country-comparison/" + c + ".png", dpi=150)
    plt.close()

# biggest differences

diff = compare.Statistics - compare.Totals
diff = diff.loc[:, list(set(compare.index.unique(1)) - {"Wind", "Solar"})]
diff = diff.sort_values(ascending=False)
diff.index = diff.index.get_level_values(0) + " " + diff.index.get_level_values(1)

fig, ax = plt.subplots(figsize=(5, 20))
diff[diff.abs() > 2].plot.barh(ax=ax, zorder=3)
ax.set_xlabel("Capacity difference (stats - ppm) [GW]")
ax.set_title(cc.convert(c, to="name"))
ax.grid(True, zorder=2)
fig.tight_layout()
fig.savefig("figures/capacity-diff-per-country-and-fueltype.png", dpi=150)

import pathlib
import time
import warnings

import country_converter as cc
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from bs4 import XMLParsedAsHTMLWarning
from entsoe import EntsoePandasClient

import powerplantmatching as pm
from powerplantmatching.cleaning import gather_fueltype_info

warnings.simplefilter(action="ignore", category=(FutureWarning, XMLParsedAsHTMLWarning))
root = pathlib.Path(__file__).parent.absolute()
figpath = root / "figures"

UPDATE = True


config = pm.get_config()

powerplants = pm.powerplants(update=UPDATE)


powerplants = powerplants.powerplant.convert_country_to_alpha2()

client = EntsoePandasClient(api_key=config["entsoe_token"])

start = pd.Timestamp("20220101", tz="Europe/Berlin")
end = pd.Timestamp("20230101", tz="Europe/Berlin")

kwargs = dict(start=start, end=end, psr_type=None)


def parse(c):
    rename = {"GB": "UK"}
    for n in range(2):
        try:
            print(c, n)
            return client.query_installed_generation_capacity(
                rename.get(c, c), **kwargs
            ).iloc[0]
        except Exception as e:
            print(f"Country {c} failed with {e}")
            time.sleep(3)
    return np.nan


stats = pd.DataFrame({c: parse(c) for c in powerplants.Country.unique()})
fueltypes = gather_fueltype_info(pd.DataFrame({"Fueltype": stats.index}), ["Fueltype"])
stats = stats.groupby(fueltypes.Fueltype.values).sum().unstack()

# Manual correction on the statistics

# https://de.wikipedia.org/wiki/Liste_von_Wasserkraftwerken_in_der_Schweiz?oldformat=true
stats.loc["CH", "Hydro"] = 17038

# %%
query = "(DateOut > 2022 or DateOut != DateOut) and (DateIn < 2023 or DateIn != DateIn)"
powerplants = powerplants.query(query)
totals = powerplants.powerplant.lookup().fillna(0)

sources = [s if isinstance(s, str) else list(s)[0] for s in config["matching_sources"]]

input_dbs = {
    s.title(): getattr(pm.data, s)()
    .powerplant.convert_country_to_alpha2()
    .query(query)
    .powerplant.lookup()
    .fillna(0)
    for s in sources
}
output_dbs = {
    s.title(): powerplants[
        powerplants.projectID.apply(lambda ds: s in ds)
    ].powerplant.lookup()
    for s in sources
}

# These are the capacities which come out of the data files
empty = pd.Series(0, index=totals.index)  # only for cosmetics
d = {"Statistics": stats, "Totals": totals, "": empty, **input_dbs}
in_compare = pd.concat(d, axis=1).fillna(0) / 1000

# These are the capacities which went into the resulting data
d = {"Statistics": stats, "Totals": totals, "": empty, **output_dbs}
out_compare = pd.concat(d, axis=1).fillna(0) / 1000


# ---------------------------------------------------------------------------- #
#                                  Differences                                 #
# ---------------------------------------------------------------------------- #

diff = (out_compare.Totals - out_compare.Statistics).to_frame("Difference")
for s in sources:
    ds = out_compare[s.title()] / out_compare.Totals * 100
    diff[s.title() + " (%)"] = ds.fillna(0)

diff = diff[out_compare.Statistics != 0]
diff = diff.loc[:, list(set(out_compare.index.unique(1)) - {"Wind", "Solar"}), :]
diff.index = diff.index.get_level_values(0) + " " + diff.index.get_level_values(1)

df = (diff[diff.Difference > 1]).sort_values("Difference", ascending=False)
print(f"\nOverestimated Capacities (clip at 1 GW): \n\n{df.round(2)}")

df = (diff[diff.Difference < -1]).sort_values("Difference", ascending=True)
print(f"\nMissing Capacities (clip at 1 GW): \n\n{df.round(2)}")


country_diff = diff.Difference.groupby(diff.index.str[:2]).sum()

df = (country_diff[country_diff > 0]).sort_values(ascending=False)
print(f"\nOverestimated Capacities per Country: \n\n{df.round(2)}")

df = (country_diff[country_diff < 0]).sort_values(ascending=True)
print(f"\nMissing Capacities per Country: \n\n{df.round(2)}")

# ---------------------------------------------------------------------------- #
#                                country figures                               #
# ---------------------------------------------------------------------------- #

fig, ax = plt.subplots(figsize=(5, 20))
diff[diff.abs() > 2].plot.barh(ax=ax, zorder=3)
ax.set_xlabel("Capacity difference (stats - ppm) [GW]")
ax.grid(True, zorder=2)
fig.tight_layout()
fig.savefig(figpath / "capacity-diff-per-country-and-fueltype.png", dpi=150)


for c in in_compare.index.unique(0):
    df = in_compare.loc[c]
    fig, ax = plt.subplots(figsize=(15, 5))
    df.plot.bar(ax=ax)
    ax.set_ylabel("Capacity [GW]")
    ax.set_title(cc.convert(c, to="name"))
    fig.tight_layout()
    fig.savefig(figpath / f"country-comparison/{c}.png", dpi=150)
    plt.close()

# %%

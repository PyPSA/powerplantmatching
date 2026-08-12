# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import pathlib
import time
import warnings

import country_converter as cc
import matplotlib.pyplot as plt
import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from entsoe import EntsoeRawClient
from entsoe.mappings import PSRTYPE_MAPPINGS

import powerplantmatching as pm
from powerplantmatching.cleaning import gather_fueltype_info

warnings.simplefilter("ignore", category=FutureWarning)
warnings.simplefilter("ignore", category=XMLParsedAsHTMLWarning)

root = pathlib.Path(__file__).parent.absolute()
figpath = root / "figures"
statspath = root / "data"

# reference year of the ENTSO-E installed capacity statistics
YEAR = 2025

# whether to rerun the matching or use the locally built dataset
UPDATE = False
# whether to requery the ENTSO-E statistics; the cache is fetched when missing
UPDATE_STATS = False

config = pm.get_config()


def query_installed_capacity(client: EntsoeRawClient, country: str, year: int):
    """Installed generation capacity per production type in MW.

    entsoe-py's own parser drops yearly-resolution documents whose period does
    not start at midnight UTC (all CET/EET zones), so the XML is read directly.
    """
    start = pd.Timestamp(f"{year}0101", tz="UTC")
    end = pd.Timestamp(f"{year + 1}0101", tz="UTC")
    xml = client.query_installed_generation_capacity(
        country, start=start, end=end, psr_type=None
    )
    soup = BeautifulSoup(xml, "html.parser")
    capacities = {}
    for timeseries in soup.find_all("timeseries"):
        fueltype = PSRTYPE_MAPPINGS[timeseries.find("psrtype").text]
        capacities[fueltype] = float(timeseries.find("point").find("quantity").text)
    return pd.Series(capacities, dtype=float)


def query_statistics(countries: list[str], year: int) -> pd.DataFrame:
    client = EntsoeRawClient(api_key=config["entsoe_token"])
    rename = {"GB": "UK"}
    capacities = {}
    for c in countries:
        for attempt in range(2):
            try:
                capacities[c] = query_installed_capacity(client, rename.get(c, c), year)
                break
            except Exception as e:
                print(f"Country {c} failed with {repr(e)}")
                time.sleep(3)
    return pd.DataFrame(capacities)


powerplants = pm.powerplants(update=UPDATE).powerplant.convert_country_to_alpha2()

statsfile = statspath / f"entsoe-installed-capacity-{YEAR}.csv"
if UPDATE_STATS or not statsfile.exists():
    stats = query_statistics(sorted(powerplants.Country.unique()), YEAR)
    statspath.mkdir(parents=True, exist_ok=True)
    stats.to_csv(statsfile)
else:
    stats = pd.read_csv(statsfile, index_col=0)

fueltypes = gather_fueltype_info(pd.DataFrame({"Fueltype": stats.index}), ["Fueltype"])
stats = stats.groupby(fueltypes.Fueltype.values).sum().unstack()

# Manual correction on the statistics
# ENTSO-E reports no Swiss hydro capacity
# https://de.wikipedia.org/wiki/Liste_von_Wasserkraftwerken_in_der_Schweiz?oldformat=true
stats.loc["CH", "Hydro"] = 17038

# %%
query = f"(DateOut > {YEAR} or DateOut != DateOut) and (DateIn < {YEAR + 1} or DateIn != DateIn)"


def lookup(df):
    # ENTSO-E reports biogas within its single Biomass category
    df = df.assign(Fueltype=df.Fueltype.replace("Biogas", "Solid Biomass"))
    return df.powerplant.lookup().fillna(0)


powerplants = powerplants.query(query)
totals = lookup(powerplants)

sources = [s if isinstance(s, str) else list(s)[0] for s in config["matching_sources"]]

input_dbs = {}
for s in sources:
    print(s.title())
    db = getattr(pm.data, s)().powerplant.convert_country_to_alpha2().query(query)
    input_dbs[s.title()] = lookup(db)

output_dbs = {
    s.title(): lookup(powerplants[powerplants.projectID.apply(lambda ds: s in ds)])
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
# Wind and Solar are not matched but extended separately, "Other" pools
# incomparable residual categories (ENTSO-E puts batteries and marine in there)
diff = diff[~diff.index.get_level_values(1).isin(["Wind", "Solar", "Other"])]
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

(figpath / "country-comparison").mkdir(parents=True, exist_ok=True)

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

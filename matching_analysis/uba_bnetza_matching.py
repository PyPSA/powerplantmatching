#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 14:17:28 2017

@author: fabian
"""
import numpy as np
import pandas as pd

import powerplantmatching as pm

# OPSD_links = pd.read_html('https://github.com/Open-Power-System-Data/conventional_power_plants/blob/4b7cf76f01f8aaa617e578fb225759bd17923757/input/matching_bnetza_uba.csv')[0]
OPSD_links = pd.read_csv("opsd_links.csv", index_col="id", encoding="utf-8").drop(
    "Unnamed: 0", axis=1
)
OPSD_links.rename(
    columns={"ID BNetzA": "BNETZA", "uba_match_name": "UBA"}, inplace=True
)
OPSD_links.uba_match_fuel.replace(
    {
        "Biomasse": "Bioenergy",
        "Gichtgas": "Other",
        "HS": "Oil",
        "Konvertergas": "Other",
        "Licht": "Solar",
        "Raffineriegas": "Other",
        "Uran": "Nuclear",
        "Wasser": "Hydro",
        "Wind (L)": "Wind",
        "\xd6lr\xfcckstand": "Oil",
    },
    inplace=True,
)


# %% get manual links from opsd
uba = pm.data.UBA()
bnetza = pm.data.BNETZA()  # prune_wind=False, prune_solar=False)
bnetza.lat = np.nan
bnetza.lon = np.nan
uba.lat = np.nan
uba.lon = np.nan
# create subset of uba and bentza entries in opsd and in ppm (due to
# different version...)
OPSD_links = OPSD_links[
    (OPSD_links.BNETZA.isin(bnetza.projectID)) & (OPSD_links.UBA.isin(uba.Name))
]
bnetza = bnetza[bnetza.projectID.isin(OPSD_links.BNETZA)]
uba = uba[uba.Name.isin(OPSD_links.UBA)]

# define a map for uba name-> projectID
duplicated_name = uba.loc[uba.Name.duplicated(False), "Name"]
uba.loc[uba.Name.isin(duplicated_name), "Name"] += (
    " " + uba.loc[uba.Name.isin(duplicated_name), "Fueltype"]
)
uba.Name = uba.Name.replace(r"\s+", " ", regex=True)
uba_projectID_map = uba.set_index("Name").projectID


# get rid of non-unique names
OPSD_links.loc[OPSD_links.UBA.isin(duplicated_name), "UBA"] += (
    " " + OPSD_links.loc[OPSD_links.UBA.isin(duplicated_name), "uba_match_fuel"]
)
OPSD_links = OPSD_links.set_index("BNETZA").UBA
OPSD_links = OPSD_links.map(uba_projectID_map)
OPSD_links_r = OPSD_links.reset_index(drop=False).set_index("UBA").BNETZA.sort_values()


UBA, groups_uba = pm.cleaning.clean_single(
    uba, aggregate_powerplant_units=True, return_aggregation_groups=True
)
BNETZA, groups_bnetza = pm.cleaning.clean_single(
    bnetza, aggregate_powerplant_units=True, return_aggregation_groups=True
)

MATCHED = pm.collection.combine_multiple_datasets([UBA, BNETZA], ["UBA", "BNETZA"])
matched = pm.collection.reduce_matched_dataframe(MATCHED)

uba = uba.set_index("projectID")
bnetza = bnetza.set_index("projectID")

opsd_links = OPSD_links.reset_index()
ppm_links = pd.Series()
ppm_links.name = "UBA"
ppm_links.index.name = "BNETZA"
eq_ppm_links = pd.Series()
ne_ppm_links = pd.Series()
for d in matched.projectID:
    for pp1 in d["BNETZA"]:
        for pp2 in d["UBA"]:
            ppm_links = ppm_links.append(pd.Series({pp1: pp2}))
            mapped = (opsd_links["BNETZA"] == pp1) & (opsd_links["UBA"] == pp2)
            if mapped.any():
                eq_ppm_links = eq_ppm_links.append(pd.Series({pp1: pp2}))
            else:
                ne_ppm_links = ne_ppm_links.append(pd.Series({pp1: pp2}))
            #           take the one out which mapped
            opsd_links = opsd_links[~mapped]
# the ones that remain are not included
missing_links = opsd_links.copy()
print(missing_links.shape[0])
ppm_links = ppm_links.sort_values()

_bnetza = bnetza.loc[missing_links.BNETZA]
_uba = uba.loc[missing_links.UBA]
_UBA = UBA[
    UBA.projectID.apply(lambda x: any([i in missing_links.UBA.tolist() for i in x]))
]
_BNETZA = BNETZA[
    BNETZA.projectID.apply(
        lambda x: any([i in missing_links.BNETZA.tolist() for i in x])
    )
]

# %% aggreagted links

opsd_links = OPSD_links.reset_index()
opsd_links = opsd_links.assign(
    groups_BNETZA=lambda df: df.BNETZA.map(groups_bnetza)
).assign(groups_UBA=lambda df: df.UBA.map(groups_uba))

opsd_linking_groups = pd.DataFrame(
    {
        "BNETZA": opsd_links.BNETZA.map(groups_bnetza),
        "UBA": opsd_links.UBA.map(groups_uba),
    }
)
opsd_linking_groups = opsd_linking_groups.drop_duplicates()

print(opsd_linking_groups.apply(lambda df: df.duplicated()).sum())
is_duplicated = opsd_linking_groups.apply(lambda df: df.duplicated(keep=False)).any(
    axis=1
)

# opsd_linking_groups = opsd_linking_groups[~is_duplicated].reset_index(drop=True)
# opsd_linking_groups = opsd_linking_groups.set_index('BNETZA')


GROUPS_bnetza = (
    groups_bnetza.reset_index()
    .groupby("grouped")
    .aggregate(lambda df: df.tolist())
    .astype(str)
    .reset_index()
    .set_index("projectID")
    .grouped
)
GROUPS_uba = (
    groups_uba.reset_index()
    .groupby("grouped")
    .aggregate(lambda df: df.tolist())
    .astype(str)
    .reset_index()
    .set_index("projectID")
    .grouped
)
ppm_linking_groups = pd.DataFrame(
    {
        "BNETZA": MATCHED.projectID.BNETZA.astype(str).map(GROUPS_bnetza),
        "UBA": MATCHED.projectID.UBA.astype(str).map(GROUPS_uba),
    }
)
ppm_linking_groups = ppm_linking_groups.drop_duplicates()


# ppm_linking_groups = ppm_linking_groups.set_index('BNETZA')

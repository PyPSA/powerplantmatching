#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  8 14:57:12 2017

@author: fabian
"""
import powerplantmatching as pm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

BNETZA = pm.collection.Collection('BNETZA', use_saved_aggregation=False)
UBA = pm.collection.Collection('UBA', use_saved_aggregation=False)

# Since UBA only comprises units >= 100 MW, BNETZA needs to be filtered
# accordingly:
BNETZA = BNETZA.loc[BNETZA.Capacity >= 100]

red_UBA_BNETZA = pm.collection.reduce_matched_dataframe(
    pm.collection.combine_multiple_datasets([UBA, BNETZA], labels=['UBA', 'BNETZA']))

bnet_gas = BNETZA[BNETZA.Fueltype == 'Natural gas']
uba_gas = UBA[UBA.Fueltype == 'Natural Gas']
opsd_de = (pm.data.OPSD(statusDE=['operating', 'reserve', 'special_case',
                                  'shutdown_temporary'])
           .query("Country=='Germany' & Capacity >= 100")
           .pipe(pm.cleaning.clean_single, dataset_name='OPSD', use_saved_aggregation=False))
red_UBA_BNETZA_gas = red_UBA_BNETZA[red_UBA_BNETZA.Fueltype == 'Natural Gas']


dfs = [pm.utils.set_uncommon_fueltypes_to_other(df) for df in
       [opsd_de, red_UBA_BNETZA, UBA, BNETZA]]
keys = ['OPSD', 'Match-UBA-BNETZA', 'UBA', 'BNetzA']

# %%
fig, ax = pm.plot.fueltype_totals_bar(dfs, keys)
ax.set_xlabel('')
fig.savefig('UBA_BNETZA_matched_comparison.png', dpi=300)

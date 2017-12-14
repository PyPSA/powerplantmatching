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


UBA_BNETZA  = pm.collection.Collection(['UBA', 'BNETZA'], update=True, use_saved_aggregation=False, reduced=False)
red_UBA_BNETZA  = pm.collection.reduce_matched_dataframe(UBA_BNETZA)
BNETZA = pm.collection.Collection('BNETZA', use_saved_aggregation=False)
UBA = pm.collection.Collection('UBA', use_saved_aggregation=False)

# Since UBA only comprises units >= 100 MW, BNETZA needs to be filtered accordingly:
BNETZA = BNETZA.loc[BNETZA.Capacity>=100]

bnet_gas= BNETZA[BNETZA.Fueltype=='Natural gas']
uba_gas= UBA[UBA.Fueltype=='Natural Gas']
opsd_de = (pm.data.OPSD().query("Country=='Germany' & Capacity >= 100")
                      .pipe(pm.cleaning.clean_single, dataset_name='OPSD', use_saved_aggregation=True))
red_UBA_BNETZA_gas = red_UBA_BNETZA[red_UBA_BNETZA.Fueltype=='Natural Gas']


dfs = [opsd_de, red_UBA_BNETZA,UBA, BNETZA]
keys = [ 'OPSD_DE', 'Match-UBA-BNETZA', 'UBA','BNetzA']
fig, ax = pm.plot.bar_fueltype_totals(dfs,keys)
fig.savefig('UBA_BNETZA_matched_comparison.png', dpi=300)

#aggregate with non matched
bnetza = pm.data.BNETZA()[lambda df : df.Capacity>100]
red_UBA_BNETZA_ext = (red_UBA_BNETZA
                    .pipe(pm.heuristics.extend_by_non_matched, extend_by='UBA')
                    .pipe(pm.heuristics.extend_by_non_matched, extend_by='BNETZA')
                    .loc[lambda df: df.Capacity>=100])

red_nuc_ext = red_UBA_BNETZA_ext[red_UBA_BNETZA_ext.Fueltype=='Nuclear']
dfs = [opsd_de, red_UBA_BNETZA_ext,UBA, BNETZA]
keys = [ 'OPSD_DE', 'Match-UBA-BNETZA', 'UBA','BNetzA']
fig, ax = pm.plot.bar_fueltype_totals(dfs,keys)
fig.savefig('UBA_BNETZA_matched_and_extended_comparison.png', dpi=300)

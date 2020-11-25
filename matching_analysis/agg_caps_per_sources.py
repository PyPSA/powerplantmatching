#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 15:26:19 2018

@author: fabian
"""

import powerplantmatching as pm

dfs = pm.get_config()['matching_sources']
s = pm.data.Capacity_stats()

for name in dfs:
    df = getattr(pm.data, name)().dropna(subset=["lat"])
    fig, ax = pm.plot.factor_comparison([df, s], [name, 'stats'])
    fig.savefig('factor_plot_{}.png'.format(name), dpi=300)

name = 'Matched Data'
m = pm.powerplants()
fig, ax = pm.plot.factor_comparison([m, s], [name, r'ENTSOE SO\&AF'])
fig.savefig('factor_plot_{}.png'.format(name), dpi=300)

name = 'Matched Data without Extention'
# Note that here, low priority reliability matches are not excluded
m_wo_ext = pm.collection.collect(pm.get_config()['matching_sources'])[
    lambda df: df.lat.notnull()]
fig, ax = pm.plot.factor_comparison([m_wo_ext, s], [name, 'stats'])
fig.savefig('factor_plot_{}.png'.format(name), dpi=300)

# %% Outlayers
s = pm.data.Capacity_stats()
stats = pm.utils.lookup([m, s], ['Matched', 'SOAF'], by='Country')
print('Underrepresented Countries: ')
for c in (stats.Matched / stats.SOAF)[lambda x: x < 0.8].index:
    print('    - ', c)

print('Overrepresented Countries: ')
for c in (stats.Matched / stats.SOAF)[lambda x: x > 1.1].index:
    print('    - ', c)

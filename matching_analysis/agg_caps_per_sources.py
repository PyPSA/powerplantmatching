#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 15:26:19 2018

@author: fabian
"""

import powerplantmatching as pm

dfs = pm.config.get_config()['matching_sources']
s = pm.data.Capacity_stats()

for name in dfs:
    df = pm.data.data_config[name]['read_function']().dropna(subset=["lat"])
    fig, ax = pm.plot.factor_comparison([df, s], [name, 'stats'])
    fig.savefig('factor_plot_{}.png'.format(name), dpi=300)

name = 'Matched Data'
m = pm.collection.matched_data()
fig, ax = pm.plot.factor_comparison([m, s],
                                    [name, 'stats'])
fig.savefig('factor_plot_{}.png'.format(name), dpi=300)


name = 'Matched Data without Extention'
fig, ax = pm.plot.factor_comparison(
        [pm.collection.collect(pm.config.get_config()['matching_sources']), s],
        [name, 'stats'])
fig.savefig('factor_plot_{}.png'.format(name), dpi=300)

#%% Outlayers
s = pm.data.Capacity_stats()
stats = pm.utils.lookup([m, s], ['Matched', 'SOAF'], by='Country')
print('Underrepresented Countries: ')
for c in (stats.Matched / stats.SOAF)[lambda x: x < 0.8].index:
    print('    - ', c)

print('Overrepresented Countries: ')
for c in (stats.Matched / stats.SOAF)[lambda x: x > 1.1].index:
    print('    - ', c)

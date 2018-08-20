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
    df = pm.data.data_config[name]['read_function']()
    fig, ax = pm.plot.factor_comparison([df, s], [name, 'stats'])
    fig.savefig('factor_plot_{}.png'.format(name), dpi=300)

name = 'Matched Data'
fig, ax = pm.plot.factor_comparison([pm.collection.matched_data(), s],
                                    [name, 'stats'])
fig.savefig('factor_plot_{}.png'.format(name), dpi=300)

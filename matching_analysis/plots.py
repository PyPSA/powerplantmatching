#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 27 15:46:48 2018

@authors: Fabian Hofmann and Fabian Gotzens
"""

# Create plots for the paper
import powerplantmatching as pm
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from powerplantmatching.config import get_config
from powerplantmatching.data import (CARMA, ENTSOE, ESE, GEO, OPSD, WRI, WEPP,
                                     GPD)
from powerplantmatching.collection import (reduce_matched_dataframe as rmd,
                                           combine_multiple_datasets as cmd)
from powerplantmatching.utils import (set_uncommon_fueltypes_to_other
                                      as to_other)
figwidth = 10
plt.rc('savefig', dpi=300)
excluded_fueltypes = ['Wind', 'Solar', 'Battery', 'Hydrogen Storage',
                      'Electro-mechanical']
fueltype_to_color = get_config()['fuel_to_color']

# %% figure 1

# Prepare
carma = pm.data.CARMA()
entsoe = pm.data.ENTSOE()
ese = pm.data.ESE()
geo = pm.data.GEO()
opsd = (pm.data.OPSD()
        .assign(Fueltype=lambda df: df.Fueltype.fillna('Hard Coal')))
wri = pm.data.WRI(filter_other_dbs=False)
wepp = pm.data.WEPP()

df_dict = {0: 'CARMA', 1: 'ENTSOE', 2: 'ESE', 3: 'GEO', 4: 'OPSD', 5: 'WEPP',
           6: 'WRI'}
for i, name in df_dict.items():
    dfp = (locals()[str.lower(name)]
           .loc[lambda x: ~x.Fueltype.isin(excluded_fueltypes)])
    dfp.loc[:, 'matches'] = float(
        i) + 0.4 * (2. * np.random.rand(len(dfp)) - 1.)
    locals()[str.lower(name)] = dfp
ft = sorted(set(entsoe.Fueltype))
fig, ax = plt.subplots(figsize=(figwidth, 6))
for i, name in df_dict.items():
    dfr = locals()[str.lower(name)].loc[lambda x: ~x.Fueltype.isna()]
    dfr.plot.scatter(ax=ax, x='Capacity', y='matches', logx=True, alpha=0.5,
                     c=dfr.Fueltype.map(fueltype_to_color),
                     rasterized=True, s=dfr.Capacity / 50.0)
ax.set_ylim(-.5, 6.5)
ax.set_yticks(df_dict.keys())
ax.set_yticklabels(df_dict.values())
ax.yaxis.label.set_visible(False)
ax.set_xlabel(u'Capacity [$MW$]')
# ax.set_title('Overview of the different input databases')
ax.legend([plt.Line2D([0, 0], [0, 0], color=fueltype_to_color[f], lw=0,
                      markersize=8., marker='o') for f in pd.Series(ft)],
          ft, frameon=False)
fig.tight_layout()
fig.savefig('input_datasets_by_fueltype.png', dpi=300)


# %% Figure 3

fig, ax = pm.plot.boxplot_gross_to_net(figsize=(9, 5), axes_style='whitegrid')
ax.set_facecolor('lavender')
ax.grid(color='white', linestyle='dotted')
fig.tight_layout(pad=0.2)
fig.savefig('gross_net_boxplot.png')


# %% Figure 4

# Prepare
bnetza = pm.collection.Collection('BNETZA', use_saved_aggregation=False)
uba = pm.collection.Collection('UBA', use_saved_aggregation=False)

# Since UBA only comprises units >= 100 MW,
# BNETZA needs to be filtered accordingly:
bnetza = bnetza.loc[bnetza.Capacity >= 100]
matched_uba_bnetza = rmd(cmd([uba, bnetza], labels=['UBA', 'BNETZA']))
opsd = (OPSD()
        .pipe(pm.cleaning.aggregate_units, dataset_name='OPSD',
              use_saved_aggregation=True)
        .query("Country=='Germany' & Capacity >= 100"))


dfs = [to_other(df) for df in [opsd, matched_uba_bnetza, uba, bnetza]]
keys = ['OPSD', 'Match-UBA-BNETZA', 'UBA', 'BNetzA']

# Plot
fig, ax = pm.plot.fueltype_totals_bar(dfs, keys, axes_style='darkgrid',
                                      figsize=(figwidth, 4))
ax.set_xlabel('')
ax.grid(color='white', linestyle='dotted')
ax.legend(framealpha=0.5)
ax.set_facecolor('lavender')
fig.tight_layout(pad=0.5)
fig.savefig('uba_bnetza_matched_comparison.png', dpi=300)

compared = pm.utils.lookup([opsd, matched_uba_bnetza], ['opsd', 'ppm'],
                           by='Fueltype')
print('''\n\nDifference between manual opsd matches and automatic ppm matches
(ppm - opsd, positive -> overestimation): \n''')
print(compared.diff(axis=1)['ppm'] / 1000)
print('\ntotal caps\n', compared.sum())


# %% Figure 5

# Prepare
dfs = [CARMA(), ENTSOE(), ESE(), GEO(), GPD(filter_other_dbs=False), OPSD(),
       pm.collection.matched_data()]
dfs = [df[lambda df: (~df.Fueltype.isin(excluded_fueltypes))] for df in dfs]
keys = ['CARMA', 'ENTSOE', 'ESE', 'GEO', 'GPD', 'OPSD', 'Matched Data']

# Plot
fig, ax = pm.plot.fueltype_totals_bar(dfs, keys, axes_style='darkgrid',
                                      last_as_marker=True,
                                      figsize=(figwidth, 4.5),
                                      exclude=excluded_fueltypes)
ax.set_xlabel('')
ax.grid(color='white', linestyle='dotted')
ax.legend(framealpha=0.5)
ax.set_facecolor('lavender')
fig.tight_layout(pad=0.5)
fig.savefig('db_matched_fueltype_comparison.png', dpi=300)

# %% Figure 6

fig, ax = pm.plot.comparison_1dim(include_WEPP=False, by='Fueltype',
                                  axes_style='darkgrid',
                                  figsize=(figwidth, 4.),
                                  exclude=excluded_fueltypes)
ax.legend(framealpha=0.5)
ax.set_facecolor('lavender')
fig.tight_layout(pad=0.2)
fig.savefig('stats_matched_fueltype_comparison.png', dpi=300)


# %% Figure 7

fig, ax = pm.plot.comparison_1dim(figsize=(figwidth, 5.5),
                                  include_WEPP=False, axes_style='darkgrid',
                                  exclude=excluded_fueltypes)
ax.legend(framealpha=0.5)
ax.set_facecolor('lavender')
fig.tight_layout(pad=0.2)
fig.savefig('stats_matched_country_comparison.png', dpi=300)


# %% Figure 8

m = pm.collection.matched_data()
m = m[~m.Fueltype.isin(excluded_fueltypes)]
fig, ax = pm.plot.powerplant_map(m, scale=200)
#ax.annotate('(a)', (-13, 65))
fig.tight_layout(pad=0.2)
fig.savefig('powerplantmap_without_wepp.png', dpi=300)

# %%
m = pm.collection.Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced()
m = m[~m.Fueltype.isin(excluded_fueltypes)]
fig, ax = pm.plot.powerplant_map(m, scale=100, alternative_color_style=True)
ax.annotate('(b)', (-13, 65))
fig.tight_layout(pad=0.2)
fig.savefig('powerplantmap_with_wepp.png', dpi=300)


# %% Figure 9

fig, ax = pm.plot.comparison_1dim(how='scatter')
fig.tight_layout(pad=0.2)
fig.savefig('comparison_statistics.png', dpi=300)


# %% Figure 10

# Prepare
opsd = OPSD()
df_dict = {0: 'CARMA', 1: 'ENTSOE', 2: 'ESE', 3: 'GEO', 4: 'OPSD',
           5: 'WEPP', 6: 'WRI'}
for i, name in df_dict.items():
    dfp = (locals()[str.lower(name)]
           .loc[lambda x: ~x.Fueltype.isin(excluded_fueltypes)])
    dfp.loc[:, 'matches'] = float(
        i) + 0.4 * (2. * np.random.rand(len(dfp)) - 1.)
    locals()[str.lower(name)] = dfp

df = pm.collection.Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced()
df['matches'] = df['projectID'].apply(len)
df['matches'] += 0.4 * (2. * np.random.rand(len(df)) - 1.)

projects = ['Matched w/ WEPP', 'OPSD', 'CARMA', 'ENTSOE', 'ESE', 'GEO', 'WRI',
            'WEPP']
cmap = pd.Series(sns.color_palette(n_colors=len(projects)), projects)

# Plot
fig, ax = plt.subplots(nrows=2, sharey=False, sharex=True,
                       figsize=(figwidth, 7.))
df.plot.scatter(x='Capacity', y='matches', logx=True, ax=ax[0], s=1.,
                c=cmap[projects[0]], rasterized=True)

for i, name in df_dict.items():
    dfr = locals()[str.lower(name)]
    dfr.plot.scatter(x='Capacity', y='matches', c=cmap[name],
                     logx=True, ax=ax[1], alpha=1, s=1., rasterized=True)

ax[0].set_xlim(1e-3, 1e4)
ax[0].set_ylim(1.5, 7.9)
ax[0].set_yticks([2, 3, 4, 5, 6, 7])
ax[0].set_ylabel('# of matches')
ax[0].set_title('(a)')
ax[0].legend([plt.Line2D([0, 0], [0, 0], color=c, lw=0, markersize=4.,
                         marker='o') for c in cmap.values],
             projects, frameon=False)

ax[1].set_ylim(-.5, 6.5)
ax[1].set_yticks(df_dict.keys())
ax[1].set_yticklabels(df_dict.values())
ax[1].yaxis.label.set_visible(False)
ax[1].set_xlabel(u'Capacity [$MW$]')
ax[1].set_title('(b)')
fig.tight_layout(pad=0.5)
fig.savefig('number_of_matches_per_capacity_subplots2.png', dpi=300)


# %% Figure 11

# Prepare
opsd = OPSD().loc[lambda x: ~x.Fueltype.isin(excluded_fueltypes)]
wepp = WEPP().loc[lambda x: ~x.Fueltype.isin(excluded_fueltypes)]
df_w_wepp = pm.collection.\
    Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced()
df_wo_wepp = pm.collection.MATCHED_dataset(include_unavailables=True)
keys = ['WEPP', 'OPSD', 'Matched w/ WEPP', 'Matched w/o WEPP']
dfs = [wepp, opsd, df_w_wepp, df_wo_wepp]
dfs = [d[lambda df: (~df.Fueltype.isin(excluded_fueltypes))] for d in dfs]
# Plot
fig, ax = pm.plot.area_yearcommissioned(dfs, keys)
# fig.tight_layout()
fig.savefig('capacity_additions_century.png', dpi=200)


# %% Table 4 - Data

yr_wepp = df_w_wepp.pivot_table(values='YearCommissioned', index='Country',
                                aggfunc='count')
count_wepp = df_w_wepp.pivot_table(values='Name', index='Country',
                                   aggfunc='count')
ratio_wepp = yr_wepp.iloc[:, 0].div(count_wepp.iloc[:, 0])

yr_wo = df_wo_wepp.pivot_table(values='YearCommissioned', index='Country',
                               aggfunc='count')
count_wo = df_wo_wepp.pivot_table(values='Name', index='Country',
                                  aggfunc='count')
ratio_wo = yr_wo.iloc[:, 0].div(count_wo.iloc[:, 0])


# %% Stats

m_with_wepp = pm.collection.\
    Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced()
m_without_wepp = pm.collection.MATCHED_dataset(include_unavailables=True)
stats = pm.data.Capacity_stats()

lu = pm.utils.lookup([m_with_wepp, m_without_wepp, stats],
                     ['with Wepp', 'without Wepp', 'stats'],
                     by=['Country', 'Fueltype'],
                     exclude=excluded_fueltypes)

# %% Figure 12
fig, ax = pm.plot.comparison_countries_fueltypes_bar(
    include_WEPP=True, include_VRE=False, show_indicators=False, year=2015,
    legend_in_subplots=False, exclude=excluded_fueltypes, figsize=(18, 10))
fig.savefig('comparison_countries_fueltypes.png', dpi=200)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 27 15:46:48 2018

@author: fabian
"""

#create plots for the paper
import powerplantmatching as pm
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from powerplantmatching.data import CARMA, ENTSOE, ESE, GEO, OPSD, WRI, WEPP
from powerplantmatching.collection import (reduce_matched_dataframe as rmd, 
                                           combine_multiple_datasets as cmd)
from powerplantmatching.utils import set_uncommon_fueltypes_to_other as to_other
from powerplantmatching.data import data_config
figwidth = 10
plt.rc('savefig', dpi=300)
excluded_fueltypes = ['Wind','Solar', 'Battery']



#%% figure 2 

fig, ax = pm.plot.boxplot_gross_to_net(figsize=(9,5), axes_style='whitegrid')
ax.set_facecolor('lavender')
ax.grid(color='white', linestyle='dotted')
fig.tight_layout(pad = 0.2)
fig.savefig('gross_net_boxplot.png')


#%%figure 3

#prepare
bnetza = pm.collection.Collection('BNETZA', use_saved_aggregation=False)
uba = pm.collection.Collection('UBA', use_saved_aggregation=False)

# Since UBA only comprises units >= 100 MW, BNETZA needs to be filtered accordingly:
bnetza = bnetza.loc[bnetza.Capacity>=100]
matched_uba_bnetza = rmd(cmd([uba, bnetza], labels=['UBA', 'BNETZA']))
opsd = (OPSD().pipe(pm.cleaning.clean_single, dataset_name='OPSD', 
                 use_saved_aggregation=True)
               .query("Country=='Germany' & Capacity >= 100"))

dfs = [to_other(df) for df in [opsd, matched_uba_bnetza,uba, bnetza]]
keys = [ 'OPSD', 'Match-UBA-BNETZA', 'UBA','BNetzA']

#plot
fig, ax = pm.plot.fueltype_totals_bar(dfs, keys, axes_style='darkgrid',
                                      figsize=(figwidth, 4))
ax.set_xlabel('')
ax.grid(color='white', linestyle='dotted')
ax.legend(framealpha=0.5)
ax.set_facecolor('lavender')
fig.tight_layout(pad=0.2)
fig.savefig('uba_bnetza_matched_comparison.png', dpi=300)

compared = pm.utils.lookup([opsd, matched_uba_bnetza], ['opsd', 'ppm'], by='Fueltype')
print '''\n\nDifference between manual opsd matches and automatic ppm matches 
(ppm - opsd, positive -> overestimation): \n'''
print compared.diff(axis=1)['ppm']/1000
print '\ntotal caps\n',  compared.sum()
                      

#%% figure 4 

#prepare
dfs = [CARMA(), ENTSOE(), ESE(), GEO(), OPSD(), WRI(), 
       pm.collection.MATCHED_dataset(include_unavailables=True)]
dfs = [df[lambda df: (~df.Fueltype.isin(excluded_fueltypes))] for df in dfs]
keys = ['CARMA', 'ENTSOE', 'ESE', 'GEO', 'OPSD', 'WRI', 'Matched w/o WEPP']

#plot
fig, ax = pm.plot.fueltype_totals_bar(dfs, keys, axes_style='darkgrid',
                                      last_as_marker=True, figsize=(figwidth,4.5),
                                      exclude=excluded_fueltypes)
ax.set_xlabel('')
ax.grid(color='white', linestyle='dotted')
ax.legend(framealpha=0.5)
ax.set_facecolor('lavender')
fig.tight_layout(pad=0.2)
fig.savefig('db_matched_fueltype_comparison.png', dpi=300)

#%% figure 5

fig, ax = pm.plot.comparison_1dim(figsize=(figwidth,5.5),
        include_WEPP=False, axes_style='darkgrid') 
ax.legend(framealpha=0.5)
ax.set_facecolor('lavender')
fig.tight_layout(pad=0.2)
fig.savefig('stats_matched_country_comparison.png', dpi=300)


#%% figure 6 

fig, ax = pm.plot.comparison_1dim(include_WEPP=False, by='Fueltype',
                                  axes_style='darkgrid', figsize=(figwidth,4.)) 
ax.legend(framealpha=0.5)
ax.set_facecolor('lavender')
fig.tight_layout(pad=0.2)
fig.savefig('stats_matched_fueltype_comparison.png', dpi=300)


#%% figure 7

m = pm.collection.MATCHED_dataset(include_unavailables=True)
m = m[(~m.Fueltype.isin(excluded_fueltypes))]
fig, ax = pm.plot.powerplant_map(m, scale=100)
fig.tight_layout(pad=0.2)
fig.savefig('powerplantmap_without_wepp.png', dpi=300)


m = pm.collection.Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced()
m = m[(~m.Fueltype.isin(excluded_fueltypes))]
fig, ax = pm.plot.powerplant_map(m, scale=100)
fig.tight_layout(pad=0.2)
fig.savefig('powerplantmap_with_wepp.png', dpi=300)


#%% figure 8

fig, ax = pm.plot.comparison_1dim(how='scatter')
fig.tight_layout(pad=0.2)
fig.savefig('comparison_statitics.png', dpi=300)


#%% figure 9 

#prepare
df_dict = {0: 'CARMA',1: 'ENTSOE',2: 'ESE',
           3: 'GEO',4: 'OPSD', 5: 'WEPP', 6: 'WRI'}
for i, name in df_dict.items():
    dfp = (data_config[name]['read_function']()
                    .loc[lambda x: ~x.Fueltype.isin(excluded_fueltypes)])
    dfp.loc[:,'matches'] = float(i) + 0.4*(2.*np.random.rand(len(dfp)) - 1.)
    locals()[str.lower(name)] = dfp
    
df = pm.collection.Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced()
df['matches'] = df['projectID'].apply(len)
df['matches'] +=  0.4*(2.*np.random.rand(len(df)) - 1.)

projects = ['Matched by at least 2','OPSD', 'CARMA', 'ENTSOE', 'ESE', 'GEO', 'WRI','WEPP']
cmap = pd.Series(sns.color_palette(n_colors=len(projects)), projects)

#plot
fig, ax = plt.subplots(nrows=2, sharey=False, sharex=True, figsize=(figwidth, 7))
df.plot.scatter(x='Capacity', y='matches', logx=True, ax=ax[0], s=1., 
                c=cmap['Matched by at least 2'], rasterized=True)

for i, name in df_dict.items():
    dfr = locals()[str.lower(name)]
    dfr.plot.scatter(x='Capacity', y='matches', c=cmap[name], 
                     logx=True, ax=ax[1], alpha=1, s=1., rasterized=True)

ax[0].set_xlim(1e-3, 1e4)
ax[0].set_ylim(1.5, 7.9)
ax[0].set_yticks([2, 3, 4, 5, 6, 7])
ax[0].set_ylabel('# of matches')
ax[0].set_title('(a)')

ax[1].set_ylim(-.5, 6.5)
ax[1].set_yticks(df_dict.keys())
ax[1].set_yticklabels(df_dict.values())
ax[1].yaxis.label.set_visible(False)
ax[1].set_xlabel(u'Capacity [$MW$]')
ax[1].set_title('(b)')

ax[0].legend([plt.Line2D([0,0], [0,0], color=c, lw=0, markersize=4., marker='o') for c in cmap.values],
           projects, frameon=False)
fig.tight_layout(pad=0.5)
fig.savefig('number_of_matches_per_capacity_subplots2.png', dpi=300)



#%% stats

m_with_wepp = pm.collection.Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced()
m_without_wepp = pm.collection.MATCHED_dataset(include_unavailables=True)
stats = pm.data.Capacity_stats()
 
lu = pm.utils.lookup([m_with_wepp, m_without_wepp, stats], 
                     ['with Wepp', 'without Wepp', 'stats'], by=['Country','Fueltype'],
                     exclude=excluded_fueltypes)














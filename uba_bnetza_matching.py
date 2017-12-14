#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 14:17:28 2017

@author: fabian
"""
import powerplantmatching as pm
import pandas as pd

OPSD_links = pd.read_html('https://github.com/Open-Power-System-Data/conventional_power_plants/blob/4b7cf76f01f8aaa617e578fb225759bd17923757/input/matching_bnetza_uba.csv')[0]
OPSD_links.rename(columns={'ID BNetzA':'BNETZA', 'uba_match_name': 'UBA'}, inplace=True)
OPSD_links.uba_match_fuel.replace({u'Biomasse':'Bioenergy',
                                 u'Gichtgas':'Other',
                                 u'HS':'Oil',
                                 u'Konvertergas':'Other',
                                 u'Licht':'Solar',
                                 u'Raffineriegas':'Other',
                                 u'Uran':'Nuclear',
                                 u'Wasser':'Hydro',
                                 'Wind (L)':'Wind',
                                 u'\xd6lr\xfcckstand':'Oil'}, inplace=True)


#%% get manual links from opsd
uba = pm.data.UBA()
bnetza = pm.data.BNETZA()
#create subset of uba and bentza entries in opsd and in ppm (due to different version...)
OPSD_links = OPSD_links[(OPSD_links.BNETZA.isin(bnetza.projectID)) & (OPSD_links.UBA.isin(uba.Name))]
bnetza = bnetza[bnetza.projectID.isin(OPSD_links.BNETZA)]
uba = uba[uba.Name.isin(OPSD_links.UBA)]

#define a map for uba name-> projectID
duplicated_name = uba.loc[uba.Name.duplicated(False), 'Name']
uba.loc[uba.Name.isin(duplicated_name), 'Name'] += ' ' + uba.loc[uba.Name.isin(duplicated_name), 'Fueltype']
uba.Name = uba.Name.replace('\s+', ' ', regex=True)
uba_projectID_map = uba.set_index('Name').projectID


#get rid of non-unique names
OPSD_links.loc[OPSD_links.UBA.isin(duplicated_name), 'UBA'] += (
        ' ' + OPSD_links.loc[OPSD_links.UBA.isin(duplicated_name), 'uba_match_fuel'])
OPSD_links = OPSD_links.set_index('BNETZA').UBA
OPSD_links = OPSD_links.map(uba_projectID_map)
OPSD_links_r = OPSD_links.reset_index(drop=False).set_index('UBA').BNETZA



UBA = pm.cleaning.clean_single(uba)
BNETZA = pm.cleaning.clean_single(bnetza)

MATCHED = pm.collection.combine_multiple_datasets([UBA, BNETZA], ['UBA', 'BNETZA'])
matched = pm.collection.reduce_matched_dataframe(MATCHED)

ppm_links = pd.Series()
ppm_links.name = 'UBA'
for d in matched.projectID:
    for pp1 in d['BNETZA']:
        for pp2 in d['UBA']:
            ppm_links.loc[pp1] = pp2
ppm_links = ppm_links.sort_index()
ppm_links.index.name = 'BNETZA'
ppm_links_r = ppm_links.reset_index(drop=False).set_index('UBA').BNETZA


opsd_de = (pm.data.OPSD(statusDE=['operating'])
               .pipe(pm.cleaning.clean_single, dataset_name='OPSD', use_saved_aggregation=False)
               .loc[lambda row: row.Country=='Germany']
               .loc[lambda row: row.Capacity>=100])
dfs = {'UBA':UBA, 'BNetzA':BNETZA, 'OPSD':opsd_de, 'Match-UBA-BNETZA':matched}
pm.plot.bar_fueltype_totals(dfs.values(), dfs.keys())

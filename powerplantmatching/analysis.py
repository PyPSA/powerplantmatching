# -*- coding: utf-8 -*-
## Copyright 2015-2016 Fabian Gotzens (FZJ)

## This program is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 3 of the
## License, or (at your option) any later version.

## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.

## You should have received a copy of the GNU General Public License
## along with this program.  If not, see <http://www.gnu.org/licenses/>.
# %%

import logging
logger = logging.getLogger(__name__)
from powerplantmatching import data
from powerplantmatching import matching as mat
from powerplantmatching.cleaning import clean_single
from powerplantmatching.collection import Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced
from powerplantmatching.heuristics import extend_by_non_matched
from powerplantmatching import plot

#%%
"""
--- Comparison_OpenDB_WEPP ---
"""
datasets = {'OpenDBs':'odb',
            'WEPP':'wepp'}
odb = Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced()
#HACK: Temporary until final naming convention for reduced dfs is implemented
#    odb.insert(0, 'Name', np.NaN)
#    odb.loc[:, 'Name'] = odb.Name.fillna(odb.OPSD).fillna(odb.ESE).fillna(odb.ENTSOE)\
#                            .fillna(odb.WRI).fillna(odb.GEO).fillna(odb.CARMA)
#    odb.drop(labels=['CARMA','ENTSOE','ESE','GEO','OPSD','WRI'], axis=1, inplace=True)
logger.info('Vertical clean WEPP')
wepp = clean_single(data.WEPP(), dataset_name='WEPP')
dfs = [eval(name) for name in datasets.values()]
logger.info('Horizontal match OpenDBs vs. WEPP')
matched = mat.combine_multiple_datasets(dfs, datasets.keys())
reduced = mat.reduce_matched_dataframe(matched)


#%%
"""
--- Comparison_UBA_BNetzA_vs_OPSD ---
"""

uba = clean_single(data.UBA(), dataset_name='UBA', use_saved_aggregation=True)
bnetza = clean_single(data.BNETZA(), dataset_name='BNETZA', use_saved_aggregation=False)
# Since UBA only comprises units >= 100 MW, BNETZA needs to be filtered accordingly:
bnetza = bnetza.loc[bnetza.Capacity>=100]
bnetza_o_100 = data.BNETZA().loc[lambda row: row.Capacity>=100]
# Match and Reduce
match_UBA_BNETZA = mat.combine_multiple_datasets([uba, bnetza], ['UBA', 'BNETZA'])
red_UBA_BNETZA = mat.reduce_matched_dataframe(match_UBA_BNETZA)
opsd_de = (data.OPSD().pipe(clean_single, dataset_name='OPSD', use_saved_aggregation=True)
                      .loc[lambda row: row.Country=='Germany' & row.Capacity>=100])
dfs = {'UBA':uba, 'BNetzA':bnetza, 'OPSD_DE':opsd_de, 'Match-UBA-BNETZA':red_UBA_BNETZA}
plot.bar_fueltype_totals(dfs.values(), dfs.keys())
red_extended = (red_UBA_BNETZA
                #.pipe(extend_by_non_matched, data.UBA(), 'UBA')
                .pipe(extend_by_non_matched, bnetza_o_100, label='BNETZA'))
dfs2 = {'UBA':uba, 'BNetzA':bnetza, 'OPSD_DE':opsd_de, 'Match-UBA-BNETZA':red_extended}
plot.bar_fueltype_totals(dfs2.values(), dfs2.keys())
hc_opsd = opsd_de.loc[lambda df: df.Fueltype=='Hard Coal']
hc_match = red_extended.loc[lambda df: df.Fueltype=='Hard Coal']

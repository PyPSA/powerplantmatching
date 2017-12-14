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
datasets = {'Matched w/o WEPP':'odb',
            'WEPP':'wepp'}
odb = Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced()
logger.info('Vertical clean WEPP')
wepp = clean_single(data.WEPP(), dataset_name='WEPP')
dfs = [eval(name) for name in datasets.values()]
logger.info('Horizontal match OpenDBs vs. WEPP')
matched = mat.combine_multiple_datasets(dfs, datasets.keys())
reduced = mat.reduce_matched_dataframe(matched)



## Copyright 2015-2016 Fabian Hofmann (FIAS), Jonas Hoersch (FIAS)

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
"""
Processed datasets of merged and/or adjusted data
"""
from __future__ import print_function

import os
import pandas as pd
import ast

from .utils import set_uncommon_fueltypes_to_other
from .data import CARMA,ENTSOE, GEO, OPSD, WRI, ESE, Oldenburgdata, WEPP
from .cleaning import clean_single
from .matching import (combine_multiple_datasets,
                       reduce_matched_dataframe)
from .heuristics import extend_by_non_matched


def Carma_ENTSOE_GEO_OPSD_matched(update=False, use_saved_aggregation=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Entsoe_Geo_Opsd.csv')
    if update: #or not os.path.exists(outfn):
        datasets = [clean_single(CARMA(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(ENTSOE(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(GEO(), aggregate_powerplant_units=False),
                    clean_single(OPSD(), use_saved_aggregation=use_saved_aggregation)]
        matched = combine_multiple_datasets(datasets, ['CARMA','ENTSOE', 'GEO', 'OPSD'])
        matched.to_csv(outfn, index_label='id', encoding='utf-8')
        return matched
    else:
        return pd.read_csv(outfn,index_col=0, header=[0,1])

def Carma_ENTSOE_GEO_OPSD_matched_reduced(update=True):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Entsoe_Geo_Opsd_reduced.csv')
    if update:
        sdf = reduce_matched_dataframe(Carma_ENTSOE_GEO_OPSD_matched())
        sdf.to_csv(outfn, index_label='id', encoding='utf-8')
        return sdf
    else:
        sdf = pd.read_csv(outfn, index_col='id')
        sdf.projectID = sdf.projectID.apply(lambda df: ast.literal_eval(df))
        return sdf

    
def Carma_ENTSOE_GEO_OPSD_WRI_matched(update=False, use_saved_aggregation=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Entsoe_Geo_Opsd_Wri.csv')
    if update: #or not os.path.exists(outfn):
        datasets = [clean_single(CARMA(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(ENTSOE(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(GEO(), aggregate_powerplant_units=False),
                    clean_single(OPSD(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(WRI(), use_saved_aggregation=use_saved_aggregation)]
        matched = combine_multiple_datasets(datasets, ['CARMA','ENTSOE', 'GEO',
                                                       'OPSD','WRI'])
        matched.to_csv(outfn, index_label='id', encoding='utf-8')
        return matched
    else:
        return pd.read_csv(outfn,index_col=0, header=[0,1])

def Carma_ENTSOE_GEO_OPSD_WRI_matched_reduced(update=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Entsoe_Geo_Opsd_Wri_reduced.csv')
    if update:
        sdf = reduce_matched_dataframe(Carma_ENTSOE_GEO_OPSD_WRI_matched())
        sdf.to_csv(outfn, index_label='id', encoding='utf-8')
        return sdf
    else:
        sdf = pd.read_csv(outfn, index_col='id')
        sdf.projectID = sdf.projectID.apply(lambda df: ast.literal_eval(df))
        return sdf




def MATCHED_dataset(aggregated_hydros=True, rescaled_hydros=False, subsume_uncommon_fueltypes=False, 
                    include_unavailables=False):
    """
    This returns the actual match between the Carma-data, GEO-data, WRI-data,
    FIAS-data and the ESE-data with an additional manipulation on the hydro 
    powerplants. The latter were adapted in terms of the power plant 
    technology (Run-of-river, Reservoir, Pumped-Storage) and were 
    quantitatively  adjusted to the ENTSOE-statistics. For more information 
    about the technology and adjustment, see the hydro-aggreation.py file.

    Parameters
    ----------
    rescaled_hydros : Boolean, defaut False
            Whether to rescale hydro powerplant capacity in order to fulfill the
            statistics of the ENTSOE-data and receive better covering of the country
            totals.
    subsume_uncommon_fueltypes : Boolean, default False
            Whether to reduce the fueltype specification such that "Geothermal", "Waste"
            and "Mixed Fueltypes" are declared as "Other".
    """

    if include_unavailables:
        matched = Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced()
    else:
        matched = Carma_ENTSOE_GEO_OPSD_WRI_matched_reduced()
        
    matched = extend_by_non_matched(matched, OPSD(), 'OPSD', clean_added_data=True)
    if include_unavailables:
        ese = ESE()
        ese.projectID
        matched = extend_by_non_matched(matched, ESE(), 'ESE', clean_added_data=False)
        
#    matched = extend_by_non_matched(matched,
#            clean_single(WRI(), use_saved_aggregation=True), 'WRI',
#                        fueltypes=['Wind'])

    if aggregated_hydros:
        hydro = Aggregated_hydro(scaled_capacity=rescaled_hydros)
        matched = matched[matched.Fueltype != 'Hydro']
        matched = pd.concat([matched, hydro]).reset_index(drop=True)
    if subsume_uncommon_fueltypes:
        matched = set_uncommon_fueltypes_to_other(matched)
    return matched  

    
def Aggregated_hydro(update=False, scaled_capacity=True):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'hydro_aggregation_beta.csv')
#    if update or not os.path.exists(outfn):
#        # Generate the matched database
#        raise NotImplemented()
#        # matched_df = ...
#
#        # matched_df.to_csv(outfn)
#        # return matched_df
    hydro = pd.read_csv(outfn, index_col='id')
    if scaled_capacity:
        hydro.Capacity = hydro.loc[:,'Scaled Capacity']
    return hydro.drop('Scaled Capacity', axis=1)


#unpublishable
def Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched(update=False, use_saved_aggregation=False,
                                                 add_Oldenburgdata=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Entsoe_Ese_Geo_Opsd_Wri.csv')
    if update: #or not os.path.exists(outfn):
        datasets = [clean_single(CARMA(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(ENTSOE(), use_saved_aggregation=use_saved_aggregation),
                    ESE(add_Oldenburgdata=add_Oldenburgdata),
                    clean_single(GEO(), aggregate_powerplant_units=False),
                    clean_single(OPSD(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(WRI(), use_saved_aggregation=use_saved_aggregation)]
        matched = combine_multiple_datasets(datasets, ['CARMA', 'ENTSOE','ESE', 
                                                       'GEO','OPSD','WRI'])
        matched.to_csv(outfn, index_label='id', encoding='utf-8')
        return matched
    else:
        return pd.read_csv(outfn, index_col=0, header=[0,1])

#unpublishable
def Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced(update=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Entsoe_Ese_Geo_Opsd_Wri_reduced.csv')
    if update:
        sdf = reduce_matched_dataframe(Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched())
        sdf.to_csv(outfn, index_label='id', encoding='utf-8')
        return sdf
    else:
        sdf = pd.read_csv(outfn, index_col='id')
        sdf.projectID = sdf.projectID.apply(lambda df: ast.literal_eval(df))
        return sdf

#unpublishable
def Carma_ENTSOE_GEO_OPSD_WEPP_WRI_matched(update=False, use_saved_aggregation=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Entsoe_Geo_Opsd_Wepp_Wri.csv')
    if update: #or not os.path.exists(outfn):
        datasets = [clean_single(CARMA(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(ENTSOE(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(GEO(), aggregate_powerplant_units=False),
                    clean_single(OPSD(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(WEPP(), use_saved_aggregation=use_saved_aggregation),
                    clean_single(WRI(), use_saved_aggregation=use_saved_aggregation)]
        matched = combine_multiple_datasets(datasets, ['CARMA','ENTSOE', 'GEO',
                                                       'OPSD','WEPP','WRI'])
        matched.to_csv(outfn, index_label='id', encoding='utf-8')
        return matched
    else:
        return pd.read_csv(outfn,index_col=0, header=[0,1])        
            


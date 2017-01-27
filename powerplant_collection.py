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

from .utils import set_uncommon_fueltypes_to_other
from .data import CARMA,ENTSOE, GEO, OPSD, WRI, ESE, Oldenburgdata, WEPP
from .cleaning import clean_single
from .matching import (combine_multiple_datasets,
                       reduce_matched_dataframe)
from .heuristics import extend_by_non_matched


def Carma_GEO_OPSD_WRI_matched(update=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Geo_Opsd_Wri.csv')
    if update: #or not os.path.exists(outfn):
        datasets = [clean_single(CARMA()),
                    clean_single(GEO(), aggregate_powerplant_units=False),
                    clean_single(OPSD()),
                    clean_single(WRI())]
        matched = combine_multiple_datasets(datasets, ['CARMA', 'GEO',
                                                       'OPSD','WRI'])
        matched.to_csv(outfn, index_label='id', encoding='utf-8')
        return matched
    else:
        return pd.read_csv(outfn,index_col=0, header=[0,1])

def Carma_GEO_OPSD_WRI_matched_reduced():
    return pd.read_csv(os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Geo_Opsd_Wri_reduced.csv'),
                        index_col='id')
            
def Carma_ENTSOE_GEO_OPSD_WRI_matched(update=False, clean=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Entsoe_Geo_Opsd_Wri.csv')
    if update: #or not os.path.exists(outfn):
        datasets = [clean_single(CARMA()),
                    clean_single(ENTSOE()),
                    clean_single(GEO(), aggregate_powerplant_units=False),
                    clean_single(OPSD()),
                    clean_single(WRI())]
        matched = combine_multiple_datasets(datasets, ['CARMA','ENTSOE', 'GEO',
                                                       'OPSD','WRI'])
        matched.to_csv(outfn, index_label='id', encoding='utf-8')
        return matched
    else:
        if clean: print("Warning: clean=True does not have any effect, if update=False!")
        return pd.read_csv(outfn,index_col=0, header=[0,1])
        
def Carma_GEO_OPSD_WRI_WEPP_matched(update=False, clean=False):
    labels = ['CARMA', 'GEO','OPSD','WRI', 'WEPP']
    outfn = os.path.join(os.path.dirname(__file__), 'data', 'Matched_Carma_Geo_Opsd_Wri_Wepp.csv')
    outcarma = os.path.join(os.path.dirname(__file__), 'data', 'Cleaned_Carma.csv')
    outgeo = os.path.join(os.path.dirname(__file__), 'data', 'Cleaned_Geo.csv')
    outopsd = os.path.join(os.path.dirname(__file__), 'data', 'Cleaned_Opsd.csv')
    outwri = os.path.join(os.path.dirname(__file__), 'data', 'Cleaned_Wri.csv')
    outwepp = os.path.join(os.path.dirname(__file__), 'data', 'Cleaned_Wepp.csv')
    
    if update: #or not os.path.exists(outfn):
        if clean: 
            datasets = [clean_single(CARMA()),
                        clean_single(GEO(), aggregate_powerplant_units=False),
                        clean_single(OPSD()),
                        clean_single(WRI()),
                        clean_single(WEPP())]
            # write these cleaned databases to CSVs
            datasets[0].to_csv(outcarma, index_label='id', encoding='utf-8')
            datasets[1].to_csv(outgeo, index_label='id', encoding='utf-8')
            datasets[2].to_csv(outopsd, index_label='id', encoding='utf-8')
            datasets[3].to_csv(outwri, index_label='id', encoding='utf-8')
            datasets[4].to_csv(outwri, index_label='id', encoding='utf-8')
        else:
            # read existing cleaned csvs
            datasets = [pd.read_csv(outcarma,index_col=0, encoding='utf-8'),
                        pd.read_csv(outgeo,index_col=0, encoding='utf-8'),
                        pd.read_csv(outopsd,index_col=0, encoding='utf-8'),
                        pd.read_csv(outwri,index_col=0, encoding='utf-8'),
                        pd.read_csv(outwepp,index_col=0, encoding='utf-8')]
        # Now trigger horizontal matching
        matched = combine_multiple_datasets(datasets, labels)
        matched.to_csv(outfn, index_label='id', encoding='utf-8')
        return matched
    else:
        if clean: print("Warning: clean=True does not have any effect, if update=False!")
        return pd.read_csv(outfn,index_col=0, header=[0,1])


#unpublishable
def Carma_ESE_GEO_OPSD_OLDENB_WRI_matched(update=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Ese_Oldenburg_Geo_Opsd_Wri.csv')
    if update: #or not os.path.exists(outfn):
        datasets = [clean_single(CARMA()),
                    pd.concat([clean_single(Oldenburgdata(), 
                                            aggregate_powerplant_units=False),
                               ESE()]).reset_index(drop=True),
                    clean_single(GEO(), aggregate_powerplant_units=False),
                    clean_single(OPSD()),
                    clean_single(WRI())]
        matched = combine_multiple_datasets(datasets, ['CARMA', 'ESE_Oldenburg', 
                                                       'GEO','OPSD','WRI'])
        matched.to_csv(outfn, index_label='id', encoding='utf-8')
        return matched
    else:
        return pd.read_csv(outfn, index_col=0, header=[0,1])

#unpublishable
def Carma_ESE_GEO_OPSD_OLDENB_WRI_matched_reduced():
    return pd.read_csv(os.path.join(os.path.dirname(__file__), 'data',
                         'Matched_Carma_Ese_Oldenburg_Geo_Opsd_Wri_reduced.csv'),
                        index_col='id')


def MATCHED_dataset(rescaled_hydros=False, subsume_uncommon_fueltypes=False, 
                    include_unavailables=False):
    """
    This returns the actual match between the Carma-data, GEO-data, WRI-data,
    FIAS-data and the ESE-data with an additional manipulation on the hydro 
    powerplants. The latter were adapted in terms of the power plant 
    classification (Run-of-river, Reservoir, Pumped-Storage) and were 
    quantitatively  adjusted to the ENTSOE-statistics. For more information 
    about the classification and adjustment, see the hydro-aggreation.py file.

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

    hydro = Aggregated_hydro(scaled_capacity=rescaled_hydros)
    matched = extend_by_non_matched(Carma_GEO_OPSD_WRI_matched_reduced(),
                                    GEO(), 'GEO')
    if include_unavailables:
        matched = extend_by_non_matched(
                                Carma_ESE_FIAS_GEO_OPSD_WRI_matched_reduced(),
                                GEO(), 'GEO')
    matched = matched[matched.Fueltype != 'Hydro']
    if subsume_uncommon_fueltypes:
        matched = set_uncommon_fueltypes_to_other(matched)
    return pd.concat([matched, hydro]).reset_index(drop=True)

    
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

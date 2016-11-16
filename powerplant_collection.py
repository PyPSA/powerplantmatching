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
from .data import CARMA_data, FIAS_data, GEO_data, OPSD_data, WRI_data
from .cleaning import clean_single
from .matching import (combine_multiple_datasets,
                       reduce_matched_dataframe)
from .heuristics import extend_by_non_matched
    
def Carma_FIAS_GEO_OPSD_WRI_matched(update=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data', 
                         'Matched_Carma_Fias_Geo_Opsd_Wri.csv')
    if update: #or not os.path.exists(outfn):
        datasets = [clean_single(CARMA_data()),
                    clean_single(FIAS_data(), aggregate_units=False),
                    clean_single(GEO_data(), aggregate_units=False), 
                    clean_single(OPSD_data()),
                    clean_single(WRI_data())]
        matched = combine_multiple_datasets(datasets, ['CARMA', 'FIAS', 'GEO','OPSD','WRI'])

#        raise NotImplemented
        # matched_df = ...
        # matched_df.to_csv(outfn)
        return matched
    else:
        return pd.read_csv(outfn, index_col='id')
        
def Carma_GEO_OPSD_WRI_matched(update=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data', 
                         'Matched_Carma_Fias_Geo_Opsd_Wri.csv')
    if update: #or not os.path.exists(outfn):
        datasets = [clean_single(CARMA_data()),
                    clean_single(GEO_data(), aggregate_units=False), 
                    clean_single(OPSD_data()),
                    clean_single(WRI_data())]
        matched = combine_multiple_datasets(datasets, ['CARMA', 'GEO','OPSD','WRI'])

#        raise NotImplemented
        # matched_df = ...
        # matched_df.to_csv(outfn)
        return matched
    else:
        return pd.read_csv(outfn, index_col='id')
        
def MATCHED_dataset(artificials=True, subsume_uncommon_fueltypes=False):
    """
    This returns the actual match between the Carma-data, GEO-data, WRI-data, 
    FIAS-data and the ESE-data with an additional manipulation on the hydro powerplants. 
    The latter were adapted in terms of the power plant classification (Run-of-river, 
    Reservoir, Pumped-Storage) and were quantitatively  adjusted to the ENTSOE-
    statistics. For more information about the classification and adjustment, 
    see the hydro-aggreation.py file.
    
    Parameters
    ----------
    artificials : Boolean, defaut True
            Wether to include 210 artificial hydro powerplants in order to fulfill the 
            statistics of the ENTSOE-data and receive better covering of the country 
            totals
    subsume_uncommon_fueltypes : Boolean, default False
            Wether to reduce the fueltype specification such that "Geothermal", "Waste"
            and "Mixed Fueltypes" are declared as "Other"
            
    """
    
    hydro = Aggregated_hydro()
    matched = extend_by_non_matched(Carma_FIAS_GEO_OPSD_WRI_matched(),
                                    GEO_data(), 'GEO')    
    matched = matched[matched.Fueltype != 'Hydro']
    if not artificials:
        hydro = hydro[hydro.Fias!="Artificial Powerplant"]
    if subsume_uncommon_fueltypes:
        matched = set_uncommon_fueltypes_to_other(matched)
    return pd.concat([matched, hydro])

def Aggregated_hydro(update=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data', 
                         'hydro_aggregation.csv')
    if update or not os.path.exists(outfn):
        # Generate the matched database
        raise NotImplemented()
        # matched_df = ...

        # matched_df.to_csv(outfn)
        # return matched_df
    else:
        return pd.read_csv(outfn, index_col='id')

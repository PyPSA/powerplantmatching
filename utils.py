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
Utility functions for checking data completness and supporting other functions 
"""

from __future__ import print_function, absolute_import

import pandas as pd
import six
from countrycode import countrycode
    

def lookup(df, keys=None, by='Country, Fueltype', exclude=None):
    """
    Returns a lookup table of the dataframe df with rounded numbers. Use different lookups
    as "Country", "Fueltype" for the different lookups.

    Parameters
    ----------
    df : pandas.Dataframe or list of pandas.Dataframe's
        powerplant databases to be analysed. If multiple dataframes are passed
        the lookup table will dusplay them in a MulitIndex
    by : string out of 'Country, Fueltype', 'Country' or 'Fueltype'
        Define the type pf lookup table you want to obtain.
    keys : list of strings
        labels of the different datasets, only nescessary if multiple dataframes
        passed
    exclude: list
        list of fueltype to exclude from the analysis

    """

    def lookup_single(df, by=by, exclude=exclude):
        df = read_csv_if_string(df)
        if exclude is not None:
            df = df[~df.Fueltype.isin(exclude)]
        if by == 'Country, Fueltype':
            return df.groupby(['Country', 'Fueltype']).Capacity.sum()\
                    .unstack(0).fillna(0).astype(int)
        elif by == 'Country':
            return df.groupby(['Country']).Capacity.sum().astype(int)
        elif by == 'Fueltype':
            return df.groupby(['Fueltype']).Capacity.sum().astype(int)
        else:
            raise NameError("``by` must be one of 'Country, Fueltype' or 'Country' or 'Fueltype'")

    if isinstance(df, list):
        dfs = pd.concat([lookup_single(a) for a in df], axis=1, keys=keys)
        if by == 'Country, Fueltype':
            dfs = dfs.reorder_levels([1, 0], axis=1)
            dfs = dfs[dfs.columns.levels[0]]
        dfs = dfs.fillna(0)
        dfs.loc['Total'] = dfs.sum()
        return dfs
    else:
        return lookup_single(df)

def set_uncommon_fueltypes_to_other(df, fueltypes={'Geothermal', 'Mixed fuel types', 'Waste'}):
    df.loc[df.Fueltype.isin(fueltypes) , 'Fueltype'] = 'Other'
    return df

def read_csv_if_string(data):
    if isinstance(data, six.string_types):
        data = pd.read_csv(data, index_col='id')
    return data   
    
def parse_Geoposition(loc, country):
    """
    Nominatim request for the Geoposition of a specific location in a country.
    Returns a tuples with (lattitude, longitude) if the request was sucessful,
    returns None otherwise.


    Parameters
    ----------
    loc : string
        description of the location, can be city, area etc.
    country : string
        name of the country which will be used as a bounding area


    """
    from geopy.geocoders import Nominatim
    if loc is not None and loc != float:
        country = countrycode(codes=[country], origin='country_name', target='iso2c')[0]
        gdata = Nominatim(timeout=100, country_bias=country).geocode(loc)
        if gdata != None:
            lat = gdata.latitude
            lon = gdata.longitude
            return (lat, lon)

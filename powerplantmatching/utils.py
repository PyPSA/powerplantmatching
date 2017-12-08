# -*- coding: utf-8 -*-
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
from os.path import dirname
import os
import pandas as pd
import six
import pycountry
import matplotlib.pyplot as plt
import logging


def _data(fn):
    return os.path.join(dirname(dirname(__file__)), 'data', fn)

def _data_in(fn):
    return os.path.join(dirname(dirname(__file__)), 'data', 'in', fn)

def _data_out(fn):
    return os.path.join(dirname(dirname(__file__)), 'data', 'out', fn)

# Logging: General Settings
#import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# Logging: File
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
fileHandler = logging.FileHandler(_data_out('PPMT.log'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
# Logging: Console
consoleHandler = logging.StreamHandler()
logger.addHandler(consoleHandler)


def lookup(df, keys=None, by='Country, Fueltype', exclude=None, show_totals=False):
    """
    Returns a lookup table of the dataframe df with rounded numbers.
    Use different lookups as "Country", "Fueltype" for the different lookups.

    Parameters
    ----------
    df : pandas.Dataframe or list of pandas.Dataframe's
        powerplant databases to be analysed. If multiple dataframes are passed
        the lookup table will display them in a MulitIndex
    by : string out of 'Country, Fueltype', 'Country' or 'Fueltype'
        Define the type of lookup table you want to obtain.
    keys : list of strings
        labels of the different datasets, only necessary if multiple dataframes
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
                    .unstack(0).fillna(0.)
        elif by == 'Country':
            return df.groupby(['Country']).Capacity.sum()
        elif by == 'Fueltype':
            return df.groupby(['Fueltype']).Capacity.sum()
        else:
            raise NameError(
            "``by` must be one of 'Country, Fueltype' or 'Country' or 'Fueltype'")

    if isinstance(df, list):
        dfs = pd.concat([lookup_single(a) for a in df], axis=1, keys=keys)
        if by == 'Country, Fueltype':
            dfs = dfs.reorder_levels([1, 0], axis=1)
            dfs = dfs[dfs.columns.levels[0]]
        dfs = dfs.fillna(0.)
        if show_totals:
            dfs.loc['Total'] = dfs.sum()
            return dfs.round(0).astype(int)
        else:
            return dfs.round(0).astype(int)
    else:
        if show_totals:
            dfs = lookup_single(df).fillna(0.)
            dfs.loc['Total'] = dfs.sum()
            return dfs.round(0).astype(int)
        else:
            return lookup_single(df).fillna(0.).round(0).astype(int)


def plot_fueltype_stats(df):
    stats = lookup(df, by='Fueltype')
    plt.pie(stats, colors=stats.index.to_series().map(tech_colors).tolist(),
           labels=stats.index, autopct='%1.1f%%')


def set_uncommon_fueltypes_to_other(df, fueltypes={'Bioenergy','Geothermal','Mixed fuel types','Waste'}):
    df.loc[df.Fueltype.isin(fueltypes), 'Fueltype'] = 'Other'
    return df


def read_csv_if_string(data):
    if isinstance(data, six.string_types):
        data = pd.read_csv(data, index_col='id')
    return data


def parse_Geoposition(loc, zipcode='', country=None, return_Country=False):
    """
    Nominatim request for the Geoposition of a specific location in a country.
    Returns a tuples with (latitude, longitude) if the request was sucessful,
    returns None otherwise.

    ToDo:   There exist further online sources for lat/long data which could be
            used, if this one fails, e.g.
        - Google Geocoding API
        - Yahoo! Placefinder
        - https://askgeo.com (??)

    Parameters
    ----------
    loc : string
        description of the location, can be city, area etc.
    country : string
        name of the country which will be used as a bounding area

    """
    from geopy.geocoders import GoogleV3 #ArcGIS  Yandex Nominatim
    import geopy.exc
    if loc is not None and loc != float:
        try:
            country = pycountry.countries.get(name=country).alpha2
        except KeyError:
            country = ''
        if zipcode is None: zipcode = ''
        #gdata = Nominatim(timeout=10, country_bias=country).geocode(loc)
        #gdata = Yandex(timeout=10, lang='en_US').geocode(loc)
        #gdata = ArcGIS(timeout=10).geocode(loc, exactly_one=True)
        gdata = None
        try:
            gdata = GoogleV3(api_key='AIzaSyCmQqxUg-0ccPbIBzsKyh_gNKBD8yVHOPc',
                             timeout=10).geocode(query=loc, components={'country':str(country),
                                       'postal_code':str(zipcode)}, exactly_one=True)
        except geopy.exc.GeocoderQueryError as e:
            logger.warn(e)

        if gdata != None:
            if return_Country:
                return gdata.address.split(', ')[-1]
            else:
                return (gdata.latitude, gdata.longitude)


tech_colors = pd.Series({"Wind" : "b",
               'windoff' : "c",
               "Hydro" : "g",
               "ror" : "g",
               "Run-Of-River" : "g",
               'Solar' : "yellow",
               "Bioenergy" : "g",
               "Natural Gas" : "brown",
               "Gas" : "brown",
               "lines" : "k",
               "H2" : "m",
               "battery" : "slategray",
               "Nuclear" : "y",
               "Nuclear marginal" : "r",
               "Hard Coal" : "k",
               "Coal" : "k",
               "Waste" : "grey",
               "Lignite" : "saddlebrown",
               "Geothermal" : "orange",
               "CCGT marginal" : "orange",
               "heat pumps" : "r",
               "water tanks" : "w",
               "PHS" : "g",
               "Ambient" : "k",
               "Electric load" : "b",
               "Oil" : "r",
               "Transport load" : "grey",
               "heat" : "r",
               "Li ion" : "grey",
               "curtailment": "r",
               "load": "k",
               "total": "k",
               "Other":"grey",
               "Total":"gold"
               })

tech_colors2 = pd.Series(data=
          {'OCGT':'brown',
          'Hydro':'darkblue',
          'Lignite':'chocolate',
          'Nuclear': 'yellow',
          'solar':'gold',
          'Solar':'gold',
          'windoff':'cornflowerblue',
          'windon':'steelblue',
          'Wind': 'steelblue',
          "Bioenergy" : "g",
          "Natural Gas" : "firebrick",
          'CCGT':'firebrick',
          'Coal':'k',
          'Hard Coal':'k',
          "Oil" : "darkgreen",
          "Other":"dimgrey",
          "Waste" : "grey",
          "Geothermal" : "orange"})

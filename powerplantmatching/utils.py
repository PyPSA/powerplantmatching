# -*- coding: utf-8 -*-
# Copyright 2015-2016 Fabian Hofmann (FIAS), Jonas Hoersch (FIAS)

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
Utility functions for checking data completness and supporting other functions
"""

from __future__ import print_function, absolute_import
from os.path import dirname
import os
import pandas as pd
import six
import pycountry as pyc
import logging
import numpy as np
import sys


def _data(fn):
    return os.path.join(dirname(__file__), '..', 'data', fn)


def _data_in(fn):
    return os.path.join(dirname(__file__), '..', 'data', 'in', fn)


def _data_out(fn):
    return os.path.join(dirname(__file__), '..', 'data', 'out', fn)


# Logging: General Settings
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# Logging: File
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
fileHandler = logging.FileHandler(_data_out('PPM.log'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
# Logging: Console
consoleHandler = logging.StreamHandler()
logger.addHandler(consoleHandler)
text = str if sys.version_info >= (3,0) else unicode


def lookup(df, keys=None, by='Country, Fueltype', exclude=None, unit='MW'):
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

    if unit == 'GW':
        scaling = 1000.
    elif unit == 'MW':
        scaling = 1.
    else:
        raise(ValueError("unit has to be MW or GW"))

    def lookup_single(df, by=by, exclude=exclude):
        df = read_csv_if_string(df)
        if isinstance(by, str):
            by = by.replace(' ', '').split(',')
        if exclude is not None:
            df = df[~df.Fueltype.isin(exclude)]
        return df.groupby(by).Capacity.sum()

    if isinstance(df, list):
        dfs = pd.concat([lookup_single(a) for a in df], axis=1, keys=keys)
        dfs = dfs.fillna(0.)
        return (dfs/scaling).round(3)
    else:
        return (lookup_single(df)/scaling).fillna(0.).round(3)


def set_uncommon_fueltypes_to_other(df, fillna_other=True, **kwargs):
    default = ['Bioenergy', 'Geothermal', 'Waste', 'Mixed fuel types',
               'Electro-mechanical', 'Hydrogen Storage']
    fueltypes = kwargs.get('fueltypes', default)
    df.loc[df.Fueltype.isin(fueltypes), 'Fueltype'] = 'Other'
    if fillna_other:
        df = df.fillna({'Fueltype': 'Other'})
    return df


def read_csv_if_string(data):
    from .data import data_config
    if isinstance(data, six.string_types):
        data = data_config[data]['read_function']()
    return data


def map_projectID(df, dataset_name, ID):
    if isinstance(df.projectID.iloc[0], text):
        return df['projectID'] == ID
    else:
        return df['projectID'].apply(lambda x: ID in x[dataset_name]
                                     if dataset_name in x.keys() else False)


def country_alpha_2(country):
    try:
        return pyc.countries.get(name=country).alpha_2
    except KeyError:
        return ''


def parse_Geoposition(loc, zipcode='', country=None, return_Country=False,
                      use_saved_position=False):
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

    from geopy.geocoders import GoogleV3  # ArcGIS  Yandex Nominatim
    import geopy.exc

    if loc is not None and loc != float:

        country = country_alpha_2(country)

        if zipcode is None:
            zipcode = ''

        if use_saved_position is not None:
            saved = pd.read_csv(_data('parsed_locations.csv'),
                                index_col=[0,1], encoding='utf-8')
            if saved.index.contains((loc, country)):
                return saved.loc[(loc, country)].values

        try:
            gdata = (GoogleV3(
                    api_key='AIzaSyCmQqxUg-0ccPbIBzsKyh_gNKBD8yVHOPc',
                    timeout=10)
                     .geocode(query=loc,
                              components={'country': str(country),
                                          'postal_code': str(zipcode)},
                              exactly_one=True))
        except geopy.exc.GeocoderQueryError as e:
            logger.warn(e)

        if gdata is not None:
            if use_saved_position:
                with open(_data('parsed_locations.csv'), 'a') as f:
                    (pd.DataFrame({'country': country,
                                   'lat': gdata.latitude,
                                   'lon': gdata.longitude},
                     index=[loc])
                     .to_csv(f, header=False, encoding='utf-8'))
            if return_Country:
                return gdata.address.split(', ')[-1]
            else:
                return (gdata.latitude, gdata.longitude)


def fill_geoposition(df, parse=False,
                     group_columns=['Country', 'Fueltype', 'Technology'],
                     only_saved_locs=False):
    """
    Fill missing power plant coordinates (lat/lon). Use the distribution of
    existing power plant coordinates which coincide in Country, Fueltype and
    Technology and propagate it to the missing entries.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame of power plants
    parse : Boolean (default False)
        whether to parse the geoposition for every power plant using the
        funtion parse_Geoposition. Otherwise use existing distribution
    group_columns : list
        list of column names which are used to determine existing coordinate
        distribution. For each power plant with missing coordinate a random
        coordinate is picked from the subset of power plants which have
        the same values in group_columns

    """

    if parse:

        coords = df[['lat', 'lon']].copy()

        saved = (pd.read_csv(_data('parsed_locations.csv'),
                             index_col=0, encoding='utf-8')
                 .rename(index=lambda df: df.replace(' Power Plant', '')))
        saved.index += ' ' + saved.country
        df = df.assign(Keys=df['Name'] + ' ' +
                       df['Country'].apply(country_alpha_2))

        coords['lat'] = coords['lat'].where(coords.lat.notnull(),
                                            df.Keys.map(saved.lat))
        coords['lon'] = coords['lon'].where(coords.lon.notnull(),
                                            df.Keys.map(saved.lon))

        df.drop(columns=['Keys'], inplace=True)

        if not only_saved_locs:
            missing_b = (coords.lat.isnull() | coords.lon.isnull())
            search_strings = pd.DataFrame(df[missing_b].Name + ' Power Plant')
            search_strings = (search_strings
                              .assign(Country=df[missing_b].Country))
            latlon = (search_strings
                      .apply(lambda ds:
                             parse_Geoposition(ds['Name'],
                                               country=ds['Country'],
                                               use_saved_position=True),
                             axis=1)
                      .rename(columns={'Name': 'lat', 'Country': 'lon'}))

            coords.loc[missing_b, 'lat'] = latlon.str[0]
            coords.loc[missing_b, 'lon'] = latlon.str[1]

        return df.assign(**{"lat": coords.lat, 'lon': coords.lon})

    else:
        def fillna(df):
            return (df.fillna(method='ffill').fillna(method='bfill'))

        coords = (df.loc[np.random.permutation(df.index)]
                    .groupby(group_columns)[['lat', 'lon']].transform(fillna)
                    .reindex(df.index))
        return df.assign(**{"lat": coords.lat, 'lon': coords.lon})


tech_colors = pd.Series({"Wind": "b",
                         'windoff': "c",
                         "Hydro": "g",
                         "ror": "g",
                         "Run-Of-River": "g",
                         'Solar': "yellow",
                         "Bioenergy": "g",
                         "Natural Gas": "brown",
                         "Gas": "brown",
                         "lines": "k",
                         "H2": "m",
                         "battery": "slategray",
                         "Nuclear": "y",
                         "Nuclear marginal": "r",
                         "Hard Coal": "k",
                         "Coal": "k",
                         "Waste": "grey",
                         "Lignite": "saddlebrown",
                         "Geothermal": "orange",
                         "CCGT marginal": "orange",
                         "heat pumps": "r",
                         "water tanks": "w",
                         "PHS": "g",
                         "Ambient": "k",
                         "Electric load": "b",
                         "Oil": "r",
                         "Transport load": "grey",
                         "heat": "r",
                         "Li ion": "grey",
                         "curtailment": "r",
                         "load": "k",
                         "total": "k",
                         "Other": "grey",
                         "Total": "gold"
                         })

# -*- coding: utf-8 -*-
# Copyright 2016-2018 Fabian Hofmann (FIAS), Jonas Hoersch (KIT, IAI) and
# Fabian Gotzens (FZJ, IEK-STE)

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

from .config import get_config
from os.path import dirname
import os
import time
import pandas as pd
import six
import pycountry as pyc
import logging
import numpy as np
import sys
import multiprocessing
from ast import literal_eval as liteval


def _data(fn):
    return os.path.join(dirname(__file__), '..', 'data', fn)


def _data_in(fn):
    return os.path.join(dirname(__file__), '..', 'data', 'in', fn)


def _data_out(fn, config=None):
    if config is None:
        return os.path.join(dirname(__file__), '..', 'data', 'out',
                            'default', fn)
    else:
        return os.path.join(dirname(__file__), '..', 'data', 'out',
                            config['hash'], fn)


# Logging: General Settings
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# Logging: File
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] "
                                 "[%(levelname)-5.5s]  %(message)s")
fileHandler = logging.FileHandler(_data_out('../PPM.log'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
# Logging: Console
# consoleHandler = logging.StreamHandler()
# logger.addHandler(consoleHandler)
text = str if sys.version_info >= (3, 0) else unicode


class Accessor(object):
    """
    Helper class to define super class of PowerPlantAccessor
    """
    pass


def get_obj_if_Acc(obj):
    if isinstance(obj, Accessor):
        return obj._obj
    else:
        return obj


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

    df = get_obj_if_Acc(df)
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
        if keys is None:
            keys = [get_name(d) for d in df]
        dfs = pd.concat([lookup_single(a) for a in df], axis=1, keys=keys)
        dfs = dfs.fillna(0.)
        return (dfs/scaling).round(3)
    else:
        return (lookup_single(df)/scaling).fillna(0.).round(3)


def config_filter(df, name=None, config=None):
    """
    Convenience function to filter data source according to the config.yaml
    file. Individual query filters are applied if argument 'name' is given.

    Parameters
    ----------
    df : pd.DataFrame
        Data to be filtered
    name : str, default None
        Name of the data source to identify query in the config.yaml file
    config : dict, default None
        Configuration overrides varying from the config.yaml file
    """
    df = get_obj_if_Acc(df)

    if config is None:
        config = get_config()
    # individual filter from config.yaml
    if name is not None:
        queries = {k: v for source in config['matching_sources']
                   for k, v in to_dict_if_string(source).items()}
        if name in queries and queries[name] is not None:
            df = df.query(queries[name])
    return (df[lambda df: df.Country.isin(config['target_countries']) &
               df.Fueltype.isin(config['target_fueltypes'])]
            .reindex(columns=config['target_columns'])
            .reset_index(drop=True))


def correct_manually(df, name, config=None):
    """
    Update powerplant data based on stored corrections in
    powerplantmatching/data/in/manual_corrections.csv. Specify the name
    of the data by the second argument.

    Parameters
    ----------
    df : pandas.DataFrame
        Powerplant data
    name : str
        Name of the data source, should be in columns of manual_corrections.csv
    """
    from .data import data_config
    if config is None:
        config = get_config()

    corrections = (pd.read_csv(_data('manual_corrections.csv'),
                               encoding='utf-8',
                               parse_dates=['last_update'])
                   [lambda df: df[name].notnull()]
                   .set_index(name))
    if len(corrections) == 0:
        return df.reindex(columns=config['target_columns'])
    source_file = data_config[name]['source_file']
    # assume OPSD files are updated on the same time
    if isinstance(source_file, list):
        source_file = source_file[0]
    outdated = (pd.Timestamp(time.ctime(os.path.getmtime(source_file)))
                > corrections.last_update).any()
    if outdated:
        logger.warning('Manual corrections in {0} for file {1} older than last'
                       ' update of the source file, please update your manual '
                       'corrections.'.format(os.path.abspath(
                               _data_in('manual_corrections.csv')), name))
    df = df.set_index('projectID').copy()
    df.update(corrections)
    return df.reset_index().reindex(columns=config['target_columns'])


def set_uncommon_fueltypes_to_other(df, fillna_other=True, **kwargs):
    """
    Replace uncommon fueltype specifications as by 'Other'. This helps to
    compare datasources with Capacity statistics given by
    powerplantmatching.data.Capacity_stats().

    Parameters
    ----------

    df : pd.DataFrame
        DataFrame to replace 'Fueltype' argument
    fillna_other : Boolean, default True
        Whether to replace NaN values in 'Fueltype' with 'Other'
    fueltypes : list
        list of replaced fueltypes, defaults to
        ['Bioenergy', 'Geothermal', 'Mixed fuel types', 'Electro-mechanical',
        'Hydrogen Storage']
    """
    df = get_obj_if_Acc(df)

    default = ['Bioenergy', 'Geothermal', 'Mixed fuel types',
               'Electro-mechanical', 'Hydrogen Storage']
    fueltypes = kwargs.get('fueltypes', default)
    df.loc[df.Fueltype.isin(fueltypes), 'Fueltype'] = 'Other'
    if fillna_other:
        df = df.fillna({'Fueltype': 'Other'})
    return df


def read_csv_if_string(data):
    """
    Convenience function to import powerplant data source if a string is given.
    """
    from .data import data_config
    if isinstance(data, six.string_types):
        data = data_config[data]['read_function']()
    return data


def to_categorical_columns(df):
    """
    Helper function to set datatype of columns 'Fueltype', 'Country', 'Set',
    'File', 'Technology' to categorical.
    """
    cols = ['Fueltype', 'Country', 'Set', 'File']
    cats = {'Fueltype': get_config()['target_fueltypes'],
            'Country': get_config()['target_countries'],
            'Set': get_config()['target_sets']}
    return df.assign(**{c: df[c].astype('category') for c in cols})\
             .assign(**{c: lambda df: df[c].cat.set_categories(v)
                     for c,v in cats.items()})


def set_column_name(df, name):
    """
    Helper function to associate dataframe with a name. This is done with the
    columns-axis name, as pd.DataFrame do not have a name attribute.
    """
    df.columns.name = name
    return df


def get_name(df):
    """
    Helper function to associate dataframe with a name. This is done with the
    columns-axis name, as pd.DataFrame do not have a name attribute.
    """
    if df.columns.name is None:
        return 'unnamed data'
    else:
        return df.columns.name


def to_list_if_other(obj):
    """
    Convenience function to ensure list-like output
    """
    if not isinstance(obj, list):
        return [obj]
    else:
        return obj


def to_dict_if_string(s):
    """
    Convenience function to ensure dict-like output
    """
    if isinstance(s, str):
        return {s: None}
    else:
        return s


def projectID_to_dict(df):
    """
    Convenience function to convert string of dict to dict type
    """
    if df.columns.nlevels > 1:
        return df.assign(projectID=(df.projectID.stack().dropna().apply(
                lambda df: liteval(df)).unstack()))
    else:
        return df.assign(projectID=df.projectID.apply(lambda df: liteval(df)))


def select_by_projectID(df, projectID, dataset_name=None):
    """
    Convenience function to select data by its projectID
    """
    df = get_obj_if_Acc(df)

    if isinstance(df.projectID.iloc[0], text):
        return df.query("projectID == @projectID")
    else:
        return df[df['projectID'].apply(lambda x:
                  projectID in sum(x.values(), []))]


def update_saved_matches_for_(name):
    """
    Update your saved matched for a single source. This is very helpful if you
    modified/updated a data source and do not want to run the whole matching
    again.

    Example
    -------

    Assume data source 'ESE' changed a little:

    >>> pm.utils.update_saved_matches_for_('ESE')
    ... <Wait for the update> ...
    >>> pm.collection.matched_data(update=True)

    Now the matched_data is updated with the modified version of ESE.
    """
    from .collection import collect
    from .matching import compare_two_datasets
    df = collect(name, use_saved_aggregation=False)
    dfs = [ds for ds in get_config()['matching_sources'] if ds != name]
    for to_match in dfs:
        compare_two_datasets([collect(to_match), df], [to_match, name])


def fun(f, q_in, q_out):
    """
    Helper function for multiprocessing in classes/functions
    """
    while True:
        i, x = q_in.get()
        if i is None:
            break
        q_out.put((i, f(x)))


def parmap(f, arg_list, config=None):
    """
    Parallel mapping function. Use this function to parallely map function
    f onto arguments in arg_list. The maximum number of parallel threads is
    taken from config.yaml:parallel_duke_processes.

    Paramters
    ---------

    f : function
        python funtion with one argument
    arg_list : list
        list of arguments mapped to f
    """
    if config is None:
        config = get_config()
    if config['parallel_duke_processes']:
        nprocs = min(multiprocessing.cpu_count(), config['process_limit'])
        logger.info('Run process with {} parallel threads.'.format(nprocs))
        q_in = multiprocessing.Queue(1)
        q_out = multiprocessing.Queue()

        proc = [multiprocessing.Process(target=fun, args=(f, q_in, q_out))
                for _ in range(nprocs)]
        for p in proc:
            p.daemon = True
            p.start()

        sent = [q_in.put((i, x)) for i, x in enumerate(arg_list)]
        [q_in.put((None, None)) for _ in range(nprocs)]
        res = [q_out.get() for _ in range(len(sent))]

        [p.join() for p in proc]

        return [x for i, x in sorted(res)]
    else:
        return list(map(f, arg_list))


def country_alpha_2(country):
    """
    Convenience function for converting country name into alpha 2 codes
    """
    try:
        return pyc.countries.get(name=country).alpha_2
    except KeyError:
        return ''


def convert_country_to_alpha2(df):
    df = get_obj_if_Acc(df)
    return df.assign(Country = df.Country.apply(country_alpha_2))


def breakdown_matches(df):
    """
    Function to inspect grouped and matched entries of a matched
    dataframe. Breaks down to all ingoing data on detailed level.

    Parameters
    ----------
    df : pd.DataFrame
        Matched data with not empty projectID-column. Keys of projectID must
        be specified in powerplantmatching.data.data_config
    """
    df = get_obj_if_Acc(df)

    from .data import data_config
    assert('projectID' in df)
    sources = set(df.projectID.apply(dict.keys).apply(list).sum())
    sources = pd.concat(
            [data_config[s]['read_function']().set_index('projectID')
             for s in sources])
    if df.index.nlevels > 1:
        stackedIDs = (df['projectID'].stack()
                      .apply(pd.Series).stack()
                      .dropna())
    else:
        stackedIDs = (df['projectID']
                      .apply(pd.Series).stack()
                      .apply(pd.Series).stack()
                      .dropna())
    return (sources
            .reindex(stackedIDs)
            .set_axis(stackedIDs.to_frame('projectID')
                      .set_index('projectID', append=True).index,
                      inplace=False))


def parse_Geoposition(location, zipcode='', country='',
                      use_saved_locations=False):
    """
    Nominatim request for the Geoposition of a specific location in a country.
    Returns a tuples with (latitude, longitude, country) if the request was
    sucessful, returns np.nan otherwise.

    ToDo:   There exist further online sources for lat/long data which could be
            used, if this one fails, e.g.
        - Google Geocoding API
        - Yahoo! Placefinder
        - https://askgeo.com (??)

    Parameters
    ----------
    location : string
        description of the location, can be city, area etc.
    country : string
        name of the country which will be used as a bounding area
    use_saved_postion : Boolean, default False
        Whether to firstly compare with cached results in
        powerplantmatching/data/parsed_locations.csv
    """

    from geopy.geocoders import GoogleV3  # ArcGIS  Yandex Nominatim
    import geopy.exc

    if location is None or location == float:
        return np.nan

    countries = [(c, country_alpha_2(c)) for c in to_list_if_other(country)]

    for country, country_abbr in countries:
        if use_saved_locations:
            saved = pd.read_csv(_data('parsed_locations.csv'),
                                index_col=[0, 1], encoding='utf-8')
            if saved.index.contains((location, country)):
                return [saved.at[(location, country), 'lat'],
                        saved.at[(location, country), 'lon'],
                        country]
        try:
            gdata = (
                    GoogleV3(api_key=get_config()['google_api_key'],
                             timeout=10)
                    .geocode(query=location,
                             components={'country': country_abbr,
                                         'postal_code': str(zipcode)},
                             exactly_one=True))
        except geopy.exc.GeocoderQueryError as e:
            logger.warn(e)
            return np.nan

        if gdata is None:
            continue
        if ',' not in gdata.address:
            continue
        values = [gdata.latitude, gdata.longitude,
                  gdata.address.split(', ')[-1]]
        if use_saved_locations:
            (pd.DataFrame(dict(zip(['lat', 'lon', 'Country'], values)),
                          index=[location])
             .to_csv(_data('parsed_locations.csv'), header=False,
                     mode='a', encoding='utf-8'))
        return values
    return np.nan


def fill_geoposition(df, use_saved_locations=False):
    """
    Fill missing 'lat' and 'lon' values. Uses geoparsing with the value given
    in 'Name', limits the search through value in 'Country'.
    df must contain 'Name', 'lat', 'lon' and 'Country' as columns.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame of power plants
    use_saved_postion : Boolean, default False
        Whether to firstly compare with cached results in
        powerplantmatching/data/parsed_locations.csv
    """
    df = get_obj_if_Acc(df)

    logger.info("Parse geopositions for missing lat/lon values")

    if use_saved_locations and get_config()['google_api_key'] is None:
        logger.warning('Geoparsing not possible as no google api key was '
                       'found, please add the key to your config.yaml if you '
                       'want to enable it.')

    geodata = (df
               .apply(lambda ds:
                      pd.Series(parse_Geoposition(
                               ds['Name'], country=ds['Country'].split(', '),
                               use_saved_locations=use_saved_locations),
                               index=['lat', 'lon', 'Country'])
                      if ds[['lat']].isnull()[0]
                      else ds[['lat', 'lon', 'Country']],
                      axis=1)
               .assign(Country=lambda gd: gd.Country.fillna(df.Country)))

    return (df
            .assign(lat=geodata.lat, lon=geodata.lon, Country=geodata.Country)
            .reindex(columns=df.columns))

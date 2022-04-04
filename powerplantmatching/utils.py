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
Utility functions for checking data completeness and supporting other functions
"""

from __future__ import absolute_import, print_function

import multiprocessing
import os
from ast import literal_eval as liteval

import numpy as np
import pandas as pd
import pycountry as pyc
import requests
import six
from deprecation import deprecated

from .core import _data_in, _package_data, get_config, get_obj_if_Acc, logger


def lookup(df, keys=None, by="Country, Fueltype", exclude=None, unit="MW"):
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
    if unit == "GW":
        scaling = 1000.0
    elif unit == "MW":
        scaling = 1.0
    else:
        raise (ValueError("unit has to be MW or GW"))

    def lookup_single(df, by=by, exclude=exclude):
        df = read_csv_if_string(df)
        if isinstance(by, str):
            by = by.replace(" ", "").split(",")
        if exclude is not None:
            df = df[~df.Fueltype.isin(exclude)]
        return df.groupby(by).Capacity.sum()

    if isinstance(df, list):
        if keys is None:
            keys = [get_name(d) for d in df]
        dfs = pd.concat([lookup_single(a) for a in df], axis=1, keys=keys, sort=False)
        dfs = dfs.fillna(0.0)
        return (dfs / scaling).round(3)
    else:
        return (lookup_single(df) / scaling).fillna(0.0).round(3)


def get_raw_file(name, update=False, config=None, skip_retrieve=False):
    if config is None:
        config = get_config()
    df_config = config[name]
    path = _data_in(df_config["fn"])

    if (not os.path.exists(path) or update) and not skip_retrieve:
        url = df_config["url"]
        logger.info(f"Retrieving data from {url}")
        r = requests.get(url)
        with open(path, "wb") as outfile:
            outfile.write(r.content)

    return path


def config_filter(df, config):
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

    name = df.powerplant.get_name()
    assert name is not None, "No name given for data source"

    countries = config["target_countries"]
    fueltypes = config["target_fueltypes"]
    cols = config["target_columns"]

    target_query = "Country in @countries and Fueltype in @fueltypes"

    main_query = config.get("main_query", "")

    # individual filter from config.yaml
    queries = {}
    for source in config["matching_sources"]:
        if isinstance(source, dict):
            queries.update(source)
        else:
            queries[source] = ""
    ds_query = queries.get(name, "")

    query = " and ".join([q for q in [target_query, main_query, ds_query] if q])

    return df.reindex(columns=cols).query(query).reset_index(drop=True)


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
    if config is None:
        config = get_config()

    corrections_fn = _package_data("manual_corrections.csv")
    corrections = pd.read_csv(corrections_fn)

    corrections = (
        corrections.query("Source == @name")
        .drop(columns="Source")
        .set_index("projectID")
    )
    if corrections.empty:
        return df

    df = df.set_index("projectID").copy()
    df.update(corrections)
    return df.reset_index().reindex(columns=config["target_columns"])


def set_uncommon_fueltypes_to_other(df, fillna_other=True, config=None, **kwargs):
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
    config = get_config() if config is None else config
    df = get_obj_if_Acc(df)

    default = [
        "Bioenergy",
        "Geothermal",
        "Mixed fuel types",
        "Electro-mechanical",
        "Hydrogen Storage",
    ]
    fueltypes = kwargs.get("fueltypes", default)
    df.loc[df.Fueltype.isin(fueltypes), "Fueltype"] = "Other"
    if fillna_other:
        df = df.fillna({"Fueltype": "Other"})
    return df


def read_csv_if_string(df):
    """
    Convenience function to import powerplant data source if a string is given.
    """
    from . import data

    if isinstance(data, six.string_types):
        df = getattr(data, df)()
    return df


def to_categorical_columns(df):
    """
    Helper function to set datatype of columns 'Fueltype', 'Country', 'Set',
    'File', 'Technology' to categorical.
    """
    cols = ["Fueltype", "Country", "Set", "File"]
    cats = {
        "Fueltype": get_config()["target_fueltypes"],
        "Country": get_config()["target_countries"],
        "Set": get_config()["target_sets"],
    }
    return df.assign(**{c: df[c].astype("category") for c in cols}).assign(
        **{c: lambda df: df[c].cat.set_categories(v) for c, v in cats.items()}
    )


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
        return df.assign(
            projectID=(
                df.projectID.stack().dropna().apply(lambda ds: liteval(ds)).unstack()
            )
        )
    else:
        return df.assign(projectID=df.projectID.apply(lambda x: liteval(x)))


def select_by_projectID(df, projectID, dataset_name=None):
    """
    Convenience function to select data by its projectID
    """
    df = get_obj_if_Acc(df)

    if isinstance(df.projectID.iloc[0], str):
        return df.query("projectID == @projectID")
    else:
        return df[df["projectID"].apply(lambda x: projectID in sum(x.values(), []))]


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
    dfs = [ds for ds in get_config()["matching_sources"] if ds != name]
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
    Parallel mapping function. Use this function to parallelly map function
    f onto arguments in arg_list. The maximum number of parallel threads is
    taken from config.yaml:parallel_duke_processes.

    Parameters
    ---------

    f : function
        python function with one argument
    arg_list : list
        list of arguments mapped to f
    """
    if config is None:
        config = get_config()
    if config["parallel_duke_processes"]:
        nprocs = min(multiprocessing.cpu_count(), config["process_limit"])
        logger.info("Run process with {} parallel threads.".format(nprocs))
        q_in = multiprocessing.Queue(1)
        q_out = multiprocessing.Queue()

        proc = [
            multiprocessing.Process(target=fun, args=(f, q_in, q_out))
            for _ in range(nprocs)
        ]
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


country_map = pd.read_csv(_package_data("country_codes.csv")).replace(
    {"name": {"Czechia": "Czech Republic"}}
)


def country_alpha2(country):
    """
    Convenience function for converting country name into alpha 2 codes
    """
    if not isinstance(country, str):
        return ""
    try:
        return pyc.countries.get(name=country).alpha_2
    except KeyError:
        return ""


def convert_alpha2_to_country(df):
    df = get_obj_if_Acc(df)
    dic = {
        "EL": "GR",  # needed, as some datasets use for Greece and United K.
        "UK": "GB",
    }  # codes that are not conform to ISO 3166-1 alpha2.
    return df.assign(
        Country=df.Country.replace(dic).map(country_map.set_index("alpha_2")["name"])
    )


def convert_country_to_alpha2(df):
    df = get_obj_if_Acc(df)
    alpha2 = df.Country.map(country_map.set_index("name")["alpha_2"]).fillna(
        country_map.dropna(subset=["official_name"]).set_index("official_name")[
            "alpha_2"
        ]
    )
    return df.assign(Country=alpha2)


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

    from . import data

    assert "projectID" in df
    if isinstance(df.projectID.iloc[0], list):
        sources = [df.powerplant.get_name()]
        single_source_b = True
    else:
        sources = df.projectID.apply(list).explode().unique()
        single_source_b = False
    sources = pd.concat(
        [getattr(data, s)().set_index("projectID") for s in sources], sort=False
    )
    if df.index.nlevels > 1:
        stackedIDs = df["projectID"].stack().apply(pd.Series).stack().dropna()
    elif single_source_b:
        stackedIDs = df["projectID"].apply(pd.Series).stack()
    else:
        stackedIDs = (
            df["projectID"].apply(pd.Series).stack().apply(pd.Series).stack().dropna()
        )
    return (
        sources.reindex(stackedIDs)
        .set_axis(
            stackedIDs.to_frame("projectID")
            .set_index("projectID", append=True)
            .droplevel(-2)
            .index,
            inplace=False,
        )
        .rename_axis(index=["id", "source", "projectID"])
    )


def restore_blocks(df, mode=2, config=None):
    """
    Restore blocks of powerplants from a matched dataframe.

    This function breaks down all matches. For each match separately it selects
    blocks from only one input data source.
    For this selection the following modi are available:

        1. Select the source with most number of blocks in the match

        2. Select the source with the highest reliability score

    Parameters
    ----------
    df : pd.DataFrame
        Matched data with not empty projectID-column. Keys of projectID must
        be specified in powerplantmatching.data.data_config
    """
    from .data import OPSD

    df = get_obj_if_Acc(df)
    assert "projectID" in df

    config = get_config() if config is None else config

    bd = breakdown_matches(df)
    if mode == 1:
        block_map = (
            bd.reset_index(["source"])["source"]
            .groupby(level="id")
            .agg(lambda x: pd.Series(x).mode()[0])
        )
        blocks_i = pd.MultiIndex.from_frame(block_map.reset_index())
        res = (
            bd.reset_index("projectID")
            .loc[blocks_i]
            .set_index("projectID", append=True)
        )
    elif mode == 2:
        sources = df.projectID.apply(list).explode().unique()
        rel_scores = pd.Series(
            {s: config[s]["reliability_score"] for s in sources}
        ).sort_values(ascending=False)
        res = pd.DataFrame().rename_axis(index="id")
        for s in rel_scores.index:
            subset = bd.reindex(index=[s], level="source")
            subset_i = subset.index.unique("id").difference(res.index.unique("id"))
            res = pd.concat([res, subset.reindex(index=subset_i, level="id")])
    else:
        raise ValueError(f"Given `mode` must be either 1 or 2 but is: {mode}")

    res = res.sort_index(level="id").reset_index(level=[0, 1])

    # Now append Block information from OPSD German list:
    df_blocks = (OPSD(rawDE_withBlocks=True).rename(columns={"name_bnetza": "Name"}))[
        "Name"
    ]
    res.update(df_blocks)
    return res


def parse_Geoposition(
    location, zipcode="", country="", use_saved_locations=False, saved_only=False
):
    """
    Nominatim request for the Geoposition of a specific location in a country.
    Returns a tuples with (latitude, longitude, country) if the request was
    successful, returns np.nan otherwise.

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

    import geopy.exc
    from geopy.geocoders import GoogleV3  # ArcGIS  Yandex Nominatim

    if location is None or location == float:
        return np.nan

    alpha2 = country_alpha2(country)
    try:
        gdata = GoogleV3(api_key=get_config()["google_api_key"], timeout=10).geocode(
            query=location,
            components={"country": alpha2, "postal_code": str(zipcode)},
            exactly_one=True,
        )
    except geopy.exc.GeocoderQueryError as e:
        logger.warn(e)

    if gdata is not None:
        return pd.Series(
            {
                "Name": location,
                "Country": country,
                "lat": gdata.latitude,
                "lon": gdata.longitude,
            }
        )


def fill_geoposition(df, use_saved_locations=False, saved_only=False):
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

    if use_saved_locations and get_config()["google_api_key"] is None:
        logger.warning(
            "Geoparsing not possible as no google api key was "
            "found, please add the key to your config.yaml if you "
            "want to enable it."
        )

    if use_saved_locations:
        locs = pd.read_csv(_package_data("parsed_locations.csv"), index_col=[0, 1])
        df = df.where(
            df[["lat", "lon"]].notnull().all(1),
            df.drop(columns=["lat", "lon"]).join(locs, on=["Name", "Country"]),
        )
    if saved_only:
        return df

    logger.info("Parse geopositions for missing lat/lon values")
    missing = df.lat.isnull()
    geodata = df[missing].apply(
        lambda ds: parse_Geoposition(ds["Name"], country=ds["Country"]), axis=1
    )
    geodata.drop_duplicates(subset=["Name", "Country"]).set_index(
        ["Name", "Country"]
    ).to_csv(_package_data("parsed_locations.csv"), mode="a", header=False)

    df.loc[missing, ["lat", "lon"]] = geodata

    return df.reindex(columns=df.columns)

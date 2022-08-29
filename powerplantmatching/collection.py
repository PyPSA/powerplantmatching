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
Processed datasets of merged and/or adjusted data
"""
import logging
import os

import pandas as pd
from deprecation import deprecated

from .cleaning import aggregate_units
from .core import _data_out, get_config
from .heuristics import extend_by_non_matched, extend_by_VRE
from .matching import combine_multiple_datasets, reduce_matched_dataframe, link_multiple_datasets
from .data import EEA, ENTSOE_generation
from .utils import (
    parmap,
    projectID_to_dict,
    set_column_name,
    set_uncommon_fueltypes_to_other,
    to_dict_if_string,
)

logger = logging.getLogger(__name__)


def collect(
    datasets,
    update=False,
    reduced=True,
    config=None,
    **dukeargs,
):
    """
    Return the collection for a given list of datasets in matched or
    reduced form.

    Parameters
    ----------
    datasets : list or str
        list containing the dataset identifiers as str, or single str
    update : bool
        Do an horizontal update (True) or read from the cache file (False)
    reduced : bool
        Switch as to return the reduced (True) or matched (False) dataset.
    **dukeargs : keyword-args for duke
    """

    from . import data

    if config is None:
        config = get_config()

    def df_by_name(name):
        conf = config[name]
        get_df = getattr(data, name)
        df = get_df(config=config)

        if not conf.get("aggregated_units", False):
            return aggregate_units(df, dataset_name=name, config=config)
        else:
            return df.assign(projectID=df.projectID.map(lambda x: {x}))

    # Deal with the case that only one dataset is requested
    if isinstance(datasets, str):
        return df_by_name(datasets)

    datasets = sorted(datasets)
    logger.info("Create combined dataset for {}".format(", ".join(datasets)))

    fn = "_".join(map(str.upper, datasets))
    outfn_matched = _data_out(f"Matched_{fn}.csv", config)

    fn = "_".join(map(str.upper, datasets))
    outfn_reduced = _data_out(f"Matched_{fn}_reduced.csv", config)

    if not update and not os.path.exists(outfn_reduced if reduced else outfn_matched):
        logger.warning("Forcing update since the cache file is missing")
        update = True

    if update:
        dfs = parmap(df_by_name, datasets)
        matched = combine_multiple_datasets(dfs, datasets, config=config, **dukeargs)
        (
            matched.assign(projectID=lambda df: df.projectID.astype(str)).to_csv(
                outfn_matched, index_label="id"
            )
        )

        reduced_df = reduce_matched_dataframe(matched, config=config)
        reduced_df.to_csv(outfn_reduced, index_label="id")

        return reduced_df if reduced else matched
    else:
        if reduced:
            df = pd.read_csv(outfn_reduced, index_col=0)
        else:
            df = pd.read_csv(
                outfn_matched, index_col=0, header=[0, 1], low_memory=False
            )
        return df.pipe(projectID_to_dict)


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="Use the collect function instead",
)
def Collection(**kwargs):
    return collect(**kwargs)


def extrapolate_efficiencies(powerplants, config=None, update=False):
    from sklearn.linear_model import LinearRegression

    eea = EEA(update=update, config=config)
    entsoe = ENTSOE_generation(update=update, config=config)
    matched = link_multiple_datasets([eea, entsoe, powerplants], ['eea', 'entsoe_generation', 'powerplants'],
                                     config=config).dropna()

    eea.loc[matched['eea'], 'matched'] = matched.set_index('eea')['entsoe_generation']
    eea.loc[matched['eea'], 'matched'] = matched['eea'].tolist()
    entsoe.loc[matched['entsoe_generation'], 'matched'] = matched.set_index('entsoe_generation')['eea']

    merged = pd.merge(eea.loc[matched['eea']], entsoe.loc[matched['entsoe_generation']], on='matched')
    merged['efficiency'] = merged['value_y'].multiply(0.0036).divide(merged['value_x'])
    efficiencies_eea = merged[(merged.efficiency > 0.3) & (merged.efficiency < 0.65)].set_index('matched')['efficiency']
    efficiencies_pm = matched.set_index('Matched Data')['eea'].map(efficiencies_eea).dropna()

    df = powerplants.copy()
    df.loc[efficiencies_pm.index, 'efficiency'] = efficiencies_pm

    # filling missing commissioning years by fuel average
    for fuel in ['Natural Gas', 'Hard Coal', 'Lignite', 'Oil']:
        df.loc[df.Fueltype == fuel, 'DateIn'] = df.loc[df.Fueltype == fuel, 'DateIn'].fillna(
            df.loc[df.Fueltype == fuel, 'DateIn'].mean().astype(int))

        if fuel in ['Lignite', 'Oil']:
            efficiency = 'Efficiency'
        else:
            efficiency = 'efficiency'

        dff = df.dropna(subset=[efficiency, 'DateIn'])

        linear_regressor = LinearRegression()

        def get_mask(fuel_):
            if fuel_ == 'Natural Gas':
                return (dff.Fueltype == fuel_) & (dff.DateIn > 1970)
            else:
                return dff.Fueltype == fuel_

        X = dff.loc[get_mask(fuel), 'DateIn'].values.reshape(-1, 1)
        Y = dff.loc[get_mask(fuel), efficiency].values.reshape(-1, 1)
        linear_regressor.fit(X, Y)
        index = df[(df.Fueltype == fuel) & (df[efficiency].isna())].index
        df.loc[index, 'Efficiency'] = linear_regressor.predict(df.loc[index, 'DateIn'].values.reshape(-1, 1))
        if fuel not in ['Lignite', 'Oil']:
            index = df[(df.Fueltype == fuel) & (~df[efficiency].isna())].index
            df.loc[index, 'Efficiency'] = df.loc[index, 'efficiency']

    return df.drop('efficiency', axis=1)


def matched_data(
    config=None,
    update=False,
    from_url=False,
    extend_by_vres=False,
    extendby_kwargs={},
    extend_by_kwargs={},
    with_efficiency=True,
    update_eff=False,
    **collection_kwargs,
):
    """
    Return the full matched dataset including all data sources listed in
    config.yaml/matching_sources. The combined data is additionally extended
    by non-matched entries of sources given in
    config.yaml/fully_included_souces.


    Parameters
    ----------
    update : Boolean, default False
            Whether to rerun the matching process. Overrides stored to False
            if True.
    from_url: Boolean, default False
            Whether to parse and store the already build data from the repo
            website.
    config : Dict, default None
            Define a configuration varying from the setting in config.yaml.
            Relevant keywords are 'matching_sources', 'fully_included_sources'.
    extend_by_vres : Boolean, default False
            Whether extend the dataset by variable renewable energy sources
            given by powerplantmatching.data.OPSD_VRE()
    extendby_kwargs : Dict,
            Dict of keywordarguments passed to powerplantmatchting.
            heuristics.extend_by_non_matched
    **collection_kwargs : kwargs
            Arguments passed to powerplantmatching.collection.Collection.

    """
    from . import __version__

    if config is None:
        config = get_config()

    deprecated_args = {"update_all", "stored"}
    used_deprecated_args = deprecated_args.intersection(collection_kwargs.keys())
    if used_deprecated_args:
        msg = "The following arguments were deprecated and are being ignored: "
        logger.warn(msg + f"{used_deprecated_args}")
    if extendby_kwargs:
        logger.warn(
            DeprecationWarning,
            "`extendby_kwargs` is deprecated in the favor of extend_by_kwargs",
        )
        extend_by_kwargs.update(extendby_kwargs)

    collection_kwargs.setdefault("update", update)

    if collection_kwargs.get("reduced", True):
        fn = _data_out("matched_data_red.csv", config)
        header = 0
    else:
        fn = _data_out("matched_data.csv", config)
        header = [0, 1]

    if from_url:
        fn = _data_out("matched_data_red.csv", config)
        url = config["matched_data_url"].format(tag="v" + __version__)
        logger.info(f"Retrieving data from {url}")
        df = (
            pd.read_csv(url, index_col=0)
            .pipe(projectID_to_dict)
            .pipe(set_column_name, "Matched Data")
        )
        if with_efficiency:
            df = extrapolate_efficiencies(df, config, update_eff)
        logger.info(f"Store data at {fn}")
        df.to_csv(fn)
        return df

    if not update and os.path.exists(fn):
        df = (
            pd.read_csv(fn, index_col=0, header=header)
            .pipe(projectID_to_dict)
            .pipe(set_column_name, "Matched Data")
        )
        if with_efficiency:
            fn_eff = _data_out("matched_data_eff.csv", config)
            if os.path.exists(fn_eff):
                df = (
                    pd.read_csv(fn_eff, index_col=0, header=header)
                        .pipe(projectID_to_dict)
                        .pipe(set_column_name, "Matched Data")
                )
            else:
                df = extrapolate_efficiencies(df, config, update_eff)
        if extend_by_vres:
            return df.pipe(
                extend_by_VRE, config=config, base_year=config["opsd_vres_base_year"]
            )
        return df

    matching_sources = [
        list(to_dict_if_string(a))[0] for a in config["matching_sources"]
    ]
    matched = collect(matching_sources, config=config, **collection_kwargs)

    if isinstance(config["fully_included_sources"], list):
        for source in config["fully_included_sources"]:
            source = to_dict_if_string(source)
            (name,) = list(source)
            extend_by_kwargs.update({"query": source[name]})
            matched = extend_by_non_matched(
                matched, name, config=config, **extend_by_kwargs
            )

    matched.to_csv(fn, index_label="id", encoding="utf-8")

    if extend_by_vres:
        matched = extend_by_VRE(
            matched, config=config, base_year=config["opsd_vres_base_year"]
        )
    matched = matched.pipe(set_column_name, "Matched Data")

    if with_efficiency:
        matched = extrapolate_efficiencies(matched, config, update_eff)

    return matched

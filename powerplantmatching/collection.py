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
Processed datasets of merged and/or adjusted data
"""
from __future__ import print_function

from .utils import set_uncommon_fueltypes_to_other, _data_out
from .data import data_config
from .cleaning import aggregate_units
from .matching import combine_multiple_datasets, reduce_matched_dataframe
from .heuristics import (extend_by_non_matched, extend_by_VRE,
                         remove_oversea_areas, manual_corrections,
                         average_empty_commyears)
from .config import get_config

import pandas as pd
import os
from ast import literal_eval as liteval
import logging
logger = logging.getLogger(__name__)


def collect(datasets, update=False, use_saved_aggregation=False,
            use_saved_matches=False, reduced=True,
            custom_config={}, config=None, **dukeargs):
    """
    Return the collection for a given list of datasets in matched or
    reduced form.

    Parameters
    ----------
    datasets : list or str
        list containing the dataset identifiers as str, or single str
    update : bool
        Do an horizontal update (True) or read from the cache file (False)
    use_saved_aggregation : bool
        Aggregate units based on cached aggregation group files (True)
        or to do an vertical update (False)
    use_saved_matches : bool
        Match datasets based on cached matched pair files (True)
        or to do an horizontal matching (False)
    reduced : bool
        Switch as to return the reduced (True) or matched (False) dataset.
    custom_config : dict
        Updates to the data_config dict from data module
    **dukeargs : keyword-args for duke
    """

    if config is None:
        config = get_config()

    def df_by_name(name):
        conf = data_config[name].copy()
        conf.update(custom_config.get(name, {}))

        df = conf['read_function'](**conf.get('read_kwargs', {}),
                                   config=config)
        if not conf.get('aggregated_units', False):
            return aggregate_units(df,
                                   use_saved_aggregation=use_saved_aggregation,
                                   dataset_name=name,
                                   config=config)
        else:
            return df.assign(projectID=df.projectID.map(lambda x: {name: [x]}))

    # Deal with the case that only one dataset is requested
    if isinstance(datasets, str):
        return df_by_name(datasets)

    datasets = sorted(datasets)
    logger.info('Collect combined dataset for {}'.format(', '.join(datasets)))
    outfn_matched = _data_out('Matched_{}.csv'
                              .format('_'.join(map(str.upper, datasets))),
                              config=config)
    outfn_reduced = _data_out('Matched_{}_reduced.csv'
                              .format('_'.join(map(str.upper, datasets))),
                              config=config)

    if not update and not os.path.exists(outfn_reduced
                                         if reduced else outfn_matched):
        logger.warning("Forcing update since the cache file is missing")
        update, use_saved_aggregation = True, True

    if update:
        dfs = [df_by_name(name) for name in datasets]
        matched = combine_multiple_datasets(
                dfs, datasets, use_saved_matches=use_saved_matches, **dukeargs,
                config=config)
        matched.to_csv(outfn_matched, index_label='id', encoding='utf-8')

        reduced_df = reduce_matched_dataframe(matched, config=config)
        reduced_df.to_csv(outfn_reduced, index_label='id', encoding='utf-8')

        return reduced_df if reduced else matched
    else:
        if reduced:
            df = pd.read_csv(outfn_reduced, index_col=0, encoding='utf-8')
            df.projectID = (df.projectID.apply(lambda df: liteval(df)))
        else:
            df = pd.read_csv(outfn_matched, index_col=0, header=[0, 1],
                             encoding='utf-8', low_memory=False)
            df.projectID = (df.projectID.stack().dropna().apply(
                    lambda df: liteval(df)).unstack())
        return df


def Collection(**kwargs):
    return collect(**kwargs)


def matched_data(config=None,
                 extend_by_vres=False,
                 extendby_kwargs={'use_saved_aggregation': True},
                 subsume_uncommon_fueltypes=False,
                 **collection_kwargs):
    """
    Return the full matched dataset including all data sources listed in
    config.yaml/matching_sources. The combined data is additionally extended
    by non-matched entries of sources given in
    config.yaml/fully_inculded_souces.


    Parameters
    ----------
    config : Dict, default None
            Define a configuration varying from the setting in config.yaml.
            Relevant keywords are 'matching_sources', 'fully_included_sources'.
    extend_by_vres : Boolean, default False
            Whether extend the dataset by variable renewable energy sources
            given by powerplantmatching.data.OPSD_VRE()
    extendby_kwargs : Dict, default {'use_saved_aggregation': True}
            Dict of keywordarguments passed to powerplatnmatchting.
            heuristics.extend_by_non_matched
    subsume_uncommon_fueltypes : Boolean, default False
            Whether to replace uncommon fueltype specification by 'Other'
    **collection_kwargs : kwargs
            keywordarguments passed to powerplantmatching.collection.Collection

    """
    if config is None:
        config = get_config()

    matched = collect(config['matching_sources'], **collection_kwargs)

    for source in config['fully_included_sources']:
        matched = extend_by_non_matched(matched,
                                        source,
                                        config=config,
                                        **extendby_kwargs)

    # drop matches between only low reliability-data, this is necessary since
    # a lot of those are decommissioned, however some countries only appear in
    # GEO and CARMA
    if len(matched.columns.levels) > 1:
        matched = (matched[matched.projectID.apply(lambda x: sorted(x.keys())
                   not in [['CARMA', 'GEO']]) |
                   matched.Country.isin(
                           ['Croatia', 'Czech Republic', 'Estonia'])])

    if extend_by_vres:
        matched = extend_by_VRE(matched,
                                base_year=config['opsd_vres_base_year'])

    if subsume_uncommon_fueltypes:
        matched = set_uncommon_fueltypes_to_other(matched)
    return matched


def MATCHED_dataset(**kwargs):
    logger.warning('MATCHED_dataset deprecated soonly, please use matched_data'
                   ' instead')
    return matched_data(**kwargs)


#  ============================================================================
# From here on, functions will be deprecated soon!

def Carma_ENTSOE_GEO_GPD_OPSD_matched(update=False,
                                      use_saved_matches=False,
                                      use_saved_aggregation=False):
    return collect(['CARMA', 'ENTSOE', 'GEO', 'GPD', 'OPSD'],
                   update=update, use_saved_matches=use_saved_matches,
                   use_saved_aggregation=use_saved_aggregation,
                   reduced=False)


def Carma_ENTSOE_GEO_GPD_OPSD_matched_reduced(update=False,
                                              use_saved_matches=False,
                                              use_saved_aggregation=False):
    return collect(['CARMA', 'ENTSOE', 'GEO', 'GPD', 'OPSD'],
                   update=update, use_saved_matches=use_saved_matches,
                   use_saved_aggregation=use_saved_aggregation,
                   reduced=True)


# --- The next two definitions include ESE as well ---

# unpublishable
def Carma_ENTSOE_ESE_GEO_GPD_OPSD_matched(update=False,
                                          use_saved_matches=False,
                                          use_saved_aggregation=False):
    return collect(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'GPD', 'OPSD'],
                   update=update,
                   use_saved_matches=use_saved_matches,
                   use_saved_aggregation=use_saved_aggregation,
                   reduced=False)


# unpublishable
def Carma_ENTSOE_ESE_GEO_GPD_OPSD_matched_reduced(update=False,
                                                  use_saved_matches=False,
                                                  use_saved_aggregation=False):
    return collect(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'GPD', 'OPSD'],
                   update=update, use_saved_matches=use_saved_matches,
                   use_saved_aggregation=use_saved_aggregation,
                   reduced=True)

# --- The next three definitions include ESE+IWPDCY as well ---


# unpublishable
def Carma_ENTSOE_ESE_GEO_GPD_IWPDCY_OPSD_matched(update=False,
                                                 use_saved_matches=False,
                                                 use_saved_aggregation=False):
    return collect(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'GPD', 'IWPDCY', 'OPSD'],
                   update=update, use_saved_matches=use_saved_matches,
                   use_saved_aggregation=use_saved_aggregation,
                   reduced=False)


# unpublishable
def Carma_ENTSOE_ESE_GEO_GPD_IWPDCY_OPSD_matched_reduced(
        update=False, use_saved_matches=False, use_saved_aggregation=False):
    return collect(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'GPD', 'IWPDCY', 'OPSD'],
                   update=update,
                   use_saved_matches=use_saved_matches,
                   use_saved_aggregation=use_saved_aggregation,
                   reduced=True)


# unpublishable
def Carma_ENTSOE_ESE_GEO_GPD_IWPDCY_OPSD_matched_reduced_VRE(
        update=False, use_saved_matches=False, use_saved_aggregation=False,
        update_concat=False, base_year=2016):
    if update_concat:
        logger.info('Read base reduced dataframe...')
        df = collect(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'GPD',
                      'IWPDCY', 'OPSD'],
                     update=update,
                     use_saved_matches=use_saved_matches,
                     use_saved_aggregation=use_saved_aggregation,
                     reduced=True)
        df = extend_by_VRE(df, base_year=base_year)
        df.to_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_'
                            'IWPDCY_OPSD_WRI_reduced_vre.csv'),
                  index_label='id', encoding='utf-8')
    else:
        logger.info('Read existing reduced_vre dataframe...')
        df = pd.read_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_'
                                   'IWPDCY_OPSD_WRI_reduced_vre.csv'),
                         index_col=0, encoding='utf-8')
    return df


# --- The next three definitions include ESE+IWPDCY+WEPP as well ---

# unpublishable
def Carma_ENTSOE_ESE_GEO_GPD_IWPDCY_OPSD_WEPP_matched(
        update=False, use_saved_matches=False, use_saved_aggregation=False):
    return collect(datasets=['CARMA', 'ENTSOE', 'ESE', 'GEO',
                                'GPD', 'IWPDCY', 'OPSD', 'WEPP'],
                      update=update, use_saved_matches=use_saved_matches,
                      use_saved_aggregation=use_saved_aggregation,
                      reduced=False)


# unpublishable
def Carma_ENTSOE_ESE_GEO_GPD_IWPDCY_OPSD_WEPP_matched_reduced(
        update=False, use_saved_matches=False, use_saved_aggregation=False):
    return collect(datasets=['CARMA', 'ENTSOE', 'ESE', 'GEO',
                                'GPD', 'IWPDCY', 'OPSD', 'WEPP'],
                      update=update,
                      use_saved_matches=use_saved_matches,
                      use_saved_aggregation=use_saved_aggregation,
                      reduced=True)


# unpublishable
def Carma_ENTSOE_ESE_GEO_GPD_IWPDCY_OPSD_WEPP_matched_reduced_VRE(
        update=False, use_saved_matches=False, use_saved_aggregation=False,
        base_year=2015, update_concat=False):
    if update_concat:
        logger.info('Read base reduced dataframe...')
        df = (Carma_ENTSOE_ESE_GEO_GPD_IWPDCY_OPSD_WEPP_matched_reduced(
                update=update,
                use_saved_matches=use_saved_matches,
                use_saved_aggregation=use_saved_aggregation)
              .pipe(manual_corrections)
              .pipe(average_empty_commyears)
              .pipe(extend_by_VRE, base_year=base_year, prune_beyond=True)
              .pipe(remove_oversea_areas))
        df.to_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_'
                            'IWPDCY_OPSD_WEPP_WRI_reduced_vre.csv'),
                  index_label='id', encoding='utf-8')
    else:
        logger.info('Read existing reduced_vre dataframe...')
        df = pd.read_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_'
                                   'IWPDCY_OPSD_WEPP_WRI_reduced_vre.csv'),
                         index_col=0, encoding='utf-8', low_memory=False)
    return df

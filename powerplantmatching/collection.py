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
Processed datasets of merged and/or adjusted data
"""
from __future__ import print_function

import pandas as pd
import os
import ast
import logging
logger = logging.getLogger(__name__)
from .utils import set_uncommon_fueltypes_to_other, _data_in, _data_out
from .data import data_config, OPSD, ESE
from .cleaning import clean_single
from .matching import combine_multiple_datasets, reduce_matched_dataframe
from .heuristics import (extend_by_non_matched, extend_by_VRE, remove_oversea_areas,
                         manual_corrections)


def Collection(datasets, update=False, use_saved_aggregation=False,
               use_saved_matches=False, reduced=True,
               custom_config={}, **dukeargs):
    """
    Return the collection for a given list of datasets in matched or reduced form.

    Parameters
    ----------
    datasets : list or str
        list containing the dataset identifiers as str, or single str
    update : bool
        Do an horizontal update (True) or read from the cache file (False)
    use_saved_aggregation : bool
        Aggregate units based on cached aggregation group files (True)
        or to do an vertical update (False)
    reduced : bool
        Switch as to return the reduced (True) or matched (False) dataset.
    custom_config : dict
        Updates to the data_config dict from data module
    **dukeargs : keyword-args for duke
    """

    # Deal with the case that only one dataset is requested
    if isinstance(datasets, str):
        name = datasets
        conf = data_config[name].copy()
        conf.update(custom_config.get(name, {}))

        df = conf['read_function'](**conf.get('read_kwargs', {}))
        return clean_single(df, use_saved_aggregation=use_saved_aggregation,
                          dataset_name=name,
                          **conf.get('clean_single_kwargs', {}))

    datasets = sorted(datasets)
    outfn_matched = _data_out('Matched_{}.csv'
                              .format('_'.join(map(str.upper, datasets))))
    outfn_reduced = _data_out('Matched_{}_reduced.csv'
                              .format('_'.join(map(str.upper, datasets))))

    if not update and not os.path.exists(outfn_reduced if reduced else outfn_matched):
        logger.warning("Forcing update since the cache file is missing")
        update = True
        use_saved_aggregation = True

    if update:
        dfs = []
        for name in datasets:
            conf = data_config[name].copy()
            conf.update(custom_config.get(name, {}))

            df = conf['read_function'](**conf.get('read_kwargs', {}))
            df = clean_single(df, use_saved_aggregation=use_saved_aggregation,
                              dataset_name=name,
                              **conf.get('clean_single_kwargs', {}))
            dfs.append(df)
        matched = combine_multiple_datasets(dfs, datasets,
                                            use_saved_matches=use_saved_matches,
                                            **dukeargs)
        matched.to_csv(outfn_matched, index_label='id', encoding='utf-8')

        reduced_df = reduce_matched_dataframe(matched)
        reduced_df.to_csv(outfn_reduced, index_label='id', encoding='utf-8')

        return reduced_df if reduced else matched
    else:
        if reduced:
            sdf = pd.read_csv(outfn_reduced, index_col=0, encoding='utf-8')
        else:
            sdf = pd.read_csv(outfn_matched, index_col=0, header=[0,1], encoding='utf-8')
        if 'projectID' in sdf and reduced:
            try: # ast.literal_eval() seems to be unstable when NaN are given.
                sdf.projectID = (sdf.projectID.str.replace('\[nan\]','[]')
                                    .apply(lambda df: ast.literal_eval(df)))
            except ValueError:
                pass
        return sdf


def Carma_ENTSOE_GEO_OPSD_WRI_matched(update=False,
                                      use_saved_matches=False,
                                      use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'GEO', 'OPSD', 'WRI'],
                      update=update, use_saved_matches=use_saved_matches,
                      use_saved_aggregation=use_saved_aggregation,
                      reduced=False)

def Carma_ENTSOE_GEO_OPSD_WRI_matched_reduced(update=False,
                                              use_saved_matches=False,
                                              use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'GEO', 'OPSD', 'WRI'],
                      update=update, use_saved_matches=use_saved_matches,
                      use_saved_aggregation=use_saved_aggregation,
                      reduced=True)

def MATCHED_dataset(aggregated_hydros=False, rescaled_hydros=False,
                    subsume_uncommon_fueltypes=False,
                    include_unavailables=False, **kwargs):
    """
    This returns the actual match between the databases Carma, ENTSOE, ESE, GEO,
    IWPDCY, OPSD and WRI with an additional manipulation on the hydro
    powerplants. The latter were adapted in terms of the power plant
    technology (Run-of-river, Reservoir, Pumped-Storage) and were
    quantitatively  adjusted to the ENTSOE-statistics. For more information
    about the technology and adjustment, see the hydro-aggreation.py file.

    Parameters
    ----------
    rescaled_hydros : Boolean, default False
            Whether to rescale hydro powerplant capacity in order to fulfill the
            statistics of the ENTSOE-data and receive better covering of the country
            totals.
    subsume_uncommon_fueltypes : Boolean, default False
            Whether to reduce the fueltype specification such that "Geothermal", "Waste"
            and "Mixed Fueltypes" are declared as "Other".
    """

    if include_unavailables:
        matched = Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_matched_reduced(**kwargs)
        matched = extend_by_non_matched(matched, ESE(), 'ESE',
                                    clean_added_data=False)

    else:
        matched = Carma_ENTSOE_GEO_OPSD_WRI_matched_reduced(**kwargs)
    columns = matched.columns
    matched = (extend_by_non_matched(matched, OPSD(), 'OPSD', clean_added_data=True,
                                    use_saved_aggregation=True)
                                    #remaining fossi fuels as hard coal
                                    .assign(Fueltype =
                                            lambda df: df.Fueltype.fillna('Hard Coal')))

#   drop matches between only low reliability-data, this is necessary since
#   a lot of those are decommissioned: Probably we should filter
#   by mean reliability larger than 3
#    some countries only appear in GEO and CARMA
#    matched = matched[matched.projectID.apply(lambda x : x.keys() not in
#                                              [['GEO', 'CARMA'], ['CARMA', 'GEO']] )
#                      | matched.Country.isin(['Croatia', 'Czech Republic', 'Estonia'])]


    if aggregated_hydros:
        hydro = Aggregated_hydro(scaled_capacity=rescaled_hydros)
        matched = matched[matched.Fueltype != 'Hydro']
        matched = pd.concat([matched, hydro], ignore_index=True)
    if subsume_uncommon_fueltypes:
        matched = set_uncommon_fueltypes_to_other(matched)
    return matched[columns]


def Aggregated_hydro(update=False, scaled_capacity=False):
    fn = _data_in('hydro_aggregation.csv')
#    if update or not os.path.exists(outfn):
#        # Generate the matched database
#        raise NotImplemented()
#        # matched_df = ...
#
#        # matched_df.to_csv(outfn)
#        # return matched_df
    hydro = pd.read_csv(fn, index_col='id')
    if scaled_capacity:
        hydro.Capacity = hydro.loc[:, 'Scaled Capacity']
    return hydro.drop('Scaled Capacity', axis=1)


# --- The next two definitions include ESE as well ---

# unpublishable
def Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched(update=False,
                                          use_saved_matches=False,
                                          use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'OPSD', 'WRI'],
                      update=update,
                      use_saved_matches=use_saved_matches,
                      use_saved_aggregation=use_saved_aggregation,
                      reduced=False)

# unpublishable
def Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced(update=False,
                                                  use_saved_matches=False,
                                                  use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'OPSD', 'WRI'],
                      update=update, use_saved_matches=use_saved_matches,
                      use_saved_aggregation=use_saved_aggregation,
                      reduced=True)

# --- The next three definitions include ESE+IWPDCY as well ---

# unpublishable
def Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_matched(update=False,
                                                 use_saved_matches=False,
                                                 use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'IWPDCY', 'OPSD', 'WRI'],
                      update=update, use_saved_matches=use_saved_matches,
                      use_saved_aggregation=use_saved_aggregation,
                      reduced=False)

# unpublishable
def Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_matched_reduced(update=False,
                                                         use_saved_matches=False,
                                                         use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'IWPDCY', 'OPSD', 'WRI'],
                      update=update, use_saved_matches=use_saved_matches,
                      use_saved_aggregation=use_saved_aggregation,
                      reduced=True)

# unpublishable
def Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_matched_reduced_VRE(update=False,
                                                             use_saved_matches=False,
                                                             use_saved_aggregation=False,
                                                             update_concat=False, base_year=2016):
    if update_concat:
        logger.info('Read base reduced dataframe...')
        df = Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'IWPDCY', 'OPSD', 'WRI'],
                        update=update, use_saved_matches=use_saved_matches,
                        use_saved_aggregation=use_saved_aggregation, reduced=True)
        df = extend_by_VRE(df, base_year=base_year)
        df.to_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_reduced_vre.csv'),
                  index_label='id', encoding='utf-8')
    else:
        logger.info('Read existing reduced_vre dataframe...')
        df = pd.read_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_reduced_vre.csv'),
                         index_col=0, encoding='utf-8')
    return df


# --- The next three definitions include ESE+IWPDCY+WEPP as well ---

#unpublishable
def Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched(update=False,
                                                      use_saved_matches=False,
                                                      use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'IWPDCY', 'OPSD', 'WEPP', 'WRI'],
                      update=update, use_saved_matches=use_saved_matches,
                      use_saved_aggregation=use_saved_aggregation,
                      reduced=False)

#unpublishable
def Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced(update=False,
                                                              use_saved_matches=False,
                                                              use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'IWPDCY', 'OPSD', 'WEPP', 'WRI'],
                      update=update, use_saved_matches=use_saved_matches,
                      use_saved_aggregation=use_saved_aggregation,
                      reduced=True)

#unpublishable
def Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced_VRE(update=False,
                                  use_saved_matches=False, use_saved_aggregation=False,
                                  base_year=2015, update_concat=False):
    if update_concat:
        logger.info('Read base reduced dataframe...')
        df = (Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced(update=update,
                                                        use_saved_matches=use_saved_matches,
                                                        use_saved_aggregation=use_saved_aggregation)
              .pipe(manual_corrections)
              .pipe(extend_by_VRE, base_year=base_year, prune_beyond=True)
              .pipe(remove_oversea_areas))
        df.to_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_reduced_vre.csv'),
                  index_label='id', encoding='utf-8')
    else:
        logger.info('Read existing reduced_vre dataframe...')
        df = pd.read_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_reduced_vre.csv'),
                         index_col=0, encoding='utf-8', low_memory=False)
    return df

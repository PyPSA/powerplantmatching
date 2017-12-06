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
import ast
import logging
logger = logging.getLogger(__name__)
from .utils import set_uncommon_fueltypes_to_other, _data_in, _data_out
from .data import data_config, OPSD, WRI
from .cleaning import clean_single
from .matching import combine_multiple_datasets, reduce_matched_dataframe
from .heuristics import (extend_by_non_matched, extend_by_VRE)


def Collection(datasets, update=False, use_saved_aggregation=False, reduced=True,
               custom_config={}):
    if 'ESE' in custom_config:
        if custom_config['ESE']['read_kwargs']['add_IWPDCY']:
            datasets = datasets + ['IWPDCY']
    datasets = sorted(datasets)
    outfn_matched = _data_out('Matched_{}.csv'
                              .format('_'.join(map(str.upper, datasets))))
    outfn_reduced = _data_out('Matched_{}_reduced.csv'
                              .format('_'.join(map(str.upper, datasets))))

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
        matched = combine_multiple_datasets(dfs, datasets)
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
            sdf.projectID = sdf.projectID.apply(lambda df: ast.literal_eval(df))
        return sdf

def Carma_ENTSOE_GEO_OPSD_matched(update=False, use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'GEO', 'OPSD'],
                      update=update, use_saved_aggregation=use_saved_aggregation,
                      reduced=False)

def Carma_ENTSOE_GEO_OPSD_matched_reduced(update=False, use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'GEO', 'OPSD'],
                      update=update, use_saved_aggregation=use_saved_aggregation,
                      reduced=True)

def Carma_ENTSOE_GEO_OPSD_WRI_matched(update=False, use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'GEO', 'OPSD', 'WRI'],
                      update=update, use_saved_aggregation=use_saved_aggregation,
                      reduced=False)

def Carma_ENTSOE_GEO_OPSD_WRI_matched_reduced(update=False, use_saved_aggregation=False):
    return Collection(['CARMA', 'ENTSOE', 'GEO', 'OPSD', 'WRI'],
                      update=update, use_saved_aggregation=use_saved_aggregation,
                      reduced=True)

def MATCHED_dataset(aggregated_hydros=True, rescaled_hydros=False,
                    subsume_uncommon_fueltypes=False,
                    include_unavailables=False):
    """
    This returns the actual match between the databases Carma, ENTSOE, ESE, GEO,
    OPSD and WRI with an additional manipulation on the hydro
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
        matched = Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced()
    else:
        matched = Carma_ENTSOE_GEO_OPSD_WRI_matched_reduced()
    columns = matched.columns
    matched = extend_by_non_matched(matched, OPSD(), 'OPSD', clean_added_data=True,
                                    use_saved_aggregation=True)
#    if include_unavailables:
#        matched = extend_by_non_matched(matched, ESE(), 'ESE', clean_added_data=True,
#                                         use_saved_aggregation=True)

    matched = extend_by_non_matched(matched, WRI(), 'WRI',
            fueltypes=['Wind'], clean_added_data=True, use_saved_aggregation=True )

    if aggregated_hydros:
        hydro = Aggregated_hydro(scaled_capacity=rescaled_hydros)
        matched = matched[matched.Fueltype != 'Hydro']
        matched = pd.concat([matched, hydro]).reset_index(drop=True)
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


# --- The next three definitions include ESE as well ---

# unpublishable
def Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched(update=False, use_saved_aggregation=False,
                                          add_IWPDCY=False):
    return Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'OPSD', 'WRI'],
                      update=update, use_saved_aggregation=use_saved_aggregation, reduced=False,
                      custom_config={'ESE': dict(read_kwargs=
                                                 {'add_IWPDCY': add_IWPDCY})})

# unpublishable
def Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced(update=False, use_saved_aggregation=False,
                                                  add_IWPDCY=False):
    return Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'OPSD', 'WRI'],
                      update=update, use_saved_aggregation=use_saved_aggregation, reduced=True,
                      custom_config={'ESE': dict(read_kwargs=
                                                 {'add_IWPDCY': add_IWPDCY})})

# unpublishable
def Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced_VRE(update=False, use_saved_aggregation=False,
                                                      add_IWPDCY=False, update_concat=False, base_year=2016):
    if update_concat:
        logger.info('Read base reduced dataframe...')
        df = Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'OPSD', 'WRI'],
                      update=update, use_saved_aggregation=use_saved_aggregation, reduced=True,
                      custom_config={'ESE': dict(read_kwargs={'add_IWPDCY': add_IWPDCY})})
        df = extend_by_VRE(df, base_year=base_year)
        df.to_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_OPSD_WRI_reduced_vre.csv'),
                  index_label='id', encoding='utf-8')
    else:
        logger.info('Read existing reduced_vre dataframe...')
        df = pd.read_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_OPSD_WRI_reduced_vre.csv'),
                         index_col=0, encoding='utf-8')
    return df


# --- The next three definitions include ESE+WEPP as well ---

#unpublishable
def Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched(update=False, use_saved_aggregation=False,
                                               add_IWPDCY=False):
    return Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'OPSD', 'WEPP', 'WRI'],
                      update=update,
                      use_saved_aggregation=use_saved_aggregation, reduced=False,
                      custom_config={'ESE': dict(read_kwargs=
                                                 {'add_IWPDCY': add_IWPDCY})})

#unpublishable
def Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced(update=False, use_saved_aggregation=False,
                                                       add_IWPDCY=False):
    return Collection(['CARMA', 'ENTSOE', 'ESE', 'GEO', 'OPSD', 'WEPP', 'WRI'],
                      update=update,
                      use_saved_aggregation=use_saved_aggregation, reduced=True,
                      custom_config={'ESE': dict(read_kwargs=
                                                 {'add_IWPDCY': add_IWPDCY})})

#unpublishable
def Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced_VRE(update=False,
                                use_saved_aggregation=False, base_year=2015, update_concat=False):
    if update_concat:
        logger.info('Read base reduced dataframe...')
        df = extend_by_VRE(Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced(
                update=update, use_saved_aggregation=use_saved_aggregation), base_year=base_year)
        df.to_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_reduced_vre.csv'),
                  index_label='id', encoding='utf-8')
    else:
        logger.info('Read existing reduced_vre dataframe...')
        df = pd.read_csv(_data_out('Matched_CARMA_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_reduced_vre.csv'),
                         index_col=0, encoding='utf-8')
    return df
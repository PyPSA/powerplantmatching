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
Functions for linking and combining different datasets
"""

from __future__ import absolute_import, print_function

from .config import get_config
from .utils import read_csv_if_string, _data_out
from .duke import duke
from .cleaning import clean_technology
from .data import data_config

import pandas as pd
import numpy as np
import itertools
import logging
logger = logging.getLogger(__name__)


def best_matches(links):
    """
    Subsequent to duke() with singlematch=True. Returns reduced list of
    matches on the base of the highest score for each duplicated entry.

    Parameters
    ----------
    links : pd.DataFrame
        Links as returned by duke
    """
    labels = links.columns.difference({'scores'})
    return (links
            .groupby(links.iloc[:, 1], as_index=False, sort=False)
            .apply(lambda x: x.loc[x.scores.idxmax(), labels]))


def compare_two_datasets(datasets, labels, use_saved_matches=False,
                         **dukeargs):
    """
    Duke-based horizontal match of two databases. Returns the matched
    dataframe including only the matched entries in a multi-indexed
    pandas.Dataframe. Compares all properties of the given columns
    ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','lat', 'lon'] in order to determine the same
    powerplant in different two datasets. The match is in one-to-one
    mode, that is every entry of the initial databases has maximally
    one link in order to obtain unique entries in the resulting
    dataframe.  Attention: When aborting this command, the duke
    process will still continue in the background, wait until the
    process is finished before restarting.

    Parameters
    ----------
    datasets : list of pandas.Dataframe or strings
        dataframes or csv-files to use for the matching
    labels : list of strings
        Names of the databases for the resulting dataframe


    """
    datasets = list(map(read_csv_if_string, datasets))
    if not ('singlematch' in dukeargs):
        dukeargs['singlematch'] = True
    saving_path = _data_out('matches/matches_{}_{}.csv'
                            .format(*np.sort(labels)))
    if use_saved_matches:
        try:
            logger.info('Reading saved matches for datasets {} and {}'
                        .format(*labels))
            return pd.read_csv(saving_path, index_col=0)
        except (ValueError, IOError):
            logger.warning("Non-existing saved matches for dataset '{}',{} "
                           "continuing by matching again".format(*labels))
    links = duke(datasets, labels=labels, **dukeargs)
    matches = best_matches(links)
    matches.to_csv(saving_path)
    return matches


def cross_matches(sets_of_pairs, labels=None):
    """
    Combines multiple sets of pairs and returns one consistent
    dataframe. Identifiers of two datasets can appear in one row even
    though they did not match directly but indirectly through a
    connecting identifier of another database.

    Parameters
    ----------
    sets_of_pairs : list
        list of pd.Dataframe's containing only the matches (without
        scores), obtained from the linkfile (duke() and
        best_matches())
    labels : list of strings
        list of names of the databases, used for specifying the order
        of the output

    """
    m_all = sets_of_pairs
    if labels is None:
        labels = np.unique([x.columns for x in m_all])
    matches = pd.DataFrame(columns=labels)
    for i in labels:
        base = [m.set_index(i) for m in m_all if i in m]
        match_base = pd.concat(base, axis=1).reset_index()
        matches = pd.concat([matches, match_base])

    matches = matches.drop_duplicates().reset_index(drop=True)
    for i in labels:
        matches = pd.concat([
            matches.groupby(i, as_index=False, sort=False)
                   .apply(lambda x: x.loc[x.isnull().sum(axis=1).idxmin()]),
            matches[matches[i].isnull()]
        ]).reset_index(drop=True)
    return (matches
            .assign(length=matches.notna().sum(axis=1))
            .sort_values(by='length', ascending=False)
            .reset_index(drop=True)
            .drop('length', axis=1)
            .reindex(columns=labels))


def link_multiple_datasets(datasets, labels, use_saved_matches=False,
                           **dukeargs):
    """
    Duke-based horizontal match of multiple databases. Returns the
    matching indices of the datasets. Compares all properties of the
    given columns ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','lat', 'lon'] in order to determine the same
    powerplant in different datasets. The match is in one-to-one mode,
    that is every entry of the initial databases has maximally one
    link to the other database.  This leads to unique entries in the
    resulting dataframe.

    Parameters
    ----------
    datasets : list of pandas.Dataframe or strings
        dataframes or csv-files to use for the matching
    labels : list of strings
        Names of the databases in alphabetical order and corresponding
        order to the datasets
    """
    datasets = list(map(read_csv_if_string, datasets))
    combinations = list(itertools.combinations(range(len(labels)), 2))
    all_matches = []
    for c, d in combinations:
        logger.info('Comparing {0} with {1}'.format(labels[c], labels[d]))
        match = compare_two_datasets(
                [datasets[c], datasets[d]], [labels[c], labels[d]],
                use_saved_matches=use_saved_matches, **dukeargs)
        all_matches.append(match)
    return cross_matches(all_matches, labels=labels)


def combine_multiple_datasets(datasets, labels, use_saved_matches=False,
                              config=None, **dukeargs):
    """
    Duke-based horizontal match of multiple databases. Returns the
    matched dataframe including only the matched entries in a
    multi-indexed pandas.Dataframe. Compares all properties of the
    given columns ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','lat', 'lon'] in order to determine the same
    powerplant in different datasets. The match is in one-to-one mode,
    that is every entry of the initial databases has maximally one
    link to the other database.  This leads to unique entries in the
    resulting dataframe.

    Parameters
    ----------
    datasets : list of pandas.Dataframe or strings
        dataframes or csv-files to use for the matching
    labels : list of strings
        Names of the databases in alphabetical order and corresponding
        order to the datasets
    """
    if config is None:
        config = get_config()

    def combined_dataframe(cross_matches, datasets, config):
        """
        Use this function to create a matched dataframe on base of the
        cross matches and a list of the databases. Always order the
        database alphabetically.

        Parameters
        ----------
        cross_matches : pandas.Dataframe of the matching indexes of
            the databases, created with
            powerplant_collection.cross_matches()
        datasets : list of pandas.Dataframes or csv-files in the same
            order as in cross_matches
        """
        datasets = list(map(read_csv_if_string, datasets))
        for i, data in enumerate(datasets):
            datasets[i] = (data
                           .reindex(cross_matches.iloc[:, i])
                           .reset_index(drop=True))
        return (pd.concat(datasets, axis=1,
                          keys=cross_matches.columns.tolist())
                .reorder_levels([1, 0], axis=1)
                .reindex(columns=config['target_columns'], level=0)
                .reset_index(drop=True))
    crossmatches = link_multiple_datasets(datasets, labels,
                                          use_saved_matches=use_saved_matches,
                                          **dukeargs)
    return (combined_dataframe(crossmatches, datasets)
            .reindex(columns=config['target_columns'], level=0))


def reduce_matched_dataframe(df, show_orig_names=False, config=None):
    """
    Reduce a matched dataframe to a unique set of columns. For each entry
    take the value of the most reliable data source included in that match.

    Parameters
    ----------
    df : pandas.Dataframe
        MultiIndex dataframe with the matched powerplants, as obtained from
        combined_dataframe() or match_multiple_datasets()
    """

    def concat_strings(s):
        if s.isnull().all():
            return np.nan
        else:
            return s[s.notnull()].str.cat(sep=', ')

    if config is None:
        config = get_config()

    # define which databases are present and get their reliability_score
    sources = df.columns.levels[1]
    rel_scores = (pd.DataFrame(data_config).loc['reliability_score', sources]
                    .sort_values(ascending=False))

    def prioritise_reliability(df, how='mean'):
        """
        Take the first most reliable value if dtype==str,
        else take mean of most reliable values
        """

        # Arrange columns in descending order of reliability
        df = df.loc[df.notnull().any(axis=1)]

        if df.empty:
            logger.warn('Empty dataframe passed to `prioritise_reliability`.')
            return pd.Series()

        df = df.reindex(columns=rel_scores.index)

        # Aggregate data with same reliability scores for numeric columns
        # (but DO maintain order)
        if not ((df.dtypes == object) | (df.dtypes == str)).any():
            # all numeric
            df = df.groupby(rel_scores, axis=1, sort=False).agg(how)

        return df.apply(lambda ds: ds.dropna().iloc[0], axis=1)

    sdf = pd.DataFrame.from_dict({
        'Name': prioritise_reliability(df['Name']),
        'Fueltype': (prioritise_reliability(df['Fueltype']
                                            .replace({'Other': np.nan}))
                     .reindex(df.index, fill_value='Other')),
        'Technology': prioritise_reliability(df['Technology']),
        'Country': prioritise_reliability(df['Country']),
        'Set': prioritise_reliability(df['Set']),
        'Capacity': prioritise_reliability(df['Capacity'], how='median'),
        'Duration': prioritise_reliability(df['Duration']),
        'YearCommissioned': df['YearCommissioned'].min(axis=1),
        'Retrofit': df['Retrofit'].max(axis=1),
        'lat': prioritise_reliability(df['lat']),
        'lon': prioritise_reliability(df['lon']),
        'File': df['File'].apply(concat_strings, axis=1),
        'projectID': df['projectID'].apply(lambda x: dict(x.dropna()), axis=1)
    }).reindex(config['target_columns'], axis=1)

    if show_orig_names:
        sdf = sdf.assign(**dict(df.Name))
    sdf = clean_technology(sdf, generalize_hydros=False)
    sdf.reset_index(drop=True)
    if show_orig_names:
        return sdf
    else:
        sdf.reindex(columns=config['target_columns'])

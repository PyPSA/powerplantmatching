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
Functions for linking and combining different datasets
"""

from __future__ import absolute_import, print_function

import pandas as pd
import numpy as np
import itertools
import logging
logger = logging.getLogger(__name__)
from .config import target_columns
from .utils import read_csv_if_string
from .duke import duke
from .cleaning import clean_technology


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

def compare_two_datasets(datasets, labels):
    """
    Duke-based horizontal match of two databases. Returns the matched
    dataframe including only the matched entries in a multi-indexed
    pandas.Dataframe. Compares all properties of the given columns
    ['Name','Fueltype', 'Technology', 'Country',
    'Capacity','Geoposition'] in order to determine the same
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
    links = duke(datasets, labels=labels, singlematch=True)
    matches = best_matches(links)
    return matches

def cross_matches(sets_of_pairs, labels=None):
    """
    Combines multiple sets of pairs and returns one consistent dataframe. Identifiers of two
    datasets can appear in one row even though they did not match directly but indirectly
    through a connecting identifier of another database.

    Parameters
    ----------
    sets_of_pairs : list
        list of pd.Dataframe's containing only the matches (without scores), obtained from the
        linkfile (duke() and best_matches())
    labels : list of strings
        list of names of the databases, used for specifying the order of the output

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
        matches = pd.concat([matches.groupby(i, as_index=False, sort=False).\
                             apply(lambda x: x.loc[x.isnull().sum(axis=1).idxmin()]),\
                             matches[matches[i].isnull()]]).reset_index(drop=True)
    return matches.loc[:,labels]

def link_multiple_datasets(datasets, labels):
    """
    Duke-based horizontal match of multiple databases. Returns the matching
    indices of the datasets. Compares all properties of the
    given columns ['Name','Fueltype', 'Technology', 'Country', 'Capacity','Geoposition'] in order
    to determine the same powerplant in different datasets. The match is in one-to-one mode,
    that is every entry of the initial databases has maximally one link to the other database.
    This leads to unique entries in the resulting dataframe.

    Parameters
    ----------
    datasets : list of pandas.Dataframe or strings
        dataframes or csv-files to use for the matching
    labels : list of strings
        Names of the databases in alphabetical order and corresponding order to the datasets
    """
#    return ArgumentError if labels not in alphabetical order
    datasets = list(map(read_csv_if_string, datasets))
    combinations = list(itertools.combinations(range(len(labels)), 2))
    all_matches = []
    for c,d in combinations:
        logger.info('Comparing {0} with {1}'.format(labels[c], labels[d]))
        match = compare_two_datasets([datasets[c],datasets[d]],[labels[c],labels[d]])
        all_matches.append(match)
    return cross_matches(all_matches, labels=labels)


def combine_multiple_datasets(datasets, labels):
    """
    Duke-based horizontal match of multiple databases. Returns the matched dataframe including
    only the matched entries in a multi-indexed pandas.Dataframe. Compares all properties of the
    given columns ['Name','Fueltype', 'Technology', 'Country', 'Capacity','Geoposition'] in order
    to determine the same powerplant in different datasets. The match is in one-to-one mode,
    that is every entry of the initial databases has maximally one link to the other database.
    This leads to unique entries in the resulting dataframe.

    Parameters
    ----------
    datasets : list of pandas.Dataframe or strings
        dataframes or csv-files to use for the matching
    labels : list of strings
        Names of the databases in alphabetical order and corresponding order to the datasets
    """
    def combined_dataframe(cross_matches, datasets):
        """
        Use this function to create a matched dataframe on base of the cross matches
        and a list of the databases. Always order the database alphabetically.

        Parameters
        ----------
        cross_matches : pandas.Dataframe of the matching indexes of the databases,
            created with powerplant_collection.cross_matches()
        datasets : list of pandas.Dataframes or csv-files in the same
            order as in cross_matches


        """
        datasets = list(map(read_csv_if_string, datasets))
        for i, data in enumerate(datasets):
            datasets[i] = data.loc[cross_matches.ix[:, i]].reset_index(drop=True)
        df = pd.concat(datasets, axis=1, keys=cross_matches.columns.tolist())
        df = df.reorder_levels([1, 0], axis=1)
        df = df[df.columns.levels[0]]
        df = df.loc[:,target_columns()]
        return df.reset_index(drop=True)
    crossmatches = link_multiple_datasets(datasets, labels)
    return combined_dataframe(crossmatches, datasets)


def reduce_matched_dataframe(df):
    """
    Returns a new reduced dataframe with all names of the powerplants, according
    to the following logic:
        - Averages: Capacity, longitude and latitude
        - Most frequent value: Country, Fueltype and Technology
        - Max: YearCommissioned*

    * Two thinkable cases in which it both makes sense to choose the latest year:
    Case A: Plant has been retrofitted (e.g. 1973,1974,1973,2008) -> Choose 2008.
    Case B: Some dbs refer to the construction year, others to grid synchronization year.
        (1973,1974,1973,1972) -> choose 1974.

    Parameters
    ----------
    df : pandas.Dataframe
        MultiIndex dataframe with the matched powerplants, as obtained from
        combined_dataframe() or match_multiple_datasets()
    """

    def most_frequent_fueltype(df):
        if df.isnull().all():
            return np.nan
        else:
            # Priority for Lignite: If any dataset claims the fueltype is Lignite -> accept!
            if df.isin(['Lignite']).any():
                return 'Lignite'
            else:
                values = df.value_counts()
                if values.idxmax() == 'Hard Coal' and len(values)>1:
                    return values.index[1]
                else:
                    return values.idxmax()

    def most_frequent(df):
        if df.isnull().all():
            return np.nan
        else:
            values = df.value_counts()
            return values.idxmax()

    def concat_strings(df):
        if df.isnull().all():
            return np.nan
        else:
            return df[df.notnull()].str.cat(sep = ', ')

    def optimised_mean(df):
        if df.notnull().sum()>2:
            return df[~((df - df.mean()).abs()>df.std())].mean()
        elif ('CARMA' in df and df.notnull().sum()==2):
            return df.drop('CARMA').mean()
        elif ('WRI' in df and df.notnull().sum()==2):
            return df.drop('WRI').mean()
        else:
            return df.mean()

    sdf = pd.DataFrame(df.Name)
    sdf.loc[:, 'Fueltype'] = df.Fueltype.apply(most_frequent_fueltype, axis=1)
    sdf.loc[:, 'Technology'] = df.Technology.apply(concat_strings, axis=1)
    sdf.loc[:, 'Country'] = df.Country.apply(most_frequent, axis=1)
    sdf.loc[:, 'Set'] = df.Set.apply(most_frequent, axis=1)
    sdf.loc[:, 'Capacity'] = df.Capacity.median(axis=1)
    sdf.loc[:, 'YearCommissioned'] = df.YearCommissioned.max(axis=1)
    sdf.loc[:, 'lat'] = df.lat.apply(optimised_mean, axis=1)
    sdf.loc[:, 'lon'] = df.lon.apply(optimised_mean, axis=1)
    sdf.loc[:, 'File'] = df.File.apply(concat_strings, axis=1)
    sdf.loc[:,'projectID'] = df.projectID.apply(lambda x:
                                dict(zip(df.columns.levels[1][x.notnull()].values
                                , x.dropna().values)), axis=1)
    sdf = clean_technology(sdf, generalize_hydros=False)
    return sdf.reset_index(drop=True)

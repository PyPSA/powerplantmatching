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
Functions for vertically cleaning a dataset
"""
from __future__ import absolute_import, print_function

import numpy as np
import pandas as pd
import networkx as nx
import logging
logger = logging.getLogger(__name__)

from .config import target_columns, target_technologies
from .utils import read_csv_if_string
from .duke import duke
from .utils import (_data_out)



def clean_powerplantname(df):
    """
    Cleans the column "Name" of the database by deleting very frequent
    words, numericals and nonalphanumerical characters of the
    column. Returns a reduced dataframe with nonempty Name-column.

    Parameters
    ----------
    df : pandas.Dataframe
        dataframe which should be cleaned

    """

    name = df.Name.replace(regex=True, value=' ',
                           to_replace=list('-/')+['\(', '\)', '\[', '\]', '[0-9]'])

    common_words = pd.Series(sum(name.str.split(), [])).value_counts()
    cw = list(common_words[common_words >= 20].index)

    pattern = [('(?i)(^|\s)'+x+'(?=\s|$)')
               for x in (cw +
                         ['[a-z]','I','II','III','IV','V','VI','VII','VIII',
                          'IX','X','XI','Grupo','parque','eolico','gas',
                          'biomasa','COGENERACION','gt','unnamed',
                          'tratamiento de purines','planta','de','la','station',
                          'power','storage','plant','stage','pumped','project'])]
    name = (name
            .replace(regex=True, to_replace=pattern, value=' ')
            .str.strip().str.replace('\\s\\s+', ' ')
            # do it twice to catch all single letters
            # .replace(regex=True, to_replace=pattern, value=' ')
            # .str.strip().str.replace('\\s\\s+', ' ')
            .str.capitalize())

    return (df
            .assign(Name=name)
            .loc[lambda x: x.Name != '']
            .sort_values('Name')
            .reset_index(drop=True))


def gather_fueltype_info(df, search_col=['Name', 'Technology']):
    fueltype = pd.Series(df['Fueltype'])

    for i in search_col:
        found_b = df[i].dropna().str.contains('(?i)lignite|brown')
        fueltype.loc[found_b.reindex(fueltype.index, fill_value=False)] = 'Lignite'
    fueltype.replace({'Coal': 'Hard Coal'}, inplace=True)

    return df.assign(Fueltype=fueltype)



def gather_technology_info(df, search_col=['Name', 'Fueltype']):
    technology = (df['Technology'].dropna()
                  if 'Technology' in df
                  else pd.Series())

    pattern = '|'.join(('(?i)'+x) for x in target_technologies())
    for i in search_col:
        found = (df[i].dropna()
                 .str.findall(pattern)
                 .loc[lambda s: s.str.len() > 0]
                 .str.join(sep=', '))

        exists_i = technology.index.intersection(found.index)
        if len(exists_i) > 0:
            technology.loc[exists_i] = (technology.loc[exists_i]
                                        .str.cat(found.loc[exists_i], sep=', '))

        new_i = found.index.difference(technology.index)
        technology = technology.append(found[new_i])

    return df.assign(Technology=technology)


def gather_set_info(df, search_col=['Name', 'Fueltype']):
    Set = (df['Set'].dropna()
           if 'Set' in df
           else pd.Series())

    if 'chp' in df:
        Set.loc[df['chp'].isin({True, 'yes', 1.0})] = 'CHP'

    pattern = '|'.join(['heizkraftwerk', 'hkw', 'chp', 'bhkw', 'cogeneration',
                        'power and heat', 'heat and power'])
    for i in search_col:
        isCHP_b = df[i].dropna().str.contains(pattern, case=False)
        Set.loc[isCHP_b] = 'CHP'

    return df.assign(Set=Set.fillna('PP'))


def clean_technology(df, generalize_hydros=False):
    tech = df['Technology'].dropna()
    if len(tech)==0:
        return df
    tech = tech.replace({' and ': ', ', ' Power Plant': ''}, regex=True)
    if generalize_hydros:
        tech[tech.str.contains('pump', case=False)] = 'Pumped Storage'
        tech[tech.str.contains('reservoir|lake', case=False)] = 'Reservoir'
        tech[tech.str.contains('run-of-river|weir|water', case=False)] = 'Run-Of-River'
        tech[tech.str.contains('dam', case=False)] = 'Reservoir'
    tech = tech.replace({'Gas turbine': 'OCGT'})
    tech[tech.str.contains('combined cycle', case=False)] = 'CCGT'
    tech[tech.str.contains('steam turbine|critical thermal', case=False)] = 'Steam Turbine'
    tech[tech.str.contains('ocgt|open cycle', case=False)] = 'OCGT'
    tech = (tech.str.title()
                .str.split(', ')
                .apply(lambda x: ', '.join(i.strip() for i in np.unique(x))))
    tech = tech.replace({'Ccgt': 'CCGT', 'Ocgt': 'OCGT'}, regex=True)
    return df.assign(Technology=tech)


def cliques(df, dataduplicates):
    """
    Locate cliques of units which are determined to belong to the same
    powerplant.  Return the same dataframe with an additional column
    "grouped" which indicates the group that the powerplant is
    belonging to.

    Parameters
    ----------
    df : pandas.Dataframe or string
        dataframe or csv-file which should be analysed
    dataduplicates : pandas.Dataframe or string
        dataframe or name of the csv-linkfile which determines the
        link within one dataset
    """
    df = read_csv_if_string(df)
    G = nx.DiGraph()
    G.add_nodes_from(df.index)
    G.add_edges_from((r.one, r.two) for r in dataduplicates.itertuples())
    H = G.to_undirected(reciprocal=True)

    grouped = pd.Series(np.nan, index=df.index)
    for i, inds in enumerate(nx.algorithms.clique.find_cliques(H)):
        grouped.loc[inds] = i

    return df.assign(grouped=grouped)


def aggregate_units(df, use_saved_aggregation=False, dataset_name=None,
                    detailed_columns=False):
    """
    Vertical cleaning of the database. Cleans the "Name"-column, sums
    up the capacity of powerplant units which are determined to belong
    to the same plant.

    Parameters
    ----------
    df : pandas.Dataframe or string
        dataframe or csv-file to use for the resulting database
    use_saved_aggregation : Boolean (default False):
        Whether to use the automaticly saved aggregation file, which
        is stored in data/aggregation_groups_XX.csv with XX being
        either a custom name for the dataset. This saves time if you
        want to have aggregated powerplants without running the
        aggregation algorithm again
    dataset_name : str
        costum name for dataset identification, choose your own
        identification in case no metadata is passed to the function
    """
    def prop_for_groups(x):
        """
        Function for grouping duplicates within one dataset. Sums up
        the capacity, takes mean from latitude and longitude, takes
        the most frequent values for the rest of the columns

        """
        results = {'Name': x.Name.value_counts().index[0],
                   'Country': x.Country.value_counts(dropna=False).index[0] ,
#                     if   x.Country.notnull().any(axis=0) else np.NaN,
                   'Fueltype': x.Fueltype.value_counts(dropna=False).index[0],
#                       if x.Fueltype.notnull().any(axis=0) else np.NaN,
                   'Technology': ', '.join(x.Technology.dropna().unique())
                                            if x.Technology.notnull().any(axis=0) else np.NaN,
                   'Set' : ', '.join(x.Set.dropna().unique()),
                   'File': x.File.value_counts(dropna=False).index[0],
                   'Capacity': x['Capacity'].fillna(0.).sum(),
                   'lat': x['lat'].astype(float).mean(),
                   'lon': x['lon'].astype(float).mean(),
                   'YearCommissioned': x['YearCommissioned'].min(),
                   'projectID': list(x.projectID)}
        if 'Duration' in x:
            results['Duration'] = (x.Duration*x.Capacity /x.Capacity.sum()).sum()

        return pd.Series(results)

    path_name = _data_out('aggregation_groups_{}.csv'.format(dataset_name))
    if use_saved_aggregation:
        try:
            logger.info('Reading saved aggregation groups for dataset: {}'.format(dataset_name))
            groups = pd.read_csv(path_name, header=None, index_col=0).loc[df.index]
            df = df.assign(grouped=groups.values)
        except ValueError:
            logger.warning("Non-existing saved links for this dataset, continuing by aggregating again")
            df.drop('grouped', axis=1, inplace=True)

    if 'grouped' not in df:
        duplicates = duke(read_csv_if_string(df))
        df = cliques(df, duplicates)
        try:
            df.grouped.to_csv(path_name)
        except IndexError:
            pass

    df = df.groupby('grouped').apply(prop_for_groups)
    if 'Duration' in df:
        df['Duration'] = df['Duration'].replace(0., np.nan)
    df.reset_index(drop=True, inplace=True)
    df = df.loc[:, target_columns(detailed_columns=detailed_columns)]
    return df

def clean_single(df, aggregate_powerplant_units=True, use_saved_aggregation=False,
                 dataset_name=None, detailed_columns=False):
    """
    Vertical cleaning of the database. Cleans the "Name"-column, sums
    up the capacity of powerplant units which are determined to belong
    to the same plant.

    Parameters
    ----------
    df : pandas.Dataframe or string
        dataframe or csv-file to use for the resulting database

    aggregate_units : Boolean, default True
        Whether or not the power plant units should be aggregated

    use_saved_aggregation : Boolean, default False
        Only sensible if aggregate_units is set to True.
        Whether to use the automatically saved aggregation file, which
        is stored in data/aggregation_groups_XX.csv with XX
        being either a custom name for the dataset or the name passed
        with the metadata of the pd.DataFrame. This saves time if you
        want to have aggregated powerplants without running the
        aggregation algorithm again

    dataset_name : str
        Only sensible if aggregate_units is set to True.  custom name
        for dataset identification, choose your own identification in
        case no metadata is passed to the function
    """
    df = clean_powerplantname(df)

    if aggregate_powerplant_units:
        df = aggregate_units(df, use_saved_aggregation=use_saved_aggregation,
                             dataset_name=dataset_name, detailed_columns=detailed_columns)
    else:
        df['projectID'] = df['projectID'].dropna().map(lambda x: [x])
    return clean_technology(df)

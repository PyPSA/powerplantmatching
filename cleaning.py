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
import os
import six

from .config import target_columns
from .utils import read_csv_if_string
from .duke import duke

def clean_powerplantname(df):
    """
    Cleans the column "Name" of the database by deleting very frequent words, numericals and
    nonalphanumerical characters of the column. Returns a reduced dataframe with nonempty
    Name-column.

    Parameters
    ----------
    df : pandas.Dataframe
        dataframe which should be cleaned

    """
    df.Name.replace(regex=True, value=' ',
                    to_replace=list('-/')+['\(', '\)', '\[', '\]', '[0-9]'],
                    inplace=True)

    common_words = pd.Series(sum(df.Name.str.split(), [])).value_counts()
    cw = list(common_words[common_words >= 20].index)

    pattern = [('(?i)(^|\s)'+x+'($|\s)') for x in cw + \
       ['[a-z]','I','II','III','IV','V','VI','VII','VIII','IX','X','XI','Grupo',
     'parque','eolico','gas','biomasa','COGENERACION'
     ,'gt','unnamed','tratamiento de purines','planta','de','la','station', 'power',
     'storage', 'plant', 'stage', 'pumped', 'project'] ]
    df.Name = df.Name.replace(regex=True, to_replace=pattern, value=' ').str.strip().\
                str.replace('\\s\\s+', ' ')
    #do it twise to cach all single letters
    df.Name = df.Name.replace(regex=True, to_replace=pattern, value=' ').str.strip().\
                str.replace('\\s\\s+', ' ').str.capitalize()
    df = df[df.Name != ''].reset_index(drop=True)
    return df


def gather_classification_info(df, search_col=['Name', 'Fueltype']):
    pattern = '|'.join(('(?i)'+x) for x in
                       ['lignite', 'Hard coal', 'Coal hard', 'ccgt', 'ocgt', 'chp'
                        'reservoir', 'pumped storage', 'run-of-river'])
    for i in search_col:
        found = df.loc[:,i].str.findall(pattern).str.join(sep=', ')
        df.loc[:, 'Classification'] = (df.loc[:, 'Classification'].fillna('')
                            .str.cat(found.fillna(''), sep=', ').str.strip())
        df.Classification = df.Classification.str.replace('^ , |^,|, $|,$', '').apply(lambda x:
                     ', '.join(list(set(x.split(', ')))) ).str.strip()
        df.Classification.replace('', np.NaN, regex=True, inplace=True)
    return df


def clean_classification(df, generalize_hydros=False):
    df.loc[:,'Classification'] = df.loc[:,'Classification'].replace([' and ',
                                ' Power Plant'],[', ', ''],regex=True )
    if generalize_hydros:
        df.loc[(df.Classification.str.contains('reservoir|lake', case=False)) &
                  (df.Classification.notnull()), 'Classification'] = 'Reservoir'
        df.loc[(df.Classification.str.contains('run-of-river|weir|water', case=False)) &
                  (df.Classification.notnull()), 'Classification'] = 'Run-Of-River'
        df.loc[(df.Classification.str.contains('dam', case=False)) &
                  (df.Classification.notnull()), 'Classification'] = 'Reservoir'
        df.loc[(df.Classification.str.contains('Pump|pumped', case=False)) &
                  (df.Classification.notnull()), 'Classification'] = 'Pumped Storage'

    df.loc[df.Classification.notnull(),'Classification'] = df.loc[df.Classification.
                               notnull(),'Classification'].str.split(', ')\
                                .apply(lambda x: ', '.join(np.unique(x)))
    df.loc[np.logical_not((df.loc[:,'Classification'].str.contains('(?i)CCGT|(?i)OCGT|(?i)CHP', regex=True)))&
           df.loc[:, 'Classification'].notnull(),'Classification'] = \
            df.loc[np.logical_not((df.loc[:,'Classification'].str.contains('(?i)CCGT|(?i)OCGT|(?i)CHP', regex=True)))&
           df.loc[:, 'Classification'].notnull(),'Classification'].str.title()
    return df


def cliques(df, dataduplicates):
    """
    Locate cliques of units which are determined to belong to the same powerplant.
    Return the same dataframe with an additional column "grouped" which indicates the
    group that the powerplant is belonging to.

    Parameters
    ----------
    df : pandas.Dataframe or string
        dataframe or csv-file which should be analysed
    dataduplicates : pandas.Dataframe or string
        dataframe or name of the csv-linkfile which determines the link within one
        dataset

    """
    df = read_csv_if_string(df)
    G = nx.DiGraph()
    G.add_nodes_from(df.index)
    G.add_edges_from((r.one, r.two) for r in dataduplicates.itertuples())
    H = G.to_undirected(reciprocal=True)
    for i, inds in enumerate(nx.algorithms.clique.find_cliques(H)):
        df.loc[inds, 'grouped'] = i

    return df

#alternativly use components (much faster however can lead to very big components)
#Example:
#adjacency_matrixWRI = sp.sparse.coo_matrix((np.ones(len(WRIDuplicates)), \
#(WRIDuplicates.one.values, WRIDuplicates.two.values)), shape=[len(WRIdata),len(WRIdata)]).toarray()
#
#n_componentsWRI, labelsWRI = sp.sparse.csgraph.connected_components(adjacency_matrixWRI,\
#connection="strong", directed=True)
#
#WRIdata["grouped"]=labelsWRI


def aggregate_units(df):
    """
    Vertical cleaning of the database. Cleans the "Name"-column, sums up the capacity
    of powerplant units which are determined to belong to the same plant.

    Parameters
    ----------
    df : pandas.Dataframe or string
        dataframe or csv-file to use for the resulting database


    """
    def prop_for_groups(x):
        """
        Function for grouping duplicates within one dataset. Sums up the capacity, takes
        mean from lattitude and longitude, takes the most frequent values for the rest of the
        columns

        """
        results = {'Name': x.Name.value_counts().index[0],
                   'Country': x.Country.value_counts().index[0] if \
                        x.Country.notnull().any(axis=0) else np.NaN,
                   'Fueltype': x.Fueltype.value_counts().index[0] if \
                        x.Fueltype.notnull().any(axis=0) else np.NaN,
                   'Classification': ', '.join(x[x.Classification.notnull()].Classification.unique())
                                            if x.Classification.notnull().any(axis=0) else np.NaN,
                   'File': x.File.value_counts().index[0] if x.File.notnull().any(axis=0) else np.NaN,
                   'Capacity': x['Capacity'].sum() if x.Capacity.notnull().any(axis=0) else np.NaN,
                   'lat': x['lat'].mean(),
                   'lon': x['lon'].mean(),
                   'projectID': list(x.projectID)}
        return pd.Series(results)

    duplicates = duke(read_csv_if_string(df))
    df = cliques(df, duplicates)

    df = df.groupby('grouped').apply(prop_for_groups)
    df.reset_index(drop=True, inplace=True)
    df = df[target_columns()]
    return df

def clean_single(df, aggregate_powerplant_units=True):
    """
    Vertical cleaning of the database. Cleans the "Name"-column, sums up the capacity
    of powerplant units which are determined to belong to the same plant.

    Parameters
    ----------
    df : pandas.Dataframe or string
        dataframe or csv-file to use for the resulting database

    aggregate_units : Boolean, default True
        Wether or not the power plant units should be aggregated

    """
    #df = gather_classification_info(df)
    df = clean_powerplantname(df)

    if aggregate_powerplant_units:
        df = aggregate_units(df)

    return df

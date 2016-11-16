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
Functions to modify and adjust power plant datasets
"""

from __future__ import absolute_import, print_function

from .utils import read_csv_if_string

def extend_by_non_matched(df, extend_by, label):
    """
    Returns the matched dataframe with additional entries of non-matched powerplants
    of a reliable source.
    
    Parameters
    ----------
    df : Pandas.DataFrame
        Already matched dataset which should be extended
    extend_by : pd.DataFrame
        Database which is partially included in the matched dataset, but 
        which should be included totally
    label : str
        Column name of the additional database within the matched dataset, this 
        string is used if the columns of the additional database do not correspond
        to the ones of the dataset
    """
    extend_by = read_csv_if_string(extend_by)
    if 'Name' in extend_by.columns:
        extend_by = extend_by.rename(columns={'Name':label})
    return (df.append(extend_by[~extend_by.loc[:, label].isin(df.loc[:,label])])
              .reset_index(drop=True))
    
def rescale_capacities_to_country_totals(df, fueltypes):
    raise NotImplemented

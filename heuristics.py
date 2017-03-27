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
import pandas as pd 

from .utils import read_csv_if_string
from .utils import lookup
from .data import ENTSOE_stats
from .cleaning import clean_single


def extend_by_non_matched(df, extend_by, label, fueltypes=None, 
                          clean_added_data=True, by_name=False):
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
    columns = df.columns
    is_included = df.projectID.map(lambda d: d.get(label)).dropna().astype(str)\
                                     .str.replace('\[|\]', '').str.split(', ').sum()
    not_included = pd.DataFrame(extend_by.projectID.tolist())
    extend_by_b = ~ (pd.concat([not_included[i].dropna()\
                            .isin(is_included) for i in not_included],axis=1)).any(axis=1)    
    if by_name:
        extend_by_b = ~extend_by.loc[:, label].isin(df.loc[:,label])
    if fueltypes is None:
        extend_by = extend_by[extend_by_b]
    else:
        extend_by = extend_by[extend_by_b & (extend_by.Fueltype.isin(fueltypes))]
    if clean_added_data:
        extend_by = clean_single(extend_by)
    extend_by = extend_by.rename(columns={'Name':label})
    extend_by.projectID = extend_by.projectID.apply(lambda x: 
                                {label : x})
    return df.append(extend_by, ignore_index=True)[columns]
        
        
        
def rescale_capacities_to_country_totals(df, fueltypes):
    """
    Returns a extra column 'Scaled Capacity' with an up or down scaled capacity in
    order to match the statistics of the ENTSOe country totals. For every
    country the information about the total capacity of each fueltype is given.
    The scaling factor is determined by the ratio of the aggregated capacity of the
    fueltype within each coutry and the ENTSOe statistics about the fueltype capacity
    total within each country.

    Parameters
    ----------
    df : Pandas.DataFrame
        Data set that should be modified
    fueltype : str or list of strings
        fueltype that should be scaled
    """
    df = df.copy()
    if isinstance(fueltypes, str):
        fueltypes = [fueltypes]
    stats_df = lookup(df).loc[fueltypes]
    stats_entsoe = lookup(ENTSOE_stats()).loc[fueltypes]
    if ((stats_df==0)&(stats_entsoe!=0)).any().any():
        print('Could not scale powerplants in the countries %s because of no occurring \
              power plants in these countries'%\
              stats_df.loc[:, ((stats_df==0)&\
                            (stats_entsoe!=0)).any()].columns.tolist())
    ratio = (stats_entsoe/stats_df).fillna(1)
    df.loc[:,'Scaled Capacity'] = df.loc[:,'Capacity']
    for country in ratio:
        for fueltype in fueltypes:
            df.loc[(df.Country==country)&(df.Fueltype==fueltype), 'Scaled Capacity'] *= \
                   ratio.loc[fueltype,country]
    return df



def add_missing_capacities(df, fueltypes):
    """
    Primarily written to add artificial missing wind- and solar capacities to 
    match these to the statistics
    
    Parameters
    ----------
    df : Pandas.DataFrame
        Dataframe that should be modified
    fueltype : str or list of strings
        fueltype that should be scaled
    """
    df = df.copy()
    if isinstance(fueltypes, str):
        fueltypes = [fueltypes]
    stats_df = lookup(df).loc[fueltypes]
    stats_entsoe = lookup(ENTSOE_stats()).loc[fueltypes]
    missing = (stats_entsoe - stats_df).fillna(0.)
    missing[missing<0] = 0
    for country in missing:
        for fueltype in fueltypes:
            if missing.loc[fueltype,country] > 0:
                row = df.index.size
                df.loc[row, ['CARMA','ENTSOE','ESE','GEO','OPSD','WEPP','WRI']] = \
                               'Artificial_' + fueltype + '_' + country
                df.loc[row,'Fueltype'] = fueltype
                df.loc[row,'Country'] = country
                df.loc[row,'Set'] = 'PP'
                df.loc[row,'Capacity'] = missing.loc[fueltype,country]
                #TODO: This section needs to be enhanced, current values based on average construction year.
                if fueltype=='Solar':
                    df.loc[row,'YearCommissioned'] = 2011
                if fueltype=='Wind':
                    df.loc[row,'YearCommissioned'] = 2008
    return df



#add artificial powerplants
#entsoe = pc.ENTSOE_data()
#lookup = pc.lookup([entsoe.loc[entsoe.Fueltype=='Hydro'], hydro], keys= ['ENTSOE', 'matched'], by='Country')
#lookup.loc[:,'Difference'] = lookup.ENTSOE - lookup.matched
#missingpowerplants = (lookup.Difference/120).round().astype(int)
#
#hydroexp = hydro
#
#for i in missingpowerplants[:-1].loc[missingpowerplants[:-1] > 0].index:
#    print i
#    try:
#        howmany = missingpowerplants.loc[i]
#        hydroexp = hydroexp.append(hydro.loc[(hydro.Country == i)& (hydro.lat.notnull()),['lat', 'lon']].sample(howmany) + np.random.uniform(-.4,.4,(howmany,2)), ignore_index=True)
#        hydroexp.loc[hydroexp.shape[0]-howmany:,'Country'] = i
#        hydroexp.loc[hydroexp.shape[0]-howmany:,'Capacity'] = 120.
#        hydroexp.loc[hydroexp.shape[0]-howmany:,'FIAS'] = 'Artificial Powerplant'
#
#
#    except:
#        for j in range(missingpowerplants.loc[i]):
#            hydroexp = hydroexp.append(hydro.loc[(hydro.Country == i)& (hydro.lat.notnull()),['lat', 'lon']].sample(1) + np.random.uniform(-1,1,(1,2)), ignore_index=True)
#            hydroexp.loc[hydroexp.shape[0]-1:,'Country'] = i
#            hydroexp.loc[hydroexp.shape[0]-1:,'Capacity'] = 120.
#            hydroexp.loc[hydroexp.shape[0]-howmany:,'FIAS'] = 'Artificial Powerplant'
#
#for i in missingpowerplants[:-1].loc[missingpowerplants[:-1] < -1].index:
#    while hydroexp.loc[hydroexp.Country == i, 'Capacity'].sum() > lookup.loc[i, 'ENTSOE'] + 300:
#        try:
#            hydroexp = hydroexp.drop(hydroexp.loc[(hydroexp.Country == i)& (hydroexp.GEO.isnull())].sample(1).index)
#        except:
#            hydroexp = hydroexp.drop(hydroexp.loc[(hydroexp.Country == i)].sample(1).index)
#
#hydroexp.Fueltype = 'Hydro'
#pc.lookup([entsoe.loc[entsoe.Fueltype=='Hydro'], hydroexp], keys= ['ENTSOE', 'matched'], by='Country')
#
#del hydro
#hydro = hydroexp
#
#print hydro.groupby(['Country', 'Technology']).Capacity.sum().unstack()

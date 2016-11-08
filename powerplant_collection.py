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

from __future__ import print_function
import os
import numpy as np
import pandas as pd
import re
import networkx as nx
from countrycode import countrycode
import subprocess as sub
import itertools
from six.moves import map
import six

def OPSD_data(raw=False):
    """
    Return standardized GEO database with target column names and fueltypes.

    """
    opsd = pd.read_csv('%s/data/conventional_power_plants_EU.csv'%
                       os.path.dirname(__file__))
    if raw:
        return opsd
    opsd.columns = opsd.columns.str.title()
    opsd.rename(columns={'Lat':'lat',
                         'Lon':'lon',
                         'Energy_Source':'Fueltype',
                         'Technology':'Classification',
                         'Source':'File'
                         }, inplace=True)
    opsd = opsd.loc[:,target_columns()]
    opsd = gather_classification_info(opsd)
    opsd.Country = countrycode(codes=opsd.Country.tolist(), 
                               target='country_name', origin='iso2c')
    opsd.Country = opsd.Country.str.title()
    d = {'Hard coal': 'Coal',
         'Lignite': 'Coal',
         'Natural gas': 'Natural Gas',
         'Biomass and biogas': 'Waste',
         'Other or unspecified energy sources': 'Other'}
    opsd.Fueltype = opsd.Fueltype.replace(d)
    opsd = clean_powerplantname(opsd)
#    opsd = add_geoposition(opsd)
    return opsd
    

def GEO_data(raw=False):
    """
    Return standardized GEO database with target column names and fueltypes.

    """
    from vresutils import dispatch as vdispatch
    GEOdata = vdispatch.read_globalenergyobservatory_detailed()
    if raw:
        return GEOdata
    eucountries = europeancountries()
    GEOdata = GEOdata[GEOdata['Country'].isin(eucountries)]
    GEOdata.drop_duplicates(inplace=True)
    GEOdata.reset_index(drop=True)
    GEOdata.rename(columns={'Type': 'Fueltype'}, inplace=True)
    GEOdata.replace({'Gas': 'Natural Gas'}, inplace=True)
    GEOdata = gather_classification_info(GEOdata, search_col=['FuelClassification1'])
    GEOdata = clean_powerplantname(GEOdata)
    GEOdata = (GEOdata.fillna(0).groupby(['Name', 'Country', 'Fueltype',
               'Classification'])
               .agg({'Capacity': np.sum,
                     'lat': np.mean,
                     'lon': np.mean}).reset_index())
    GEOdata.Classification = GEOdata.Classification.replace({
       'Combined Cycle Gas Turbine':'CCGT',
       'Cogeneration Power and Heat Steam Turbine':'CHP',
       'Sub-critical Thermal|Super-critical Thermal':'Thermal',
       'Open Cycle Gas Turbine|Power and Heat OCGT':'OCGT',
       'Combined Cycle Gas Engine (CCGE)':'CCGE',
    'Power and Heat Combined Cycle Gas Turbine':'CCGT',
    'Both Sub and Super Critical Thermal|Ultra-Super-Critical Thermal':'Thermal', 
    'Cogeneration Power and Heat Steam Turbine':'CHP', 
    'Heat and Power Steam Turbine|Sub-critical Steam Turbine':'CHP'}
            , regex=True).str.strip()
    GEOdata.replace(0, np.NaN, inplace=True)
#    GEOdata = add_geoposition(GEOdata)
    return GEOdata.loc[:,target_columns()]


def CARMA_data(raw=False):
    """
    Return standardized Carma database with target column names and fueltypes.
    Only includes powerplants with capacity > 4 MW.
    """
    carmadata = pd.read_csv('%s/data/Full_CARMA_2009_Dataset_1.csv'\
    %os.path.dirname(__file__))
    if raw:
        return carmadata
    d = {'COAL': 'Coal',
     'WAT': 'Hydro',
     'FGAS': 'Natural Gas',
     'NUC': 'Nuclear',
     'FLIQ': 'Oil',
     'WIND': 'Wind',
     'BSOL': 'Waste',
     'EMIT': 'Other',
     'GEO': 'Geothermal',
     'WSTH': 'Waste',
     'SUN': 'Solar',
     'BLIQ': 'Waste',
     'BGAS': 'Waste',
     'BLIQ': 'Waste',
     'OTH': 'Other'}
    rename = {'Geoposition': 'Geoposition',
     'cap': 'Capacity',
     'city': 'location',
     'country': 'Country',
     'fuel1': 'Fueltype',
     'lat': 'lat',
     'lon': 'lon',
     'plant': 'Name'}
    carmadata.rename(columns=rename, inplace=True)
    carmadata =  carmadata.loc[:,target_columns()]
    carmadata = gather_classification_info(carmadata)
    carmadata = carmadata[carmadata.Capacity > 4]
    carmadata.drop_duplicates(inplace=True)
    carmadata = carmadata.replace(d)
    carmadata = carmadata[carmadata.Country.isin(europeancountries())]
#    carmadata = carmadata.loc[:, target_columns()]
    carmadata = clean_powerplantname(carmadata).sort_values('Name')
    carmadata.reset_index(drop=True, inplace=True)
#    carmadata = add_geoposition(carmadata)
    return carmadata


def Energy_storage_exchange_data():
    esed = pd.read_csv('%s/data/energy_storage_exchange.csv'
                       %os.path.dirname(__file__), index_col='id')
    return clean_powerplantname(esed)

def FIAS_data():
    return pd.read_csv('%s/data/FiasHydro.csv'%os.path.dirname(__file__), 
                       encoding='utf-8', index_col='id')

def ENTSOE_data(raw=False):
    """
    Standardize the entsoe database for statistical use.
    """
    opsd = pd.read_csv('%s/data/aggregated_capacity.csv'%os.path.dirname(__file__))
    if raw:
        return opsd
    entsoedata = opsd[opsd['source'].isin(['entsoe']) & opsd['year'].isin([2014])]
    cCodes = list(entsoedata.country)
    countries = countrycode(codes=cCodes, target='country_name', origin='iso2c')
    entsoedata = entsoedata.replace({'Bioenergy and other renewable fuels': 'Natural Gas',
     'Coal derivatives': 'Coal',
     'Differently categorized fossil fuels': 'Other',
     'Hard coal': 'Coal',
     'Lignite': 'Coal',
     'Mixed fossil fuels': 'Mixed fuel types',
     'Natural gas': 'Natural Gas',
     'Other or unspecified energy sources': 'Other',
     'Tide, wave, and ocean': 'Other'})
    entsoedata.country = countries
    entsoedata.country = entsoedata.country.str.title()
    entsoedata = entsoedata[entsoedata['technology_level_2'] == True]
    entsoedata.rename(columns={'technology': 'Fueltype'}, inplace=True)
    entsoedata.columns = entsoedata.columns.str.title()
    return entsoedata

def WRI_data():
    wri = pd.read_csv('%s/data/WRIdata.csv'%os.path.dirname(__file__), 
                      index_col='id')
#    wri.Name = wri.Name.str.title()
    wri = wri.loc[:,target_columns()]
    return wri
    
def Carma_FIAS_GEO_OPSD_WRI_matched(update=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data', 
                         'Matched_Carma_Fias_Geo_Opsd_Wri.csv')
    if update: #or not os.path.exists(outfn):
        matched = reduce_matched_dataframe(
             match_multiple_datasets([deduplicate(CARMA_data()),
                                    FIAS_data(), GEO_data(), 
                                    deduplicate(OPSD_data()),
                                    deduplicate(WRI_data())], 
                                    ['CARMA', 'FIAS', 'GEO','OPSD','WRI']))
#        raise NotImplemented
        # matched_df = ...
        # matched_df.to_csv(outfn)
        return matched
    else:
        return pd.read_csv(outfn, index_col='id')

def MATCHED_dataset(artificials=True, mainfueltypes=False):
    """
    This returns the actual match between the Carma-data, GEO-data, WRI-data, 
    FIAS-data and the ESE-data with an additional manipulation on the hydro powerplants. 
    The latter were adapted in terms of the power plant classification (Run-of-river, 
    Reservoir, Pumped-Storage) and were quantitatively  adjusted to the ENTSOE-
    statistics. For more information about the classification and adjustment, 
    see the hydro-aggreation.py file.
    
    Parameters
    ----------
    artificials : Boolean, defaut True
            Wether to include 210 artificial hydro powerplants in order to fulfill the 
            statistics of the ENTSOE-data and receive better covering of the country 
            totals
    mainfueltypes : Boolean, default False
            Wether to reduce the fueltype specification such that "Geothermal", "Waste"
            and "Mixed Fueltypes" are declared as "Other"
            
    """
    
    hydro = Aggregated_hydro()
    matched = extend_by_non_matched(Carma_FIAS_GEO_OPSD_WRI_matched(),
                                    GEO_data(), 'GEO')    
    matched = matched[matched.Fueltype != 'Hydro']
    if artificials==False:
        hydro=hydro[hydro.Fias!="Artificial Powerplant"]
    if mainfueltypes:
        matched=main_fueltypes(matched)
    return pd.concat([matched, hydro])

def Aggregated_hydro(update=False):
    outfn = os.path.join(os.path.dirname(__file__), 'data', 
                         'hydro_aggregation.csv')
    if update or not os.path.exists(outfn):
        # Generate the matched database
        raise NotImplemented()
        # matched_df = ...

        # matched_df.to_csv(outfn)
        # return matched_df
    else:
        return pd.read_csv(outfn, index_col='id')

#%%Tools
    
def lookup(df, keys=None, by='Country, Fueltype', exclude=None):
    """
    Returns a lookup table of the dataframe df with rounded numbers. Use different lookups
    as "Country", "Fueltype" for the different lookups.

    Parameters
    ----------
    df : pandas.Dataframe or list of pandas.Dataframe's
        powerplant databases to be analysed. If multiple dataframes are passed
        the lookup table will dusplay them in a MulitIndex
    by : string out of 'Country, Fueltype', 'Country' or 'Fueltype'
        Define the type pf lookup table you want to obtain.
    keys : list of strings
        labels of the different datasets, only nescessary if multiple dataframes
        passed
    exclude: list
        list of fueltype to exclude from the analysis

    """

    def lookup_single(df, by=by, exclude=exclude):
        df = read_csv_if_string(df)
        if exclude is not None:
            df = df[~df.Fueltype.isin(exclude)]
        if by == 'Country, Fueltype':
            return df.groupby(['Country', 'Fueltype']).Capacity.sum()\
                    .unstack(0).fillna(0).astype(int)
        elif by == 'Country':
            return df.groupby(['Country']).Capacity.sum().astype(int)
        elif by == 'Fueltype':
            return df.groupby(['Fueltype']).Capacity.sum().astype(int)
        else:
            raise NameError("``by` must be one of 'Country, Fueltype' or 'Country' or 'Fueltype'")

    if isinstance(df, list):
        dfs = pd.concat([lookup_single(a) for a in df], axis=1, keys=keys)
        if by == 'Country, Fueltype':
            dfs = dfs.reorder_levels([1, 0], axis=1)
            dfs = dfs[dfs.columns.levels[0]]
        dfs = dfs.fillna(0)
        dfs.loc['Total'] = dfs.sum()
        return dfs
    else:
        return lookup_single(df)



def europeancountries():
    """
    Returns a list of countries in Europe
    """
    return ['Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Czech Republic',
            'Denmark', 'Estonia', 'Finland', 'France', 'Germany',
            'Greece', 'Hungary', 'Ireland', 'Italy', 'Latvia',
            'Lithuania', 'Luxembourg', 'Netherlands', 'Norway',
            'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia',
            'Spain', 'Sweden', 'Switzerland', 'United Kingdom']


def target_fueltypes():
    """
    Returns a list of fueltypes to which the powerplants should be standardized
    """
    return ['Natural Gas', 'Wind', 'Hydro', 'Oil', 'Waste', 'Coal',
            'Nuclear', 'Other', 'Solar', 'Mixed fuel types', 'Geothermal']


def target_columns():
    """
    Returns a list of columns to which the powerplants should be standardized. For renaming
    columns use df.rename(columns=dic, inplace=True) with dic being a dictionary
    of the replacements
    """
    return ['Name', 'Fueltype', 'Classification', 'Country',
            'Capacity', 'lat', 'lon', 'File']

def main_fueltypes(df):
    df.loc[(df.Fueltype=='Geothermal')|(df.Fueltype=='Mixed fuel types')|
           (df.Fueltype=='Waste') , 'Fueltype']='Other'
    return df

def add_geoposition(df):
    """
    Returns the same pandas.Dataframe with an additional column "Geoposition" which
    concats the lattitude and longitude of the powerplant in a string

    """
    df.loc[df.lat.notnull(), 'Geoposition'] = df[df.lat.notnull()].lat.apply(str)\
           .str.cat(df[df.lat.notnull()].lon.apply(str), sep=',')
    return df

def gather_classification_info(df, search_col=['Name', 'Fueltype']):
    pattern = '|'.join(('(?i)'+x) for x in
                       ['lignite', 'Hard coal', 'Coal hard', 'ccgt', 'ocgt', 'chp'])
    for i in search_col:
        found = df.loc[:,i].str.findall(pattern).str.join(sep=', ')
        df.loc[:, 'Classification'] = df.loc[:, 'Classification'
                                                ].fillna('').str.cat(found, 
                                                sep=', ').str.strip()
        df.Classification = df.Classification.str.replace('^ , |^,|, $|,$', '')
        df.Classification.replace('', np.NaN, regex=True, inplace=True)
    return df
    
def prop_for_groups(x):
    """
    Function for grouping duplicates within one dataset. Sums up the capacity, takes
    mean from lattitude and longitude, takes the most frequent values for the rest of the
    columns

    """
    results = {'Name': x.Name.value_counts().index[0],
               'Country': x.Country.value_counts().index[0],
               'Fueltype': x.Fueltype.value_counts().index[0] if \
                    x.Fueltype.notnull().any(axis=0) else np.NaN,
               'Classification': ', '.join(x[x.Classification.notnull()].Classification.unique())
                                        if x.Classification.notnull().any(axis=0) else np.NaN,
               'File': x.File.value_counts().index[0] if x.File.notnull().any(axis=0) else np.NaN,
               'Capacity': x['Capacity'].sum() if x.Capacity.notnull().any(axis=0) else np.NaN,
               'lat': x['lat'].mean(),
               'lon': x['lon'].mean(),
               'ids': list(x.index)}
    return pd.Series(results)


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
    if isinstance(dataduplicates, six.string_types):
        dataduplicates = pd.read_csv(dataduplicates, 
                                     usecols=[1, 2], names=['one', 'two'])
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

def parse_Geoposition(loc, country):
    """
    Nominatim request for the Geoposition of a specific location in a country.
    Returns a tuples with (lattitude, longitude) if the request was sucessful,
    returns None otherwise.


    Parameters
    ----------
    loc : string
        description of the location, can be city, area etc.
    country : string
        name of the country which will be used as a bounding area


    """
    from geopy.geocoders import Nominatim
    if loc != None and loc != float:
        gdata = Nominatim(timeout=100, country_bias=str.lower(countrycode
              (codes=[country], origin='country_name', target='iso2c')[0])).geocode(loc)
        if gdata != None:
            lat = gdata.latitude
            lon = gdata.longitude
            return (lat, lon)
    else:
        return

def most_frequent(df):
    if df.isnull().all():
        return np.nan
    else:
        return df.value_counts().idxmax()


def read_csv_if_string(data):
    if isinstance(data, six.string_types):
        data = pd.read_csv(data, index_col='id')
    return data        

def duke(config, linkfile=None, singlematch=False, showmatches=False, wait=True):
    """
    Run duke in different modes (Deduplication or Record Linkage Mode) to either
    locate duplicates in one database or find the similar entries in two different datasets.
    In RecordLinkagesMode (match two databases) please set singlematch=True and use
    best_matches() afterwards

    Parameters
    ----------

    config : str
        Configruation file (.xml) for the Duke process
    linkfile : str, default None
        txt-file where to record the links
    singlematch: boolean, default False
        Only in Record Linkage Mode. Only report the best match for each entry of the first named
        dataset. This does not guarantee a unique match in the second named dataset.
    wait : boolean, default True
        wait until the process is finished


    """
    os.environ['CLASSPATH'] = ":".join([os.path.join(
                    os.path.dirname(__file__), "duke_binaries", r) 
                    for r in os.listdir(os.path.join(
                    os.path.dirname(__file__), "duke_binaries"))])
    args = []
    if linkfile is not None:
        args.append('--linkfile=%s' % linkfile)
    if singlematch:
        args.append('--singlematch')
    if showmatches:
        args.append('--showmatches')
    run = sub.Popen(['java', 'no.priv.garshol.duke.Duke'] + args + [config], stdout=sub.PIPE)
    if showmatches:
        print("\n For displaying matches run: 'for line in _.stdout: print line'")
    if wait:
        run.wait()
    else:
        print("\n The process will continue in the background, type '_.kill()' to abort ")
    return run

def best_matches(linkfile, labels):
    """
    Subsequent to powerplant_collection.duke() with singlematch=True. Returns reduced list of
    matches on the base of the highest score for each duplicated entry.

    Parameters
    ----------

    linkfile : string
        txt-file with the recorded links
    labels : list of strings
        Names of the databases for the resulting dataframe


    """
    matches = pd.read_csv(linkfile, usecols=[1, 2, 3], names=[labels[0],
                                             labels[1], 'scores'])
    return matches.groupby(matches.iloc[:, 1], as_index=False, 
                           sort=False).apply(lambda x: x.ix[x.scores.idxmax(), 0:2])

def deduplicate(df):
    """
    Vertical cleaning of the database. Cleans the "Name"-column, sums up the capacity
    of powerplant units which are determined to belong to the same plant.

    Parameters
    ----------
    df = pandas.Dataframe or string
        dataframe or csv-file to use for the resulting database


    """
    df = read_csv_if_string(df)
    df = clean_powerplantname(df)
    df = add_geoposition(df)
    datadedup_path = '/tmp/DataDedup.csv'
    df.to_csv(datadedup_path, index_label='id', encoding='utf-8')
    duplicates_path = '/tmp/duplicates.txt'
    duke(os.path.join(os.path.dirname(__file__),'data','Deleteduplicates.xml'),
                      linkfile=duplicates_path)
    df = cliques(df, duplicates_path)
    os.remove(datadedup_path)
    os.remove(duplicates_path)
    df = df.groupby('grouped').apply(prop_for_groups)
    df.reset_index(drop=True, inplace=True)
    df = add_geoposition(df)
    df = df[target_columns()]
    return df

def compare_two_datasets(datasets, labels):
    """
    Duke-based horizontal match of two databases. Returns the matched dataframe including only the
    matched entries in a multi-indexed pandas.Dataframe. Compares all properties of the
    given columns ['Name','Fueltype', 'Classification', 'Country', 'Capacity','Geoposition'] in order
    to determine the same powerplant in different two datasets. The match is in one-to-one mode,
    that is every entry of the initial databases has maximally one link in order to obtain
    unique entries in the resulting dataframe.
    Attention: When abording this command, the duke process will still continue in the background,
    wait until the process is finished before restarting.

    Parameters
    ----------
    datasets : list of pandas.Dataframe or strings
        dataframes or csv-files to use for the matching
    labels : list of strings
        Names of the databases for the resulting dataframe


    """
    datasets = list(map(read_csv_if_string, datasets))
    datasets = list(map(add_geoposition, datasets))
    datasets[0].to_csv('/tmp/Data_to_Match1.csv', encoding='utf-8', index_label='id')
    datasets[1].to_csv('/tmp/Data_to_Match2.csv', encoding='utf-8', index_label='id')
    duke(os.path.join(os.path.dirname(__file__),'data','Comparison.xml'),
            linkfile='/tmp/matches.txt', singlematch=True)
    matches = best_matches('/tmp/matches.txt', labels)
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


def match_multiple_datasets(datasets, labels):
    """
    Duke-based horizontal match of multiple databases. Returns the matched dataframe including
    only the matched entries in a multi-indexed pandas.Dataframe. Compares all properties of the
    given columns ['Name','Fueltype', 'Classification', 'Country', 'Capacity','Geoposition'] in order
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
        match = compare_two_datasets([datasets[c], datasets[d]], 
                                   [labels[c], labels[d]])
        all_matches.append(match)
    matches = cross_matches(all_matches, labels=labels)
    matched = combined_dataframe(matches, datasets)
    return matched
        

def reduce_matched_dataframe(df):
    """
    Returns a new dataframe with all names of the powerplants, but the Capacity on average
    as well as longitude and lattitude. In the Country, Fueltype and Classification 
    column the most common value is returned.

    Parameters
    ----------
    df : pandas.Dataframe
        MultiIndex dataframe with the matched powerplants, as obatained from
        combined_dataframe() or match_multiple_datasets()
    """
    
    def concat_strings(df):
        if df.isnull().all():
            return np.nan
        else:
            return df[df.notnull()].str.cat(sep = ', ')

    sdf = df.Name
    sdf.loc[:, 'Country'] = df.Country.apply(most_frequent, axis=1)
    sdf.loc[:, 'Fueltype'] = df.Fueltype.apply(most_frequent, axis=1)
    sdf.loc[:, 'Classification'] = df.Classification.apply(concat_strings, axis=1)
    if 'Geo' in df.Name:
        sdf.loc[df.Name.Geo.notnull(), 'Capacity'] = df[df.Name.Geo.notnull()].Capacity.Geo
        sdf.loc[df.Name.Geo.isnull(), 'Capacity'] = df[df.Name.Geo.isnull()].Capacity.max(axis=1)
    else:
        sdf.loc[:, 'Capacity'] = df.Capacity.max(axis=1)
    sdf.loc[:, 'lat'] = df.lat.mean(axis=1)
    sdf.loc[:, 'lon'] = df.lon.mean(axis=1)
    sdf.loc[:, 'File'] = df.File.apply(concat_strings, axis=1)
    sdf.loc[np.logical_not(sdf.Classification.str.contains('OCGT|CCGT|CHP')),'Classification'] \
            = sdf.loc[np.logical_not(sdf.Classification.str.contains('OCGT|CCGT|CHP')),
            'Classification'].str.title()
    return sdf

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
    return df.append(extend_by[~extend_by.loc[:, label].
                             isin(df.loc[:,label])]).reset_index(drop=True)
    
    

        



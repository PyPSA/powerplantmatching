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
Collection of power plant data bases and statistical data
"""

from __future__ import print_function, absolute_import

import os
import numpy as np
import pandas as pd
from countrycode import countrycode

from .cleaning import clean_single
from .config import europeancountries, target_columns, target_fueltypes
from .cleaning import gather_fueltype_info, gather_set_info, gather_technology_info, \
                        clean_powerplantname, clean_technology
import requests
import xml.etree.ElementTree as ET
import re
from powerplantmatching import cleaning
from . import utils
from .utils import parse_Geoposition


def pass_datasetID_as_metadata(df, ID):
    for i in df._metadata:
        for i in df._metadata:
            df._metadata.remove(i)
    df._metadata.append(ID)



def OPSD(rawEU=False, rawDE=False):
    """
    Return standardized OPSD (Open Power Systems Data) database with target column names and fueltypes.

    """
    opsd_EU = pd.read_csv('%s/data/conventional_power_plants_EU.csv'%
                       os.path.dirname(__file__), encoding='utf-8')
    opsd_DE = pd.read_csv('%s/data/conventional_power_plants_DE.csv'%
                       os.path.dirname(__file__), encoding='utf-8')
    if rawEU and rawDE:
        raise(NotImplementedError('''
                It is not possible to show both DE and EU raw databases at the 
                same time as they have different formats. Choose only one!
                '''))
    if rawEU:
        return opsd_EU
    if rawDE:
        return opsd_DE
    opsd_EU.columns = opsd_EU.columns.str.title()
    opsd_EU.rename(columns={'Lat':'lat',
                            'Lon':'lon',
                            'Energy_Source':'Fueltype',
                            'Commissioned':'YearCommissioned',
                            'Source':'File'
                            }, inplace=True)    
    opsd_EU.loc[:,'projectID'] = 'OEU' + opsd_EU.index.astype(str)
    opsd_EU = opsd_EU.loc[:,target_columns()]
    opsd_DE.columns = opsd_DE.columns.str.title()
    # If BNetzA-Name is empty replace by company, if this is empty by city.
    opsd_DE.Name_Bnetza.fillna(opsd_DE.Company, inplace=True)
    opsd_DE.Name_Bnetza.fillna(opsd_DE.City, inplace=True)    
    opsd_DE.rename(columns={'Lat':'lat',
                            'Lon':'lon',
                            'Name_Bnetza':'Name',
                            'Energy_Source_Level_2':'Fueltype',
                            'Type':'Set',
                            'Country_Code':'Country',
                            'Capacity_Net_Bnetza':'Capacity',
                            'Commissioned':'YearCommissioned',
                            'Source':'File'
                            }, inplace=True)
    opsd_DE.loc[:,'projectID'] = opsd_DE.Id
    opsd_DE = opsd_DE.loc[:,target_columns()]
    opsd = pd.concat([opsd_EU, opsd_DE]).reset_index(drop=True)
    opsd.lat.replace(' ', np.nan, inplace=True, regex=True)
    d = {'Natural gas': 'Natural Gas',
         'Biomass and biogas': 'Bioenergy',
         'Other or unspecified energy sources': 'Other',
         'Other fossil fuels': 'Other'}
    opsd.Fueltype = opsd.Fueltype.replace(d).str.title()
    opsd = gather_technology_info(opsd)
    opsd = gather_set_info(opsd)    
    opsd = clean_technology(opsd)
    opsd.Country = countrycode(codes=opsd.Country.tolist(),
                               target='country_name', origin='iso2c')
    opsd.Name = opsd.Name.str.title()
    opsd.Country = opsd.Country.str.title()
    pass_datasetID_as_metadata(opsd, 'OPSD')
    return opsd


def GEO(raw=False):
    """
    Return standardized GEO database with target column names and fueltypes.

    """
    def read_globalenergyobservatory():
        import pandas as pd
        import sqlite3

        db = sqlite3.connect('%s/data/global_energy_observatory_power_plants.sqlite'%
                       os.path.dirname(__file__))
        
        """
        f.gotzens@fz-juelich.de: Could anyone please check if Year_rng2_yr1 is
        the correct column for commissioning / grid synchronization year?!
        """
        
        cur = db.execute(
        "select"
        "   name, type, Type_of_Plant_rng1 , Type_of_Fuel_rng1_Primary, "
        "   Type_of_Fuel_rng2_Secondary, country, design_capacity_mwe_nbr, "
        "   CAST(longitude_start AS REAL) as lon,"
        "   CAST(latitude_start AS REAL) as lat "
        "from"
        "   powerplants "
        "where"
        "   lat between 33 and 71 and"
        "   lon between -12 and 41 and"
        "   status_of_plant_itf=='Operating Fully' and"
        "   design_capacity_mwe_nbr > 0"
        )

        return pd.DataFrame(cur.fetchall(), columns=["Name", "Fueltype","Technology",
        "FuelClassification1","FuelClassification2", "Country", "Capacity", 
        "lon", "lat"])
    GEOdata = read_globalenergyobservatory()
    if raw:
        return GEOdata
    GEOdata.loc[:,'projectID'] = 'G' + GEOdata.index.astype(str)
    GEOdata = GEOdata[GEOdata['Country'].isin(europeancountries())]
    GEOdata.drop_duplicates(subset=GEOdata.columns.drop(['projectID']), inplace=True)
    GEOdata.replace({'Gas': 'Natural Gas'}, inplace=True)    
    #coaltypes = pd.read_csv('%s/coal-types.csv'%
    #                   os.path.dirname(__file__), sep=',')        
    GEOdata = gather_fueltype_info(GEOdata, search_col=['FuelClassification1'])    
    GEOdata = gather_technology_info(GEOdata, search_col=['FuelClassification1'])
    GEOdata = gather_set_info(GEOdata)
    GEOdata = clean_powerplantname(GEOdata)
#    GEOdata.Technology = GEOdata.Technology.replace({
#       'Combined Cycle Gas Turbine':'CCGT',
#       'Cogeneration Power and Heat Steam Turbine':'Steam Turbine',
#       'Sub-critical Thermal|Super-critical Thermal':'Steam Turbine',
#       'Open Cycle Gas Turbine|Power and Heat OCGT':'OCGT',
#       'Combined Cycle Gas Engine (CCGE)':'CCGT',
#       'Power and Heat Combined Cycle Gas Turbine':'CCGT',
#       'Both Sub and Super Critical Thermal|Ultra-Super-Critical Thermal':'Steam Turbine',
#       'Cogeneration Power and Heat Steam Turbine':'Steam Turbine',
#       'Heat and Power Steam Turbine|Sub-critical Steam Turbine':'Steam Turbine'}
#            , regex=True).str.strip()
    GEOdata = clean_technology(GEOdata, generalize_hydros=True)
    pass_datasetID_as_metadata(GEOdata, 'GEO')
    return GEOdata.loc[:,target_columns()]


def CARMA(raw=False):
    """
    Return standardized Carma database with target column names and fueltypes.
    Only includes powerplants with capacity > 4 MW.
    """
    carmadata = pd.read_csv('%s/data/Full_CARMA_2009_Dataset_1.csv'\
    %os.path.dirname(__file__), encoding='utf-8')
    if raw:
        return carmadata
    d = {'COAL': 'Hard Coal',
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
         'BLIQ': 'Bioenergy',
         'BGAS': 'Bioenergy',
         'BLIQ': 'Bioenergy',
         'OTH': 'Other'}
    rename = {'Geoposition': 'Geoposition',
     'cap': 'Capacity',
     'city': 'location',
     'country': 'Country',
     'fuel1': 'Fueltype',
     'lat': 'lat',
     'lon': 'lon',
     'plant': 'Name', 
     'plant.id':'projectID'}
    carmadata = carmadata.rename(columns=rename).loc[:,target_columns()+['chp']]
    carmadata = carmadata[carmadata.Capacity > 3]
    carmadata = carmadata[carmadata.Country.isin(europeancountries())]
    carmadata = gather_technology_info(carmadata)
    carmadata = gather_set_info(carmadata)
    carmadata = clean_technology(carmadata)
    carmadata.drop_duplicates(inplace=True)
    carmadata = carmadata.replace(d)
    carmadata = clean_powerplantname(carmadata)
    pass_datasetID_as_metadata(carmadata, 'CARMA')
    return carmadata[target_columns()]

def Oldenburgdata():
    """
    This data is not yet available.
    """
    return pd.read_csv('%s/data/OldenburgHydro.csv'%os.path.dirname(__file__),
                       encoding='utf-8', index_col='id')[target_columns()]

def ENTSOE_stats(raw=False):
    """
    Standardize the entsoe database for statistical use.
    """
    opsd = pd.read_csv('%s/data/aggregated_capacity.csv'%os.path.dirname(__file__),
                       encoding='utf-8')
    if raw:
        return opsd
    entsoedata = opsd[opsd['source'].isin(['entsoe']) & opsd['year'].isin([2014])]
    cCodes = list(entsoedata.country)
    countries = countrycode(codes=cCodes, target='country_name', origin='iso2c')
    entsoedata = entsoedata.replace({'Bioenergy and other renewable fuels': 'Bioenergy',
     'Coal derivatives': 'Coal',
     'Differently categorized fossil fuels': 'Other',
     'Hard coal': 'Hard Coal',
     'Lignite': 'Lignite',
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

def WRI(reduced_data=True):
    wri = pd.read_csv('%s/data/WRIdata.csv'%os.path.dirname(__file__),
                      encoding='utf-8', index_col='id')
#    wri.Name = wri.Name.str.title()
    wri.loc[:,'projectID'] = wri.index.values
    wri = gather_set_info(wri)
    if reduced_data:
        #wri data consists of ENTSOE data and OPSD, drop those:
        wri = wri.loc[~wri.File.str.contains('ENTSOE', case=False)]
        wri = wri.loc[~wri.Country.isin(['Germany','Poland', 'France', 'Switzerland'])]
    pass_datasetID_as_metadata(wri, 'WRI')
    return wri.loc[:,target_columns()]


def ESE(update=False, path=None):
    """
    This database is not given within the repository because of open source rights.
    Just download the database from the link given in the README file
    (last section: Data Sources) and set the arguments of this function to update=True and
    path='path/to/database/projects.xls'. This will integrate the database into your
    local powerplantmatching/data and can then be used as the other databases.

    Parameters
    ----------
    update : Boolean, Default False
        Wether to update the database according to the database given in path
    path : str
        location of the downloaded projects.xls file

    """
    saved_version = '%s/data/energy_storage_exchange.csv'%os.path.dirname(__file__)
    if (not os.path.exists(saved_version)) and (update is False) and (path is None):
        raise(NotImplementedError( '''
        This database is not yet in your local repository.
        Just download the database from the link given in the README file (last section:
        Data Sources, you might change the format of the Longitude and 'Commissioning Date'
         column to number format since there seems to be a problem with the date format)
        and set the arguments of this function to update=True and
        path='path/to/database/projects.xls'. This will integrate the database
        into your local powerplantmatching/data and can then be used as
        the other databases.
        '''))
    if os.path.exists(saved_version) and (update is False) :
        return pd.read_csv(saved_version, index_col='id')
    if path is None:
        raise(ValueError('No path defined for update'))
    if not os.path.exists(path):
        raise(ValueError('The given path does not exist'))
    data = pd.read_excel(path, encoding='utf-8')
    data.loc[:,'Name'] = data.loc[:,'Project Name']
    data.loc[:,'Technology'] = data.loc[:,'Technology Type']
    data.loc[:,'lon'] = data.Longitude
    data.loc[:,'lat'] = data.Latitude
    data.loc[:,'Capacity'] = data.loc[:,'Rated Power in kW']/1000
    data.loc[:,'projectID'] = data.index.values
    # The following lambda expression doesn't work for some reason, as 
    #'np.where(type(x) is int...' doesn't filter the integers - why???
    #         data.loc[:,'Commissioning Date'].apply(lambda x: np.where(type(x) 
#               is int, np.floor(float(str(x))/365.25+1900), x))
    # That's why I wrote this (probably inefficient) workaround:
    A = []
    for x in data.loc[:,'Commissioning Date']:
        if type(x) == pd.tslib.Timestamp:
            A.append(x.year)
        elif type(x) == int:
            # As Excel dates are a daily-incremented number starting on 01.01.1900
            A.append(np.floor(float(x)/365.25+1900))
        elif type(x) == pd.tslib.NaTType:
            A.append(np.NaN)
        else:
            print(x)
            A.append(np.NaN)
    data.loc[:,'YearCommissioned'] = A
    
    data.loc[(data.Technology.str.contains('Pumped'))&(data.Technology.
         notnull()), 'Technology'] = 'Pumped storage'
    data = data.loc[data.Technology == 'Pumped storage', target_columns()]
    data.Fueltype = 'Hydro'
    data = data.reset_index(drop = True)
    data = clean_single(data)
    data.File = 'energy_storage_exchange'
    data.to_csv(saved_version, index_label='id', encoding='utf-8')
    pass_datasetID_as_metadata(data, 'ESE')
    return data

    
def ENTSOE(update=False, raw=False):
    """
    Returns the list of installed generators provided by the ENTSO-E
    Trasparency Project. Geographical information is not given.
    If update=True, the dataset is parsed through a request to 
    'https://transparency.entsoe.eu/generation/r2/installedCapacityPerProductionUnit/show',
    Internet connection requiered. If raw=True, the same request is done, but
    the unprocessed data is returned.    

    Parameters
    ----------
    update : Boolean, Default False
        Wether to update the database through a request to the ENTSO-E transparency
        plattform
    raw : Boolean, Default False
        Wether to return the raw data, obtained from the request to 
        the ENTSO-E transparency plattform
    """
    if update or raw:
        Domains = pd.read_csv('/home/fabian/Desktop/EuropeanPowerGrid/ENTSO-E/areamap',
                            sep=';', header=None)
        def full_country_name(l):
            return [country.title() for country in filter(None, 
                                                countrycode(l, origin='iso2c', 
                                                    target='country_name'))]        
        pattern = '|'.join(('(?i)'+x) for x in europeancountries())            
        found = Domains.loc[:,1].str.findall(pattern).str.join(sep=', ')
        Domains.loc[:, 'Country'] = found
        found = Domains[1].replace('[0-9]', '', regex=True).str.split(' |,|\+|\-')\
                    .apply(full_country_name).str.join(sep=', ').str.findall(pattern)\
                    .str.join(sep=', ').str.strip()
        Domains.Country = (Domains.loc[:, 'Country'].fillna('')
                                    .str.cat(found.fillna(''), sep=', ').str.strip())
        Domains.Country = Domains.Country.str.replace('^ , |^,|, $|,$', '')
        Domains.Country.replace('', np.NaN, regex=True, inplace=True)      
        Domains.Country = Domains.loc[Domains.Country.notnull(), 'Country']\
                                      .str.strip().apply(lambda x:
                             ', '.join(list(set(x.split(', ')))) )            
        fdict= {'A03': 'Mixed',
          'A04': 'Generation',
          'A05': 'Load',
          'B01': 'Biomass',
          'B02': 'Lignite',              #'Fossil Brown coal/Lignite',
          'B03': 'Fossil Coal-derived gas',
          'B04': 'Fossil Gas',
          'B05': 'Fossil Hard coal',
          'B06': 'Fossil Oil',
          'B07': 'Fossil Oil shale',
          'B08': 'Fossil Peat',
          'B09': 'Geothermal',
          'B10': 'Hydro Pumped Storage',
          'B11': 'Hydro Run-of-river and poundage',
          'B12': 'Hydro Water Reservoir',
          'B13': 'Marine',
          'B14': 'Nuclear',
          'B15': 'Other renewable',
          'B16': 'Solar',
          'B17': 'Waste',
          'B18': 'Wind Offshore',
          'B19': 'Wind Onshore',
          'B20': 'Other'}  
        level1 = ['registeredResource.name', 'registeredResource.mRID']
        level2 = ['voltage_PowerSystemResources.highVoltageLimit','psrType']
        level3 = ['quantity']
        entsoe = pd.DataFrame(columns=level1+level2+level3+['Country'])
        def namespace(element):
            m = re.match('\{.*\}', element.tag)
            return m.group(0) if m else ''
        def attribute(etree_sel):
            return etree_sel.text
        for i in Domains.index:        
            ret = requests.get('https://transparency.entsoe.eu/api',
                params=dict(securityToken='42efae2a-7325-4952-9756-174713499e83',
                documentType='A71', processType='A33',
                In_Domain=Domains.loc[i,0],
                periodStart='201512312300', periodEnd='201612312300'))
            etree = ET.fromstring(ret.content) #create an ElementTree object 
            ns = namespace(etree)
            df = pd.DataFrame(columns=level1+level2+level3+['Country'])
            for arg in level1:
                df.loc[:,arg] = map(attribute , etree.findall('*/%s%s'%(ns, arg)))
            for arg in level2:
                df.loc[:,arg] = map(attribute , etree.findall('*/*/%s%s'%(ns, arg)))
            for arg in level3:
                df.loc[:,arg] = map(attribute , etree.findall('*/*/*/%s%s'%(ns,arg)))
            df.loc[:,'Country'] = Domains.loc[i,'Country']
            entsoe = pd.concat([entsoe,df],ignore_index=True)
        if raw:
            return entsoe
        entsoe.psrType = entsoe.psrType.map(fdict)
        entsoe.columns = ['Name', 'projectID', 'High Volage Limit', 'Fueltype',
                              'Capacity', 'Country']
        entsoe.loc[:,'Name'] = entsoe.loc[:,'Name'].str.title()
        entsoe = entsoe.loc[entsoe.Country.notnull()]
        entsoe = entsoe.loc[~((entsoe.projectID.duplicated(keep=False))&
                   (~entsoe.Country.isin(europeancountries())))]
        entsoe = entsoe.drop_duplicates('projectID').reset_index(drop=True)
        entsoe.loc[:,'File'] = '''https://transparency.entsoe.eu/generation/r2/
                                  installedCapacityPerProductionUnit/show'''
        entsoe = entsoe.loc[:,target_columns()] 
        entsoe = cleaning.gather_technology_info(entsoe)
        entsoe = cleaning.gather_set_info(entsoe)
        entsoe.Fueltype.replace(to_replace=['.*Hydro.*','Fossil Gas', '.*(?i)coal.*','.*Peat', 
                   'Marine', 'Wind.*', '.*Oil.*', 'Biomass'], value=['Hydro','Natural Gas', 
                   'Hard Coal', 'Lignite', 'Other', 'Wind', 'Oil', 'Bioenergy'] ,
                                regex=True, inplace=True)
        entsoe.Capacity = pd.to_numeric(entsoe.Capacity)
        entsoe.loc[entsoe.Country=='Austria, Germany, Luxembourg', 'Country'] = \
                  [utils.parse_Geoposition(powerplant , return_Country=True) for 
                   powerplant in entsoe.loc[entsoe.Country=='Austria, Germany, Luxembourg', 'Name']]        
        entsoe.Country.replace(to_replace=['Deutschland','.*sterreich' ,
                           'L.*tzebuerg'], value=['Germany','Austria',
                        'Luxembourg'],regex=True, inplace=True)
        entsoe = entsoe.loc[entsoe.Country.isin(europeancountries()+[None])]
        entsoe.Country = entsoe.Country.astype(str)
        entsoe.loc[entsoe.Country=='None', 'Country'] = np.NaN
        entsoe.to_csv('%s/data/entsoe_powerplants.csv'%os.path.dirname(__file__), 
                       index_label='id', encoding='utf-8')
        entsoe.datasetID = 'ENTSOE'
        return entsoe
    else:
        entsoe = pd.read_csv('%s/data/entsoe_powerplants.csv'%os.path.dirname(__file__), 
                       index_col='id', encoding='utf-8')
        pass_datasetID_as_metadata(entsoe, 'ENTSOE')
        return entsoe
        

    
def WEPP(raw=False, parseGeoLoc=False):
    """
    Return standardized WEPP (Platts, World Elecrtric Power Plants Database) 
    database with target column names and fueltypes.

    """
    # Define the appropriate datatype for each column (some columns e.g. 'YEAR' cannot
    # be integers, as there are N/A values, which np.int does not yet(!) support.)
    datatypes = {'UNIT':str,'PLANT':str,'COMPANY':str,'MW':np.float32,'STATUS':str,
                 'YEAR':np.float32,'UTYPE':str,'FUEL':str,'FUELTYPE':str,'ALTFUEL':str,
                 'SSSMFR':str,'BOILTYPE':str,'TURBMFR':str,'TURBTYPE':str,'GENMFR':str,
                 'GENTYPE':str,'SFLOW':np.float32,'SPRESS':np.float32,'STYPE':str,
                 'STEMP':np.float32,'REHEAT1':np.float32,'REHEAT2':np.float32,
                 'PARTCTL':str,'PARTMFR':str,'SO2CTL':str,'FGDMFR':str,'NOXCTL':str,
                 'NOXMFR':str,'AE':str,'CONstr,UCT':str,'COOL':str,'RETIRE':np.float32,
                 'CITY':str,'STATE':str,'COUNTRY':str,'AREA':str,'SUBREGION':str,
                 'POSTCODE':str,'PARENT':str,'ELECTYPE':str,'BUSTYPE':str,
                 'COMPID':np.int32,'LOCATIONID':np.int32,'UNITID':np.int32,
                }
    # Now read the Platts WEPP Database
    filename = 'platts_wepp.csv'
    wepp = pd.read_csv('%s/data/%s' % (os.path.dirname(__file__),filename),
                       encoding='utf-8', dtype=datatypes)
    
    if raw:
        return wepp
        
    # Try to parse lat-lon geo coordinates of each unit     
    if parseGeoLoc:
        for index, row in wepp.iterrows():
            query = None
            while True:
                query = parse_Geoposition(row['UNIT'], row['COUNTRY'])      # 1st try
                if query != None: break
                query = parse_Geoposition(row['POSTCODE'], row['COUNTRY'])  # 2nd try
                if query != None: break
                query = parse_Geoposition(row['CITY'], row['COUNTRY'])      # 3rd try
                break
            if isinstance(query, tuple):    
                wepp.at[index, 'LAT'] = query[0] # write latitude
                wepp.at[index, 'LON'] = query[1] # write longitude
            
    # str.title(): Return a titlecased version of the string where words start
    # with an uppercase character and the remaining characters are lowercase.
    wepp.columns = wepp.columns.str.title()
    # Fit WEPP-column names to our specifications
    wepp.rename(columns={'Unit':'Name',
                         'Fuel':'Fueltype',
                         'Fueltype':'Technology',
                         'Mw':'Capacity',
                         'Year':'YearCommissioned',
                         'Lat':'lat',
                         'Lon':'lon'
                         }, inplace=True)
    # drop any columns we do not need
    wepp = wepp.loc[:,target_columns()]
    # drop any rows with countries we do not need
    wepp.Country = wepp.Country.str.title()
    #wepp = wepp[wepp.Country.isin(europeancountries())]
    # Set Technology infos, not working yet
    wepp = gather_technology_info(wepp, search_col=['Name'])    
    
    # Replace fueltypes
    #
    ### THIS NEEDS TO BE ENHANCED SOON!
    #
    d = {'WAT': 'Hydro',
         'GAS': 'Natural Gas',
         'BGAS': 'Waste',
         'Other or unspecified energy sources': 'Other'}
    wepp.Fueltype = wepp.Fueltype.replace(d)            
    # Done!    
    wepp.datasetID = 'WEPP'

    return wepp

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
from .cleaning import gather_classification_info, clean_powerplantname, clean_classification
import requests
import xml.etree.ElementTree as ET
import re
from powerplantmatching import cleaning
from . import utils




def OPSD(raw=False):
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
    opsd.loc[:,'projectID'] = opsd.index.values
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
#    opsd = add_geoposition(opsd)
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
        cur = db.execute(
        "select"
        "   name, type, Type_of_Plant_rng1 , Type_of_Fuel_rng1_Primary, "
        " Type_of_Fuel_rng2_Secondary ,country, design_capacity_mwe_nbr,"
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

        return pd.DataFrame(cur.fetchall(), columns=["Name", "Type","Classification",
        "FuelClassification1","FuelClassification2", "Country", "Capacity", "lon", "lat"])

    GEOdata = read_globalenergyobservatory()
    if raw:
        return GEOdata
    GEOdata.loc[:,'projectID'] = GEOdata.index.values
    GEOdata = GEOdata[GEOdata['Country'].isin(europeancountries())]
    GEOdata.drop_duplicates(subset=GEOdata.columns.drop(['projectID']), inplace=True)
    GEOdata.rename(columns={'Type': 'Fueltype'}, inplace=True)
    GEOdata.replace({'Gas': 'Natural Gas'}, inplace=True)
    GEOdata = gather_classification_info(GEOdata, search_col=['FuelClassification1'])
    GEOdata = clean_powerplantname(GEOdata)
#    GEOdata = (GEOdata.fillna(0)
#               .groupby(['Name', 'Country', 'Fueltype', 'Classification'])
#               .agg({'Capacity': np.sum,
#                     'lat': np.mean,
#                     'lon': np.mean}).reset_index())
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
    GEOdata = clean_classification(GEOdata, generalize_hydros=True)
    return GEOdata.loc[:,target_columns()]


def CARMA(raw=False):
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
     'plant': 'Name', 
     'plant.id':'projectID'}
    carmadata = carmadata.rename(columns=rename).loc[:,target_columns()]
    carmadata = carmadata[carmadata.Capacity > 3]
    carmadata = carmadata[carmadata.Country.isin(europeancountries())]
    carmadata = gather_classification_info(carmadata)
    carmadata.drop_duplicates(inplace=True)
    carmadata = carmadata.replace(d)
    carmadata = clean_powerplantname(carmadata)
    return carmadata

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

def WRI():
    wri = pd.read_csv('%s/data/WRIdata.csv'%os.path.dirname(__file__),
                      index_col='id')
#    wri.Name = wri.Name.str.title()
    wri.loc[:,'projectID'] = wri.index.values
    wri = wri.loc[:,target_columns()]
    return wri


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
        location of the downloaded .xls file

    """
    saved_version = '%s/data/energy_storage_exchange.csv'%os.path.dirname(__file__)
    if (not os.path.exists(saved_version)) and (update is False) and (path is None):
        raise(NotImplementedError( '''
        This database is not yet in your local repository.
        Just download the database from the link given in the README file (last section:
        Data Sources, you might change the format of the Longitude column to number format
        since there seems to be a problem with the date format)
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
    data.loc[:,'Classification'] = data.loc[:,'Technology Type']
    data.loc[:, 'lon'] = data.Longitude
    data.loc[:, 'lat'] = data.Latitude
    data.loc[:,'Capacity'] = data.loc[:,'Rated Power in kW']/1000
    data.loc[:,'projectID'] = data.index.values
    data.loc[(data.Classification.str.contains('Pumped'))&(data.Classification.
         notnull()), 'Classification'] = 'Pumped storage'
    data = data.loc[data.Classification == 'Pumped storage', target_columns()]
    data.Fueltype = 'Hydro'
    data = data.reset_index(drop = True)
    data = clean_single(data)
    data.File = 'energy_storage_exchange'
    data.to_csv(saved_version, index_label='id',
            encoding='utf-8')
    return data


def ENTSOE(update=False, raw=False, entsoe_token=None):
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
        Whether to update the database through a request to the ENTSO-E transparency
        plattform
    raw : Boolean, Default False
        Whether to return the raw data, obtained from the request to
        the ENTSO-E transparency platform
    entsoe_token: String
        Security token of the ENTSO-E Transparency platform

    Note: For obtaining a security token refer to section 2 of the
    RESTful API documentation of the ENTSOE-E Transparency platform
    https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_authentication_and_authorisation
    """
    if update or raw:
        assert entsoe_token is not None, "entsoe_token is missing"

        domains = pd.read_csv('%s/data/entsoe-areamap.csv' % os.path.dirname(__file__),
                              sep=';', header=None)
        def full_country_name(l):
            return [country.title()
                    for country in filter(None, countrycode(l, origin='iso2c',
                                                            target='country_name'))]
        pattern = '|'.join(('(?i)'+x) for x in europeancountries())
        found = domains.loc[:,1].str.findall(pattern).str.join(sep=', ')
        domains.loc[:, 'Country'] = found
        found = (domains[1].replace('[0-9]', '', regex=True).str.split(' |,|\+|\-')
                 .apply(full_country_name).str.join(sep=', ').str.findall(pattern)
                 .str.join(sep=', ').str.strip())
        domains.Country = (domains.loc[:, 'Country'].fillna('')
                           .str.cat(found.fillna(''), sep=', ')
                           .str.replace('^ ?, ?|, ?$', '').str.strip())
        domains.Country.replace('', np.NaN, inplace=True)
        domains.Country = (domains.loc[domains.Country.notnull(), 'Country']
                           .apply(lambda x: ', '.join(list(set(x.split(', '))))))
        fdict= {'A03': 'Mixed',
                'A04': 'Generation',
                'A05': 'Load',
                'B01': 'Biomass',
                'B02': 'Fossil Brown coal/Lignite',
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
        for i in domains.index:
            ret = requests.get('https://transparency.entsoe.eu/api',
                               params=dict(securityToken=entsoe_token,
                                           documentType='A71', processType='A33',
                                           In_Domain=domains.loc[i,0],
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
            df.loc[:,'Country'] = domains.loc[i,'Country']
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
        entsoe.loc[:,'File'] = "https://transparency.entsoe.eu/generation/r2/\ninstalledCapacityPerProductionUnit/show"
        entsoe = entsoe.loc[:,target_columns()]
        entsoe = cleaning.gather_classification_info(entsoe)
        entsoe.Fueltype.replace(to_replace=['.*Hydro.*','Fossil Gas', '(.*(?i)coal.*)|(.*Peat)',
                                            'Biomass|Marine', 'Wind.*', '.*Oil.*'],
                                value=['Hydro','Natural Gas', 'Coal', 'Other', 'Wind', 'Oil'],
                                regex=True, inplace=True)
        entsoe.Capacity = pd.to_numeric(entsoe.Capacity)
        entsoe.loc[entsoe.Country=='Austria, Germany, Luxembourg', 'Country'] = \
                  [utils.parse_Geoposition(powerplant , return_Country=True)
                   for powerplant in entsoe.loc[entsoe.Country=='Austria, Germany, Luxembourg', 'Name']]
        entsoe.Country.replace(to_replace=['Deutschland','.*sterreich' ,'L.*tzebuerg'],
                               value=['Germany','Austria', 'Luxembourg'],
                               regex=True, inplace=True)
        entsoe = entsoe.loc[entsoe.Country.isin(europeancountries()+[None])]
        entsoe.Country = entsoe.Country.astype(str)
        entsoe.loc[entsoe.Country=='None', 'Country'] = np.NaN
        entsoe.to_csv('%s/data/entsoe_powerplants.csv'%os.path.dirname(__file__),
                       index_label='id', encoding='utf-8')
        return entsoe
    else:
        return pd.read_csv('%s/data/entsoe_powerplants.csv'%os.path.dirname(__file__),
                       index_col='id', encoding='utf-8')

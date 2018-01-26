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
Collection of power plant data bases and statistical data
"""

from __future__ import print_function, absolute_import

import os
import xlrd
import numpy as np
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import re
import pycountry
import logging
from textwrap import dedent
logger = logging.getLogger(__name__)
from six import iteritems
from six.moves import reduce
from .config import europeancountries, target_columns, additional_data_config
from .cleaning import (gather_fueltype_info, gather_set_info,
                       gather_technology_info, clean_powerplantname,
                       clean_technology)
from .utils import (parse_Geoposition, _data, _data_in, _data_out)
from .heuristics import scale_to_net_capacities

net_caps = additional_data_config()['display_net_caps']
data_config = {}


def OPSD(rawEU=False, rawDE=False, statusDE=['operating']):
    """
    Return standardized OPSD (Open Power Systems Data) database with target column
    names and fueltypes.

    Parameters
    ----------

    rawEU : Boolean, default False
        Whether to return the raw EU (=non-DE) database.
    rawDE : Boolean, default False
        Whether to return the raw DE database.
    statusDE : list
        Filter DE entries by operational status ['operating', 'shutdown', 'reserve', etc.]
    """

    opsd_EU = pd.read_csv(_data_in('conventional_power_plants_EU.csv'), na_values=' ', encoding='utf-8')
    opsd_DE = pd.read_csv(_data_in('conventional_power_plants_DE.csv'), na_values=' ', encoding='utf-8')
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
                            'Source':'File'},
                   inplace=True)
    opsd_EU.loc[:,'projectID'] = 'OEU' + opsd_EU.index.astype(str)
    opsd_EU = opsd_EU.reindex(columns=target_columns())
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
                            'Source':'File'},
                   inplace=True)
    opsd_DE['Fueltype'].fillna(opsd_DE['Energy_Source_Level_1'], inplace=True)
    opsd_DE['projectID'] = opsd_DE['Id']
    if statusDE is not None:
        opsd_DE = opsd_DE.loc[opsd_DE.Status.isin(statusDE)]
    opsd_DE = opsd_DE.reindex(columns=target_columns())
    return (pd.concat([opsd_EU, opsd_DE]).reset_index(drop=True)
            .replace(dict(Fueltype={'Biomass and biogas': 'Bioenergy',
                                    'Fossil fuels': 'Other',
                                    'Mixed fossil fuels': 'Other',
                                    'Natural gas': 'Natural Gas',
                                    'Non-renewable waste': 'Waste',
                                    'Other bioenergy and renewable waste': 'Bioenergy',
                                    'Other or unspecified energy sources': 'Other',
                                    'Other fossil fuels': 'Other',
                                    'Other fuels': 'Other'}))
            .replace({'Country': {'UK': u'GB','[ \t]+|[ \t]+$.':''}}, regex=True) #UK->GB, strip whitespace
            .assign(Name=lambda df: df.Name.str.title(),
                    Fueltype=lambda df: df.Fueltype.str.title(),
                    Country=lambda df: (pd.Series(df.Country.apply(
                                        lambda c: pycountry.countries.get(alpha_2=c).name),
                                        index=df.index).str.title()))
            .pipe(gather_technology_info)
            .pipe(gather_set_info)
            .pipe(clean_technology)
            .loc[lambda df: df.Country.isin(europeancountries())]
            .pipe(scale_to_net_capacities,
                  (not data_config['OPSD']['net_capacity'])))

data_config['OPSD'] = {'read_function': OPSD, 'reliability_score':5,
                       'net_capacity':True}


def GEO(raw=False):
    """
    Return standardized GEO database with target column names and fueltypes.

    """
    def read_globalenergyobservatory():
        import pandas as pd
        import sqlite3

        db = sqlite3.connect(_data_in('global_energy_observatory_power_plants.sqlite'))

        # f.gotzens@fz-juelich.de: Could anyone please check if Year_rng2_yr1 is
        # the correct column for commissioning / grid synchronization year?!

        cur = db.execute(
        "select"
        "   GEO_Assigned_Identification_Number, "
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

        return pd.DataFrame(cur.fetchall(),
                            columns=["projectID", "Name", "Fueltype","Technology",
                                     "FuelClassification1","FuelClassification2",
                                     "Country", "Capacity", "lon", "lat"])
    GEOdata = read_globalenergyobservatory()
    if raw:
        return GEOdata
    return (GEOdata
            .loc[lambda df: df.Country.isin(europeancountries())]
            .replace({col: {'Gas': 'Natural Gas'}
                      for col in {'Fueltype', 'FuelClassification1', 'FuelClassification2'}})
            .pipe(gather_fueltype_info, search_col=['FuelClassification1'])
            .pipe(gather_technology_info, search_col=['FuelClassification1'])
            .pipe(gather_set_info)
            .pipe(clean_powerplantname)
            .pipe(clean_technology, generalize_hydros=True)
            .reindex(columns=target_columns())
            .pipe(scale_to_net_capacities,
                  (not data_config['GEO']['net_capacity'])))

data_config['GEO'] = {'read_function': GEO,
                      'clean_single_kwargs': dict(aggregate_powerplant_units=False),
                      'reliability_score':3, 'net_capacity':False}


def CARMA(raw=False):
    """
    Return standardized Carma database with target column names and fueltypes.
    Only includes powerplants with capacity > 4 MW.
    """
    carmadata = pd.read_csv(_data_in('Full_CARMA_2009_Dataset_1.csv'),
                            encoding='utf-8', low_memory=False)
    if raw:
        return carmadata
    return (carmadata
            .rename(columns={'Geoposition': 'Geoposition',
                             'cap': 'Capacity',
                             'city': 'location',
                             'country': 'Country',
                             'fuel1': 'Fueltype',
                             'lat': 'lat',
                             'lon': 'lon',
                             'plant': 'Name',
                             'plant.id':'projectID'})
            .loc[lambda df: df.Capacity > 3]
            .loc[lambda df: df.Country.isin(europeancountries())]
            .replace(dict(Fueltype={'COAL': 'Hard Coal',
                                    'WAT': 'Hydro',
                                    'FGAS': 'Natural Gas',
                                    'NUC': 'Nuclear',
                                    'FLIQ': 'Oil',
                                    'WIND': 'Wind',
                                    'EMIT': 'Other',
                                    'GEO': 'Geothermal',
                                    'WSTH': 'Waste',
                                    'SUN': 'Solar',
                                    'BLIQ': 'Bioenergy',
                                    'BGAS': 'Bioenergy',
                                    'BSOL': 'Bioenergy',
                                    'OTH': 'Other'}))
            .pipe(clean_powerplantname)
            .pipe(gather_technology_info)
            .pipe(gather_set_info)
            .pipe(clean_technology)
            .drop_duplicates()
            .reindex(columns=target_columns())
            .pipe(scale_to_net_capacities,
                  (not data_config['CARMA']['net_capacity'])))

data_config['CARMA'] = {'read_function': CARMA,
                        'clean_single_kwargs': dict(aggregate_powerplant_units=False),
                        'reliability_score':1, 'net_capacity':False}


def IWPDCY():
     """
     This data is not yet available. Was extracted manually from the 'International
     Water Power & Dam Country Yearbook'.
     """
     fn = 'IWPDCY.csv'
     IWPDCY = (pd.read_csv(_data_in(fn), encoding='utf-8', index_col='id')
                 .reindex(columns=target_columns())
                 .pipe(gather_set_info))
     IWPDCY.File = fn
     IWPDCY.projectID = 'IWPDCY' + IWPDCY.index.astype(str)
     return IWPDCY

data_config['IWPDCY'] = {'read_function': IWPDCY,
           'clean_single_kwargs': dict(aggregate_powerplant_units=False),
           'reliability_score':3}



def Capacity_stats(raw=False, level=2, **selectors):
    """
    Standardize the entsoe database for statistical use.

    Parameters
    ----------
    year : int
        Year of the data (range usually 2013-2017)
        (defaults to 2016)
    source : str
        Which statistics source from
        {'entsoe SO&AF', 'entsoe Statistics', 'EUROSTAT', ...}
        (defaults to 'entsoe SO&AF')

    Returns
    -------
    df : pd.DataFrame
         Capacity statistics per country and fuel-type
    """
    opsd_aggregated = pd.read_csv(_data_in('national_generation_capacity_stacked.csv'),
                                  encoding='utf-8', index_col=0)

    selectors.setdefault('year', 2016)
    selectors.setdefault('source', 'entsoe SO&AF')

    if raw:
        return opsd_aggregated
    entsoedata = (opsd_aggregated
            [lambda df: reduce(lambda x, y: x&y,
                (df[k] == v
                    for k, v in iteritems(selectors)
                    if v is not None),
                    df['energy_source_level_%d' % level])]
            .assign(country=lambda df: (pd.Series(df.country.apply(
                lambda c: pycountry.countries.get(alpha_2=c).name),
                index=df.index).str.title()))
            .replace(dict(country={'Czechia':'Czech Republic'})) #due to pycountry
            .loc[lambda df: df.country.isin(europeancountries())]
            .rename(columns={'technology': 'Fueltype'})
            .replace(dict(Fueltype={'Bioenergy and other renewable fuels': 'Bioenergy',
                                    'Bioenergy and renewable waste': 'Waste',
                                    'Coal derivatives': 'Hard Coal',
                                    'Differently categorized fossil fuels': 'Other',
                                    'Differently categorized renewable energy sources': 'Other',
                                    'Hard coal': 'Hard Coal',
                                    'Mixed fossil fuels': 'Mixed fuel types',
                                    'Natural gas': 'Natural Gas',
                                    'Other or unspecified energy sources': 'Other',
                                    'Tide, wave, and ocean': 'Other'})))
    entsoedata.columns = entsoedata.columns.str.title()
    return entsoedata


def WRI(reduced_data=True):
    wri = pd.read_csv(_data_in('WRIdata.csv'),
                      encoding='utf-8', index_col='id')
    wri['projectID'] = wri.index
    wri = (wri[wri.Country.isin(europeancountries())]
           .replace(dict(Fueltype={'Coal':'Hard Coal'}))
           .pipe(gather_set_info))

    if reduced_data:
        #wri data consists of ENTSOE data and OPSD, drop those:
        wri = wri.loc[~wri.File.str.contains('ENTSOE', case=False)]
        wri = wri.loc[~wri.Country.isin(['Germany','Poland', 'France', 'Switzerland'])]
    return wri.reindex(columns=target_columns())

data_config['WRI'] = {'read_function': WRI,
                      'clean_single_kwargs': dict(aggregate_powerplant_units=False),
                      'reliability_score':2}


def ESE(update=False, path=None, raw=False):
    """
    This database is not given within the repository because of its restrictive license.
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
    saved_version = _data_in('energy_storage_exchange.csv')
    if os.path.exists(saved_version) and (update is False) :
        return pd.read_csv(saved_version, index_col='id', encoding='utf-8')

    if path is None:
        path = additional_data_config()['ese_path']

    assert os.path.exists(path), dedent('''
        The ESE database has not been cached in your local repository, yet (or
        you requested an update). Due to copyright issues it cannot be
        downloaded automatically. Get it by clicking 'Export Data XLS' on
        https://goo.gl/gVMwKJ and set ese_path in your config.csv to its full
        path. We couldn't find it at '{}' just now.
    ''').format(path)

    # Work-around to rewrite the longitude cell types from dates to
    # numbers, would also work the other way around, if there were
    # numbers in a date column
    book = xlrd.open_workbook(path)
    sheet = book.sheets()[0]
    col_longitude = sheet.row_values(0).index('Longitude')
    # col_date = sheet.row_values(0).index('Commissioning Date')
    for row in sheet._cell_types:
        if row[col_longitude] == 3:
            row[col_longitude] = 2
        # if row[col_date] == 2:
        #     row[col_date] = 3

    data = pd.read_excel(book, na_values=u'n/a', engine='xlrd')
    if raw:
        return data
    data = (data
            .rename(columns={'Project Name': 'Name',
                             'Technology Type': 'Technology',
                             'Longitude': 'lon',
                             'Latitude': 'lat'})
            .assign(Set='PP',
                    Fueltype='Hydro',
                    File='energy_storage_exchange',
                    projectID=data.index.values,
                    Capacity=data['Rated Power in kW']/1e3,
                    YearCommissioned=pd.DatetimeIndex(data['Commissioning Date']).year))
    data.loc[data.Technology.str.contains('Pumped') &
             data.Technology.notnull(), 'Technology'] = 'Pumped storage'
    data = data.loc[data.Technology == 'Pumped storage'].reindex(columns=target_columns(detailed_columns=True))
    data = data.reset_index(drop = True)
    data = data.loc[data.Country.isin(europeancountries())]
    data.projectID = 'ESE' + data.projectID.astype(str)
    data.to_csv(saved_version, index_label='id', encoding='utf-8')
    return data

data_config['ESE'] = {'read_function': ESE,
                      'clean_single_kwargs': dict(detailed_columns=True),
                      'reliability_score':4}


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
    https://transparency.entsoe.eu/content/static_content/Static%20content/
    web%20api/Guide.html#_authentication_and_authorisation
    """
    if update or raw:
        if additional_data_config()['entsoe_token'] is not np.nan:
            entsoe_token = additional_data_config()['entsoe_token']
        assert entsoe_token is not None, "entsoe_token is missing"

        def full_country_name(l):
            import types
            def pycountry_try(c):
                try:
                    return pycountry.countries.get(alpha_2=c).name
                except KeyError:
                    return None
            if isinstance(l, types.StringTypes):
                return filter(None, [pycountry_try(l)])
            else: # iterable
                return filter(None, [pycountry_try(country) for country in l])

        domains = pd.read_csv(_data('in/entsoe-areamap.csv'), sep=';', header=None)
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
        def namespace(element):
            m = re.match('\{.*\}', element.tag)
            return m.group(0) if m else ''
        def attribute(etree_sel):
            return etree_sel.text

        entsoe = []
        for i in domains.index[domains.Country.notnull()]:
            logger.info("Fetching power plants for domain %s (%s)",
                        domains.loc[i, 0],
                        domains.loc[i, 'Country'])

            #https://transparency.entsoe.eu/content/static_content/
            #Static%20content/web%20api/Guide.html_generation_domain
            ret = requests.get('https://transparency.entsoe.eu/api',
                               params=dict(securityToken=entsoe_token,
                                           documentType='A71', processType='A33',
                                           In_Domain=domains.loc[i,0],
                                           periodStart='201512312300', periodEnd='201612312300'))
            try:
                etree = ET.fromstring(ret.content) #create an ElementTree object
            except ET.ParseError:
                #hack for dealing with unencoded '&' in ENTSOE-API
                etree = ET.fromstring(re.sub(r'&(?=[^;]){6}', r'&amp;', ret.text)
                                      .encode('utf-8') )
            ns = namespace(etree)
            df = pd.DataFrame(columns=level1+level2+level3+['Country'])
            for arg in level1:
                df[arg] = map(attribute , etree.findall('*/%s%s'%(ns, arg)))
            for arg in level2:
                df[arg] = map(attribute , etree.findall('*/*/%s%s'%(ns, arg)))
            for arg in level3:
                df[arg] = map(attribute , etree.findall('*/*/*/%s%s'%(ns, arg)))
            df['Country'] = domains.loc[i,'Country']
            logger.info("Received data on %d power plants", len(df))
            entsoe.append(df)
        entsoe = pd.concat(entsoe, ignore_index=True)
        if raw:
            return entsoe
        entsoe.columns = ['Name', 'projectID', 'High Voltage Limit', 'Fueltype',
                          'Capacity', 'Country']
        entsoe['Fueltype'] = entsoe['Fueltype'].map(fdict)
        entsoe['Name'] = entsoe['Name'].str.title()
        entsoe = entsoe.loc[entsoe.Country.notnull()]
        entsoe = entsoe.loc[~((entsoe.projectID.duplicated(keep=False))&
                              (~entsoe.Country.isin(europeancountries())))]
        entsoe = entsoe.drop_duplicates('projectID').reset_index(drop=True)
        entsoe['File'] = "https://transparency.entsoe.eu/generation/r2/\ninstalledCapacityPerProductionUnit/show"
        entsoe = entsoe.reindex(columns=target_columns())
        entsoe = gather_technology_info(entsoe)
        entsoe = gather_set_info(entsoe)
        entsoe = clean_technology(entsoe)
        entsoe.Fueltype.replace(to_replace=['.*Hydro.*','Fossil Gas', '.*(?i)coal.*','.*Peat',
                                            'Marine', 'Wind.*', '.*Oil.*', 'Biomass'],
                                value=['Hydro','Natural Gas', 'Hard Coal', 'Lignite', 'Other',
                                       'Wind', 'Oil', 'Bioenergy'],
                                regex=True, inplace=True)
        entsoe.loc[:,'Capacity'] = pd.to_numeric(entsoe.Capacity)
        entsoe.loc[entsoe.Country=='Austria, Germany, Luxembourg', 'Country'] = \
                  [parse_Geoposition(powerplant , return_Country=True)
                   for powerplant in entsoe.loc[entsoe.Country=='Austria, Germany, Luxembourg', 'Name']]
        entsoe.Country.replace(to_replace=['Deutschland','.*sterreich' ,'L.*tzebuerg'],
                               value=['Germany','Austria','Luxembourg'],
                               regex=True, inplace=True)
        entsoe = entsoe.loc[entsoe.Country.isin(europeancountries()+[None])]
        entsoe.loc[:,'Country'] = entsoe.Country.astype(str)
        entsoe.loc[entsoe.Country=='None', 'Country'] = np.NaN
        entsoe.reset_index(drop=True, inplace=True)
        entsoe.to_csv(_data_out('entsoe_powerplants.csv'),
                      index_label='id', encoding='utf-8')
        return entsoe
    else:
        entsoe = pd.read_csv(_data_out('entsoe_powerplants.csv'),
                             index_col='id', encoding='utf-8')
        return (entsoe[entsoe.Country.isin(europeancountries())]
                    .pipe(scale_to_net_capacities,(not data_config['ENTSOE']['net_capacity'])))

data_config['ENTSOE'] = {'read_function': ENTSOE,
           'reliability_score':4, 'net_capacity':True}


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
    wepp = pd.read_csv(_data_in('platts_wepp.csv'), dtype=datatypes, encoding='utf-8')
    if raw:
        return wepp

    # Try to parse lat-lon geo coordinates of each unit
    if parseGeoLoc:
        for index, row in wepp.iterrows():
            query = None
            if pd.isnull(row['LAT']):
                while True:
                    query = parse_Geoposition(row['UNIT'], row['POSTCODE'], row['COUNTRY'])      # 1st try
#                    if query != None: break
#                    query = parse_Geoposition(row['POSTCODE'], row['COUNTRY'])  # 2nd try
#                    if query != None: break
#                    query = parse_Geoposition(row['CITY'], row['COUNTRY'])      # 3rd try
                    break

                if isinstance(query, tuple):
                    wepp.at[index, 'LAT'] = query[0] # write latitude
                    wepp.at[index, 'LON'] = query[1] # write longitude
                    logger.info(u"Index {0} | Unit '{1}' in {2} returned geoposition: ({3},{4})."\
                          .format(index,row['UNIT'],row['COUNTRY'],query[0],query[1]))
            else:
                logger.info("Index {0} | Geoposition already exists.".format(index))
        # Loop done: Make backup of original file and save querying results
        os.rename(_data_in('platts_wepp.csv'), _data_in('platts_wepp_backup.csv'))
        wepp.to_csv(_data_in('platts_wepp.csv'), encoding='utf-8')

    # str.title(): Return a titlecased version of the string where words start
    # with an uppercase character and the remaining characters are lowercase.
    wepp.columns = wepp.columns.str.title()
    # Fit WEPP-column names to our specifications
    wepp.rename(columns={'Unit':'Name',
                         'Fuel':'Fueltype',
                         'Fueltype':'Technology',
                         'Mw':'Capacity',
                         'Year':'YearCommissioned',
                         #'Retire':'YearDecommissioned',
                         'Lat':'lat',
                         'Lon':'lon',
                         'Unitid':'projectID'
                         }, inplace=True)
    # Do country transformations and drop those which are not in definded scope
    c = {'ENGLAND & WALES':u'UNITED KINGDOM',
         'GIBRALTAR':u'SPAIN',
         'SCOTLAND':u'UNITED KINGDOM'}
    wepp.Country = wepp.Country.replace(c).str.title()
    wepp = wepp[wepp.Country.isin(europeancountries())]
    # Drop any rows with plants which are not: In operation (OPR) or under construction (CON)
    wepp = wepp[wepp.Status.isin(['OPR', 'CON'])]
    # Replace fueltypes
    d = {'AGAS':'Bioenergy',    # Syngas from gasified agricultural waste or poultry litter
         'BFG':'Other',         # blast furnance gas -> "Hochofengas"
         'BGAS':'Bioenergy',
         'BIOMASS':'Bioenergy',
         'BL':'Bioenergy',
         'CGAS':'Hard Coal',
         'COAL':'Hard Coal',
         'COG':'Other',         # coke oven gas -> deutsch: "Hochofengas"
         'COKE':'Hard Coal',
         'CSGAS':'Hard Coal',   # Coal-seam-gas
         'CWM':'Hard Coal',     # Coal-water mixture (aka coal-water slurry)
         'DGAS':'Other',        # sewage digester gas -> deutsch: "Klaergas"
         'FGAS':'Other',        # Flare gas or wellhead gas or associated gas
         'GAS':'Natural Gas',
         'GEO':'Geothermal',
         'H2':'Other',          # Hydrogen gas
         'HZDWST':'Waste',      # Hazardous waste
         'INDWST':'Waste',      # Industrial waste or refinery waste
         'JET':'Oil',           # Jet fuels
         'KERO':'Oil',          # Kerosene
         'LGAS':'Other',        # landfill gas -> deutsch: "Deponiegas"
         'LIGNIN':'Bioenergy',
         'LIQ':'Other',         # (black) liqour -> deutsch: "Schwarzlauge", die bei Papierherstellung anfaellt
         'LNG':'Natural Gas',   # Liquified natural gas
         'LPG':'Natural Gas',   # Liquified petroleum gas (usually butane or propane)
         'MBM':'Bioenergy',     # Meat and bonemeal
         'MEDWST':'Bioenergy',  # Medical waste
         'MGAS':'Other',        # mine gas -> deutsch: "Grubengas"
         'NAP':'Oil',           # naphta
         'OGAS':'Oil',          # Gasified crude oil or refinery bottoms or bitumen
         'PEAT':'Lignite',
         'REF':'Waste',
         'REFGAS':'Other',      # Syngas from gasified refuse
         'RPF':'Waste',         # Waste paper and/or waste plastic
         'PWST':'Other',        # paper mill waste
         'RGAS':'Other',        # refinery off-gas -> deutsch: "Raffineriegas"
         'SHALE':'Oil',
         'SUN':'Solar',
         'TGAS':'Other',        # top gas -> deutsch: "Hochofengas"
         'TIRES':'Other',       # Scrap tires
         'UNK':'Other',
         'UR':'Nuclear',
         'WAT':'Hydro',
         'WOOD':'Bioenergy',
         'WOODGAS':'Bioenergy',
         'WSTGAS':'Other',      # waste gas -> deutsch: "Industrieabgas"
         'WSTWSL':'Waste',      # Wastewater sludge
         'WSTH':'Waste'}
    wepp.Fueltype = wepp.Fueltype.replace(d)
    ## Fill NaNs to allow str actions
    wepp.Technology.fillna('', inplace=True)
    wepp.Turbtype.fillna('', inplace=True)
    # Correct technology infos:
    wepp.loc[wepp.Technology.str.contains('LIG', case=False), 'Fueltype'] = 'Lignite'
    wepp.loc[wepp.Turbtype.str.contains('KAPLAN|BULB', case=False), 'Technology'] = 'Run-Of-River'
    wepp.Technology = wepp.Technology.replace({'CONV/PS':'Pumped Storage',
                                               'CONV':'Reservoir',
                                               'PS':'Pumped Storage'})
    tech_st_pattern = ['ANTH', 'BINARY', 'BIT', 'BIT/ANTH', 'BIT/LIG', 'BIT/SUB',
                       'BIT/SUB/LIG', 'COL', 'DRY ST', 'HFO', 'LIG', 'LIG/BIT',
                       'PWR', 'RDF', 'SUB']
    tech_ocgt_pattern = ['AGWST', 'LITTER', 'RESID', 'RICE', 'STRAW']
    tech_ccgt_pattern = ['LFO']
    wepp.loc[wepp.Technology.isin(tech_st_pattern), 'Technology'] = 'Steam Turbine'
    wepp.loc[wepp.Technology.isin(tech_ocgt_pattern), 'Technology'] = 'OCGT'
    wepp.loc[wepp.Technology.isin(tech_ccgt_pattern), 'Technology'] = 'CCGT'
    ut_ccgt_pattern = ['CC','GT/C','GT/CP','GT/CS','GT/ST','ST/C','ST/CC/GT','ST/CD',
                       'ST/CP','ST/CS','ST/GT','ST/GT/IC','ST/T', 'IC/CD','IC/CP','IC/GT']
    ut_ocgt_pattern = ['GT','GT/D','GT/H','GT/HY','GT/IC','GT/S','GT/T','GTC']
    ut_st_pattern = ['ST','ST/D']
    ut_ic_pattern = ['IC','IC/H']
    wepp.loc[wepp.Utype.isin(ut_ccgt_pattern), 'Technology'] = 'CCGT'
    wepp.loc[wepp.Utype.isin(ut_ocgt_pattern), 'Technology'] = 'OCGT'
    wepp.loc[wepp.Utype.isin(ut_st_pattern), 'Technology'] = 'Steam Turbine'
    wepp.loc[wepp.Utype.isin(ut_ic_pattern), 'Technology'] = 'Combustion Engine'
    wepp.loc[wepp.Utype=='WTG', 'Technology'] = 'Onshore'
    wepp.loc[wepp.Utype=='WTG/O', 'Technology'] = 'Offshore'
    wepp.loc[(wepp.Fueltype=='Solar')&(wepp.Utype.isin(ut_st_pattern)), 'Technology'] = 'CSP'
    # Derive the SET column
    chp_pattern = ['CC/S','CC/CP','CCSS/P','GT/CP','GT/CS','GT/S','GT/H','IC/CP',
                   'IC/H','ST/S','ST/H','ST/CP','ST/CS','ST/D']
    wepp.loc[wepp.Utype.isin(chp_pattern), 'Set'] = 'CHP'
    wepp.loc[wepp.Set.isnull(), 'Set' ] = 'PP'
    # Drop any columns we do not need
    wepp = wepp.reindex(columns=target_columns())
    # Clean up the mess
    wepp.Fueltype = wepp.Fueltype.str.title()
    wepp.loc[wepp.Technology.str.len()>4, 'Technology'] = \
        wepp.loc[wepp.Technology.str.len()>4, 'Technology'].str.title()
    wepp.reset_index(drop=True)
    # Done!
    wepp.datasetID = 'WEPP'
    return wepp.pipe(scale_to_net_capacities, (not data_config['WEPP']['net_capacity']))

data_config['WEPP'] = {'read_function': WEPP,
           'reliability_score':4, 'net_capacity':False}


def UBA(header=9, skip_footer=26, prune_wind=True, prune_solar=True):
    """
    Returns the UBA Database.
    The user has to download the database from:
        ``https://www.umweltbundesamt.de/dokument/datenbank-kraftwerke-in-deutschland``
    and has to place it into the ``data/In`` folder.

    Parameters:
    -----------
        header : int, Default 9
            The zero-indexed row in which the column headings are found.
        skip_footer : int, Default 26

    """
    filename = 'kraftwerke-de-ab-100-mw.xls'
    uba = pd.read_excel(_data_in(filename), header=header, skip_footer=skip_footer,
                        na_values='n.b.')
    uba = uba.rename(columns={u'Kraftwerksname / Standort': 'Name',
                              u'Elektrische Bruttoleistung (MW)': 'Capacity',
                              u'Inbetriebnahme  (ggf. Ertüchtigung)':'YearCommissioned',
                              u'Primärenergieträger':'Fueltype',
                              u'Anlagenart':'Technology',
                              u'Fernwärme-leistung (MW)':'CHP',
                              u'Standort-PLZ':'PLZ'})
    uba.Name = uba.Name.replace({'\s\s+':' '}, regex=True)
    from .heuristics import PLZ_to_LatLon_map
    uba['lon'] = uba.PLZ.map(PLZ_to_LatLon_map()['lon'])
    uba['lat'] = uba.PLZ.map(PLZ_to_LatLon_map()['lat'])

    uba.loc[:, 'Country'] = 'Germany'
    uba.loc[:, 'File'] = filename
    uba.loc[:, 'projectID'] = ['UBA{:03d}'.format(i + header + 2) for i in uba.index]
    uba.loc[uba.CHP.notnull(), 'Set'] = 'CHP'
    uba = (uba.pipe(gather_set_info))
#              .pipe(clean_powerplantname))
    uba.Technology = uba.Technology.replace({u'DKW':'Steam Turbine',
                                             u'DWR':'Pressurized Water Reactor',
                                             u'G/AK':'Steam Turbine',
                                             u'GT':'OCGT',
                                             u'GuD':'CCGT',
                                             u'GuD / HKW':'CCGT',
                                             u'HKW':'Steam Turbine',
                                             u'HKW (DT)':'Steam Turbine',
                                             u'HKW / GuD':'CCGT',
                                             u'HKW / SSA':'Steam Turbine',
                                             u'IKW':'OCGT',
                                             u'IKW / GuD':'CCGT',
                                             u'IKW / HKW':'Steam Turbine',
                                             u'IKW / HKW / GuD':'CCGT',
                                             u'IKW / SSA':'OCGT',
                                             u'IKW /GuD':'CCGT',
                                             u'LWK':'Run-Of-River',
                                             u'PSW':'Pumped Storage',
                                             u'SWK':'Reservoir Storage',
                                             u'SWR':'Boiled Water Reactor'})
    uba.loc[uba.Fueltype=='Wind (O)', 'Technology'] = 'Offshore'
    uba.loc[uba.Fueltype=='Wind (L)', 'Technology'] = 'Onshore'
    uba.loc[uba.Fueltype.str.contains('Wind'), 'Fueltype'] = 'Wind'
    uba.loc[uba.Fueltype.str.contains('Braunkohle'), 'Fueltype'] = 'Lignite'
    uba.loc[uba.Fueltype.str.contains('Steinkohle'), 'Fueltype'] = 'Hard Coal'
    uba.loc[uba.Fueltype.str.contains('Erdgas'), 'Fueltype'] = 'Natural Gas'
    uba.loc[uba.Fueltype.str.contains('HEL'), 'Fueltype'] = 'Oil'
    uba.Fueltype = uba.Fueltype.replace({u'Biomasse':'Bioenergy',
                                         u'Gichtgas':'Other',
                                         u'HS':'Oil',
                                         u'Konvertergas':'Other',
                                         u'Licht':'Solar',
                                         u'Raffineriegas':'Other',
                                         u'Uran':'Nuclear',
                                         u'Wasser':'Hydro',
                                         u'\xd6lr\xfcckstand':'Oil'})
    uba.Name.replace([r'(?i)oe', r'(?i)ue'], [u'ö', u'ü'], regex=True, inplace=True)
    if prune_wind:
        uba = uba.loc[lambda x: x.Fueltype!='Wind']
    if prune_solar:
        uba = uba.loc[lambda x: x.Fueltype!='Solar']
    uba = (uba.reindex(columns=target_columns()).pipe(scale_to_net_capacities,
                  (not data_config['UBA']['net_capacity'])))
    return uba

data_config['UBA'] = {'read_function': UBA,
           'clean_single_kwargs': dict(aggregate_powerplant_units=False),
           'net_capacity':False, 'reliability_score':2}


def BNETZA(header=9, sheet_name='Gesamtkraftwerksliste BNetzA', prune_wind=True, prune_solar=True):
    """
    Returns the database put together by Germany's 'Federal Network Agency'
    (dt. 'Bundesnetzagentur' (BNetzA)). The user has to download the database from:
    ``https://www.bundesnetzagentur.de/DE/Sachgebiete/ElektrizitaetundGas/
    Unternehmen_Institutionen/Versorgungssicherheit/Erzeugungskapazitaeten/
    Kraftwerksliste/kraftwerksliste-node.html``
    and has to place it into the ``data/In`` folder.

    Parameters:
    -----------
        header : int, Default 9
            The zero-indexed row in which the column headings are found.
    """
    filename = 'Kraftwerksliste_2017_2.xlsx'
    bnetza = pd.read_excel(_data_in(filename), header=header, sheet_name=sheet_name)
    bnetza = bnetza.rename(columns={
            u'Kraftwerksnummer Bundesnetzagentur': 'projectID',
            u'Kraftwerksname': 'Name',
            u'Netto-Nennleistung (elektrische Wirkleistung) in MW': 'Capacity',
            u'Wärmeauskopplung (KWK)\n(ja/nein)':'Set',
            u'Ort\n(Standort Kraftwerk)':'Ort',
            (u'Auswertung\nEnergieträger (Zuordnung zu einem Hauptenergieträger '
             u'bei Mehreren Energieträgern)'):'Fueltype',
            (u'Kraftwerksstatus \n(in Betrieb/\nvorläufig stillgelegt/\nsaisonale '
             u'Konservierung\nGesetzlich an Stilllegung gehindert/\nSonderfall)'):'Status',
            (u'Aufnahme der kommerziellen Stromerzeugung der derzeit in Betrieb '
             u'befindlichen Erzeugungseinheit\n(Jahr)'):'YearCommissioned',
             u'PLZ\n(Standort Kraftwerk)':'PLZ'})
    # If BNetzA-Name is empty replace by company, if this is empty by city.
    from .heuristics import PLZ_to_LatLon_map
    bnetza['lon'] = bnetza.PLZ.map(PLZ_to_LatLon_map()['lon'])
    bnetza['lat'] = bnetza.PLZ.map(PLZ_to_LatLon_map()['lat'])
    bnetza.loc[bnetza.Name.str.len().fillna(0.0)<=4, 'Name'] =\
        bnetza.loc[bnetza.Name.str.len().fillna(0.0)<=4, 'Unternehmen'] + ' ' +\
        bnetza.loc[bnetza.Name.str.len().fillna(0.0)<=4, 'Name'].fillna('')
    bnetza.Name.fillna(bnetza.Ort, inplace=True)
    add_location_b = bnetza[bnetza.Ort.notnull()].apply(lambda ds: (ds['Ort'] not in ds['Name'])
                                            and (unicode.title(ds['Ort']) not in ds['Name']), axis=1)
    bnetza.loc[bnetza.Ort.notnull() & add_location_b, 'Name'] =  (
                bnetza.loc[bnetza.Ort.notnull() & add_location_b,'Ort'] + ' ' +
                bnetza.loc[bnetza.Ort.notnull() & add_location_b,'Name'])
    bnetza.Name.replace('\s+', ' ', regex=True, inplace=True)
    # Filter by Status
    pattern = '|'.join(['.*(?i)betrieb', '.*(?i)gehindert', '(?i)vorl.*ufig.*',
                        'Sicherheitsbereitschaft', 'Sonderfall'])
    bnetza = (bnetza.loc[bnetza.Status.str.contains(pattern, regex=True, case=False)]
                    .loc[lambda df: df.projectID.notna()])
    # Technologies
    bnetza.Blockname.replace(
            to_replace=['.*(GT|gasturbine).*', '.*(DT|HKW|(?i)dampfturbine|(?i)heizkraftwerk).*', '.*GuD.*'],
            value=['OCGT', 'Steam Turbine', 'CCGT'], regex=True, inplace=True)
    bnetza = gather_technology_info(bnetza, search_col=['Name', 'Fueltype', 'Blockname'])
    bnetza.loc[bnetza.Fueltype.str.contains('Onshore', case=False), 'Technology'] = 'Onshore'
    bnetza.loc[bnetza.Fueltype.str.contains('Offshore', case=False), 'Technology'] = 'Offshore'
    bnetza.loc[bnetza.Fueltype.str.contains('solare', case=False), 'Technology'] = 'PV'
    bnetza.loc[bnetza.Fueltype.str.contains('Laufwasser', case=False), 'Technology'] = 'Run-Of-River'
    bnetza.loc[bnetza.Fueltype.str.contains('Speicherwasser', case=False), 'Technology'] = 'Reservoir'
    bnetza.loc[bnetza.Fueltype==u'Pumpspeicher', 'Technology'] = 'Pumped Storage'
    # Fueltypes
    bnetza.Fueltype.replace(
            to_replace=['(.*(?i)wasser.*|Pump.*)', 'Erdgas', 'Steinkohle', 'Braunkohle',
                        'Wind.*', 'Solar.*', '.*(?i)energietr.*ger.*\n.*', 'Kern.*',
                        'Mineral.l.*', 'Biom.*', '.*(?i)(e|r|n)gas', 'Geoth.*', 'Abfall'],
            value=['Hydro', 'Natural Gas', 'Hard Coal', 'Lignite', 'Wind', 'Solar', 'Other',
                   'Nuclear', 'Oil', 'Bioenergy', 'Other', 'Geothermal', 'Waste'],
            regex=True, inplace=True)
    if prune_wind:
        bnetza = bnetza.loc[lambda x: x.Fueltype!='Wind']
    if prune_solar:
        bnetza = bnetza.loc[lambda x: x.Fueltype!='Solar']
    # Filter by country
    bnetza = bnetza[~bnetza.Bundesland.isin([u'Österreich', 'Schweiz', 'Luxemburg'])]
    bnetza.loc[:, 'Country'] = 'Germany'
    # Remaining columns
    bnetza.loc[:, 'File'] = filename
    bnetza.loc[:, 'Set'] = bnetza.Set.fillna('Nein').str.title().replace({u'Ja':'CHP',u'Nein':'PP'})
    bnetza = (bnetza.reindex(columns=target_columns()).pipe(scale_to_net_capacities,
                  (not data_config['BNETZA']['net_capacity'])))
    return bnetza

data_config['BNETZA'] = {'read_function': BNETZA, 'net_capacity':True,
            'reliability_score':3}


def OPSD_VRE():
    """
    Return standardized OPSD (Open Power Systems Data) renewables (VRE)
    database with target column names and fueltypes.

    This sqlite database is very big and therefore not part of the package.
    It needs to be obtained here: http://data.open-power-system-data.org/renewable_power_plants/

    """
    def read_opsd_res(country):
        import pandas as pd
        import sqlite3
        db = sqlite3.connect(_data_in('renewable_power_plants.sqlite'))
        if country == 'CH':
            cur = db.execute(
            "SELECT"
            "   substr(commissioning_date,1,4), "
            "   energy_source_level_2, technology, electrical_capacity, lat, lon "
            "FROM"
            "   renewable_power_plants_CH "
            )
        elif country == 'DE':
            cur = db.execute(
            "SELECT"
            "   substr(commissioning_date,1,4), "
            "   energy_source_level_2, technology, electrical_capacity, lat, lon "
            "FROM"
            "   renewable_power_plants_DE "
            "WHERE"
            "   (DATE(substr(decommissioning_date,1,4)||substr(decommissioning_date,6,2)|| "
            "   substr(decommissioning_date,9,2)) > DATE(20153112)) OR decommissioning_date=='NaT'"
            "AND NOT"
            "   comment LIKE '%R_%'"
            )
        elif country == 'DK':
            cur = db.execute(
            "SELECT"
            "   substr(commissioning_date,1,4), "
            "   energy_source_level_2, technology, electrical_capacity, lat, lon "
            "FROM"
            "   renewable_power_plants_DK "
            )
        else:
            raise NotImplementedError("The country '{0}' is not supported yet.".format(country))

        df = pd.DataFrame(cur.fetchall(),
                          columns=["YearCommissioned", "Fueltype", "Technology",
                                   "Capacity", "lat", "lon"])
        df.loc[:, 'Country'] = pycountry.countries.get(alpha_2=country).name
        df.loc[:, 'projectID'] = pd.Series(['OPSD-VRE_{}_{}'.format(country,i) for i in df.index])
        return df

    df = pd.concat((read_opsd_res(r) for r in ['DE','DK','CH']), ignore_index=True)
    df.loc[:, 'Country'] = df.Country.str.title()
    df.loc[:, 'File'] = 'renewable_power_plants.sqlite'
    df.loc[:, 'Set'] = 'PP'
    df = df.replace({'NaT':np.NaN,
                     None:np.NaN,
                     '':np.NaN})
    for col in ['YearCommissioned', 'Capacity', 'lat', 'lon']:
        df.loc[:, col] = df[col].astype(np.float)
    d = {u'Connected unit':'PV',
         u'Integrated unit':'PV',
         u'Photovoltaics':'PV',
         u'Photovoltaics ground':'PV',
         u'Stand alone unit':'PV',
         u'Onshore wind energy':'Onshore',
         u'Offshore wind energy':'Offshore'}
    df.Technology.replace(d, inplace=True)
    return df


def IRENA_stats():
    """
    Reads the IRENA Capacity Statistics 2017 Database
    """
    # Read the raw dataset
    df = pd.read_csv(_data_in('IRENA_CapacityStatistics2017.csv'), encoding='utf-8')
    # "Unpivot"
    df = pd.melt(df, id_vars=['Indicator', 'Technology', 'Country'], var_name='Year',
                 value_vars=[unicode(i) for i in range(2000,2017,1)], value_name='Capacity')
    # Drop empty
    df.dropna(axis=0, subset=['Capacity'], inplace=True)
    # Drop generations
    df = df[df.Indicator=='Electricity capacity (MW)']
    df.drop('Indicator', axis=1, inplace=True)
    # Drop countries out of scope
    df.Country.replace({'Czechia':u'Czech Republic',
                        'UK':u'United Kingdom'}, inplace=True)
    df = df[df.Country.isin(europeancountries())]
    # Convert to numeric
    df.Year = df.Year.astype(int)
    df.Capacity = df.Capacity.str.strip().str.replace(' ','').astype(float)
    # Handle Fueltypes and Technologies
    d = {u'Bagasse':'Bioenergy',
         u'Biogas':'Bioenergy',
         u'Concentrated solar power':'Solar',
         u'Geothermal':'Geothermal',
         u'Hydro 1-10 MW':'Hydro',
         u'Hydro 10+ MW':'Hydro',
         u'Hydro <1 MW':'Hydro',
         u'Liquid biofuels':'Bioenergy',
         u'Marine':'Hydro',
         u'Mixed and pumped storage':'Hydro',
         u'Offshore wind energy':'Wind',
         u'Onshore wind energy':'Wind',
         u'Other solid biofuels':'Bioenergy',
         u'Renewable municipal waste':'Bioenergy',
         u'Solar photovoltaic':'Solar'}
    df.loc[:,'Fueltype'] = df.Technology.map(d)
    d = {u'Concentrated solar power':'CSP',
         u'Solar photovoltaic':'PV',
         u'Onshore wind energy':'Onshore',
         u'Offshore wind energy':'Offshore'}
    df.Technology.replace(d, inplace=True)
    df.loc[:,'Set'] = 'PP'
    return df.reset_index(drop=True)

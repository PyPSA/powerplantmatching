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
Collection of power plant data bases and statistical data
"""

from __future__ import print_function, absolute_import

import os
import sys
import xlrd
import numpy as np
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import re
import pycountry
import logging
from textwrap import dedent
from six.moves import reduce

from .config import get_config
from .cleaning import (gather_fueltype_info, gather_set_info,
                       gather_technology_info, clean_powerplantname,
                       clean_technology)
from .utils import (fill_geoposition, _data, _data_in, _data_out,
                    correct_manually, config_filter)
from .heuristics import scale_to_net_capacities
from six import iteritems, string_types

logger = logging.getLogger(__name__)
text = str if sys.version_info >= (3, 0) else unicode
cget = pycountry.countries.get
net_caps = get_config()['display_net_caps']
data_config = {}


def OPSD(rawEU=False, rawDE=False,
         statusDE=['operating', 'reserve', 'special_case'],
         config=None):
    """
    Importer for the OPSD (Open Power Systems Data) database.

    Parameters
    ----------
    rawEU : Boolean, default False
        Whether to return the raw EU (=non-DE) database.
    rawDE : Boolean, default False
        Whether to return the raw DE database.
    statusDE : list, default ['operating', 'reserve', 'special_case']
        Filter DE entries by operational status ['operating', 'shutdown',
        'reserve', etc.]
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    if config is None:
        config = get_config()

    opsd_EU = pd.read_csv(data_config['OPSD']['source_file'][0],
                          na_values=' ', encoding='utf-8')
    opsd_DE = pd.read_csv(data_config['OPSD']['source_file'][1],
                          na_values=' ', encoding='utf-8')
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
    opsd_EU.rename(columns={'Lat': 'lat',
                            'Lon': 'lon',
                            'Energy_Source': 'Fueltype',
                            'Commissioned': 'YearCommissioned',
                            'Source': 'File'},
                   inplace=True)
    opsd_EU.loc[:, 'Retrofit'] = opsd_EU.YearCommissioned
    opsd_EU.loc[:, 'projectID'] = 'OEU' + opsd_EU.index.astype(str)
    opsd_EU = opsd_EU.reindex(columns=config['target_columns'])
    opsd_DE.columns = opsd_DE.columns.str.title()
    # If BNetzA-Name is empty replace by company, if this is empty by city.
    opsd_DE.Name_Bnetza.fillna(opsd_DE.Company, inplace=True)
    opsd_DE.Name_Bnetza.fillna(opsd_DE.City, inplace=True)
    opsd_DE.rename(columns={'Lat': 'lat',
                            'Lon': 'lon',
                            'Name_Bnetza': 'Name',
                            'Energy_Source_Level_2': 'Fueltype',
                            'Type': 'Set',
                            'Country_Code': 'Country',
                            'Capacity_Net_Bnetza': 'Capacity',
                            'Commissioned': 'YearCommissioned',
                            'Source': 'File'},
                   inplace=True)
    opsd_DE['Fueltype'].fillna(opsd_DE['Energy_Source_Level_1'], inplace=True)
    opsd_DE['Retrofit'].fillna(opsd_DE['YearCommissioned'], inplace=True)
    opsd_DE['projectID'] = opsd_DE['Id']
    if statusDE is not None:
        opsd_DE = opsd_DE.loc[opsd_DE.Status.isin(statusDE)]
    opsd_DE = opsd_DE.reindex(columns=config['target_columns'])
    return (pd.concat([opsd_EU, opsd_DE], ignore_index=True)
            .replace(dict(Fueltype={'Biomass and biogas': 'Bioenergy',
                                    'Fossil fuels': np.nan,
                                    'Mixed fossil fuels': 'Other',
                                    'Natural gas': 'Natural Gas',
                                    'Non-renewable waste': 'Waste',
                                    'Other bioenergy and renewable waste':
                                        'Bioenergy',
                                    'Other or unspecified energy sources':
                                        'Other',
                                    'Other fossil fuels': 'Other',
                                    'Other fuels': 'Other'},
                          Set={'IPP': 'PP'}))
            .replace({'Country': {'UK': u'GB', '[ \t]+|[ \t]+$.': ''},
                      'Capacity': {0.: np.nan}}, regex=True)
            .dropna(subset=['Capacity'])
            .assign(Name=lambda df: df.Name.str.title().str.strip(),
                    Fueltype=lambda df: df.Fueltype.str.title().str.strip(),
                    Country=lambda df: (pd.Series(df.Country.apply(
                                        lambda c: cget(alpha_2=c).name),
                                        index=df.index).str.title()))
            .loc[lambda df: df.Country.isin(config['target_countries'])]
            .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
            .pipe(gather_set_info)
            .pipe(clean_technology)
            .reset_index(drop=True)
            .pipe(scale_to_net_capacities,
                  (not data_config['OPSD']['net_capacity']))
            .pipe(correct_manually, 'OPSD', config=config)
            )


data_config['OPSD'] = {'read_function': OPSD,
                       'reliability_score': 5,
                       'net_capacity': True,
                       'source_file':
                           [_data_in('conventional_power_plants_EU.csv'),
                            _data_in('conventional_power_plants_DE.csv')]}


def GEO(raw=False, config=None):
    """
    Importer for the GEO database.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    if config is None:
        config = get_config()

    def read_globalenergyobservatory():
        import pandas as pd
        import sqlite3

        db = sqlite3.connect(data_config['GEO']['source_file'])

        cur = db.execute(
            "select"
            "   GEO_Assigned_Identification_Number, "
            "   name, type, Type_of_Plant_rng1 , Type_of_Fuel_rng1_Primary, "
            "   Type_of_Fuel_rng2_Secondary,"
            "   country, design_capacity_mwe_nbr, "
            "   Year_Project_Commissioned, Year_Rng1_yr1, "
            "   CAST(longitude_start AS REAL) as lon,"
            "   CAST(latitude_start AS REAL) as lat "
            "from"
            "   powerplants "
            "where"
            "   status_of_plant_itf=='Operating Fully' and"
            "   design_capacity_mwe_nbr > 0"
        )

        return pd.DataFrame(cur.fetchall(),
                            columns=["projectID", "Name", "Fueltype",
                                     "Technology", "FuelClassification1",
                                     "FuelClassification2", "Country",
                                     "Capacity", "YearCommissioned",
                                     "Retrofit", "lon", "lat"])
    geo = read_globalenergyobservatory()
    if raw:
        return geo
    geo = (geo.assign(Retrofit=geo.Retrofit.astype(float),
                      projectID='GEO' + geo.projectID.astype(str)))
    # Necessary to do this in two steps, since Retrofit cannot be assigned to
    # itself (see above) and re-used for YearCommissioned in one step.
    geo = geo.assign(YearCommissioned=(
                             geo.YearCommissioned.astype(text)
                             .str.replace("[a-zA-Z]", '')
                             .str.replace("[^0-9.]", " ")
                             .str.split(' ')
                             .str[0].replace('', np.nan)
                             .str.slice(0, 4)
                             .astype(float)
                             .fillna(geo.Retrofit)))
    geo = geo.assign(Retrofit=geo.Retrofit.fillna(geo.YearCommissioned))
    return (geo
            .loc[lambda df: df.Country.isin(config['target_countries'])]
            .replace({col: {'Gas': 'Natural Gas'}
                      for col in {'Fueltype', 'FuelClassification1',
                                  'FuelClassification2'}})
            .pipe(gather_fueltype_info, search_col=['FuelClassification1'])
            .pipe(gather_technology_info, search_col=['FuelClassification1'],
                  config=config)
            .pipe(gather_set_info)
            .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
            .pipe(clean_powerplantname)
            .pipe(clean_technology, generalize_hydros=True)
            .pipe(scale_to_net_capacities,
                  (not data_config['GEO']['net_capacity']))
            .pipe(correct_manually, 'GEO', config=config))


data_config['GEO'] = {'read_function': GEO,
                      'aggregated_units': False,
                      'reliability_score': 3,
                      'net_capacity': False,
                      'source_file': _data_in('global_energy_observatory'
                                              '_power_plants.sqlite')}


def CARMA(raw=False, config=None):
    """
    Importer for the Carma database.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    if config is None:
        config = get_config()

    carmadata = pd.read_csv(data_config['CARMA']['source_file'],
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
                             'plant.id': 'projectID'})
            .assign(projectID=lambda df: 'CARMA' + df.projectID.astype(str))
            .loc[lambda df: df.Country.isin(config['target_countries'])]
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
            .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
            .pipe(clean_powerplantname)
            .pipe(gather_technology_info, config=config)
            .pipe(gather_set_info)
            .pipe(clean_technology)
            .drop_duplicates()
            .pipe(scale_to_net_capacities,
                  (not data_config['CARMA']['net_capacity']))
            .pipe(correct_manually, 'CARMA', config=config))


data_config['CARMA'] = {'read_function': CARMA,
                        'reliability_score': 1, 'net_capacity': False,
                        'source_file':
                            _data_in('Full_CARMA_2009_Dataset_1.csv')}


def IWPDCY(config=None):
    """
    This data is not yet available. Was extracted manually from
    the 'International Water Power & Dam Country Yearbook'.

    Parameters
    ----------
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    if config is None:
        config = get_config()

    return (pd.read_csv(data_config['IWPDCY']['source_file'],
                        encoding='utf-8', index_col='id')
            .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
            .loc[lambda df: df.Country.isin(config['target_countries'])]
            .dropna(subset=['Capacity'])
            .pipe(gather_set_info)
            .dropna(subset=['Capacity'])
            .assign(File='IWPDCY.csv',
                    projectID=lambda df: 'IWPDCY' + df.index.astype(str))
            .pipe(correct_manually, 'IWPDCY', config=config))


data_config['IWPDCY'] = {'read_function': IWPDCY,
                         'aggregated_units': True,
                         'reliability_score': 3,
                         'source_file': _data_in('IWPDCY.csv')}


def Capacity_stats(raw=False, level=2, config=None, **selectors):
    """
    Standardize the aggregated capacity statistics provided by the ENTSO-E.

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
    if config is None:
        config = get_config()

    opsd_aggregated = pd.read_csv(
            _data_in('national_generation_capacity_stacked.csv'),
            encoding='utf-8', index_col=0)

    selectors.setdefault('year', 2016)
    selectors.setdefault('source', 'entsoe SO&AF')

    if raw:
        return opsd_aggregated
    entsoedata = (opsd_aggregated
                  [lambda df: reduce(lambda x, y: x & y,
                                     (df[k] == v
                                      for k, v in iteritems(selectors)
                                      if v is not None),
                                     df['energy_source_level_%d' % level])]
                  .assign(country=lambda df: (pd.Series(df.country.apply(
                                              lambda c: cget(alpha_2=c).name),
                                              index=df.index).str.title()))
                  .replace(dict(country={'Czechia': 'Czech Republic'}))
                  .loc[lambda df: df.country.isin(config['target_countries'])]
                  .rename(columns={'technology': 'Fueltype'})
                  .replace(dict(Fueltype={
                          'Bioenergy and other renewable fuels': 'Bioenergy',
                          'Bioenergy and renewable waste': 'Waste',
                          'Coal derivatives': 'Hard Coal',
                          'Differently categorized fossil fuels': 'Other',
                          'Differently categorized renewable energy sources':
                          'Other',
                          'Hard coal': 'Hard Coal',
                          'Mixed fossil fuels': 'Other',
                          'Natural gas': 'Natural Gas',
                          'Other or unspecified energy sources': 'Other',
                          'Tide, wave, and ocean': 'Other'}))
                  .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
                  )
    entsoedata.columns = entsoedata.columns.str.title()
    return entsoedata


def Capacity_stats_factsheet(config=None):
    if config is None:
        config = get_config()

    df = pd.read_csv(_data_in('entsoe_factsheet.csv'), encoding='utf-8')
    return (df.replace(dict(Country={'Czechia': 'Czech Republic'}))
              .loc[lambda df: df.Country.isin(config['target_countries'])]
              .replace(dict(Fueltype={'Gas': 'Natural Gas',
                                      'Biomass': 'Bioenergy'}))
              .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])])


def GPD(raw=False, filter_other_dbs=True, config=None):
    """
    Importer for the `Global Power Plant Database`.

    Parameters
    ----------

    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """
    if raw:
        return pd.read_csv(_data_in('global_power_plant_database.csv'),
                           encoding='utf-8')

    if config is None:
        config = get_config()

    if filter_other_dbs:
        other_dbs = ['GEODB', 'CARMA', 'Open Power System Data']
    else:
        other_dbs = []
    return (pd.read_csv(data_config['GPD']['source_file'],
                        encoding='utf-8')
            [lambda df: (df.country_long.isin(config['target_countries']) &
                         ~df.geolocation_source.isin(other_dbs))]
            .rename(columns=lambda x: x.title())
            .assign(Country=lambda df: df['Country_Long'],
                    Retrofit=lambda df: df['Commissioning_Year'])
            .rename(columns={'Fuel1': 'Fueltype',
                             'Latitude': 'lat',
                             'Longitude': 'lon',
                             'Capacity_Mw': 'Capacity',
                             'Commissioning_Year': 'YearCommissioned',
                             'Source': 'File'
                             })
            .replace(dict(Fueltype={'Coal': 'Hard Coal',
                                    'Biomass': 'Bioenergy',
                                    'Gas': 'Natural Gas',
                                    'Wave and Tidal': 'Other'}))
            .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
            .pipe(gather_technology_info, config=config)
            .pipe(gather_set_info)
            .pipe(clean_powerplantname)
            .assign(projectID=lambda df: 'GPD' + df.index.astype(str))
            .pipe(correct_manually, 'GPD', config=config)
            )


data_config['GPD'] = {'read_function': GPD,
                      'aggregated_units': False,
                      'reliability_score':  3,
                      'source_file': _data_in('global_power_'
                                              'plant_database.csv')}


def WRI(**kwargs):
    logger.warning("'WRI' deprecated soon, please use GPD instead")
    return GPD(**kwargs)


def ESE(raw=False, config=None):
    """
    Importer for the ESE database.
    This database is not given within the repository because of its
    restrictive license.
    Get it by clicking 'Export Data XLS' on https://goo.gl/gVMwKJ and
    save the downloaded 'projects.xls' file in
    /path/to/powerplantmatching/data/in/.


    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    if config is None:
        config = get_config()

    path = data_config['ESE']['source_file']

    assert os.path.exists(path), dedent('''
        The ESE database has not been cached in your local repository, yet.
        Due to copyright issues it cannot be downloaded automatically.
        Get it by clicking 'Export Data XLS' on https://goo.gl/gVMwKJ and
        put the downloaded 'projects.xls' file into
        /path/to/powerplantmatching/data/in/.
        ''').format(path)

    book = xlrd.open_workbook(path)
    sheet = book.sheets()[0]
    col_longitude = sheet.row_values(0).index('Longitude')
    for row in sheet._cell_types:
        if row[col_longitude] == 3:
            row[col_longitude] = 2

    data = pd.read_excel(book, na_values=u'n/a', engine='xlrd')
    if raw:
        return data
    return (data
            .rename(columns={'Project Name': 'Name',
                             'Technology Type': 'Technology',
                             'Longitude': 'lon',
                             'Latitude': 'lat',
                             'Technology Type Category 2': 'Fueltype'})
            .assign(Set='Store',
                    File='energy_storage_exchange',
                    projectID=data.index.values,
                    Capacity=data['Rated Power in kW']/1e3,
                    YearCommissioned=pd.DatetimeIndex(
                            data['Commissioning Date']).year,
                    Retrofit=pd.DatetimeIndex(
                            data['Commissioning Date']).year)
            [lambda df: (df.Status == 'Operational') &
                        (df.Country.isin(config['target_countries']))]
            .pipe(clean_powerplantname)
            .pipe(clean_technology, generalize_hydros=True)
            .replace(dict(Fueltype={u'Electro-chemical': 'Battery',
                                    u'Pumped Hydro Storage': 'Hydro'}))
            .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
            .reset_index(drop=True)
            .assign(projectID=lambda df: 'ESE' + df.projectID.astype(str))
            .pipe(correct_manually, 'ESE', config=config))


data_config['ESE'] = {'read_function': ESE,
                      'reliability_score': 6,
                      'source_file': _data_in('projects.xls')}


def ENTSOE(update=False, raw=False, entsoe_token=None, config=None):
    """
    Importer for the list of installed generators provided by the ENTSO-E
    Trasparency Project. Geographical information is not given.
    If update=True, the dataset is parsed through a request to
    'https://transparency.entsoe.eu/generation/r2/\
    installedCapacityPerProductionUnit/show',
    Internet connection requiered. If raw=True, the same request is done, but
    the unprocessed data is returned.

    Parameters
    ----------
    update : Boolean, Default False
        Whether to update the database through a request to the ENTSO-E
        transparency plattform
    raw : Boolean, Default False
        Whether to return the raw data, obtained from the request to
        the ENTSO-E transparency platform
    entsoe_token: String
        Security token of the ENTSO-E Transparency platform
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    Note: For obtaining a security token refer to section 2 of the
    RESTful API documentation of the ENTSOE-E Transparency platform
    https://transparency.entsoe.eu/content/static_content/Static%20content/
    web%20api/Guide.html#_authentication_and_authorisation. Please save the
    token in your config.yaml file (key 'entsoe_token').
    """
    if config is None:
        config = get_config()

    if update or raw:
        if config['entsoe_token'] is not np.nan:
            entsoe_token = config['entsoe_token']
        assert entsoe_token is not None, "entsoe_token is missing"

        if update:
            fn = _data_out('aggregations/aggregation_groups_ENTSOE.csv')
            if os.path.isfile(fn):
                os.remove(fn)

        def full_country_name(l):
            def pycountry_try(c):
                try:
                    return pycountry.countries.get(alpha_2=c).name
                except KeyError:
                    return None
            if isinstance(l, string_types):
                return filter(None, [pycountry_try(l)])
            else:  # iterable
                return filter(None, [pycountry_try(country) for country in l])

        domains = pd.read_csv(_data('in/entsoe-areamap.csv'), sep=';',
                              header=None)
        # Search for Country abbreviations in each Name
        pattern = '|'.join(('(?i)'+x) for x in config['target_countries'])
        domains = domains.assign(Country=domains[1]
                                 .str.findall(pattern)
                                 .str.join(', '))
        found = (domains[1]
                 .replace('[0-9]', '', regex=True)
                 .str.split(' |,|\+|\-')
                 .apply(full_country_name).str.join(sep=', ')
                 .str.findall(pattern)
                 .str.join(sep=', ').str.strip())
        domains.Country = (domains.loc[:, 'Country'].fillna('')
                           .str.cat(found.fillna(''), sep=', ')
                           .str.replace('^ ?, ?|, ?$', '').str.strip())
        domains.Country.replace('', np.NaN, inplace=True)
        domains.Country = (domains.loc[domains.Country.notnull(), 'Country']
                           .apply(lambda x:
                                  ', '.join(list(set(x.split(', '))))))
        fdict = {'A03': 'Mixed',
                 'A04': 'Generation',
                 'A05': 'Load',
                 'B01': 'Biomass',
                 'B02': 'Lignite',  # 'Fossil Brown coal/Lignite',
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
        level2 = ['voltage_PowerSystemResources.highVoltageLimit', 'psrType']
        level3 = ['quantity']

        def namespace(element):
            m = re.match('\{.*\}', element.tag)
            return m.group(0) if m else ''

        def attribute(etree_sel):
            return etree_sel.text

        entsoe = pd.DataFrame()
        for i in domains.index[domains.Country.notnull()]:
            logger.info("Fetching power plants for domain %s (%s)",
                        domains.loc[i, 0],
                        domains.loc[i, 'Country'])

            # https://transparency.entsoe.eu/content/static_content/
            # Static%20content/web%20api/Guide.html_generation_domain
            ret = requests.get('https://transparency.entsoe.eu/api',
                               params=dict(securityToken=entsoe_token,
                                           documentType='A71',
                                           processType='A33',
                                           In_Domain=domains.loc[i, 0],
                                           periodStart='201512312300',
                                           periodEnd='201612312300'))
            try:
                # Create an ElementTree object
                etree = ET.fromstring(ret.content)
            except ET.ParseError:
                # hack for dealing with unencoded '&' in ENTSOE-API
                etree = ET.fromstring(re.sub(r'&(?=[^;]){6}', r'&amp;',
                                             ret.text).encode('utf-8'))
            ns = namespace(etree)
            df = pd.DataFrame(columns=level1+level2+level3+['Country'])
            for arg in level1:
                df[arg] = [attribute(e) for e in
                           etree.findall('*/%s%s' % (ns, arg))]
            for arg in level2:
                df[arg] = [attribute(e) for e in
                           etree.findall('*/*/%s%s' % (ns, arg))]
            for arg in level3:
                df[arg] = [attribute(e) for e in
                           etree.findall('*/*/*/%s%s' % (ns, arg))]
            df['Country'] = domains.loc[i, 'Country']
            logger.info("Received data on %d power plants", len(df))
            entsoe = entsoe.append(df, ignore_index=True)

        if raw:
            return entsoe

        entsoe = (
                entsoe
                .rename(columns={'psrType': 'Fueltype',
                                 'quantity': 'Capacity',
                                 'registeredResource.mRID': 'projectID',
                                 'registeredResource.name': 'Name'})
                .reindex(columns=config['target_columns'])
                .replace({'Fueltype': fdict})
                .assign(Country_length=lambda df: df.Country.str.len())
                .sort_values('Country_length')
                .drop_duplicates('projectID')
                .sort_values('Country')
                .reset_index(drop=True)
                .assign(Name=lambda df: df.Name.str.title(),
                        file='''https://transparency.entsoe.eu/generation/
                        r2/installedCapacityPerProductionUnit/''',
                        Fueltype=lambda df: df.Fueltype.replace(
                                {'Fossil Hard coal': 'Hard Coal',
                                 'Fossil Coal-derived gas': 'Other',
                                 '.*Hydro.*': 'Hydro',
                                 '.*Oil.*': 'Oil',
                                 '.*Peat': 'Bioenergy',
                                 'Biomass': 'Bioenergy',
                                 'Fossil Gas': 'Natural Gas',
                                 'Marine': 'Other',
                                 'Wind Offshore': 'Offshore',
                                 'Wind Onshore': 'Onshore'}, regex=True),
                        Capacity=lambda df: pd.to_numeric(df.Capacity))
                .pipe(clean_powerplantname)
                .pipe(fill_geoposition, use_saved_locations=True)
                .pipe(config_filter, config=config)
                .replace({'Capacity': {0.: np.nan}})
                .dropna(subset=['Capacity'])
                .pipe(gather_technology_info, config=config)
                .pipe(gather_set_info)
                .pipe(clean_technology))

        entsoe.to_csv(data_config['ENTSOE']['source_file'],
                      index_label='id', encoding='utf-8')

    else:
        entsoe = pd.read_csv(data_config['ENTSOE']['source_file'],
                             index_col='id', encoding='utf-8')

    return (entsoe[entsoe.Country.isin(config['target_countries'])]
            .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
            .pipe(scale_to_net_capacities,
                  (not data_config['ENTSOE']['net_capacity']))
            .pipe(correct_manually, 'ENTSOE', config=config))


data_config['ENTSOE'] = {'read_function': ENTSOE,
                         'reliability_score': 4,
                         'net_capacity': True,
                         'source_file': _data_in('entsoe_powerplants.csv')}


def WEPP(raw=False, config=None):
    """
    Importer for the standardized WEPP (Platts, World Elecrtric Power
    Plants Database). This database is not provided by this repository because
    of its restrictive licence.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """
    if config is None:
        config = get_config()

    # Define the appropriate datatype for each column (some columns e.g.
    # 'YEAR' cannot be integers, as there are N/A values, which np.int
    # does not yet(?) support.)
    datatypes = {'UNIT': str, 'PLANT': str, 'COMPANY': str, 'MW': np.float64,
                 'STATUS': str, 'YEAR': np.float64, 'UTYPE': str, 'FUEL': str,
                 'FUELTYPE': str, 'ALTFUEL': str, 'SSSMFR': str,
                 'BOILTYPE': str, 'TURBMFR': str, 'TURBTYPE': str,
                 'GENMFR': str, 'GENTYPE': str, 'SFLOW': np.float64,
                 'SPRESS': np.float64, 'STYPE': str, 'STEMP': np.float64,
                 'REHEAT1': np.float64, 'REHEAT2': np.float64, 'PARTCTL': str,
                 'PARTMFR': str, 'SO2CTL': str, 'FGDMFR': str, 'NOXCTL': str,
                 'NOXMFR': str, 'AE': str, 'CONstr, UCT': str, 'COOL': str,
                 'RETIRE': np.float64, 'CITY': str, 'STATE': str,
                 'COUNTRY': str, 'AREA': str, 'SUBREGION': str,
                 'POSTCODE': str, 'PARENT': str, 'ELECTYPE': str,
                 'BUSTYPE': str, 'COMPID': str, 'LOCATIONID': str,
                 'UNITID': str}
    # Now read the Platts WEPP Database
    wepp = pd.read_csv(data_config['WEPP']['source_file'], dtype=datatypes,
                       encoding='utf-8')
    if raw:
        return wepp

    # Fit WEPP-column names to our specifications
    wepp.columns = wepp.columns.str.title()
    wepp.rename(columns={'Unit': 'Name',
                         'Fuel': 'Fueltype',
                         'Fueltype': 'Technology',
                         'Mw': 'Capacity',
                         'Year': 'YearCommissioned',
                         #  'Retire':'YearDecommissioned',
                         'Lat': 'lat',
                         'Lon': 'lon',
                         'Unitid': 'projectID'}, inplace=True)
    wepp.loc[:, 'Retrofit'] = wepp.YearCommissioned
    # Do country transformations and drop those which are not in definded scope
    c = {'ENGLAND & WALES': u'UNITED KINGDOM',
         'GIBRALTAR': u'SPAIN',
         'SCOTLAND': u'UNITED KINGDOM'}
    wepp.Country = wepp.Country.replace(c).str.title()
    wepp = (wepp.loc[lambda df: df.Country.isin(config['target_countries'])]
                .loc[lambda df: df.Status.isin(['OPR', 'CON'])]
                .assign(File=data_config['WEPP']['source_file']))
    # Replace fueltypes
    d = {'AGAS': 'Bioenergy',    # Syngas from gasified agricultural waste
         'BFG': 'Other',         # blast furnance gas -> "Hochofengas"
         'BGAS': 'Bioenergy',
         'BIOMASS': 'Bioenergy',
         'BL': 'Bioenergy',
         'CGAS': 'Hard Coal',
         'COAL': 'Hard Coal',
         'COG': 'Other',         # coke oven gas -> deutsch: "Hochofengas"
         'COKE': 'Hard Coal',
         'CSGAS': 'Hard Coal',   # Coal-seam-gas
         'CWM': 'Hard Coal',     # Coal-water mixture (aka coal-water slurry)
         'DGAS': 'Other',        # sewage digester gas -> deutsch: "Klaergas"
         'FGAS': 'Other',        # Flare gas or wellhead gas or associated gas
         'GAS': 'Natural Gas',
         'GEO': 'Geothermal',
         'H2': 'Other',          # Hydrogen gas
         'HZDWST': 'Waste',      # Hazardous waste
         'INDWST': 'Waste',      # Industrial waste or refinery waste
         'JET': 'Oil',           # Jet fuels
         'KERO': 'Oil',          # Kerosene
         'LGAS': 'Other',        # landfill gas -> deutsch: "Deponiegas"
         'LIGNIN': 'Bioenergy',
         'LIQ': 'Other',         # (black) liqour -> deutsch: "Schwarzlauge",
                                 #    die bei Papierherstellung anfaellt
         'LNG': 'Natural Gas',   # Liquified natural gas
         'LPG': 'Natural Gas',   # Liquified petroleum gas (u. butane/propane)
         'MBM': 'Bioenergy',     # Meat and bonemeal
         'MEDWST': 'Bioenergy',  # Medical waste
         'MGAS': 'Other',        # mine gas -> deutsch: "Grubengas"
         'NAP': 'Oil',           # naphta
         'OGAS': 'Oil',          # Gasified crude oil/refinery bottoms/bitumen
         'PEAT': 'Other',
         'REF': 'Waste',
         'REFGAS': 'Other',      # Syngas from gasified refuse
         'RPF': 'Waste',         # Waste paper and/or waste plastic
         'PWST': 'Other',        # paper mill waste
         'RGAS': 'Other',        # refinery off-gas -> deutsch: "Raffineriegas"
         'SHALE': 'Oil',
         'SUN': 'Solar',
         'TGAS': 'Other',        # top gas -> deutsch: "Hochofengas"
         'TIRES': 'Other',       # Scrap tires
         'UNK': 'Other',
         'UR': 'Nuclear',
         'WAT': 'Hydro',
         'WOOD': 'Bioenergy',
         'WOODGAS': 'Bioenergy',
         'WSTGAS': 'Other',      # waste gas -> deutsch: "Industrieabgas"
         'WSTWSL': 'Waste',      # Wastewater sludge
         'WSTH': 'Waste'}
    wepp.Fueltype = wepp.Fueltype.replace(d)
    # Fill NaNs to allow str actions
    wepp.Technology.fillna('', inplace=True)
    wepp.Turbtype.fillna('', inplace=True)
    # Correct technology infos:
    wepp.loc[wepp.Technology.str.contains('LIG', case=False),
             'Fueltype'] = 'Lignite'
    wepp.loc[wepp.Turbtype.str.contains('KAPLAN|BULB', case=False),
             'Technology'] = 'Run-Of-River'
    wepp.Technology = wepp.Technology.replace({'CONV/PS': 'Pumped Storage',
                                               'CONV': 'Reservoir',
                                               'PS': 'Pumped Storage'})
    tech_st_pattern = ['ANTH', 'BINARY', 'BIT', 'BIT/ANTH', 'BIT/LIG',
                       'BIT/SUB', 'BIT/SUB/LIG', 'COL', 'DRY ST', 'HFO', 'LIG',
                       'LIG/BIT', 'PWR', 'RDF', 'SUB']
    tech_ocgt_pattern = ['AGWST', 'LITTER', 'RESID', 'RICE', 'STRAW']
    tech_ccgt_pattern = ['LFO']
    wepp.loc[wepp.Technology.isin(tech_st_pattern),
             'Technology'] = 'Steam Turbine'
    wepp.loc[wepp.Technology.isin(tech_ocgt_pattern), 'Technology'] = 'OCGT'
    wepp.loc[wepp.Technology.isin(tech_ccgt_pattern), 'Technology'] = 'CCGT'
    ut_ccgt_pattern = ['CC', 'GT/C', 'GT/CP', 'GT/CS', 'GT/ST', 'ST/C',
                       'ST/CC/GT', 'ST/CD', 'ST/CP', 'ST/CS', 'ST/GT',
                       'ST/GT/IC', 'ST/T', 'IC/CD', 'IC/CP', 'IC/GT']
    ut_ocgt_pattern = ['GT', 'GT/D', 'GT/H', 'GT/HY', 'GT/IC', 'GT/S', 'GT/T',
                       'GTC']
    ut_st_pattern = ['ST', 'ST/D']
    ut_ic_pattern = ['IC', 'IC/H']
    wepp.loc[wepp.Utype.isin(ut_ccgt_pattern), 'Technology'] = 'CCGT'
    wepp.loc[wepp.Utype.isin(ut_ocgt_pattern), 'Technology'] = 'OCGT'
    wepp.loc[wepp.Utype.isin(ut_st_pattern), 'Technology'] = 'Steam Turbine'
    wepp.loc[wepp.Utype.isin(ut_ic_pattern),
             'Technology'] = 'Combustion Engine'
    wepp.loc[wepp.Utype == 'WTG', 'Technology'] = 'Onshore'
    wepp.loc[wepp.Utype == 'WTG/O', 'Technology'] = 'Offshore'
    wepp.loc[(wepp.Fueltype == 'Solar') & (wepp.Utype.isin(ut_st_pattern)),
             'Technology'] = 'CSP'
    # Derive the SET column
    chp_pattern = ['CC/S', 'CC/CP', 'CCSS/P', 'GT/CP', 'GT/CS', 'GT/S', 'GT/H',
                   'IC/CP', 'IC/H', 'ST/S', 'ST/H', 'ST/CP', 'ST/CS', 'ST/D']
    wepp.loc[wepp.Utype.isin(chp_pattern), 'Set'] = 'CHP'
    wepp.loc[wepp.Set.isnull(), 'Set'] = 'PP'
    # Clean up the mess
    wepp.Fueltype = wepp.Fueltype.str.title()
    wepp.loc[wepp.Technology.str.len() > 4, 'Technology'] = \
        wepp.loc[wepp.Technology.str.len() > 4, 'Technology'].str.title()
    # Done!
    wepp.datasetID = 'WEPP'
    return (wepp.loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
                .reset_index(drop=True)
                .pipe(scale_to_net_capacities,
                      (not data_config['WEPP']['net_capacity']))
                .pipe(correct_manually, 'WEPP', config=config))


data_config['WEPP'] = {
        'read_function': WEPP,
        'reliability_score': 4,
        'net_capacity': False,
        'source_file': _data_in('platts_wepp.csv')}


def UBA(header=9, skipfooter=26, prune_wind=True, prune_solar=True,
        config=None):
    """
    Importer for the UBA Database. Please download the data from
    ``https://www.umweltbundesamt.de/dokument/datenbank-kraftwerke-in
    -deutschland`` and place it in ``powerplantmatching/data/in``.

    Parameters:
    -----------
    header : int, Default 9
        The zero-indexed row in which the column headings are found.
    skipfooter : int, Default 26
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """
    if config is None:
        config = get_config()

    uba = pd.read_excel(data_config['UBA']['source_file'], header=header,
                        skipfooter=skipfooter, na_values='n.b.')
    uba = uba.rename(columns={
            u'Kraftwerksname / Standort': 'Name',
            u'Elektrische Bruttoleistung (MW)': 'Capacity',
            u'Inbetriebnahme  (ggf. Ertüchtigung)': 'YearCommissioned',
            u'Primärenergieträger': 'Fueltype',
            u'Anlagenart': 'Technology',
            u'Fernwärme-leistung (MW)': 'CHP',
            u'Standort-PLZ': 'PLZ'})
    from .heuristics import PLZ_to_LatLon_map
    uba = (uba.assign(
            Name=uba.Name.replace({'\s\s+': ' '}, regex=True),
            lon=uba.PLZ.map(PLZ_to_LatLon_map()['lon']),
            lat=uba.PLZ.map(PLZ_to_LatLon_map()['lat']),
            YearCommissioned=uba.YearCommissioned.str.replace(
                    "\(|\)|\/|\-", " ").str.split(' ').str[0].astype(float),
            Country='Germany',
            File='kraftwerke-de-ab-100-mw.xls',
            projectID=['UBA{:03d}'.format(i + header + 2) for i in uba.index],
            Technology=uba.Technology.replace({
                    u'DKW': 'Steam Turbine',
                    u'DWR': 'Pressurized Water Reactor',
                    u'G/AK': 'Steam Turbine',
                    u'GT': 'OCGT',
                    u'GuD': 'CCGT',
                    u'GuD / HKW': 'CCGT',
                    u'HKW': 'Steam Turbine',
                    u'HKW (DT)': 'Steam Turbine',
                    u'HKW / GuD': 'CCGT',
                    u'HKW / SSA': 'Steam Turbine',
                    u'IKW': 'OCGT',
                    u'IKW / GuD': 'CCGT',
                    u'IKW / HKW': 'Steam Turbine',
                    u'IKW / HKW / GuD': 'CCGT',
                    u'IKW / SSA': 'OCGT',
                    u'IKW /GuD': 'CCGT',
                    u'LWK': 'Run-Of-River',
                    u'PSW': 'Pumped Storage',
                    u'SWK': 'Reservoir Storage',
                    u'SWR': 'Boiled Water Reactor'})))
    uba.loc[uba.CHP.notnull(), 'Set'] = 'CHP'
    uba = uba.pipe(gather_set_info)
    uba.loc[uba.Fueltype == 'Wind (O)', 'Technology'] = 'Offshore'
    uba.loc[uba.Fueltype == 'Wind (L)', 'Technology'] = 'Onshore'
    uba.loc[uba.Fueltype.str.contains('Wind'), 'Fueltype'] = 'Wind'
    uba.loc[uba.Fueltype.str.contains('Braunkohle'), 'Fueltype'] = 'Lignite'
    uba.loc[uba.Fueltype.str.contains('Steinkohle'), 'Fueltype'] = 'Hard Coal'
    uba.loc[uba.Fueltype.str.contains('Erdgas'), 'Fueltype'] = 'Natural Gas'
    uba.loc[uba.Fueltype.str.contains('HEL'), 'Fueltype'] = 'Oil'
    uba.Fueltype = uba.Fueltype.replace({u'Biomasse': 'Bioenergy',
                                         u'Gichtgas': 'Other',
                                         u'HS': 'Oil',
                                         u'Konvertergas': 'Other',
                                         u'Licht': 'Solar',
                                         u'Raffineriegas': 'Other',
                                         u'Uran': 'Nuclear',
                                         u'Wasser': 'Hydro',
                                         u'\xd6lr\xfcckstand': 'Oil'})
    uba.Name.replace([r'(?i)oe', r'(?i)ue'], [u'ö', u'ü'], regex=True,
                     inplace=True)
    if prune_wind:
        uba = uba.loc[lambda x: x.Fueltype != 'Wind']
    if prune_solar:
        uba = uba.loc[lambda x: x.Fueltype != 'Solar']
    return (uba.loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
            .pipe(scale_to_net_capacities,
                  (not data_config['UBA']['net_capacity']))
            .pipe(correct_manually, 'UBA', config=config))


data_config['UBA'] = {
        'read_function': UBA,
        'aggregated_units': False,
        'net_capacity': False,
        'reliability_score': 5,
        'source_file': _data_in('kraftwerke-de-ab-100-mw.xls')}


def BNETZA(header=9, sheet_name='Gesamtkraftwerksliste BNetzA',
           prune_wind=True, prune_solar=True, raw=False,
           config=None):
    """
    Importer for the database put together by Germany's 'Federal Network
    Agency' (dt. 'Bundesnetzagentur' (BNetzA)).
    Please download the data from
    ``https://www.bundesnetzagentur.de/DE/Sachgebiete/ElektrizitaetundGas/
    Unternehmen_Institutionen/Versorgungssicherheit/Erzeugungskapazitaeten/
    Kraftwerksliste/kraftwerksliste-node.html``
    and place it in ``powerplantmatching/data/in``.

    Parameters:
    -----------
    header : int, Default 9
        The zero-indexed row in which the column headings are found.
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    if config is None:
        config = get_config()

    bnetza = pd.read_excel(data_config['BNETZA']['source_file'], header=header,
                           sheet_name=sheet_name, encoding='utf-8')
    if raw:
        return bnetza
    bnetza = bnetza.rename(columns={
            u'Kraftwerksnummer Bundesnetzagentur': 'projectID',
            u'Kraftwerksname': 'Name',
            u'Netto-Nennleistung (elektrische Wirkleistung) in MW': 'Capacity',
            u'Wärmeauskopplung (KWK)\n(ja/nein)': 'Set',
            u'Ort\n(Standort Kraftwerk)': 'Ort',
            (u'Auswertung\nEnergieträger (Zuordnung zu einem '
             u'Hauptenergieträger bei Mehreren Energieträgern)'): 'Fueltype',
            (u'Kraftwerksstatus \n(in Betrieb/\nvorläufig '
             u'stillgelegt/\nsaisonale Konservierung\nGesetzlich an '
             u'Stilllegung gehindert/\nSonderfall)'): 'Status',
            (u'Aufnahme der kommerziellen Stromerzeugung der derzeit in '
             u'Betrieb befindlichen Erzeugungseinheit\n(Jahr)'):
            'YearCommissioned',
            u'PLZ\n(Standort Kraftwerk)': 'PLZ'})
    # If BNetzA-Name is empty replace by company, if this is empty by city.

    from .heuristics import PLZ_to_LatLon_map

    pattern = '|'.join(['.*(?i)betrieb', '.*(?i)gehindert', '(?i)vorl.*ufig.*',
                        'Sicherheitsbereitschaft', 'Sonderfall'])

    bnetza = (bnetza.assign(
              lon=bnetza.PLZ.map(PLZ_to_LatLon_map()['lon']),
              lat=bnetza.PLZ.map(PLZ_to_LatLon_map()['lat']),
              Name=bnetza.Name.where(bnetza.Name.str.len().fillna(0) > 4,
                                     bnetza.Unternehmen + ' ' +
                                     bnetza.Name.fillna(''))
                              .fillna(bnetza.Ort),
              YearCommissioned=bnetza.YearCommissioned
              .astype(text)
              .str.replace("[^0-9.]", " ")
              .str.split(' ')
              .str[0].replace('', np.nan)
              .astype(float),
              Blockname=bnetza.Blockname.replace
              (to_replace=['.*(GT|gasturbine).*',
                           '.*(DT|HKW|(?i)dampfturbine|(?i)heizkraftwerk).*',
                           '.*GuD.*'],
               value=['OCGT', 'Steam Turbine', 'CCGT'],
               regex=True))
              [lambda df: df.projectID.notna() &
               df.Status.str.contains(pattern, regex=True, case=False)]
              .pipe(gather_technology_info,
                    search_col=['Name', 'Fueltype', 'Blockname'],
                    config=config))

    add_location_b = (bnetza[bnetza.Ort.notnull()]
                      .apply(lambda ds: (ds['Ort'] not in ds['Name'])
                             and (text.title(ds['Ort']) not in ds['Name']),
                             axis=1))
    bnetza.loc[bnetza.Ort.notnull() & add_location_b, 'Name'] = (
                bnetza.loc[bnetza.Ort.notnull() & add_location_b, 'Ort']
                + ' '
                + bnetza.loc[bnetza.Ort.notnull() & add_location_b, 'Name'])
    bnetza.Name.replace('\s+', ' ', regex=True, inplace=True)

    techmap = {'solare': 'PV',
               'Laufwasser': 'Run-Of-River',
               'Speicherwasser': 'Reservoir',
               'Pumpspeicher': 'Pumped Storage'}
    for fuel in techmap:
        bnetza.loc[bnetza.Fueltype.str.contains(fuel, case=False),
                   'Technology'] = techmap[fuel]
    # Fueltypes
    bnetza.Fueltype.replace(
            to_replace=['(.*(?i)wasser.*|Pump.*)', 'Erdgas', 'Steinkohle',
                        'Braunkohle', 'Wind.*', 'Solar.*',
                        '.*(?i)energietr.*ger.*\n.*', 'Kern.*', 'Mineral.l.*',
                        'Biom.*', '.*(?i)(e|r|n)gas', 'Geoth.*', 'Abfall'],
            value=['Hydro', 'Natural Gas', 'Hard Coal', 'Lignite', 'Wind',
                   'Solar', 'Other', 'Nuclear', 'Oil', 'Bioenergy', 'Other',
                   'Geothermal', 'Waste'],
            regex=True, inplace=True)
    if prune_wind:
        bnetza = bnetza[lambda x: x.Fueltype != 'Wind']
    if prune_solar:
        bnetza = bnetza[lambda x: x.Fueltype != 'Solar']
    # Filter by country
    bnetza = bnetza[~bnetza.Bundesland.isin([u'Österreich', 'Schweiz',
                                             'Luxemburg'])]
    return (bnetza.assign(Country='Germany',
                          File=data_config['BNETZA']['source_file']
                          .split('/')[-1],
                          Set=bnetza.Set.fillna('Nein')
                          .str.title()
                          .replace({u'Ja': 'CHP', u'Nein': 'PP'}))
            .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
            .pipe(scale_to_net_capacities,
                  not data_config['BNETZA']['net_capacity'])
            .pipe(correct_manually, 'BNETZA', config=config)
            .reset_index(drop=True))


data_config['BNETZA'] = {'read_function': BNETZA, 'net_capacity': True,
                         'reliability_score': 3,
                         'source_file': _data_in('Kraftwerksliste_'
                                                 '2017_2.xlsx')}


def OPSD_VRE(config=None):
    """
    Importer for the OPSD (Open Power Systems Data) renewables (VRE)
    database.

    This sqlite database is very big and hence not part of the package.
    It needs to be obtained here:
        http://data.open-power-system-data.org/renewable_power_plants/

    Parameters
    ----------
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    if config is None:
        config = get_config()

    def read_opsd_res(country):
        import pandas as pd
        import sqlite3
        db = sqlite3.connect(_data_in('renewable_power_plants.sqlite'))
        if country == 'CH':
            cur = db.execute(
                    "SELECT"
                    "   substr(commissioning_date,1,4), "
                    "   energy_source_level_2, technology, "
                    "   electrical_capacity, lat, lon "
                    "FROM"
                    "   renewable_power_plants_CH "
            )
        elif country == 'DE':
            cur = db.execute(
                    "SELECT"
                    "   substr(commissioning_date,1,4), "
                    "   energy_source_level_2, technology, "
                    "   electrical_capacity, lat, lon "
                    "FROM"
                    "   renewable_power_plants_DE "
                    # "AND NOT"
                    # "   comment LIKE '%R_%'"
            )
        elif country == 'DK':
            cur = db.execute(
                    "SELECT"
                    "   substr(commissioning_date,1,4), "
                    "   energy_source_level_2, technology, "
                    "   electrical_capacity, lat, lon "
                    "FROM"
                    "   renewable_power_plants_DK "
            )
        else:
            raise NotImplementedError(
                    "The country '{0}' is not supported yet.".format(country))

        df = pd.DataFrame(cur.fetchall(),
                          columns=['YearCommissioned', 'Fueltype',
                                   'Technology', 'Capacity', 'lat', 'lon'])
        df.loc[:, 'Country'] = pycountry.countries.get(alpha_2=country).name
        df.loc[:, 'projectID'] = pd.Series(
                ['OPSD-VRE_{}_{}'.format(country, i) for i in df.index])
        return df

    df = pd.concat((read_opsd_res(r) for r in ['DE', 'DK', 'CH']),
                   ignore_index=True)
    df = (df.assign(Retrofit=df.YearCommissioned,
                    Country=df.Country.str.title(),
                    File='renewable_power_plants.sqlite',
                    Set='PP')
            .replace({'NaT': np.NaN,
                      None: np.NaN,
                      '': np.NaN}))
    for col in ['YearCommissioned', 'Retrofit', 'Capacity', 'lat', 'lon']:
        df.loc[:, col] = df[col].astype(np.float)
    d = {u'Connected unit': 'PV',
         u'Integrated unit': 'PV',
         u'Photovoltaics': 'PV',
         u'Photovoltaics ground': 'PV',
         u'Stand alone unit': 'PV',
         u'Onshore wind energy': 'Onshore',
         u'Offshore wind energy': 'Offshore'}
    df.Technology.replace(d, inplace=True)
    return (df.reindex(columns=config['target_columns'])
              .loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
              .reset_index(drop=True)
              .drop('Name', axis=1))


def IRENA_stats(config=None):
    """
    Reads the IRENA Capacity Statistics 2017 Database

    Parameters
    ----------
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    if config is None:
        config = get_config()

    # Read the raw dataset
    df = pd.read_csv(_data_in('IRENA_CapacityStatistics2017.csv'),
                     encoding='utf-8')
    # "Unpivot"
    df = pd.melt(df, id_vars=['Indicator', 'Technology', 'Country'],
                 var_name='Year',
                 value_vars=[text(i) for i in range(2000, 2017, 1)],
                 value_name='Capacity')
    # Drop empty
    df.dropna(axis=0, subset=['Capacity'], inplace=True)
    # Drop generations
    df = df[df.Indicator == 'Electricity capacity (MW)']
    df.drop('Indicator', axis=1, inplace=True)
    # Drop countries out of scope
    df.Country.replace({'Czechia': u'Czech Republic',
                        'UK': u'United Kingdom'}, inplace=True)
    df = df.loc[lambda df: df.Country.isin(config['target_countries'])]
    # Convert to numeric
    df.Year = df.Year.astype(int)
    df.Capacity = df.Capacity.str.strip().str.replace(' ', '').astype(float)
    # Handle Fueltypes and Technologies
    d = {u'Bagasse': 'Bioenergy',
         u'Biogas': 'Bioenergy',
         u'Concentrated solar power': 'Solar',
         u'Geothermal': 'Geothermal',
         u'Hydro 1-10 MW': 'Hydro',
         u'Hydro 10+ MW': 'Hydro',
         u'Hydro <1 MW': 'Hydro',
         u'Liquid biofuels': 'Bioenergy',
         u'Marine': 'Hydro',
         u'Mixed and pumped storage': 'Hydro',
         u'Offshore wind energy': 'Wind',
         u'Onshore wind energy': 'Wind',
         u'Other solid biofuels': 'Bioenergy',
         u'Renewable municipal waste': 'Waste',
         u'Solar photovoltaic': 'Solar'}
    df.loc[:, 'Fueltype'] = df.Technology.map(d)
    df = df.loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
    d = {u'Concentrated solar power': 'CSP',
         u'Solar photovoltaic': 'PV',
         u'Onshore wind energy': 'Onshore',
         u'Offshore wind energy': 'Offshore'}
    df.Technology.replace(d, inplace=True)
    df.loc[:, 'Set'] = 'PP'
    return df.reset_index(drop=True)

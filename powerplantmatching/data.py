# -*- coding: utf-8 -*-
# Copyright 2016-2018 Fabian Hofmann (FIAS), Jonas Hoersch (KIT, IAI) and
# Fabian Gotzens (FZJ, IEK-STE)

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

import numpy as np
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import re
import pycountry
import logging
import entsoe as entsoe_api

from .core import get_config, _package_data, _data_in, package_config
from .utils import (parse_if_not_stored, fill_geoposition, correct_manually,
                    config_filter, set_column_name)
from .heuristics import scale_to_net_capacities
from .cleaning import (gather_fueltype_info, gather_set_info,
                       gather_technology_info, clean_powerplantname,
                       clean_technology)

logger = logging.getLogger(__name__)
cget = pycountry.countries.get
net_caps = get_config()['display_net_caps']


def OPSD(rawEU=False, rawDE=False, update=False,
         statusDE=['operating', 'reserve', 'special_case', 'shutdown_temporary'],
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
    config = get_config() if config is None else config

    opsd_DE = parse_if_not_stored('OPSD_DE', update, config, na_values=' ')
    opsd_EU = parse_if_not_stored('OPSD_EU', update, config, na_values=' ')
    if rawEU and rawDE:
        raise(NotImplementedError('''
                It is not possible to show both DE and EU raw databases at the
                same time as they have different formats. Choose only one!
                '''))
    if rawEU:
        return opsd_EU
    if rawDE:
        return opsd_DE
    opsd_EU = (opsd_EU.rename(columns=str.title)
                     .rename(columns={'Lat': 'lat',
                                      'Lon': 'lon',
                                      'Energy_Source': 'Fueltype',
                                      'Commissioned': 'YearCommissioned',
                                      'Eic_Code': 'eic_code'})
                     .eval('Retrofit = YearCommissioned')
                     .assign(projectID = lambda s: 'OEU' +
                             pd.Series(s.index.astype(str), s.index))
                     .reindex(columns=config['target_columns']))

    opsd_DE = (opsd_DE.rename(columns=str.title)
                      .rename(columns={'Lat': 'lat',
                                       'Lon': 'lon',
                                       'Fuel': 'Fueltype',
                                       'Type': 'Set',
                                       'Country_Code': 'Country',
                                       'Capacity_Net_Bnetza': 'Capacity',
                                       'Commissioned': 'YearCommissioned',
                                       'Eic_Code_Plant': 'eic_code',
                                       'Id': 'projectID'})
                      .assign(
                      Name = lambda d: d.Name_Bnetza.fillna(d.Name_Uba),
                      Fueltype = lambda d:
                                  d.Fueltype.fillna(d.Energy_Source_Level_1),
                      Retrofit = lambda d:
                                  d.Retrofit.fillna(d.YearCommissioned)))
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
                    Fueltype=lambda df: df.Fueltype.str.title().str.strip())
            .powerplant.convert_alpha2_to_country()
            .pipe(set_column_name, 'OPSD')
#            .pipe(correct_manually, 'OPSD', config=config)
            .pipe(config_filter, name='OPSD', config=config)
            .pipe(gather_set_info)
            .pipe(clean_technology))


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
    config = get_config() if config is None else config

    countries = config['target_countries']
    rename_cols = {'GEO_Assigned_Identification_Number': 'projectID',
                   'Name': 'Name',
                   'Type': 'Fueltype',
                   'Type_of_Plant_rng1': 'Technology',
                   'Type_of_Fuel_rng1_Primary': 'FuelClassification1',
                   'Type_of_Fuel_rng2_Secondary': 'FuelClassification2',
                   'Country': 'Country',
                   'Design_Capacity_MWe_nbr': 'Capacity',
                   'Year_Project_Commissioned': 'YearCommissioned',
                   'Year_rng1_yr1': 'Retrofit',
                   'Longitude_Start': 'lon',
                   'Latitude_Start': 'lat'}

#    geo = pd.read_csv(_data_in(config['GEO']['fn']), low_memory=False,
#                      usecols=list(rename_cols.keys()))
    geo = parse_if_not_stored('GEO', config=config, low_memory=False)
    if raw:
        return geo


    return (geo.rename(columns=rename_cols)
              .assign(Retrofit=lambda s: s.Retrofit.astype(float),
                      projectID=lambda s: 'GEO' + s.projectID.astype(str))
              .assign(YearCommissioned=(lambda s: s.YearCommissioned
                                        .str[:4]
                                        .apply(pd.to_numeric, errors='coerce')
                                        .where(lambda x: x > 1900)
                                        .fillna(s.Retrofit)))
              .assign(Retrofit=lambda s: s.Retrofit.fillna(s.YearCommissioned))
              .query("Country in @countries")
              .replace({col: {'Gas': 'Natural Gas'} for col in
                        {'Fueltype', 'FuelClassification1',
                         'FuelClassification2'}})
             .pipe(gather_fueltype_info, search_col=['FuelClassification1'])
             .pipe(gather_technology_info, search_col=['FuelClassification1'],
                          config=config)
             .pipe(gather_set_info)
             .pipe(set_column_name, 'GEO')
             .pipe(config_filter, name='GEO', config=config)
             .pipe(clean_powerplantname)
             .pipe(clean_technology, generalize_hydros=True)
             .pipe(scale_to_net_capacities,
                   (not config['GEO']['net_capacity']))
             .pipe(config_filter, name='GEO', config=config)
#             .pipe(correct_manually, 'GEO', config=config)
             )

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
    config = get_config() if config is None else config

#    carmadata = pd.read_csv(_datconfig['CARMA']['fn'], low_memory=False)
    carma = parse_if_not_stored('CARMA', config=config, low_memory=False)
    if raw: return carma

    return (carma
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
            .pipe(clean_powerplantname)
            .drop_duplicates()
            .pipe(set_column_name, 'CARMA')
            .pipe(config_filter, name='CARMA', config=config)
            .pipe(gather_technology_info, config=config)
            .pipe(gather_set_info)
            .pipe(clean_technology)
            .pipe(scale_to_net_capacities, not config['CARMA']['net_capacity'])
#            .pipe(correct_manually, 'CARMA', config=config)
            )

def JRC(raw=False, config=None, update=False):
    """
    Importer for the JRC Hydro-power plants database retrieves from
    https://github.com/energy-modelling-toolkit/hydro-power-database.
    """

    config = get_config() if config is None else config
    url = config['JRC']['url']
    default_url = get_config(_package_data('config.yaml'))['JRC']['url']

    err = IOError(f'The URL seems to be outdated, please copy the new url '
                  f'\n\n\t{default_url}\n\nin your custom config file '
                  f'\n\n\t{package_config["custom_config"]}\n\nunder tag "JRC" '
                  '-> "url"')

    def parse_func():
        import requests
        from zipfile import ZipFile
        from io import BytesIO
        parse = requests.get(url)
        if parse.ok:
            content = parse.content
        else:
            raise(err)
        file = ZipFile(BytesIO(content))
        key = 'jrc-hydro-power-plant-database.csv'
        if key in file.namelist():
            return pd.read_csv(file.open(key))
        else:
            raise(err)


    df = parse_if_not_stored('JRC', update, config, parse_func=parse_func)
    if raw:
        return df
    return (df.rename(columns={'id': 'projectID',
                              'name': 'Name',
                              'installed_capacity_MW': 'Capacity',
                              'country_code': 'Country',
                              'type': 'Technology',
                              'dam_height_m': 'DamHeight_m',
                              'volume_Mm3': 'Volume_Mm3'})
            .eval('Duration = storage_capacity_MWh / Capacity')
            .replace({'HDAM': 'Reservoir',
                      'HPHS': 'Pumped Storage',
                      'HROR': 'Run-Of-River'})
            .drop(columns = ['pypsa_id', 'GEO', 'storage_capacity_MWh'])
            .assign(Set='Store', Fueltype='Hydro')
            .powerplant.convert_alpha2_to_country()
            .pipe(config_filter))


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
    config = get_config() if config is None else config

    return (pd.read_csv(config['IWPDCY']['fn'],
                        encoding='utf-8', index_col='id')
            .assign(File='IWPDCY.csv',
                    projectID=lambda df: 'IWPDCY' + df.index.astype(str))
            .dropna(subset=['Capacity'])
            .pipe(set_column_name, 'IWPDCY')
            .pipe(config_filter, name='IWPDY', config=config)
            .pipe(gather_set_info)
            .pipe(correct_manually, 'IWPDCY', config=config))


def Capacity_stats(raw=False, level=2, config=None, update=False,
                   source='entsoe SO&AF', year='2016'):
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

    df = parse_if_not_stored('Capacity_stats', update, config, index_col=0)
    if raw: return df

    countries = config['target_countries']
    df = (df.query('source == @source & year == @year')
          .rename(columns={'technology': 'Fueltype'}).rename(columns=str.title)
          .powerplant.convert_alpha2_to_country()
#          .query('Country in @countries')
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
          .pipe(set_column_name, source.title()))
    return df


def GPD(raw=False, filter_other_dbs=True, update=False, config=None):
    """
    Importer for the `Global Power Plant Database`.

    Parameters
    ----------

    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """


    config = get_config() if config is None else config

    #if outdated have a look at
    #http://datasets.wri.org/dataset/globalpowerplantdatabase
    url = config['GPD']['url']
    def parse_func():
        import requests
        from zipfile import ZipFile
        from io import BytesIO
        parse = requests.get(url)
        if parse.ok:
            content = parse.content
        else:
            IOError(f'URL {url} seems to be outdated, please doulble the '
                          'address at http://datasets.wri.org/dataset/'
                          'globalpowerplantdatabase update the and update the '
                          'url in your custom config file '
                          '{package_config["custom_config"]}')
        return pd.read_csv(ZipFile(BytesIO(content))
                           .open('global_power_plant_database.csv'))

    df = parse_if_not_stored('GPD', update, config, parse_func, index_col=0)
    if raw: return df

    other_dbs = []
    if filter_other_dbs:
        other_dbs = ['GEODB', 'CARMA', 'Open Power System Data', 'ENTSOE']
    countries = config['target_countries']
    return (df.rename(columns=lambda x: x.title())
            .query("Country_Long in @countries &"
                   " Geolocation_Source not in @other_dbs")
            .drop(columns='Country')
            .rename(columns={'Gppd_Idnr': 'projectID',
                             'Country_Long': 'Country',
                             'Primary_Fuel': 'Fueltype',
                             'Latitude': 'lat',
                             'Longitude': 'lon',
                             'Capacity_Mw': 'Capacity',
#                             'Source': 'File'
                             'Commissioning_Year': 'YearCommissioned'})
            .replace(dict(Fueltype={'Coal': 'Hard Coal',
                                    'Biomass': 'Bioenergy',
                                    'Gas': 'Natural Gas',
                                    'Wave and Tidal': 'Other'}))
            .pipe(clean_powerplantname)
            .pipe(set_column_name, 'GPD')
            .pipe(config_filter, name='GPD', config=config)
#            .pipe(gather_technology_info, config=config)
#            .pipe(gather_set_info)
#            .pipe(correct_manually, 'GPD', config=config)
            )


#def WIKIPEDIA(raw=False):
#    from bs4 import BeautifulSoup
#
#    url = 'https://en.wikipedia.org/wiki/List_of_power_stations_in_Germany'
#
#    dfs = pd.read_html(url, attrs={"class": ["wikitable","wikitable sortable"]})
#    soup = BeautifulSoup(requests.get(url).text)
#    all_headers = [h.text for h in soup.find_all("h2")]
#    headers = [header[:-6] for header in all_headers if header[-6:] == '[edit]']
#    headers = headers[:len(dfs)]
#    df = pd.concat(dfs, keys=headers, axis=0, sort=True)
#

def ESE(raw=False, update=False, config=None):
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
    config = get_config() if config is None else config
    df = parse_if_not_stored('ESE', update, config, error_bad_lines=False)
    if raw: return df

    target_countries = config['target_countries']
    return (df.rename(columns=str.strip)
            .rename(columns={'Title': 'Name',
                             'Technology Mid-Type': 'Technology',
                             'Longitude': 'lon',
                             'Latitude': 'lat',
                             'Technology Broad Category': 'Fueltype'})
            .assign(Set='Store',
                    projectID='ESE' + df.index.astype(str),
                    YearCommissioned=lambda df: df['Commissioned'].str[-4:]
                                .apply(pd.to_numeric, errors='coerce'),
                    Capacity = df['Rated Power'] /1e3)
            .query("Status == 'Operational' & Country in @target_countries")
            .pipe(clean_powerplantname)
            .pipe(clean_technology, generalize_hydros=True)
            .replace(dict(Fueltype={u'Electro-chemical': 'Battery',
                                    u'Pumped Hydro Storage': 'Hydro'}))
            .pipe(set_column_name, 'ESE')
            .pipe(config_filter, name='ESE', config=config)
#            .pipe(correct_manually, 'ESE', config=config)
            )

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
    config = get_config() if config is None else config

    def parse_entsoe():
        assert entsoe_token is not None, "entsoe_token is missing"
        url = 'https://transparency.entsoe.eu/api'
        # retrieved from pd.read_html('https://transparency.entsoe.eu/content/stat
        # ic_content/Static%20content/web%20api/Guide.html#_request_methods')[-1]
        domains = list(entsoe_api.mappings.BIDDING_ZONES.values())

        level1 = ['registeredResource.name', 'registeredResource.mRID']
        level2 = ['voltage_PowerSystemResources.highVoltageLimit', 'psrType']
        level3 = ['quantity']

        def namespace(element):
            m = re.match('\{.*\}', element.tag)
            return m.group(0) if m else ''

        entsoe = pd.DataFrame()
        logger.info(f"Retrieving data from {url}")
        for domain in domains:
            ret = requests.get(url, params=dict(securityToken=entsoe_token,
                                           documentType='A71',
                                           processType='A33',
                                           In_Domain=domain,
                                           periodStart='201612312300',
                                           periodEnd='201712312300'))
            etree = ET.fromstring(ret.content)
            ns = namespace(etree)
            df_domain = pd.DataFrame(columns=level1+level2+level3+['Country'])
            for i, level in enumerate([level1, level2, level3]):
                for arg in level:
                    df_domain[arg] = [e.text for e
                      in etree.findall('*/'* (i+1) + ns + arg)]
            entsoe = entsoe.append(df_domain, ignore_index=True)
        return entsoe

    if config['entsoe_token'] is not None:
        entsoe_token = config['entsoe_token']
        df = parse_if_not_stored('ENTSOE', update, config, parse_entsoe)
    else:
        if update:
            logger.info('No entsoe_token in config.yaml given, '
                        'falling back to stored version.')
        df = parse_if_not_stored('ENTSOE', update, config)

    if raw: return df

    fuelmap = entsoe_api.mappings.PSRTYPE_MAPPINGS
    country_map_entsoe = pd.read_csv(_package_data('entsoe_country_codes.csv'),
                                     index_col=0).rename(index=str).Country
    countries = config['target_countries']

    return (df.rename(columns={'psrType': 'Fueltype',
                             'quantity': 'Capacity',
                             'registeredResource.mRID': 'projectID',
                             'registeredResource.name': 'Name'})
            .reindex(columns=config['target_columns'])
            .replace({'Fueltype': fuelmap})
            .drop_duplicates('projectID')
            .assign(eic_code = lambda df: df.projectID,
                    Country=lambda df: df.projectID.str[:2]
                                         .map(country_map_entsoe),
                    Name=lambda df: df.Name.str.title(),
                    Fueltype=lambda df: df.Fueltype.replace(
                            {'Fossil Hard coal': 'Hard Coal',
                             'Fossil Coal-derived gas': 'Other',
                             '.*Hydro.*': 'Hydro',
                             '.*Oil.*': 'Oil',
                             '.*Peat': 'Bioenergy',
                             'Fossil Brown coal/Lignite':'Lignite',
                             'Biomass': 'Bioenergy',
                             'Fossil Gas': 'Natural Gas',
                             'Marine': 'Other',
                             'Wind Offshore': 'Offshore',
                             'Wind Onshore': 'Onshore'}, regex=True),
                    Capacity=lambda df: pd.to_numeric(df.Capacity))
            .powerplant.convert_alpha2_to_country()
            .pipe(clean_powerplantname)
#            .query('Country in @countries')
            .pipe(fill_geoposition, use_saved_locations=True, saved_only=True)
            .query('Capacity > 0')
            .pipe(gather_technology_info, config=config)
            .pipe(gather_set_info)
            .pipe(clean_technology)
            .pipe(set_column_name, 'ENTSOE')
            .pipe(config_filter, name='ENTSOE', config=config)
#            .pipe(correct_manually, 'ENTSOE', config=config)
            )


#def OSM():
#    """
#    Parser and Importer for Open Street Map power plant data.
#    """
#    import requests
#    overpass_url = "http://overpass-api.de/api/interpreter"
#    overpass_query = """
#    [out:json][timeout:210];
#    area["name"="Luxembourg"]->.boundaryarea;
#    (
#    // query part for: “power=plant”
#    node["power"="plant"](area.boundaryarea);
#    way["power"="plant"](area.boundaryarea);
#    relation["power"="plant"](area.boundaryarea);
#    node["power"="generator"](area.boundaryarea);
#    way["power"="generator"](area.boundaryarea);
#    relation["power"="generator"](area.boundaryarea);
#    );
#    out body;
#    """
#    response = requests.get(overpass_url,
#                            params={'data': overpass_query})
#    data = response.json()
#    df = pd.DataFrame(data['elements'])
#    df = pd.concat([df.drop(columns='tags'), df.tags.apply(pd.Series)], axis=1)
#

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
    config = get_config() if config is None else config

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
    wepp = pd.read_csv(config['WEPP']['source_file'], dtype=datatypes,
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
                .assign(File=config['WEPP']['source_file']))
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
    return (wepp
            .pipe(set_column_name, 'WEPP')
            .pipe(config_filter, name='WEPP', config=config)
            .pipe(scale_to_net_capacities,
                  (not config['WEPP']['net_capacity']))
            .pipe(correct_manually, 'WEPP', config=config))


def UBA(header=9, skipfooter=26, prune_wind=True, prune_solar=True,
        config=None, update=False, raw=False):
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
    config = get_config() if config is None else config

    parse_func = lambda url: pd.read_excel(url, skipfooter=skipfooter,
                                           na_values='n.b.', header=header)
    uba = parse_if_not_stored('UBA', update, config, parse_func)
    if raw:
        return uba
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
    return (uba
            .pipe(set_column_name, 'UBA')
            .pipe(scale_to_net_capacities, not config['UBA']['net_capacity'])
#            .pipe(config_filter, name='UBA', config=config)
#            .pipe(correct_manually, 'UBA', config=config)
            )


def BNETZA(header=9, sheet_name='Gesamtkraftwerksliste BNetzA',
           prune_wind=True, prune_solar=True, raw=False, update=False,
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
    config = get_config() if config is None else config

    parse_func = lambda url: pd.read_excel(url, header=header,
                                           sheet_name=sheet_name,
                                           parse_dates=False)
    bnetza = parse_if_not_stored('BNETZA', update, config, parse_func)

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
           'Kraftwerksstatus \n(in Betrieb/\nvorläufig '
           'stillgelegt/\nsaisonale Konservierung\nNetzreserve/ '
           'Sicherheitsbereitschaft/\nSonderfall)': 'Status',
            ('Aufnahme der kommerziellen Stromerzeugung der derzeit '
             'in Betrieb befindlichen Erzeugungseinheit\n(Datum/Jahr)'):
            'YearCommissioned',
            u'PLZ\n(Standort Kraftwerk)': 'PLZ'})
    # If BNetzA-Name is empty replace by company, if this is empty by city.

    from .heuristics import PLZ_to_LatLon_map

    pattern = '|'.join(['.*(?i)betrieb', '.*(?i)gehindert', '(?i)vorläufig.*',
                        'Sicherheitsbereitschaft', 'Sonderfall'])
    bnetza = (bnetza.assign(
              lon=bnetza.PLZ.map(PLZ_to_LatLon_map()['lon']),
              lat=bnetza.PLZ.map(PLZ_to_LatLon_map()['lat']),
              Name=bnetza.Name.where(bnetza.Name.str.len().fillna(0) > 4,
                                     bnetza.Unternehmen + ' ' +
                                     bnetza.Name.fillna(''))
                              .fillna(bnetza.Ort).str.strip(),
              YearCommissioned=
                  bnetza.YearCommissioned.str[:4]
                        .apply(pd.to_numeric, errors='coerce'),
              Blockname=bnetza.Blockname.replace(
                      {'.*(GT|gasturbine).*': 'OCGT',
                       '.*(DT|HKW|(?i)dampfturbine|(?i)heizkraftwerk).*':
                           'Steam Turbine',
                       '.*GuD.*': 'CCGT'}, regex=True))
              [lambda df: df.projectID.notna() &
               df.Status.str.contains(pattern, regex=True, case=False)]
              .pipe(gather_technology_info,
                    search_col=['Name', 'Fueltype', 'Blockname'],
                    config=config))

    add_location_b = (bnetza[bnetza.Ort.notnull()]
                      .apply(lambda ds: (ds['Ort'] not in ds['Name'])
                             and (str.title(ds['Ort']) not in ds['Name']),
                             axis=1))
    bnetza.loc[bnetza.Ort.notnull() & add_location_b, 'Name'] = (
                bnetza.loc[bnetza.Ort.notnull() & add_location_b, 'Ort']
                + ' '
                + bnetza.loc[bnetza.Ort.notnull() & add_location_b, 'Name'])

    techmap = {'solare': 'PV',
               'Laufwasser': 'Run-Of-River',
               'Speicherwasser': 'Reservoir',
               'Pumpspeicher': 'Pumped Storage'}
    for fuel in techmap:
        bnetza.loc[bnetza.Fueltype.str.contains(fuel, case=False),
                   'Technology'] = techmap[fuel]
    # Fueltypes
    bnetza.Fueltype.replace({'Erdgas': 'Natural Gas',
                             'Steinkohle': 'Hard Coal',
                             'Braunkohle': 'Lignite',
                             'Wind.*': 'Wind',
                             'Solar.*': 'Solar',
                             '.*(?i)energietr.*ger.*\n.*': 'Other',
                             'Kern.*': 'Nuclear',
                             'Mineral.l.*': 'Oil',
                             'Biom.*': 'Bioenergy',
                             '.*(?i)(e|r|n)gas': 'Other',
                             'Geoth.*': 'Geothermal',
                             'Abfall': 'Waste',
                             '.*wasser.*':'Hydro',
                             '.*solar.*':'PV'},
                            regex=True, inplace=True)
    if prune_wind:
        bnetza = bnetza[lambda x: x.Fueltype != 'Wind']
    if prune_solar:
        bnetza = bnetza[lambda x: x.Fueltype != 'Solar']
    # Filter by country
    bnetza = bnetza[~bnetza.Bundesland.isin([u'Österreich', 'Schweiz',
                                             'Luxemburg'])]
    return (bnetza.assign(Country='Germany',
                          Set=bnetza.Set.fillna('Nein').str.title()
                              .replace({'Ja': 'CHP', 'Nein': 'PP'}))
            .pipe(set_column_name, 'BNETZA')
#            .pipe(config_filter, name='BNETZA', config=config)
#            .pipe(correct_manually, 'BNETZA', config=config)
            )


def OPSD_VRE(config=None, raw=False):
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
    config = get_config() if config is None else config

    df = parse_if_not_stored('OPSD_VRE', index_col=0, low_memory=False)
    if raw: return df

    return df.rename(columns={'energy_source_level_2': 'Fueltype',
                              'technology': 'Technology',
                              'data_source': 'file',
                              'country': 'Country',
                              'electrical_capacity': 'Capacity',
                              'municipality': 'Name'})\
            .assign(YearCommissioned=lambda df:
                    df.commissioning_date.str[:4].astype(float))\
            .powerplant.convert_alpha2_to_country()\
            .pipe(set_column_name, 'OPSD_VRE')\
            .pipe(config_filter, config=config)\
            .drop('Name', axis=1)


def OPSD_VRE_country(country, config=None, raw=False):
    """
    Get country specifig data from OPSD for renewables. For using this,
    the config has to be adjusted.
    """
    config = get_config() if config is None else config

    df = parse_if_not_stored(f'OPSD_VRE_{country}')
    if raw: return df

    return (df.assign(Country = 'DE')
            .rename(columns={'energy_source_level_2': 'Fueltype',
                             'technology': 'Technology',
                             'data_source': 'file',
                             'country': 'Country',
                             'electrical_capacity': 'Capacity',
                             'municipality': 'Name'})\
                .powerplant.convert_alpha2_to_country()\
                .pipe(config_filter, config=config)\
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
                 value_vars=[str(i) for i in range(2000, 2017, 1)],
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
#    df = df.loc[lambda df: df.Fueltype.isin(config['target_fueltypes'])]
    d = {u'Concentrated solar power': 'CSP',
         u'Solar photovoltaic': 'PV',
         u'Onshore wind energy': 'Onshore',
         u'Offshore wind energy': 'Offshore'}
    df.Technology.replace(d, inplace=True)
    df.loc[:, 'Set'] = 'PP'
    return df.reset_index(drop=True).pipe(set_column_name, 'IRENA Statistics')

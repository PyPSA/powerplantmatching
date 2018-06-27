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
import yaml
import os
from textwrap import dedent
from .utils import _data
import pandas as pd
"""
This file is used for basic configurations of the datasets, defining the fueltypes,
the given arguments of each power plant, and the restriction to european countries
"""

#countries
def set_target_countries(countries=None):
    global c
    if countries is None:
        c = sorted(['Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Czech Republic',
             'Denmark','Estonia', 'Finland', 'France', 'Germany', 'Greece',
             'Hungary', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg',
             'Netherlands', 'Norway', 'Poland', 'Portugal', 'Romania',
             'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'Switzerland',
             'United Kingdom'])
    else:
        if isinstance(countries, str):
            countries = [countries]
        c = countries

set_target_countries()


def target_countries():
    """
    Returns a list of selected countries, defaults to European countries
    """
    return c


def set_target_fueltypes(fueltypes=None):
    global f
    if fueltypes is None:
        f = sorted(['Natural Gas', 'Wind', 'Hydro', 'Oil', 'Waste',
                    'Hard Coal', 'Lignite',
                    'Nuclear', 'Other', 'Solar', 'Bioenergy', 'Geothermal'])
    else:
        if isinstance(fueltypes, str):
            fueltypes = [fueltypes]
        f = fueltypes

set_target_fueltypes()


def target_fueltypes():
    """
    Returns a list of fueltypes to which the powerplants should be standardized
    """
    return f



def target_sets():
    return ['PP', 'CHP']


def target_technologies():
    return ['CCGT', 'OCGT', 'Steam Turbine', 'Combustion Engine', # Thermal types
            'Run-Of-River', 'Pumped Storage', 'Reservoir', 'Marine', # Hydro types
            'Onshore', 'Offshore', # Wind types
            'PV', 'CSP'] # Solar types



def target_columns(detailed_columns=False):
    """
    Returns a list of columns to which the powerplants should be standardized. For renaming
    columns use df.rename(columns=dic, inplace=True) with dic being a dictionary
    of the replacements
    """
    if detailed_columns:
        return ['Name', 'Fueltype', 'Technology', 'Set', 'Country', 'Capacity',
                'Duration', 'YearCommissioned', 'lat', 'lon', 'File', 'projectID']
    else:
        return ['Name', 'Fueltype', 'Technology', 'Set', 'Country', 'Capacity',
                'YearCommissioned', 'lat', 'lon', 'File', 'projectID']


def fueltype_to_life():
    """
    Returns an approximation for the technical lifetime of a power plant in
    years, depending on its fueltype.
    """
    data = {'Bioenergy':20,
             'Geothermal':15,
             'Hard Coal':45,
             'Hydro':100,
             'Lignite':45,
             'Natural Gas':40,
             'Nuclear':50,
             'Oil':40,
             'Other':5,
             'Solar':25,
             'Waste':25,
             'Wind':25}
    return data


def fueltype_to_color(alternative_style=False):
    """
    Maps a fueltype to a specific color for the plots
    """
    if alternative_style:
#    Alternative (nicer?) fuetlype-color map
        return pd.Series(data=
              {'OCGT':'darkorange',
               'Hydro':'royalblue',
               'Run-of-river':'navy',
               'Ror':'navy',
               'Lignite':'indianred',
              'Nuclear': 'yellow',
              'Solar':'gold',
              'Windoff':'cornflowerblue',
              'Windon':'steelblue',
              'Offshore':'cornflowerblue',
              'Onshore':'steelblue',
              'Wind': 'steelblue',
              "Bioenergy" : "g",
              "Natural Gas" : "firebrick",
              'CCGT':'firebrick',
              'Coal':'k',
              'Hard Coal':'dimgray',
              "Oil" : "darkgreen",
              "Other":"silver",
              "Waste" : "grey",
              "Geothermal" : "orange",
              'Battery' : 'purple',
              'Hydrogen Storage' : 'teal',
              'Total':'gold'})

    return pd.Series({'Bioenergy':'darkgreen',
            'Geothermal':'pink',
            'Hard Coal':'dimgray',
            'Hydro':'blue',
            'Lignite':'darkgoldenrod',
            'Natural Gas':'red',
            'Nuclear':'orange',
            'Oil':'black',
            'Other':'silver',
            'Solar':'yellow',
            'Waste':'purple',
            'Wind':'cyan',
            'Total':'gold'})


def additional_data_config():
    """
    reads the config.yaml file where additional information about tokens, and
    paths is stored (e.g entsoe token, path to ESE file)

    contents should be

    entsoe_token : entsoe security token for the REST API
    ese_path : absolute path to the downloaded ese file,
                default 'Downloads/projects.xls'

    returns dict
    """
    fn = _data('../config.yaml')
    assert os.path.exists(fn), dedent("""
        The config file '{}' does not exist yet. Copy config_example.yaml to
        config.yaml and fill in details, as necessary.
    """)
    with open(fn) as f:
        return yaml.load(f)


def textbox_position():
    """
    Returns a value for the `loc` argument of the mpl.offsetbox by country.
    """
    return {'Austria': 1,
            'Belgium': 1,
            'Bulgaria': 1,
            'Croatia': 1,
            'Czech Republic': 1,
            'Denmark': 9,
            'Estonia': 2,
            'Finland': 1,
            'France': 2,
            'Germany': 9,
            'Greece': 1,
            'Hungary': 2,
            'Ireland': 1,
            'Italy': 1,
            'Latvia': 1,
            'Lithuania': 2,
            'Luxembourg': 1,
            'Netherlands': 1,
            'Norway': 1,
            'Poland': 1,
            'Portugal': 1,
            'Romania': 1,
            'Slovakia': 1,
            'Slovenia': 1,
            'Spain': 2,
            'Sweden': 1,
            'Switzerland': 9,
            'United Kingdom': 1}
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
import pandas as pd
from .utils import _data
"""
This file is used for basic configurations of the datasets, defining the fueltypes,
the given arguments of each power plant, and the restriction to european countries
"""

def europeancountries():
    """
    Returns a list of countries in Europe
    """
    c = ['Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Czech Republic', 'Denmark',
         'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland',
         'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Netherlands', 'Norway',
         'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden',
         'Switzerland', 'United Kingdom']
    return sorted(c)


def target_fueltypes():
    """
    Returns a list of fueltypes to which the powerplants should be standardized
    """
    return ['Natural Gas', 'Wind', 'Hydro', 'Oil', 'Waste', 'Hard Coal', 'Lignite',
            'Nuclear', 'Other', 'Solar', 'Bioenergy', 'Geothermal']


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


def fueltype_to_color():
    """
    Maps a fueltype to a specific color for the plots
    """
    data = {'Bioenergy':'white',
            'Geothermal':'pink',
            'Hard Coal':'gray',
            'Hydro':'blue',
            'Lignite':'brown',
            'Natural Gas':'red',
            'Nuclear':'orange',
            'Oil':'black',
            'Other':'pink',
            'Solar':'yellow',
            'Waste':'purple',
            'Wind':'green'}
    return data


def additional_data_config():
    """
    reads the ./data/data_config file where additional information about tokens,
    and paths is stored (e.g entsoe token, path to ESE file)

    contents should be

    entsoe_token : entose security token for the REST API
    path_to_ese: absolute path to the downloaded ese file,
                default 'Downloads/projects.xls'

    returns pandas.Series
    """
    return pd.read_csv(_data('additional_data.config'),
                       index_col=0, sep=':', header=None).loc[:,1]

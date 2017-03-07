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
This file is used for basic configurations of the datasets, defining the fueltypes,
the given arguments of each power plant, and the restriction to european countries
"""

def europeancountries():
    """
    Returns a list of countries in Europe
    """
    return ['Austria', 'Belgium', 'Czech Republic', 'Denmark', 'France', 'Germany',
            'Ireland', 'Italy', 'Hungary', 'Luxembourg', 'Netherlands', 'Norway', 'Poland',
            'Portugal', 'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'Switzerland', 'United Kingdom']
#    return ['Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Czech Republic',
#            'Denmark', 'Estonia', 'Finland', 'France', 'Germany',
#            'Greece', 'Hungary', 'Ireland', 'Italy', 'Latvia',
#            'Lithuania', 'Luxembourg', 'Netherlands', 'Norway',
#            'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia',
#            'Spain', 'Sweden', 'Switzerland', 'United Kingdom']


def target_fueltypes():
    """
    Returns a list of fueltypes to which the powerplants should be standardized
    """
    return ['Natural Gas', 'Wind', 'Hydro', 'Oil', 'Waste', 'Hard Coal', 'Lignite',
            'Nuclear', 'Other', 'Solar', 'Bioenergy', 'Geothermal']
    
def fueltype_to_abbrev():
    """
    Returns the fueltype-specific abbreviation.
    """
    data = {'Bioenergy':'BIO',
            'Geothermal':'GEO',
            'Hard Coal':'COA',
            'Hydro':'HYD',
            'Lignite':'LIG',
            'Natural Gas':'NG',
            'Nuclear':'NUC',
            'Oil':'OIL',
            'Other':'OTH',
            'Solar':'SPV',
            'Waste':'WST',
            'Wind':'WO'}
    return data
    
def timestype_to_life():
    """
    Returns the timestype-specific technical lifetime    
    """
    data = {'Bioenergy':20,
            'Geothermal':15,
            'Hard Coal':35,
            'Hydro':75,
            'Lignite':35,
            'Natural Gas':25,
            'Nuclear':50,
            'Oil':20,
            'Other':5,
            'Solar':25,
            'Waste':25,
            'Wind':25}
    return data


def target_sets():
    return ['PP', 'CHP']

def target_technologies():
    return ['CCGT', 'OCGT', 'Steam Turbine', 'Combustion Engine', 'Run-Of-River',
            'Pumped Storage', 'Reservoir', 'Onshore', 'Offshore']
    
def target_columns():
    """
    Returns a list of columns to which the powerplants should be standardized. For renaming
    columns use df.rename(columns=dic, inplace=True) with dic being a dictionary
    of the replacements
    """

    return ['Name', 'Fueltype', 'Technology', 'Set', 'Country', 'Capacity',
            'YearCommissioned', 'lat', 'lon', 'File',
            'projectID']
#    return ['Name', 'Fueltype', 'Technology', 'Set', 'Country', 'Capacity',
#            'YearCommissioned','YearDecommissioned', 'lat', 'lon', 'File',
#            'projectID']

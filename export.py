# -*- coding: utf-8 -*-
## Copyright 2015-2016 Fabian Gotzens (FZJ)

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

## This export script is intented for the users of the VEDA-TIMES modelling
## framework <http://iea-etsap.org/index.php/etsap-tools/data-handling-shells/veda>

import os
import pandas as pd
import numpy as np

from countrycode import countrycode
from .collection import Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced

def Export_TIMES(df=None, use_scaled_capacity=False):

    if df is None:
        df = Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced()
        if df is None:
            raise RuntimeError("The data to be exported does not yet exist.")
    df = df.copy()

    # replace country names by iso3166-2 codes
    df.loc[:,'Country'] = countrycode(codes=df.Country, origin='country_name', target='iso2c')


    # add column with TIMES-specific type. The pattern is as follows:
    # 'ConELC-' + Set + '_' + Fueltype + '-' Technology
    df.loc[:,'Technology'].fillna('', inplace=True)
    df.insert(10, 'TimesType', np.nan)
    df.loc[:,'TimesType'] = pd.Series('ConELC-' for _ in range(len(df))) +\
          np.where(df.loc[:,'Set'].str.contains('CHP'),'CHP','PP') +\
          '_' + df.loc[:,'Fueltype'].map(fueltype_to_abbrev())
    df.loc[(df['Fueltype']=='Wind') & (df['Technology'].str.contains('offshore', case=False)),'TimesType'] += 'F'
    df.loc[(df['Fueltype']=='Wind') & (df['Technology'].str.contains('offshore', case=False)==False),'TimesType'] += 'N'
    df.loc[(df['Fueltype']=='Solar') & (df['Technology'].str.contains('CSP', case=False)),'TimesType'] += 'CST'
    df.loc[(df['Fueltype']=='Solar') & (df['Technology'].str.contains('CSP', case=False)==False),'TimesType'] += 'SPV'
    df.loc[(df['Fueltype']=='Natural Gas') & (df['Technology'].str.contains('CCGT', case=False)),'TimesType'] += '-CCGT'       
    df.loc[(df['Fueltype']=='Natural Gas') & (df['Technology'].str.contains('CCGT', case=False)==False)\
           & (df['Technology'].str.contains('OCGT', case=False)),'TimesType'] += '-OCGT'
    df.loc[(df['Fueltype']=='Natural Gas') & (df['Technology'].str.contains('CCGT', case=False)==False)\
           & (df['Technology'].str.contains('OCGT', case=False)==False),'TimesType'] += '-ST'
    df.loc[(df['Fueltype']=='Hydro') & (df['Technology'].str.contains('pumped storage', case=False)),'TimesType'] += '-PST'
    df.loc[(df['Fueltype']=='Hydro') & (df['Technology'].str.contains('run-of-river', case=False))\
           & (df['Technology'].str.contains('pumped storage', case=False)==False),'TimesType'] += '-ROR'
    df.loc[(df['Fueltype']=='Hydro') & (df['Technology'].str.contains('run-of-river', case=False)==False)\
           & (df['Technology'].str.contains('pumped storage', case=False)==False),'TimesType'] += '-STO'

    # add column with technical lifetime
    df.insert(12, 'Life', np.nan)
    df.loc[:,'Life'] = df.TimesType.map(timestype_to_life())

    # add column with decommissioning year
    df.insert(13, 'YearDecommissioned', np.nan)
    df.loc[:,'YearDecommissioned'] = df.loc[:,'YearCommissioned'] + df.loc[:,'Life']

    # Now create new export dataframe with headers
    countries = sorted(set(df.Country))
    if None in countries:
        raise ValueError("""There are rows without a valid country identifier
                         in the dataframe. Please check!""")
    columns = ['Attribute','*Unit','LimType','Year']
    columns.extend(countries)
    columns.append('Pset_Pn')
    df_exp = pd.DataFrame(columns=columns)

    # Loop stepwise through technologies, years and countries
    row = 0
    timestypes = sorted(set(df.TimesType))
    if None in timestypes:
        raise ValueError("""There are rows without a valid TIMES-Type identifier
                         in the dataframe. Please check!""")
    data_timestypes = df.groupby(df.TimesType)
    cap_column='Scaled Capacity' if use_scaled_capacity else 'Capacity'
    for tt in timestypes:
        tt_group = data_timestypes.get_group(tt)
        for yr in range(2010, 2055, 5):
            df_exp.loc[row,'Year'] = yr
            data_countries = tt_group.groupby(tt_group.Country)
            for ct in countries:
                if ct in data_countries.groups:
                    ct_group = data_countries.get_group(ct)
                    # Here, the matched elements are being filtered
                    series = ct_group.apply(lambda x: x[cap_column] \
                        if yr >= x['YearCommissioned'] and yr <= x['YearDecommissioned'] else 0, axis=1)
                    # Divide the sum by 1000 (MW->GW) and write into the export dataframe
                    df_exp.loc[row,ct] = series.sum()/1000
                else:
                    df_exp.loc[row,ct] = 0
            df_exp.loc[row,'Pset_Pn'] = tt
            row = row + 1
    df_exp.loc[:,'Attribute'] = 'STOCK'
    df_exp.loc[:,'*Unit'] = 'GW'
    df_exp.loc[:,'LimType'] = 'FX'

    # Write resulting dataframe to file
    outfn = os.path.join(os.path.dirname(__file__),'data','out','Export_Stock_TIMES.xlsx')
    df_exp.to_excel(outfn)
    return df_exp


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
            'Solar':'', # DO NOT delete this entry!
            'Waste':'WST',
            'Wind':'WO'}
    return data

def timestype_to_life():
    """
    Returns the timestype-specific technical lifetime
    """
    data = {'ConELC-PP_COA':35,
            'ConELC-PP_LIG':35,
            'ConELC-PP_NG-OCGT':25,
            'ConELC-PP_NG-ST':35,
            'ConELC-PP_NG-CCGT':25,
            'ConELC-PP_OIL':20,
            'ConELC-PP_NUC':50,
            'ConELC-PP_BIO':20,
            'ConELC-PP_HYD-ROR':75,
            'ConELC-PP_HYD-STO':100,
            'ConELC-PP_HYD-PST':100,
            'ConELC-PP_WON':25,
            'ConELC-PP_WOF':30,
            'ConELC-PP_SPV':25,
            'ConELC-PP_CST':25,
            'ConELC-PP_WST':25,
            'ConELC-PP_SYN':5,
            'ConELC-PP_CAES':40,
            'ConELC-PP_GEO':15,
            'ConELC-PP_OTH':5,
            'ConELC-CHP_COA':35,
            'ConELC-CHP_LIG':35,
            'ConELC-CHP_NG-OCGT':25,
            'ConELC-CHP_NG-ST':35,
            'ConELC-CHP_NG-CCGT':25,
            'ConELC-CHP_OIL':20,
            'ConELC-CHP_BIO':20,
            'ConELC-CHP_WST':25,
            'ConELC-CHP_SYN':5,
            'ConELC-CHP_OTH':5,
            }
    return data

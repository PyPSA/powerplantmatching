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

import os
import pandas as pd
import numpy as np

from countrycode import countrycode
from .powerplant_collection import Carma_ENTSOE_GEO_OPSD_WEPP_WRI_matched_reduced
from .config import fueltype_to_abbrev, timestype_to_life, target_fueltypes

def Export_TIMES(df):
    
    if df is None :
        #raise RuntimeError("The data to be exported does not yet exist.")
        df = Carma_ENTSOE_GEO_OPSD_WEPP_WRI_matched_reduced()

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
    df.loc[:,'Life'] = df.Fueltype.map(timestype_to_life())
    
    # add column with decommissioning year
    df.insert(13, 'YearDecommissioned', np.nan)
    df.loc[:,'YearDecommissioned'] = df.loc[:,'YearCommissioned'] + df.loc[:,'Life']
    
    # Now create new export dataframe with headers
    countries = list(set(df.Country))
    countries.sort()
    columns = ['Attribute','*Unit','LimType','Year']
    columns.extend(countries)
    columns.append('Pset_Pn')
    df_exp = pd.DataFrame(columns=columns)
    
    # Loop stepwise through technologies, years and countries
    row = 0
    timestypes = list(set(df.TimesType))
    timestypes.sort()
    data_timestypes = df.groupby(df.TimesType)
    for tt in timestypes:
        tt_group = data_timestypes.get_group(tt)
        for yr in range(2010, 2055, 5):
            df_exp.loc[row,'Year'] = yr
            data_countries = tt_group.groupby(tt_group.Country)
            for ct in countries:
                if ct in data_countries.groups:
                    ct_group = data_countries.get_group(ct)
                    # Here the matched elements are being filtered
                    series = ct_group.apply(lambda x: x['Scaled Capacity'] \
                        if yr >= x['YearCommissioned'] and yr <= x['YearDecommissioned'] else 0, axis=1)
                    # Write the sum into the export dataframe
                    df_exp.loc[row,ct] = series.sum()/1000 # division by 1000 to convert MW -> GW
                else:
                    df_exp.loc[row,ct] = 0
            df_exp.loc[row,'Pset_Pn'] = tt
            row = row + 1
    df_exp.loc[:,'Attribute'] = 'STOCK'
    df_exp.loc[:,'*Unit'] = 'GW'
    df_exp.loc[:,'LimType'] = 'FX'
    
    # Write resulting dataframe to file
    outfn = os.path.join(os.path.dirname(__file__), 'data','Export_TIMES.xlsx')
    df_exp.to_excel(outfn)
    return

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
from .config import fueltype_to_Life, target_fueltypes

def Export_TIMES():
    df = Carma_ENTSOE_GEO_OPSD_WEPP_WRI_matched_reduced()
    
    if df == None :
        raise RuntimeError("The data to be exported does not yet exist.")

    # replace country names by iso3166-2 codes
    df.loc[:,'Country'] = countrycode(codes=df.Country, origin='country_name', target='iso2c')

    # add column with technical lifetime
    df.insert(12, 'Life', np.nan)
    df.loc[:,'Life'] = df.Fueltype.map(fueltype_to_life())
    
    # add column with decommissioning year
    df.insert(13, 'Life', np.nan)
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
    data_fueltypes = df.groupby(df.Fueltype)
    for ft in target_fueltypes():
        ft_group = data_fueltypes.get_group(ft)
        for yr in range(2010, 2055, 5):
            df_exp.loc[row,'Year'] = yr
            data_countries = ft_group.groupby(ft_group.Country)
            for ct in countries:
                if ct in data_countries.groups:
                    ct_group = data_countries.get_group(ct)
                    # Here the matched elements are being filtered
                    ct_group.loc[:,'match'] = ct_group.apply(lambda x: x['Capacity'] \
                        if yr >= x['YearCommissioned'] and yr <= x['YearDecommissioned'] else 0, axis=1)
                    # Write the sum into the export dataframe
                    df_exp.loc[row,ct] = ct_group.loc[:,'match'].sum()/1000 # division by 1000 to convert MW -> GW
                else:
                    df_exp.loc[row,ct] = 0
            df_exp.loc[row,'Pset_Pn'] = ft
            row = row + 1
    df_exp.loc[:,'Attribute'] = 'STOCK'
    df_exp.loc[:,'*Unit'] = 'GW'
    df_exp.loc[:,'LimType'] = 'FX'
    
    return

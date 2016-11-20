# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 10:46:33 2016

@author: godula
"""

import matplotlib
import pandas as pd 
from powerplantmatching import cleaning, utils, config, data, heuristics
import matplotlib.pyplot as plt
import countrycode
from powerplantmatching import powerplant_collection as pc
import numpy as np
reload(pc)

import xarray as xr
import scipy as sp, scipy.spatial
import sklearn as sk
from sklearn import svm
from sklearn import linear_model, datasets


#%% Expand the database with non-matches

matched = pc.Carma_ESE_FIAS_GEO_OPSD_WRI_matched_reduced()[lambda df: df.loc[:,["lat", "lon"]].notnull().all(axis=1)]
hydro = matched[matched.Fueltype=="Hydro"]

entsoestats = pd.read_excel('StatisticsHydro.xls')
entsoestats = entsoestats.iloc[5:,[0,2,3,4,5]]
entsoestats.set_axis(1,entsoestats.loc[5])
entsoestats = entsoestats[1:]
entsoestats.columns=['country', 'hydro', 'Reservoir', 'Ror', 'Pumped Storage']
countrynames = pd.Series(index=entsoestats.country, data=countrycode.countrycode\
                         (entsoestats.country, origin='iso2c', target='country_name')).str.title()
entsoestats = entsoestats.loc[entsoestats.country.map(countrynames).isin(hydro.Country)]
entsoestats.country = entsoestats.country.map(countrynames)
entsoestats = entsoestats.sort_values(by='country')
entsoestats = entsoestats.set_index('country')



GEO = data.GEO()
GEO = GEO[GEO.Fueltype=='Hydro']
FIAS = pd.concat([cleaning.clean_single(data.FIAS(), aggregate_powerplant_units=False),
                               data.ESE()]).reset_index(drop=True)

columns = hydro.columns
hydro = heuristics.extend_by_non_matched(hydro, GEO, 'GEO')
hydro = cleaning.clean_classification(hydro, generalize_hydros=True)

#%% Whether official or non-official

#hydro = heuristics.extend_by_non_matched(hydro, FIAS, 'FIAS')
hydro = hydro.drop('ESE_and_FIAS', axis=1).\
       replace('energy_storage_exchange, |energy_storage_exchange', '', regex=True)
hydro.loc[hydro.File=='', 'File']= np.NaN



#%% 

hydro = hydro.loc[:,columns]
hydro = hydro.loc[hydro.Capacity.notnull()]
hydro = hydro.reset_index(drop=True)

hydro = cleaning.clean_classification(hydro, generalize_hydros=True)

hydrogrouped = hydro.groupby(['Country', 'Classification']).Capacity.sum().unstack()


#%% Scale countrywise


hydro = heuristics.rescale_capacities_to_country_totals(hydro, 'Hydro')


#%% Parse europe geographic properties

# parse 3-dim goedata from http://www.ngdc.noaa.gov/mgg/global/global.html


geodata = xr.open_dataset('ETOPO1_Ice_c_gmt4.grd')
eu = geodata['z'].sel(y=slice(35, 71), x=slice(-9, 30))
eu = eu.to_dataframe(name='height')
eu = pd.DataFrame(eu.height).reset_index()
eu = eu.loc[:,['x','y','height']]
eumap= eu.pivot('x','y','height')

def compute_window_mean_and_var_strided(image, window_w, window_h):
    w, h = image.shape
    strided_image = np.lib.stride_tricks.as_strided(image, 
                                                    shape=[w - window_w + 1, h - window_h + 1, window_w, window_h],
                                                    strides=image.strides + image.strides)

    return strided_image.std(axis=(2,3)), strided_image.mean(axis=(2,3)) 

variation, mean = compute_window_mean_and_var_strided(eumap.values,5,5)
euslopes = pd.DataFrame(data=variation, index=eumap.index[2:-2], columns=eumap.columns[2:-2]).reindex(index=eumap.index, columns=eumap.columns)
eusmeans = pd.DataFrame(data=mean, index=eumap.index[2:-2], columns=eumap.columns[2:-2]).reindex(index=eumap.index, columns=eumap.columns)
coords = eu.reset_index().loc[:,['x','y']].values


#s = sp.spatial.cKDTree(coords)

#%% Add geographic properties

spots = eu.loc[s.query(hydro.loc[:,['lon','lat']].values)[1]]
spots = spots.reset_index(drop=True).loc[:,['x','y']]



hydro.loc[:,'stdheight']= np.diag(euslopes.loc[spots.x, spots.y])
hydro.loc[:,'height']= np.diag(eusmeans.loc[spots.x, spots.y])

print hydro.Classification.value_counts()



#%% Train classification


hydro.loc[hydro.Classification=='Run-Of-River', 'Classification']='Ror'                    
                    
 

# Make only classification for run-of-river and reservoir since pumped storage is already covered

#trainhydro = hydro.loc[(~(hydro.Country == 'Sweden')) & (hydro.Classification.isin(['Ror', 'Reservoir']))].reset_index(drop = True)
trainhydro = hydro.loc[(hydro.Classification.isin(['Ror', 'Reservoir']))].reset_index(drop = True)

training = trainhydro[trainhydro.loc[:,['height', 'stdheight', 'Capacity', 'Classification']]\
    .notnull().all(axis=1)].reset_index(drop= True)

print trainhydro.Classification.value_counts()



#%% Fit to database
hydrofit = hydro.copy()
relations = hydrogrouped-entsoestats[entsoestats!=0]
relations = relations.loc[:,['Reservoir', 'Ror']]


bias = pd.DataFrame(index=relations.index, columns=relations.columns)
bias.Reservoir = 1.
bias.Ror = 1.

bias.loc["Austria"]=[1.5,.0]
bias.loc["Bulgaria"]= [1.5,.0]
bias.loc["Croatia"] = [1.1,0.]
bias.loc['Czech Republic'] = [1.,1.]
bias.loc["Denmark"] = [1.,1.]
bias.loc["France"] = [1.5,.4]
bias.loc["Greece"] = [1.,1.]
bias.loc["Italy"] = [1.,1.3]
bias.loc["Norway"] = [1.,0.]
bias.loc["Poland"] = [0.,1.]
bias.loc["Portugal"] = [1.,1.3]
bias.loc['Romania'] = [1.5,.7]
bias.loc["Spain"] = [1.5,.9]
bias.loc["Switzerland"] = [.8,1.4]




for c in bias.index:
    clf = linear_model.LogisticRegression(  \
                                 class_weight=  \
                                {"Reservoir":bias.loc[c,'Reservoir'],
                                'Ror':bias.loc[c,'Ror']} \
                                )
    data = training.loc[:,['height', 'stdheight', 'Capacity']].values
    target = training.loc[: ,'Classification'].values
    clf.fit(data, target)
    
    missing_class_in_c_b = (hydrofit.Classification.isnull())&(hydrofit.Country == c)&(hydrofit.loc[:,[ 'height', 'stdheight', 'Capacity']]\
            .notnull().all(axis=1))
    
    
    try:
        hydrofit.loc[missing_class_in_c_b,'Classification'] = \
        clf.predict(hydrofit.loc[missing_class_in_c_b,['height', 'stdheight', 'Capacity']].values)
    except:
        None

        
        
columnsnew = list(columns.drop('ESE_and_FIAS')) + ['Scaled Capacity']

hydrofit = hydrofit.loc[:,columnsnew]
hydrofitgrouped = hydrofit.groupby(['Country', 'Classification'])['Scaled Capacity'].sum().unstack()
hydrofitgrouped.loc[:,'hydro']=hydrofit.groupby('Country')['Scaled Capacity'].sum()
relationsfit = hydrofitgrouped-entsoestats[entsoestats!=0]
print relationsfit
hydrofit.Classification.replace('Ror', 'Run-Of-River', regex=True, inplace=True)
hydrofit.to_csv('hydro_aggregation_beta.csv', index_label='id', encoding='utf-8')



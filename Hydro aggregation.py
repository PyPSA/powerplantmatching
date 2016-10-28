# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 10:46:33 2016

@author: godula
"""

import matplotlib
import pandas as pd 
from powerplantmatching import powerplant_collection as pc
import matplotlib.pyplot as plt
import countrycode

import numpy as np


import xarray as xr
import scipy as sp, scipy.spatial
import sklearn as sk
from sklearn import svm
from sklearn import linear_model, datasets


#%% Expand the database with non-matches

data= pc.FIAS_WRI_GEO_Carma_matched()[lambda df: df.loc[:,["lat", "lon"]].notnull().all(axis=1)]
data.loc[data.Classification == 'Run-of-river', 'Classification'] = 'Ror'
hydro = data[data.Fueltype=="Hydro"]

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


GEO = pd.read_csv('/Users/godula/Desktop/EuropeanPowerGrid/DukeOnPowerplants/GEOdataDuked.csv', index_col='id')
FIAS = pd.read_csv('/Users/godula/Desktop/EuropeanPowerGrid/DukeOnPowerplants/FiasHydro.csv', index_col='id')

GEO = GEO[GEO.Fueltype=='Hydro']
#FIAS = FIAS[FIAS.Country=='Sweden']


GEO[~GEO.Name.isin(hydro.Geo)].Capacity.sum()

FIAS[~FIAS.Name.isin(hydro.Fias)].Capacity.sum()


GEO.loc[:,'Geo'] = GEO.Name
FIAS.loc[:,'Fias'] = FIAS.Name

columns = hydro.columns
hydro = hydro.append(GEO[~GEO.Geo.isin(hydro.Geo)])
hydro = hydro.append(FIAS[~FIAS.Fias.isin(hydro.Fias)])
hydro = hydro.loc[:,columns]
hydro = hydro.loc[hydro.Capacity.notnull()]
hydro = hydro.reset_index(drop=True)

hydro.loc[(hydro.Classification.str.contains('reservoir|lake', case=False)) & (hydro.Classification.notnull())\
    , 'Classification'] = 'Reservoir'
hydro.loc[(hydro.Classification.str.contains('run-of-river|weir|water', case=False)) & (hydro.Classification.notnull())\
    , 'Classification'] = 'Ror'
hydro.loc[(hydro.Classification.str.contains('dam', case=False)) & (hydro.Classification.notnull())\
    , 'Classification'] = 'Reservoir'
hydro.loc[(hydro.Classification.str.contains('Pump|pumped', case=False)) & (hydro.Classification.notnull())\
    , 'Classification'] = 'Pumped Storage'
    


hydrogrouped = hydro.groupby(['Country', 'Classification']).Capacity.sum().unstack()

#%%Delete some Duplicates manually

hydro = hydro.loc[~(hydro.Fias=="Grand'maison dam")]

#%% Scale countrywise



entsoe = pc.entsoe_data()
lookup = pc.lookup([entsoe.loc[entsoe.Fueltype=='Hydro'], hydro], keys= ['ENTSOE', 'matched'], by='Country')
lookup.loc[:,'Difference'] = lookup.ENTSOE - lookup.matched
missingpowerplants = (lookup.Difference/120).round().astype(int)

hydroexp = hydro

for i in missingpowerplants[:-1].loc[missingpowerplants[:-1] > 0].index:
    print i
    try:
        howmany = missingpowerplants.loc[i]
        hydroexp = hydroexp.append(hydro.loc[(hydro.Country == i)& (hydro.lat.notnull()),['lat', 'lon']].sample(howmany) + np.random.uniform(-.4,.4,(howmany,2)), ignore_index=True)
        hydroexp.loc[hydroexp.shape[0]-howmany:,'Country'] = i
        hydroexp.loc[hydroexp.shape[0]-howmany:,'Capacity'] = 120.
        hydroexp.loc[hydroexp.shape[0]-howmany:,'Fias'] = 'Artificial Powerplant'

        
    except: 
        for j in range(missingpowerplants.loc[i]):
            hydroexp = hydroexp.append(hydro.loc[(hydro.Country == i)& (hydro.lat.notnull()),['lat', 'lon']].sample(1) + np.random.uniform(-1,1,(1,2)), ignore_index=True)
            hydroexp.loc[hydroexp.shape[0]-1:,'Country'] = i
            hydroexp.loc[hydroexp.shape[0]-1:,'Capacity'] = 120.
            hydroexp.loc[hydroexp.shape[0]-howmany:,'Fias'] = 'Artificial Powerplant'
        
for i in missingpowerplants[:-1].loc[missingpowerplants[:-1] < -1].index:
    while hydroexp.loc[hydroexp.Country == i, 'Capacity'].sum() > lookup.loc[i, 'ENTSOE'] + 300:
        try:
            hydroexp = hydroexp.drop(hydroexp.loc[(hydroexp.Country == i)& (hydroexp.Geo.isnull())].sample(1).index)
        except:
            hydroexp = hydroexp.drop(hydroexp.loc[(hydroexp.Country == i)].sample(1).index)

hydroexp.Fueltype = 'Hydro'
pc.lookup([entsoe.loc[entsoe.Fueltype=='Hydro'], hydroexp], keys= ['ENTSOE', 'matched'], by='Country')

del hydro
hydro = hydroexp

print hydro.groupby(['Country', 'Classification']).Capacity.sum().unstack()

#%% Parse europe geographic properties

# parse 3-dim goedata from http://www.ngdc.noaa.gov/mgg/global/global.html



geodata = xr.open_dataset('/Users/godula/Desktop/EuropeanPowerGrid/ETOPO1_Ice_c_gmt4.grd')
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


hydro_save = hydro
print hydro.Classification.value_counts()

#%% Train classification


                    
                    
hydro = hydro_save
 

# Make only classification for run-of-river and reservoir since pumped storage is already covered
trainhydro = hydro.loc[(~(hydro.Country == 'Sweden')) & (hydro.Classification.isin(['Ror', 'Reservoir']))].reset_index(drop = True)


training = trainhydro[trainhydro.loc[:,['height', 'stdheight', 'Capacity', 'Classification']]\
    .notnull().all(axis=1)].reset_index(drop= True)

print trainhydro.Classification.value_counts()


hydro.to_csv('hydrotemporary.csv', index_label='id')


#%% Fit to database

hydrofit = pd.read_csv('hydrotemporary.csv', index_col='id')


relations = hydrogrouped-entsoestats[entsoestats!=0]
relations = relations.loc[:,['Reservoir', 'Ror']]

#bias = relations.div(relations.max(axis=1), axis=0)
#bias[bias<0]=0.1
#bias = bias.div(bias.mean(axis=1), axis=0)

bias = pd.DataFrame(index=relations.index, columns=relations.columns)
bias.Reservoir = 1.
bias.Ror = 1.

bias.loc["Austria"]=[1.,1.]
bias.loc["Bulgaria"]= [1.5,1.]
bias.loc["Croatia"] = [1.1,0.]
bias.loc['Czech Republic'] = [1.,1.]
bias.loc["Denmark"] = [1.,1.]
bias.loc["France"] = [1.,1.3]
bias.loc["Greece"] = [1.,1.]
bias.loc["Italy"] = [1.,1.7]
bias.loc["Norway"] = [1.,0.]
bias.loc["Poland"] = [0.,1.]
bias.loc["Portugal"] = [1.,1.3]
bias.loc['Romania'] = [.8,1.7]
bias.loc["Spain"] = [1.2,1.]
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

        
        


hydrofit = hydrofit.loc[:,columns]
hydrofitgrouped = hydrofit.groupby(['Country', 'Classification']).Capacity.sum().unstack()
relationsfit = hydrofitgrouped-entsoestats[entsoestats!=0]
print relationsfit




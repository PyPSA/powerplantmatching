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
import matplotlib.pyplot as plt

from .config import fueltype_to_life, europeancountries
from .cleaning import clean_single
from .data import CARMA, ENTSOE, ENTSOE_stats, ESE, GEO, OPSD, WEPP, WRI
from .collection import Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced
from .utils import lookup


def Show_all_plots():
    Plot_bar_comparison_single_matched()
    Plot_hbar_comparison_countries()
    return


def Plot_bar_comparison_single_matched(df=None, cleaned=True, use_saved_aggregation=True):
    """
    Plots two bar charts for comparison
    1.) Fueltypes on x-axis and capacity on y-axis, categorized by originating database.
    2.) Fueltypes on x-axis and capacity on y-axis, matched database
    """
    if df is None:
        df = Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced()
        if df is None:
            raise RuntimeError("The data to be plotted does not yet exist.")
    
    if cleaned:
        carma = clean_single(CARMA(), use_saved_aggregation=use_saved_aggregation)
        entsoe = clean_single(ENTSOE(), use_saved_aggregation=use_saved_aggregation)
        geo = clean_single(GEO(), aggregate_powerplant_units=False)
        opsd = clean_single(OPSD(), use_saved_aggregation=use_saved_aggregation)
        wepp = clean_single(WEPP(), use_saved_aggregation=use_saved_aggregation)
        wri = clean_single(WRI(), use_saved_aggregation=use_saved_aggregation)
    else:
        carma = CARMA()
        entsoe = ENTSOE()
        geo = GEO()
        opsd = OPSD()
        wepp = WEPP()
        wri = WRI()
    ese = ESE()
    stats = lookup([carma, entsoe, ese, geo, opsd, wepp, wri],
                   keys=['CARMA','ENTSO-E','ESE','GEO','OPSD','WEPP','WRI'], by='Fueltype')/1000
    stats_reduced = lookup(df, by='Fueltype')/1000
    # Presettings for the plots
    font={#'family' : 'normal',
          #'weight' : 'bold',
          'size'   : 24}
    plt.rc('font', **font)
    # Create figure with two subplots
    fig, ax = plt.subplots(nrows=1, ncols=2, sharex=False, sharey=True, figsize = (25,13))
    # 1st Plot with single datasets on the left side. The df.plot() function returns a
    #     matplotlib.axes.AxesSubplot object. You can set the labels on that object. 
    stats.plot.bar(ax=ax[0], stacked=False, legend=True, colormap='Accent')
    ax[0].set_ylabel('Installed Capacity [GW]')
    ax[0].set_title('Capacities of Single DBs')
    ax[0].set_facecolor('#d9d9d9')                  # gray background
    ax[0].set_axisbelow(True)                       # puts the grid behind the bars
    ax[0].grid(color='white', linestyle='dotted')   # adds white dotted grid
    # 2nd Plot with reduced dataset
    stats_reduced.plot.bar(ax=ax[1], stacked=False, colormap='jet')
    ax[1].xaxis.label.set_visible(False)
    ax[1].set_title('Capacities of Matched DB')
    ax[1].set_facecolor('#d9d9d9')
    ax[1].set_axisbelow(True)
    ax[1].grid(color='white', linestyle='dotted')
    fig.tight_layout()
    return


def Plot_hbar_comparison_countries(df=None):
    """
    Plots a horizontal bar chart with capacity on x-axis, country on y-axis.
    """
    if df is None:
        df = Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced()
        if df is None:
            raise RuntimeError("The data to be plotted does not yet exist.")
            
    statistics = ENTSOE_stats()
    stats = lookup([df, statistics], keys=['Matched dataset','Statistics ENTSO-E'], by='Country')/1000
    # Presettings for the plots
    font={#'family' : 'normal',
          #'weight' : 'bold',
          'size'   : 24}
    plt.rc('font', **font)
    ax = stats.plot.barh(stacked=False, legend=True, colormap='jet', figsize = (22,13))
    ax.set_xlabel('Installed Capacity [GW]')
    ax.yaxis.label.set_visible(False)
    ax.set_facecolor('#d9d9d9')                  # gray background
    ax.set_axisbelow(True)                       # puts the grid behind the bars
    ax.grid(color='white', linestyle='dotted')   # adds white dotted grid
    ax.invert_yaxis()
    return


def Plot_bar_decomissioning_curves(df=None):
    """
    Plots per country a decommissioning curve as a bar chart with capacity on y-axis,
    period on x-axis and categorized by fueltype.
    """
    if df is None:
        df = Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced()
        if df is None:
            raise RuntimeError("The data to be plotted does not yet exist.")
    df = df.copy()
            
    df.loc[:,'Life'] = df.Fueltype.map(fueltype_to_life())
    
    # Insert periodwise capacities
    df.loc[:,2015] = df.loc[:,'Capacity']
    for yr in range(2020, 2055, 5):
        df.loc[yr<=(df.loc[:,'YearCommissioned']+df.loc[:,'Life']),yr] = df.loc[:,'Capacity']
        df.loc[:,yr].fillna(0., inplace=True)
        
    # Presettings for the plots
    font={#'family' : 'normal',
          #'weight' : 'bold',
          'size'   : 16}
    plt.rc('font', **font)

    fig, ax = plt.subplots(nrows=4, ncols=5, sharex=True, sharey=False, figsize = (25,16))
    data_countries = df.groupby(['Country'])
    i,j = [0,0]
    for a, country in enumerate(europeancountries()):
        if j==5:
            i=i+1
            j=0
        cntry_grp = data_countries.get_group(country)
        stats = pd.DataFrame(columns=[2015,2020,2025,2030,2035,2040,2045,2050])
        for yr in range(2015, 2055, 5):
            k = cntry_grp.groupby(['Fueltype']).sum()/1000
            stats.loc[:,yr] = k[yr]
        # ---------------------------------------------------------------------
        # f.gotzens@fz-juelich.de: This workaround adds 'missing' fueltypes to 
        # the stats df, such that the legend is exactly the same for each country.
        # If there's a more elegant way to achieve this, please let me know.
        fueltypes_in_df = set(df.Fueltype)
        missing_fueltypes = fueltypes_in_df.difference(set(stats.index))
        for m in missing_fueltypes:
            stats.loc[m,:] = 0.0
        stats.sort_index(inplace=True)
        # ---------------------------------------------------------------------
        stats.T.plot.bar(ax=ax[i,j],stacked=True,legend=False,colormap='Paired')
        ax[i,j].set_facecolor('#d9d9d9')
        ax[i,j].set_axisbelow(True)
        ax[i,j].grid(color='white', linestyle='dotted')
        ax[i,j].set_title(country)
        ax[i,j].legend(fontsize=9, loc='upper right')
        ax[i,0].set_ylabel('Capacity [GW]')
        ax[3,j].xaxis.label.set_visible(False)
        j=j+1
    fig.tight_layout()
    
    return
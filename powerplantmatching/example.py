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

import pandas as pd
import collections
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from .config import fueltype_to_life, fueltype_to_color
from .cleaning import clean_single
from .data import CARMA, ENTSOE, Capacity_stats, ESE, GEO, OPSD, WEPP, WRI
from .collection import (Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced_VRE,
                         Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced,
                         #TODO: Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced_RES,
                         Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced)
from .utils import lookup


def Show_all_plots():
    Plot_bar_comparison_single_matched()
    Plot_hbar_comparison_1dim(by='Country')
    Plot_hbar_comparison_1dim(by='Fueltype')
    return


def Plot_bar_comparison_single_matched(df=None, cleaned=True, use_saved_aggregation=True):
    """
    Plots two bar charts for comparison
    1.) Fueltypes on x-axis and capacity on y-axis, categorized by originating database.
    2.) Fueltypes on x-axis and capacity on y-axis, matched database
    """
    if df is None:
        df = Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced_VRE()
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


def Plot_hbar_comparison_1dim(by='Country', include_WEPP=True, include_RES=False):
    """
    Plots a horizontal bar chart with capacity on x-axis, country on y-axis.

    Parameters
    ----------
    by : string, defines how to group data
        Allowed values: 'Country' or 'Fueltype'

    """
    red_w_wepp, red_wo_wepp, wepp, statistics = gather_comparison_data(include_WEPP=include_WEPP,
                                                                       include_RES=include_RES)
    if include_WEPP:
        stats = lookup([red_w_wepp, red_wo_wepp, wepp, statistics],
                       keys=['Matched dataset w/ WEPP', 'Matched dataset w/o WEPP',
                             'WEPP only', 'Statistics OPSD'], by=by)/1000
    else:
        stats = lookup([red_wo_wepp, statistics],
                       keys=['Matched dataset w/o WEPP', 'Statistics OPSD'],
                       by=by)/1000

    # Presettings for the plots
    font={#'family' : 'normal',
          #'weight' : 'bold',
          'size'   : 24}
    plt.rc('font', **font)
    ax = stats.plot.barh(stacked=False, colormap='jet', figsize = (22,13))
    ax.set_xlabel('Installed Capacity [GW]')
    ax.yaxis.label.set_visible(False)
    ax.set_facecolor('#d9d9d9')                  # gray background
    ax.set_axisbelow(True)                       # puts the grid behind the bars
    ax.grid(color='white', linestyle='dotted')   # adds white dotted grid
    ax.legend(loc='best')
    ax.invert_yaxis()
    return


def Plot_bar_comparison_countries_fueltypes(ylabel=None, include_WEPP=True,
        include_RES=False, show_coverage=True, show_legend=True):
    """
    Plots per country an analysis, how the matched dataset and the statistics
    differ by fueltype.
    """
    if ylabel is None: ylabel = 'Capacity [GW]'

    red_w_wepp, red_wo_wepp, wepp, statistics = gather_comparison_data(include_WEPP=include_WEPP,
                                                                       include_RES=include_RES)
    countries = set()
    if include_WEPP:
        stats = lookup([red_w_wepp, red_wo_wepp, wepp, statistics],
                       keys=['Matched dataset w/ WEPP', 'Matched dataset w/o WEPP',
                             'WEPP only', 'Statistics OPSD'],
                       by='Country, Fueltype')/1000
        set.update(countries, set(red_w_wepp.Country), set(red_wo_wepp.Country),
                   set(wepp.Country), set(statistics.Country))
    else:
        stats = lookup([red_wo_wepp, statistics],
                       keys=['Matched dataset w/o WEPP', 'Statistics OPSD'],
                       by='Country, Fueltype')/1000
        set.update(countries, set(red_wo_wepp.Country), set(statistics.Country))

    # Presettings for the plots
    font={#'family' : 'normal',
          #'weight' : 'bold',
          'size'   : 12}
    plt.rc('font', **font)

    # Loop through countries.
    nrows, ncols = gather_nrows_ncols(len(countries))
    i,j = [0, 0]
    fig, ax = plt.subplots(nrows=nrows, ncols=ncols, sharex=True, sharey=False, figsize=(32,18))
    for country in sorted(countries):
        if j==ncols:
            i+=1
            j=0
        # Perform the plot
        stats[country].plot.bar(ax=ax[i,j], stacked=False, legend=False, colormap='jet')
        # Format the subplots nicely
        if show_coverage:
            ctry = stats[country]
            ctry.loc[:,u'Delta_squared'] = (ctry.iloc[:,0]-ctry.iloc[:,1])**2
            cov = 1 - ctry.Delta_squared.sum()/((ctry.iloc[:,1]**2).sum())
            ax[i,j].text(0.0, ax[i,j].get_ylim()[1]*0.95, u'Coverage = '+unicode(round(cov, 3)),
                         ha='left', va='top')
        if show_legend:
            ax[i,j].legend(fontsize=9, loc='best')
        ax[i,j].set_facecolor('#d9d9d9')
        ax[i,j].set_axisbelow(True)
        ax[i,j].grid(color='white', linestyle='dotted')
        ax[i,j].set_title(country)
        ax[i,0].set_ylabel(ylabel)
        ax[3,j].xaxis.label.set_visible(False)
        j+=1
    ax[0,0].legend(fontsize=9, loc='best')
    # After the loop, do the rest of the layouting.
    fig.tight_layout()
    return


def gather_comparison_data(include_WEPP=True, include_RES=False):
    queryexpr = 'Fueltype != "Solar" and Fueltype != "Wind" and Fueltype != "Geothermal"'
    # 1+2: WEPP itself + Reduced w/ WEPP
    if include_WEPP:
        wepp = WEPP()
        #TODO: This hack should be removed, as soon as OPSD releases its 'National Generation Capacity' update
        wepp.query('(YearCommissioned <= 2014) or (YearCommissioned != YearCommissioned)', inplace=True)

        if include_RES:
            red_w_wepp = Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced_VRE()
        else:
            red_w_wepp = Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced()
            red_w_wepp.query(queryexpr, inplace=True)
            wepp.query(queryexpr, inplace=True)
        #TODO: This hack should be removed, as soon as OPSD releases its 'National Generation Capacity' update
        red_w_wepp.query('(YearCommissioned <= 2014) or (YearCommissioned != YearCommissioned)', inplace=True)
    else:
        wepp = None
        red_w_wepp = None
    # 3: Reduced w/o WEPP
    if include_RES:
        #red_wo_wepp = Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced_RES()
        #TODO: The aforementioned one should be used, the lower is just a workaround.
        red_wo_wepp = Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced()
    else:
        red_wo_wepp = Carma_ENTSOE_ESE_GEO_OPSD_WRI_matched_reduced()
        red_wo_wepp.query(queryexpr, inplace=True)
    #TODO: This hack should be removed, as soon as OPSD releases its 'National Generation Capacity' update
    red_wo_wepp.query('(YearCommissioned <= 2014) or (YearCommissioned != YearCommissioned)', inplace=True)
    # 4: Statistics
    statistics = Capacity_stats()
    statistics.Fueltype.replace({'Mixed fuel types':'Other'}, inplace=True)
    statistics.query(queryexpr, inplace=True)
    return red_w_wepp, red_wo_wepp, wepp, statistics


def Plot_bar_decomissioning_curves(df=None, ylabel=None, title=None, legend_in_subplots=False):
    """
    Plots per country a decommissioning curve as a bar chart with capacity on y-axis,
    period on x-axis and categorized by fueltype.
    """
    if ylabel is None:
        ylabel = 'Capacity [GW]'
    if df is None:
        df = Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI_matched_reduced_VRE()
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

    nrows, ncols = gather_nrows_ncols(len(set(df.Country)))
    fig, ax = plt.subplots(nrows=nrows, ncols=ncols, sharex=True, sharey=False, figsize=(32,18))
    data_countries = df.groupby(['Country'])
    i,j = [0,0]
    labels_mpatches = collections.OrderedDict()
    for country in sorted(set(df.Country)):
        if j==ncols:
            i+=1
            j=0
        cntry_grp = data_countries.get_group(country)
        stats = pd.DataFrame(columns=[2015,2020,2025,2030,2035,2040,2045,2050])
        for yr in range(2015, 2055, 5):
            k = cntry_grp.groupby(['Fueltype']).sum()/1000
            stats.loc[:,yr] = k[yr]
        colors = stats.index.to_series().map(fueltype_to_color()).tolist()
        stats.T.plot.bar(ax=ax[i,j],stacked=True,legend=False,color=colors)
        # Pass the legend information into the Ordered Dict
        if not legend_in_subplots:
            stats_handle, stats_labels = ax[i,j].get_legend_handles_labels()
            for u, v in enumerate(stats_labels):
                if v not in labels_mpatches:
                    labels_mpatches[v] = mpatches.Patch(color=colors[u], label=v)
        else:
            ax[i,j].legend(fontsize=9, loc='best')
        # Format the subplots nicely
        ax[i,j].set_facecolor('#d9d9d9')
        ax[i,j].set_axisbelow(True)
        ax[i,j].grid(color='white', linestyle='dotted')
        ax[i,j].set_title(country)
        ax[i,0].set_ylabel(ylabel)
        ax[-1,j].xaxis.label.set_visible(False)
        j+=1
    # After the loop, do the rest of the layouting.
    fig.tight_layout()
    if isinstance(title, str):
        fig.suptitle(title, fontsize=24)
        fig.subplots_adjust(top=0.93)
    if not legend_in_subplots:
        fig.subplots_adjust(bottom=0.08)
        labels_mpatches = collections.OrderedDict(sorted(labels_mpatches.items()))
        fig.legend(handles=labels_mpatches.values(),labels=labels_mpatches.keys(),
                   loc=8, ncol=len(labels_mpatches), facecolor='#d9d9d9')
    return


def Plot_matchcount_stats(df):
    """
    Plots the number of matches across all databases
    """
    df = df.copy()
    df = df.iloc[:,0:7]
    df.loc[:,'MatchCount'] = df.notnull().sum(axis=1)
    stats = df.groupby(['MatchCount']).size()
    stats.plot.bar()
    return


def gather_nrows_ncols(x):
    """
    Derives [nrows, ncols] based on x plots, so that a subplot looks nicely.

    Parameters
    ----------
    x : int, Number of subplots between [0, 42]
    """
    import math
    def calc(n, m):
        while (n*m < x): m += 1
        return n, m

    if not isinstance(x, int):
        raise ValueError('An integer needs to be passed to this function.')
    elif x <= 0:
        raise ValueError('The given number of subplots is less or equal zero.')
    elif x > 42:
        raise ValueError('Are you sure that you want to put more than 42 subplots in one diagram?\n'+\
                         'You better don\'t, it looks squeezed. Otherwise adapt the code.')
    k = math.sqrt(x)
    if k.is_integer():
        return [int(k), int(k)] #square format
    else:
        k = int(math.floor(k))
        # Solution 1
        n, m = calc(k, k+1)
        sol1 = n*m
        # Solution 2:
        n, m = calc(k-1, k+1)
        sol2 = n*m
        if sol2 > sol1: n, m = calc(k, k+1)
        return [n, m]
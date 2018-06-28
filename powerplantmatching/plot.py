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
import numpy as np
import pandas as pd
import collections
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from mpl_toolkits.basemap import Basemap
from matplotlib.patches import Circle, Ellipse
from matplotlib.legend_handler import HandlerPatch
from matplotlib import rcParams, cycler
from matplotlib.lines import Line2D
from matplotlib.offsetbox import AnchoredText
import seaborn as sns

from .config import fueltype_to_life, fueltype_to_color
from .cleaning import clean_single
from .data import CARMA, ENTSOE, Capacity_stats, ESE, GEO, OPSD, WEPP, WRI
from .collection import (Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced_VRE,
                         Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced,
                         Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_matched_reduced_VRE,
                         Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_matched_reduced)
from .utils import lookup, set_uncommon_fueltypes_to_other


def Show_all_plots():
    bar_comparison_single_matched()
    comparison_1dim(by='Country')
    comparison_1dim(by='Fueltype')
    return


def fueltype_stats(df):
    stats = lookup(set_uncommon_fueltypes_to_other(df), by='Fueltype')
    plt.pie(stats, colors=stats.index.to_series().map(fueltype_to_color(True)).fillna('gray'),
           labels=stats.index, autopct='%1.1f%%')


def powerplant_map(df, alternative_color_style=True, scale=1e5, european_bounds=True, 
                   legendscale=1, **kwargs):
#   TODO: add reference circle in legend
    figsize = kwargs.get('figsize', (7,5))
    with sns.axes_style('darkgrid'):
        df = set_uncommon_fueltypes_to_other(df)
        shown_fueltypes = df.Fueltype.unique()
        df = df[df.lat.notnull()]
        fig, ax = plt.subplots(figsize=figsize)

        ax.scatter(df.lon, df.lat, s=df.Capacity/scale, 
                   c=df.Fueltype.map(fueltype_to_color(alternative_color_style)))

        ax.set_xlabel('')
        ax.set_ylabel('')
        if european_bounds:
            ax.set_xlim(-13, 34)
            ax.set_ylim(35, 71.65648314)
        draw_basemap()
        ax.set_facecolor('white')
        fig.tight_layout(pad=0.5)

        legendcols = pd.Series(fueltype_to_color(alternative_color_style)
                    ).reindex(shown_fueltypes)
        handles = sum(legendcols.apply(lambda x :
            make_legend_circles_for([10.], scale=scale*legendscale, facecolor=x)).tolist(), [])
        fig.legend(handles, legendcols.index,
                   handler_map=make_handler_map_to_scale_circles_as_in(ax),
                   ncol=kwargs.get('ncol', 3), loc=kwargs.get('loc', "upper left"),
                   frameon=False, fontsize=kwargs.get('fontsize', 11))
        return fig, ax


def comparison_single_matched_bar(df=None, include_WEPP=True, cleaned=True,
                                  use_saved_aggregation=True, figsize=(9,5),
                                  exclude=['Geothermal','Solar','Wind'],
                                  axes_style='whitegrid'):
    """
    Plots two bar charts for comparison
    1.) Fueltypes on x-axis and capacity on y-axis, categorized by originating database.
    2.) Fueltypes on x-axis and capacity on y-axis, matched database
    """
    if df is None:
        if include_WEPP:
            df = (Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced()
                  .loc[lambda x:  ~x.Fueltype.isin(exclude)])
        else:
            df = (Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_matched_reduced()
                  .loc[lambda x:  ~x.Fueltype.isin(exclude)])

    if cleaned:
        carma = clean_single(CARMA(), dataset_name='CARMA', use_saved_aggregation=use_saved_aggregation)
        entsoe = clean_single(ENTSOE(), dataset_name='ENTSOE', use_saved_aggregation=use_saved_aggregation)
        geo = clean_single(GEO(), dataset_name='GEO', aggregate_powerplant_units=False)
        opsd = clean_single(OPSD(), dataset_name='OPSD', use_saved_aggregation=use_saved_aggregation)
        wri = clean_single(WRI(), dataset_name='WRI', use_saved_aggregation=use_saved_aggregation)
        if include_WEPP:
            wepp = clean_single(WEPP(), dataset_name='WEPP', use_saved_aggregation=use_saved_aggregation)
    else:
        carma = CARMA()
        entsoe = ENTSOE()
        geo = GEO()
        opsd = OPSD()
        wri = WRI()
        if include_WEPP:
            wepp = WEPP()
    ese = ESE()
    if include_WEPP:
        stats = lookup(df=[carma, entsoe, ese, geo, opsd, wepp, wri],
                       keys=['CARMA','ENTSO-E','ESE','GEO','OPSD','WEPP','WRI'],
                       exclude=exclude, by='Fueltype')/1000
    else:
        stats = lookup(df=[carma, entsoe, ese, geo, opsd, wri],
                       keys=['CARMA','ENTSO-E','ESE','GEO','OPSD','WRI'],
                       exclude=exclude, by='Fueltype')/1000
    stats_reduced = lookup(df, by='Fueltype')/1000
    # Presettings for the plots
    with sns.axes_style(axes_style):
        font={'size'   : 12}
        plt.rc('font', **font)
        fig, ax = plt.subplots(nrows=1, ncols=2, sharex=False, sharey=True, figsize=figsize)
        # 1st Plot with single datasets on the left side.
        stats.plot.bar(ax=ax[0], stacked=False, legend=True, colormap='Accent')
        ax[0].set_ylabel('Installed Capacity [$GW$]')
        ax[0].set_title('Capacities of Single Databases')
        ax[0].set_facecolor('#d9d9d9')                  # gray background
        ax[0].set_axisbelow(True)                       # puts the grid behind the bars
        ax[0].grid(color='white', linestyle='dotted')   # adds white dotted grid
        # 2nd Plot with reduced dataset
        stats_reduced.plot.bar(ax=ax[1], stacked=False, colormap='jet')
        ax[1].xaxis.label.set_visible(False)
        ax[1].set_title('Capacities of Matched Dataset')
        ax[1].set_facecolor('#d9d9d9')
        ax[1].set_axisbelow(True)
        ax[1].grid(color='white', linestyle='dotted')
        fig.tight_layout()
    return fig, ax


def comparison_1dim(by='Country', include_WEPP=True, include_VRE=False,
                    year=2016, how='hbar', axes_style='whitegrid', 
                    **kwargs):
    """
    Plots a horizontal bar chart with capacity on x-axis, ``by`` on y-axis.

    Parameters
    ----------
    by : string, defines how to group data
        Allowed values: 'Country' or 'Fueltype'

    """
    red_w_wepp, red_wo_wepp, wepp, statistics = \
                    gather_comparison_data(include_WEPP=include_WEPP,
                                           include_VRE=include_VRE,
                                           year=year)
    if include_WEPP:
        stats = lookup([red_w_wepp, red_wo_wepp, wepp, statistics],
                       keys=['Matched dataset w/ WEPP', 'Matched dataset w/o WEPP',
                             'WEPP only', 'Statistics OPSD'],
                       by=by, exclude=['Geothermal','Solar','Wind','Battery'])/1000
    else:
        stats = lookup([red_wo_wepp, statistics],
                       keys=['Matched dataset w/o WEPP', 'Statistics OPSD'],
                       by=by, exclude=['Geothermal','Solar','Wind','Battery'])/1000

    font={'size'   : 12}
    plt.rc('font', **font)
    if how == 'hbar':
        with sns.axes_style(axes_style):
            figsize = kwargs.get('figsize', (7,6))
            fig, ax = plt.subplots(figsize=figsize)
            stats.plot.barh(ax=ax, stacked=False)#, colormap='jet')
            ax.set_xlabel('Installed Capacity [GW]')
            ax.yaxis.label.set_visible(False)
#           ax.set_facecolor('#d9d9d9')                  # gray background
            ax.set_facecolor('lightgrey')
            ax.set_axisbelow(True)                       # puts the grid behind the bars
            ax.grid(color='white', linestyle='dotted')   # adds white dotted grid
            ax.legend(loc='best')
            ax.invert_yaxis()
            return fig, ax
    if how == 'scatter':
        stats.loc[:, by] = stats.index.astype(str)  #Required for seaborn scatter plot
        if len(stats.columns)-1 >= 3:
            g = sns.pairplot(stats, diag_kind='kde', hue=by, palette='Set2')
        else:
            g = sns.pairplot(stats, diag_kind='kde', hue=by, palette='Set2',
                             x_vars=stats.columns[0], y_vars=stats.columns[1])
        for i in range(0, len(g.axes)):
            for j in range(0, len(g.axes[0])):
                g.axes[i, j].set(xscale='log', yscale='log', xlim=(1,200), ylim=(1,200))
                if i != j: # plot 45 degree identity line
                    g.axes[i, j].plot([1,200], [1,200], 'k-', alpha=0.25, zorder=0)
        figsize = kwargs.get('figsize', None)
        if figsize is not None:
            g.fig.set_size_inches(figsize)
        return g.fig, g.axes


def comparison_countries_fueltypes_bar(dfs=None, ylabel=None, include_WEPP=True,
        include_VRE=False, show_indicators=True, legend_in_subplots=False, year=2015,
        figsize=(7,5)):
    """
    Plots per country an analysis, how the given datasets differ by fueltype.

    Parameters
    ----------
    dfs : dict
        keys : labels
        values : pandas.Dataframe containing the data to be plotted
    ylabel : str
        Label for y-axis
    include_WEPP : bool
        Switch to include WEPP-based data
    include_VRE : bool
        Switch to include VRE data
    show_coverage : bool
        Switch whether to calculate a coverage between to datasets
    legend_in_subplots : bool
        Switch whether to show the legend in subplots (True) or below (False)
    year : int
        Only plot units which have a commissioning year smaller or equal this value
    """
    if ylabel is None: ylabel = 'Capacity [GW]'

    countries = set()
    if dfs is None:
        red_w_wepp, red_wo_wepp, wepp, statistics = (
                gather_comparison_data(include_WEPP=include_WEPP,
                                       include_VRE=include_VRE, year=year))
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
    else:
        stats = lookup(dfs.values(), keys=dfs.keys(), by='Country, Fueltype')/1000
        stats.sort_index(axis=1, inplace=True)
        for k, v in dfs.items():
            set.update(countries, set(v.Country))

    # Presettings for the plots
    font={'size'   : 12}
    plt.rc('font', **font)
    # Loop through countries.
    nrows, ncols = gather_nrows_ncols(len(countries))
    i,j = [0, 0]
    labels_mpatches = collections.OrderedDict()
    fig, ax = plt.subplots(nrows=nrows, ncols=ncols, sharex=True, sharey=False,
                           squeeze=False, figsize=figsize)
    for country in sorted(countries):
        if j==ncols: i+=1; j=0
        # Perform the plot
        stats.loc[country].plot.bar(ax=ax[i,j], stacked=False, legend=False, colormap='jet')
        if show_indicators:
            ctry = stats.loc[country]
            r_sq = round(ctry.corr().iloc[0,1]**2, 3)
            #TODO: Make sure that matched is always in first and stats in last column!
            cov = round(ctry.iloc[:,0].sum() / ctry.iloc[:,-1].sum(), 3)
            txt = AnchoredText("\n" + r'$R^{2} = $%s'%r_sq + "\n"
                               r'$\frac{\sum P_{match}}{\sum P_{stats}} = $%s'%cov,
                               loc=2, prop={'size':11})
            txt.patch.set(boxstyle='round', alpha=0.5)
            ax[i,j].add_artist(txt)
        # Pass the legend information into the Ordered Dict
        if not legend_in_subplots:
            stats_handle, stats_labels = ax[i,j].get_legend_handles_labels()
            for u, v in enumerate(stats_labels):
                if v not in labels_mpatches:
                    labels_mpatches[v] = mpatches.Patch(
                            color=stats_handle[u].patches[0].get_facecolor(), label=v)
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
    if not legend_in_subplots:
        fig.subplots_adjust(bottom=0.14)
        labels_mpatches = collections.OrderedDict(sorted(labels_mpatches.items()))
        fig.legend(labels_mpatches.values(), labels_mpatches.keys(),
                   loc=8, ncol=len(labels_mpatches), facecolor='#d9d9d9')
    return fig, ax


#this approach could be an alternative to bar_comparison_countries_fueltypes, but not sure
def fueltype_and_country_totals_bar(dfs, keys, figsize=(18,8)):
    df = lookup(dfs, keys)
    countries = df.index.get_level_values('Country').unique()
    n = len(countries)
    subplots = gather_nrows_ncols(n)
    fig, ax = plt.subplots(*subplots, figsize=figsize)

    if sum(subplots)>2:
        ax_iter = ax.flat
    else:
        ax_iter = np.array(ax).flat
    for country in countries:
        ax = next(ax_iter)
        df.loc[country].plot.bar(ax=ax, sharex=True, legend=None)
        ax.set_xlabel('')
        ax.ticklabel_format(axis='y', style='sci', scilimits=(-2,2))
        ax.set_title(country)
    handles, labels = ax.get_legend_handles_labels()
    fig.tight_layout(pad=1)
    fig.legend(handles, labels, loc=9, ncol=2, bbox_to_anchor=(0.53,0.99))
    fig.subplots_adjust(top=0.9)
    return fig, ax


def fueltype_totals_bar(dfs, keys, figsize=(7,4), unit='GW',
                        last_as_marker=False, axes_style='whitegrid',
                        exclude=[], **kwargs):
    with sns.axes_style(axes_style):
        fig, ax = plt.subplots(1,1, figsize=figsize)
        if last_as_marker:
            as_marker = dfs[-1]
            dfs = dfs[:-1]
            as_marker_key = keys[-1]
            keys = keys[:-1]
        fueltotals = lookup(dfs, keys=keys, by='Fueltype', unit=unit)
        fueltotals.plot(kind="bar",
                           ax=ax, legend='reverse', edgecolor='none', rot=90,
                           **kwargs)
        if last_as_marker:
            fueltotals = lookup(as_marker, keys=as_marker_key, by='Fueltype', unit=unit)
            fueltotals.plot(ax=ax, label=as_marker_key, markeredgecolor='none', rot=90,
                            marker='D', markerfacecolor='darkslategray', linestyle='None',
                            **kwargs)
        ax.legend(loc=0)
        ax.set_ylabel(r'Capacity [$%s$]'%unit)
        ax.xaxis.grid(False)
        fig.tight_layout(pad=0.5)
        return fig, ax


def matched_fueltype_totals_bar(figsize=(9,4), axes_style='whitegrid'):
    from . import data
    from .collection import Carma_ENTSOE_GEO_OPSD_WRI_matched_reduced
    matched = set_uncommon_fueltypes_to_other(
            Carma_ENTSOE_GEO_OPSD_WRI_matched_reduced())
    matched.loc[matched.Fueltype=='Waste', 'Fueltype']='Other'
    geo=set_uncommon_fueltypes_to_other(data.GEO())
    carma=set_uncommon_fueltypes_to_other(data.CARMA())
    wri=set_uncommon_fueltypes_to_other(data.WRI())
    ese=set_uncommon_fueltypes_to_other(data.ESE())
    entsoe = set_uncommon_fueltypes_to_other(data.Capacity_stats())
    opsd = set_uncommon_fueltypes_to_other(data.OPSD())
    entsoedata = set_uncommon_fueltypes_to_other(data.ENTSOE())

    matched.Capacity = matched.Capacity/1000.
    geo.Capacity = geo.Capacity/1000.
    carma.Capacity = carma.Capacity/1000.
    wri.Capacity = wri.Capacity/1000.
    ese.Capacity = ese.Capacity/1000.
    entsoe.Capacity = entsoe.Capacity/1000.
    opsd.Capacity = opsd.Capacity/1000.
    entsoedata.Capacity =  entsoedata.Capacity/1000.

    with sns.axes_style(axes_style):
        fig, (ax1,ax2) = plt.subplots(1,2, figsize=figsize, sharey=True)
        databases = lookup([carma, entsoedata, ese, geo, opsd, wri],
                   keys=[ 'CARMA', 'ENTSOE','ESE', 'GEO','OPSD', 'WRI'], by='Fueltype')
        databases = databases[databases.sum(1)>5]
        databases.plot(kind='bar', ax=ax1, edgecolor='none', rot=70)
        datamatched = lookup(matched, by='Fueltype')
        datamatched.index.name=''
        datamatched.name='Matched Database'
        datamatched.plot(kind='bar', ax=ax2, color='steelblue',
                         edgecolor='none', rot=70)
        ax2.legend()
        ax1.set_ylabel('Capacity [GW]')
        ax1.xaxis.grid(False)
        ax2.xaxis.grid(False)
        fig.tight_layout(pad=0.5)
        return fig, [ax1,ax2]


def country_totals_hbar(dfs, keys, exclude_fueltypes=['Solar', 'Wind'],
                        figsize=(7,5), unit='GW',axes_style='whitegrid'):
    with sns.axes_style(axes_style):
        fig, ax = plt.subplots(1,1, figsize=figsize)
        countrytotals = lookup(dfs,
                   keys=keys, by='Country',
                    exclude=exclude_fueltypes,
                    unit=unit)
        countrytotals[::-1][1:].plot(kind="barh",
                           ax=ax, legend='reverse', edgecolor='none')
        ax.set_xlabel('Capacity [%s]'%unit)
        ax.yaxis.grid(False)
        ax.set_ylabel('')
        fig.tight_layout(pad=0.5)
        return fig, ax


def factor_comparison(dfs, keys, figsize=(7,9)):
    dfs = [set_uncommon_fueltypes_to_other(df) for df in dfs]
    compare = lookup(dfs, keys=keys, exclude=['Solar', 'Wind']).fillna(0.)
    compare = compare.append(pd.concat([compare.groupby(level='Country').sum()],
                              keys=['Total']).swaplevel()).sort_index()/1000
    n_countries, n_fueltypes = compare.index.levshape
    

    c= [fueltype_to_color(True)[i] for i in compare.index.levels[1]]
    rcParams["axes.prop_cycle"] = cycler(color=c)

    #where both are zero,
    compare[compare.sum(1)<0.5]=np.nan
    
    fig, ax = plt.subplots(1,1, figsize=figsize)
    compare = compare.unstack('Country').swaplevel(axis=1).sort_index(axis=1)
    compare.T.plot(ax=ax, markevery=(0,2),
                   style='o', markersize=5)
    compare.T.plot(ax=ax, markevery=(1,2),
                   style='s', legend=None, markersize=4.5)

    lgd = ax.get_legend_handles_labels()

    for i,j in enumerate(compare.columns.levels[0]):
        ax.plot(np.array([0,1])+(2*i), compare[j].T)

    indexhandles = [Line2D([0.4,.6],[.4,.6], marker=m, linewidth=0.,
                                     markersize=msize,
                                     color='w', markeredgecolor='k', markeredgewidth=0.5)
                    for m, msize in [['o', 5.], ['s', 4.5]]]
    ax.add_artist(ax.legend(handles=indexhandles, labels=keys))
    ax.legend(handles= lgd[0][:len(c)], labels=lgd[1][:len(c)]
               ,title=False, loc=2)

    ax.set_xlim(-1,n_countries*2 +1)
    ax.xaxis.grid(False)
    ax.set_xticks(np.linspace(0.5,n_countries*2-1.5,n_countries))
    ax.set_xticklabels(compare.columns.levels[0].values, rotation=90)
    ax.set_xlabel('')
    ax.set_ylabel('Capacity [GW]')
    fig.tight_layout(pad=0.5)


def bar_decomissioning_curves(df=None, ylabel=None, title=None, legend_in_subplots=False):
    """
    Plots per country a decommissioning curve as a bar chart with capacity on y-axis,
    period on x-axis and categorized by fueltype.
    """
    if ylabel is None:
        ylabel = 'Capacity [GW]'
    if df is None:
        df = Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced_VRE()
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
    fig, ax = plt.subplots(nrows=nrows, ncols=ncols, sharex=True, sharey=False,
                           squeeze=False, figsize=(32/1.2,18/1.2))
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
        fig.legend(labels_mpatches.values(), labels_mpatches.keys(),
                   loc=8, ncol=len(labels_mpatches), facecolor='#d9d9d9')
    return fig, ax


def boxplot_gross_to_net(axes_style='darkgrid', **kwargs):
    """
    """
    from .heuristics import gross_to_net_factors as gtn
    with sns.axes_style(axes_style):
        df = gtn(return_entire_data=True).loc[lambda df: df.energy_source_level_2!='Hydro']
        df.loc[:,'FuelTech'] = df.energy_source_level_2 +'\n(' + df.technology + ')'
        df = df.groupby('FuelTech').filter(lambda x: len(x)>=10)
        dfg = df.groupby('FuelTech')
        # plot
        fig, ax = plt.subplots(**kwargs)
        df.boxplot(ax=ax, column='ratio', by='FuelTech', rot=90, showmeans=True)
        ax.title.set_visible(False)
        ax.xaxis.label.set_visible(False)
        ax2 = ax.twiny()
        ax2.set_xlim(ax.get_xlim())
        ax2.set_xticks([i+1 for i in range(len(dfg))])
        ax2.set_xticklabels(['$n$=%d'%(len(v)) for k, v in dfg])
        fig.suptitle('')
        return fig, ax


def area_yearcommissioned(dfs, figsize=(7,5), ylabel='Capacity [$GW$]'):
    """
    Plots an area chart by commissioning year.
    """
    dfp = {}
    for n, df in dfs.items():
        dfp[n] = df.pivot_table(values='Capacity', index='YearCommissioned',
                                columns='Fueltype', aggfunc='sum')/1000
    fig, ax = plt.subplots(nrows=2, ncols=2, sharex=True, sharey=True,
                           squeeze=False, figsize=figsize)
    ax[0,0].set_xlim((1900,2016))
    colors = dfp[dfs.keys()[-1]].columns.to_series().map(fueltype_to_color()).tolist()
    i,j = (0,0)
    for n, df in dfp.items():
        if j==2: i+=1; j=0
        df.plot.area(ax=ax[i,j],stacked=True,legend=False,color=colors)
        ax[i,j].set_title(n)
        ax[i,j].set_facecolor('#d9d9d9')
        ax[i,j].set_axisbelow(True)
        ax[i,j].grid(color='white', linestyle='dotted')
        ax[i,j].set_ylabel(ylabel)
        j+=1
    ax[-1,-1].legend(fontsize=12, loc='center left', bbox_to_anchor=(1, 0.5))
    fig.tight_layout()
    return fig, ax


#%% Plot utilities


def gather_comparison_data(include_WEPP=True, include_VRE=False, **kwargs):
    yr = kwargs.get('year', 2016)
    queryexpr = kwargs.get('queryexpr', 'Fueltype != "Solar" and Fueltype != "Wind" and Fueltype != "Geothermal"')
    # 1+2: WEPP itself + Reduced w/ WEPP
    if include_WEPP:
        wepp = WEPP()
        wepp.query('(YearCommissioned <= {:04d}) or (YearCommissioned != YearCommissioned)'.format(yr), inplace=True)

        if include_VRE:
            red_w_wepp = Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced_VRE()
        else:
            red_w_wepp = Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WEPP_WRI_matched_reduced()
            red_w_wepp.query(queryexpr, inplace=True)
            wepp.query(queryexpr, inplace=True)
        red_w_wepp.query('(YearCommissioned <= {:04d}) or (YearCommissioned != YearCommissioned)'.format(yr), inplace=True)
    else:
        wepp = None
        red_w_wepp = None
    # 3: Reduced w/o WEPP
    if include_VRE:
        red_wo_wepp = Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_matched_reduced_VRE()
    else:
        red_wo_wepp = Carma_ENTSOE_ESE_GEO_IWPDCY_OPSD_WRI_matched_reduced()
        red_wo_wepp.query(queryexpr, inplace=True)
    red_wo_wepp.query('(YearCommissioned <= {:04d}) or (YearCommissioned != YearCommissioned)'.format(yr), inplace=True)
    # 4: Statistics
    statistics = Capacity_stats(year=yr)
    statistics.Fueltype.replace({'Mixed fuel types':'Other'}, inplace=True)
    if not include_VRE:
        statistics.query(queryexpr, inplace=True)
    return red_w_wepp, red_wo_wepp, wepp, statistics


def matchcount_stats(df):
    """
    Plots the number of matches against the number of involved databases, across all databases.
    """
    df = df.copy().iloc[:,0:7]
    df.loc[:,'MatchCount'] = df.notnull().sum(axis=1)
    df.groupby(['MatchCount']).size().plot.bar()
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

def make_handler_map_to_scale_circles_as_in(ax, dont_resize_actively=False):
    fig = ax.get_figure()
    def axes2pt():
        return np.diff(
                ax.transData.transform([(0,0), (1,1)]), axis=0)[0] * (72./fig.dpi)
    ellipses = []
    if not dont_resize_actively:
        def update_width_height(event):
            dist = axes2pt()
            for e, radius in ellipses: e.width, e.height = 2. * radius * dist
        fig.canvas.mpl_connect('resize_event', update_width_height)
        ax.callbacks.connect('xlim_changed', update_width_height)
        ax.callbacks.connect('ylim_changed', update_width_height)

    def legend_circle_handler(legend, orig_handle, xdescent, ydescent,
                              width, height, fontsize):
        w, h = 2. * orig_handle.get_radius() * axes2pt()
        e = Ellipse(xy=(0.5*width-0.5*xdescent, 0.5*height-0.5*ydescent),
                        width=w, height=w)
        ellipses.append((e, orig_handle.get_radius()))
        return e
    return {Circle: HandlerPatch(patch_func=legend_circle_handler)}

def make_legend_circles_for(sizes, scale=1.0, **kw):
    return [Circle((0,0), radius=(s/scale)**0.5, **kw) for s in sizes]

def draw_basemap(resolution='l', ax=None,country_linewidth=0.3, coast_linewidth=
                     0.4, zorder=None,  **kwds):
    if ax is None:
        ax = plt.gca()
    m = Basemap(*(list(ax.viewLim.min) + list(ax.viewLim.max)), resolution=resolution, ax=ax, **kwds)
    m.drawcoastlines(linewidth=coast_linewidth, zorder=zorder)
    m.drawcountries(linewidth=country_linewidth, zorder=zorder)
    return m


orderdedfuels = ['Hydro', #'Solar', 'Wind',
                     'Nuclear','Hard Coal', 'Lignite', 'Oil', 'Natural Gas','Other']


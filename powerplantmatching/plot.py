# Copyright 2016-2018 Fabian Hofmann (FIAS), Jonas Hoersch (KIT, IAI) and
# Fabian Gotzens (FZJ, IEK-STE)

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import logging
import math

# import collections
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from cycler import cycler
from matplotlib import rcParams
from matplotlib.legend_handler import HandlerPatch
from matplotlib.lines import Line2D

# import matplotlib.patches as mpatches
from matplotlib.patches import Circle, Ellipse

from .core import get_config, get_obj_if_Acc
from .utils import lookup, set_uncommon_fueltypes_to_other, to_list_if_other

logger = logging.getLogger(__name__)


cartopy_present = True
try:
    import cartopy
    import cartopy.crs as ccrs
except (ModuleNotFoundError, ImportError):
    cartopy_present = False

if not cartopy_present:
    logger.warning("Cartopy not existent.")


def fueltype_stats(df):
    df = get_obj_if_Acc(df)
    stats = lookup(set_uncommon_fueltypes_to_other(df), by="Fueltype")
    plt.pie(
        stats,
        colors=stats.index.to_series()
        .map(get_config()["fuel_to_color"])
        .fillna("gray"),
        labels=stats.index,
        autopct="%1.1f%%",
    )


def powerplant_map(
    df,
    scale=2e1,
    alpha=0.6,
    european_bounds=True,
    fillcontinents=False,
    legendscale=1,
    resolution=True,
    figsize=None,
    ncol=2,
    loc="upper left",
):
    df = get_obj_if_Acc(df)
    # TODO: add reference circle in legend
    with sns.axes_style("darkgrid"):
        df = set_uncommon_fueltypes_to_other(df)
        shown_fueltypes = df.Fueltype.unique()
        df = df[df.lat.notnull()]
        sub_kw = {"projection": ccrs.PlateCarree()} if cartopy_present else {}
        fig, ax = plt.subplots(figsize=figsize, subplot_kw=sub_kw)

        ax.scatter(
            df.lon,
            df.lat,
            s=df.Capacity / scale,
            c=df.Fueltype.map(get_config()["fuel_to_color"]),
            edgecolor="face",
            facecolor="face",
            alpha=alpha,
        )

        legendcols = pd.Series(get_config()["fuel_to_color"]).reindex(shown_fueltypes)
        handles = sum(
            legendcols.apply(
                lambda x: make_legend_circles_for(
                    [10.0], scale=scale * legendscale, facecolor=x
                )
            ).tolist(),
            [],
        )
        ax.legend(
            handles,
            legendcols.index,
            handler_map=make_handler_map_to_scale_circles_as_in(ax),
            markerscale=1,
            ncol=ncol,
            loc=loc,
            frameon=True,
            fancybox=True,
            edgecolor="w",
            facecolor="w",
            framealpha=1,
        )

        ax.set_xlabel("")
        ax.set_ylabel("")
        if european_bounds:
            ax.set_xlim(-13, 40)
            ax.set_ylim(35, 72)
        draw_basemap(ax=ax, resolution=resolution, fillcontinents=fillcontinents)
        ax.set_facecolor("w")

        fig.tight_layout(pad=0.5)
        if cartopy_present:
            ax.spines["geo"].set_visible(False)
        return fig, ax


# This approach is an alternative to bar_comparison_countries_fueltypes()
def fueltype_and_country_totals_bar(dfs, keys=None, figsize=(18, 8)):
    dfs = get_obj_if_Acc(dfs)
    dfs = to_list_if_other(dfs)
    df = lookup(dfs, keys)
    countries = df.index.get_level_values("Country").unique()
    n = len(countries)
    subplots = gather_nrows_ncols(n)
    fig, ax = plt.subplots(*subplots, figsize=figsize)

    if sum(subplots) > 2:
        ax_iter = ax.flat
    else:
        ax_iter = np.array(ax).flat
    for country in countries:
        ax = next(ax_iter)
        df.loc[country].plot.bar(ax=ax, sharex=True, legend=None)
        ax.set_xlabel("")
        ax.ticklabel_format(axis="y", style="sci", scilimits=(-2, 2))
        ax.set_title(country)
    handles, labels = ax.get_legend_handles_labels()
    fig.tight_layout(pad=1)
    fig.legend(handles, labels, loc=9, ncol=2, bbox_to_anchor=(0.53, 0.99))
    fig.subplots_adjust(top=0.9)
    return fig, ax


def fueltype_totals_bar(
    dfs,
    keys=None,
    figsize=(7, 4),
    unit="GW",
    last_as_marker=False,
    axes_style="whitegrid",
    exclude=[],
    **kwargs,
):
    dfs = get_obj_if_Acc(dfs)
    dfs = to_list_if_other(dfs)
    dfs = [df[~df.Fueltype.isin(exclude)] for df in dfs]
    with sns.axes_style(axes_style):
        fig, ax = plt.subplots(1, 1, figsize=figsize)
        if last_as_marker:
            as_marker = dfs[-1]
            dfs = dfs[:-1]
            as_marker_key = keys[-1]
            keys = keys[:-1]
        fueltotals = lookup(dfs, keys=keys, by="Fueltype", unit=unit)
        fueltotals.plot(
            kind="bar", ax=ax, legend="reverse", edgecolor="none", rot=90, **kwargs
        )
        if last_as_marker:
            fueltotals = lookup(as_marker, keys=as_marker_key, by="Fueltype", unit=unit)
            fueltotals.plot(
                ax=ax,
                label=as_marker_key,
                markeredgecolor="none",
                rot=90,
                marker="D",
                linestyle="None",
                markerfacecolor="darkslategray",
                **kwargs,
            )
        ax.legend(loc=0)
        ax.set_ylabel(f"Capacity [${unit}$]")
        ax.xaxis.grid(False)
        fig.tight_layout(pad=0.5)
        return fig, ax


def country_totals_hbar(
    dfs,
    keys=None,
    exclude_fueltypes=["Solar", "Wind"],
    figsize=(7, 5),
    unit="GW",
    axes_style="whitegrid",
):
    with sns.axes_style(axes_style):
        fig, ax = plt.subplots(1, 1, figsize=figsize)
        countrytotals = lookup(
            dfs, keys=keys, by="Country", exclude=exclude_fueltypes, unit=unit
        )
        countrytotals[::-1][1:].plot(
            kind="barh", ax=ax, legend="reverse", edgecolor="none"
        )
        ax.set_xlabel(f"Capacity [{unit}]")
        ax.yaxis.grid(False)
        ax.set_ylabel("")
        fig.tight_layout(pad=0.5)
        return fig, ax


def factor_comparison(dfs, keys=None, figsize=(12, 9)):
    with sns.axes_style("whitegrid"):
        compare = lookup(dfs, keys=keys, exclude=["Solar", "Wind"]).fillna(0.0)
        compare = (
            pd.concat(
                [
                    compare,
                    pd.concat(
                        [compare.groupby(level="Country").sum()], keys=["Total"]
                    ).swaplevel(),
                ]
            ).sort_index()
            / 1000
        )
        n_countries, n_fueltypes = compare.index.levshape
        c = [get_config()["fuel_to_color"][i] for i in compare.index.levels[1]]
        rcParams["axes.prop_cycle"] = cycler(color=c)

        # where both are zero,
        compare[compare.sum(1) < 0.5] = np.nan

        fig, ax = plt.subplots(1, 1, figsize=figsize)
        compare = (
            compare.unstack("Country")
            .swaplevel(axis=1)
            .sort_index(axis=1)
            .reindex(columns=keys, level=1)
        )
        compare.T.plot(ax=ax, markevery=(0, 2), style="o", markersize=5)
        compare.T.plot(ax=ax, markevery=(1, 2), style="s", legend=None, markersize=4.5)

        lgd = ax.get_legend_handles_labels()

        for i, j in enumerate(compare.columns.levels[0]):
            ax.plot(np.array([0, 1]) + (2 * i), compare[j].T)

        indexhandles = [
            Line2D(
                [0.4, 0.6],
                [0.4, 0.6],
                marker=m,
                linewidth=0.0,
                markersize=msize,
                color="w",
                markeredgecolor="k",
                markeredgewidth=0.5,
            )
            for m, msize in [["o", 5.0], ["s", 4.5]]
        ]
        ax.add_artist(ax.legend(handles=indexhandles, labels=keys))
        ax.legend(handles=lgd[0][: len(c)], labels=lgd[1][: len(c)], title=False, loc=2)

        ax.set_xlim(-1, n_countries * 2 + 1)
        ax.xaxis.grid(False)
        ax.set_xticks(np.linspace(0.5, n_countries * 2 - 1.5, n_countries))
        ax.set_xticklabels(compare.columns.levels[0].values, rotation=90)
        ax.set_xlabel("")
        ax.set_ylabel("Capacity [GW]")
        fig.tight_layout(pad=0.5)
        return fig, ax


def boxplot_gross_to_net(axes_style="darkgrid", **kwargs):
    """ """
    from .heuristics import gross_to_net_factors as gtn

    with sns.axes_style(axes_style):
        df = gtn(return_entire_data=True).loc[
            lambda df: df.energy_source_level_2 != "Hydro"
        ]
        df["FuelTech"] = df.energy_source_level_2 + "\n(" + df.technology + ")"
        df = df.groupby("FuelTech").filter(lambda x: len(x) >= 10)
        dfg = df.groupby("FuelTech")
        # plot
        fig, ax = plt.subplots(**kwargs)
        df.boxplot(ax=ax, column="ratio", by="FuelTech", rot=90, showmeans=True)
        ax.title.set_visible(False)
        ax.set_ylabel("Ratio of gross/net [$-$]")
        ax.xaxis.label.set_visible(False)
        ax.set_ylabel("Net / Gross")
        ax2 = ax.twiny()
        ax2.set_xlim(ax.get_xlim())
        ax2.set_xticks([i + 1 for i in range(len(dfg))])
        ax2.set_xticklabels([f"$n$={len(v)}" for k, v in dfg])
        fig.suptitle("")
        return fig, ax


def boxplot_matchcount(df):
    """
    Makes a boxplot for the capacities grouped by the number of matches.
    Attention: Currently only works for the full dataset with original
    names as the last columns.
    """
    # Mend needed data
    df["Matches"] = df.projectID.apply(len)

    # Plot
    fig, ax = plt.subplots(figsize=(8, 4.5))
    df.boxplot(ax=ax, column="Capacity", by="Matches", showmeans=True)
    ax.title.set_visible(False)
    ax.set_xlabel("Number of datasets participating in match [$-$]")
    ax.set_ylabel("Capacity [$MW$]")
    fig.suptitle("")
    return fig


def make_handler_map_to_scale_circles_as_in(ax, dont_resize_actively=False):
    fig = ax.get_figure()

    def axes2pt():
        return np.diff(ax.transData.transform([(0, 0), (1, 1)]), axis=0)[0] * (
            72.0 / fig.dpi
        )

    ellipses = []
    if not dont_resize_actively:

        def update_width_height(event):
            dist = axes2pt()
            for e, radius in ellipses:
                e.width, e.height = 2.0 * radius * dist

        fig.canvas.mpl_connect("resize_event", update_width_height)
        ax.callbacks.connect("xlim_changed", update_width_height)
        ax.callbacks.connect("ylim_changed", update_width_height)

    def legend_circle_handler(
        legend, orig_handle, xdescent, ydescent, width, height, fontsize
    ):
        w, h = 2.0 * orig_handle.get_radius() * axes2pt()
        e = Ellipse(
            xy=(0.5 * width - 0.5 * xdescent, 0.5 * height - 0.5 * ydescent),
            width=w,
            height=w,
        )
        ellipses.append((e, orig_handle.get_radius()))
        return e

    return {Circle: HandlerPatch(patch_func=legend_circle_handler)}


def make_legend_circles_for(sizes, scale=1.0, **kw):
    return [Circle((0, 0), radius=(s / scale) ** 0.5, **kw) for s in sizes]


def draw_basemap(
    resolution=True,
    ax=None,
    country_linewidth=0.3,
    coast_linewidth=0.4,
    zorder=None,
    fillcontinents=True,
    **kwds,
):
    if cartopy_present:
        if ax is None:
            ax = plt.gca(projection=ccrs.PlateCarree())
        resolution = "50m" if isinstance(resolution, bool) else resolution
        assert resolution in [
            "10m",
            "50m",
            "110m",
        ], "Resolution has to be one of '10m', '50m', '110m'."
        ax.set_extent(ax.get_xlim() + ax.get_ylim(), crs=ccrs.PlateCarree())
        ax.coastlines(linewidth=0.4, zorder=-1, resolution=resolution)
        border = cartopy.feature.BORDERS.with_scale(resolution)
        ax.add_feature(border, linewidth=0.3)
        ax.spines["geo"].set_visible(False)
        if fillcontinents:
            land = cartopy.feature.LAND.with_scale(resolution)
            ax.add_feature(land, facecolor="lavender", alpha=0.25)


def gather_nrows_ncols(x, orientation="landscape"):
    """
    Derives [nrows, ncols] based on x plots, so that a subplot looks nicely.

    Parameters
    ----------
    x : int, Number of subplots between [0, 42]
    """

    def calc(n, m):
        if n <= 0:
            n = 1
        if m <= 0:
            m = 1
        while n * m < x:
            m += 1
        return n, m

    if not isinstance(x, int):
        raise ValueError("An integer needs to be passed to this function.")
    elif x <= 0:
        raise ValueError("The given number of subplots is less or equal zero.")
    elif x > 42:
        raise ValueError(
            "Are you sure that you want to put more than 42 "
            "subplots in one diagram?\n You better don't, it "
            "looks squeezed. Otherwise adapt the code."
        )
    k = math.sqrt(x)
    if k.is_integer():
        return [int(k), int(k), 0]  # Square format!
    else:
        k = int(math.floor(k))
        # Solution 1
        n, m = calc(k, k + 1)
        sol1 = {"n": n, "m": m, "dif": (m * n) - x}
        # Solution 2:
        n, m = calc(k - 1, k + 1)
        sol2 = {"n": n, "m": m, "dif": (m * n) - x}
        if ((sol1["dif"] <= sol2["dif"]) & (sol1["n"] >= 2)) | (x in [7, 13, 14]):
            n, m = [sol1["n"], sol1["m"]]
        else:
            n, m = [sol2["n"], sol2["m"]]
        remainder = m * n - x
        if orientation == "landscape":
            return n, m, remainder
        elif orientation == "portrait":
            return m, n, remainder
        else:
            raise ValueError("Wrong `orientation` given!")


def plotly_map(df) -> "plotly.graph_objs._figure.Figure":  # noqa
    """
    Plot a map using plotly. Use fig.update_layout(height=800, width=1200) and fig.show(config={'scrollZoom': True}) to make it interactive

    Parameters
    ----------
    df : DataFrame

    Returns
    -------
    plotly.graph_objs._figure.Figure
    """
    try:
        import plotly  # noqa
        import plotly.express as px
    except ImportError:
        logger.warning("Plotly is not installed. Install it with `pip install plotly`.")
        raise ImportError(
            "Plotly is not installed. Install it with `pip install plotly`."
        )

    import plotly.express as px

    df = get_obj_if_Acc(df)

    fig = px.scatter_mapbox(
        df,
        lat="lat",
        lon="lon",
        hover_name="Capacity",
        hover_data=["Name", "Country", "Technology"],
        color="Fueltype",
        color_continuous_scale=px.colors.sequential.Darkmint,
        zoom=3,  # Initial zoom level,
        opacity=0.75,
    )

    fig.update_layout(mapbox_style="open-street-map")

    return fig

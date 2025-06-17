import logging
import math
import os

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from powerplantmatching.core import get_config, get_obj_if_Acc
from powerplantmatching.data import GEM, OSM
from powerplantmatching.utils import lookup, to_list_if_other

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def fueltype_totals_bar(
    dfs,
    keys=None,
    figsize=(7, 4),
    unit="GW",
    last_as_marker=False,
    axes_style="whitegrid",
    exclude=[],
    log_y=True,  # Added parameter with default True
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

        # Set y-axis to logarithmic scale if log_y is True
        if log_y:
            ax.set_yscale("log")
            # Add minor grid lines which are helpful for reading log scales
            ax.grid(True, which="both", ls="-", alpha=0.2)

        ax.legend(loc=0)
        ax.set_ylabel(
            f"Capacity [${unit}$] (log scale)" if log_y else f"Capacity [${unit}$]"
        )
        ax.xaxis.grid(False)
        fig.tight_layout(pad=0.5)
        return fig, ax


def fueltype_and_country_totals_bar(dfs, keys=None, figsize=(18, 8), log_y=True):
    dfs = get_obj_if_Acc(dfs)
    dfs = to_list_if_other(dfs)
    df = lookup(dfs, keys)
    countries = df.index.get_level_values("Country").unique()
    n = len(countries)

    # Get only the first two elements (nrows, ncols) from gather_nrows_ncols
    nrows, ncols, _ = gather_nrows_ncols(
        n
    )  # Unpacking all three values but only using first two

    # Create subplots with just nrows and ncols
    fig, ax = plt.subplots(nrows, ncols, figsize=figsize)

    if nrows > 1 and ncols > 1:  # Multiple rows and columns
        ax_iter = ax.flat
    else:
        ax_iter = np.array(ax).flat

    for country in countries:
        ax = next(ax_iter)
        df.loc[country].plot.bar(ax=ax, sharex=True, legend=None)
        ax.set_xlabel("")

        # Set y-axis to logarithmic scale if log_y is True
        if log_y:
            ax.set_yscale("log")
            # Add minor grid lines for better readability of log scale
            ax.grid(True, which="both", ls="-", alpha=0.2)
            # For log scale, we don't use scientific notation
        else:
            # Only use scientific notation for linear scale
            ax.ticklabel_format(axis="y", style="sci", scilimits=(-2, 2))

        ax.set_title(country)

    handles, labels = ax.get_legend_handles_labels()
    fig.tight_layout(pad=1)
    fig.legend(handles, labels, loc=9, ncol=2, bbox_to_anchor=(0.53, 0.99))
    fig.subplots_adjust(top=0.9)
    return fig, ax


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


# Main execution
if __name__ == "__main__":
    # Set up output directory
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)

    # List of countries to process
    countries = [
        "Chile",
        "South Africa",
        "Indonesia",
    ]

    # Get the base configuration
    config = get_config()
    config["main_query"] = ""
    config["target_countries"] = countries
    config["OSM"]["force_refresh"] = True  # True to test rejection tracker logic
    config["OSM"]["plants_only"] = True
    config["OSM"]["missing_name_allowed"] = False
    config["OSM"]["missing_technology_allowed"] = False
    config["OSM"]["missing_start_date_allowed"] = True
    config["OSM"]["capacity_estimation"]["enabled"] = True
    config["OSM"]["units_clustering"]["enabled"] = False
    config["OSM"]["units_reconstruction"]["enabled"] = True

    # Set target countries to process all countries at once

    # Get combined data for all countries
    osm_data = OSM(raw=False, update=False, config=config)
    gem_data = GEM(raw=False, update=False, config=config)

    fig, axis = fueltype_totals_bar(
        [gem_data, osm_data], keys=["GEM", "OSM"], log_y=True
    )
    plt.savefig(os.path.join(output_dir, "osm_gem_ppm_log.png"))

    fig, axis = fueltype_totals_bar(
        [gem_data, osm_data], keys=["GEM", "OSM"], log_y=False
    )
    plt.savefig(os.path.join(output_dir, "osm_gem_ppm_linear.png"))

    fig, axis = fueltype_and_country_totals_bar(
        [gem_data, osm_data], keys=["GEM", "OSM"], log_y=False
    )
    plt.savefig(os.path.join(output_dir, "osm_gem_ppm_country.png"))

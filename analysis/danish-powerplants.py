import copy

import hvplot
import hvplot.pandas  # noqa
import hvplot.xarray  # noqa
import panel as pn

import powerplantmatching as pm

config = pm.get_config()

config_da2020 = copy.copy(config)
config_da2020["main_query"] = "Country == 'Denmark'"
config_da2020["hash"] = "DA-2020"
config_da2020["matching_sources"].remove("WIKIPEDIA")
config_da2020["matching_sources"].remove("JRC")


for config in [config_da2020]:
    label = config["hash"]

    df = pm.powerplants(config=config, update=True)
    df = df[df.lat.notnull()]

    grouped = (
        df.groupby(["Fueltype", "Country"])
        .Capacity.sum()
        .div(1e3)
        .sort_index(level=1, ascending=False)
    )
    grouped.name = "Capacity [GW]"

    config = pm.get_config()
    cmap = {
        k: v for k, v in config["fuel_to_color"].items() if k in df.Fueltype.unique()
    }
    checkbox = pn.widgets.CheckButtonGroup(
        name="Select", options=["Fueltype", "Country"]
    )

    map = df.hvplot.points(
        "lon",
        "lat",
        color="Fueltype",
        # groupby=checkbox,
        s="Capacity",
        scale=0.5,
        hover_cols=["Name", "Technology", "Country", "DateOut"],
        legend=False,
        cmap=cmap,
        geo=True,
        alpha=0.4,
        frame_height=500,
        frame_width=500,
        tiles="EsriTerrain",
        xaxis=None,
        yaxis=None,
        title="",
        # features={"rivers": "10m", "lakes": "10m"},
    )

    bars = grouped.hvplot.barh(
        by="Fueltype",
        stacked=True,
        cmap=cmap,
        alpha=0.5,
        title="",
        frame_height=500,
        frame_width=500,
    )
    # bars.opts(opts.Overlay(title=None))

    plot = map + bars

    hvplot.save(plot, "figures/" + label + ".html")

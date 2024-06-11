import copy

import hvplot
import hvplot.pandas  # noqa
import hvplot.xarray  # noqa
import panel as pn

import powerplantmatching as pm

config = pm.get_config()

query = (
    "(DateOut >= {year} or DateOut != DateOut) and "
    "(DateIn <= {year} or DateIn != DateIn) and "
    "Country == 'Germany'"
)

config_de2020 = copy.copy(config)
config_de2020["main_query"] = query.format(year=2019)
config_de2020["hash"] = "DE-2020"

config_de2030 = copy.copy(config)
config_de2030["main_query"] = query.format(year=2030)
config_de2030["matching_sources"].remove("WIKIPEDIA")
config_de2030["hash"] = "DE-2030"


for config in [config_de2020, config_de2030]:
    label = config["hash"]

    df = pm.powerplants(config=config)
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
        features={"rivers": "10m", "lakes": "10m"},
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

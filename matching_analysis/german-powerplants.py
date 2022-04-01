import cartopy.crs as ccrs
import hvplot
import hvplot.pandas  # noqa
import hvplot.xarray  # noqa
import pandas as pd
import panel as pn
import xarray as xr
from holoviews import opts
from holoviews.plotting.util import process_cmap
from xarray import align

import powerplantmatching as pm

matched = pm.powerplants()
matched = matched[matched.lat.notnull()]
matched.DateIn.fillna(1, inplace=True)
matched.DateOut.fillna(9999, inplace=True)
de2020 = matched.query("DateIn <= 2020 and DateOut >= 2020 and Country == 'Germany'")
de2030 = matched.query("DateIn <= 2030 and DateOut >= 2030 and Country == 'Germany'")

for (label, df) in [("DE-2020", de2020), ("DE-2030", de2030)]:

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

    hvplot.save(plot, "figures/" / label + ".html")

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

df = pm.data.BEYONDCOAL()

grouped = (
    df.groupby(["Fueltype", "Country"])
    .Capacity.sum()
    .div(1e3)
    .sort_index(level=1, ascending=False)
)
grouped.name = "Capacity [GW]"

config = pm.get_config()
cmap = {k: v for k, v in config["fuel_to_color"].items() if k in df.Fueltype.unique()}
checkbox = pn.widgets.CheckButtonGroup(name="Select", options=["Fueltype", "Country"])

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

hvplot.show(plot)

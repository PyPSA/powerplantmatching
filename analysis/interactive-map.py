import hvplot
import hvplot.pandas  # noqa
import hvplot.xarray  # noqa

import powerplantmatching as pm

df = pm.data.ENTSOE()
df = df[df.lat.notnull()]

grouped = (
    df.groupby(["Fueltype", "Country"])
    .Capacity.sum()
    .div(1e3)
    .sort_index(level=1, ascending=False)
)
grouped.name = "Capacity [GW]"

config = pm.get_config()
cmap = {k: v for k, v in config["fuel_to_color"].items() if k in df.Fueltype.unique()}
# checkbox = pn.widgets.CheckButtonGroup(name="Select", options=["Fueltype", "Country"])

map = df.hvplot.points(
    "lon",
    "lat",
    color="Fueltype",
    # groupby=checkbox,
    s="Capacity",
    scale=0.25,
    hover_cols=["Name", "Technology", "Country", "DateOut"],
    legend=True,
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

hvplot.save(map, "figures/powerplant-map.html")

bars = grouped.hvplot.barh(
    by="Fueltype",
    stacked=True,
    cmap=cmap,
    alpha=0.5,
    title="",
    frame_height=500,
    frame_width=500,
)

hvplot.save(bars, "figures/powerplant-bars.html")

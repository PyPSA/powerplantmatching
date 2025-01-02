import hvplot
import hvplot.pandas  # noqa
import hvplot.xarray  # noqa
import panel as pn

import powerplantmatching as pm

df = pm.powerplants()
df = df[df.lat.notnull()]

config = pm.get_config()
cmap = {k: v for k, v in config["fuel_to_color"].items() if k in df.Fueltype.unique()}
checkbox = pn.widgets.CheckButtonGroup(name="Select", options=["Fueltype", "Country"])

map = df.hvplot.points(
    "lon",
    "lat",
    color="Fueltype",
    groupby=checkbox,
    s="Capacity",
    scale=0.5,
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
    features={"rivers": "10m", "lakes": "10m"},
)


panel = pn.Column(checkbox, map)
panel.show()

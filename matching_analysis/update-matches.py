# Run this from the root directory

import powerplantmatching as pm

df = pm.powerplants(update=False)
df = df[df.lat.notnull()].reset_index(drop=True)
df = df.powerplant.fill_geopositions()
df = df.drop_duplicates(["Name", "Fueltype", "Country"])
df.to_csv("powerplants.csv", index_label="id")

# Run this from this directory:

import powerplantmatching as pm

df = pm.powerplants(update=False)
df = df[df.lat.notnull()].reset_index(drop=True)
df.to_csv("../powerplants.csv", index_label="id")

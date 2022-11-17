# Run this from the root directory

import powerplantmatching as pm

if __name__ == "__main__":
    df = pm.powerplants(update=True)
    df.to_csv("powerplants.csv", index_label="id")

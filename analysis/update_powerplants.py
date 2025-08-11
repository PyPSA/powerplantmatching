# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import pathlib

import powerplantmatching as pm

if __name__ == "__main__":
    root = pathlib.Path(__file__).parent.parent.absolute()
    df = pm.powerplants(update=True)
    df.to_csv(root / "powerplants.csv", index_label="id")

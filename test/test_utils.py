# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import pandas as pd

from powerplantmatching.core import get_config
from powerplantmatching.utils import to_categorical_columns


def test_to_categorical_columns_keeps_values():
    df = pd.DataFrame(
        {
            "Fueltype": ["Hydro", "Wind"],
            "Country": ["Germany", "France"],
            "Set": ["PP", "Storage"],
            "File": ["a.csv", "b.csv"],
        }
    )
    res = to_categorical_columns(df)

    config = get_config()
    for col, key in [
        ("Fueltype", "target_fueltypes"),
        ("Country", "target_countries"),
        ("Set", "target_sets"),
    ]:
        assert isinstance(res[col].dtype, pd.CategoricalDtype)
        assert list(res[col].cat.categories) == list(config[key])
        assert res[col].tolist() == df[col].tolist()
    assert res["File"].tolist() == df["File"].tolist()

# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import pytest

from powerplantmatching.core import get_config
from powerplantmatching.matching import reduce_matched_dataframe


@pytest.mark.parametrize(
    "status, expected",
    [
        (["operating", "retired"], "retired"),
        (["retired", "operating"], "retired"),
        (["announced", "operating"], "operating"),
        (["mothballed", np.nan], "mothballed"),
        ([np.nan, np.nan], np.nan),
    ],
)
def test_reduce_status_by_life_cycle(status, expected):
    config = get_config()
    sources = ["GCPT", "OPSD"]
    columns = pd.MultiIndex.from_product([config["target_columns"], sources])
    df = pd.DataFrame(np.nan, index=[0], columns=columns, dtype=object)
    df["Name"] = "Kraftwerk Musterstadt"
    df.loc[0, "Status"] = status
    res = reduce_matched_dataframe(df, config=config)
    expected = pd.Series([expected], name="Status")
    pd.testing.assert_series_equal(res.Status, expected, check_dtype=False)

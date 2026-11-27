# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from powerplantmatching import collection
from powerplantmatching.changes import (
    MISSING,
    SECTIONS,
    _read,
    compare,
    figures,
    render_release,
    write_release,
)

NAN = np.nan
COLUMNS = ["Country", "Fueltype", "Name", "Capacity", "DateIn", "Technology"]

# France and Nuclear only in old, Spain only in new. F in Germany is incomplete
# (no Technology), Spain has zero capacity and no DateIn. Italy is unchanged.
# Entries without country change. The technology ST of France is not in the config.
OLD = pd.DataFrame(
    [
        ["Germany", "Wind", "I", 1000.0, 2000.0, "Onshore"],
        ["Germany", "Solar", "B", 500.0, 2010.0, "PV"],
        ["France", "Nuclear", "C", 2000.0, 1980.0, "ST"],
        ["Italy", "Hydro", "D", 300.0, 1960.0, "Reservoir"],
        [None, "Wind", "E", 100.0, 1990.0, "Onshore"],
    ],
    columns=COLUMNS,
).assign(
    Set="PP",
    lat=50.0,
    lon=10.0,
    DateOut=NAN,
    projectID=[
        {"A": {"1"}},
        {"A": {"2"}},
        {"B": {"3"}},
        {"A": {"4"}, "B": {"5"}},
        {"A": {"6"}},
    ],
)
NEW = pd.DataFrame(
    [
        ["Germany", "Wind", "F", 700.0, 2000.0, None],
        ["Germany", "Wind", "I", 1000.0, 2000.0, "Onshore"],
        ["Germany", "Solar", "H", 300.0, 2020.0, "PV"],
        ["Germany", "Solar", "G", 300.0, 2020.0, "PV"],
        ["Spain", "Solar", "S", 0.0, NAN, "PV"],
        ["Italy", "Hydro", "D", 300.0, 1960.0, "Reservoir"],
        [None, "Wind", "E", 200.0, 1990.0, "Onshore"],
    ],
    columns=COLUMNS,
).assign(
    Set="PP",
    lat=50.0,
    lon=10.0,
    DateOut=NAN,
    projectID=[{"A": {"1"}}] * 2
    + [{"B": {"2"}}] * 2
    + [{"B": {"3"}}]
    + [{"A": {"4"}, "B": {"5"}}, {"A": {"6"}}],
)

# Map input: one entry without coordinates (not shown) and one whose name would


@pytest.fixture(scope="module")
def table() -> pd.DataFrame:
    return compare(OLD, NEW, year=2020)


@pytest.mark.parametrize(
    "key, values",
    [
        (("Country", "Germany", "Wind"), [1000, 1700, 1, 2, 1, 1]),
        (("Country", MISSING, "Wind"), [100, 200, 1, 1, 1, 1]),
        (("Country", "France", "Nuclear"), [2000, 0, 1, 0, 1, 0]),
        (("Source", "B", "Solar"), [0, 600, 0, 3, 0, 2]),
        (("Status", "undated", "Solar"), [0, 0, 0, 1, 0, 0]),
        (("Technology", MISSING, "Wind"), [0, 700, 0, 1, 0, 0]),
        (("Check", "Category value not in config", "Nuclear"), [2000, 0, 1, 0, 1, 0]),
        (("Check", "Capacity missing or not positive", "Solar"), [0, 0, 0, 1, 0, 0]),
    ],
)
def test_compare(table: pd.DataFrame, key: tuple, values: list[float]) -> None:
    assert table.loc[key].tolist() == values


@pytest.mark.parametrize(
    "date_in, date_out, status",
    [
        (NAN, 2000.0, "undated"),
        (2021.0, NAN, "planned"),
        (2000.0, 2019.0, "retired"),
        (2000.0, 2020.0, "operating"),
        (2020.0, NAN, "operating"),
    ],
)
def test_compare_status(date_in: float, date_out: float, status: str) -> None:
    df = NEW.iloc[:1].assign(DateIn=date_in, DateOut=date_out)
    statuses = compare(df, df, year=2020).loc["Status"].index.unique("Group")
    assert statuses.tolist() == [status]


def test_compare_requires_source_dicts() -> None:
    with pytest.raises(TypeError):
        compare(OLD, NEW.assign(projectID="A"))


def test_figures(table: pd.DataFrame) -> None:
    figs = figures(table)
    assert list(figs) == ["country_fueltype", *SECTIONS]
    assert all(fig.layout.height > 0 for fig in figs.values())


@pytest.fixture
def release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    datasets = {"v0.1.0": OLD, "v0.2.0": NEW}
    monkeypatch.setattr(collection, "powerplants", lambda from_url: datasets[from_url])
    return write_release("v0.1.0", "v0.2.0", folder=tmp_path, year=2020)


def test_write_release(release: Path, table: pd.DataFrame) -> None:
    assert sorted(p.name for p in release.iterdir()) == ["changes.csv", "release.json"]
    pd.testing.assert_frame_equal(_read(release / "changes.csv"), table)


def test_render_release(release: Path) -> None:
    files = render_release(release)
    page = files["index.md"]
    assert page.startswith("# Data changes from v0.1.0 to v0.2.0\n")
    assert "## By status in 2020\n" in page
    assert page.count("## ") == len(SECTIONS) + 2
    row = "| Germany | 1.500 | 2.300 | +0.800 (+53.3%) | 2 | 4 | +2 (+100.0%) | 100.0 | 75.0 | -25.0 |"
    assert row in page
    assert page.count("cdn.plot.ly") == 1
    assert collection.matched_data_url("v0.1.0") in files["map.html"]
    assert "__PPM_" not in files["map.html"]

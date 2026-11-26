# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import json
import re
import textwrap
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from powerplantmatching.changes import (
    CLASS_COLORS,
    INTROS,
    TABS,
    Comparison,
    MapSource,
    _markdown,
    _number,
    compare,
    figures,
    read_release,
    read_tables,
    render,
    render_release,
    write_map,
    write_release_report,
    write_report,
    write_tables,
)
from powerplantmatching.collection import matched_data_url
from powerplantmatching.core import _package_data, get_config

NAN = np.nan
COLUMNS = ["Country", "Fueltype", "Name", "Capacity", "DateIn", "Technology"]

# France and Nuclear only in old, Spain only in new (no change in %). Germany changes: F is
# incomplete (no Technology), F and I tie on DateIn (capacity decides, not the
# name), H and G tie on DateIn and capacity (name decides). Spain has zero
# capacity and no DateIn. Italy is unchanged. Entries without country (NaN)
# change. The technology ST of France is not in the config.
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
# close the script block of the map if it were not escaped
MAP_NEW = pd.concat(
    [
        NEW,
        pd.DataFrame(
            {
                "Name": ["no coordinates", "</script><script>alert(1)</script>"],
                "Fueltype": "Solar",
                "Capacity": [1.0, NAN],
                "lat": [NAN, 51.0],
                "lon": [NAN, 11.0],
            }
        ),
    ],
    ignore_index=True,
)
URL_OLD = matched_data_url("v0.9.0")
URL_NEW = matched_data_url("v0.9.1")

COUNTRIES = pd.Index(["France", "Germany", "Italy", "Spain", None], name="Country")
FUELTYPES = pd.Index(["Hydro", "Nuclear", "Solar", "Wind"], name="Fueltype")


def oldest_newest(rows: list[list], index: pd.Index) -> pd.DataFrame:
    columns = ["Oldest", "Oldest DateIn", "Newest", "Newest DateIn"]
    table = pd.DataFrame(rows, columns=columns, index=index)
    return table.astype({"Oldest DateIn": "Int64", "Newest DateIn": "Int64"})


@pytest.fixture
def comparison() -> Comparison:
    return compare(OLD, NEW)


def test_compare_total(comparison: Comparison) -> None:
    expected = pd.DataFrame(
        {"old": [3.9, 5], "new": [2.8, 7], "delta": [-1.1, 2]},
        index=pd.Index(["Capacity [GW]", "Entries"], name="Quantity"),
        dtype=object,
    ).assign(**{"delta [%]": [-28.2, 40.0]})
    pd.testing.assert_frame_equal(comparison.overview["total"], expected)


@pytest.mark.parametrize("empty", ["old", "new"])
def test_compare_total_relative_to_zero(empty: str) -> None:
    versions = {"old": OLD, "new": NEW} | {empty: OLD.iloc[:0]}
    if empty == "old":
        with pytest.raises(ValueError, match="old value of 0"):
            compare(**versions)
    else:
        delta = compare(**versions).overview["total"]["delta [%]"]
        assert delta.tolist() == [-100.0, -100.0]


def test_compare_tabs(comparison: Comparison) -> None:
    tabs = ["capacity", "entries", "completeness", "oldest_newest"]
    assert list(comparison.by_country) == [*tabs, "capacity_share", "entry_share"]
    assert list(comparison.by_fueltype) == tabs
    assert list(comparison.overview) == [
        "total",
        "country_fueltype",
        "sources",
        "status",
    ]
    assert list(comparison.checks) == ["counts", "categories", "not_in_config"]
    assert set(TABS.values()) == set(comparison.by_country)


@pytest.mark.parametrize(
    "grouping, tab, expected",
    [
        (
            "by_country",
            "capacity",
            pd.DataFrame(
                {
                    "Capacity old [GW]": [2.0, 1.5, 0.3, 0.0, 0.1],
                    "Capacity new [GW]": [0.0, 2.3, 0.3, 0.0, 0.2],
                    "Capacity delta [GW]": [-2.0, 0.8, 0.0, 0.0, 0.1],
                    "Capacity delta [%]": [-100.0, 53.3, 0.0, NAN, 100.0],
                },
                index=COUNTRIES,
            ),
        ),
        (
            "by_country",
            "entries",
            pd.DataFrame(
                {
                    "Entries old": [1, 2, 1, 0, 1],
                    "Entries new": [0, 4, 1, 1, 1],
                    "Entries delta": [-1, 2, 0, 1, 0],
                    "Entries delta [%]": [-100.0, 100.0, 0.0, NAN, 0.0],
                },
                index=COUNTRIES,
            ),
        ),
        (
            "by_country",
            "completeness",
            pd.DataFrame(
                {
                    "Complete old [%]": [100.0, 100.0, 100.0, NAN, 100.0],
                    "Complete new [%]": [NAN, 75.0, 100.0, 0.0, 100.0],
                    "Complete delta [pp]": [NAN, -25.0, 0.0, NAN, 0.0],
                },
                index=COUNTRIES,
            ),
        ),
        (
            "by_country",
            "oldest_newest",
            oldest_newest(
                [
                    ["I", 2000, "G", 2020],
                    ["D", 1960, "D", 1960],
                    ["E", 1990, "E", 1990],
                ],
                pd.Index(["Germany", "Italy", None], name="Country"),
            ),
        ),
        (
            "by_country",
            "capacity_share",
            pd.DataFrame(
                {
                    "Wind old [%]": [0.0, 66.7, 0.0, NAN, 100.0],
                    "Wind [%]": [NAN, 73.9, 0.0, NAN, 100.0],
                    "Wind delta [pp]": [NAN, 7.2, 0.0, NAN, 0.0],
                    "Solar old [%]": [0.0, 33.3, 0.0, NAN, 0.0],
                    "Solar [%]": [NAN, 26.1, 0.0, NAN, 0.0],
                    "Solar delta [pp]": [NAN, -7.2, 0.0, NAN, 0.0],
                    "Hydro old [%]": [0.0, 0.0, 100.0, NAN, 0.0],
                    "Hydro [%]": [NAN, 0.0, 100.0, NAN, 0.0],
                    "Hydro delta [pp]": [NAN, 0.0, 0.0, NAN, 0.0],
                    "Nuclear old [%]": [100.0, 0.0, 0.0, NAN, 0.0],
                    "Nuclear [%]": [NAN, 0.0, 0.0, NAN, 0.0],
                    "Nuclear delta [pp]": [NAN, 0.0, 0.0, NAN, 0.0],
                },
                index=COUNTRIES,
            ),
        ),
        (
            "by_country",
            "entry_share",
            pd.DataFrame(
                {
                    "Solar old [%]": [0.0, 50.0, 0.0, NAN, 0.0],
                    "Solar [%]": [NAN, 50.0, 0.0, 100.0, 0.0],
                    "Solar delta [pp]": [NAN, 0.0, 0.0, NAN, 0.0],
                    "Wind old [%]": [0.0, 50.0, 0.0, NAN, 100.0],
                    "Wind [%]": [NAN, 50.0, 0.0, 0.0, 100.0],
                    "Wind delta [pp]": [NAN, 0.0, 0.0, NAN, 0.0],
                    "Hydro old [%]": [0.0, 0.0, 100.0, NAN, 0.0],
                    "Hydro [%]": [NAN, 0.0, 100.0, 0.0, 0.0],
                    "Hydro delta [pp]": [NAN, 0.0, 0.0, NAN, 0.0],
                    "Nuclear old [%]": [100.0, 0.0, 0.0, NAN, 0.0],
                    "Nuclear [%]": [NAN, 0.0, 0.0, 0.0, 0.0],
                    "Nuclear delta [pp]": [NAN, 0.0, 0.0, NAN, 0.0],
                },
                index=COUNTRIES,
            ),
        ),
        (
            "by_fueltype",
            "capacity",
            pd.DataFrame(
                {
                    "Capacity old [GW]": [0.3, 2.0, 0.5, 1.1],
                    "Capacity new [GW]": [0.3, 0.0, 0.6, 1.9],
                    "Capacity delta [GW]": [0.0, -2.0, 0.1, 0.8],
                    "Capacity delta [%]": [0.0, -100.0, 20.0, 72.7],
                },
                index=FUELTYPES,
            ),
        ),
        (
            "by_fueltype",
            "entries",
            pd.DataFrame(
                {
                    "Entries old": [1, 1, 1, 2],
                    "Entries new": [1, 0, 3, 3],
                    "Entries delta": [0, -1, 2, 1],
                    "Entries delta [%]": [0.0, -100.0, 200.0, 50.0],
                },
                index=FUELTYPES,
            ),
        ),
        (
            "by_fueltype",
            "completeness",
            pd.DataFrame(
                {
                    "Complete old [%]": [100.0, 100.0, 100.0, 100.0],
                    "Complete new [%]": [100.0, NAN, 66.7, 66.7],
                    "Complete delta [pp]": [0.0, NAN, -33.3, -33.3],
                },
                index=FUELTYPES,
            ),
        ),
        (
            "by_fueltype",
            "oldest_newest",
            oldest_newest(
                [
                    ["D", 1960, "D", 1960],
                    ["G", 2020, "G", 2020],
                    ["E", 1990, "I", 2000],
                ],
                pd.Index(["Hydro", "Solar", "Wind"], name="Fueltype"),
            ),
        ),
    ],
)
def test_compare(
    comparison: Comparison, grouping: str, tab: str, expected: pd.DataFrame
) -> None:
    tables = {
        "by_country": comparison.by_country,
        "by_fueltype": comparison.by_fueltype,
    }
    pd.testing.assert_frame_equal(tables[grouping][tab], expected)


@pytest.mark.parametrize("stem", ["capacity_share", "entry_share"])
# Old shares are "<Fueltype> old [%]", new shares "<Fueltype> [%]"
@pytest.mark.parametrize(
    "columns", [r" old \[%\]$", r"^\w+ \[%\]$"], ids=["old", "new"]
)
def test_compare_shares_sum_to_100(
    comparison: Comparison, stem: str, columns: str
) -> None:
    shares = comparison.by_country[stem].filter(regex=columns).dropna()
    assert not shares.empty
    assert shares.sum(axis=1).tolist() == pytest.approx([100.0] * len(shares), abs=0.1)


COUNTRY_FUELTYPE = pd.MultiIndex.from_tuples(
    [
        ("France", "Nuclear"),
        ("Germany", "Solar"),
        ("Germany", "Wind"),
        ("Italy", "Hydro"),
        ("Spain", "Solar"),
        (NAN, "Wind"),
    ],
    names=["Country", "Fueltype"],
)


def test_compare_country_fueltype(comparison: Comparison) -> None:
    expected = pd.DataFrame(
        {
            "Capacity old [GW]": [2.0, 0.5, 1.0, 0.3, 0.0, 0.1],
            "Capacity new [GW]": [0.0, 0.6, 1.7, 0.3, 0.0, 0.2],
            "Capacity delta [GW]": [-2.0, 0.1, 0.7, 0.0, 0.0, 0.1],
            "Capacity delta [%]": [-100.0, 20.0, 70.0, 0.0, NAN, 100.0],
            "Entries old": [1, 1, 1, 1, 0, 1],
            "Entries new": [0, 2, 2, 1, 1, 1],
            "Entries delta": [-1, 1, 1, 0, 1, 0],
            "Entries delta [%]": [-100.0, 100.0, 100.0, 0.0, NAN, 0.0],
        },
        index=COUNTRY_FUELTYPE,
    )
    pd.testing.assert_frame_equal(comparison.overview["country_fueltype"], expected)


def test_compare_sources(comparison: Comparison) -> None:
    # D belongs to both sources, so the sources add up to more than the total
    expected = pd.DataFrame(
        {
            "Capacity old [GW]": [1.9, 2.3],
            "Capacity new [GW]": [2.2, 0.9],
            "Capacity delta [GW]": [0.3, -1.4],
            "Capacity delta [%]": [15.8, -60.9],
            "Entries old": [4, 2],
            "Entries new": [4, 4],
            "Entries delta": [0, 2],
            "Entries delta [%]": [0.0, 100.0],
        },
        index=pd.Index(["A", "B"], name="Source"),
    )
    pd.testing.assert_frame_equal(comparison.overview["sources"], expected)


@pytest.mark.parametrize(
    "broken",
    [OLD.drop(columns="projectID"), OLD.assign(projectID="A: 1")],
    ids=["missing", "string"],
)
def test_compare_sources_require_dicts(broken: pd.DataFrame) -> None:
    with pytest.raises(ValueError, match="projectID"):
        compare(OLD, broken)


def entry(**columns: object) -> pd.DataFrame:
    """One entry of 1 GW with all columns of the report, overridden by `columns`."""
    values = dict(
        Country="Germany",
        Fueltype="Wind",
        Name="A",
        Capacity=1000.0,
        DateIn=2000.0,
        DateOut=NAN,
        Technology="Onshore",
        Set="PP",
        lat=50.0,
        lon=10.0,
        projectID={"X": {"1"}},
    )
    return pd.DataFrame([values | columns])


@pytest.mark.parametrize(
    "date_in, date_out, status",
    [
        (2000, NAN, "operating"),
        (2020, NAN, "operating"),
        (2000, 2020, "operating"),
        (2021, NAN, "planned"),
        (2000, 2019, "retired"),
        (NAN, NAN, "undated"),
        (NAN, 2019, "undated"),
    ],
)
def test_compare_status_rules(date_in: float, date_out: float, status: str) -> None:
    new = entry(DateIn=date_in, DateOut=date_out)
    table = compare(entry(), new, year=2020).overview["status"]
    assert table["Capacity new [GW]"].to_dict() == {
        s: float(s == status) for s in table.index
    }


def test_compare_status() -> None:
    old = pd.concat([entry(), entry(DateOut=2019.0, Capacity=500.0)])
    new = pd.concat(
        [
            entry(DateOut=2020.0),
            entry(DateIn=2021.0, Capacity=2000.0),
            entry(DateOut=2019.0),
            entry(DateIn=NAN, DateOut=2019.0, Capacity=300.0),
        ]
    )
    expected = pd.DataFrame(
        {
            "Capacity old [GW]": [1.0, 0.0, 0.5, 0.0],
            "Capacity new [GW]": [1.0, 2.0, 1.0, 0.3],
            "Capacity delta [GW]": [0.0, 2.0, 0.5, 0.3],
            "Capacity delta [%]": [0.0, NAN, 100.0, NAN],
        },
        index=pd.Index(["operating", "planned", "retired", "undated"], name="Status"),
    )
    pd.testing.assert_frame_equal(
        compare(old, new, year=2020).overview["status"], expected
    )


def test_compare_checks() -> None:
    old = pd.concat([entry(), entry(Name="Z", Fueltype="Coal", Technology="ST")])
    # One entry per check, and a repeat of the first entry
    new = pd.concat(
        [
            entry(),
            entry(),
            entry(Name="B", Capacity=-1000.0, Set="Storage"),
            entry(Name="C", lat=NAN),
            entry(Name="D", Country=None),
            entry(Name="E", DateOut=1999.0),
        ]
    )
    checks = compare(old, new).checks
    # Coal and ST of the old version are not in the config
    expected = pd.DataFrame(
        {"old": [0] * 5 + [1], "new": [1] * 5 + [0], "delta": [1] * 5 + [-1]},
        index=pd.Index(
            [
                "Capacity missing or not positive",
                "No coordinates",
                "No country",
                "DateOut before DateIn",
                "Repeated entries",
                "Category value not in config",
            ],
            name="Check",
        ),
    )
    pd.testing.assert_frame_equal(checks["counts"], expected)
    categories = pd.DataFrame(
        {
            "Only in": ["old", "old", "new", "old"],
            "Entries": [1, 1, 1, 1],
            "Capacity [GW]": [1.0, 1.0, -1.0, 1.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("Fueltype", "Coal"),
                ("Fueltype / Technology", "Coal / ST"),
                ("Set", "Storage"),
                ("Technology", "ST"),
            ],
            names=["Category", "Value"],
        ),
    )
    pd.testing.assert_frame_equal(checks["categories"], categories)


def test_compare_checks_not_in_config() -> None:
    old = pd.concat([entry(), entry(Name="Z", Fueltype="Coal", Technology="ST")])
    new = pd.concat(
        [
            entry(Name="F", Fueltype="Coal", Capacity=500.0),
            # Two values not in the config count as one entry
            entry(Name="T", Technology="ST", Set="Store"),
            entry(Name="S", Set="Storage"),
            entry(Name="N", Fueltype=None, Technology=None, Set=None),
        ]
    )
    config = {
        "target_fueltypes": {"Wind": ""},
        "target_technologies": {"Onshore": ""},
        "target_sets": {"PP": "", "Storage": ""},
    }
    checks = compare(old, new, config=config).checks
    assert checks["counts"].loc["Category value not in config"].to_dict() == {
        "old": 1,
        "new": 2,
        "delta": 1,
    }
    expected = pd.DataFrame(
        {
            "Entries old": [1, 0, 1],
            "Entries new": [1, 1, 1],
            "Entries delta": [0, 1, 0],
            "Capacity old [GW]": [1.0, 0.0, 1.0],
            "Capacity new [GW]": [0.5, 1.0, 1.0],
            "Capacity delta [GW]": [-0.5, 1.0, 0.0],
        },
        index=pd.MultiIndex.from_tuples(
            [("Fueltype", "Coal"), ("Set", "Store"), ("Technology", "ST")],
            names=["Category", "Value"],
        ),
    )
    pd.testing.assert_frame_equal(checks["not_in_config"], expected)


def test_compare_delta_before_rounding() -> None:
    def version(capacity: float, technologies: list[str | None]) -> pd.DataFrame:
        return pd.DataFrame(
            {"Name": ["A", "B", "C"], "Capacity": [capacity, 0.0, 0.0]}
        ).assign(
            Country="Germany",
            Fueltype="Wind",
            Technology=technologies,
            DateIn=2000.0,
            DateOut=NAN,
            Set="PP",
            lat=50.0,
            lon=10.0,
            projectID=[{"X": {"1"}}] * 3,
        )

    comparison = compare(
        version(1000.6, [None, "Onshore", "Onshore"]),
        version(1000.4, [None, None, "Onshore"]),
    )
    # Rounding before the delta would give -0.001 GW, -0.1 % and -33.4 pp. As
    # strings, since -0.0 == 0.0.
    capacity = comparison.overview["total"].loc["Capacity [GW]"].map(str).tolist()
    assert capacity == ["1.001", "1.0", "0.0", "0.0"]
    completeness = comparison.by_country["completeness"].loc["Germany"]
    assert completeness.tolist() == [66.7, 33.3, -33.3]


def by_label(table: pd.DataFrame) -> pd.DataFrame:
    """The table with the group labels of the figures, "n/a" for no group."""
    return table.set_axis(table.index.fillna("n/a"))


def test_figures_keys(comparison: Comparison, tmp_path: Path) -> None:
    write_report(comparison, tmp_path, title="Test report")
    figure_files = {
        p.relative_to(tmp_path).with_suffix("").as_posix()
        for p in tmp_path.glob("*/*.html")
    }
    assert set(figures(comparison)) == figure_files
    # Every table has a figure, except oldest_newest and the checks
    tables = {
        p.relative_to(tmp_path).with_suffix("").as_posix()
        for p in tmp_path.glob("*/*.csv")
    }
    without = {key for key in tables if re.match(r"checks/|.*/oldest_newest$", key)}
    assert figure_files == tables - without


@pytest.mark.parametrize("grouping", ["by_country", "by_fueltype"])
@pytest.mark.parametrize("stem", ["capacity", "entries", "completeness"])
def test_change_figure_data(comparison: Comparison, grouping: str, stem: str) -> None:
    table = by_label(comparison.sections()[grouping][stem])
    old, new, delta = table.columns[:3]
    fig = figures(comparison)[f"{grouping}/{stem}"]
    bars = {bar.name: bar for bar in fig.data}
    for name, column in [("old", old), ("new", new), (delta, delta)]:
        plotted = pd.Series(bars[name].x, index=list(bars[name].y), name=column)
        # Table values, NaN (a group missing in a version) as a gap
        pd.testing.assert_series_equal(plotted, table[column].loc[plotted.index])
    # Groups in the alphabetical order of the table, missing label last
    assert list(bars[delta].y) == list(table.index)
    # d3 sign flag: every delta tick has its sign, also +0
    axis = fig.layout[bars[delta].xaxis.replace("x", "xaxis")]
    assert axis.tickformat.startswith("+")


@pytest.mark.parametrize(
    ("key", "delta", "group", "line"),
    [
        ("by_country/capacity", "Capacity delta [GW]", "Italy", "+0 (+0%)"),
        # No change in % from zero
        ("by_country/capacity", "Capacity delta [GW]", "Spain", "+0"),
        # Second quantity of a two-quantity table
        ("overview/sources", "Entries delta", "A", "+0 (+0%)"),
    ],
)
def test_change_figure_hover(
    comparison: Comparison, key: str, delta: str, group: str, line: str
) -> None:
    bars = {bar.name: bar for bar in figures(comparison)[key].data}
    hover = dict(zip(bars[delta].y, bars[delta].hovertext))
    # The delta is the last line of the hover text
    assert hover[group].split("<br>")[-1] == f"{delta}: {line}"


@pytest.mark.parametrize("stem", ["capacity_share", "entry_share"])
def test_share_figure_data(comparison: Comparison, stem: str) -> None:
    table = by_label(comparison.by_country[stem])
    bars = [
        t for t in figures(comparison)[f"by_country/{stem}"].data if t.type == "bar"
    ]
    columns = {"old": "{} old [%]", "new": "{} [%]"}
    plotted = {
        columns[bar.offsetgroup].format(bar.name): pd.Series(bar.x, index=list(bar.y))
        for bar in bars
    }
    # Groups in the alphabetical order of the table
    assert all(list(bar.y) == list(table.index) for bar in bars)
    shares = table.filter(regex=r"\[%\]$")
    assert set(plotted) == set(shares.columns)
    for column, values in plotted.items():
        expected = shares[column].loc[values.index]
        pd.testing.assert_series_equal(values, expected, check_names=False)


def test_total_figure_data(comparison: Comparison) -> None:
    capacity, label, entries = figures(comparison)["overview/total"].data
    assert [list(capacity.x), list(entries.x)] == [[3.9, 2.8, -1.1], [5, 7, 2]]
    assert [capacity.text[-1], entries.text[-1]] == ["-1.1 (-28.2%)", "+2 (+40%)"]
    # The label of a negative delta stands right of zero, not left of its bar
    assert capacity.textposition[-1] == "none"
    assert (label.x, label.text, label.textposition) == (
        (0,),
        ("-1.1 (-28.2%)",),
        "middle right",
    )
    assert entries.textposition[-1] == "auto"


def test_matrix_figure_data(comparison: Comparison) -> None:
    capacity, entries = figures(comparison)["overview/country_fueltype"].data
    # Countries and fuel types alphabetical, missing label last
    assert capacity.y == ("France", "Germany", "Italy", "Spain", "n/a")
    assert list(capacity.x) == sorted(capacity.x)
    z = pd.DataFrame(capacity.z, index=capacity.y, columns=capacity.x)
    # Signed decade class of the delta (peak 2 GW: bounds 0.001, 0.01, 0.1 GW),
    # blank where neither version has entries
    assert z.loc["France", "Nuclear"] == -4
    assert z.loc["Germany", "Wind"] == 4
    assert z.loc["Spain", "Solar"] == 0
    assert np.isnan(z.loc["France", "Wind"])
    hover = pd.DataFrame(entries.text, index=entries.y, columns=entries.x)
    assert hover.loc["Spain", "Solar"].endswith("Entries delta: +1")
    assert capacity.colorbar.ticktext == (
        "-0.1",
        "-0.01",
        "-0.001",
        "+0",
        "+0.001",
        "+0.01",
        "+0.1",
    )


def read_map_data(html: str, block: str) -> object:
    """Parse a JSON block of a map page."""
    match = re.search(
        f'<script type="application/json" id="{block}">(.*?)</script>', html
    )
    return json.loads(match.group(1))


# Descriptions below the tables of a report: total, matrix, sources, status, one per grouping
# section and three for the checks
NOTES_BLOCKS = 9


def tab_bodies(index: str) -> dict[str, list[str]]:
    """Bodies of the content tabs of each label, unindented, in page order."""
    bodies: dict[str, list[str]] = {}
    for label, body in re.findall(r'^=== "(.+)"\n\n((?:    .*\n|\n)+)', index, re.M):
        bodies.setdefault(label, []).append(textwrap.dedent(body).strip())
    return bodies


@pytest.mark.parametrize("map_new", [None, MapSource(NEW, "new")])
def test_write_report_index(
    comparison: Comparison, tmp_path: Path, map_new: MapSource | None
) -> None:
    path = write_report(
        comparison, tmp_path, title="Test report", map_new=map_new, html=False
    )
    assert path == tmp_path / "index.md"
    index = path.read_text()
    assert index.startswith("# Test report\n")
    sections = ["Overview", "Map", "By country", "By fuel type", "Checks"]
    if map_new is None:
        sections.remove("Map")
    else:
        assert '<iframe src="map.html"' in index
    assert re.findall(r"^## (.+)$", index, re.M) == sections
    # Each section starts with its static description
    expected = {
        "By country": INTROS["group"].format(group="country"),
        "By fuel type": INTROS["group"].format(group="fuel type"),
    }
    assert re.findall(r"^## (.+)\n\n(.+)$", index, re.M) == [
        (name, expected.get(name, INTROS.get(name))) for name in sections
    ]
    assert re.findall(r"^### (.+)$", index, re.M) == [
        "By country and fuel type",
        "Sources",
        f"Status in {comparison.year}",
    ]
    assert "from 3.9 GW to 2.8 GW (-1.1 GW)" in index
    assert "from 5 to 7 (+2)" in index
    assert "| Capacity [GW] | 3.9 | 2.8 | -1.1 (-28.2%) |\n" in index
    assert "| Entries | 5 | 7 | +2 (+40%) |\n" in index
    # Per tab one figure with the height of the figure, then the table,
    # collapsed. A table without figure is shown as it is.
    bodies = tab_bodies(index)
    heights = {key: fig.layout.height for key, fig in figures(comparison).items()}
    for label, stem in TABS.items():
        expected = (stem in comparison.by_country) + (stem in comparison.by_fueltype)
        assert len(bodies[label]) == expected
        for body in bodies[label]:
            if stem == "oldest_newest":
                assert body.startswith("| ")
                continue
            src, height = re.match(
                r'<div style="position: relative; margin-bottom: 1em">\n'
                r"<p .*?>Loading figure…</p>\n"
                r'<iframe src="(.+?)".*?height: (\d+)px.*?</iframe>\n</div>\n\n'
                r'\?\?\? note "Table"\n\n    \| ',
                body,
                re.S,
            ).groups()
            assert body.count("<iframe") == 1
            assert (tmp_path / src).is_file()
            assert int(height) == heights[src.removesuffix(".html")]
    # Tables of the groupings are inside content tabs, whose bodies are indented
    groupings = index[index.index("\n## By country") : index.index("\n## Checks")]
    assert not any(line.startswith("|") for line in groupings.splitlines())
    # Ordered by capacity change, with the net change of entries
    assert (
        "The countries with the largest capacity change are France (-2.0 GW, "
        "-1 entry), Germany (+0.8 GW, +2 entries), n/a (+0.1 GW, +0 entries).\n"
        in index
    )
    assert (
        "are Nuclear (-2.0 GW, -1 entry), Wind (+0.8 GW, +1 entry), "
        "Solar (+0.1 GW, +2 entries)." in index
    )
    assert (
        "pairs of country and fuel type with the largest capacity change are "
        "France / Nuclear (-2.0 GW, -1 entry), Germany / Wind (+0.7 GW, +1 entry), "
        "Germany / Solar (+0.1 GW, +1 entry)." in index
    )
    assert (
        "Entries of the new version fail these checks: "
        "Capacity missing or not positive (1), No country (1)." in index
    )
    assert "| Capacity missing or not positive | 0 | 1 | +1 |\n" in index
    assert "| Fueltype | Nuclear | old | 1 | 2 |\n" in index
    assert "| Category value not in config | 1 | 0 | -1 |\n" in index
    assert "| Technology | ST | 1 | 0 | -1 | 2 | 0 | -2 |\n" in index
    # Each description below a table is a paragraph of the notes class
    assert index.count("\n{: .table-notes }\n") == NOTES_BLOCKS


@pytest.mark.parametrize(
    "line",
    [
        # Signed delta with the change in % in brackets, in a collapsed table
        # of a content tab (indented twice)
        "        | Germany | 1.5 | 2.3 | +0.8 (+53.3%) |",
        "        | Nuclear | 1 | 0 | -1 (-100%) |",
        # No change in % for an old value of 0
        "        | Spain | 0 | 1 | +1 |",
        "        | Hydro | 1 | 1 | +0 (+0%) |",
        "        | Germany | 100 | 75 | -25 |",
        # Without figure not collapsed
        "    | Germany | I | 2000 | G | 2020 |",
    ],
)
def test_write_report_tables(comparison: Comparison, tmp_path: Path, line: str) -> None:
    write_report(comparison, tmp_path, title="Test report")
    assert line in (tmp_path / "index.md").read_text().splitlines()


SHARE_HEADER = (
    "| Country | Fueltype | Share old [%] | Share new [%] | Share delta [pp] |\n"
    "| :-- | :-- | --: | --: | --: |\n"
)


@pytest.mark.parametrize(
    ("label", "rows"),
    [
        (
            "Capacity share by fuel type",
            # Spain has no capacity, so no share in either version
            "| France | Nuclear | 100 |  |  |\n"
            "| Germany | Wind | 66.7 | 73.9 | +7.2 |\n"
            "| Germany | Solar | 33.3 | 26.1 | -7.2 |\n"
            "| Italy | Hydro | 100 | 100 | +0 |\n"
            "|  | Wind | 100 | 100 | +0 |",
        ),
        (
            "Entry share by fuel type",
            "| France | Nuclear | 100 |  |  |\n"
            "| Germany | Solar | 50 | 50 | +0 |\n"
            "| Germany | Wind | 50 | 50 | +0 |\n"
            "| Italy | Hydro | 100 | 100 | +0 |\n"
            "| Spain | Solar |  | 100 |  |\n"
            "|  | Wind | 100 | 100 | +0 |",
        ),
    ],
)
def test_write_report_share_tables(
    comparison: Comparison, tmp_path: Path, label: str, rows: str
) -> None:
    # One row per country and fuel type with a share in a version, in the
    # order of the countries and the fuel types of the wide CSV table
    index = write_report(comparison, tmp_path, title="Test report", html=False)
    (body,) = tab_bodies(index.read_text())[label]
    assert body.endswith("\n\n" + textwrap.indent(SHARE_HEADER + rows, " " * 4))


def test_markdown() -> None:
    table = pd.DataFrame(
        {
            "Name": ["a|<b>", "c\nd"],
            "Capacity new [GW]": [1234.5, 0.0],
            "Capacity delta [GW]": [1319.5, NAN],
            "Capacity delta [%]": [5.0, NAN],
            "Newest DateIn": pd.array([2000, None], dtype="Int64"),
        },
        index=pd.Index(["X", None], name="Country"),
    )
    assert _markdown(table) == (
        "| Country | Name | Capacity new [GW] | Capacity delta [GW] | Newest DateIn |\n"
        "| :-- | :-- | --: | --: | --: |\n"
        "| X | a\\|&lt;b&gt; | 1\u202f234.5 | +1\u202f319.5 (+5%) | 2000 |\n"
        "|  | c d | 0 |  |  |\n"
    )


@pytest.mark.parametrize(
    ("value", "spec", "expected"),
    [
        (1234.56, ",.1f", "1\u202f234.6"),
        (-1234.0, "+,.1f", "-1\u202f234.0"),
        (12345, "+,d", "+12\u202f345"),
        (NAN, ",.15g", ""),
        # No negative zero after rounding
        (-0.04, "+z,.1f", "+0.0"),
    ],
)
def test_number(value: float, spec: str, expected: str) -> None:
    assert _number(value, spec) == expected


# Tables per section (field of Comparison) with and without figure
FIGURE_TABLES = {
    "overview": ["total", "country_fueltype", "sources", "status"],
    "by_country": [
        "capacity",
        "entries",
        "completeness",
        "capacity_share",
        "entry_share",
    ],
    "by_fueltype": ["capacity", "entries", "completeness"],
}
CSV_ONLY_TABLES = {
    "by_country": ["oldest_newest"],
    "by_fueltype": ["oldest_newest"],
    "checks": ["counts", "categories", "not_in_config"],
}


@pytest.mark.parametrize("map_new", [None, MapSource(NEW, "new")])
def test_write_report_files(
    comparison: Comparison, tmp_path: Path, map_new: MapSource | None
) -> None:
    write_report(comparison, tmp_path, title="Test report", map_new=map_new)
    figure_files = {
        f"{section}/{stem}.{suffix}"
        for section, stems in FIGURE_TABLES.items()
        for stem in stems
        for suffix in ("csv", "html")
    }
    csv_files = {
        f"{section}/{stem}.csv"
        for section, stems in CSV_ONLY_TABLES.items()
        for stem in stems
    }
    pages = {"index.md", "report.html"} | ({"map.html"} if map_new else set())
    written = {
        p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*") if p.is_file()
    }
    assert written == pages | figure_files | csv_files
    # CSV files hold the full tables, e.g. the old shares
    for name in sorted(f for f in written if f.endswith(".csv")):
        section, stem = name.removesuffix(".csv").split("/")
        table = comparison.sections()[section][stem]
        index = list(range(table.index.nlevels))
        written_table = pd.read_csv(tmp_path / name, index_col=index)
        pd.testing.assert_frame_equal(written_table, table, check_dtype=False)


def test_write_report_html(comparison: Comparison, tmp_path: Path) -> None:
    path = write_report(
        comparison, tmp_path, title="Old & new", map_new=MapSource(NEW, "new")
    )
    assert path == tmp_path / "report.html"
    page = path.read_text()
    assert "__PPM_" not in page
    assert "<title>Old &amp; new</title>" in page
    tabs = len(comparison.by_country) + len(comparison.by_fueltype)
    assert page.count('<div class="tabbed-block">') == tabs
    # The tab rules of the template cover all tabs of a section
    assert f"input:nth-child({len(TABS)}):checked ~ .tabbed-content" in page
    sources = re.findall(r'<iframe src="([^"]+)"', page)
    assert len(sources) == len(figures(comparison)) + 1
    assert all((tmp_path / src).is_file() for src in sources)
    # Every table is rendered, except the country-fuel type matrix (figure only)
    tables = sum(map(len, comparison.sections().values())) - 1
    assert page.count("<table>") == tables
    # The tables below the figures of the tabs are collapsed
    tab_figures = [k for k in figures(comparison) if k.startswith("by_")]
    collapsed = re.findall(
        r'<details class="note">\s*<summary>Table</summary>\s*<table>', page
    )
    assert len(collapsed) == len(tab_figures)
    assert page.count('<p class="table-notes">') == NOTES_BLOCKS
    assert re.findall(r"<h2[^>]*>(.+?)</h2>\s*<p>(.+?)</p>", page, re.S)[0] == (
        "Overview",
        INTROS["Overview"],
    )
    # The table script is inlined
    assert "function shadeDeltas(table)" in page


def test_write_report_figure_pages(comparison: Comparison, tmp_path: Path) -> None:
    # A loading message precedes plotly.js and hides once plotly drew its svg
    write_report(comparison, tmp_path, title="Test report", html=False)
    for key in figures(comparison):
        page = (tmp_path / key).with_suffix(".html").read_text()
        message = page.index('<p id="loading">Loading figure\u2026</p>')
        assert message < page.index("cdn.plot.ly") < page.index("Plotly.newPlot")
        assert "body:has(.main-svg) #loading { display: none; }" in page
        assert "if (!window.Plotly)" in page[page.index("Plotly.newPlot") :]


def test_write_report_without_html(comparison: Comparison, tmp_path: Path) -> None:
    path = write_report(comparison, tmp_path, title="Test report", html=False)
    assert path == tmp_path / "index.md"
    assert not (tmp_path / "report.html").exists()


@pytest.mark.parametrize("given", [True, False], ids=["new-given", "new-fetched"])
@pytest.mark.parametrize("year", [2020, None])
def test_write_release_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, given: bool, year: int | None
) -> None:
    # Released datasets by tag, the new one only if not given
    releases = {"v0.9.0": OLD} | ({} if given else {"v0.9.1": NEW})
    monkeypatch.setattr(
        "powerplantmatching.collection.powerplants",
        lambda from_url: releases[from_url],
    )
    path = write_release_report(
        "v0.9.0", "v0.9.1", new=NEW if given else None, folder=tmp_path, year=year
    )
    assert path == tmp_path / "v0.9.1"
    # Data only, the page is rendered by the docs
    written = {p.relative_to(path).as_posix() for p in path.rglob("*") if p.is_file()}
    tables = {
        f"{section}/{stem}.csv"
        for group in (FIGURE_TABLES, CSV_ONLY_TABLES)
        for section, stems in group.items()
        for stem in stems
    }
    assert written == tables | {"release.json"}
    assert read_release(path) == {
        "old": "v0.9.0",
        "new": "v0.9.1",
        "year": year or pd.Timestamp.now().year,
    }


def test_render_release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "powerplantmatching.collection.powerplants", lambda from_url: OLD
    )
    path = write_release_report("v0.9.0", "v0.9.1", new=NEW, folder=tmp_path, year=2020)
    files = render_release(path)
    assert set(files) == {"index.md", "map.html"} | {
        f"{key}.html" for key in figures(compare(OLD, NEW))
    }
    assert files["index.md"].startswith("# Data changes from v0.9.0 to v0.9.1\n")
    assert "### Status in 2020\n" in files["index.md"]
    assert read_map_data(files["map.html"], "ppm-data") == {
        "new": {"label": "v0.9.1", "url": URL_NEW},
        "old": {"label": "v0.9.0", "url": URL_OLD},
    }


# Without the technology ST, which is not in the config, a version compared
# with itself has empty tables of category values (checks)
WITHOUT_ST = OLD[OLD.Technology != "ST"]


@pytest.mark.parametrize(
    "old, new, empty",
    [
        (OLD, NEW, []),
        (WITHOUT_ST, WITHOUT_ST, ["categories", "not_in_config"]),
        # A name "NA" is text, not a missing value
        (OLD, NEW.replace({"Name": {"I": "NA"}}), []),
    ],
    ids=["fixture", "empty-tables", "text-NA"],
)
def test_read_tables(
    tmp_path: Path, old: pd.DataFrame, new: pd.DataFrame, empty: list[str]
) -> None:
    comparison = compare(old, new, year=2020)
    assert [stem for stem, t in comparison.checks.items() if t.empty] == empty
    write_tables(comparison, tmp_path)
    read = read_tables(tmp_path, 2020)
    assert read.year == 2020
    assert {s: list(t) for s, t in read.sections().items()} == {
        s: list(t) for s, t in comparison.sections().items()
    }
    for section, tables in comparison.sections().items():
        for stem, table in tables.items():
            pd.testing.assert_frame_equal(read.sections()[section][stem], table)
    # The entries of the total stay integers next to the capacity
    total = read.overview["total"][["old", "new", "delta"]]
    assert total.map(type).to_numpy().tolist() == [[float] * 3, [int] * 3]
    pages = {
        version: render(c, "Test", MapSource(URL_NEW, "new"), MapSource(URL_OLD, "old"))
        for version, c in [("written", comparison), ("read", read)]
    }
    assert pages["read"] == pages["written"]


def test_write_report_map_old_requires_map_new(
    comparison: Comparison, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="map_new"):
        write_report(
            comparison, tmp_path / "report", "Test", map_old=MapSource(OLD, "old")
        )
    assert not (tmp_path / "report").exists()


MAP_COLUMNS = [
    "Name",
    "Country",
    "Fueltype",
    "Technology",
    "Capacity",
    "DateIn",
    "DateOut",
    "lat",
    "lon",
]


def test_map_columns_match_template() -> None:
    """The template reads the columns that embedded versions hold."""
    page = Path(_package_data("map.html")).read_text(encoding="utf-8")
    text = re.search(r"const TEXT_COLUMNS = (\[.*\]);", page)[1]
    rest = re.search(r"const COLUMNS = \[\.\.\.TEXT_COLUMNS, (.*)\];", page)[1]
    assert json.loads(text) + json.loads(f"[{rest}]") == MAP_COLUMNS


def test_change_colors_match_tables(tmp_path: Path) -> None:
    """Figures, table shading (tables.js) and map share the colours of a change."""
    script = Path(_package_data("tables.js")).read_text(encoding="utf-8")
    block = re.search(r"const CLASS_COLORS = \[(.*?)\];", script, re.DOTALL)[1]
    assert re.findall(r'"(#[0-9a-f]{6})"', block) == CLASS_COLORS
    page = write_map(tmp_path, MapSource(NEW, "new")).read_text()
    gain, loss = CLASS_COLORS[-1], CLASS_COLORS[0]
    assert read_map_data(page, "ppm-change-colors") == {"gain": gain, "loss": loss}


def embedded(label: str, rows: int) -> dict:
    """Expected map data of an embedded version, with its rows as their number."""
    return {"label": label, "columns": MAP_COLUMNS, "rows": rows}


# Embedded versions hold the entries with coordinates: MAP_NEW drops
# "no coordinates" and keeps 8 of 9 rows, OLD keeps all 5.
@pytest.mark.parametrize(
    "new, old, expected",
    [
        (
            MapSource(MAP_NEW, "local build"),
            None,
            {"new": embedded("local build", 8), "old": None},
        ),
        (
            MapSource(MAP_NEW, "build B"),
            MapSource(OLD, "build A"),
            {"new": embedded("build B", 8), "old": embedded("build A", 5)},
        ),
        (
            MapSource(MAP_NEW, "local build"),
            MapSource(URL_OLD, "v0.9.0"),
            {
                "new": embedded("local build", 8),
                "old": {"label": "v0.9.0", "url": URL_OLD},
            },
        ),
        (
            MapSource(URL_NEW, "v0.9.1"),
            MapSource(URL_OLD, "v0.9.0"),
            {
                "new": {"label": "v0.9.1", "url": URL_NEW},
                "old": {"label": "v0.9.0", "url": URL_OLD},
            },
        ),
    ],
    ids=["frame", "frame-frame", "frame-url", "url-url"],
)
def test_write_map(
    tmp_path: Path, new: MapSource, old: MapSource | None, expected: dict
) -> None:
    path = write_map(tmp_path / "map", new, old)
    assert path == tmp_path / "map" / "map.html"
    assert (
        read_map_data(path.read_text(), "ppm-colors") == get_config()["fuel_to_color"]
    )
    data = read_map_data(path.read_text(), "ppm-data")
    counted = {
        key: {**version, "rows": len(version["rows"])}
        if version is not None and "rows" in version
        else version
        for key, version in data.items()
    }
    assert counted == expected


def test_write_map_rows(tmp_path: Path) -> None:
    path = write_map(tmp_path, MapSource(MAP_NEW, "new"))
    last = read_map_data(path.read_text(), "ppm-data")["new"]["rows"][-1]
    # NaN is null, and the name with "</script>" stays inside the JSON block
    name = "</script><script>alert(1)</script>"
    assert last == [name, None, "Solar", None, None, None, None, 51.0, 11.0]

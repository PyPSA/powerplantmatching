# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Compare two versions of the matched dataset and write a report for the docs.

Compute: `compare` computes all tables of the report without any I/O,
collected in a `Comparison` with one field per section (overview, by_country,
by_fueltype, checks) and the tables of a section keyed by stem. Per country
and per fuel type there is one table per tab (`TABS`), e.g. capacity,
entries and completeness.

Plot: `figures` draws one interactive plotly figure per table that has a
builder in `FIGURES`, keyed "<section>/<stem>" (`_key`).

Tables: `write_tables` writes each table as CSV file with plain numbers,
`read_tables` reads such a folder back into an equal `Comparison`, using the
schema of `TABLES` (index levels) and `_dtype` (column types).

Render: `render` turns a comparison into the text of the report files,
without any I/O: an `index.md` for MkDocs and per figure a standalone HTML
file, embedded as iframe above its table, which starts collapsed in the
tabs by country and fuel type. Each section of `SECTIONS` returns its
markdown. Tables are rendered by `_markdown`, with SI number format and the
change in % in brackets behind the delta. The table script
`package_data/tables.js` adds sorting and shading to the tables, in the docs
and in `report.html`. The optional map of all entries is a standalone
`map.html` (`_map_page`), embedded as second section.

Write: `write_report` writes the tables and the rendered files to a
self-contained folder that works anywhere under `docs/`, plus a standalone
`report.html` of the same text. `write_map` writes the map alone. A release
page is data only: `write_release_report` writes the tables and the release
metadata `release.json` (`read_release`), and the MkDocs hook
(docs/hooks.py) renders the page from them at docs build time with
`render_release`, so a change of the layout applies to all release pages.

Folder layout, the files of a table at `<section>/<stem>.{csv,html}`:

    index.md, report.html, map.html (with a map), release.json (release)
    overview/      total, country_fueltype, sources, status
    by_country/    capacity, entries, completeness, oldest_newest (CSV only),
                   capacity_share, entry_share
    by_fueltype/   capacity, entries, completeness, oldest_newest (CSV only)
    checks/        counts, categories, not_in_config (CSV only)

A release folder holds only the CSV files and `release.json`.

A new report section adds one field to `Comparison`, fills it in `compare`,
adds its tables to `TABLES`, registers its figure builders in `FIGURES` and
adds one section function to `SECTIONS`.
"""

import json
import re
import textwrap
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import TypedDict, TypeVar, cast

import markdown
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from pandas.api.typing import DataFrameGroupBy
from plotly.subplots import make_subplots

from . import collection
from .core import _package_data, get_config

MW_PER_GW = 1e3

# Thousands separator of numbers shown in the docs: narrow no-break space (SI)
THOUSANDS = "\u202f"

# Tab label -> CSV stem. Renaming a label must not rename the export.
TABS = {
    "Capacity": "capacity",
    "Entries": "entries",
    "Completeness": "completeness",
    "Oldest and newest": "oldest_newest",
    "Capacity share by fuel type": "capacity_share",
    "Entry share by fuel type": "entry_share",
}

# CSV stem -> tab label
LABELS = {stem: label for label, stem in TABS.items()}

# Tables of `compare` per section (field of `Comparison`) in report order, with
# the number of their index levels, which their CSV files do not keep
TABLES = {
    "overview": {"total": 1, "country_fueltype": 2, "sources": 1, "status": 1},
    "by_country": {stem: 1 for stem in TABS.values()},
    "by_fueltype": {"capacity": 1, "entries": 1, "completeness": 1, "oldest_newest": 1},
    "checks": {"counts": 1, "categories": 2, "not_in_config": 2},
}

# Metadata of a release folder: old and new tag and the year of the comparison
RELEASE = "release.json"


class Release(TypedDict):
    """
    Metadata of a release folder of `write_release_report`.

    Attributes
    ----------
    old : str
        Tag of the old release.
    new : str
        Tag of the new release.
    year : int
        Reference year of the status table.
    """

    old: str
    new: str
    year: int


# Share tables: the markdown shows them long, one row per group and fuel type
# (see `_long_shares`), so they fit the page. The CSV file keeps the wide table.
SHARES = ["capacity_share", "entry_share"]

# Columns of a share table in the markdown, the old, new and changed share
SHARE_COLUMNS = ["Share old [%]", "Share new [%]", "Share delta [pp]"]

# Suffixes of the old, new and delta column of a fuel type in a wide share table
SHARE_SUFFIXES = [" old [%]", " [%]", " delta [pp]"]

# Status classes of the overview in table order, see `_status`
STATUSES = ["operating", "planned", "retired", "undated"]

# Entries with equal values in these columns are repeated entries
IDENTITY_COLUMNS = ["Name", "Country", "Fueltype", "Capacity", "lat", "lon"]

# Category column -> config mapping whose keys are its allowed values
CONFIG_CATEGORIES = {
    "Fueltype": "target_fueltypes",
    "Technology": "target_technologies",
    "Set": "target_sets",
}

# An entry is complete if all of these columns are filled
CORE_COLUMNS = ["Fueltype", "Technology", "Set", "Capacity", "DateIn", "lat", "lon"]

# Explanation per tab label of `TABS`, joined in the notes below the tabs of a section
NOTES = {
    "Capacity": "Capacities are the sum over all entries in the file, including "
    "planned and retired plants, in GW. The delta in brackets is in % of the old "
    "version, left out if the old version has none.",
    "Entries": "The change of entries is a net change, so equal numbers of added "
    "and removed entries cancel out.",
    "Completeness": "An entry is complete if "
    + ", ".join(CORE_COLUMNS[:-1])
    + f" and {CORE_COLUMNS[-1]} are all filled.",
    "Oldest and newest": "Oldest and newest are the entries with the earliest and "
    "latest DateIn (year) over all entries of the new version, including planned "
    "plants. Ties go to the larger capacity, then to the name.",
    "Capacity share by fuel type": "Capacity shares are in % of the capacity of the "
    "country, their changes in percentage points (pp). The capacity share table "
    "leaves out the fuel types a country has entries of in neither version and the "
    "countries with capacity in neither version.",
    "Entry share by fuel type": "Entry shares are in % of the entries of the "
    "country, their changes in percentage points (pp). The entry share table "
    "leaves out the fuel types a country has entries of in neither version.",
}

# Columns of the map entries. Embedded versions hold these, CSV files must have them.
# Name, Country, Fueltype, lat and lon identify an entry across versions.
# Keep in sync with COLUMNS and TEXT_COLUMNS in package_data/map.html.
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

# A figure or the map in the page, loaded when shown (e.g. on opening its tab).
# Until the iframe paints its page, its transparent box shows the loading
# message behind it, which the message of the figure page then covers. The
# positioned iframe paints over the following block, so a bottom margin like
# that of a table keeps the description below, pulled up by tables.css, clear.
IFRAME = (
    '<div style="position: relative; margin-bottom: 1em">\n'
    '<p style="position: absolute; inset: 0; margin: 0; display: grid;'
    ' place-items: center; color: #757575">Loading figure…</p>\n'
    '<iframe src="{src}" title="{title}" loading="lazy"\n'
    '  style="position: relative; display: block; width: 100%;'
    ' height: {height}px; border: 0"></iframe>\n'
    "</div>\n"
)

# Page of a figure file. Without the default margin of the body, the figure
# fits the iframe of its height, see `_entry`. Without scrollbars, a figure
# drawn while its page loads (e.g. on switching tabs) takes the full width.
# A message fills the page until plotly draws its first svg (.main-svg), or
# tells when plotly.js did not load from the CDN, e.g. offline.
FIGURE_PAGE = """<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<style>
#loading {{
  position: absolute; inset: 0; margin: 0; display: grid; place-items: center;
  font: 14px system-ui, sans-serif; color: #757575; background: #fff;
}}
body:has(.main-svg) #loading {{ display: none; }}
</style>
</head>
<body style="margin: 0; overflow: hidden">
<p id="loading">Loading figure…</p>
{div}
<script>
if (!window.Plotly) document.getElementById("loading").textContent =
  "The figure needs plotly.js from cdn.plot.ly, which did not load.";
</script>
</body>
</html>
"""

# A table below its figure in a content tab, collapsed (pymdownx.details)
COLLAPSED = '??? note "Table"\n\n{table}'

# Static description under each section heading, before the generated text. The
# groupings share one text, `{group}` is their name.
INTROS = {
    "Overview": "Total capacity and number of entries of both versions, with the "
    "change in absolute terms and in % of the old version. The heatmap shows the "
    "change per country and fuel type, the tables the data sources and the status.",
    "Map": "The entries with coordinates of both versions, each loaded on a button "
    "click. Switch between the versions, the entries only in one version (matched "
    "by name, country, fuel type and position) and the capacity change per grid "
    "cell.",
    "group": "The change per {group}, with one tab per table. A figure shows old, "
    "new and change, the table below it is collapsed and sorted by a click on a "
    "column header.",
    "Checks": "The number of entries that fail a data-quality check, and the "
    "category values found in only one version or not in the config. A non-zero "
    "count is a hint to inspect, not an error.",
}


def _heading(name: str, text: str | None = None) -> str:
    """Section heading with its static description."""
    return f"## {name}\n\n{text or INTROS[name]}\n\n"


MAP_SECTION = (
    _heading("Map")
    + IFRAME.format(src="map.html", title="Map of all entries", height=600)
    + "\n[Open the map in a full window](map.html)\n"
)

# Markdown extensions of mkdocs.yaml that the report uses, for report.html
MARKDOWN_EXTENSIONS = [
    "tables",
    "md_in_html",
    "attr_list",
    "pymdownx.tabbed",
    "pymdownx.details",
]
MARKDOWN_CONFIG = {"pymdownx.tabbed": {"alternate_style": True}}

# Colour classes of a change in the heatmaps and the table shading, from the
# largest loss to the largest gain: orange and blue with equal OKLab lightness
# per class (50, 63, 76, 88 from outer to inner), no change in the middle.
# Neighbouring classes stay apart under simulated protan, deutan and tritan
# vision. Keep in sync with CLASS_COLORS in package_data/tables.js.
CLASS_COLORS = [
    "#8e5318",
    "#bd7634",
    "#e0a26f",
    "#f6cfb0",
    "#f0f0f0",
    "#badbfe",
    "#7fb6ee",
    "#4b8dcf",
    "#2e669c",
]
# Size classes per sign, one per decade of the change (`_class_bounds`)
CLASSES = (len(CLASS_COLORS) - 1) // 2

# Figure colours: a change by its sign in the outer class, versions in grey
LOSS, NO_CHANGE, GAIN = CLASS_COLORS[0], CLASS_COLORS[CLASSES], CLASS_COLORS[-1]
OLD_COLOR = "rgb(166, 166, 166)"
NEW_COLOR = "rgb(72, 72, 72)"

# Height of one group (row) of a figure in px, without margins
GROUP_HEIGHT = 22

# Label of a missing group (no country or fuel type) in figures, as in the prose
MISSING = "n/a"

# Hover templates: the group (and for a heatmap cell the column) as heading
# above the lines of `_hover`
BAR_HOVER = "<b>%{y}</b><br>%{hovertext}<extra></extra>"
CELL_HOVER = "<b>%{y} · %{x}</b><br>%{text}<extra></extra>"

# d3 tick format of delta axes: sign also on zero, as in the tables
DELTA_TICKS = "+,~g"

FrameT = TypeVar("FrameT", pd.Series, pd.DataFrame)


@dataclass(frozen=True)
class MapSource:
    """
    A version of the matched dataset for the map.

    Attributes
    ----------
    data : pd.DataFrame or str
        Dataset embedded in the page, or the URL of a CSV file fetched in the
        browser at runtime.
    label : str
        Name of the version in the layer switch and the legend.
    """

    data: pd.DataFrame | str
    label: str


@dataclass(frozen=True)
class Comparison:
    """
    Tables comparing an old and a new version of the matched dataset.

    Each field but `year` is one section of the report. Tables are keyed by
    their CSV stem. In the sections by_country and by_fueltype the tab label
    of a table is `LABELS[stem]`.

    Attributes
    ----------
    overview : dict[str, pd.DataFrame]
        "total": capacity in GW and number of entries (rows) with columns old,
        new, delta and delta [%] (change in % of old).
        "country_fueltype": capacity and entries per country and fuel type
        (union of both versions), "sources": per data source (an entry with
        several sources counts for each), "status": capacity per status in the
        reference year. All with columns `<Quantity> old/new/delta` and
        `<Quantity> delta [%]`, Quantity being Capacity (with unit [GW]) or
        Entries.
    by_country : dict[str, pd.DataFrame]
        Stem (value of `TABS`) -> table indexed by country, sorted
        alphabetically with entries without country last. The capacity and
        entries tables hold old, new, delta and delta [%], NaN for an old
        value of 0. The share tables hold per fuel type `<fuel> old [%]`,
        `<fuel> [%]` and `<fuel> delta [pp]`.
    by_fueltype : dict[str, pd.DataFrame]
        The same per fuel type, without the share tables.
    checks : dict[str, pd.DataFrame]
        "counts": old, new and delta of entries failing a data-quality check.
        "categories": category values found in only one version.
        "not_in_config": values of Fueltype, Technology and Set that are not
        keys of their config mapping (`CONFIG_CATEGORIES`), with columns
        `<Quantity> old/new/delta`.
    year : int
        Reference year of the status table.
    """

    overview: dict[str, pd.DataFrame]
    by_country: dict[str, pd.DataFrame]
    by_fueltype: dict[str, pd.DataFrame]
    checks: dict[str, pd.DataFrame]
    year: int

    def sections(self) -> dict[str, dict[str, pd.DataFrame]]:
        """Tables by section (field name) and stem, in report order."""
        return {
            "overview": self.overview,
            "by_country": self.by_country,
            "by_fueltype": self.by_fueltype,
            "checks": self.checks,
        }


def _old_new_delta(old: pd.Series, new: pd.Series) -> pd.DataFrame:
    """
    Align old and new values with 0 for missing keys, add new minus old (delta)
    and its change in % of old (delta [%]).
    """
    table = pd.concat({"old": old, "new": new}, axis=1, sort=True)
    table = table.fillna(0).astype(old.dtype)
    return table.assign(
        **{
            "delta": table["new"] - table["old"],
            "delta [%]": _relative(table["old"], table["new"]),
        }
    )


def _round(values: FrameT, decimals: int) -> FrameT:
    """
    Round for publishing, after any delta is taken from the unrounded values.

    Adding 0.0 turns -0.0 of a small negative delta into 0.0.
    """
    return values.round(decimals) + 0.0


def _capacity(old: pd.Series, new: pd.Series) -> pd.DataFrame:
    """
    Old, new and delta capacity in GW, rounded to 3 decimals (MW precision).

    delta [%] stays at 1 decimal, as `_relative` rounds it already.
    """
    return _round(_old_new_delta(*(s.div(MW_PER_GW) for s in (old, new))), 3)


def _percent(old: FrameT, new: FrameT) -> tuple[FrameT, FrameT, FrameT]:
    """
    Old and new fractions in % and their change in pp, all rounded to 1 decimal.

    The change is taken before rounding. The union of the keys of both versions
    is kept. Keys missing in a version are NaN, and so is their change.
    """
    old, new = (x.mul(100) for x in old.align(new))
    return _round(old, 1), _round(new, 1), _round(new - old, 1)


def _relative(old: pd.Series, new: pd.Series) -> pd.Series:
    """Change from old to new in % of old, rounded to 1 decimal, NaN for old 0."""
    return _round((new - old) / old.where(old != 0) * 100, 1)


def _total(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Total capacity in GW and number of entries, with the change in % of old."""
    # Also for an empty old version
    if old.Capacity.sum() == 0:
        raise ValueError("The change in % is undefined for an old value of 0.")
    capacity = [pd.Series([df.Capacity.sum()]) for df in (old, new)]
    entries = [pd.Series([len(df)]) for df in (old, new)]
    rows = {"Capacity [GW]": _capacity(*capacity), "Entries": _old_new_delta(*entries)}
    # object keeps the entries integer next to the capacity in one column
    total = pd.DataFrame({q: t.astype(object).iloc[0] for q, t in rows.items()})
    return total.T.rename_axis("Quantity").astype({"delta [%]": float})


def _completeness(old: pd.DataFrame, new: pd.DataFrame, key: str) -> pd.DataFrame:
    """Share of entries per group with all core columns filled."""
    complete = _percent(
        *(
            df[CORE_COLUMNS].notna().all(axis=1).groupby(df[key], dropna=False).mean()
            for df in (old, new)
        )
    )
    columns = ["Complete old [%]", "Complete new [%]", "Complete delta [pp]"]
    return pd.concat(complete, axis=1, keys=columns)


def _oldest_newest(new: pd.DataFrame, key: str) -> pd.DataFrame:
    """Name and DateIn of the oldest and newest entry per group of the new version."""
    dated = new.dropna(subset=["DateIn"])

    def extreme(label: str, ascending: bool) -> pd.DataFrame:
        # Whole rows, so name and year come from the same entry. Ties go to the
        # larger capacity, then to the name.
        rows = dated.sort_values(
            ["DateIn", "Capacity", "Name"],
            ascending=[ascending, False, True],
            kind="stable",
        ).drop_duplicates(key)
        rows = rows.set_index(key)[["Name", "DateIn"]].astype({"DateIn": "Int64"})
        return rows.set_axis([label, f"{label} DateIn"], axis=1)

    # sort=True restores the alphabetical group order with NaN last
    return pd.concat(
        [extreme("Oldest", True), extreme("Newest", False)], axis=1, sort=True
    )


def _shares(old: pd.Series, new: pd.Series) -> pd.DataFrame:
    """
    Share of each fuel type per group in % and its change in pp.

    `old` and `new` hold a quantity (e.g. capacity) indexed by group and fuel
    type. A fuel type missing in a version counts as 0% in a group of that
    version. A group missing in a version, or with a zero total, has no share.
    """
    old_pivot, new_pivot = (s.unstack(fill_value=0) for s in (old, new))
    # Fuel types by descending total of the new version
    fuels = (
        new_pivot.sum()
        .reindex(old_pivot.columns.union(new_pivot.columns), fill_value=0)
        .sort_values(ascending=False, kind="stable")
        .index
    )

    def fractions(pivot: pd.DataFrame) -> pd.DataFrame:
        total = pivot.sum(axis=1)
        # NaN for a zero total. 0 / 0 may give a NaN with the sign bit set,
        # which a figure keeps, but a CSV file does not (`read_tables`).
        return pivot.reindex(columns=fuels, fill_value=0).div(
            total.where(total != 0), axis=0
        )

    old_share, new_share, delta = _percent(fractions(old_pivot), fractions(new_pivot))
    table = pd.concat(
        [
            share.add_suffix(suffix)
            for share, suffix in zip((old_share, new_share, delta), SHARE_SUFFIXES)
        ],
        axis=1,
    )
    columns = [f"{fuel}{suffix}" for fuel in fuels for suffix in SHARE_SUFFIXES]
    return table[columns].rename_axis(columns=None)


def _share_columns(table: pd.DataFrame) -> dict[str, list[str]]:
    """Old, new and delta column of each fuel type of a share table, in table order."""
    delta = SHARE_SUFFIXES[-1]
    fuels = [c.removesuffix(delta) for c in map(str, table) if c.endswith(delta)]
    return {fuel: [f"{fuel}{suffix}" for suffix in SHARE_SUFFIXES] for fuel in fuels}


def _labelled(table: pd.DataFrame, name: str, unit: str = "") -> pd.DataFrame:
    """Name the columns of `_old_new_delta` after the quantity, e.g. "Capacity old [GW]"."""
    return table.rename(
        columns={
            "old": f"{name} old{unit}",
            "new": f"{name} new{unit}",
            "delta": f"{name} delta{unit}",
            "delta [%]": f"{name} delta [%]",
        }
    )


def _volume(groups: list[DataFrameGroupBy]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Capacity and entries tables of the groups of the old and the new version."""
    capacity = _capacity(*(g.Capacity.sum() for g in groups))
    entries = _old_new_delta(*(g.size() for g in groups))
    return _labelled(capacity, "Capacity", " [GW]"), _labelled(entries, "Entries")


def _by(old: pd.DataFrame, new: pd.DataFrame, key: str) -> dict[str, pd.DataFrame]:
    """Tables per group of `key` shared by all groupings, by stem."""
    capacity, entries = _volume([df.groupby(key, dropna=False) for df in (old, new)])
    return {
        "capacity": capacity,
        "entries": entries,
        "completeness": _completeness(old, new, key),
        "oldest_newest": _oldest_newest(new, key),
    }


def _sources(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Capacity and entries per data source, an entry counts for each of its sources."""
    exploded = []
    for df in (old, new):
        if (
            "projectID" not in df
            or not df.projectID.map(lambda v: isinstance(v, dict)).all()
        ):
            raise ValueError("`projectID` must hold a dict of sources in every entry.")
        exploded.append(
            df[["Capacity"]].assign(Source=df.projectID.map(list)).explode("Source")
        )
    return pd.concat(_volume([df.groupby("Source") for df in exploded]), axis=1)


def _status(df: pd.DataFrame, year: int) -> pd.Series:
    """
    Status of each entry in `year`: undated (no DateIn), planned (DateIn after
    year), retired (DateOut before year), else operating. DateOut in `year`
    still operates. The first matching rule wins, so an entry without DateIn
    is undated even if its DateOut is before `year`.
    """
    return pd.Series(
        np.select(
            [df.DateIn.isna(), df.DateIn > year, df.DateOut < year],
            ["undated", "planned", "retired"],
            "operating",
        ),
        index=df.index,
        name="Status",
    )


def _by_status(old: pd.DataFrame, new: pd.DataFrame, year: int) -> pd.DataFrame:
    """Capacity per status in `year`, in the order of `STATUSES`."""
    sums = [
        df.Capacity.groupby(_status(df, year)).sum().reindex(STATUSES, fill_value=0)
        for df in (old, new)
    ]
    return _labelled(_capacity(*sums), "Capacity", " [GW]")


def _allowed(config: dict) -> dict[str, list[str]]:
    """Allowed values per category column: the keys of its config mapping."""
    return {column: list(config[key]) for column, key in CONFIG_CATEGORIES.items()}


def _counts(old: pd.DataFrame, new: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Old, new and delta number of entries failing each data-quality check."""
    allowed = _allowed(config)

    def count(df: pd.DataFrame) -> pd.Series:
        not_in_config = pd.concat(
            [df[c].notna() & ~df[c].isin(keys) for c, keys in allowed.items()], axis=1
        )
        failing = {
            "Capacity missing or not positive": ~(df.Capacity > 0),
            "No coordinates": df.lat.isna() | df.lon.isna(),
            "No country": df.Country.isna(),
            "DateOut before DateIn": df.DateOut < df.DateIn,
            "Repeated entries": df.duplicated(IDENTITY_COLUMNS),
            "Category value not in config": not_in_config.any(axis=1),
        }
        return pd.Series({check: mask.sum() for check, mask in failing.items()})

    old_count = count(old)
    table = _old_new_delta(old_count, count(new))[["old", "new", "delta"]]
    return table.reindex(old_count.index).rename_axis("Check")


def _category_values(df: pd.DataFrame) -> pd.DataFrame:
    """Entries and capacity in GW per category and value, e.g. Fueltype, Wind."""
    categories = {
        "Fueltype": df.Fueltype,
        "Technology": df.Technology,
        "Set": df.Set,
        "Fueltype / Technology": df.Fueltype + " / " + df.Technology,
    }
    return pd.concat(
        {
            category: df.Capacity.groupby(values).agg(Entries="size", Capacity="sum")
            for category, values in categories.items()
        },
        names=["Category", "Value"],
    )


def _categories(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Category values found in only one version, with their entries and capacity."""
    values = {"old": _category_values(old), "new": _category_values(new)}
    only = {
        version: table.loc[table.index.difference(values[other].index)]
        for (version, table), other in zip(values.items(), ["new", "old"])
    }
    table = pd.concat(only, names=["Only in"]).reset_index("Only in")
    table["Capacity [GW]"] = _round(table.pop("Capacity") / MW_PER_GW, 3)
    return table[["Only in", "Entries", "Capacity [GW]"]].sort_index()


def _not_in_config(old: pd.DataFrame, new: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Values of the `CONFIG_CATEGORIES` not allowed by the config, per version."""
    allowed = _allowed(config)

    def not_allowed(df: pd.DataFrame) -> pd.DataFrame:
        table = _category_values(df)
        return table.loc[[c in allowed and v not in allowed[c] for c, v in table.index]]

    values = [not_allowed(df) for df in (old, new)]
    capacity = _capacity(*(v.Capacity for v in values))
    entries = _old_new_delta(*(v.Entries for v in values))
    columns = ["old", "new", "delta"]
    return pd.concat(
        [
            _labelled(entries[columns], "Entries"),
            _labelled(capacity[columns], "Capacity", " [GW]"),
        ],
        axis=1,
    )


def compare(
    old: pd.DataFrame,
    new: pd.DataFrame,
    year: int | None = None,
    config: dict | None = None,
) -> Comparison:
    """
    Compare two versions of the matched dataset.

    Parameters
    ----------
    old, new : pd.DataFrame
        Matched datasets, e.g. from `powerplants()`, with at least the columns
        Country, Fueltype, Name, Capacity in MW, DateIn, DateOut, projectID
        (dict of sources) and the remaining columns of `CORE_COLUMNS`.
    year : int, optional
        Reference year of the status table. Defaults to the current year.
    config : dict, optional
        Configuration with the allowed category values of the checks
        (`CONFIG_CATEGORIES`), default `get_config()`.

    Returns
    -------
    Comparison
        All tables of the report.
    """
    if year is None:
        year = pd.Timestamp.now().year
    if config is None:
        config = get_config()
    by_fuel = [df.groupby(["Country", "Fueltype"], dropna=False) for df in (old, new)]
    by_country = {
        **_by(old, new, "Country"),
        "capacity_share": _shares(*(g.Capacity.sum() for g in by_fuel)),
        "entry_share": _shares(*(g.size() for g in by_fuel)),
    }
    return Comparison(
        overview={
            "total": _total(old, new),
            "country_fueltype": pd.concat(_volume(by_fuel), axis=1),
            "sources": _sources(old, new),
            "status": _by_status(old, new, year),
        },
        by_country=by_country,
        by_fueltype=_by(old, new, "Fueltype"),
        checks={
            "counts": _counts(old, new, config),
            "categories": _categories(old, new),
            "not_in_config": _not_in_config(old, new, config),
        },
        year=year,
    )


# Figures: one builder per table, each takes the table only. Groups (rows of
# a table) are on the y axis, ordered by the absolute change, largest on top.
# Figures plot the exact table values. Hover texts format values like the
# markdown tables via `_cells`.


def _group_labels(table: pd.DataFrame) -> pd.DataFrame:
    """The table with missing group labels (NaN) as MISSING, in every index level."""
    index = pd.MultiIndex.from_frame(table.index.to_frame().fillna(MISSING))
    return table.set_axis(index if index.nlevels > 1 else index.get_level_values(0))


def _by_change(values: pd.Series) -> pd.Index:
    """Labels ordered by the absolute value, largest first, NaN last."""
    return values.sort_values(
        key=abs, ascending=False, kind="stable", na_position="last"
    ).index


def _signs(values: pd.Series) -> list[str]:
    """Colour per change: GAIN, LOSS, or NO_CHANGE for zero and NaN."""
    return np.select([values > 0, values < 0], [GAIN, LOSS], NO_CHANGE).tolist()


def _columns(table: pd.DataFrame, name: str) -> tuple[str, str, str, str | None]:
    """
    Old, new and delta column of quantity `name`, found by the suffixes " old",
    " new" and " delta" with an optional unit, e.g. "Capacity old [GW]", and
    the change in % ("<name> delta [%]") or None if the table has none.
    """
    percent = f"{name} delta [%]"

    def find(suffix: str) -> str:
        pattern = rf"{re.escape(name)} {suffix}( \[[^]]+\])?"
        # Unpacking fails on a missing or ambiguous column
        columns = map(str, table)
        (column,) = [c for c in columns if c != percent and re.fullmatch(pattern, c)]
        return column

    return (
        find("old"),
        find("new"),
        find("delta"),
        percent if percent in table else None,
    )


def _quantities(table: pd.DataFrame) -> list[tuple[str, str, str, str | None]]:
    """`_columns` of each quantity of the table in column order, e.g. Capacity."""
    pattern = r"(.+) old( \[[^]]+\])?"
    names = [m[1] for c in map(str, table) if (m := re.fullmatch(pattern, c))]
    return [_columns(table, name) for name in names]


def _hover(table: pd.DataFrame, columns: Sequence[str | None]) -> pd.Series:
    """
    Hover text per row: one line per column with its value as in the tables
    (deltas with sign, also +0), MISSING for NaN. A change in % goes in
    brackets behind the line before it. None is skipped. The group heading
    comes from the hover template (BAR_HOVER, CELL_HOVER).
    """
    text = pd.Series("", index=table.index)
    for column in filter(None, columns):
        cells, _ = _cells(table[column], column)
        if column.endswith("delta [%]"):
            text += [f" ({cell}%)" if cell else "" for cell in cells]
        else:
            text += [f"<br>{column}: {cell or MISSING}" for cell in cells]
    return text.str.removeprefix("<br>")


def _layout(fig: go.Figure, groups: int, top: int = 40, bottom: int = 50) -> go.Figure:
    """
    Layout of all figures: no title (the tab label is the title), legend on
    top, first group on top. The height is fixed to GROUP_HEIGHT per group
    plus the margins, so a page embedding the figure takes it from
    `fig.layout.height`.
    """
    fig.update_layout(
        template="plotly_white",
        height=groups * GROUP_HEIGHT + top + bottom,
        margin={"t": top, "b": bottom, "l": 10, "r": 10},
        legend={"orientation": "h", "x": 0, "y": 1, "yanchor": "bottom"},
        hoverlabel={"align": "left"},
        bargap=0.3,
        # Decimal and thousands separator as in the tables
        separators=f".{THOUSANDS}",
    )
    # Plotly draws the first category at the bottom
    fig.update_yaxes(autorange="reversed", automargin=True)
    fig.update_xaxes(automargin=True)
    return fig


def _class_bounds(peak: float) -> np.ndarray:
    """
    Lower bounds of the size classes 2 to CLASSES of changes up to `peak` > 0:
    whole decades, the largest class from one decade below the decade of the
    peak, e.g. 0.1, 1 and 10 for a peak of 103. As classBounds in tables.js.
    """
    return 10.0 ** (np.floor(np.log10(peak)) - np.arange(CLASSES - 1, 0, -1))


def _heatmap(z: pd.DataFrame, hover: pd.DataFrame, colorbar: dict) -> go.Heatmap:
    """
    Heatmap of changes in the colour classes of the table shading (tables.js):
    no change, then CLASSES classes per sign, one per decade of |v|
    (`_class_bounds` of the largest |v|), so small changes stay visible next
    to large ones. The colourbar ticks show the class bounds. NaN cells stay
    blank.
    """
    peak = z.abs().max().max()
    bounds = _class_bounds(peak if peak > 0 else 1.0)
    # Signed class: 0 for no change, else 1 plus the bounds reached by |v|
    classes = np.sign(z) * (1 + sum(z.abs() >= bound for bound in bounds))
    # One constant colour step per class on [-CLASSES - 0.5, CLASSES + 0.5]
    steps = len(CLASS_COLORS)
    colorscale = [
        [(i + edge) / steps, color]
        for i, color in enumerate(CLASS_COLORS)
        for edge in (0, 1)
    ]
    # Ticks on the edges between the classes of each sign, and on no change
    edges = np.arange(1, CLASSES) + 0.5
    ticks = [*-edges[::-1], 0, *edges]
    labels = [_number(v, "+,.15g") for v in (*-bounds[::-1], 0, *bounds)]
    return go.Heatmap(
        z=classes.to_numpy(),
        x=list(z.columns),
        y=list(z.index),
        text=hover.to_numpy(),
        hovertemplate=CELL_HOVER,
        hoverongaps=False,
        colorscale=colorscale,
        zmin=-CLASSES - 0.5,
        zmax=CLASSES + 0.5,
        xgap=1,
        ygap=1,
        colorbar={"tickvals": ticks, "ticktext": labels, "thickness": 10, **colorbar},
    )


def _total_figure(table: pd.DataFrame) -> go.Figure:
    """
    One panel per quantity (row of the total table) with the bars old, new and
    delta, each labelled with its value, the delta also with its change in %.
    """
    cells = {c: _cells(values, str(c))[0] for c, values in table.items()}
    fig = make_subplots(
        rows=1,
        cols=len(table),
        subplot_titles=list(table.index),
        horizontal_spacing=0.1,
    )
    for i, quantity in enumerate(table.index):
        row = table.loc[quantity]
        percent = cells["delta [%]"][i]
        delta = cells["delta"][i] + (f" ({percent}%)" if percent else "")
        # Outside its bar, the label of a negative delta would run over the
        # y labels, so it goes right of zero, where the delta row is empty
        negative = row["delta"] < 0
        fig.add_trace(
            go.Bar(
                x=row[["old", "new", "delta"]].astype(float),
                y=["old", "new", "delta"],
                orientation="h",
                marker_color=[OLD_COLOR, NEW_COLOR, *_signs(row[["delta"]])],
                text=[cells["old"][i], cells["new"][i], delta],
                textposition=["auto", "auto", "none" if negative else "auto"],
                cliponaxis=False,
                hovertemplate="%{y}: %{text}<extra></extra>",
                showlegend=False,
            ),
            row=1,
            col=i + 1,
        )
        if negative:
            label = go.Scatter(
                x=[0],
                y=["delta"],
                mode="text",
                text=[delta],
                textposition="middle right",
                hoverinfo="skip",
                showlegend=False,
            )
            fig.add_trace(label, row=1, col=i + 1)
    # Inside labels keep a readable size on the thin bars
    fig.update_layout(uniformtext={"minsize": 11, "mode": "show"})
    return _layout(fig, 3)


def _change_figure(table: pd.DataFrame) -> go.Figure:
    """
    Two panels per quantity, groups shared on y: old and new as grouped bars,
    and the delta coloured by its sign. Groups keep the order of the table:
    alphabetical with missing labels last, statuses in the order of STATUSES.
    """
    table = _group_labels(table)
    quantities = _quantities(table)
    fig = make_subplots(
        rows=1,
        cols=2 * len(quantities),
        shared_yaxes=True,
        column_widths=[3, 2] * len(quantities),
        horizontal_spacing=0.03,
    )
    for i, (old, new, delta, percent) in enumerate(quantities):
        bars = {
            "y": list(table.index),
            "orientation": "h",
            "hovertext": _hover(table, [old, new, delta, percent]),
            "hovertemplate": BAR_HOVER,
        }
        for column, name, color in ((old, "old", OLD_COLOR), (new, "new", NEW_COLOR)):
            bar = go.Bar(
                x=table[column],
                name=name,
                marker_color=color,
                legendgroup=name,
                showlegend=i == 0,
                **bars,
            )
            fig.add_trace(bar, row=1, col=2 * i + 1)
        bar = go.Bar(
            x=table[delta],
            name=delta,
            marker_color=_signs(table[delta]),
            showlegend=False,
            **bars,
        )
        fig.add_trace(bar, row=1, col=2 * i + 2)
        fig.update_xaxes(title_text=old.replace(" old", ""), row=1, col=2 * i + 1)
        fig.update_xaxes(title_text=delta, tickformat=DELTA_TICKS, row=1, col=2 * i + 2)
    return _layout(fig, len(table))


def _matrix_figure(table: pd.DataFrame) -> go.Figure:
    """
    One heatmap of the delta per quantity, side by side: the first index level
    (country) on y, the second (fuel type) on x, both alphabetical with missing
    labels last. Each heatmap has its own scale, its colourbar on top. Pairs
    absent in both versions are blank.
    """
    # Sorted before NaN becomes MISSING, which would sort among the letters
    rows, columns = (
        table.index.unique(level).sort_values(na_position="last").fillna(MISSING)
        for level in (0, 1)
    )
    table = _group_labels(table)
    quantities = _quantities(table)
    fig = make_subplots(
        rows=1, cols=len(quantities), shared_yaxes=True, horizontal_spacing=0.06
    )
    for i, (old, new, delta, percent) in enumerate(quantities):
        domain = fig.get_subplot(1, i + 1).xaxis.domain
        heatmap = _heatmap(
            table[delta].unstack().reindex(index=rows, columns=columns),
            _hover(table, [old, new, delta, percent])
            .unstack()
            .reindex(index=rows, columns=columns),
            colorbar={
                "title": {"text": delta, "side": "top"},
                "orientation": "h",
                "x": sum(domain) / 2,
                "xanchor": "center",
                "len": 0.8 * (domain[1] - domain[0]),
                "y": 1,
                "yanchor": "bottom",
            },
        )
        fig.add_trace(heatmap, row=1, col=i + 1)
    return _layout(fig, len(rows), top=80, bottom=130)


def _share_figure(table: pd.DataFrame) -> go.Figure:
    """
    Left: 100% stacked bars of the fuel types per group, old (faded, above) and
    new as two adjacent bars, segments in the fuel colours. Right: heatmap of
    the delta [pp] per group and fuel type. Groups keep the alphabetical order
    of the table, fuel types the order of `_shares`.
    """
    table = _group_labels(table)
    shares = _share_columns(table)
    fuels = list(shares)
    deltas = table[[shares[fuel][-1] for fuel in fuels]].set_axis(fuels, axis=1)
    colors = get_config()["fuel_to_color"]
    fig = make_subplots(
        rows=1,
        cols=2,
        shared_yaxes=True,
        column_widths=[0.45, 0.55],
        horizontal_spacing=0.03,
    )
    hovers = {}
    for fuel in fuels:
        columns = shares[fuel]
        hovers[fuel] = _hover(table, columns)
        # Bars of the same offsetgroup stack, the two groups stand side by side
        for version, column, opacity in (
            ("old", columns[0], 0.5),
            ("new", columns[1], 1),
        ):
            bar = go.Bar(
                x=table[column],
                y=list(table.index),
                orientation="h",
                name=fuel,
                legendgroup=fuel,
                showlegend=version == "new",
                offsetgroup=version,
                marker_color=colors[fuel],
                opacity=opacity,
                hovertext=hovers[fuel],
                hovertemplate=BAR_HOVER,
            )
            fig.add_trace(bar, row=1, col=1)
    heatmap = _heatmap(deltas, pd.DataFrame(hovers), {"title": {"text": "delta [pp]"}})
    fig.add_trace(heatmap, row=1, col=2)
    fig.update_layout(barmode="relative", legend_title_text="Old (faded) above new:")
    fig.update_xaxes(title_text="Share [%]", range=[0, 100], row=1, col=1)
    return _layout(fig, len(table), top=100, bottom=130)


# Figure builder per table stem and section (field of `Comparison`). Tables
# without builder, e.g. oldest_newest and the checks, have no figure.
Builder = Callable[[pd.DataFrame], go.Figure]
GROUP_FIGURES: dict[str, Builder] = {
    "capacity": _change_figure,
    "entries": _change_figure,
    "completeness": _change_figure,
    "capacity_share": _share_figure,
    "entry_share": _share_figure,
}
FIGURES: dict[str, dict[str, Builder]] = {
    "overview": {
        "total": _total_figure,
        "country_fueltype": _matrix_figure,
        "sources": _change_figure,
        "status": _change_figure,
    },
    "by_country": GROUP_FIGURES,
    "by_fueltype": GROUP_FIGURES,
    "checks": {},
}


def _key(section: str, stem: str) -> str:
    """
    Key of a table: of its figure in `figures` and, with suffix .csv or .html,
    of its files relative to the report folder. `section` is the field of
    `Comparison`, e.g. "by_country/capacity".
    """
    return f"{section}/{stem}"


def figures(comparison: Comparison) -> dict[str, go.Figure]:
    """
    Interactive figure per table, each showing old, new and delta.

    Parameters
    ----------
    comparison : Comparison
        Result of `compare`.

    Returns
    -------
    dict[str, go.Figure]
        Figures keyed by "<section>/<stem>", e.g. "overview/total" or
        "by_country/capacity", section being the field of `Comparison`.
        Tables without figure (oldest_newest, checks) have no key. The height
        in px of each figure is `fig.layout.height`.
    """
    return {
        _key(section, stem): builders[stem](table)
        for section, builders in FIGURES.items()
        for stem, table in comparison.sections()[section].items()
        if stem in builders
    }


def _number(value: float, spec: str) -> str:
    """
    Format a number by `spec`, with `.` as decimal and THOUSANDS as thousands
    separator. NA is blank.
    """
    return "" if pd.isna(value) else format(value, spec).replace(",", THOUSANDS)


def _cells(values: pd.Series | pd.Index, name: str) -> tuple[list[str], str]:
    """
    Markdown cells of a column and their alignment.

    Text is left aligned. Numbers are right aligned: years (`DateIn`) not
    grouped, deltas always with their sign (also +0), `.15g` without trailing
    zeros.
    """
    if pd.api.types.infer_dtype(values, skipna=True) in ("string", "empty"):
        text = ("" if pd.isna(v) else str(v) for v in values)
        cells = (escape(t, quote=False).replace("|", "\\|") for t in text)
        return [c.replace("\n", " ") for c in cells], ":--"
    if name.endswith("DateIn"):
        spec = "d"
    elif "delta" in name:
        spec = "+,.15g"
    else:
        spec = ",.15g"
    return [_number(v, spec) for v in values], "--:"


def _markdown(table: pd.DataFrame) -> str:
    """
    Pipe table with the index as first column, further index levels as the
    next columns.

    A column ending in "delta [%]" is not shown on its own: its value goes in
    brackets behind the cell of the column before it, left out where NaN.
    """
    if table.index.nlevels > 1:
        table = table.reset_index(level=list(range(1, table.index.nlevels)))
    index = str(table.index.name)
    columns = [(index, *_cells(table.index, index))]
    for column, values in table.items():
        name = str(column)
        if name.endswith("delta [%]"):
            previous, cells, align = columns.pop()
            percents = [_number(v, "+,.15g") for v in values]
            cells = [f"{c} ({p}%)" if p else c for c, p in zip(cells, percents)]
            columns.append((previous, cells, align))
        else:
            columns.append((name, *_cells(values, name)))
    names, rows, aligns = zip(*columns)
    lines = [names, aligns, *zip(*rows)]
    return "".join(f"| {' | '.join(line)} |\n" for line in lines)


def _long_shares(table: pd.DataFrame, pairs: pd.Index) -> pd.DataFrame:
    """
    Share table (see `_shares`) with one row per group and fuel type and the
    columns SHARE_COLUMNS. Groups keep their order, fuel types the order of
    the columns. Kept are the (group, fuel type) `pairs`, e.g. those with
    entries in a version, that have a share in a version: the wide table holds
    0 for a fuel type a group lacks and NaN for a group without a total (e.g.
    without capacity), so neither marks a pair as present.
    """
    wide = pd.concat(
        {
            fuel: table[columns].set_axis(SHARE_COLUMNS, axis=1)
            for fuel, columns in _share_columns(table).items()
        },
        axis=1,
        names=["Fueltype", None],
    )
    # Stacking one of two column levels gives a DataFrame
    long = cast(pd.DataFrame, wide.stack("Fueltype", future_stack=True))
    # Matched by the labels with MISSING for NaN, as a NaN label may not match
    # itself, depending on how the index holds it (e.g. read from a CSV file)
    paired = _group_labels(long).index.isin(_group_labels(pairs.to_frame()).index)
    return long[paired & long[SHARE_COLUMNS[:2]].notna().any(axis=1)]


def _listed(words: list[str]) -> str:
    """Words as prose list, e.g. "a, b and c"."""
    return ", ".join(words[:-1]) + f" and {words[-1]}"


def _largest(capacity: pd.Series, entries: pd.Series) -> str:
    """
    The three groups with the largest absolute capacity change in GW, with
    their net change of entries, e.g. "Germany (+0.8 GW, +2 entries)". Takes
    the deltas of capacity and entries of `_volume`, on the same index. Index
    levels are joined by " / ", MISSING for NaN.
    """
    index = capacity.index.to_frame().fillna(MISSING).astype(str)
    labels = index.agg(" / ".join, axis=1).to_numpy()
    delta = capacity.set_axis(labels)
    count = entries.set_axis(labels)
    return ", ".join(
        f"{group} ({_number(delta[group], '+z,.1f')} GW, "
        f"{_number(count[group], '+,d')} {'entry' if abs(count[group]) == 1 else 'entries'})"
        for group in _by_change(delta)[:3]
    )


@dataclass(frozen=True)
class _Report:
    """A comparison and its figures."""

    comparison: Comparison
    figures: dict[str, go.Figure]

    def tables(self, section: str) -> dict[str, pd.DataFrame]:
        """Tables of a section (field of `Comparison`) by stem."""
        return self.comparison.sections()[section]


def _figure_page(key: str, fig: go.Figure) -> str:
    """Standalone page of the figure of table `key` (`FIGURE_PAGE`)."""
    # No mode bar: it would overlap the legend on top. A fixed div id keeps
    # the page the same across runs.
    div = fig.to_html(
        full_html=False,
        include_plotlyjs="cdn",
        config={"displayModeBar": False},
        div_id=key.replace("/", "-"),
    )
    return FIGURE_PAGE.format(div=div)


def _entry(
    report: _Report,
    section: str,
    stem: str,
    title: str,
    show_table: bool = True,
    collapse: bool = False,
) -> str:
    """
    Markdown of a table: its figure, if any, as iframe of <key>.html with
    `title` and the height of the figure, then the table (unless
    `show_table` is False). With `collapse`, a table below a figure starts
    collapsed (`COLLAPSED`). Share tables are shown long, see `_long_shares`.
    """
    key = _key(section, stem)
    table = report.tables(section)[stem]
    parts = []
    if key in report.figures:
        height = report.figures[key].layout.height
        parts.append(IFRAME.format(src=f"{key}.html", title=title, height=height))
    if show_table:
        if stem in SHARES:
            pairs = report.comparison.overview["country_fueltype"].index
            table = _long_shares(table, pairs)
        text = _markdown(table)
        if collapse and key in report.figures:
            text = COLLAPSED.format(table=textwrap.indent(text, " " * 4))
        parts.append(text)
    return "\n".join(parts)


def _tabs(report: _Report, section: str, heading: str) -> str:
    """
    One content tab per table of the section, labelled from `TABS`. A table
    below a figure is collapsed, as it can be long (one row per group).
    """
    # The body of a content tab is indented by 4 spaces
    return "\n".join(
        f'=== "{LABELS[stem]}"\n\n'
        + textwrap.indent(
            _entry(report, section, stem, f"{heading}: {LABELS[stem]}", collapse=True),
            " " * 4,
        )
        for stem in report.tables(section)
    )


def _notes(text: str) -> str:
    """
    Description below a table, as paragraph of class "table-notes" (attr_list),
    set in small, muted type by docs/assets/stylesheets/tables.css and
    package_data/report.html.
    """
    return f"{text}\n{{: .table-notes }}\n"


def _overview_section(report: _Report) -> str:
    """Total, change per country and fuel type, sources and status."""
    overview = report.comparison.overview
    # As dicts, since pandas-stubs type `.loc[label]` as Series or DataFrame
    capacity, entries = (
        overview["total"].loc[q].to_dict() for q in ("Capacity [GW]", "Entries")
    )
    year = report.comparison.year
    matrix = _key("overview", "country_fueltype")
    pairs = overview["country_fueltype"]
    return (
        _heading("Overview")
        + f"The total capacity changes from {_number(capacity['old'], ',.1f')} GW to "
        f"{_number(capacity['new'], ',.1f')} GW "
        f"({_number(capacity['delta'], '+z,.1f')} GW). "
        f"The number of entries changes from {_number(entries['old'], ',d')} to "
        f"{_number(entries['new'], ',d')} ({_number(entries['delta'], '+,d')}).\n\n"
        f"{_entry(report, 'overview', 'total', 'Total')}\n"
        f"{_notes(NOTES['Capacity'])}\n"
        "### By country and fuel type\n\n"
        "The pairs of country and fuel type with the largest capacity change are "
        f"{_largest(pairs['Capacity delta [GW]'], pairs['Entries delta'])}.\n\n"
        + _entry(
            report,
            "overview",
            "country_fueltype",
            "Change by country and fuel type",
            show_table=False,
        )
        + "\n"
        + _notes(
            "The colour shows the size of the change in classes of one decade, the "
            "largest from one decade below the decade of the largest change, so "
            "small changes stay visible. Blank cells have entries in "
            "neither version. Hover a cell for old, new and delta. The table is in "
            f"[{matrix}.csv]({matrix}.csv)."
        )
        + "\n### Sources\n\n"
        f"{_entry(report, 'overview', 'sources', 'Sources')}\n"
        + _notes(
            "Capacity and entries per data source. An entry with several sources "
            "counts for each, so the sources add up to more than the total."
        )
        + f"\n### Status in {year}\n\n"
        f"{_entry(report, 'overview', 'status', f'Status in {year}')}\n"
        + _notes(
            f"Capacity by status in {year}: operating (DateIn in or before {year}, "
            f"no DateOut or DateOut in or after {year}), planned (DateIn after "
            f"{year}), retired (DateOut before {year}) and undated (no DateIn)."
        )
    )


def _group_section(
    report: _Report, section: str, heading: str, group: str, noun: str
) -> str:
    """Section with the largest capacity changes and one tab per table."""
    tables = report.tables(section)
    changes = _largest(
        tables["capacity"]["Capacity delta [GW]"], tables["entries"]["Entries delta"]
    )
    notes = " ".join(NOTES[LABELS[stem]] for stem in tables)
    return (
        _heading(heading, INTROS["group"].format(group=group))
        + f"The {noun} with the largest capacity change are {changes}.\n\n"
        f"{_tabs(report, section, heading)}\n"
        + _notes(
            f"{notes} Blank cells are undefined, e.g. for a group missing in one "
            "version. Click a column header to sort. Shading marks the size of the "
            "change in classes of one decade, as in the overview figure."
        )
    )


def _country_section(report: _Report) -> str:
    return _group_section(report, "by_country", "By country", "country", "countries")


def _fueltype_section(report: _Report) -> str:
    return _group_section(
        report, "by_fueltype", "By fuel type", "fuel type", "fuel types"
    )


def _checks_section(report: _Report) -> str:
    """Data-quality counts and category values to inspect."""
    new = report.comparison.checks["counts"]["new"]
    failing = new[new > 0]
    mappings = ", ".join(f"{c} in {key}" for c, key in CONFIG_CATEGORIES.items())
    not_in_config = "Category values not in config"
    if failing.empty:
        found = "No entry of the new version fails a check."
    else:
        found = (
            "Entries of the new version fail these checks: "
            + ", ".join(f"{check} ({_number(n, ',d')})" for check, n in failing.items())
            + "."
        )
    return (
        f"{_heading('Checks')}{found}\n\n"
        f"{_entry(report, 'checks', 'counts', 'Checks')}\n"
        + _notes(
            "Capacity missing or not positive: no capacity or 0 MW or less. "
            "No coordinates: lat or lon missing. DateOut before DateIn: retired "
            "before commissioned. Repeated entries: the same "
            f"{_listed(IDENTITY_COLUMNS)} as an earlier entry. Category value not "
            "in config: a value that is not a key of its mapping in the config "
            f"({mappings})."
        )
        + f"\n{_entry(report, 'checks', 'categories', 'Category values')}\n"
        + _notes(
            "Category values found in only one version, e.g. a renamed fuel type, "
            "with their entries and capacity in GW in that version."
        )
        + f"\n{_entry(report, 'checks', 'not_in_config', not_in_config)}\n"
        + _notes(
            f"{not_in_config}, with their entries and capacity in GW in both versions."
        )
        + "\nA non-zero count or a listed value is a hint to inspect, not an error.\n"
    )


# Sections of the report page in order, the map goes after the first. Each
# returns its markdown.
SECTIONS: list[Callable[[_Report], str]] = [
    _overview_section,
    _country_section,
    _fueltype_section,
    _checks_section,
]


def _map_version(source: MapSource) -> dict:
    """Map data of a version: its URL, or its entries with coordinates as rows."""
    if isinstance(source.data, str):
        return {"label": source.label, "url": source.data}
    entries = source.data[MAP_COLUMNS].dropna(subset=["lat", "lon"])
    rows = entries.astype(object).where(entries.notna(), None).values.tolist()
    return {"label": source.label, "columns": MAP_COLUMNS, "rows": rows}


def _json_script(value: object) -> str:
    """JSON for a <script> block. Escaping "<" keeps names from closing the block."""
    return json.dumps(value, allow_nan=False).replace("<", "\\u003c")


def _fill(template: str, values: dict[str, str]) -> str:
    """
    Template of the package data with each placeholder replaced by its value.

    Plain replace, as the JavaScript of the templates uses ${...}. Each
    placeholder must occur once, in the order of `values`.
    """
    page = Path(_package_data(template)).read_text(encoding="utf-8")
    for placeholder, value in values.items():
        if (count := page.count(placeholder)) != 1:
            raise ValueError(f"{placeholder} occurs {count} times in the template.")
        page = page.replace(placeholder, value)
    return page


def _map_page(new: MapSource, old: MapSource | None = None) -> str:
    """Page of the map of `write_map`."""
    old_version = None if old is None else _map_version(old)
    data = {"new": _map_version(new), "old": old_version}
    return _fill(
        "map.html",
        {
            "__PPM_COLORS__": _json_script(get_config()["fuel_to_color"]),
            "__PPM_CHANGE_COLORS__": _json_script({"gain": GAIN, "loss": LOSS}),
            "__PPM_DATA__": _json_script(data),
        },
    )


def write_map(folder: Path | str, new: MapSource, old: MapSource | None = None) -> Path:
    """
    Write a standalone interactive map of one or two versions to folder/map.html.

    The map clusters the entries by zoom level and colours them by fuel type.
    Each version is a layer that can be switched on and off: the new one as
    filled points, the old one as rings. A DataFrame is embedded in the page
    and shown at once. A URL of a CSV file, e.g. from `matched_data_url`, is
    fetched in the browser on a button click. The page needs no MkDocs.

    With `old`, two more views are computed in the browser once both versions
    are loaded. "Entry changes" shows the entries only in new (new) or only
    in old (dropped). Entries are the same if Name, Country, Fueltype and
    lat/lon rounded to 4 decimals are equal, counted as a multiset for
    duplicates. "Capacity change" shows new minus old capacity per grid
    cell, with the cell size following the zoom level.

    Parameters
    ----------
    folder : Path or str
        Output folder, created if missing. An existing map is overwritten.
    new : MapSource
        Version drawn on top.
    old : MapSource, optional
        Version drawn below for comparison.

    Returns
    -------
    Path
        Path of the written map.
    """
    path = Path(folder) / "map.html"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_map_page(new, old), encoding="utf-8")
    return path


def write_tables(comparison: Comparison, folder: Path | str) -> None:
    """
    Write each table of the comparison as CSV file with plain numbers.

    The table of section (field of `Comparison`) and stem is written to
    `folder/<section>/<stem>.csv`, e.g. `by_country/capacity.csv`, with its
    index as first columns. `read_tables` reads the folder back.

    Parameters
    ----------
    comparison : Comparison
        Result of `compare`.
    folder : Path or str
        Output folder, created if missing. Existing files are overwritten.
    """
    for section, tables in comparison.sections().items():
        for stem, table in tables.items():
            path = Path(folder) / f"{_key(section, stem)}.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            table.to_csv(path)


def _dtype(column: str) -> str:
    """
    Dtype of a table column of `compare` by its name, as its CSV file does not
    keep it for an empty table or a nullable integer: Int64 for years
    (DateIn), float64 for capacities, % and pp, int64 for counts (Entries and
    the old, new and delta of the checks), str for text and index columns.
    """
    if column.endswith("DateIn"):
        return "Int64"
    if re.search(r"\[(GW|%|pp)\]$", column):
        return "float64"
    if re.fullmatch(r"Entries( old| new| delta)?|old|new|delta", column):
        return "int64"
    return "str"


def _read_table(folder: Path, section: str, stem: str) -> pd.DataFrame:
    """Table `stem` of `section` from its CSV file of `write_tables`."""
    path = folder / f"{_key(section, stem)}.csv"
    total = (section, stem) == ("overview", "total")
    names = pd.read_csv(path, nrows=0).columns
    table = pd.read_csv(
        path,
        index_col=list(range(TABLES[section][stem])),
        # The columns of the total hold numbers of different dtype, set below
        dtype=None if total else {name: _dtype(name) for name in names},
        # Blank is NaN, e.g. no country, but text like "NA" stays
        keep_default_na=False,
        na_values=[""],
        float_precision="round_trip",
    )
    if not total:
        return table
    # Each row of the total is a quantity of its own dtype (capacity float,
    # entries int), in object columns as `_total` returns them
    numbers = table.drop(columns="delta [%]").T
    numbers = numbers.astype({q: _dtype(str(q)) for q in numbers}).astype(object).T
    return numbers.join(table["delta [%]"])


def read_tables(folder: Path | str, year: int) -> Comparison:
    """
    Read the CSV files of `write_tables` back into the comparison.

    The tables equal those of `compare`, with the same index, dtypes and
    order of rows and columns, so they render the same report.

    Parameters
    ----------
    folder : Path or str
        Folder written by `write_tables`.
    year : int
        Reference year of the status table, see `compare`.

    Returns
    -------
    Comparison
        All tables of the report.
    """
    return Comparison(
        **{
            section: {stem: _read_table(Path(folder), section, stem) for stem in stems}
            for section, stems in TABLES.items()
        },
        year=year,
    )


def render(
    comparison: Comparison,
    title: str,
    map_new: MapSource | None = None,
    map_old: MapSource | None = None,
) -> dict[str, str]:
    """
    Render the report of the comparison, without writing files.

    The report is `index.md` for MkDocs and, per figure (see `figures`), its
    standalone page `<section>/<stem>.html`, e.g. `by_country/capacity.html`.
    With `map_new`, it also has `map.html` (see `write_map`), embedded as
    second section. `index.md` links the CSV files of `write_tables` next
    to it. Figures, map and the table script load their libraries from the
    internet.

    Parameters
    ----------
    comparison : Comparison
        Result of `compare`.
    title : str
        Title of the page.
    map_new : MapSource, optional
        New version of the map. Without it, the report has no map.
    map_old : MapSource, optional
        Old version of the map, requires `map_new`.

    Returns
    -------
    dict[str, str]
        Text of each file by its path relative to the report folder.
    """
    if map_new is None and map_old is not None:
        raise ValueError("`map_old` requires `map_new`.")
    report = _Report(comparison, figures(comparison))
    sections = [section(report) for section in SECTIONS]
    pages = {
        f"{key}.html": _figure_page(key, fig) for key, fig in report.figures.items()
    }
    if map_new is not None:
        pages["map.html"] = _map_page(map_new, map_old)
        # After the overview
        sections.insert(1, MAP_SECTION)
    return {"index.md": "\n".join([f"# {title}\n", *sections]), **pages}


def write_report(
    comparison: Comparison,
    folder: Path | str,
    title: str,
    map_new: MapSource | None = None,
    map_old: MapSource | None = None,
    html: bool = True,
) -> Path:
    """
    Write the comparison as a report folder for the docs and for local use.

    The folder holds the CSV files of `write_tables` and the files of
    `render`: `index.md` for MkDocs, the figure of each table at
    `<section>/<stem>.html` next to its CSV file, e.g.
    `by_country/capacity.{csv,html}`, and with `map_new` the map `map.html`.
    With `html`, `report.html` is the same page for any browser without
    MkDocs. Figures, map and the table script load their libraries from the
    internet.

    Parameters
    ----------
    comparison : Comparison
        Result of `compare`.
    folder : Path or str
        Output folder, created if missing. Existing files are overwritten.
    title : str
        Title of the page.
    map_new : MapSource, optional
        New version of the map. Without it, the report has no map.
    map_old : MapSource, optional
        Old version of the map, requires `map_new`.
    html : bool, default True
        Also write `report.html`.

    Returns
    -------
    Path
        Path of `report.html`, or of `index.md` without `html`.
    """
    # Before any file is written, as it checks the arguments
    files = render(comparison, title, map_new, map_old)
    folder = Path(folder)
    write_tables(comparison, folder)
    for name, text in files.items():
        path = folder / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    if not html:
        return folder / "index.md"
    # The same text as rendered by MkDocs, with the extensions of the docs
    body = markdown.markdown(
        files["index.md"],
        extensions=MARKDOWN_EXTENSIONS,
        extension_configs=MARKDOWN_CONFIG,
    )
    page = _fill(
        "report.html",
        {
            "__PPM_TITLE__": escape(title),
            "__PPM_TABLES_JS__": Path(_package_data("tables.js")).read_text(
                encoding="utf-8"
            ),
            # Last, so the text of the report cannot hold another placeholder
            "__PPM_BODY__": body,
        },
    )
    path = folder / "report.html"
    path.write_text(page, encoding="utf-8")
    return path


def write_release_report(
    old_tag: str,
    new_tag: str,
    new: pd.DataFrame | None = None,
    folder: Path | str = "docs/data-changes",
    year: int | None = None,
) -> Path:
    """
    Write the data of the page of release `new_tag` against `old_tag` to
    folder/new_tag: the CSV files of `write_tables` and `release.json` with
    the tags and the year (see `read_release`). The MkDocs hook renders the
    page from them with `render_release`. `old` is read with
    pm.powerplants(from_url=old_tag), `new` too unless given (the CI passes
    the dataset it just built). `year` defaults to the current year, i.e.
    the release year in the CI.

    Parameters
    ----------
    old_tag : str
        Tag of the old release, e.g. "v0.6.0".
    new_tag : str
        Tag of the new release.
    new : pd.DataFrame, optional
        Dataset of the new release. Fetched by `new_tag` if not given.
    folder : Path or str, default "docs/data-changes"
        Parent folder of the release folder `new_tag`.
    year : int, optional
        Reference year of the status table, see `compare`.

    Returns
    -------
    Path
        The release folder.
    """
    old = collection.powerplants(from_url=old_tag)
    if new is None:
        new = collection.powerplants(from_url=new_tag)
    comparison = compare(old, new, year)
    path = Path(folder) / new_tag
    write_tables(comparison, path)
    release = Release(old=old_tag, new=new_tag, year=comparison.year)
    (path / RELEASE).write_text(json.dumps(release, indent=2) + "\n", encoding="utf-8")
    return path


def read_release(folder: Path | str) -> Release:
    """
    Metadata of a release folder of `write_release_report`.

    Parameters
    ----------
    folder : Path or str
        Release folder.

    Returns
    -------
    Release
        Tags of the old and the new release and the reference year.
    """
    return json.loads((Path(folder) / RELEASE).read_text(encoding="utf-8"))


def render_release(folder: Path | str) -> dict[str, str]:
    """
    Render the page of a release folder of `write_release_report`.

    The page is titled "Data changes from <old> to <new>", its map fetches
    both releases by URL (`matched_data_url`).

    Parameters
    ----------
    folder : Path or str
        Release folder.

    Returns
    -------
    dict[str, str]
        Text of each file by its path relative to the release folder, see
        `render`.
    """
    release = read_release(folder)
    old, new = release["old"], release["new"]
    return render(
        read_tables(folder, release["year"]),
        title=f"Data changes from {old} to {new}",
        map_new=MapSource(collection.matched_data_url(new), new),
        map_old=MapSource(collection.matched_data_url(old), old),
    )

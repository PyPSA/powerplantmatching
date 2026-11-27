# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Compare two versions of the matched dataset, e.g. two releases.

`compare` sums capacity, entries and complete entries per dimension (country,
source, status, technology, set, check), group and fuel type for both
versions. All tables and figures of a page derive from this one table.

A release folder `docs/data-changes/<tag>` holds the table as `changes.csv`
and the metadata `release.json`, written by `write_release`. The MkDocs hook
`docs/hooks.py` renders the page at build time with `render_release`.
"""

import json
import textwrap
from html import escape
from pathlib import Path
from typing import NamedTuple, TypedDict

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from . import collection
from .core import _package_data, get_config

DATA = "changes.csv"
RELEASE = "release.json"
QUANTITIES = ["Capacity", "Entries", "Complete"]
VERSIONS = ["old", "new"]
MISSING = "n/a"
THOUSANDS = " "
GAIN, LOSS = "#2e669c", "#8e5318"
OLD_COLOR, NEW_COLOR = "#a6a6a6", "#484848"
GROUP_HEIGHT = 22
CORE_COLUMNS = ["Fueltype", "Technology", "Set", "Capacity", "DateIn", "lat", "lon"]
IDENTITY_COLUMNS = ["Name", "Country", "Fueltype", "Capacity", "lat", "lon"]
CONFIG_CATEGORIES = {
    "Fueltype": "target_fueltypes",
    "Technology": "target_technologies",
    "Set": "target_sets",
}


class Release(TypedDict):
    """Metadata of a release folder: old and new tag, reference year of the status."""

    old: str
    new: str
    year: int


class Section(NamedTuple):
    heading: str
    dimension: str
    level: str
    note: str = ""


SECTIONS = {
    "country": Section("By country", "Country", "Group"),
    "fueltype": Section("By fuel type", "Country", "Fueltype"),
    "source": Section(
        "By source",
        "Source",
        "Group",
        "An entry with several sources counts for each, so the sources add up to "
        "more than the total.",
    ),
    "status": Section(
        "By status in {year}",
        "Status",
        "Group",
        "Operating: DateIn in or before {year} and no DateOut before {year}. "
        "Planned: DateIn after {year}. Retired: DateOut before {year}. "
        "Undated: no DateIn.",
    ),
    "technology": Section("By technology", "Technology", "Group"),
    "set": Section("By set", "Set", "Group"),
    "check": Section(
        "Checks",
        "Check",
        "Group",
        "Entries failing a data-quality check; an entry can fail several. "
        "Repeated entries share " + ", ".join(IDENTITY_COLUMNS) + " with an "
        "earlier entry. Category value not in config: a Fueltype, Technology or "
        "Set that is not a key of its mapping in the config. A failing entry is "
        "a hint to inspect, not an error.",
    ),
}


def _status(df: pd.DataFrame, year: int) -> pd.Series:
    """Status of each entry in `year`, the first matching rule wins."""
    conditions = [df.DateIn.isna(), df.DateIn > year, df.DateOut < year]
    labels = np.select(conditions, ["undated", "planned", "retired"], "operating")
    return pd.Series(labels, index=df.index)


def _checks(df: pd.DataFrame, config: dict) -> pd.Series:
    """Name of each failed check per entry, an entry repeated per failed check."""
    not_in_config = [
        df[column].notna() & ~df[column].isin(list(config[key]))
        for column, key in CONFIG_CATEGORIES.items()
    ]
    failing = pd.DataFrame(
        {
            "Capacity missing or not positive": ~(df.Capacity > 0),
            "No coordinates": df.lat.isna() | df.lon.isna(),
            "No country": df.Country.isna(),
            "DateOut before DateIn": df.DateOut < df.DateIn,
            "Repeated entries": df.duplicated(IDENTITY_COLUMNS),
            "Category value not in config": pd.concat(not_in_config, axis=1).any(
                axis=1
            ),
        }
    )
    entries, checks = np.nonzero(failing.to_numpy())
    return pd.Series(failing.columns[checks], index=failing.index[entries])


def _sums(df: pd.DataFrame, year: int, config: dict) -> pd.DataFrame:
    """Capacity, entries and complete entries per dimension, group and fuel type."""
    df = df.reset_index(drop=True)
    values = pd.DataFrame(
        {
            "Fueltype": df.Fueltype.fillna(MISSING),
            "Capacity": df.Capacity,
            "Entries": 1,
            "Complete": df[CORE_COLUMNS].notna().all(axis=1).astype(int),
        }
    )
    dimensions = {
        "Country": df.Country,
        "Source": df.projectID.map(lambda ids: list(dict.keys(ids))).explode(),
        "Status": _status(df, year),
        "Technology": df.Technology,
        "Set": df.Set,
        "Check": _checks(df, config),
    }
    sums = {}
    for dimension, groups in dimensions.items():
        rows = values.loc[groups.index]
        keys = [groups.fillna(MISSING).to_numpy(), rows.Fueltype.to_numpy()]
        sums[dimension] = rows[QUANTITIES].groupby(keys).sum()
    return pd.concat(sums, names=["Dimension", "Group", "Fueltype"])


def compare(
    old: pd.DataFrame,
    new: pd.DataFrame,
    year: int | None = None,
    config: dict | None = None,
) -> pd.DataFrame:
    """
    Compare two versions of the matched dataset.

    Parameters
    ----------
    old, new : pd.DataFrame
        Matched datasets, e.g. from `powerplants()`.
    year : int, optional
        Reference year of the status, default the current year.
    config : dict, optional
        Configuration with the allowed category values, default `get_config()`.

    Returns
    -------
    pd.DataFrame
        Capacity in MW, entries and complete entries (all of `CORE_COLUMNS`
        filled) of both versions, indexed by dimension, group and fuel type,
        with columns (quantity, version). Missing labels are `MISSING`.
    """
    year = pd.Timestamp.now().year if year is None else year
    config = get_config() if config is None else config
    sums = {
        version: _sums(df, year, config) for version, df in zip(VERSIONS, (old, new))
    }
    columns = pd.MultiIndex.from_product([QUANTITIES, VERSIONS])
    table = pd.concat(sums, axis=1).swaplevel(axis=1)[columns].fillna(0).sort_index()
    return table.round(3).astype({c: int for c in columns if c[0] != "Capacity"})


def _write(table: pd.DataFrame, path: Path) -> None:
    table.set_axis([" ".join(c) for c in table.columns], axis=1).to_csv(path)


def _read(path: Path) -> pd.DataFrame:
    table = pd.read_csv(
        path, index_col=[0, 1, 2], keep_default_na=False, float_precision="round_trip"
    )
    return table.set_axis(table.columns.str.split(" ").map(tuple), axis=1)


def _view(table: pd.DataFrame, section: Section) -> pd.DataFrame:
    """Old, new and delta per group, capacity in GW and completeness in %."""
    rows = table.xs(section.dimension, drop_level=False)
    sums = rows.groupby(level=section.level).sum()
    entries = sums["Entries"]
    quantities = {
        "Capacity [GW]": sums["Capacity"] / 1e3,
        "Entries": entries,
        "Complete [%]": sums["Complete"] / entries.where(entries > 0) * 100,
    }
    columns = {
        (quantity, version): values.get(version, values["new"] - values["old"])
        for quantity, values in quantities.items()
        for version in [*VERSIONS, "delta"]
    }
    name = section.dimension if section.level == "Group" else section.level
    return pd.concat(columns, axis=1).rename_axis(name)


def _number(value: float, spec: str) -> str:
    return "" if pd.isna(value) else format(value, spec).replace(",", THOUSANDS)


def _markdown(view: pd.DataFrame) -> str:
    """Pipe table, the change of capacity and entries also in % of old."""
    columns: dict[str, list[str]] = {
        str(view.index.name or ""): [
            escape(str(group)).replace("|", "\\|") for group in view.index
        ]
    }
    for quantity in dict.fromkeys(view.columns.get_level_values(0)):
        name, _, unit = quantity.partition(" ")
        digits = {"Entries": 0, "Complete": 1}.get(name, 3)
        old, new, delta = (view[quantity, version] for version in [*VERSIONS, "delta"])
        percent = delta / old.where(old != 0) * 100
        for version, values in [("old", old), ("new", new)]:
            columns[f"{name} {version} {unit}".strip()] = [
                _number(v, f",.{digits}f") for v in values
            ]
        columns[f"{name} delta {unit.replace('%', 'pp')}".strip()] = [
            _number(d, f"+z,.{digits}f")
            + ("" if name == "Complete" or pd.isna(p) else f" ({p:+z.1f}%)")
            for d, p in zip(delta, percent)
        ]
    rows: list[list[str]] = [
        list(columns),
        [":--", *["--:"] * (len(columns) - 1)],
        *map(list, zip(*columns.values())),
    ]
    return "".join(f"| {' | '.join(row)} |\n" for row in rows)


def _largest(view: pd.DataFrame) -> str:
    """The three groups with the largest absolute capacity change."""
    delta = view["Capacity [GW]", "delta"]
    top = delta.abs().sort_values(ascending=False, kind="stable").index[:3]
    entries = view["Entries", "delta"]
    return ", ".join(
        f"{group} ({_number(delta[group], '+z,.1f')} GW, "
        f"{_number(entries[group], '+,d')} entries)"
        for group in top
    )


def _layout(fig: go.Figure, groups: int, top: int = 40, bottom: int = 50) -> go.Figure:
    fig.update_layout(
        template="plotly_white",
        height=groups * GROUP_HEIGHT + top + bottom,
        margin={"t": top, "b": bottom, "l": 10, "r": 10},
        legend={"orientation": "h", "x": 0, "y": 1, "yanchor": "bottom"},
        separators=f".{THOUSANDS}",
    )
    return fig.update_yaxes(autorange="reversed", automargin=True)


def _bars(view: pd.DataFrame) -> go.Figure:
    """Old and new as grouped bars and the delta by sign, per capacity and entries."""
    quantities = ["Capacity [GW]", "Entries"]
    fig = make_subplots(
        cols=2 * len(quantities),
        shared_yaxes=True,
        column_widths=[3, 2] * len(quantities),
        horizontal_spacing=0.03,
    )
    groups = list(map(str, view.index))
    for i, quantity in enumerate(quantities):
        for version, color in [("old", OLD_COLOR), ("new", NEW_COLOR)]:
            bar = go.Bar(
                x=view[quantity, version],
                y=groups,
                orientation="h",
                name=version,
                marker_color=color,
                showlegend=i == 0,
            )
            fig.add_trace(bar, col=2 * i + 1, row=1)
        delta = view[quantity, "delta"]
        colors = np.where(delta < 0, LOSS, GAIN).tolist()
        bar = go.Bar(
            x=delta, y=groups, orientation="h", marker_color=colors, showlegend=False
        )
        fig.add_trace(bar, col=2 * i + 2, row=1)
        fig.update_xaxes(title_text=quantity, col=2 * i + 1)
        fig.update_xaxes(title_text=f"delta {quantity}", col=2 * i + 2)
    return _layout(fig, len(view))


def _heatmap(table: pd.DataFrame) -> go.Figure:
    """Capacity change per country and fuel type on a signed log scale."""
    country = table.xs("Country")
    old, new = (country["Capacity", version].unstack() for version in VERSIONS)
    delta = pd.DataFrame(new - old)
    z = np.sign(delta) * np.log10(1 + delta.abs())
    decades = 10.0 ** np.arange(1, max(2, np.ceil(np.nanmax(np.abs(z), initial=0))))
    ticks = np.log10(1 + decades)
    custom = np.dstack([x.to_numpy() / 1e3 for x in (old, new, delta)])
    heatmap = go.Heatmap(
        z=z,
        x=list(delta.columns),
        y=list(delta.index),
        customdata=custom,
        hovertemplate="<b>%{y} · %{x}</b><br>old %{customdata[0]:,.3f} GW"
        "<br>new %{customdata[1]:,.3f} GW<br>delta %{customdata[2]:+,.3f} GW"
        "<extra></extra>",
        colorscale=[[0, LOSS], [0.5, "#f0f0f0"], [1, GAIN]],
        zmin=-ticks[-1],
        zmax=ticks[-1],
        xgap=1,
        ygap=1,
        colorbar={
            "title": {"text": "delta [GW]", "side": "top"},
            "orientation": "h",
            "y": 1,
            "yanchor": "bottom",
            "tickvals": [*-ticks[::-1], 0, *ticks],
            "ticktext": [*(f"{-v:g}" for v in decades[::-1] / 1e3), "0"]
            + [f"+{v:g}" for v in decades / 1e3],
        },
    )
    return _layout(go.Figure(heatmap), len(delta), top=90, bottom=120)


def figures(table: pd.DataFrame) -> dict[str, go.Figure]:
    """
    Interactive figures of a comparison.

    Parameters
    ----------
    table : pd.DataFrame
        Result of `compare`.

    Returns
    -------
    dict[str, go.Figure]
        "country_fueltype": heatmap of the capacity change, and per key of
        `SECTIONS` the old, new and changed capacity and entries per group.
    """
    bars = {key: _bars(_view(table, section)) for key, section in SECTIONS.items()}
    return {"country_fueltype": _heatmap(table), **bars}


def _embed(fig: go.Figure, first: bool) -> str:
    return fig.to_html(
        full_html=False,
        include_plotlyjs="cdn" if first else False,
        config={"displayModeBar": False, "responsive": True},
    )


def _page(table: pd.DataFrame, release: Release) -> str:
    """Markdown page of a comparison of two releases."""
    figs = figures(table)
    total = _view(table, Section("", "Country", "Dimension")).set_axis(["Total"])
    capacity = total.iloc[0]["Capacity [GW]"]
    entries = total.iloc[0]["Entries"]
    parts = [
        f"# Data changes from {release['old']} to {release['new']}\n",
        f"The total capacity changes from {_number(capacity['old'], ',.1f')} GW to "
        f"{_number(capacity['new'], ',.1f')} GW "
        f"({_number(capacity['delta'], '+z,.1f')} GW). The number of entries changes "
        f"from {_number(entries['old'], ',.0f')} to {_number(entries['new'], ',.0f')} "
        f"({_number(entries['delta'], '+,.0f')}).\n",
        _markdown(total),
        "Capacity includes planned and retired plants. An entry is complete if "
        + ", ".join(CORE_COLUMNS)
        + " are filled.\n",
        "## By country and fuel type\n",
        "The capacity change per country and fuel type. The colour scale is "
        "logarithmic, so small changes stay visible. Blank cells have entries in "
        "neither version.\n",
        _embed(figs["country_fueltype"], first=True),
        "## Map\n",
        "The entries of both versions, each loaded on a button click.\n",
        '<iframe src="map.html" title="Map" loading="lazy" '
        'style="width: 100%; height: 600px; border: 0"></iframe>\n',
        "[Open the map in a full window](map.html)\n",
    ]
    for key, section in SECTIONS.items():
        view = _view(table, section)
        table_md = textwrap.indent(_markdown(view), " " * 4)
        parts += [
            f"## {section.heading.format(year=release['year'])}\n",
            f"Largest capacity changes: {_largest(view)}.\n",
            _embed(figs[key], first=False),
            f'??? note "Table"\n\n{table_md}',
            section.note.format(year=release["year"]) + "\n",
        ]
    return "\n".join(parts)


def _map(release: Release) -> str:
    """Map page fetching both releases by URL."""
    data = {
        version: {"label": tag, "url": collection.matched_data_url(tag)}
        for version, tag in [("old", release["old"]), ("new", release["new"])]
    }
    values = {
        "__PPM_COLORS__": get_config()["fuel_to_color"],
        "__PPM_CHANGE_COLORS__": {"gain": GAIN, "loss": LOSS},
        "__PPM_DATA__": data,
    }
    page = Path(_package_data("map.html")).read_text(encoding="utf-8")
    for placeholder, value in values.items():
        page = page.replace(placeholder, json.dumps(value).replace("<", "\\u003c"))
    return page


def write_release(
    old_tag: str,
    new_tag: str,
    new: pd.DataFrame | None = None,
    folder: Path | str = "docs/data-changes",
    year: int | None = None,
) -> Path:
    """
    Write the release folder `folder/new_tag` comparing `new_tag` with `old_tag`.

    Parameters
    ----------
    old_tag : str
        Tag of the old release, e.g. "v0.9.0".
    new_tag : str
        Tag of the new release.
    new : pd.DataFrame, optional
        Dataset of the new release, fetched by `new_tag` if not given.
    folder : Path or str, default "docs/data-changes"
        Parent folder of the release folder.
    year : int, optional
        Reference year of the status, default the current year.

    Returns
    -------
    Path
        The release folder.
    """
    old = collection.powerplants(from_url=old_tag)
    new = collection.powerplants(from_url=new_tag) if new is None else new
    year = pd.Timestamp.now().year if year is None else year
    path = Path(folder) / new_tag
    path.mkdir(parents=True, exist_ok=True)
    _write(compare(old, new, year), path / DATA)
    release = Release(old=old_tag, new=new_tag, year=year)
    (path / RELEASE).write_text(json.dumps(release, indent=2) + "\n", encoding="utf-8")
    return path


def render_release(folder: Path | str) -> dict[str, str]:
    """
    Render the page of a release folder of `write_release`.

    Parameters
    ----------
    folder : Path or str
        Release folder.

    Returns
    -------
    dict[str, str]
        Text of `index.md` and `map.html` by file name.
    """
    folder = Path(folder)
    release: Release = json.loads((folder / RELEASE).read_text(encoding="utf-8"))
    return {
        "index.md": _page(_read(folder / DATA), release),
        "map.html": _map(release),
    }

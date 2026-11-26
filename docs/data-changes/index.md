<!--
SPDX-FileCopyrightText: 2025 Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>

SPDX-License-Identifier: MIT
-->

# Data changes

Each release page compares the matched dataset `powerplants.csv` of a release with the previous release. Most tables have an interactive figure above them that shows the old and the new version and the change. Click a column header to sort a table, shading marks the size of the change. The page has these sections:

- **Overview**: total capacity and number of entries, with the change in absolute terms and in % of the previous release. Two heatmaps show the change in capacity and entries per country and fuel type. **Sources** shows capacity and entries per data source, **Status** the capacity that is operating, planned, retired or undated in a reference year.
- **Map**: the entries with coordinates of both releases, each loaded on a button click. Once both are loaded, the map has three views:
    - **Versions**: the entries of each release as a layer that can be switched off, clustered by zoom level and coloured by fuel type, the new release as filled points, the old one as rings.
    - **Entry changes**: entries only in the new release as blue triangles (new) and entries only in the old release as orange crosses (dropped), with their counts in the legend. Two entries are the same if name, country, fuel type and position (latitude and longitude rounded to 4 decimals, about 10 m) are equal, so a renamed or moved plant counts as dropped and new. Entries with the same key are counted: of 3 entries in the old and 2 in the new release, 2 match and 1 is dropped.
    - **Capacity change**: capacity of the new minus the old release per grid cell, blue for a gain and orange for a loss, shaded by the size of the change relative to the largest change in view. The cells shrink when zooming in. Hover or click a cell for its old, new and changed capacity in MW.
- **By country** and **By fuel type**: the largest capacity changes and one tab per table: capacity and entries (with the change in % of the previous release in brackets), completeness (share of entries with all core columns filled) and the oldest and newest entry (table only). The country section adds the capacity and entry share per fuel type, one row per country and fuel type.
- **Checks**: the number of entries that fail a data-quality check (missing or non-positive capacity, no coordinates, no country, DateOut before DateIn, repeated entries, a Fueltype, Technology or Set value that is not a key of its mapping in the config), the category values found in only one release and the category values not in the config. A non-zero count is a hint to inspect, not an error.

Each table is also a CSV file next to the page, each figure a standalone HTML file:

```text
<report>/
  index.md, map.html, report.html (not written for the release pages)
  overview/      total, country_fueltype, sources, status
  by_country/    capacity, entries, completeness, oldest_newest (CSV only),
                 capacity_share, entry_share
  by_fueltype/   capacity, entries, completeness, oldest_newest (CSV only)
  checks/        counts, categories, not_in_config (CSV only)
```

## Run locally

Compare your own build with a release using [`powerplantmatching.changes`](../api-changes.md):

```python
import powerplantmatching as pm

old = pm.powerplants(from_url="v0.9.1")  # a release
new = pm.powerplants()  # your build, e.g. after update=True
path = pm.changes.write_report(
    pm.changes.compare(old, new),
    "changes",
    title="My build vs v0.9.1",
    map_new=pm.changes.MapSource(new, "my build"),
    map_old=pm.changes.MapSource(old, "v0.9.1"),
)
print(path)  # changes/report.html, open it in a browser
```

This writes the folder `changes` with the layout above. `report.html` is the same page as in the docs and opens in any browser without MkDocs. The map embeds the local data. Without `map_new`, the report has no map. The page needs internet for plotly.js and the map tiles.

In a notebook, `pm.changes.figures()` returns the figures as plotly figures, keyed by section and table:

```python
comparison = pm.changes.compare(old, new)
pm.changes.figures(comparison)["overview/total"].show()
```

For a map alone, e.g. of two local builds, use `write_map`. A version is a dataset (embedded) or the URL of a CSV file (fetched in the browser on a button click):

```python
from powerplantmatching.changes import MapSource, write_map
from powerplantmatching.collection import matched_data_url

write_map("map", MapSource(build_b, "build B"), MapSource(build_a, "build A"))
write_map(
    "map",
    MapSource(new, "local build"),
    MapSource(matched_data_url("v0.9.1"), "v0.9.1"),
)
```

## Add a release

The release workflow writes the page of a new release to `docs/data-changes/<tag>/` and commits it with the dataset. The docs add the page to the navigation, to the list below and as a button to the [release notes](../release-notes.md). Nothing to edit.

To write a page by hand, e.g. for a missed release or to regenerate one, run from the repository root and commit the folder:

```python
import powerplantmatching as pm

pm.changes.write_release_report("v0.9.1", "v0.9.2")
```

## Releases

<!-- releases -->

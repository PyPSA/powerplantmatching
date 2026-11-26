<!--
SPDX-FileCopyrightText: 2025 Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>

SPDX-License-Identifier: MIT
-->

# Data changes

For each release we compare the newly released dataset in `powerplants.csv` with the previous release.
Select on the left the changes between releases you want to view.
The figures show the old and the new values, as well as changes.

Each page provides a short overview, lists the changes by country and fuel type, and also contains some quality indicators at the end to report on the completeness of entries and possible issues with data quality.

## Run locally

The same report can also be built locally for comparing e.g. a custom configuration with a release, or two local builds with each other.

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

This writes the folder `changes` with the CSV tables, the figures as HTML files, `map.html` and `report.html`.
`report.html` is the same page as in the docs and can be directly opened in a browser (the page needs internet for it to work).

In a Jupyter notebook, `pm.changes.figures()` returns the figures as plotly figures, keyed by section and table:

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

## Run for a release (maintainers only)

When a release is triggered in the repository, it only commits the necessary data to the repository (the CSV tables and `release.json` with the old and the new release and the reference year of the status table).
When the docs are built, MkDocs generates the necessary plots and markdown files via a hook in `docs/hooks.py` and also adds a button to the release notes.
The release pipeline only writes the files if `powerplants.csv` differs between the new and the previous release; non-data-changing releases have no page with data changes.

To write a page by hand, e.g. for a missed release or to regenerate one, run from the repository root and commit the folder (CSV tables and `release.json`).
v0.5.0 is the first release with a dataset, so the oldest release to compare with:

```python
import powerplantmatching as pm

pm.changes.write_release_report("v0.9.1", "v0.9.2")
```

For a past release pass its year as `year=` (the workflow uses the current year), and skip a release whose `powerplants.csv` is unchanged, as the workflow does.
There's also a `pytest` that writes the changes for the previous releases.

## Releases

<!-- releases -->

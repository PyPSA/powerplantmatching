<!--
SPDX-FileCopyrightText: 2025 Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>

SPDX-License-Identifier: MIT
-->

# Data changes

For each release with a changed `powerplants.csv`, a page compares the dataset with the previous release.
Select a release on the left.
Each page shows the total change, the change by country, fuel type, source, status, technology and set, a map of both versions and data-quality checks.

## Run locally

Compare any two datasets, e.g. your own build with a release, with [`powerplantmatching.changes`](../api-changes.md):

```python
import powerplantmatching as pm

old = pm.powerplants(from_url="v0.9.1")
new = pm.powerplants()
table = pm.changes.compare(old, new)
pm.changes.figures(table)["country"].show()
```

`table` holds the capacity, entries and complete entries of both versions per dimension (e.g. `Country`), group (e.g. `Germany`) and fuel type.

## Release pages (maintainers)

The release workflow writes the folder `docs/data-changes/<tag>` if `powerplants.csv` changed: the table `changes.csv` and `release.json` with the old and new tag and the reference year of the status.
At docs build time, the hook `docs/hooks.py` renders the page of each folder and adds it to the navigation.
To write a folder by hand, e.g. for a missed release, run from the repository root and commit the folder:

```python
pm.changes.write_release("v0.9.1", "v0.9.2")
```

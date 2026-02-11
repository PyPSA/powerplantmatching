<!--
SPDX-FileCopyrightText: 2025 Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>

SPDX-License-Identifier: MIT
-->

# Release Notes

<!-- ## Upcoming Version -->

## [v0.8.1](https:://github.com/PyPSA/powerplantmatching/releases/tag/v0.8.1) (11th February 2026)

* Updated Global Energy Monitor / Transition Zero datasets to the latest versions (February 2026) for wind and solar power plants.

## [v0.8.0](https:://github.com/PyPSA/powerplantmatching/releases/tag/v0.8.0) (13th January 2026)

* Fix 403 Forbidden responses for Zenodo downloads by sending `powerplantmatching/{base_version}` as user agent. ([PR #276](https://github.com/PyPSA/powerplantmatching/pull/276))
* Added [OpenStreetMap (OSM)](https://github.com/open-energy-transition/osm-powerplants) power plant data as optional data source via `pm.data.OSM()`. Data is provided by the external `osm-powerplants` package. ([PR #272](https://github.com/PyPSA/powerplantmatching/pull/272))
* ENTSOE API retrieval now falls back to URL download if API call fails. ([PR #278](https://github.com/PyPSA/powerplantmatching/pull/278))
* Update Marktstammdatenregister data for Germany from [open-MaStR (February 25, 2025)](https://zenodo.org/records/14783581).
* Drop support for Python 3.9, add support for Python 3.13. Minimum required Python version is now 3.10.
* Restructure documentation and move to use `mkdocs` for a nicer user experience.
* Added [GeoNuclearData](github.com/cristianst85/GeoNuclearData) dataset as `pm.data.GND()`.
* Added [European Energy Storage Inventory](https://ses.jrc.ec.europa.eu/storage-inventory-maps) dataset as `pm.data.EESI()`.
* Added [GloHydroRES](https://zenodo.org/records/14526360) dataset as `pm.data.GHR()`.
* Updated ENTSOE, BEYONDCOAL, JRC, IRENASTAT and the Global Energy Monitor datasets to the latest versions.
* Fix in `pm.data.MASTR()` the distinction of hydro technologies and between offshore and onshore wind. Also read in storage technologies.
* Improved recognition of CHP power plants.
* In Global Energy Monitor datasets, also read entries below capacity threshold.
* In `pm.data.GCPT()`, add estimate for coal plant efficiency.
* Include mothballed gas, oil and coal power plants.
* Initially, include unit/block name in power plant name before matching.
* Added option to retain blocks for subsets of fuel types (e.g. `clean_name: fueltypes_with_blocks: ['Nuclear']`).
* For fully included datasets, add option to only aggregate units included in the matching process (e.g. `aggregate_only_matching_sources: ['MASTR']`).
* Added option for multiprocessing when aggregating units of non-matched power plants (e.g. `threads_extend_by_non_matched: 16`).
* Updating matching logic configuration.
* Update GBPT importer to support newer version of the database (from V3 on without sheet "Below Threshold").
* Corrects GPD file name in `config.yaml`.
* Sets `parallel_duke_processes` to false (instead of 16) to make powerplantmatching executable out-of-the-box also for Windows systems.
* Updates path to `powerplants.png` in README.
* Fixes typo in docstring of `gather_fueltype_info()` (`cleaning.py`) and `MASTR()` (`data.py`).

## [v0.7.1](https://github.com/PyPSA/powerplantmatching/releases/tag/v0.7.1) (30th January 2024)

### Bug fixes

* Patch for a bug in matching caused by faulty names for BNA hydro powerplants in the OPSD_EU input dataset. ([PR #217](https://github.com/PyPSA/powerplantmatching/pull/217))
* Minor manual correction for CCGT powerplant. ([PR #221](https://github.com/PyPSA/powerplantmatching/pull/221))

## [v0.7.0](https://github.com/PyPSA/powerplantmatching/releases/tag/v0.7.0) (23rd January 2025)

### Features

* Add "Marktstammdatenregister" (MaStR) data source for Germany ([PR #165](https://github.com/PyPSA/powerplantmatching/pull/165)).

## [v0.6.1](https://github.com/PyPSA/powerplantmatching/releases/tag/v0.6.1) (15th January 2025)

### Bug fixes

* Remove duplicate conventional power plants coming from different OPSD input files ([PR #213](https://github.com/PyPSA/powerplantmatching/pull/213)).

## [v0.6.0](https://github.com/PyPSA/powerplantmatching/releases/tag/v0.6.0) (18th September 2024)

### Features

* Add support for new power plant types: biomass and geothermal. ([PR #200](https://github.com/PyPSA/powerplantmatching/pull/200))
* Include additional parameters for hydro power plants. ([PR #205](https://github.com/PyPSA/powerplantmatching/pull/205))

### Bug fixes

* Fix issue with negative capacity values for some plants. ([PR #207](https://github.com/PyPSA/powerplantmatching/pull/207))
* Corrected data for onshore and offshore wind farms. ([PR #209](https://github.com/PyPSA/powerplantmatching/pull/209))

## [v0.5.0](https://github.com/PyPSA/powerplantmatching/releases/tag/v0.5.0) (10th August 2024)

### Features

* Introduce new matching algorithm for improved accuracy. ([PR #180](https://github.com/PyPSA/powerplantmatching/pull/180))
* Allow user-defined priority for input data sources. ([PR #185](https://github.com/PyPSA/powerplantmatching/pull/185))

### Bug fixes

* Resolve issue with duplicate entries in the final output. ([PR #190](https://github.com/PyPSA/powerplantmatching/pull/190))
* Fix incorrect mapping of some fuel types. ([PR #195](https://github.com/PyPSA/powerplantmatching/pull/195))

## [v0.4.0](https://github.com/PyPSA/powerplantmatching/releases/tag/v0.4.0) (5th June 2024)

### Features

* Enhanced support for renewable energy sources. ([PR #150](https://github.com/PyPSA/powerplantmatching/pull/150))
* Added new geographical regions for plant matching. ([PR #155](https://github.com/PyPSA/powerplantmatching/pull/155))

### Bug fixes

* Fixed critical bug causing crashes with certain input data. ([PR #160](https://github.com/PyPSA/powerplantmatching/pull/160))
* Minor documentation fixes and improvements. ([PR #165](https://github.com/PyPSA/powerplantmatching/pull/165))

## [v0.3.0](https://github.com/PyPSA/powerplantmatching/releases/tag/v0.3.0) (1st March 2024)

### Features

* Initial release of the Powerplantmatching tool.
* Support for matching power plants based on various input data sources.

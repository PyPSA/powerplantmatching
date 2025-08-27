<!--
SPDX-FileCopyrightText: 2025 Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>

SPDX-License-Identifier: MIT
-->

# Release Notes

## Upcoming Version

* Update Marktstammdatenregister data for Germany from [open-MaStR (February 25, 2025)](https://zenodo.org/records/14783581).
* Drop support for Python 3.9, add support for Python 3.13. Minimum required Python version is now 3.10.
* Restructure documentation and move to use `mkdocs` for a nicer user experience.

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

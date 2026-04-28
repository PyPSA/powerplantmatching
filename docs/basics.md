<!--
SPDX-FileCopyrightText: 2025 Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>

SPDX-License-Identifier: MIT
-->

# General Structure

The dataset combines the data of all the data sources listed in [Data-Sources](#data-sources) and provides the following information:

- **Power plant name** - claim of each database
- **Fueltype** - {Solid Biomass, Biogas, Geothermal, Hard Coal, Hydro, Lignite, Nuclear, Natural Gas, Oil, Solar, Wind, Other}
- **Technology** - {CCGT, OCGT, Steam Turbine, Combustion Engine, Run-Of-River, Pumped Storage, Reservoir}
- **Set** - {Power Plant (PP), Combined Heat and Power (CHP), Storages (Stores)}
- **Capacity** - [MW]
- **Duration** - Maximum state of charge capacity in terms of hours at full output capacity
- **Dam Information** - Dam volume [Mm^3] and Dam Height [m]
- **Geo-position** - Latitude, Longitude
- **Country** - EU-27 + CH + NO (+ UK) minus Cyprus and Malta
- **YearCommissioned** - Commmisioning year of the powerplant
- **RetroFit** - Year of last retrofit
- **projectID** - Immutable identifier of the power plant

All data files of the package will be stored in the folder given by `pm.core.package_config['data_dir']`

## Data Sources

- OPSD - [Open Power System Data](http://data.open-power-system-data.org/) publish their [data](http://data.open-power-system-data.org/conventional_power_plants/) under a free license
- GEO - [Global Energy Observatory](http://globalenergyobservatory.org/), the data is not directly available on the website, but can be obtained from an sqlite
- OSM - [OpenStreetMap Power Plants](https://github.com/open-energy-transition/osm-powerplants) provides global power plant data extracted from OpenStreetMap via the `osm-powerplants` package (optional, not included in default configuration). The dataset (~29,000 plants / ~2,900 GW across 173 countries) is regenerated monthly in the upstream repo; the URL in the default config is pinned to a specific commit so each PPM release serves a stable snapshot — bump the pinned SHA in `package_data/config.yaml` to pick up a refreshed dataset. A recommended overlay for activating OSM as a `fully_included_sources` entry in the 35 countries where the upstream evaluation found defensible unique contribution is published at [`osm-powerplants/evaluation/config.ppm_with_osm.yaml`](https://github.com/open-energy-transition/osm-powerplants/blob/main/evaluation/config.ppm_with_osm.yaml) — see [`evaluation/osm_global_report.md`](https://github.com/open-energy-transition/osm-powerplants/blob/main/evaluation/osm_global_report.md) for the methodology behind the country selection and the per-fueltype duplicate filter.

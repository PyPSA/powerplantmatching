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

Title: Global Power Plant Database
Description: A comprehensive, global, open source database of power plants
Version: 1.3.0
Release Date: 2021-06-02
URI: http://datasets.wri.org/dataset/globalpowerplantdatabase
Copyright: Copyright 2018-2021 World Resources Institute and Data Contributors
License: Creative Commons Attribution 4.0 International -- CC BY 4.0
Contact: powerexplorer@wri.org
Citation: Global Energy Observatory, Google, KTH Royal Institute of Technology in Stockholm, Enipedia, World Resources Institute. 2019. Global Power Plant Database. Published on Resource Watch and Google Earth Engine. http://resourcewatch.org/ https://earthengine.google.com/  


Abstract:

An affordable, reliable, and environmentally sustainable power sector is central to modern society.
Governments, utilities, and companies make decisions that both affect and depend on the power sector.
For example, if governments apply a carbon price to electricity generation, it changes how plants run and which plants are built over time.
On the other hand, each new plant affects the electricity generation mix, the reliability of the system, and system emissions.
Plants also have significant impact on climate change, through carbon dioxide (CO2) emissions; on water stress, through water withdrawal and consumption; and on air quality, through sulfur oxides (SOx), nitrogen oxides (NOx), and particulate matter (PM) emissions.

The Global Power Plant Database is an open-source open-access dataset of grid-scale (1 MW and greater) electricity generating facilities operating across the world.

The Database currently contains nearly 35000 power plants in 167 countries, representing about 72% of the world's capacity.
Entries are at the facility level only, generally defined as a single transmission grid connection point.
Generation unit-level information is not currently available. 
The methodology for the dataset creation is given in the World Resources Institute publication "A Global Database of Power Plants" [0].
Associated code for the creation of the dataset can be found on GitHub [1].
See also the technical note published in early 2020 on an improved methodology to estimate annual generation [2].

To stay updated with news about the project and future database releases, please sign up for our newsletter for the release announcement [3].


[0] www.wri.org/publication/global-power-plant-database
[1] https://github.com/wri/global-power-plant-database
[2] https://www.wri.org/publication/estimating-power-plant-generation-global-power-plant-database
[3] https://goo.gl/ivTvkd


Package Description:

The Global Power Plant Database is available in an archived directory of 5 files.
	- `global_power_plant_database.csv`: The core dataset of the world's power plants, released as a comma-delimited plain text file.
	- `RELEASE_NOTES.txt`: Information on the changes and history between database versions.
	- `README.txt`: This file, the metadata document.
	- `A_Global_Database_of_Power_Plants.pdf`: The WRI Technical Note describing the database development [0].
	- `Estimating_Power_Plant_Generation_in_the_Global_Power_Plant_Database.pdf`: The WRI Technical Note describing the generation estimation methodology [2].


File Description [global_power_plant_database.csv]

This file is a CSV with the following conventions:
	- file encoding: UTF-8
	- field delimiter: , (comma; 0x2C)
	- line terminator: \r\n (CRLF) (carriage-return line-feed; 0x0D 0x0A)
	- header line: true
	- field quoting: Only in instances where a double-quote (0x22) is contained within a text field; in which case the double-quote is escaped by a double-quote as in RFC 4180 2.7 [4].


[4] https://tools.ietf.org/html/rfc4180#section-2


Fields:

	- `country` (text): 3 character country code corresponding to the ISO 3166-1 alpha-3 specification [5]
	- `country_long` (text): longer form of the country designation
	- `name` (text): name or title of the power plant, generally in Romanized form
	- `gppd_idnr` (text): 10 or 12 character identifier for the power plant
	- `capacity_mw` (number): electrical generating capacity in megawatts
	- `latitude` (number): geolocation in decimal degrees; WGS84 (EPSG:4326)
	- `longitude` (number): geolocation in decimal degrees; WGS84 (EPSG:4326)
	- `primary_fuel` (text): energy source used in primary electricity generation or export
	- `other_fuel1` (text): energy source used in electricity generation or export
	- `other_fuel2` (text): energy source used in electricity generation or export
	- `other_fuel3` (text): energy source used in electricity generation or export
	- `commissioning_year` (number): year of plant operation, weighted by unit-capacity when data is available
	- `owner` (text): majority shareholder of the power plant, generally in Romanized form
	- `source` (text): entity reporting the data; could be an organization, report, or document, generally in Romanized form
	- `url` (text): web document corresponding to the `source` field
	- `geolocation_source` (text): attribution for geolocation information
	- `wepp_id` (text): a reference to a unique plant identifier in the widely-used PLATTS-WEPP database.
	- `year_of_capacity_data` (number): year the capacity information was reported
	- `generation_gwh_2013` (number): electricity generation in gigawatt-hours reported for the year 2013
	- `generation_gwh_2014` (number): electricity generation in gigawatt-hours reported for the year 2014
	- `generation_gwh_2015` (number): electricity generation in gigawatt-hours reported for the year 2015
	- `generation_gwh_2016` (number): electricity generation in gigawatt-hours reported for the year 2016
	- `generation_gwh_2017` (number): electricity generation in gigawatt-hours reported for the year 2017
	- `generation_gwh_2018` (number): electricity generation in gigawatt-hours reported for the year 2018
	- `generation_gwh_2019` (number): electricity generation in gigawatt-hours reported for the year 2019
	- `generation_data_source` (text): attribution for the reported generation information
	- `estimated_generation_gwh_2013` (number): estimated electricity generation in gigawatt-hours for the year 2013 (see [2])
	- `estimated_generation_gwh_2014` (number): estimated electricity generation in gigawatt-hours for the year 2014 (see [2])
	- `estimated_generation_gwh_2015` (number): estimated electricity generation in gigawatt-hours for the year 2015 (see [2])
	- `estimated_generation_gwh_2016` (number): estimated electricity generation in gigawatt-hours for the year 2016 (see [2])
	- `estimated_generation_gwh_2017` (number): estimated electricity generation in gigawatt-hours for the year 2017 (see [2])
	- `estimated_generation_note_2013` (text): label of the model/method used to estimate generation for the year 2013 (see section on this field below)
	- `estimated_generation_note_2014` (text): label of the model/method used to estimate generation for the year 2014 (see section on this field below)
	- `estimated_generation_note_2015` (text): label of the model/method used to estimate generation for the year 2015 (see section on this field below)
	- `estimated_generation_note_2016` (text): label of the model/method used to estimate generation for the year 2016 (see section on this field below)
	- `estimated_generation_note_2017` (text): label of the model/method used to estimate generation for the year 2017 (see section on this field below)

[5] https://www.iso.org/iso-3166-country-codes.html


Generation Estimates:
Generation is available in two forms in the database: reported generation and estimated generation.
Estimation of annual generation is performed by separate models/methods depending on the attributes of the plant, primarily the fuel type, but also whether certain fields contain values or are unknown.
The fields `estimated_generation_note_YYYY` exist to communicate the estimation method that is used for each plant, for each year.
The following are the potential values for these note fields:
	- SOLAR-V1: a model for solar plants which have commissioning year available
	- SOLAR-V1-NO-AGE: a model for solar plants which do not have commissioning year available
	- WIND-V1: a model for wind plants
	- HYDRO-V1: a model for hydro plants
	- CAPACITY-FACTOR-V1: estimated value is based on externally published average capacity factors for the specific country & fuel combination; this is the “baseline” model described in [2] and follows the intention of estimated generation as described in [0]. This model is only permissible for year 2017.
	- NO-ESTIMATION: there is no estimation due to (1) the estimation year preceding the plant commissioning date; (2) insufficient availability of a model for the year in question; (3) selective non-application of CAPACITY-FACTOR-V1 in year 2017 (see caveat below)
All values containing V1 in the label are associated with the models described in publication [2].


Caveats:

`primary_fuel` is the fuel that has been identified to provide the largest portion of generated electricity for the plant or has been identified as the primary fuel by the data source.
For power plants that have data in multiple `other_fuel` fields, the ordering of the fuels should not be taken to indicate any priority or preference of the fuel for operating the power plant or generating units.
Though the `other_fuel` columns in the database are numbered sequentially from 1, the ordering is insignificant.

Generation is provided at the year scale for the years 2013-2019.
The reported generation values may correspond to a calendar year or a fiscal/regulatory year; no distinction is provided in the database.

The generation estimation note CAPACITY-FACTOR-V1 is only available for year 2017.
This model is not available for rows with `primary_fuel` values of: Biomass, Cogeneration, Petcoke, Storage, Wave and Tidal.
This model is selectively not applied to some country-fuel combinations for which estimations of capacity factors were greater than 90%. 

Rows with `estimated_generation_note_2017` of NO-ESTIMATION account for ~5% of all rows (~1800 of ~35000) and ~1.3% of total database capacity (~77 GW of ~5700 GW). 


Updates:

Contributions, suggestions, and corrections to the database are highly encouraged.
The proper channels for contributions are through opening a GitHub issue for the Global Power Plant Database [1] or by email [6].

Though updates have been infrequent, there is the potential that this is the last major update to this database.
The authors, advisors, and contributors hope to find continued funding and sustainable mechanisms for development of this product in the future.


[6] powerexplorer@wri.org




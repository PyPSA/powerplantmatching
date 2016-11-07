# powerplantmatching
A set of tools for cleaning, standardising and combining multiple
power plant databases.

This package helps with simplifying the data collection of power
plants. Information on power plants, particularly European ones is
scattered over a couple of different projects and databases that are
based on different standards. Thus, we firstly provide functions to
vertically clean databases and convert them into one coherent
standard, that does not distinguish between aggregates the units of a
power plant. Secondly, we provide functions to horizontally merge
different databases in order to check their consistency and improve
the reliability.


## Usage 

If you are just interested in the power plant data, we provide 
our actual match between five databases [here](../blob/master/data/Matched_Carma_Fias_Geo_Opsd_Wri.csv)
This set combines the data of all our data sources (see Data-Sources) 
giving the following information:

- **Power plant name** - claim of each database
- **Fueltype** - Hydro, Wind, Solar, Nuclear, Natural Gas, Oil, Coal, Other
- **Classification**, e.g. Run-of-River (Hydro), Reservoir (Hydro), CCGT (Coal) etc.
- **Capacity**
- **Geoposition** - Lattitude, Longitude
- **Country** - EU-27 countries and UK


We roughly keep the names given by the different databases. The other 
quantities are composed from the different claims of the matched
databases: 
Capacity and Geoposition are averaged through different claims of each matched
database. In case of differing claims of Classification, the Classifications 
are set in a row.
In case for different claims for Fueltype and Country, we keep the most 
frequent claimed one. 

If you are, for scientific use, further interested in a modified database, 
we provide an adapted which nearly covers all capacities totals stated by the ENTSOe
statistics. This is done by including powerplants that were not matched
but come out of a reliable source, e.g. the GEO data. Furthermore, a
learning algorithm was used to specify information about missing 
Hydro classification (Run-of-River, Pumped Storage and Reservoir). 
Additionally, we provide a feature for artificial hydro power plants, which can
be used as dummies on order to fulfil all country totals. 
This, database is available using the python command 
_powerplant\_collection.Matched\_dataset()_. 


## Combining Data From Different Sources

Whereas single databases as the CARMA or the GEO database provide 
non standardized and lacking information, the data sets can complement 
each other and check their reliability. Therefore, we combine 
five different databases (see below) and take only matched power plants.


![alt tag](https://cloud.githubusercontent.com/assets/19226431/20011654/a683952c-a2ac-11e6-8ce8-8e4982fb18d1.jpg)

...


## Data-Sources: 

- OPSD - [Open Power System Data](http://data.open-power-system-data.org/), provide their data on 
	[here](http://data.open-power-system-data.org/conventional_power_plants/)

- GEO - [Global Energy Observatory](http://globalenergyobservatory.org/docs/HelpGeoPower.php#), the 
	data is not directly available on the website, but can be parsed using [this sqlite scraper](https://morph.io/coroa/global_energy_observatory_power_plants)

- WRI - [World Resource Institute](http://www.wri.org), provide their data open source 
	on their [repository](https://github.com/Arjay7891/WRI-Powerplant/blob/master/output_database/powerwatch2_data.csv)

- CARMA - [Carbon Monitoring for Action](http://carma.org/plant) 

- FIAS - [Frankfurt Institute for Advanced Studies](https://fias.uni-frankfurt.de/de/) 

- ENTSOe - [European ....](), annually provides statistics about aggregated power plant 
	capacities which is available [here](https://www.entsoe.eu/db-query/miscellaneous/net-generating-capacity)
	Their data can be used as a validation reference. We further use their [annual energy
	generation report]() in order to adjust hydro power plant classification2  

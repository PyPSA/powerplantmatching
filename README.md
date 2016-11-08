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


## Prefabricated Data 

If you are only interested in the power plant data, we provide 
our actual match between five databases in a [csv-file](../blob/master/data/Matched_Carma_Fias_Geo_Opsd_Wri.csv)
This set combines the data of all our data sources (see Data-Sources) 
giving the following information:

- **Power plant name** 		- claim of each database
- **Fueltype** 			- Hydro, Wind, Solar, Nuclear, Natural Gas, Oil, Coal, Other
- **Classification**		- Run-of-River, Reservoir, CCGT etc.
- **Capacity**			- \[MW\]
- **Geoposition**		- Lattitude, Longitude
- **Country** 			- EU-27 countries and UK


We roughly keep the names statet by the different databases. The other 
quantities are composed from the different claims of the matched
databases: 
Geoposition is averaged whereas for the Capacity we keep the maximum value of different claims, 
since some datasets do not include all units of powerplants.
In case of differing claims for the classification, all classification claims 
are set in a row. In case for different claims for the fueltype, we keep the most 
frequent one. The claims for the country cannot differ, otherwise they don't match.

![alt tag](https://cloud.githubusercontent.com/assets/19226431/20011654/a683952c-a2ac-11e6-8ce8-8e4982fb18d1.jpg)

### Adjusted Dataset
If you are, for scientific use, further interested in a modified database, 
we provide an adapted dataset which nearly covers all capacities totals stated by the ENTSOe
statistics (except for Wind and Solar). 
![alt tag](https://cloud.githubusercontent.com/assets/19226431/20011650/a654e858-a2ac-11e6-93a2-2ed0e938f642.jpg)
This was done by also including powerplants that were not matched
but come out of a reliable source, e.g. the GEO data. Furthermore, a
learning algorithm was used to specify information about missing 
Hydro classification (Run-of-River, Pumped Storage and Reservoir). 

![alt tag](https://cloud.githubusercontent.com/assets/19226431/20104077/e0534900-a5cc-11e6-8d3f-002756cc8110.jpg)

Additionally, we provide a feature for artificial hydro power plants, which can
be used as dummies on order to fulfil all country totals. The database is available using the python command 
```python
powerplant_collection.Matched_dataset() 
```
or 
```python
powerplant_collection.Matched_dataset(artificials=False) 
```
if you do not want to include artificial powerplants.

## Combining Data From Different Sources (horizontal matching)

Whereas single databases as the CARMA or the GEO database provide 
non standardized and lacking information, the data sets can complement 
each other and check their reliability. Therefore, we combine 
five different databases (see below) and take only matched power plants.



The matching itself is done by [DUKE](https://github.com/larsga/Duke), a java application specialised for
deduplicating and linking data. It provides many built-in comparators such as numerical, string or geoposition comparators.
The engine does a detailed comparison for each single argument (power plant name, fueltype etc.) using adjusted 
comparators and weights. When comparing two power plants, Duke will set a matching score for each argument and 
according to the given weights, combine those to one matching score. This global score indicates the chance 
that the two entries relate to the same power plant. Exceeding a given threshold, the power plant entries of the two  
datasets are linked and merged into one dataset.

We specify this through the following example:

Consider the following two datasets

Dataset 1: 

|    | Name                | Fueltype   |   Classification | Country        |   Capacity |     lat |        lon |   File |
|---:|:--------------------|:-----------|-----------------:|:---------------|-----------:|--------:|-----------:|-------:|
|  0 | Aarberg             | Hydro      |              nan | Switzerland    |     14.609 | 47.0444 |  7.27578   |    nan |
|  1 | Abbey mills pumping | Oil        |              nan | United Kingdom |      6.4   | 51.687  | -0.0042057 |    nan |
|  2 | Abertay             | Other      |              nan | United Kingdom |      8     | 57.1785 | -2.18679   |    nan |
|  3 | Aberthaw            | Coal       |              nan | United Kingdom |   1552.5   | 51.3875 | -3.40675   |    nan |
|  4 | Ablass              | Wind       |              nan | Germany        |     18     | 51.2333 | 12.95      |    nan |
|  5 | Abono               | Coal       |              nan | Spain          |    921.7   | 43.5588 | -5.72287   |    nan |

and 

Dataset 2:

|    | Name              | Fueltype    | Classification   | Country        |   Capacity |     lat |     lon |   File |
|---:|:------------------|:------------|:-----------------|:---------------|-----------:|--------:|--------:|-------:|
|  0 | Aarberg           | Hydro       | nan              | Switzerland    |       15.5 | 47.0378 |  7.272  |    nan |
|  1 | Aberthaw          | Coal        | Thermal          | United Kingdom |     1500   | 51.3873 | -3.4049 |    nan |
|  2 | Abono             | Coal        | Thermal          | Spain          |      921.7 | 43.5528 | -5.7231 |    nan |
|  3 | Abwinden asten    | Hydro       | nan              | Austria        |      168   | 48.248  | 14.4305 |    nan |
|  4 | Aceca             | Oil         | CHP              | Spain          |      629   | 39.941  | -3.8569 |    nan |
|  5 | Aceca fenosa      | Natural Gas | CCGT             | Spain          |      400   | 39.9427 | -3.8548 |    nan |

Apparently the entries 0, 3 and 5 of Dataset 1 relate to the same power plants as the entries 0,1 and 2 of Dataset 2. 
Applying the matching algorithm to the two datasets, we obtain the following set:

|    | Dataset 1   | Dataset 2   | Country        | Fueltype   | Classification   |   Capacity |     lat |      lon |   File |
|---:|:------------|:------------|:---------------|:-----------|:-----------------|-----------:|--------:|---------:|-------:|
|  0 | Aarberg     | Aarberg     | Switzerland    | Hydro      | nan              |       15.5 | 47.0411 |  7.27389 |    nan |
|  1 | Aberthaw    | Aberthaw    | United Kingdom | Coal       | Thermal          |     1552.5 | 51.3874 | -3.40583 |    nan |
|  2 | Abono       | Abono       | Spain          | Coal       | Thermal          |      921.7 | 43.5558 | -5.72299 |    nan |

Note, that the names are kept from the different sources, whereas the rest is composed of the claims of the two datasets. 
You can of course, obtain the matched dataset in a decomposed frame or create your one composition of teh different claims. 


## Vertical Matching





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

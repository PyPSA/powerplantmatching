# powerplantmatching
A toolset for cleaning, standardizing and combining multiple power
plant databases. 

This package provides ready-to-use power plant data for the European power system.
Starting from openly available power plant datasets, the package cleans, standardizes 
and merges the input data to create a new combining dataset, which includes all the important information.
The major advantage of this procedure is that the resulting dataset 
can be easily updated as soon as new input datasets are released.

![Map of power plants in Europe](https://user-images.githubusercontent.com/19226431/31497088-1ed25900-af5e-11e7-8da7-9ff76fe18c3e.png)

powerplantmatching was initially developed by the
[Renewable Energy Group](https://fias.uni-frankfurt.de/physics/schramm/complex-renewable-energy-networks/)
at [FIAS](https://fias.uni-frankfurt.de/) to build power plant data
inputs to [PyPSA](http://www.pypsa.org/)-based models for carrying
out simulations for the [CoNDyNet project](http://condynet.de/),
financed by the
[German Federal Ministry for Education and Research (BMBF)](https://www.bmbf.de/en/)
as part of the
[Stromnetze Research Initiative](http://forschung-stromnetze.info/projekte/grundlagen-und-konzepte-fuer-effiziente-dezentrale-stromnetze/). 



## What it can do

- clean and standardize power plant data sets
- aggregate power plants units which belong to the same plant 
- compare and combine different data sets
- create lookups and give statistical insight to power plant goodness
- provide cleaned data from different sources 
- choose between gros/net capacity
- provide an already merged data set of six different data-sources 



## Installation

1. Make sure that [git lfs](https://git-lfs.github.com/) is installed, in case of doubt just run `git lfs install` 
2. Copy or clone the repository to your preferred directory 
3. Install the package via 'pip install -e /path/to/powerplantmatching' 
4. Copy config_example.yaml to config.yaml.

Optional but recommended:

5. Download the [ESE dataset](https://goo.gl/gVMwKJ). For integrating the data into powerplantmatching, the path of the downloaded file has to be added to the config.yaml file with the keyword 'ese_path' (default is set to 'Downloads/projects.xls').
6. Add your ENTSOE security token to the config file. The token can be obtained by following section 2 of the [RESTful API documentation](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_authentication_and_authorisation) of the ENTSOE-E Transparency platform.
    



## Processed Data 

If you are only interested in the power plant data, we provide our
current merged dataset as a
[csv-file](../master/data/out/Matched_CARMA_ENTSOE_GEO_OPSD_WRI_reduced.csv). This
set combines the data of all the data sources listed in
[Data-Sources](#Data-Sources) and provides the following information:

- **Power plant name** 		- claim of each database
- **Fueltype** 			- {Bioenergy, Geothermal, Hard Coal, Hydro, Lignite, Nuclear, Natural Gas, Oil, Solar, Wind, Other}
- **Technology**		- {CCGT, OCGT, Steam Turbine, Combustion Engine, Run-Of-River, Pumped Storage, Reservoir}
- **Set**			- {Power Plant (PP), Combined Heat and Power (CHP)}
- **Capacity**			- \[MW\]
- **Geo-position**		- Latitude, Longitude
- **Country** 			- EU-27 + CH + NO (+ UK) minus Cyprus and Malta
- **YearCommissioned**		- Commmisioning year of the powerplant
- **File**			- Source file of the data entry
- **projectID**			- Immutable identifier of the power plant


The following picture compares the total capacities per fuel type
between the different data sources and our merged dataset.

![Total capacities per fuel type for the different data sources and the merged dataset.](https://user-images.githubusercontent.com/19226431/31497124-45ea4b10-af5e-11e7-8153-7046f17ca05f.png)



## Data-Sources: 

- OPSD - [Open Power System Data](http://data.open-power-system-data.org/) publish their [data](http://data.open-power-system-data.org/conventional_power_plants/) under a free license
- GEO - [Global Energy Observatory](http://globalenergyobservatory.org/), the data is not directly available on the website, but can be obtained from an [sqlite scraper](https://morph.io/coroa/global_energy_observatory_power_plants)
- WRI - [World Resource Institute](http://www.wri.org) provide their data under a free license on their [institute website](http://datasets.wri.org/dataset/globalpowerplantdatabase)
- CARMA - [Carbon Monitoring for Action](http://carma.org/plant) 
- ESE - [Energy Storage Exchange](http://www.energystorageexchange.org/) provide a database for storage units. Especially the hydro storage data is of big use for a combining power plant database. Since the data is not free, it is optional and can be [downloaded separately](http://www.energystorageexchange.org/projects/advanced_search?utf8=%E2%9C%93&name_eq=&country_sort_eq%5B%5D=Austria&country_sort_eq%5B%5D=Belgium&country_sort_eq%5B%5D=Bulgaria&country_sort_eq%5B%5D=Croatia&country_sort_eq%5B%5D=Czeck+Republic&country_sort_eq%5B%5D=Denmark&country_sort_eq%5B%5D=Estonia&country_sort_eq%5B%5D=Finland&country_sort_eq%5B%5D=France&country_sort_eq%5B%5D=Germany&country_sort_eq%5B%5D=Greece&country_sort_eq%5B%5D=Hungary&country_sort_eq%5B%5D=Ireland&country_sort_eq%5B%5D=Italy&country_sort_eq%5B%5D=Latvia&country_sort_eq%5B%5D=Lithuania&country_sort_eq%5B%5D=Luxembourg&country_sort_eq%5B%5D=Netherlands&country_sort_eq%5B%5D=Norway&country_sort_eq%5B%5D=Poland&country_sort_eq%5B%5D=Portugal&country_sort_eq%5B%5D=Romainia&country_sort_eq%5B%5D=Slovakia&country_sort_eq%5B%5D=Slovenia&country_sort_eq%5B%5D=Spain&country_sort_eq%5B%5D=Sweden&country_sort_eq%5B%5D=Switzerland&country_sort_eq%5B%5D=United+Kingdom&size_kw_ll=&size_kw_ul=&kW=&size_kwh_ll=&size_kwh_ul=&kWh=&%5Bannouncement_on_ll%281i%29%5D=&%5Bannouncement_on_ll%282i%29%5D=&%5Bannouncement_on_ll%283i%29%5D=1&%5Bannouncement_on_ul%281i%29%5D=&%5Bannouncement_on_ul%282i%29%5D=&%5Bannouncement_on_ul%283i%29%5D=1&%5Bconstruction_on_ll%281i%29%5D=&%5Bconstruction_on_ll%282i%29%5D=&%5Bconstruction_on_ll%283i%29%5D=1&%5Bconstruction_on_ul%281i%29%5D=&%5Bconstruction_on_ul%282i%29%5D=&%5Bconstruction_on_ul%283i%29%5D=1&%5Bcommissioning_on_ll%281i%29%5D=&%5Bcommissioning_on_ll%282i%29%5D=&%5Bcommissioning_on_ll%283i%29%5D=1&%5Bcommissioning_on_ul%281i%29%5D=&%5Bcommissioning_on_ul%282i%29%5D=&%5Bcommissioning_on_ul%283i%29%5D=1&%5Bdecommissioning_on_ll%281i%29%5D=&%5Bdecommissioning_on_ll%282i%29%5D=&%5Bdecommissioning_on_ll%283i%29%5D=1&%5Bdecommissioning_on_ul%281i%29%5D=&%5Bdecommissioning_on_ul%282i%29%5D=&%5Bdecommissioning_on_ul%283i%29%5D=1&owner_in=&vendor_company=&electronics_provider=&utility=&om_contractor=&developer=&order_by=&sort_order=&search_page=&search_search=search).
- ENTSOe - [European Network of Transmission System Operators for Electricity](http://entsoe.eu/), annually provides statistics about aggregated power plant capacities which is available [here]() Their data can be used as a validation reference. We further use their [annual energy generation report from 2010](https://www.entsoe.eu/db-query/miscellaneous/net-generating-capacity) as an input for the hydro power plant classification.
- IRENA - [International Renewable Energy Agency](http://resourceirena.irena.org/gateway/dashboard/) open available statistics on power plant capacities.
- BNETZA - [Bundesnetzagentur](https://www.bundesnetzagentur.de/EN/Areas/Energy/Companies/SecurityOfSupply/GeneratingCapacity/PowerPlantList/PubliPowerPlantList_node.html) open available data source for Germany's power plants


The merged dataset is available in two versions: The [bigger dataset](../master/data/out/Matched_CARMA_ENTSOE_GEO_OPSD_WRI.csv)
links the entries of the matched power plants and lists all the related
properties given by the different data-sources. The [smaller merged dataset](../master/data/out/Matched_CARMA_ENTSOE_GEO_OPSD_WRI_reduced.csv) 
claims only the value of the most reliable data source being matched in the individual power plant data entry.
The considered reliability scores are:


|Dataset         |Reliabilty score  |
|:---------------|:---------------|
| BNETZA         | 5              |
| CARMA          | 1              |
| ENTSOE 	 | 4              |
| ESE            | 4              |
| GEO            | 3              |
| IWPDCY         | 3              |
| OPSD           | 5              |
| UBA            | 5              |
| WRI		 | 3              |


The toolset provides additional funcitons to easily manipulate your merged, e.g. you can 

- extend your data by non-matched power plant entries

- scale the power plant capacities in order to match country specific statistics about total power plant capacities

- extend your data by renewable power plants given by the [OPSD](https://data.open-power-system-data.org/renewable_power_plants/2018-03-08/)


The database is available using the python command 
```python
import powerplantmatching as pm
pm.collection.MATCHED_dataset() 
```
or 
```python
import powerplantmatching as pm
pm.collection.MATCHED_dataset(rescaled_hydros=True)
```
if you want to scale hydro power plants.


There is a (bit out of date) ![Documentation](https://github.com/FRESNA/powerplantmatching/files/1380529/PowerplantmatchingDoc.pdf) available, which (however) gives you some more extensive insight 
on the coding level.




## Module Structure

The package consists of ten modules. For creating a new dataset you
can make most use of the modules data, clean and match, which provide
you with function for data supply, vertical cleaning and horizontal
matching, respectively.

![Modular package structure](https://user-images.githubusercontent.com/19226431/31513014-2feef76e-af8d-11e7-9b4d-f1be929e2dba.png)

## Combining Data From Different Sources - Horizontal Matching

Whereas single databases as the CARMA or the GEO database provide non
standardized and incomplete information, the datasets can complement
each other and improve their reliability. The merged dataset combines
five different databases (see below) by only keeping powerplants which
appear in more than one source.

The matching process heavily relies on
[DUKE](https://github.com/larsga/Duke), a java application specialized
for deduplicating and linking data. It provides many built-in
comparators such as numerical, string or geoposition comparators.  The
engine does a detailed comparison for each single argument (power
plant name, fuel-type etc.) using adjusted comparators and weights.
From the individual scores for each column it computes a compound
score for the likeliness that the two powerplant records refer to the
same powerplant. If the score exceeds a given threshold, the two
records of the power plant are linked and merged into one data set.

Let's make that a bit more concrete by giving a quick
example. Consider the following two data sets

### Dataset 1: 

|    | Name                | Fueltype   |   Classification | Country        |   Capacity |     lat |        lon |   File |
|---:|:--------------------|:-----------|-----------------:|:---------------|-----------:|--------:|-----------:|-------:|
|  0 | Aarberg             | Hydro      |              nan | Switzerland    |     14.609 | 47.0444 |  7.27578   |    nan |
|  1 | Abbey mills pumping | Oil        |              nan | United Kingdom |      6.4   | 51.687  | -0.0042057 |    nan |
|  2 | Abertay             | Other      |              nan | United Kingdom |      8     | 57.1785 | -2.18679   |    nan |
|  3 | Aberthaw            | Coal       |              nan | United Kingdom |   1552.5   | 51.3875 | -3.40675   |    nan |
|  4 | Ablass              | Wind       |              nan | Germany        |     18     | 51.2333 | 12.95      |    nan |
|  5 | Abono               | Coal       |              nan | Spain          |    921.7   | 43.5588 | -5.72287   |    nan |

and 

### Dataset 2:

|    | Name              | Fueltype    | Classification   | Country        |   Capacity |     lat |     lon |   File |
|---:|:------------------|:------------|:-----------------|:---------------|-----------:|--------:|--------:|-------:|
|  0 | Aarberg           | Hydro       | nan              | Switzerland    |       15.5 | 47.0378 |  7.272  |    nan |
|  1 | Aberthaw          | Coal        | Thermal          | United Kingdom |     1500   | 51.3873 | -3.4049 |    nan |
|  2 | Abono             | Coal        | Thermal          | Spain          |      921.7 | 43.5528 | -5.7231 |    nan |
|  3 | Abwinden asten    | Hydro       | nan              | Austria        |      168   | 48.248  | 14.4305 |    nan |
|  4 | Aceca             | Oil         | CHP              | Spain          |      629   | 39.941  | -3.8569 |    nan |
|  5 | Aceca fenosa      | Natural Gas | CCGT             | Spain          |      400   | 39.9427 | -3.8548 |    nan |

Apparently the entries 0, 3 and 5 of Data set 1 relate to the same
power plants as the entries 0,1 and 2 of Data set 2.  Applying the
matching algorithm to the two data sets, we obtain the following set:

|    | Dataset 1   | Dataset 2   | Country        | Fueltype   | Classification   |   Capacity |     lat |      lon |   File |
|---:|:------------|:------------|:---------------|:-----------|:-----------------|-----------:|--------:|---------:|-------:|
|  0 | Aarberg     | Aarberg     | Switzerland    | Hydro      | nan              |       15.5 | 47.0411 |  7.27389 |    nan |
|  1 | Aberthaw    | Aberthaw    | United Kingdom | Coal       | Thermal          |     1552.5 | 51.3874 | -3.40583 |    nan |
|  2 | Abono       | Abono       | Spain          | Coal       | Thermal          |      921.7 | 43.5558 | -5.72299 |    nan |

Note, that the names from the different sources are kept for ease of
referencing, whereas the claims about the other plant parameters have
been reduced an aggregate value using the rules described in
[Processed data](#processed-data). The intermediary, unreduced dataset
with all the claims is, of course, also available to provide a basis
for your own reduction.

![Power plant coverage](https://cloud.githubusercontent.com/assets/19226431/20011650/a654e858-a2ac-11e6-93a2-2ed0e938f642.jpg)


## Vertical Cleaning

In order to compare and combine information from multiple databases, uni-
form standards must be guaranteed. That is, the datasets should be based on
the same set of arguments having consistent formats. With the module cleaning.py you can 
easily handle data alignment, that is, after renaming the basic columns of
an unprocessed dataset, one simply has to apply several provided functions.
Furthermore, you can aggregate power plant units from the same power
plant together. 


## Acknowledgements

The development of powerplantmatching was helped considerably by
in-depth discussions and exchanges of ideas and code with

- Fabian Gotzens from University Juelich
- Chris Davis from University of Groningen and
- Johannes Friedrich, Roman Hennig and Colin McCormick of the World Resources Institute

## Licence

powerplantmatching is released as free software under the
[GPLv3](http://www.gnu.org/licenses/gpl-3.0.en.html), see
[LICENSE](LICENSE).


# powerplantmatching
A set of tools for cleaning, standardizing and combining multiple
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

## What it can do

- clean and standardize power plant data sets
- merge power plant units to one power plant
- compare and combine different data sets
- create lookups and give statistical insight to power plant goodness
- provide cleaned data from different sources 
- provide an already merged data set of five different data-sources 


## Processed Data 

If you are only interested in the power plant data, we provide 
our actual match in a [csv-file](../master/data/Matched_Carma_Fias_Geo_Opsd_Wri.csv)
This set combines the data of all our data sources (see Data-Sources) 
giving the following information:

- **Power plant name** 		- claim of each database
- **Fueltype** 			- Hydro, Wind, Solar, Nuclear, Natural Gas, Oil, Coal, Other
- **Classification**		- Run-of-River, Reservoir, CCGT etc.
- **Capacity**			- \[MW\]
- **Geo-position**		- Latitude, Longitude
- **Country** 			- EU-27 countries and UK


There are two sets available. One bigger data set links the entries of the matched power plant and displays them in one data set.
The other data set reduces this bigger one by applying different reduction rules in order to keep as much information as possible but
having one dataset that can be worked with. The applied rules are the following  

|:Argument       |:Rule                                       |
|:---------------|:-------------------------------------------|
| Name           | Every name of the different databases      |
| Fueltype       | Most frequent claimed one                  |
| Classification | All _different_ Classification in a row    |
| Country        | Take the uniquely stated country           |
| Capacity       | Maximum of all claims                      |
| lat            | Average                                    |
| lon            | Average                                    |
| File           | All files in a row                         |

Note, that for the Capacity we keep the maximum value of different claims, 
since some data sets do not include all units of power plants.
The claims for the country cannot differ, otherwise the power plants cannot match.
The following picture show the fuel type totals of our different data sources and of their merged 
data set.

![alt tag](https://cloud.githubusercontent.com/assets/19226431/20011654/a683952c-a2ac-11e6-8ce8-8e4982fb18d1.jpg)

### Modified Data set
If you are, for scientific use, further interested in a modified database, 
we provide an adapted data set which nearly covers all capacities totals stated by the ENTSOe
statistics (except for Wind and Solar). 
![alt tag](https://cloud.githubusercontent.com/assets/19226431/20011650/a654e858-a2ac-11e6-93a2-2ed0e938f642.jpg)
Here, also non-matched power plants were included
assuming that they come out of a reliable source, mainly for the GEO data. Furthermore, a
learning algorithm was used to specify information about missing 
hydro classification (Run-of-River, Pumped Storage and Reservoir). 

Additionally, a feature for scaled hydro power plant capacities was included in order to fulfil all country totals. 
The database is available using the python command 
```python
from powerplantmatching import powerplant_collection as pc
pc.MATCHED_dataset() 
```
or 
```python
from powerplantmatching import powerplant_collection as pc
pc.MATCHED_dataset(scaled_hydros=True) 
```
if you do not want to include scaled hydros power plants.


##Modul Structure
![alt tag](https://cloud.githubusercontent.com/assets/19226431/20449087/984dc0c2-ade7-11e6-96e0-f1169c9a7fef.png)

The modul consists of different submoduls with different applications. If you want to create your own dataset, you will probably derive the most benefit from the 
submoduls powerplantmatching.data, powerplantmatching.clean and powerplantmatching.match, which include data supply, vertical cleaning and 
horizontal matching. 


## Combining Data From Different Sources - Horizontal Matching

Whereas single databases as the CARMA or the GEO database provide 
non standardized and lacking information, the data sets can complement 
each other and check their reliability. For our matched data set, we combine 
five different databases (see below) and take only matched power plants.



The matching itself is done by [DUKE](https://github.com/larsga/Duke), a java application specialized for
deduplicating and linking data. It provides many built-in comparators such as numerical, string or geoposition comparators.
The engine does a detailed comparison for each single argument (power plant name, fuel-type etc.) using adjusted 
comparators and weights. When comparing two power plants, Duke will compare every single argument of each power plant of one data set against 
every single argument of each power plant of the other data set.
For every argument it will set a matching score and, 
according to the given weights, combine those to one matching score. This global score indicates the chance 
that the two entries relate to the same power plant. Exceeding a given threshold, the power plant entries of the two  
data sets are linked and merged into one data set.

We specify this through the following example:

Consider the following two data sets

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

Apparently the entries 0, 3 and 5 of Data set 1 relate to the same power plants as the entries 0,1 and 2 of Data set 2. 
Applying the matching algorithm to the two data sets, we obtain the following set:

|    | Dataset 1   | Dataset 2   | Country        | Fueltype   | Classification   |   Capacity |     lat |      lon |   File |
|---:|:------------|:------------|:---------------|:-----------|:-----------------|-----------:|--------:|---------:|-------:|
|  0 | Aarberg     | Aarberg     | Switzerland    | Hydro      | nan              |       15.5 | 47.0411 |  7.27389 |    nan |
|  1 | Aberthaw    | Aberthaw    | United Kingdom | Coal       | Thermal          |     1552.5 | 51.3874 | -3.40583 |    nan |
|  2 | Abono       | Abono       | Spain          | Coal       | Thermal          |      921.7 | 43.5558 | -5.72299 |    nan |

Note, that the names are kept from the different sources, whereas the rest is composed of the claims of the two data sets. 
You can of course, obtain the matched data set in a decomposed frame or create your one composition of the different claims. 


## Vertical Cleaning





## Data-Sources: 

- OPSD - [Open Power System Data](http://data.open-power-system-data.org/), provide their data on 
	[here](http://data.open-power-system-data.org/conventional_power_plants/)

- GEO - [Global Energy Observatory](http://globalenergyobservatory.org/docs/HelpGeoPower.php#), the 
	data is not directly available on the website, but can be parsed using [this sqlite scraper](https://morph.io/coroa/global_energy_observatory_power_plants)

- WRI - [World Resource Institute](http://www.wri.org), provide their data open source 
	on their [repository](https://github.com/Arjay7891/WRI-Powerplant/blob/master/output_database/powerwatch2_data.csv)

- CARMA - [Carbon Monitoring for Action](http://carma.org/plant) 

- FIAS - [Frankfurt Institute for Advanced Studies](https://fias.uni-frankfurt.de/de/) 

- ESE - [Energy Storage Exchange]() provide a database for storage units. Especially the hydro storage data is of big use for a combining power plant database. Since the data is not 		open source, one have to download by oneself. [Here](http://www.energystorageexchange.org/projects/advanced_search?utf8=%E2%9C%93&name_eq=&country_sort_eq%5B%5D=Austria&country_sort_eq%5B%5D=Belgium&country_sort_eq%5B%5D=Bulgaria&country_sort_eq%5B%5D=Croatia&country_sort_eq%5B%5D=Czeck+Republic&country_sort_eq%5B%5D=Denmark&country_sort_eq%5B%5D=Estonia&country_sort_eq%5B%5D=Finland&country_sort_eq%5B%5D=France&country_sort_eq%5B%5D=Germany&country_sort_eq%5B%5D=Greece&country_sort_eq%5B%5D=Hungary&country_sort_eq%5B%5D=Ireland&country_sort_eq%5B%5D=Italy&country_sort_eq%5B%5D=Latvia&country_sort_eq%5B%5D=Lithuania&country_sort_eq%5B%5D=Luxembourg&country_sort_eq%5B%5D=Netherlands&country_sort_eq%5B%5D=Norway&country_sort_eq%5B%5D=Poland&country_sort_eq%5B%5D=Portugal&country_sort_eq%5B%5D=Romainia&country_sort_eq%5B%5D=Slovakia&country_sort_eq%5B%5D=Slovenia&country_sort_eq%5B%5D=Spain&country_sort_eq%5B%5D=Sweden&country_sort_eq%5B%5D=Switzerland&country_sort_eq%5B%5D=United+Kingdom&size_kw_ll=&size_kw_ul=&kW=&size_kwh_ll=&size_kwh_ul=&kWh=&%5Bannouncement_on_ll%281i%29%5D=&%5Bannouncement_on_ll%282i%29%5D=&%5Bannouncement_on_ll%283i%29%5D=1&%5Bannouncement_on_ul%281i%29%5D=&%5Bannouncement_on_ul%282i%29%5D=&%5Bannouncement_on_ul%283i%29%5D=1&%5Bconstruction_on_ll%281i%29%5D=&%5Bconstruction_on_ll%282i%29%5D=&%5Bconstruction_on_ll%283i%29%5D=1&%5Bconstruction_on_ul%281i%29%5D=&%5Bconstruction_on_ul%282i%29%5D=&%5Bconstruction_on_ul%283i%29%5D=1&%5Bcommissioning_on_ll%281i%29%5D=&%5Bcommissioning_on_ll%282i%29%5D=&%5Bcommissioning_on_ll%283i%29%5D=1&%5Bcommissioning_on_ul%281i%29%5D=&%5Bcommissioning_on_ul%282i%29%5D=&%5Bcommissioning_on_ul%283i%29%5D=1&%5Bdecommissioning_on_ll%281i%29%5D=&%5Bdecommissioning_on_ll%282i%29%5D=&%5Bdecommissioning_on_ll%283i%29%5D=1&%5Bdecommissioning_on_ul%281i%29%5D=&%5Bdecommissioning_on_ul%282i%29%5D=&%5Bdecommissioning_on_ul%283i%29%5D=1&owner_in=&vendor_company=&electronics_provider=&utility=&om_contractor=&developer=&order_by=&sort_order=&search_page=&search_search=search) is the link to the european data, which 	can easily be downloaded and integrated into the set of modul-provided databases 

- ENTSOe - [European Network of Transmission System Operators for Electricity](), annually provides statistics about aggregated power plant 
	capacities which is available [here]()
	Their data can be used as a validation reference. We further use their [annual energy
	generation report from 2010](https://www.entsoe.eu/db-query/miscellaneous/net-generating-capacity) in order to adjust hydro power plant classification

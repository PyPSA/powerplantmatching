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
our actual match between five databases here 
This set combines the data of all our data sources (see below) 
giving the following information:

- Name
- Fueltype
- Classification
- Capacity
- Geoposition
- Country


We roughly keep the names giving by the different databases.
However, the other quantities consist of combinations of 
all the matched databases, e.g. average of claimed capacities. 

If you further interested in a modified database, we provide an adapted 
dataset which fulfills the statitics of the ENTSOe 



## Data-Sources: 

OPSD - Open Power System Data, provide data on 
	http://data.open-power-system-data.org/conventional_power_plants/

GEO - Global Energy Observatory

WRI - World Resource Institute (http://www.wri.org), provide their data open source 
	on https://github.com/Arjay7891/WRI-Powerplant/blob/master/output_database/powerwatch_data.csv


ENTSOe - provides statistics :
	https://www.entsoe.eu/db-query/miscellaneous/net-generating-capacity

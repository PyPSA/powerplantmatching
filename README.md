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

If you are further interested in a modified database, we provide an adapted 
dataset which nearly covers all capacities totals claimed by the ENTSOe
statistics. This is done by including powerplants that were not matched
but come out of a reliable source, e.g. the GEO data. Furthermore, a
learning algorithm was used to specify information about missing 
Hydro classification (Run-of-River, Pumped Storage and Reservoir).
This, database is available using the python command 
powerplant_collection.Matched_dataset(). 


## Combining Data From Different Sources

Whereas single databases as the CARMA or the GEO database provide 
non standardized and lacking information, the data sets can complement 
each other and check their reliability. Therefore, we combine 
five different databases (see below) and take only matched power plants.
The result can be seen here:


https://cloud.githubusercontent.com/assets/19226431/20011654/a683952c-a2ac-11e6-8ce8-8e4982fb18d1.jpg



...


## Data-Sources: 

OPSD - Open Power System Data, provide data on 
	http://data.open-power-system-data.org/conventional_power_plants/

GEO - Global Energy Observatory

WRI - World Resource Institute (http://www.wri.org), provide their data open source 
	on https://github.com/Arjay7891/WRI-Powerplant/blob/master/output_database/powerwatch_data.csv


ENTSOe - provides statistics :
	https://www.entsoe.eu/db-query/miscellaneous/net-generating-capacity

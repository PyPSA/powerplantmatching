=================
General Structure
=================

The dataset combines the data of all the data sources listed in
`Data-Sources <#Data-Sources>`__ and provides the following information:

-  **Power plant name** - claim of each database
-  **Fueltype** - {Bioenergy, Geothermal, Hard Coal, Hydro, Lignite,
   Nuclear, Natural Gas, Oil, Solar, Wind, Other}
-  **Technology** - {CCGT, OCGT, Steam Turbine, Combustion Engine,
   Run-Of-River, Pumped Storage, Reservoir}
-  **Set** - {Power Plant (PP), Combined Heat and Power (CHP), Storages
   (Stores)}
-  **Capacity** - [MW]
-  **Duration** - Maximum state of charge capacity in terms of hours at
   full output capacity
-  **Dam Information** - Dam volume [Mm^3] and Dam Height [m]
-  **Geo-position** - Latitude, Longitude
-  **Country** - EU-27 + CH + NO (+ UK) minus Cyprus and Malta
-  **YearCommissioned** - Commmisioning year of the powerplant
-  **RetroFit** - Year of last retrofit
-  **projectID** - Immutable identifier of the power plant


All data files of the package will be stored in the folder given by
``pm.core.package_config['data_dir']``


Data Sources
------------

-  OPSD - `Open Power System
   Data <http://data.open-power-system-data.org/>`__ publish their
   `data <http://data.open-power-system-data.org/conventional_power_plants/>`__
   under a free license
-  GEO - `Global Energy
   Observatory <http://globalenergyobservatory.org/>`__, the data is not
   directly available on the website, but can be obtained from an
   `sqlite
   scraper <https://morph.io/coroa/global_energy_observatory_power_plants>`__
-  GPD - `Global Power Plant
   Database <http://datasets.wri.org/dataset/globalpowerplantdatabase>`__
   provide their data under a free license
-  CARMA - `Carbon Monitoring for Action <http://carma.org/plant>`__
-  ENTSOe - `European Network of Transmission System Operators for
   Electricity <http://entsoe.eu/>`__, annually provides statistics
   about aggregated power plant capacities. Their data can be used as a
   validation reference. We further use their `annual energy generation
   report from
   2010 <https://www.entsoe.eu/db-query/miscellaneous/net-generating-capacity>`__
   as an input for the hydro power plant classification. The `power
   plant
   dataset <https://transparency.entsoe.eu/generation/r2/installedCapacityPerProductionUnit/show>`__
   on the ENTSO-E transparency website is downloaded using the `ENTSO-E
   Transparency
   API <https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html>`__.
-  JRC - `Joint Research Centre Hydro-power plants
   database <https://github.com/energy-modelling-toolkit/hydro-power-database>`__
-  IRENA - `International Renewable Energy
   Agency <http://resourceirena.irena.org/gateway/dashboard/>`__ open
   available statistics on power plant capacities.
-  BNETZA -
   `Bundesnetzagentur <https://www.bundesnetzagentur.de/EN/Areas/Energy/Companies/SecurityOfSupply/GeneratingCapacity/PowerPlantList/PubliPowerPlantList_node.html>`__
   open available data source for Germany’s power plants
-  UBA (Umwelt Bundesamt Datenbank “Kraftwerke in Deutschland)

Not available but supported sources:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  IWPDCY (International Water Power & Dam Country Yearbook)
-  WEPP (Platts, World Elecrtric Power Plants Database)


Reliabilty Score 
----------------

When the matched power plant entries from different sources are combined, the resulting value per column is determined by the most reliable source. The corresponding reliability scores
are:
   
======= ================
Dataset Reliabilty score
======= ================
JRC     6
ESE     6
UBA     5
OPSD    5
OPSD_EU 5
OPSD_DE 5
WEPP    4
ENTSOE  4
IWPDCY  3
GPD     3
GEO     3
BNETZA  3
CARMA   1
======= ================



How it works
------------

Whereas single databases as the CARMA, GEO or the OPSD database provide
non standardized and incomplete information, the datasets can complement
each other and improve their reliability. In a first step,
powerplantmatching converts all powerplant dataset into a standardized
format with a defined set of columns and values. The second part
consists of aggregating power plant blocks together into units. Since
some of the datasources provide their powerplant records on unit level,
without detailed information about lower-level blocks, comparing with
other sources is only possible on unit level. In the third and
name-giving step the tool combines (or matches)different, standardized
and aggregated input sources keeping only powerplants units which appear
in more than one source. The matched data afterwards is complemented by
data entries of reliable sources which have not matched.

The aggregation and matching process heavily relies on
`DUKE <https://github.com/larsga/Duke>`__, a java application
specialized for deduplicating and linking data. It provides many
built-in comparators such as numerical, string or geoposition
comparators. The engine does a detailed comparison for each single
argument (power plant name, fuel-type etc.) using adjusted comparators
and weights. From the individual scores for each column it computes a
compound score for the likeliness that the two powerplant records refer
to the same powerplant. If the score exceeds a given threshold, the two
records of the power plant are linked and merged into one data set.

Let’s make that a bit more concrete by giving a quick example. Consider
the following two data sets

Dataset 1:
~~~~~~~~~~

+---+-------+-------+-------+-------+-------+-------+-------+------+
|   | Name  | Fue   | Clas  | Co    | Cap   | lat   | lon   | File |
|   |       | ltype | sific | untry | acity |       |       |      |
|   |       |       | ation |       |       |       |       |      |
+===+=======+=======+=======+=======+=======+=======+=======+======+
| 0 | Aa    | Hydro | nan   | S     | 1     | 47    | 7.    | nan  |
|   | rberg |       |       | witze | 4.609 | .0444 | 27578 |      |
|   |       |       |       | rland |       |       |       |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+
| 1 | Abbey | Oil   | nan   | U     | 6.4   | 5     | -0.00 | nan  |
|   | mills |       |       | nited |       | 1.687 | 42057 |      |
|   | pu    |       |       | Ki    |       |       |       |      |
|   | mping |       |       | ngdom |       |       |       |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+
| 2 | Ab    | Other | nan   | U     | 8     | 57    | -2.   | nan  |
|   | ertay |       |       | nited |       | .1785 | 18679 |      |
|   |       |       |       | Ki    |       |       |       |      |
|   |       |       |       | ngdom |       |       |       |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+
| 3 | Abe   | Coal  | nan   | U     | 1     | 51    | -3.   | nan  |
|   | rthaw |       |       | nited | 552.5 | .3875 | 40675 |      |
|   |       |       |       | Ki    |       |       |       |      |
|   |       |       |       | ngdom |       |       |       |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+
| 4 | A     | Wind  | nan   | Ge    | 18    | 51    | 12.95 | nan  |
|   | blass |       |       | rmany |       | .2333 |       |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+
| 5 | Abono | Coal  | nan   | Spain | 921.7 | 43    | -5.   | nan  |
|   |       |       |       |       |       | .5588 | 72287 |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+

and

Dataset 2:
~~~~~~~~~~

+---+-------+-------+-------+-------+-------+-------+-------+------+
|   | Name  | Fue   | Clas  | Co    | Cap   | lat   | lon   | File |
|   |       | ltype | sific | untry | acity |       |       |      |
|   |       |       | ation |       |       |       |       |      |
+===+=======+=======+=======+=======+=======+=======+=======+======+
| 0 | Aa    | Hydro | nan   | S     | 15.5  | 47    | 7.272 | nan  |
|   | rberg |       |       | witze |       | .0378 |       |      |
|   |       |       |       | rland |       |       |       |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+
| 1 | Abe   | Coal  | Th    | U     | 1500  | 51    | -3    | nan  |
|   | rthaw |       | ermal | nited |       | .3873 | .4049 |      |
|   |       |       |       | Ki    |       |       |       |      |
|   |       |       |       | ngdom |       |       |       |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+
| 2 | Abono | Coal  | Th    | Spain | 921.7 | 43    | -5    | nan  |
|   |       |       | ermal |       |       | .5528 | .7231 |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+
| 3 | Abw   | Hydro | nan   | Au    | 168   | 4     | 14    | nan  |
|   | inden |       |       | stria |       | 8.248 | .4305 |      |
|   | asten |       |       |       |       |       |       |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+
| 4 | Aceca | Oil   | CHP   | Spain | 629   | 3     | -3    | nan  |
|   |       |       |       |       |       | 9.941 | .8569 |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+
| 5 | Aceca | Na    | CCGT  | Spain | 400   | 39    | -3    | nan  |
|   | f     | tural |       |       |       | .9427 | .8548 |      |
|   | enosa | Gas   |       |       |       |       |       |      |
+---+-------+-------+-------+-------+-------+-------+-------+------+

where Dataset 2 has the higher reliability score. Apparently entries 0,
3 and 5 of Dataset 1 relate to the same power plants as the entries 0,1
and 2 of Dataset 2. The toolset detects those similarities and combines
them into the following set, but prioritising the values of Dataset 2:

+---+----------+----------------+----------+----------------+----------+---------+---------+------+
|   | Name     | Country        | Fueltype | Classification | Capacity | lat     | lon     | File |
+===+==========+================+==========+================+==========+=========+=========+======+
| 0 | Aarberg  | Switzerland    | Hydro    | nan            | 15.5     | 47.0378 | 7.272   | nan  |
+---+----------+----------------+----------+----------------+----------+---------+---------+------+
| 1 | Aberthaw | United Kingdom | Coal     | Thermal        | 1500     | 51.3873 | -3.4049 | nan  |
+---+----------+----------------+----------+----------------+----------+---------+---------+------+
| 2 | Abono    | Spain          | Coal     | Thermal        | 921.7    | 43.5528 | -5.7231 | nan  |
+---+----------+----------------+----------+----------------+----------+---------+---------+------+

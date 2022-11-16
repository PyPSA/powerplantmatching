History of Changes
==================


Upcoming Version
----------------

**New Features**

* New `EXTERNAL_DATABASE` interface to integrate additional custom data of raw data matching the powerplantmatching format.

**Bug fixes**

* Fix `GEM_GGPT <https://globalenergymonitor.org/projects/global-gas-plant-tracker/>`_ interface.


Version 0.5.5 (05.09.2022)
-------------------------

**Bug fixes**

* Spanish hydro stores with an capacity larger than 50GWh in the `JRC` data base are assumed to be reservoirs even if stated differently.


Version 0.5.4 (02.08.2022)
-------------------------

**New Features**

* The `GEM_GGPT <https://globalenergymonitor.org/projects/global-gas-plant-tracker/>`_ data containing global gas power plant data for all countries is now available.

**Bug fixes**

* Fix capacity-weighted mean calculation of efficiencies. The previous implementation lead to underestimated efficiencies for power plants consisting of multiple power units.

Version 0.5.3 (08.04.2022)
-------------------------

* Bug fix for renewable power plants from OPSD

Version 0.5.2 (07.04.2022)
-------------------------

* The overall config setting was fine-tuned in order to improve the matching results. 
* New scripts were added to the folder `matching_analysis`

Version 0.5.1 (04.04.2022)
-------------------------

**New Features**

* The `IRENASTATS <https://pxweb.irena.org/pxweb/en/IRENASTAT>`_ data is now available containing powerplant capacities for all countries from 2000 to 2020.

**Bug fix**

* The url of the ``powerplants`` function was fixed. 


**Other**

* The removal of the column ``DateMothBall`` was caught up on. 
* The manual corrections were reactivated.
* Improved country code and name conversion by using ``country_converter``.


Version 0.5 (04.04.2022)
------------------------


This release contains many breaking changes. Due to time-constraints we cannot ensure a smooth transition to the new release. If you are using a custom config file (e.g. ``~/powerplantmatching_config.yaml``) please be aware of the following config changes: 

**Configuration Changes**

* The custom configuration now only updates the package default configuration, which makes the compatibility of configuration updates much easier. So, instead of replacing the whole package configuration (the default config provided by powerplantmatching), the new purpose of the custom config is to adjust individual values. So, please make sure to only add keys to the custom config which you want to change in comparison to the default config.
* The following sections of the configuration file ``~/powerplantmatching_config.yaml`` changed: 
  * the ``target_fueltypes`` section is now mapping the representative fueltypes to the regular expressions that are used in order to determine them.  
  * the ``target_technologies`` section is now mapping the representative technologies to the regular expressions that are used in order to determine them.  
  * the ``target_set`` section is now mapping the representative sets to the regular expressions that are used in order to determine them.  
  * a section ``clean_name`` was added. This section contains the regular expressions and lists of words that are used to clean the names of the plants.
In order to ensure compatibility with the new code, please delete these sections in your custom config. 

**Deprecations**

* The ``CARMA`` dataset was deprecated as the data is not publicly available anymore.
* The ``IWPDCY`` dataset was deprecated in the favor of the `JRC` data.
* The ``WEPP`` dataset was deprecated due to restrictive license.
* The ``UBA`` dataset was deprecated in the favor of the ``OPSD`` data.
* The ``BENTZA`` dataset was deprecated in the favor of the ``OPSD`` data.
* The ``IRENA_stats`` dataset was deprecated as the data is not publicly available anymore.
* The following functions were deprecated and will be removed in ``v0.6``:
  * ``powerplantmatching.export.to_TIMES``
  * ``powerplantmatching.export.store_open_dataset``
  * ``powerplantmatching.export.fueltype_to_abbrev` `
  * ``powerplantmatching.heuristics.set_denmark_region_id``
  * ``powerplantmatching.heuristics.remove_oversea_areas``
  * ``powerplantmatching.heuristics.set_known_retire_years``
* The argument ``extendby_kwargs`` in the function ``powerplantmatching.collection.matched_data`` was deprecated in the favor of ``extend_by_kwargs``.


**New Features**

* The `BEYOND COAL <https://beyond-coal.eu/coal-exit-tracker/>`_ data is now available as an data source. 
* A new dataset ``WIKIPEDIA`` on nuclear powerplants in europe from wikipedia was added. 
* The ``GEO`` dataset returns powerplant blocks instead of whole plants. 
* All scripts were aligned with the ``black`` coding style.
* A documentation on readthedocs was added.
* The config has now a key `main_query` which is applied to all datasets. 
* A CI was added. 
* A new function ``powerplantmatching.heuristics.isin`` was added. It checks which data entries of a non-matched dataset is included in a matched dataset.

**Breaking Code Changes:**

* The argument `rawDE` and `rawEU` in ``powerplantmatching.data.OPSD`` was deprecated in the favor of `raw`. If ``True`` the function returns a dictionary with the raw datasets.
* All keyword arguments of the data functions in ``powerplantmatching.data`` were sorted according to ``raw``, ``update``, ``config``. This lead to some breaking changes of the arguments order.
* The Fueltype `Other` was replaced by NaN. 
* The `GEO` data now returns a dataset containing power plant units.  
* The ``ESE`` dataset was removed due the hosting website taken down. 
* The argument ``subsume_uncommon_fueltypes_to_other`` in ``powerplantmatching.collection.matched_data`` was removed. 
* The function ``powerplantmatching.cleaning.aggregate_units`` does not support the arguments `use_saved_aggregation` and `save_aggregation` anymore due to it's unsecure behavior.
* The function ``powerplantmatching.matching.compare_two_datasets`` does not support the arguments `use_saved_matches` anymore due to it's unsecure behavior.


Version 0.4.6 (25.11.2020)
--------------------------

| Triggered by the ongoing phase-outs of especially nuclear, coal and
  lignite plants in many countries, we acknowledge that time-related
  data of power stations and their single blocks becomes increasingly
  important.
| Therefore, we decided to - adapt the columns: - rename
  ``YearCommissioned`` to ``DateIn`` (reflects when a station/block had
  initially started operation) - rename ``Retrofit`` to ``DateRetrofit``
  (reflects when a station/block has been retrofitted) - add
  ``DateMothball`` (reflects when a station/block has been mothballed) -
  add ``DateOut`` (reflects when a station/block has been finally
  decommissioned)
| **Please note:** Currently, these columns only contain the year, but
  we aim in future to provide exact dates (i.e. including day and month)
  wherever possible.

| Further changes: - new target_columns: - add ``EIC`` (the European
  *Energy Identification Code*) - add ``StorageCapacity_MWh`` - update
  `JRC Hydro
  Database <https://github.com/energy-modelling-toolkit/hydro-power-database>`__
  to v5 and add quick workaround so that pm can deal with non-unique
  identifiers - replace deprecated by current pandas functions - custom
  configuration and package configuration are now merged when calling
  ``get_config()`` (values in the custom configuration are prioritized)
  - fix retrieving BNETZA data
| - export.py has now a function for mapping bus coordinates to the
  power plant list

Version 0.4.1 (02.08.2019)
--------------------------

Data structure
~~~~~~~~~~~~~~

-  abolish git lfs in the favour of direct url parsing
-  store data in user folders

   -  Linux ``~/.local/share/powerplantmatching``
   -  Windows ``C:\Users\<USERNAME>\AppData\Roaming\powerplantmatching``

-  move necessary files to package_data in powerplantmatching folder
   (such as duke binaries, xml files etc.)
-  include `JRC Hydro
   Database <https://github.com/energy-modelling-toolkit/hydro-power-database>`__

Code
~~~~

-  get rid of mutual module imports
-  speed up grouping (cleaning.py, matching.py)
-  revise/rewrite code in data.py
-  enable switch for matching powerplants of the same country only (is
   now default, speeds up the matching and aggregation process
   significantly)
-  boil down plot.py which caused long import times
-  get rid of config.py in the favour of core.py and accessor.py
-  drop deprecated functions in collection.py which now only includes
   collect() and matched_data()

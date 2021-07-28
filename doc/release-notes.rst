History of Changes
==================

Upcoming Release
----------------

* The `BEYOND COAL <https://beyond-coal.eu/coal-exit-tracker/>`_ data is now available as an data source. 
* All scripts were aligned with the ``black`` coding style.
* A documentation on readthedocs was added.
* The custom configuration in ``~/powerplantmatching_config.yaml`` now only updates the package configuration, which makes the compatibility with configuration updates much easier. So, instead of replacing the package config, the new purpose of the custom config is to add/modify configuration values.  

**Breaking Changes:**
* All keyword arguments of the data functions in ``powerplantmatching.data`` were sorted according to ``raw``, ``update``, ``config``. This lead to some hard changes of the arguments order.

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

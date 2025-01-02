============
Contributing
============

We welcome anyone interested in contributing to this project,
be it with new ideas, suggestions, by filing bug reports or
contributing code.

You are invited to submit pull requests / issues to our
`Github repository <https://github.com/PyPSA/powerplantmatching>`_.

Therefore, we encourage to install ``powerplantmatching`` together with packages used for developing:

.. code:: bash

  pip install powerplantmatching[dev]


This will also install the ``pre-commit`` package which checks that new changes are aligned with the guidelines. 
To automatically activate ``pre-commit`` on every ``git commit``, run ``pre-commit install``. 
To manually run it, execute ``pre-commit run --all``.

To double-check that your code is working, we welcome you to write a unit test. Run all tests with 

.. code:: bash

  pytest



Integrating new Data-Sources
----------------------------

Let’s say you have a new dataset “FOO.csv” which you want to combine
with the other data bases. Follow these steps to properly integrate it.
Please, before starting, make sure that you’ve installed
``powerplantmatching`` from your downloaded local repository (link).

1. Look where powerplantmatching stores all data files

  .. code:: python
  
    import powerplantmatching as pm     
    pm.core.package_config['data_dir']

2. Store FOO.csv in this directory under the subfolder ``data/in``. So
   on Linux machines the total path under which you store your data file
   would be:
   ``/home/<user>/.local/share/powerplantmatching/data/in/FOO.csv``

3. Look where powerplantmatching looks for a custom configuration file
 
  .. code:: python

    pm.core.package_config["custom_config"]

  
  If this file does not yet exist on your machine, download the
  `standard
  configuration <https://raw.githubusercontent.com/PyPSA/powerplantmatching/master/powerplantmatching/package_data/config.yaml>`__
  and store it under the given path as
  ``.powerplantmatching_config.yaml``.

4. Open the yaml file and add a new entry under the section
   ``#data config``. The new entry should look like this

   .. code:: yaml

    FOO:       
      reliability_score: 4       
      fn: FOO.csv 
     
   The ``reliability_score`` indicates the reliability of your data, choose
   a number between 1 (low quality data) and 7 (high quality data). If
   the data is openly available, you can add an ``url`` argument linking
   directly to the .csv file, which will enable automatic downloading.

   Add the name of the new entry to the ``matching_sources`` in your
   yaml file like shown below

   .. code:: yaml

    #matching config  
    matching_sources:      
      ...      
      - OPSD      
      - FOO

5. Add a function ``FOO()`` to the data.py in the powerplantmatching
   source code. You find the file in your local repository under
   ``powerplantmatching/data.py``. The function should be structured
   like this: 
   
  .. code:: python

    def FOO(raw=False, config=None): 
    """
    Importer for the FOO database.


    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """

    config = get_config() if config is None else config
    df = pd.read_csv(get_raw_file("FOO"))
    if raw:
        return foo
    df = (df
        .rename(columns){'Latitude': 'lat',
                            'Longitude': 'lon'})
        .loc[lambda df: df.Country.isin(config['target_countries'])]
        .pipe(set_column_name, 'FOO')
        )

    return df

  Note that the code given after ``df =`` is just a placeholder for anything necessary to turn the raw data into the standardized format. You should ensure that the data gets the appropriate column names and that any attributes are in the correct format (all of the standard labels can be found in the yaml or by ``pm.get_config()[‘target_x’]``
  when replacing x by``\ columns, countries, fueltypes, sets or technologies`.

6. Make sure the FOO entry is given in the configuration

  .. code:: python
    
    pm.get_config()

  and load the file 
   
  .. code:: python

    pm.data.FOO()

7. If everything works fine, you can run the whole matching process with

   .. code:: python
    
      pm.powerplants(update=True)

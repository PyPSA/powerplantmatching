<!--
SPDX-FileCopyrightText: 2025 Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>

SPDX-License-Identifier: MIT
-->

# Make your own configuration

You have the option to easily manipulate the resulting data by modifying the global configuration. Just save the [config.yaml file](https://github.com/PyPSA/powerplantmatching/blob/master/powerplantmatching/package_data/config.yaml) as `~/.powerplantmatching_config.yaml` manually or for Linux users:

```bash
wget -O ~/.powerplantmatching_config.yaml https://raw.githubusercontent.com/PyPSA/powerplantmatching/master/powerplantmatching/package_data/config.yaml
```

and change the `.powerplantmaching_config.yaml` file according to your wishes. Thereby you can:

- determine the global set of **countries** and **fueltypes**
- determine which data sources to combine and which data sources should completely be contained in the final dataset
- individually filter data sources via [`pandas.DataFrame.query`](http://pandas.pydata.org/pandas-docs/stable/indexing.html#the-query-method) statements set as an argument of data source name. See the default [config.yaml file](https://github.com/PyPSA/powerplantmatching/blob/master/powerplantmatching/package_data/config.yaml) as an example

Optionally you can:

- add your ENTSOE security token to the `.powerplantmaching_config.yaml` file. To enable updating the ENTSOE data by yourself. The token can be obtained by following section 2 of the [RESTful API documentation](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_authentication_and_authorisation) of the ENTSOE-E Transparency platform.
- add your Google API key to the config.yaml file to enable geoparsing. The key can be obtained by following the [instructions](https://developers.google.com/maps/documentation/geocoding/get-api-key).

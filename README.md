# powerplantmatching

[![pypi](https://img.shields.io/pypi/v/powerplantmatching.svg)](https://pypi.org/project/powerplantmatching/) 
[![conda](https://img.shields.io/conda/vn/conda-forge/powerplantmatching.svg)](https://anaconda.org/conda-forge/powerplantmatching) 
![pythonversion](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2FPyPSA%2Fpowerplantmatching%2Fmaster%2Fpyproject.toml) 
[![Tests](https://github.com/PyPSA/powerplantmatching/actions/workflows/test.yml/badge.svg)](https://github.com/PyPSA/powerplantmatching/actions/workflows/test.yml)
[![doc](https://readthedocs.org/projects/powerplantmatching/badge/?version=latest)](https://powerplantmatching.readthedocs.io/en/latest/) 
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/PyPSA/powerplantmatching/master.svg)](https://results.pre-commit.ci/latest/github/PyPSA/powerplantmatching/master)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![LICENSE](https://img.shields.io/pypi/l/powerplantmatching.svg) 
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3358985.svg)](https://zenodo.org/record/3358985#.XUReFPxS_MU) 
[![Stack Exchange questions](https://img.shields.io/stackexchange/stackoverflow/t/pypsa)](https://stackoverflow.com/questions/tagged/pypsa)

A toolset for cleaning, standardizing and combining multiple power
plant databases.

This package provides ready-to-use power plant data for the European power system.
Starting from openly available power plant datasets, the package cleans, standardizes
and merges the input data to create a new combined dataset, which includes all the important information.
The package allows to easily update the combined data as soon as new input datasets are released.

You can directly [download the current version of the data](https://downgit.github.io/#/home?url=https://github.com/PyPSA/powerplantmatching/blob/master/powerplants.csv) as a CSV file.

Initially, powerplantmatching was developed by the
[Renewable Energy Group](https://fias.uni-frankfurt.de/physics/schramm/complex-renewable-energy-networks/)
at [FIAS](https://fias.uni-frankfurt.de/) and is now maintained by the [Digital Transformation in Energy Systems Group](https://tub-ensys.github.io/) at the Technical University of Berlin to build power plant data
inputs to [PyPSA](http://www.pypsa.org/)-based models for carrying
out simulations.

### Main Features

- clean and standardize power plant data sets
- aggregate power plant units which belong to the same plant
- compare and combine different data sets
- create lookups and give statistical insight to power plant goodness
- provide cleaned data from different sources
- choose between gross/net capacity
- provide an already merged data set of multiple different open data sources
- scale the power plant capacities in order to match country-specific statistics about total power plant capacities
- visualize the data
- export your powerplant data to a [PyPSA](https://github.com/PyPSA/PyPSA)-based model

## Map

![powerplants.png](doc/powerplants.png)

## Installation

 Using pip

```bash
pip install powerplantmatching
```

or conda

```bash
conda install -c conda-forge powerplantmatching
```

# Contributing and Support
We strongly welcome anyone interested in contributing to this project. If you have any ideas, suggestions or encounter problems, feel invited to file issues or make pull requests on GitHub.
-   In case of code-related **questions**, please post on [stack overflow](https://stackoverflow.com/questions/tagged/pypsa).
-   For non-programming related and more general questions please refer to the [PyPSA mailing list](https://groups.google.com/group/pypsa).
-   To **discuss** with other PyPSA & technology-data users, organise projects, share news, and get in touch with the community you can use the [discord server](https://discord.gg/JTdvaEBb).
-   For **bugs and feature requests**, please use the [powerplantmatching Github Issues page](https://github.com/PyPSA/powerplantmatching/issues).


## Citing powerplantmatching

If you want to cite powerplantmatching, use the following paper

- F. Gotzens, H. Heinrichs, J. Hörsch, and F. Hofmann, [Performing energy modelling exercises in a transparent way - The issue of data quality in power plant databases](https://www.sciencedirect.com/science/article/pii/S2211467X18301056?dgcid=author), Energy Strategy Reviews, vol. 23, pp. 1–12, Jan. 2019.

with bibtex

```
@article{gotzens_performing_2019,
 title = {Performing energy modelling exercises in a transparent way - {The} issue of data quality in power plant databases},
 volume = {23},
 issn = {2211467X},
 url = {https://linkinghub.elsevier.com/retrieve/pii/S2211467X18301056},
 doi = {10.1016/j.esr.2018.11.004},
 language = {en},
 urldate = {2018-12-03},
 journal = {Energy Strategy Reviews},
 author = {Gotzens, Fabian and Heinrichs, Heidi and Hörsch, Jonas and Hofmann, Fabian},
 month = jan,
 year = {2019},
 pages = {1--12}
}
```

and/or the current release stored on Zenodo with a release-specific DOI:

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3358985.svg)](https://zenodo.org/record/3358985#.XURat99fjRY)

## Licence

Copyright 2018-2022 Fabian Hofmann (EnSys TU Berlin), Fabian Gotzens (FZ Jülich), Jonas Hörsch (KIT),

powerplantmatching is released as free software under the
[GPLv3](http://www.gnu.org/licenses/gpl-3.0.en.html), see
[LICENSE](LICENSE) for further information.

# powerplantmatching

 [![pypi](https://img.shields.io/pypi/v/powerplantmatching.svg)](https://pypi.org/project/powerplantmatching/) [![conda](https://img.shields.io/conda/vn/conda-forge/powerplantmatching.svg)](https://anaconda.org/conda-forge/powerplantmatching) ![pythonversion](https://img.shields.io/pypi/pyversions/powerplantmatching) ![LICENSE](https://img.shields.io/pypi/l/powerplantmatching.svg) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3358985.svg)](https://zenodo.org/record/3358985#.XUReFPxS_MU) [![doc](https://readthedocs.org/projects/powerplantmatching/badge/?version=latest)](https://powerplantmatching.readthedocs.io/en/latest/) [![pre-commit.ci status](https://results.pre-commit.ci/badge/github/FRESNA/powerplantmatching/master.svg)](https://results.pre-commit.ci/latest/github/FRESNA/powerplantmatching/master)

A toolset for cleaning, standardizing and combining multiple power
plant databases.

This package provides ready-to-use power plant data for the European power system.
Starting from openly available power plant datasets, the package cleans, standardizes
and merges the input data to create a new combining dataset, which includes all the important information.
The package allows to easily update the combined data as soon as new input datasets are released.

![Map of power plants in Europe](https://user-images.githubusercontent.com/19226431/46086361-36a13080-c1a8-11e8-82ed-9f04167273e5.png)

powerplantmatching was initially developed by the
[Renewable Energy Group](https://fias.uni-frankfurt.de/physics/schramm/complex-renewable-energy-networks/)
at [FIAS](https://fias.uni-frankfurt.de/) to build power plant data
inputs to [PyPSA](http://www.pypsa.org/)-based models for carrying
out simulations for the [CoNDyNet project](http://condynet.de/),
financed by the
[German Federal Ministry for Education and Research (BMBF)](https://www.bmbf.de/en/)
as part of the
[Stromnetze Research Initiative](http://forschung-stromnetze.info/projekte/grundlagen-und-konzepte-fuer-effiziente-dezentrale-stromnetze/).

### Main Features

- clean and standardize power plant data sets
- aggregate power plants units which belong to the same plant
- compare and combine different data sets
- create lookups and give statistical insight to power plant goodness
- provide cleaned data from different sources
- choose between gros/net capacity
- provide an already merged data set of six different data-sources
- scale the power plant capacities in order to match country specific statistics about total power plant capacities
- visualize the data
- export your powerplant data to a [PyPSA](https://github.com/PyPSA/PyPSA)

## Installation

 Using pip

```bash
pip install powerplantmatching
```

or conda

```bash
conda install -c conda-forge powerplantmatching
```

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

## Acknowledgements

The development of powerplantmatching was helped considerably by
in-depth discussions and exchanges of ideas and code with

- Tom Brown from Karlsruhe Institute for Technology
- Chris Davis from University of Groningen and
- Johannes Friedrich, Roman Hennig and Colin McCormick of the World Resources Institute

## Licence

Copyright 2018-2020 Fabian Gotzens (FZ Jülich), Jonas Hörsch (KIT), Fabian Hofmann (FIAS)

powerplantmatching is released as free software under the
[GPLv3](http://www.gnu.org/licenses/gpl-3.0.en.html), see
[LICENSE](LICENSE) for further information.

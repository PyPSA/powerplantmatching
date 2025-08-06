# Welcome to powerplantmatching's documentation!

![PyPI](https://img.shields.io/pypi/v/powerplantmatching.svg)
[![Conda](https://img.shields.io/conda/vn/conda-forge/powerplantmatching.svg)](https://anaconda.org/conda-forge/powerplantmatching)
![Python Version](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2FPyPSA%2Fpowerplantmatching%2Fmaster%2Fpyproject.toml)
[![Tests](https://github.com/PyPSA/powerplantmatching/actions/workflows/test.yml/badge.svg)](https://github.com/PyPSA/powerplantmatching/actions/workflows/test.yml)
[![Docs](https://readthedocs.org/projects/powerplantmatching/badge/?version=latest)](https://powerplantmatching.readthedocs.io/en/latest/)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/PyPSA/powerplantmatching/master.svg)](https://results.pre-commit.ci/latest/github/PyPSA/powerplantmatching/master)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![LICENSE](https://img.shields.io/pypi/l/powerplantmatching.svg)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3358985.svg)](https://zenodo.org/record/3358985#.XUReFPxS_MU)

## Introduction

A toolset for cleaning, standardizing and combining multiple power plant
databases.

This package provides ready-to-use power plant data for the European
power system. Starting from openly available power plant datasets, the
package cleans, standardizes and merges the input data to create a new
combining dataset, which includes all the important information. The
package allows to easily update the combined data as soon as new input
datasets are released.

![Map of power plants in Europe](assets/images/powerplants.png)

`powerplantmatching` was initially developed by the [Renewable Energy Group](https://fias.uni-frankfurt.de/physics/schramm/complex-renewable-energy-networks/) at [FIAS](https://fias.uni-frankfurt.de/) to build power plant data inputs to [PyPSA](http://www.pypsa.org/)-based models for carrying out simulations for the [CoNDyNet project](http://condynet.de/), financed by the [German Federal Ministry for Education and Research (BMBF)](https://www.bmbf.de/en/) as part of the [Stromnetze Research Initiative](http://forschung-stromnetze.info/projekte/grundlagen-und-konzepte-fuer-effiziente-dezentrale-stromnetze/).

## Main Features

- clean and standardize power plant data sets
- aggregate power plants units which belong to the same plant
- compare and combine different data sets
- create lookups and give statistical insight to power plant goodness
- provide cleaned data from different sources
- choose between gros/net capacity
- provide an already merged data set of six different data-sources
- scale the power plant capacities in order to match country specific
  statistics about total power plant capacities
- visualize the data
- export your powerplant data to a
    - [PyPSA](https://github.com/PyPSA/PyPSA), or
    - [TIMES](https://iea-etsap.org/index.php/etsap-tools/model-generators/times) model
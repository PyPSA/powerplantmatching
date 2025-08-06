# SPDX-FileCopyrightText: The powerplantmatching contributors
#
# SPDX-License-Identifier: MIT

"""
powerplantmatching

A set of tools for cleaning, standardising and combining multiple
power plant databases.
"""

from importlib.metadata import version

from . import core, data, heuristics, plot, utils
from .accessor import PowerPlantAccessor
from .collection import powerplants
from .core import get_config, package_config

__author__ = "Fabian Hofmann"
__copyright__ = "Copyright 2017-2024 Technical University of Berlin"
# The rough hierarchy of this package is
# core, utils, heuristics, cleaning, matching, collection, data

# e.g. "0.5.15" or "0.5.15.post27+g761e814.d20240722" (if installed from master branch)
__version__ = version("powerplantmatching")
# e.g. "0.5.15", without the post part (if it exists, otherwise the same as __version__)
latest_release = __version__.split(".post")[0]

__all__ = [
    "powerplants",
    "get_config",
    "package_config",
    "PowerPlantAccessor",
    "core",
    "data",
    "heuristics",
    "plot",
    "utils",
]

# Copyright 2016-2024 Fabian Hofmann (FIAS), Jonas Hoersch (KIT, IAI) and
# Fabian Gotzens (FZJ, IEK-STE)

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
powerplantmatching

A set of tools for cleaning, standardising and combining multiple
power plant databases.
"""

import re
from importlib.metadata import version

from . import core, data, heuristics, plot, utils
from .accessor import PowerPlantAccessor
from .collection import powerplants
from .core import get_config, package_config

__author__ = "Fabian Hofmann"
__copyright__ = "Copyright 2017-2024 Technical University of Berlin"
# The rough hierarchy of this package is
# core, utils, heuristics, cleaning, matching, collection, data

# e.g. "0.17.1" or "0.17.1.dev4+ga3890dc0" (if installed from git)
__version__ = version("powerplantmatching")
# e.g. "0.17.0" # TODO, in the network structure it should use the dev version
release_version = re.match(r"(\d+\.\d+(\.\d+)?)", __version__).group(0)

# Assert that version is not 0.1 (which is the default version in the setup.py)
assert release_version != "0.1", "setuptools_scm could not find the version number"


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

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

__version__ = "0.5.15"
__author__ = "Fabian Hofmann"
__copyright__ = "Copyright 2017-2024 Technical University of Berlin"
# The rough hierarchy of this package is
# core, utils, heuristics, cleaning, matching, collection, data

from . import core, data, heuristics, plot, utils
from .accessor import PowerPlantAccessor
from .collection import powerplants
from .core import get_config, package_config

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

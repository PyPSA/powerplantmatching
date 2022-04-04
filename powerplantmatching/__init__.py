# Copyright 2016-2020 Fabian Hofmann (FIAS), Jonas Hoersch (KIT, IAI) and
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


from __future__ import absolute_import

__version__ = "0.5.0"
__author__ = "Fabian Hofmann, Jonas Hoersch, Fabian Gotzens"
__copyright__ = "Copyright 2017-2020 Frankfurt Institute for Advanced Studies"
# The rough hierarchy of this package is
# core, utils, heuristics, cleaning, matching, collection, data

# from . import cleaning
# from . import matching
# from . import collection
# Commonly used sub-modules. Imported here to provide end-user
# convenience.
from . import core, data, heuristics, plot, utils
from .accessor import PowerPlantAccessor
from .collection import matched_data as powerplants
from .core import get_config, package_config

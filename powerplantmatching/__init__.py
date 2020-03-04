# Copyright 2016-2018 Fabian Hofmann (FIAS), Jonas Hoersch (KIT, IAI) and
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

__version__ = "0.4.4"
__author__ = "Fabian Hofmann, Jonas Hoersch"
__copyright__ = "Copyright 2017-2020 Frankfurt Institute for Advanced Studies"
#The rough hierarchy of this package is
#core, utils, heuristics, cleaning, matching, collection, data

# Commonly used sub-modules. Imported here to provide end-user
# convenience.
from . import core
from . import utils
from . import heuristics
from . import data
#from . import cleaning
#from . import matching
#from . import collection
from . import plot
from .core import get_config
from .collection import matched_data as powerplants
from .accessor import PowerPlantAccessor



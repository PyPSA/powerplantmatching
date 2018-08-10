# Copyright 2015-2016 Fabian Hofmann (FIAS), Jonas Hoersch (FIAS)

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

from .utils import _data_out
from . import (config, cleaning, data, heuristics, export, matching, utils,
               collection, plot)

# Logging: General Settings
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=20)
logger.setLevel('INFO')
# Logging: File
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] " +
                                 "[%(levelname)-5.5s]  %(message)s")
fileHandler = logging.FileHandler(_data_out('PPM.log'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
# logger.info('Initialization complete.')

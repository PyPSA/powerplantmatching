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


import hashlib
import base64
import os
from six.moves import cPickle as pickle
import yaml
from .utils import _data


def get_config(filename=None, **overrides):
    if filename is None:
        filename = _data('../config.yaml')
    assert os.path.exists(filename), (
            "The config file '{}' does not exist yet. "
            "Copy config_example.yaml to config.yaml and fill in details, "
            "as necessary.".format(filename))
    with open(filename) as f:
        config = yaml.load(f)
        config.update(overrides)

        sha1digest = hashlib.sha1(pickle.dumps(overrides)).digest()
        config['hash'] = base64.encodestring(sha1digest).decode('ascii')[2:12]

    return config

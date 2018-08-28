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


import hashlib
import base64
import os
from six.moves import cPickle as pickle
import yaml
import logging
logger = logging.getLogger(__name__)


def get_config(filename=None, **overrides):
    from .utils import _data, _data_out
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
        if len(dict(**overrides)) == 0:
            config['hash'] = 'default'
        else:
            config['hash'] = base64.encodestring(sha1digest)\
                             .decode('ascii')[2:12]
    if not os.path.isdir(_data_out('.', config=config)):
        os.mkdir(os.path.abspath(_data_out('.', config=config)))
        os.mkdir(os.path.abspath(_data_out('matches', config=config)))
        os.mkdir(os.path.abspath(_data_out('aggregations', config=config)))
        logger.info('Outputs for this configuration will be saved under {}'
                    .format(os.path.abspath(
                            _data_out('.', config=config))))
        with open(_data_out('config.yaml', config=config), 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
    return config

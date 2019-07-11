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
from os.path import dirname, exists
from six.moves import cPickle as pickle
import yaml
import logging
logger = logging.getLogger(__name__)

# for the writable data directory (i.e. the one where new data goes), follow
# the XDG guidelines found at
# https://standards.freedesktop.org/basedir-spec/basedir-spec-latest.html
_writable_dir = os.path.join(os.path.expanduser('~'), '.local', 'share')
_data_dir = os.path.join(os.environ.get("XDG_DATA_HOME", _writable_dir),
                         'powerplantmatching')

configuration = {
        'custom_config': os.path.join(os.path.expanduser('~'),
                                      '.powerplantmatching_config.yaml'),
        'data_dir': _data_dir,
        'repo_data_dir': os.path.join(dirname(__file__), 'package_data'),
        'downloaders': {}}



def _package_data(fn):
    return os.path.join(configuration['repo_data_dir'], fn)


def _data_in(fn):
    return os.path.join(configuration['data_dir'], 'data', 'in', fn)


def _data_out(fn, config=None):
    if config is None:
        return os.path.join(configuration['data_dir'], 'data', 'out',
                            'default', fn)
    else:
        return os.path.join(configuration['data_dir'], 'data', 'out',
                            config['hash'], fn)

del _data_dir
del _writable_dir


if not os.path.exists(_data_in('.')):
    os.makedirs(_data_in('.'))

def get_config(filename=None, **overrides):
    if filename is None:
        custom_config = configuration['custom_config']
        if exists(custom_config):
            filename = custom_config
        else:
            filename = _package_data('config.yaml')

    assert os.path.exists(filename), (
            "The config file '{}' does not exist yet. "
            "Copy config_example.yaml to config.yaml and fill in details, "
            "as necessary.".format(filename))
    with open(filename) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        config.update(overrides)

        sha1digest = hashlib.sha1(pickle.dumps(overrides)).digest()
        if len(dict(**overrides)) == 0:
            config['hash'] = 'default'
        else:
            config['hash'] = base64.encodestring(sha1digest)\
                             .decode('ascii')[2:12]
    if not os.path.isdir(_data_out('.', config=config)):
        os.makedirs(os.path.abspath(_data_out('.', config=config)))
        os.makedirs(os.path.abspath(_data_out('matches', config=config)))
        os.makedirs(os.path.abspath(_data_out('aggregations', config=config)))
        logger.info('Outputs for this configuration will be saved under {}'
                    .format(os.path.abspath(
                            _data_out('.', config=config))))
        with open(_data_out('config.yaml', config=config), 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
    return config






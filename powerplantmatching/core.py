#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 15:47:46 2019

@author: fabian
"""

import os
import logging
import pandas.api.extensions

# for the writable data directory (i.e. the one where new data goes), follow
# the XDG guidelines found at
# https://standards.freedesktop.org/basedir-spec/basedir-spec-latest.html
_writable_dir = os.path.join(os.path.expanduser('~'), '.local', 'share')
_data_dir = os.path.join(os.environ.get("XDG_DATA_HOME",
                            os.environ.get("APPDATA", _writable_dir)),
                                'powerplantmatching')
#data file configuration
package_config = {
        'custom_config': os.path.join(os.path.expanduser('~'),
                                      '.powerplantmatching_config.yaml'),
        'data_dir': _data_dir,
        'repo_data_dir': os.path.join(os.path.dirname(__file__), 'package_data'),
        'downloaders': {}}

os.makedirs(os.path.join(package_config['data_dir'], 'data', 'in'), exist_ok=True)
os.makedirs(os.path.join(package_config['data_dir'], 'data', 'out'), exist_ok=True)

def _package_data(fn):
    return os.path.join(package_config['repo_data_dir'], fn)


def _data_in(fn):
    return os.path.join(package_config['data_dir'], 'data', 'in', fn)


def _data_out(fn, config=None):
    if config is None:
        return os.path.join(package_config['data_dir'], 'data', 'out',
                            'default', fn)
    else:
        return os.path.join(package_config['data_dir'], 'data', 'out',
                            config['hash'], fn)

del _data_dir
del _writable_dir


if not os.path.exists(_data_in('.')):
    os.makedirs(_data_in('.'))

# Logging: General Settings
logger = logging.getLogger(__name__)
logger.setLevel('INFO')
# Logging: File
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] " +
                                 "[%(levelname)-5.5s]  %(message)s")
fileHandler = logging.FileHandler(
        os.path.join(package_config['data_dir'], 'PPM.log'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
# logger.info('Initialization complete.')

del logFormatter
del fileHandler



def get_config(filename=None, **overrides):
    from hashlib import sha1
    from base64 import encodestring
    from six.moves import cPickle
    import yaml
    from logging import info

    if filename is None:
        custom_config = package_config['custom_config']
        if os.path.exists(custom_config):
            filename = custom_config
        else:
            filename = _package_data('config.yaml')

    with open(filename) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        config.update(overrides)

        sha1digest = sha1(cPickle.dumps(overrides)).digest()
        if len(dict(**overrides)) == 0:
            config['hash'] = 'default'
        else:
            config['hash'] = encodestring(sha1digest)\
                             .decode('ascii')[2:12]
    if not os.path.isdir(_data_out('.', config=config)):
        os.makedirs(os.path.abspath(_data_out('.', config=config)))
        os.makedirs(os.path.abspath(_data_out('matches', config=config)))
        os.makedirs(os.path.abspath(_data_out('aggregations', config=config)))
        info('Outputs for this configuration will be saved under {}'
                    .format(os.path.abspath(
                            _data_out('.', config=config))))
        with open(_data_out('config.yaml', config=config), 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
    return config


def get_obj_if_Acc(obj):
    from .accessor import PowerPlantAccessor
    if isinstance(obj, PowerPlantAccessor):
        return obj._obj
    else:
        return obj


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 15:47:46 2019

@author: fabian
"""

import logging
from os.path import join, expanduser, dirname, exists, isdir, abspath
from os import environ, makedirs

# for the writable data directory (i.e. the one where new data goes), follow
# the XDG guidelines found at
# https://standards.freedesktop.org/basedir-spec/basedir-spec-latest.html
_writable_dir = join(expanduser('~'), '.local', 'share')
_data_dir = join(environ.get("XDG_DATA_HOME",
                             environ.get("APPDATA", _writable_dir)),
                 'powerplantmatching')

# data file configuration
package_config = {'custom_config': join(expanduser('~'),
                                        '.powerplantmatching_config.yaml'),
                  'data_dir': _data_dir,
                  'repo_data_dir': join(dirname(__file__), 'package_data'),
                  'downloaders': {}}

makedirs(join(package_config['data_dir'], 'data', 'in'), exist_ok=True)
makedirs(join(package_config['data_dir'], 'data', 'out'), exist_ok=True)


def _package_data(fn):
    return join(package_config['repo_data_dir'], fn)


def _data_in(fn):
    return join(package_config['data_dir'], 'data', 'in', fn)


def _data_out(fn, config=None):
    if config is None:
        return join(package_config['data_dir'], 'data', 'out', 'default', fn)
    else:
        return join(package_config['data_dir'], 'data', 'out', config['hash'],
                    fn)


del _data_dir
del _writable_dir


if not exists(_data_in('.')):
    makedirs(_data_in('.'))

# Logging: General Settings
logger = logging.getLogger(__name__)
logging.basicConfig(level=20)
logger.setLevel('INFO')
# Logging: File
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] "
                                 "[%(levelname)-5.5s]  %(message)s")
fileHandler = logging.FileHandler(join(package_config['data_dir'], 'PPM.log'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)
# logger.info('Initialization complete.')

del logFormatter
del fileHandler


def get_config(filename=None, **overrides):
    """
    Import the default configuration file and update custom settings.

    Parameters
    ----------
    filename : str, optional
        DESCRIPTION. The default is None.
    **overrides : dict
        DESCRIPTION.

    Returns
    -------
    config : dict
        The configuration dictionary
    """
    from hashlib import sha1
    from base64 import encodestring
    from six.moves import cPickle
    import yaml
    from logging import info

    package_config = _package_data('config.yaml')
    custom_config = filename if filename else _package_data('custom.yaml')

    with open(package_config) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    if exists(custom_config):
        with open(custom_config) as f:
            config.update(yaml.load(f, Loader=yaml.FullLoader))
    config.update(overrides)

    sha1digest = sha1(cPickle.dumps(overrides)).digest()
    if len(dict(**overrides)) == 0:
        config['hash'] = 'default'
    else:
        config['hash'] = encodestring(sha1digest).decode('ascii')[2:12]

    if not isdir(_data_out('.', config=config)):
        makedirs(abspath(_data_out('.', config=config)))
        makedirs(abspath(_data_out('matches', config=config)))
        makedirs(abspath(_data_out('aggregations', config=config)))
        info('Outputs for this configuration will be saved under {}'
             .format(abspath(_data_out('.', config=config))))
        with open(_data_out('config.yaml', config=config), 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
    return config


def get_obj_if_Acc(obj):
    from .accessor import PowerPlantAccessor
    if isinstance(obj, PowerPlantAccessor):
        return obj._obj
    else:
        return obj

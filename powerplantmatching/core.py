#!/usr/bin/env python3
"""
Created on Tue Jul 16 15:47:46 2019

@author: fabian
"""

import logging
from os import environ, makedirs
from os.path import abspath, dirname, exists, expanduser, isdir, join

# for the writable data directory (i.e. the one where new data goes), follow
# the XDG guidelines found at
# https://standards.freedesktop.org/basedir-spec/basedir-spec-latest.html
_writable_dir = join(expanduser("~"), ".local", "share")
_data_dir = join(
    environ.get("XDG_DATA_HOME", environ.get("APPDATA", _writable_dir)),
    "powerplantmatching",
)

# data file configuration
package_config = {
    "custom_config": join(expanduser("~"), ".powerplantmatching_config.yaml"),
    "data_dir": _data_dir,
    "repo_data_dir": join(dirname(__file__), "package_data"),
    "downloaders": {},
}

makedirs(join(package_config["data_dir"], "data", "in"), exist_ok=True)
makedirs(join(package_config["data_dir"], "data", "out"), exist_ok=True)


def _package_data(fn):
    return join(package_config["repo_data_dir"], fn)


def _data_in(fn):
    return join(package_config["data_dir"], "data", "in", fn)


def _data_out(fn, config):
    if config is None:
        directory = join(package_config["data_dir"], "data", "out", "default")
    else:
        directory = join(package_config["data_dir"], "data", "out", config["hash"])
    makedirs(directory, exist_ok=True)
    return join(directory, fn)


del _data_dir
del _writable_dir

if not exists(_data_in(".")):
    makedirs(_data_in("."))

# Logging: General Settings
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Logging: File
logFormatter = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] " "[%(levelname)-5.5s]  %(message)s"
)
fileHandler = logging.FileHandler(join(package_config["data_dir"], "PPM.log"))
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
    from base64 import encodebytes
    from hashlib import sha1
    from logging import info

    import yaml
    from six.moves import cPickle

    base_config = _package_data("config.yaml")
    if filename is not None:
        assert exists(filename)
        custom_config = filename
    else:
        custom_config = package_config["custom_config"]

    with open(base_config, encoding="utf8") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    if exists(custom_config):
        with open(custom_config, encoding="utf8") as f:
            config.update(yaml.load(f, Loader=yaml.FullLoader))
    config.update(overrides)

    sha1digest = sha1(cPickle.dumps(overrides)).digest()
    if len(dict(**overrides)) == 0:
        config["hash"] = "default"
    else:
        config["hash"] = (
            encodebytes(sha1digest)
            .decode("ascii")[2:12]
            .replace("\\", "")
            .replace("/", "")
        )

    if not isdir(_data_out(".", config)):
        makedirs(abspath(_data_out(".", config)))
        makedirs(abspath(_data_out("matches", config)))
        makedirs(abspath(_data_out("aggregations", config)))
        info(
            "Outputs for this configuration will be saved under {}".format(
                abspath(_data_out(".", config))
            )
        )
        with open(_data_out("config.yaml", config), "w") as file:
            yaml.dump(config, file, default_flow_style=False)

    changed_cols = ["target_fueltypes", "target_technologies", "target_sets"]
    old_config = any(isinstance(config[key], list) for key in changed_cols)
    if old_config:
        logger.warning(
            "Your configuration file seems to be from a powerplantmatching version "
            f"lower than v0.5.0. Please delete (or adjust) the keys {changed_cols} "
            f"in your custom config file at `{custom_config}`. For more information, "
            "see the release notes of version v0.5.0 at "
            "https://powerplantmatching.readthedocs.io/en/latest/release-notes.html."
        )
    return config


def get_obj_if_Acc(obj):
    from .accessor import PowerPlantAccessor

    if isinstance(obj, PowerPlantAccessor):
        return obj._obj
    else:
        return obj

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

import pandas.api.extensions
import os
import logging

# for the writable data directory (i.e. the one where new data goes), follow
# the XDG guidelines found at
# https://standards.freedesktop.org/basedir-spec/basedir-spec-latest.html
_writable_dir = os.path.join(os.path.expanduser('~'), '.local', 'share')
_data_dir = os.path.join(os.environ.get("XDG_DATA_HOME", _writable_dir),
                         'powerplantmatching')

#data file configuration
package_config = {
        'custom_config': os.path.join(os.path.expanduser('~'),
                                      '.powerplantmatching_config.yaml'),
        'data_dir': _data_dir,
        'repo_data_dir': os.path.join(os.path.dirname(__file__), 'package_data'),
        'downloaders': {}}



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
logging.basicConfig(level=20)
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



@pandas.api.extensions.register_dataframe_accessor("powerplant")
class PowerPlantAccessor():
    """
    Accessor object for DataFrames created with powerplantmatching.
    This simplifies the access to common functions applicable to dataframes
    with powerplant data. Note even though this is a general DataFrame
    accessor, the functions will only work for powerplantmatching related
    DataFrames.


    Examples
    --------

    import powerplantmatching as pm
    entsoe = pm.data.ENTSOE()
    entsoe.powerplant.plot_aggregated()

    """
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    from .plot import powerplant_map as plot_map
    from .utils import (lookup, set_uncommon_fueltypes_to_other,
                        select_by_projectID, breakdown_matches,
                        fill_geoposition, convert_country_to_alpha2,
                        convert_alpha2_to_country)
    from .export import to_pypsa_names
    from .heuristics import (
            extend_by_non_matched, scale_to_net_capacities,
            fill_missing_commyears, extend_by_VRE, fill_missing_duration,
            rescale_capacities_to_country_totals)
    from .cleaning import clean_powerplantname, aggregate_units
    from .matching import reduce_matched_dataframe

    def plot_aggregated(self, by=['Country', 'Fueltype'], figsize=(12,20),
                        **kwargs):
        """
        Plotting function for fast inspection of the capacity distribution.
        Returns figure and axes of a matplotlib barplot.

        Parameters
        -----------

        by : list, default ['Country', 'Fueltype']
            Define the columns of the dataframe to be grouped on.
        figsize : tuple, default (12,20)
            width and height of the figure
        **kwargs
            keywordargument for matplotlib plotting

        """
        import matplotlib.pyplot as plt
        from .utils import lookup, convert_country_to_alpha2
        subplots = True if len(by)>1 else False
        fig, ax = plt.subplots(figsize=figsize, **kwargs)
        df = lookup(convert_country_to_alpha2(self._obj), by=by)
        df = df.unstack().rename_axis(None) if subplots else df
        df.plot.bar(subplots=subplots, sharex=False, ax=ax)
        fig.tight_layout(h_pad=1.)
        return fig, ax

    def set_name(self, name):
        self._obj.columns.name = name

    def get_name(self):
        return self._obj.columns.name

    def match_with(self, df, labels=None, use_saved_matches=False,
                   config=None, reduced=True, **dukeargs):
        from .matching import combine_multiple_datasets, \
                              reduce_matched_dataframe
        from .utils import to_list_if_other

        dfs = [self._obj] + to_list_if_other(df)
        res = combine_multiple_datasets(
                dfs, labels, use_saved_matches=use_saved_matches,
                config=config, **dukeargs)
        if reduced:
            return res.pipe(reduce_matched_dataframe, config=config)
        return res
    pass


#df = get_obj_if_Acc(df)


# Commonly used sub-modules. Imported here to provide end-user
# convenience.
from . import (cleaning, data, heuristics, export, matching, utils,
               collection, plot)
from .collection import matched_data as powerplants


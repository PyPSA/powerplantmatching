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

from .utils import _data_out
from . import (config, cleaning, data, heuristics, export, matching, utils,
               collection, plot)
from .collection import matched_data as powerplants
import pandas as pd
import matplotlib.pyplot as plt
import os

@pd.api.extensions.register_dataframe_accessor("powerplant")
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
        subplots = True if len(by)>1 else False
        fig, ax = plt.subplots(figsize=figsize, **kwargs)
        data = utils.lookup(utils.convert_country_to_alpha2(self._obj), by=by)
        data = data.unstack().rename_axis(None) if subplots else data
        data.plot.bar(subplots=subplots, sharex=False, ax=ax)
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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 18:09:56 2019

@author: fabian
"""

import pandas

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
            rescale_capacities_to_country_totals, fill_missing_decommyears)
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



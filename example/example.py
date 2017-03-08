## Copyright 2015-2016 Fabian Gotzens (FZJ)

## This program is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 3 of the
## License, or (at your option) any later version.

## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.

## You should have received a copy of the GNU General Public License
## along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import powerplant_collection as pc

from .data import CARMA, ENTSOE, ESE, GEO, OPSD, WEPP, WRI
from .cleaning import clean_single
from .utils import lookup

# Invoke the plots
matched = pc.MATCHED_dataset()
Plot_bar_comparison_Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI()



def Plot_bar_comparison_Carma_ENTSOE_ESE_GEO_OPSD_WEPP_WRI(cleaned=False, use_saved_aggregation=False):
    '''
    Plots a bar chart with fueltypes on x-axis and capacity on y-axis,
    categorized by its originating database.
    '''
    if cleaned:
        carma = clean_single(CARMA(), use_saved_aggregation=use_saved_aggregation)
        entsoe = clean_single(ENTSOE(), use_saved_aggregation=use_saved_aggregation),
        geo = clean_single(GEO(), aggregate_powerplant_units=False),
        opsd = clean_single(OPSD(), use_saved_aggregation=use_saved_aggregation),
        wepp = clean_single(WEPP(), use_saved_aggregation=use_saved_aggregation),
        wri = clean_single(WRI(), use_saved_aggregation=use_saved_aggregation)
    else:
        carma = CARMA()
        entsoe = ENTSOE()
        geo = GEO()
        opsd = OPSD()
        wepp = WEPP()
        wri = WRI()
    ese = ESE()
    stats = lookup([carma, entsoe, ese, geo, opsd, wepp, wri],
                   keys=['CARMA','ENTSO-E','ESE','GEO','OPSD','WEPP','WRI'], by='Fueltype')/1000
    stats.plot.bar(stacked=False,  legend=True, figsize=(10,5))

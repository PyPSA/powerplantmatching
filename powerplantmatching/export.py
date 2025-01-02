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

# This export script is intended for the users of PyPSA:
#   https://www.pypsa.org/
# or the VEDA-TIMES modelling framework:
#   http://iea-etsap.org/index.php/etsap-tools/data-handling-shells/veda

import logging

import numpy as np
import pandas as pd
import pycountry
from deprecation import deprecated
from scipy.spatial import cKDTree as KDTree

from .core import _data_out, get_obj_if_Acc
from .heuristics import set_denmark_region_id, set_known_retire_years

logger = logging.getLogger(__name__)
cget = pycountry.countries.get


def to_pypsa_names(df):
    """Rename the columns of the powerplant data according to the
    convention in PyPSA.

    Arguments:
        df {pandas.DataFrame} -- powerplant data

    Returns:
        pandas.DataFrame -- Column renamed dataframe
    """
    df = get_obj_if_Acc(df)
    return df.assign(Fueltype=df["Fueltype"].str.lower()).rename(
        columns={
            "Fueltype": "carrier",
            "Capacity": "p_nom",
            "Duration": "max_hours",
            "Set": "component",
        }
    )


def map_bus(df, buses):
    """
    Assign a 'bus' column to the dataframe based on a list of coordinates.

    Parameters
    ----------
    df : pd.DataFrame
        power plant list with coordinates 'lat' and 'lon'
    buses : pd.DataFrame
        bus list with coordites 'x' and 'y'

    Returns
    -------
    DataFrame with an extra column 'bus' indicating the nearest bus.
    """
    df = get_obj_if_Acc(df)
    non_empty_buses = buses.dropna()
    kdtree = KDTree(non_empty_buses[["x", "y"]])
    if non_empty_buses.empty:
        buses_i = pd.Index([np.nan])
    else:
        buses_i = non_empty_buses.index.append(pd.Index([np.nan]))
    return df.assign(bus=buses_i[kdtree.query(df[["lon", "lat"]].values)[1]])


def map_country_bus(df, buses):
    """
    Assign a 'bus' column based on a list of coordinates and countries.

    Parameters
    ----------
    df : pd.DataFrame
        power plant list with coordinates 'lat', 'lon' and 'Country'
    buses : pd.DataFrame
        bus list with coordites 'x', 'y', 'country'

    Returns
    -------
    DataFrame with an extra column 'bus' indicating the nearest bus.
    """
    df = get_obj_if_Acc(df)
    diff = set(df.Country.unique()) - set(buses.country)
    if len(diff):
        logger.warning(
            f'Power plants in {", ".join(diff)} cannot be mapped '
            "because the countries do not appear in `buses`."
        )
    res = []
    for c in df.Country.unique():
        res.append(map_bus(df.query("Country == @c"), buses.query("country == @c")))
    return pd.concat(res)


def to_pypsa_network(df, network, buslist=None):
    """
    Export a powerplant dataframe to a pypsa.Network(), specify specific buses
    to allocate the plants (buslist).

    """
    df = get_obj_if_Acc(df)
    df = map_bus(df, network.buses.reindex(buslist))
    df["Set"] = df.Set.replace("CHP", "PP")
    if "Duration" in df:
        df["weighted_duration"] = df["Duration"] * df["Capacity"]
        df = df.groupby(["bus", "Fueltype", "Set"]).aggregate(
            {"Capacity": sum, "weighted_duration": sum}
        )
        df = df.assign(Duration=df["weighted_duration"] / df["Capacity"])
        df = df.drop(columns="weighted_duration")
    else:
        df = df.groupby(["bus", "Fueltype", "Set"]).aggregate({"Capacity": sum})
    df = df.reset_index()
    df = to_pypsa_names(df)
    df.index = df.bus + " " + df.carrier
    network.import_components_from_dataframe(
        df[df["component"] != "Store"], "Generator"
    )
    network.import_components_from_dataframe(
        df[df["component"] == "Store"], "StorageUnit"
    )


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function is not maintained anymore and will be removed in the future.",
)
def to_TIMES(df=None, use_scaled_capacity=False, baseyear=2015):
    """
    Transform a given dataset into the TIMES format and export as .xlsx.
    """
    if df is None:
        from .collection import matched_data

        df = matched_data()
        if df is None:
            raise RuntimeError("The data to be exported does not yet exist.")
    df = df.loc[(df.DateIn.isnull()) | (df.DateIn <= baseyear)]
    plausible = True

    # Set region via country names by iso3166-2 codes
    if "Region" not in df:
        pos = [i for i, x in enumerate(df.columns) if x == "Country"][0]
        df.insert(pos + 1, "Region", np.nan)
    df.Country = df.Country.replace({"Czech Republic": "Czechia"})
    df["Region"] = df.Country.apply(lambda c: cget(name=c).alpha_2)
    df = set_denmark_region_id(df)
    regions = sorted(set(df.Region))
    if None in regions:
        raise ValueError(
            "There are rows without a valid country identifier "
            "in the dataframe. Please check!"
        )

    # add column with TIMES-specific type. The pattern is as follows:
    # 'ConELC-' + Set + '_' + Fueltype + '-' Technology
    df["Technology"].fillna("", inplace=True)
    if "TimesType" not in df:
        pos = [i for i, x in enumerate(df.columns) if x == "Technology"][0]
        df.insert(pos + 1, "TimesType", np.nan)
    df["TimesType"] = (
        pd.Series("ConELC-" for _ in range(len(df)))
        + np.where(df.loc[:, "Set"].str.contains("CHP"), "CHP", "PP")
        + "_"
        + df.loc[:, "Fueltype"].map(fueltype_to_abbrev())
    )
    df.loc[
        (df.Fueltype == "Wind") & (df.Technology.str.contains("offshore", case=False)),
        "TimesType",
    ] += "F"
    df.loc[
        (df.Fueltype == "Wind") & ~(df.Technology.str.contains("offshore", case=False)),
        "TimesType",
    ] += "N"
    df.loc[
        (df.Fueltype == "Solar") & (df.Technology.str.contains("CSP", case=False)),
        "TimesType",
    ] += "CSP"
    df.loc[
        (df.Fueltype == "Solar") & ~(df.Technology.str.contains("CSP", case=False)),
        "TimesType",
    ] += "SPV"
    df.loc[
        (df.Fueltype == "Natural Gas")
        & (df.Technology.str.contains("CCGT", case=False)),
        "TimesType",
    ] += "-CCGT"
    df.loc[
        (df.Fueltype == "Natural Gas")
        & ~(df.Technology.str.contains("CCGT", case=False))
        & (df.Technology.str.contains("OCGT", case=False)),
        "TimesType",
    ] += "-OCGT"
    df.loc[
        (df.Fueltype == "Natural Gas")
        & ~(df.Technology.str.contains("CCGT", case=False))
        & ~(df["Technology"].str.contains("OCGT", case=False)),
        "TimesType",
    ] += "-ST"
    df.loc[
        (df.Fueltype == "Hydro")
        & (df.Technology.str.contains("pumped storage", case=False)),
        "TimesType",
    ] += "-PST"
    df.loc[
        (df.Fueltype == "Hydro")
        & (df.Technology.str.contains("run-of-river", case=False))
        & ~(df.Technology.str.contains("pumped storage", case=False)),
        "TimesType",
    ] += "-ROR"
    df.loc[
        (df.Fueltype == "Hydro")
        & ~(df.Technology.str.contains("run-of-river", case=False))
        & ~(df.Technology.str.contains("pumped storage", case=False)),
        "TimesType",
    ] += "-STO"

    if None in set(df.TimesType):
        raise ValueError(
            "There are rows without a valid TIMES-Type "
            "identifier in the dataframe. Please check!"
        )

    # add column with technical lifetime
    if "Life" not in df:
        pos = [i for i, x in enumerate(df.columns) if x == "Retrofit"][0]
        df.insert(pos + 1, "Life", np.nan)
    df["Life"] = df.TimesType.map(timestype_to_life())
    if df.Life.isnull().any():
        raise ValueError(
            "There are rows without a given lifetime in the " "dataframe. Please check!"
        )

    # add column with decommissioning year
    if "YearRetire" not in df:
        pos = [i for i, x in enumerate(df.columns) if x == "Life"][0]
        df.insert(pos + 1, "YearRetire", np.nan)
    df["YearRetire"] = df.loc[:, "Retrofit"] + df.loc[:, "Life"]
    df = set_known_retire_years(df)

    # Now create empty export dataframe with headers
    columns = ["Attribute", "*Unit", "LimType", "Year"]
    columns.extend(regions)
    columns.append("Pset_Pn")

    # Loop stepwise through technologies, years and countries
    df_exp = pd.DataFrame(columns=columns)
    cap_column = "Scaled Capacity" if use_scaled_capacity else "Capacity"
    row = 0
    for tt, df_tt in df.groupby("TimesType"):
        for yr in range(baseyear, 2055, 5):
            df_exp.loc[row, "Year"] = yr
            data_regions = df_tt.groupby("Region")
            for reg in regions:
                if reg in data_regions.groups:
                    ct_group = data_regions.get_group(reg)
                    # Here, all matched units existing in the dataset are being
                    # considered. This is needed since there can be units in
                    # the system which are actually already beyond their
                    # assumed technical lifetimes but still online in baseyear.
                    if yr == baseyear:
                        series = ct_group.apply(lambda x: x[cap_column], axis=1)
                    # Here all matched units that are not retired in yr,
                    # are being filtered.
                    elif yr > baseyear:
                        series = ct_group.apply(
                            lambda x: (
                                x[cap_column]
                                if yr >= x["DateIn"] and yr <= x["YearRetire"]
                                else 0
                            ),
                            axis=1,
                        )
                    else:
                        message = "loop yr({}) below baseyear({})"
                        raise ValueError(message.format(yr, baseyear))
                    # Divide the sum by 1000 (MW->GW) and write into export df
                    df_exp.loc[row, reg] = series.sum() / 1000.0
                else:
                    df_exp.loc[row, reg] = 0.0
                # Plausibility-Check:
                if yr > baseyear and (df_exp.loc[row, reg] > df_exp.loc[row - 1, reg]):
                    plausible = False
                    logger.error(
                        "For region '{}' and timestype '{}' the value for "
                        "year {} ({0.000}) is higher than in the year before "
                        "({0.000}).",
                        reg,
                        tt,
                        yr,
                    )
            df_exp.loc[row, "Pset_Pn"] = tt
            row += 1
    df_exp["Attribute"] = "STOCK"
    df_exp["*Unit"] = "GW"
    df_exp["LimType"] = "FX"

    # Write resulting dataframe to file
    if plausible:
        df_exp.to_excel(_data_out("Export_Stock_TIMES.xlsx"))
    return df_exp


@deprecated(deprecated_in="0.5.0", removed_in="0.6.0")
def store_open_dataset():
    from .collection import matched_data, reduce_matched_dataframe

    m = matched_data(reduced=False).reindex(
        columns=["CARMA", "ENTSOE", "GEO", "GPD", "OPSD"], level=1
    )[lambda df: df.Name.notnull().any(1)]
    m.to_csv(_data_out("powerplants_large.csv"))
    m = m.pipe(reduce_matched_dataframe)
    m.to_csv(_data_out("powerplants.csv"))
    return m


@deprecated(deprecated_in="0.5.0", removed_in="0.6.0")
def fueltype_to_abbrev():
    """
    Return the fueltype-specific abbreviation.
    """
    data = {
        "Solid Biomass": "BIO",
        "Biogas": "BIG",
        "Geothermal": "GEO",
        "Hard Coal": "COA",
        "Hydro": "HYD",
        "Lignite": "LIG",
        "Natural Gas": "NG",
        "Nuclear": "NUC",
        "Oil": "OIL",
        "Other": "OTH",
        "Solar": "",  # DO NOT delete this entry!
        "Waste": "WST",
        "Wind": "WO",
    }
    return data


@deprecated(deprecated_in="0.5.0", removed_in="0.6.0")
def timestype_to_life():
    """
    Returns the timestype-specific technical lifetime.
    """
    return {
        "ConELC-PP_COA": 45,
        "ConELC-PP_LIG": 45,
        "ConELC-PP_NG-OCGT": 40,
        "ConELC-PP_NG-ST": 40,
        "ConELC-PP_NG-CCGT": 40,
        "ConELC-PP_OIL": 40,
        "ConELC-PP_NUC": 50,
        "ConELC-PP_BIO": 25,
        "ConELC-PP_HYD-ROR": 200,  # According to A.K. Riekkolas comment,
        "ConELC-PP_HYD-STO": 200,  # these will not retire after 75-100 a,
        "ConELC-PP_HYD-PST": 200,  # but exist way longer at retrofit costs
        "ConELC-PP_WON": 25,
        "ConELC-PP_WOF": 25,
        "ConELC-PP_SPV": 30,
        "ConELC-PP_CSP": 30,
        "ConELC-PP_WST": 30,
        "ConELC-PP_SYN": 5,
        "ConELC-PP_CAES": 40,
        "ConELC-PP_GEO": 30,
        "ConELC-PP_OTH": 5,
        "ConELC-CHP_COA": 45,
        "ConELC-CHP_LIG": 45,
        "ConELC-CHP_NG-OCGT": 40,
        "ConELC-CHP_NG-ST": 40,
        "ConELC-CHP_NG-CCGT": 40,
        "ConELC-CHP_OIL": 40,
        "ConELC-CHP_BIO": 25,
        "ConELC-CHP_WST": 30,
        "ConELC-CHP_SYN": 5,
        "ConELC-CHP_GEO": 30,
        "ConELC-CHP_OTH": 5,
    }

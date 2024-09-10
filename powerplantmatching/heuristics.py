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
Functions to modify and adjust power plant datasets
"""

import logging

import numpy as np
import pandas as pd
from deprecation import deprecated
from six import iteritems

from .core import _package_data, get_config, get_obj_if_Acc
from .utils import lookup

logger = logging.getLogger(__name__)


def extend_by_non_matched(
    df,
    extend_by,
    label=None,
    query=None,
    aggregate_added_data=True,
    config=None,
    **aggkwargs,
):
    """
    Returns the matched dataframe with additional entries of non-matched
    powerplants of a reliable source.

    Parameters
    ----------
    df : Pandas.DataFrame
        Already matched dataset which should be extended
    extend_by : pd.DataFrame | str
        Database which is partially included in the matched dataset, but
        which should be included totally. If str is passed, is will be used
        to call the corresponding data from data.py
    label : str
        Column name of the additional database within the matched dataset, this
        string is used if the columns of the additional database do not
        correspond to the ones of the dataset
    """
    from . import data
    from .cleaning import aggregate_units

    df = get_obj_if_Acc(df)

    if config is None:
        config = get_config()

    if isinstance(extend_by, str):
        label = extend_by
        extend_by = getattr(data, extend_by)(config=config)

    if query is not None:
        extend_by.query(query, inplace=True)

    # Fully included queries might lead to disjunct datasets
    if extend_by.empty:
        return df

    is_included = isin(extend_by, df, label=label)
    extend_by = extend_by[~is_included]

    if aggregate_added_data and not extend_by.empty:
        extend_by = aggregate_units(
            extend_by, dataset_name=label, config=config, **aggkwargs
        )
        extend_by["projectID"] = extend_by.projectID.map(lambda x: {label: x})
    else:
        extend_by = extend_by.assign(
            projectID=extend_by.projectID.map(lambda x: {label: [x]})
        )

    if df.columns.nlevels > 1:
        extend_by = (
            pd.concat([extend_by], keys=[label], axis=1)
            .swaplevel(axis=1)
            .reindex(columns=df.columns)
        )
        return pd.concat([df, extend_by], ignore_index=True)
    else:
        return pd.concat([df, extend_by.reindex(columns=df.columns)], ignore_index=True)


def isin(df, matched, label=None):
    """
    Checks if a given dataframe is included in a matched dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to be checked
    matched : pd.DataFrame
        The matched dataframe

    Returns
    -------
    bool
        True if all dataframes are included in the matched dataframe, False
        otherwise
    """
    df = get_obj_if_Acc(df)

    if not isinstance(df.projectID.iat[0], str):
        raise TypeError(
            "`projectID` contains multiple values per row. This is likely "
            "because the powerplants are already aggregated, please use a "
            "non-aggregated dataset."
        )

    if label is None:
        label = df.powerplant.get_name()
    assert label is not None, "No label given"

    if matched.columns.nlevels > 1:
        included_ids = matched["projectID", label].dropna().apply(list).sum()
    else:
        included_ids = (
            matched.projectID.map(lambda d: d.get(label)).dropna().apply(list).sum()
        )
    if included_ids == 0:
        included_ids = []

    return df.projectID.isin(included_ids)


def rescale_capacities_to_country_totals(df, fueltypes=None):
    """
    Returns a extra column 'Scaled Capacity' with an up or down scaled capacity
    in order to match the statistics of the ENTSOe country totals. For every
    country the information about the total capacity of each fueltype is given.
    The scaling factor is determined by the ratio of the aggregated capacity of
    the fueltype within each country and the ENTSOe statistics about the
    fueltype capacity total within each country.

    Parameters
    ----------
    df : Pandas.DataFrame
        Data set that should be modified
    fueltype : str or list of strings
        fueltype that should be scaled
    """
    from .data import Capacity_stats

    df = get_obj_if_Acc(df)
    df = df.copy()
    if fueltypes is None:
        fueltypes = df.Fueltype.unique()
    if isinstance(fueltypes, str):
        fueltypes = [fueltypes]
    stats_df = lookup(df).loc[fueltypes]
    stats_entsoe = lookup(Capacity_stats()).loc[fueltypes]
    if ((stats_df == 0) & (stats_entsoe != 0)).any().any():
        country_list = stats_df.loc[
            :, ((stats_df == 0) & (stats_entsoe != 0)).any()
        ].columns.tolist()
        print(
            f"Could not scale powerplants in the countries {country_list} because of "
            f"no occurring power plants in these countries"
        )
    ratio = (stats_entsoe / stats_df).fillna(1)
    df["Scaled Capacity"] = df.loc[:, "Capacity"]
    for country in ratio:
        for fueltype in fueltypes:
            df.loc[
                (df.Country == country) & (df.Fueltype == fueltype), "Scaled Capacity"
            ] *= ratio.loc[fueltype, country]
    return df


def fill_missing_duration(df):
    df = get_obj_if_Acc(df)
    mean_duration = df[df.Set == "Store"].groupby("Fueltype").Duration.mean()
    df = get_obj_if_Acc(df)
    for store in mean_duration.index:
        df.loc[(df["Set"] == "Store") & (df["Fueltype"] == store), "Duration"] = (
            mean_duration.at[store]
        )
    return df


def extend_by_VRE(df, config=None, base_year=2017, prune_beyond=True):
    """
    Extends a given reduced dataframe by externally given VREs.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe to be extended
    base_year : int
        Needed for deriving cohorts from IRENA's capacity statistics

    Returns
    -------
    df : pd.DataFrame
         Extended dataframe
    """
    from .data import OPSD_VRE

    df = get_obj_if_Acc(df)
    config = get_config() if config is None else config

    vre = (
        OPSD_VRE(config=config)
        .query('Fueltype != "Hydro"')
        .reindex(columns=config["target_columns"])
    )
    return pd.concat([df, vre], sort=False)


def fill_missing_commissioning_years(df):
    """
    Fills the empty commissioning years with averages.
    """
    df = get_obj_if_Acc(df)
    df = df.copy()
    # 1st try: Fill with both country- and fueltypespecific averages
    df["DateIn"] = df.DateIn.fillna(
        df.groupby(["Country", "Fueltype"]).DateIn.transform("mean")
    )
    # 2nd try: Fill remaining with only fueltype-specific average
    df["DateIn"] = df.DateIn.fillna(df.groupby(["Fueltype"]).DateIn.transform("mean"))
    # 3rd try: Fill remaining with only country-specific average
    df["DateIn"] = df.DateIn.fillna(df.groupby(["Country"]).DateIn.transform("mean"))
    if df.DateIn.isnull().any():
        count = len(df[df.DateIn.isnull()])
        logger.warning(
            f"""There are still *{count}* empty values for
                        'DateIn' in the DataFrame. These should
                        be either be filled manually or dropped.
            """
        )
    df["DateIn"] = df.DateIn.astype(float)
    df["DateRetrofit"] = df.DateRetrofit.fillna(df.DateIn)
    return df


def fill_missing_decommissioning_years(df, config=None):
    """
    Function which sets/fills a column 'DateOut' with roughly
    estimated values for decommissioning years, based on the estimated lifetimes
    per `Fueltype` given in the config and corresponding commissioning years.
    Note that the latter is filled up using `fill_missing_commissioning_years`.
    """
    df = get_obj_if_Acc(df)
    if config is None:
        config = get_config()
    if "DateOut" not in df:
        df = df.reindex(columns=list(df.columns) + ["DateOut"])
    lifetime = df.Fueltype.map(config["fuel_to_lifetime"])
    df = fill_missing_commissioning_years(df)
    df["DateOut"] = df.DateOut.fillna(
        df[["DateIn", "DateRetrofit"]].max(1) + lifetime
    ).astype(float)
    return df


def aggregate_VRE_by_commissioning_year(df, target_fueltypes=None, agg_geo_by=None):
    """
    Aggregate the vast number of VRE (e.g. vom data.OPSD_VRE()) units to one
    specific (Fueltype + Technology) cohorte per commissioning year.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the data to aggregate
    target_fueltypes : list
        list of fueltypes to be aggregated (Others are cut!)
    agg_by_geo : str
        How to deal with lat/lon positions. Allowed:
            NoneType : Do not show geoposition at all
            'mean'   : Average geoposition
            'wm'     : Average geoposition weighted by capacity
    """
    df = df.copy()
    if agg_geo_by is None:
        f = {"Capacity": ["sum"]}
    elif agg_geo_by == "mean":
        f = {"Capacity": ["sum"], "lat": ["mean"], "lon": ["mean"]}
    elif agg_geo_by == "wm":
        # TODO: This does not work yet, when NaNs are in lat/lon columns.
        def wm(x):
            return np.average(x, weights=df.loc[x.index, "Capacity"])

        f = {
            "Capacity": ["sum"],
            "lat": {"weighted mean": wm},
            "lon": {"weighted mean": wm},
        }
    else:
        raise TypeError(
            f"Value given for `agg_geo_by` is '{agg_geo_by}' but must be either \
                        'NoneType' or 'mean' or 'wm'."
        )

    if target_fueltypes is None:
        target_fueltypes = ["Wind", "Solar", "Biogas", "Solid Biomass"]
    df = df[df.Fueltype.isin(target_fueltypes)]
    df = fill_missing_commissioning_years(df)
    df["Technology"] = df.Technology.fillna("-")
    df = (
        df.groupby(["Country", "DateIn", "Fueltype", "Technology"])
        .agg(f)
        .reset_index()
        .replace({"-": np.nan})
    )
    df.columns = df.columns.droplevel(level=1)
    return df.assign(Set="PP", DateRetrofit=df.DateIn)


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function was renamed to `fill_missing_commissioning_years`",
)
def fill_missing_commyears(df):
    return fill_missing_commissioning_years(df)


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function was renamed to `fill_missing_decommissioning_years`",
)
def fill_missing_decommyears(df, config=None):
    return fill_missing_decommissioning_years(df, config=config)


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function was renamed to `aggregate_VRE_by_commissioning_year`",
)
def aggregate_VRE_by_commyear(df, config=None):
    return aggregate_VRE_by_commissioning_year(df, config=config)


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function is not maintained anymore and will be removed in the future.",
)
def derive_vintage_cohorts_from_statistics(df, base_year=2015, config=None):
    """
    This function assumes an age-distribution for given capacity statistics
    and returns a df, containing how much of capacity has been built for every
    year.
    """

    def setInitial_Flat(mat, df, life):
        y_start = int(df.index[0])
        height_flat = float(df.loc[y_start].Capacity) / life
        for y in range(int(mat.index[0]), y_start + 1):
            y_end = min(y + life - 1, mat.columns[-1])
            mat.loc[y, y:y_end] = height_flat
        return mat

    def setInitial_Triangle(mat, df, life):
        y_start = int(df.index[0])
        years = range(y_start - life + 1, y_start + 1)
        height_flat = float(df.loc[y_start].Capacity) / life
        # decrement per period, 'slope' of the triangle
        decr = 2.0 * height_flat / life
        # height of triangle at right side
        height_tri = 2.0 * height_flat - decr / 2.0
        series = [(height_tri - i * decr) for i in range(0, life)][::-1]
        dic = dict(zip(years, series))  # create dictionary
        for y in range(int(mat.index[0]), y_start + 1):
            y_end = min(y + life - 1, mat.columns[-1])
            mat.loc[y, y:y_end] = dic[y]
        return mat

    def setHistorical(mat, df, life):
        # Base year was already handled in setInitial()->Start one year later.
        year = df.index[1]
        while year <= df.index.max():
            if year in df.index:
                addition = df.loc[year].Capacity - mat.loc[:, year].sum()
                if addition >= 0:
                    mat.loc[year, year : year + life - 1] = addition
                else:
                    mat.loc[year, year : year + life - 1] = 0
                    mat = reduceVintages(addition, mat, life, year)
            else:
                mat.loc[year, year : year + life - 1] = 0
            year += 1
        return mat

    def reduceVintages(addition, mat, life, y_pres):
        for year in mat.index:
            val_rem = float(mat.loc[year, y_pres])
            #            print ('In year %i are %.2f units left from year %i, while '
            #                   'addition delta is %.2f' % (y_pres, val_rem, year,
            #                                               addition))
            if val_rem > 0:
                if abs(addition) > val_rem:
                    mat.loc[year, y_pres : year + life - 1] = 0
                    addition += val_rem
                else:
                    mat.loc[year, y_pres : year + life - 1] = val_rem + addition
                    break
        return mat

    if config is None:
        config = get_config()

    dfe = pd.DataFrame(columns=df.columns)
    for c, df_country in df.groupby(["Country"]):
        for tech, dfs in df_country.groupby(["Technology"]):
            dfs.set_index("DateIn", drop=False, inplace=True)
            y_start = int(dfs.index[0])
            y_end = int(dfs.index[-1])
            life = config["fuel_to_lifetime"][dfs.Fueltype.iloc[0]]
            mat = pd.DataFrame(
                columns=range(y_start - life + 1, y_end + life),
                index=range(y_start - life + 1, y_end),
            ).astype(float)
            fuels = ["Solar", "Wind", "Biogas", "Solid Biomass", "Geothermal"]
            if dfs.Fueltype.iloc[0] in fuels:
                mat = setInitial_Triangle(mat, dfs, life)
            else:
                mat = setInitial_Flat(mat, dfs, life)
            if y_end > y_start:
                mat = setHistorical(mat, dfs, life)
            add = pd.DataFrame(columns=dfs.columns)
            add.Capacity = list(mat.loc[:, base_year])
            add.Year = mat.index.tolist()
            add.Technology = tech
            add.Country = c
            add.Fueltype = dfs.Fueltype.iloc[0]
            add.Set = dfs.Set.iloc[0]
            dfe = pd.concat([dfe, add[add.Capacity > 0.0]], ignore_index=True)
    dfe.Year = dfe.Year.apply(pd.to_numeric)
    dfe.rename(columns={"Year": "DateIn"}, inplace=True)
    dfe = dfe.assign(DateRetrofit=dfe.DateIn)
    return dfe[~np.isclose(dfe.Capacity, 0)]


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function is not maintained anymore and will be removed in the future.",
)
def set_denmark_region_id(df):
    """
    Used to set the Region column to DKE/DKW (East/West) for electricity models
    based on lat,lon-coordinates and a heuristic for unknowns.
    """
    if "Region" not in df:
        pos = [i for i, x in enumerate(df.columns) if x == "Country"][0]
        df.insert(pos + 1, "Region", np.nan)
    else:
        if ("DKE" in set(df.Region)) | ("DKW" in set(df.Region)):
            return df
        df.loc[(df.Country == "Denmark"), "Region"] = np.nan
    # TODO: This does not work yet.
    # import geopandas as gpd
    # df = gpd.read_file('/tmp/ne_10m_admin_0_countries/')
    # df = df.query("ISO_A2 != '-99'").set_index('ISO_A2')
    # Point(9, 52).within(df.loc['DE', 'geometry'])
    # Workaround:
    df.loc[(df.Country == "Denmark") & (df.lon >= 10.96), "Region"] = "DKE"
    df.loc[(df.Country == "Denmark") & (df.lon < 10.96), "Region"] = "DKW"
    df.loc[df.Name.str.contains("Jegerspris", case=False).fillna(False), "Region"] = (
        "DKE"
    )
    df.loc[df.Name.str.contains("Jetsmark", case=False).fillna(False), "Region"] = "DKW"
    df.loc[df.Name.str.contains("Fellinggard", case=False).fillna(False), "Region"] = (
        "DKW"
    )
    # Copy the remaining ones without Region and handle in copy
    dk_o = df.loc[(df.Country == "Denmark") & (df.Region.isnull())].reset_index(
        drop=True
    )
    dk_o["Capacity"] *= 0.5
    dk_o["Region"] = "DKE"
    # Handle remaining in df
    df.loc[(df.Country == "Denmark") & (df.Region.isnull()), "Capacity"] *= 0.5
    df.loc[(df.Country == "Denmark") & (df.Region.isnull()), "Region"] = "DKW"
    # Concat
    df = pd.concat([df, dk_o], ignore_index=True)
    return df


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function is not maintained anymore and will be removed in the future.",
)
def remove_oversea_areas(df, lat=[36, 72], lon=[-10.6, 31]):
    """
    Remove plants outside continental Europe such as the Canarian Islands etc.
    """
    df = df.loc[
        (df.lat.isnull() | df.lon.isnull())
        | (
            (df.lat >= lat[0])
            & (df.lat <= lat[1])
            & (df.lon >= lon[0])
            & (df.lon <= lon[1])
        )
    ]
    return df


def gross_to_net_factors(reference="opsd", aggfunc="median", return_entire_data=False):
    """ """
    from .cleaning import gather_technology_info

    if reference == "opsd":
        from .data import OPSD

        reference = OPSD(raw=True)["DE"]
    df = reference.copy()
    df = df[df.capacity_gross_uba.notnull() & df.capacity_net_bnetza.notnull()]
    df["ratio"] = df.capacity_net_bnetza / df.capacity_gross_uba
    df = df[df.ratio <= 1.0]  # drop obvious data errors
    if return_entire_data:
        return df
    else:
        df["energy_source_level_2"] = df.energy_source_level_2.fillna(
            value=df.energy_source
        )
        df.replace(
            dict(
                energy_source_level_2={
                    "Biomass and biogas": "Biogas",
                    "Fossil fuels": "Other",
                    "Mixed fossil fuels": "Other",
                    "Natural gas": "Natural Gas",
                    "Non-renewable waste": "Waste",
                    "Other bioenergy and renewable waste": "Solid Biomass",
                    "Other or unspecified energy sources": "Other",
                    "Other fossil fuels": "Other",
                    "Other fuels": "Other",
                }
            ),
            inplace=True,
        )
        df.rename(columns={"technology": "Technology"}, inplace=True)
        df = gather_technology_info(df, ["Technology", "energy_source_level_2"])
        df = df.assign(
            energy_source_level_2=lambda df: df.energy_source_level_2.str.title()
        )
        ratios = df.groupby(["energy_source_level_2", "Technology"]).ratio.mean()
        return ratios


def scale_to_net_capacities(df, is_gross=True, catch_all=True):
    df = get_obj_if_Acc(df)
    if is_gross:
        factors = gross_to_net_factors()
        for ftype, tech in factors.index.values:
            df.loc[(df.Fueltype == ftype) & (df.Technology == tech), "Capacity"] *= (
                factors.loc[(ftype, tech)]
            )
        if catch_all:
            for ftype in factors.index.levels[0]:
                techs = factors.loc[ftype].index.tolist()
                df.loc[
                    (df.Fueltype == ftype) & (~df.Technology.isin(techs)), "Capacity"
                ] *= factors.loc[ftype].mean()
        return df
    else:
        return df


def PLZ_to_LatLon_map():
    return pd.read_csv(_package_data("PLZ_Coords_map.csv"), index_col="PLZ")


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function is not maintained anymore and will be removed in the future.",
)
def set_known_retire_years(df):
    """
    Integrate known retire years, e.g. for German nuclear plants with fixed
    decommissioning dates.
    """

    if "YearRetire" not in df:
        df["YearRetire"] = np.nan

    YearRetire = {
        "Grafenrheinfeld": 2015,
        "Philippsburg": 2019,
        "Brokdorf": 2021,
        "Grohnde": 2021,
        "Gundremmingen": 2021,
        "Emsland": 2022,
        "Isar": 2022,
        "Neckarwestheim": 2022,
    }

    ppl_de_nuc = pd.DataFrame(
        df.loc[
            (df.Country == "Germany") & (df.Fueltype == "Nuclear"),
            ["Name", "YearRetire"],
        ]
    )
    for name, year in iteritems(YearRetire):
        name_match_b = ppl_de_nuc.Name.str.contains(name, case=False, na=False)
        if name_match_b.any():
            ppl_de_nuc.loc[name_match_b, "YearRetire"] = year
        else:
            logger.warning(f"'{name}' was not found in given DataFrame.")
    df.loc[ppl_de_nuc.index, "YearRetire"] = ppl_de_nuc["YearRetire"]
    return df

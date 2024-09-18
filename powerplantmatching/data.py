# Copyright 2016-2020 Fabian Hofmann (FIAS), Jonas Hoersch (KIT, IAI) and
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
Collection of power plant data bases and statistical data
"""

import logging
import os
from zipfile import ZipFile

import entsoe
import numpy as np
import pandas as pd
import pycountry
import requests
from deprecation import deprecated

from .cleaning import (
    clean_name,
    gather_fueltype_info,
    gather_set_info,
    gather_specifications,
    gather_technology_info,
)
from .core import _package_data, get_config
from .heuristics import scale_to_net_capacities
from .utils import (
    config_filter,
    convert_to_short_name,
    correct_manually,
    get_raw_file,
    set_column_name,
)

logger = logging.getLogger(__name__)
cget = pycountry.countries.get
net_caps = get_config()["display_net_caps"]


def BEYONDCOAL(raw=False, update=False, config=None):
    """
    Importer for the BEYOND COAL database.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """

    config = get_config() if config is None else config

    fn = get_raw_file("BEYONDCOAL", update=update, config=config)
    df = pd.read_excel(fn, sheet_name="Plant", header=[0, 1, 2], skiprows=[3])

    if raw:
        return df

    RENAME_COLUMNS = {
        "Plant name": "Name",
        "Fuel type": "Fueltype",
        "Latitude": "lat",
        "Longitude": "lon",
        "Commissioning year of first unit": "DateIn",
        "(Announced) Retirement year of last unit": "DateOut",
        "Coal capacity open": "Capacity",
        "Plant status\n(gross)": "status",
        "EBC plant ID": "projectID",
    }

    phaseout_col = "Covered by country phase-out? [if yes: country phase-out year]"

    df = (
        df["Plant Data"]
        .droplevel(1, axis=1)
        .rename(columns=RENAME_COLUMNS)
        .query('status != "Cancelled"')
        .assign(
            DateOut=lambda df: df.DateOut.fillna(df[phaseout_col]).where(
                lambda ds: ds <= 8000
            ),
            projectID=lambda df: "BEYOND-" + df.projectID,
            Fueltype=lambda df: df.Fueltype.str.title().replace("Unknown", "Other"),
            Set="PP",
            Technology=np.nan,
        )
        .pipe(scale_to_net_capacities)
        .pipe(clean_name)
        .pipe(convert_to_short_name)
        .pipe(set_column_name, "BEYONDCOAL")
        .pipe(config_filter, config)
    )
    return df


def OPSD(
    raw=False,
    update=False,
    statusDE=None,
    config=None,
    **fill_geoposition_kwargs,
):
    """
    Importer for the OPSD (Open Power Systems Data) database.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return a dictionary of the raw databases.
    update: bool, default False
        Whether to update the data from the url.
    statusDE : list, default ['operating', 'reserve', 'special_case']
        Filter DE entries by operational status ['operating', 'shutdown',
        'reserve', etc.]
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    fill_geoposition_kwargs:
        Keyword arguments for fill_geoposition.
    """
    config = get_config() if config is None else config

    opsd_DE = pd.read_csv(get_raw_file("OPSD_DE", update, config), na_values=" ")
    opsd_EU = pd.read_csv(get_raw_file("OPSD_EU", update, config), na_values=" ")

    if raw:
        return {"EU": opsd_EU, "DE": opsd_DE}

    EU_RENAME_COLUMNS = {
        "Lat": "lat",
        "Lon": "lon",
        "Energy_Source": "Fueltype",
        "Commissioned": "DateIn",
        "Retrofit": "DateRetrofit",
        "Shutdown": "DateOut",
        "Efficiency_Estimate": "Efficiency",
        "Eic_Code": "EIC",
        "Chp": "Set",
    }

    DE_RENAME_COLUMNS = {
        "Lat": "lat",
        "Lon": "lon",
        "Energy_Source": "Fueltype",
        # "Type": "Set",
        "Country_Code": "Country",
        "Capacity_Net_Bnetza": "Capacity",
        "Commissioned": "DateIn",
        "Retrofit": "DateRetrofit",
        "Shutdown": "DateOut",
        "Efficiency_Estimate": "Efficiency",
        "Eic_Code_Plant": "EIC",
        "Chp": "Set",
        "Id": "projectID",
    }

    opsd_EU = (
        opsd_EU.rename(columns=str.title)
        .rename(columns=EU_RENAME_COLUMNS)
        .eval("DateRetrofit = DateIn")
        .query("Country != 'DE'")
        .assign(
            Name=lambda df: df.Name.str.replace("\x96", " "),  # for geoparsing
            projectID=lambda s: "OEU-" + s.index.astype(str),
            Fueltype=lambda d: d.Fueltype.fillna(d.Energy_Source_Level_1),
            # We don't want to include this as this is overestimating CHP capacities
            # Set=lambda df: np.where(df.Set.isin(["yes", "Yes"]), "CHP", "PP"),
        )
        .reindex(columns=config["target_columns"])
    )

    opsd_DE = (
        opsd_DE.rename(columns=str.title)
        .rename(columns=DE_RENAME_COLUMNS)
        .assign(
            Name=lambda d: d.Name_Bnetza.fillna(d.Name_Uba),
            Fueltype=lambda d: d.Fueltype.fillna(d.Energy_Source_Level_1),
            DateRetrofit=lambda d: d.DateRetrofit.fillna(d.DateIn),
            # We don't want to include this as this is overestimating CHP capacities
            # Set=lambda df: np.where(df.Set.isin(["yes", "Yes"]), "CHP", "PP"),
        )
    )
    if statusDE is not None:
        opsd_DE = opsd_DE.loc[opsd_DE.Status.isin(statusDE)]

    opsd_DE = opsd_DE.reindex(columns=config["target_columns"])

    return (
        pd.concat([opsd_EU, opsd_DE], ignore_index=True)
        .replace(
            {
                "Country": {"UK": "GB", "[ \t]+|[ \t]+$.": ""},
                "Capacity": {0.0: np.nan},
            },
            regex=True,
        )
        .pipe(gather_specifications, config=config)
        .pipe(clean_name)
        .dropna(subset=["Capacity"])
        .powerplant.convert_alpha2_to_country()
        # .pipe(fill_geoposition, **fill_geoposition_kwargs)
        .pipe(set_column_name, "OPSD")
        .pipe(config_filter, config)
    )


def GEO(raw=False, update=False, config=None):
    """
    Importer for the GEO database.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config

    UNITS_RENAME_COLS = {
        "GEO_Assigned_Identification_Number": "projectID",
        "Capacity_MWe_nbr": "Capacity",
        "Date_Commissioned_dt": "DateIn",
        "Decommission_Date_dt": "DateOut",
        "Unit_Efficiency_Percent": "Efficiency",
    }

    PPL_RENAME_COLS = {
        "GEO_Assigned_Identification_Number": "projectID",
        "Name": "Name",
        "Type": "Fueltype",
        "Type_of_Plant_rng1": "Technology",
        "Type_of_Fuel_rng1_Primary": "FuelClassification1",
        "Type_of_Fuel_rng2_Secondary": "FuelClassification2",
        "Country": "Country",
        "Design_Capacity_MWe_nbr": "Capacity",
        "Year_Project_Commissioned": "DateIn",
        "Year_rng1_yr1": "DateRetrofit",
        "Longitude_Start": "lon",
        "Latitude_Start": "lat",
    }

    def to_year(ds):
        years = pd.to_numeric(ds.dropna().astype(str).str[:4], errors="coerce")
        year = years[lambda x: x > 1900]  # noqa
        return years.reindex_like(ds)

    fn = get_raw_file("GEO_units", update=update, config=config)
    units = pd.read_csv(fn, low_memory=False)

    fn = get_raw_file("GEO", update=update, config=config)
    ppl = pd.read_csv(fn, low_memory=False)

    if raw:
        return {"Units": units, "Plants": ppl}

    date_cols = ["Date_Commissioned_dt", "Decommission_Date_dt"]
    units[date_cols] = units[date_cols].apply(to_year)
    units = units.rename(columns=UNITS_RENAME_COLS)
    units.Efficiency = units.Efficiency.str.replace("%", "").astype(float) / 100

    date_cols = ["Year_Project_Commissioned", "Year_rng1_yr1"]
    ppl[date_cols] = ppl[date_cols].apply(to_year)
    ppl = ppl.rename(columns=PPL_RENAME_COLS)
    cols = [
        "Name",
        "Fueltype",
        "Technology",
        "FuelClassification1",
        "FuelClassification2",
    ]
    ppl = gather_specifications(ppl, parse_columns=cols)
    ppl = clean_name(ppl)

    res = units.join(ppl.set_index("projectID"), "projectID", rsuffix="_ppl")
    res["DateIn"] = res.DateIn.fillna(res.DateIn_ppl)
    not_included_ppl = ppl.query("projectID not in @res.projectID")
    res = pd.concat([res, not_included_ppl]).pipe(set_column_name, "GEO")
    res = scale_to_net_capacities(res)
    res = convert_to_short_name(res)
    res = set_column_name(res, "GEO")
    res = config_filter(res, config)
    res["projectID"] = "GEO-" + res.projectID.astype(str)

    return res


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="Removed since data is not publicly available anymore",
)
def CARMA(raw=False, update=False, config=None):
    """
    Importer for the Carma database.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config

    carma = pd.read_csv(get_raw_file("CARMA", update, config), low_memory=False)
    if raw:
        return carma

    return (
        carma.rename(
            columns={
                "Geoposition": "Geoposition",
                "cap": "Capacity",
                "city": "location",
                "country": "Country",
                "fuel1": "Fueltype",
                "lat": "lat",
                "lon": "lon",
                "plant": "Name",
                "plant.id": "projectID",
            }
        )
        .assign(projectID=lambda df: "CARMA-" + df.projectID.astype(str))
        .loc[lambda df: df.Country.isin(config["target_countries"])]
        .replace(
            dict(
                Fueltype={
                    "COAL": "Hard Coal",
                    "WAT": "Hydro",
                    "FGAS": "Natural Gas",
                    "NUC": "Nuclear",
                    "FLIQ": "Oil",
                    "WIND": "Wind",
                    "EMIT": "Other",
                    "GEO": "Geothermal",
                    "WSTH": "Waste",
                    "SUN": "Solar",
                    "BLIQ": "Solid Biomass",
                    "BGAS": "Biogas",
                    "BSOL": "Solid Biomass",
                    "OTH": "Other",
                }
            )
        )
        .pipe(gather_specifications, config=config)
        .pipe(clean_name)
        .pipe(set_column_name, "CARMA")
        .drop_duplicates()
        .pipe(config_filter, config)
        .pipe(scale_to_net_capacities, not config["CARMA"]["net_capacity"])
    )


def JRC(raw=False, update=False, config=None):
    """
    Importer for the JRC Hydro-power plants database retrieves from
    https://github.com/energy-modelling-toolkit/hydro-power-database.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """

    config = get_config() if config is None else config

    fn = get_raw_file("JRC", update, config)

    df = pd.read_csv(fn)

    if raw:
        return df

    RENAME_COLUMNS = {
        "id": "projectID",
        "name": "Name",
        "installed_capacity_MW": "Capacity",
        "country_code": "Country",
        "type": "Technology",
        "dam_height_m": "DamHeight_m",
        "volume_Mm3": "Volume_Mm3",
        "storage_capacity_MWh": "StorageCapacity_MWh",
    }

    def set_large_spanish_stores_to_reservoirs(df):
        """
        Set large Spanish stores which are larger than 50GWh to reservoirs.

        These should will all assumed to be reservoirs.
        Used to fix https://github.com/PyPSA/powerplantmatching/issues/72"
        """
        where = (df.Country == "Spain") & (df.StorageCapacity_MWh > 50e3)
        technology = np.where(where, "Reservoir", df.Technology)
        return df.assign(Technology=technology)

    df = (
        df.rename(columns=RENAME_COLUMNS)
        .assign(projectID=lambda df: "JRC-" + df.projectID.astype(str))
        .eval("Duration = StorageCapacity_MWh / Capacity")
        .replace(
            dict(
                Technology={
                    "HDAM": "Reservoir",
                    "HPHS": "Pumped Storage",
                    "HROR": "Run-Of-River",
                }
            )
        )
        .assign(
            Set=lambda df: np.where(df.Technology == "Run-Of-River", "PP", "Store"),
            Fueltype="Hydro",
        )
        .drop(columns=["pypsa_id", "GEO"])
        .powerplant.convert_alpha2_to_country()
        .pipe(clean_name)
        .pipe(set_large_spanish_stores_to_reservoirs)
        .pipe(set_column_name, "JRC")
        .pipe(config_filter, config)
    )
    return df


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="Use the JRC data instead",
)
def IWPDCY(config=None):
    """
    This data is not yet available. Was extracted manually from
    the 'International Water Power & Dam Country Yearbook'.

    Parameters
    ----------
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config

    return (
        pd.read_csv(config["IWPDCY"]["fn"], encoding="utf-8", index_col="id")
        .assign(
            File="IWPDCY.csv",
            projectID=lambda df: "IWPDCY-" + df.index.astype(str),
        )
        .dropna(subset=["Capacity"])
        .pipe(set_column_name, "IWPDCY")
        .pipe(config_filter, config)
        .pipe(gather_set_info)
        .pipe(correct_manually, "IWPDCY", config=config)
    )


def Capacity_stats(
    raw=False,
    config=None,
    update=False,
    source="ENTSO-E SOAF",
    year=2015,
):
    """
    Standardize the aggregated capacity statistics provided by the ENTSO-E.

    Parameters
    ----------
    year : int
        Year of the data (range usually 2013-2017)
        (defaults to 2016)
    source : str
        Which statistics source from
        {'ENTSO-E Transparency Platform', 'EUROSTAT', ...}
        (defaults to 'ENTSO-E Transparency Platform')

    Returns
    -------
    df : pd.DataFrame
         Capacity statistics per country and fuel-type
    """
    if config is None:
        config = get_config()

    df = pd.read_csv(get_raw_file("Capacity_stats", update, config), index_col=0)

    if raw:
        return df

    if source:
        df = df.query("source == @source")
    else:
        source = "Capacity statistics"

    fueltypes = config["target_fueltypes"]  # noqa
    df = (
        df.query("year == @year")
        .rename(columns={"technology": "Fueltype"})
        .rename(columns=str.title)
        .powerplant.convert_alpha2_to_country()
        .pipe(gather_fueltype_info, config=config, search_col=["Fueltype"])
        .query("Fueltype in @fueltypes and Fueltype != 'Other'")
        .pipe(set_column_name, source.title())
    )
    return df


def GPD(raw=False, update=False, config=None, filter_other_dbs=True):
    """
    Importer for the `Global Power Plant Database`.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """
    config = get_config() if config is None else config

    fn = get_raw_file("GPD", update, config)
    key = "global_power_plant_database.csv"

    with ZipFile(fn, "r") as file:
        df = pd.read_csv(file.open(key), low_memory=False)

    if raw:
        return df

    RENAME_COLS = {
        "Gppd_Idnr": "projectID",
        "Country_Long": "Country",
        "Primary_Fuel": "Fueltype",
        "Latitude": "lat",
        "Longitude": "lon",
        "Capacity_Mw": "Capacity",
        "Commissioning_Year": "DateIn",
    }

    other_dbs = []
    if filter_other_dbs:
        other_dbs = ["GEODB", "Open Power System Data", "ENTSOE"]  # noqa
    countries = config["target_countries"]  # noqa
    return (
        df.rename(columns=lambda x: x.title())
        .drop(columns="Country")
        .rename(columns=RENAME_COLS)
        .pipe(convert_to_short_name)
        .query("Country in @countries &" " Source not in @other_dbs")
        .pipe(
            gather_specifications,
            parse_columns=["Name", "Fueltype"],
            config=config,
        )
        .pipe(clean_name)
        .pipe(set_column_name, "GPD")
        .pipe(config_filter, config)
        .pipe(gather_technology_info, config=config)
    )


def WIKIPEDIA(raw=False, update=False, config=None):
    """
    Importer for the WIKIPEDIA nuclear power plant database.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """

    config = get_config() if config is None else config

    fn = get_raw_file("WIKIPEDIA", update=update, config=config)
    df = pd.read_csv(fn, index_col=0)

    if raw:
        return df

    RENAME_COLUMNS = {
        "Net performance MW": "Capacity",
        "country": "Country",
        "decommission_year": "DateOut",
        "commission_year": "DateIn",
    }

    df = (
        df.rename(columns=RENAME_COLUMNS)
        .pipe(clean_name)
        .pipe(convert_to_short_name)
        .assign(
            Fueltype="Nuclear",
            Set="PP",
            projectID=lambda df: "WIKIPEDIA-" + df.index.astype(str),
            # plants which are not yet built are set to 2027
            DateIn=lambda df: df.DateIn.where(~df.Status.str.contains("In Bau"), 2027),
        )
        .pipe(set_column_name, "WIKIPEDIA")
        .pipe(config_filter, config)
    )
    return df


def ENTSOE(
    raw=False,
    update=False,
    config=None,
    entsoe_token=None,
    **fill_geoposition_kwargs,
):
    """
    Importer for the list of installed generators provided by the ENTSO-E
    Transparency Project. Geographical information is not given.
    If update=True, the dataset is parsed through a request to
    'https://transparency.entsoe.eu/generation/r2/\
    installedCapacityPerProductionUnit/show',
    Internet connection required. If raw=True, the same request is done, but
    the unprocessed data is returned.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    entsoe_token: String
        Security token of the ENTSO-E Transparency platform
    fill_geoposition_kwargs:
        Keyword arguments passed to `fill_geoposition`.

    Note: For obtaining a security token refer to section 2 of the
    RESTful API documentation of the ENTSOE-E Transparency platform
    https://transparency.entsoe.eu/content/static_content/Static%20content/
    web%20api/Guide.html#_authentication_and_authorisation. Please save the
    token in your config.yaml file (key 'entsoe_token').
    """
    config = get_config() if config is None else config

    def retrieve_data(token):
        client = entsoe.EntsoePandasClient(api_key=token)

        start = pd.Timestamp("20190101", tz="Europe/Brussels")
        end = pd.Timestamp("20200101", tz="Europe/Brussels")

        not_retrieved = []
        dfs = []
        for area in entsoe.mappings.Area:
            kwargs = dict(start=start, end=end)
            try:
                dfs.append(
                    client.query_installed_generation_capacity_per_unit(
                        area.name, **kwargs
                    )
                )
            except (entsoe.exceptions.NoMatchingDataError, requests.HTTPError):
                not_retrieved.append(area.name)
                pass

        if not_retrieved:
            logger.warning(
                f"Data for area(s) {', '.join(not_retrieved)} could not be retrieved."
            )

        return pd.concat(dfs)

    path = get_raw_file("ENTSOE", config=config, skip_retrieve=True)

    if os.path.exists(path) and not update:
        df = pd.read_csv(path, index_col=0)
    else:
        token = config.get("entsoe_token")
        if token is not None:
            df = retrieve_data(token)
            df.to_csv(path)
        else:
            logger.info(
                "No entsoe_token in config.yaml given, "
                "falling back to stored version."
            )
            df = pd.read_csv(get_raw_file("ENTSOE", update, config), index_col=0)

    if raw:
        return df

    RENAME_COLUMNS = {
        "Production Type": "Fueltype",
        "Installed Capacity [MW]": "Capacity",
    }

    fn = _package_data("entsoe_country_codes.csv")
    COUNTRY_MAP = pd.read_csv(fn, index_col=0).rename(index=str).Country

    return (
        df.rename_axis(index="projectID")
        .reset_index()
        .rename(columns=RENAME_COLUMNS)
        .drop_duplicates("projectID")
        .assign(
            Name=lambda df: df.Name.str.replace("_", " "),  # for geoparsing
            EIC=lambda df: df.projectID,
            Country=lambda df: df.projectID.str[:2].map(COUNTRY_MAP),
            Capacity=lambda df: pd.to_numeric(df.Capacity),
            Technology=np.nan,
            Set=np.nan,
            lat=np.nan,
            lon=np.nan,
        )
        .powerplant.convert_alpha2_to_country()
        # .pipe(fill_geoposition, **fill_geoposition_kwargs)
        .query("Capacity > 0")
        .pipe(gather_specifications, config=config)
        .pipe(clean_name)
        .pipe(set_column_name, "ENTSOE")
        .pipe(config_filter, config)
    )


def ENTSOE_EIC(raw=False, update=False, config=None, entsoe_token=None):
    """
    Importer for the meta data given for each ENTSOE entry.

    This data serves to fill up geographical information.
    If update=True an internet connection is required.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    entsoe_token: String
        Security token of the ENTSO-E Transparency platform

    Note: For obtaining a security token refer to section 2 of the
    RESTful API documentation of the ENTSOE-E Transparency platform
    https://transparency.entsoe.eu/content/static_content/Static%20content/
    web%20api/Guide.html#_authentication_and_authorisation. Please save the
    token in your config.yaml file (key 'entsoe_token').
    """
    config = get_config() if config is None else config

    RENAME_COLS = {
        "EicCode": "projectID",
        "EicLongName": "Name",
        "MarketParticipantPostalCode": "postalcode",
        "MarketParticipantIsoCountryCode": "Country",
    }

    fn = get_raw_file("ENTSOE-EIC", update=update, config=config)
    df = pd.read_csv(fn, sep=";", on_bad_lines="skip")

    if raw:
        return df

    # Maybe use this to get additional data on ENTSOE power plants, like postal codes
    # entsoe_eic = ENTSOE_EIC(update=update).set_index("projectID").drop_duplicates()
    # index = df.index.drop_duplicates()
    # postalcode = entsoe_eic.postalcode.reindex(index, fill_value="")

    return (
        df.rename(columns=RENAME_COLS)
        .powerplant.convert_to_short_name()
        .pipe(set_column_name, "ENTSOE-EIC")
        .assign(lat=np.nan, lon=np.nan)
    )


# def OSM():
#    """
#    Parser and Importer for Open Street Map power plant data.
#    """
#    import requests
#    overpass_url = "http://overpass-api.de/api/interpreter"
#    overpass_query = """
#    [out:json][timeout:210];
#    area["name"="Luxembourg"]->.boundaryarea;
#    (
#    // query part for: “power=plant”
#    node["power"="plant"](area.boundaryarea);
#    way["power"="plant"](area.boundaryarea);
#    relation["power"="plant"](area.boundaryarea);
#    node["power"="generator"](area.boundaryarea);
#    way["power"="generator"](area.boundaryarea);
#    relation["power"="generator"](area.boundaryarea);
#    );
#    out body;
#    """
#    response = requests.get(overpass_url,
#                            params={'data': overpass_query})
#    data = response.json()
#    df = pd.DataFrame(data['elements'])
#    df = pd.concat([df.drop(columns='tags'), df.tags.apply(pd.Series)], axis=1)
#


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function is not maintained anymore.",
)
def WEPP(raw=False, config=None):
    """
    Importer for the standardized WEPP (Platts, World Elecrtric Power
    Plants Database). This database is not provided by this repository because
    of its restrictive licence.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()

    """
    config = get_config() if config is None else config

    # Define the appropriate datatype for each column (some columns e.g.
    # 'YEAR' cannot be integers, as there are N/A values, which np.int
    # does not yet(?) support.)
    datatypes = {
        "UNIT": str,
        "PLANT": str,
        "COMPANY": str,
        "MW": np.float64,
        "STATUS": str,
        "YEAR": np.float64,
        "UTYPE": str,
        "FUEL": str,
        "FUELTYPE": str,
        "ALTFUEL": str,
        "SSSMFR": str,
        "BOILTYPE": str,
        "TURBMFR": str,
        "TURBTYPE": str,
        "GENMFR": str,
        "GENTYPE": str,
        "SFLOW": np.float64,
        "SPRESS": np.float64,
        "STYPE": str,
        "STEMP": np.float64,
        "REHEAT1": np.float64,
        "REHEAT2": np.float64,
        "PARTCTL": str,
        "PARTMFR": str,
        "SO2CTL": str,
        "FGDMFR": str,
        "NOXCTL": str,
        "NOXMFR": str,
        "AE": str,
        "CONstr, UCT": str,
        "COOL": str,
        "RETIRE": np.float64,
        "CITY": str,
        "STATE": str,
        "COUNTRY": str,
        "AREA": str,
        "SUBREGION": str,
        "POSTCODE": str,
        "PARENT": str,
        "ELECTYPE": str,
        "BUSTYPE": str,
        "COMPID": str,
        "LOCATIONID": str,
        "UNITID": str,
    }
    # Now read the Platts WEPP Database
    wepp = pd.read_csv(config["WEPP"]["source_file"], dtype=datatypes, encoding="utf-8")
    if raw:
        return wepp

    # Fit WEPP-column names to our specifications
    wepp.columns = wepp.columns.str.title()
    wepp.rename(
        columns={
            "Unit": "Name",
            "Fuel": "Fueltype",
            "Fueltype": "Technology",
            "Mw": "Capacity",
            "Year": "DateIn",
            "Retire": "DateOut",
            "Lat": "lat",
            "Lon": "lon",
            "Unitid": "projectID",
        },
        inplace=True,
    )
    wepp["DateRetrofit"] = wepp.DateIn
    # Do country transformations and drop those which are not in defined scope
    c = {
        "ENGLAND & WALES": "UNITED KINGDOM",
        "GIBRALTAR": "SPAIN",
        "SCOTLAND": "UNITED KINGDOM",
    }
    wepp.Country = wepp.Country.replace(c).str.title()
    wepp = (
        wepp.loc[lambda df: df.Country.isin(config["target_countries"])]
        .loc[lambda df: df.Status.isin(["OPR", "CON"])]
        .assign(File=config["WEPP"]["source_file"])
    )
    # Replace fueltypes
    d = {
        "AGAS": "Solid Biomass",  # Syngas from gasified agricultural waste
        "BFG": "Other",  # blast furnance gas -> "Hochofengas"
        "BGAS": "Biogas",
        "BIOMASS": "Solid Biomass",
        "BL": "Solid Biomass",
        "CGAS": "Hard Coal",
        "COAL": "Hard Coal",
        "COG": "Other",  # coke oven gas -> deutsch: "Hochofengas"
        "COKE": "Hard Coal",
        "CSGAS": "Hard Coal",  # Coal-seam-gas
        "CWM": "Hard Coal",  # Coal-water mixture (aka coal-water slurry)
        "DGAS": "Other",  # sewage digester gas -> deutsch: "Klaergas"
        "FGAS": "Other",  # Flare gas or wellhead gas or associated gas
        "GAS": "Natural Gas",
        "GEO": "Geothermal",
        "H2": "Other",  # Hydrogen gas
        "HZDWST": "Waste",  # Hazardous waste
        "INDWST": "Waste",  # Industrial waste or refinery waste
        "JET": "Oil",  # Jet fuels
        "KERO": "Oil",  # Kerosene
        "LGAS": "Other",  # landfill gas -> deutsch: "Deponiegas"
        "LIGNIN": "Solid Biomass",
        "LIQ": "Other",  # (black) liqour -> deutsch: "Schwarzlauge",
        #    die bei Papierherstellung anfaellt
        "LNG": "Natural Gas",  # Liquified natural gas
        "LPG": "Natural Gas",  # Liquified petroleum gas (u. butane/propane)
        "MBM": "Solid Biomass",  # Meat and bonemeal
        "MEDWST": "Solid Biomass",  # Medical waste
        "MGAS": "Other",  # mine gas -> deutsch: "Grubengas"
        "NAP": "Oil",  # naphta
        "OGAS": "Oil",  # Gasified crude oil/refinery bottoms/bitumen
        "PEAT": "Other",
        "REF": "Waste",
        "REFGAS": "Other",  # Syngas from gasified refuse
        "RPF": "Waste",  # Waste paper and/or waste plastic
        "PWST": "Other",  # paper mill waste
        "RGAS": "Other",  # refinery off-gas -> deutsch: "Raffineriegas"
        "SHALE": "Oil",
        "SUN": "Solar",
        "TGAS": "Other",  # top gas -> deutsch: "Hochofengas"
        "TIRES": "Other",  # Scrap tires
        "UNK": "Other",
        "UR": "Nuclear",
        "WAT": "Hydro",
        "WOOD": "Solid Biomass",
        "WOODGAS": "Solid Biomass",
        "WSTGAS": "Other",  # waste gas -> deutsch: "Industrieabgas"
        "WSTWSL": "Waste",  # Wastewater sludge
        "WSTH": "Waste",
    }
    wepp.Fueltype = wepp.Fueltype.replace(d)
    # Fill NaNs to allow str actions
    wepp.Technology.fillna("", inplace=True)
    wepp.Turbtype.fillna("", inplace=True)
    # Correct technology infos:
    wepp.loc[wepp.Technology.str.contains("LIG", case=False), "Fueltype"] = "Lignite"
    wepp.loc[wepp.Turbtype.str.contains("KAPLAN|BULB", case=False), "Technology"] = (
        "Run-Of-River"
    )
    wepp.Technology = wepp.Technology.replace(
        {
            "CONV/PS": "Pumped Storage",
            "CONV": "Reservoir",
            "PS": "Pumped Storage",
        }
    )
    tech_st_pattern = [
        "ANTH",
        "BINARY",
        "BIT",
        "BIT/ANTH",
        "BIT/LIG",
        "BIT/SUB",
        "BIT/SUB/LIG",
        "COL",
        "DRY ST",
        "HFO",
        "LIG",
        "LIG/BIT",
        "PWR",
        "RDF",
        "SUB",
    ]
    tech_ocgt_pattern = ["AGWST", "LITTER", "RESID", "RICE", "STRAW"]
    tech_ccgt_pattern = ["LFO"]
    wepp.loc[wepp.Technology.isin(tech_st_pattern), "Technology"] = "Steam Turbine"
    wepp.loc[wepp.Technology.isin(tech_ocgt_pattern), "Technology"] = "OCGT"
    wepp.loc[wepp.Technology.isin(tech_ccgt_pattern), "Technology"] = "CCGT"
    ut_ccgt_pattern = [
        "CC",
        "GT/C",
        "GT/CP",
        "GT/CS",
        "GT/ST",
        "ST/C",
        "ST/CC/GT",
        "ST/CD",
        "ST/CP",
        "ST/CS",
        "ST/GT",
        "ST/GT/IC",
        "ST/T",
        "IC/CD",
        "IC/CP",
        "IC/GT",
    ]
    ut_ocgt_pattern = [
        "GT",
        "GT/D",
        "GT/H",
        "GT/HY",
        "GT/IC",
        "GT/S",
        "GT/T",
        "GTC",
    ]
    ut_st_pattern = ["ST", "ST/D"]
    ut_ic_pattern = ["IC", "IC/H"]
    wepp.loc[wepp.Utype.isin(ut_ccgt_pattern), "Technology"] = "CCGT"
    wepp.loc[wepp.Utype.isin(ut_ocgt_pattern), "Technology"] = "OCGT"
    wepp.loc[wepp.Utype.isin(ut_st_pattern), "Technology"] = "Steam Turbine"
    wepp.loc[wepp.Utype.isin(ut_ic_pattern), "Technology"] = "Combustion Engine"
    wepp.loc[wepp.Utype == "WTG", "Technology"] = "Onshore"
    wepp.loc[wepp.Utype == "WTG/O", "Technology"] = "Offshore"
    wepp.loc[
        (wepp.Fueltype == "Solar") & (wepp.Utype.isin(ut_st_pattern)),
        "Technology",
    ] = "CSP"
    # Derive the SET column
    chp_pattern = [
        "CC/S",
        "CC/CP",
        "CCSS/P",
        "GT/CP",
        "GT/CS",
        "GT/S",
        "GT/H",
        "IC/CP",
        "IC/H",
        "ST/S",
        "ST/H",
        "ST/CP",
        "ST/CS",
        "ST/D",
    ]
    wepp.loc[wepp.Utype.isin(chp_pattern), "Set"] = "CHP"
    wepp.loc[wepp.Set.isnull(), "Set"] = "PP"
    # Clean up the mess
    wepp.Fueltype = wepp.Fueltype.str.title()
    wepp.loc[wepp.Technology.str.len() > 4, "Technology"] = wepp.loc[
        wepp.Technology.str.len() > 4, "Technology"
    ].str.title()
    # Done!
    wepp.datasetID = "WEPP"
    return (
        wepp.pipe(set_column_name, "WEPP")
        .pipe(config_filter, config)
        .pipe(scale_to_net_capacities, (not config["WEPP"]["net_capacity"]))
        .pipe(correct_manually, "WEPP", config=config)
    )


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function is not maintained anymore.",
)
def UBA(
    raw=False,
    update=False,
    config=None,
    header=9,
    skipfooter=26,
    prune_wind=True,
    prune_solar=True,
):
    """
    Importer for the UBA Database. Please download the data from
    `<https://www.umweltbundesamt.de/dokument/datenbank-kraftwerke-in-deutschland>`_.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    header : int, Default 9
        The zero-indexed row in which the column headings are found.
    skipfooter : int, Default 26

    """
    config = get_config() if config is None else config

    fn = get_raw_file("UBA", update, config)
    uba = pd.read_excel(fn, skipfooter=skipfooter, na_values="n.b.", header=header)

    if raw:
        return uba

    RENAME_COLUMNS = {
        "Kraftwerksname / Standort": "Name",
        "Elektrische Bruttoleistung (MW)": "Capacity",
        "Inbetriebnahme  (ggf. Ertüchtigung)": "DateIn",
        "Primärenergieträger": "Fueltype",
        "Anlagenart": "Technology",
        "Fernwärme-leistung (MW)": "CHP",
        "Standort-PLZ": "PLZ",
    }

    RENAME_TECHNOLOGY = {
        "DKW": "Steam Turbine",
        "DWR": "Pressurized Water Reactor",
        "G/AK": "Steam Turbine",
        "GT": "OCGT",
        "GuD": "CCGT",
        "GuD / HKW": "CCGT",
        "HKW": "Steam Turbine",
        "HKW (DT)": "Steam Turbine",
        "HKW / GuD": "CCGT",
        "HKW / SSA": "Steam Turbine",
        "IKW": "OCGT",
        "IKW / GuD": "CCGT",
        "IKW / HKW": "Steam Turbine",
        "IKW / HKW / GuD": "CCGT",
        "IKW / SSA": "OCGT",
        "IKW /GuD": "CCGT",
        "LWK": "Run-Of-River",
        "PSW": "Pumped Storage",
        "SWK": "Reservoir Storage",
        "SWR": "Boiled Water Reactor",
    }

    uba = uba.rename(RENAME_COLUMNS)
    from .heuristics import PLZ_to_LatLon_map

    uba = uba.assign(
        Name=uba.Name.replace({r"\s\s+": " "}, regex=True),
        lon=uba.PLZ.map(PLZ_to_LatLon_map()["lon"]),
        lat=uba.PLZ.map(PLZ_to_LatLon_map()["lat"]),
        DateIn=uba.DateIn.str.replace(r"\(|\)|\/|\-", " ", regex=True)
        .str.split(" ")
        .str[0]
        .astype(float),
        Country="Germany",
        projectID=[f"UBA{i + header + 2:03d}" for i in uba.index],
        Technology=uba.Technology.replace(RENAME_TECHNOLOGY),
    )
    uba.loc[uba.CHP.notnull(), "Set"] = "CHP"
    uba = uba.pipe(gather_set_info)
    uba.loc[uba.Fueltype == "Wind (O)", "Technology"] = "Offshore"
    uba.loc[uba.Fueltype == "Wind (L)", "Technology"] = "Onshore"
    uba.loc[uba.Fueltype.str.contains("Wind"), "Fueltype"] = "Wind"
    uba.loc[uba.Fueltype.str.contains("Braunkohle"), "Fueltype"] = "Lignite"
    uba.loc[uba.Fueltype.str.contains("Steinkohle"), "Fueltype"] = "Hard Coal"
    uba.loc[uba.Fueltype.str.contains("Erdgas"), "Fueltype"] = "Natural Gas"
    uba.loc[uba.Fueltype.str.contains("HEL"), "Fueltype"] = "Oil"
    uba.Fueltype = uba.Fueltype.replace(
        {
            "Biomasse": "Solid Biomass",
            "Gichtgas": "Other",
            "HS": "Oil",
            "Konvertergas": "Other",
            "Licht": "Solar",
            "Raffineriegas": "Other",
            "Uran": "Nuclear",
            "Wasser": "Hydro",
            "\xd6lr\xfcckstand": "Oil",
        }
    )
    uba["Name"] = uba.Name.replace([r"(?i)oe", r"(?i)ue"], ["ö", "ü"], regex=True)
    if prune_wind:
        uba = uba.loc[lambda x: x.Fueltype != "Wind"]
    if prune_solar:
        uba = uba.loc[lambda x: x.Fueltype != "Solar"]
    return (
        uba.pipe(set_column_name, "UBA")
        .pipe(scale_to_net_capacities, not config["UBA"]["net_capacity"])
        .pipe(config_filter, config)
        # .pipe(correct_manually, 'UBA', config=config)
    )


@deprecated(
    deprecated_in="0.5.0",
    removed_in="0.6.0",
    details="This function is not maintained anymore.",
)
def BNETZA(
    raw=False,
    update=False,
    config=None,
    header=9,
    sheet_name="Gesamtkraftwerksliste BNetzA",
    prune_wind=True,
    prune_solar=True,
):
    """
    Importer for the database put together by Germany's 'Federal Network
    Agency' (dt. 'Bundesnetzagentur' (BNetzA)).
    Please download the data from
    `<https://www.bundesnetzagentur.de/DE/Sachgebiete/ElektrizitaetundGas/
    Unternehmen_Institutionen/Versorgungssicherheit/Erzeugungskapazitaeten/
    Kraftwerksliste/kraftwerksliste-node.html>`_.

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    header : int, Default 9
        The zero-indexed row in which the column headings are found.
    """
    config = get_config() if config is None else config

    fn = get_raw_file("BNETZA", update, config)
    bnetza = pd.read_excel(fn, header=header, sheet_name=sheet_name, parse_dates=False)

    if raw:
        return bnetza

    bnetza = bnetza.rename(
        columns={
            "Kraftwerksnummer Bundesnetzagentur": "projectID",
            "Kraftwerksname": "Name",
            "Netto-Nennleistung (elektrische Wirkleistung) in MW": "Capacity",
            "Wärmeauskopplung (KWK)\n(ja/nein)": "Set",
            "Ort\n(Standort Kraftwerk)": "Ort",
            (
                "Auswertung\nEnergieträger (Zuordnung zu einem "
                "Hauptenergieträger bei Mehreren Energieträgern)"
            ): "Fueltype",
            "Kraftwerksstatus \n(in Betrieb/\nvorläufig "
            "stillgelegt/\nsaisonale Konservierung\nNetzreserve/ "
            "Sicherheitsbereitschaft/\nSonderfall)": "Status",
            (
                "Aufnahme der kommerziellen Stromerzeugung der derzeit "
                "in Betrieb befindlichen Erzeugungseinheit\n(Datum/Jahr)"
            ): "DateIn",
            "PLZ\n(Standort Kraftwerk)": "PLZ",
        }
    )
    # If BNetzA-Name is empty replace by company, if this is empty by city.

    from .heuristics import PLZ_to_LatLon_map

    pattern = "|".join(
        [
            ".*(?i)betrieb",
            ".*(?i)gehindert",
            "(?i)vorläufig.*",
            "Sicherheitsbereitschaft",
            "Sonderfall",
        ]
    )
    bnetza = bnetza.assign(
        lon=bnetza.PLZ.map(PLZ_to_LatLon_map()["lon"]),
        lat=bnetza.PLZ.map(PLZ_to_LatLon_map()["lat"]),
        Name=bnetza.Name.where(
            bnetza.Name.str.len().fillna(0) > 4,
            bnetza.Unternehmen + " " + bnetza.Name.fillna(""),
        )
        .fillna(bnetza.Ort)
        .str.strip(),
        DateIn=bnetza.DateIn.str[:4].apply(pd.to_numeric, errors="coerce"),
        Blockname=bnetza.Blockname.replace(
            {
                ".*(GT|gasturbine).*": "OCGT",
                ".*(DT|HKW|(?i)dampfturbine|(?i)heizkraftwerk).*": "Steam Turbine",
                ".*GuD.*": "CCGT",
            },
            regex=True,
        ),
    )[
        lambda df: df.projectID.notna()
        & df.Status.str.contains(pattern, regex=True, case=False)
    ].pipe(
        gather_technology_info,
        search_col=["Name", "Fueltype", "Blockname"],
        config=config,
    )

    add_location_b = bnetza[bnetza.Ort.notnull()].apply(
        lambda ds: (ds["Ort"] not in ds["Name"])
        and (str.title(ds["Ort"]) not in ds["Name"]),
        axis=1,
    )
    bnetza.loc[bnetza.Ort.notnull() & add_location_b, "Name"] = (
        bnetza.loc[bnetza.Ort.notnull() & add_location_b, "Ort"]
        + " "
        + bnetza.loc[bnetza.Ort.notnull() & add_location_b, "Name"]
    )

    techmap = {
        "solare": "PV",
        "Laufwasser": "Run-Of-River",
        "Speicherwasser": "Reservoir",
        "Pumpspeicher": "Pumped Storage",
    }
    for fuel in techmap:
        bnetza.loc[bnetza.Fueltype.str.contains(fuel, case=False), "Technology"] = (
            techmap[fuel]
        )
    # Fueltypes
    bnetza.Fueltype.replace(
        {
            "Erdgas": "Natural Gas",
            "Steinkohle": "Hard Coal",
            "Braunkohle": "Lignite",
            "Wind.*": "Wind",
            "Solar.*": "Solar",
            ".*(?i)energietr.*ger.*\n.*": "Other",
            "Kern.*": "Nuclear",
            "Mineral.l.*": "Oil",
            "Biom.*": "Solid Biomass",
            ".*(?i)(e|r|n)gas": "Other",
            "Geoth.*": "Geothermal",
            "Abfall": "Waste",
            ".*wasser.*": "Hydro",
            ".*solar.*": "PV",
        },
        regex=True,
        inplace=True,
    )
    if prune_wind:
        bnetza = bnetza[lambda x: x.Fueltype != "Wind"]
    if prune_solar:
        bnetza = bnetza[lambda x: x.Fueltype != "Solar"]
    # Filter by country
    bnetza = bnetza[~bnetza.Bundesland.isin(["Österreich", "Schweiz", "Luxemburg"])]
    return (
        bnetza.assign(
            Country="Germany",
            Set=bnetza.Set.fillna("Nein")
            .str.title()
            .replace({"Ja": "CHP", "Nein": "PP"}),
        ).pipe(set_column_name, "BNETZA")
        # .pipe(config_filter, name='BNETZA', config=config)
        # .pipe(correct_manually, 'BNETZA', config=config)
    )


def OPSD_VRE(raw=False, update=False, config=None):
    """
    Importer for the OPSD (Open Power Systems Data) renewables (VRE)
    database.

    This sqlite database is very big and hence not part of the package.
    It needs to be obtained from
    `<http://data.open-power-system-data.org/renewable_power_plants/>`_

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config

    fn = get_raw_file("OPSD_VRE", update=update, config=config)
    df = pd.read_csv(fn, low_memory=False)

    if raw:
        return df

    return (
        df.rename(
            columns={
                "energy_source_level_2": "Fueltype",
                "technology": "Technology",
                "data_source": "file",
                "country": "Country",
                "electrical_capacity": "Capacity",
                "municipality": "Name",
            }
        )
        .assign(
            DateIn=lambda df: df.commissioning_date.str[:4].astype(float),
            Set="PP",
        )
        .powerplant.convert_alpha2_to_country()
        .pipe(set_column_name, "OPSD_VRE")
        .pipe(config_filter, config)
    )


def OPSD_VRE_country(country, raw=False, update=False, config=None):
    """
    Get country specific data from OPSD for renewables, if available.
    Available for DE, FR, PL, CH, DK, CZ and SE (last update: 09/2020).

    Parameters
    ----------
    raw : Boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config

    # there is a problem with GB in line 1651 (version 20/08/20) use low_memory
    fn = get_raw_file(f"OPSD_VRE_{country}", update=update, config=config)
    df = pd.read_csv(fn, low_memory=False)

    if raw:
        return df

    return (
        df.assign(Country=country, Set="PP")
        .rename(
            columns={
                "energy_source_level_2": "Fueltype",
                "technology": "Technology",
                "electrical_capacity": "Capacity",
                "municipality": "Name",
            }
        )
        # there is a problem with GB in line 1651 (version 20/08/20)
        .assign(Capacity=lambda df: pd.to_numeric(df.Capacity, "coerce"))
        .powerplant.convert_alpha2_to_country()
        .pipe(set_column_name, f"OPSD_VRE_{country}")
        .pipe(config_filter, config)
    )


def IRENASTAT(raw=False, update=False, config=None):
    """
    Importer for the IRENASTAT renewable capacity statistics.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    if config is None:
        config = get_config()

    fn = get_raw_file("IRENA", update=update, config=config)

    df = pd.read_csv(fn, comment="#", quotechar='"')

    if raw:
        return df

    RENAME_COLUMNS = {
        "Electricity statistics": "Capacity",
        "Country/area": "Country",
        "Grid connection": "Grid",
    }
    df.rename(columns=RENAME_COLUMNS, inplace=True)

    df.drop(columns="Data Type", inplace=True)

    # Rename all entries "Congo (the)" to "Congo" under the column
    # "Country"; the former confuses country_converter.
    df["Country"] = df["Country"].replace("Congo (the)", "Congo")

    # Consistent country names for dataset
    df = convert_to_short_name(df)

    df.dropna(subset="Capacity", inplace=True)

    # Remove all rows where Technology is just a Total
    df = df[
        ~df.Technology.str.contains("Total Renewable|Total Non-Renewable", na=False)
    ]

    fueltype_dict = {
        "Solar photovoltaic": "Solar",
        "Solar thermal energy": "Solar",
        "Onshore wind energy": "Wind",
        "Offshore wind energy": "Wind",
        "Renewable hydropower": "Hydro",
        "Mixed Hydro Plants": "Hydro",
        "Pumped storage": "Hydro",
        "Solid biofuels": "Solid Biomass",
        "Renewable municipal waste": "Waste",
        "Liquid biofuels": "Solid Biomass",
        "Biogas": "Biogas",
        "Geothermal energy": "Geothermal",
        "Marine energy": "Marine",
        "Coal and peat": "Hard Coal",
        "Oil": "Oil",
        "Natural gas": "Natural Gas",
        "Nuclear": "Nuclear",
        "Fossil fuels n.e.s.": "Other",
        "Other non-renewable energy": "Other",
    }

    technology_dict = {
        "Solar photovoltaic": "PV",
        "Solar thermal energy": "CSP",
        "Onshore wind energy": "Onshore",
        "Offshore wind energy": "Offshore",
        "Pumped storage": "Pumped Storage",
        "Geothermal energy": "Geothermal",
        "Marine energy": "Marine",
    }

    df["Fueltype"] = df.Technology.map(fueltype_dict)
    df["Technology"] = df.Technology.replace(technology_dict)

    non_capacity_columns = list(set(df.columns).difference(set(["Capacity"])))
    df = df.groupby(non_capacity_columns, as_index=False, dropna=True).sum()

    return df


def GBPT(raw=False, update=False, config=None):
    """
    Importer for the global bioenergy powerplant tracker from global energy monitor.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config
    fn = get_raw_file("GBPT", update=update, config=config)
    df = pd.read_excel(fn, sheet_name="Data")

    if raw:
        return df

    RENAME_COLUMNS = {
        "Project name": "Name",
        "Capacity (MW)": "Capacity",
        "Fuel 1": "Fueltype",
        "Operating status": "Status",
        "Latitude": "lat",
        "Longitude": "lon",
        "Unit start year": "DateIn",
        "Retired year": "DateOut",
        "GEM phase ID": "projectID",
    }
    fueltype_dict = {
        "bioenergy - agricultural waste (solids)": "Solid Biomass",
        "bioenergy - refuse (municipal and industrial wastes)": "Solid Biomass",
        "bioenergy - refuse (syngas)": "Solid Biomass",
        "bioenergy - agricultural waste (biogas)": "Biogas",
        "bioenergy - wood & other biomass (solids)": "Solid Biomass",
        "bioenergy - ethanol": "Solid Biomass",
        "bioenergy - paper mill wastes": "Solid Biomass",
        "bioenergy - biodiesel": "Solid Biomass",
        "bioenergy - unknown": "Solid Biomass",
        "bioenergy - wastewater and sewage sludge (solids or biogas)": "Solid Biomass",
        "bioenergy - refuse (landfill gas)": "Biogas",
        "bioenergy - agricultural waste (unknown)": "Solid Biomass",
        "bioenergy - agricultural waste (syngas)": "Solid Biomass",
        "bioenergy - wood & other biomass (biocoal)": "Solid Biomass",
    }

    status_list = config["GBPT"].get("status", ["operating"])  # noqa: F841

    df = df.rename(columns=RENAME_COLUMNS)
    df_final = (
        df.pipe(clean_name)
        .pipe(set_column_name, "GBPT")
        .pipe(convert_to_short_name)
        .dropna(subset="Capacity")
        .assign(
            DateIn=df["DateIn"].apply(pd.to_numeric, errors="coerce"),
            DateOut=df["DateOut"].apply(pd.to_numeric, errors="coerce"),
            lat=df["lat"].apply(pd.to_numeric, errors="coerce"),
            lon=df["lon"].apply(pd.to_numeric, errors="coerce"),
        )
        .query("Status in @status_list")
        .pipe(lambda x: x[df.columns.intersection(config.get("target_columns"))])
        .pipe(lambda x: x.replace({"Fueltype": fueltype_dict}))
        .assign(Technology="Steam Turbine")
        .assign(Set="PP")
        .pipe(config_filter, config)
    )
    return df_final


def GNPT(raw=False, update=False, config=None):
    """
    Importer for the global nuclear energy powerplant tracker from global energy monitor.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config
    fn = get_raw_file("GNPT", update=update, config=config)
    df = pd.read_excel(fn, sheet_name="Data")

    if raw:
        return df

    RENAME_COLUMNS = {
        "Project Name": "Name",
        "Capacity (MW)": "Capacity",
        "Latitude": "lat",
        "Longitude": "lon",
        "Start Year": "DateIn",
        "Retirement Year": "DateOut",
        "Country/Area": "Country",
        "GEM unit ID": "projectID",
    }

    status_list = config["GNPT"].get("status", ["operating"])  # noqa: F841

    df = df.rename(columns=RENAME_COLUMNS)
    df_final = (
        df.pipe(clean_name)
        .pipe(set_column_name, "GNPT")
        .pipe(convert_to_short_name)
        .dropna(subset="Capacity")
        .assign(
            DateIn=df["DateIn"].apply(pd.to_numeric, errors="coerce"),
            DateOut=df["DateOut"].apply(pd.to_numeric, errors="coerce"),
            lat=df["lat"].apply(pd.to_numeric, errors="coerce"),
            lon=df["lon"].apply(pd.to_numeric, errors="coerce"),
        )
        .query("Status in @status_list")
        .pipe(lambda x: x[df.columns.intersection(config.get("target_columns"))])
        .assign(Fueltype="Nuclear")
        .assign(Technology="Steam Turbine")
        .assign(Set="PP")
        .pipe(config_filter, config)
    )
    return df_final


def GCPT(raw=False, update=False, config=None):
    """
    Importer for the global coal powerplant tracker from global energy monitor.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """

    config = get_config() if config is None else config
    fn = get_raw_file("GCPT", update=update, config=config)
    df = pd.read_excel(fn, sheet_name="Units")

    if raw:
        return df

    RENAME_COLUMNS = {
        "Plant name": "Name",
        "Combustion technology": "Technology",
        "Coal type": "Fueltype",
        "Capacity (MW)": "Capacity",
        "Latitude": "lat",
        "Longitude": "lon",
        "Start year": "DateIn",
        "Retired year": "DateOut",
        "Country/Area": "Country",
        "GEM unit/phase ID": "projectID",
    }
    fueltype_dict = {
        "bituminous": "Hard Coal",
        "lignite": "Lignite",
        "unknown": "Hard Coal",
        "subbituminous": "Hard Coal",
        "waste coal": "Hard Coal",
        "anthracite": "Hard Coal",
        "lignite with CCS": "Lignite",
        "bituminous with CCS": "Hard Coal",
        "subbituminous with CCS": "Hard Coal",
        "unknown with CCS": "Hard Coal",
    }

    planned_retirement = df["Planned retirement"].apply(pd.to_numeric, errors="coerce")

    status_list = config["GCPT"].get("status", ["operating"])  # noqa: F841

    df = df.rename(columns=RENAME_COLUMNS)
    df_final = (
        df.pipe(clean_name)
        .pipe(set_column_name, "GCPT")
        .pipe(convert_to_short_name)
        .dropna(subset="Capacity")
        .assign(
            DateIn=df["DateIn"].apply(pd.to_numeric, errors="coerce"),
            DateOut=df["DateOut"].apply(pd.to_numeric, errors="coerce"),
            lat=df["lat"].apply(pd.to_numeric, errors="coerce"),
            lon=df["lon"].apply(pd.to_numeric, errors="coerce"),
        )
        .assign(DateOut=lambda x: x["DateOut"].combine_first(planned_retirement))
        .query("Status in @status_list")
        .pipe(lambda x: x[df.columns.intersection(config.get("target_columns"))])
        .pipe(lambda x: x.replace({"Fueltype": fueltype_dict}))
        .pipe(lambda x: x.assign(Technology="Steam Turbine"))
        .pipe(lambda x: x.assign(Set="PP"))
        .pipe(config_filter, config)
    )

    return df_final


def GGTPT(raw=False, update=False, config=None):
    """
    Importer for the global geothermal powerplant tracker from global energy monitor.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config
    fn = get_raw_file("GGTPT", update=update, config=config)
    df = pd.read_excel(fn, sheet_name="Data")

    if raw:
        return df

    RENAME_COLUMNS = {
        "Project Name": "Name",
        "Capacity (MW)": "Capacity",
        "Latitude": "lat",
        "Longitude": "lon",
        "Start year": "DateIn",
        "Retired year": "DateOut",
        "Country/Area": "Country",
        "GEM unit ID": "projectID",
    }

    status_list = config["GGTPT"].get("status", ["operating"])  # noqa: F841

    df = df.rename(columns=RENAME_COLUMNS)
    df_final = (
        df.pipe(clean_name)
        .pipe(set_column_name, "GGTPT")
        .pipe(convert_to_short_name)
        .dropna(subset="Capacity")
        .assign(
            DateIn=df["DateIn"].apply(pd.to_numeric, errors="coerce"),
            DateOut=df["DateOut"].apply(pd.to_numeric, errors="coerce"),
            lat=df["lat"].apply(pd.to_numeric, errors="coerce"),
            lon=df["lon"].apply(pd.to_numeric, errors="coerce"),
        )
        .query("Status in @status_list")
        .pipe(lambda x: x[df.columns.intersection(config.get("target_columns"))])
        .assign(Fueltype="Geothermal")
        .assign(Technology="Steam Turbine")
        .assign(Set="PP")
        .pipe(config_filter, config)
    )
    return df_final


def GWPT(raw=False, update=False, config=None):
    """
    Importer for the global wind powerplant tracker from global energy monitor.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config
    fn = get_raw_file("GWPT", update=update, config=config)
    df = pd.read_excel(fn, sheet_name="Data")

    RENAME_COLUMNS = {
        "Project Name": "Name",
        "Capacity (MW)": "Capacity",
        "Latitude": "lat",
        "Longitude": "lon",
        "Start year": "DateIn",
        "Retired year": "DateOut",
        "GEM phase ID": "projectID",
        "Installation Type": "Technology",
        "Country/Area": "Country",
    }

    technology_dict = {
        "Onshore": "Onshore",
        "Offshore hard mount": "Offshore",
        "Offshore floating": "Offshore",
        "Offshore mount unknown": "Offshore",
    }

    status_list = config["GWPT"].get("status", ["operating"])  # noqa: F841

    df = df.rename(columns=RENAME_COLUMNS)
    df_final = (
        df.pipe(clean_name)
        .pipe(set_column_name, "GWPT")
        .pipe(convert_to_short_name)
        .dropna(subset="Capacity")
        .assign(
            DateIn=df["DateIn"].apply(pd.to_numeric, errors="coerce"),
            DateOut=df["DateOut"].apply(pd.to_numeric, errors="coerce"),
            lat=df["lat"].apply(pd.to_numeric, errors="coerce"),
            lon=df["lon"].apply(pd.to_numeric, errors="coerce"),
        )
        .query("Status in @status_list")
        .pipe(lambda x: x[df.columns.intersection(config.get("target_columns"))])
        .pipe(lambda x: x.replace({"Technology": technology_dict}))
        .assign(Fueltype="Wind")
        .assign(Set="PP")
        .pipe(config_filter, config)
    )
    return df_final


def GSPT(raw=False, update=False, config=None):
    """
    Importer for the global solar powerplant tracker from global energy monitor.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """

    config = get_config() if config is None else config
    fn = get_raw_file("GSPT", update=update, config=config)
    large = pd.read_excel(fn, sheet_name="20 MW+")
    small = pd.read_excel(fn, sheet_name="1-20 MW")
    df = pd.concat([large, small], ignore_index=True)

    if raw:
        return df

    RENAME_COLUMNS = {
        "Project Name": "Name",
        "Capacity (MW)": "Capacity",
        "Latitude": "lat",
        "Longitude": "lon",
        "Start year": "DateIn",
        "Retired year": "DateOut",
        "Technology Type": "Technology",
        "GEM phase ID": "projectID",
        "Country/Area": "Country",
    }

    technology_dict = {
        "PV": "PV",
        "Solar Thermal": "CSP",
        "Assumed PV": "PV",
    }

    status_list = config["GSPT"].get("status", ["operating"])  # noqa: F841

    df = df.rename(columns=RENAME_COLUMNS)
    df_final = (
        df.pipe(clean_name)
        .pipe(set_column_name, "GSPT")
        .pipe(convert_to_short_name)
        .dropna(subset="Capacity")
        .assign(
            DateIn=df["DateIn"].apply(pd.to_numeric, errors="coerce"),
            DateOut=df["DateOut"].apply(pd.to_numeric, errors="coerce"),
            lat=df["lat"].apply(pd.to_numeric, errors="coerce"),
            lon=df["lon"].apply(pd.to_numeric, errors="coerce"),
        )
        .query("Status in @status_list")
        .pipe(lambda x: x[df.columns.intersection(config.get("target_columns"))])
        .pipe(lambda x: x.replace({"Technology": technology_dict}))
        .assign(Fueltype="Solar")
        .assign(Set="PP")
        .pipe(config_filter, config)
    )
    return df_final


def GGPT(raw=False, update=False, config=None):
    """
    Importer for the global gas powerplant tracker from global energy monitor.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config
    fn = get_raw_file("GGPT", update=update, config=config)
    df = pd.read_excel(fn, sheet_name="Gas & Oil Units")

    if raw:
        return df

    RENAME_COLUMNS = {
        "Plant name": "Name",
        "Fuel": "Fueltype",
        "Capacity (MW)": "Capacity",
        "Latitude": "lat",
        "Longitude": "lon",
        "Start year": "DateIn",
        "Retired year": "DateOut",
        "CHP": "Set",
        "GEM location ID": "projectID",
    }

    technology_dict = {
        "GT": "Steam Turbine",
        "IC": "Steam Turbine",
        "CC": "CCGT",
        "GT/IC": "Steam Turbine",
        "ICCC": "CCGT",
        "ISCC": "CCGT",
        "ST": "Steam Turbine",
        "AFC": "CCGT",
    }

    set_dict = {
        "Y": "CHP",
        "N": "PP",
        "not found": "PP",
    }

    status_list = config["GGPT"].get("status", ["operating"])  # noqa: F841
    gas_fuels = ["NG", "LNG", "BU", "LFG", "BG", "BFG", "COG", "CM", "H", "OG"]

    df = df.rename(columns=RENAME_COLUMNS)
    df_final = (
        df.pipe(clean_name)
        .pipe(set_column_name, "GGPT")
        .pipe(convert_to_short_name)
        .dropna(subset="Capacity")
        .pipe(lambda x: x.query("Capacity != 'not found'"))
        .assign(
            DateIn=df["DateIn"].apply(pd.to_numeric, errors="coerce"),
            DateOut=df["DateOut"].apply(pd.to_numeric, errors="coerce"),
            lat=df["lat"].apply(pd.to_numeric, errors="coerce"),
            lon=df["lon"].apply(pd.to_numeric, errors="coerce"),
            Capacity=lambda df: pd.to_numeric(df.Capacity, "coerce"),
            Fueltype=df["Fueltype"].apply(
                lambda s: (
                    "Natural Gas"
                    if any(sub in gas_fuels for sub in s.split("/"))
                    else "Oil"
                )
            ),
        )
        .query("Status in @status_list")
        .pipe(lambda x: x[df.columns.intersection(config.get("target_columns"))])
        .pipe(lambda x: x.replace({"Technology": technology_dict}))
        .pipe(lambda x: x.replace({"Set": set_dict}).fillna({"Set": "PP"}))
        .assign(Fueltype="Natural Gas")
        .pipe(config_filter, config)
    )
    return df_final


def GHPT(raw=False, update=False, config=None):
    """
    Importer for the global gas powerplant tracker from global energy monitor.

    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default False
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    config = get_config() if config is None else config
    fn = get_raw_file("GHPT", update=update, config=config)
    df = pd.read_excel(fn, sheet_name="Data")

    if raw:
        return df

    RENAME_COLUMNS = {
        "Project Name": "Name",
        "Capacity (MW)": "Capacity",
        "Latitude": "lat",
        "Longitude": "lon",
        "Start Year": "DateIn",
        "Retired Year": "DateOut",
        "GEM unit ID": "projectID",
        "Country 1": "Country",
        "Technology Type": "Technology",
    }
    technology_dict = {
        "conventional storage": "Reservoir",
        "pumped storage": "Pumped Storage",
        "run-of-river": "Run-Of-River",
        "conventional and pumped storage": "Pumped Storage",
        "conventional and run-of-river": "Run-Of-River",
    }
    status_list = config["GHPT"].get("status", ["operating"])  # noqa: F841
    df = df.rename(columns=RENAME_COLUMNS)
    df_final = (
        df.pipe(clean_name)
        .pipe(set_column_name, "GHPT")
        .pipe(convert_to_short_name)
        .dropna(subset="Capacity")
        .assign(
            DateIn=df["DateIn"].apply(pd.to_numeric, errors="coerce"),
            DateOut=df["DateOut"].apply(pd.to_numeric, errors="coerce"),
            lat=df["lat"].apply(pd.to_numeric, errors="coerce"),
            lon=df["lon"].apply(pd.to_numeric, errors="coerce"),
        )
        .query("Status in @status_list")
        .pipe(lambda x: x[df.columns.intersection(config.get("target_columns"))])
        .pipe(lambda x: x.replace({"Technology": technology_dict}))
        .assign(Fueltype="Hydro")
        .assign(Set="PP")
        .pipe(config_filter, config)
    )
    return df_final


def GEM(raw=False, update=False, config=None):
    """
    Get the combined dataset of all GEM (https://globalenergymonitor.org/) datasets.

    Parameters
    ----------
    raw : bool, optional
        Whether to return the raw dataset, by default False
    update : bool, optional
        Whether to update the raw dataset, by default False
    config : _type_, optional
        Custom configuration, by default None

    """
    GEMS_FUNTIONS = [GBPT, GGPT, GCPT, GGTPT, GNPT, GSPT, GWPT, GHPT]
    data = [f(raw=raw, update=update, config=config) for f in GEMS_FUNTIONS]
    return pd.concat(data, ignore_index=True)


# deprecated alias for GGPT
@deprecated(
    deprecated_in="0.5.5",
    removed_in="0.6",
    details="Use `GGPT` instead.",
)
def GEM_GGPT(*args, **kwargs):
    return GGPT(*args, **kwargs)


def EXTERNAL_DATABASE(raw=False, update=True, config=None):
    """
    Importer for external custom databases.
    Parameters
    ----------
    raw : boolean, default False
        Whether to return the original dataset
    update: bool, default True (unused)
        Whether to update the data from the url.
    config : dict, default None
        Add custom specific configuration,
        e.g. powerplantmatching.config.get_config(target_countries='Italy'),
        defaults to powerplantmatching.config.get_config()
    """
    if config is None:
        return pd.DataFrame()

    df = pd.read_csv(config["EXTERNAL_DATABASE"]["fn"], low_memory=False)

    if raw:
        return df

    df = df.pipe(set_column_name, "EXTERNAL_DATABASE").pipe(config_filter, config)

    return df

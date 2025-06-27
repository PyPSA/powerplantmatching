"""OpenStreetMap power plant data interface.

This module provides the main interface for extracting and processing power plant
data from OpenStreetMap. It handles country validation, multi-level caching,
and data standardization.

Main functions:
    process_countries: Process multiple countries with caching
    validate_countries: Validate country names with fuzzy matching
    validate_and_standardize_df: Ensure data consistency
"""

import logging
import os
from difflib import get_close_matches

import numpy as np
import pandas as pd

from .models import Unit, Units
from .quality.rejection import RejectionTracker
from .retrieval.client import OverpassAPIClient
from .utils import get_country_code
from .workflow import Workflow

logger = logging.getLogger(__name__)

# Valid fuel types for power plant categorization
# These match powerplantmatching's standard fuel types
VALID_FUELTYPES = [
    "Nuclear",
    "Solid Biomass",
    "Biogas",
    "Wind",
    "Hydro",
    "Solar",
    "Oil",
    "Natural Gas",
    "Hard Coal",
    "Lignite",
    "Geothermal",
    "Waste",
    "Other",
]

# Valid technology types for power generation methods
# These match powerplantmatching's standard technologies
VALID_TECHNOLOGIES = [
    "Steam Turbine",
    "OCGT",
    "CCGT",
    "Run-Of-River",
    "Reservoir",
    "Pumped Storage",
    "Offshore",
    "Onshore",
    "PV",
    "CSP",
    "Combustion Engine",
    "Marine",
]

# Valid power plant set types
VALID_SETS = ["PP", "CHP", "Store"]


def get_client_params(osm_config, api_url, cache_dir):
    """Build parameters for OverpassAPIClient initialization."""
    return {
        "api_url": api_url,
        "cache_dir": cache_dir,
        "timeout": osm_config.get("overpass_api", {}).get("timeout", 300),
        "max_retries": osm_config.get("overpass_api", {}).get("max_retries", 3),
        "retry_delay": osm_config.get("overpass_api", {}).get("retry_delay", 5),
        "show_progress": osm_config.get("overpass_api", {}).get("show_progress", True),
    }


def validate_countries(countries: list[str]) -> tuple[list[str], dict[str, str]]:
    """Validate country names and provide helpful suggestions for invalid entries.

    Uses pycountry to validate country names, supporting full names, ISO codes,
    and common variations. Provides fuzzy matching suggestions for typos or
    incorrect names.

    Parameters
    ----------
    countries : list of str
        Country names to validate. Accepts:
        - Full names: 'Germany', 'United States'
        - ISO 3166-1 alpha-2: 'DE', 'US'
        - ISO 3166-1 alpha-3: 'DEU', 'USA'
        - Common variations: 'USA', 'UK', 'South Korea'

    Returns
    -------
    valid_countries : list of str
        List of validated country names as provided
    country_code_map : dict
        Mapping of country names to ISO alpha-2 codes

    Raises
    ------
    ValueError
        If any country names are invalid. Error message includes
        suggestions for similar valid names and shows which entries
        were valid vs invalid.

    Examples
    --------
    >>> valid, codes = validate_countries(['Germany', 'France'])
    >>> print(codes)
    {'Germany': 'DE', 'France': 'FR'}

    >>> validate_countries(['Germny'])  # Typo
    ValueError: ❌ Invalid country names detected...
         ℹ️  Did you mean: 'Germany', 'Armenia'
    """
    import pycountry

    valid_countries = []
    invalid_countries = []
    country_code_map = {}

    for country in countries:
        country_code = get_country_code(country)
        if country_code is not None:
            valid_countries.append(country)
            country_code_map[country] = country_code
        else:
            invalid_countries.append(country)

    if invalid_countries:
        all_country_names = [c.name for c in pycountry.countries]  # type: ignore
        all_country_names.extend([c.alpha_2 for c in pycountry.countries])  # type: ignore
        all_country_names.extend([c.alpha_3 for c in pycountry.countries])  # type: ignore

        country_variations = {
            "USA": "United States",
            "UK": "United Kingdom",
            "South Korea": "Korea, Republic of",
            "North Korea": "Korea, Democratic People's Republic of",
            "Russia": "Russian Federation",
            "Iran": "Iran, Islamic Republic of",
            "Syria": "Syrian Arab Republic",
            "Venezuela": "Venezuela, Bolivarian Republic of",
            "Bolivia": "Bolivia, Plurinational State of",
            "Tanzania": "Tanzania, United Republic of",
            "Vietnam": "Viet Nam",
            "Czech Republic": "Czechia",
            "Macedonia": "North Macedonia",
            "Turkey": "Türkiye",
        }
        all_country_names.extend(country_variations.keys())

        error_parts = [
            f"❌ Invalid country names detected: {len(invalid_countries)} out of {len(countries)} countries",
            "\nInvalid entries:",
        ]

        for invalid in invalid_countries:
            error_parts.append(f"  ❌ '{invalid}'")

            suggestions = get_close_matches(invalid, all_country_names, n=3, cutoff=0.6)
            if suggestions:
                strings = []
                for suggestion in suggestions:
                    strings.append(f"'{suggestion}'")
                error_parts.append(f"     ℹ️  Did you mean: {', '.join(strings)}")

            if invalid in country_variations:
                error_parts.append(
                    f"     ℹ️  Try using: '{country_variations[invalid]}'"
                )

        error_parts.extend(
            [
                "\n✅ Valid entries:",
                *[
                    f"  ✅ '{valid}' → {country_code_map[valid]}"
                    for valid in valid_countries[:5]
                ],
                f"  ... and {len(valid_countries) - 5} more"
                if len(valid_countries) > 5
                else "",
                "\n📝 Accepted formats:",
                "  - Full name: 'Germany', 'United States'",
                "  - ISO 3166-1 alpha-2: 'DE', 'US'",
                "  - ISO 3166-1 alpha-3: 'DEU', 'USA'",
                "  - Common names: 'USA', 'UK', 'South Korea'",
                "\n⚠️  All countries must be valid before processing can begin.",
                "Please correct the invalid entries and try again.",
            ]
        )

        error_msg = "\n".join(filter(None, error_parts))
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"✅ Successfully validated all {len(valid_countries)} countries")
    logger.debug(
        f"Country codes: {', '.join(f'{c}={code}' for c, code in country_code_map.items())}"
    )

    return valid_countries, country_code_map


def process_countries(
    countries, csv_cache_path, cache_dir, update, osm_config, target_columns, raw=False
):
    """Process OpenStreetMap power plant data for specified countries.

    Main entry point for extracting power plant data. Handles the complete
    pipeline: validation, caching, downloading, processing, and standardization.

    Parameters
    ----------
    countries : list of str
        List of country names or ISO codes to process
    csv_cache_path : str
        Path to CSV cache file (usually osm_data.csv)
    cache_dir : str
        Directory containing OSM cache subdirectories
    update : bool
        If True, forces reprocessing even if cache exists
    osm_config : dict
        OSM configuration with quality settings and API parameters
    target_columns : list of str
        Expected columns in output DataFrame
    raw : bool, optional
        If True, skip validation and standardization. Default False.

    Returns
    -------
    pd.DataFrame
        Power plant data with standardized columns:
        projectID, Country, lat, lon, Fueltype, Technology,
        Capacity, Name, Set, DateIn, etc.

    Raises
    ------
    ValueError
        If country validation fails

    Examples
    --------
    >>> config = get_config()
    >>> df = process_countries(['Luxembourg', 'Malta'],
    ...                       'cache/osm.csv', 'cache/',
    ...                       False, config['OSM'],
    ...                       config['target_columns'])
    >>> print(f"Found {len(df)} power plants")
    Found 89 power plants

    Notes
    -----
    The function uses multi-level caching:
    1. CSV cache (fastest)
    2. Units cache (pre-processed)
    3. API cache (raw responses)
    4. Live API (slowest)
    """
    logger.info(f"Starting country validation for {len(countries)} countries...")
    try:
        valid_countries, country_code_map = validate_countries(countries)
    except ValueError as e:
        raise ValueError(
            f"Country validation failed. Cannot proceed with OSM data processing.\n{str(e)}"
        ) from e

    logger.info(
        f"Country validation successful! Processing OSM data for {len(valid_countries)} countries: "
        f"{', '.join(valid_countries[:5])}"
        f"{f' and {len(valid_countries) - 5} more' if len(valid_countries) > 5 else ''}"
    )

    api_url = osm_config.get("overpass_api", {}).get("url")
    current_config_hash = Unit._generate_config_hash(osm_config)

    all_valid_data = pd.DataFrame()

    for i, country in enumerate(valid_countries, 1):
        logger.info(
            f"Processing country {i}/{len(valid_countries)}: {country} ({country_code_map[country]})"
        )

        country_data = process_single_country(
            country,
            csv_cache_path,
            cache_dir,
            api_url,
            current_config_hash,
            update,
            osm_config,
        )

        if country_data is not None and not country_data.empty:
            if not raw:
                country_data = validate_and_standardize_df(
                    country_data,
                    target_columns,
                    VALID_FUELTYPES,
                    VALID_TECHNOLOGIES,
                    VALID_SETS,
                )
            all_valid_data = pd.concat(
                [all_valid_data, country_data], ignore_index=True
            )

    logger.info(f"✅ Successfully processed all {len(valid_countries)} countries")
    return all_valid_data


def process_single_country(
    country, csv_cache_path, cache_dir, api_url, config_hash, update, osm_config
):
    """Process a single country with cache hierarchy.

    Checks multiple cache levels before downloading from API:
    1. CSV cache (if not forced refresh)
    2. Units cache (if CSV miss)
    3. API download (if all caches miss)

    Parameters
    ----------
    country : str
        Country name to process
    csv_cache_path : str
        Path to CSV cache file
    cache_dir : str
        OSM cache directory
    api_url : str
        Overpass API URL
    config_hash : str
        Hash of current configuration
    update : bool
        Force cache update if True
    osm_config : dict
        OSM configuration

    Returns
    -------
    pd.DataFrame or None
        Country power plant data, or None if no data found
    """
    force_refresh = osm_config.get("force_refresh", False)

    if force_refresh:
        return process_from_api(csv_cache_path, cache_dir, api_url, country, osm_config)

    country_data = check_csv_cache(csv_cache_path, country, config_hash, update)

    if country_data is None:
        client_params = get_client_params(osm_config, api_url, cache_dir)

        country_data = check_units_cache(
            csv_cache_path, country, config_hash, client_params
        )

    if country_data is None:
        country_data = process_from_api(
            csv_cache_path, cache_dir, api_url, country, osm_config
        )

    return country_data


def check_csv_cache(cache_path, country, config_hash, update):
    """Check if valid data exists in CSV cache for country."""
    if update or not os.path.exists(cache_path):
        return None

    try:
        csv_data = pd.read_csv(cache_path)

        if csv_data.empty:
            logger.debug(f"CSV cache exists but is empty: {cache_path}")
            return None

        country_rows = csv_data[csv_data["Country"] == country]

        if country_rows.empty:
            logger.debug(f"No data for {country} in CSV cache")
            return None

        if "config_hash" not in country_rows.columns:
            logger.debug(f"CSV cache for {country} missing config_hash column")
            return None

        if country_rows["config_hash"].iloc[0] == config_hash:
            logger.info(f"Using CSV cache for {country} (matching config)")
            return country_rows.copy()
        else:
            logger.debug(f"CSV cache for {country} has outdated config_hash")
            return None

    except pd.errors.EmptyDataError:
        logger.debug(f"CSV cache file is empty: {cache_path}")
        return None
    except Exception as e:
        logger.debug(f"Error reading CSV cache: {str(e)}")
        return None


def check_units_cache(csv_cache_path, country, config_hash, client_params):
    """Check if valid units exist in units cache for country."""
    country_code = get_country_code(country)
    if country_code is None:
        logger.warning(f"Invalid country name: {country}, skipping units cache check")
        return None

    try:
        with OverpassAPIClient(**client_params) as client:
            cached_units = client.cache.get_units(country_code)

            valid_units = []
            for unit in cached_units:
                if hasattr(unit, "config_hash") and unit.config_hash == config_hash:
                    valid_units.append(unit)

            if valid_units:
                logger.info(
                    f"Found {len(valid_units)} valid cached units for {country}"
                )
                country_data = pd.DataFrame([unit.to_dict() for unit in valid_units])

                update_csv_cache(csv_cache_path, country, country_data)
                return country_data
    except Exception as e:
        logger.error(f"Error accessing units cache for {country}: {str(e)}")

    return None


def process_from_api(csv_cache_path, cache_dir, api_url, country, osm_config):
    """Download and process country data from Overpass API."""
    logger.info(f"No valid cache for {country}, processing from API")

    try:
        client_params = get_client_params(osm_config, api_url, cache_dir)

        with OverpassAPIClient(**client_params) as client:
            units_collection = Units()
            rejection_tracker = RejectionTracker()

            workflow = Workflow(
                client=client,
                rejection_tracker=rejection_tracker,
                units=units_collection,
                config=osm_config,
            )

            updated_units_collection, _ = workflow.process_country_data(country)

            country_units = updated_units_collection.filter_by_country(country)

            if len(country_units) > 0:
                logger.info(f"Processed {len(country_units)} units for {country}")
                country_data = pd.DataFrame([unit.to_dict() for unit in country_units])

                update_csv_cache(csv_cache_path, country, country_data)

                return country_data
            else:
                logger.warning(f"No units found for {country}")
                return None
    except Exception as e:
        logger.error(f"Error processing {country} from API: {str(e)}")
        return None


def update_csv_cache(cache_path, country, country_data):
    """Update CSV cache with new country data."""
    if country_data.empty:
        return

    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)

        if os.path.exists(cache_path):
            try:
                full_csv = pd.read_csv(cache_path)
                full_csv = full_csv[full_csv["Country"] != country]
                updated_csv = pd.concat([full_csv, country_data], ignore_index=True)
                updated_csv.to_csv(cache_path, index=False)
                logger.info(
                    f"Updated CSV cache with {len(country_data)} entries for {country}"
                )
            except pd.errors.EmptyDataError:
                country_data.to_csv(cache_path, index=False)
                logger.info(
                    f"Created new CSV cache with {len(country_data)} entries for {country}"
                )
        else:
            country_data.to_csv(cache_path, index=False)
            logger.info(
                f"Created new CSV cache with {len(country_data)} entries for {country}"
            )
    except Exception as e:
        logger.warning(f"Error updating CSV cache: {str(e)}")


def validate_and_standardize_df(
    df, target_columns, valid_fueltypes=None, valid_technologies=None, valid_sets=None
):
    """Validate and standardize power plant DataFrame.

    Ensures data consistency by validating fuel types, technologies,
    and sets against allowed values. Adds missing columns and removes
    metadata columns.

    Parameters
    ----------
    df : pd.DataFrame
        Raw power plant data
    target_columns : list of str
        Expected columns in output
    valid_fueltypes : list of str, optional
        Allowed fuel types. Uses VALID_FUELTYPES if None
    valid_technologies : list of str, optional
        Allowed technologies. Uses VALID_TECHNOLOGIES if None
    valid_sets : list of str, optional
        Allowed set types. Uses VALID_SETS if None

    Returns
    -------
    pd.DataFrame
        Standardized DataFrame with validated values and
        consistent column structure

    Notes
    -----
    Invalid values are not removed but warnings are logged.
    Missing columns are added with NaN values.
    """
    if df.empty:
        return df

    if valid_fueltypes is None:
        valid_fueltypes = VALID_FUELTYPES
    if valid_technologies is None:
        valid_technologies = VALID_TECHNOLOGIES
    if valid_sets is None:
        valid_sets = VALID_SETS

    df = df.copy()

    metadata_columns = [
        "created_at",
        "config_hash",
        "config_version",
        "processing_parameters",
    ]
    df = df.drop(columns=[col for col in metadata_columns if col in df.columns])

    if "Fueltype" in df.columns:
        invalid_fuels = (
            df["Fueltype"].dropna().apply(lambda x: x not in valid_fueltypes)
        )
        if invalid_fuels.any():
            logger.warning(
                f"Found {invalid_fuels.sum()} rows with invalid Fueltype values"
            )
    else:
        logger.warning("Fueltype column is missing")
        df["Fueltype"] = np.nan

    if "Technology" in df.columns:
        invalid_techs = (
            df["Technology"].dropna().apply(lambda x: x not in valid_technologies)
        )
        if invalid_techs.any():
            logger.warning(
                f"Found {invalid_techs.sum()} rows with invalid Technology values"
            )
    else:
        logger.warning("Technology column is missing")
        df["Technology"] = np.nan

    if "Set" in df.columns:
        invalid_sets = df["Set"].dropna().apply(lambda x: x not in valid_sets)
        if invalid_sets.any():
            logger.warning(f"Found {invalid_sets.sum()} rows with invalid Set values")
    else:
        logger.warning("Set column is missing")
        df["Set"] = np.nan

    for col in target_columns:
        if col not in df.columns:
            df[col] = np.nan

    cols_to_keep = [col for col in target_columns if col in df.columns]
    df = df[cols_to_keep]

    return df

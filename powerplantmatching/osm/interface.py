"""
Interface functions for the OSM module.
Provides high-level functions to interact with OSM data for powerplantmatching.
"""

import logging
import os
from difflib import get_close_matches

import numpy as np
import pandas as pd

from .client import OverpassAPIClient
from .models import Unit, Units
from .rejection import RejectionTracker
from .utils import get_country_code
from .workflow import Workflow

logger = logging.getLogger(__name__)

# Constants
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

VALID_SETS = ["PP", "CHP", "Store"]


def get_client_params(osm_config, api_url, cache_dir):
    """
    Extract client parameters from config.

    Parameters
    ----------
    osm_config : dict
        OSM-specific configuration dictionary
    api_url : str
        URL for the Overpass API
    cache_dir : str
        Directory for caching OSM data

    Returns
    -------
    dict
        Parameters for OverpassAPIClient
    """
    return {
        "api_url": api_url,
        "cache_dir": cache_dir,
        "timeout": osm_config.get("overpass_api", {}).get("timeout", 300),
        "max_retries": osm_config.get("overpass_api", {}).get("max_retries", 3),
        "retry_delay": osm_config.get("overpass_api", {}).get("retry_delay", 5),
        "show_progress": osm_config.get("show_progress", True),
    }


def validate_all_countries(countries: list[str]) -> tuple[list[str], dict[str, str]]:
    """
    Validate all countries before processing begins.

    Parameters
    ----------
    countries : list[str]
        List of country names to validate

    Returns
    -------
    tuple[list[str], dict[str, str]]
        (valid_countries, country_code_map)

    Raises
    ------
    ValueError
        If any countries are invalid, with helpful error message
    """
    import pycountry

    valid_countries = []
    invalid_countries = []
    country_code_map = {}

    # First pass: validate all countries
    for country in countries:
        country_code = get_country_code(country)
        if country_code is not None:
            valid_countries.append(country)
            country_code_map[country] = country_code
        else:
            invalid_countries.append(country)

    # If we have invalid countries, provide detailed error with suggestions
    if invalid_countries:
        # Get all valid country names for suggestions
        all_country_names = [c.name for c in pycountry.countries]  # type: ignore[attr-defined]
        all_country_names.extend([c.alpha_2 for c in pycountry.countries])  # type: ignore[attr-defined]
        all_country_names.extend([c.alpha_3 for c in pycountry.countries])  # type: ignore[attr-defined]

        # Add common variations
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
            "Turkey": "Türkiye",  # Recent name change
        }
        all_country_names.extend(country_variations.keys())

        error_parts = [
            f"❌ Invalid country names detected: {len(invalid_countries)} out of {len(countries)} countries",
            "\nInvalid entries:",
        ]

        for invalid in invalid_countries:
            error_parts.append(f"  ❌ '{invalid}'")

            # Get suggestions
            suggestions = get_close_matches(invalid, all_country_names, n=3, cutoff=0.6)
            if suggestions:
                strings = []
                for suggestion in suggestions:
                    strings.append(f"'{suggestion}'")
                error_parts.append(f"     ℹ️  Did you mean: {', '.join(strings)}")

            # Check if it's a common variation
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

    # Log successful validation
    logger.info(f"✅ Successfully validated all {len(valid_countries)} countries")
    logger.debug(
        f"Country codes: {', '.join(f'{c}={code}' for c, code in country_code_map.items())}"
    )

    return valid_countries, country_code_map


def process_countries(
    countries, csv_cache_path, cache_dir, update, osm_config, target_columns, raw=False
):
    """
    Process multiple countries and combine their data.

    Parameters
    ----------
    countries : list[str]
        List of country names
    csv_cache_path : str
        Path to the CSV cache file
    cache_dir : str
        Directory for caching OSM data
    update : bool
        Whether to force refresh data
    osm_config : dict
        OSM-specific configuration dictionary
    target_columns : list
        List of target columns to include in the result
    raw : bool, default False
        Whether to return data in raw OSM format without additional formatting

    Returns
    -------
    pd.DataFrame
        Combined data from all countries
    """
    # VALIDATE ALL COUNTRIES FIRST - This is the key addition!
    logger.info(f"Starting country validation for {len(countries)} countries...")
    try:
        valid_countries, country_code_map = validate_all_countries(countries)
    except ValueError as e:
        # Re-raise with additional context
        raise ValueError(
            f"Country validation failed. Cannot proceed with OSM data processing.\n{str(e)}"
        ) from e

    # Log the processing plan
    logger.info(
        f"Country validation successful! Processing OSM data for {len(valid_countries)} countries: "
        f"{', '.join(valid_countries[:5])}"
        f"{f' and {len(valid_countries) - 5} more' if len(valid_countries) > 5 else ''}"
    )

    api_url = osm_config.get("overpass_api", {}).get("url")
    current_config_hash = Unit._generate_config_hash(osm_config)

    # Initialize empty DataFrame for all valid data
    all_valid_data = pd.DataFrame()

    # Process each country individually (using validated country list)
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

        # Add this country's data to the overall result
        if country_data is not None and not country_data.empty:
            # If not in raw mode, cleanup before adding to overall results
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
    """
    Process a single country's data, using cache if valid.

    Parameters
    ----------
    country : str
        Country name
    csv_cache_path : str
        Path to the CSV cache file
    cache_dir : str
        Directory for caching OSM data
    api_url : str
        URL for the Overpass API
    config_hash : str
        Hash of the current configuration
    update : bool
        Whether to force refresh data
    osm_config : dict
        OSM-specific configuration dictionary

    Returns
    -------
    pd.DataFrame
        Country data if processing successful, None otherwise
    """
    force_refresh = osm_config.get("force_refresh", False)

    # If update (force_refresh) is True, skip all cache checks
    if force_refresh:
        return process_from_api(csv_cache_path, cache_dir, api_url, country, osm_config)

    # First try CSV cache
    country_data = check_csv_cache(csv_cache_path, country, config_hash, update)

    if country_data is None:
        # Extract client parameters
        client_params = get_client_params(osm_config, api_url, cache_dir)

        # Check units cache
        country_data = check_units_cache(
            csv_cache_path, country, config_hash, client_params
        )

    # If both caches failed, process from scratch
    if country_data is None:
        country_data = process_from_api(
            csv_cache_path, cache_dir, api_url, country, osm_config
        )

    return country_data


def check_csv_cache(cache_path, country, config_hash, update):
    """
    Check if valid data exists in CSV cache.

    Parameters
    ----------
    cache_path : str
        Path to the CSV cache file
    country : str
        Country name
    config_hash : str
        Hash of the current configuration
    update : bool
        Whether to force refresh data

    Returns
    -------
    pd.DataFrame or None
        Country data if valid cache found, None otherwise
    """
    # Skip cache if update is True or cache doesn't exist
    if update or not os.path.exists(cache_path):
        return None

    try:
        # Try reading the CSV file
        csv_data = pd.read_csv(cache_path)

        # Check if the CSV has any data
        if csv_data.empty:
            logger.debug(f"CSV cache exists but is empty: {cache_path}")
            return None

        # Filter to just this country
        country_rows = csv_data[csv_data["Country"] == country]

        # Check if we have data for this country
        if country_rows.empty:
            logger.debug(f"No data for {country} in CSV cache")
            return None

        # Check if we have the config hash column
        if "config_hash" not in country_rows.columns:
            logger.debug(f"CSV cache for {country} missing config_hash column")
            return None

        # Check if the config hash matches
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
    """
    Check if valid data exists in the units cache.

    Parameters
    ----------
    csv_cache_path : str
        Path to the CSV cache file
    country : str
        Country name
    config_hash : str
        Hash of the current configuration
    client_params : dict
        Parameters for OverpassAPIClient

    Returns
    -------
    pd.DataFrame or None
        Country data if valid cache found, None otherwise
    """
    country_code = get_country_code(country)
    if country_code is None:
        logger.warning(f"Invalid country name: {country}, skipping units cache check")
        return None

    try:
        with OverpassAPIClient(**client_params) as client:
            # Get cached units for this country
            cached_units = client.cache.get_units(country_code)

            # Filter only valid units for current config
            valid_units = []
            for unit in cached_units:
                if hasattr(unit, "config_hash") and unit.config_hash == config_hash:
                    valid_units.append(unit)

            if valid_units:
                logger.info(
                    f"Found {len(valid_units)} valid cached units for {country}"
                )
                # Convert units to DataFrame
                country_data = pd.DataFrame([unit.to_dict() for unit in valid_units])

                # Add to main CSV cache for faster access next time
                update_csv_cache(csv_cache_path, country, country_data)
                return country_data
    except Exception as e:
        logger.error(f"Error accessing units cache for {country}: {str(e)}")

    return None


def process_from_api(csv_cache_path, cache_dir, api_url, country, osm_config):
    """
    Process country data from the API.

    Parameters
    ----------
    csv_cache_path : str
        Path to the CSV cache file
    cache_dir : str
        Directory for caching OSM data
    api_url : str
        URL for the Overpass API
    country : str
        Country name
    osm_config : dict
        OSM-specific configuration dictionary

    Returns
    -------
    pd.DataFrame or None
        Country data if processing successful, None otherwise
    """
    logger.info(f"No valid cache for {country}, processing from API")

    try:
        # Get client parameters
        client_params = get_client_params(osm_config, api_url, cache_dir)

        with OverpassAPIClient(**client_params) as client:
            # Initialize Units collection and rejection tracker
            units_collection = Units()
            rejection_tracker = RejectionTracker()

            # Create workflow with proper initialization
            workflow = Workflow(
                client=client,
                rejection_tracker=rejection_tracker,
                units=units_collection,
                config=osm_config,
            )

            # Process country data - returns (units_collection, rejection_tracker)
            updated_units_collection, _ = workflow.process_country_data(country)

            # Filter to get only units from the current country
            # This is important because the Units collection may accumulate data
            country_units = updated_units_collection.filter_by_country(country)

            if len(country_units) > 0:
                logger.info(f"Processed {len(country_units)} units for {country}")
                # Convert units to DataFrame
                country_data = pd.DataFrame([unit.to_dict() for unit in country_units])

                # Save to CSV cache for faster access next time
                update_csv_cache(csv_cache_path, country, country_data)

                return country_data
            else:
                logger.warning(f"No units found for {country}")
                return None
    except Exception as e:
        logger.error(f"Error processing {country} from API: {str(e)}")
        return None


def update_csv_cache(cache_path, country, country_data):
    """
    Update the CSV cache with new country data.

    Parameters
    ----------
    cache_path : str
        Path to the CSV cache file
    country : str
        Country name
    country_data : pd.DataFrame
        Data to cache
    """
    if country_data.empty:
        return

    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)

        if os.path.exists(cache_path):
            # Update existing CSV
            try:
                full_csv = pd.read_csv(cache_path)
                # Remove existing entries for this country
                full_csv = full_csv[full_csv["Country"] != country]
                # Append new data
                updated_csv = pd.concat([full_csv, country_data], ignore_index=True)
                updated_csv.to_csv(cache_path, index=False)
                logger.info(
                    f"Updated CSV cache with {len(country_data)} entries for {country}"
                )
            except pd.errors.EmptyDataError:
                # File exists but is empty or corrupted
                country_data.to_csv(cache_path, index=False)
                logger.info(
                    f"Created new CSV cache with {len(country_data)} entries for {country}"
                )
        else:
            # Create new CSV
            country_data.to_csv(cache_path, index=False)
            logger.info(
                f"Created new CSV cache with {len(country_data)} entries for {country}"
            )
    except Exception as e:
        logger.warning(f"Error updating CSV cache: {str(e)}")


def validate_and_standardize_df(
    df, target_columns, valid_fueltypes=None, valid_technologies=None, valid_sets=None
):
    """
    Clean and validate a DataFrame according to powerplantmatching standards.

    Parameters
    ----------
    df : pd.DataFrame
        Data to validate
    target_columns : list
        List of target columns to keep
    valid_fueltypes : list, optional
        List of valid fuel types
    valid_technologies : list, optional
        List of valid technologies
    valid_sets : list, optional
        List of valid set types

    Returns
    -------
    pd.DataFrame
        Validated data
    """
    if df.empty:
        return df

    # Use default valid types if not provided
    if valid_fueltypes is None:
        valid_fueltypes = VALID_FUELTYPES
    if valid_technologies is None:
        valid_technologies = VALID_TECHNOLOGIES
    if valid_sets is None:
        valid_sets = VALID_SETS

    # Make a copy to avoid modifying the original
    df = df.copy()

    # Remove metadata columns
    metadata_columns = [
        "created_at",
        "config_hash",
        "config_version",
        "processing_parameters",
    ]
    df = df.drop(columns=[col for col in metadata_columns if col in df.columns])

    # Validate Fueltype values
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

    # Validate Technology values
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

    # Ensure Set column has a value
    if "Set" in df.columns:
        invalid_sets = df["Set"].dropna().apply(lambda x: x not in valid_sets)
        if invalid_sets.any():
            logger.warning(f"Found {invalid_sets.sum()} rows with invalid Set values")
    else:
        logger.warning("Set column is missing")
        df["Set"] = np.nan

    # Ensure all target columns exist
    for col in target_columns:
        if col not in df.columns:
            df[col] = np.nan

    # Keep only target columns
    cols_to_keep = [col for col in target_columns if col in df.columns]
    df = df[cols_to_keep]

    return df

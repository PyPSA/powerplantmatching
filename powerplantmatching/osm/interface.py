"""
Interface functions for the OSM module.
Provides high-level functions to interact with OSM data for powerplantmatching.
"""

import logging
import os

import numpy as np
import pandas as pd

from .client import OverpassAPIClient
from .models import Unit
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
    "Boiled Water Reactor",
    "Pressurized Water Reactor",
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
]

VALID_SETS = ["PP", "CHP", "Store"]


def process_countries(countries, csv_cache_path, cache_dir, update, config):
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
    config : dict
        Configuration dictionary

    Returns
    -------
    pd.DataFrame
        Combined data from all countries
    """
    api_url = config.get("OSM", {}).get("overpass_api", {}).get("url")
    current_config_hash = Unit._generate_config_hash(config.get("OSM", {}))
    target_columns = config["target_columns"]

    # Initialize empty DataFrame for all valid data
    all_valid_data = pd.DataFrame()

    # Process each country individually
    for country in countries:
        country_data = process_single_country(
            country,
            csv_cache_path,
            cache_dir,
            api_url,
            current_config_hash,
            update,
            config,
        )

        # Add this country's data to the overall result
        if country_data is not None and not country_data.empty:
            # Cleanup before adding to overall results
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

    return all_valid_data


def process_single_country(
    country, csv_cache_path, cache_dir, api_url, config_hash, update, config
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
    config : dict
        Configuration dictionary

    Returns
    -------
    pd.DataFrame
        Country's data, or None if processing failed
    """
    # First try CSV cache
    country_data = check_csv_cache(csv_cache_path, country, config_hash, update)

    # If CSV cache failed, try units cache
    if country_data is None:
        country_data = check_units_cache(
            csv_cache_path, cache_dir, api_url, country, config_hash
        )

    # If both caches failed, process from scratch
    if country_data is None:
        country_data = process_from_api(
            csv_cache_path, cache_dir, api_url, country, update, config
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
    if not os.path.exists(cache_path) or update:
        return None

    try:
        csv_data = pd.read_csv(cache_path)
        # Filter to just this country
        country_rows = csv_data[csv_data["Country"] == country]

        if not country_rows.empty and "config_hash" in country_rows.columns:
            # Check if this country's data has matching config hash
            if country_rows["config_hash"].iloc[0] == config_hash:
                logger.info(f"Using CSV cache for {country} (matching config)")
                return country_rows.copy()
    except Exception as e:
        logger.debug(f"Error reading CSV cache: {str(e)}")

    return None


def check_units_cache(csv_cache_path, cache_dir, api_url, country, config_hash):
    """
    Check if valid data exists in the units cache.

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
    config_hash : str
        Hash of the current configuration

    Returns
    -------
    pd.DataFrame or None
        Country data if valid cache found, None otherwise
    """
    country_code = get_country_code(country)
    try:
        with OverpassAPIClient(api_url=api_url, cache_dir=cache_dir) as client:
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


def process_from_api(csv_cache_path, cache_dir, api_url, country, update, config):
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
    update : bool
        Whether to force refresh data
    config : dict
        Configuration dictionary

    Returns
    -------
    pd.DataFrame or None
        Country data if processing successful, None otherwise
    """
    logger.info(f"No valid cache for {country}, processing from API")

    try:
        with OverpassAPIClient(api_url=api_url, cache_dir=cache_dir) as client:
            workflow = Workflow(client=client, config=config["OSM"])

            units, _ = workflow.process_country_data(country, force_process=update)

            if units:
                logger.info(f"Processed {len(units)} units for {country}")
                # Convert units to DataFrame
                country_data = pd.DataFrame([unit.to_dict() for unit in units])

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
    if not country_data.empty:
        try:
            if os.path.exists(cache_path):
                # Update existing CSV
                full_csv = pd.read_csv(cache_path)
                # Remove existing entries for this country
                full_csv = full_csv[full_csv["Country"] != country]
                # Append new data
                updated_csv = pd.concat([full_csv, country_data], ignore_index=True)
                updated_csv.to_csv(cache_path, index=False)
            else:
                # Create new CSV
                country_data.to_csv(cache_path, index=False)
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

    Returns
    -------
    pd.DataFrame
        Validated data
    """
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
    for col in metadata_columns:
        if col in df.columns:
            df = df.drop(columns=[col])

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

    # Keep only target columns
    cols_to_keep = [col for col in target_columns if col in df.columns]
    df = df[cols_to_keep]

    return df


def clean_and_format_data(df, target_columns):
    """
    Remove metadata columns and format data for return.

    Parameters
    ----------
    df : pd.DataFrame
        Data to clean
    target_columns : list
        List of target columns to keep

    Returns
    -------
    pd.DataFrame
        Cleaned data
    """
    # Remove metadata columns
    metadata_columns = [
        "created_at",
        "config_hash",
        "config_version",
        "processing_parameters",
    ]
    for col in metadata_columns:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Ensure all target columns exist
    for col in target_columns:
        if col not in df.columns:
            df[col] = np.nan

    # Keep only target columns
    cols_to_keep = [col for col in target_columns if col in df.columns]
    return df[cols_to_keep]

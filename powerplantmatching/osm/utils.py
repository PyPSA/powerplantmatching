# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Utility functions for OSM power plant processing.

This module provides helper functions for capacity parsing, country
validation, geometric calculations, and configuration handling.
"""

import logging
import math
import os
import re
from typing import Any

import pycountry

from powerplantmatching.core import _data_in, get_config

logger = logging.getLogger(__name__)


def is_valid_unit(element: dict[str, Any], unit_type: str) -> bool:
    """Check if OSM element is a valid power unit."""
    assert unit_type in ["plant", "generator"], "Invalid unit type"

    if "tags" not in element:
        return False

    tags: dict[str, str] = element["tags"]
    if "power" not in tags:
        return False

    return tags.get("power") == unit_type


def get_source_config(
    config: dict[str, Any], source_type: str, section: str | None = None
) -> dict[str, Any]:
    """Get configuration for a specific source type."""
    source_config = config.get("sources", {}).get(source_type, {})
    if section:
        return source_config.get(section) or {}
    return source_config


def parse_capacity_value(
    value: str,
    advanced_extraction: bool,
    regex_patterns: list[str] | None = None,
) -> tuple[bool, float | None, str]:
    """Parse capacity value string to MW.

    Handles various formats and units for power capacity values,
    converting them to megawatts. Supports both basic formats
    (e.g., "50 MW") and advanced patterns with suffixes.

    Parameters
    ----------
    value : str
        Capacity string to parse (e.g., "50 MW", "1.5GW", "100kWp")
    advanced_extraction : bool
        If True, use flexible regex patterns for complex formats
    regex_patterns : list[str], optional
        Custom regex patterns for advanced extraction

    Returns
    -------
    tuple[bool, float or None, str]
        (success, capacity_mw, original_value_or_error)
        - success: True if parsing succeeded
        - capacity_mw: Capacity in megawatts or None
        - original_value_or_error: Original string or error type

    Examples
    --------
    >>> parse_capacity_value("50 MW", False)
    (True, 50.0, "50 MW")

    >>> parse_capacity_value("1.5GW", True)
    (True, 1500.0, "1.5GW")

    >>> parse_capacity_value("100kWp", True)  # 'p' suffix for peak
    (True, 0.1, "100kWp")
    """
    value_str = value.strip()
    original_value_str = value_str

    if "," in value_str and "." not in value_str:
        value_str = value_str.replace(",", ".")

    if not advanced_extraction:
        pattern = r"^(\d+(?:\.\d+)?)\s*(mw|mwp|MW|MWP)$"
        try:
            match = re.match(pattern, value_str)
            if not match:
                pattern_no_space = r"^(\d+(?:\.\d+)?)(mw|mwp|MW|MWP)$"
                match = re.match(pattern_no_space, value_str)
        except Exception as e:
            logger.error(
                f"Error parsing capacity value '{original_value_str}' with regex: {e}"
            )
            return False, None, "regex_error"
    else:
        if regex_patterns is None:
            regex_patterns = [
                # Matches: "100 MW", "15.5 kW", "50 MWp" (number with optional space and unit)
                r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+(?:p|el|e)?)$",
                # Matches: "100MW", "15.5kW", "50MWp" (number directly followed by unit)
                r"^(\d+(?:\.\d+)?)([a-zA-Z]+(?:p|el|e)?)$",
            ]

        match = None
        for pattern in regex_patterns:
            try:
                match = re.match(pattern, value_str)
                if match:
                    break
            except Exception as e:
                logger.error(
                    f"Error parsing capacity value '{original_value_str}' with regex: {e}"
                )
                continue

    if match:
        number_str, unit = match.groups()

        try:
            number = float(number_str)

            unit_lower = unit.lower()

            base_unit = unit_lower
            for suffix in ["el", "p", "e"]:
                if base_unit.endswith(suffix):
                    base_unit = base_unit[: -len(suffix)]
                    break

            if base_unit in ["w", "watt", "watts"]:
                return True, number * 0.000001, original_value_str
            elif base_unit in ["kw", "kilowatt", "kilowatts"]:
                return True, number * 0.001, original_value_str
            elif base_unit in ["mw", "megawatt", "megawatts"]:
                return True, number, original_value_str
            elif base_unit in ["gw", "gigawatt", "gigawatts"]:
                return True, number * 1000, original_value_str
            else:
                return False, None, "unknown_unit"
        except ValueError:
            return False, None, "value_error"
    else:
        return False, None, "regex_no_match"


def get_country_code(country: str) -> str | None:
    """Get ISO 3166-1 alpha-2 code for country.

    Parameters
    ----------
    country : str
        Country name, ISO code, or common variation

    Returns
    -------
    str or None
        Two-letter country code or None if not found

    Examples
    --------
    >>> get_country_code("Germany")
    'DE'
    >>> get_country_code("USA")
    'US'
    >>> get_country_code("DEU")
    'DE'
    """
    try:
        country_obj = pycountry.countries.lookup(country)
        return country_obj.alpha_2
    except LookupError:
        logger.warning(f"Invalid country name: {country}")
        return None


def standardize_country_name(country_value: str) -> str:
    """Standardize country codes to full country names.

    Ensures all output uses consistent country names rather than codes.

    Parameters
    ----------
    country_value : str
        Either a country code (e.g., 'CL', 'CHI') or country name (e.g., 'Chile')

    Returns
    -------
    str
        Full country name, or original value if not found

    Examples
    --------
    >>> standardize_country_name("CL")
    'Chile'
    >>> standardize_country_name("Chile")
    'Chile'
    >>> standardize_country_name("USA")
    'United States'
    """
    # If it's already a known country name, return it
    try:
        # First try to find by name
        country = pycountry.countries.lookup(country_value)
        return country.name
    except LookupError:
        pass

    # Return original value if no match found
    return country_value


def calculate_area(coordinates: list[dict[str, float]]) -> float:
    """Calculate area of polygon from coordinates.

    Uses the shoelace formula with coordinates projected to meters
    using haversine distance for accuracy.

    Parameters
    ----------
    coordinates : list[dict[str, float]]
        List of coordinate dicts with 'lat' and 'lon' keys

    Returns
    -------
    float
        Area in square meters
    """
    if len(coordinates) < 3:
        return 0.0

    ref_lat = coordinates[0]["lat"]
    ref_lon = coordinates[0]["lon"]

    points = []
    for coord in coordinates:
        dy = haversine_distance(ref_lat, ref_lon, coord["lat"], ref_lon)
        dx = haversine_distance(ref_lat, ref_lon, ref_lat, coord["lon"])

        if coord["lat"] < ref_lat:
            dy = -dy
        if coord["lon"] < ref_lon:
            dx = -dx

        points.append((dx, dy))

    area = 0.0
    n = len(points)
    for i in range(n):
        j = (i + 1) % n
        area += points[i][0] * points[j][1]
        area -= points[j][0] * points[i][1]
    area = abs(area) / 2.0

    return area


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points using haversine formula.

    Parameters
    ----------
    lat1, lon1 : float
        First point coordinates (degrees)
    lat2, lon2 : float
        Second point coordinates (degrees)

    Returns
    -------
    float
        Distance in meters
    """
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    r = 6371000
    return c * r


def get_osm_cache_paths(config: dict | None = None) -> tuple[str, str]:
    """Get OSM cache directory and CSV file paths.

    Resolves cache paths from configuration, handling relative paths,
    home directory expansion, and defaults.

    Parameters
    ----------
    config : dict, optional
        Configuration dict. If None, loads default config.

    Returns
    -------
    tuple[str, str]
        (cache_dir, csv_cache_path) where:
        - cache_dir: Directory containing all OSM caches
        - csv_cache_path: Full path to CSV cache file

    Examples
    --------
    >>> cache_dir, csv_path = get_osm_cache_paths()
    >>> print(cache_dir)
    /home/user/powerplantmatching/data/osm_cache

    >>> # With custom config
    >>> config = {'OSM': {'cache_dir': '~/osm_data', 'fn': 'plants.csv'}}
    >>> cache_dir, csv_path = get_osm_cache_paths(config)
    >>> print(csv_path)
    /home/user/osm_data/plants.csv
    """
    if config is None:
        actual_config: dict = get_config()
    else:
        actual_config = config

    osm_config = actual_config.get("OSM", {})
    fn = osm_config.get("fn", "osm_data.csv")

    cache_dir = osm_config.get("cache_dir")
    if cache_dir:
        cache_dir = os.path.expanduser(cache_dir)
        if not os.path.isabs(cache_dir):
            data_dir = os.path.dirname(_data_in(""))
            cache_dir = os.path.join(data_dir, cache_dir)
    else:
        data_dir = os.path.dirname(_data_in(""))
        cache_dir = os.path.join(data_dir, "osm_cache")

    csv_cache_path = os.path.join(cache_dir, fn)

    return cache_dir, csv_cache_path


def determine_set_type(technology: str | None, config: dict[str, Any]) -> str | None:
    """Determine Set type based on technology using simple mapping."""
    if technology is None:
        return None
    technology = technology.strip()
    if technology == "":
        return None

    set_mapping = config.get("set_mapping", {})

    for set_type, technologies in set_mapping.items():
        if technology in technologies:
            return set_type

    return None

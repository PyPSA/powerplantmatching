import logging
import math
import re
from typing import Any, Optional

import pycountry

logger = logging.getLogger(__name__)


def is_valid_unit(element: dict[str, Any], unit_type: str) -> bool:
    """
    Check if an element is a valid power unit

    Parameters
    ----------
    element : dict[str, Any]
        OSM element data
    unit_type : str

    """
    assert unit_type in ["plant", "generator"], "Invalid unit type"

    if "tags" not in element:
        return False

    tags: dict[str, str] = element["tags"]
    if "power" not in tags:
        return False

    return tags.get("power") == unit_type


# Configuration utilities
def get_source_config(
    config: dict[str, Any], source_type: str, section: Optional[str] = None
) -> dict[str, Any]:
    """
    Get source-specific configuration

    Parameters
    ----------
    config : dict[str, Any]
        Main configuration dictionary
    source_type : str
        Source type (e.g., "solar", "wind")
    section : Optional[str]
        Configuration section to retrieve

    Returns
    -------
    dict[str, Any]
        Source-specific configuration
    """
    source_config = config.get("sources", {}).get(source_type, {})
    if section:
        return source_config.get(section) or {}
    return source_config


def parse_capacity_value(
    value: str,
    advanced_extraction: bool,
    regex_patterns: list[str] | None = None,
) -> tuple[bool, float | None, str]:
    """
    Parse capacity value from string with improved error reporting

    Parameters
    ----------
    value : str
        Capacity value string
    advanced_extraction : bool
        Whether to use advanced extraction with regex
    regex_patterns : list[str]
        List of regular expressions for extracting value and unit

    Returns
    -------
    tuple[bool, float | None, str]
        (success, parsed_value, unit_or_error_info)
    """
    # Normalize the input string - trim
    value_str = value.strip()
    original_value_str = value_str

    # Try to handle comma as decimal separator
    if "," in value_str and "." not in value_str:
        value_str = value_str.replace(",", ".")

    if not advanced_extraction:
        # Basic extraction with simple pattern
        pattern = r"^(\d+(?:\.\d+)?)\s*(mw|mwp|MW|MWP)$"
        try:
            match = re.match(pattern, value_str)
            if not match:
                # Try without space between number and unit
                pattern_no_space = r"^(\d+(?:\.\d+)?)(mw|mwp|MW|MWP)$"
                match = re.match(pattern_no_space, value_str)
        except Exception as e:
            logger.error(
                f"Error parsing capacity value '{original_value_str}' with regex: {e}"
            )
            return False, None, "regex_error"
    else:
        # Use default patterns if none provided
        if regex_patterns is None:
            regex_patterns = [
                # Pattern with optional space between number and unit, allowing various suffixes
                r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+(?:p|el|e)?)$",
                # Pattern without space (e.g., "460KW", "500kWel")
                r"^(\d+(?:\.\d+)?)([a-zA-Z]+(?:p|el|e)?)$",
            ]

        # Try to match with patterns
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

            # Convert to MW based on unit
            unit_lower = unit.lower()

            # Strip common suffixes (p=power, el/e=electrical)
            # to get the base unit
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
    """
    Get the ISO 3166-1 alpha-2 country code for a country

    Parameters
    ----------
    country : str
        Country name

    Returns
    -------
    str
        Two-letter country code

    Raises
    ------
    ValueError
        If the country name is invalid
    """
    try:
        country_obj = pycountry.countries.lookup(country)
        return country_obj.alpha_2
    except LookupError:
        logger.warning(f"Invalid country name: {country}")
        return None


def calculate_area(coordinates: list[dict[str, float]]) -> float:
    """
    Calculate area of a polygon in square meters using haversine formula

    Parameters
    ----------
    coordinates : list[dict[str, float]]
        list of {lat, lon} coordinate dictionaries

    Returns
    -------
    float
        Area in square meters
    """
    if len(coordinates) < 3:
        return 0.0

    # Reference point for flat earth approximation
    ref_lat = coordinates[0]["lat"]
    ref_lon = coordinates[0]["lon"]

    # Convert coordinates to flat earth approximation
    points = []
    for coord in coordinates:
        dy = haversine_distance(ref_lat, ref_lon, coord["lat"], ref_lon)
        dx = haversine_distance(ref_lat, ref_lon, ref_lat, coord["lon"])

        # Adjust sign based on direction
        if coord["lat"] < ref_lat:
            dy = -dy
        if coord["lon"] < ref_lon:
            dx = -dx

        points.append((dx, dy))

    # Calculate area using shoelace formula
    area = 0.0
    n = len(points)
    for i in range(n):
        j = (i + 1) % n
        area += points[i][0] * points[j][1]
        area -= points[j][0] * points[i][1]
    area = abs(area) / 2.0

    return area


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points
    on the earth specified in decimal degrees

    Parameters
    ----------
    lat1 : float
        Latitude of first point
    lon1 : float
        Longitude of first point
    lat2 : float
        Latitude of second point
    lon2 : float
        Longitude of second point

    Returns
    -------
    float
        Distance in meters
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    r = 6371000  # Radius of earth in meters
    return c * r

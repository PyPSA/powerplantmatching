import math
import re
from typing import Any, Optional

import pycountry


# Element processing utilities
def get_element_coordinates(
    element: dict[str, Any],
) -> tuple[Optional[float], Optional[float]]:
    """
    Get coordinates for an OSM element

    Parameters
    ----------
    element : dict[str, Any]
        OSM element data

    Returns
    -------
    tuple[Optional[float], Optional[float]]
        (latitude, longitude) if found, (None, None) otherwise
    """
    if element["type"] == "node":
        if "lat" in element and "lon" in element:
            return element["lat"], element["lon"]
    elif element["type"] == "way":
        if "center" in element:
            return element["center"]["lat"], element["center"]["lon"]
    elif element["type"] == "relation":
        if "center" in element:
            return element["center"]["lat"], element["center"]["lon"]

    return None, None


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

    tags = element.get("tags")
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
    regex_patterns: list[str] = [r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+p?)$"],
) -> tuple[bool, Optional[float], str]:
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
    tuple[bool, Optional[float], str]
        (success, parsed_value, unit_or_error_info)
    """
    # Normalize the input string - trim and convert to lowercase
    value_str = value.strip().lower()
    original_value_str = value_str

    # Proceed with normal parsing
    if not advanced_extraction:
        # Basic extraction with simple pattern
        pattern = r"(\d+(?:\.\d+)?)\s*(mw|mwp)"
        match = re.match(pattern, value_str)

    else:
        # Try to handle comma as decimal separator
        if "," in value_str and not "." in value_str:
            # Try with comma replacement
            value_str = value_str.replace(",", ".")

        try:
            # Check if it is a number. We assume it is megawatts if no unit is provided
            number = float(value_str)
            return True, number, original_value_str
        except ValueError:
            # Advanced extraction with multiple configurable regex patterns
            match = None
            for pattern in regex_patterns:
                match = re.match(pattern, value_str)
                if match:
                    break

    if match:
        number_str, unit = match.groups()
        try:
            number = float(number_str)

            # Convert to MW based on unit
            unit_lower = unit.lower()
            if unit_lower in ["w", "wp", "watt", "watts"]:
                return True, number * 0.000001, original_value_str
            elif unit_lower in ["kw", "kwp", "kilowatt", "kilowatts"]:
                return True, number * 0.001, original_value_str
            elif unit_lower in ["mw", "mwp", "megawatt", "megawatts"]:
                return True, number, original_value_str
            elif unit_lower in ["gw", "gwp", "gigawatt", "gigawatts"]:
                return True, number * 1000, original_value_str
            else:
                return False, None, "unknown_unit"
        except ValueError:
            return False, None, "value_error"
    else:
        return False, None, "regex_no_match"


def get_country_code(country: str) -> str:
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
        raise ValueError(f"Invalid country name: {country}")


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

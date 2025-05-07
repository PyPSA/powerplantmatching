import re
from typing import Any, Optional


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
                return True, number * 0.000001, unit
            elif unit_lower in ["kw", "kwp", "kilowatt", "kilowatts"]:
                return True, number * 0.001, unit
            elif unit_lower in ["mw", "mwp", "megawatt", "megawatts"]:
                return True, number, unit
            elif unit_lower in ["gw", "gwp", "gigawatt", "gigawatts"]:
                return True, number * 1000, unit
            else:
                return False, None, "unknown_unit"
        except ValueError:
            return False, None, "value_error"
    else:
        return False, None, "regex_no_match"

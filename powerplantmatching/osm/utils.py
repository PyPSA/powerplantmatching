import re
from typing import Any, Optional

import pycountry

from .models import ElementType, RejectionReason


def extract_source_from_tags(tags: dict[str, str]) -> Optional[str]:
    """
    Extract power source from OSM tags

    Parameters
    ----------
    tags : dict[str, str]
        OSM element tags

    Returns
    -------
    Optional[str]
        Power source if found, None otherwise
    """
    source_keys = [
        "plant:source",
        "generator:source",
        "power:source",
        "energy:source",
        "source",
        "generator:type",
        "plant:type",
    ]

    for key in source_keys:
        if key in tags:
            source = tags[key].lower()
            # Normalize common variations
            if source in ["pv", "photovoltaic", "solar_photovoltaic"]:
                return "solar"
            elif source in ["wind_turbine", "wind_generator"]:
                return "wind"
            # Add more normalizations as needed
            return source

    return None


def extract_technology_from_tags(tags: dict[str, str]) -> Optional[str]:
    """
    Extract technology information from OSM tags

    Parameters
    ----------
    tags : dict[str, str]
        OSM element tags

    Returns
    -------
    Optional[str]
        Technology if found, None otherwise
    """
    tech_keys = [
        "plant:method",
        "plant:type",
        "generator:method",
        "generator:type",
        "power:method",
        "power:type",
    ]

    for key in tech_keys:
        if key in tags:
            return tags[key]

    return None


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


def is_valid_plant(element: dict[str, Any]) -> bool:
    """
    Check if an element is a valid power plant

    Parameters
    ----------
    element : dict[str, Any]
        OSM element data

    Returns
    -------
    bool
        True if element is a valid power plant, False otherwise
    """
    if "tags" not in element:
        return False

    tags = element.get("tags", {})
    return tags.get("power") == "plant"


def is_valid_generator(element: dict[str, Any]) -> bool:
    """
    Check if an element is a valid generator

    Parameters
    ----------
    element : dict[str, Any]
        OSM element data

    Returns
    -------
    bool
        True if element is a valid generator, False otherwise
    """
    if "tags" not in element:
        return False

    tags = element.get("tags", {})
    return tags.get("power") == "generator"


# Rejection utilities
def create_rejection_entry(
    element: dict[str, Any],
    reason: RejectionReason,
    details: Optional[str] = None,
    category: str = "processor",
) -> dict[str, Any]:
    """
    Create a standardized rejection entry

    Parameters
    ----------
    element : dict[str, Any]
        OSM element data
    reason : RejectionReason
        Reason for rejection
    details : Optional[str]
        Additional details about the rejection
    category : str
        Category for grouping rejections

    Returns
    -------
    dict[str, Any]
        Standardized rejection entry
    """
    element_id = f"{element['type']}/{element['id']}"
    element_type = ElementType(element["type"])

    return {
        "element_id": element_id,
        "element_type": element_type,
        "reason": reason,
        "details": details,
        "category": category,
    }


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


def get_capacity_extraction_config(
    config: dict[str, Any], source_type: Optional[str] = None
) -> dict[str, Any]:
    """
    Get capacity extraction configuration

    Parameters
    ----------
    config : dict[str, Any]
        Main configuration dictionary
    source_type : Optional[str]
        Source type for source-specific configuration

    Returns
    -------
    dict[str, Any]
        Capacity extraction configuration
    """
    # Get base capacity extraction config
    extraction_config = config.get("capacity_extraction", {})

    # If source type is provided, override with source-specific config
    if source_type:
        source_config = get_source_config(config, source_type, "capacity_extraction")
        # Merge configs, with source config taking precedence
        for key, value in source_config.items():
            if key == "additional_tags" and "tags" in extraction_config:
                # Special handling for tag lists - combine them
                extraction_config[key] = extraction_config.get("tags", []) + value
            else:
                extraction_config[key] = value

    return extraction_config


def parse_capacity_value(
    value: str,
    extraction: bool,
    regex_patterns: list[str] = [r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+p?)$"],
) -> tuple[bool, float, str]:
    """
    Parse capacity value from string with improved error reporting

    Parameters
    ----------
    value : str
        Capacity value string
    extraction : bool
        Whether to use advanced extraction with regex
    regex_patterns : list[str]
        List of regular expressions for extracting value and unit

    Returns
    -------
    tuple[bool, float, str]
        (success, parsed_value, unit_or_error_info)
    """
    if not value or not isinstance(value, str):
        return False, -1000.0, "invalid_input"

    # Normalize the input string - trim and convert to lowercase
    value_str = value.strip().lower()

    # Handle common placeholder values
    if value_str in ["yes", "true"]:
        return False, -1000.0, "placeholder_value"

    # Try to handle comma as decimal separator
    if "," in value_str and not "." in value_str:
        # Try with comma replacement
        try:
            corrected = value_str.replace(",", ".")
            # See if it's a valid number after replacement
            float(corrected.split()[0])
            # If we got here, it seems to be a valid number with comma decimal
            return False, -1000.0, "decimal_comma_format"
        except (ValueError, IndexError):
            pass

    # Proceed with normal parsing
    if extraction:
        # Advanced extraction with multiple configurable regex patterns
        match = None
        for pattern in regex_patterns:
            match = re.match(pattern, value_str)
            if match:
                break
    else:
        # Basic extraction with simple pattern
        pattern = r"(\d+(?:\.\d+)?)\s*(mw|mwp)"
        match = re.match(pattern, value_str)

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
                return False, -1000.0, "unknown_unit"
        except ValueError:
            return False, -1000.0, "value_error"
    else:
        return False, -1000.0, "regex_no_match"


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

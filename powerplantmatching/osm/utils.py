import logging
import math
import os
import re
from typing import Any, Optional

import pycountry

from powerplantmatching.core import _data_in, get_config

logger = logging.getLogger(__name__)


def is_valid_unit(element: dict[str, Any], unit_type: str) -> bool:
    assert unit_type in ["plant", "generator"], "Invalid unit type"

    if "tags" not in element:
        return False

    tags: dict[str, str] = element["tags"]
    if "power" not in tags:
        return False

    return tags.get("power") == unit_type


def get_source_config(
    config: dict[str, Any], source_type: str, section: Optional[str] = None
) -> dict[str, Any]:
    source_config = config.get("sources", {}).get(source_type, {})
    if section:
        return source_config.get(section) or {}
    return source_config


def parse_capacity_value(
    value: str,
    advanced_extraction: bool,
    regex_patterns: list[str] | None = None,
) -> tuple[bool, float | None, str]:
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
                r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+(?:p|el|e)?)$",
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
    try:
        country_obj = pycountry.countries.lookup(country)
        return country_obj.alpha_2
    except LookupError:
        logger.warning(f"Invalid country name: {country}")
        return None


def calculate_area(coordinates: list[dict[str, float]]) -> float:
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


def get_osm_cache_paths(config: Optional[dict] = None) -> tuple[str, str]:
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

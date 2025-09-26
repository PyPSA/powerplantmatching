# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Regional download functionality for OpenStreetMap power plant data.

This module provides tools for downloading OSM data for custom regions
(bounding boxes, circles, polygons) rather than entire countries. It
handles automatic country determination and cache updates.
"""

import logging
import math
from datetime import datetime
from typing import Any

from powerplantmatching.core import get_config
from powerplantmatching.osm.utils import get_osm_cache_paths

from .client import OverpassAPIClient

logger = logging.getLogger(__name__)


def region_download(
    regions: list[dict[str, Any]] | dict[str, Any] | None = None,
    download_type: str = "both",
    update_country_caches: bool = True,
    show_element_counts: bool = True,
    config: dict | None = None,
    api_url: str | None = None,
    cache_dir: str | None = None,
    timeout: int | None = None,
    max_retries: int | None = None,
    retry_delay: int | None = None,
    client: OverpassAPIClient | None = None,
) -> dict[str, Any]:
    """Download power infrastructure data for custom regions.

    Supports downloading data for regions defined by bounding boxes,
    circles (radius around center), or polygons. Automatically determines
    which countries the regions belong to and updates country caches.

    Parameters
    ----------
    regions : list[dict] or dict, optional
        Region definitions. Each region dict should have:
        - 'type': 'bbox', 'radius', or 'polygon'
        - 'name': Region name (optional)
        - Type-specific parameters (see examples)
    download_type : {'both', 'plants', 'generators'}
        Which element types to download
    update_country_caches : bool
        Update country caches with regional data
    show_element_counts : bool
        Show expected element counts before download
    config : dict, optional
        Configuration dict
    api_url : str, optional
        Overpass API URL
    cache_dir : str, optional
        Cache directory path
    timeout : int, optional
        Query timeout in seconds
    max_retries : int, optional
        Maximum retry attempts
    retry_delay : int, optional
        Delay between retries
    client : OverpassAPIClient, optional
        Existing client to use

    Returns
    -------
    dict
        Results with status, counts, and affected countries

    Examples
    --------
    Bounding box region:
    >>> region = {
    ...     'type': 'bbox',
    ...     'name': 'Berlin Area',
    ...     'bounds': [52.3, 13.1, 52.7, 13.7]  # [min_lat, min_lon, max_lat, max_lon]
    ... }

    Radius region:
    >>> region = {
    ...     'type': 'radius',
    ...     'name': 'Paris 50km',
    ...     'center': [48.8566, 2.3522],  # [lat, lon]
    ...     'radius_km': 50
    ... }

    Polygon region:
    >>> region = {
    ...     'type': 'polygon',
    ...     'name': 'Custom Area',
    ...     'coordinates': [[lon1, lat1], [lon2, lat2], ...]  # Close the polygon
    ... }
    """
    if client is not None:
        if regions is None:
            regions = []
        elif isinstance(regions, dict):
            regions = [regions]
        return _region_download_with_client(
            client=client,
            regions=regions,
            download_type=download_type,
            update_country_caches=update_country_caches,
            show_element_counts=show_element_counts,
        )

    if config is None:
        config = get_config()

    if config is None:
        logger.error("Unable to get configuration")
        return {
            "success": False,
            "regions_processed": 0,
            "regions_failed": 0,
            "results": {},
            "total_elements_updated": 0,
            "total_elements_added": 0,
            "error": "Configuration not available",
        }

    osm_config = config.get("OSM", {})

    if regions is None:
        regions = osm_config.get("region_download", [])
        if not regions:
            logger.warning(
                "No regions specified and no regions found in config. "
                "Please provide regions or add them to config.yaml under OSM.region_download"
            )
            return {
                "success": False,
                "regions_processed": 0,
                "regions_failed": 0,
                "results": {},
                "total_elements_updated": 0,
                "total_elements_added": 0,
                "error": "No regions specified",
            }
    elif isinstance(regions, dict):
        regions = [regions]

    valid_regions = []
    for i, region in enumerate(regions):
        if not isinstance(region, dict):
            logger.warning(
                f"Region at index {i} is not a dictionary (type: {type(region)}), skipping"
            )
            continue

        if "name" not in region:
            region["name"] = f"Region_{i + 1}"

        if "type" not in region:
            logger.warning(
                f"Region '{region['name']}' missing required 'type' field - skipping this region"
            )
            continue

        if region["type"] not in ["bbox", "radius", "polygon"]:
            logger.warning(
                f"Region '{region['name']}' has invalid type '{region['type']}'. "
                "Must be one of: bbox, radius, polygon - skipping this region"
            )
            continue

        valid_regions.append(region)

    if not valid_regions:
        logger.warning("No valid regions found after validation")
        return {
            "success": False,
            "regions_processed": 0,
            "regions_failed": len(regions),
            "results": {},
            "total_elements_updated": 0,
            "total_elements_added": 0,
            "error": "No valid regions to process",
        }

    regions = valid_regions

    if cache_dir is None:
        cache_dir, _ = get_osm_cache_paths(config)

    overpass_config = osm_config.get("overpass_api", {})

    if api_url is None:
        api_url = overpass_config.get("url", "https://overpass-api.de/api/interpreter")

    if timeout is None:
        timeout = overpass_config.get("timeout", 300)

    if max_retries is None:
        max_retries = overpass_config.get("max_retries", 3)

    if retry_delay is None:
        retry_delay = overpass_config.get("retry_delay", 5)

    logger.info(
        f"Starting regional download for {len(regions)} region(s) "
        f"(type={download_type}, update_caches={update_country_caches})"
    )

    try:
        with OverpassAPIClient(
            api_url=api_url,
            cache_dir=cache_dir,
            timeout=timeout if timeout is not None else 300,
            max_retries=max_retries if max_retries is not None else 3,
            retry_delay=retry_delay if retry_delay is not None else 5,
        ) as client:
            results = _region_download_with_client(
                client=client,
                regions=regions,
                download_type=download_type,
                update_country_caches=update_country_caches,
                show_element_counts=show_element_counts,
            )

            if results["success"]:
                logger.info(
                    f"Regional download completed successfully. "
                    f"Updated {results['total_elements_updated']} elements, "
                    f"added {results['total_elements_added']} new elements."
                )
            else:
                logger.warning(
                    f"Regional download completed with errors. "
                    f"{results['regions_failed']} region(s) failed."
                )

            return results

    except Exception as e:
        logger.error(f"Regional download failed: {str(e)}")
        return {
            "success": False,
            "regions_processed": 0,
            "regions_failed": len(regions),
            "results": {
                region.get("name", f"Region_{i + 1}"): {
                    "status": "failed",
                    "error": str(e),
                }
                for i, region in enumerate(regions)
            },
            "total_elements_updated": 0,
            "total_elements_added": 0,
            "error": str(e),
        }


def _region_download_with_client(
    client: OverpassAPIClient,
    regions: list[dict[str, Any]],
    download_type: str = "both",
    update_country_caches: bool = True,
    show_element_counts: bool = True,
) -> dict[str, Any]:
    """Process regional downloads with an existing client.

    Parameters
    ----------
    client : OverpassAPIClient
        API client instance
    regions : list[dict]
        Region definitions
    download_type : str
        Element types to download
    update_country_caches : bool
        Update country caches
    show_element_counts : bool
        Show counts before download

    Returns
    -------
    dict
        Processing results
    """
    results = {
        "success": True,
        "regions_processed": 0,
        "regions_failed": 0,
        "results": {},
        "total_elements_updated": 0,
        "total_elements_added": 0,
        "timestamp": datetime.now().isoformat(),
    }

    for i, region in enumerate(regions):
        region_name = region.get("name", f"region_{i}")
        logger.info(f"Processing region {i + 1}/{len(regions)}: {region_name}")

        try:
            if show_element_counts and hasattr(client, "count_region_elements"):
                logger.info(f"Counting elements in {region_name}...")
                expected_counts = client.count_region_elements(region, download_type)
                logger.info(
                    f"Expected elements in {region_name}: "
                    f"{expected_counts.get('plants', 0)} plants, "
                    f"{expected_counts.get('generators', 0)} generators"
                )

            region_data = _download_single_region(client, region, download_type)

            if update_country_caches:
                update_stats = _update_caches_with_regional_data(
                    client, region_data, region
                )
            else:
                update_stats = {
                    "elements_updated": 0,
                    "elements_added": 0,
                    "countries_affected": [],
                }

            results["results"][region_name] = {
                "status": "success",
                "plants_count": len(region_data.get("plants", [])),
                "generators_count": len(region_data.get("generators", [])),
                "elements_updated": update_stats["elements_updated"],
                "elements_added": update_stats["elements_added"],
                "countries_affected": update_stats["countries_affected"],
                "timestamp": datetime.now().isoformat(),
            }
            results["regions_processed"] += 1
            results["total_elements_updated"] += update_stats["elements_updated"]
            results["total_elements_added"] += update_stats["elements_added"]

            logger.info(
                f"Successfully processed {region_name}: "
                f"{results['results'][region_name]['plants_count']} plants, "
                f"{results['results'][region_name]['generators_count']} generators "
                f"({update_stats['elements_updated']} updated, {update_stats['elements_added']} added)"
            )

        except Exception as e:
            logger.error(f"Failed to process region {region_name}: {str(e)}")
            results["results"][region_name] = {
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }
            results["regions_failed"] += 1
            results["success"] = False

    logger.info(
        f"Regional download complete: {results['regions_processed']} succeeded, "
        f"{results['regions_failed']} failed. "
        f"Total: {results['total_elements_updated']} updated, {results['total_elements_added']} added"
    )

    return results


def _download_single_region(
    client: OverpassAPIClient, region: dict[str, Any], download_type: str
) -> dict[str, list[dict]]:
    """Download data for a single region."""
    results = {"plants": [], "generators": []}

    area_filter = _build_area_filter(region)

    if not area_filter:
        logger.warning(
            f"Empty area filter for region '{region.get('name', 'unnamed')}' - skipping download"
        )
        return results

    if download_type in ["plants", "both"]:
        query = f"""
        [out:json][timeout:{client.timeout}];
        (
            node["power"="plant"]{area_filter};
            way["power"="plant"]{area_filter};
            relation["power"="plant"]{area_filter};
        );
        out body;
        """

        logger.debug(f"Querying plants for region {region.get('name', 'unnamed')}")
        plant_data = client.query_overpass(query)
        results["plants"] = plant_data.get("elements", [])

        _fetch_referenced_elements(client, results["plants"])

    if download_type in ["generators", "both"]:
        query = f"""
        [out:json][timeout:{client.timeout}];
        (
            node["power"="generator"]{area_filter};
            way["power"="generator"]{area_filter};
            relation["power"="generator"]{area_filter};
        );
        out body;
        """

        logger.debug(f"Querying generators for region {region.get('name', 'unnamed')}")
        generator_data = client.query_overpass(query)
        results["generators"] = generator_data.get("elements", [])

        _fetch_referenced_elements(client, results["generators"])

    return results


def _fetch_referenced_elements(client: OverpassAPIClient, elements: list[dict]) -> None:
    """Fetch all elements referenced by ways and relations."""
    node_ids_to_fetch = set()
    way_ids_to_fetch = set()
    relation_ids_to_fetch = set()

    for element in elements:
        if element["type"] == "way" and "nodes" in element:
            for node_id in element["nodes"]:
                if not client.cache.get_node(node_id):
                    node_ids_to_fetch.add(node_id)

        elif element["type"] == "relation" and "members" in element:
            for member in element["members"]:
                if member["type"] == "node" and not client.cache.get_node(
                    member["ref"]
                ):
                    node_ids_to_fetch.add(member["ref"])
                elif member["type"] == "way" and not client.cache.get_way(
                    member["ref"]
                ):
                    way_ids_to_fetch.add(member["ref"])
                elif member["type"] == "relation" and not client.cache.get_relation(
                    member["ref"]
                ):
                    relation_ids_to_fetch.add(member["ref"])

    if node_ids_to_fetch:
        logger.debug(f"Fetching {len(node_ids_to_fetch)} referenced nodes")
        client.get_nodes(list(node_ids_to_fetch))

    if way_ids_to_fetch:
        logger.debug(f"Fetching {len(way_ids_to_fetch)} referenced ways")
        client.get_ways(list(way_ids_to_fetch))

    if relation_ids_to_fetch:
        logger.debug(f"Fetching {len(relation_ids_to_fetch)} referenced relations")
        client.get_relations(list(relation_ids_to_fetch))


def _build_area_filter(region: dict[str, Any]) -> str:
    """Build Overpass area filter for region type.

    Parameters
    ----------
    region : dict
        Region definition

    Returns
    -------
    str
        Overpass filter string
    """
    if region["type"] == "bbox":
        lat_min, lon_min, lat_max, lon_max = region["bounds"]
        return f"({lat_min},{lon_min},{lat_max},{lon_max})"

    elif region["type"] == "radius":
        lat, lon = region["center"]
        radius_m = region["radius_km"] * 1000
        return f"(around:{radius_m},{lat},{lon})"

    elif region["type"] == "polygon":
        poly_str = 'poly:"'
        for lon, lat in region["coordinates"]:
            poly_str += f"{lat} {lon} "
        poly_str = poly_str.strip() + '"'
        return f"({poly_str})"

    else:
        logger.warning(
            f"Unknown region type: {region['type']}' for region '{region.get('name', 'unnamed')}' - returning empty filter"
        )
        return ""


def _update_caches_with_regional_data(
    client: OverpassAPIClient,
    region_data: dict[str, list[dict]],
    region: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Update country caches with downloaded regional data."""
    stats = {"elements_updated": 0, "elements_added": 0, "countries_affected": set()}

    for data_type in ["plants", "generators"]:
        elements = region_data.get(data_type, [])

        if not elements:
            continue

        elements_by_country = _group_elements_by_country(client, elements, region)

        for country_code, country_elements in elements_by_country.items():
            stats["countries_affected"].add(country_code)

            if data_type == "plants":
                existing_data = client.cache.get_plants(country_code) or {
                    "elements": []
                }
                cache_store_func = client.cache.store_plants
            else:
                existing_data = client.cache.get_generators(country_code) or {
                    "elements": []
                }
                cache_store_func = client.cache.store_generators

            existing_elements = {
                f"{elem['type']}/{elem['id']}": elem
                for elem in existing_data.get("elements", [])
            }

            for new_elem in country_elements:
                elem_key = f"{new_elem['type']}/{new_elem['id']}"

                if elem_key in existing_elements:
                    stats["elements_updated"] += 1
                    existing_elements[elem_key] = new_elem
                else:
                    stats["elements_added"] += 1
                    existing_elements[elem_key] = new_elem

            updated_data = {"elements": list(existing_elements.values())}
            cache_store_func(country_code, updated_data)

    stats["countries_affected"] = sorted(list(stats["countries_affected"]))

    return stats


def _group_elements_by_country(
    client: OverpassAPIClient,
    elements: list[dict],
    region: dict[str, Any] | None = None,
) -> dict[str, list[dict]]:
    """Group elements by their country location."""
    if region:
        region_countries = _determine_countries_for_region(client, region)

        if len(region_countries) == 1:
            country_code = region_countries[0]
            logger.info(
                f"Region is entirely within {country_code}, assigning all "
                f"{len(elements)} elements to this country"
            )
            return {country_code: elements}
        elif len(region_countries) > 1:
            logger.info(
                f"Region spans {len(region_countries)} countries: {region_countries}, "
                "will check each element individually"
            )
        else:
            logger.warning("Could not determine region countries")

    elements_by_country = {}

    for element in elements:
        lat, lon = _get_element_coordinates(client, element)

        if lat is None or lon is None:
            logger.warning(
                f"Could not determine coordinates for {element['type']}/{element['id']}"
            )
            continue

        country_code = _determine_country_from_coordinates(client, lat, lon)

        if country_code:
            if country_code not in elements_by_country:
                elements_by_country[country_code] = []
            elements_by_country[country_code].append(element)
        else:
            logger.warning(
                f"Could not determine country for {element['type']}/{element['id']} "
                f"at coordinates {lat}, {lon}"
            )

    return elements_by_country


def _determine_countries_for_region(
    client: OverpassAPIClient, region: dict[str, Any]
) -> list[str]:
    """Determine which countries a region overlaps with.

    Parameters
    ----------
    client : OverpassAPIClient
        API client
    region : dict
        Region definition

    Returns
    -------
    list[str]
        ISO country codes
    """
    try:
        logger.info(
            f"Determining countries for region: {region.get('name', 'unnamed')}"
        )

        test_points = []

        if region["type"] == "bbox":
            lat_min, lon_min, lat_max, lon_max = region["bounds"]
            test_points = [
                (lat_min, lon_min),
                (lat_min, lon_max),
                (lat_max, lon_min),
                (lat_max, lon_max),
                ((lat_min + lat_max) / 2, (lon_min + lon_max) / 2),
            ]
        elif region["type"] == "radius":
            center_lat, center_lon = region["center"]
            radius_km = region["radius_km"]
            lat_offset = radius_km / 111.0
            lon_offset = radius_km / (111.0 * abs(math.cos(math.radians(center_lat))))

            test_points = [
                (center_lat, center_lon),
                (center_lat + lat_offset, center_lon),
                (center_lat - lat_offset, center_lon),
                (center_lat, center_lon + lon_offset),
                (center_lat, center_lon - lon_offset),
            ]
        elif region["type"] == "polygon":
            for lon, lat in region["coordinates"]:
                test_points.append((lat, lon))
            lons = [coord[0] for coord in region["coordinates"]]
            lats = [coord[1] for coord in region["coordinates"]]
            center_lon = sum(lons) / len(lons)
            center_lat = sum(lats) / len(lats)
            test_points.append((center_lat, center_lon))
        else:
            logger.warning(
                f"Unknown region type: {region['type']}' in _determine_countries_for_region - skipping"
            )
            return []

        countries = set()
        for lat, lon in test_points:
            query = f"""
            [out:json][timeout:30];
            is_in({lat},{lon})->.a;
            relation(pivot.a)["admin_level"="2"]["ISO3166-1"];
            out tags;
            """

            try:
                result = client.query_overpass(query)
                for element in result.get("elements", []):
                    country_code = element.get("tags", {}).get("ISO3166-1")
                    if country_code:
                        countries.add(country_code)
            except Exception as e:
                logger.warning(f"Failed to check point ({lat}, {lon}): {e}")

        countries_list = sorted(list(countries))
        logger.info(
            f"Region overlaps with {len(countries_list)} countries: {countries_list}"
        )
        return countries_list

    except Exception as e:
        logger.warning(f"Could not determine countries for region: {e}")
        return []


def _get_element_coordinates(
    client: OverpassAPIClient, element: dict
) -> tuple[float | None, float | None]:
    """Extract coordinates from OSM element.

    Parameters
    ----------
    client : OverpassAPIClient
        Client for cache access
    element : dict
        OSM element

    Returns
    -------
    lat : float or None
        Latitude
    lon : float or None
        Longitude
    """
    if element["type"] == "node":
        return element.get("lat"), element.get("lon")

    elif element["type"] == "way":
        if "nodes" in element and element["nodes"]:
            lats, lons = [], []
            for node_id in element["nodes"]:
                node = client.cache.get_node(node_id)
                if node and "lat" in node and "lon" in node:
                    lats.append(node["lat"])
                    lons.append(node["lon"])

            if lats and lons:
                return sum(lats) / len(lats), sum(lons) / len(lons)

    elif element["type"] == "relation":
        if "members" in element:
            for member in element["members"]:
                if member["type"] == "node":
                    node = client.cache.get_node(member["ref"])
                    if node and "lat" in node and "lon" in node:
                        return node["lat"], node["lon"]
                elif member["type"] == "way":
                    way = client.cache.get_way(member["ref"])
                    if way:
                        lat, lon = _get_element_coordinates(client, way)
                        if lat is not None and lon is not None:
                            return lat, lon

    return None, None


def _determine_country_from_coordinates(
    client: OverpassAPIClient, lat: float, lon: float, use_cache: bool = True
) -> str | None:
    """Determine country code from coordinates.

    Parameters
    ----------
    client : OverpassAPIClient
        API client
    lat : float
        Latitude
    lon : float
        Longitude
    use_cache : bool
        Use coordinate cache

    Returns
    -------
    str or None
        ISO country code
    """
    if use_cache:
        country = client._country_cache.get_with_tolerance(lat, lon, tolerance=0.01)
        if country:
            return country

    query = f"""
    [out:json][timeout:30];
    is_in({lat},{lon})->.a;
    relation(pivot.a)["admin_level"="2"]["ISO3166-1"];
    out tags;
    """

    try:
        result = client.query_overpass(query)
        elements = result.get("elements", [])

        if elements:
            country_code = elements[0].get("tags", {}).get("ISO3166-1", "")

            if use_cache and country_code:
                client._country_cache[(lat, lon)] = country_code

            return country_code if country_code else None

    except Exception as e:
        logger.warning(f"Could not determine country for coordinates {lat},{lon}: {e}")

    return None

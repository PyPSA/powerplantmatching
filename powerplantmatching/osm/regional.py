"""
Regional download functionality for OSM power plant data.
Provides functions for downloading and updating regional data in the main cache.
"""

import logging
import math
import os
from datetime import datetime
from typing import Any, Optional, Union

import powerplantmatching as ppm

from .client import OverpassAPIClient

logger = logging.getLogger(__name__)


def region_download(
    regions: Optional[Union[list[dict[str, Any]], dict[str, Any]]] = None,
    download_type: str = "both",
    update_country_caches: bool = True,
    show_element_counts: bool = True,
    config: Optional[dict] = None,
    api_url: Optional[str] = None,
    cache_dir: Optional[str] = None,
    timeout: Optional[int] = None,
    max_retries: Optional[int] = None,
    retry_delay: Optional[int] = None,
    client: Optional[OverpassAPIClient] = None,
) -> dict[str, Any]:
    """
    Download OSM power plant data for specific regions with automatic configuration.

    This function handles client creation and configuration automatically,
    making it easy to download regional data. It can also accept an existing client
    for advanced use cases.

    Parameters
    ----------
    regions : list[dict] or dict, optional
        Region(s) to download. Can be:
        - A single region dict
        - A list of region dicts
        - None (uses regions from config file)

        Each region dict should have:
        - {"type": "bbox", "bounds": [lat_min, lon_min, lat_max, lon_max], "name": "Region Name"}
        - {"type": "radius", "center": [lat, lon], "radius_km": 50, "name": "Region Name"}
        - {"type": "polygon", "coordinates": [[lon, lat], ...], "name": "Region Name"}

    download_type : str, default "both"
        What to download: "plants", "generators", or "both"

    update_country_caches : bool, default True
        Whether to update the country caches with the downloaded data

    show_element_counts : bool, default True
        Whether to show element counts before downloading

    config : dict, optional
        Custom configuration. If None, uses powerplantmatching.get_config()

    api_url : str, optional
        Overpass API URL. If None, uses value from config or default

    cache_dir : str, optional
        Cache directory. If None, uses the standard OSM cache location

    timeout : int, optional
        API timeout in seconds. If None, uses value from config or default (300)

    max_retries : int, optional
        Maximum retry attempts. If None, uses value from config or default (3)

    retry_delay : int, optional
        Delay between retries in seconds. If None, uses value from config or default (5)

    client : OverpassAPIClient, optional
        Existing client to use. If provided, other client parameters are ignored.

    Returns
    -------
    dict[str, Any]
        Download results with structure:
        {
            "success": bool,
            "regions_processed": int,
            "regions_failed": int,
            "results": {
                "region_name": {
                    "status": "success" or "failed",
                    "plants_count": int,
                    "generators_count": int,
                    "elements_updated": int,
                    "elements_added": int,
                    "countries_affected": list[str],
                    "error": str (if failed),
                    "timestamp": str
                }
            },
            "total_elements_updated": int,
            "total_elements_added": int,
            "timestamp": str
        }

    Examples
    --------
    >>> from powerplantmatching.osm import region_download
    >>>
    >>> # Download a single region
    >>> region = {
    ...     "type": "radius",
    ...     "name": "Berlin Area",
    ...     "center": [52.5200, 13.4050],
    ...     "radius_km": 30
    ... }
    >>> results = region_download(region)
    >>>
    >>> # Download multiple regions
    >>> regions = [
    ...     {"type": "bbox", "name": "Test Area", "bounds": [48.1, 9.1, 48.2, 9.2]},
    ...     {"type": "radius", "name": "Munich", "center": [48.1351, 11.5820], "radius_km": 20}
    ... ]
    >>> results = region_download(regions)
    >>>
    >>> # Use regions from config file
    >>> results = region_download()  # Uses OSM.region_download from config.yaml
    """
    # If client is provided, use it directly (for backward compatibility)
    if client is not None:
        return _region_download_with_client(
            client=client,
            regions=regions,
            download_type=download_type,
            update_country_caches=update_country_caches,
            show_element_counts=show_element_counts,
        )

    # Get configuration
    if config is None:
        config = ppm.get_config()

    osm_config = config.get("OSM", {})

    # Handle regions parameter
    if regions is None:
        # Get regions from config
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
        # Convert single region to list
        regions = [regions]

    # Validate regions
    for i, region in enumerate(regions):
        if "name" not in region:
            region["name"] = f"Region_{i + 1}"

        if "type" not in region:
            raise ValueError(f"Region '{region['name']}' missing required 'type' field")

        if region["type"] not in ["bbox", "radius", "polygon"]:
            raise ValueError(
                f"Region '{region['name']}' has invalid type '{region['type']}'. "
                "Must be one of: bbox, radius, polygon"
            )

    # Set up cache directory
    if cache_dir is None:
        # Use the same cache directory as the OSM() function
        fn = ppm.data._data_in(osm_config.get("fn", "osm_data.csv"))
        cache_dir = os.path.join(os.path.dirname(fn), "osm_cache")

    # Extract client parameters with defaults
    overpass_config = osm_config.get("overpass_api", {})

    if api_url is None:
        api_url = overpass_config.get("url", "https://overpass-api.de/api/interpreter")

    if timeout is None:
        timeout = overpass_config.get("timeout", 300)

    if max_retries is None:
        max_retries = overpass_config.get("max_retries", 3)

    if retry_delay is None:
        retry_delay = overpass_config.get("retry_delay", 5)

    # Log the operation
    logger.info(
        f"Starting regional download for {len(regions)} region(s) "
        f"(type={download_type}, update_caches={update_country_caches})"
    )

    # Create client and perform download
    try:
        with OverpassAPIClient(
            api_url=api_url,
            cache_dir=cache_dir,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
        ) as client:
            # Call the core download function
            results = _region_download_with_client(
                client=client,
                regions=regions,
                download_type=download_type,
                update_country_caches=update_country_caches,
                show_element_counts=show_element_counts,
            )

            # Log summary
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


def download_and_integrate(
    regions: Optional[Union[list[dict[str, Any]], dict[str, Any]]] = None, **kwargs
) -> dict[str, Any]:
    """
    Download regional data and immediately integrate it with OSM data.

    This is a convenience function that downloads regional data and shows
    how it integrates with the standard OSM() function.

    Parameters
    ----------
    regions : list[dict] or dict, optional
        Region(s) to download. Same format as region_download()
    **kwargs
        Additional keyword arguments passed to region_download()

    Returns
    -------
    dict[str, Any]
        Dictionary with download results and integration info:
        {
            "download_results": dict,  # Results from region_download
            "total_osm_plants": int,   # Total plants after integration
            "regional_plants": int     # Plants in downloaded regions
        }

    Examples
    --------
    >>> from powerplantmatching.osm import download_and_integrate
    >>>
    >>> region = {"type": "radius", "name": "Test", "center": [52.52, 13.405], "radius_km": 10}
    >>> info = download_and_integrate(region)
    >>> print(f"Downloaded {info['regional_plants']} plants in the region")
    """
    # Perform the download
    results = region_download(regions, **kwargs)

    # Get OSM data (which now includes the regional updates)
    from powerplantmatching.data import OSM

    osm_data = OSM()

    # Count plants in the affected regions
    regional_plants = 0
    if results["success"] and not osm_data.empty:
        # Get bounds of all downloaded regions
        for region_name, region_result in results["results"].items():
            if region_result.get("status") == "success":
                # This is approximate - actual count would need region bounds
                regional_plants += region_result.get("plants_count", 0)

    return {
        "download_results": results,
        "total_osm_plants": len(osm_data) if not osm_data.empty else 0,
        "regional_plants": regional_plants,
    }


def _region_download_with_client(
    client: OverpassAPIClient,
    regions: list[dict[str, Any]],
    download_type: str = "both",
    update_country_caches: bool = True,
    show_element_counts: bool = True,
) -> dict[str, Any]:
    """
    Core function that performs the actual regional download with an existing client.

    This is the original region_download function logic, separated to support
    both the simplified interface and backward compatibility.
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
            # Count elements first if requested
            if show_element_counts and hasattr(client, "count_region_elements"):
                logger.info(f"Counting elements in {region_name}...")
                expected_counts = client.count_region_elements(region, download_type)
                logger.info(
                    f"Expected elements in {region_name}: "
                    f"{expected_counts.get('plants', 0)} plants, "
                    f"{expected_counts.get('generators', 0)} generators"
                )

            # Download data for this region
            region_data = _download_single_region(client, region, download_type)

            # Update caches with the downloaded data
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

            # Record success
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
    """
    Download data for a single region.

    Parameters
    ----------
    client : OverpassAPIClient
        OSM client
    region : dict[str, Any]
        Region specification
    download_type : str
        What to download: "plants", "generators", or "both"

    Returns
    -------
    dict[str, list[dict]]
        Downloaded elements: {"plants": [...], "generators": [...]}
    """
    results = {"plants": [], "generators": []}

    # Build area filter based on region type
    area_filter = _build_area_filter(region)

    # Download plants if requested
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

        # Also fetch referenced ways and nodes for relations/ways
        _fetch_referenced_elements(client, results["plants"])

    # Download generators if requested
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

        # Also fetch referenced ways and nodes
        _fetch_referenced_elements(client, results["generators"])

    return results


def _fetch_referenced_elements(client: OverpassAPIClient, elements: list[dict]) -> None:
    """
    Fetch referenced nodes and ways for the given elements and update caches.

    Parameters
    ----------
    client : OverpassAPIClient
        OSM client
    elements : list[dict]
        List of OSM elements that may reference other elements
    """
    node_ids_to_fetch = set()
    way_ids_to_fetch = set()
    relation_ids_to_fetch = set()

    for element in elements:
        if element["type"] == "way" and "nodes" in element:
            # Check which nodes we don't have cached
            for node_id in element["nodes"]:
                if not client.cache.get_node(node_id):
                    node_ids_to_fetch.add(node_id)

        elif element["type"] == "relation" and "members" in element:
            # Check which members we don't have cached
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

    # Fetch missing elements
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
    """
    Build Overpass area filter from region specification.

    Parameters
    ----------
    region : dict[str, Any]
        Region specification

    Returns
    -------
    str
        Overpass area filter string
    """
    if region["type"] == "bbox":
        lat_min, lon_min, lat_max, lon_max = region["bounds"]
        return f"({lat_min},{lon_min},{lat_max},{lon_max})"

    elif region["type"] == "radius":
        lat, lon = region["center"]
        radius_m = region["radius_km"] * 1000
        return f"(around:{radius_m},{lat},{lon})"

    elif region["type"] == "polygon":
        # Convert to Overpass polygon format
        poly_str = 'poly:"'
        for lon, lat in region["coordinates"]:
            poly_str += f"{lat} {lon} "
        poly_str = poly_str.strip() + '"'
        return f"({poly_str})"

    else:
        raise ValueError(f"Unknown region type: {region['type']}")


def _update_caches_with_regional_data(
    client: OverpassAPIClient,
    region_data: dict[str, list[dict]],
    region: dict[str, Any] = None,
) -> dict[str, Any]:
    """
    Update the main caches with regional data, merging by element ID.

    Parameters
    ----------
    client : OverpassAPIClient
        OSM client with cache access
    region_data : dict[str, list[dict]]
        Regional data: {"plants": [...], "generators": [...]}
    region : dict[str, Any], optional
        Region specification for country determination optimization

    Returns
    -------
    dict[str, Any]
        Update statistics: {
            "elements_updated": int,
            "elements_added": int,
            "countries_affected": list[str]
        }
    """
    stats = {"elements_updated": 0, "elements_added": 0, "countries_affected": set()}

    # Process plants and generators
    for data_type in ["plants", "generators"]:
        elements = region_data.get(data_type, [])

        if not elements:
            continue

        # Group elements by country with region optimization
        elements_by_country = _group_elements_by_country(client, elements, region)

        for country_code, country_elements in elements_by_country.items():
            stats["countries_affected"].add(country_code)

            # Get existing cache for this country
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

            # Create a map of existing elements by ID for efficient lookup
            existing_elements = {
                f"{elem['type']}/{elem['id']}": elem
                for elem in existing_data.get("elements", [])
            }

            # Update or add new elements
            for new_elem in country_elements:
                elem_key = f"{new_elem['type']}/{new_elem['id']}"

                if elem_key in existing_elements:
                    # Update existing element
                    stats["elements_updated"] += 1
                    existing_elements[elem_key] = new_elem
                else:
                    # Add new element
                    stats["elements_added"] += 1
                    existing_elements[elem_key] = new_elem

            # Convert back to list and store
            updated_data = {"elements": list(existing_elements.values())}
            cache_store_func(country_code, updated_data)

    # Convert set to list for JSON serialization
    stats["countries_affected"] = sorted(list(stats["countries_affected"]))

    return stats


def _group_elements_by_country(
    client: OverpassAPIClient, elements: list[dict], region: dict[str, Any] = None
) -> dict[str, list[dict]]:
    """
    Group elements by their country using an efficient two-step process.

    Parameters
    ----------
    client : OverpassAPIClient
        OSM client
    elements : list[dict]
        List of OSM elements
    region : dict[str, Any], optional
        Region specification for optimization

    Returns
    -------
    dict[str, list[dict]]
        Elements grouped by country code
    """
    # Step 1: If region is provided, determine which countries it overlaps with
    if region:
        region_countries = _determine_countries_for_region(client, region)

        if len(region_countries) == 1:
            # Optimization: If region is entirely within one country,
            # assign all elements to it without individual checks
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

    # Step 2: Check each element individually
    elements_by_country = {}

    for element in elements:
        # Determine coordinates
        lat, lon = _get_element_coordinates(client, element)

        if lat is None or lon is None:
            logger.warning(
                f"Could not determine coordinates for {element['type']}/{element['id']}"
            )
            continue

        # Determine country for this specific element
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
    """
    Determine which countries overlap with a region.

    Parameters
    ----------
    client : OverpassAPIClient
        OSM client
    region : dict[str, Any]
        Region specification (bbox, radius, or polygon)

    Returns
    -------
    list[str]
        List of country codes that overlap with the region
    """
    try:
        logger.info(
            f"Determining countries for region: {region.get('name', 'unnamed')}"
        )

        # For accurate results, we need to check multiple points in the region
        test_points = []

        if region["type"] == "bbox":
            lat_min, lon_min, lat_max, lon_max = region["bounds"]
            # Check corners and center
            test_points = [
                (lat_min, lon_min),  # SW corner
                (lat_min, lon_max),  # SE corner
                (lat_max, lon_min),  # NW corner
                (lat_max, lon_max),  # NE corner
                ((lat_min + lat_max) / 2, (lon_min + lon_max) / 2),  # Center
            ]
        elif region["type"] == "radius":
            center_lat, center_lon = region["center"]
            radius_km = region["radius_km"]
            # Check center and points on the perimeter
            # Approximate: 1 degree latitude ≈ 111 km
            lat_offset = radius_km / 111.0
            # Longitude varies by latitude, approximate
            lon_offset = radius_km / (111.0 * abs(math.cos(math.radians(center_lat))))

            test_points = [
                (center_lat, center_lon),  # Center
                (center_lat + lat_offset, center_lon),  # North
                (center_lat - lat_offset, center_lon),  # South
                (center_lat, center_lon + lon_offset),  # East
                (center_lat, center_lon - lon_offset),  # West
            ]
        elif region["type"] == "polygon":
            # Check each vertex of the polygon
            for lon, lat in region["coordinates"]:
                test_points.append((lat, lon))
            # Also add centroid
            lons = [coord[0] for coord in region["coordinates"]]
            lats = [coord[1] for coord in region["coordinates"]]
            center_lon = sum(lons) / len(lons)
            center_lat = sum(lats) / len(lats)
            test_points.append((center_lat, center_lon))
        else:
            raise ValueError(f"Unknown region type: {region['type']}")

        # Query each test point to find countries
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
) -> tuple[Optional[float], Optional[float]]:
    """
    Get coordinates for an OSM element.

    Parameters
    ----------
    client : OverpassAPIClient
        OSM client for accessing cached nodes
    element : dict
        OSM element

    Returns
    -------
    tuple[Optional[float], Optional[float]]
        (latitude, longitude) or (None, None) if not found
    """
    if element["type"] == "node":
        return element.get("lat"), element.get("lon")

    elif element["type"] == "way":
        # Get center point from nodes
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
        # For relations, try to find a node or way member to get coordinates
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
) -> Optional[str]:
    """
    Determine country code from coordinates using Overpass API.

    Parameters
    ----------
    client : OverpassAPIClient
        OSM client
    lat : float
        Latitude
    lon : float
        Longitude
    use_cache : bool
        Whether to use a simple cache for repeated lookups

    Returns
    -------
    Optional[str]
        ISO country code or None
    """
    # Simple in-memory cache to avoid repeated API calls for nearby coordinates
    # In production, this could be more sophisticated (e.g., grid-based)
    if use_cache and hasattr(client, "_country_cache"):
        # Check if we have a cached result for nearby coordinates
        for (cached_lat, cached_lon), country in client._country_cache.items():
            # If coordinates are within ~1km, reuse the cached country
            if abs(lat - cached_lat) < 0.01 and abs(lon - cached_lon) < 0.01:
                return country

    # Query Overpass API for the country at these coordinates
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
            # Get the country code from the first matching relation
            country_code = elements[0].get("tags", {}).get("ISO3166-1", "")

            # Cache the result
            if use_cache:
                if not hasattr(client, "_country_cache"):
                    client._country_cache = {}
                client._country_cache[(lat, lon)] = country_code

                # Limit cache size to prevent memory issues
                if len(client._country_cache) > 1000:
                    # Remove oldest entries (simple FIFO)
                    items = list(client._country_cache.items())
                    client._country_cache = dict(items[-500:])

            return country_code if country_code else None

    except Exception as e:
        logger.warning(f"Could not determine country for coordinates {lat},{lon}: {e}")

    return None

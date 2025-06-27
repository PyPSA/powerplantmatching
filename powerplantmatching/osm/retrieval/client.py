"""Overpass API client for retrieving OpenStreetMap power plant data.

This module provides a robust client for querying the Overpass API with
features like automatic retrying, caching, progress tracking, and country
coordinate determination.
"""

import logging
import time
from typing import Optional, Union

import requests
from tqdm import tqdm

from powerplantmatching.core import get_config
from powerplantmatching.osm.utils import get_country_code, get_osm_cache_paths

from .cache import CountryCoordinateCache, ElementCache

logger = logging.getLogger(__name__)


class OverpassAPIClient:
    """Client for interacting with the Overpass API to retrieve OSM data.

    Provides methods for querying power plants and generators by country or
    region, with automatic caching, retry logic, and progress tracking.
    Handles the complexity of OSM data structures including nodes, ways,
    and relations with their dependencies.

    Attributes
    ----------
    api_url : str
        Overpass API endpoint URL
    cache : ElementCache
        Multi-level cache for OSM elements
    timeout : int
        Query timeout in seconds
    max_retries : int
        Maximum retry attempts for failed queries
    retry_delay : int
        Delay between retries in seconds
    show_progress : bool
        Whether to show progress bars
    _country_cache : CountryCoordinateCache
        Cache for country coordinate lookups

    Examples
    --------
    >>> with OverpassAPIClient() as client:
    ...     plants, generators = client.get_country_data("Malta")
    ...     print(f"Found {len(plants['elements'])} plants")
    """

    def __init__(
        self,
        api_url: Optional[str] = None,
        cache_dir: Optional[str] = None,
        timeout: int = 300,
        max_retries: int = 3,
        retry_delay: int = 5,
        show_progress: bool = True,
        country_cache: Optional[Union[dict, "CountryCoordinateCache"]] = None,
    ):
        """Initialize the Overpass API client.

        Parameters
        ----------
        api_url : str, optional
            Overpass API URL. Defaults to public instance.
        cache_dir : str, optional
            Directory for caching. Uses config default if None.
        timeout : int
            Query timeout in seconds
        max_retries : int
            Maximum retry attempts
        retry_delay : int
            Seconds between retries
        show_progress : bool
            Show progress bars during downloads
        country_cache : dict or CountryCoordinateCache, optional
            Country coordinate cache
        """
        if cache_dir is None:
            config = get_config()
            cache_dir, _ = get_osm_cache_paths(config)

        self.api_url = api_url or "https://overpass-api.de/api/interpreter"
        self.cache = ElementCache(cache_dir)
        self.cache.load_all_caches()

        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.show_progress = show_progress

        if country_cache is None:
            self._country_cache = CountryCoordinateCache(precision=2, max_size=1000)
        elif isinstance(country_cache, CountryCoordinateCache):
            self._country_cache = country_cache
        else:
            self._country_cache = CountryCoordinateCache(precision=2, max_size=1000)
            if isinstance(country_cache, dict):
                self._country_cache._legacy_cache.update(country_cache)

        self._country_cache.set_client(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Save modified caches on client close."""
        if hasattr(self, "cache"):
            if any(
                [
                    self.cache.plants_modified,
                    self.cache.generators_modified,
                    self.cache.ways_modified,
                    self.cache.nodes_modified,
                    self.cache.relations_modified,
                ]
            ):
                logging.getLogger(__name__).info("Saving caches")
                self.cache.save_all_caches(force=False)

    def query_overpass(self, query: str) -> dict:
        """Execute an Overpass API query with retry logic.

        Parameters
        ----------
        query : str
            Overpass QL query string

        Returns
        -------
        dict
            Query results with 'elements' list

        Notes
        -----
        Automatically adds timeout if not present in query.
        Retries on connection errors with exponential backoff.
        """
        if "[timeout:" not in query:
            query = query.replace("[out:json]", f"[out:json][timeout:{self.timeout}]")

        retries = 0
        last_error = None

        while retries < self.max_retries:
            try:
                response = requests.post(
                    self.api_url, data={"data": query}, timeout=self.timeout + 30
                )
                response.raise_for_status()
                return response.json()

            except requests.RequestException as e:
                last_error = e
                retries += 1
                if retries < self.max_retries:
                    logger.warning(
                        f"Overpass API request failed (attempt {retries}/{self.max_retries}): {str(e)}"
                        f" - retrying in {self.retry_delay} seconds"
                    )
                    time.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"Overpass API request failed after {self.max_retries} attempts: {str(e)}"
                    )

        logger.error(
            f"Failed to query Overpass API after {self.max_retries} attempts: {str(last_error)}"
        )
        return {"elements": [], "error": f"API connection failed: {str(last_error)}"}

    def count_country_elements(
        self, country: str, element_type: str = "both"
    ) -> dict[str, int]:
        """Count power plants and/or generators in a country.

        Parameters
        ----------
        country : str
            Country name or ISO code
        element_type : {'plants', 'generators', 'both'}
            Which elements to count

        Returns
        -------
        dict[str, int]
            Counts by type, -1 indicates error
        """
        country_code = get_country_code(country)
        if country_code is None:
            logger.error(f"Invalid country name: {country}")
            return {"plants": -1, "generators": -1}

        counts = {}

        queries = {}
        if element_type in ["plants", "both"]:
            queries["plants"] = f"""[out:json][timeout:{self.timeout}];
area["ISO3166-1"="{country_code}"][admin_level=2]->.boundaryarea;
(node["power"="plant"](area.boundaryarea);
way["power"="plant"](area.boundaryarea);
relation["power"="plant"](area.boundaryarea););
out count;"""

        if element_type in ["generators", "both"]:
            queries["generators"] = f"""[out:json][timeout:{self.timeout}];
area["ISO3166-1"="{country_code}"][admin_level=2]->.boundaryarea;
(node["power"="generator"](area.boundaryarea);
way["power"="generator"](area.boundaryarea);
relation["power"="generator"](area.boundaryarea););
out count;"""

        for elem_type, query in queries.items():
            try:
                data = self.query_overpass(query)
                total = 0
                for elem in data.get("elements", []):
                    if elem.get("type") == "count" and "tags" in elem:
                        count_val = elem["tags"].get("total", 0)
                        if isinstance(count_val, str):
                            count_val = int(count_val)
                        total += count_val
                counts[elem_type] = total
            except Exception as e:
                logger.error(f"Error counting {elem_type} in {country}: {e}")
                counts[elem_type] = -1

        return counts

    def count_region_elements(
        self, region: dict, element_type: str = "both"
    ) -> dict[str, int]:
        """Count power elements in a custom region.

        Parameters
        ----------
        region : dict
            Region definition with type and parameters
        element_type : {'plants', 'generators', 'both'}
            Which elements to count

        Returns
        -------
        dict[str, int]
            Counts by type
        """
        counts = {}

        if region["type"] == "bbox":
            lat_min, lon_min, lat_max, lon_max = region["bounds"]
            area_filter = f"({lat_min},{lon_min},{lat_max},{lon_max})"
        elif region["type"] == "radius":
            lat, lon = region["center"]
            radius_m = region["radius_km"] * 1000
            area_filter = f"(around:{radius_m},{lat},{lon})"
        elif region["type"] == "polygon":
            poly_str = 'poly:"'
            for lon, lat in region["coordinates"]:
                poly_str += f"{lat} {lon} "
            poly_str = poly_str.strip() + '"'
            area_filter = f"({poly_str})"
        else:
            logger.warning(
                f"Unknown region type: {region['type']}' for region '{region.get('name', 'unnamed')}' - returning zero count"
            )
            area_filter = None

        if area_filter is None:
            counts["plants"] = 0
            if element_type in ["generators", "both"]:
                counts["generators"] = 0
            return counts

        queries = {}
        if element_type in ["plants", "both"]:
            queries["plants"] = f"""[out:json][timeout:{self.timeout}];
(node["power"="plant"]{area_filter};
way["power"="plant"]{area_filter};
relation["power"="plant"]{area_filter};);
out count;"""

        if element_type in ["generators", "both"]:
            queries["generators"] = f"""[out:json][timeout:{self.timeout}];
(node["power"="generator"]{area_filter};
way["power"="generator"]{area_filter};
relation["power"="generator"]{area_filter};);
out count;"""

        for elem_type, query in queries.items():
            try:
                data = self.query_overpass(query)
                total = 0
                for elem in data.get("elements", []):
                    if elem.get("type") == "count" and "tags" in elem:
                        count_val = elem["tags"].get("total", 0)
                        if isinstance(count_val, str):
                            count_val = int(count_val)
                        total += count_val
                counts[elem_type] = total
            except Exception as e:
                logger.error(f"Error counting {elem_type} in region: {e}")
                counts[elem_type] = -1

        return counts

    def get_plants_data(self, country: str, force_refresh: bool = False) -> dict:
        """Get all power plants for a country.

        Parameters
        ----------
        country : str
            Country name or ISO code
        force_refresh : bool
            Skip cache and download fresh data

        Returns
        -------
        dict
            OSM data with 'elements' list
        """
        country_code = get_country_code(country)
        if country_code is None:
            logger.error(f"Invalid country name: {country}")
            return {"elements": [], "error": f"Invalid country: {country}"}

        if not force_refresh:
            cached_data = self.cache.get_plants(country_code)
            if cached_data:
                for element in cached_data.get("elements", []):
                    if "_country" not in element:
                        element["_country"] = country_code
                return cached_data

        query = f"""
        [out:json][timeout:{self.timeout}];
        area["ISO3166-1"="{country_code}"][admin_level=2]->.boundaryarea;
        (
            node["power"="plant"](area.boundaryarea);
            way["power"="plant"](area.boundaryarea);
            relation["power"="plant"](area.boundaryarea);
        );
        out body;
        """

        logger.info(f"Fetching power plants for {country}")
        data = self.query_overpass(query)

        for element in data.get("elements", []):
            element["_country"] = country_code

        self.cache.store_plants(country_code, data)

        return data

    def get_generators_data(self, country: str, force_refresh: bool = False) -> dict:
        """Get all power generators for a country.

        Parameters
        ----------
        country : str
            Country name or ISO code
        force_refresh : bool
            Skip cache and download fresh data

        Returns
        -------
        dict
            OSM data with 'elements' list
        """
        country_code = get_country_code(country)
        if country_code is None:
            logger.error(f"Invalid country name: {country}")
            return {"elements": [], "error": f"Invalid country: {country}"}

        if not force_refresh:
            cached_data = self.cache.get_generators(country_code)
            if cached_data:
                for element in cached_data.get("elements", []):
                    if "_country" not in element:
                        element["_country"] = country_code
                return cached_data

        query = f"""
        [out:json][timeout:{self.timeout}];
        area["ISO3166-1"="{country_code}"][admin_level=2]->.boundaryarea;
        (
            node["power"="generator"](area.boundaryarea);
            way["power"="generator"](area.boundaryarea);
            relation["power"="generator"](area.boundaryarea);
        );
        out body;
        """

        logger.info(f"Fetching power generators for {country}")
        data = self.query_overpass(query)

        for element in data.get("elements", []):
            element["_country"] = country_code

        self.cache.store_generators(country_code, data)

        return data

    def get_elements(
        self,
        element_type: str,
        element_ids: list[int],
        recursion_level: int = 0,
        country_code: Optional[str] = None,
    ) -> list[dict]:
        """Get OSM elements by ID with dependency resolution.

        Parameters
        ----------
        element_type : {'node', 'way', 'relation'}
            Type of elements to retrieve
        element_ids : list[int]
            Element IDs to fetch
        recursion_level : int
            Current recursion depth for dependency resolution
        country_code : str, optional
            Country code to tag elements with

        Returns
        -------
        list[dict]
            Retrieved elements

        Notes
        -----
        Automatically fetches dependencies (nodes for ways, members for
        relations) up to recursion depth of 2.
        """
        if not element_ids:
            return []

        element_ids = list(set(element_ids))

        cached_elements = []
        uncached_ids = []

        for element_id in element_ids:
            element = None
            if element_type == "node":
                element = self.cache.get_node(element_id)
            elif element_type == "way":
                element = self.cache.get_way(element_id)
            elif element_type == "relation":
                element = self.cache.get_relation(element_id)

            if element:
                if country_code and "_country" not in element:
                    element["_country"] = country_code
                cached_elements.append(element)
            else:
                uncached_ids.append(element_id)

        if not uncached_ids:
            logger.debug(
                f"All {len(element_ids)} requested {element_type}s found in cache"
            )
            return cached_elements

        logger.info(
            f"Fetching {len(uncached_ids)} uncached {element_type}s out of {len(element_ids)} requested"
        )

        ids_str = ",".join(map(str, uncached_ids))

        query = f"""
        [out:json][timeout:300];
        {element_type}(id:{ids_str});
        out body;
        """

        if element_type in ["way", "relation"]:
            query = f"""
            [out:json][timeout:300];
            {element_type}(id:{ids_str});
            (._;>;);  // Get all child elements
            out body;
            """

        data = self.query_overpass(query)

        elements = data.get("elements", [])

        nodes = [e for e in elements if e["type"] == "node"]
        ways = [e for e in elements if e["type"] == "way"]
        relations = [e for e in elements if e["type"] == "relation"]
        logger.info(
            f"Fetched {len(elements)} elements: {len(nodes)} nodes, {len(ways)} ways, {len(relations)} relations"
        )

        if element_type == "node":
            if country_code:
                for node in nodes:
                    node["_country"] = country_code
            self.cache.store_nodes_bulk(nodes)
            fetched_elements = nodes
        elif element_type == "way":
            if country_code:
                for way in ways:
                    way["_country"] = country_code
                for node in nodes:
                    node["_country"] = country_code
            self.cache.store_ways_bulk(ways)
            self.cache.store_nodes_bulk(nodes)
            fetched_elements = ways

            if recursion_level < 2:
                for way in ways:
                    if "nodes" in way:
                        uncached_nodes = [
                            node_id
                            for node_id in way["nodes"]
                            if not self.cache.get_node(node_id)
                        ]
                        if uncached_nodes:
                            logger.info(
                                f"Resolving {len(uncached_nodes)} missing nodes from way {way['id']} (recursion level {recursion_level + 1})"
                            )
                            self.get_nodes(
                                uncached_nodes,
                                recursion_level=recursion_level + 1,
                                country_code=country_code,
                            )
                        elif way["nodes"]:
                            logger.debug(
                                f"All {len(way['nodes'])} nodes from way {way['id']} already in cache"
                            )
            elif recursion_level >= 2 and ways:
                logger.warning(
                    f"Recursion limit reached when processing way nodes (level {recursion_level})"
                )
        elif element_type == "relation":
            if country_code:
                for relation in relations:
                    relation["_country"] = country_code
                for way in ways:
                    way["_country"] = country_code
                for node in nodes:
                    node["_country"] = country_code
            self.cache.store_relations_bulk(relations)
            self.cache.store_ways_bulk(ways)
            self.cache.store_nodes_bulk(nodes)
            fetched_elements = relations

            if recursion_level < 2:
                for relation in relations:
                    if "members" in relation:
                        way_members = [
                            m["ref"] for m in relation["members"] if m["type"] == "way"
                        ]
                        node_members = [
                            m["ref"] for m in relation["members"] if m["type"] == "node"
                        ]

                        uncached_ways = [
                            way_id
                            for way_id in way_members
                            if not self.cache.get_way(way_id)
                        ]
                        if uncached_ways:
                            logger.info(
                                f"Resolving {len(uncached_ways)} missing ways from relation {relation['id']} (recursion level {recursion_level + 1})"
                            )
                            self.get_ways(
                                uncached_ways,
                                recursion_level=recursion_level + 1,
                                country_code=country_code,
                            )
                        elif way_members:
                            logger.debug(
                                f"All {len(way_members)} ways from relation {relation['id']} already in cache"
                            )

                        uncached_nodes = [
                            node_id
                            for node_id in node_members
                            if not self.cache.get_node(node_id)
                        ]
                        if uncached_nodes:
                            logger.info(
                                f"Resolving {len(uncached_nodes)} missing nodes from relation {relation['id']} (recursion level {recursion_level + 1})"
                            )
                            self.get_nodes(
                                uncached_nodes,
                                recursion_level=recursion_level + 1,
                                country_code=country_code,
                            )
                        elif node_members:
                            logger.debug(
                                f"All {len(node_members)} nodes from relation {relation['id']} already in cache"
                            )
            elif recursion_level >= 2 and (relations or ways):
                logger.warning(
                    f"Recursion limit reached when processing relation members (level {recursion_level})"
                )

        return cached_elements + fetched_elements

    def get_nodes(
        self,
        node_ids: list[int],
        recursion_level: int = 0,
        country_code: Optional[str] = None,
    ) -> list[dict]:
        """Get nodes by ID."""
        return self.get_elements("node", node_ids, recursion_level, country_code)

    def get_ways(
        self,
        way_ids: list[int],
        recursion_level: int = 0,
        country_code: Optional[str] = None,
    ) -> list[dict]:
        """Get ways by ID with node resolution."""
        return self.get_elements("way", way_ids, recursion_level, country_code)

    def get_relations(
        self,
        relation_ids: list[int],
        recursion_level: int = 0,
        country_code: Optional[str] = None,
    ) -> list[dict]:
        """Get relations by ID with member resolution."""
        return self.get_elements(
            "relation", relation_ids, recursion_level, country_code
        )

    def get_country_data(
        self, country: str, force_refresh: bool = False, plants_only: bool = False
    ) -> tuple[dict, dict]:
        """Get all power infrastructure data for a country.

        Parameters
        ----------
        country : str
            Country name or ISO code
        force_refresh : bool
            Skip cache and download fresh data
        plants_only : bool
            Only download plants, not generators

        Returns
        -------
        plants_data : dict
            Power plant elements
        generators_data : dict
            Generator elements (empty if plants_only=True)

        Notes
        -----
        Automatically resolves all dependencies (nodes for ways, members
        for relations) and shows progress if enabled.
        """

        def type_order(element):
            order = {"relation": 0, "way": 1, "node": 2}
            return order[element["type"]]

        logger.info(f"Getting OSM data for {country}")

        country_code = get_country_code(country)
        if country_code is None:
            logger.error(f"Invalid country name: {country}")
            return {"elements": []}, {"elements": []}

        pbar = None
        if self.show_progress:
            logger.info(f"Counting elements in {country}...")
            counts = self.count_country_elements(
                country, "plants" if plants_only else "both"
            )
            total_expected = counts.get("plants", 0)
            if not plants_only:
                total_expected += counts.get("generators", 0)

            if total_expected > 0:
                pbar = tqdm(
                    total=total_expected, desc=f"Downloading {country}", unit="elements"
                )

        try:
            plants_data = self.get_plants_data(country, force_refresh)
            if pbar:
                pbar.update(len(plants_data.get("elements", [])))

            if plants_only:
                generators_data = {"elements": []}
            else:
                generators_data = self.get_generators_data(country, force_refresh)
                if pbar:
                    pbar.update(len(generators_data.get("elements", [])))

            way_ids = []
            relation_ids = []
            node_ids = []

            for element in plants_data.get("elements", []):
                if element["type"] == "way":
                    way_ids.append(element["id"])
                    if "nodes" in element:
                        node_ids.extend(element["nodes"])
                elif element["type"] == "relation":
                    relation_ids.append(element["id"])
                elif element["type"] == "node":
                    node_ids.append(element["id"])

            for element in generators_data.get("elements", []):
                if element["type"] == "way":
                    way_ids.append(element["id"])
                    if "nodes" in element:
                        node_ids.extend(element["nodes"])
                elif element["type"] == "relation":
                    relation_ids.append(element["id"])
                elif element["type"] == "node":
                    node_ids.append(element["id"])

            unique_node_ids = list(set(node_ids))
            unique_way_ids = list(set(way_ids))
            unique_relation_ids = list(set(relation_ids))

            uncached_node_ids = [
                node_id
                for node_id in unique_node_ids
                if not self.cache.get_node(node_id)
            ]
            uncached_way_ids = [
                way_id for way_id in unique_way_ids if not self.cache.get_way(way_id)
            ]
            uncached_relation_ids = [
                rel_id
                for rel_id in unique_relation_ids
                if not self.cache.get_relation(rel_id)
            ]

            if pbar:
                pbar.set_description(f"{country} - resolving references")

            if uncached_node_ids:
                logger.info(
                    f"Resolving {len(uncached_node_ids)} uncached nodes out of {len(unique_node_ids)} referenced"
                )
                self.get_nodes(uncached_node_ids, country_code=country_code)
            elif unique_node_ids:
                logger.info(
                    f"All {len(unique_node_ids)} referenced nodes already in cache"
                )

            if uncached_way_ids:
                logger.info(
                    f"Resolving {len(uncached_way_ids)} uncached ways out of {len(unique_way_ids)} referenced"
                )
                self.get_ways(uncached_way_ids, country_code=country_code)
            elif unique_way_ids:
                logger.info(
                    f"All {len(unique_way_ids)} referenced ways already in cache"
                )

            if uncached_relation_ids:
                logger.info(
                    f"Resolving {len(uncached_relation_ids)} uncached relations out of {len(unique_relation_ids)} referenced"
                )
                self.get_relations(uncached_relation_ids, country_code=country_code)
            elif unique_relation_ids:
                logger.info(
                    f"All {len(unique_relation_ids)} referenced relations already in cache"
                )

            plants_data["elements"] = sorted(plants_data["elements"], key=type_order)
            generators_data["elements"] = sorted(
                generators_data["elements"], key=type_order
            )

            if pbar:
                pbar.set_description(f"{country} - complete")

        finally:
            if pbar:
                pbar.close()

        return plants_data, generators_data

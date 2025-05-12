import logging
import time
from typing import Optional

import requests

from .cache import ElementCache
from .utils import get_country_code

logger = logging.getLogger(__name__)


class OverpassAPIClient:
    """Client for fetching data from the OpenStreetMap Overpass API"""

    def __init__(
        self,
        api_url: Optional[str] = None,
        cache_dir: str = "osm_cache",
        timeout: int = 300,
        max_retries: int = 3,
        retry_delay: int = 5,
    ):
        """
        Initialize the Overpass API client

        Parameters
        ----------
        api_url : Optional[str]
            URL of the Overpass API endpoint
        cache_dir : str
            Directory for caching OSM data
        timeout : int
            Timeout for API requests in seconds
        max_retries : int
            Maximum number of retries for API requests
        retry_delay : int
            Delay between retries in seconds
        """
        self.api_url = api_url or "https://overpass-api.de/api/interpreter"
        self.cache = ElementCache(cache_dir)
        self.cache.load_all_caches()

        # Set default values
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def __enter__(self):
        """Enter context manager"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and save caches"""
        self.close()

    def close(self):
        """Save caches and clean up resources"""
        if hasattr(self, "cache"):
            # Only save if there are modifications
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
        """
        Execute a query against the Overpass API with retry logic and timeout

        Parameters
        ----------
        query : str
            Overpass QL query

        Returns
        -------
        dict
            Response from the API as JSON

        Raises
        ------
        ConnectionError
            If the API request fails after retries
        """
        if "[timeout:" not in query:
            # Overpass timeout is in seconds
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

        raise ConnectionError(
            f"Failed to query Overpass API after {self.max_retries} attempts: {str(last_error)}"
        )

    def get_plants_data(self, country: str, force_refresh: bool = False) -> dict:
        """
        Get power plant data for a country

        Parameters
        ----------
        country : str
            Country name
        force_refresh : bool
            Whether to force a refresh from the API

        Returns
        -------
        dict
            Power plant data
        """
        country_code = get_country_code(country)

        # Check cache first
        if not force_refresh:
            cached_data = self.cache.get_plants(country_code)
            if cached_data:
                return cached_data

        # Build the query
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

        # Execute the query
        logger.info(f"Fetching power plants for {country}")
        data = self.query_overpass(query)

        # Cache the results
        self.cache.store_plants(country_code, data)

        return data

    def get_generators_data(self, country: str, force_refresh: bool = False) -> dict:
        """
        Get power generator data for a country

        Parameters
        ----------
        country : str
            Country name
        force_refresh : bool
            Whether to force a refresh from the API

        Returns
        -------
        dict
            Power generator data
        """
        country_code = get_country_code(country)

        # Check cache first
        if not force_refresh:
            cached_data = self.cache.get_generators(country_code)
            if cached_data:
                return cached_data

        # Build the query
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

        # Execute the query
        logger.info(f"Fetching power generators for {country}")
        data = self.query_overpass(query)

        # Cache the results
        self.cache.store_generators(country_code, data)

        return data

    def get_elements(
        self, element_type: str, element_ids: list[int], recursion_level: int = 0
    ) -> list[dict]:
        """
        Get details of specific elements

        Parameters
        ----------
        element_type : str
            Type of element ('node', 'way', or 'relation')
        element_ids : list[int]
            list of element IDs
        recursion_level : int, default 0
            Current recursion level for nested element fetching

        Returns
        -------
        list[dict]
            Element details
        """
        if not element_ids:
            return []

        # Remove duplicates
        element_ids = list(set(element_ids))

        # First, check which elements are already in the cache
        cached_elements = []
        uncached_ids = []

        # Get cached elements and identify uncached IDs
        for element_id in element_ids:
            element = None
            if element_type == "node":
                element = self.cache.get_node(element_id)
            elif element_type == "way":
                element = self.cache.get_way(element_id)
            elif element_type == "relation":
                element = self.cache.get_relation(element_id)

            if element:
                cached_elements.append(element)
            else:
                uncached_ids.append(element_id)

        # If all elements are cached, return them
        if not uncached_ids:
            logger.debug(
                f"All {len(element_ids)} requested {element_type}s found in cache"
            )
            return cached_elements

        # Query the API for uncached elements
        logger.info(
            f"Fetching {len(uncached_ids)} uncached {element_type}s out of {len(element_ids)} requested"
        )

        # Join IDs into a comma-separated string
        ids_str = ",".join(map(str, uncached_ids))

        # Build query
        query = f"""
        [out:json][timeout:300];
        {element_type}(id:{ids_str});
        out body;
        """

        # For ways and relations, we also need to get their nodes
        if element_type in ["way", "relation"]:
            query = f"""
            [out:json][timeout:300];
            {element_type}(id:{ids_str});
            (._;>;);  // Get all child elements
            out body;
            """

        # Execute query
        data = self.query_overpass(query)

        # Extract the elements
        elements = data.get("elements", [])

        # Log counts for debugging
        nodes = [e for e in elements if e["type"] == "node"]
        ways = [e for e in elements if e["type"] == "way"]
        relations = [e for e in elements if e["type"] == "relation"]
        logger.info(
            f"Fetched {len(elements)} elements: {len(nodes)} nodes, {len(ways)} ways, {len(relations)} relations"
        )

        # Cache the elements
        if element_type == "node":
            self.cache.store_nodes_bulk(nodes)
            fetched_elements = nodes
        elif element_type == "way":
            self.cache.store_ways_bulk(ways)
            self.cache.store_nodes_bulk(nodes)
            fetched_elements = ways

            # Process nodes from ways (with recursion control)
            if recursion_level < 2:  # Prevent deep recursion
                for way in ways:
                    if "nodes" in way:
                        # Check if any node isn't in our cache
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
                                uncached_nodes, recursion_level=recursion_level + 1
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
            self.cache.store_relations_bulk(relations)
            self.cache.store_ways_bulk(ways)
            self.cache.store_nodes_bulk(nodes)
            fetched_elements = relations

            # Process members from relations (with recursion control)
            if recursion_level < 2:  # Prevent deep recursion
                for relation in relations:
                    if "members" in relation:
                        way_members = [
                            m["ref"] for m in relation["members"] if m["type"] == "way"
                        ]
                        node_members = [
                            m["ref"] for m in relation["members"] if m["type"] == "node"
                        ]

                        # Fetch any uncached ways
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
                                uncached_ways, recursion_level=recursion_level + 1
                            )
                        elif way_members:
                            logger.debug(
                                f"All {len(way_members)} ways from relation {relation['id']} already in cache"
                            )

                        # Fetch any uncached nodes
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
                                uncached_nodes, recursion_level=recursion_level + 1
                            )
                        elif node_members:
                            logger.debug(
                                f"All {len(node_members)} nodes from relation {relation['id']} already in cache"
                            )
            elif recursion_level >= 2 and (relations or ways):
                logger.warning(
                    f"Recursion limit reached when processing relation members (level {recursion_level})"
                )

        # Combine cached and newly fetched elements
        return cached_elements + fetched_elements

    def get_nodes(self, node_ids: list[int], recursion_level: int = 0) -> list[dict]:
        """
        Get node details

        Parameters
        ----------
        node_ids : list[int]
            list of node IDs to fetch
        recursion_level : int, default 0
            Current recursion level

        Returns
        -------
        list[dict]
            Node elements
        """
        return self.get_elements("node", node_ids, recursion_level)

    def get_ways(self, way_ids: list[int], recursion_level: int = 0) -> list[dict]:
        """
        Get way details including their nodes

        Parameters
        ----------
        way_ids : list[int]
            list of way IDs to fetch
        recursion_level : int, default 0
            Current recursion level

        Returns
        -------
        list[dict]
            Way elements and their referenced nodes
        """
        return self.get_elements("way", way_ids, recursion_level)

    def get_relations(
        self, relation_ids: list[int], recursion_level: int = 0
    ) -> list[dict]:
        """
        Get relation details including their members

        Parameters
        ----------
        relation_ids : list[int]
            list of relation IDs to fetch
        recursion_level : int, default 0
            Current recursion level

        Returns
        -------
        list[dict]
            Relation elements and their referenced members
        """
        return self.get_elements("relation", relation_ids, recursion_level)

    def get_country_data(
        self, country: str, force_refresh: bool = False, plants_only: bool = False
    ) -> tuple[dict, dict]:
        """
        Get all power plant and generator data for a country

        Parameters
        ----------
        country : str
            Country name
        force_refresh : bool
            Whether to force a refresh from the API
        plants_only : bool
            If True, only fetch plant data

        Returns
        -------
        tuple[dict, dict]
            (plants_data, generators_data)
        """

        def type_order(element):
            order = {"relation": 0, "way": 1, "node": 2}
            return order[element["type"]]

        logger.info(f"Getting OSM data for {country}")

        # Get plant data
        plants_data = self.get_plants_data(country, force_refresh)

        # Get generator data if needed
        if plants_only:
            generators_data = {"elements": []}
        else:
            generators_data = self.get_generators_data(country, force_refresh)

        # Extract node, way, and relation IDs from responses for resolution
        way_ids = []
        relation_ids = []
        node_ids = []

        # Extract from plants data
        for element in plants_data.get("elements", []):
            if element["type"] == "way":
                way_ids.append(element["id"])
                # Extract node IDs from ways if they exist
                if "nodes" in element:
                    node_ids.extend(element["nodes"])
            elif element["type"] == "relation":
                relation_ids.append(element["id"])
            elif element["type"] == "node":
                node_ids.append(element["id"])

        # Extract from generators data
        for element in generators_data.get("elements", []):
            if element["type"] == "way":
                way_ids.append(element["id"])
                # Extract node IDs from ways if they exist
                if "nodes" in element:
                    node_ids.extend(element["nodes"])
            elif element["type"] == "relation":
                relation_ids.append(element["id"])
            elif element["type"] == "node":
                node_ids.append(element["id"])

        # Filter out duplicates
        unique_node_ids = list(set(node_ids))
        unique_way_ids = list(set(way_ids))
        unique_relation_ids = list(set(relation_ids))

        # Only fetch elements that aren't already in the cache
        uncached_node_ids = [
            node_id for node_id in unique_node_ids if not self.cache.get_node(node_id)
        ]
        uncached_way_ids = [
            way_id for way_id in unique_way_ids if not self.cache.get_way(way_id)
        ]
        uncached_relation_ids = [
            rel_id
            for rel_id in unique_relation_ids
            if not self.cache.get_relation(rel_id)
        ]

        # Fetch and cache the uncached elements
        if uncached_node_ids:
            logger.info(
                f"Resolving {len(uncached_node_ids)} uncached nodes out of {len(unique_node_ids)} referenced"
            )
            self.get_nodes(uncached_node_ids)
        elif unique_node_ids:
            logger.info(f"All {len(unique_node_ids)} referenced nodes already in cache")

        if uncached_way_ids:
            logger.info(
                f"Resolving {len(uncached_way_ids)} uncached ways out of {len(unique_way_ids)} referenced"
            )
            self.get_ways(uncached_way_ids)
        elif unique_way_ids:
            logger.info(f"All {len(unique_way_ids)} referenced ways already in cache")

        if uncached_relation_ids:
            logger.info(
                f"Resolving {len(uncached_relation_ids)} uncached relations out of {len(unique_relation_ids)} referenced"
            )
            self.get_relations(uncached_relation_ids)
        elif unique_relation_ids:
            logger.info(
                f"All {len(unique_relation_ids)} referenced relations already in cache"
            )

        plants_data["elements"] = sorted(plants_data["elements"], key=type_order)
        generators_data["elements"] = sorted(
            generators_data["elements"], key=type_order
        )

        return plants_data, generators_data

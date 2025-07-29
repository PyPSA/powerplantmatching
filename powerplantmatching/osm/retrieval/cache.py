"""Multi-level caching system for OpenStreetMap data.

This module provides caching functionality for OSM elements, processed units,
and country coordinate lookups. It reduces API calls and improves performance
by storing data locally in JSON format.
"""

import json
import logging
import os
from functools import lru_cache

from powerplantmatching.osm.models import Unit

logger = logging.getLogger(__name__)


class ElementCache:
    """Multi-level cache for OSM elements and processed units.

    Manages separate caches for different element types (nodes, ways, relations)
    and aggregated data (plants, generators by country). Also caches processed
    Unit objects to avoid reprocessing.

    Attributes
    ----------
    cache_dir : str
        Directory for cache files
    plants_cache : dict[str, dict]
        Country code to plant data mapping
    generators_cache : dict[str, dict]
        Country code to generator data mapping
    ways_cache : dict[str, dict]
        Way ID to element mapping
    nodes_cache : dict[str, dict]
        Node ID to element mapping
    relations_cache : dict[str, dict]
        Relation ID to element mapping
    units_cache : dict[str, list[Unit]]
        Country code to processed units mapping
    *_modified : bool
        Flags tracking which caches have unsaved changes
    """

    def __init__(self, cache_dir: str):
        """Initialize cache with specified directory.

        Parameters
        ----------
        cache_dir : str
            Directory path for cache files
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

        self.plants_cache: dict[str, dict] = {}
        self.generators_cache: dict[str, dict] = {}
        self.ways_cache: dict[str, dict] = {}
        self.nodes_cache: dict[str, dict] = {}
        self.relations_cache: dict[str, dict] = {}
        self.units_cache: dict[str, list[Unit]] = {}

        self.plants_cache_file = os.path.join(self.cache_dir, "plants_power.json")
        self.generators_cache_file = os.path.join(
            self.cache_dir, "generators_power.json"
        )
        self.ways_cache_file = os.path.join(self.cache_dir, "ways_data.json")
        self.nodes_cache_file = os.path.join(self.cache_dir, "nodes_data.json")
        self.relations_cache_file = os.path.join(self.cache_dir, "relations_data.json")
        self.units_cache_file = os.path.join(self.cache_dir, "processed_units.json")

        self.plants_modified = False
        self.generators_modified = False
        self.ways_modified = False
        self.nodes_modified = False
        self.relations_modified = False
        self.units_modified = False

    def load_all_caches(self) -> None:
        """Load all cache files into memory."""
        self.plants_cache = self._load_cache(self.plants_cache_file)
        self.generators_cache = self._load_cache(self.generators_cache_file)
        self.ways_cache = self._load_cache(self.ways_cache_file)
        self.nodes_cache = self._load_cache(self.nodes_cache_file)
        self.relations_cache = self._load_cache(self.relations_cache_file)
        self.units_cache = self._load_units_cache(self.units_cache_file)

    def save_all_caches(self, force: bool = False) -> None:
        """Save all modified caches to disk.

        Parameters
        ----------
        force : bool
            Save all caches regardless of modification status
        """
        os.makedirs(self.cache_dir, exist_ok=True)
        logger.info(
            f"Saving caches to {self.cache_dir} (modified only unless force={force})"
        )

        if self.plants_modified or force:
            self._save_cache(self.plants_cache_file, self.plants_cache)
            self.plants_modified = False

        if self.generators_modified or force:
            self._save_cache(self.generators_cache_file, self.generators_cache)
            self.generators_modified = False

        if self.ways_modified or force:
            self._save_cache(self.ways_cache_file, self.ways_cache)
            self.ways_modified = False

        if self.nodes_modified or force:
            self._save_cache(self.nodes_cache_file, self.nodes_cache)
            self.nodes_modified = False

        if self.relations_modified or force:
            self._save_cache(self.relations_cache_file, self.relations_cache)
            self.relations_modified = False

        if self.units_modified or force:
            self._save_units_cache(self.units_cache_file, self.units_cache)
            self.units_modified = False

        cache_files = {
            "plants": (self.plants_cache_file, self.plants_modified),
            "generators": (self.generators_cache_file, self.generators_modified),
            "ways": (self.ways_cache_file, self.ways_modified),
            "nodes": (self.nodes_cache_file, self.nodes_modified),
            "relations": (self.relations_cache_file, self.relations_modified),
            "units": (self.units_cache_file, self.units_modified),
        }

        for _, (file_path, modified) in cache_files.items():
            if os.path.exists(file_path):
                if force or modified:
                    size = os.path.getsize(file_path) / 1024
                    logger.debug(f"Cache file exists: {file_path} ({size:.1f} KB)")
            elif force or modified:
                logger.warning(f"Cache file was not created: {file_path}")

    def _load_cache(self, cache_path: str) -> dict:
        """Load JSON cache file."""
        if os.path.exists(cache_path):
            try:
                with open(cache_path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load cache {cache_path}: {str(e)}")
                return {}
        return {}

    def _save_cache(self, cache_path: str, data: dict) -> None:
        """Save dictionary to JSON cache file."""
        cache_data = data if data else {}
        try:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "w") as f:
                json.dump(cache_data, f, indent=2)
            logger.info(f"Successfully saved cache to {cache_path}")
        except Exception as e:
            logger.error(f"Failed to save cache to {cache_path}: {str(e)}")

    def get_node(self, node_id: int) -> dict | None:
        """Get cached node by ID."""
        return self.nodes_cache.get(str(node_id))

    def get_way(self, way_id: int) -> dict | None:
        """Get cached way by ID."""
        return self.ways_cache.get(str(way_id))

    def get_relation(self, relation_id: int) -> dict | None:
        """Get cached relation by ID."""
        return self.relations_cache.get(str(relation_id))

    def get_plants(self, country_code: str) -> dict | None:
        """Get cached plant data for country."""
        return self.plants_cache.get(country_code)

    def get_generators(self, country_code: str) -> dict | None:
        """Get cached generator data for country."""
        return self.generators_cache.get(country_code)

    def store_plants(self, country_code: str, data: dict) -> None:
        """Store plant data for country."""
        if country_code is None:
            logger.error("Attempted to store plants with None country_code")
            return
        self.plants_cache[country_code] = data
        self.plants_modified = True

    def store_generators(self, country_code: str, data: dict) -> None:
        """Store generator data for country."""
        if country_code is None:
            logger.error("Attempted to store generators with None country_code")
            return
        self.generators_cache[country_code] = data
        self.generators_modified = True

    def store_nodes_bulk(self, nodes: list[dict]) -> None:
        """Store multiple nodes at once."""
        modified = False
        for node in nodes:
            if node["type"] == "node":
                self.nodes_cache[str(node["id"])] = node
                modified = True
        if modified:
            self.nodes_modified = True

    def store_ways_bulk(self, ways: list[dict]) -> None:
        """Store multiple ways at once."""
        modified = False
        for way in ways:
            if way["type"] == "way":
                self.ways_cache[str(way["id"])] = way
                modified = True
        if modified:
            self.ways_modified = True

    def store_relations_bulk(self, relations: list[dict]) -> None:
        """Store multiple relations at once."""
        modified = False
        for relation in relations:
            if relation["type"] == "relation":
                self.relations_cache[str(relation["id"])] = relation
                modified = True
        if modified:
            self.relations_modified = True

    def _load_units_cache(self, cache_path: str) -> dict[str, list[Unit]]:
        """Load processed units from JSON cache."""
        if os.path.exists(cache_path):
            try:
                with open(cache_path) as f:
                    units_data: dict = json.load(f)

                units_cache = {}
                for country, units in units_data.items():
                    units_cache[country] = [Unit(**unit_dict) for unit_dict in units]
                return units_cache
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load units cache {cache_path}: {str(e)}")
                return {}
        return {}

    def _save_units_cache(self, cache_path: str, data: dict[str, list[Unit]]) -> None:
        """Save processed units to JSON cache."""
        try:
            units_data = {}
            for country, units in data.items():
                units_data[country] = [unit.to_dict() for unit in units]

            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "w") as f:
                json.dump(units_data, f, indent=2)
            logger.info(f"Successfully saved units cache to {cache_path}")
        except Exception as e:
            logger.error(f"Failed to save units cache to {cache_path}: {str(e)}")

    def get_units(self, country_code: str) -> list[Unit]:
        """Get cached processed units for country.

        Parameters
        ----------
        country_code : str
            ISO country code

        Returns
        -------
        list[Unit]
            Processed units, empty list if not cached
        """
        return self.units_cache.get(country_code, [])

    def store_units(self, country_code: str | None, units: list[Unit]) -> None:
        """Store processed units for country.

        Parameters
        ----------
        country_code : str or None
            ISO country code
        units : list[Unit]
            Processed units to cache
        """
        if country_code is None:
            logger.error("Attempted to store units with None country_code")
            return
        self.units_cache[country_code] = units
        self.units_modified = True


class CountryCoordinateCache:
    """Cache for determining country from coordinates.

    Uses LRU cache with configurable precision to minimize API calls
    when determining which country a coordinate belongs to. Includes
    legacy cache compatibility and tolerance-based lookups.

    Attributes
    ----------
    precision : int
        Decimal places for coordinate rounding
    max_size : int
        Maximum LRU cache size
    _lookup : lru_cache
        LRU cached lookup function
    _legacy_cache : dict
        Fallback cache for compatibility
    _client : OverpassAPIClient
        Client for API queries
    """

    def __init__(self, precision: int = 2, max_size: int = 1000):
        """Initialize country coordinate cache.

        Parameters
        ----------
        precision : int
            Decimal places for rounding coordinates
        max_size : int
            Maximum cache entries
        """
        self.precision = precision
        self.max_size = max_size
        self._lookup = lru_cache(maxsize=max_size)(self._uncached_lookup)
        self._legacy_cache = {}
        self._client = None

    def set_client(self, client):
        """Set the API client for lookups."""
        self._client = client

    def _round_coords(self, lat: float, lon: float) -> tuple[float, float]:
        """Round coordinates to configured precision."""
        return (round(lat, self.precision), round(lon, self.precision))

    def _uncached_lookup(self, coords: tuple[float, float]) -> str | None:
        """Look up country for coordinates via API."""
        if self._client is None:
            logger.error("Client not set for country cache")
            return None

        lat, lon = coords

        query = f"""
        [out:json][timeout:30];
        is_in({lat},{lon})->.a;
        relation(pivot.a)["admin_level"="2"]["ISO3166-1"];
        out tags;
        """

        try:
            result = self._client.query_overpass(query)
            elements = result.get("elements", [])

            if elements:
                country_code = elements[0].get("tags", {}).get("ISO3166-1", "")
                return country_code if country_code else None

        except Exception as e:
            logger.warning(
                f"Could not determine country for coordinates {lat},{lon}: {e}"
            )

        return None

    def get(self, lat: float, lon: float) -> str | None:
        """Get country code for coordinates.

        Parameters
        ----------
        lat : float
            Latitude
        lon : float
            Longitude

        Returns
        -------
        str or None
            ISO country code
        """
        rounded_lat, rounded_lon = self._round_coords(lat, lon)
        country = self._lookup((rounded_lat, rounded_lon))

        if country:
            self._legacy_cache[(lat, lon)] = country

            if len(self._legacy_cache) > 1000:
                items = list(self._legacy_cache.items())
                self._legacy_cache = dict(items[-500:])

        return country

    def get_with_tolerance(
        self, lat: float, lon: float, tolerance: float = 0.01
    ) -> str | None:
        """Get country with coordinate tolerance.

        Parameters
        ----------
        lat : float
            Latitude
        lon : float
            Longitude
        tolerance : float
            Coordinate tolerance in degrees

        Returns
        -------
        str or None
            ISO country code
        """
        country = self.get(lat, lon)
        if country:
            return country

        for (cached_lat, cached_lon), cached_country in self._legacy_cache.items():
            if abs(lat - cached_lat) < tolerance and abs(lon - cached_lon) < tolerance:
                return cached_country

        return None

    def items(self):
        """Get legacy cache items."""
        return self._legacy_cache.items()

    def __getitem__(self, key):
        return self._legacy_cache[key]

    def __setitem__(self, key, value):
        self._legacy_cache[key] = value

    def __len__(self):
        return len(self._legacy_cache)

    def get_stats(self) -> dict:
        """Get cache performance statistics.

        Returns
        -------
        dict
            Cache hits, misses, sizes, and hit rate
        """
        cache_info = self._lookup.cache_info()
        return {
            "lru_hits": cache_info.hits,
            "lru_misses": cache_info.misses,
            "lru_size": cache_info.currsize,
            "lru_maxsize": cache_info.maxsize,
            "legacy_size": len(self._legacy_cache),
            "hit_rate": cache_info.hits / (cache_info.hits + cache_info.misses)
            if (cache_info.hits + cache_info.misses) > 0
            else 0.0,
        }

    def clear(self):
        """Clear all caches."""
        self._lookup.cache_clear()
        self._legacy_cache.clear()

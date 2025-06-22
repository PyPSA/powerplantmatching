import json
import logging
import os

from .models import Unit

logger = logging.getLogger(__name__)


class ElementCache:
    """Cache for OSM elements to avoid repeated API calls"""

    def __init__(self, cache_dir: str):
        """
        Initialize the cache

        Parameters
        ----------
        cache_dir : str
            Directory to store cache files
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

        # In-memory caches
        self.plants_cache: dict[str, dict] = {}
        self.generators_cache: dict[str, dict] = {}
        self.ways_cache: dict[str, dict] = {}
        self.nodes_cache: dict[str, dict] = {}
        self.relations_cache: dict[str, dict] = {}
        self.units_cache: dict[str, list[Unit]] = {}

        # Cache file paths
        self.plants_cache_file = os.path.join(self.cache_dir, "plants_power.json")
        self.generators_cache_file = os.path.join(
            self.cache_dir, "generators_power.json"
        )
        self.ways_cache_file = os.path.join(self.cache_dir, "ways_data.json")
        self.nodes_cache_file = os.path.join(self.cache_dir, "nodes_data.json")
        self.relations_cache_file = os.path.join(self.cache_dir, "relations_data.json")
        self.units_cache_file = os.path.join(self.cache_dir, "processed_units.json")

        # Tracking modified state
        self.plants_modified = False
        self.generators_modified = False
        self.ways_modified = False
        self.nodes_modified = False
        self.relations_modified = False
        self.units_modified = False

    def load_all_caches(self) -> None:
        """Load all caches from disk"""
        self.plants_cache = self._load_cache(self.plants_cache_file)
        self.generators_cache = self._load_cache(self.generators_cache_file)
        self.ways_cache = self._load_cache(self.ways_cache_file)
        self.nodes_cache = self._load_cache(self.nodes_cache_file)
        self.relations_cache = self._load_cache(self.relations_cache_file)
        self.units_cache = self._load_units_cache(self.units_cache_file)

    def save_all_caches(self, force: bool = False) -> None:
        """Save all caches to disk"""
        # Ensure directory exists
        os.makedirs(self.cache_dir, exist_ok=True)
        logger.info(
            f"Saving caches to {self.cache_dir} (modified only unless force={force})"
        )

        # Only save modified caches unless forced
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

        # Verify files exist (but only log at debug level)
        cache_files = {
            "plants": (self.plants_cache_file, self.plants_modified),
            "generators": (self.generators_cache_file, self.generators_modified),
            "ways": (self.ways_cache_file, self.ways_modified),
            "nodes": (self.nodes_cache_file, self.nodes_modified),
            "relations": (self.relations_cache_file, self.relations_modified),
            "units": (self.units_cache_file, self.units_modified),
        }

        for cache_name, (file_path, modified) in cache_files.items():
            if os.path.exists(file_path):
                if force or modified:
                    size = os.path.getsize(file_path) / 1024  # Size in KB
                    logger.debug(f"Cache file exists: {file_path} ({size:.1f} KB)")
            elif force or modified:
                logger.warning(f"Cache file was not created: {file_path}")

    def _load_cache(self, cache_path: str) -> dict:
        """Load a cache file from disk"""
        if os.path.exists(cache_path):
            try:
                with open(cache_path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load cache {cache_path}: {str(e)}")
                return {}
        return {}

    def _save_cache(self, cache_path: str, data: dict) -> None:
        """Save a cache to disk"""
        cache_data = data if data else {}
        try:
            # Make sure directory exists
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "w") as f:
                json.dump(cache_data, f, indent=2)
            logger.info(f"Successfully saved cache to {cache_path}")
        except Exception as e:
            logger.error(f"Failed to save cache to {cache_path}: {str(e)}")

    def get_node(self, node_id: int) -> dict | None:
        """Get a node from cache"""
        return self.nodes_cache.get(str(node_id))

    def get_way(self, way_id: int) -> dict | None:
        """Get a way from cache"""
        return self.ways_cache.get(str(way_id))

    def get_relation(self, relation_id: int) -> dict | None:
        """Get a relation from cache"""
        return self.relations_cache.get(str(relation_id))

    def get_plants(self, country_code: str) -> dict | None:
        """Get plants for a country from cache"""
        return self.plants_cache.get(country_code)

    def get_generators(self, country_code: str) -> dict | None:
        """Get generators for a country from cache"""
        return self.generators_cache.get(country_code)

    def store_node(self, node_id: int, data: dict) -> None:
        """Store a node in cache"""
        self.nodes_cache[str(node_id)] = data
        self.nodes_modified = True

    def store_way(self, way_id: int, data: dict) -> None:
        """Store a way in cache"""
        self.ways_cache[str(way_id)] = data
        self.ways_modified = True

    def store_relation(self, relation_id: int, data: dict) -> None:
        """Store a relation in cache"""
        self.relations_cache[str(relation_id)] = data
        self.relations_modified = True

    def store_plants(self, country_code: str, data: dict) -> None:
        """Store plants for a country in cache"""
        if country_code is None:
            logger.error("Attempted to store plants with None country_code")
            return
        self.plants_cache[country_code] = data
        self.plants_modified = True

    def store_generators(self, country_code: str, data: dict) -> None:
        """Store generators for a country in cache"""
        if country_code is None:
            logger.error("Attempted to store generators with None country_code")
            return
        self.generators_cache[country_code] = data
        self.generators_modified = True

    def store_nodes_bulk(self, nodes: list[dict]) -> None:
        """Store multiple nodes in cache"""
        modified = False
        for node in nodes:
            if node["type"] == "node":
                self.nodes_cache[str(node["id"])] = node
                modified = True
        if modified:
            self.nodes_modified = True

    def store_ways_bulk(self, ways: list[dict]) -> None:
        """Store multiple ways in cache"""
        modified = False
        for way in ways:
            if way["type"] == "way":
                self.ways_cache[str(way["id"])] = way
                modified = True
        if modified:
            self.ways_modified = True

    def store_relations_bulk(self, relations: list[dict]) -> None:
        """Store multiple relations in cache"""
        modified = False
        for relation in relations:
            if relation["type"] == "relation":
                self.relations_cache[str(relation["id"])] = relation
                modified = True
        if modified:
            self.relations_modified = True

    def _load_units_cache(self, cache_path: str) -> dict[str, list[Unit]]:
        """Load processed units from cache file."""
        if os.path.exists(cache_path):
            try:
                with open(cache_path) as f:
                    units_data = json.load(f)

                # Convert dict back to Units
                units_cache = {}
                for country, units in units_data.items():
                    units_cache[country] = [Unit(**unit_dict) for unit_dict in units]
                return units_cache
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to load units cache {cache_path}: {str(e)}")
                return {}
        return {}

    def _save_units_cache(self, cache_path: str, data: dict[str, list[Unit]]) -> None:
        """Save processed units to cache file."""
        try:
            # Convert Units to dicts
            units_data = {}
            for country, units in data.items():
                units_data[country] = [unit.to_dict() for unit in units]

            # Make sure directory exists
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "w") as f:
                json.dump(units_data, f, indent=2)
            logger.info(f"Successfully saved units cache to {cache_path}")
        except Exception as e:
            logger.error(f"Failed to save units cache to {cache_path}: {str(e)}")

    def get_units(self, country_code: str) -> list[Unit]:
        """Get processed units for a country from cache."""
        return self.units_cache.get(country_code, [])

    def store_units(self, country_code: str | None, units: list[Unit]) -> None:
        """Store processed units for a country in cache."""
        if country_code is None:
            logger.error("Attempted to store units with None country_code")
            return
        self.units_cache[country_code] = units
        self.units_modified = True

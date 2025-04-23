import inspect
import json
import logging
import os
import re
from dataclasses import asdict, dataclass
from functools import cache
from math import atan2, cos, radians, sin, sqrt
from typing import Any

import numpy as np
import pandas as pd
import pycountry
import requests
from shapely.errors import GEOSException
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
from sklearn.cluster import DBSCAN

from .core import _data_in, get_config

logger = logging.getLogger(__name__)


class OverpassAPI:
    def __init__(
        self,
        filename: str | None = None,
        custom_config: dict[str, Any] | None = None,
    ):
        self.config = get_config(filename=filename, **custom_config)
        osm_config = self.config.get("OSM", {})

        self.osm_cache = _data_in("osm_cache")
        os.makedirs(self.osm_cache, exist_ok=True)

        self.api_url = osm_config.get(
            "api_url", "https://overpass-api.de/api/interpreter"
        )
        self.force_refresh = osm_config.get("force_refresh", False)

        # Cache file paths
        self.plants_cache_file = os.path.join(self.osm_cache, "plants_power.json")
        self.generators_cache_file = os.path.join(
            self.osm_cache, "generators_power.json"
        )
        self.ways_cache_file = os.path.join(self.osm_cache, "ways_data.json")
        self.nodes_cache_file = os.path.join(self.osm_cache, "nodes_data.json")
        self.relations_cache_file = os.path.join(self.osm_cache, "relations_data.json")

        # In-memory caches
        self.plants_cache = {}
        self.generators_cache = {}
        self.ways_cache = {}
        self.nodes_cache = {}
        self.relations_cache = {}

    def load_all_caches(self):
        self.plants_cache = self._load_cache(self.plants_cache_file)
        self.generators_cache = self._load_cache(self.generators_cache_file)
        self.ways_cache = self._load_cache(self.ways_cache_file)
        self.nodes_cache = self._load_cache(self.nodes_cache_file)
        self.relations_cache = self._load_cache(self.relations_cache_file)

    def save_all_caches(self):
        self._save_cache(self.plants_cache_file, self.plants_cache)
        self._save_cache(self.generators_cache_file, self.generators_cache)
        self._save_cache(self.ways_cache_file, self.ways_cache)
        self._save_cache(self.nodes_cache_file, self.nodes_cache)
        self._save_cache(self.relations_cache_file, self.relations_cache)

    def _load_cache(self, cache_path: str) -> dict:
        if os.path.exists(cache_path):
            try:
                with open(cache_path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, KeyError):
                return {}
        return {}

    def _save_cache(self, cache_path: str, data: dict):
        with open(cache_path, "w") as f:
            json.dump(data, f, indent=2)

    @cache
    def get_country_code(self, country: str) -> str:
        try:
            country_obj = pycountry.countries.lookup(country)
            return country_obj.alpha_2
        except LookupError:
            raise ValueError(f"Invalid country name: {country}")

    def get_plants_data(self, country: str, force_refresh: bool = False) -> dict:
        country_code = self.get_country_code(country)

        if not force_refresh and country_code in self.plants_cache:
            return self.plants_cache[country_code]

        query = f"""
        [out:json][timeout:300];
        area["ISO3166-1"="{country_code}"][admin_level=2]->.boundaryarea;
        (
            node["power"="plant"](area.boundaryarea);
            way["power"="plant"](area.boundaryarea);
            relation["power"="plant"](area.boundaryarea);
        );
        out body;
        """

        try:
            logger.warning(f"    downloading plants data for {country}")
            response = requests.post(self.api_url, data={"data": query})
            response.raise_for_status()
            data = response.json()

            self.plants_cache[country_code] = data
            return data
        except requests.RequestException as e:
            raise ConnectionError(f"Failed to fetch plants data: {str(e)}")

    def get_generators_data(self, country: str, force_refresh: bool = False) -> dict:
        country_code = self.get_country_code(country)

        if not force_refresh and country_code in self.generators_cache:
            return self.generators_cache[country_code]

        query = f"""
        [out:json][timeout:300];
        area["ISO3166-1"="{country_code}"][admin_level=2]->.boundaryarea;
        (
            node["power"="generator"](area.boundaryarea);
            way["power"="generator"](area.boundaryarea);
            relation["power"="generator"](area.boundaryarea);
        );
        out body;
        """

        try:
            logger.warning(f"    downloading generators data for {country}")
            response = requests.post(self.api_url, data={"data": query})
            response.raise_for_status()
            data = response.json()

            self.generators_cache[country_code] = data
            return data
        except requests.RequestException as e:
            raise ConnectionError(f"Failed to fetch generators data: {str(e)}")

    @cache
    def get_node_data(self, node_id: int) -> dict | None:
        return self.nodes_cache.get(str(node_id))

    @cache
    def get_way_data(self, way_id: int) -> dict | None:
        return self.ways_cache.get(str(way_id))

    @cache
    def get_relation_data(self, relation_id: int) -> dict | None:
        return self.relations_cache.get(str(relation_id))

    def get_nodes_data(self, node_ids: list[int]) -> None:
        nodes_to_fetch = [
            node_id for node_id in node_ids if str(node_id) not in self.nodes_cache
        ]

        if nodes_to_fetch:
            nodes_str = ",".join(map(str, nodes_to_fetch))
            query = f"""
            [out:json][timeout:300];
            node(id:{nodes_str});
            out body;
            """

            try:
                response = requests.post(self.api_url, data={"data": query})
                response.raise_for_status()
                new_data = response.json()

                for element in new_data["elements"]:
                    if element["type"] == "node":
                        self.nodes_cache[str(element["id"])] = element

            except requests.RequestException as e:
                raise ConnectionError(f"Failed to fetch nodes data: {str(e)}")

    def get_ways_data(self, way_ids: list[int]) -> None:
        ways_to_fetch = [
            way_id for way_id in way_ids if str(way_id) not in self.ways_cache
        ]

        if ways_to_fetch:
            ways_str = ",".join(map(str, ways_to_fetch))
            query = f"""
            [out:json][timeout:300];
            (
                way(id:{ways_str});
                >;  // Get all nodes for ways
            );
            out body;
            way(id:{ways_str});
            out center;
            """

            try:
                response = requests.post(self.api_url, data={"data": query})
                response.raise_for_status()
                new_data = response.json()

                for element in new_data["elements"]:
                    if element["type"] == "node":
                        self.nodes_cache[str(element["id"])] = element
                    elif element["type"] == "way":
                        self.ways_cache[str(element["id"])] = element

            except requests.RequestException as e:
                raise ConnectionError(f"Failed to fetch ways data: {str(e)}")

    def get_relations_data(self, relation_ids: list[int]) -> None:
        relations_to_fetch = [
            relation_id
            for relation_id in relation_ids
            if str(relation_id) not in self.relations_cache
        ]

        if relations_to_fetch:
            relations_str = ",".join(map(str, relations_to_fetch))
            query = f"""
            [out:json][timeout:300];
            relation(id:{relations_str});
            out center;
            """

            try:
                response = requests.post(self.api_url, data={"data": query})
                response.raise_for_status()
                new_data = response.json()

                for element in new_data["elements"]:
                    if element["type"] == "relation":
                        self.relations_cache[str(element["id"])] = element
                        for member in element["members"]:
                            if member["type"] == "node":
                                if str(member["ref"]) not in self.nodes_cache:
                                    self.get_nodes_data([member["ref"]])
                            elif member["type"] == "way":
                                if str(member["ref"]) not in self.ways_cache:
                                    self.get_ways_data([member["ref"]])

            except requests.RequestException as e:
                raise ConnectionError(f"Failed to fetch relations data: {str(e)}")

    def get_country_data(
        self, country: str, force_refresh: bool = False, plants_only: bool = False
    ) -> dict:
        logger.warning(f"Fetching data for {country}")
        plants_data = self.get_plants_data(country, force_refresh)

        if not plants_only:
            generators_data = self.get_generators_data(country, force_refresh)
        else:
            generators_data = {"elements": []}

        # Collect all way IDs, relation IDs, and node IDs from both datasets
        way_ids = []
        relation_ids = []
        node_ids = []
        for dataset in [plants_data, generators_data]:
            way_ids.extend(
                [
                    element["id"]
                    for element in dataset["elements"]
                    if element["type"] == "way"
                ]
            )
            relation_ids.extend(
                [
                    element["id"]
                    for element in dataset["elements"]
                    if element["type"] == "relation"
                ]
            )
            node_ids.extend(
                [
                    element["id"]
                    for element in dataset["elements"]
                    if element["type"] == "node"
                ]
            )

        self.get_nodes_data(node_ids)
        self.get_ways_data(way_ids)
        self.get_relations_data(relation_ids)

        return plants_data, generators_data

    def get_countries_data(
        self,
        countries: list[str] | None = None,
        force_refresh: bool = False,
        plants_only: bool = False,
    ) -> dict:
        all_plants_data = {}
        all_generators_data = {}

        if countries is None or len(countries) == 0:
            return {
                "plants_data": self.plants_cache,
                "generators_data": self.generators_cache,
            }

        for country in countries:
            try:
                country_code = self.get_country_code(country)

                plants_data, generators_data = self.get_country_data(
                    country, force_refresh=force_refresh, plants_only=plants_only
                )

                all_plants_data[country_code] = plants_data
                all_generators_data[country_code] = generators_data

            except Exception as e:
                logger.warning(f"Error processing country {country}: {str(e)}")
                continue

        if not any([all_plants_data, all_generators_data]):
            raise ValueError("No data could be retrieved for the specified countries")

        return {
            "plants_data": all_plants_data,
            "generators_data": all_generators_data,
        }


@dataclass
class PowerSource:
    clustering: dict | None = None
    estimation: dict | None = None


@dataclass
class PlantPolygon:
    id: str
    type: str
    obj: Polygon


@dataclass
class Plant:
    id: str
    type: str
    source: str
    lat: float
    lon: float
    capacity_mw: float | None = None
    capacity_source: str | None = None
    country: str | None = None
    name: str | None = None
    generator_count: int | None = None
    case: str | None = None
    technology: str | None = None

    def to_dict(self):
        return {k: v for k, v in asdict(self).items() if v is not None}


class PowerPlantExtractor:
    def __init__(self, filename: str | None = None, custom_config: dict[str, Any] = {}):
        self.config = get_config(filename=filename, **custom_config)
        self.load_configurations()
        self.api = OverpassAPI(filename=filename, custom_config=self.config)
        self.clusters = {}

    def load_configurations(self):
        osm_config = self.config.get("OSM", {})
        self.sources = {
            name: PowerSource(**config)
            for name, config in osm_config.get("sources", {}).items()
        }

        self.enable_estimation = osm_config.get("enable_estimation", False)
        self.enable_clustering = osm_config.get("enable_clustering", False)

    def query_cached_element(self, element_type: str, element_id: str) -> dict:
        try:
            if element_type == "node":
                return self.api.get_node_data(int(element_id))
            elif element_type == "way":
                return self.api.get_way_data(int(element_id))
            elif element_type == "relation":
                return self.api.get_relation_data(int(element_id))
            else:
                raise ValueError(f"Invalid element type: {element_type}")
        except Exception as e:
            raise ValueError(f"Error querying element {element_id}: {str(e)}")

    def extract_plants(
        self,
        countries: list[str],
        force_refresh: bool | None = None,
        plants_only: bool | None = None,
    ) -> pd.DataFrame:
        self.clusters = {}
        all_plants = []

        if plants_only is None:
            plants_only = self.config["OSM"].get("plants_only", False)

        if force_refresh is None:
            force_refresh = self.config["OSM"].get("force_refresh", False)

        # Load all caches at the beginning
        self.api.load_all_caches()

        for country in countries:
            plants_data, generators_data = self.api.get_country_data(
                country, force_refresh=force_refresh, plants_only=plants_only
            )

            country_obj = pycountry.countries.lookup(country)

            if country_obj is None:
                logger.warning(f"Invalid country name: {country}")
                continue

            if country_obj.name != country:
                logger.warning(
                    f"Country name mismatch: {country_obj.name} != {country}. Using {country} instead."
                )

            primary_plants, plant_polygons = self._process_plants(
                plants_data, country=country
            )

            if not plants_only:
                secondary_plants = self._process_generators(
                    generators_data, plant_polygons, country=country
                )
            else:
                secondary_plants = []

            country_plants = primary_plants + secondary_plants
            all_plants.extend(country_plants)

        # Save all caches at the end
        self.api.save_all_caches()

        df = pd.DataFrame([plant.to_dict() for plant in all_plants])

        df = df.drop_duplicates(subset="name", keep="first")

        self.last_extracted_df = df

        return df

    def _process_plants(
        self, plants_data: dict, country: str | None = None
    ) -> tuple[list[Plant], list[PlantPolygon]]:
        processed_plants = []
        plant_polygons = []

        plants_data["elements"] = sorted(
            plants_data["elements"],
            key=lambda x: ["relation", "way", "node"].index(x["type"]),
        )

        self.ways_in_relations = set()
        self.ways_rel_mapping = {}
        for element in plants_data["elements"]:
            plant = self._process_plant_element(element, country=country, case="plants")
            if plant:
                if element["type"] == "relation":
                    relation_polygon = self._create_relation_polygon(element)
                    if relation_polygon:
                        plant_polygons.append(relation_polygon)
                    for rel_element in element["members"]:
                        if rel_element["type"] == "way":
                            self.ways_in_relations.add(str(rel_element["ref"]))
                            self.ways_rel_mapping[str(rel_element["ref"])] = str(
                                element["id"]
                            )
                    processed_plants.append(plant)
                elif element["type"] == "way":
                    if str(element["id"]) in self.ways_in_relations:
                        logger.debug(
                            f"Way {element['id']} is in relation: {self.ways_rel_mapping[str(element['id'])]}. Skipping..."
                        )
                        continue
                    way_polygon = self._create_way_polygon(element)
                    if way_polygon:
                        plant_polygons.append(way_polygon)
                    else:
                        logger.debug(
                            f"Failed to create polygon for element {element['id']} of type {element['type']} with nodes {element['nodes']}"
                        )
                    processed_plants.append(plant)
                elif element["type"] == "node":
                    processed_plants.append(plant)
            else:
                logger.debug(
                    f"Failed to process element {element['id']} of type {element['type']}"
                )

        return processed_plants, plant_polygons

    def _process_plant_element(
        self, element: dict, country: str | None = None, case: str | None = None
    ) -> Plant | None:
        plant_data = self._extract_plant_data(element, country=country, case=case)
        if not plant_data:
            return None

        return Plant(**plant_data) if self._validate_plant_data(plant_data) else None

    def _create_way_polygon(self, element: dict) -> PlantPolygon | None:
        way_data = self.api.get_way_data(element["id"])
        if way_data and "nodes" in way_data:
            coords = []
            for node_id in way_data["nodes"]:
                node_data = self.api.get_node_data(node_id)
                if node_data:
                    coords.append((node_data["lon"], node_data["lat"]))
            if len(coords) >= 3:
                polygon = Polygon(coords)
                plantpolygon = PlantPolygon(
                    id=str(element["id"]), type=element["type"], obj=polygon
                )
                return plantpolygon
            else:
                logger.debug(
                    f"Failed to create polygon for element {element['id']} of type {element['type']} with nodes {element['nodes']}"
                )
        return None

    def _create_relation_polygon(self, element: dict) -> Polygon | None:
        way_polygons = []
        for member in element["members"]:
            if member["type"] == "way":
                way_data = self.api.get_way_data(member["ref"])
                if way_data and "nodes" in way_data:
                    coords = []
                    for node_id in way_data["nodes"]:
                        node_data = self.api.get_node_data(node_id)
                        if node_data:
                            coords.append((node_data["lon"], node_data["lat"]))
                    if len(coords) >= 3:
                        way_polygons.append(Polygon(coords))
                    else:
                        logger.debug(
                            f"Failed to create polygon for way {member['ref']} of element {element['id']} of type {element['type']} with nodes {way_data['nodes']}"
                        )

        if way_polygons:
            for way_polygon in way_polygons:
                if not way_polygon.is_valid:
                    logger.debug(f"Polygon for way {way_polygon} is not valid")
                    way_polygons.remove(way_polygon)

            if len(way_polygons) == 1:
                return PlantPolygon(
                    id=str(element["id"]), type=element["type"], obj=way_polygons[0]
                )
            try:
                joint_polygon = unary_union(way_polygons)
                if joint_polygon.is_valid:
                    plantpolygon = PlantPolygon(
                        id=str(element["id"]), type=element["type"], obj=joint_polygon
                    )
                    return plantpolygon
            except GEOSException as e:
                logger.debug(
                    f"Failed to create polygon for element {element['id']} of type {element['type']}: {str(e)}"
                )
        return None

    def _extract_plant_data(
        self, element: dict, country: str | None = None, case: str | None = None
    ) -> dict | None:
        plant_data = {
            "id": f"OSM_{element['tags']['power']}:{element['type']}_{element['id']}",
            "type": f"{element['tags']['power']}:{element['type']}",
            "source": element.get("tags", {}).get("plant:source")
            or element.get("tags", {}).get("generator:source"),
            "name": element.get("tags", {}).get("name"),
            "country": country,
            "case": case,
            "technology": element.get("tags", {}).get(
                "plant:method", element.get("tags", {}).get("plant:type")
            )
            or element.get("tags", {}).get(
                "generator:method", element.get("tags", {}).get("generator:type")
            ),
        }

        coords = self._get_element_coordinates(element)
        if coords:
            plant_data.update(coords)
        else:
            return None

        capacity, capacity_source = self._get_plant_capacity(element)
        if capacity:
            plant_data["capacity_mw"] = capacity
            plant_data["capacity_source"] = capacity_source

        return plant_data if self._validate_plant_data(plant_data) else None

    @staticmethod
    def _validate_plant_data(plant_data: dict) -> bool:
        required_fields = ["id", "type", "source", "lat", "lon"]
        return all(field in plant_data for field in required_fields)

    def _get_plant_capacity(self, element: dict) -> tuple[float | None, str]:
        tags = element.get("tags", {})

        capacity_keys = [
            "plant:output:electricity",
            "generator:output:electricity",
        ]

        for key in capacity_keys:
            if key in tags:
                normalized_capacity = self._normalize_capacity(tags[key])
                if normalized_capacity:
                    return normalized_capacity, "direct"

        if self.enable_estimation:
            source_type = tags.get("plant:source") or tags.get("generator:source")
            if source_type in self.sources:
                source_config = self.sources[source_type]
                if source_config.estimation:
                    if source_config.estimation["method"] == "area_based":
                        if element["type"] == "way":
                            way_data = self.query_cached_element(
                                "way", str(element["id"])
                            )
                            if way_data and "nodes" in way_data:
                                coords = []
                                for node_id in way_data["nodes"]:
                                    node_data = self.query_cached_element(
                                        "node", str(node_id)
                                    )
                                    if node_data:
                                        coords.append(
                                            {
                                                "lat": node_data["lat"],
                                                "lon": node_data["lon"],
                                            }
                                        )
                                if coords:
                                    area = calculate_area(coords)
                                    efficiency = source_config.estimation["efficiency"]
                                    return (area * efficiency) / 1e6, "estimated"
                    elif source_config.estimation["method"] == "default_value":
                        return source_config.estimation[
                            "default_capacity"
                        ] / 1000, "estimated"

        return None, "unknown"

    def _normalize_capacity(self, capacity_str: str) -> float | None:
        if not capacity_str:
            return None

        capacity_str = str(capacity_str).strip().lower()
        capacity_str = capacity_str.replace(",", ".")

        match = re.match(r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+p?)?$", capacity_str)
        if match:
            value, unit = match.groups()
            value = float(value)

            if unit in ["w", "watts", "wp"]:
                return value / 1e6
            elif unit in ["kw", "kilowatts", "kwp"]:
                return value / 1000
            elif unit in ["mw", "megawatts", "mwp"]:
                return value
            elif unit in ["gw", "gigawatts", "gwp"]:
                return value * 1000
            else:
                return value / 1000  # If no unit is specified, assume to kilowatts
        else:
            try:
                return float(capacity_str) / 1000
            except ValueError:
                logger.debug(f"Failed to parse capacity string: {capacity_str}")
                return None

    def _get_element_coordinates(self, element: dict) -> dict | None:
        if element["type"] == "node":
            node_data = self.query_cached_element("node", str(element["id"]))
            if node_data:
                return {"lat": node_data["lat"], "lon": node_data["lon"]}
        elif element["type"] == "way":
            way_data = self.query_cached_element("way", str(element["id"]))
            if way_data and "center" in way_data:
                return way_data["center"]
        elif element["type"] == "relation":
            relation_data = self.query_cached_element("relation", str(element["id"]))
            if relation_data and "members" in relation_data:
                coords = []
                for member in relation_data["members"]:
                    if member["type"] == "node":
                        node_data = self.query_cached_element(
                            "node", str(member["ref"])
                        )
                        if node_data:
                            coords.append(
                                {"lat": node_data["lat"], "lon": node_data["lon"]}
                            )
                    elif member["type"] == "way":
                        way_data = self.query_cached_element("way", str(member["ref"]))
                        if way_data and "center" in way_data:
                            coords.append(way_data["center"])
                if coords:
                    return calculate_polygon_centroid(coords)
                else:
                    logger.debug(f"Failed to get coordinates for element {element}")
        else:
            logger.debug(f"Unsupported element type: {element['type']}")
        return None

    def _process_generators(
        self,
        generators_data: dict,
        plant_polygons: list[PlantPolygon],
        country: str | None = None,
    ) -> list[Plant]:
        generators_by_type = self._group_generators_by_type(generators_data["elements"])
        processed_plants = []

        for source_type, generators in generators_by_type.items():
            filtered_generators = self._filter_generators(generators, plant_polygons)
            if source_type not in self.sources:
                for generator in filtered_generators:
                    plant = self._process_plant_element(
                        generator, country=country, case="excluded_source"
                    )
                    if plant:
                        processed_plants.append(plant)
                    else:
                        logger.debug(
                            f"Failed to extract data for generator {generator['id']}"
                        )
            else:
                source_config = self.sources[source_type]

                if self.enable_clustering and source_config.clustering:
                    labels = self._cluster_generators(
                        filtered_generators, source_config.clustering
                    )

                    cluster_bin = {}
                    units = []
                    for i, label in enumerate(labels):
                        if label >= 0:
                            cluster_bin.setdefault(label, []).append(generators[i])
                        else:
                            units.append(generators[i])

                    clusters = list(cluster_bin.values())

                    if units:
                        for generator in units:
                            plant = self._process_plant_element(
                                generator, country=country, case="noise_point"
                            )
                            if plant:
                                processed_plants.append(plant)
                            else:
                                logger.debug(
                                    f"Failed to extract data for generator {generator['id']}"
                                )

                    viz_data = self.prepare_cluster_visualization_data(
                        filtered_generators, labels
                    )
                    if country not in self.clusters:
                        self.clusters[country] = {}
                    self.clusters[country][source_type] = viz_data

                    if not clusters:
                        logger.warning(
                            f"No clusters found for source type: {source_type}"
                        )
                        continue

                    for cluster in clusters:
                        plant = self._process_generator_cluster(
                            cluster,
                            clusters.index(cluster),
                            source_type,
                            country=country,
                            case="cluster_point",
                        )

                        processed_plants.append(plant)

        return processed_plants

    def _process_generator_cluster(
        self,
        cluster: list[dict],
        index: int,
        source_type: str,
        country: str | None = None,
        case: str | None = None,
    ) -> Plant | None:
        if not cluster:
            return None

        lats = []
        lons = []
        total_capacity = 0

        for generator in cluster:
            coords = self._get_element_coordinates(generator)
            if coords:
                lats.append(coords["lat"])
                lons.append(coords["lon"])

            capacity, _ = self._get_plant_capacity(generator)
            if capacity:
                total_capacity += capacity

        if not lats or not lons:
            logger.debug(
                f"Failed to get coordinates for generator cluster {cluster[0]['id']}"
            )
            return None

        return Plant(
            id=f"OSM_cluster_{cluster[0]['type']}_{cluster[0]['id']}",
            type="generator:cluster",
            source=source_type,
            country=country,
            lat=sum(lats) / len(lats),
            lon=sum(lons) / len(lons),
            capacity_mw=total_capacity,
            capacity_source="aggregated",
            generator_count=len(cluster),
            name=f"cluster_{str(index).zfill(3)}_{country}_{source_type}",
            case=case,
            technology=cluster[0]
            .get("tags", {})
            .get(
                "generator:method", cluster[0].get("tags", {}).get("generator:type", "")
            ),
        )

    def _group_generators_by_type(
        self, generators: list[dict]
    ) -> dict[str, list[dict]]:
        grouped = {}
        for generator in generators:
            source = generator.get("tags", {}).get("generator:source")
            if source:
                grouped.setdefault(source, []).append(generator)
        return grouped

    def _filter_generators(
        self, generators: list[dict], plant_polygons: list[PlantPolygon]
    ) -> list[dict]:
        filtered_generators = []
        for generator in generators:
            if generator["type"] == "way":
                if generator["id"] in self.ways_in_relations:
                    logger.debug(
                        f"Generator '{generator['id']}' is in a relation '{self.ways_rel_mapping[generator['id']]}'. Skipping..."
                    )

                    continue
            coords = self._get_element_coordinates(generator)
            continue_flag = False
            if coords:
                point = Point(coords["lon"], coords["lat"])
                for plantpolygon in plant_polygons:
                    if plantpolygon.obj.contains(point):
                        logger.debug(
                            f"Generator {generator['id']} is in the plant polygon {plantpolygon.type}/{plantpolygon.id}. Skipping..."
                        )
                        continue_flag = True
                        break
            else:
                continue_flag = True
                logger.debug(
                    f"Failed to get coordinates for generator {generator['id']}. Skipping..."
                )
            if continue_flag:
                continue
            filtered_generators.append(generator)
        return filtered_generators

    @staticmethod
    def _cluster_fn(fn, coords, config):
        if "to_radians" in config:
            if config["to_radians"]:
                coords = np.radians(coords)

        signature = inspect.signature(fn)
        possible_parameters = list(signature.parameters.keys())
        model = fn(
            **{param: config[param] for param in config if param in possible_parameters}
        )
        clustering = model.fit(coords)
        return clustering

    def _cluster_generators(
        self, generators: list[dict], clustering_config: dict
    ) -> tuple[list[list[dict]], np.ndarray]:
        arrays = []
        for gen in generators:
            array = self._get_element_coordinates(gen)
            if array:
                arrays.append([array["lat"], array["lon"]])

        if not arrays:
            logger.warning(
                f"No generators found for clustering with config: {clustering_config}"
            )
            return np.array([])

        coords = np.array(arrays)

        if clustering_config["method"] == "dbscan":
            clustering = self._cluster_fn(DBSCAN, coords, clustering_config)
        else:
            raise ValueError(
                f"Unknown clustering method: {clustering_config['method']}"
            )

        return clustering.labels_

    def prepare_cluster_visualization_data(
        self, generators: list[dict], labels: np.ndarray
    ) -> dict[str, list]:
        data = {"lat": [], "lon": [], "cluster": [], "capacity": [], "id": []}

        for i, generator in enumerate(generators):
            coords = self._get_element_coordinates(generator)
            if coords:
                data["lat"].append(coords["lat"])
                data["lon"].append(coords["lon"])
                data["cluster"].append(int(labels[i]))
                capacity, _ = self._get_plant_capacity(generator)
                data["capacity"].append(capacity if capacity is not None else 0)
                data["id"].append(generator["id"])

        return data

    def _combine_cluster_data(
        self, country: str = "all", source_type: str = "all"
    ) -> pd.DataFrame:
        combined_data = []

        for c, country_data in self.clusters.items():
            if country != "all" and c != country:
                continue
            for s, source_data in country_data.items():
                if source_type != "all" and s != source_type:
                    continue
                df = pd.DataFrame(source_data)
                df["country"] = c
                df["source"] = s
                df["text"] = df.apply(
                    lambda row: f"Country: {c}<br>Source: {s}<br>ID: {row['id']}<br>Capacity: {row['capacity']:.2f} MW",
                    axis=1,
                )
                combined_data.append(df)

        return (
            pd.concat(combined_data, ignore_index=True)
            if combined_data
            else pd.DataFrame()
        )

    def plot_clusters(
        self, country: str = "all", source_type: str = "all", show: bool = False
    ):
        try:
            import plotly  # noqa

            go = plotly.graph_objs
        except ImportError:
            logger.warning("Plotly is not installed. Skipping cluster visualization.")
            return None

        combined_data = self._combine_cluster_data(country, source_type)

        if combined_data.empty:
            logger.warning(
                f"No data available for country {country} and source type {source_type}."
            )
            return None

        fig = go.Figure()

        for (country, source, cluster), group in combined_data.groupby(
            ["country", "source", "cluster"]
        ):
            fig.add_trace(
                go.Scattermapbox(
                    lat=group["lat"],
                    lon=group["lon"],
                    mode="markers",
                    marker=dict(size=10),
                    text=group["text"],
                    name=f"{country}:{source}:{cluster}",
                )
            )

        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox=dict(
                center=dict(
                    lat=combined_data["lat"].mean(), lon=combined_data["lon"].mean()
                ),
                zoom=3,
            ),
            showlegend=True,
            height=800,
            width=1200,
            title_text="Generator Clusters",
        )

        if show:
            fig.show(config={"scrollZoom": True})

        return fig

    def summary(self, df: pd.DataFrame = None) -> pd.DataFrame:
        if df is None:
            if not hasattr(self, "last_extracted_df"):
                raise ValueError(
                    "No data available. Please run extract_plants() first or provide a dataframe."
                )
            df = self.last_extracted_df

        df["capacity_mw"] = pd.to_numeric(df["capacity_mw"], errors="coerce")
        summary = (
            df.groupby(["country", "source"])["capacity_mw"].sum().unstack(fill_value=0)
        )
        summary["Total"] = summary.sum(axis=1)
        source_totals = summary.sum().to_frame("Total").T
        summary = pd.concat([summary, source_totals])
        summary = summary.round(2)
        summary = summary.sort_values("Total", ascending=False)
        return summary


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000

    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c

    return distance


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


def calculate_polygon_centroid(coordinates: list[dict[str, float]]) -> dict[str, float]:
    if not coordinates:
        return None

    lat_sum = sum(coord["lat"] for coord in coordinates)
    lon_sum = sum(coord["lon"] for coord in coordinates)
    n = len(coordinates)

    return {"lat": lat_sum / n, "lon": lon_sum / n}


def main():
    pass


if __name__ == "__main__":
    main()

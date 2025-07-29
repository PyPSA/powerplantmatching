"""Geometry handling for OSM power plant elements.

This module provides geometric operations for OSM elements including
coordinate extraction, polygon creation, and spatial relationship checks.
"""

import logging
from typing import Any

from shapely.errors import ShapelyError
from shapely.geometry import MultiPoint, Point, Polygon
from shapely.ops import unary_union

from powerplantmatching.osm.models import PlantGeometry, create_plant_geometry
from powerplantmatching.osm.quality.rejection import RejectionTracker
from powerplantmatching.osm.retrieval.client import OverpassAPIClient

logger = logging.getLogger(__name__)


class GeometryHandler:
    """Handles geometric operations for OSM elements.

    Provides methods to create and manipulate geometries from OSM nodes,
    ways, and relations. Supports coordinate extraction, polygon creation,
    and spatial relationship testing.

    Attributes
    ----------
    client : OverpassAPIClient
        API client for retrieving element dependencies
    rejection_tracker : RejectionTracker
        Tracker for invalid geometries

    Examples
    --------
    >>> handler = GeometryHandler(client, rejection_tracker)
    >>> geometry = handler.get_element_geometry(way_element)
    >>> lat, lon = geometry.get_centroid()
    """

    def __init__(self, client: OverpassAPIClient, rejection_tracker: RejectionTracker):
        """Initialize geometry handler.

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing cached elements
        rejection_tracker : RejectionTracker
            Tracker for geometry errors
        """
        self.client = client
        self.rejection_tracker = rejection_tracker

    def create_node_geometry(self, node: dict[str, Any]) -> PlantGeometry | None:
        """Create point geometry from OSM node."""
        if "lat" not in node or "lon" not in node:
            logger.debug(f"Node {node.get('id', 'unknown')} missing coordinates")
            return None

        try:
            point = Point(node["lon"], node["lat"])
            return create_plant_geometry(node, point)
        except Exception as e:
            logger.debug(
                f"Error creating point geometry for node {node.get('id')}: {str(e)}"
            )
            return None

    def create_way_geometry(self, way: dict[str, Any]) -> PlantGeometry | None:
        """Create polygon or point geometry from OSM way.

        Parameters
        ----------
        way : dict
            OSM way element with nodes list

        Returns
        -------
        PlantGeometry or None
            Polygon for closed ways, point for others
        """
        if "nodes" not in way:
            logger.debug(f"Way {way['id']} does not have nodes")
            return None

        coords = []
        for node_id in way["nodes"]:
            node = self.client.cache.get_node(node_id)
            if node and "lat" in node and "lon" in node:
                coords.append((node["lon"], node["lat"]))

        if len(coords) == 1:
            return create_plant_geometry(way, Point(coords[0]))
        elif len(coords) == 2:
            mid_lon = (coords[0][0] + coords[1][0]) / 2
            mid_lat = (coords[0][1] + coords[1][1]) / 2
            return create_plant_geometry(way, Point(mid_lon, mid_lat))

        else:
            try:
                polygon = Polygon(coords)
                if not polygon.is_valid:
                    logger.debug(f"Invalid polygon for way {way['id']}")
                    return None

                return create_plant_geometry(way, polygon)
            except ShapelyError as e:
                logger.debug(f"Error creating polygon for way/{way['id']}: {str(e)}")
                return None

    def create_relation_geometry(
        self, relation: dict[str, Any]
    ) -> PlantGeometry | None:
        """Create geometry from OSM relation members.

        Combines member ways into polygons or creates convex hull
        from member nodes. Handles multipolygon relations.

        Parameters
        ----------
        relation : dict
            OSM relation with members list

        Returns
        -------
        PlantGeometry or None
            Combined geometry from members
        """
        if "members" not in relation:
            logger.debug(f"Relation {relation['id']} does not have members")
            return None

        way_members = [m for m in relation["members"] if m["type"] == "way"]
        node_members = [m for m in relation["members"] if m["type"] == "node"]

        if not way_members and not node_members:
            logger.debug(f"Relation {relation['id']} does not have way or node members")
            return None

        polygons = []
        for way_member in way_members:
            way_id = way_member["ref"]
            way = self.client.cache.get_way(way_id)
            if way:
                way_geom = self.create_way_geometry(way)
                if way_geom and isinstance(way_geom.geometry, Polygon):
                    polygons.append(way_geom.geometry)

        if not polygons and node_members:
            points = []
            for node_member in node_members:
                node_id = node_member["ref"]
                node = self.client.cache.get_node(node_id)
                if node and "lat" in node and "lon" in node:
                    points.append(Point(node["lon"], node["lat"]))

            if len(points) == 1:
                return create_plant_geometry(relation, points[0])
            elif len(points) == 2:
                return create_plant_geometry(relation, points[0])
            elif len(points) >= 3:
                hull = MultiPoint(points).convex_hull
                return create_plant_geometry(relation, hull)

        if polygons:
            try:
                if len(polygons) == 1:
                    union = polygons[0]
                else:
                    union = unary_union(polygons)

                if not union.is_valid:
                    logger.debug(f"Invalid union polygon for relation {relation['id']}")
                    return None

                return create_plant_geometry(relation, union)
            except ShapelyError as e:
                logger.debug(
                    f"Error creating union polygon for relation {relation['id']}: {str(e)}"
                )
                return None

        return None

    def get_element_geometry(self, element: dict[str, Any]) -> PlantGeometry | None:
        """Get geometry for any OSM element type.

        Parameters
        ----------
        element : dict
            OSM element (node, way, or relation)

        Returns
        -------
        PlantGeometry or None
            Appropriate geometry for element type
        """
        element_type = element.get("type")

        if element_type == "node":
            return self.create_node_geometry(element)
        elif element_type == "way":
            return self.create_way_geometry(element)
        elif element_type == "relation":
            return self.create_relation_geometry(element)
        else:
            logger.warning(f"Unknown element type: {element_type}")
            return None

    def process_element_coordinates(
        self, element: dict[str, Any]
    ) -> tuple[float | None, float | None]:
        """Extract coordinates from any OSM element.

        Parameters
        ----------
        element : dict
            OSM element to process

        Returns
        -------
        tuple[float, float] or tuple[None, None]
            (latitude, longitude) or (None, None) if not found
        """
        plant_geometry = self.get_element_geometry(element)
        if plant_geometry:
            return plant_geometry.get_centroid()

        if element.get("type") == "node" and "lat" in element and "lon" in element:
            return (element["lat"], element["lon"])

        if element.get("type") == "way" and "nodes" in element:
            for node_id in element["nodes"]:
                node = self.client.cache.get_node(node_id)
                if node and "lat" in node and "lon" in node:
                    logger.debug(
                        f"Using first available node coordinate for way {element['id']}"
                    )
                    return (node["lat"], node["lon"])

        if element.get("type") == "relation":
            lat, lon = self.get_relation_centroid_from_members(element)
            if lat is not None and lon is not None:
                return (lat, lon)

        return (None, None)

    def get_relation_centroid_from_members(
        self, relation: dict[str, Any]
    ) -> tuple[float | None, float | None]:
        """Calculate centroid from relation members.

        Prioritizes members with capacity tags when computing
        the centroid for more accurate plant location.
        """
        if "members" not in relation:
            return None, None

        points = []

        capacity_member_points = []

        for member in relation["members"]:
            member_type = member["type"]
            member_id = member["ref"]

            member_elem = None
            if member_type == "node":
                member_elem = self.client.cache.get_node(member_id)
            elif member_type == "way":
                member_elem = self.client.cache.get_way(member_id)
            elif member_type == "relation":
                continue

            if not member_elem:
                continue

            has_capacity = False
            if "tags" in member_elem:
                tags = member_elem["tags"]
                for tag in ["plant:output:electricity", "generator:output:electricity"]:
                    if tag in tags:
                        has_capacity = True
                        break

            lat, lon = None, None

            if member_type == "node" and "lat" in member_elem and "lon" in member_elem:
                lat, lon = member_elem["lat"], member_elem["lon"]
            elif member_type == "way":
                way_geom = self.create_way_geometry(member_elem)
                if way_geom:
                    lat, lon = way_geom.get_centroid()

            if lat is not None and lon is not None:
                if has_capacity:
                    capacity_member_points.append((lat, lon))
                else:
                    points.append((lat, lon))

        if capacity_member_points:
            lat_sum = sum(p[0] for p in capacity_member_points)
            lon_sum = sum(p[1] for p in capacity_member_points)
            return lat_sum / len(capacity_member_points), lon_sum / len(
                capacity_member_points
            )

        if points:
            lat_sum = sum(p[0] for p in points)
            lon_sum = sum(p[1] for p in points)
            return lat_sum / len(points), lon_sum / len(points)

        return None, None

    def is_element_within_plant_geometries(
        self,
        element: dict[str, Any],
        plant_geometries: list[PlantGeometry],
        buffer_meters: float | None = None,
    ) -> tuple[bool, str | None]:
        """Check if element is within any plant boundary.

        Parameters
        ----------
        element : dict
            OSM element to check
        plant_geometries : list[PlantGeometry]
            Plant boundaries to test against
        buffer_meters : float, optional
            Buffer distance for containment check

        Returns
        -------
        tuple[bool, str or None]
            (is_within, plant_id) where plant_id is like "way/123456"
        """
        element_geometry = self.get_element_geometry(element)
        if not element_geometry:
            return False, None

        lat, lon = element_geometry.get_centroid()
        if lat is None or lon is None:
            return False, None

        for plant_geom in plant_geometries:
            try:
                if plant_geom.contains_point(lat, lon, buffer_meters):
                    return True, f"{plant_geom.type}/{plant_geom.id}"

                if not isinstance(element_geometry.geometry, Point):
                    if element_geometry.intersects(plant_geom, buffer_meters):
                        return True, f"{plant_geom.type}/{plant_geom.id}"

            except Exception as e:
                logger.debug(f"Error checking geometry containment: {str(e)}")
                continue

        return False, None

    def check_point_within_geometries(
        self,
        lat: float,
        lon: float,
        plant_geometries: dict[str, PlantGeometry],
        buffer_meters: float | None = None,
    ) -> str | None:
        """Check if point is within any plant geometry."""
        for geom_id, plant_geom in plant_geometries.items():
            if plant_geom.contains_point(lat, lon, buffer_meters):
                return geom_id
        return None

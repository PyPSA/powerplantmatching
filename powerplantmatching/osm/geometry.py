import logging
from typing import Any, Optional

from shapely.errors import ShapelyError
from shapely.geometry import MultiPoint, Point, Polygon
from shapely.ops import unary_union

from .client import OverpassAPIClient
from .models import PlantGeometry, create_plant_geometry
from .rejection import RejectionTracker

logger = logging.getLogger(__name__)


class GeometryHandler:
    """Handles geometry operations for OSM elements"""

    def __init__(self, client: OverpassAPIClient, rejection_tracker: RejectionTracker):
        """
        Initialize the geometry handler

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        rejection_tracker : RejectionTracker
            Tracker for rejected elements
        """
        self.client = client
        self.rejection_tracker = rejection_tracker

    def create_node_geometry(self, node: dict[str, Any]) -> Optional[PlantGeometry]:
        """
        Create a PlantGeometry for a node element.

        Parameters
        ----------
        node : dict[str, Any]
            OSM node data

        Returns
        -------
        Optional[PlantGeometry]
            PlantGeometry with Point geometry
        """
        if "lat" not in node or "lon" not in node:
            logger.debug(f"Node {node.get('id', 'unknown')} missing coordinates")
            return None

        try:
            point = Point(node["lon"], node["lat"])  # GIS standard is (lon, lat)
            return create_plant_geometry(node, point)
        except Exception as e:
            logger.debug(
                f"Error creating point geometry for node {node.get('id')}: {str(e)}"
            )
            return None

    def create_way_geometry(self, way: dict[str, Any]) -> Optional[PlantGeometry]:
        """
        Create a PlantGeometry for a way element.

        Parameters
        ----------
        way : dict[str, Any]
            OSM way data

        Returns
        -------
        Optional[PlantGeometry]
            PlantGeometry with Polygon geometry
        """
        # Check if way has nodes
        if "nodes" not in way:
            logger.debug(f"Way {way['id']} does not have nodes")
            return None

        # Get coordinates for each node
        coords = []
        for node_id in way["nodes"]:
            node = self.client.cache.get_node(node_id)
            if node and "lat" in node and "lon" in node:
                coords.append((node["lon"], node["lat"]))  # GIS standard is (lon, lat)

        if len(coords) == 1:
            # Single point - use it directly
            return create_plant_geometry(way, Point(coords[0]))
        elif len(coords) == 2:
            # Two points - use the midpoint
            mid_lon = (coords[0][0] + coords[1][0]) / 2
            mid_lat = (coords[0][1] + coords[1][1]) / 2
            return create_plant_geometry(way, Point(mid_lon, mid_lat))

        else:
            try:
                # Create polygon
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
    ) -> Optional[PlantGeometry]:
        """
        Create a PlantGeometry for a relation element.

        Parameters
        ----------
        relation : dict[str, Any]
            OSM relation data

        Returns
        -------
        Optional[PlantGeometry]
            PlantGeometry with appropriate geometry type
        """
        # Check if relation has members
        if "members" not in relation:
            logger.debug(f"Relation {relation['id']} does not have members")
            return None

        # Extract way members
        way_members = [m for m in relation["members"] if m["type"] == "way"]
        node_members = [m for m in relation["members"] if m["type"] == "node"]

        # Check if we have any way members
        if not way_members and not node_members:
            logger.debug(f"Relation {relation['id']} does not have way or node members")
            return None

        # Create polygons from ways
        polygons = []
        for way_member in way_members:
            way_id = way_member["ref"]
            way = self.client.cache.get_way(way_id)
            if way:
                way_geom = self.create_way_geometry(way)
                if way_geom and isinstance(way_geom.geometry, Polygon):
                    polygons.append(way_geom.geometry)

        # If no polygons from ways, try to create from nodes
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

        # Process polygons from ways if any
        if polygons:
            try:
                # Create union of polygons
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

    def get_element_geometry(self, element: dict[str, Any]) -> Optional[PlantGeometry]:
        """
        Get PlantGeometry for any OSM element type.

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        Returns
        -------
        Optional[PlantGeometry]
            PlantGeometry object if successful, None otherwise
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
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Extract coordinates from an OSM element.

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        Returns
        -------
        tuple[Optional[float], Optional[float]]
            (lat, lon) coordinates or (None, None) if not found
        """
        # Get the element's geometry
        plant_geometry = self.get_element_geometry(element)
        if plant_geometry:
            return plant_geometry.get_centroid()

        # Fallback for direct coordinate extraction if geometry creation failed
        if element.get("type") == "node" and "lat" in element and "lon" in element:
            return (element["lat"], element["lon"])

        # Additional fallback for ways - try to get ANY coordinate from nodes
        if element.get("type") == "way" and "nodes" in element:
            for node_id in element["nodes"]:
                node = self.client.cache.get_node(node_id)
                if node and "lat" in node and "lon" in node:
                    logger.debug(
                        f"Using first available node coordinate for way {element['id']}"
                    )
                    return (node["lat"], node["lon"])

        # Additional fallback for relations
        if element.get("type") == "relation":
            lat, lon = self.get_relation_centroid_from_members(element)
            if lat is not None and lon is not None:
                return (lat, lon)

        return (None, None)

    def get_relation_centroid_from_members(
        self, relation: dict[str, Any]
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Get centroid for a relation by examining its members

        Parameters
        ----------
        relation : dict[str, Any]
            OSM relation data

        Returns
        -------
        tuple[Optional[float], Optional[float]]
            (lat, lon) of centroid if found, (None, None) otherwise
        """
        if "members" not in relation:
            return None, None

        # Collect points from all members
        points = []

        # First look at members with capacity info (higher priority)
        capacity_member_points = []

        for member in relation["members"]:
            member_type = member["type"]
            member_id = member["ref"]

            # Get member element
            member_elem = None
            if member_type == "node":
                member_elem = self.client.cache.get_node(member_id)
            elif member_type == "way":
                member_elem = self.client.cache.get_way(member_id)
            elif member_type == "relation":
                # Skip nested relations to avoid recursion
                continue

            if not member_elem:
                continue

            # Check if member has capacity info
            has_capacity = False
            if "tags" in member_elem:
                tags = member_elem["tags"]
                for tag in ["plant:output:electricity", "generator:output:electricity"]:
                    if tag in tags:
                        has_capacity = True
                        break

            # Get coordinates
            lat, lon = None, None

            if member_type == "node" and "lat" in member_elem and "lon" in member_elem:
                lat, lon = member_elem["lat"], member_elem["lon"]
            elif member_type == "way":
                # For ways, try to get centroid
                way_geom = self.create_way_geometry(member_elem)
                if way_geom:
                    lat, lon = way_geom.get_centroid()

            if lat is not None and lon is not None:
                # Add to appropriate list
                if has_capacity:
                    capacity_member_points.append((lat, lon))
                else:
                    points.append((lat, lon))

        # Use capacity points if available
        if capacity_member_points:
            # Calculate centroid of capacity points
            lat_sum = sum(p[0] for p in capacity_member_points)
            lon_sum = sum(p[1] for p in capacity_member_points)
            return lat_sum / len(capacity_member_points), lon_sum / len(
                capacity_member_points
            )

        # Fall back to all points
        if points:
            # Calculate centroid of all points
            lat_sum = sum(p[0] for p in points)
            lon_sum = sum(p[1] for p in points)
            return lat_sum / len(points), lon_sum / len(points)

        return None, None

    def is_element_within_plant_geometries(
        self,
        element: dict[str, Any],
        plant_geometries: list[PlantGeometry],
        buffer_meters: Optional[float] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        Check if an element is within any plant geometry.

        This method handles all geometry type combinations properly:
        - Node vs Node: distance check with buffer
        - Node vs Polygon: point-in-polygon check
        - Polygon vs Polygon: intersection check

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data to check
        plant_geometries : list[PlantGeometry]
            List of plant geometries to check against
        buffer_meters : float, optional
            Buffer distance for node comparisons (default 50m)

        Returns
        -------
        tuple[bool, Optional[str]]
            (is_within, plant_id) - True and plant ID if within any geometry
        """
        # Get geometry for the element
        element_geometry = self.get_element_geometry(element)
        if not element_geometry:
            return False, None

        # Get coordinates for the element (for point checks)
        lat, lon = element_geometry.get_centroid()
        if lat is None or lon is None:
            return False, None

        # Check against each plant geometry
        for plant_geom in plant_geometries:
            try:
                # Use the PlantGeometry's contains_point method which handles all cases
                if plant_geom.contains_point(lat, lon, buffer_meters):
                    return True, f"{plant_geom.type}/{plant_geom.id}"

                # For polygon elements, also check intersection
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
        buffer_meters: Optional[float] = None,
    ) -> Optional[str]:
        """
        Check if a point is within any of the given plant geometries.

        Parameters
        ----------
        lat : float
            Latitude of the point
        lon : float
            Longitude of the point
        plant_geometries : dict[str, PlantGeometry]
            Dictionary mapping IDs to plant geometries
        buffer_meters : float, optional
            Buffer distance for node comparisons

        Returns
        -------
        str | None
            ID of the containing geometry, or None if not within any
        """
        for geom_id, plant_geom in plant_geometries.items():
            if plant_geom.contains_point(lat, lon, buffer_meters):
                return geom_id
        return None

"""
Geometry handling for OSM elements
"""

import logging
import math
from dataclasses import dataclass
from typing import Any, Optional, Union

from .client import OverpassAPIClient
from .models import PlantPolygon

logger = logging.getLogger(__name__)

try:
    from shapely.errors import ShapelyError
    from shapely.geometry import MultiPolygon, Point, Polygon
    from shapely.ops import unary_union

    SHAPELY_AVAILABLE = True
except ImportError:
    logger.warning("Shapely not installed, geometry functionality will be limited")
    SHAPELY_AVAILABLE = False

    class Point:
        pass

    class Polygon:
        pass

    class MultiPolygon:
        pass


@dataclass
class Coordinate:
    """Simple coordinate class for when shapely is not available"""

    lat: float
    lon: float


class GeometryHandler:
    """Handles geometry operations for OSM elements"""

    def __init__(self, client: OverpassAPIClient):
        """
        Initialize the geometry handler

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        """
        self.client = client

    def create_way_polygon(self, way: dict[str, Any]) -> Optional[PlantPolygon]:
        """
        Create a polygon from a way

        Parameters
        ----------
        way : dict[str, Any]
            OSM way data

        Returns
        -------
        Optional[PlantPolygon]
            Polygon if successful, None otherwise
        """
        if not SHAPELY_AVAILABLE:
            logger.warning("Shapely not available, cannot create polygon from way")
            return None

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

        # Check if we have enough coordinates
        if len(coords) < 3:
            logger.debug(
                f"Way {way['id']} does not have enough coordinates to form a polygon"
            )
            return None

        try:
            # Create polygon
            polygon = Polygon(coords)
            if not polygon.is_valid:
                logger.debug(f"Invalid polygon for way {way['id']}")
                return None

            return PlantPolygon(id=str(way["id"]), type=way["type"], geometry=polygon)
        except ShapelyError as e:
            logger.debug(f"Error creating polygon for way {way['id']}: {str(e)}")
            return None

    def create_relation_polygon(
        self, relation: dict[str, Any]
    ) -> Optional[PlantPolygon]:
        """
        Create a polygon from a relation

        Parameters
        ----------
        relation : dict[str, Any]
            OSM relation data

        Returns
        -------
        Optional[PlantPolygon]
            Polygon if successful, None otherwise
        """
        if not SHAPELY_AVAILABLE:
            logger.warning("Shapely not available, cannot create polygon from relation")
            return None

        # Check if relation has members
        if "members" not in relation:
            logger.debug(f"Relation {relation['id']} does not have members")
            return None

        # Extract way members
        way_members = [m for m in relation["members"] if m["type"] == "way"]
        if not way_members:
            logger.debug(f"Relation {relation['id']} does not have way members")
            return None

        # Create polygons from ways
        polygons = []
        for way_member in way_members:
            way_id = way_member["ref"]
            way = self.client.cache.get_way(way_id)
            if way:
                way_polygon = self.create_way_polygon(way)
                if (
                    way_polygon
                    and hasattr(way_polygon.geometry, "is_valid")
                    and way_polygon.geometry.is_valid
                ):
                    polygons.append(way_polygon.geometry)

        # Check if we have any valid polygons
        if not polygons:
            logger.debug(f"No valid polygons found for relation {relation['id']}")
            return None

        try:
            # Create union of polygons
            if len(polygons) == 1:
                union = polygons[0]
            else:
                union = unary_union(polygons)

            if not union.is_valid:
                logger.debug(f"Invalid union polygon for relation {relation['id']}")
                return None

            return PlantPolygon(
                id=str(relation["id"]), type=relation["type"], geometry=union
            )
        except ShapelyError as e:
            logger.debug(
                f"Error creating union polygon for relation {relation['id']}: {str(e)}"
            )
            return None

    def get_element_geometry(
        self, element: dict[str, Any]
    ) -> Optional[Union[PlantPolygon, Point]]:
        """
        Get geometry for an OSM element

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        Returns
        -------
        Optional[Union[PlantPolygon, Point]]
            Geometry object if successful, None otherwise
        """
        element_type = element["type"]

        if element_type == "node":
            if not SHAPELY_AVAILABLE:
                return Coordinate(element["lat"], element["lon"])
            return Point(element["lon"], element["lat"])  # GIS standard is (lon, lat)
        elif element_type == "way":
            return self.create_way_polygon(element)
        elif element_type == "relation":
            return self.create_relation_polygon(element)
        else:
            logger.warning(f"Unknown element type: {element_type}")
            return None

    def get_geometry_centroid(
        self, geometry: Union[PlantPolygon, Point, Polygon, MultiPolygon]
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Get centroid coordinates for a geometry

        Parameters
        ----------
        geometry : Union[PlantPolygon, Point, Polygon, MultiPolygon]
            Geometry object

        Returns
        -------
        tuple[Optional[float], Optional[float]]
            (lat, lon) coordinates of centroid
        """
        if not SHAPELY_AVAILABLE:
            if isinstance(geometry, Coordinate):
                return geometry.lat, geometry.lon
            logger.warning("Shapely not available, cannot calculate centroid")
            return None, None

        # Handle PlantPolygon case
        if isinstance(geometry, PlantPolygon):
            geometry = geometry.geometry

        try:
            # Get centroid
            centroid = geometry.centroid
            return centroid.y, centroid.x  # Return as (lat, lon)
        except (AttributeError, ShapelyError) as e:
            logger.debug(f"Error calculating centroid: {str(e)}")
            return None, None

    def calculate_area(self, coordinates: list[dict[str, float]]) -> float:
        """
        Calculate area of a polygon in square meters using haversine formula

        Parameters
        ----------
        coordinates : list[dict[str, float]]
            list of {lat, lon} coordinate dictionaries

        Returns
        -------
        float
            Area in square meters
        """
        if len(coordinates) < 3:
            return 0.0

        # Reference point for flat earth approximation
        ref_lat = coordinates[0]["lat"]
        ref_lon = coordinates[0]["lon"]

        # Convert coordinates to flat earth approximation
        points = []
        for coord in coordinates:
            dy = self.haversine_distance(ref_lat, ref_lon, coord["lat"], ref_lon)
            dx = self.haversine_distance(ref_lat, ref_lon, ref_lat, coord["lon"])

            # Adjust sign based on direction
            if coord["lat"] < ref_lat:
                dy = -dy
            if coord["lon"] < ref_lon:
                dx = -dx

            points.append((dx, dy))

        # Calculate area using shoelace formula
        area = 0.0
        n = len(points)
        for i in range(n):
            j = (i + 1) % n
            area += points[i][0] * points[j][1]
            area -= points[j][0] * points[i][1]
        area = abs(area) / 2.0

        return area

    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate the great circle distance between two points
        on the earth specified in decimal degrees

        Parameters
        ----------
        lat1 : float
            Latitude of first point
        lon1 : float
            Longitude of first point
        lat2 : float
            Latitude of second point
        lon2 : float
            Longitude of second point

        Returns
        -------
        float
            Distance in meters
        """
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))
        r = 6371000  # Radius of earth in meters
        return c * r

    def is_point_in_polygon(
        self, point: tuple[float, float], plant_polygon: PlantPolygon
    ) -> bool:
        """
        Check if a point is inside a polygon

        Parameters
        ----------
        point : tuple[float, float]
            Point coordinates as (lat, lon)
        plant_polygon : PlantPolygon
            Polygon to check

        Returns
        -------
        bool
            True if point is in polygon, False otherwise
        """
        if not SHAPELY_AVAILABLE:
            logger.warning("Shapely not available, cannot check if point is in polygon")
            return False

        # Create point with (lon, lat) as shapely uses (x, y)
        point_obj = Point(point[1], point[0])

        try:
            # Check if point is in polygon
            return plant_polygon.geometry.contains(point_obj)
        except ShapelyError as e:
            logger.debug(f"Error checking if point is in polygon: {str(e)}")
            return False

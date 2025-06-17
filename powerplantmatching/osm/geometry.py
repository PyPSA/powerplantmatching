import logging
from typing import Any, Optional, Union

from shapely.errors import ShapelyError
from shapely.geometry import MultiPolygon, Point, Polygon
from shapely.ops import unary_union

from .client import OverpassAPIClient
from .models import ElementType, PlantPolygon, RejectionReason
from .rejection import RejectionTracker
from .utils import get_element_coordinates

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
        Create a polygon from a relation with improved member handling

        Parameters
        ----------
        relation : dict[str, Any]
            OSM relation data

        Returns
        -------
        Optional[PlantPolygon]
            Polygon if successful, None otherwise
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
                way_polygon = self.create_way_polygon(way)
                if (
                    way_polygon
                    and hasattr(way_polygon.geometry, "is_valid")
                    and way_polygon.geometry.is_valid
                ):
                    polygons.append(way_polygon.geometry)

        # If no polygons from ways, try to create from nodes
        if not polygons and node_members:
            points = []
            for node_member in node_members:
                node_id = node_member["ref"]
                node = self.client.cache.get_node(node_id)
                if node and "lat" in node and "lon" in node:
                    points.append(Point(node["lon"], node["lat"]))

            if len(points) > 0:
                # Create a geometric representation of the node cluster
                if len(points) >= 3:
                    # With 3+ points, create convex hull as polygon
                    try:
                        from shapely.geometry import MultiPoint

                        hull = MultiPoint(points).convex_hull
                        return PlantPolygon(
                            id=str(relation["id"]), type=relation["type"], geometry=hull
                        )
                    except Exception as e:
                        logger.debug(
                            f"Error creating hull for relation {relation['id']}: {str(e)}"
                        )
                        return None
                else:
                    # With 1-2 points, just return first point as centroid marker
                    return PlantPolygon(
                        id=str(relation["id"]),
                        type=relation["type"],
                        geometry=points[0],
                    )

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

                return PlantPolygon(
                    id=str(relation["id"]), type=relation["type"], geometry=union
                )
            except ShapelyError as e:
                logger.debug(
                    f"Error creating union polygon for relation {relation['id']}: {str(e)}"
                )
                return None

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
                way_geom = self.create_way_polygon(member_elem)
                if way_geom:
                    lat, lon = self.get_geometry_centroid(way_geom)

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
        # Create point with (lon, lat) as shapely uses (x, y)
        point_obj = Point(point[1], point[0])

        try:
            # Check if point is in polygon
            return plant_polygon.geometry.contains(point_obj)
        except ShapelyError as e:
            logger.debug(f"Error checking if point is in polygon: {str(e)}")
            return False

    def check_point_within_polygons(
        self, lat: float, lon: float, polygons: dict[str, PlantPolygon]
    ) -> str | None:
        """
        Check if a point is within any of the given polygons.

        Parameters
        ----------
        lat : float
            Latitude of the point
        lon : float
            Longitude of the point
        polygons : dict[str, PlantPolygon]
            Dictionary mapping IDs to plant polygons

        Returns
        -------
        str | None
            ID of the containing polygon, or None if not within any polygon
        """
        point = Point(lon, lat)

        for polygon_id, polygon in polygons.items():
            if hasattr(polygon.geometry, "contains") and polygon.geometry.contains(
                point
            ):
                return polygon_id

        return None

    def is_element_within_any_plant(
        self, element: dict[str, Any], plant_polygons: list[PlantPolygon]
    ) -> tuple[bool, Optional[str]]:
        """
        Check if an element is within any plant polygon

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        plant_polygons : list[PlantPolygon]
            List of plant polygons to check against

        Returns
        -------
        tuple[bool, Optional[str]]
            (is_within, plant_id) - True and plant ID if within any polygon, False and None otherwise
        """
        # Get element coordinates
        lat, lon = self.process_element_coordinates(element)
        if lat is None or lon is None:
            return False, "coordinates not found"

        # Check each polygon
        for plant_polygon in plant_polygons:
            if not hasattr(plant_polygon.geometry, "contains"):
                logger.debug(
                    f"Plant polygon '{plant_polygon.type}/{plant_polygon.id}' has no geometry"
                )
                continue

            if self.is_point_in_polygon((lat, lon), plant_polygon):
                return True, f"{plant_polygon.type}/{plant_polygon.id}"

        return False, "element not within any polygon"

    def process_element_coordinates(
        self, element: dict[str, Any]
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Process an element to extract its coordinates using various fallback methods

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        category : str
            Category for rejection tracking

        Returns
        -------
        tuple[Optional[float], Optional[float]]
            (lat, lon) coordinates or (None, None) if not found
        """
        # Try to get geometry
        geometry = self.get_element_geometry(element)

        # Get coordinates from geometry
        lat, lon = None, None
        if geometry:
            lat, lon = self.get_geometry_centroid(geometry)
            return lat, lon

        # For relations with no coordinates, try special processing
        if element["type"] == "relation" and (lat is None or lon is None):
            # Try to get centroid from members
            lat, lon = self.get_relation_centroid_from_members(element)

            if lat is not None and lon is not None:
                logger.info(
                    f"Found coordinates for relation {element['id']} from members: {lat}, {lon}"
                )
            return lat, lon

        # Fall back to direct coordinates if geometry failed
        if lat is None or lon is None:
            lat, lon = get_element_coordinates(element)
            return lat, lon

        # Track rejection if coordinates not found
        if self.rejection_tracker and (lat is None or lon is None):
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.COORDINATES_NOT_FOUND,
                details="Could not determine coordinates for element",
                category="GeometryHandler:process_element_coordinates",
            )

        return None, None

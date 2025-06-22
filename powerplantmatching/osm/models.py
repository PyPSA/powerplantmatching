import json
import logging
from dataclasses import asdict, dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

import pandas as pd

# Import shapely types for proper type hints
if TYPE_CHECKING:
    from shapely.geometry import MultiPolygon, Point, Polygon

logger = logging.getLogger(__name__)

# Define literal types for OSM elements
OSMElementType = Literal["node", "way", "relation"]

# Define literal types for power plant attributes
FuelType = Literal[
    "Nuclear",
    "Solid Biomass",
    "Biogas",
    "Wind",
    "Hydro",
    "Solar",
    "Oil",
    "Natural Gas",
    "Hard Coal",
    "Lignite",
    "Geothermal",
    "Waste",
    "Other",
]

TechnologyType = Literal[
    "Steam Turbine",
    "OCGT",
    "CCGT",
    "Run-Of-River",
    "Reservoir",
    "Pumped Storage",
    "Offshore",
    "Onshore",
    "PV",
    "CSP",
    "Combustion Engine",
    "Marine",
]

SetType = Literal["PP", "CHP", "Store"]

PROCESSING_PARAMETERS = [
    "capacity_extraction",
    "capacity_estimation",
    "units_clustering",
    "source_mapping",
    "technology_mapping",
    "source_technology_mapping",
    "plants_only",
    "missing_name_allowed",
    "missing_technology_allowed",
    "missing_start_date_allowed",
    "sources",
    "units_reconstruction",
]


class RejectionReason(Enum):
    INVALID_ELEMENT_TYPE = "Invalid element type"
    COORDINATES_NOT_FOUND = "Could not determine coordinates"
    MISSING_TECHNOLOGY_TAG = "Missing technology tag"
    MISSING_TECHNOLOGY_TYPE = "Missing technology type"
    MISSING_SOURSE_TAG = "Missing source tag"
    MISSING_SOURCE_TYPE = "Missing source type"
    CAPACITY_PLACEHOLDER = "Capacity placeholder value"
    MISSING_OUTPUT_TAG = "Missing output tag"
    MISSING_NAME_TAG = "Missing name tag"
    CAPACITY_REGEX_NO_MATCH = "Capacity regex no match"
    ESTIMATION_METHOD_UNKNOWN = "Unknown estimation method"
    CAPACITY_DECIMAL_FORMAT = "Capacity decimal format"
    CAPACITY_REGEX_ERROR = "Capacity regex error"
    CAPACITY_NON_NUMERIC = "Capacity non-numeric"
    CAPACITY_UNSUPPORTED_UNIT = "Unsupported capacity unit"
    CAPACITY_ZERO = "Capacity zero"
    ELEMENT_ALREADY_PROCESSED = "Element already processed"
    WITHIN_EXISTING_PLANT = "Element within existing plant geometry"
    INVALID_START_DATE_FORMAT = "Invalid start date format"
    MISSING_START_DATE_TAG = "Missing start date tag"
    OTHER = "Other reason"


class ElementType(Enum):
    NODE = "node"
    WAY = "way"
    RELATION = "relation"


@dataclass
class Unit:
    # Using PowerPlantMatching column names directly
    projectID: str
    Country: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    type: Optional[str] = None  # Reverted to str for compatibility
    Fueltype: Optional[str] = None  # Reverted to str for compatibility
    Technology: Optional[str] = None  # Reverted to str for compatibility
    Capacity: Optional[float] = None
    Name: Optional[str] = None
    generator_count: Optional[int] = None
    Set: Optional[str] = None  # Reverted to str for compatibility
    capacity_source: Optional[str] = None
    DateIn: Optional[str] = None
    id: Optional[str] = None

    # Metadata fields for caching
    created_at: str | None = None
    config_hash: str | None = None
    config_version: str | None = None
    processing_parameters: dict | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    def is_valid_for_config(self, current_config: dict) -> bool:
        """Check if this unit is valid for the current configuration."""
        if not self.config_hash:
            return False

        # Generate hash of relevant parts of current config
        current_hash = self._generate_config_hash(current_config)
        return current_hash == self.config_hash

    @staticmethod
    def _generate_config_hash(config: dict) -> str:
        """Generate a hash from configuration parameters that affect processing."""
        import hashlib
        import json

        # Create a subset of the config with only the relevant keys
        relevant_config = {
            k: config.get(k) for k in PROCESSING_PARAMETERS if k in config
        }

        # Generate a hash
        config_str = json.dumps(relevant_config, sort_keys=True, indent=4)
        return hashlib.md5(config_str.encode()).hexdigest()


@dataclass
class PlantGeometry:
    """
    Unified geometry representation for OSM power plant elements.

    This class provides a consistent interface for spatial operations
    across different OSM element types (node, way, relation).
    """

    id: str
    type: Literal["node", "way", "relation"]  # Using literal type directly
    geometry: Union["Point", "Polygon", "MultiPolygon"]  # Shapely geometry types

    # Optional metadata
    element_data: Optional[dict[str, Any]] = None

    def contains_point(
        self, lat: float, lon: float, buffer_meters: Optional[float] = None
    ) -> bool:
        """
        Check if this geometry contains a point.

        For nodes: checks if the point is within buffer_meters (default 50m)
        For ways/relations: checks if point is within the polygon

        Parameters
        ----------
        lat : float
            Latitude of the point to check
        lon : float
            Longitude of the point to check
        buffer_meters : float, optional
            Buffer distance in meters for node comparisons (default 50m)

        Returns
        -------
        bool
            True if the point is contained within this geometry
        """
        from shapely.errors import ShapelyError
        from shapely.geometry import MultiPolygon, Point, Polygon

        point = Point(lon, lat)  # Shapely uses (x, y) = (lon, lat)

        try:
            if isinstance(self.geometry, Point):
                # For nodes, check distance
                if buffer_meters is None:
                    buffer_meters = 50.0  # Default 50m buffer

                # Convert buffer from meters to degrees (approximate)
                # At equator: 1 degree ≈ 111,320 meters
                # This is a rough approximation that varies by latitude
                buffer_degrees = buffer_meters / 111320.0

                # For more accurate distance calculation at higher latitudes
                # we adjust by the cosine of latitude
                from math import cos, radians

                if lat != 0:
                    lon_correction = abs(cos(radians(lat)))
                    # Use average of lat/lon corrections for circular buffer
                    buffer_degrees = buffer_meters / (
                        111320.0 * ((1 + lon_correction) / 2)
                    )

                distance = self.geometry.distance(point)
                return distance <= buffer_degrees

            elif isinstance(self.geometry, (Polygon, MultiPolygon)):
                # For polygons, use standard contains check
                return self.geometry.contains(point)

            else:
                logger.warning(
                    f"Unknown geometry type for element {self.id}: {type(self.geometry)}"
                )
                return False

        except ShapelyError as e:
            logger.debug(f"Error checking containment for {self.id}: {str(e)}")
            return False

    def intersects(
        self, other: "PlantGeometry", buffer_meters: Optional[float] = None
    ) -> bool:
        """
        Check if this geometry intersects with another PlantGeometry.

        Parameters
        ----------
        other : PlantGeometry
            The other geometry to check intersection with
        buffer_meters : float, optional
            Buffer distance in meters for node comparisons

        Returns
        -------
        bool
            True if the geometries intersect
        """
        from shapely.errors import ShapelyError
        from shapely.geometry import MultiPolygon, Point, Polygon

        try:
            # Handle node-to-node intersection
            if isinstance(self.geometry, Point) and isinstance(other.geometry, Point):
                if buffer_meters is None:
                    buffer_meters = 50.0
                buffer_degrees = buffer_meters / 111320.0
                distance = self.geometry.distance(other.geometry)
                return distance <= buffer_degrees

            # Handle node-to-polygon intersection
            elif isinstance(self.geometry, Point) and isinstance(
                other.geometry, (Polygon, MultiPolygon)
            ):
                if buffer_meters is None:
                    buffer_meters = 0  # No buffer for point-in-polygon
                if buffer_meters > 0:
                    buffer_degrees = buffer_meters / 111320.0
                    buffered_point = self.geometry.buffer(buffer_degrees)
                    return other.geometry.intersects(buffered_point)
                else:
                    return other.geometry.contains(self.geometry)

            # Handle polygon-to-node intersection (reverse of above)
            elif isinstance(self.geometry, (Polygon, MultiPolygon)) and isinstance(
                other.geometry, Point
            ):
                return other.intersects(self, buffer_meters)  # Delegate to reverse case

            # Handle polygon-to-polygon intersection
            else:
                return self.geometry.intersects(other.geometry)

        except ShapelyError as e:
            logger.debug(
                f"Error checking intersection between {self.id} and {other.id}: {str(e)}"
            )
            return False

    def get_centroid(self) -> tuple[float, float]:
        """
        Get the centroid coordinates of this geometry.

        Returns
        -------
        tuple[float, float]
            (lat, lon) coordinates of the centroid
        """
        from shapely.errors import ShapelyError
        from shapely.geometry import Point

        try:
            centroid = self.geometry.centroid
            return (centroid.y, centroid.x)  # Return as (lat, lon)
        except (AttributeError, ShapelyError) as e:
            # For points, return the point itself
            if isinstance(self.geometry, Point):
                return (self.geometry.y, self.geometry.x)
            logger.debug(f"Error calculating centroid for {self.id}: {str(e)}")
            return (None, None)  # type: ignore[return-value]

    def get_area_sq_meters(self) -> Optional[float]:
        """
        Get the area of this geometry in square meters.

        Returns
        -------
        float or None
            Area in square meters, or None for point geometries
        """
        from math import cos, radians

        from shapely.geometry import Point

        if isinstance(self.geometry, Point):
            return None

        try:
            # Note: This is approximate as we're using unprojected coordinates
            # For more accurate area calculation, project to a local coordinate system
            area_degrees = self.geometry.area

            # Get centroid latitude for area correction
            lat, _ = self.get_centroid()
            if lat is not None:
                # Correct for latitude distortion
                # At latitude, 1 degree longitude = 111320 * cos(lat) meters
                # 1 degree latitude = 111320 meters
                lat_correction = 111320.0
                lon_correction = 111320.0 * abs(cos(radians(lat)))
                # Use average for area calculation
                avg_correction = (lat_correction + lon_correction) / 2
                return area_degrees * (avg_correction**2)
            else:
                # Fallback to equator approximation
                return area_degrees * (111320.0**2)
        except Exception as e:
            logger.debug(f"Error calculating area for {self.id}: {str(e)}")
            return None

    def buffer(self, distance_meters: float) -> "PlantGeometry":
        """
        Create a buffered version of this geometry.

        Parameters
        ----------
        distance_meters : float
            Buffer distance in meters

        Returns
        -------
        PlantGeometry
            New PlantGeometry with buffered geometry
        """
        from shapely.errors import ShapelyError

        try:
            # Convert meters to degrees (approximate)
            buffer_degrees = distance_meters / 111320.0
            buffered_geom = self.geometry.buffer(buffer_degrees)

            return PlantGeometry(
                id=f"{self.id}_buffered",
                type=self.type,
                geometry=buffered_geom,
                element_data=self.element_data,
            )
        except ShapelyError as e:
            logger.error(f"Error buffering geometry for {self.id}: {str(e)}")
            # Return original geometry if buffering fails
            return self

    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """
        Get the bounding box of this geometry.

        Returns
        -------
        tuple[float, float, float, float]
            (min_lon, min_lat, max_lon, max_lat)
        """
        bounds = self.geometry.bounds  # (minx, miny, maxx, maxy)
        return (bounds[0], bounds[1], bounds[2], bounds[3])

    def __repr__(self) -> str:
        from shapely.geometry import MultiPolygon, Point, Polygon

        if isinstance(self.geometry, Point):
            geom_type = "Point"
        elif isinstance(self.geometry, Polygon):
            geom_type = "Polygon"
        elif isinstance(self.geometry, MultiPolygon):
            geom_type = "MultiPolygon"
        else:
            geom_type = type(self.geometry).__name__

        return f"PlantGeometry(id='{self.id}', type='{self.type}', geometry_type='{geom_type}')"


@dataclass
class RejectedPlantInfo:
    """Store info about rejected plants that might be completed from members"""

    element_id: str
    polygon: PlantGeometry
    missing_fields: dict[str, bool]  # e.g., {'name': True, 'source': False}
    member_generators: list[dict]  # Store generator members


@dataclass
class GeneratorGroup:
    """Group of generators that belong to the same rejected plant"""

    plant_id: str
    generators: list[dict]
    plant_polygon: PlantGeometry
    aggregated_name: str | None = None


class Units:
    """Collection class for managing multiple Unit objects with GeoJSON export capabilities"""

    def __init__(self, units: list[Unit] | None = None):
        """
        Initialize Units collection

        Parameters
        ----------
        units : list[Unit] | None
            Initial list of units
        """
        self.units: list[Unit] = units or []

    def add_unit(self, unit: Unit) -> None:
        """Add a single unit to the collection"""
        self.units.append(unit)

    def add_units(self, units: list[Unit]) -> None:
        """Add multiple units to the collection"""
        self.units.extend(units)

    def __len__(self) -> int:
        """Return number of units in collection"""
        return len(self.units)

    def __iter__(self):
        """Make collection iterable"""
        return iter(self.units)

    def __getitem__(self, index):
        """Allow indexing"""
        return self.units[index]

    def filter_by_country(self, country: str) -> "Units":
        """Filter units by country"""
        filtered = [unit for unit in self.units if unit.Country == country]
        return Units(filtered)

    def filter_by_fueltype(self, fueltype: str) -> "Units":
        """Filter units by fuel type"""
        filtered = [unit for unit in self.units if unit.Fueltype == fueltype]
        return Units(filtered)

    def filter_by_technology(self, technology: str) -> "Units":
        """Filter units by technology"""
        filtered = [unit for unit in self.units if unit.Technology == technology]
        return Units(filtered)

    def get_statistics(self) -> dict[str, Any]:
        """Get basic statistics about the units collection"""
        if not self.units:
            return {"total_units": 0}

        # Count valid coordinates
        units_with_coords = [
            u for u in self.units if u.lat is not None and u.lon is not None
        ]

        # Get countries
        countries = set(u.Country for u in self.units if u.Country)

        # Get fuel types
        fueltypes = set(u.Fueltype for u in self.units if u.Fueltype)

        # Get technologies
        technologies = set(u.Technology for u in self.units if u.Technology)

        # Calculate total capacity
        total_capacity = sum(u.Capacity for u in self.units if u.Capacity is not None)

        return {
            "total_units": len(self.units),
            "units_with_coordinates": len(units_with_coords),
            "coverage_percentage": round(
                len(units_with_coords) / len(self.units) * 100, 1
            ),
            "countries": sorted(list(countries)),
            "fuel_types": sorted(list(fueltypes)),
            "technologies": sorted(list(technologies)),
            "total_capacity_mw": round(total_capacity, 2) if total_capacity else 0,
            "average_capacity_mw": round(total_capacity / len(self.units), 2)
            if total_capacity and self.units
            else 0,
        }

    def generate_geojson_report(self) -> dict[str, Any]:
        """
        Generate a clean GeoJSON report of all units with coordinates

        Returns
        -------
        dict[str, Any]
            GeoJSON FeatureCollection containing power plant units with clean structure
        """
        features = []

        for unit in self.units:
            # Skip units without coordinates
            if unit.lat is None or unit.lon is None:
                continue

            # Create clean feature with only essential power plant data
            properties = {}

            # Add non-null unit properties (excluding metadata fields)
            if unit.Name:
                properties["name"] = unit.Name
            if unit.projectID:
                properties["project_id"] = unit.projectID
            if unit.Country:
                properties["country"] = unit.Country
            if unit.Fueltype:
                properties["fuel_type"] = unit.Fueltype
            if unit.Technology:
                properties["technology"] = unit.Technology
            if unit.Capacity is not None:
                properties["capacity_mw"] = unit.Capacity
            if unit.capacity_source:
                properties["capacity_source"] = unit.capacity_source
            if unit.DateIn:
                properties["date_in"] = unit.DateIn
            if unit.type:
                properties["plant_type"] = unit.type
            if unit.Set:
                properties["set_type"] = unit.Set
            if unit.id:
                properties["osm_id"] = unit.id
            if unit.generator_count is not None:
                properties["generator_count"] = unit.generator_count

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        unit.lon,
                        unit.lat,
                    ],  # GeoJSON uses [lon, lat] order
                },
                "properties": properties,
            }
            features.append(feature)

        # Create clean GeoJSON structure without nested statistics
        geojson = {"type": "FeatureCollection", "features": features}

        logger.info(
            f"Generated GeoJSON report with {len(features)} power plant features from {len(self.units)} total units"
        )
        return geojson

    def save_geojson_report(self, filepath: str) -> None:
        """
        Generate and save a GeoJSON report to file

        Parameters
        ----------
        filepath : str
            Path to save the GeoJSON file
        styled : bool, default False
            If True, include styling properties for web mapping
        """
        geojson_data = self.generate_geojson_report()

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(geojson_data, f, indent=2, ensure_ascii=False)

        stats = self.get_statistics()
        logger.info(f"Saved GeoJSON report to {filepath}")
        logger.info(
            f"Report contains {stats['units_with_coordinates']} units with coordinates out of {stats['total_units']} total units"
        )

    def to_dataframe(self) -> pd.DataFrame:
        """Convert units collection to pandas DataFrame"""

        if not self.units:
            return pd.DataFrame()

        units_dicts = [unit.to_dict() for unit in self.units]
        return pd.DataFrame(units_dicts)

    def save_csv(self, filepath: str) -> None:
        """Save units collection as CSV file"""
        df = self.to_dataframe()
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(self.units)} units to CSV: {filepath}")


def create_plant_geometry(
    element: dict[str, Any],
    geometry: Any,  # Union[Point, Polygon, MultiPolygon]
) -> PlantGeometry:
    """
    Factory function to create a PlantGeometry from an OSM element and its geometry.

    Parameters
    ----------
    element : dict[str, Any]
        OSM element data
    geometry : Any
        The shapely geometry object (Point, Polygon, or MultiPolygon)

    Returns
    -------
    PlantGeometry
        New PlantGeometry instance
    """
    return PlantGeometry(
        id=str(element.get("id", "unknown")),
        type=element.get("type", "unknown"),
        geometry=geometry,
        element_data=element,
    )

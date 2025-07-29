"""Data models for OpenStreetMap power plant processing.

This module defines the core data structures used throughout the OSM module,
including power plant units, geometries, and rejection tracking. It provides
type-safe representations of OSM elements and their attributes.

Key components:
    Unit: Individual power plant representation
    Units: Collection of power plants with filtering and export
    PlantGeometry: Spatial representation with geometric operations
    RejectionReason: Enumeration of data quality issues
"""

import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from enum import Enum
from math import cos, radians
from typing import Any, Literal, Union

import pandas as pd
from shapely.errors import ShapelyError
from shapely.geometry import MultiPolygon, Point, Polygon

logger = logging.getLogger(__name__)

# Type aliases for OSM element types
OSMElementType = Literal["node", "way", "relation"]

# Standard fuel types used in powerplantmatching
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

# Standard technology types for power generation methods
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

# Power plant set types
SetType = Literal["PP", "CHP", "Store"]

# Configuration parameters that affect processing
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
    """Enumeration of reasons why OSM elements are rejected during processing.

    Each reason represents a specific data quality issue that prevents
    an element from being included in the final dataset.
    """

    INVALID_ELEMENT_TYPE = "Invalid element type"
    COORDINATES_NOT_FOUND = "Could not determine coordinates"
    MISSING_TECHNOLOGY_TAG = "Missing technology tag"
    MISSING_TECHNOLOGY_TYPE = "Missing technology type"
    MISSING_SOURCE_TAG = "Missing source tag"
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
    """OSM element types."""

    NODE = "node"
    WAY = "way"
    RELATION = "relation"


@dataclass
class Unit:
    """Power plant unit data structure.

    Represents a single power generation unit with standardized attributes.
    This is the core data structure for power plant information, compatible
    with powerplantmatching's standard format.

    Attributes
    ----------
    projectID : str
        Unique identifier for the unit
    Country : str, optional
        Country name or code
    lat : float, optional
        Latitude coordinate
    lon : float, optional
        Longitude coordinate
    type : str, optional
        Plant type (e.g., 'plant', 'generator')
    Fueltype : str, optional
        Primary fuel type from standard list
    Technology : str, optional
        Generation technology from standard list
    Capacity : float, optional
        Electrical capacity in MW
    Name : str, optional
        Plant name
    generator_count : int, optional
        Number of generators if aggregated
    Set : str, optional
        Plant set type (PP, CHP, Store)
    capacity_source : str, optional
        How capacity was determined ('tag', 'estimated', etc.)
    DateIn : str, optional
        Commissioning date
    id : str, optional
        OSM element ID (e.g., 'node/123456')
    created_at : str, optional
        Timestamp when unit was processed
    config_hash : str, optional
        Hash of configuration used for processing
    config_version : str, optional
        Version of configuration
    processing_parameters : dict, optional
        Parameters used during processing

    Examples
    --------
    >>> unit = Unit(
    ...     projectID="OSM_node_123456",
    ...     Country="Germany",
    ...     lat=52.5200,
    ...     lon=13.4050,
    ...     Fueltype="Solar",
    ...     Technology="PV",
    ...     Capacity=10.5
    ... )
    """

    projectID: str
    Country: str | None = None
    lat: float | None = None
    lon: float | None = None
    type: str | None = None
    Fueltype: str | None = None
    Technology: str | None = None
    Capacity: float | None = None
    Name: str | None = None
    generator_count: int | None = None
    Set: str | None = None
    capacity_source: str | None = None
    DateIn: str | None = None
    id: str | None = None

    created_at: str | None = None
    config_hash: str | None = None
    config_version: str | None = None
    processing_parameters: dict | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert unit to dictionary, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    def is_valid_for_config(self, current_config: dict) -> bool:
        """Check if unit was processed with compatible configuration.

        Parameters
        ----------
        current_config : dict
            Current processing configuration

        Returns
        -------
        bool
            True if unit's config hash matches current config
        """
        if not self.config_hash:
            return False

        current_hash = self._generate_config_hash(current_config)
        return current_hash == self.config_hash

    @staticmethod
    def _generate_config_hash(config: dict) -> str:
        """Generate hash of relevant configuration parameters."""
        relevant_config = {
            k: config.get(k) for k in PROCESSING_PARAMETERS if k in config
        }

        config_str = json.dumps(relevant_config, sort_keys=True, indent=4)
        return hashlib.md5(config_str.encode()).hexdigest()


@dataclass
class PlantGeometry:
    """Spatial representation of a power plant.

    Handles geometric operations for plant boundaries, supporting
    point, polygon, and multi-polygon geometries from OSM data.

    Attributes
    ----------
    id : str
        OSM element identifier
    type : {'node', 'way', 'relation'}
        OSM element type
    geometry : shapely.geometry
        Shapely geometry object (Point, Polygon, or MultiPolygon)
    element_data : dict, optional
        Original OSM element data

    Examples
    --------
    >>> from shapely.geometry import Point
    >>> geom = PlantGeometry(
    ...     id="way/123456",
    ...     type="way",
    ...     geometry=Point(13.4050, 52.5200)
    ... )
    >>> geom.contains_point(52.5201, 13.4051, buffer_meters=100)
    True
    """

    id: str
    type: Literal["node", "way", "relation"]
    geometry: Union["Point", "Polygon", "MultiPolygon"]

    element_data: dict[str, Any] | None = None

    def contains_point(
        self, lat: float, lon: float, buffer_meters: float | None = None
    ) -> bool:
        """Check if a coordinate is within the plant geometry.

        Parameters
        ----------
        lat : float
            Latitude coordinate
        lon : float
            Longitude coordinate
        buffer_meters : float, optional
            Buffer distance in meters. Default 50m for points.

        Returns
        -------
        bool
            True if point is within geometry (with buffer if applicable)
        """
        point = Point(lon, lat)

        try:
            if isinstance(self.geometry, Point):
                if buffer_meters is None:
                    buffer_meters = 50.0

                buffer_degrees = buffer_meters / 111320.0

                if lat != 0:
                    lon_correction = abs(cos(radians(lat)))
                    buffer_degrees = buffer_meters / (
                        111320.0 * ((1 + lon_correction) / 2)
                    )

                distance = self.geometry.distance(point)
                return distance <= buffer_degrees

            elif isinstance(self.geometry, Polygon | MultiPolygon):
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
        self, other: "PlantGeometry", buffer_meters: float | None = None
    ) -> bool:
        """Check if this geometry intersects with another.

        Parameters
        ----------
        other : PlantGeometry
            Another plant geometry to check intersection with
        buffer_meters : float, optional
            Buffer distance in meters for point geometries

        Returns
        -------
        bool
            True if geometries intersect
        """
        try:
            if isinstance(self.geometry, Point) and isinstance(other.geometry, Point):
                if buffer_meters is None:
                    buffer_meters = 50.0
                buffer_degrees = buffer_meters / 111320.0
                distance = self.geometry.distance(other.geometry)
                return distance <= buffer_degrees

            elif isinstance(self.geometry, Point) and isinstance(
                other.geometry, Polygon | MultiPolygon
            ):
                if buffer_meters is None:
                    buffer_meters = 0
                if buffer_meters > 0:
                    buffer_degrees = buffer_meters / 111320.0
                    buffered_point = self.geometry.buffer(buffer_degrees)
                    return other.geometry.intersects(buffered_point)
                else:
                    return other.geometry.contains(self.geometry)

            elif isinstance(self.geometry, Polygon | MultiPolygon) and isinstance(
                other.geometry, Point
            ):
                return other.intersects(self, buffer_meters)

            else:
                return self.geometry.intersects(other.geometry)

        except ShapelyError as e:
            logger.debug(
                f"Error checking intersection between {self.id} and {other.id}: {str(e)}"
            )
            return False

    def get_centroid(self) -> tuple[float, float] | tuple[None, None]:
        """Get the centroid coordinates of the geometry.

        Returns
        -------
        tuple[float, float]
            (latitude, longitude) of centroid, or (None, None) if error
        """
        try:
            centroid = self.geometry.centroid
            return (centroid.y, centroid.x)
        except (AttributeError, ShapelyError) as e:
            if isinstance(self.geometry, Point):
                return (self.geometry.y, self.geometry.x)
            logger.debug(f"Error calculating centroid for {self.id}: {str(e)}")
            return (None, None)

    def buffer(self, distance_meters: float) -> "PlantGeometry":
        """Create a buffered version of this geometry.

        Parameters
        ----------
        distance_meters : float
            Buffer distance in meters

        Returns
        -------
        PlantGeometry
            New geometry with buffer applied
        """
        try:
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
            return self

    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """Get bounding box coordinates.

        Returns
        -------
        tuple
            (min_lon, min_lat, max_lon, max_lat)
        """
        bounds = self.geometry.bounds
        return (bounds[0], bounds[1], bounds[2], bounds[3])

    def __repr__(self) -> str:
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
    """Information about a rejected plant for potential reconstruction."""

    element_id: str
    polygon: PlantGeometry
    missing_fields: dict[str, bool]
    member_generators: list[dict]


@dataclass
class GeneratorGroup:
    """Group of generators that may form a plant."""

    plant_id: str
    generators: list[dict]
    plant_polygon: PlantGeometry
    aggregated_name: str | None = None


class Units:
    """Collection of power plant units with filtering and analysis methods.

    Provides methods for filtering, statistics, and exporting power plant
    data in various formats. Acts as a container for Unit objects with
    convenient access patterns.

    Parameters
    ----------
    units : list[Unit], optional
        Initial list of units to store

    Examples
    --------
    >>> units = Units()
    >>> units.add_unit(Unit(projectID="test1", Country="Germany"))
    >>> units.filter_by_country("Germany")
    <Units with 1 units>
    >>> stats = units.get_statistics()
    >>> units.save_csv("output.csv")
    """

    def __init__(self, units: list[Unit] | None = None):
        self.units: list[Unit] = units or []

    def add_unit(self, unit: Unit) -> None:
        """Add a single unit to the collection."""
        self.units.append(unit)

    def add_units(self, units: list[Unit]) -> None:
        """Add multiple units to the collection."""
        self.units.extend(units)

    def __len__(self) -> int:
        return len(self.units)

    def __iter__(self):
        return iter(self.units)

    def __getitem__(self, index):
        return self.units[index]

    def filter_by_country(self, country: str) -> "Units":
        """Filter units by country.

        Parameters
        ----------
        country : str
            Country name to filter by

        Returns
        -------
        Units
            New Units instance with filtered data
        """
        filtered = [unit for unit in self.units if unit.Country == country]
        return Units(filtered)

    def filter_by_fueltype(self, fueltype: str) -> "Units":
        """Filter units by fuel type.

        Parameters
        ----------
        fueltype : str
            Fuel type to filter by

        Returns
        -------
        Units
            New Units instance with filtered data
        """
        filtered = [unit for unit in self.units if unit.Fueltype == fueltype]
        return Units(filtered)

    def filter_by_technology(self, technology: str) -> "Units":
        """Filter units by technology.

        Parameters
        ----------
        technology : str
            Technology type to filter by

        Returns
        -------
        Units
            New Units instance with filtered data
        """
        filtered = [unit for unit in self.units if unit.Technology == technology]
        return Units(filtered)

    def get_statistics(self) -> dict[str, Any]:
        """Calculate summary statistics for the collection.

        Returns
        -------
        dict
            Statistics including counts, capacity totals, and coverage

        Notes
        -----
        Statistics include:
        - Total unit count
        - Units with valid coordinates
        - Coverage percentage
        - Unique countries, fuel types, and technologies
        - Total and average capacity
        """
        if not self.units:
            return {"total_units": 0}

        units_with_coords = [
            u for u in self.units if u.lat is not None and u.lon is not None
        ]

        countries = set(u.Country for u in self.units if u.Country)

        fueltypes = set(u.Fueltype for u in self.units if u.Fueltype)

        technologies = set(u.Technology for u in self.units if u.Technology)

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
        """Generate GeoJSON FeatureCollection from units.

        Creates a GeoJSON representation suitable for mapping tools.
        Only includes units with valid coordinates.

        Returns
        -------
        dict
            GeoJSON FeatureCollection with power plant features
        """
        features = []

        for unit in self.units:
            if unit.lat is None or unit.lon is None:
                continue

            properties = {}

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
                    ],
                },
                "properties": properties,
            }
            features.append(feature)

        geojson = {"type": "FeatureCollection", "features": features}

        logger.info(
            f"Generated GeoJSON report with {len(features)} power plant features from {len(self.units)} total units"
        )
        return geojson

    def save_geojson_report(self, filepath: str) -> None:
        """Save units as GeoJSON file.

        Parameters
        ----------
        filepath : str
            Path to save GeoJSON file
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
        """Convert units to pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            DataFrame with unit data, empty if no units
        """
        if not self.units:
            return pd.DataFrame()

        units_dicts = [unit.to_dict() for unit in self.units]
        return pd.DataFrame(units_dicts)

    def save_csv(self, filepath: str) -> None:
        """Save units to CSV file.

        Parameters
        ----------
        filepath : str
            Path to save CSV file
        """
        df = self.to_dataframe()
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(self.units)} units to CSV: {filepath}")


def create_plant_geometry(
    element: dict[str, Any],
    geometry: Any,
) -> PlantGeometry:
    """Factory function to create PlantGeometry from OSM element."""
    return PlantGeometry(
        id=str(element.get("id", "unknown")),
        type=element.get("type", "unknown"),
        geometry=geometry,
        element_data=element,
    )

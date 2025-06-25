import json
import logging
from dataclasses import asdict, dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

import pandas as pd

if TYPE_CHECKING:
    from shapely.geometry import MultiPolygon, Point, Polygon

logger = logging.getLogger(__name__)

OSMElementType = Literal["node", "way", "relation"]

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
    NODE = "node"
    WAY = "way"
    RELATION = "relation"


@dataclass
class Unit:
    projectID: str
    Country: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    type: Optional[str] = None
    Fueltype: Optional[str] = None
    Technology: Optional[str] = None
    Capacity: Optional[float] = None
    Name: Optional[str] = None
    generator_count: Optional[int] = None
    Set: Optional[str] = None
    capacity_source: Optional[str] = None
    DateIn: Optional[str] = None
    id: Optional[str] = None

    created_at: str | None = None
    config_hash: str | None = None
    config_version: str | None = None
    processing_parameters: dict | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    def is_valid_for_config(self, current_config: dict) -> bool:
        if not self.config_hash:
            return False

        current_hash = self._generate_config_hash(current_config)
        return current_hash == self.config_hash

    @staticmethod
    def _generate_config_hash(config: dict) -> str:
        import hashlib
        import json

        relevant_config = {
            k: config.get(k) for k in PROCESSING_PARAMETERS if k in config
        }

        config_str = json.dumps(relevant_config, sort_keys=True, indent=4)
        return hashlib.md5(config_str.encode()).hexdigest()


@dataclass
class PlantGeometry:
    id: str
    type: Literal["node", "way", "relation"]
    geometry: Union["Point", "Polygon", "MultiPolygon"]

    element_data: Optional[dict[str, Any]] = None

    def contains_point(
        self, lat: float, lon: float, buffer_meters: Optional[float] = None
    ) -> bool:
        from shapely.errors import ShapelyError
        from shapely.geometry import MultiPolygon, Point, Polygon

        point = Point(lon, lat)

        try:
            if isinstance(self.geometry, Point):
                if buffer_meters is None:
                    buffer_meters = 50.0

                buffer_degrees = buffer_meters / 111320.0

                from math import cos, radians

                if lat != 0:
                    lon_correction = abs(cos(radians(lat)))
                    buffer_degrees = buffer_meters / (
                        111320.0 * ((1 + lon_correction) / 2)
                    )

                distance = self.geometry.distance(point)
                return distance <= buffer_degrees

            elif isinstance(self.geometry, (Polygon, MultiPolygon)):
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
        from shapely.errors import ShapelyError
        from shapely.geometry import MultiPolygon, Point, Polygon

        try:
            if isinstance(self.geometry, Point) and isinstance(other.geometry, Point):
                if buffer_meters is None:
                    buffer_meters = 50.0
                buffer_degrees = buffer_meters / 111320.0
                distance = self.geometry.distance(other.geometry)
                return distance <= buffer_degrees

            elif isinstance(self.geometry, Point) and isinstance(
                other.geometry, (Polygon, MultiPolygon)
            ):
                if buffer_meters is None:
                    buffer_meters = 0
                if buffer_meters > 0:
                    buffer_degrees = buffer_meters / 111320.0
                    buffered_point = self.geometry.buffer(buffer_degrees)
                    return other.geometry.intersects(buffered_point)
                else:
                    return other.geometry.contains(self.geometry)

            elif isinstance(self.geometry, (Polygon, MultiPolygon)) and isinstance(
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

    def get_centroid(self) -> tuple[float, float]:
        from shapely.errors import ShapelyError
        from shapely.geometry import Point

        try:
            centroid = self.geometry.centroid
            return (centroid.y, centroid.x)
        except (AttributeError, ShapelyError) as e:
            if isinstance(self.geometry, Point):
                return (self.geometry.y, self.geometry.x)
            logger.debug(f"Error calculating centroid for {self.id}: {str(e)}")
            return (None, None)

    def buffer(self, distance_meters: float) -> "PlantGeometry":
        from shapely.errors import ShapelyError

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
        bounds = self.geometry.bounds
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
    element_id: str
    polygon: PlantGeometry
    missing_fields: dict[str, bool]
    member_generators: list[dict]


@dataclass
class GeneratorGroup:
    plant_id: str
    generators: list[dict]
    plant_polygon: PlantGeometry
    aggregated_name: str | None = None


class Units:
    def __init__(self, units: list[Unit] | None = None):
        self.units: list[Unit] = units or []

    def add_unit(self, unit: Unit) -> None:
        self.units.append(unit)

    def add_units(self, units: list[Unit]) -> None:
        self.units.extend(units)

    def __len__(self) -> int:
        return len(self.units)

    def __iter__(self):
        return iter(self.units)

    def __getitem__(self, index):
        return self.units[index]

    def filter_by_country(self, country: str) -> "Units":
        filtered = [unit for unit in self.units if unit.Country == country]
        return Units(filtered)

    def filter_by_fueltype(self, fueltype: str) -> "Units":
        filtered = [unit for unit in self.units if unit.Fueltype == fueltype]
        return Units(filtered)

    def filter_by_technology(self, technology: str) -> "Units":
        filtered = [unit for unit in self.units if unit.Technology == technology]
        return Units(filtered)

    def get_statistics(self) -> dict[str, Any]:
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
        geojson_data = self.generate_geojson_report()

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(geojson_data, f, indent=2, ensure_ascii=False)

        stats = self.get_statistics()
        logger.info(f"Saved GeoJSON report to {filepath}")
        logger.info(
            f"Report contains {stats['units_with_coordinates']} units with coordinates out of {stats['total_units']} total units"
        )

    def to_dataframe(self) -> pd.DataFrame:
        if not self.units:
            return pd.DataFrame()

        units_dicts = [unit.to_dict() for unit in self.units]
        return pd.DataFrame(units_dicts)

    def save_csv(self, filepath: str) -> None:
        df = self.to_dataframe()
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(self.units)} units to CSV: {filepath}")


def create_plant_geometry(
    element: dict[str, Any],
    geometry: Any,
) -> PlantGeometry:
    return PlantGeometry(
        id=str(element.get("id", "unknown")),
        type=element.get("type", "unknown"),
        geometry=geometry,
        element_data=element,
    )

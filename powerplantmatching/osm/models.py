"""
Data models for OSM power plant extraction
"""

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Optional


class RejectionReason(Enum):
    # Original reasons
    MISSING_REQUIRED_FIELD = "Missing required field"
    IN_HIGHER_HIERARCHY = "Element is part of a higher hierarchy element"
    INSIDE_PLANT_POLYGON = "Generator is inside a plant polygon"
    INVALID_GEOMETRY = "Failed to create valid geometry"
    COORDINATES_NOT_FOUND = "Could not determine coordinates"
    CAPACITY_ZERO = "Capacity is zero or negative"
    DUPLICATE_NAME = "Duplicate name"
    INVALID_ELEMENT_TYPE = "Invalid element type"

    # Capacity extraction rejections
    CAPACITY_EXTRACTION_DISABLED = "Capacity extraction from tags disabled"
    CAPACITY_VALUE_NOT_NUMERIC = "Capacity value is not a valid number"
    CAPACITY_FORMAT_UNSUPPORTED = "Capacity format is not supported"
    CAPACITY_UNIT_UNSUPPORTED = "Capacity unit is not supported"
    CAPACITY_BELOW_THRESHOLD = "Capacity is below minimum threshold"
    CAPACITY_ABOVE_THRESHOLD = "Capacity is above maximum threshold"

    # Capacity estimation rejections
    CAPACITY_ESTIMATION_DISABLED = "Capacity estimation disabled"
    ESTIMATION_METHOD_UNSUPPORTED = "Unsupported estimation method"
    ESTIMATION_MISSING_PARAMETERS = "Missing parameters for estimation"
    ESTIMATION_FAILED = "Estimation attempt failed"

    # Source rejections
    SOURCE_TYPE_MISSING = "Source type missing or unknown"

    # Other
    OTHER = "Other reason"


class ElementType(Enum):
    NODE = "node"
    WAY = "way"
    RELATION = "relation"


@dataclass
class OSMElement:
    id: str
    type: ElementType
    tags: dict[str, str]
    lat: Optional[float] = None
    lon: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Unit:
    id: str
    type: str
    source: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    capacity_mw: Optional[float] = None
    capacity_source: Optional[str] = None
    country: Optional[str] = None
    name: Optional[str] = None
    generator_count: Optional[int] = None
    case: Optional[str] = None
    technology: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class PlantPolygon:
    id: str
    type: str
    geometry: Any  # This would be a shapely Polygon in practice

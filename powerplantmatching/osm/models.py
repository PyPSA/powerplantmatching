from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Optional


class RejectionReason(Enum):
    # Required field and validation rejections
    MISSING_REQUIRED_FIELD = "Missing required field"
    INVALID_ELEMENT_TYPE = "Invalid element type"

    # Geometry-related rejections
    INVALID_GEOMETRY = "Failed to create valid geometry"
    COORDINATES_NOT_FOUND = "Could not determine coordinates"

    # Hierarchy and duplication rejections
    IN_HIGHER_HIERARCHY = "Element is part of a higher hierarchy element"
    INSIDE_PLANT_POLYGON = "Generator is inside a plant polygon"
    DUPLICATE_NAME = "Duplicate name"

    # Capacity extraction rejections - refined based on Ecuador data
    CAPACITY_TAG_MISSING = "No capacity tags found"
    CAPACITY_DECIMAL_FORMAT = "Capacity uses non-standard decimal format"
    CAPACITY_NON_NUMERIC = "Capacity value is not a valid number"
    CAPACITY_VALUE_NOT_NUMERIC = "Failed to parse capacity value"
    CAPACITY_PLACEHOLDER = "Capacity tag contains placeholder value"
    CAPACITY_UNSUPPORTED_UNIT = "Capacity unit is not supported"
    CAPACITY_FORMAT_UNSUPPORTED = "No valid advanced capacity format found"
    CAPACITY_REGEX_NO_MATCH = "Capacity regex did not match"
    CAPACITY_ZERO = "Capacity is zero or negative"
    CAPACITY_REGEX_ERROR = "Error in capacity regex parsing"

    # Source-related rejections
    SOURCE_TYPE_MISSING = "Source type missing or unknown"

    # Estimation rejections
    ESTIMATION_FAILED = "Capacity estimation failed"
    CAPACITY_ESTIMATION_DISABLED = "Capacity estimation disabled"
    ESTIMATION_MISSING_PARAMETERS = "Missing parameters for estimation"

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

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


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
    OTHER = "Other reason"


class ElementType(Enum):
    NODE = "node"
    WAY = "way"
    RELATION = "relation"


@dataclass
class Unit:
    # Using PowerPlantMatching column names directly
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
    id: str | None = None

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

        # Extract the config keys that affect processing
        relevant_keys = [
            "capacity_extraction",
            "capacity_estimation",
            "units_clustering",
            "source_mapping",
            "technology_mapping",
            "source_technology_mapping",
        ]

        # Create a subset of the config with only the relevant keys
        relevant_config = {k: config.get(k) for k in relevant_keys if k in config}

        # Generate a hash
        config_str = json.dumps(relevant_config, sort_keys=True)
        return hashlib.md5(config_str.encode()).hexdigest()


@dataclass
class PlantPolygon:
    id: str
    type: str
    geometry: Any  # This would be a shapely Polygon in practice

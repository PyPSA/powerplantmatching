"""
OpenStreetMap (OSM) power plant data extraction package.

This package provides a clean, modular implementation for extracting and processing
power plant data from OpenStreetMap via the Overpass API.

Requirements:
    requests
    pycountry
    pandas
    shapely
    scikit-learn
    numpy
"""

from .cache import ElementCache
from .client import OverpassAPIClient
from .clustering import ClusteringManager
from .estimation import CapacityEstimator
from .extractor import CapacityExtractor
from .geometry import GeometryHandler
from .interface import (
    VALID_FUELTYPES,
    VALID_TECHNOLOGIES,
    check_csv_cache,
    check_units_cache,
    process_countries,
    process_from_api,
    process_single_country,
    update_csv_cache,
    validate_and_standardize_df,
)
from .models import ElementType, PlantPolygon, RejectionReason, Unit, Units
from .rejection import RejectedElement, RejectionTracker
from .workflow import GeneratorParser, PlantParser, Workflow

__all__ = [
    "OverpassAPIClient",
    "ElementCache",
    "Unit",
    "Units",
    "PlantPolygon",
    "ElementType",
    "RejectionReason",
    "RejectedElement",
    "RejectionTracker",
    "PlantParser",
    "GeneratorParser",
    "GeometryHandler",
    "ClusteringManager",
    "CapacityEstimator",
    "CapacityExtractor",
    "Workflow",
    "process_countries",
    "process_single_country",
    "check_csv_cache",
    "check_units_cache",
    "process_from_api",
    "update_csv_cache",
    "validate_and_standardize_df",
    "VALID_FUELTYPES",
    "VALID_TECHNOLOGIES",
]

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

# Import utility functions
from .coverage import (
    find_outdated_caches,
    get_continent_mapping,
    show_country_coverage,
)
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
    validate_countries,
)
from .models import (
    ElementType,
    PlantGeometry,
    RejectionReason,
    Unit,
    Units,
    create_plant_geometry,
)
from .populate import populate_cache
from .reconstruction import (
    NameAggregator,
    PlantReconstructor,
)
from .regional import region_download
from .rejection import RejectedElement, RejectionTracker
from .unit_factory import UnitFactory
from .workflow import GeneratorParser, PlantParser, Workflow

__all__ = [
    "OverpassAPIClient",
    "ElementCache",
    "Unit",
    "Units",
    "PlantGeometry",
    "create_plant_geometry",
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
    "validate_countries",
    "validate_and_standardize_df",
    "VALID_FUELTYPES",
    "VALID_TECHNOLOGIES",
    "NameAggregator",
    "PlantReconstructor",
    "UnitFactory",
    "region_download",
    "populate_cache",
    "show_country_coverage",
    "find_outdated_caches",
    "get_continent_mapping",
]

# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
OpenStreetMap (OSM) power plant data extraction and processing.

This module provides comprehensive tools for extracting, processing, and analyzing
power plant data from OpenStreetMap. It handles the complete pipeline from raw OSM
data to clean, standardized power plant datasets.

Key Components
--------------
Data Processing:
    process_countries : Main entry point for country-level processing
    validate_countries : Country name validation with fuzzy matching
    Workflow : Orchestrates the processing pipeline

Data Models:
    Unit : Individual power plant representation
    Units : Collection of power plants with statistics
    PlantGeometry : Spatial representation of plants

Quality Control:
    RejectionTracker : Tracks why elements were rejected
    get_country_coverage_data: Analyze cache coverage
    print_coverage_report: Print formatted coverage report
    find_outdated_caches : Identify stale data

Data Retrieval:
    OverpassAPIClient : Interface to Overpass API
    ElementCache : Multi-level caching system
    region_download : Download data for custom regions

Enhancement Features:
    ClusteringManager : Group nearby generators
    CapacityEstimator : Estimate missing capacities
    PlantReconstructor : Reconstruct plants from parts

Examples
--------
Basic usage for loading OSM data:

>>> from powerplantmatching import get_config
>>> from powerplantmatching.osm import process_countries
>>> config = get_config()
>>> df = process_countries(
...     countries=['Luxembourg'],
...     csv_cache_path=config['OSM']['fn'],
...     cache_dir=config['OSM']['cache_dir'],
...     update=False,
...     osm_config=config['OSM'],
...     target_columns=config['target_columns']
... )

Check cache coverage:

>>> from powerplantmatching.osm import get_country_coverage_data, print_coverage_report
>>> data = get_country_coverage_data(show_missing=True)
>>> print_coverage_report(data)

Notes
-----
The module uses a multi-level caching strategy:
1. CSV cache - Fastest, contains processed data
2. Units cache - Pre-processed Unit objects
3. API cache - Raw API responses
4. Live API - Slowest, queries OpenStreetMap

See the tutorials in analysis/ for detailed usage examples.
"""

from .enhancement.clustering import ClusteringManager
from .enhancement.estimation import CapacityEstimator
from .enhancement.geometry import GeometryHandler
from .enhancement.reconstruction import (
    NameAggregator,
    PlantReconstructor,
)
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
from .parsing.capacity import CapacityExtractor
from .parsing.factory import UnitFactory
from .quality.coverage import (
    find_outdated_caches,
    get_continent_mapping,
    get_country_coverage_data,
    print_coverage_report,
)
from .quality.rejection import RejectedElement, RejectionTracker
from .retrieval.cache import ElementCache
from .retrieval.client import OverpassAPIClient
from .retrieval.populate import populate_cache
from .retrieval.regional import region_download
from .workflow import GeneratorParser, PlantParser, Workflow

__all__ = [
    # API Client and Cache
    "OverpassAPIClient",
    "ElementCache",
    # Data Models
    "Unit",
    "Units",
    "PlantGeometry",
    "create_plant_geometry",
    "ElementType",
    "RejectionReason",
    # Quality Control
    "RejectedElement",
    "RejectionTracker",
    # Parsers
    "PlantParser",
    "GeneratorParser",
    # Enhancement
    "GeometryHandler",
    "ClusteringManager",
    "CapacityEstimator",
    "CapacityExtractor",
    "NameAggregator",
    "PlantReconstructor",
    "UnitFactory",
    # Main Processing
    "Workflow",
    "process_countries",
    "process_single_country",
    "validate_countries",
    "validate_and_standardize_df",
    # Cache Functions
    "check_csv_cache",
    "check_units_cache",
    "process_from_api",
    "update_csv_cache",
    # Regional and Coverage
    "region_download",
    "populate_cache",
    "get_country_coverage_data",
    "print_coverage_report",
    "find_outdated_caches",
    "get_continent_mapping",
    # Constants
    "VALID_FUELTYPES",
    "VALID_TECHNOLOGIES",
]

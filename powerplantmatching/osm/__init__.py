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

"""

from .cache import ElementCache
from .client import OverpassAPIClient
from .clustering import ClusteringManager
from .estimation import EstimationManager
from .geometry import GeometryHandler
from .models import PlantPolygon, Unit
from .processors import GeneratorProcessor, PlantProcessor, Processor
from .rejection import ElementType, RejectedElement, RejectionReason
from .workflow import Workflow

__all__ = [
    "OverpassAPIClient",
    "ElementCache",
    "Unit",
    "PlantPolygon",
    "ElementType",
    "RejectionReason",
    "RejectedElement",
    "Processor",
    "PlantProcessor",
    "GeneratorProcessor",
    "GeometryHandler",
    "ClusteringManager",
    "EstimationManager",
    "Workflow",
]

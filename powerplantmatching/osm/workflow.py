"""
Integrated processor combining geometry, clustering, and estimation
"""

import logging
from typing import Any, Optional

from .client import OverpassAPIClient
from .clustering import ClusteringManager
from .estimation import EstimationManager
from .geometry import GeometryHandler
from .models import PlantPolygon, Unit
from .processors import GeneratorProcessor, PlantProcessor
from .rejection import RejectionReason, RejectionTracker

logger = logging.getLogger(__name__)


class PlantParser(PlantProcessor):
    """Unit processor with integrated features"""

    def __init__(
        self,
        client: OverpassAPIClient,
        geometry_handler: GeometryHandler,
        estimation_manager: EstimationManager,
        rejection_tracker: Optional[RejectionTracker] = None,
    ):
        """
        Initialize the integrated plant processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        geometry_handler : GeometryHandler
            Handler for geometry operations
        estimation_manager : EstimationManager
            Manager for capacity estimation
        rejection_tracker : Optional[RejectionTracker]
            Tracker for rejected elements
        """
        super().__init__(client, rejection_tracker)
        self.geometry_handler = geometry_handler
        self.estimation_manager = estimation_manager
        self.plant_polygons: list[PlantPolygon] = []

    def process_element(
        self, element: dict[str, Any], country: Optional[str] = None
    ) -> Optional[Unit]:
        """
        Process a plant element using all integrated features

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        country : Optional[str]
            Country code

        Returns
        -------
        Optional[Unit]
            Unit object if processing succeeded, None otherwise
        """
        # Check if element has power=plant tag
        tags = element.get("tags", {})
        if tags.get("power") != "plant":
            self.reject_element(
                element,
                RejectionReason.INVALID_ELEMENT_TYPE,
                f"Expected power=plant, got power={tags.get('power')}",
                category="plant",
            )
            return None

        # Try to get geometry
        geometry = self.geometry_handler.get_element_geometry(element)

        # Store polygon if applicable
        if geometry and isinstance(geometry, PlantPolygon):
            self.plant_polygons.append(geometry)

        # Get coordinates from geometry
        lat, lon = None, None
        if geometry:
            lat, lon = self.geometry_handler.get_geometry_centroid(geometry)

        # Fall back to direct coordinates if geometry failed
        if lat is None or lon is None:
            lat, lon = self.get_element_coordinates(element)

        # Check if we have valid coordinates
        if lat is None or lon is None:
            self.reject_element(
                element,
                RejectionReason.COORDINATES_NOT_FOUND,
                "Could not determine coordinates for plant",
                category="plant",
            )
            return None

        # Extract source type
        source = self.extract_source_from_tags(tags)

        # Get capacity (direct from tags or estimated)
        capacity, capacity_source = self.estimation_manager.estimate_capacity(
            element, source
        )

        # Create ID and name
        element_id = f"OSM_plant:{element['type']}_{element['id']}"
        name = tags.get("name")

        # Extract technology
        technology = self.extract_technology_from_tags(tags)

        # Create plant object
        plant = Unit(
            id=element_id,
            type=f"plant:{element['type']}",
            source=source,
            lat=lat,
            lon=lon,
            capacity_mw=capacity,
            capacity_source=capacity_source,
            country=country,
            name=name,
            case="plant",
            technology=technology,
        )

        return plant


class GeneratorParser(GeneratorProcessor):
    """Generator processor with integrated features"""

    def __init__(
        self,
        client: OverpassAPIClient,
        geometry_handler: GeometryHandler,
        estimation_manager: EstimationManager,
        rejection_tracker: Optional[RejectionTracker] = None,
    ):
        """
        Initialize the integrated generator processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        geometry_handler : GeometryHandler
            Handler for geometry operations
        estimation_manager : EstimationManager
            Manager for capacity estimation
        rejection_tracker : Optional[RejectionTracker]
            Tracker for rejected elements
        """
        super().__init__(client, rejection_tracker)
        self.geometry_handler = geometry_handler
        self.estimation_manager = estimation_manager

    def process_element(
        self,
        element: dict[str, Any],
        country: Optional[str] = None,
        plant_polygons: Optional[list[PlantPolygon]] = None,
    ) -> Optional[Unit]:
        """
        Process a generator element using integrated features

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        country : Optional[str]
            Country code
        plant_polygons : Optional[list[PlantPolygon]]
            list of plant polygons to check if generator is within any

        Returns
        -------
        Optional[Unit]
            Unit object if processing succeeded, None otherwise
        """
        # Check if element has power=generator tag
        tags = element.get("tags", {})
        if tags.get("power") != "generator":
            self.reject_element(
                element,
                RejectionReason.INVALID_ELEMENT_TYPE,
                f"Expected power=generator, got power={tags.get('power')}",
                category="generator",
            )
            return None

        # Try to get geometry
        geometry = self.geometry_handler.get_element_geometry(element)

        # Get coordinates from geometry
        lat, lon = None, None
        if geometry:
            lat, lon = self.geometry_handler.get_geometry_centroid(geometry)

        # Fall back to direct coordinates if geometry failed
        if lat is None or lon is None:
            lat, lon = self.get_element_coordinates(element)

        # Check if we have valid coordinates
        if lat is None or lon is None:
            self.reject_element(
                element,
                RejectionReason.COORDINATES_NOT_FOUND,
                "Could not determine coordinates for generator",
                category="generator",
            )
            return None

        # Check if generator is inside any plant polygon
        if plant_polygons:
            for polygon in plant_polygons:
                if self.geometry_handler.is_point_in_polygon((lat, lon), polygon):
                    self.reject_element(
                        element,
                        RejectionReason.INSIDE_PLANT_POLYGON,
                        f"Generator is inside plant polygon {polygon.id}",
                        category="generator",
                    )
                    return None

        # Extract source type
        source = self.extract_source_from_tags(tags)

        # Get capacity (direct from tags or estimated)
        capacity, capacity_source = self.estimation_manager.estimate_capacity(
            element, source
        )

        # Create ID and name
        element_id = f"OSM_generator:{element['type']}_{element['id']}"
        name = tags.get("name")

        # Extract technology
        technology = self.extract_technology_from_tags(tags)

        # Create plant object (generators are treated as individual plants)
        plant = Unit(
            id=element_id,
            type=f"generator:{element['type']}",
            source=source,
            lat=lat,
            lon=lon,
            capacity_mw=capacity,
            capacity_source=capacity_source,
            country=country,
            name=name,
            case="generator",
            technology=technology,
        )

        return plant


class Workflow:
    """
    Integrated processor combining geometry, clustering, and estimation
    """

    def __init__(
        self,
        client: OverpassAPIClient,
        config: Optional[dict[str, Any]] = None,
        rejection_tracker: Optional[RejectionTracker] = None,
    ):
        """
        Initialize the integrated processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        config : Optional[dict[str, Any]]
            Configuration for processing
        rejection_tracker : Optional[RejectionTracker]
            Tracker for rejected elements
        """
        self.client = client
        self.config = config or {}

        # Create rejection tracker
        self.rejection_tracker = rejection_tracker or RejectionTracker()

        # Create components
        self.geometry_handler = GeometryHandler(client)
        self.estimation_manager = EstimationManager(
            client, self.geometry_handler, self.config, self.rejection_tracker
        )
        self.clustering_manager = ClusteringManager(client, self.config)

        # Create processors
        self.plant_parser = PlantParser(
            client,
            self.geometry_handler,
            self.estimation_manager,
            self.rejection_tracker,
        )
        self.generator_parser = GeneratorParser(
            client,
            self.geometry_handler,
            self.estimation_manager,
            self.rejection_tracker,
        )

        # Tracking processed elements
        self.processed_elements: set[str] = set()

    def process_country_data(
        self, country: str, plants_only: bool = False
    ) -> list[Unit]:
        """
        Process OSM data for a country into Unit objects

        Parameters
        ----------
        country : str
            Country name
        plants_only : bool
            Whether to process only plants (not generators)

        Returns
        -------
        list[Unit]
            list of processed plants
        """
        # Get country data
        plants_data, generators_data = self.client.get_country_data(
            country, plants_only=plants_only
        )

        # Process plants
        plants = []
        for element in plants_data.get("elements", []):
            # Skip already processed elements
            element_id = f"{element['type']}_{element['id']}"
            if element_id in self.processed_elements:
                continue

            # Process element
            plant = self.plant_parser.process_element(element, country)
            if plant:
                plants.append(plant)
                self.processed_elements.add(element_id)

        # Process generators if requested
        if not plants_only:
            # Get plant polygons for generator filtering
            plant_polygons = self.plant_parser.plant_polygons

            # Process generators
            generators = []
            for element in generators_data.get("elements", []):
                # Skip already processed elements
                element_id = f"{element['type']}_{element['id']}"
                if element_id in self.processed_elements:
                    continue

                # Process element
                plant = self.generator_parser.process_element(
                    element, country, plant_polygons=plant_polygons
                )
                if plant:
                    generators.append(plant)
                    self.processed_elements.add(element_id)

            # Cluster generators if enabled
            if self.config.get("clustering", {}).get("enabled", False):
                # Group generators by source type
                generators_by_source = {}
                for gen in generators:
                    source = gen.source or "unknown"
                    if source not in generators_by_source:
                        generators_by_source[source] = []
                    generators_by_source[source].append(gen)

                # Cluster each source type
                clustered_generators = []
                for source, source_generators in generators_by_source.items():
                    # Skip if too few generators
                    if len(source_generators) < 2:
                        clustered_generators.extend(source_generators)
                        continue

                    # Cluster generators
                    clusters = self.clustering_manager.cluster_generators(
                        source_generators, source, self.config
                    )

                    # Create cluster plants
                    cluster_plants = self.clustering_manager.create_cluster_plants(
                        clusters, source
                    )
                    clustered_generators.extend(cluster_plants)

                generators = clustered_generators

            # Add generators to plants
            plants.extend(generators)

        return plants

    def get_rejected_summary(self) -> dict[str, dict[str, int]]:
        """
        Get a summary of rejected elements

        Returns
        -------
        dict[str, dict[str, int]]
            Summary of rejected elements by category and reason
        """
        # Get summary from centralized rejection tracker
        return self.rejection_tracker.get_summary()

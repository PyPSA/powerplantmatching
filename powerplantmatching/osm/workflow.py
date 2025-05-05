"""
Integrated processor combining geometry, clustering, and estimation
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

from .client import OverpassAPIClient
from .clustering import ClusteringManager
from .estimation import EstimationManager
from .extractor import CapacityExtractor
from .geometry import GeometryHandler
from .models import PlantPolygon, Unit
from .rejection import RejectionReason, RejectionTracker
from .utils import (
    create_rejection_entry,
    extract_source_from_tags,
    extract_technology_from_tags,
    get_element_coordinates,
    is_valid_generator,
    is_valid_plant,
)

logger = logging.getLogger(__name__)


class ElementProcessor(ABC):
    """Base class for processing OSM elements"""

    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: Optional[RejectionTracker] = None,
        config: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize the element processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        rejection_tracker : Optional[RejectionTracker]
            Tracker for rejected elements
        """
        self.client = client
        self.rejection_tracker = rejection_tracker or RejectionTracker()
        self.config = config or {}

    @abstractmethod
    def process_element(
        self, element: dict[str, Any], country: Optional[str] = None
    ) -> Optional[Unit]:
        """
        Process a single OSM element into a Unit object

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
        pass

    def reject_element(
        self,
        element: dict[str, Any],
        reason: RejectionReason,
        details: Optional[str] = None,
        category: str = "processor",
    ) -> None:
        """
        Track a rejected element

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        reason : RejectionReason
            Reason for rejection
        details : Optional[str]
            Additional details about the rejection
        category : str
            Category for grouping rejections
        """
        # Create rejection entry using the utility function
        rejection_entry = create_rejection_entry(element, reason, details, category)

        # Add to tracker
        self.rejection_tracker.add_rejection(
            element_id=rejection_entry["element_id"],
            element_type=rejection_entry["element_type"],
            reason=reason,
            details=details,
            category=category,
        )

        logger.debug(
            f"Rejected element {rejection_entry['element_id']}: {reason.value} - {details or ''}"
        )

    def get_rejection_summary(self) -> dict[str, int]:
        """
        Get a summary of element rejections by reason

        Returns
        -------
        dict[str, int]
            Count of rejected elements by reason
        """
        # Get summary from the tracker
        category_summary = self.rejection_tracker.get_summary().get("processor", {})
        return category_summary


class PlantParser(ElementProcessor):
    """Plant processor with integrated features"""

    def __init__(
        self,
        client: OverpassAPIClient,
        geometry_handler: GeometryHandler,
        capacity_extractor: CapacityExtractor,
        estimation_manager: EstimationManager,
        rejection_tracker: Optional[RejectionTracker] = None,
        config: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize the integrated plant processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        geometry_handler : GeometryHandler
            Handler for geometry operations
        capacity_extractor : CapacityExtractor
            Extractor for capacity values
        estimation_manager : EstimationManager
            Manager for capacity estimation
        rejection_tracker : Optional[RejectionTracker]
            Tracker for rejected elements
        """
        super().__init__(client, rejection_tracker, config)
        self.geometry_handler = geometry_handler
        self.capacity_extractor = capacity_extractor
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
        # Check if element is a valid plant
        if not is_valid_plant(element):
            self.reject_element(
                element,
                RejectionReason.INVALID_ELEMENT_TYPE,
                f"Expected power=plant, got power={element.get('tags', {}).get('power')}",
                category="plant",
            )
            return None

        # Get tags for later use
        tags = element.get("tags", {})

        # Try to get geometry
        geometry = self.geometry_handler.get_element_geometry(element)

        # Store polygon if applicable
        if geometry and isinstance(geometry, PlantPolygon):
            self.plant_polygons.append(geometry)

        # Get coordinates from geometry
        lat, lon = None, None
        if geometry:
            lat, lon = self.geometry_handler.get_geometry_centroid(geometry)

        # For relations with no coordinates, try special processing
        if element["type"] == "relation" and (lat is None or lon is None):
            # Try to get centroid from members
            lat, lon = self.geometry_handler.get_relation_centroid_from_members(element)

            if lat is not None and lon is not None:
                logger.info(
                    f"Found coordinates for relation {element['id']} from members: {lat}, {lon}"
                )

        # Fall back to direct coordinates if geometry failed
        if lat is None or lon is None:
            lat, lon = get_element_coordinates(element)

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
        source = extract_source_from_tags(tags)

        # Process capacity
        capacity, capacity_source = self._process_capacity(element, source)

        # For relations with no capacity, try to get from members
        if element["type"] == "relation" and capacity is None:
            capacity, capacity_source = self._get_relation_member_capacity(
                element, source
            )

        # Create ID and name
        element_id = f"OSM_plant:{element['type']}/{element['id']}"
        name = tags.get("name")

        # Extract technology
        technology = extract_technology_from_tags(tags)

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

    def _get_relation_member_capacity(
        self, relation: dict[str, Any], source_type: Optional[str]
    ) -> tuple[Optional[float], str]:
        """
        Get capacity from relation members

        Parameters
        ----------
        relation : dict[str, Any]
            Relation element data
        source_type : Optional[str]
            Source type

        Returns
        -------
        tuple[Optional[float], str]
            (capacity_mw, capacity_source)
        """
        if "members" not in relation:
            return None, "unknown"

        # Collect members with capacity information
        members_with_capacity = []

        for member in relation["members"]:
            member_type = member["type"]
            member_id = member["ref"]

            # Get member element
            member_elem = None
            if member_type == "node":
                member_elem = self.client.cache.get_node(member_id)
            elif member_type == "way":
                member_elem = self.client.cache.get_way(member_id)
            elif member_type == "relation":
                # Skip nested relations to avoid recursion
                continue

            if not member_elem or "tags" not in member_elem:
                continue

            # Try to extract capacity using the full process with source_type
            # This will try basic, advanced, and estimation as configured
            capacity, capacity_source = self._process_capacity(member_elem, source_type)

            if capacity is not None:
                members_with_capacity.append((member_elem, capacity))

        # If no members with capacity, return None
        if not members_with_capacity:
            return None, "unknown"

        # If only one member with capacity, use that
        if len(members_with_capacity) == 1:
            return members_with_capacity[0][1], "member_capacity"

        # If multiple members with capacity, sum them
        total_capacity = sum(capacity for _, capacity in members_with_capacity)
        return total_capacity, "aggregated_member_capacity"

    def _process_capacity(
        self, element: dict[str, Any], source_type: Optional[str]
    ) -> tuple[Optional[float], str]:
        """
        Process capacity using extraction and estimation

        Parameters
        ----------
        element : dict[str, Any]
            Element data
        source_type : Optional[str]
            Source type

        Returns
        -------
        tuple[Optional[float], str]
            (capacity_mw, capacity_source)
        """
        # 1. Basic extraction (always attempted)
        capacity, capacity_source = self.capacity_extractor.basic_extraction(element)

        # 2. If basic extraction fails, try advanced extraction (if enabled)
        advanced_extraction = (
            self.config.get("capacity_extraction", {})
            .get("advanced_extraction", {})
            .get("enabled", False)
        )
        if capacity is None and advanced_extraction:
            capacity, capacity_source = self.capacity_extractor.advanced_extraction(
                element, source_type
            )

        # 3. If extraction fails, try estimation (if enabled)
        capacity_estimation = self.config.get("capacity_estimation", {}).get(
            "enabled", False
        )
        if capacity is None and capacity_estimation:
            capacity, capacity_source = self.estimation_manager.estimate_capacity(
                element, source_type
            )

        return capacity, capacity_source


class GeneratorParser(ElementProcessor):
    """Generator processor with integrated features"""

    def __init__(
        self,
        client: OverpassAPIClient,
        geometry_handler: GeometryHandler,
        capacity_extractor: CapacityExtractor,
        estimation_manager: EstimationManager,
        rejection_tracker: Optional[RejectionTracker] = None,
        config: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize the integrated generator processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        geometry_handler : GeometryHandler
            Handler for geometry operations
        capacity_extractor : CapacityExtractor
            Extractor for capacity values
        estimation_manager : EstimationManager
            Manager for capacity estimation
        rejection_tracker : Optional[RejectionTracker]
            Tracker for rejected elements
        """
        super().__init__(client, rejection_tracker, config)
        self.geometry_handler = geometry_handler
        self.capacity_extractor = capacity_extractor
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
        # Check if element is a valid generator
        if not is_valid_generator(element):
            self.reject_element(
                element,
                RejectionReason.INVALID_ELEMENT_TYPE,
                f"Expected power=generator, got power={element.get('tags', {}).get('power')}",
                category="generator",
            )
            return None

        # Get tags for later use
        tags = element.get("tags", {})

        # Try to get geometry
        geometry = self.geometry_handler.get_element_geometry(element)

        # Get coordinates from geometry
        lat, lon = None, None
        if geometry:
            lat, lon = self.geometry_handler.get_geometry_centroid(geometry)

        # Fall back to direct coordinates if geometry failed
        if lat is None or lon is None:
            lat, lon = get_element_coordinates(element)

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
        source = extract_source_from_tags(tags)

        # Sequential capacity determination with clear fallbacks:

        # 1. Basic extraction (always attempted)
        capacity, capacity_source = self.capacity_extractor.basic_extraction(element)

        # 2. If basic extraction fails, try advanced extraction (if enabled)
        advanced_extraction = (
            self.config.get("capacity_extraction", {})
            .get("advanced_extraction", {})
            .get("enabled", False)
        )
        if capacity is None and advanced_extraction:
            capacity, capacity_source = self.capacity_extractor.advanced_extraction(
                element, source
            )

        # 3. If extraction fails, try estimation (if enabled)
        capacity_estimation = self.config.get("capacity_estimation", {}).get(
            "enabled", False
        )
        if capacity is None and capacity_estimation:
            capacity, capacity_source = self.estimation_manager.estimate_capacity(
                element, source
            )

        # Create ID and name
        element_id = f"OSM_generator:{element['type']}/{element['id']}"
        name = tags.get("name")

        # Extract technology
        technology = extract_technology_from_tags(tags)

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
        self.capacity_extractor = CapacityExtractor(self.config, self.rejection_tracker)
        self.estimation_manager = EstimationManager(
            client, self.geometry_handler, self.config, self.rejection_tracker
        )
        self.clustering_manager = ClusteringManager(client, self.config)

        # Create processors
        self.plant_parser = PlantParser(
            client,
            self.geometry_handler,
            self.capacity_extractor,
            self.estimation_manager,
            self.rejection_tracker,
            self.config,
        )
        self.generator_parser = GeneratorParser(
            client,
            self.geometry_handler,
            self.capacity_extractor,
            self.estimation_manager,
            self.rejection_tracker,
            self.config,
        )

        # Tracking processed elements
        self.processed_elements: set[str] = set()

    def get_valid_units(self) -> list[Unit]:
        """
        Get list of valid (non-rejected) units

        Returns
        -------
        list[Unit]
            List of units that were not rejected
        """
        # Get all processed plants and generators
        all_units = []

        # Add all plants and generators from both parsers
        for plant in getattr(self, "processed_plants", []):
            all_units.append(plant)
        for generator in getattr(self, "processed_generators", []):
            all_units.append(generator)

        # Get all rejected element IDs (need to massage them to match Unit.id format)
        rejected_ids = set()
        for rejection in self.rejection_tracker.get_all_rejections():
            # Most common format is "type/id"
            raw_id = rejection.element_id
            if "/" in raw_id:
                elem_type, elem_id = raw_id.split("/", 1)
                # Convert to Unit.id format
                if elem_type == "node":
                    rejected_ids.add(f"OSM_plant:node/{elem_id}")
                    rejected_ids.add(f"OSM_generator:node/{elem_id}")
                elif elem_type == "way":
                    rejected_ids.add(f"OSM_plant:way/{elem_id}")
                    rejected_ids.add(f"OSM_generator:way/{elem_id}")
                elif elem_type == "relation":
                    rejected_ids.add(f"OSM_plant:relation/{elem_id}")
                    rejected_ids.add(f"OSM_generator:relation/{elem_id}")
            else:
                # Just add the raw ID in case format is different
                rejected_ids.add(raw_id)

        # Filter out rejected units
        valid_units = [unit for unit in all_units if unit.id not in rejected_ids]

        return valid_units

    def process_country_data(
        self,
        country: str,
        export_rejections: bool = True,
        rejections_file: Optional[str] = None,
    ) -> tuple[list[Unit], dict[str, dict[str, int]]]:
        """
        Process OSM data for a country

        Parameters
        ----------
        country : str
            Country name
        export_rejections : bool
            Whether to export rejections to CSV
        rejections_file : Optional[str]
            File path for rejections CSV

        Returns
        -------
        tuple[list[Unit], dict[str, dict[str, int]]]
            (list of valid units, rejection summary)
        """
        plants_only = self.config.get("plants_only", True)
        # Get country data
        plants_data, generators_data = self.client.get_country_data(
            country, plants_only=plants_only
        )

        # Initialize tracking attributes
        self.processed_plants = []
        self.processed_generators = []
        self.processed_elements = set()

        # Process plants
        for element in plants_data.get("elements", []):
            # Skip already processed elements
            element_id = f"{element['type']}/{element['id']}"
            if element_id in self.processed_elements:
                continue

            # Process element
            plant = self.plant_parser.process_element(element, country)
            if plant:
                self.processed_plants.append(plant)
                self.processed_elements.add(element_id)

            logger.debug(f"Processed plant element {element_id}")

        # Process generators if requested
        if not plants_only:
            # Get plant polygons for generator filtering
            plant_polygons = self.plant_parser.plant_polygons

            # Process generators
            for element in generators_data.get("elements", []):
                # Skip already processed elements
                element_id = f"{element['type']}/{element['id']}"
                if element_id in self.processed_elements:
                    continue

                # Process element
                generator = self.generator_parser.process_element(
                    element, country, plant_polygons=plant_polygons
                )
                if generator:
                    self.processed_generators.append(generator)
                    self.processed_elements.add(element_id)

                logger.debug(f"Processed generator element {element_id}")

            # Cluster generators if enabled
            if self.config.get("units_clustering", {}).get("enabled", False):
                # Group generators by source type
                generators_by_source: dict[str, list[Unit]] = {}
                for gen in self.processed_generators:
                    source = gen.source or "unknown"
                    if source not in generators_by_source:
                        generators_by_source[source] = []
                    generators_by_source[source].append(gen)

                # Reset processed generators list
                self.processed_generators = []

                # Cluster each source type
                for source, source_generators in generators_by_source.items():
                    # Skip if too few generators
                    if len(source_generators) < 2:
                        self.processed_generators.extend(source_generators)
                        continue

                    # Cluster generators
                    success, clusters = self.clustering_manager.cluster_generators(
                        source_generators, source
                    )
                    if success:
                        logger.info(
                            f"Clustering successful for {len(source_generators)} generators of type {source}"
                        )

                        # Create cluster plants
                        cluster_plants = self.clustering_manager.create_cluster_plants(
                            clusters, source
                        )
                        self.processed_generators.extend(cluster_plants)
                    else:
                        logger.warning(
                            f"Clustering failed for {len(source_generators)} generators of type {source}"
                        )
                        self.processed_generators.extend(source_generators)

        # Get only valid (non-rejected) units
        valid_units = self.get_valid_units()

        # Create rejection summary
        rejection_summary = self.get_rejected_summary()

        # Export rejections if requested
        if export_rejections:
            if rejections_file is None:
                # Use default file name
                rejections_file = f"{country.lower()}_rejections.csv"

            # Export to CSV
            self._export_rejections_to_csv(rejections_file)

        return valid_units, rejection_summary

    def _export_rejections_to_csv(self, file_path: str) -> None:
        """
        Export rejection data to CSV

        Parameters
        ----------
        file_path : str
            Path to CSV file
        """
        with open(file_path, "w") as f:
            # Write header
            f.write("element_id,element_type,reason,details,timestamp,category\n")

            # Write each rejection
            for rejection in self.rejection_tracker.get_all_rejections():
                element_type = rejection.element_type.value
                reason = rejection.reason.value
                details = rejection.details or ""
                timestamp = rejection.timestamp

                # Find category
                category = ""
                for cat, rejections in self.rejection_tracker.categories.items():
                    if rejection in rejections:
                        category = cat
                        break

                # Write row
                f.write(
                    f"{rejection.element_id},{element_type},{reason},{details},{timestamp},{category}\n"
                )

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

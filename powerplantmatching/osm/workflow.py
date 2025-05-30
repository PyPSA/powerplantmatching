"""
Integrated processor combining geometry, clustering, and estimation
"""

import datetime
import logging
from abc import ABC, abstractmethod
from typing import Any

from .client import OverpassAPIClient
from .clustering import ClusteringManager
from .estimation import CapacityEstimator
from .extractor import CapacityExtractor
from .geometry import GeometryHandler
from .models import PROCESSING_PARAMETERS, ElementType, PlantPolygon, Unit, Units
from .rejection import RejectionReason, RejectionTracker
from .utils import get_country_code, is_valid_unit

logger = logging.getLogger(__name__)


class ElementProcessor(ABC):
    """Base class for processing OSM elements"""

    def __init__(
        self,
        client: OverpassAPIClient,
        geometry_handler: GeometryHandler,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any] | None = None,
    ):
        """
        Initialize the element processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        rejection_tracker : RejectionTracker
            Tracker for rejected elements
        """
        self.client = client
        self.config = config or {}
        self.rejection_tracker = rejection_tracker
        self.capacity_extractor = CapacityExtractor(
            self.rejection_tracker,
            self.config,
        )
        self.capacity_estimator = CapacityEstimator(
            self.client, self.rejection_tracker, self.config
        )
        self.geometry_handler = geometry_handler

    def extract_name_from_tags(
        self, element: dict[str, Any], unit_type: str
    ) -> str | None:
        """
        Extract name from OSM tags

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        Returns
        -------
        str | None
            Name if found, None otherwise
        """
        assert unit_type in ["plant", "generator"], "Invalid unit type"

        tags = element.get("tags", {})

        unit_type_tags_keys = {
            "plant": "plant_tags",
            "generator": "generator_tags",
        }

        default_name = {
            "plant_tags": ["name:en", "name"],
            "generator_tags": ["name:en", "name"],
        }

        name_keys = self.config.get(unit_type_tags_keys[unit_type], {}).get(
            "name_tags_keys", default_name[unit_type_tags_keys[unit_type]]
        )

        # Check if name is in tags
        name = None
        for key in name_keys:
            if key in tags:
                name = tags[key]
                if name:
                    return name

        missing_name_allowed = self.config.get("missing_name_allowed", False)
        if missing_name_allowed:
            return ""

        # TODO: Perform a research to find the name

        if not name:
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.MISSING_NAME_TAG,
                details=f"tags: {tags}",
                category=f"{'PlantParser' if unit_type == 'plant' else 'GeneratorParser'}:extract_name_from_tags",
            )

        return None

    def extract_source_from_tags(
        self, element: dict[str, Any], unit_type: str
    ) -> str | None:
        """
        Extract power source from OSM tags

        Parameters
        ----------
        tags : dict[str, str]
            OSM element tags

        Returns
        -------
        str | None
            Power source if found, None otherwise
        """
        assert unit_type in ["plant", "generator"], "Invalid unit type"

        tags = element.get("tags", {})

        unit_type_tags_keys = {
            "plant": "plant_tags",
            "generator": "generator_tags",
        }

        default_source = {
            "plant_tags": ["plant:source"],
            "generator_tags": ["generator:source"],
        }

        source_keys = self.config.get(unit_type_tags_keys[unit_type], {}).get(
            "source_tags_keys", default_source[unit_type_tags_keys[unit_type]]
        )
        source_mapping = self.config.get("source_mapping", {})

        store_element_source = ""

        for key in source_keys:
            if key in tags:
                element_source = tags[key].lower()
                store_element_source = element_source
                for config_source in source_mapping:
                    if element_source in source_mapping[config_source]:
                        return config_source

        if not store_element_source:
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.MISSING_SOURSE_TAG,
                details=f"tags: {tags}",
                category=f"{'PlantParser' if unit_type == 'plant' else 'GeneratorParser'}:extract_source_from_tags",
            )
        else:
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.MISSING_SOURCE_TYPE,
                details=f"""'{store_element_source}' not found in source_mapping""",
                category=f"{'PlantParser' if unit_type == 'plant' else 'GeneratorParser'}:extract_source_from_tags",
            )

        return None

    def extract_technology_from_tags(
        self, element: dict[str, Any], unit_type: str, source_type: str
    ) -> str | None:
        """
        Extract technology information from OSM tags

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        unit_type : str
            Type of unit ("plant" or "generator")

        source_type : str
            Type of power source

        Returns
        -------
        str | None
            Technology if found, None otherwise
        """
        assert unit_type in ["plant", "generator"], "Invalid unit type"

        tags = element.get("tags", {})

        unit_type_tags_keys = {
            "plant": "plant_tags",
            "generator": "generator_tags",
        }
        default_technology = {
            "plant_tags": ["plant:method", "plant:type"],
            "generator_tags": ["generator:method", "generator:type"],
        }

        technology_keys = self.config.get(unit_type_tags_keys[unit_type], {}).get(
            "technology_tags_keys", default_technology[unit_type_tags_keys[unit_type]]
        )
        technology_mapping = self.config.get("technology_mapping", {})
        source_tech_mapping = self.config.get("source_technology_mapping", {})

        store_element_technology = ""

        for key in technology_keys:
            if key in tags:
                element_technology = tags[key].lower()
                store_element_technology = element_technology
                for config_technology in technology_mapping:
                    if config_technology in source_tech_mapping[source_type]:
                        if element_technology in technology_mapping[config_technology]:
                            return config_technology

        missing_technology_allowed = self.config.get(
            "missing_technology_allowed", False
        )
        if missing_technology_allowed:
            return ""

        if not store_element_technology:
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.MISSING_TECHNOLOGY_TAG,
                details=f"tags: {tags}",
                category=f"{'PlantParser' if unit_type == 'plant' else 'GeneratorParser'}:extract_technology_from_tags",
            )
        else:
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.MISSING_TECHNOLOGY_TYPE,
                details=f'''"{store_element_technology}" not found in technology_mapping. Ensure updating source_technology_mapping with "{source_type}"''',
                category=f"{'PlantParser' if unit_type == 'plant' else 'GeneratorParser'}:extract_technology_from_tags",
            )

        return None

    def extract_output_key_from_tags(
        self, element: dict[str, Any], unit_type: str, source_type: str | None = None
    ) -> str | None:
        """
        Extract output electricity information from OSM tags

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        unit_type : str
            Type of unit ("plant" or "generator")

        Returns
        -------
        str | None
            Output if found, None otherwise
        """
        assert unit_type in ["plant", "generator"], "Invalid unit type"

        tags = element.get("tags", {})

        unit_type_tags_keys = {
            "plant": "plant_tags",
            "generator": "generator_tags",
        }

        default_output = {
            "plant_tags": ["plant:output:electricity"],
            "generator_tags": ["generator:output:electricity"],
        }

        output_keys = self.config.get(unit_type_tags_keys[unit_type], {}).get(
            "output_tags_keys", default_output[unit_type_tags_keys[unit_type]]
        )
        if source_type:
            source_output_keys = (
                self.config.get("sources", {})
                .get(source_type, {})
                .get("capacity_extraction", {})
                .get("additional_tags", [])
            )
            output_keys.extend(source_output_keys)

        for key in output_keys:
            if key in tags:
                return key

        self.rejection_tracker.add_rejection(
            element_id=element["id"],
            element_type=ElementType(element["type"]),
            reason=RejectionReason.MISSING_OUTPUT_TAG,
            details=f"tags: {tags}",
            category=f"{'PlantParser' if unit_type == 'plant' else 'GeneratorParser'}:extract_output_key_from_tags",
        )
        return None

    def extract_start_date_key_from_tags(
        self, element: dict[str, Any], unit_type: str
    ) -> str | None:
        """
        Extract start date information from OSM tags

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        unit_type : str
            Type of unit ("plant" or "generator")

        Returns
        -------
        str | None
            Start date if found, None otherwise
        """
        assert unit_type in ["plant", "generator"], "Invalid unit type"

        tags = element.get("tags", {})

        unit_type_tags_keys = {
            "plant": "plant_tags",
            "generator": "generator_tags",
        }

        default_start_date = {
            "plant_tags": ["start_date", "year"],
            "generator_tags": ["start_date", "year"],
        }

        start_date_keys = self.config.get(unit_type_tags_keys[unit_type], {}).get(
            "start_date_tags_keys", default_start_date[unit_type_tags_keys[unit_type]]
        )

        store_raw_date = ""
        for key in start_date_keys:
            if key in tags:
                if isinstance(tags[key], str):
                    date_string = tags[key].strip()
                elif isinstance(tags[key], int | float):
                    date_string = str(int(tags[key]))
                else:
                    date_string = str(tags[key])
                store_raw_date = date_string
                datum = self._parse_date_string(date_string)
                return datum

        missing_start_date_allowed = self.config.get(
            "missing_start_date_allowed", False
        )
        if missing_start_date_allowed:
            return ""

        if not store_raw_date:
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.MISSING_START_DATE_TAG,
                details=f"tags: {tags}",
                category=f"{'PlantParser' if unit_type == 'plant' else 'GeneratorParser'}:extract_start_date_key_from_tags",
            )
        else:
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.INVALID_START_DATE_FORMAT,
                details=f"tags: {tags.keys()} - '{key}':'{store_raw_date}'",
                category=f"{'PlantParser' if unit_type == 'plant' else 'GeneratorParser'}:extract_start_date_key_from_tags",
            )

        return None

    def _parse_date_string(self, date_string: str) -> str:
        """
        Parse date string from OSM tags into a standardized format.
        Handles various date formats including incomplete dates.

        Parameters
        ----------
        date_string : str
            Date string from OSM tags

        Returns
        -------
        str
            Standardized date string in ISO format (YYYY-MM-DD)
        """
        import re

        from dateutil import parser

        # Strip whitespace and handle empty strings
        if not date_string or not date_string.strip():
            raise ValueError("Empty date string")

        date_string = date_string.strip()

        # Try to extract year from the string (for fallback)
        year_match = re.search(r"\b(19\d{2}|20\d{2})\b", date_string)
        if not year_match:
            raise ValueError(f"No valid year found in '{date_string}'")

        year = int(year_match.group(1))

        try:
            # Try to parse the date string
            date_obj = parser.parse(date_string, fuzzy=True)

            # Return full date if it was fully specified
            if date_string.count(str(date_obj.year)) > 0:
                # Check if month and day were part of the original string or defaults
                month_detected = any(
                    m in date_string.lower()
                    for m in [
                        "jan",
                        "feb",
                        "mar",
                        "apr",
                        "may",
                        "jun",
                        "jul",
                        "aug",
                        "sep",
                        "oct",
                        "nov",
                        "dec",
                    ]
                ) or re.search(r"\b\d{1,2}[/.-]\d{1,2}\b", date_string)

                day_detected = re.search(r"\b\d{1,2}\b", date_string) and month_detected

                if month_detected and day_detected:
                    return date_obj.strftime("%Y-%m-%d")
                elif month_detected:
                    return f"{date_obj.year}-{date_obj.month:02d}-01"
                else:
                    return f"{date_obj.year}-01-01"

            # Fallback to just the year
            return f"{year}-01-01"

        except (ValueError, TypeError):
            # If parsing failed, fallback to just using the year
            return f"{year}-01-01"

    def setup_element_coordinates(
        self, element: dict[str, Any]
    ) -> tuple[float | None, float | None]:
        """
        Extract and store coordinates for an element early in processing.
        This ensures coordinates are available for all subsequent rejections.

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        Returns
        -------
        tuple[float | None, float | None]
            (lat, lon) coordinates
        """
        element_type = ElementType(element["type"])
        element_id = element["id"]

        lat, lon = self.geometry_handler.process_element_coordinates(element)

        # Store coordinates in rejection tracker for future use
        if lat is not None and lon is not None:
            self.rejection_tracker.set_element_coordinates(
                element_id, element_type, (lat, lon)
            )

        return lat, lon

    def setup_element_context(
        self, element: dict[str, Any], country: str | None = None
    ) -> tuple[float | None, float | None]:
        """
        Extract and store coordinates and country for an element early in processing.
        This ensures context is available for all subsequent rejections.

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        country : str | None
            Country where the element is located

        Returns
        -------
        tuple[float | None, float | None]
            (lat, lon) coordinates
        """
        element_type = ElementType(element["type"])
        element_id = element["id"]

        lat, lon = self.geometry_handler.process_element_coordinates(element)

        # Store coordinates and country in rejection tracker for future use
        if lat is not None and lon is not None:
            self.rejection_tracker.set_element_coordinates(
                element_id, element_type, (lat, lon)
            )

        if country is not None:
            self.rejection_tracker.set_element_country(
                element_id, element_type, country
            )

        return lat, lon

    @abstractmethod
    def process_element(
        self, element: dict[str, Any], country: str | None = None
    ) -> Unit | None:
        """
        Process a single OSM element into a Unit object

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        country : str | None
            Country code

        Returns
        -------
        Unit | None
            Unit object if processing succeeded, None otherwise
        """
        pass

    def _process_capacity(
        self,
        element: dict[str, Any],
        source_type: str | None,
        output_key: str,
        unit_type: str,
    ) -> tuple[float | None, str]:
        """
        Process capacity using extraction and estimation

        Parameters
        ----------
        element : dict[str, Any]
            Element data
        source_type : str | None
            Source type

        Returns
        -------
        tuple[float | None, str]
            (capacity_mw, info)
        """
        assert unit_type in ["plant", "generator"], "Invalid unit type"
        # 1. Basic extraction (always attempted)
        is_valid, capacity, info = self.capacity_extractor.basic_extraction(
            element, output_key
        )

        # 2. If basic extraction fails, try advanced extraction (if enabled)
        advanced_extraction_enabled = self.config.get("capacity_extraction", {}).get(
            "enabled", False
        )

        if not is_valid and advanced_extraction_enabled and info != "placeholder_value":
            is_valid, capacity, info = self.capacity_extractor.advanced_extraction(
                element, output_key
            )

        # 3. If extraction fails, try estimation (if enabled)
        capacity_estimation_enabled = self.config.get("capacity_estimation", {}).get(
            "enabled", False
        )
        if not is_valid and capacity_estimation_enabled:
            capacity, info = self.capacity_estimator.estimate_capacity(
                element, source_type, unit_type=unit_type
            )

        return capacity, info

    def _get_relation_member_capacity(
        self, relation: dict[str, Any], source_type: str | None, unit_type: str
    ) -> tuple[float | None, str]:
        """
        Get capacity from relation members

        Parameters
        ----------
        relation : dict[str, Any]
            Relation element data
        source_type : str | None
            Source type

        Returns
        -------
        tuple[float | None, str]
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

            has_power_tag = False
            has_output_tag = False
            store_keys = []
            for key in member_elem["tags"]:
                if key.startswith("power:"):
                    has_power_tag = True
                if "output" in key:
                    has_output_tag = True
                    store_keys.append(key)
            if not has_power_tag and not has_output_tag:
                continue
            else:
                if len(store_keys) == 1:
                    output_key = store_keys[0]
                else:
                    for key in store_keys:
                        if "output:electricity" in key:
                            output_key = key
                            break

            capacity, _ = self._process_capacity(
                member_elem, source_type, output_key, unit_type
            )

            if capacity is not None:
                members_with_capacity.append((member_elem, capacity))
            else:
                # estimation if allowed
                estimation_enabled = self.config.get("capacity_estimation", {}).get(
                    "enabled", False
                )
                if estimation_enabled:
                    capacity, _ = self.capacity_estimator.estimate_capacity(
                        member_elem, source_type, unit_type
                    )
                    if capacity is not None:
                        members_with_capacity.append((member_elem, capacity))
                    else:
                        continue
                else:
                    continue

        # If no members with capacity, return None
        if not members_with_capacity:
            return None, "unknown"

        # If only one member with capacity, use that
        if len(members_with_capacity) == 1:
            return members_with_capacity[0][1], "member_capacity"

        # If multiple members with capacity, sum them
        total_capacity = sum(capacity for _, capacity in members_with_capacity)
        return total_capacity, "aggregated_capacity"


class PlantParser(ElementProcessor):
    """Plant processor with integrated features"""

    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any] | None = None,
    ):
        """
        Initialize the integrated plant processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        rejection_tracker : RejectionTracker
            Tracker for rejected elements
        config : dict[str, Any] | None
            Configuration for processing
        """
        super().__init__(
            client,
            GeometryHandler(client, rejection_tracker),
            rejection_tracker,
            config,
        )
        self.plant_polygons: list[PlantPolygon] = []

    def process_element(
        self, element: dict[str, Any], country: str | None = None
    ) -> Unit | None:
        """
        Process a plant element using all integrated features

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        country : str | None
            Country code

        Returns
        -------
        Unit | None
            Unit object if processing succeeded, None otherwise
        """
        # EARLY COORDINATE EXTRACTION - Get coordinates first for rejection tracking
        lat, lon = self.setup_element_context(element, country)

        # Check if element is a valid plant
        if not is_valid_unit(element, "plant"):
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.INVALID_ELEMENT_TYPE,
                details=f"Expected power=plant, got power={element.get('tags', {}).get('power')}",
                category="PlantParser:process_element",
            )
            return None

        # Extract source type
        source = self.extract_source_from_tags(element, "plant")
        if source is None:
            return None

        # Extract technology
        technology = self.extract_technology_from_tags(element, "plant", source)
        if technology is None:
            return None

        # Extract name
        name = self.extract_name_from_tags(element, "plant")
        if name is None:
            return None

        # Extract output key
        output_key = self.extract_output_key_from_tags(element, "plant", source)
        if output_key is None:
            return None

        start_date = self.extract_start_date_key_from_tags(element, "plant")
        if start_date is None:
            return None

        # Get geometry
        geometry = self.geometry_handler.get_element_geometry(element)

        # Store polygon if applicable
        if geometry and isinstance(geometry, PlantPolygon):
            self.plant_polygons.append(geometry)

        # Check if we have valid coordinates (coordinates were extracted earlier)
        if lat is None or lon is None:
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.COORDINATES_NOT_FOUND,
                details="Could not determine coordinates for element",
                category="PlantParser:process_element",
            )
            return None

        # Process capacity
        capacity, info = self._process_capacity(element, source, output_key, "plant")
        if capacity is None:
            if element["type"] == "relation":
                # If relation, try to get capacity from members
                capacity, info = self._get_relation_member_capacity(
                    element, source, "plant"
                )
                if capacity is None:
                    return None
            else:
                return None

        # Clear coordinates from context (cleanup)
        self.rejection_tracker.clear_element_coordinates(
            element["id"], ElementType(element["type"])
        )

        unit = Unit(
            projectID=f"OSM_plant:{element['type']}/{element['id']}",
            type=f"plant:{element['type']}",
            Fueltype=source,
            lat=lat,
            lon=lon,
            Capacity=capacity,
            capacity_source=info,
            Country=country,
            Name=name,
            Set="PP",
            Technology=technology,
            DateIn=start_date,
            id=f"{element['type']}/{element['id']}",
        )

        return unit


class GeneratorParser(ElementProcessor):
    """Generator processor with integrated features"""

    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any] | None = None,
    ):
        """
        Initialize the integrated generator processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        rejection_tracker : RejectionTracker
            Tracker for rejected elements
        """
        super().__init__(
            client,
            GeometryHandler(client, rejection_tracker),
            rejection_tracker,
            config,
        )

    def process_element(
        self,
        element: dict[str, Any],
        country: str | None = None,
        plant_polygons: list[PlantPolygon] | None = None,
    ) -> Unit | None:
        """
        Process a generator element using integrated features

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        country : str | None
            Country code
        plant_polygons : list[PlantPolygon] | None
            list of plant polygons to check if generator is within any

        Returns
        -------
        Unit | None
            Unit object if processing succeeded, None otherwise
        """
        # EARLY COORDINATE EXTRACTION - Get coordinates first for rejection tracking
        lat, lon = self.setup_element_context(element, country)

        # Check if element is a valid generator
        if not is_valid_unit(element, "generator"):
            # Reject invalid elements (no tags or invalid power type)
            self.rejection_tracker.add_rejection(
                element_id=element["id"],
                element_type=ElementType(element["type"]),
                reason=RejectionReason.INVALID_ELEMENT_TYPE,
                details=f"Expected power=generator, got power={element.get('tags', {}).get('power')}",
                category="GeneratorParser:process_element",
            )
            return None

        # Extract source type
        source = self.extract_source_from_tags(element, "generator")
        if source is None:
            return None

        # Extract technology
        technology = self.extract_technology_from_tags(element, "generator", source)
        if technology is None:
            return None

        # Extract name
        name = self.extract_name_from_tags(element, "generator")
        if name is None:
            return None

        # Extract output key
        output_key = self.extract_output_key_from_tags(element, "generator", source)
        if output_key is None:
            return None

        start_date = self.extract_start_date_key_from_tags(element, "generator")
        if start_date is None:
            return None

        # Process capacity
        capacity, info = self._process_capacity(
            element, source, output_key, "generator"
        )
        if capacity is None:
            if element["type"] == "relation":
                # If relation, try to get capacity from members
                capacity, info = self._get_relation_member_capacity(
                    element, source, "generator"
                )
                if capacity is None:
                    return None
            else:
                return None

        # Clear coordinates from context (cleanup)
        self.rejection_tracker.clear_element_coordinates(
            element["id"], ElementType(element["type"])
        )

        unit = Unit(
            projectID=f"OSM_generator:{element['type']}/{element['id']}",
            type=f"generator:{element['type']}",
            Fueltype=source,
            lat=lat,
            lon=lon,
            Capacity=capacity,
            capacity_source=info,
            Country=country,
            Name=name,
            Set="PP",
            Technology=technology,
            DateIn=start_date,
            id=f"{element['type']}/{element['id']}",
        )

        return unit


class Workflow:
    """
    Integrated processor combining geometry, clustering, and estimation
    """

    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: RejectionTracker,
        units: Units,
        config: dict[str, Any] | None = None,
    ):
        """
        Initialize the integrated processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        rejection_tracker : RejectionTracker
            Tracker for rejected elements
        units : Units
            Units collection to store processed units
        config : dict[str, Any] | None
            Configuration for processing
        """
        self.client = client
        self.config = config or {}
        self.units = units
        self.rejection_tracker = rejection_tracker

        self.clustering_manager = ClusteringManager(self.config)

        # Create processors
        self.plant_parser = PlantParser(
            client,
            self.rejection_tracker,
            self.config,
        )
        self.generator_parser = GeneratorParser(
            client,
            self.rejection_tracker,
            self.config,
        )

        # Store key processing parameters
        processing_parameters = {}
        for param in PROCESSING_PARAMETERS:
            if param in self.config:
                if param not in processing_parameters:
                    processing_parameters[param] = {}
                processing_parameters[param] = self.config[param]

        self.processing_parameters = processing_parameters

        # Generate config hash for validation
        self.config_hash = Unit._generate_config_hash(processing_parameters)

        # Tracking processed elements
        self.processed_elements: set[str] = (
            set()
        )  # Set of element IDs that have been processed

    def delete_rejections(self, units: list[Unit]) -> None:
        """
        Remove rejections from the rejection tracker of valid units
        """
        deleted = 0
        for unit in units:
            success = self.rejection_tracker.delete_rejection(unit.id)
            if success:
                deleted += 1

        logger.info(f"Deleted {deleted} rejections")

    def process_country_data(
        self,
        country: str,
        force_refresh: bool | None = None,
    ) -> Units:
        """
        Process OSM data for a country and add units to the collection.

        Parameters
        ----------
        country : str
            Country name
        force_refresh : bool | None
            Whether to force processing even if cache is valid

        Returns
        -------
        Units
            The updated units collection
        """
        if force_refresh is None:
            force_refresh = self.config.get("force_refresh", False)

        country_code = get_country_code(country)

        # Check for cached units
        cached_units = []
        if not force_refresh:
            cached_units = self.client.cache.get_units(country_code)

            # Filter only valid units for current config
            cached_units = [
                unit for unit in cached_units if unit.is_valid_for_config(self.config)
            ]

            if cached_units:
                logger.info(
                    f"Found {len(cached_units)} valid cached units for {country}"
                )
                # Add cached units to the collection
                self.units.add_units(cached_units)
                return self.units, self.rejection_tracker

        # If no valid cached units or force processing, process from raw data
        plants_only = self.config.get("plants_only", True)

        # Get country data
        plants_data, generators_data = self.client.get_country_data(
            country, force_refresh=force_refresh, plants_only=plants_only
        )
        # Initialize tracking attributes
        self.processed_plants: list[Unit] = []
        self.processed_generators: list[Unit] = []

        # Process plants
        for element in plants_data.get("elements", []):
            element_id = f"{element['type']}/{element['id']}"

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
                    self.rejection_tracker.add_rejection(
                        element_id=element["id"],
                        element_type=ElementType(element["type"]),
                        reason=RejectionReason.ELEMENT_ALREADY_PROCESSED,
                        details="Element processed already in plants processing",
                        category="Workflow:process_country_data",
                    )
                    continue

                # Check if generator coordinates are within any existing plant geometry
                if plant_polygons:
                    is_within, plant_id = (
                        self.generator_parser.geometry_handler.is_element_within_any_plant(
                            element, plant_polygons
                        )
                    )
                    if is_within:
                        self.rejection_tracker.add_rejection(
                            element_id=element["id"],
                            element_type=ElementType(element["type"]),
                            reason=RejectionReason.WITHIN_EXISTING_PLANT,
                            details=f"Element '{element['type']}/{element['id']}' is within existing plant geometry: {plant_id}",
                            category="Workflow:process_country_data",
                        )
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
                    source = gen.Fueltype or "unknown"
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

        # Get all processed plants and generators
        all_units: list[Unit] = []

        # Add all plants and generators from both parsers
        for plant in getattr(self, "processed_plants", []):
            all_units.append(plant)
        for generator in getattr(self, "processed_generators", []):
            all_units.append(generator)

        self.delete_rejections(all_units)

        # Enhance units with metadata before storing
        for unit in all_units:
            unit.created_at = datetime.datetime.now().isoformat()
            unit.config_hash = self.config_hash
            unit.config_version = "1.0"
            unit.processing_parameters = self.processing_parameters

        # Store processed units in cache
        self.client.cache.store_units(country_code, all_units)

        # Add processed units to the collection
        self.units.add_units(all_units)

        logger.info(f"Added {len(all_units)} units for {country} to collection")

        return self.units, self.rejection_tracker

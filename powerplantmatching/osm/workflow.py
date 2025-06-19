"""
Integrated processor combining geometry, clustering, and estimation
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

from .client import OverpassAPIClient
from .clustering import ClusteringManager
from .estimation import CapacityEstimator
from .extractor import CapacityExtractor
from .geometry import GeometryHandler
from .models import (
    PROCESSING_PARAMETERS,
    ElementType,
    GeneratorGroup,
    PlantPolygon,
    RejectedPlantInfo,
    Unit,
    Units,
)
from .reconstruction import (
    NameAggregator,
    OrphanedGeneratorSalvager,
    PlantReconstructor,
)
from .rejection import RejectionReason, RejectionTracker
from .unit_factory import UnitFactory
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

        missing_start_date_allowed = self.config.get(
            "missing_start_date_allowed", False
        )
        store_raw_date = ""
        datum = ""
        for key in start_date_keys:
            if key in tags:
                if isinstance(tags[key], str):
                    date_string = tags[key].strip()
                elif isinstance(tags[key], int | float):
                    date_string = str(int(tags[key]))
                else:
                    date_string = str(tags[key])
                store_raw_date = date_string
                datum = self._parse_date_string(
                    element=element, date_string=date_string
                )
                if datum:
                    return datum
                else:
                    if missing_start_date_allowed:
                        return ""
                    else:
                        self.rejection_tracker.add_rejection(
                            element_id=element["id"],
                            element_type=ElementType(element["type"]),
                            reason=RejectionReason.INVALID_START_DATE_FORMAT,
                            details=f"tags: {tags.keys()} - '{key}':'{store_raw_date}'",
                            category=f"{'PlantParser' if unit_type == 'plant' else 'GeneratorParser'}:extract_start_date_key_from_tags",
                        )
                        return None

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

    def _parse_date_string(self, element: dict[str, Any], date_string: str) -> str:
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
            logger.warning(
                f"Empty date string provided for plant {element['type']}/{element['id']}"
            )
            return ""

        date_string = date_string.strip()

        # Try to extract year from the string (for fallback)
        year_match = re.search(r"\b(1[0-9]{3}|2[0-9]{3})\b", date_string)
        if not year_match:
            logger.warning(
                f"No valid year found for plant {element['type']}/{element['id']} in '{date_string}'"
            )
            return ""

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

        except (ValueError, TypeError) as e:
            # If parsing failed, fallback to just using the year
            logger.warning(
                f"Date parsing failed for plant {element['type']}/{element['id']} in '{date_string}': {str(e)}"
            )
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
        self.unit_factory = UnitFactory(config)

        # Initialize reconstruction components if feature is enabled
        reconstruct_config = self.config.get("units_reconstruction", {})
        if reconstruct_config.get("enabled", False):
            self.name_aggregator = NameAggregator(config)
            self.plant_reconstructor = PlantReconstructor(config, self.name_aggregator)
            self.orphaned_salvager = OrphanedGeneratorSalvager(
                config, self.name_aggregator
            )
            self.salvaged_plants: list[Unit] = []

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

        # Track missing fields for salvage logic
        missing_fields = {
            "name": False,
            "source": source is None,
            "technology": False,
            "start_date": False,
        }

        # Extract technology (only if source exists)
        technology = None
        if source is not None:
            technology = self.extract_technology_from_tags(element, "plant", source)
            missing_fields["technology"] = technology is None
        else:
            missing_fields["technology"] = True

        # Extract name
        name = self.extract_name_from_tags(element, "plant")
        missing_fields["name"] = name is None

        # Extract start date
        start_date = self.extract_start_date_key_from_tags(element, "plant")
        missing_fields["start_date"] = start_date is None

        # NEW: Always try to extract capacity, even if other fields are missing
        existing_capacity = None
        existing_capacity_source = None

        # Only try to extract capacity if we have a source (needed for proper extraction)
        if source is not None:
            output_key = self.extract_output_key_from_tags(element, "plant", source)
            if output_key:
                # Try to extract capacity from the plant relation
                capacity, info = self._process_capacity(
                    element, source, output_key, "plant"
                )
                if capacity is not None and capacity > 0:
                    existing_capacity = capacity
                    existing_capacity_source = info
                    logger.debug(
                        f"Plant {element['id']} has existing capacity: {existing_capacity} MW ({info})"
                    )

        # Check if salvage feature is enabled and we have missing fields
        reconstruct_config = self.config.get("units_reconstruction", {})
        if reconstruct_config.get("enabled", False) and any(missing_fields.values()):
            # Try to salvage from members
            salvaged_unit = self._try_salvage_from_members(
                element,
                missing_fields,
                country,
                lat,
                lon,
                name,
                source,
                technology,
                start_date,
                existing_capacity,
                existing_capacity_source,
            )
            if salvaged_unit:
                self.salvaged_plants.append(salvaged_unit)
                return salvaged_unit

            # If we couldn't salvage but it's a relation, store for later
            if element["type"] == "relation":
                self._store_rejected_plant(element, missing_fields)
                return None

        # If salvage is not enabled or no missing fields, proceed with normal validation
        if source is None:
            return None
        if technology is None:
            return None
        if name is None:
            return None

        # Extract output key
        output_key = self.extract_output_key_from_tags(element, "plant", source)
        if output_key is None:
            return None

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

        return self.unit_factory.create_plant_unit(
            element_id=element["id"],
            element_type=element["type"],
            country=country,
            lat=lat,
            lon=lon,
            name=name,
            source=source,
            technology=technology,
            capacity=capacity,
            capacity_source=info,
            start_date=start_date,
        )

    def _get_member_element(self, member: dict[str, Any]) -> dict[str, Any] | None:
        """Get member element from cache"""
        member_type = member["type"]
        member_id = member["ref"]

        if member_type == "node":
            return self.client.cache.get_node(member_id)
        elif member_type == "way":
            return self.client.cache.get_way(member_id)
        elif member_type == "relation":
            # Skip nested relations to avoid recursion
            return None

        return None

    def _is_generator(self, element: dict[str, Any]) -> bool:
        """Check if element is a generator"""
        tags = element.get("tags", {})
        return tags.get("power") == "generator"

    def _try_salvage_from_members(
        self,
        relation: dict[str, Any],
        missing_fields: dict[str, bool],
        country: str,
        lat: float,
        lon: float,
        existing_name: str | None,
        existing_source: str | None,
        existing_technology: str | None,
        existing_start_date: str | None,
        existing_capacity: float | None = None,
        existing_capacity_source: str | None = None,
    ) -> Unit | None:
        """Try to complete missing fields from relation members using PlantReconstructor"""
        if "members" not in relation:
            return None

        # Collect generator members
        generator_members = []
        for member in relation["members"]:
            if member["type"] in ["node", "way"]:
                member_elem = self._get_member_element(member)
                if member_elem and self._is_generator(member_elem):
                    generator_members.append(member_elem)

        # Check if we can reconstruct
        if not self.plant_reconstructor.can_reconstruct(len(generator_members)):
            logger.debug(
                f"Not enough generators ({len(generator_members)}) for reconstruction "
                f"of relation {relation['id']}"
            )
            return None

        # Aggregate generator information
        aggregated_info = self.plant_reconstructor.aggregate_generator_info(
            generator_members
        )

        # Extract info from generators for missing fields
        for generator in generator_members:
            if missing_fields["name"]:
                gen_name = self.extract_name_from_tags(generator, "generator")
                if gen_name:
                    aggregated_info["names"].add(gen_name)

            if missing_fields["source"]:
                gen_source = self.extract_source_from_tags(generator, "generator")
                if gen_source:
                    aggregated_info["sources"].add(gen_source)

            if missing_fields["technology"] and existing_source:
                gen_tech = self.extract_technology_from_tags(
                    generator, "generator", existing_source
                )
                if gen_tech:
                    aggregated_info["technologies"].add(gen_tech)

            if missing_fields["start_date"]:
                gen_date = self.extract_start_date_key_from_tags(generator, "generator")
                if gen_date:
                    aggregated_info["start_dates"].add(gen_date)

        # Determine final values
        existing_values = {
            "name": existing_name,
            "source": existing_source,
            "technology": existing_technology,
            "start_date": existing_start_date,
        }

        final_values = self.plant_reconstructor.determine_final_values(
            aggregated_info, existing_values
        )

        # Check if we have all required fields now
        can_salvage = True
        if missing_fields["name"] and not final_values["name"]:
            can_salvage = False
        if missing_fields["source"] and not final_values["source"]:
            can_salvage = False
        if missing_fields["technology"] and not final_values["technology"]:
            can_salvage = False
        if missing_fields["start_date"] and not final_values["start_date"]:
            if not self.config.get("missing_start_date_allowed", False):
                can_salvage = False

        if can_salvage:
            # Create unit with salvaged data
            return self._create_unit_with_salvaged_data(
                relation,
                final_values,
                country,
                lat,
                lon,
                generator_members,
                existing_capacity,
                existing_capacity_source,
            )

        return None

    def _create_unit_with_salvaged_data(
        self,
        relation: dict[str, Any],
        salvaged_data: dict[str, Any],
        country: str,
        lat: float,
        lon: float,
        generator_members: list[dict],
        existing_capacity: float | None = None,
        existing_capacity_source: str | None = None,
    ) -> Unit:
        """Create a Unit object with salvaged data from generators"""
        # Use existing capacity if available
        if existing_capacity is not None and existing_capacity > 0:
            final_capacity = existing_capacity
            capacity_source = existing_capacity_source or "plant_relation"

            # Optionally, calculate generator capacity for validation
            generator_capacity = 0.0
            valid_generator_count = 0

            for generator in generator_members:
                output_key = self.extract_output_key_from_tags(
                    generator, "generator", salvaged_data["source"]
                )
                if output_key:
                    capacity, _ = self._process_capacity(
                        generator, salvaged_data["source"], output_key, "generator"
                    )
                    if capacity is not None and capacity > 0:
                        generator_capacity += capacity
                        valid_generator_count += 1

            # Log if there's a significant mismatch
            if valid_generator_count > 0 and generator_capacity > 0:
                mismatch_ratio = (
                    abs(existing_capacity - generator_capacity) / existing_capacity
                )
                if mismatch_ratio > 0.2:  # More than 20% difference
                    logger.warning(
                        f"Capacity mismatch for plant relation/{relation['id']}: "
                        f"Plant declares {existing_capacity} MW, "
                        f"but {valid_generator_count} generators sum to {generator_capacity:.1f} MW "
                        f"({mismatch_ratio * 100:.1f}% difference)"
                    )
        else:
            # Fall back to aggregating from generators
            total_capacity = 0.0
            capacity_count = 0
            capacity_source = "aggregated_from_generators"

            for generator in generator_members:
                # Try to get capacity from generator
                output_key = self.extract_output_key_from_tags(
                    generator, "generator", salvaged_data["source"]
                )
                if output_key:
                    capacity, _ = self._process_capacity(
                        generator, salvaged_data["source"], output_key, "generator"
                    )
                    if capacity is not None:
                        total_capacity += capacity
                        capacity_count += 1

            final_capacity = total_capacity if capacity_count > 0 else None

        # Create the unit using factory
        unit = self.unit_factory.create_reconstructed_plant(
            relation_id=relation["id"],
            country=country,
            lat=lat,
            lon=lon,
            name=salvaged_data["name"],
            source=salvaged_data["source"],
            technology=salvaged_data["technology"],
            capacity=final_capacity,
            generator_count=len(generator_members),
            start_date=salvaged_data["start_date"],
        )

        capacity_str = f"{final_capacity:.2f}" if final_capacity is not None else "0"
        logger.info(
            f"Salvaged plant {relation['id']} with {len(generator_members)} generators, "
            f"capacity: {capacity_str} MW ({capacity_source})"
        )

        return unit

    def _store_rejected_plant(
        self, element: dict[str, Any], missing_fields: dict[str, bool]
    ):
        """Store rejected plant info for later generator matching using OrphanedGeneratorSalvager"""
        polygon = self.geometry_handler.get_element_geometry(element)
        if polygon:
            plant_info = RejectedPlantInfo(
                element_id=str(element["id"]),
                polygon=polygon,
                missing_fields=missing_fields,
                member_generators=[],
            )
            self.orphaned_salvager.store_rejected_plant(plant_info)


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

        self.unit_factory = UnitFactory(config)

        # Initialize salvage-related attributes if feature is enabled
        reconstruct_config = self.config.get("units_reconstruction", {})
        if reconstruct_config.get("enabled", False):
            self.name_aggregator = NameAggregator(config)
            self.rejected_plant_polygons: dict[str, PlantPolygon] = {}
            self.generator_groups: dict[str, GeneratorGroup] = {}

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

        # Check if salvage feature is enabled and generator belongs to rejected plant
        reconstruct_config = self.config.get("units_reconstruction", {})
        if reconstruct_config.get("enabled", False) and hasattr(
            self, "rejected_plant_polygons"
        ):
            # Check if generator is within a rejected plant
            rejected_plant_id = self.geometry_handler.check_point_within_polygons(
                lat, lon, self.rejected_plant_polygons
            )
            if rejected_plant_id:
                # Add to generator group for later aggregation
                self._add_to_generator_group(element, rejected_plant_id)
                logger.debug(
                    f"Generator {element['id']} added to rejected plant group {rejected_plant_id}"
                )
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

        return self.unit_factory.create_generator_unit(
            element_id=element["id"],
            element_type=element["type"],
            country=country,
            lat=lat,
            lon=lon,
            name=name,
            source=source,
            technology=technology,
            capacity=capacity,
            capacity_source=info,
            start_date=start_date,
        )

    def set_rejected_plant_info(self, rejected_plant_info: dict[str, Any]):
        """Set rejected plant info from PlantParser"""
        self.rejected_plant_polygons = {
            plant_id: info.polygon for plant_id, info in rejected_plant_info.items()
        }

    def _add_to_generator_group(self, element: dict[str, Any], plant_id: str):
        """Add generator to a group for the rejected plant"""
        if plant_id not in self.generator_groups:
            self.generator_groups[plant_id] = GeneratorGroup(
                plant_id=plant_id,
                generators=[],
                plant_polygon=self.rejected_plant_polygons[plant_id],
            )

        self.generator_groups[plant_id].generators.append(element)

    def finalize_generator_groups(self) -> list[Unit]:
        """Create aggregated units from generator groups"""
        aggregated_units = []

        for plant_id, group in self.generator_groups.items():
            if len(group.generators) > 0:
                unit = self._create_aggregated_unit(group)
                if unit:
                    aggregated_units.append(unit)

        return aggregated_units

    def _create_aggregated_unit(self, group: GeneratorGroup) -> Unit | None:
        """Create an aggregated unit from a generator group"""
        # Aggregate information from all generators
        names = set()
        sources = set()
        technologies = set()
        start_dates = set()
        total_capacity = 0.0
        capacity_count = 0

        for generator in group.generators:
            # Extract information
            name = self.extract_name_from_tags(generator, "generator")
            if name:
                names.add(name)

            source = self.extract_source_from_tags(generator, "generator")
            if source:
                sources.add(source)

            if source:
                tech = self.extract_technology_from_tags(generator, "generator", source)
                if tech:
                    technologies.add(tech)

            date = self.extract_start_date_key_from_tags(generator, "generator")
            if date:
                start_dates.add(date)

            # Get capacity
            if source:
                output_key = self.extract_output_key_from_tags(
                    generator, "generator", source
                )
                if output_key:
                    capacity, _ = self._process_capacity(
                        generator, source, output_key, "generator"
                    )
                    if capacity is not None:
                        total_capacity += capacity
                        capacity_count += 1

        # Determine final values
        final_name = (
            self.name_aggregator.aggregate_names(names)
            if names
            else f"Plant Group {group.plant_id}"
        )

        # Use most common source and technology
        final_source = (
            max(sources, key=lambda x: sum(1 for s in sources if s == x))
            if sources
            else None
        )
        final_technology = (
            max(technologies, key=lambda x: sum(1 for t in technologies if t == x))
            if technologies
            else None
        )
        final_start_date = min(start_dates) if start_dates else None

        # Get coordinates from plant polygon
        lat, lon = self.geometry_handler.get_geometry_centroid(group.plant_polygon)

        # Get country from context
        country = self.rejection_tracker.get_element_country(
            group.generators[0]["id"], ElementType(group.generators[0]["type"])
        )

        if final_source and lat is not None and lon is not None:
            return self.unit_factory.create_salvaged_plant(
                plant_id=group.plant_id,
                country=country,
                lat=lat,
                lon=lon,
                name=final_name,
                source=final_source,
                technology=final_technology,
                capacity=total_capacity if capacity_count > 0 else None,
                generator_count=len(group.generators),
                start_date=final_start_date,
            )

        return None


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

            # Pass rejected plant info to generator parser if salvage feature is enabled
            reconstruct_config = self.config.get("units_reconstruction", {})
            if reconstruct_config.get("enabled", False) and hasattr(
                self.plant_parser, "rejected_plant_info"
            ):
                self.generator_parser.set_rejected_plant_info(
                    self.plant_parser.rejected_plant_info
                )

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

            # Finalize generator groups from rejected plants if salvage feature is enabled
            reconstruct_config = self.config.get("units_reconstruction", {})
            if reconstruct_config.get("enabled", False) and hasattr(
                self.generator_parser, "finalize_generator_groups"
            ):
                aggregated_units = self.generator_parser.finalize_generator_groups()
                if aggregated_units:
                    self.processed_generators.extend(aggregated_units)
                    logger.info(
                        f"Created {len(aggregated_units)} aggregated units from generator groups"
                    )

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

        # Add salvaged plants if salvage feature is enabled
        reconstruct_config = self.config.get("units_reconstruction", {})
        if reconstruct_config.get("enabled", False) and hasattr(
            self.plant_parser, "salvaged_plants"
        ):
            for salvaged_plant in self.plant_parser.salvaged_plants:
                all_units.append(salvaged_plant)
            if self.plant_parser.salvaged_plants:
                logger.info(
                    f"Added {len(self.plant_parser.salvaged_plants)} salvaged plants"
                )

        for generator in getattr(self, "processed_generators", []):
            all_units.append(generator)

        self.delete_rejections(all_units)

        # Store processed units in cache
        self.client.cache.store_units(country_code, all_units)

        # Add processed units to the collection
        self.units.add_units(all_units)

        logger.info(f"Added {len(all_units)} units for {country} to collection")

        return self.units, self.rejection_tracker

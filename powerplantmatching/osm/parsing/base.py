# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Base classes for parsing OpenStreetMap power plant elements.

This module provides the abstract base class for parsing OSM elements
(nodes, ways, relations) into standardized power plant units. It handles
common parsing tasks like extracting names, sources, technologies, and
capacities from OSM tags.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

import numpy as np

from powerplantmatching.osm.enhancement.estimation import CapacityEstimator
from powerplantmatching.osm.enhancement.geometry import GeometryHandler
from powerplantmatching.osm.models import Unit
from powerplantmatching.osm.quality.rejection import RejectionReason, RejectionTracker
from powerplantmatching.osm.retrieval.client import OverpassAPIClient

from .capacity import CapacityExtractor

logger = logging.getLogger(__name__)


class ElementProcessor(ABC):
    """Abstract base class for processing OSM elements into power plant units.

    Provides common functionality for extracting and validating power plant
    attributes from OSM tags. Subclasses implement specific processing logic
    for plants and generators.

    The processor handles:
    - Tag extraction with configurable mappings
    - Data validation and rejection tracking
    - Capacity extraction and estimation
    - Date parsing and standardization

    Attributes
    ----------
    client : OverpassAPIClient
        API client for additional data retrieval
    config : dict
        Processing configuration including tag mappings
    rejection_tracker : RejectionTracker
        Tracks elements rejected for data quality issues
    capacity_extractor : CapacityExtractor
        Handles capacity value extraction from tags
    capacity_estimator : CapacityEstimator
        Estimates missing capacities
    geometry_handler : GeometryHandler
        Handles spatial operations

    Notes
    -----
    This is an abstract base class. Use PlantParser or GeneratorParser
    for actual element processing.
    """

    def __init__(
        self,
        client: OverpassAPIClient,
        geometry_handler: GeometryHandler,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any] | None = None,
    ):
        """Initialize the element processor.

        Parameters
        ----------
        client : OverpassAPIClient
            Client for API access
        geometry_handler : GeometryHandler
            Handler for spatial operations
        rejection_tracker : RejectionTracker
            Tracker for rejected elements
        config : dict, optional
            Processing configuration
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
        """Extract name from OSM tags with fallback handling.

        Parameters
        ----------
        element : dict
            OSM element with tags
        unit_type : {'plant', 'generator'}
            Type of unit being processed

        Returns
        -------
        str or None
            Extracted name, empty string if allowed missing, or None if rejected

        Notes
        -----
        Checks multiple name tag keys in order of preference.
        If missing_name_allowed=True in config, returns empty string.
        Otherwise adds rejection and returns None.
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

        name = None
        for key in name_keys:
            if key in tags:
                name = tags[key]
                if name:
                    return name

        missing_name_allowed = self.config.get("missing_name_allowed", False)
        if missing_name_allowed:
            return ""

        if not name:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.MISSING_NAME_TAG,
                details=f"tags: {tags}",
                keywords="none",
            )

        return None

    def extract_source_from_tags(
        self, element: dict[str, Any], unit_type: str
    ) -> str | None:
        """Extract and map source (fuel type) from OSM tags.

        Parameters
        ----------
        element : dict
            OSM element with tags
        unit_type : {'plant', 'generator'}
            Type of unit being processed

        Returns
        -------
        str or None
            Mapped fuel type (e.g., 'Solar', 'Wind'), or None if not found/mapped

        Notes
        -----
        Uses source_mapping from config to map OSM values to standard fuel types.
        Tracks both missing tags and unmapped values separately.
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
                element=element,
                reason=RejectionReason.MISSING_SOURCE_TAG,
                details=f"tags: {tags}",
                keywords="none",
            )
        else:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.MISSING_SOURCE_TYPE,
                details=f"Source value '{store_element_source}' from tag '{key}' is not recognized",
                keywords=store_element_source,
            )

        return None

    def extract_technology_from_tags(
        self, element: dict[str, Any], unit_type: str, source_type: str
    ) -> str | None:
        """Extract and map technology from OSM tags based on source type.

        Parameters
        ----------
        element : dict
            OSM element with tags
        unit_type : {'plant', 'generator'}
            Type of unit being processed
        source_type : str
            Fuel type to filter valid technologies

        Returns
        -------
        str or None
            Mapped technology (e.g., 'PV', 'CCGT'), empty string if allowed missing,
            or None if not found/mapped

        Notes
        -----
        Technology mapping is source-specific. For example, 'photovoltaic' maps
        to 'PV' only for Solar sources. Uses source_technology_mapping to ensure
        valid technology-fuel combinations.
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
                element=element,
                reason=RejectionReason.MISSING_TECHNOLOGY_TAG,
                details=f"No technology tag found. Element has {len(tags)} tags but none specify technology",
                keywords="none",
            )
        else:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.MISSING_TECHNOLOGY_TYPE,
                details=f'''"{store_element_technology}" not found in technology_mapping. Ensure updating source_technology_mapping with "{source_type}"''',
                keywords=store_element_technology,
            )

        return None

    def extract_output_key_from_tags(
        self, element: dict[str, Any], unit_type: str, source_type: str | None = None
    ) -> str | None:
        """Find the tag key containing capacity/output information.

        Parameters
        ----------
        element : dict
            OSM element with tags
        unit_type : {'plant', 'generator'}
            Type of unit being processed
        source_type : str, optional
            Fuel type for source-specific tag keys

        Returns
        -------
        str or None
            Tag key containing capacity (e.g., 'plant:output:electricity'),
            or None if not found

        Notes
        -----
        Returns the key itself, not the value. The actual capacity extraction
        is handled by CapacityExtractor using this key.
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
            element=element,
            reason=RejectionReason.MISSING_OUTPUT_TAG,
            details=f"tags: {tags}",
            keywords="none",
        )
        return None

    def extract_start_date_key_from_tags(
        self, element: dict[str, Any], unit_type: str
    ) -> str | None:
        """Extract and parse start date from OSM tags.

        Parameters
        ----------
        element : dict
            OSM element with tags
        unit_type : {'plant', 'generator'}
            Type of unit being processed

        Returns
        -------
        str or None
            Standardized date string (YYYY-MM-DD format), empty string if allowed
            missing, or None if invalid/missing

        Notes
        -----
        Handles various date formats using fuzzy parsing. Falls back to
        year-only dates (YYYY-01-01) when full date cannot be determined.
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
                            element=element,
                            reason=RejectionReason.INVALID_START_DATE_FORMAT,
                            details=f"Date value '{store_raw_date}' in tag '{key}' could not be parsed to standard format",
                            keywords=store_raw_date,
                        )
                        return None

        if missing_start_date_allowed:
            return ""

        if not store_raw_date:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.MISSING_START_DATE_TAG,
                details=f"tags: {tags}",
                keywords="none",
            )
        else:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.INVALID_START_DATE_FORMAT,
                details=f"Date value '{store_raw_date}' could not be parsed to standard format",
                keywords=store_raw_date,
            )

        return None

    def _parse_date_string(self, element: dict[str, Any], date_string: str) -> float:
        """Parse various date formats into standardized YYYY-MM-DD format."""
        import re

        from dateutil import parser

        if not date_string or not date_string.strip():
            logger.warning(
                f"Empty date string provided for plant {element['type']}/{element['id']}"
            )
            return np.nan

        date_string = date_string.strip()

        year_match = re.search(r"\b(1[0-9]{3}|2[0-9]{3})\b", date_string)
        if not year_match:
            logger.warning(
                f"No valid year found for plant {element['type']}/{element['id']} in '{date_string}'"
            )
            return np.nan

        year = float(int(year_match.group(1)))

        try:
            date_obj = parser.parse(date_string, fuzzy=True)

            if date_string.count(str(date_obj.year)) > 0:
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
                    return float(date_obj.year)
                elif month_detected:
                    return float(date_obj.year)
                else:
                    return float(date_obj.year)

            return year

        except (ValueError, TypeError) as e:
            logger.warning(
                f"Date parsing failed {element['type']}/{element['id']} in '{date_string}': {str(e)}. Using year only."
            )
            return year

    @abstractmethod
    def process_element(
        self, element: dict[str, Any], country: str | None = None
    ) -> Unit | None:
        """Process an OSM element into a power plant unit.

        Parameters
        ----------
        element : dict
            OSM element to process
        country : str, optional
            Country name for the element

        Returns
        -------
        Unit or None
            Processed unit or None if rejected

        Notes
        -----
        This is an abstract method that must be implemented by subclasses.
        """
        pass

    def _process_capacity(
        self,
        element: dict[str, Any],
        source_type: str,
        output_key: str,
        unit_type: str,
    ) -> tuple[float | None, str]:
        """Process capacity through extraction and estimation pipeline.

        Parameters
        ----------
        element : dict
            OSM element with tags
        source_type : str
            Fuel type for estimation parameters
        output_key : str
            Tag key containing capacity value
        unit_type : {'plant', 'generator'}
            Type of unit being processed

        Returns
        -------
        capacity : float or None
            Extracted/estimated capacity in MW
        info : str
            Source of capacity ('tag', 'estimated', etc.)
        """
        assert unit_type in ["plant", "generator"], "Invalid unit type"
        is_valid, capacity, info = self.capacity_extractor.basic_extraction(
            element, output_key
        )

        advanced_extraction_enabled = self.config.get("capacity_extraction", {}).get(
            "enabled", False
        )

        if not is_valid and advanced_extraction_enabled and info != "placeholder_value":
            is_valid, capacity, info = self.capacity_extractor.advanced_extraction(
                element, output_key
            )

        capacity_estimation_enabled = self.config.get("capacity_estimation", {}).get(
            "enabled", False
        )
        if not is_valid and capacity_estimation_enabled:
            capacity, info = self.capacity_estimator.estimate_capacity(
                element, source_type, unit_type=unit_type
            )

        return capacity, info

    def _get_relation_member_capacity(
        self, relation: dict[str, Any], source_type: str, unit_type: str
    ) -> tuple[float | None, str, list]:
        """Calculate total capacity from relation members.

        Parameters
        ----------
        relation : dict
            OSM relation with members
        source_type : str
            Fuel type for capacity processing
        unit_type : {'plant', 'generator'}
            Type of unit being processed

        Returns
        -------
        total_capacity : float or None
            Sum of member capacities or None if no valid members
        info : str
            'member_capacity' or 'aggregated_capacity'
        members_with_capacity : list
            List of (member_element, capacity) tuples
        """
        if "members" not in relation:
            return None, "unknown", []

        relation_country = relation.get("_country")

        members_with_capacity = []

        for member in relation["members"]:
            member_type = member["type"]
            member_id = member["ref"]

            member_elem = None
            if member_type == "node":
                member_elem = self.client.cache.get_node(member_id)
            elif member_type == "way":
                member_elem = self.client.cache.get_way(member_id)
            elif member_type == "relation":
                continue

            if not member_elem or "tags" not in member_elem:
                continue

            if relation_country and "_country" not in member_elem:
                member_elem["_country"] = relation_country

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

        if not members_with_capacity:
            return None, "unknown", []

        if len(members_with_capacity) == 1:
            return members_with_capacity[0][1], "member_capacity", members_with_capacity

        total_capacity = sum(capacity for _, capacity in members_with_capacity)
        return total_capacity, "aggregated_capacity", members_with_capacity

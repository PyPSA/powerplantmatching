"""
Processors for converting OSM elements into power plant objects
"""

import logging
import re
from abc import ABC, abstractmethod
from typing import Any, Optional

from .client import OverpassAPIClient
from .models import Unit
from .rejection import ElementType, RejectionReason, RejectionTracker

logger = logging.getLogger(__name__)


class OSMElementProcessor(ABC):
    """Base class for processing OSM elements"""

    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: Optional[RejectionTracker] = None,
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
        element_id = f"{element['type']}_{element['id']}"
        element_type = ElementType(element["type"])

        # Add to tracker
        self.rejection_tracker.add_rejection(
            element_id=element_id,
            element_type=element_type,
            reason=reason,
            details=details,
            category=category,
        )

        logger.debug(f"Rejected element {element_id}: {reason.value} - {details or ''}")

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

    def normalize_capacity(
        self, capacity_str: Optional[str]
    ) -> tuple[Optional[float], str]:
        """
        Normalize capacity values from various formats

        Parameters
        ----------
        capacity_str : Optional[str]
            Capacity string to normalize

        Returns
        -------
        tuple[Optional[float], str]
            (normalized_capacity_in_MW, capacity_source)
        """
        if not capacity_str:
            return None, "unknown"

        capacity_str = str(capacity_str).strip().lower()
        capacity_str = capacity_str.replace(",", ".")

        match = re.match(r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+p?)?$", capacity_str)
        if match:
            value, unit = match.groups()
            value = float(value)

            if unit in ["w", "watts", "wp"]:
                return value / 1e6, "direct"
            elif unit in ["kw", "kilowatts", "kwp"]:
                return value / 1000, "direct"
            elif unit in ["mw", "megawatts", "mwp"]:
                return value, "direct"
            elif unit in ["gw", "gigawatts", "gwp"]:
                return value * 1000, "direct"
            else:
                return value / 1000, "direct"  # If no unit, assume kilowatts
        else:
            try:
                return float(capacity_str) / 1000, "direct"
            except ValueError:
                logger.debug(f"Failed to parse capacity string: {capacity_str}")
                return None, "unknown"

    def extract_source_from_tags(self, tags: dict[str, str]) -> Optional[str]:
        """
        Extract power source from tags

        Parameters
        ----------
        tags : dict[str, str]
            OSM element tags

        Returns
        -------
        Optional[str]
            Power source if found, None otherwise
        """
        # Try different tag combinations for source
        source_keys = [
            "plant:source",
            "generator:source",
            "power:source",
            "energy:source",
            "source",
        ]

        for key in source_keys:
            if key in tags:
                return tags[key]

        return None

    def extract_technology_from_tags(self, tags: dict[str, str]) -> Optional[str]:
        """
        Extract technology from tags

        Parameters
        ----------
        tags : dict[str, str]
            OSM element tags

        Returns
        -------
        Optional[str]
            Technology if found, None otherwise
        """
        # Try different tag combinations for technology
        tech_keys = [
            "plant:method",
            "plant:type",
            "generator:method",
            "generator:type",
            "power:method",
            "power:type",
        ]

        for key in tech_keys:
            if key in tags:
                return tags[key]

        return None

    def get_element_coordinates(
        self, element: dict[str, Any]
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Get coordinates for an element

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        Returns
        -------
        tuple[Optional[float], Optional[float]]
            (latitude, longitude) if found, (None, None) otherwise
        """
        if element["type"] == "node":
            if "lat" in element and "lon" in element:
                return element["lat"], element["lon"]
        elif element["type"] == "way":
            if "center" in element:
                return element["center"]["lat"], element["center"]["lon"]
        elif element["type"] == "relation":
            if "center" in element:
                return element["center"]["lat"], element["center"]["lon"]

        return None, None


class PlantProcessor(OSMElementProcessor):
    """Processor for plant elements"""

    def process_element(
        self, element: dict[str, Any], country: Optional[str] = None
    ) -> Optional[Unit]:
        """
        Process a plant element into a Unit object

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

        # Get coordinates
        lat, lon = self.get_element_coordinates(element)
        if lat is None or lon is None:
            self.reject_element(
                element,
                RejectionReason.COORDINATES_NOT_FOUND,
                "Could not determine coordinates for plant",
                category="plant",
            )
            return None

        # Get capacity
        capacity_str = tags.get("plant:output:electricity")
        capacity, capacity_source = self.normalize_capacity(capacity_str)

        # Create ID and name
        element_id = f"OSM_plant:{element['type']}_{element['id']}"
        name = tags.get("name")

        # Extract source and technology
        source = self.extract_source_from_tags(tags)
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


class GeneratorProcessor(OSMElementProcessor):
    """Processor for generator elements"""

    def process_element(
        self, element: dict[str, Any], country: Optional[str] = None
    ) -> Optional[Unit]:
        """
        Process a generator element into a Unit object

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

        # Get coordinates
        lat, lon = self.get_element_coordinates(element)
        if lat is None or lon is None:
            self.reject_element(
                element,
                RejectionReason.COORDINATES_NOT_FOUND,
                "Could not determine coordinates for generator",
                category="generator",
            )
            return None

        # Get capacity
        capacity_str = tags.get("generator:output:electricity") or tags.get(
            "generator:output"
        )
        capacity, capacity_source = self.normalize_capacity(capacity_str)

        # Create ID and name
        element_id = f"OSM_generator:{element['type']}_{element['id']}"
        name = tags.get("name")

        # Extract source and technology
        source = self.extract_source_from_tags(tags)
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


class Processor:
    """Main processor for OSM power plant data"""

    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: Optional[RejectionTracker] = None,
    ):
        """
        Initialize the OSM processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        rejection_tracker : Optional[RejectionTracker]
            Tracker for rejected elements
        """
        self.client = client
        self.rejection_tracker = rejection_tracker or RejectionTracker()
        self.plant_processor = PlantProcessor(client, self.rejection_tracker)
        self.generator_processor = GeneratorProcessor(client, self.rejection_tracker)
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
            plant = self.plant_processor.process_element(element, country)
            if plant:
                plants.append(plant)
                self.processed_elements.add(element_id)

        # Process generators if requested
        if not plants_only:
            for element in generators_data.get("elements", []):
                # Skip already processed elements
                element_id = f"{element['type']}_{element['id']}"
                if element_id in self.processed_elements:
                    continue

                # Process element
                plant = self.generator_processor.process_element(element, country)
                if plant:
                    plants.append(plant)
                    self.processed_elements.add(element_id)

        return plants

    def get_rejected_summary(self) -> dict[str, dict[str, int]]:
        """
        Get a summary of rejected elements

        Returns
        -------
        dict[str, dict[str, int]]
            Summary of rejected elements by processor and reason
        """
        # Get summary from the centralized rejection tracker
        return self.rejection_tracker.get_summary()

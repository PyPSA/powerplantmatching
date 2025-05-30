import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from .models import ElementType, RejectionReason

logger = logging.getLogger(__name__)


@dataclass
class RejectedElement:
    id: str
    element_id: str
    element_type: ElementType
    reason: RejectionReason
    details: str | None = None
    category: str | None = None
    timestamp: datetime = None
    url: str | None = None
    coordinates: tuple[float, float] | None = None  # (lat, lon)
    country: str | None = None  # Country where the element is located

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.url is None:
            if not "cluster" in self.id:
                self.url = f"https://www.openstreetmap.org/{self.id}"


class RejectionTracker:
    """Centralized system for tracking and managing rejected elements"""

    def __init__(self):
        """Initialize the rejection tracker"""
        self.rejected_elements: dict[str, list[RejectedElement]] = {}
        self.ids: set[str] = set()
        self.categories: set[str] = set()
        # Context for storing element coordinates during processing
        self._element_coordinates: dict[str, tuple[float, float]] = {}
        # Context for storing element country during processing
        self._element_countries: dict[str, str] = {}

    def set_element_coordinates(
        self,
        element_id: str,
        element_type: ElementType,
        coordinates: tuple[float, float] | None,
    ) -> None:
        """
        Set coordinates for an element to be used in subsequent rejections

        Parameters
        ----------
        element_id : str
            Identifier for the element
        element_type : ElementType
            Type of the element
        coordinates : tuple[float, float] | None
            Latitude and longitude coordinates, if available
        """
        if coordinates is not None:
            identification = f"{element_type.value}/{element_id}"
            self._element_coordinates[identification] = coordinates

    def set_element_country(
        self, element_id: str, element_type: ElementType, country: str | None
    ) -> None:
        """
        Set country for an element to be used in subsequent rejections

        Parameters
        ----------
        element_id : str
            Identifier for the element
        element_type : ElementType
            Type of the element
        country : str | None
            Country where the element is located
        """
        if country is not None:
            identification = f"{element_type.value}/{element_id}"
            self._element_countries[identification] = country

    def clear_element_coordinates(
        self, element_id: str, element_type: ElementType
    ) -> None:
        """
        Clear stored coordinates and country for an element

        Parameters
        ----------
        element_id : str
            Identifier for the element
        element_type : ElementType
            Type of the element
        """
        identification = f"{element_type.value}/{element_id}"
        if identification in self._element_coordinates:
            del self._element_coordinates[identification]
        if identification in self._element_countries:
            del self._element_countries[identification]

    def add_rejection(
        self,
        element_id: str,
        element_type: ElementType,
        reason: RejectionReason,
        details: str | None = None,
        category: str | None = None,
        coordinates: tuple[float, float] | None = None,
        country: str | None = None,
    ) -> None:
        """
        Add a rejected element to the tracker

        Parameters
        ----------
        element_id : str
            Identifier for the element
        element_type : ElementType
            Type of the element
        reason : RejectionReason
            Reason for rejection
        details : Optional[str]
            Additional details about the rejection
        category : str
            Category for grouping rejections (e.g., 'plant', 'generator')
        coordinates : Optional[tuple[float, float]]
            Latitude and longitude coordinates, if available
        country : Optional[str]
            Country where the element is located
        """
        identification = f"{element_type.value}/{element_id}"

        # If no coordinates provided, try to get from stored context
        if coordinates is None:
            coordinates = self._element_coordinates.get(identification)

        # If no country provided, try to get from stored context
        if country is None:
            country = self._element_countries.get(identification)

        rejected = RejectedElement(
            id=identification,
            element_id=element_id,
            element_type=element_type,
            reason=reason,
            details=details,
            category=category,
            coordinates=coordinates,
            country=country,
        )

        # Add to main list
        if identification not in self.rejected_elements:
            self.rejected_elements[identification] = []
        self.rejected_elements[identification].append(rejected)

        # Add to id set
        self.ids.add(rejected.id)

        # Log rejection
        logger.debug(
            f"Rejected element {element_id} ({category}): {reason.value} - {details or ''}"
        )

    def get_rejection(self, id: str) -> list[RejectedElement] | None:
        """
        Get rejected elements by ID
        Parameters
        ----------
        id : str
            ID of the rejected element

        Returns
        -------
        list[RejectedElement] | None
            List of rejected elements with the given ID, or None if not found
        """
        return self.rejected_elements.get(id, None)

    def delete_rejection(self, id: str) -> bool:
        """
        Delete a rejected element by ID

        Parameters
        ----------
        id : str
            ID of the rejected element
        """
        success = False
        if id in self.rejected_elements:
            del self.rejected_elements[id]
            success = True
            logger.debug(f"Deleted rejection with ID: {id}")
        else:
            logger.debug(f"Rejection with ID {id} not found for deletion.")

        # Remove from ids
        if id in self.ids:
            self.ids.remove(id)

        return success

    def get_all_rejections(self) -> list[RejectedElement]:
        """
        Get all rejected elements

        Returns
        -------
        list[RejectedElement]
            list of all rejected elements
        """
        return self.rejected_elements

    def get_rejections_by_category(self, category: str) -> Iterator[RejectedElement]:
        """
        Get rejected elements for a specific category

        Parameters
        ----------
        category : str
            Category name

        Returns
        -------
        Iterator[RejectedElement]
            Iterator over rejected elements in the category
        """
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                if rejection.category == category:
                    yield rejection

    def get_summary(self) -> dict[str, dict[str, int]]:
        """
        Get a summary of rejection counts by category and reason

        Returns
        -------
        dict[str, dict[str, int]]
            Summary of rejections by category and reason
        """
        summary = {}
        for category in self.categories:
            summary[category] = {}
            for rejection in self.get_rejections_by_category(category):
                summary[category][rejection.reason.value] = (
                    summary[category].get(rejection.reason.value, 0) + 1
                )

        return summary

    def get_total_count(self) -> int:
        """
        Get total count of rejections

        Returns
        -------
        int
            Total number of rejections
        """
        return len(self.rejected_elements)

    def get_category_count(self, category: str) -> int:
        """
        Get count of rejections for a specific category

        Parameters
        ----------
        category : str
            Category name

        Returns
        -------
        int
            Number of rejections in the category
        """
        return len(self.get_rejections_by_category(category))

    def clear(self) -> None:
        """Clear all rejection data"""
        self.rejected_elements = {}
        self.categories = set()
        self.ids = set()
        self._element_coordinates = {}
        self._element_countries = {}

    def get_rejections_by_country(self, country: str) -> list[RejectedElement]:
        """
        Get all rejected elements for a specific country

        Parameters
        ----------
        country : str
            Country to filter by

        Returns
        -------
        list[RejectedElement]
            List of rejected elements in the specified country
        """
        filtered_rejections = []
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                if rejection.country == country:
                    filtered_rejections.append(rejection)
        return filtered_rejections

    def get_unique_countries(self) -> list[str]:
        """
        Get all unique countries present in the rejection data

        Returns
        -------
        list[str]
            List of unique countries
        """
        countries = set()
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                if rejection.country:
                    countries.add(rejection.country)
        return sorted(list(countries))

    def get_country_statistics(self) -> dict[str, int]:
        """
        Get statistics about rejections by country

        Returns
        -------
        dict[str, int]
            Dictionary mapping country names to rejection counts
        """
        stats = {}
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                country = rejection.country or "Unknown"
                stats[country] = stats.get(country, 0) + 1
        return dict(sorted(stats.items(), key=lambda x: x[1], reverse=True))

    def get_rejections_by_reason(
        self, reason: RejectionReason
    ) -> list[RejectedElement]:
        """
        Get all rejected elements for a specific reason

        Parameters
        ----------
        reason : RejectionReason
            Reason to filter by

        Returns
        -------
        list[RejectedElement]
            List of rejected elements with the specified reason
        """
        filtered_rejections = []
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                if rejection.reason == reason:
                    filtered_rejections.append(rejection)
        return filtered_rejections

    def get_unique_rejection_reasons(self) -> list[RejectionReason]:
        """
        Get all unique rejection reasons present in the data

        Returns
        -------
        list[RejectionReason]
            List of unique rejection reasons
        """
        reasons = set()
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                reasons.add(rejection.reason)
        return sorted(list(reasons), key=lambda x: x.value)

    def get_rejection_statistics(self) -> dict[str, int]:
        """
        Get statistics about rejections by reason

        Returns
        -------
        dict[str, int]
            Dictionary mapping rejection reason names to counts
        """
        stats = {}
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                reason_name = rejection.reason.value
                stats[reason_name] = stats.get(reason_name, 0) + 1
        return dict(sorted(stats.items(), key=lambda x: x[1], reverse=True))

    def generate_geojson_report_by_reason(
        self, reason: RejectionReason
    ) -> dict[str, Any]:
        """
        Generate a GeoJSON report filtered by rejection reason

        Parameters
        ----------
        reason : RejectionReason
            Reason to filter by

        Returns
        -------
        dict[str, Any]
            GeoJSON FeatureCollection containing only rejections with the specified reason
        """
        features = []

        # Get rejections for the specific reason
        filtered_rejections = self.get_rejections_by_reason(reason)

        for rejection in filtered_rejections:
            # Skip elements without coordinates
            if rejection.coordinates is None:
                continue

            lat, lon = rejection.coordinates
            if lat is None or lon is None:
                continue

            # Skip cluster elements
            if "cluster" in rejection.id.lower():
                continue

            # Determine the rejection type for the GeoJSON properties
            rejection_type = self._map_rejection_to_type(rejection.reason)

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat],  # GeoJSON uses [lon, lat] order
                },
                "properties": {
                    "label": f"Rejected {rejection.element_type.value} {rejection.element_id}",
                    "type": rejection_type,
                    "osm_element": f"https://www.openstreetmap.org/{rejection.id}",
                    "rejection_reason": rejection.reason.value,
                    "rejection_details": rejection.details or "",
                    "rejection_category": rejection.category or "",
                    "timestamp": rejection.timestamp.isoformat()
                    if rejection.timestamp
                    else "",
                    "element_type": rejection.element_type.value,
                    "element_id": rejection.element_id,
                    "country": rejection.country,
                },
            }
            features.append(feature)

        # Create GeoJSON structure
        geojson = {"type": "FeatureCollection", "features": features}

        logger.info(
            f"Generated GeoJSON report for '{reason.value}' with {len(features)} features"
        )
        return geojson

    def save_geojson_report_by_reason(
        self, reason: RejectionReason, filepath: str
    ) -> None:
        """
        Generate and save a GeoJSON report filtered by rejection reason

        Parameters
        ----------
        reason : RejectionReason
            Reason to filter by
        filepath : str
            Path to save the GeoJSON file
        """
        geojson_data = self.generate_geojson_report_by_reason(reason)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(geojson_data, f, indent=2, ensure_ascii=False)

        feature_count = len(geojson_data["features"])
        logger.info(
            f"Saved '{reason.value}' rejections GeoJSON to {filepath} ({feature_count} features)"
        )

    def save_all_rejection_reasons_geojson(
        self, output_dir: str, prefix: str = "rejections"
    ) -> None:
        """
        Generate separate GeoJSON files for each rejection reason

        Parameters
        ----------
        output_dir : str
            Directory to save the GeoJSON files
        prefix : str, default "rejections"
            Prefix for the output filenames
        """
        import os

        # Get all unique reasons
        unique_reasons = self.get_unique_rejection_reasons()

        # Get statistics for reporting
        stats = self.get_rejection_statistics()

        logger.info(
            f"Generating GeoJSON files for {len(unique_reasons)} rejection reasons..."
        )

        for reason in unique_reasons:
            # Create safe filename
            reason_name = reason.value.lower().replace(" ", "_").replace("/", "_")
            filename = f"{prefix}_{reason_name}.geojson"
            filepath = os.path.join(output_dir, filename)

            # Save GeoJSON for this reason
            self.save_geojson_report_by_reason(reason, filepath)

            # Log statistics
            count = stats.get(reason.value, 0)
            print(f"  - {reason.value}: {count} rejections → {filename}")

    def generate_geojson_report(self) -> dict[str, Any]:
        """
        Generate a GeoJSON report of rejections with coordinates.

        Returns
        -------
        dict[str, Any]
            GeoJSON FeatureCollection containing rejected elements with locations

        Examples
        --------
        >>> tracker = RejectionTracker()
        >>> # ... add rejections with coordinates ...
        >>> geojson_data = tracker.generate_geojson_report()
        >>> # Save to file
        >>> with open('rejections.geojson', 'w') as f:
        ...     json.dump(geojson_data, f, indent=2)
        """
        features = []

        # Process all rejection entries
        for element_id, rejections in self.rejected_elements.items():
            # Skip cluster elements and elements without coordinates
            if "cluster" in element_id.lower():
                continue

            # Create features for each rejection of this element that has coordinates
            for rejection in rejections:
                if rejection.coordinates is None:
                    continue

                lat, lon = rejection.coordinates
                if lat is None or lon is None:
                    continue

                # Determine the rejection type for the GeoJSON properties
                rejection_type = self._map_rejection_to_type(rejection.reason)

                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat],  # GeoJSON uses [lon, lat] order
                    },
                    "properties": {
                        "label": f"Rejected {rejection.element_type.value} {rejection.element_id}",
                        "type": rejection_type,
                        "osm_element": f"https://www.openstreetmap.org/{rejection.id}",
                        "rejection_reason": rejection.reason.value,
                        "rejection_details": rejection.details or "",
                        "rejection_category": rejection.category or "",
                        "timestamp": rejection.timestamp.isoformat()
                        if rejection.timestamp
                        else "",
                        "element_type": rejection.element_type.value,
                        "element_id": rejection.element_id,
                    },
                }
                features.append(feature)

        # Create GeoJSON structure
        geojson = {"type": "FeatureCollection", "features": features}

        logger.info(
            f"Generated GeoJSON report with {len(features)} features from {len(self.rejected_elements)} rejected elements"
        )
        return geojson

    def _map_rejection_to_type(self, reason: RejectionReason) -> str:
        """
        Map rejection reasons to GeoJSON feature types.

        Parameters
        ----------
        reason : RejectionReason
            The rejection reason

        Returns
        -------
        str
            Feature type string for GeoJSON properties
        """
        # Create a mapping from rejection reasons to meaningful types
        reason_to_type = {
            RejectionReason.INVALID_ELEMENT_TYPE: "invalid_power_element",
            RejectionReason.COORDINATES_NOT_FOUND: "missing_coordinates",
            RejectionReason.MISSING_TECHNOLOGY_TAG: "missing_technology",
            RejectionReason.MISSING_TECHNOLOGY_TYPE: "unknown_technology",
            RejectionReason.MISSING_SOURSE_TAG: "missing_source",
            RejectionReason.MISSING_SOURCE_TYPE: "unknown_source",
            RejectionReason.CAPACITY_PLACEHOLDER: "placeholder_capacity",
            RejectionReason.MISSING_OUTPUT_TAG: "missing_capacity",
            RejectionReason.MISSING_NAME_TAG: "missing_name",
            RejectionReason.CAPACITY_REGEX_NO_MATCH: "unparseable_capacity",
            RejectionReason.ESTIMATION_METHOD_UNKNOWN: "estimation_failed",
            RejectionReason.CAPACITY_DECIMAL_FORMAT: "invalid_capacity_format",
            RejectionReason.CAPACITY_REGEX_ERROR: "capacity_parsing_error",
            RejectionReason.CAPACITY_NON_NUMERIC: "non_numeric_capacity",
            RejectionReason.CAPACITY_UNSUPPORTED_UNIT: "unsupported_capacity_unit",
            RejectionReason.CAPACITY_ZERO: "zero_capacity",
            RejectionReason.ELEMENT_ALREADY_PROCESSED: "duplicate_element",
            RejectionReason.WITHIN_EXISTING_PLANT: "generator_within_plant",
            RejectionReason.INVALID_START_DATE_FORMAT: "invalid_date",
            RejectionReason.MISSING_START_DATE_TAG: "missing_date",
            RejectionReason.OTHER: "other_rejection",
        }

        return reason_to_type.get(reason, "unknown_rejection")

    def save_geojson_report(self, filepath: str) -> None:
        """
        Generate and save a GeoJSON report to file.

        Parameters
        ----------
        filepath : str
            Path to save the GeoJSON file

        Examples
        --------
        >>> tracker = RejectionTracker()
        >>> # ... add rejections with coordinates ...
        >>> tracker.save_geojson_report('rejections.geojson')
        """
        geojson_data = self.generate_geojson_report()

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(geojson_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved GeoJSON rejection report to {filepath}")

    def generate_report(self) -> "pd.DataFrame":
        """
        Generate a detailed report of rejections as a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            DataFrame containing all rejections, sorted by details, reason, timestamp, and ID
        """
        import pandas as pd

        # Create a list to store all rejection data
        rejection_data = []

        # Process all rejection entries
        for element_id, rejections in self.rejected_elements.items():
            for rejection in rejections:
                # Extract coordinates if available
                lat, lon = None, None
                if rejection.coordinates:
                    lat, lon = rejection.coordinates

                rejection_data.append(
                    {
                        "id": rejection.id,
                        "element_id": rejection.element_id,
                        "element_type": rejection.element_type.value,
                        "reason": rejection.reason.value,
                        "details": rejection.details if rejection.details else "",
                        "category": rejection.category if rejection.category else "",
                        "country": rejection.country,
                        "timestamp": rejection.timestamp,
                        "url": rejection.url,
                        "lat": lat,
                        "lon": lon,
                    }
                )

        # If no rejections, return empty DataFrame with appropriate columns
        if not rejection_data:
            return pd.DataFrame(
                columns=[
                    "id",
                    "element_id",
                    "element_type",
                    "reason",
                    "details",
                    "category",
                    "country",
                    "timestamp",
                    "url",
                    "lat",
                    "lon",
                ]
            )

        # Convert to DataFrame
        df = pd.DataFrame(rejection_data)
        df = df.sort_values(by=["timestamp", "id"], ascending=[True, True])

        return df

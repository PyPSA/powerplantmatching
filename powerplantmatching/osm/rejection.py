import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from .models import ElementType, RejectionReason, Unit

logger = logging.getLogger(__name__)


@dataclass
class RejectedElement:
    id: str
    element_id: str
    element_type: ElementType
    reason: RejectionReason
    details: str | None = None
    keywords: str = "none"  # Structured data for pattern analysis
    timestamp: datetime | None = None
    url: str | None = None
    coordinates: tuple[float, float] | None = None  # (lat, lon)
    country: str | None = None  # Country where the element is located
    unit_type: str | None = None  # "plant" or "generator" from power tag

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

    def add_rejection(
        self,
        element: dict[str, Any] | None = None,
        element_id: str | None = None,
        element_type: str | None = None,
        reason: RejectionReason | None = None,
        details: str | None = None,
        keywords: str = "none",
        coordinates: tuple[float, float] | None = None,
        country: str | None = None,
        unit_type: str | None = None,
    ) -> None:
        """
        Add a rejected element to the tracker.

        Parameters
        ----------
        element : dict[str, Any] | None
            The OSM element being rejected (preferred way)
        element_id : str | None
            Identifier for the element (legacy parameter)
        element_type : str | None
            Type of the element (legacy parameter)
        reason : RejectionReason
            Reason for rejection
        details : Optional[str]
            Additional details about the rejection
        keywords : str
            Actual keywords extracted from the element
        coordinates : Optional[tuple[float, float]]
            Latitude and longitude coordinates (extracted from element if not provided)
        country : Optional[str]
            Country where element is located (extracted from element if not provided)
        unit_type : Optional[str]
            Power type ("plant" or "generator") extracted from tags if not provided
        """
        # If element is provided, extract data from it
        if element is not None:
            element_id = element.get("id") if element_id is None else element_id
            element_type = element.get("type") if element_type is None else element_type

            # Extract country from element if not explicitly provided
            if country is None:
                country = element.get("_country")

            # Extract power type from tags if not explicitly provided
            if unit_type is None and "tags" in element:
                tags = element.get("tags", {})
                power_tag = tags.get("power")
                if power_tag in ["plant", "generator"]:
                    unit_type = power_tag

            # Extract coordinates if not provided and element has them
            if coordinates is None:
                if element_type == "node" and "lat" in element and "lon" in element:
                    coordinates = (element["lat"], element["lon"])
                elif element_type in ["way", "relation"]:
                    # For ways and relations, check if coordinates were pre-computed and stored
                    if "_lat" in element and "_lon" in element:
                        coordinates = (element["_lat"], element["_lon"])

        # Validate required parameters
        if element_id is None or element_type is None or reason is None:
            raise ValueError("element_id, element_type, and reason are required")

        identification = f"{element_type}/{element_id}"

        rejected = RejectedElement(
            id=identification,
            element_id=element_id,
            element_type=ElementType(element_type),
            reason=reason,
            details=details,
            keywords=keywords,
            coordinates=coordinates,
            country=country,
            unit_type=unit_type,
        )

        # Add to main list
        if identification not in self.rejected_elements:
            self.rejected_elements[identification] = []

        # Check if this exact rejection already exists to prevent duplicates
        duplicate_found = False
        for existing_rejection in self.rejected_elements[identification]:
            if (
                existing_rejection.reason == reason
                and existing_rejection.details == details
                and existing_rejection.keywords == keywords
            ):
                duplicate_found = True
                logger.debug(
                    f"Duplicate rejection ignored for {identification}: {reason.value}"
                )
                break

        if not duplicate_found:
            self.rejected_elements[identification].append(rejected)
            # Add to id set
            self.ids.add(rejected.id)
            # Log rejection
            logger.debug(
                f"Rejected element {element_id}: {reason.value} - {details or ''}"
            )

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

    def delete_for_units(self, units: list[Unit]) -> int:
        """
        Delete rejections for a list of units.

        Parameters
        ----------
        units : list[Unit]
            List of Unit objects to remove rejections for

        Returns
        -------
        int
            Number of rejections successfully deleted
        """
        deleted = 0
        for unit in units:
            if hasattr(unit, "id") and unit.id is not None:
                success = self.delete_rejection(unit.id)
                if success:
                    deleted += 1
            else:
                logger.warning(f"Unit {unit} has None id, skipping rejection deletion")

        logger.info(f"Deleted {deleted} rejections for {len(units)} units")
        return deleted

    def get_summary(self) -> dict[str, int]:
        """
        Get a summary of rejection counts by reason

        Returns
        -------
        dict[str, int]
            Summary of rejections by reason
        """
        summary = {}
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                reason_value = rejection.reason.value
                summary[reason_value] = summary.get(reason_value, 0) + 1

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

    def get_summary_string(self) -> str:
        """
        Get a formatted string summary of all rejections.

        Returns
        -------
        str
            Formatted summary string suitable for logging or display
        """
        summary = self.get_summary()
        if not summary:
            return "No rejections recorded"

        lines = ["Rejection Summary:"]
        total = sum(summary.values())
        lines.append(f"Total rejections: {total}")
        lines.append("-" * 40)

        for reason, count in sorted(summary.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total) * 100
            lines.append(f"{reason}: {count} ({percentage:.1f}%)")

        return "\n".join(lines)

    def get_statistics(self) -> dict[str, Any]:
        """
        Get comprehensive statistics about rejections.

        Returns
        -------
        dict[str, Any]
            dictionary containing various statistics
        """
        total_rejections = self.get_total_count()
        by_reason = self.get_summary()
        by_country = self.get_country_statistics()

        # Count rejections with/without coordinates
        with_coords = 0
        without_coords = 0
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                if rejection.coordinates is not None:
                    with_coords += 1
                else:
                    without_coords += 1

        return {
            "total_rejections": total_rejections,
            "by_reason": by_reason,
            "by_country": by_country,
            "unique_reasons": len(self.get_unique_rejection_reasons()),
            "unique_countries": len(set(by_country.keys())),
            "has_coordinates": with_coords,
            "missing_coordinates": without_coords,
            "percentage_with_coordinates": (with_coords / total_rejections * 100)
            if total_rejections > 0
            else 0,
        }

    def get_country_statistics(self) -> dict[str, int]:
        """
        Get statistics about rejections by country

        Returns
        -------
        dict[str, int]
            dictionary mapping country names to rejection counts
        """
        stats = {}
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                country = rejection.country or "Unknown"
                stats[country] = stats.get(country, 0) + 1
        return dict(sorted(stats.items(), key=lambda x: x[1], reverse=True))

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

    def generate_geojson(
        self, reason: Optional[RejectionReason] = None
    ) -> dict[str, Any]:
        """
        Generate a GeoJSON report of rejections with coordinates.

        Parameters
        ----------
        reason : Optional[RejectionReason]
            If provided, filter to only this rejection reason

        Returns
        -------
        dict[str, Any]
            GeoJSON FeatureCollection containing rejected elements with locations
        """
        features = []

        # Get rejections to process
        if reason is not None:
            rejections_to_process = self.get_rejections_by_reason(reason)
        else:
            # Flatten all rejections
            rejections_to_process = []
            for rejections in self.rejected_elements.values():
                rejections_to_process.extend(rejections)

        # Process rejections
        for rejection in rejections_to_process:
            # Skip elements without coordinates or cluster elements
            if rejection.coordinates is None or "cluster" in rejection.id.lower():
                continue

            lat, lon = rejection.coordinates
            if lat is None or lon is None:
                continue

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat],  # GeoJSON uses [lon, lat] order
                },
                "properties": {
                    "label": f"Rejected {rejection.element_type.value} {rejection.element_id}",
                    "type": rejection.reason.value,
                    "osm_element": f"https://www.openstreetmap.org/{rejection.id}",
                    "rejection_reason": rejection.reason.value,
                    "rejection_details": rejection.details or "",
                    "rejection_keywords": rejection.keywords or "none",
                    "timestamp": rejection.timestamp.isoformat()
                    if rejection.timestamp
                    else "",
                    "element_type": rejection.element_type.value,
                    "element_id": rejection.element_id,
                    "country": rejection.country,
                    "unit_type": rejection.unit_type,
                },
            }
            features.append(feature)

        # Create GeoJSON structure
        geojson = {"type": "FeatureCollection", "features": features}

        logger.info(
            f"Generated GeoJSON report {'for ' + reason.value if reason else ''} "
            f"with {len(features)} features"
        )
        return geojson

    def save_geojson(
        self, filepath: str, reason: Optional[RejectionReason] = None
    ) -> None:
        """
        Generate and save a GeoJSON report to file.

        Parameters
        ----------
        filepath : str
            Path to save the GeoJSON file
        reason : Optional[RejectionReason]
            If provided, filter to only this rejection reason
        """
        geojson_data = self.generate_geojson(reason)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(geojson_data, f, indent=2, ensure_ascii=False)

        feature_count = len(geojson_data["features"])
        reason_str = f" for '{reason.value}'" if reason else ""
        logger.info(
            f"Saved{reason_str} rejections GeoJSON to {filepath} ({feature_count} features)"
        )

    def save_geojson_by_reasons(
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
        stats = self.get_summary()

        logger.info(
            f"Generating GeoJSON files for {len(unique_reasons)} rejection reasons..."
        )

        for reason in unique_reasons:
            # Create safe filename
            reason_name = reason.value.lower().replace(" ", "_").replace("/", "_")
            filename = f"{prefix}_{reason_name}.geojson"
            filepath = os.path.join(output_dir, filename)

            # Save GeoJSON for this reason
            self.save_geojson(filepath, reason)

            # Log statistics
            count = stats.get(reason.value, 0)
            print(f"  - {reason.value}: {count} rejections → {filename}")

    def generate_report(self) -> pd.DataFrame:
        """
        Generate a detailed report of rejections as a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            DataFrame containing all rejections, sorted by timestamp and ID
        """
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
                        "country": rejection.country,
                        "unit_type": rejection.unit_type,
                        "reason": rejection.reason.value,
                        "keywords": rejection.keywords,
                        "details": rejection.details if rejection.details else "",
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
                    "country",
                    "unit_type",
                    "reason",
                    "keywords",
                    "details",
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

    def get_unique_keyword(
        self,
        reason: RejectionReason,
        country: str | None = None,
    ) -> dict[str, int]:
        """
        Get unique keyword values for a specific rejection reason.

        Parameters
        ----------
        reason : RejectionReason
            Filter by rejection reason
        country : Optional[str]
            Filter by country

        Returns
        -------
        dict[str, int]
            Dictionary mapping unique keywords to their counts, sorted by count descending

        Examples
        --------
        >>> # Get all unique values that caused capacity placeholder rejections
        >>> placeholders = tracker.get_unique_keyword(RejectionReason.CAPACITY_PLACEHOLDER)
        >>> # Returns: {"yes": 45, "true": 23, "1": 12}

        >>> # Get unique missing source types in Germany
        >>> sources = tracker.get_unique_keyword(RejectionReason.MISSING_SOURCE_TYPE, country="DE")
        >>> # Returns: {"biomass": 45, "tidal": 23, "wave": 12}
        """
        value_counts = {}

        for element_id, rejections in self.rejected_elements.items():
            for rejection in rejections:
                # Apply filters
                if rejection.reason != reason:
                    continue
                if country and rejection.country != country:
                    continue

                # Check keywords
                if rejection.keywords and rejection.keywords != "none":
                    value = rejection.keywords
                    value_counts[value] = value_counts.get(value, 0) + 1

        # Sort by count descending
        return dict(sorted(value_counts.items(), key=lambda x: x[1], reverse=True))

    def filter_rejections(
        self, reason: Optional[RejectionReason] = None, country: Optional[str] = None
    ) -> list[RejectedElement]:
        """
        Filter rejections by various criteria.

        Parameters
        ----------
        reason : Optional[RejectionReason]
            Filter by rejection reason
        country : Optional[str]
            Filter by country

        Returns
        -------
        list[RejectedElement]
            Filtered list of rejected elements
        """
        filtered = []
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                if reason and rejection.reason != reason:
                    continue
                if country and rejection.country != country:
                    continue
                filtered.append(rejection)
        return filtered

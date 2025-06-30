"""Quality tracking for OSM power plant data processing.

This module provides comprehensive tracking of rejected OSM elements,
helping to identify data quality issues and generate reports for
OpenStreetMap contributors to improve the data.

Key components:
    RejectedElement: Data structure for a single rejection
    RejectionTracker: Main tracking and analysis system
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from powerplantmatching.osm.models import ElementType, RejectionReason, Unit
from powerplantmatching.osm.utils import standardize_country_name

logger = logging.getLogger(__name__)


@dataclass
class RejectedElement:
    """Information about a rejected OSM element.

    Stores details about why an OSM element was rejected during processing,
    including location, reason, and metadata for analysis and reporting.

    Attributes
    ----------
    id : str
        Combined identifier (e.g., "node/123456")
    element_id : str
        OSM element ID
    element_type : ElementType
        Type of OSM element (node, way, relation)
    reason : RejectionReason
        Reason for rejection
    details : str, optional
        Additional details about the rejection
    keywords : str
        Keywords from the rejected value (default: "none")
    timestamp : datetime, optional
        When the rejection occurred
    url : str, optional
        OpenStreetMap URL for the element
    coordinates : tuple[float, float], optional
        (latitude, longitude) if available
    country : str, optional
        Country where element is located
    unit_type : str, optional
        Type of power unit (plant or generator)
    """

    id: str
    element_id: str
    element_type: ElementType
    reason: RejectionReason
    details: str | None = None
    keywords: str = "none"
    timestamp: datetime | None = None
    url: str | None = None
    coordinates: tuple[float, float] | None = None
    country: str | None = None
    unit_type: str | None = None

    def __post_init__(self):
        """Initialize timestamp and URL if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.url is None:
            if not "cluster" in self.id:
                self.url = f"https://www.openstreetmap.org/{self.id}"


class RejectionTracker:
    """Tracks and analyzes rejected OSM elements.

    Provides comprehensive tracking of data quality issues, with methods
    for analysis, reporting, and export. Helps identify patterns in
    rejected data to improve OpenStreetMap quality.

    Attributes
    ----------
    rejected_elements : dict[str, list[RejectedElement]]
        Rejected elements grouped by ID
    ids : set[str]
        Set of all rejection IDs for quick lookup

    Examples
    --------
    >>> tracker = RejectionTracker()
    >>> tracker.add_rejection(
    ...     element_id="123456",
    ...     element_type="node",
    ...     reason=RejectionReason.MISSING_CAPACITY_TAG,
    ...     details="No plant:output:electricity tag"
    ... )
    >>> print(tracker.get_summary_string())
    Rejection Summary:
    Total rejections: 1
    ----------------------------------------
    Missing capacity tag: 1 (100.0%)
    """

    def __init__(self):
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
        """Add a rejection record for an OSM element.

        Can accept either a full element dict or individual parameters.
        Extracts metadata from element if provided.

        Parameters
        ----------
        element : dict, optional
            OSM element data (extracts id, type, coordinates, etc.)
        element_id : str, optional
            Element ID (required if element not provided)
        element_type : str, optional
            Element type (required if element not provided)
        reason : RejectionReason, optional
            Reason for rejection (required)
        details : str, optional
            Additional details about rejection
        keywords : str
            Keywords from rejected value
        coordinates : tuple[float, float], optional
            (lat, lon) coordinates
        country : str, optional
            Country code or name
        unit_type : str, optional
            'plant' or 'generator'

        Raises
        ------
        ValueError
            If required parameters are missing
        """
        if element is not None:
            element_id = element.get("id") if element_id is None else element_id
            element_type = element.get("type") if element_type is None else element_type

            if country is None:
                country = element.get("_country")

            if unit_type is None and "tags" in element:
                tags = element.get("tags", {})
                power_tag = tags.get("power")
                if power_tag in ["plant", "generator"]:
                    unit_type = power_tag

            if coordinates is None:
                if element_type == "node" and "lat" in element and "lon" in element:
                    coordinates = (element["lat"], element["lon"])
                elif element_type in ["way", "relation"]:
                    if "_lat" in element and "_lon" in element:
                        coordinates = (element["_lat"], element["_lon"])

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
            country=standardize_country_name(country) if country else None,
            unit_type=unit_type,
        )

        if identification not in self.rejected_elements:
            self.rejected_elements[identification] = []

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
            self.ids.add(rejected.id)
            logger.debug(
                f"Rejected element {element_id}: {reason.value} - {details or ''}"
            )

    def delete_rejection(self, id: str) -> bool:
        """Delete a rejection by ID."""
        success = False
        if id in self.rejected_elements:
            del self.rejected_elements[id]
            success = True
            logger.debug(f"Deleted rejection with ID: {id}")
        else:
            logger.debug(f"Rejection with ID {id} not found for deletion.")

        if id in self.ids:
            self.ids.remove(id)

        return success

    def delete_for_units(self, units: list[Unit]) -> int:
        """Delete rejections for successfully processed units.

        Parameters
        ----------
        units : list[Unit]
            Units that were successfully processed

        Returns
        -------
        int
            Number of rejections deleted
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
        """Get count of rejections by reason.

        Returns
        -------
        dict[str, int]
            Mapping of rejection reason to count
        """
        summary = {}
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                reason_value = rejection.reason.value
                summary[reason_value] = summary.get(reason_value, 0) + 1

        return summary

    def get_total_count(self) -> int:
        """Get total number of rejected elements."""
        return len(self.rejected_elements)

    def get_summary_string(self) -> str:
        """Generate human-readable summary of rejections.

        Returns
        -------
        str
            Formatted summary with counts and percentages
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
        """Calculate comprehensive statistics about rejections.

        Returns
        -------
        dict
            Statistics including totals, breakdowns by reason/country,
            and coordinate coverage
        """
        total_rejections = self.get_total_count()
        by_reason = self.get_summary()
        by_country = self.get_country_statistics()

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
        """Get rejection counts by country."""
        stats = {}
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                country = rejection.country or "Unknown"
                stats[country] = stats.get(country, 0) + 1
        return dict(sorted(stats.items(), key=lambda x: x[1], reverse=True))

    def get_unique_rejection_reasons(self) -> list[RejectionReason]:
        """Get list of all unique rejection reasons."""
        reasons = set()
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                reasons.add(rejection.reason)
        return sorted(list(reasons), key=lambda x: x.value)

    def get_rejections_by_reason(
        self, reason: RejectionReason
    ) -> list[RejectedElement]:
        """Get all rejections for a specific reason."""
        filtered_rejections = []
        for rejections in self.rejected_elements.values():
            for rejection in rejections:
                if rejection.reason == reason:
                    filtered_rejections.append(rejection)
        return filtered_rejections

    def generate_geojson(
        self, reason: Optional[RejectionReason] = None
    ) -> dict[str, Any]:
        """Generate GeoJSON for visualization in mapping tools.

        Creates a FeatureCollection with rejected elements that have
        coordinates. Useful for JOSM or iD editor to fix issues.

        Parameters
        ----------
        reason : RejectionReason, optional
            Filter to specific rejection reason

        Returns
        -------
        dict
            GeoJSON FeatureCollection

        Notes
        -----
        Only includes rejections with valid coordinates.
        Excludes cluster rejections (synthetic elements).
        """
        features = []

        if reason is not None:
            rejections_to_process = self.get_rejections_by_reason(reason)
        else:
            rejections_to_process = []
            for rejections in self.rejected_elements.values():
                rejections_to_process.extend(rejections)

        for rejection in rejections_to_process:
            if rejection.coordinates is None or "cluster" in rejection.id.lower():
                continue

            lat, lon = rejection.coordinates
            if lat is None or lon is None:
                continue

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat],
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

        geojson = {"type": "FeatureCollection", "features": features}

        logger.info(
            f"Generated GeoJSON report {'for ' + reason.value if reason else ''} "
            f"with {len(features)} features"
        )
        return geojson

    def save_geojson(
        self, filepath: str, reason: Optional[RejectionReason] = None
    ) -> None:
        """Save rejections as GeoJSON file.

        Parameters
        ----------
        filepath : str
            Path to save GeoJSON file
        reason : RejectionReason, optional
            Filter to specific rejection reason
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
        """Save separate GeoJSON files for each rejection reason.

        Useful for OSM contributors to focus on specific issues.

        Parameters
        ----------
        output_dir : str
            Directory to save files
        prefix : str
            Filename prefix (default: "rejections")

        Notes
        -----
        Creates files like: rejections_missing_name_tag.geojson
        """
        import os

        unique_reasons = self.get_unique_rejection_reasons()

        stats = self.get_summary()

        logger.info(
            f"Generating GeoJSON files for {len(unique_reasons)} rejection reasons..."
        )

        for reason in unique_reasons:
            reason_name = reason.value.lower().replace(" ", "_").replace("/", "_")
            filename = f"{prefix}_{reason_name}.geojson"
            filepath = os.path.join(output_dir, filename)

            self.save_geojson(filepath, reason)

            count = stats.get(reason.value, 0)
            print(f"  - {reason.value}: {count} rejections → {filename}")

    def generate_report(self) -> pd.DataFrame:
        """Generate detailed DataFrame report of all rejections.

        Returns
        -------
        pd.DataFrame
            Report with columns: id, element_id, element_type, country,
            unit_type, reason, keywords, details, timestamp, url, lat, lon
        """
        rejection_data = []

        for element_id, rejections in self.rejected_elements.items():
            for rejection in rejections:
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

        df = pd.DataFrame(rejection_data)
        df = df.sort_values(by=["timestamp", "id"], ascending=[True, True])

        return df

    def get_unique_keyword(
        self,
        reason: RejectionReason,
        country: str | None = None,
    ) -> dict[str, int]:
        """Get unique keywords for a specific rejection reason.

        Useful for identifying problematic tag values.

        Parameters
        ----------
        reason : RejectionReason
            Reason to filter by
        country : str, optional
            Country to filter by

        Returns
        -------
        dict[str, int]
            Keywords and their counts, sorted by frequency
        """
        value_counts = {}

        for element_id, rejections in self.rejected_elements.items():
            for rejection in rejections:
                if rejection.reason != reason:
                    continue
                if country and rejection.country != country:
                    continue

                if rejection.keywords and rejection.keywords != "none":
                    value = rejection.keywords
                    value_counts[value] = value_counts.get(value, 0) + 1

        return dict(sorted(value_counts.items(), key=lambda x: x[1], reverse=True))

    def filter_rejections(
        self, reason: Optional[RejectionReason] = None, country: Optional[str] = None
    ) -> list[RejectedElement]:
        """Filter rejections by reason and/or country.

        Parameters
        ----------
        reason : RejectionReason, optional
            Filter by rejection reason
        country : str, optional
            Filter by country

        Returns
        -------
        list[RejectedElement]
            Filtered list of rejections
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

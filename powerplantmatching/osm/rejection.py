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
    keywords: dict[str, Any] | None = None  # Structured data for pattern analysis
    timestamp: datetime | None = None
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

    def add_rejection(
        self,
        element: dict[str, Any] | None = None,
        element_id: str | None = None,
        element_type: str | None = None,
        reason: RejectionReason | None = None,
        details: str | None = None,
        keywords: dict[str, Any] | None = None,
        coordinates: tuple[float, float] | None = None,
        country: str | None = None,
    ) -> None:
        """
        Add a rejected element to the tracker.

        Can be called in two ways:
        1. With element dict: add_rejection(element=elem, reason=..., details=...)
        2. Legacy way: add_rejection(element_id=..., element_type=..., reason=..., ...)

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
        keywords : Optional[dict[str, Any]]
            Structured data for pattern analysis (e.g., tag names, values, error types)
        coordinates : Optional[tuple[float, float]]
            Latitude and longitude coordinates (extracted from element if not provided)
        country : Optional[str]
            Country where element is located (extracted from element if not provided)
        """
        # If element is provided, extract data from it
        if element is not None:
            element_id = element.get("id") if element_id is None else element_id
            element_type = element.get("type") if element_type is None else element_type

            # Extract country from element if not explicitly provided
            if country is None:
                country = element.get("_country")

            # Extract coordinates if not provided and element has them
            if coordinates is None:
                if element_type == "node" and "lat" in element and "lon" in element:
                    coordinates = (element["lat"], element["lon"])

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
        )

        # Add to main list
        if identification not in self.rejected_elements:
            self.rejected_elements[identification] = []
        self.rejected_elements[identification].append(rejected)

        # Add to id set
        self.ids.add(rejected.id)

        # Log rejection
        logger.debug(f"Rejected element {element_id}: {reason.value} - {details or ''}")

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

    def delete_rejections_for_units(self, units: list[Unit]) -> int:
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

    def get_rejection_summary_string(self) -> str:
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

    def clear_rejections_by_reason(self, reason: RejectionReason) -> int:
        """
        Clear all rejections of a specific type.

        Parameters
        ----------
        reason : RejectionReason
            The rejection reason to clear

        Returns
        -------
        int
            Number of rejections cleared
        """
        rejections_to_clear = self.get_rejections_by_reason(reason)
        cleared_count = 0

        for rejection in rejections_to_clear:
            if self.delete_rejection(rejection.id):
                cleared_count += 1

        logger.info(f"Cleared {cleared_count} rejections of type {reason.value}")
        return cleared_count

    def get_rejection_statistics(self) -> dict[str, Any]:
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
            "unique_countries": len(self.get_unique_countries()),
            "has_coordinates": with_coords,
            "missing_coordinates": without_coords,
            "percentage_with_coordinates": (with_coords / total_rejections * 100)
            if total_rejections > 0
            else 0,
        }

    def export_summary_to_file(
        self, filepath: str, include_statistics: bool = True
    ) -> None:
        """
        Export a comprehensive rejection summary to a text file.

        Parameters
        ----------
        filepath : str
            Path to save the summary file
        include_statistics : bool, default True
            Whether to include detailed statistics
        """
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("OSM REJECTION SUMMARY\n")
            f.write("=" * 80 + "\n\n")

            # Basic summary
            f.write(self.get_rejection_summary_string())
            f.write("\n\n")

            if include_statistics:
                # Detailed statistics
                stats = self.get_rejection_statistics()
                f.write("DETAILED STATISTICS\n")
                f.write("-" * 40 + "\n")
                f.write(f"Total rejections: {stats['total_rejections']}\n")
                f.write(f"Unique rejection reasons: {stats['unique_reasons']}\n")
                f.write(f"Unique countries: {stats['unique_countries']}\n")
                f.write(
                    f"Rejections with coordinates: {stats['has_coordinates']} ({stats['percentage_with_coordinates']:.1f}%)\n"
                )
                f.write(
                    f"Rejections without coordinates: {stats['missing_coordinates']}\n\n"
                )

                # By country
                f.write("REJECTIONS BY COUNTRY\n")
                f.write("-" * 40 + "\n")
                for country, count in sorted(
                    stats["by_country"].items(), key=lambda x: x[1], reverse=True
                )[:10]:
                    f.write(f"{country}: {count}\n")
                if len(stats["by_country"]) > 10:
                    f.write(f"... and {len(stats['by_country']) - 10} more countries\n")

        logger.info(f"Exported rejection summary to {filepath}")

    def get_all_rejections(self) -> dict[str, list[RejectedElement]]:
        """
        Get all rejected elements

        Returns
        -------
        list[RejectedElement]
            list of all rejected elements
        """
        return self.rejected_elements

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

    def clear(self) -> None:
        """Clear all rejection data"""
        self.rejected_elements = {}
        self.ids = set()

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
            dictionary mapping country names to rejection counts
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
                        "keywords": json.dumps(rejection.keywords)
                        if rejection.keywords
                        else "",
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
                    "keywords",
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

    def get_keyword_patterns(
        self,
        reason: Optional[RejectionReason] = None,
        country: Optional[str] = None,
        keyword_field: Optional[str] = None,
    ) -> dict[str, dict[str, Any]]:
        """
        Get rejection patterns based on keywords field.

        Parameters
        ----------
        reason : Optional[RejectionReason]
            Filter by rejection reason
        country : Optional[str]
            Filter by country
        keyword_field : Optional[str]
            Specific keyword field to analyze (e.g., "value", "keyword")

        Returns
        -------
        dict[str, dict[str, Any]]
            Pattern analysis based on keywords, sorted by count descending
        """
        from collections import defaultdict

        patterns = defaultdict(
            lambda: {"count": 0, "reasons": set(), "countries": set(), "examples": []}
        )

        for element_id, rejections in self.rejected_elements.items():
            for rejection in rejections:
                # Apply filters
                if reason and rejection.reason != reason:
                    continue
                if country and rejection.country != country:
                    continue

                # Skip if no keywords
                if not rejection.keywords:
                    continue

                # Create pattern key based on keywords
                if keyword_field and keyword_field in rejection.keywords:
                    # Analyze specific field
                    pattern_key = f"{keyword_field}={rejection.keywords[keyword_field]}"
                else:
                    # Use all keywords for pattern
                    sorted_keys = sorted(rejection.keywords.keys())
                    pattern_parts = []
                    for key in sorted_keys:
                        value = rejection.keywords[key]
                        if isinstance(value, (list, set)):
                            value = f"[{len(value)} items]"
                        pattern_parts.append(f"{key}={value}")
                    pattern_key = ", ".join(pattern_parts)

                # Update pattern data
                pattern_info = patterns[pattern_key]
                pattern_info["count"] += 1
                pattern_info["reasons"].add(rejection.reason.value)
                if rejection.country:
                    pattern_info["countries"].add(rejection.country)

                # Add example
                if len(pattern_info["examples"]) < 3:
                    pattern_info["examples"].append(
                        {
                            "element_id": rejection.element_id,
                            "element_type": rejection.element_type.value,
                            "keywords": rejection.keywords,
                            "details": rejection.details,
                            "url": rejection.url,
                        }
                    )

        # Convert sets to lists
        for pattern_data in patterns.values():
            pattern_data["reasons"] = sorted(list(pattern_data["reasons"]))
            pattern_data["countries"] = sorted(list(pattern_data["countries"]))

        # Sort by count
        sorted_patterns = dict(
            sorted(patterns.items(), key=lambda x: x[1]["count"], reverse=True)
        )

        return sorted_patterns

    def get_unique_values_by_keyword(
        self,
        keyword: str,
        reason: Optional[RejectionReason] = None,
        country: Optional[str] = None,
    ) -> dict[str, int]:
        """
        Get unique values for a specific keyword across rejections.

        Parameters
        ----------
        keyword : str
            The keyword field to analyze (e.g., "source_value", "tag_key")
        reason : Optional[RejectionReason]
            Filter by rejection reason
        country : Optional[str]
            Filter by country

        Returns
        -------
        dict[str, int]
            dictionary mapping unique values to their counts, sorted by count descending

        Examples
        --------
        >>> # Get all unique source values that caused rejections
        >>> sources = tracker.get_unique_values_by_keyword("source_value")
        >>> # Returns: {"biomass": 45, "tidal": 23, "wave": 12}
        """
        value_counts = {}

        for element_id, rejections in self.rejected_elements.items():
            for rejection in rejections:
                # Apply filters
                if reason and rejection.reason != reason:
                    continue
                if country and rejection.country != country:
                    continue

                # Check keywords
                if rejection.keywords and keyword in rejection.keywords:
                    value = str(rejection.keywords[keyword])
                    value_counts[value] = value_counts.get(value, 0) + 1

        # Sort by count
        return dict(sorted(value_counts.items(), key=lambda x: x[1], reverse=True))

    def get_rejection_summary_by_keywords(
        self,
        reason: Optional[RejectionReason] = None,
        country: Optional[str] = None,
        top_n: int = 10,
    ) -> str:
        """
        Get a summary of rejections grouped by keyword patterns.

        Parameters
        ----------
        reason : Optional[RejectionReason]
            Filter by rejection reason
        country : Optional[str]
            Filter by country
        top_n : int, default 10
            Number of top patterns to show

        Returns
        -------
        str
            Formatted summary string
        """
        patterns = self.get_keyword_patterns(reason, country)

        lines = ["Rejection Summary by Keywords"]
        lines.append("=" * 60)

        if reason:
            lines.append(f"Reason filter: {reason.value}")
        if country:
            lines.append(f"Country filter: {country}")

        lines.append(f"\nTotal patterns: {len(patterns)}")
        lines.append(f"Total rejections: {sum(p['count'] for p in patterns.values())}")
        lines.append("")

        # Show top patterns
        for i, (pattern, data) in enumerate(list(patterns.items())[:top_n], 1):
            lines.append(f"{i}. {pattern}")
            lines.append(f"   Count: {data['count']}")
            lines.append(f"   Reasons: {', '.join(data['reasons'])}")
            if data["countries"]:
                countries_str = ", ".join(data["countries"][:5])
                if len(data["countries"]) > 5:
                    countries_str += f" + {len(data['countries']) - 5} more"
                lines.append(f"   Countries: {countries_str}")
            lines.append("")

        if len(patterns) > top_n:
            lines.append(f"... and {len(patterns) - top_n} more patterns")

        return "\n".join(lines)

    def get_unique_keyword_values(
        self,
        keyword_field: str,
        reason: Optional[RejectionReason] = None,
        country: Optional[str] = None,
        error_type: Optional[str] = None,
        expected_type: Optional[str] = None,
        actual_type: Optional[str] = None,
        element_type: Optional[str] = None,
        include_none: bool = False,
        min_count: int = 1,
    ) -> dict[str, dict[str, Any]]:
        """
        Get unique values for a specific keyword field from thousands of rejections with advanced filtering.

        This method is designed to handle large-scale analysis of rejection data by extracting
        unique values from any keyword field while applying multiple filters.

        Parameters
        ----------
        keyword_field : str
            The keyword field to extract unique values from (e.g., "tag_key", "tag_value",
            "expected_type", "actual_type", "error_type", "comment":)
        reason : Optional[RejectionReason]
            Filter by specific rejection reason
        country : Optional[str]
            Filter by country code (e.g., "DE", "FR")
        error_type : Optional[str]
            Filter by error type keyword (e.g., "missing", "invalid_format", "placeholder")
        expected_type : Optional[str]
            Filter by expected type keyword (e.g., "numeric", "date", "technology")
        actual_type : Optional[str]
            Filter by actual type keyword (e.g., "missing", "placeholder", "invalid_format")
        element_type : Optional[str]
            Filter by element type keyword (e.g., "plant", "generator", "way", "node")
        include_none : bool, default False
            Whether to include None/null values in the results
        min_count : int, default 1
            Minimum count threshold for including a value in results

        Returns
        -------
        dict[str, dict[str, Any]]
            dictionary mapping unique values to their statistics:
            {
                "value": {
                    "count": int,
                    "reasons": List[str],
                    "countries": List[str],
                    "element_types": List[str],
                    "examples": List[dict]  # Up to 3 example rejections
                }
            }
            Sorted by count in descending order.

        Examples
        --------
        >>> # Get all unique tag keys that cause rejections
        >>> tag_keys = tracker.get_unique_keyword_values("tag_key")
        >>> # Returns: {"start_date": {"count": 8362, "reasons": ["Missing start date tag"], ...}}

        >>> # Get all placeholder values used in Germany
        >>> placeholders = tracker.get_unique_keyword_values(
        ...     "tag_value",
        ...     country="DE",
        ...     error_type="placeholder"
        ... )
        >>> # Returns: {"yes": {"count": 4625, ...}, "true": {"count": 12, ...}}

        >>> # Find all unknown technology values across all countries
        >>> unknown_tech = tracker.get_unique_keyword_values(
        ...     "tag_value",
        ...     reason=RejectionReason.MISSING_TECHNOLOGY_TYPE,
        ...     min_count=5  # Only show values appearing 5+ times
        ... )

        >>> # Analyze what types of data are missing
        >>> missing_types = tracker.get_unique_keyword_values(
        ...     "expected_type",
        ...     error_type="missing"
        ... )
        >>> # Returns: {"date": {"count": 8362, ...}, "name": {"count": 5759, ...}}
        """
        from collections import defaultdict

        # Initialize results structure
        value_stats = defaultdict(
            lambda: {
                "count": 0,
                "reasons": set(),
                "countries": set(),
                "element_types": set(),
                "examples": [],
            }
        )

        # Process all rejections
        for element_id, rejections in self.rejected_elements.items():
            for rejection in rejections:
                # Apply basic filters
                if reason and rejection.reason != reason:
                    continue
                if country and rejection.country != country:
                    continue

                # Skip if no keywords
                if not rejection.keywords:
                    continue

                # Apply keyword-based filters
                keywords = rejection.keywords

                if error_type and keywords.get("error_type") != error_type:
                    continue
                if expected_type and keywords.get("expected_type") != expected_type:
                    continue
                if actual_type and keywords.get("actual_type") != actual_type:
                    continue
                if element_type and keywords.get("element_type") != element_type:
                    continue

                # Extract the requested field value
                if keyword_field in keywords:
                    value = keywords[keyword_field]

                    # Handle None values
                    if value is None:
                        if not include_none:
                            continue
                        value = "None"

                    # Convert to string for consistency
                    value = str(value)

                    # Update statistics
                    stats = value_stats[value]
                    stats["count"] += 1
                    stats["reasons"].add(rejection.reason.value)
                    if rejection.country:
                        stats["countries"].add(rejection.country)
                    if "element_type" in keywords:
                        stats["element_types"].add(keywords["element_type"])

                    # Add example (limit to 3)
                    if len(stats["examples"]) < 3:
                        stats["examples"].append(
                            {
                                "element_id": rejection.element_id,
                                "reason": rejection.reason.value,
                                "country": rejection.country,
                                "details": rejection.details[:100] + "..."
                                if rejection.details and len(rejection.details) > 100
                                else rejection.details,
                                "keywords": keywords,
                            }
                        )

        # Convert sets to sorted lists and filter by min_count
        results = {}
        for value, stats in value_stats.items():
            if stats["count"] >= min_count:
                results[value] = {
                    "count": stats["count"],
                    "reasons": sorted(list(stats["reasons"])),
                    "countries": sorted(list(stats["countries"])),
                    "element_types": sorted(list(stats["element_types"])),
                    "examples": stats["examples"],
                }

        # Sort by count descending
        return dict(sorted(results.items(), key=lambda x: x[1]["count"], reverse=True))

    def analyze_keyword_fields(
        self,
        reason: Optional[RejectionReason] = None,
        country: Optional[str] = None,
        top_n: int = 10,
    ) -> dict[str, dict[str, int]]:
        """
        Analyze all keyword fields to understand data distribution.

        This method provides a comprehensive overview of what keyword fields are used
        and their most common values across rejections.

        Parameters
        ----------
        reason : Optional[RejectionReason]
            Filter by rejection reason
        country : Optional[str]
            Filter by country
        top_n : int, default 10
            Number of top values to show per field

        Returns
        -------
        dict[str, dict[str, int]]
            dictionary mapping keyword fields to their top values and counts

        Examples
        --------
        >>> analysis = tracker.analyze_keyword_fields(country="DE")
        >>> # Returns: {
        >>> #     "value": {"missing": 15234, "placeholder": 4625, ...},
        >>> #     "keyword": {"start_date": 8362, "plant:output:electricity": 5313, ...},
        >>> #     ...
        >>> # }
        """
        # Common keyword fields to analyze
        keyword_fields = ["keyword", "value", "comment"]

        results = {}

        for field in keyword_fields:
            # Get unique values for this field
            values = self.get_unique_keyword_values(
                field, reason=reason, country=country, include_none=True
            )

            # Take top N values
            top_values = {}
            for i, (value, stats) in enumerate(values.items()):
                if i >= top_n:
                    break
                top_values[value] = stats["count"]

            if top_values:
                results[field] = top_values

        return results

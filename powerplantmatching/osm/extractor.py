import logging
from typing import Any, Optional

from .models import ElementType, RejectionReason
from .rejection import RejectionTracker
from .utils import (
    get_capacity_extraction_config,
    parse_capacity_value,
)

logger = logging.getLogger(__name__)


class CapacityExtractor:
    """Handles extraction of capacity values from OSM element tags"""

    def __init__(
        self,
        config: dict[str, Any],
        rejection_tracker: Optional[RejectionTracker] = None,
    ):
        """
        Initialize the capacity extractor

        Parameters
        ----------
        config : dict[str, Any]
            Configuration for extraction
        rejection_tracker : Optional[RejectionTracker]
            Tracker for rejected elements
        """
        self.config = config
        self.rejection_tracker = rejection_tracker or RejectionTracker()

    def basic_extraction(self, element: dict[str, Any]) -> tuple[Optional[float], str]:
        """
        Perform basic capacity extraction (always attempted)

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        Returns
        -------
        tuple[Optional[float], str]
            (capacity_mw, capacity_source) or (None, "unknown")
        """
        if "tags" not in element:
            self.rejection_tracker.add_rejection(
                element_id=f"{element['type']}/{element['id']}",
                element_type=ElementType(element["type"]),
                reason=RejectionReason.MISSING_REQUIRED_FIELD,
                details="No tags found in element",
                category="basic_extraction",
            )
            return None, "unknown"

        tags = element.get("tags", {})

        standard_tags = self.config.get("capacity_extraction", {}).get(
            "tags", ["plant:output:electricity", "generator:output:electricity"]
        )
        for tag in standard_tags:
            if tag in tags:
                value_str = tags[tag]

                # Check for placeholder values (yes, true)
                if value_str.lower() in ["yes", "true"]:
                    self.rejection_tracker.add_rejection(
                        element_id=f"{element['type']}/{element['id']}",
                        element_type=ElementType(element["type"]),
                        reason=RejectionReason.CAPACITY_PLACEHOLDER,
                        details=f"Tag '{tag}' contains placeholder value '{value_str}'",
                        category="basic_extraction",
                    )
                    return None, "unknown"

                # Check for comma as decimal separator
                if "," in value_str and not "." in value_str:
                    self.rejection_tracker.add_rejection(
                        element_id=f"{element['type']}/{element['id']}",
                        element_type=ElementType(element["type"]),
                        reason=RejectionReason.CAPACITY_DECIMAL_FORMAT,
                        details=f"Capacity value '{value_str}' uses comma as decimal separator",
                        category="basic_extraction",
                    )
                    return None, "unknown"

                # Regular parsing attempt
                is_valid, value, unit_or_note = parse_capacity_value(
                    tags[tag], extraction=False
                )
                if is_valid and value > 0:
                    return value, "basic_extraction"
                else:
                    if not is_valid:
                        if unit_or_note == "value_error":
                            reason = RejectionReason.CAPACITY_NON_NUMERIC
                        elif unit_or_note == "unknown_unit":
                            reason = RejectionReason.CAPACITY_UNSUPPORTED_UNIT
                        elif unit_or_note == "placeholder_value":
                            reason = RejectionReason.CAPACITY_PLACEHOLDER
                        elif unit_or_note == "decimal_comma_format":
                            reason = RejectionReason.CAPACITY_DECIMAL_FORMAT
                        elif unit_or_note == "regex_no_match":
                            reason = RejectionReason.CAPACITY_REGEX_NO_MATCH
                        else:
                            reason = RejectionReason.OTHER

                        self.rejection_tracker.add_rejection(
                            element_id=f"{element['type']}/{element['id']}",
                            element_type=ElementType(element["type"]),
                            reason=reason,
                            details=f"Failed to parse capacity '{unit_or_note}' from tag '{tag}'",
                            category="basic_extraction",
                        )
                    return None, "unknown"

        # If we get here, no matching tags were found
        self.rejection_tracker.add_rejection(
            element_id=f"{element['type']}/{element['id']}",
            element_type=ElementType(element["type"]),
            reason=RejectionReason.CAPACITY_TAG_MISSING,
            details="No match between element tags and config capacity extraction tags",
            category="basic_extraction",
        )
        return None, "unknown"

    def advanced_extraction(
        self, element: dict[str, Any], source_type: Optional[str] = None
    ) -> tuple[Optional[float], str]:
        """
        Perform advanced capacity extraction (if enabled in config)

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        source_type : Optional[str]
            Type of power source

        Returns
        -------
        tuple[Optional[float], str]
            (capacity_mw, capacity_source) or (None, "unknown")
        """
        if "tags" not in element:
            self.rejection_tracker.add_rejection(
                element_id=f"{element['type']}/{element['id']}",
                element_type=ElementType(element["type"]),
                reason=RejectionReason.MISSING_REQUIRED_FIELD,
                details="No tags found in element",
                category="advanced_extraction",
            )
            return None, "unknown"

        tags = element.get("tags", {})

        # Get extraction config
        extraction_config = get_capacity_extraction_config(self.config, source_type)
        regex_patterns = extraction_config.get("advanced_extraction").get(
            "regex_patterns", [r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+p?)$"]
        )

        backup_tags = extraction_config.get("tags", [])
        if source_type:
            extraction_tags = extraction_config.get("additional_tags", backup_tags)
        else:
            extraction_tags = backup_tags

        # Try extracting from each tag
        for tag in extraction_tags:
            if tag in tags:
                try:
                    is_valid, value, unit = parse_capacity_value(
                        tags[tag], extraction=True, regex_patterns=regex_patterns
                    )
                    if is_valid and value > 0:
                        return value, f"tag_{tag}_from_{unit}_to_mw"
                except (ValueError, TypeError):
                    self.rejection_tracker.add_rejection(
                        element_id=f"{element['type']}/{element['id']}",
                        element_type=ElementType(element["type"]),
                        reason=RejectionReason.CAPACITY_VALUE_NOT_NUMERIC,
                        details=f"Failed to parse capacity from tag '{tag}': '{tags[tag]}'",
                        category="advanced_extraction",
                    )

        # If we get here, advanced extraction failed
        self.rejection_tracker.add_rejection(
            element_id=f"{element['type']}/{element['id']}",
            element_type=ElementType(element["type"]),
            reason=RejectionReason.CAPACITY_FORMAT_UNSUPPORTED,
            details="No valid advanced capacity format found in tags",
            category="advanced_extraction",
        )
        return None, "unknown"

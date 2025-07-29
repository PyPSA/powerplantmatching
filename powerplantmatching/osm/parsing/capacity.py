"""Capacity extraction from OpenStreetMap tags.

This module handles the extraction and parsing of power plant capacity
values from various OSM tag formats, including unit conversion and
validation.
"""

import logging
from typing import Any

from powerplantmatching.osm.models import RejectionReason
from powerplantmatching.osm.quality.rejection import RejectionTracker
from powerplantmatching.osm.utils import parse_capacity_value

logger = logging.getLogger(__name__)


class CapacityExtractor:
    """Extracts and validates capacity values from OSM tags.

    Handles various capacity formats including different units (W, kW, MW, GW),
    decimal separators, and placeholder values. Supports both basic and
    advanced extraction with configurable regex patterns.

    Attributes
    ----------
    config : dict
        Configuration with extraction settings
    rejection_tracker : RejectionTracker
        Tracks elements with invalid capacity values
    """

    def __init__(
        self,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any],
    ):
        """Initialize the capacity extractor.

        Parameters
        ----------
        rejection_tracker : RejectionTracker
            Tracker for rejected elements
        config : dict
            Configuration with regex patterns and settings
        """
        self.config = config
        self.rejection_tracker = rejection_tracker

    def basic_extraction(
        self, element: dict[str, Any], output_key: str
    ) -> tuple[bool, float | None, str]:
        """Perform basic capacity extraction with standard patterns.

        Parameters
        ----------
        element : dict
            OSM element containing tags
        output_key : str
            Tag key containing capacity value

        Returns
        -------
        is_valid : bool
            Whether extraction was successful
        value : float or None
            Extracted capacity in MW
        info : str
            Source/reason for the result

        Notes
        -----
        Basic extraction handles standard formats like "10 MW", "5.5 MWp".
        Rejects placeholder values like "yes" or "true".
        """
        value_str = element["tags"][output_key].strip()

        if value_str.lower() in ["yes", "true"]:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.CAPACITY_PLACEHOLDER,
                details=f"Tag '{output_key}' contains placeholder value '{value_str}' instead of actual capacity",
                keywords=value_str,
            )
            return False, None, "placeholder_value"

        if "," in value_str and not "." in value_str:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.CAPACITY_DECIMAL_FORMAT,
                details=f"Tag '{output_key}' uses comma as decimal separator in value '{value_str}'",
                keywords=value_str,
            )
            return False, None, "decimal_comma_format"

        is_valid, value, identifier = parse_capacity_value(
            value_str, advanced_extraction=False
        )
        return self.parse_and_track(element, output_key, is_valid, value, identifier)

    def advanced_extraction(
        self, element: dict[str, Any], output_key: str
    ) -> tuple[bool, float | None, str]:
        """Perform advanced capacity extraction with custom regex patterns.

        Parameters
        ----------
        element : dict
            OSM element containing tags
        output_key : str
            Tag key containing capacity value

        Returns
        -------
        is_valid : bool
            Whether extraction was successful
        value : float or None
            Extracted capacity in MW
        info : str
            Source/reason for the result

        Notes
        -----
        Advanced extraction uses configurable regex patterns to handle
        non-standard formats. Falls back to basic patterns if custom
        patterns are not configured.
        """
        value_str = element["tags"][output_key].strip()

        regex_patterns = self.config.get("capacity_extraction", {}).get(
            "regex_patterns", [r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+p?)$"]
        )

        try:
            is_valid, value, identifier = parse_capacity_value(
                value_str, advanced_extraction=True, regex_patterns=regex_patterns
            )
            return self.parse_and_track(
                element, output_key, is_valid, value, identifier
            )

        except Exception as e:
            logger.error(f"Error parsing capacity value '{value_str}' with regex: {e}")
            is_valid = False
            value = None
            identifier = "regex_error"
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.CAPACITY_REGEX_ERROR,
                details=f"Regex parsing failed for tag '{output_key}' with value '{value_str}': {str(e)}",
                keywords=value_str,
            )
            return False, None, identifier

    def parse_and_track(
        self,
        element: dict[str, Any],
        output_key: str,
        is_valid: bool,
        value: float | None,
        identifier: str,
    ) -> tuple[bool, float | None, str]:
        """Validate parsed capacity and track rejections.

        Parameters
        ----------
        element : dict
            OSM element for rejection tracking
        output_key : str
            Tag key being processed
        is_valid : bool
            Whether parsing was successful
        value : float or None
            Parsed capacity value
        identifier : str
            Reason/source identifier

        Returns
        -------
        is_valid : bool
            Final validation result
        value : float or None
            Validated capacity or None
        identifier : str
            Final identifier
        """
        if is_valid and value is not None:
            if value > 0:
                return True, value, identifier
            elif value <= 0:
                self.rejection_tracker.add_rejection(
                    element=element,
                    reason=RejectionReason.CAPACITY_ZERO,
                    details=f"Tag '{output_key}' parsed to zero capacity from value '{element['tags'][output_key]}'",
                    keywords=element["tags"][output_key],
                )
                return False, None, identifier

        elif not is_valid:
            if identifier == "value_error":
                reason = RejectionReason.CAPACITY_NON_NUMERIC
            elif identifier == "unknown_unit":
                reason = RejectionReason.CAPACITY_UNSUPPORTED_UNIT
            elif identifier == "regex_no_match":
                reason = RejectionReason.CAPACITY_REGEX_NO_MATCH
            elif identifier == "regex_error":
                reason = RejectionReason.CAPACITY_REGEX_ERROR
            else:
                reason = RejectionReason.OTHER

            self.rejection_tracker.add_rejection(
                element=element,
                reason=reason,
                details=f"Tag '{output_key}' has value '{element['tags'][output_key]}' which could not be parsed",
                keywords=element["tags"][output_key],
            )
            return False, None, identifier

        self.rejection_tracker.add_rejection(
            element=element,
            reason=RejectionReason.OTHER,
            details=f"Unexpected error parsing tag '{output_key}' with value '{element['tags'][output_key]}'",
            keywords=element["tags"][output_key],
        )
        return False, None, identifier

import logging
from typing import Any, Optional

from .models import RejectionReason
from .rejection import RejectionTracker
from .utils import parse_capacity_value

logger = logging.getLogger(__name__)


class CapacityExtractor:
    """Handles extraction of capacity values from OSM element tags"""

    def __init__(
        self,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any],
    ):
        """
        Initialize the capacity extractor

        Parameters
        ----------
        config : dict[str, Any]
            Configuration for extraction
        rejection_tracker : RejectionTracker
            Tracker for rejected elements
        """
        self.config = config
        self.rejection_tracker = rejection_tracker

    def basic_extraction(
        self, element: dict[str, Any], output_key: str
    ) -> tuple[bool, Optional[float], str]:
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
        value_str = element["tags"][output_key].strip()

        # Check for placeholder values (yes, true)
        if value_str.lower() in ["yes", "true"]:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.CAPACITY_PLACEHOLDER,
                details=f"Tag '{output_key}' contains placeholder value '{value_str}' instead of actual capacity",
                keywords={
                    "keyword": output_key,
                    "value": value_str,
                    "comment": None,  # No automatic suggestion for placeholders
                },
            )
            return False, None, "placeholder_value"

        # Check for comma as decimal separator
        if "," in value_str and not "." in value_str:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.CAPACITY_DECIMAL_FORMAT,
                details=f"Tag '{output_key}' uses comma as decimal separator in value '{value_str}'",
                keywords={
                    "keyword": output_key,
                    "value": value_str,
                    "comment": value_str.replace(",", "."),
                },
            )
            return False, None, "decimal_comma_format"

        is_valid, value, identifier = parse_capacity_value(
            value_str, advanced_extraction=False
        )
        return self.parse_and_track(element, output_key, is_valid, value, identifier)

    def advanced_extraction(
        self, element: dict[str, Any], output_key: str
    ) -> tuple[bool, Optional[float], str]:
        """
        Perform advanced capacity extraction using regex patterns

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data

        Returns
        -------
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
                keywords={
                    "keyword": output_key,
                    "value": value_str,
                    "comment": None,
                },
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
        """
        Parse capacity value and track rejections

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        output_key : str
            Key for the capacity value in the element's tags
        is_valid : bool
            Whether the capacity value is valid
        value : float
            Parsed capacity value
        identifier : str
            Identifier for rejection tracking
        function_name : str
            Name of the calling function

        Returns
        -------
        tuple[bool, Optional[float], str]
            (is_valid, capacity_mw, identifier)

        """
        if is_valid and value is not None:
            if value > 0:
                return True, value, identifier
            elif value <= 0:
                self.rejection_tracker.add_rejection(
                    element=element,
                    reason=RejectionReason.CAPACITY_ZERO,
                    details=f"Tag '{output_key}' parsed to zero capacity from value '{element['tags'][output_key]}'",
                    keywords={
                        "keyword": output_key,
                        "value": element["tags"][output_key],
                        "comment": None,
                    },
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
                keywords={
                    "keyword": output_key,
                    "value": element["tags"][output_key],
                    "comment": None,
                },
            )
            return False, None, identifier

        self.rejection_tracker.add_rejection(
            element=element,
            reason=RejectionReason.OTHER,
            details=f"Unexpected error parsing tag '{output_key}' with value '{element['tags'][output_key]}'",
            keywords={
                "keyword": output_key,
                "value": element["tags"][output_key],
                "comment": None,
            },
        )
        return False, None, identifier

import logging
from typing import Any, Optional

from .models import ElementType, RejectionReason
from .rejection import RejectionTracker
from .utils import parse_capacity_value

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

    def _validate_capacity_value(
        self,
        element: dict[str, Any],
        tag: str,
        value_str: str,
        category: str = "validation",
    ) -> tuple[bool, Optional[float], str]:
        """
        Validate a capacity value string and convert to float if valid

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        tag : str
            Tag name containing the capacity value
        value_str : str
            Value string to validate
        category : str
            Category for rejection tracking

        Returns
        -------
        tuple[bool, Optional[float], str]
            (is_valid, capacity_mw, identifier, details)
        """
        # Check for placeholder values (yes, true)
        if value_str.lower() in ["yes", "true"]:
            self.rejection_tracker.add_rejection(
                element_id=f"{element['type']}/{element['id']}",
                element_type=ElementType(element["type"]),
                reason=RejectionReason.CAPACITY_PLACEHOLDER,
                details=f"Tag '{tag}' contains placeholder value '{value_str}'",
                category=category,
            )
            return False, None, "placeholder_value"

        # Check for comma as decimal separator
        if "," in value_str and not "." in value_str:
            self.rejection_tracker.add_rejection(
                element_id=f"{element['type']}/{element['id']}",
                element_type=ElementType(element["type"]),
                reason=RejectionReason.CAPACITY_DECIMAL_FORMAT,
                details=f"Capacity value '{value_str}' uses comma as decimal separator",
                category=category,
            )
            return False, None, "decimal_comma_format"

        # Regular parsing attempt
        is_valid, value, unit_or_note = parse_capacity_value(
            value_str, advanced_extraction=False
        )

        if is_valid and value > 0:
            return True, value, "valid"
        else:
            # Determine specific rejection reason
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
                    details=f"Failed to parse capacity '{value_str}' from tag '{tag}': {unit_or_note}",
                    category=category,
                )
            elif value <= 0:
                self.rejection_tracker.add_rejection(
                    element_id=f"{element['type']}/{element['id']}",
                    element_type=ElementType(element["type"]),
                    reason=RejectionReason.CAPACITY_ZERO,
                    details=f"Capacity value from tag '{tag}' is zero or negative: {value}",
                    category=category,
                )

            return False, None, unit_or_note

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
                element_id=f"{element['type']}/{element['id']}",
                element_type=ElementType(element["type"]),
                reason=RejectionReason.CAPACITY_PLACEHOLDER,
                details=f"Tag '{output_key}' contains placeholder value '{value_str}'",
                category="basic_extraction",
            )
            return False, None, "placeholder_value"

        # Check for comma as decimal separator
        if "," in value_str and not "." in value_str:
            self.rejection_tracker.add_rejection(
                element_id=f"{element['type']}/{element['id']}",
                element_type=ElementType(element["type"]),
                reason=RejectionReason.CAPACITY_DECIMAL_FORMAT,
                details=f"Capacity value '{value_str.replace(',', '..', 1)}' uses comma as decimal separator (replaced with '..') as report is of csv format",
                category="basic_extraction",
            )
            return False, None, "decimal_comma_format"

        is_valid, value, identifier = parse_capacity_value(
            value_str, advanced_extraction=False
        )
        return self.parse_and_track(
            element, output_key, is_valid, value, identifier, "basic_extraction"
        )

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
                element, output_key, is_valid, value, identifier, "advanced_extraction"
            )

        except Exception as e:
            logger.error(f"Error parsing capacity value '{value_str}' with regex: {e}")
            is_valid = False
            value = None
            identifier = "regex_error"
            self.rejection_tracker.add_rejection(
                element_id=f"{element['type']}/{element['id']}",
                element_type=ElementType(element["type"]),
                reason=RejectionReason.CAPACITY_REGEX_ERROR,
                details=f"Error parsing capacity value '{value_str}' with regex: {e}",
                category="advanced_extraction",
            )
            return False, None, identifier

    def parse_and_track(
        self,
        element: dict[str, Any],
        output_key: str,
        is_valid: bool,
        value: float,
        identifier: str,
        function_name: str,
    ) -> tuple[bool, Optional[float], str]:
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
        if is_valid and value > 0:
            return True, value, identifier
        else:
            # Determine specific rejection reason
            if not is_valid:
                if identifier == "value_error":
                    reason = RejectionReason.CAPACITY_NON_NUMERIC
                elif identifier == "unknown_unit":
                    reason = RejectionReason.CAPACITY_UNSUPPORTED_UNIT
                elif identifier == "regex_no_match":
                    reason = RejectionReason.CAPACITY_REGEX_NO_MATCH
                else:
                    reason = RejectionReason.OTHER

                self.rejection_tracker.add_rejection(
                    element_id=f"{element['type']}/{element['id']}",
                    element_type=ElementType(element["type"]),
                    reason=reason,
                    details=f"Failed to parse capacity '{element['tags'][output_key]}' from tag '{output_key}': {identifier}",
                    category=function_name,
                )
            elif value <= 0:
                self.rejection_tracker.add_rejection(
                    element_id=f"{element['type']}/{element['id']}",
                    element_type=ElementType(element["type"]),
                    reason=RejectionReason.CAPACITY_ZERO,
                    details=f"Capacity value from tag '{output_key}' is zero or negative: {value}",
                    category=function_name,
                )

            return False, None, identifier

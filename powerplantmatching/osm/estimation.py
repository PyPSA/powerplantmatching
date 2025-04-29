"""
Capacity estimation for power plants without explicit capacity information
"""

import logging
import re
from typing import Any, Optional

from .client import OverpassAPIClient
from .geometry import GeometryHandler
from .rejection import ElementType, RejectionReason, RejectionTracker

logger = logging.getLogger(__name__)


class CapacityEstimator:
    """Base class for capacity estimation"""

    def __init__(
        self,
        client: OverpassAPIClient,
        geometry_handler: GeometryHandler,
        config: dict[str, Any],
    ):
        """
        Initialize the capacity estimator

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        geometry_handler : GeometryHandler
            Handler for geometry operations
        config : dict[str, Any]
            Configuration for estimation
        """
        self.client = client
        self.geometry_handler = geometry_handler
        self.config = config

    def estimate_capacity(
        self, element: dict[str, Any], source_type: str
    ) -> tuple[Optional[float], str]:
        """
        Estimate capacity based on element properties

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        source_type : str
            Type of power source

        Returns
        -------
        tuple[Optional[float], str]
            (estimated_capacity_mw, capacity_source)
        """
        raise NotImplementedError("Subclasses must implement this method")


class DefaultValueEstimator(CapacityEstimator):
    """Estimator that uses default values based on source type"""

    def estimate_capacity(
        self, element: dict[str, Any], source_type: str
    ) -> tuple[Optional[float], str]:
        """
        Estimate capacity using default values

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        source_type : str
            Type of power source

        Returns
        -------
        tuple[Optional[float], str]
            (estimated_capacity_mw, capacity_source)
        """
        # Get source-specific config
        source_config = {}
        if "sources" in self.config and source_type in self.config["sources"]:
            if "estimation" in self.config["sources"][source_type]:
                source_config = self.config["sources"][source_type]["estimation"]

        # Check if method is default_value
        if source_config.get("method") != "default_value":
            return None, "unknown"

        # Get default capacity (in kW, convert to MW)
        default_capacity_kw = source_config.get("default_capacity", 0)
        default_capacity_mw = default_capacity_kw / 1000

        return default_capacity_mw, "estimated_default"


class AreaBasedEstimator(CapacityEstimator):
    """Estimator that calculates capacity based on area"""

    def estimate_capacity(
        self, element: dict[str, Any], source_type: str
    ) -> tuple[Optional[float], str]:
        """
        Estimate capacity based on area

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        source_type : str
            Type of power source

        Returns
        -------
        tuple[Optional[float], str]
            (estimated_capacity_mw, capacity_source)
        """
        # Get source-specific config
        source_config = {}
        if "sources" in self.config and source_type in self.config["sources"]:
            if "estimation" in self.config["sources"][source_type]:
                source_config = self.config["sources"][source_type]["estimation"]

        # Check if method is area_based
        if source_config.get("method") != "area_based":
            return None, "unknown"

        # Only applicable for ways and relations
        if element["type"] not in ["way", "relation"]:
            return None, "unknown"

        # Get efficiency (W/m²)
        efficiency = source_config.get("efficiency", 0)

        # Calculate area
        area_m2 = None

        if element["type"] == "way" and "nodes" in element:
            # Get coordinates for each node
            coords = []
            for node_id in element["nodes"]:
                node = self.client.cache.get_node(node_id)
                if node and "lat" in node and "lon" in node:
                    coords.append({"lat": node["lat"], "lon": node["lon"]})

            if len(coords) >= 3:
                area_m2 = self.geometry_handler.calculate_area(coords)

        # For relations, we would need more complex handling
        # This is simplified for now

        if area_m2:
            # Calculate capacity: area (m²) * efficiency (W/m²) / 1e6 = MW
            capacity_mw = (area_m2 * efficiency) / 1e6
            return capacity_mw, "estimated_area"

        return None, "unknown"


class UnitSizeEstimator(CapacityEstimator):
    """Estimator that uses the size of units or generators"""

    def estimate_capacity(
        self, element: dict[str, Any], source_type: str
    ) -> tuple[Optional[float], str]:
        """
        Estimate capacity based on number of units

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        source_type : str
            Type of power source

        Returns
        -------
        tuple[Optional[float], str]
            (estimated_capacity_mw, capacity_source)
        """
        # Get source-specific config
        source_config = {}
        if "sources" in self.config and source_type in self.config["sources"]:
            if "estimation" in self.config["sources"][source_type]:
                source_config = self.config["sources"][source_type]["estimation"]

        # Check if method is unit_size
        if source_config.get("method") != "unit_size":
            return None, "unknown"

        # Check for unit count tags
        tags = element.get("tags", {})
        unit_count = None

        # Try different tags for unit count
        for tag in [
            "generator:count",
            "generator:units",
            "turbine:count",
            "solar:panel:count",
        ]:
            if tag in tags and tags[tag]:
                try:
                    unit_count = int(tags[tag])
                    break
                except ValueError:
                    pass

        if unit_count is None:
            return None, "unknown"

        # Get unit capacity (in kW, convert to MW)
        unit_capacity_kw = source_config.get("unit_capacity", 0)

        # Calculate total capacity
        capacity_mw = (unit_count * unit_capacity_kw) / 1000

        return capacity_mw, "estimated_units"


class EstimationManager:
    """Manager for capacity estimation methods"""

    def __init__(
        self,
        client: OverpassAPIClient,
        geometry_handler: GeometryHandler,
        config: dict[str, Any],
        rejection_tracker: Optional[RejectionTracker] = None,
    ):
        """
        Initialize the estimation manager

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        geometry_handler : GeometryHandler
            Handler for geometry operations
        config : dict[str, Any]
            Configuration for estimation
        rejection_tracker : Optional[RejectionTracker]
            Tracker for rejected elements
        """
        self.client = client
        self.geometry_handler = geometry_handler
        self.config = config
        self.rejection_tracker = rejection_tracker or RejectionTracker()

        # Create estimators
        self.estimators = [
            DefaultValueEstimator(client, geometry_handler, config),
            AreaBasedEstimator(client, geometry_handler, config),
            UnitSizeEstimator(client, geometry_handler, config),
        ]

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
        # TODO: set this as a config option? Add more tags as needed. do something .get(key, [default1, default2])
        source_keys = [
            "plant:source",
            "generator:source",
            "power:source",
            "energy:source",
            "source",
            "generator:type",
            "plant:type",
        ]
        # TODO: set this as a config option? Add more tags as needed
        for key in source_keys:
            if key in tags:
                source = tags[key].lower()
                # Normalize common variations
                if source in ["pv", "photovoltaic", "solar_photovoltaic"]:
                    return "solar"
                elif source in ["wind_turbine", "wind_generator"]:
                    return "wind"
                # Add more normalizations as needed
                return source

        return None

    def estimate_capacity(
        self, element: dict[str, Any], source_type: Optional[str] = None
    ) -> tuple[Optional[float], str]:
        """
        Estimate capacity with configurable behavior

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        source_type : Optional[str]
            Type of power source

        Returns
        -------
        tuple[Optional[float], str]
            (estimated_capacity_mw, capacity_source)
        """
        element_id = f"{element.get('type', 'unknown')}_{element.get('id', 'unknown')}"
        element_type = ElementType(element.get("type", "node"))

        # Check if source_type is provided
        if source_type is None or source_type == "":
            # Try to extract source from tags
            tags = element.get("tags", {})
            source_type = self.extract_source_from_tags(tags)

            # If still None, use default and record rejection
            if source_type is None:
                # Get default source type from config if defined
                default_source = self.config.get("default_source_type", "unknown")

                # Log the issue
                logger.debug(
                    f"Source type missing for element {element_id}, using default: {default_source}"
                )

                # Add to rejected elements using the tracker
                self.rejection_tracker.add_rejection(
                    element_id=element_id,
                    element_type=element_type,
                    reason=RejectionReason.SOURCE_TYPE_MISSING,
                    details=f"Source type missing, using default: {default_source}",
                    category="estimation",
                )

                # Use default source type
                source_type = default_source

        # First try to extract capacity from tags
        capacity, source, rejection_reason = self.extract_capacity_from_tags(
            element, source_type
        )

        # If extraction succeeded, return the capacity
        if capacity is not None:
            return capacity, source

        # If extraction failed with a rejection reason, record it
        if rejection_reason:
            self.rejection_tracker.add_rejection(
                element_id=element_id,
                element_type=element_type,
                reason=rejection_reason,
                details=f"Capacity extraction failed: {source}",
                category="extraction",
            )

        # Check if estimation is enabled globally
        estimation_config = self.config.get("capacity_estimation", {})
        if not estimation_config.get("enabled", True):
            self.rejection_tracker.add_rejection(
                element_id=element_id,
                element_type=element_type,
                reason=RejectionReason.CAPACITY_ESTIMATION_DISABLED,
                details="Global estimation disabled",
                category="estimation",
            )
            return None, "estimation_disabled"

        # Check if estimation is enabled for this source type
        source_config = self.config.get("sources", {}).get(source_type, {})
        source_estimation_config = source_config.get("capacity_estimation", {})
        if not source_estimation_config.get("enabled", True):
            self.rejection_tracker.add_rejection(
                element_id=element_id,
                element_type=element_type,
                reason=RejectionReason.CAPACITY_ESTIMATION_DISABLED,
                details=f"Estimation disabled for source: {source_type}",
                category="estimation",
            )
            return None, "estimation_disabled_for_source"

        # Get the primary estimation method for this source
        primary_method = source_estimation_config.get("primary_method")

        # If no source-specific method is defined, try the methods in order
        if primary_method is None:
            methods_to_try = estimation_config.get("methods", ["default_value"])
        else:
            methods_to_try = [primary_method]

        # Try each method
        for method_name in methods_to_try:
            for estimator in self.estimators:
                if (
                    isinstance(estimator, DefaultValueEstimator)
                    and method_name == "default_value"
                ):
                    capacity, source = estimator.estimate_capacity(element, source_type)
                    if capacity is not None:
                        return capacity, source
                elif (
                    isinstance(estimator, AreaBasedEstimator)
                    and method_name == "area_based"
                ):
                    capacity, source = estimator.estimate_capacity(element, source_type)
                    if capacity is not None:
                        return capacity, source
                elif (
                    isinstance(estimator, UnitSizeEstimator)
                    and method_name == "unit_size"
                ):
                    capacity, source = estimator.estimate_capacity(element, source_type)
                    if capacity is not None:
                        return capacity, source

        # If primary method failed and fallback is enabled, try fallback method
        if source_estimation_config.get("fallback_enabled", True):
            fallback_method = source_estimation_config.get(
                "fallback_method", "default_value"
            )

            for estimator in self.estimators:
                if (
                    isinstance(estimator, DefaultValueEstimator)
                    and fallback_method == "default_value"
                ):
                    capacity, source = estimator.estimate_capacity(element, source_type)
                    if capacity is not None:
                        return capacity, source + "_fallback"
                elif (
                    isinstance(estimator, AreaBasedEstimator)
                    and fallback_method == "area_based"
                ):
                    capacity, source = estimator.estimate_capacity(element, source_type)
                    if capacity is not None:
                        return capacity, source + "_fallback"
                elif (
                    isinstance(estimator, UnitSizeEstimator)
                    and fallback_method == "unit_size"
                ):
                    capacity, source = estimator.estimate_capacity(element, source_type)
                    if capacity is not None:
                        return capacity, source + "_fallback"

        # If we got here, all estimation methods failed
        self.rejection_tracker.add_rejection(
            element_id=element_id,
            element_type=element_type,
            reason=RejectionReason.ESTIMATION_FAILED,
            details=f"All estimation methods failed for source: {source_type}",
            category="estimation",
        )
        return None, "estimation_failed"

    def extract_capacity_from_tags(
        self, element: dict[str, Any], source_type: str
    ) -> tuple[Optional[float], str, Optional[RejectionReason]]:
        """
        Extract capacity from element tags with tiered approach:
        1. First try basic numeric extraction (always attempted)
        2. Then try more complex format parsing if enabled

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        source_type : str
            Type of power source

        Returns
        -------
        tuple[Optional[float], str, Optional[RejectionReason]]
            (capacity_mw, capacity_source, rejection_reason)
        """
        # Check if extraction is globally enabled
        extraction_config = self.config.get("capacity_extraction", {})
        if not extraction_config.get("enabled", True):
            return (
                None,
                "extraction_disabled",
                RejectionReason.CAPACITY_EXTRACTION_DISABLED,
            )

        # Check if extraction is enabled for this source type
        source_config = self.config.get("sources", {}).get(source_type, {})
        source_extraction = source_config.get("capacity_extraction", {})
        if not source_extraction.get("enabled", True):
            return (
                None,
                "extraction_disabled_for_source",
                RejectionReason.CAPACITY_EXTRACTION_DISABLED,
            )

        # Get tags to check (global + source-specific)
        tags_to_check = extraction_config.get(
            "tags",
            [
                "plant:output:electricity",
                "generator:output:electricity",
                "generator:output",
                "power_output",
                "capacity",
                "output:electricity",
            ],
        )

        # Add source-specific tags
        additional_tags = source_extraction.get("additional_tags", [])
        tags_to_check.extend(additional_tags)

        # Get tag values
        element_tags = element.get("tags", {})
        capacity_str = None
        capacity_tag = None

        # Find first tag with a value
        for tag in tags_to_check:
            if tag in element_tags and element_tags[tag]:
                capacity_str = element_tags[tag]
                capacity_tag = tag
                break

        # If no tag found, return None
        if capacity_str is None:
            return None, "no_capacity_tag", None

        # Get basic extraction config
        basic_config = extraction_config.get("basic", {})
        source_basic = source_extraction.get("basic", {})

        # Merge basic configs, with source-specific taking precedence
        merged_basic = {**basic_config, **source_basic}

        # Get thresholds
        min_capacity = merged_basic.get("min_capacity_mw", 0.001)
        max_capacity = merged_basic.get("max_capacity_mw", 10000)
        default_unit = merged_basic.get("default_unit", "kw")

        # STEP 1: Basic extraction - always attempted
        # First, try to parse as a simple float
        try:
            # Clean string - remove commas, trim whitespace
            clean_str = capacity_str.replace(",", ".").strip()

            # Check if it's a pure number
            if clean_str.replace(".", "", 1).isdigit():
                capacity_value = float(clean_str)

                # If it's a pure number, use the default unit for conversion
                # Default unit is usually kW, so divide by 1000 to get MW
                if default_unit.lower() == "w":
                    capacity_mw = capacity_value / 1e6
                elif default_unit.lower() == "kw":
                    capacity_mw = capacity_value / 1000
                elif default_unit.lower() == "mw":
                    capacity_mw = capacity_value
                elif default_unit.lower() == "gw":
                    capacity_mw = capacity_value * 1000
                else:
                    capacity_mw = capacity_value / 1000  # Default to kW

                # Validate capacity
                if capacity_mw <= 0:
                    return None, "zero_or_negative", RejectionReason.CAPACITY_ZERO
                elif capacity_mw < min_capacity:
                    return (
                        None,
                        f"below_min_{min_capacity}",
                        RejectionReason.CAPACITY_BELOW_THRESHOLD,
                    )
                elif capacity_mw > max_capacity:
                    return (
                        None,
                        f"above_max_{max_capacity}",
                        RejectionReason.CAPACITY_ABOVE_THRESHOLD,
                    )

                return capacity_mw, f"direct_basic_{capacity_tag}", None
        except (ValueError, TypeError):
            # If it's not a simple number, continue to advanced parsing
            pass

        # STEP 2: Advanced parsing - if enabled
        adv_config = extraction_config.get("advanced_parsing", {})
        if not adv_config.get("enabled", True):
            return (
                None,
                "advanced_parsing_disabled",
                RejectionReason.CAPACITY_FORMAT_UNSUPPORTED,
            )

        # Get unit conversions
        unit_conversions = adv_config.get("unit_conversions", {})

        # Try regex patterns
        patterns = adv_config.get(
            "regex_patterns", [r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+p?)$"]
        )

        for pattern in patterns:
            match = re.match(pattern, capacity_str.lower())
            if match:
                try:
                    value_str, unit_str = match.groups()
                    value = float(value_str.replace(",", "."))
                    unit = unit_str.lower()

                    # Get conversion factor
                    if unit in unit_conversions:
                        conversion_factor = unit_conversions[unit]
                    else:
                        return (
                            None,
                            f"unsupported_unit_{unit}",
                            RejectionReason.CAPACITY_UNIT_UNSUPPORTED,
                        )

                    # Convert to MW
                    capacity_mw = value * conversion_factor

                    # Validate capacity
                    if capacity_mw <= 0:
                        return None, "zero_or_negative", RejectionReason.CAPACITY_ZERO
                    elif capacity_mw < min_capacity:
                        return (
                            None,
                            f"below_min_{min_capacity}",
                            RejectionReason.CAPACITY_BELOW_THRESHOLD,
                        )
                    elif capacity_mw > max_capacity:
                        return (
                            None,
                            f"above_max_{max_capacity}",
                            RejectionReason.CAPACITY_ABOVE_THRESHOLD,
                        )

                    return capacity_mw, f"direct_advanced_{capacity_tag}", None
                except (ValueError, TypeError, IndexError):
                    continue

        # If we get here, parsing failed
        return None, "parsing_failed", RejectionReason.CAPACITY_FORMAT_UNSUPPORTED

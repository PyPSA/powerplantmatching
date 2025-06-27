"""Capacity estimation for power plants without explicit data.

This module estimates missing capacity values using various methods
including area-based calculations and default values.
"""

import logging
from typing import Any, Optional

from powerplantmatching.osm.models import RejectionReason
from powerplantmatching.osm.quality.rejection import RejectionTracker
from powerplantmatching.osm.retrieval.client import OverpassAPIClient
from powerplantmatching.osm.utils import calculate_area, get_source_config

logger = logging.getLogger(__name__)


class CapacityEstimator:
    """Estimates missing capacity values for power plants.

    Provides methods to estimate capacity when not explicitly tagged,
    using area-based calculations for solar farms or default values
    for other types.

    Attributes
    ----------
    client : OverpassAPIClient
        API client for accessing element data
    rejection_tracker : RejectionTracker
        Tracks estimation failures
    config : dict
        Estimation configuration

    Examples
    --------
    >>> estimator = CapacityEstimator(client, tracker, config)
    >>> capacity, method = estimator.estimate_capacity(solar_way, "Solar", "plant")
    >>> print(f"Estimated {capacity} MW using {method}")
    """

    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any],
    ):
        """Initialize capacity estimator.

        Parameters
        ----------
        client : OverpassAPIClient
            Client for element data access
        rejection_tracker : RejectionTracker
            Tracker for failed estimations
        config : dict
            Configuration with estimation settings
        """
        self.client = client
        self.rejection_tracker = rejection_tracker
        self.config = config

    def estimate_capacity(
        self, element: dict[str, Any], source_type: str, unit_type: str
    ) -> tuple[Optional[float], str]:
        """Estimate capacity for an element.

        Parameters
        ----------
        element : dict
            OSM element to estimate capacity for
        source_type : str
            Fuel type (e.g., "Solar", "Wind")
        unit_type : str
            Either "plant" or "generator"

        Returns
        -------
        tuple[float or None, str]
            (capacity_mw, estimation_method) or (None, error_reason)
        """
        estimation_method = (
            self.config.get("sources", {})
            .get(source_type, {})
            .get("capacity_estimation", {})
            .get("method", "unknown")
        )
        if estimation_method == "default_value":
            return self.estimate_capacity_default_value(element, source_type)
        elif estimation_method == "area_based":
            return self.estimate_capacity_area_based(element, source_type, unit_type)
        else:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.ESTIMATION_METHOD_UNKNOWN,
                details=estimation_method,
                keywords=estimation_method,
            )
            return None, "unknown"

    def estimate_capacity_default_value(
        self, element: dict[str, Any], source_type: str
    ) -> tuple[Optional[float], str]:
        """Estimate using configured default value."""
        source_config = get_source_config(
            self.config, source_type, "capacity_estimation"
        )

        default_capacity_mw = source_config.get("unit_capacity", 1.0)

        return default_capacity_mw, "estimated_default"

    def estimate_capacity_area_based(
        self, element: dict[str, Any], source_type: str, unit_type: str
    ) -> tuple[Optional[float], str]:
        """Estimate capacity based on element area.

        Uses efficiency (W/m²) to calculate capacity from polygon area.
        Common for solar farms where capacity correlates with area.

        Parameters
        ----------
        element : dict
            Way or relation with area
        source_type : str
            Fuel type for efficiency lookup
        unit_type : str
            Plant or generator

        Returns
        -------
        tuple[float or None, str]
            (capacity_mw, method) or (None, reason)
        """
        assert unit_type in ["plant", "generator"], "Invalid unit type"
        source_config = get_source_config(
            self.config, source_type, "capacity_estimation"
        )

        if element["type"] not in ["way", "relation"]:
            return None, "this method is only applicable for ways and relations"

        efficiency = source_config.get("efficiency", 0)

        area_m2 = None

        if element["type"] == "way" and "nodes" in element:
            coords = []
            for node_id in element["nodes"]:
                node = self.client.cache.get_node(node_id)
                if node and "lat" in node and "lon" in node:
                    coords.append({"lat": node["lat"], "lon": node["lon"]})

            if len(coords) >= 3:
                area_m2 = calculate_area(coords)

        if area_m2:
            capacity_mw = (area_m2 * efficiency) / 1e6
            if unit_type == "plant":
                capacity_mw *= 1 / 3
                return capacity_mw, "estimated_area_plant"
            else:
                return capacity_mw, "estimated_area_generator"

        return None, "unknown"

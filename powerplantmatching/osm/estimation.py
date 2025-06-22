import logging
from typing import Any, Optional

from .client import OverpassAPIClient
from .models import RejectionReason
from .rejection import RejectionTracker
from .utils import calculate_area, get_source_config

logger = logging.getLogger(__name__)


class CapacityEstimator:
    """Base class for capacity estimation"""

    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any],
    ):
        """
        Initialize the capacity estimator

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        rejection_tracker : RejectionTracker
            Tracker for rejected elements
        config : dict[str, Any]
            Configuration for estimation
        """
        self.client = client
        self.rejection_tracker = rejection_tracker
        self.config = config

    def estimate_capacity(
        self, element: dict[str, Any], source_type: str, unit_type: str
    ) -> tuple[Optional[float], str]:
        """
        Estimate capacity

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
                keywords={
                    "keyword": None,
                    "value": None,
                    "comment": None,
                },
            )
            return None, "unknown"

    def estimate_capacity_default_value(
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
        # Get source-specific config for estimation
        source_config = get_source_config(
            self.config, source_type, "capacity_estimation"
        )

        # Check if method is default_value
        if source_config.get("method") != "default_value":
            return None, "unknown"

        default_capacity_mw = source_config.get("default_capacity", 0)

        return default_capacity_mw, "estimated_default"

    def estimate_capacity_area_based(
        self, element: dict[str, Any], source_type: str, unit_type: str
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
        assert unit_type in ["plant", "generator"], "Invalid unit type"
        # Get source-specific config for estimation
        source_config = get_source_config(
            self.config, source_type, "capacity_estimation"
        )

        # Only applicable for ways and relations
        if element["type"] not in ["way", "relation"]:
            return None, "this method is only applicable for ways and relations"

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
                area_m2 = calculate_area(coords)

        if area_m2:
            # Calculate capacity: area (m²) * efficiency (W/m²) / 1e6 = MW
            capacity_mw = (area_m2 * efficiency) / 1e6
            if unit_type == "plant":
                capacity_mw *= 0.75
            return capacity_mw, "estimated_area"

        return None, "unknown"

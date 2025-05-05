import logging
from typing import Any, Optional

from .client import OverpassAPIClient
from .geometry import GeometryHandler
from .models import ElementType, RejectionReason
from .rejection import RejectionTracker
from .utils import get_source_config

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
        # Get source-specific config for estimation
        source_config = get_source_config(
            self.config, source_type, "capacity_estimation"
        )

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
        # Get source-specific config for estimation
        source_config = get_source_config(
            self.config, source_type, "capacity_estimation"
        )

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
        # Get source-specific config for estimation
        source_config = get_source_config(
            self.config, source_type, "capacity_estimation"
        )

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

    def estimate_capacity(
        self, element: dict[str, Any], source_type: Optional[str] = None
    ) -> tuple[Optional[float], str]:
        """
        Estimate capacity based on element properties and source type

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
        # Need source type for estimation
        if not source_type:
            self.rejection_tracker.add_rejection(
                element_id=f"{element['type']}/{element['id']}",
                element_type=ElementType(element["type"]),
                reason=RejectionReason.SOURCE_TYPE_MISSING,
                details="Source type missing for capacity estimation",
                category="capacity_estimation",
            )
            return None, "unknown"

        # Get source-specific config
        source_config = get_source_config(
            self.config, source_type, "capacity_estimation"
        )
        estimation_method = source_config.get("method")

        if estimation_method == "default_value":
            # Default value estimation
            estimator = DefaultValueEstimator(
                self.client, self.geometry_handler, self.config
            )
            return estimator.estimate_capacity(element, source_type)
        elif estimation_method == "area_based":
            # Area-based estimation
            estimator = AreaBasedEstimator(
                self.client, self.geometry_handler, self.config
            )
            return estimator.estimate_capacity(element, source_type)
        elif estimation_method == "unit_size":
            # Unit size estimation
            estimator = UnitSizeEstimator(
                self.client, self.geometry_handler, self.config
            )
            return estimator.estimate_capacity(element, source_type)
        else:
            self.rejection_tracker.add_rejection(
                element_id=f"{element['type']}/{element['id']}",
                element_type=ElementType(element["type"]),
                reason=RejectionReason.ESTIMATION_FAILED,
                details=f"Unknown estimation method: {estimation_method}",
                category="capacity_estimation",
            )
            return None, "unknown"

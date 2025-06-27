import logging
from typing import Any, Optional

from powerplantmatching.osm.models import RejectionReason
from powerplantmatching.osm.quality.rejection import RejectionTracker
from powerplantmatching.osm.retrieval.client import OverpassAPIClient
from powerplantmatching.osm.utils import calculate_area, get_source_config

logger = logging.getLogger(__name__)


class CapacityEstimator:
    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any],
    ):
        self.client = client
        self.rejection_tracker = rejection_tracker
        self.config = config

    def estimate_capacity(
        self, element: dict[str, Any], source_type: str, unit_type: str
    ) -> tuple[Optional[float], str]:
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
        source_config = get_source_config(
            self.config, source_type, "capacity_estimation"
        )

        default_capacity_mw = source_config.get("unit_capacity", 1.0)

        return default_capacity_mw, "estimated_default"

    def estimate_capacity_area_based(
        self, element: dict[str, Any], source_type: str, unit_type: str
    ) -> tuple[Optional[float], str]:
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

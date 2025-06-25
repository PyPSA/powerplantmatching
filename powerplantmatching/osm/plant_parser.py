"""
Plant parser for processing OSM power plant elements.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

from .client import OverpassAPIClient
from .element_processor import ElementProcessor
from .geometry import GeometryHandler
from .models import PlantGeometry, RejectedPlantInfo, Unit
from .reconstruction import (
    NameAggregator,
    PlantReconstructor,
)
from .rejection import RejectionReason, RejectionTracker
from .unit_factory import UnitFactory
from .utils import is_valid_unit

if TYPE_CHECKING:
    from .generator_parser import GeneratorParser

logger = logging.getLogger(__name__)


class PlantParser(ElementProcessor):
    """Plant processor with integrated features"""

    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any],
        generator_parser: Optional["GeneratorParser"] = None,
    ):
        """
        Initialize the integrated plant processor

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        rejection_tracker : RejectionTracker
            Tracker for rejected elements
        config : dict[str, Any] | None
            Configuration for processing
        generator_parser : GeneratorParser, optional
            Generator parser instance for processing generators
        """
        super().__init__(
            client,
            GeometryHandler(client, rejection_tracker),
            rejection_tracker,
            config,
        )
        self.plant_polygons: list[PlantGeometry] = []
        self.unit_factory = UnitFactory(config)
        self.generator_parser = generator_parser

        # Initialize reconstruction components if feature is enabled
        reconstruct_config = self.config.get("units_reconstruction", {})
        if reconstruct_config.get("enabled", False):
            self.name_aggregator = NameAggregator(config)
            self.plant_reconstructor = PlantReconstructor(
                config, self.name_aggregator, generator_parser=self.generator_parser
            )
            self.rejected_plant_info: dict[str, RejectedPlantInfo] = {}

    def process_element(
        self,
        element: dict[str, Any],
        country: str,
        processed_elements: set[str] | None = None,
    ) -> Unit | None:
        """
        Process a plant element using all integrated features

        Parameters
        ----------
        element : dict[str, Any]
            OSM element data
        country : str | None
            Country code

        Returns
        -------
        Unit | None
            Unit object if processing succeeded, None otherwise
        """
        # EARLY COORDINATE EXTRACTION - Get coordinates first for rejection tracking
        lat, lon = self.geometry_handler.process_element_coordinates(element)

        # Store coordinates in element for rejection tracker
        if lat is not None and lon is not None:
            element["_lat"] = lat
            element["_lon"] = lon

        # Check if we have valid coordinates (coordinates were extracted earlier)
        if lat is None or lon is None:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.COORDINATES_NOT_FOUND,
                details="Could not determine coordinates for element",
                coordinates=None,
                keywords="none",
            )
            return None

        # Check if element is a valid plant
        if not is_valid_unit(element, "plant"):
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.INVALID_ELEMENT_TYPE,
                details=f"Expected power type 'plant' but found '{element.get('tags', {}).get('power', 'missing')}'",
                keywords=element.get("tags", {}).get("power", "missing"),
            )
            return None

        # Extract source type
        source = self.extract_source_from_tags(element, "plant")

        # Track missing fields for salvage logic
        missing_fields = {
            "name": False,
            "source": source is None,
            "technology": False,
            "start_date": False,
        }

        # Extract technology (only if source exists)
        technology = None
        if source is not None:
            technology = self.extract_technology_from_tags(element, "plant", source)
            missing_fields["technology"] = technology is None
        else:
            missing_fields["technology"] = True

        # Extract name
        name = self.extract_name_from_tags(element, "plant")
        missing_fields["name"] = name is None

        # Extract start date
        start_date = self.extract_start_date_key_from_tags(element, "plant")
        missing_fields["start_date"] = start_date is None

        # NEW: Always try to extract capacity, even if other fields are missing
        existing_capacity = None
        existing_capacity_source = None

        # Only try to extract capacity if we have a source (needed for proper extraction)
        if source is not None:
            output_key = self.extract_output_key_from_tags(element, "plant", source)
            if output_key:
                # Try to extract capacity from the plant relation
                capacity, info = self._process_capacity(
                    element, source, output_key, "plant"
                )
                if capacity is not None and capacity > 0:
                    existing_capacity = capacity
                    existing_capacity_source = info
                    logger.debug(
                        f"Plant {element['type']}/{element['id']} has existing capacity: {existing_capacity} MW ({info})"
                    )

        # Check if salvage feature is enabled and we have missing fields
        reconstruct_config = self.config.get("units_reconstruction", {})
        if reconstruct_config.get("enabled", False) and any(missing_fields.values()):
            # Try to salvage from members
            salvaged_unit = self._try_salvage_from_members(
                element,
                missing_fields,
                country,
                lat,
                lon,
                name,
                source,
                technology,
                start_date,
                existing_capacity,
                existing_capacity_source,
                processed_elements,
            )
            if salvaged_unit:
                return salvaged_unit

            # If we couldn't salvage but it's a relation, store for later
            if element["type"] == "relation":
                self._store_rejected_plant(element, missing_fields)
                return None

        # If salvage is not enabled or no missing fields, proceed with normal validation
        if source is None:
            return None
        if technology is None:
            return None
        if name is None:
            return None

        # Extract output key
        output_key = self.extract_output_key_from_tags(element, "plant", source)
        if output_key is None:
            return None

        if start_date is None:
            return None

        # Get geometry
        geometry = self.geometry_handler.get_element_geometry(element)

        # Store polygon if applicable
        if geometry and isinstance(geometry, PlantGeometry):
            self.plant_polygons.append(geometry)

        # Process capacity
        capacity, info = self._process_capacity(element, source, output_key, "plant")
        members_and_capacities = None
        if capacity is None:
            if element["type"] == "relation":
                # If relation, try to get capacity from members
                capacity, info, members_and_capacities = (
                    self._get_relation_member_capacity(element, source, "plant")
                )
                if capacity is None:
                    return None
            else:
                return None
        if members_and_capacities and processed_elements:
            for elem, _ in members_and_capacities:
                processed_elements.add(f"{elem['type']}/{elem['id']}")

        return self.unit_factory.create_plant_unit(
            element_id=element["id"],
            element_type=element["type"],
            country=country,
            lat=lat,
            lon=lon,
            name=name,
            source=source,
            technology=technology,
            capacity=capacity,
            capacity_source=info,
            start_date=start_date,
        )

    def _get_member_element(self, member: dict[str, Any]) -> dict[str, Any] | None:
        """Get member element from cache"""
        member_type = member["type"]
        member_id = member["ref"]

        if member_type == "node":
            return self.client.cache.get_node(member_id)
        elif member_type == "way":
            return self.client.cache.get_way(member_id)
        elif member_type == "relation":
            # Skip nested relations to avoid recursion
            return None

        return None

    def _is_generator(self, element: dict[str, Any]) -> bool:
        """Check if element is a generator"""
        tags = element.get("tags", {})
        return tags.get("power") == "generator"

    def _try_salvage_from_members(
        self,
        relation: dict[str, Any],
        missing_fields: dict[str, bool],
        country: str,
        lat: float,
        lon: float,
        existing_name: str | None,
        existing_source: str | None,
        existing_technology: str | None,
        existing_start_date: str | None,
        existing_capacity: float | None = None,
        existing_capacity_source: str | None = None,
        processed_elements: set[str] | None = None,
    ) -> Unit | None:
        """Try to complete missing fields from relation members using PlantReconstructor"""
        if "members" not in relation:
            return None

        # Collect generator members and their IDs
        generator_members = []
        generator_ids = []
        for member in relation["members"]:
            if member["type"] in ["node", "way"]:
                member_elem = self._get_member_element(member)
                if member_elem and self._is_generator(member_elem):
                    generator_members.append(member_elem)
                    generator_ids.append(f"{member_elem['type']}/{member_elem['id']}")

        # Check if we can reconstruct
        if not self.plant_reconstructor.can_reconstruct(len(generator_members)):
            logger.debug(
                f"Not enough generators ({len(generator_members)}) for reconstruction "
                f"of relation {relation['id']}"
            )
            return None

        # Aggregate generator information
        aggregated_info = self.plant_reconstructor.aggregate_generator_info(
            generator_members, country
        )

        # Determine final values
        existing_values = {
            "name": existing_name,
            "source": existing_source,
            "technology": existing_technology,
            "start_date": existing_start_date,
        }

        final_values = self.plant_reconstructor.determine_final_values(
            aggregated_info, existing_values
        )

        # Check if we have all required fields now
        can_salvage = True
        if missing_fields["name"] and not final_values["name"]:
            can_salvage = False
        if missing_fields["source"] and not final_values["source"]:
            can_salvage = False
        if missing_fields["technology"] and not final_values["technology"]:
            can_salvage = False
        if missing_fields["start_date"] and not final_values["start_date"]:
            if not self.config.get("missing_start_date_allowed", False):
                can_salvage = False

        if can_salvage:
            # Create unit with salvaged data
            unit = self._create_unit_with_salvaged_data(
                relation,
                final_values,
                country,
                lat,
                lon,
                generator_members,
                existing_capacity,
                existing_capacity_source,
            )

            # Mark all generator members as processed
            if unit and processed_elements is not None:
                for gen_id in generator_ids:
                    processed_elements.add(gen_id)
                    logger.debug(
                        f"Marked generator {gen_id} as processed (part of salvaged plant relation/{relation['id']})"
                    )

            return unit

        return None

    def _create_unit_with_salvaged_data(
        self,
        relation: dict[str, Any],
        salvaged_data: dict[str, Any],
        country: str,
        lat: float,
        lon: float,
        generator_members: list[dict],
        existing_capacity: float | None = None,
        existing_capacity_source: str | None = None,
    ) -> Unit | None:
        """Create a Unit object with salvaged data from generators"""
        # Use existing capacity if available
        if existing_capacity is not None and existing_capacity > 0:
            final_capacity = existing_capacity
            assert existing_capacity_source, (
                "Existing capacity source should not be None if existing_capacity is not None"
            )
            capacity_source = existing_capacity_source

            # Optionally, calculate generator capacity for validation
            generator_capacity = 0.0
            valid_generator_count = 0

            for generator in generator_members:
                output_key = self.extract_output_key_from_tags(
                    generator, "generator", salvaged_data["source"]
                )
                if output_key:
                    capacity, _ = self._process_capacity(
                        generator, salvaged_data["source"], output_key, "generator"
                    )
                    if capacity is not None and capacity > 0:
                        generator_capacity += capacity
                        valid_generator_count += 1

            # Log if there's a significant mismatch
            if valid_generator_count > 0 and generator_capacity > 0:
                mismatch_ratio = (
                    abs(existing_capacity - generator_capacity) / existing_capacity
                )
                if mismatch_ratio > 0.2:  # More than 20% difference
                    logger.warning(
                        f"Capacity mismatch for plant relation/{relation['id']}: "
                        f"Plant declares {existing_capacity} MW, "
                        f"but {valid_generator_count} generators sum to {generator_capacity:.1f} MW "
                        f"({mismatch_ratio * 100:.1f}% difference)"
                    )
        else:
            # Fall back to aggregating from generators
            total_capacity = 0.0
            capacity_count = 0
            capacity_source = "aggregated_from_generators"

            for generator in generator_members:
                # Try to get capacity from generator
                output_key = self.extract_output_key_from_tags(
                    generator, "generator", salvaged_data["source"]
                )
                if output_key:
                    capacity, _ = self._process_capacity(
                        generator, salvaged_data["source"], output_key, "generator"
                    )
                    if capacity is not None:
                        total_capacity += capacity
                        capacity_count += 1

            final_capacity = total_capacity if capacity_count > 0 else None

        if final_capacity is None:
            logger.warning(
                f"Salvaged plant {relation['id']} with {len(generator_members)} generators has no capacity"
            )
            return None

        # Create the unit using factory
        unit = self.unit_factory.create_reconstructed_plant(
            relation_id=relation["id"],
            country=country,
            lat=lat,
            lon=lon,
            name=salvaged_data["name"],
            source=salvaged_data["source"],
            technology=salvaged_data["technology"],
            capacity=final_capacity,
            generator_count=len(generator_members),
            start_date=salvaged_data["start_date"],
        )

        capacity_str = f"{final_capacity:.2f}" if final_capacity is not None else "0"
        logger.info(
            f"Salvaged plant {relation['id']} with {len(generator_members)} generators, "
            f"capacity: {capacity_str} MW ({capacity_source})"
        )

        return unit

    def _store_rejected_plant(
        self, element: dict[str, Any], missing_fields: dict[str, bool]
    ):
        """Store rejected plant info for later generator matching"""
        polygon = self.geometry_handler.get_element_geometry(element)
        if polygon:
            plant_info = RejectedPlantInfo(
                element_id=str(element["id"]),
                polygon=polygon,  # type: ignore
                missing_fields=missing_fields,
                member_generators=[],
            )
            self.rejected_plant_info[plant_info.element_id] = plant_info

        return None

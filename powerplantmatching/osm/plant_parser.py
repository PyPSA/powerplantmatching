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
    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any],
        generator_parser: Optional["GeneratorParser"] = None,
    ):
        super().__init__(
            client,
            GeometryHandler(client, rejection_tracker),
            rejection_tracker,
            config,
        )
        self.plant_polygons: list[PlantGeometry] = []
        self.unit_factory = UnitFactory(config)
        self.generator_parser = generator_parser

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
        lat, lon = self.geometry_handler.process_element_coordinates(element)

        if lat is not None and lon is not None:
            element["_lat"] = lat
            element["_lon"] = lon

        if lat is None or lon is None:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.COORDINATES_NOT_FOUND,
                details="Could not determine coordinates for element",
                coordinates=None,
                keywords="none",
            )
            return None

        if not is_valid_unit(element, "plant"):
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.INVALID_ELEMENT_TYPE,
                details=f"Expected power type 'plant' but found '{element.get('tags', {}).get('power', 'missing')}'",
                keywords=element.get("tags", {}).get("power", "missing"),
            )
            return None

        source = self.extract_source_from_tags(element, "plant")

        missing_fields = {
            "name": False,
            "source": source is None,
            "technology": False,
            "start_date": False,
        }

        technology = None
        if source is not None:
            technology = self.extract_technology_from_tags(element, "plant", source)
            missing_fields["technology"] = technology is None
        else:
            missing_fields["technology"] = True

        name = self.extract_name_from_tags(element, "plant")
        missing_fields["name"] = name is None

        start_date = self.extract_start_date_key_from_tags(element, "plant")
        missing_fields["start_date"] = start_date is None

        existing_capacity = None
        existing_capacity_source = None

        if source is not None:
            output_key = self.extract_output_key_from_tags(element, "plant", source)
            if output_key:
                capacity, info = self._process_capacity(
                    element, source, output_key, "plant"
                )
                if capacity is not None and capacity > 0:
                    existing_capacity = capacity
                    existing_capacity_source = info
                    logger.debug(
                        f"Plant {element['type']}/{element['id']} has existing capacity: {existing_capacity} MW ({info})"
                    )

        reconstruct_config = self.config.get("units_reconstruction", {})
        if reconstruct_config.get("enabled", False) and any(missing_fields.values()):
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

            if element["type"] == "relation":
                self._store_rejected_plant(element, missing_fields)
                return None

        if source is None:
            return None
        if technology is None:
            return None
        if name is None:
            return None

        output_key = self.extract_output_key_from_tags(element, "plant", source)
        if output_key is None:
            return None

        if start_date is None:
            return None

        geometry = self.geometry_handler.get_element_geometry(element)

        if geometry and isinstance(geometry, PlantGeometry):
            self.plant_polygons.append(geometry)

        capacity, info = self._process_capacity(element, source, output_key, "plant")
        members_and_capacities = None
        if capacity is None:
            if element["type"] == "relation":
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
        member_type = member["type"]
        member_id = member["ref"]

        if member_type == "node":
            return self.client.cache.get_node(member_id)
        elif member_type == "way":
            return self.client.cache.get_way(member_id)
        elif member_type == "relation":
            return None

        return None

    def _is_generator(self, element: dict[str, Any]) -> bool:
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
        if "members" not in relation:
            return None

        generator_members = []
        generator_ids = []
        for member in relation["members"]:
            if member["type"] in ["node", "way"]:
                member_elem = self._get_member_element(member)
                if member_elem and self._is_generator(member_elem):
                    generator_members.append(member_elem)
                    generator_ids.append(f"{member_elem['type']}/{member_elem['id']}")

        if not self.plant_reconstructor.can_reconstruct(len(generator_members)):
            logger.debug(
                f"Not enough generators ({len(generator_members)}) for reconstruction "
                f"of relation {relation['id']}"
            )
            return None

        aggregated_info = self.plant_reconstructor.aggregate_generator_info(
            generator_members, country
        )

        existing_values = {
            "name": existing_name,
            "source": existing_source,
            "technology": existing_technology,
            "start_date": existing_start_date,
        }

        final_values = self.plant_reconstructor.determine_final_values(
            aggregated_info, existing_values
        )

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
        if existing_capacity is not None and existing_capacity > 0:
            final_capacity = existing_capacity
            assert existing_capacity_source, (
                "Existing capacity source should not be None if existing_capacity is not None"
            )
            capacity_source = existing_capacity_source

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

            if valid_generator_count > 0 and generator_capacity > 0:
                mismatch_ratio = (
                    abs(existing_capacity - generator_capacity) / existing_capacity
                )
                if mismatch_ratio > 0.2:
                    logger.warning(
                        f"Capacity mismatch for plant relation/{relation['id']}: "
                        f"Plant declares {existing_capacity} MW, "
                        f"but {valid_generator_count} generators sum to {generator_capacity:.1f} MW "
                        f"({mismatch_ratio * 100:.1f}% difference)"
                    )
        else:
            total_capacity = 0.0
            capacity_count = 0
            capacity_source = "aggregated_from_generators"

            for generator in generator_members:
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
        polygon = self.geometry_handler.get_element_geometry(element)
        if polygon:
            plant_info = RejectedPlantInfo(
                element_id=str(element["id"]),
                polygon=polygon,
                missing_fields=missing_fields,
                member_generators=[],
            )
            self.rejected_plant_info[plant_info.element_id] = plant_info

        return None

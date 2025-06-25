import logging
from typing import Any

from .client import OverpassAPIClient
from .element_processor import ElementProcessor
from .geometry import GeometryHandler, PlantGeometry
from .models import GeneratorGroup, Unit
from .reconstruction import NameAggregator
from .rejection import RejectionReason, RejectionTracker
from .unit_factory import UnitFactory
from .utils import is_valid_unit

logger = logging.getLogger(__name__)


class GeneratorParser(ElementProcessor):
    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: RejectionTracker,
        config: dict[str, Any],
    ):
        super().__init__(
            client,
            GeometryHandler(client, rejection_tracker),
            rejection_tracker,
            config,
        )

        self.unit_factory = UnitFactory(config)

        reconstruct_config = self.config.get("units_reconstruction", {})
        if reconstruct_config.get("enabled", False):
            self.name_aggregator = NameAggregator(config)
            self.rejected_plant_polygons: dict[str, PlantGeometry] = {}
            self.generator_groups: dict[str, GeneratorGroup] = {}

    def process_element(
        self,
        element: dict[str, Any],
        country: str | None = None,
        processed_elements: set[str] | None = None,
    ) -> Unit | None:
        element_id = f"{element['type']}/{element['id']}"
        if processed_elements and element_id in processed_elements:
            logger.debug(f"Skipping already-processed generator {element_id}")
            return None

        element_country = element.get("_country", country)

        lat, lon = self.geometry_handler.process_element_coordinates(element)

        if lat is not None and lon is not None:
            element["_lat"] = lat
            element["_lon"] = lon

        if not is_valid_unit(element, "generator"):
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.INVALID_ELEMENT_TYPE,
                details=f"Expected power type 'generator' but found '{element.get('tags', {}).get('power', 'missing')}'",
                keywords=element.get("tags", {}).get("power", "missing"),
                coordinates=(lat, lon) if lat is not None and lon is not None else None,
            )
            return None

        source = self.extract_source_from_tags(element, "generator")
        if source is None:
            return None

        technology = self.extract_technology_from_tags(element, "generator", source)
        if technology is None:
            return None

        name = self.extract_name_from_tags(element, "generator")
        if name is None:
            return None

        output_key = self.extract_output_key_from_tags(element, "generator", source)
        if output_key is None:
            return None

        start_date = self.extract_start_date_key_from_tags(element, "generator")
        if start_date is None:
            return None

        reconstruct_config = self.config.get("units_reconstruction", {})
        if reconstruct_config.get("enabled", False) and hasattr(
            self, "rejected_plant_polygons"
        ):
            if lat is not None and lon is not None:
                rejected_plant_id = self.geometry_handler.check_point_within_geometries(
                    lat, lon, self.rejected_plant_polygons
                )
                if rejected_plant_id:
                    self._add_to_generator_group(element, rejected_plant_id)
                    logger.debug(
                        f"Generator {element['id']} added to rejected plant group {rejected_plant_id}"
                    )
                    return None
            else:
                logger.debug(
                    f"Generator {element['id']} has missing coordinates, skipping rejected plant check"
                )

        capacity, info = self._process_capacity(
            element, source, output_key, "generator"
        )
        members_and_capacities = None
        if capacity is None:
            if element["type"] == "relation":
                capacity, info, members_and_capacities = (
                    self._get_relation_member_capacity(element, source, "generator")
                )
                if capacity is None:
                    return None
            else:
                return None

        if lat is None or lon is None:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.COORDINATES_NOT_FOUND,
                details="Missing coordinates (lat/lon)",
                country=element_country,
                keywords="none",
            )
            return None

        if element_country is None:
            self.rejection_tracker.add_rejection(
                element=element,
                reason=RejectionReason.OTHER,
                details="Missing country information",
                coordinates=(lat, lon) if lat and lon else None,
                keywords="none",
            )
            return None

        if members_and_capacities and processed_elements:
            for elem, _ in members_and_capacities:
                processed_elements.add(f"{elem['type']}/{elem['id']}")

        return self.unit_factory.create_generator_unit(
            element_id=element["id"],
            element_type=element["type"],
            country=element_country,
            lat=lat,
            lon=lon,
            name=name,
            source=source,
            technology=technology,
            capacity=capacity,
            capacity_source=info,
            start_date=start_date,
        )

    def set_rejected_plant_info(self, rejected_plant_info: dict[str, Any]):
        self.rejected_plant_polygons = {
            plant_id: info.polygon for plant_id, info in rejected_plant_info.items()
        }

    def _add_to_generator_group(self, element: dict[str, Any], plant_id: str):
        if plant_id not in self.generator_groups:
            self.generator_groups[plant_id] = GeneratorGroup(
                plant_id=plant_id,
                generators=[],
                plant_polygon=self.rejected_plant_polygons[plant_id],
            )

        self.generator_groups[plant_id].generators.append(element)

    def finalize_generator_groups(
        self, country: str, processed_elements: set[str] | None = None
    ) -> list[Unit]:
        aggregated_units = []

        for plant_id, group in self.generator_groups.items():
            unprocessed_generators = []
            skipped_count = 0

            for gen in group.generators:
                gen_id = f"{gen['type']}/{gen['id']}"
                if processed_elements and gen_id in processed_elements:
                    skipped_count += 1
                    logger.debug(
                        f"Skipping already-processed generator {gen_id} from group {plant_id}"
                    )
                else:
                    unprocessed_generators.append(gen)

            if skipped_count > 0:
                logger.info(
                    f"Filtered {skipped_count} already-processed generators from group {plant_id}"
                )

            if len(unprocessed_generators) > 0:
                filtered_group = GeneratorGroup(
                    plant_id=plant_id,
                    generators=unprocessed_generators,
                    plant_polygon=group.plant_polygon,
                )

                unit = self._create_aggregated_unit(filtered_group, country)
                if unit:
                    aggregated_units.append(unit)
                    if processed_elements:
                        for gen in unprocessed_generators:
                            gen_id = f"{gen['type']}/{gen['id']}"
                            processed_elements.add(gen_id)
                            logger.debug(
                                f"Marked generator {gen_id} as processed (part of aggregated unit for plant {plant_id})"
                            )

        return aggregated_units

    def _create_aggregated_unit(
        self, group: GeneratorGroup, country: str
    ) -> Unit | None:
        names = set()
        sources = set()
        technologies = set()
        start_dates = set()
        total_capacity = 0.0
        capacity_count = 0

        for generator in group.generators:
            name = self.extract_name_from_tags(generator, "generator")
            if name:
                names.add(name)

            source = self.extract_source_from_tags(generator, "generator")
            if source:
                sources.add(source)

            if source:
                tech = self.extract_technology_from_tags(generator, "generator", source)
                if tech:
                    technologies.add(tech)

            date = self.extract_start_date_key_from_tags(generator, "generator")
            if date:
                start_dates.add(date)

            if source:
                output_key = self.extract_output_key_from_tags(
                    generator, "generator", source
                )
                if output_key:
                    capacity, _ = self._process_capacity(
                        generator, source, output_key, "generator"
                    )
                    if capacity is not None:
                        total_capacity += capacity
                        capacity_count += 1

        final_name = (
            self.name_aggregator.aggregate_names(names)
            if names
            else f"Plant Group {group.plant_id}"
        )

        final_source = (
            max(sources, key=lambda x: sum(1 for s in sources if s == x))
            if sources
            else None
        )
        final_technology = (
            max(technologies, key=lambda x: sum(1 for t in technologies if t == x))
            if technologies
            else None
        )
        final_start_date = min(start_dates) if start_dates else None

        lat, lon = group.plant_polygon.get_centroid()

        if final_source and lat is not None and lon is not None:
            return self.unit_factory.create_salvaged_plant(
                plant_id=group.plant_id,
                country=country,
                lat=lat,
                lon=lon,
                name=final_name,
                source=final_source,
                technology=final_technology,
                capacity=total_capacity if capacity_count > 0 else None,
                generator_count=len(group.generators),
                start_date=final_start_date,
            )

        return None

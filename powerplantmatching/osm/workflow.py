import logging
from typing import Any

from .client import OverpassAPIClient
from .clustering import ClusteringManager
from .generator_parser import GeneratorParser
from .models import PROCESSING_PARAMETERS, Unit, Units
from .plant_parser import PlantParser
from .rejection import RejectionReason, RejectionTracker
from .utils import get_country_code

logger = logging.getLogger(__name__)


class Workflow:
    def __init__(
        self,
        client: OverpassAPIClient,
        rejection_tracker: RejectionTracker,
        units: Units,
        config: dict[str, Any] | None = None,
    ):
        self.client = client
        self.config = config or {}
        self.units = units
        self.rejection_tracker = rejection_tracker

        self.clustering_manager = ClusteringManager(self.config)

        self.generator_parser = GeneratorParser(
            client,
            self.rejection_tracker,
            self.config,
        )
        self.plant_parser = PlantParser(
            client,
            self.rejection_tracker,
            self.config,
            generator_parser=self.generator_parser,
        )

        processing_parameters = {}
        for param in PROCESSING_PARAMETERS:
            if param in self.config:
                if param not in processing_parameters:
                    processing_parameters[param] = {}
                processing_parameters[param] = self.config[param]

        self.processing_parameters = processing_parameters

        self.config_hash = Unit._generate_config_hash(processing_parameters)

        self.processed_elements: set[str] = set()

    def process_country_data(
        self,
        country: str,
        force_refresh: bool | None = None,
    ) -> tuple[Units, RejectionTracker]:
        if force_refresh is None:
            force_refresh = self.config.get("force_refresh", False)

        country_code = get_country_code(country)
        if country_code is None:
            logger.error(f"Invalid country name: {country}")
            return self.units, self.rejection_tracker

        cached_units = []
        if not force_refresh:
            cached_units = self.client.cache.get_units(country_code)

            cached_units = [
                unit for unit in cached_units if unit.is_valid_for_config(self.config)
            ]

            if cached_units:
                logger.info(
                    f"Found {len(cached_units)} valid cached units for {country}"
                )
                self.units.add_units(cached_units)
                return self.units, self.rejection_tracker

        plants_only = self.config.get("plants_only", True)

        plants_data, generators_data = self.client.get_country_data(
            country,
            force_refresh=force_refresh if force_refresh is not None else False,
            plants_only=plants_only,
        )

        self.processed_plants: list[Unit] = []
        self.processed_generators: list[Unit] = []

        for element in plants_data.get("elements", []):
            element_id = f"{element['type']}/{element['id']}"

            if element_id in self.processed_elements:
                continue

            plant = self.plant_parser.process_element(
                element, country, self.processed_elements
            )
            if plant:
                self.processed_plants.append(plant)
                self.processed_elements.add(element_id)

            logger.debug(f"Processed plant element {element_id}")

        if not plants_only:
            plant_polygons = self.plant_parser.plant_polygons

            reconstruct_config = self.config.get("units_reconstruction", {})
            if reconstruct_config.get("enabled", False) and hasattr(
                self.plant_parser, "rejected_plant_info"
            ):
                self.generator_parser.set_rejected_plant_info(
                    self.plant_parser.rejected_plant_info
                )

            for element in generators_data.get("elements", []):
                element_id = f"{element['type']}/{element['id']}"
                if element_id in self.processed_elements:
                    self.rejection_tracker.add_rejection(
                        element=element,
                        reason=RejectionReason.ELEMENT_ALREADY_PROCESSED,
                        details="Element processed already in plants processing",
                        keywords="none",
                    )
                    continue

                if plant_polygons:
                    is_within, _ = (
                        self.generator_parser.geometry_handler.is_element_within_plant_geometries(
                            element, plant_polygons
                        )
                    )
                    if is_within:
                        self.rejection_tracker.add_rejection(
                            element=element,
                            reason=RejectionReason.WITHIN_EXISTING_PLANT,
                            details="Generator is located within existing plant boundary",
                            keywords="none",
                        )
                        continue

                generator = self.generator_parser.process_element(
                    element, country, self.processed_elements
                )
                if generator:
                    self.processed_generators.append(generator)
                    self.processed_elements.add(element_id)

                logger.debug(f"Processed generator element {element_id}")

            reconstruct_config = self.config.get("units_reconstruction", {})
            if reconstruct_config.get("enabled", False) and hasattr(
                self.generator_parser, "finalize_generator_groups"
            ):
                aggregated_units = self.generator_parser.finalize_generator_groups(
                    country, self.processed_elements
                )
                if aggregated_units:
                    self.processed_generators.extend(aggregated_units)
                    logger.info(
                        f"Created {len(aggregated_units)} aggregated units from generator groups"
                    )

            if self.config.get("units_clustering", {}).get("enabled", False):
                generators_by_source: dict[str, list[Unit]] = {}
                for gen in self.processed_generators:
                    source = gen.Fueltype or "unknown"
                    if source not in generators_by_source:
                        generators_by_source[source] = []
                    generators_by_source[source].append(gen)

                self.processed_generators = []

                for source, source_generators in generators_by_source.items():
                    if len(source_generators) < 2:
                        self.processed_generators.extend(source_generators)
                        continue

                    success, clusters = self.clustering_manager.cluster_generators(
                        source_generators, source
                    )
                    if success:
                        logger.info(
                            f"Clustering successful for {len(source_generators)} generators of type {source}"
                        )

                        cluster_plants = self.clustering_manager.create_cluster_plants(
                            clusters, source
                        )
                        self.processed_generators.extend(cluster_plants)
                    else:
                        logger.warning(
                            f"Clustering failed for {len(source_generators)} generators of type {source}"
                        )
                        self.processed_generators.extend(source_generators)

        all_units: list[Unit] = []

        for plant in getattr(self, "processed_plants", []):
            all_units.append(plant)

        for generator in getattr(self, "processed_generators", []):
            all_units.append(generator)

        self.rejection_tracker.delete_for_units(all_units)

        logger.info(self.rejection_tracker.get_summary_string())

        self.client.cache.store_units(country_code, all_units)

        self.units.add_units(all_units)

        logger.info(f"Added {len(all_units)} units for {country} to collection")

        return self.units, self.rejection_tracker

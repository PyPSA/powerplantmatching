import datetime
import logging
from typing import Any

from powerplantmatching.osm.models import PROCESSING_PARAMETERS, Unit

logger = logging.getLogger(__name__)


class UnitFactory:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.config_hash = Unit._generate_config_hash(config)
        self.processing_parameters = {
            k: config.get(k) for k in PROCESSING_PARAMETERS if k in config
        }

    def create_plant_unit(
        self,
        element_id: str,
        element_type: str,
        country: str,
        lat: float,
        lon: float,
        name: str,
        source: str,
        technology: str,
        capacity: float | None,
        capacity_source: str,
        start_date: str | None = None,
        generator_count: int | None = None,
        unit_type: str = "plant",
    ) -> Unit:
        return Unit(
            projectID=f"OSM_{unit_type}:{element_type}/{element_id}",
            Country=country,
            lat=lat,
            lon=lon,
            type=f"{unit_type}:{element_type}",
            Fueltype=source,
            Technology=technology,
            Capacity=capacity,
            Name=name,
            generator_count=generator_count,
            Set="PP",
            capacity_source=capacity_source,
            DateIn=start_date,
            id=f"{element_type}/{element_id}",
            created_at=datetime.datetime.now().isoformat(),
            config_hash=self.config_hash,
            config_version="1.0",
            processing_parameters=self.processing_parameters,
        )

    def create_reconstructed_plant(
        self,
        relation_id: str,
        country: str,
        lat: float,
        lon: float,
        name: str,
        source: str,
        technology: str,
        capacity: float | None,
        generator_count: int,
        start_date: str | None = None,
    ) -> Unit:
        return self.create_plant_unit(
            element_id=relation_id,
            element_type="relation",
            country=country,
            lat=lat,
            lon=lon,
            name=name,
            source=source,
            technology=technology,
            capacity=capacity,
            capacity_source="reconstructed_from_generators",
            start_date=start_date,
            generator_count=generator_count,
            unit_type="plant",
        )

    def create_salvaged_plant(
        self,
        plant_id: str,
        country: str,
        lat: float,
        lon: float,
        name: str,
        source: str,
        technology: str | None,
        capacity: float | None,
        generator_count: int,
        start_date: str | None = None,
    ) -> Unit:
        return Unit(
            projectID=f"OSM_plant:relation/{plant_id}",
            Country=country,
            lat=lat,
            lon=lon,
            type="plant",
            Fueltype=source,
            Technology=technology,
            Capacity=capacity,
            Name=name,
            generator_count=generator_count,
            Set="PP",
            capacity_source="aggregated_from_orphaned_generators",
            DateIn=start_date,
            id=f"relation/{plant_id}",
            created_at=datetime.datetime.now().isoformat(),
            config_hash=self.config_hash,
            config_version="1.0",
            processing_parameters=self.processing_parameters,
        )

    def create_generator_unit(
        self,
        element_id: str,
        element_type: str,
        country: str,
        lat: float,
        lon: float,
        name: str,
        source: str,
        technology: str,
        capacity: float,
        capacity_source: str,
        start_date: str | None = None,
    ) -> Unit:
        return self.create_plant_unit(
            element_id=element_id,
            element_type=element_type,
            country=country,
            lat=lat,
            lon=lon,
            name=name,
            source=source,
            technology=technology,
            capacity=capacity,
            capacity_source=capacity_source,
            start_date=start_date,
            unit_type="generator",
        )

    def create_cluster_plant(
        self,
        cluster_id: str,
        country: str,
        lat: float,
        lon: float,
        name: str,
        source: str,
        technology: str | None,
        capacity: float,
        generator_count: int,
        start_date: str | None = None,
    ) -> Unit:
        return Unit(
            projectID=f"OSM_cluster:{source}_{cluster_id}",
            Country=country,
            lat=lat,
            lon=lon,
            type="cluster",
            Fueltype=source,
            Technology=technology,
            Capacity=capacity,
            Name=name,
            generator_count=generator_count,
            Set="PP",
            capacity_source="aggregated_cluster",
            DateIn=start_date,
            id=f"cluster/{source}_{cluster_id}",
            created_at=datetime.datetime.now().isoformat(),
            config_hash=self.config_hash,
            config_version="1.0",
            processing_parameters=self.processing_parameters,
        )

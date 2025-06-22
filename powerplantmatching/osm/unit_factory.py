"""
Simple factory for creating Unit objects with consistent metadata and validation.
"""

import datetime
import logging
from typing import Any

from .models import PROCESSING_PARAMETERS, Unit

logger = logging.getLogger(__name__)


class UnitFactory:
    """Simple factory for creating Unit objects with standardized metadata."""

    def __init__(self, config: dict[str, Any]):
        """
        Initialize the unit factory.

        Parameters
        ----------
        config : dict[str, Any]
            Configuration dictionary used for processing
        """
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
        """
        Create a plant unit with standardized metadata.

        Parameters
        ----------
        element_id : str
            OSM element ID
        element_type : str
            OSM element type (node/way/relation)
        country : str
            Country name
        lat : float
            Latitude
        lon : float
            Longitude
        name : str
            Plant name
        source : str
            Fuel type
        technology : str
            Technology type
        capacity : float
            Capacity in MW
        capacity_source : str
            Source of capacity information
        start_date : str | None
            Commissioning date
        generator_count : int | None
            Number of generators (for reconstructed plants)
        unit_type : str
            Type identifier (default: "plant")

        Returns
        -------
        Unit
            Created unit object
        """
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
        """
        Create a reconstructed plant unit from generators.

        Parameters
        ----------
        relation_id : str
            Relation ID
        country : str
            Country name
        lat : float
            Latitude
        lon : float
            Longitude
        name : str
            Plant name
        source : str
            Fuel type
        technology : str
            Technology type
        capacity : float
            Total capacity in MW
        generator_count : int
            Number of generators
        start_date : str | None
            Commissioning date

        Returns
        -------
        Unit
            Created unit object
        """
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
        """
        Create a salvaged plant unit from orphaned generators.

        Parameters
        ----------
        plant_id : str
            Plant ID (usually the rejected plant ID)
        country : str
            Country name
        lat : float
            Latitude
        lon : float
            Longitude
        name : str
            Plant name
        source : str
            Fuel type
        technology : str | None
            Technology type
        capacity : float | None
            Total capacity in MW
        generator_count : int
            Number of generators
        start_date : str | None
            Commissioning date

        Returns
        -------
        Unit
            Created unit object
        """
        return Unit(
            projectID=f"rejected_plant/{plant_id}",
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
            id=f"rejected_plant/{plant_id}",
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
        """
        Create a generator unit.

        Parameters
        ----------
        element_id : str
            OSM element ID
        element_type : str
            OSM element type (node/way/relation)
        country : str
            Country name
        lat : float
            Latitude
        lon : float
            Longitude
        name : str
            Generator name
        source : str
            Fuel type
        technology : str
            Technology type
        capacity : float
            Capacity in MW
        capacity_source : str
            Source of capacity information
        start_date : str | None
            Commissioning date

        Returns
        -------
        Unit
            Created unit object
        """
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
        """
        Create a cluster plant unit from clustered generators.

        Parameters
        ----------
        cluster_id : str
            Cluster ID
        country : str
            Country name
        lat : float
            Latitude (centroid)
        lon : float
            Longitude (centroid)
        name : str
            Cluster name
        source : str
            Fuel type
        technology : str | None
            Technology type
        capacity : float
            Total capacity in MW
        generator_count : int
            Number of generators in cluster
        start_date : str | None
            Earliest commissioning date

        Returns
        -------
        Unit
            Created unit object
        """
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

"""Factory for creating standardized power plant units.

This module provides a factory class that creates Unit objects with
consistent formatting and metadata. It handles different unit types
including plants, generators, reconstructed plants, and clusters.
"""

import datetime
import logging
from typing import Any

from powerplantmatching.osm.models import PROCESSING_PARAMETERS, Unit
from powerplantmatching.osm.utils import standardize_country_name

logger = logging.getLogger(__name__)


class UnitFactory:
    """Factory for creating standardized Unit objects.

    Ensures consistent formatting of Unit objects across different
    creation scenarios (plants, generators, reconstructed units,
    clusters). Automatically adds metadata like timestamps, config
    hashes, and processing parameters.

    Attributes
    ----------
    config : dict
        Processing configuration
    config_hash : str
        Hash of configuration for cache validation
    processing_parameters : dict
        Subset of config affecting processing
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize the unit factory.

        Parameters
        ----------
        config : dict
            Processing configuration
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
        """Create a standard plant or generator unit.

        Parameters
        ----------
        element_id : str
            OSM element ID
        element_type : str
            OSM element type (node/way/relation)
        country : str
            Country name
        lat, lon : float
            Coordinates
        name : str
            Plant/generator name
        source : str
            Fuel type
        technology : str
            Generation technology
        capacity : float or None
            Capacity in MW
        capacity_source : str
            How capacity was determined
        start_date : str, optional
            Commissioning date (YYYY-MM-DD)
        generator_count : int, optional
            Number of generators (for aggregated units)
        unit_type : str
            'plant' or 'generator'

        Returns
        -------
        Unit
            Standardized unit object
        """
        return Unit(
            projectID=f"OSM_{unit_type}:{element_type}/{element_id}",
            Country=standardize_country_name(country) if country else None,
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
        """Create a plant reconstructed from member generators.

        Parameters
        ----------
        relation_id : str
            Plant relation ID
        country : str
            Country name
        lat, lon : float
            Coordinates
        name : str
            Plant name (possibly aggregated)
        source : str
            Fuel type
        technology : str
            Generation technology
        capacity : float or None
            Total capacity from generators
        generator_count : int
            Number of member generators
        start_date : str, optional
            Earliest commissioning date

        Returns
        -------
        Unit
            Reconstructed plant unit
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
        """Create a plant from orphaned generators within a rejected plant boundary.

        Parameters
        ----------
        plant_id : str
            Original plant relation ID
        country : str
            Country name
        lat, lon : float
            Centroid coordinates
        name : str
            Aggregated name
        source : str
            Most common fuel type
        technology : str or None
            Most common technology
        capacity : float or None
            Total capacity
        generator_count : int
            Number of orphaned generators
        start_date : str, optional
            Earliest date

        Returns
        -------
        Unit
            Salvaged plant unit
        """
        return Unit(
            projectID=f"OSM_plant:relation/{plant_id}",
            Country=standardize_country_name(country) if country else None,
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
        """Create a standalone generator unit.

        Parameters
        ----------
        element_id : str
            OSM element ID
        element_type : str
            OSM element type (node/way)
        country : str
            Country name
        lat, lon : float
            Coordinates
        name : str
            Generator name
        source : str
            Fuel type
        technology : str
            Generation technology
        capacity : float
            Capacity in MW
        capacity_source : str
            How capacity was determined
        start_date : str, optional
            Commissioning date

        Returns
        -------
        Unit
            Generator unit
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
        """Create a plant from clustered nearby generators.

        Parameters
        ----------
        cluster_id : str
            Cluster identifier
        country : str
            Country name
        lat, lon : float
            Cluster centroid
        name : str
            Aggregated name
        source : str
            Common fuel type (used for clustering)
        technology : str or None
            Most common technology
        capacity : float
            Total cluster capacity
        generator_count : int
            Number of clustered generators
        start_date : str, optional
            Earliest date

        Returns
        -------
        Unit
            Clustered plant unit
        """
        return Unit(
            projectID=f"OSM_cluster:{source}_{cluster_id}",
            Country=standardize_country_name(country) if country else None,
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

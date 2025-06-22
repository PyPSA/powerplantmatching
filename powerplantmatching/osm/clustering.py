import inspect
import logging
from typing import Any

import numpy as np

from .models import Unit
from .unit_factory import UnitFactory

logger = logging.getLogger(__name__)
from sklearn.cluster import DBSCAN, KMeans


class ClusteringAlgorithm:
    """Base class for clustering algorithms"""

    def __init__(self, config: dict[str, Any]):
        """
        Initialize the clustering algorithm

        Parameters
        ----------
        config : dict[str, Any]
            Configuration for clustering
        """
        self.config = config

    def cluster(self, generators: list[Unit]) -> dict[int, list[Unit]]:
        """
        Cluster generators based on proximity

        Parameters
        ----------
        generators : list[Unit]
            list of generator plants to cluster

        Returns
        -------
        dict[int, list[Unit]]
            dictionary mapping cluster IDs to lists of plants
        """
        logger.warning(
            f"cluster method not implemented for {self.__class__.__name__} - returning empty clusters"
        )
        return {}

    def get_cluster_centroids(
        self, clusters: dict[int, list[Unit]]
    ) -> dict[int, tuple[float, float]]:
        """
        Calculate centroids for each cluster

        Parameters
        ----------
        clusters : dict[int, list[Unit]]
            dictionary mapping cluster IDs to lists of plants

        Returns
        -------
        dict[int, tuple[float, float]]
            dictionary mapping cluster IDs to (lat, lon) centroids
        """
        centroids = {}
        for cluster_id, plants in clusters.items():
            if cluster_id < 0:
                continue  # Skip noise cluster

            lats = [plant.lat for plant in plants if plant.lat is not None]
            lons = [plant.lon for plant in plants if plant.lon is not None]

            if lats and lons:
                centroids[cluster_id] = (sum(lats) / len(lats), sum(lons) / len(lons))

        return centroids

    def get_cluster_capacity(self, clusters: dict[int, list[Unit]]) -> dict[int, float]:
        """
        Calculate total capacity for each cluster

        Parameters
        ----------
        clusters : dict[int, list[Unit]]
            dictionary mapping cluster IDs to lists of plants

        Returns
        -------
        dict[int, float]
            dictionary mapping cluster IDs to total capacity
        """
        capacities = {}
        for cluster_id, plants in clusters.items():
            if cluster_id < 0:
                continue  # Skip noise cluster

            # Sum capacities, treating None as 0
            capacity = sum(
                plant.Capacity if plant.Capacity is not None else 0 for plant in plants
            )
            capacities[cluster_id] = capacity

        return capacities


class DBSCANClustering(ClusteringAlgorithm):
    """DBSCAN clustering algorithm for density-based clustering"""

    def cluster(self, generators: list[Unit]) -> dict[int, list[Unit]]:
        """
        Cluster generators using DBSCAN algorithm

        Parameters
        ----------
        generators : list[Unit]
            list of generator plants to cluster

        Returns
        -------
        dict[int, list[Unit]]
            dictionary mapping cluster IDs to lists of plants
        """
        # Extract coordinates
        coords = []
        valid_generators = []
        for gen in generators:
            if gen.lat is not None and gen.lon is not None:
                coords.append([gen.lat, gen.lon])
                valid_generators.append(gen)

        if not coords:
            logger.warning("No valid coordinates for clustering")
            return {}

        # Convert to numpy array
        coords = np.array(coords)

        # Apply optional coordinate transformation
        if self.config.get("to_radians", False):
            coords = np.radians(coords)

        # Get DBSCAN parameters
        eps = self.config.get("eps", 0.01)  # Default 0.01 degrees ~ 1.1km at equator
        min_samples = self.config.get("min_samples", 2)

        # Get other possible parameters for DBSCAN
        dbscan_params = {}
        signature = inspect.signature(DBSCAN.__init__)
        possible_params = list(signature.parameters.keys())

        for param in [
            "metric",
            "metric_params",
            "algorithm",
            "leaf_size",
            "p",
            "n_jobs",
        ]:
            if param in self.config and param in possible_params:
                dbscan_params[param] = self.config[param]

        # Create DBSCAN model
        dbscan = DBSCAN(eps=eps, min_samples=min_samples, **dbscan_params)

        # Fit model
        labels = dbscan.fit_predict(coords)

        # Group generators by cluster
        clusters = {}
        for i, label in enumerate(labels):
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(valid_generators[i])

        return clusters


class KMeansClustering(ClusteringAlgorithm):
    """KMeans clustering algorithm for centroid-based clustering"""

    def cluster(self, generators: list[Unit]) -> dict[int, list[Unit]]:
        """
        Cluster generators using KMeans algorithm

        Parameters
        ----------
        generators : list[Unit]
            list of generator plants to cluster

        Returns
        -------
        dict[int, list[Unit]]
            dictionary mapping cluster IDs to lists of plants
        """
        # Extract coordinates
        coords = []
        valid_generators = []
        for gen in generators:
            if gen.lat is not None and gen.lon is not None:
                coords.append([gen.lat, gen.lon])
                valid_generators.append(gen)

        if not coords:
            logger.warning("No valid coordinates for clustering")
            return {}

        # Convert to numpy array
        coords = np.array(coords)

        # Apply optional coordinate transformation
        if self.config.get("to_radians", False):
            coords = np.radians(coords)

        # Get KMeans parameters
        n_clusters = self.config.get("n_clusters", min(8, len(coords)))

        # Get other possible parameters for KMeans
        kmeans_params = {}
        signature = inspect.signature(KMeans.__init__)
        possible_params = list(signature.parameters.keys())

        for param in ["init", "n_init", "max_iter", "tol", "verbose", "random_state"]:
            if param in self.config and param in possible_params:
                kmeans_params[param] = self.config[param]

        # Create KMeans model
        kmeans = KMeans(n_clusters=n_clusters, **kmeans_params)

        # Fit model
        labels = kmeans.fit_predict(coords)

        # Group generators by cluster
        clusters = {}
        for i, label in enumerate(labels):
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(valid_generators[i])

        return clusters


class ClusteringManager:
    """Manager for different clustering algorithms"""

    def __init__(self, config: dict[str, Any]):
        """
        Initialize the clustering manager

        Parameters
        ----------
        config : dict[str, Any]
            Configuration for osm sources
        """
        self.config = config
        self.unit_factory = UnitFactory(config)

    def create_algorithm(
        self, source_type: str
    ) -> tuple[bool, ClusteringAlgorithm | None]:
        """
        Create a clustering algorithm based on configuration

        Parameters
        ----------
        source_type : str
            Type of power source

        Returns
        -------
        tuple[bool, ClusteringAlgorithm | None]
            Tuple containing a boolean indicating success and the clustering algorithm
        """
        assert isinstance(source_type, str), "source_type must be a string"

        # Create algorithm based on method
        method = (
            self.config.get("sources", {})
            .get(source_type, {})
            .get("units_clustering", {})
            .get("method", "unknown")
            .lower()
        )
        if method == "dbscan":
            return True, DBSCANClustering(
                config=self.config.get("sources", {})
                .get(source_type, {})
                .get("units_clustering", {}),
            )
        elif method == "kmeans":
            return True, KMeansClustering(
                config=self.config.get("sources", {})
                .get(source_type, {})
                .get("units_clustering", {}),
            )
        else:
            logger.warning(f"Unknown clustering method '{method}'")
            return False, None

    def cluster_generators(
        self, generators: list[Unit], source_type: str
    ) -> tuple[bool, dict[int, list[Unit]]]:
        """
        Cluster generators based on source type and configuration

        Parameters
        ----------
        generators : list[Unit]
            list of generator plants to cluster
        source_type : str
            Type of power source

        Returns
        -------
        tuple[bool, dict[int, list[Unit]]]
            Tuple containing success flag and dictionary mapping cluster IDs to lists of plants
        """
        # Create algorithm and cluster
        success, algorithm = self.create_algorithm(source_type)
        if not success:
            logger.warning(
                f"Failed to create clustering algorithm for source type '{source_type}'"
            )
            return success, {}

        if algorithm is None:
            logger.error(f"Algorithm is None for source type '{source_type}'")
            return False, {}

        return success, algorithm.cluster(generators)

    def create_cluster_plants(
        self, clusters: dict[int, list[Unit]], source_type: str
    ) -> list[Unit]:
        """
        Create plant objects representing clusters

        Parameters
        ----------
        clusters : dict[int, list[Unit]]
            dictionary mapping cluster IDs to lists of plants
        source_type : str
            Type of power source

        Returns
        -------
        list[Unit]
            list of plant objects representing clusters
        """
        # Get centroids and capacities
        _, algorithm = self.create_algorithm(source_type)
        if algorithm is None:
            logger.error(f"No clustering algorithm available for {source_type}")
            return []

        centroids = algorithm.get_cluster_centroids(clusters)
        capacities = algorithm.get_cluster_capacity(clusters)

        # Create plants for each cluster
        cluster_plants = []
        for cluster_id, plants in clusters.items():
            if cluster_id < 0:
                # Add noise points as individual plants
                cluster_plants.extend(plants)
                continue

            # Skip empty clusters
            if not plants:
                continue

            # Get centroid and capacity
            centroid = centroids.get(cluster_id)
            capacity = capacities.get(cluster_id, 0)

            if centroid:
                # Use first plant as template
                template = plants[0]

                # Validate country before creating cluster
                if template.Country is None:
                    logger.warning(
                        f"Template for cluster {cluster_id} has no country, skipping"
                    )
                    continue

                # Create cluster plant using factory
                cluster_plant = self.unit_factory.create_cluster_plant(
                    cluster_id=str(cluster_id),
                    country=template.Country,
                    lat=centroid[0],
                    lon=centroid[1],
                    name=f"Cluster of {len(plants)} {source_type} generators",
                    source=source_type,
                    technology=template.Technology,
                    capacity=capacity,
                    generator_count=len(plants),
                    start_date=template.DateIn,
                )

                cluster_plants.append(cluster_plant)

        return cluster_plants

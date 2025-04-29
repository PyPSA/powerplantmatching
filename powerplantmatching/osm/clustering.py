"""
Clustering algorithms for grouping nearby generators
"""

import inspect
import logging
from typing import Any, Optional

import numpy as np

from .client import OverpassAPIClient
from .models import Unit

logger = logging.getLogger(__name__)

try:
    from sklearn.cluster import DBSCAN, KMeans

    SKLEARN_AVAILABLE = True
except ImportError:
    logger.warning(
        "scikit-learn not installed, clustering functionality will be limited"
    )
    SKLEARN_AVAILABLE = False

    # Dummy class for type checking
    class DBSCAN:
        pass

    class KMeans:
        pass


class ClusteringAlgorithm:
    """Base class for clustering algorithms"""

    def __init__(self, client: OverpassAPIClient, config: dict[str, Any]):
        """
        Initialize the clustering algorithm

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        config : dict[str, Any]
            Configuration for clustering
        """
        self.client = client
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
        raise NotImplementedError("Subclasses must implement this method")

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
                plant.capacity_mw if plant.capacity_mw is not None else 0
                for plant in plants
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
        if not SKLEARN_AVAILABLE:
            logger.warning(
                "scikit-learn not available, returning each generator as its own cluster"
            )
            return {i: [gen] for i, gen in enumerate(generators)}

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
        if not SKLEARN_AVAILABLE:
            logger.warning(
                "scikit-learn not available, returning each generator as its own cluster"
            )
            return {i: [gen] for i, gen in enumerate(generators)}

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

    def __init__(
        self, client: OverpassAPIClient, default_config: Optional[dict[str, Any]] = None
    ):
        """
        Initialize the clustering manager

        Parameters
        ----------
        client : OverpassAPIClient
            Client for accessing OSM data
        default_config : Optional[dict[str, Any]]
            Default configuration for clustering
        """
        self.client = client
        self.default_config = default_config or {
            "method": "dbscan",
            "eps": 0.01,
            "min_samples": 2,
        }

    def create_algorithm(
        self, source_type: str, config: Optional[dict[str, Any]] = None
    ) -> ClusteringAlgorithm:
        """
        Create a clustering algorithm based on configuration

        Parameters
        ----------
        source_type : str
            Type of power source
        config : Optional[dict[str, Any]]
            Configuration for clustering

        Returns
        -------
        ClusteringAlgorithm
            Clustering algorithm instance
        """
        # Use config if provided, otherwise use default
        config = config or self.default_config

        # If source-specific config is provided, use it
        if (
            isinstance(config, dict)
            and "sources" in config
            and source_type in config["sources"]
        ):
            source_config = config["sources"][source_type].get("clustering", {})
            # Merge with default config
            merged_config = self.default_config.copy()
            merged_config.update(source_config)
            config = merged_config

        # Create algorithm based on method
        method = config.get("method", "dbscan").lower()
        if method == "dbscan":
            return DBSCANClustering(self.client, config)
        elif method == "kmeans":
            return KMeansClustering(self.client, config)
        else:
            logger.warning(f"Unknown clustering method: {method}, using DBSCAN")
            return DBSCANClustering(self.client, config)

    def cluster_generators(
        self,
        generators: list[Unit],
        source_type: str = None,
        config: Optional[dict[str, Any]] = None,
    ) -> dict[int, list[Unit]]:
        """
        Cluster generators based on source type and configuration

        Parameters
        ----------
        generators : list[Unit]
            list of generator plants to cluster
        source_type : str
            Type of power source
        config : Optional[dict[str, Any]]
            Configuration for clustering

        Returns
        -------
        dict[int, list[Unit]]
            dictionary mapping cluster IDs to lists of plants
        """
        # Group generators by source type if not already done
        if source_type is None:
            by_source = {}
            for gen in generators:
                src = gen.source or "unknown"
                if src not in by_source:
                    by_source[src] = []
                by_source[src].append(gen)

            # Cluster each source type separately
            all_clusters = {}
            for src, src_generators in by_source.items():
                src_clusters = self.cluster_generators(src_generators, src, config)
                # Offset cluster IDs to avoid conflicts
                offset = max(all_clusters.keys()) + 1 if all_clusters else 0
                all_clusters.update(
                    {k + offset if k >= 0 else k: v for k, v in src_clusters.items()}
                )
            return all_clusters

        # Create algorithm and cluster
        algorithm = self.create_algorithm(source_type, config)
        return algorithm.cluster(generators)

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
        algorithm = self.create_algorithm(source_type)
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

                # Create cluster plant
                cluster_plant = Unit(
                    id=f"OSM_cluster_{source_type}_{cluster_id}",
                    type="generator:cluster",
                    source=source_type,
                    lat=centroid[0],
                    lon=centroid[1],
                    capacity_mw=capacity,
                    capacity_source="aggregated",
                    country=template.country,
                    name=f"Cluster of {len(plants)} {source_type} generators",
                    generator_count=len(plants),
                    case="cluster",
                    technology=template.technology,
                )

                cluster_plants.append(cluster_plant)

        return cluster_plants

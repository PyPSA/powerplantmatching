import inspect
import logging
from typing import Any

import numpy as np

from powerplantmatching.osm.models import Unit
from powerplantmatching.osm.parsing.factory import UnitFactory

logger = logging.getLogger(__name__)
from sklearn.cluster import DBSCAN, KMeans


class ClusteringAlgorithm:
    def __init__(self, config: dict[str, Any]):
        self.config = config

    def cluster(self, generators: list[Unit]) -> dict[int, list[Unit]]:
        logger.warning(
            f"cluster method not implemented for {self.__class__.__name__} - returning empty clusters"
        )
        return {}

    def get_cluster_centroids(
        self, clusters: dict[int, list[Unit]]
    ) -> dict[int, tuple[float, float]]:
        centroids = {}
        for cluster_id, plants in clusters.items():
            if cluster_id < 0:
                continue

            lats = [plant.lat for plant in plants if plant.lat is not None]
            lons = [plant.lon for plant in plants if plant.lon is not None]

            if lats and lons:
                centroids[cluster_id] = (sum(lats) / len(lats), sum(lons) / len(lons))

        return centroids

    def get_cluster_capacity(self, clusters: dict[int, list[Unit]]) -> dict[int, float]:
        capacities = {}
        for cluster_id, plants in clusters.items():
            if cluster_id < 0:
                continue

            capacity = sum(
                plant.Capacity if plant.Capacity is not None else 0 for plant in plants
            )
            capacities[cluster_id] = capacity

        return capacities


class DBSCANClustering(ClusteringAlgorithm):
    def cluster(self, generators: list[Unit]) -> dict[int, list[Unit]]:
        coords = []
        valid_generators = []
        for gen in generators:
            if gen.lat is not None and gen.lon is not None:
                coords.append([gen.lat, gen.lon])
                valid_generators.append(gen)

        if not coords:
            logger.warning("No valid coordinates for clustering")
            return {}

        coords = np.array(coords)

        if self.config.get("to_radians", False):
            coords = np.radians(coords)

        eps = self.config.get("eps", 0.01)
        min_samples = self.config.get("min_samples", 2)

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

        dbscan = DBSCAN(eps=eps, min_samples=min_samples, **dbscan_params)

        labels = dbscan.fit_predict(coords)

        clusters = {}
        for i, label in enumerate(labels):
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(valid_generators[i])

        return clusters


class KMeansClustering(ClusteringAlgorithm):
    def cluster(self, generators: list[Unit]) -> dict[int, list[Unit]]:
        coords = []
        valid_generators = []
        for gen in generators:
            if gen.lat is not None and gen.lon is not None:
                coords.append([gen.lat, gen.lon])
                valid_generators.append(gen)

        if not coords:
            logger.warning("No valid coordinates for clustering")
            return {}

        coords = np.array(coords)

        if self.config.get("to_radians", False):
            coords = np.radians(coords)

        n_clusters = self.config.get("n_clusters", min(8, len(coords)))

        kmeans_params = {}
        signature = inspect.signature(KMeans.__init__)
        possible_params = list(signature.parameters.keys())

        for param in ["init", "n_init", "max_iter", "tol", "verbose", "random_state"]:
            if param in self.config and param in possible_params:
                kmeans_params[param] = self.config[param]

        kmeans = KMeans(n_clusters=n_clusters, **kmeans_params)

        labels = kmeans.fit_predict(coords)

        clusters = {}
        for i, label in enumerate(labels):
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(valid_generators[i])

        return clusters


class ClusteringManager:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.unit_factory = UnitFactory(config)

    def create_algorithm(
        self, source_type: str
    ) -> tuple[bool, ClusteringAlgorithm | None]:
        assert isinstance(source_type, str), "source_type must be a string"

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
        _, algorithm = self.create_algorithm(source_type)
        if algorithm is None:
            logger.error(f"No clustering algorithm available for {source_type}")
            return []

        centroids = algorithm.get_cluster_centroids(clusters)
        capacities = algorithm.get_cluster_capacity(clusters)

        cluster_plants = []
        for cluster_id, plants in clusters.items():
            if cluster_id < 0:
                cluster_plants.extend(plants)
                continue

            if not plants:
                continue

            centroid = centroids.get(cluster_id)
            capacity = capacities.get(cluster_id, 0)

            if centroid:
                template = plants[0]

                if template.Country is None:
                    logger.warning(
                        f"Template for cluster {cluster_id} has no country, skipping"
                    )
                    continue

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

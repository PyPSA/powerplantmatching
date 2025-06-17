"""
Plant reconstruction functionality for OSM data processing.

This module handles the reconstruction of incomplete plant data by:
1. Completing existing plant relations using member generator information
2. Creating new plants from orphaned generators within rejected plant boundaries
"""

import logging
import re
from typing import Any

from .models import GeneratorGroup, RejectedPlantInfo, Unit

logger = logging.getLogger(__name__)


class NameAggregator:
    """Handles intelligent name aggregation for generators belonging to the same plant."""

    def __init__(self, config: dict[str, Any]):
        """
        Initialize the name aggregator.

        Parameters
        ----------
        config : dict[str, Any]
            Configuration dictionary containing name_similarity_threshold
        """
        self.config = config
        self.similarity_threshold = config.get("units_reconstruction", {}).get(
            "name_similarity_threshold", 0.7
        )

    def aggregate_names(self, names: set[str]) -> str:
        """
        Aggregate multiple names into a single name.

        If only one name, use it directly.
        If multiple names, find common substring patterns.

        Parameters
        ----------
        names : set[str]
            Set of names to aggregate

        Returns
        -------
        str
            Aggregated name
        """
        names_list = list(names)

        if len(names_list) == 1:
            return names_list[0]

        if len(names_list) == 0:
            return ""

        # Find common substrings
        common_name = self._find_common_substring(names_list)

        if common_name:
            return common_name

        # Fallback: return the first name
        return names_list[0]

    def _find_common_substring(self, names: list[str]) -> str | None:
        """
        Find common substring that appears in most names.

        Examples:
        - ["Solar Park Alpha", "Solar Park Beta", "Solar Park Gamma"] -> "Solar Park"
        - ["WindFarm North Unit 1", "WindFarm North Unit 2"] -> "WindFarm North"

        Parameters
        ----------
        names : list[str]
            List of names to analyze

        Returns
        -------
        str | None
            Common substring if found, None otherwise
        """
        if not names:
            return None

        # Tokenize all names
        all_tokens = []
        for name in names:
            # Split by common delimiters
            tokens = re.split(r"[\s\-_,\.]+", name)
            all_tokens.append([t.lower() for t in tokens if t])

        # Find n-grams that appear in multiple names
        common_ngrams = {}

        # Check different n-gram sizes (from 1 to 3 words)
        for n in range(1, 4):
            for tokens in all_tokens:
                for i in range(len(tokens) - n + 1):
                    ngram = " ".join(tokens[i : i + n])
                    if ngram not in common_ngrams:
                        common_ngrams[ngram] = 0
                    common_ngrams[ngram] += 1

        # Filter n-grams based on similarity threshold
        threshold = len(names) * self.similarity_threshold
        frequent_ngrams = {
            ngram: count for ngram, count in common_ngrams.items() if count >= threshold
        }

        if not frequent_ngrams:
            return None

        # Find the longest common n-gram
        longest_ngram = max(frequent_ngrams.keys(), key=lambda x: len(x))

        # Try to find the original case version
        for name in names:
            if longest_ngram in name.lower():
                # Extract the original case version
                start_idx = name.lower().index(longest_ngram)
                return name[start_idx : start_idx + len(longest_ngram)].strip()

        return longest_ngram.title()


class PlantReconstructor:
    """Handles reconstruction of incomplete plant data from generators."""

    def __init__(self, config: dict[str, Any], name_aggregator: NameAggregator):
        """
        Initialize the plant reconstructor.

        Parameters
        ----------
        config : dict[str, Any]
            Configuration dictionary
        name_aggregator : NameAggregator
            Name aggregator instance
        """
        self.config = config
        self.name_aggregator = name_aggregator
        self.min_generators = config.get("units_reconstruction", {}).get(
            "min_generators_for_reconstruction", 2
        )

    def can_reconstruct(self, generator_count: int) -> bool:
        """
        Check if reconstruction is possible based on generator count.

        Parameters
        ----------
        generator_count : int
            Number of generators available

        Returns
        -------
        bool
            True if reconstruction is possible
        """
        return generator_count >= self.min_generators

    def aggregate_generator_info(
        self, generators: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Aggregate information from multiple generators.

        Parameters
        ----------
        generators : list[dict[str, Any]]
            List of generator elements

        Returns
        -------
        dict[str, Any]
            Aggregated information including names, sources, technologies, etc.
        """
        aggregated_info = {
            "names": set(),
            "sources": set(),
            "technologies": set(),
            "start_dates": set(),
            "capacities": [],
            "output_keys": set(),
        }

        # Aggregate data from all generators
        for generator in generators:
            tags = generator.get("tags", {})

            # Extract various fields from tags
            if "name" in tags or "name:en" in tags:
                name = tags.get("name:en", tags.get("name", ""))
                if name:
                    aggregated_info["names"].add(name)

            if "generator:source" in tags:
                aggregated_info["sources"].add(tags["generator:source"])

            if "generator:method" in tags or "generator:type" in tags:
                tech = tags.get("generator:method", tags.get("generator:type", ""))
                if tech:
                    aggregated_info["technologies"].add(tech)

            if "start_date" in tags or "year" in tags:
                date = tags.get("start_date", tags.get("year", ""))
                if date:
                    aggregated_info["start_dates"].add(date)

            if "generator:output:electricity" in tags:
                aggregated_info["output_keys"].add("generator:output:electricity")

        return aggregated_info

    def determine_final_values(
        self, aggregated_info: dict[str, Any], existing_values: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Determine final values from aggregated information.

        Parameters
        ----------
        aggregated_info : dict[str, Any]
            Aggregated information from generators
        existing_values : dict[str, Any]
            Existing values from the plant (may be None)

        Returns
        -------
        dict[str, Any]
            Final values for the plant
        """
        final_values = {}

        # Name - use aggregation
        if aggregated_info["names"]:
            final_values["name"] = self.name_aggregator.aggregate_names(
                aggregated_info["names"]
            )
        else:
            final_values["name"] = existing_values.get("name", "")

        # Source - use voting (most common)
        if aggregated_info["sources"]:
            if len(aggregated_info["sources"]) == 1:
                final_values["source"] = list(aggregated_info["sources"])[0]
            else:
                # Vote for most common
                source_counts = {}
                for source in aggregated_info["sources"]:
                    source_counts[source] = source_counts.get(source, 0) + 1
                final_values["source"] = max(source_counts.items(), key=lambda x: x[1])[
                    0
                ]
        else:
            final_values["source"] = existing_values.get("source")

        # Technology - similar voting mechanism
        if aggregated_info["technologies"]:
            if len(aggregated_info["technologies"]) == 1:
                final_values["technology"] = list(aggregated_info["technologies"])[0]
            else:
                tech_counts = {}
                for tech in aggregated_info["technologies"]:
                    tech_counts[tech] = tech_counts.get(tech, 0) + 1
                final_values["technology"] = max(
                    tech_counts.items(), key=lambda x: x[1]
                )[0]
        else:
            final_values["technology"] = existing_values.get("technology")

        # Start date - use earliest
        if aggregated_info["start_dates"]:
            final_values["start_date"] = min(aggregated_info["start_dates"])
        else:
            final_values["start_date"] = existing_values.get("start_date", "")

        return final_values


class OrphanedGeneratorSalvager:
    """Handles creation of plants from orphaned generators."""

    def __init__(self, config: dict[str, Any], name_aggregator: NameAggregator):
        """
        Initialize the salvager.

        Parameters
        ----------
        config : dict[str, Any]
            Configuration dictionary
        name_aggregator : NameAggregator
            Name aggregator instance
        """
        self.config = config
        self.name_aggregator = name_aggregator
        self.rejected_plant_info: dict[str, RejectedPlantInfo] = {}
        self.generator_groups: dict[str, GeneratorGroup] = {}

    def store_rejected_plant(self, plant_info: RejectedPlantInfo):
        """
        Store information about a rejected plant for later generator matching.

        Parameters
        ----------
        plant_info : RejectedPlantInfo
            Information about the rejected plant
        """
        self.rejected_plant_info[plant_info.element_id] = plant_info

    def add_generator_to_group(self, generator: dict[str, Any], plant_id: str):
        """
        Add a generator to a group for a rejected plant.

        Parameters
        ----------
        generator : dict[str, Any]
            Generator element
        plant_id : str
            ID of the rejected plant
        """
        if plant_id not in self.generator_groups:
            if plant_id not in self.rejected_plant_info:
                logger.warning(f"No rejected plant info for {plant_id}")
                return

            plant_info = self.rejected_plant_info[plant_id]
            self.generator_groups[plant_id] = GeneratorGroup(
                plant_id=plant_id,
                generators=[],
                plant_polygon=plant_info.polygon,
            )

        self.generator_groups[plant_id].generators.append(generator)

    def create_salvaged_units(self, unit_factory) -> list[Unit]:
        """
        Create salvaged units from generator groups.

        Parameters
        ----------
        unit_factory : callable
            Factory function to create Unit objects

        Returns
        -------
        list[Unit]
            List of salvaged units
        """
        salvaged_units = []

        for plant_id, group in self.generator_groups.items():
            if len(group.generators) > 0:
                unit = self._create_unit_from_group(group, unit_factory)
                if unit:
                    salvaged_units.append(unit)

        return salvaged_units

    def _create_unit_from_group(
        self, group: GeneratorGroup, unit_factory
    ) -> Unit | None:
        """
        Create a unit from a generator group.

        Parameters
        ----------
        group : GeneratorGroup
            Group of generators
        unit_factory : callable
            Factory function to create Unit objects

        Returns
        -------
        Unit | None
            Created unit or None if creation failed
        """
        # Aggregate generator information
        aggregated_info = {
            "names": set(),
            "sources": set(),
            "technologies": set(),
            "start_dates": set(),
            "total_capacity": 0.0,
            "capacity_count": 0,
        }

        for generator in group.generators:
            tags = generator.get("tags", {})

            # Extract information from tags
            if "name" in tags or "name:en" in tags:
                name = tags.get("name:en", tags.get("name", ""))
                if name:
                    aggregated_info["names"].add(name)

            if "generator:source" in tags:
                aggregated_info["sources"].add(tags["generator:source"])

            if "generator:method" in tags or "generator:type" in tags:
                tech = tags.get("generator:method", tags.get("generator:type", ""))
                if tech:
                    aggregated_info["technologies"].add(tech)

            if "start_date" in tags or "year" in tags:
                date = tags.get("start_date", tags.get("year", ""))
                if date:
                    aggregated_info["start_dates"].add(date)

        # Determine final values
        final_name = (
            self.name_aggregator.aggregate_names(aggregated_info["names"])
            if aggregated_info["names"]
            else f"Plant Group {group.plant_id}"
        )

        # Use voting for source and technology
        final_source = None
        if aggregated_info["sources"]:
            source_counts = {}
            for source in aggregated_info["sources"]:
                source_counts[source] = source_counts.get(source, 0) + 1
            final_source = max(source_counts.items(), key=lambda x: x[1])[0]

        final_technology = None
        if aggregated_info["technologies"]:
            tech_counts = {}
            for tech in aggregated_info["technologies"]:
                tech_counts[tech] = tech_counts.get(tech, 0) + 1
            final_technology = max(tech_counts.items(), key=lambda x: x[1])[0]

        final_start_date = (
            min(aggregated_info["start_dates"])
            if aggregated_info["start_dates"]
            else None
        )

        # Create unit using factory
        return unit_factory(
            group=group,
            name=final_name,
            source=final_source,
            technology=final_technology,
            start_date=final_start_date,
            capacity_source="aggregated_from_orphaned_generators",
        )

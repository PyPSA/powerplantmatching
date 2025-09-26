# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""
Plant reconstruction from incomplete OSM data.

This module provides functionality to reconstruct power plants from
orphaned generators and incomplete plant relations.
"""

import logging
import re
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from powerplantmatching.osm.parsing.generators import GeneratorParser

logger = logging.getLogger(__name__)


class NameAggregator:
    """Aggregates multiple generator names into a plant name.

    Uses n-gram analysis to find common substrings among generator
    names to create a meaningful plant name.

    Attributes
    ----------
    similarity_threshold : float
        Minimum similarity ratio for common substrings
    """

    def __init__(self, similarity_threshold: float = 0.7):
        """Initialize name aggregator.

        Parameters
        ----------
        similarity_threshold : float, optional
            Minimum similarity ratio for common substrings.
            Default is 0.7.
        """
        self.similarity_threshold = similarity_threshold

    def aggregate_names(self, names: set[str]) -> str:
        """Aggregate multiple names into one representative name.

        Parameters
        ----------
        names : set[str]
            Generator names to aggregate

        Returns
        -------
        str
            Aggregated name or empty string
        """
        names_list = list(names)

        if len(names_list) == 1:
            return names_list[0]

        if len(names_list) == 0:
            return ""

        common_name = self._find_common_substring(names_list)

        if common_name:
            return common_name

        return names_list[0]

    def _find_common_substring(self, names: list[str]) -> str | None:
        """Find common substring among names using n-gram analysis."""
        if not names:
            return None

        all_tokens = []
        for name in names:
            tokens = re.split(r"[\s\-_,\.]+", name)
            all_tokens.append([t.lower() for t in tokens if t])

        common_ngrams = {}

        for n in range(1, 4):
            for tokens in all_tokens:
                for i in range(len(tokens) - n + 1):
                    ngram = " ".join(tokens[i : i + n])
                    if ngram not in common_ngrams:
                        common_ngrams[ngram] = 0
                    common_ngrams[ngram] += 1

        threshold = len(names) * self.similarity_threshold
        frequent_ngrams = {
            ngram: count for ngram, count in common_ngrams.items() if count >= threshold
        }

        if not frequent_ngrams:
            return None

        longest_ngram = max(frequent_ngrams.keys(), key=lambda x: len(x))

        for name in names:
            if longest_ngram in name.lower():
                start_idx = name.lower().index(longest_ngram)
                return name[start_idx : start_idx + len(longest_ngram)].strip()

        return longest_ngram.title()


class PlantReconstructor:
    """Reconstructs plants from orphaned generators.

    Analyzes groups of generators within plant boundaries to reconstruct
    missing plant data by aggregating generator attributes.

    Attributes
    ----------
    name_aggregator : NameAggregator
        Name aggregation utility
    generator_parser : GeneratorParser
        Parser for processing generators
    min_generators : int
        Minimum generators needed for reconstruction

    Examples
    --------
    >>> reconstructor = PlantReconstructor(2, name_aggregator)
    >>> if reconstructor.can_reconstruct(len(generators)):
    ...     plant_info = reconstructor.aggregate_generator_info(generators, country)
    """

    def __init__(
        self,
        min_generators: int,
        name_aggregator: NameAggregator,
        generator_parser: Optional["GeneratorParser"] = None,
    ):
        """Initialize plant reconstructor.

        Parameters
        ----------
        min_generators : int
            Minimum number of generators needed for reconstruction
        name_aggregator : NameAggregator
            Name aggregation utility
        generator_parser : GeneratorParser, optional
            Parser for generator processing
        """
        self.name_aggregator = name_aggregator
        self.generator_parser = generator_parser
        self.min_generators = min_generators

    def can_reconstruct(self, generator_count: int) -> bool:
        """Check if enough generators for reconstruction."""
        return generator_count >= self.min_generators

    def process_generators_for_reconstruction(
        self, generators: list[dict[str, Any]], country: str
    ) -> list[dict[str, Any]]:
        """Process generators to extract reconstruction data.

        Parameters
        ----------
        generators : list[dict]
            Generator elements to process
        country : str
            Country for context

        Returns
        -------
        list[dict]
            Processed generator information
        """
        if not self.generator_parser:
            raise ValueError("Generator parser required for reconstruction")

        processed_info = []

        for generator in generators:
            source = self.generator_parser.extract_source_from_tags(
                generator, "generator"
            )
            if not source:
                continue

            technology = self.generator_parser.extract_technology_from_tags(
                generator, "generator", source
            )
            name = self.generator_parser.extract_name_from_tags(generator, "generator")
            start_date = self.generator_parser.extract_start_date_key_from_tags(
                generator, "generator"
            )

            output_key = self.generator_parser.extract_output_key_from_tags(
                generator, "generator", source
            )
            capacity = None
            capacity_source = None

            if output_key:
                capacity, capacity_source = self.generator_parser._process_capacity(
                    generator, source, output_key, "generator"
                )

                if capacity is None and generator["type"] == "relation":
                    capacity, capacity_source, _ = (
                        self.generator_parser._get_relation_member_capacity(
                            generator, source, "generator"
                        )
                    )

            processed_info.append(
                {
                    "element": generator,
                    "source": source,
                    "technology": technology,
                    "name": name,
                    "start_date": start_date,
                    "capacity": capacity,
                    "capacity_source": capacity_source,
                    "output_key": output_key,
                }
            )

        return processed_info

    def aggregate_generator_info(
        self, generators: list[dict[str, Any]], country: str
    ) -> dict[str, Any]:
        """Aggregate information from multiple generators.

        Parameters
        ----------
        generators : list[dict]
            Generators to aggregate
        country : str
            Country context

        Returns
        -------
        dict
            Aggregated information with names, sources, capacities, etc.
        """
        processed_generators = self.process_generators_for_reconstruction(
            generators, country
        )

        aggregated_info = {
            "names": set(),
            "sources": set(),
            "technologies": set(),
            "start_dates": set(),
            "total_capacity": 0.0,
            "capacity_count": 0,
            "valid_generators": [],
            "output_keys": set(),
        }

        for gen_info in processed_generators:
            if gen_info["source"]:
                aggregated_info["valid_generators"].append(gen_info)

                if gen_info["name"]:
                    aggregated_info["names"].add(gen_info["name"])
                if gen_info["source"]:
                    aggregated_info["sources"].add(gen_info["source"])
                if gen_info["technology"]:
                    aggregated_info["technologies"].add(gen_info["technology"])
                if gen_info["start_date"]:
                    aggregated_info["start_dates"].add(gen_info["start_date"])
                if gen_info["capacity"] is not None and gen_info["capacity"] > 0:
                    aggregated_info["total_capacity"] += gen_info["capacity"]
                    aggregated_info["capacity_count"] += 1
                if gen_info["output_key"]:
                    aggregated_info["output_keys"].add(gen_info["output_key"])

        return aggregated_info

    def determine_final_values(
        self, aggregated_info: dict[str, Any], existing_values: dict[str, Any]
    ) -> dict[str, Any]:
        """Determine final plant values from aggregated data.

        Parameters
        ----------
        aggregated_info : dict
            Aggregated generator information
        existing_values : dict
            Any existing plant values

        Returns
        -------
        dict
            Final values for reconstructed plant
        """
        final_values = {}

        if aggregated_info["names"]:
            final_values["name"] = self.name_aggregator.aggregate_names(
                aggregated_info["names"]
            )
        else:
            final_values["name"] = existing_values.get("name", "")

        if aggregated_info["sources"]:
            if len(aggregated_info["sources"]) == 1:
                final_values["source"] = list(aggregated_info["sources"])[0]
            else:
                source_counts = {}
                for source in aggregated_info["sources"]:
                    source_counts[source] = source_counts.get(source, 0) + 1
                final_values["source"] = max(source_counts.items(), key=lambda x: x[1])[
                    0
                ]
        else:
            final_values["source"] = existing_values.get("source")

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

        if aggregated_info["start_dates"]:
            final_values["start_date"] = min(aggregated_info["start_dates"])
        else:
            final_values["start_date"] = existing_values.get("start_date", "")

        if aggregated_info["capacity_count"] > 0:
            final_values["capacity"] = aggregated_info["total_capacity"]
            final_values["capacity_source"] = "aggregated_from_generators"
        else:
            final_values["capacity"] = existing_values.get("capacity")
            final_values["capacity_source"] = existing_values.get("capacity_source")

        return final_values

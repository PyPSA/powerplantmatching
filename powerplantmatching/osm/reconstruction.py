"""
Plant reconstruction functionality for OSM data processing.

This module handles the reconstruction of incomplete plant data by:
1. Completing existing plant relations using member generator information
2. Creating new plants from orphaned generators within rejected plant boundaries
"""

import logging
import re
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .generator_parser import GeneratorParser

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

    def __init__(
        self,
        config: dict[str, Any],
        name_aggregator: NameAggregator,
        generator_parser: Optional["GeneratorParser"] = None,
    ):
        """
        Initialize the plant reconstructor.

        Parameters
        ----------
        config : dict[str, Any]
            Configuration dictionary
        name_aggregator : NameAggregator
            Name aggregator instance
        generator_parser : GeneratorParser, optional
            Generator parser instance for processing generators
        """
        self.config = config
        self.name_aggregator = name_aggregator
        self.generator_parser = generator_parser
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

    def process_generators_for_reconstruction(
        self, generators: list[dict[str, Any]], country: str
    ) -> list[dict[str, Any]]:
        """
        Process generators using the generator parser to extract all information.

        Parameters
        ----------
        generators : list[dict[str, Any]]
            List of generator elements
        country : str
            Country code for processing

        Returns
        -------
        list[dict[str, Any]]
            List of processed generator info dictionaries
        """
        if not self.generator_parser:
            raise ValueError("Generator parser required for reconstruction")

        processed_info = []

        for generator in generators:
            # Use generator parser's extraction methods
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

            # Extract capacity using generator parser's sophisticated logic
            output_key = self.generator_parser.extract_output_key_from_tags(
                generator, "generator", source
            )
            capacity = None
            capacity_source = None

            if output_key:
                capacity, capacity_source = self.generator_parser._process_capacity(
                    generator, source, output_key, "generator"
                )

                # Handle relation members if needed
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
        """
        Aggregate information from multiple generators using generator parser.

        Parameters
        ----------
        generators : list[dict[str, Any]]
            List of generator elements
        country : str
            Country code for processing

        Returns
        -------
        dict[str, Any]
            Aggregated information including names, sources, technologies, etc.
        """
        # Process all generators through generator parser
        processed_generators = self.process_generators_for_reconstruction(
            generators, country
        )

        # Aggregate the results
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
            if gen_info["source"]:  # Only include valid generators
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
        """
        Determine final values from aggregated information.

        Parameters
        ----------
        aggregated_info : dict[str, Any]
            Aggregated information from generators (already processed and mapped)
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

        # Source - use voting (most common) from already mapped sources
        if aggregated_info["sources"]:
            if len(aggregated_info["sources"]) == 1:
                final_values["source"] = list(aggregated_info["sources"])[0]
            else:
                # Vote for most common mapped source
                source_counts = {}
                for source in aggregated_info["sources"]:
                    source_counts[source] = source_counts.get(source, 0) + 1
                final_values["source"] = max(source_counts.items(), key=lambda x: x[1])[
                    0
                ]
        else:
            final_values["source"] = existing_values.get("source")

        # Technology - use voting (most common) from already mapped technologies
        if aggregated_info["technologies"]:
            if len(aggregated_info["technologies"]) == 1:
                final_values["technology"] = list(aggregated_info["technologies"])[0]
            else:
                # Vote for most common
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

        # Capacity - use total if available
        if aggregated_info["capacity_count"] > 0:
            final_values["capacity"] = aggregated_info["total_capacity"]
            final_values["capacity_source"] = "aggregated_from_generators"
        else:
            final_values["capacity"] = existing_values.get("capacity")
            final_values["capacity_source"] = existing_values.get("capacity_source")

        return final_values

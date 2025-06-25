#!/usr/bin/env python3
import logging
import os
import sys
from datetime import datetime

import powerplantmatching as pm
from powerplantmatching.osm import (
    find_outdated_caches,
    populate_cache,
    show_country_coverage,
)


class TeeOutput:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log_file = open(filename, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)
        self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

    def close(self):
        self.log_file.close()


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def example_basic_usage():
    print("\n" + "=" * 60)
    print("EXAMPLE 1: Basic OSM Data Loading")
    print("=" * 60)

    config = pm.get_config()

    config["target_countries"] = ["Luxembourg", "Malta"]

    try:
        df = pm.data.OSM(config=config)

        if df is not None and not df.empty:
            print(f"✓ Loaded {len(df)} power plants")
            print(f"Countries: {df['Country'].unique()}")

            if "Capacity" in df.columns:
                print("\nCapacity Statistics:")
                print(f"  Total: {df['Capacity'].sum():.1f} MW")
                print(f"  Average: {df['Capacity'].mean():.1f} MW")
                print(f"  Max: {df['Capacity'].max():.1f} MW")

            if "Fueltype" in df.columns:
                print("\nFuel Type Distribution:")
                for fuel, count in df["Fueltype"].value_counts().items():
                    print(f"  {fuel}: {count}")
        else:
            print("No data returned - check if cache is populated")

    except Exception as e:
        logger.error(f"Error loading OSM data: {e}")


def example_cache_population():
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Cache Population")
    print("=" * 60)

    test_countries = [
        "Kenya",
        "Egypt",
        "Panama",
    ]

    print(f"Populating cache for: {', '.join(test_countries)}")

    try:
        result = populate_cache(
            countries=test_countries,
            force_refresh=False,
            show_progress=True,
        )

        print("\nResults:")
        print(f"  Downloaded: {result['succeeded']}")
        print(f"  Cached: {result['skipped']}")
        print(f"  Failed: {result['failed']}")
        print(f"  Time: {result['elapsed_time']:.1f}s")

    except Exception as e:
        logger.error(f"Error populating cache: {e}")


def example_cache_coverage():
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Cache Coverage Analysis")
    print("=" * 60)

    show_country_coverage(show_missing=False, check_live_counts=True)


def example_find_outdated():
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Finding Outdated Caches")
    print("=" * 60)

    try:
        outdated = find_outdated_caches(threshold=0.95)

        if outdated:
            print(f"\nFound {len(outdated)} outdated caches:")
            for country in outdated:
                print(f"  {country['name']}: {country['total_missing']} new elements")
        else:
            print("\nAll checked caches are up to date!")

    except Exception as e:
        logger.error(f"Error checking outdated caches: {e}")


def example_diagnostic():
    print("\n" + "=" * 60)
    print("EXAMPLE 5: Data Quality Diagnostic")
    print("=" * 60)

    from powerplantmatching.osm import ElementCache, OverpassAPIClient
    from powerplantmatching.osm.models import Units
    from powerplantmatching.osm.rejection import RejectionTracker
    from powerplantmatching.osm.utils import get_country_code
    from powerplantmatching.osm.workflow import Workflow

    config = pm.get_config()
    osm_config = config.get("OSM", {})
    cache_dir = os.path.join(
        os.path.dirname(pm.data._data_in(osm_config.get("fn", "osm_data.csv"))),
        "osm_cache",
    )

    test_country = "Kenya"
    test_code = get_country_code(test_country)

    print(f"Diagnosing {test_country} ({test_code})...")

    cache = ElementCache(cache_dir)
    cache.load_all_caches()

    plants_data = cache.get_plants(test_code)
    generators_data = cache.get_generators(test_code)

    print("\nRaw Cache Data:")
    print(f"  Plants: {len(plants_data.get('elements', [])) if plants_data else 0}")
    print(
        f"  Generators: {len(generators_data.get('elements', [])) if generators_data else 0}"
    )

    try:
        with OverpassAPIClient(
            api_url=osm_config.get("overpass_api", {}).get("url"),
            cache_dir=cache_dir,
            show_progress=False,
        ) as client:
            units = Units()
            rejections = RejectionTracker()

            workflow = Workflow(
                client=client,
                rejection_tracker=rejections,
                units=units,
                config=osm_config,
            )

            updated_units, _ = workflow.process_country_data(test_country)
            country_units = updated_units.filter_by_country(test_country)

            print("\nProcessing Results:")
            print(f"  Valid units: {len(country_units)}")
            print(f"  Rejections: {rejections.get_total_count()}")

            if rejections.get_total_count() > 0:
                print("\nTop rejection reasons:")
                stats = rejections.get_rejection_statistics()
                for reason, count in list(stats.items())[:5]:
                    print(f"  - {reason}: {count}")

    except Exception as e:
        logger.error(f"Error in diagnostic: {e}")


def main(save_output=True, output_filename=None):
    tee_output = None
    if save_output:
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"osm_coverage_output_{timestamp}.txt"

        print(f"Saving output to: {output_filename}")
        tee_output = TeeOutput(output_filename)
        sys.stdout = tee_output

    try:
        print("=" * 80)
        print("POWERPLANTMATCHING OSM MODULE EXAMPLES")
        print("=" * 80)
        print(f"Started at: {datetime.now()}")

        example_basic_usage()
        example_cache_population()
        example_cache_coverage()
        example_find_outdated()
        example_diagnostic()

        print("\n" + "=" * 80)
        print(f"Completed at: {datetime.now()}")
        print("=" * 80)

        if save_output:
            print(f"\nOutput saved to: {output_filename}")

    finally:
        if tee_output:
            sys.stdout = tee_output.terminal
            tee_output.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="OSM Coverage Analysis Examples")
    parser.add_argument(
        "--no-save", action="store_true", help="Don't save output to file"
    )
    parser.add_argument(
        "--output", type=str, help="Custom output filename", default=None
    )

    args = parser.parse_args()

    main(save_output=not args.no_save, output_filename=args.output)

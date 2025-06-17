import logging
import os

import pandas as pd

from powerplantmatching.core import _data_in, get_config
from powerplantmatching.osm.client import OverpassAPIClient
from powerplantmatching.osm.models import Units
from powerplantmatching.osm.rejection import RejectionTracker
from powerplantmatching.osm.workflow import Workflow

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Main execution
if __name__ == "__main__":
    # Load configuration
    config_main = get_config()
    config = config_main["OSM"]
    config["force_refresh"] = True  # True to test rejection tracker logic
    config["plants_only"] = True
    config["missing_name_allowed"] = False
    config["missing_technology_allowed"] = False
    config["missing_start_date_allowed"] = True
    config["capacity_estimation"]["enabled"] = False
    config["units_clustering"]["enabled"] = False
    config["units_reconstruction"]["enabled"] = True

    # Initialize organized directory structure
    output_dir = "outputs"

    # Create organized subdirectories
    subdirs = {
        "csv": os.path.join(output_dir, "csv"),
        "geojson": os.path.join(output_dir, "geojson"),
        "by_country": os.path.join(output_dir, "by_country"),
        "by_fuel_type": os.path.join(output_dir, "by_fuel_type"),
        "by_rejection_reason": os.path.join(output_dir, "by_rejection_reason"),
        "rejections": os.path.join(output_dir, "rejections"),
    }

    # Create all directories
    for subdir in subdirs.values():
        os.makedirs(subdir, exist_ok=True)

    fn = _data_in(config.get("fn", "osm_data.csv"))
    cache_dir = os.path.join(os.path.dirname(fn), "osm_cache")
    os.makedirs(cache_dir, exist_ok=True)

    # List of countries to process
    countries = [
        "Chile",
        "South Africa",
        "Indonesia",
    ]  # "Uruguay", "Costa Rica", "Kenya", "Zambia"

    # Initialize collections
    rejection_tracker = RejectionTracker()
    all_units = Units()  # Use new Units collection class

    # Process each country
    with OverpassAPIClient(
        api_url=config["overpass_api"]["url"], cache_dir=cache_dir
    ) as client:
        for country_name in countries:
            logger.info(f"Processing {country_name}...")
            workflow = Workflow(client, rejection_tracker, all_units, config)
            updated_units = workflow.process_country_data(country=country_name)

            # Save individual country CSV in by_country folder
            country_units = all_units.filter_by_country(country_name)
            if len(country_units) > 0:
                output_file = os.path.join(
                    subdirs["by_country"],
                    f"{country_name.lower().replace(' ', '_')}_power_plants.csv",
                )
                country_units.save_csv(output_file)
                print(f"Saved {len(country_units)} power plants to {output_file}")
            else:
                print(f"No units found for {country_name}")

    # Generate comprehensive reports
    logger.info("Generating comprehensive reports...")

    # Print statistics
    stats = all_units.get_statistics()
    print("\n" + "=" * 60)
    print("PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Total Units Processed: {stats['total_units']}")
    print(
        f"Units with Coordinates: {stats['units_with_coordinates']} ({stats['coverage_percentage']}%)"
    )
    print(f"Countries: {', '.join(stats['countries'])}")
    print(f"Fuel Types: {', '.join(stats['fuel_types'])}")
    print(f"Technologies: {', '.join(stats['technologies'])}")
    print(f"Total Capacity: {stats['total_capacity_mw']} MW")
    print(f"Average Capacity: {stats['average_capacity_mw']} MW")
    print("=" * 60)

    # Save combined data in csv folder
    combined_csv_path = os.path.join(subdirs["csv"], "all_countries_power_plants.csv")
    all_units.save_csv(combined_csv_path)
    print(f"Saved combined data: {combined_csv_path}")

    # Save main units GeoJSON in geojson folder
    units_geojson_path = os.path.join(subdirs["geojson"], "power_plants.geojson")
    all_units.save_geojson_report(units_geojson_path, styled=True)
    print(f"Saved units GeoJSON: {units_geojson_path}")

    # Save main rejections reports in rejections folder
    rejections_geojson_path = os.path.join(subdirs["rejections"], "rejections.geojson")
    rejection_tracker.save_geojson_report(rejections_geojson_path)
    print(f"Saved rejections GeoJSON: {rejections_geojson_path}")

    rejections_csv_path = os.path.join(subdirs["rejections"], "rejections.csv")
    rejection_tracker.generate_report().to_csv(rejections_csv_path, index=False)
    print(f"Saved rejections CSV: {rejections_csv_path}")

    # Generate fuel type specific files in by_fuel_type folder
    print("\nGenerating fuel type specific files...")
    fuel_types = stats["fuel_types"]
    for fuel_type in fuel_types:
        fuel_units = all_units.filter_by_fueltype(fuel_type)
        if len(fuel_units) > 0:
            fuel_name = fuel_type.lower().replace(" ", "_")

            # Save CSV
            fuel_csv_path = os.path.join(
                subdirs["by_fuel_type"], f"{fuel_name}_power_plants.csv"
            )
            fuel_units.save_csv(fuel_csv_path)

            # Save GeoJSON
            fuel_geojson_path = os.path.join(
                subdirs["by_fuel_type"], f"{fuel_name}_power_plants.geojson"
            )
            fuel_units.save_geojson_report(fuel_geojson_path, styled=True)

            print(
                f"  - {fuel_type}: {len(fuel_units)} units → CSV & GeoJSON in by_fuel_type/"
            )

    # Generate country specific files in by_country folder
    print("\nGenerating country specific files...")
    countries_processed = stats["countries"]
    for country in countries_processed:
        country_units = all_units.filter_by_country(country)
        if len(country_units) > 0:
            country_name = country.lower().replace(" ", "_")

            # CSV already saved during processing

            # Save GeoJSON
            country_geojson_path = os.path.join(
                subdirs["by_country"], f"{country_name}_power_plants.geojson"
            )
            country_units.save_geojson_report(country_geojson_path, styled=True)

            print(
                f"  - {country}: {len(country_units)} units → CSV & GeoJSON in by_country/"
            )

    # Generate rejection reason specific files in by_rejection_reason folder
    print("\nGenerating rejection reason specific files...")
    rejection_stats = rejection_tracker.get_rejection_statistics()
    country_stats = rejection_tracker.get_country_statistics()

    print(
        f"Found {len(rejection_stats)} different rejection reasons across {len(country_stats)} countries:"
    )

    # Show top rejection reasons
    for reason, count in list(rejection_stats.items())[:10]:  # Top 10 reasons
        print(f"  - {reason}: {count:,} rejections")

    if len(rejection_stats) > 10:
        remaining_count = sum(list(rejection_stats.values())[10:])
        print(
            f"  - ... and {len(rejection_stats) - 10} more reasons ({remaining_count:,} rejections)"
        )

    print("\nRejections by country:")
    for country, count in country_stats.items():
        print(f"  - {country}: {count:,} rejections")

    # Generate GeoJSON files for each rejection reason in by_rejection_reason folder
    rejection_tracker.save_all_rejection_reasons_geojson(
        subdirs["by_rejection_reason"], "rejections"
    )

    # Also save rejection statistics as CSV
    rejection_stats_csv = os.path.join(
        subdirs["rejections"], "rejection_statistics.csv"
    )

    # Create rejection statistics summary
    rejection_summary_data = []
    for reason, count in rejection_stats.items():
        rejection_summary_data.append(
            {
                "rejection_reason": reason,
                "count": count,
                "percentage": round(count / sum(rejection_stats.values()) * 100, 2),
            }
        )

    rejection_summary_df = pd.DataFrame(rejection_summary_data)
    rejection_summary_df.to_csv(rejection_stats_csv, index=False)
    print(f"Saved rejection statistics: {rejection_stats_csv}")

    # Create country statistics summary
    country_stats_csv = os.path.join(
        subdirs["rejections"], "country_rejection_statistics.csv"
    )
    country_summary_data = []
    for country, count in country_stats.items():
        country_summary_data.append(
            {
                "country": country,
                "rejection_count": count,
                "percentage": round(count / sum(country_stats.values()) * 100, 2),
            }
        )

    country_summary_df = pd.DataFrame(country_summary_data)
    country_summary_df.to_csv(country_stats_csv, index=False)
    print(f"Saved country rejection statistics: {country_stats_csv}")

    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE!")
    print("=" * 60)
    print("📁 Organized file structure:")
    print(f"  📂 {output_dir}/")
    print("    📂 csv/                     - Combined CSV files")
    print("    📂 geojson/                 - Main GeoJSON files")
    print("    📂 by_country/              - Country-specific files")
    print("    📂 by_fuel_type/            - Fuel type-specific files")
    print("    📂 by_rejection_reason/     - Rejection reason-specific files")
    print("    📂 rejections/              - Rejection analysis files")
    print("")
    print("📊 Generated files:")
    print("  - Combined power plants CSV & GeoJSON")
    print("  - Country-specific CSV & GeoJSON files")
    print("  - Fuel type-specific CSV & GeoJSON files")
    print("  - Rejection reason-specific GeoJSON files")
    print("  - Comprehensive rejection analysis CSVs")
    print("=" * 60)

"""
This script demonstrates the end-to-end usage of the PowerPlantMatching (PPM) tool with a focus on OSM data.
It configures the pipeline, processes power plant data from OpenStreetMap for selected countries, 
tracks rejected power plants from the searched countries, and exports results into structured CSV and GeoJSON files.

Main functionalities:
- Run country-level plant extraction using OSM and Overpass API.
- Track rejections and categorize them.
- Export valid and rejected power plants to file structures organized by country, source type, and rejection reason.

Outputs:
- CSV and GeoJSON of valid power plants.
- Rejection statistics and corresponding GeoJSON/CSV.
"""


import logging
import os
import pandas as pd

from powerplantmatching.core import _data_in, get_config
from powerplantmatching.osm import (
    OverpassAPIClient,
    RejectionTracker,
    Units,
    Workflow,
    validate_countries,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def prepare_output_directories(base_dir: str = "outputs") -> dict:
    """
    Create and return output directory paths for various result types.

    Args:
        base_dir (str): Root output directory.

    Returns:
        dict: Dictionary of subdirectory paths.
    """
    subdirs = {
        "csv": os.path.join(base_dir, "csv"),
        "geojson": os.path.join(base_dir, "geojson"),
        "by_country": os.path.join(base_dir, "by_country"),
        "by_fuel_type": os.path.join(base_dir, "by_fuel_type"),
        "by_rejection_reason": os.path.join(base_dir, "by_rejection_reason"),
        "rejections": os.path.join(base_dir, "rejections"),
    }
    for subdir in subdirs.values():
        os.makedirs(subdir, exist_ok=True)
    return subdirs


def run_country_pipeline(country: str, client, config, units: Units, rejections: RejectionTracker, output_dir: str):
    """
    Process a single country's data using PPM pipeline.

    Args:
        country (str): Country name
        client (OverpassAPIClient): OSM client
        config (dict): Configuration dictionary
        units (Units): Units container (shared across countries)
        rejections (RejectionTracker): Rejection tracker
        output_dir (str): Path to store country-specific outputs
    """
    logger.info(f"Processing {country}...")
    workflow = Workflow(client, rejections, units, config)
    _, _ = workflow.process_country_data(country=country)

    country_units = units.filter_by_country(country)
    if len(country_units) > 0:
        file_path = os.path.join(
            output_dir,
            f"{country.lower().replace(' ', '_')}_power_plants.csv",
        )
        country_units.save_csv(file_path)
        logger.info(f"Saved {len(country_units)} units to {file_path}")
    else:
        logger.warning(f"No valid units found for {country}")


def generate_summary_reports(units: Units, stats: dict, subdirs: dict):
    """
    Save global CSV/GeoJSON, and fuel or country specific reports.

    Args:
        units (Units): All processed units
        stats (dict): Statistics from Units
        subdirs (dict): Output subdirectories
    """
    # Save full CSV + GeoJSON
    units.save_csv(os.path.join(subdirs["csv"], "all_countries_power_plants.csv"))
    units.save_geojson_report(os.path.join(subdirs["geojson"], "power_plants.geojson"))

    # Save by fuel type
    for fuel_type in stats["fuel_types"]:
        fuel_units = units.filter_by_fueltype(fuel_type)
        if len(fuel_units) == 0:
            continue

        name = fuel_type.lower().replace(" ", "_")
        fuel_units.save_csv(os.path.join(subdirs["by_fuel_type"], f"{name}_power_plants.csv"))
        fuel_units.save_geojson_report(os.path.join(subdirs["by_fuel_type"], f"{name}_power_plants.geojson"))

    # Save by country
    for country in stats["countries"]:
        country_units = units.filter_by_country(country)
        if len(country_units) == 0:
            continue

        name = country.lower().replace(" ", "_")
        country_units.save_geojson_report(os.path.join(subdirs["by_country"], f"{name}_power_plants.geojson"))


def save_rejection_stats(rejection_tracker: RejectionTracker, subdirs: dict):
    """
    Save rejection statistics and reason-specific GeoJSONs.

    Args:
        rejection_tracker (RejectionTracker): Tracker with rejections
        subdirs (dict): Output subdirectories
    """
    # Save main rejections
    rejection_tracker.save_geojson(os.path.join(subdirs["rejections"], "rejections.geojson"))
    rejection_tracker.generate_report().to_csv(
        os.path.join(subdirs["rejections"], "rejections.csv"), index=False
    )

    # Save by reason
    rejection_tracker.save_geojson_by_reasons(subdirs["by_rejection_reason"], "rejections")

    # Stats by reason
    rejection_summary = rejection_tracker.get_statistics()["by_reason"]
    rejection_stats_csv = os.path.join(subdirs["rejections"], "rejection_statistics.csv")
    pd.DataFrame([
        {
            "rejection_reason": reason,
            "count": count,
            "percentage": round(count / sum(rejection_summary.values()) * 100, 2),
        }
        for reason, count in rejection_summary.items()
    ]).to_csv(rejection_stats_csv, index=False)

    # Stats by country
    country_stats = rejection_tracker.get_country_statistics()
    country_stats_csv = os.path.join(subdirs["rejections"], "country_rejection_statistics.csv")
    pd.DataFrame([
        {
            "country": country,
            "rejection_count": count,
            "percentage": round(count / sum(country_stats.values()) * 100, 2),
        }
        for country, count in country_stats.items()
    ]).to_csv(country_stats_csv, index=False)


def main():
    config_main = get_config()
    config = config_main["OSM"]

    # Configuration overrides
    config.update({
        "force_refresh": True,
        "plants_only": True,
        "missing_name_allowed": False,
        "missing_technology_allowed": False,
        "missing_start_date_allowed": True,
    })
    config["capacity_estimation"]["enabled"] = False
    config["units_clustering"]["enabled"] = False
    config["units_reconstruction"]["enabled"] = True

    subdirs = prepare_output_directories()

    fn = _data_in(config.get("fn", "osm_data.csv"))
    cache_dir = os.path.join(os.path.dirname(fn), "osm_cache")
    os.makedirs(cache_dir, exist_ok=True)

    countries = ["Chile"]  # Extend as needed

    rejection_tracker = RejectionTracker()
    all_units = Units()

    with OverpassAPIClient(
        api_url=config["overpass_api"]["api_url"], cache_dir=cache_dir
    ) as client:
        validate_countries(countries)
        for country in countries:
            run_country_pipeline(
                country=country,
                client=client,
                config=config,
                units=all_units,
                rejections=rejection_tracker,
                output_dir=subdirs["by_country"]
            )

    stats = all_units.get_statistics()
    generate_summary_reports(all_units, stats, subdirs)
    save_rejection_stats(rejection_tracker, subdirs)


    # Final summary report
    logger.info("\n" + "=" * 60)
    logger.info("PROCESSING COMPLETE!")
    logger.info("=" * 60)
    logger.info("📁 Organized file structure:")
    logger.info(f"  📂 {subdirs['csv']}/                 - Combined CSV files")
    logger.info(f"  📂 {subdirs['geojson']}/             - Main GeoJSON files")
    logger.info(f"  📂 {subdirs['by_country']}/          - Country-specific files")
    logger.info(f"  📂 {subdirs['by_fuel_type']}/        - Fuel type-specific files")
    logger.info(f"  📂 {subdirs['by_rejection_reason']}/ - Rejection reason-specific files")
    logger.info(f"  📂 {subdirs['rejections']}/          - Rejection analysis files")
    logger.info("")
    logger.info("📊 Generated files:")
    logger.info("  - Combined power plants CSV & GeoJSON")
    logger.info("  - Country-specific CSV & GeoJSON files")
    logger.info("  - Fuel type-specific CSV & GeoJSON files")
    logger.info("  - Rejection reason-specific GeoJSON files")
    logger.info("  - Comprehensive rejection analysis CSVs")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

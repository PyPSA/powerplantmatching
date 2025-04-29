# Example usage of OSM module with configuration
import logging
import os

import pandas as pd

from powerplantmatching.osm.client import OverpassAPIClient
from powerplantmatching.osm.rejection import RejectionTracker
from powerplantmatching.osm.workflow import Workflow

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Load configuration
config = {
    "default_source_type": "unknown",
    "capacity_extraction": {
        "enabled": True,
        "tags": [
            "plant:output:electricity",
            "generator:output:electricity",
            "generator:output",
            "power_output",
            "capacity",
            "output:electricity",
        ],
        "basic": {
            "min_capacity_mw": 0.001,
            "max_capacity_mw": 10000,
            "default_unit": "kw",
        },
        "advanced_parsing": {
            "enabled": True,
            "regex_patterns": ["^(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+p?)$"],
            "unit_conversions": {
                "w": 1e-06,
                "wp": 1e-06,
                "watt": 1e-06,
                "watts": 1e-06,
                "kw": 0.001,
                "kwp": 0.001,
                "kilowatt": 0.001,
                "kilowatts": 0.001,
                "mw": 1.0,
                "mwp": 1.0,
                "megawatt": 1.0,
                "megawatts": 1.0,
                "gw": 1000.0,
                "gwp": 1000.0,
                "gigawatt": 1000.0,
                "gigawatts": 1000.0,
            },
        },
    },
    "capacity_estimation": {
        "enabled": True,
        "methods": ["default_value", "area_based", "unit_size"],
    },
    "clustering": {
        "enabled": True,
        "method": "dbscan",
        "eps": 0.01,
        "min_samples": 2,
        "n_clusters": 8,
        "to_radians": False,
    },
    "sources": {
        "solar": {
            "clustering": {"method": "dbscan", "eps": 0.005, "min_samples": 2},
            "capacity_extraction": {
                "enabled": True,
                "additional_tags": ["solar:output", "pv:output"],
                "basic": {"min_capacity_mw": 0.0005, "max_capacity_mw": 2000},
            },
            "estimation": {
                "method": "area_based",
                "efficiency": 150,
                "default_capacity": 10,
                "fallback_enabled": True,
                "fallback_method": "default_value",
            },
        },
        "wind": {
            "clustering": {"method": "dbscan", "eps": 0.02, "min_samples": 2},
            "capacity_extraction": {
                "enabled": True,
                "additional_tags": ["wind:output", "turbine:output"],
                "basic": {"min_capacity_mw": 0.1, "max_capacity_mw": 7000},
            },
            "estimation": {
                "method": "unit_size",
                "unit_capacity": 2000,
                "fallback_enabled": True,
                "fallback_method": "default_value",
            },
        },
        "hydro": {
            "clustering": {"method": "dbscan", "eps": 0.01, "min_samples": 2},
            "capacity_extraction": {
                "enabled": True,
                "additional_tags": ["hydro:output"],
            },
            "estimation": {
                "method": "default_value",
                "default_capacity": 1000,
                "fallback_enabled": False,
            },
        },
        "nuclear": {
            "clustering": {"method": "dbscan", "eps": 0.01, "min_samples": 2},
            "capacity_extraction": {"enabled": True},
            "estimation": {"method": "default_value", "default_capacity": 500000},
        },
        "coal": {
            "clustering": {"method": "dbscan", "eps": 0.01, "min_samples": 2},
            "estimation": {"method": "default_value", "default_capacity": 300000},
        },
        "gas": {
            "clustering": {"method": "dbscan", "eps": 0.01, "min_samples": 2},
            "estimation": {"method": "default_value", "default_capacity": 100000},
        },
        "biomass": {
            "clustering": {"method": "dbscan", "eps": 0.01, "min_samples": 2},
            "estimation": {"method": "default_value", "default_capacity": 10000},
        },
        "geothermal": {
            "clustering": {"method": "dbscan", "eps": 0.01, "min_samples": 2},
            "estimation": {"method": "default_value", "default_capacity": 20000},
        },
        "oil": {
            "clustering": {"method": "dbscan", "eps": 0.01, "min_samples": 2},
            "estimation": {"method": "default_value", "default_capacity": 75000},
        },
        "diesel": {
            "clustering": {"method": "dbscan", "eps": 0.01, "min_samples": 2},
            "estimation": {"method": "default_value", "default_capacity": 30000},
        },
        "unknown": {
            "clustering": {"method": "dbscan", "eps": 0.01, "min_samples": 2},
            "estimation": {"method": "default_value", "default_capacity": 0},
        },
    },
    "rejection_handling": {
        "log_rejections": True,
        "save_rejections": True,
        "rejections_file": "osm_rejections.json",
    },
    "overpass_api": {
        "url": "https://overpass-api.de/api/interpreter",
        "timeout": 300,
        "max_retries": 3,
        "retry_delay": 5,
    },
}

# Initialize client and rejection tracker
cache_dir = "osm_cache"
client = OverpassAPIClient(api_url=config["overpass_api"]["url"], cache_dir=cache_dir)
rejection_tracker = RejectionTracker()

# Create workflow
workflow = Workflow(client=client, config=config, rejection_tracker=rejection_tracker)


# Function to process a country
def process_country(country_name, output_dir="outputs"):
    """Process a country and save the results"""
    print(f"\nProcessing {country_name}...")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Process the country
    try:
        # Get the power plants
        units = workflow.process_country_data(
            country=country_name,
            plants_only=False,
        )

        # Convert to DataFrame
        if units:
            units_dicts = [unit.to_dict() for unit in units]
            units_df = pd.DataFrame(units_dicts)

            # Save to CSV
            output_file = os.path.join(
                output_dir, f"{country_name.lower()}_power_plants.csv"
            )
            units_df.to_csv(output_file, index=False)
            print(f"Saved {len(units)} power plants to {output_file}")

            # Display summary
            if not units_df.empty:
                source_summary = units_df["source"].value_counts().to_dict()
                capacity_total = units_df["capacity_mw"].sum()
                print(f"Total capacity: {capacity_total:.2f} MW")
                print("Sources distribution:")
                for source, count in source_summary.items():
                    print(f"  - {source}: {count} units")
        else:
            print("No units found for this country")

        # Get rejection summary
        rejection_summary = workflow.get_rejected_summary()
        print("\nRejection summary:")

        total_rejections = 0
        for category, reasons in rejection_summary.items():
            category_total = sum(reasons.values())
            total_rejections += category_total
            print(f"  - {category}: {category_total} rejections")

        print(f"Total rejections: {total_rejections}")

        # Save rejections if enabled
        if config["rejection_handling"]["save_rejections"]:
            # Create a DataFrame from rejections
            all_rejections = rejection_tracker.get_all_rejections()
            rejection_dicts = [
                {
                    "element_id": r.element_id,
                    "element_type": r.element_type.value,
                    "reason": r.reason.value,
                    "details": r.details,
                    "timestamp": r.timestamp.isoformat(),
                    "category": next(
                        (
                            c
                            for c, rejections in rejection_tracker.categories.items()
                            if r in rejections
                        ),
                        "unknown",
                    ),
                }
                for r in all_rejections
            ]

            if rejection_dicts:
                rejection_df = pd.DataFrame(rejection_dicts)
                rejection_file = os.path.join(
                    output_dir, f"{country_name.lower()}_rejections.csv"
                )
                rejection_df.to_csv(rejection_file, index=False)
                print(f"Saved {len(rejection_dicts)} rejections to {rejection_file}")

        return True
    except Exception as e:
        print(f"Error processing {country_name}: {str(e)}")
        return False
    finally:
        # Always save the cache
        print("Saving cache...")
        client.cache.save_all_caches(force=True)


# Main execution
if __name__ == "__main__":
    # List of countries to process
    countries = ["Ecuador", "Uruguay", "Costa Rica", "Kenya", "Zambia"]

    # Process each country
    for country in countries:
        success = process_country(country)
        if not success:
            print(f"Skipping further processing due to error with {country}")
            break

    print("\nProcessing complete")

import logging
import os

import pandas as pd

from powerplantmatching.core import get_config
from powerplantmatching.osm.client import OverpassAPIClient
from powerplantmatching.osm.rejection import RejectionTracker
from powerplantmatching.osm.workflow import Workflow

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Load configuration
config_main = get_config()
config = config_main["OSM"]


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
        # Get the power plants and rejection summary
        units, rejection_summary = workflow.process_country_data(
            country=country_name,
            export_rejections=config["rejection_handling"]["save_rejections"],
            rejections_file=os.path.join(
                output_dir,
                config["rejection_handling"]["rejections_file"].format(
                    country=country_name.lower()
                ),
            ),
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

        # Print rejection summary
        print("\nRejection summary:")

        total_rejections = 0
        for category, reasons in rejection_summary.items():
            category_total = sum(reasons.values())
            total_rejections += category_total
            print(f"  - {category}: {category_total} rejections")

        print(f"Total rejections: {total_rejections}")

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
    countries = ["Ecuador"]  # "Uruguay", "Costa Rica", "Kenya", "Zambia"

    # Process each country
    for country in countries:
        success = process_country(country, output_dir="outputs3")
        if not success:
            print(f"Skipping further processing due to error with {country}")
            break

    print("\nProcessing complete")

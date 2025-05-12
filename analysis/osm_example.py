import logging
import os

import pandas as pd

from powerplantmatching.core import _data_in, get_config
from powerplantmatching.osm.client import OverpassAPIClient
from powerplantmatching.osm.rejection import RejectionTracker
from powerplantmatching.osm.workflow import Workflow

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


# Main execution
if __name__ == "__main__":
    # Load configuration
    config_main = get_config()
    config = config_main["OSM"]
    config["force_refresh"] = True
    config["plants_only"] = False
    config["units_clustering"]["enabled"] = False
    config["missing_name_allowed"] = False
    config["missing_technology_allowed"] = False
    config["missing_start_date_allowed"] = False

    # Initialize client and rejection tracker
    output_dir = "outputs"
    fn = _data_in(config.get("fn", "osm_data.csv"))
    cache_dir = os.path.join(os.path.dirname(fn), "osm_cache")
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # List of countries to process
    countries = [
        "Chile",
        "South Africa",
        "Indonesia",
    ]  # "Uruguay", "Costa Rica", "Kenya", "Zambia"

    rejection_tracker = RejectionTracker()
    with OverpassAPIClient(
        api_url=config["overpass_api"]["url"], cache_dir=cache_dir
    ) as client:
        for country_name in countries:
            workflow = Workflow(client, rejection_tracker, config)
            units, rejection_tracker = workflow.process_country_data(
                country=country_name
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
            else:
                print("No units found for this country")

    df = rejection_tracker.generate_report()
    df.to_csv(os.path.join(output_dir, "rejections.csv"), index=False)

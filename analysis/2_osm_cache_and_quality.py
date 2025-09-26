#!/usr/bin/env python3
"""
OSM Tutorial Part 2: Cache Management and Data Quality
=====================================================

Learn how to:
1. Manage the OSM cache system
2. Track data quality and rejections
3. Download data for new countries

Cache Structure:
The OSM module uses a unified cache directory containing:
- osm_data.csv: Combined CSV cache for all countries
- plants/: Raw plant data from OpenStreetMap
- generators/: Generator data from OpenStreetMap
- units/: Processed unit data

You can set a custom cache location in config.yaml:
OSM:
  cache_dir: ~/osm_caches/global
  fn: osm_data.csv
"""

from powerplantmatching.core import get_config
from powerplantmatching.osm import (
    find_outdated_caches,
    get_country_coverage_data,
    populate_cache,
    print_coverage_report,
)

# Example 1: Check what's in the cache
# ====================================
print("=== Current Cache Status ===")

# You can specify a custom cache directory
# If not specified, it uses the value from config.yaml
# or defaults to ./osm_cache
data = get_country_coverage_data(
    cache_dir=None,  # Uses config value or default
    check_live_counts=False,  # Don't query live OSM data
)

print_coverage_report(
    coverage_data=data,
    show_missing=False,
    check_live_counts=False,
    show_outdated_only=False,
)

# Using a specific cache directory:
# get_country_coverage_data(cache_dir="~/osm_caches/europe")

# Note: check_live_counts=True would:
# - Query the Overpass API for current element counts
# - Compare cached vs. live data to identify outdated caches
# - Show which countries have new power plants since last download
# - This is slower as it makes API calls for each country


# Example 2: Find outdated caches
# ===============================
# Identify countries where OSM has new data since last download

print("\n=== Checking for Outdated Data ===")
outdated = find_outdated_caches(
    threshold=0.95,  # Flag if cache has <95% of current OSM data
    check_specific_countries=["Germany", "France", "Spain"],
)

if outdated:
    print(f"Found {len(outdated)} countries with outdated data:")
    for country in outdated[:3]:  # Show first 3
        print(f"  {country['name']}: {country['total_missing']} new elements")
else:
    print("All checked countries are up to date!")


# Example 3: Populate cache for new countries
# ===========================================
print("\n=== Downloading New Data ===")

# Download data for small countries
result = populate_cache(
    countries=["Liechtenstein", "Monaco"],
    cache_dir=None,  # Uses config value or default ./osm_cache
    force_refresh=False,  # Skip if already cached
    show_progress=True,  # Show download progress
)

# Or use a custom cache directory:
# result = populate_cache(
#     countries=["Kenya", "Uganda"],
#     cache_dir="~/osm_caches/africa",
#     force_refresh=False,
#     show_progress=True,
# )

print("\nResults:")
print(f"  Successfully downloaded: {result['succeeded']}")
print(f"  Already cached: {result['skipped']}")
print(f"  Failed: {result['failed']}")


# Example 4: Understanding rejections
# ===================================
# See why some OSM elements were rejected during processing

from powerplantmatching.osm import OverpassAPIClient, RejectionTracker, Units, Workflow

config = get_config()["OSM"]
config["missing_name_allowed"] = False  # Strict: require names

# Process with rejection tracking
rejection_tracker = RejectionTracker()
units = Units()

with OverpassAPIClient(cache_dir=None) as client:  # Uses config value
    workflow = Workflow(client, rejection_tracker, units, config)
    workflow.process_country_data("Malta")  # Use Malta instead of Kenya

# Analyze rejections
print("\n=== Data Quality Report for Malta ===")
print(f"Valid power plants: {len(units)}")
print(f"Rejected elements: {rejection_tracker.get_total_count()}")

if rejection_tracker.get_total_count() > 0:
    print("\nTop rejection reasons:")
    for reason, count in list(rejection_tracker.get_summary().items())[:3]:
        print(f"  {reason}: {count}")

    # Save detailed rejection report
    import os

    os.makedirs("output", exist_ok=True)
    rejection_tracker.generate_report().to_csv(
        "output/malta_rejections.csv", index=False
    )
    print("\nDetailed rejection report saved to output/malta_rejections.csv")


# Example 5: Force refresh specific countries
# ==========================================
# Update cache for countries with significant changes

# This would re-download even if cached
# result = populate_cache(
#     countries=["South Africa"],
#     force_refresh=True,  # Force new download
#     show_progress=True
# )

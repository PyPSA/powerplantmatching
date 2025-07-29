#!/usr/bin/env python3
"""
OSM Tutorial Part 1: Data Loading and Configuration
==================================================

Learn how to:
1. Load OSM power plant data
2. Configure data processing options
3. Handle data quality settings
"""

from powerplantmatching.core import get_config
from powerplantmatching.data import OSM

# Understanding the OSM() function
# ================================
# The OSM() function is a high-level interface that automatically:
# - Downloads or loads cached OpenStreetMap data
# - Processes raw OSM elements into power plants
# - Applies quality filters and validation
# - Estimates missing capacities (if enabled)
# - Reconstructs plants from generators (if enabled)
# - Returns a clean pandas DataFrame ready for analysis
#
# For more control over these steps, see tutorials 2 & 3

# Example 1: Basic data loading
# =============================
config = get_config()
config["target_countries"] = ["Luxembourg"]

# Load with default settings
df = OSM(config=config)
print(f"Loaded {len(df)} power plants from Luxembourg\n")


# Example 2: Configure data quality requirements
# ==============================================
# The OSM module can filter data based on completeness

# First, get baseline count with permissive settings
config_baseline = get_config()
config_baseline["target_countries"] = ["Luxembourg"]
config_baseline["OSM"]["missing_name_allowed"] = True  # Allow unnamed plants
df_baseline = OSM(config=config_baseline)

# Now apply strict requirements
config["OSM"]["missing_name_allowed"] = False  # Reject unnamed plants
config["OSM"]["missing_technology_allowed"] = True  # Allow missing technology
config["OSM"]["missing_start_date_allowed"] = True  # Allow missing start dates

# This will return fewer plants due to stricter requirements
df_strict = OSM(config=config)
print(f"With strict name requirement: {len(df_strict)} plants")
print(f"Filtered out: {len(df_baseline) - len(df_strict)} plants without names\n")


# Example 3: Control data processing features
# ===========================================
config["OSM"]["capacity_extraction"]["enabled"] = True  # Extract capacity from tags
config["OSM"]["capacity_estimation"]["enabled"] = True  # Estimate missing capacities
config["OSM"]["units_clustering"]["enabled"] = False  # Don't cluster nearby generators
config["OSM"]["units_reconstruction"]["enabled"] = (
    True  # Reconstruct plants from generators
)

df_processed = OSM(config=config)
print(f"With extraction, estimation and reconstruction: {len(df_processed)} plants\n")

# Note: capacity_extraction vs capacity_estimation
# - Extraction: Reads capacity from OSM tags (plant:output:electricity=10 MW)
# - Estimation: Calculates capacity when missing (e.g., from area for solar)


# Example 4: Cache behavior - force_refresh vs update
# ===================================================
# Two parameters control how OSM handles cached data:
# - force_refresh: controls the OSM module's internal behavior
# - update: controls powerplantmatching's high-level cache

# Case 1: Use all caches (fastest)
config["OSM"]["force_refresh"] = False  # Use OSM's cache
df_cached = OSM(config=config, update=False)  # Use PPM's cache

# Case 2: Update PPM cache from OSM cache
config["OSM"]["force_refresh"] = False  # Use OSM's cache
df_updated = OSM(config=config, update=True)  # Refresh PPM's cache

# Case 3: Full refresh from OpenStreetMap (slowest)
# config["OSM"]["force_refresh"] = True  # Download from OSM
# df_fresh = OSM(config=config, update=True)  # Update PPM's cache

# Summary:
# - force_refresh=False, update=False: Use all cached data
# - force_refresh=False, update=True: Refresh PPM cache from OSM cache
# - force_refresh=True, update=True: Download fresh from OpenStreetMap


# Example 5: Load multiple countries efficiently
# ==============================================
config["target_countries"] = ["Luxembourg", "Malta", "Cyprus"]
config["OSM"]["plants_only"] = True  # Only load plants, not generators

df_multi = OSM(config=config)
print(f"Loaded {len(df_multi)} plants from 3 countries")

# The module handles each country separately for memory efficiency


# Example 6: Custom cache directory
# ================================================
# The OSM module supports custom cache directories via config
# This is useful for managing large caches or sharing between projects

# Method 1: Set in config.yaml
# OSM:
#   cache_dir: ~/osm_caches/project1  # Custom location
#   fn: osm_data.csv                  # CSV filename (stored IN cache_dir)

# Method 2: Set programmatically
config["OSM"]["cache_dir"] = "~/osm_caches/europe"  # Will be expanded
df_custom_cache = OSM(config=config)

# Benefits:
# - Keep large caches (6GB for 249 countries) separate from project
# - Share cache across multiple projects
# - Use faster/larger storage for cache
# - Separate test/dev/prod caches
# - All OSM data in one place (CSV + API caches)

# The cache_dir path can be:
# - Absolute: /data/osm_cache
# - Relative: ./cache/osm (relative to data directory)
# - With ~: ~/osm_caches/global (expands to home directory)

# The CSV cache file (osm_data.csv) is stored INSIDE cache_dir
# Structure:
#   cache_dir/
#   ├── osm_data.csv     # CSV cache (all countries)
#   ├── plants/          # API cache
#   ├── generators/      # API cache
#   └── units/           # API cache

print(f"✓ Example 6 complete! Loaded {len(df_custom_cache)} plants")


# Example 7: Understanding source and technology mapping
# ======================================================
# OSM data uses various tags that are mapped to standard categories
# This ensures consistency across different tagging conventions

# The mapping is defined in config.yaml under OSM section:
# - source_mapping: Maps OSM generator:source tags to standard fuel types
# - technology_mapping: Maps OSM generator:method tags to standard technologies

# Standard fuel types (see powerplantmatching.CONSTANT_FUELTYPE):
# ['Bioenergy', 'Geothermal', 'Hard Coal', 'Hydro', 'Lignite',
#  'Natural Gas', 'Nuclear', 'Oil', 'Other', 'Solar', 'Wind']

# Standard technologies (see powerplantmatching documentation):
# ['CCGT', 'OCGT', 'Steam Turbine', 'Combustion Engine',
#  'Run-Of-River', 'Reservoir', 'Pumped Storage',
#  'Onshore', 'Offshore', 'PV', 'CSP']

# Example mappings from config.yaml:
# source_mapping:
#   Solar: [solar, photovoltaic, solar_thermal, pv]
#   Wind: [wind, wind_power, wind_turbine]
#   Natural Gas: [gas, natural_gas, lng]

# This means:
# - generator:source=solar → Fueltype="Solar"
# - generator:source=gas → Fueltype="Natural Gas"
# - generator:method=photovoltaic → Technology="PV"

# You can extend mappings for regional variations:
config["OSM"]["source_mapping"]["Solar"].append("sonnenkraft")  # German
config["OSM"]["technology_mapping"]["PV"].append("fotovoltaico")  # Spanish

# Reload with extended mappings
df_extended = OSM(config=config, update=True)

print("\n✓ Mapping example complete!")

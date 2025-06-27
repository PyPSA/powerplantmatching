#!/usr/bin/env python3
"""
OSM Tutorial Part 3: Regional Downloads and Special Features
===========================================================

Learn how to:
1. Download data for specific regions (not countries)
2. Use reconstruction features

"""

from powerplantmatching.core import get_config
from powerplantmatching.data import OSM
from powerplantmatching.osm import region_download

# Example 1: Download data for a city region
# ==========================================
print("=== Regional Download Example ===")

# Define a circular region around a city
city_region = {
    "type": "radius",
    "name": "Montevideo Area",
    "center": [-34.9011, -56.1645],  # Montevideo, Uruguay
    "radius_km": 50,
}

# Download power plants in this region
result = region_download(regions=city_region)

if result["success"]:
    data = result["results"]["Montevideo Area"]
    print(f"Found {data['plants_count']} plants around Montevideo")
    print(f"Found {data['generators_count']} generators")


# Example 2: Download using bounding box
# =====================================
# Useful for rectangular study areas

bbox_region = {
    "type": "bbox",
    "name": "Northern Uruguay",
    "bounds": [-32.5, -58.0, -30.0, -53.0],  # [min_lat, min_lon, max_lat, max_lon]
}

result = region_download(
    regions=bbox_region,
    download_type="plants",  # Only plants, not generators
    update_country_caches=False,  # Don't update country cache
)

if result["success"]:
    data = result["results"]["Northern Uruguay"]
    print(f"\nFound {data['plants_count']} plants in Northern Uruguay bbox")


# Example 3: Custom polygon regions
# =================================
# Define arbitrary shaped regions

# Polygon covering area around Cairo, Egypt
river_triangle = {
    "type": "polygon",
    "name": "Cairo Power Area",
    "coordinates": [
        [30.0, 31.2],
        [30.0, 31.4],
        [30.2, 31.4],
        [30.2, 31.2],
        [30.0, 31.2],  # Close the polygon
    ],
}

# This downloads just that triangular area
result = region_download(regions=river_triangle)
if result["success"]:
    data = result["results"]["Cairo Power Area"]
    print("\n=== Polygon Region Result ===")
    print(f"Found {data['plants_count']} plants in custom polygon area")


# Example 4: Multiple regions at once
# ===================================
regions = [
    {
        "type": "radius",
        "name": "Buenos Aires",
        "center": [-34.6037, -58.3816],
        "radius_km": 5,
    },
    {
        "type": "radius",
        "name": "São Paulo",
        "center": [-23.5505, -46.6333],
        "radius_km": 20,
    },
]

results = region_download(regions=regions)
print("\n=== Multiple Region Results ===")
for region_name, data in results["results"].items():
    if data["status"] == "success":
        print(f"{region_name}: {data['plants_count']} plants")


# Example 5: Reconstruction feature demo
# =====================================
# Reconstruct incomplete power plants from their generators

config = get_config()
config["target_countries"] = ["Greece"]

# Without reconstruction
config["OSM"]["units_reconstruction"]["enabled"] = False
df_basic = OSM(config=config)

# With reconstruction enabled
config["OSM"]["units_reconstruction"]["enabled"] = True
config["OSM"]["units_reconstruction"]["min_generators_for_reconstruction"] = 2
df_reconstructed = OSM(config=config)

print("\n=== Reconstruction Impact ===")
print(f"Without reconstruction: {len(df_basic)} plants")
print(f"With reconstruction: {len(df_reconstructed)} plants")
print(f"Additional plants found: {len(df_reconstructed) - len(df_basic)}")

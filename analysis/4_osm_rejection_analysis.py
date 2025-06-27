#!/usr/bin/env python3
"""
OSM Tutorial Part 4: Rejection Analysis for OSM Mappers
======================================================

Learn how to:
1. Use rejection tracking to identify OSM data issues
2. Iteratively refine configurations to find problems
3. Generate reports for fixing OSM elements
4. Contribute back to OpenStreetMap quality

This tutorial uses Chile as an example - a country with moderate
OSM coverage that demonstrates common data quality issues.
"""

import os

from powerplantmatching.core import get_config
from powerplantmatching.osm import (
    OverpassAPIClient,
    RejectionTracker,
    Units,
    Workflow,
)

# Create output directory for all files
os.makedirs("output", exist_ok=True)

# The Iterative Refinement Process
# ================================
# We'll run 4 iterations with progressively relaxed configurations:
# 1. Very strict: Find plants missing critical data
# 2. Allow missing dates: Focus on name/technology issues
# 3. Enable estimation: Identify capacity tagging problems
# 4. Allow missing names: Find unnamed infrastructure

print("OSM Rejection Analysis - Chile")
print("==================================================")
print("This analysis helps OSM contributors identify and fix")
print("power plant data quality issues in OpenStreetMap.\n")


# Iteration 1: Very Strict Configuration
# ======================================
print("=== ITERATION 1: Strict Configuration ===")
print("Purpose: Identify plants with missing critical data\n")

config = get_config()["OSM"]
config.update(
    {
        "force_refresh": False,
        "missing_name_allowed": False,
        "missing_start_date_allowed": False,
        "missing_technology_allowed": False,
        "capacity_extraction": {"enabled": False},
        "capacity_estimation": {"enabled": False},
    }
)

rejection_tracker_1 = RejectionTracker()
units_1 = Units()

with OverpassAPIClient() as client:
    workflow = Workflow(client, rejection_tracker_1, units_1, config)
    workflow.process_country_data("Chile")

print(f"Accepted plants: {len(units_1)}")
print(f"Rejected elements: {rejection_tracker_1.get_total_count()}")

print("\nTop rejection reasons:")
for reason, count in list(rejection_tracker_1.get_summary().items())[:5]:
    print(f"  {reason}: {count}")

# Save iteration 1 results
rejection_tracker_1.save_geojson("output/iteration1_rejections.geojson")
print("\nSaved rejection map to output/iteration1_rejections.geojson")


# Iteration 2: Allow Missing Dates
# ================================
print("\n\n=== ITERATION 2: Relaxed Date Requirement ===")
print("Purpose: Focus on name and technology issues\n")

config["missing_start_date_allowed"] = True  # Allow missing dates

rejection_tracker_2 = RejectionTracker()
units_2 = Units()

with OverpassAPIClient() as client:
    workflow = Workflow(client, rejection_tracker_2, units_2, config)
    workflow.process_country_data("Chile")

print(f"Accepted plants: {len(units_2)} (+{len(units_2) - len(units_1)})")
print(f"Rejected elements: {rejection_tracker_2.get_total_count()}")

print("\nRejection reasons shift:")
prev_rejections = rejection_tracker_1.get_summary()
new_rejections = rejection_tracker_2.get_summary()

for reason, count in list(new_rejections.items())[:5]:
    prev_count = prev_rejections.get(reason, 0)
    change = count - prev_count
    sign = "+" if change >= 0 else ""
    print(f"  {reason}: {count} ({sign}{change})")


# Iteration 3: Enable Capacity Estimation
# =======================================
print("\n\n=== ITERATION 3: With Capacity Estimation ===")
print("Purpose: Identify capacity tagging problems\n")

config["capacity_estimation"]["enabled"] = True  # Enable estimation

rejection_tracker_3 = RejectionTracker()
units_3 = Units()

with OverpassAPIClient() as client:
    workflow = Workflow(client, rejection_tracker_3, units_3, config)
    workflow.process_country_data("Chile")

print(f"Accepted plants: {len(units_3)} (+{len(units_3) - len(units_2)})")
print(f"Rejected elements: {rejection_tracker_3.get_total_count()}")

# Show which plants now have estimated capacity
print("\nCapacity estimation impact:")
if len(units_3) > len(units_2):
    print(f"  {len(units_3) - len(units_2)} plants now have estimated capacity")


# Iteration 4: Allow Missing Names
# ================================
print("\n\n=== ITERATION 4: Allow Missing Names ===")
print("Purpose: Find unnamed power infrastructure\n")

config["missing_name_allowed"] = True  # Allow missing names

rejection_tracker_4 = RejectionTracker()
units_4 = Units()

with OverpassAPIClient() as client:
    workflow = Workflow(client, rejection_tracker_4, units_4, config)
    workflow.process_country_data("Chile")

print(f"Accepted plants: {len(units_4)} (+{len(units_4) - len(units_3)})")
print(f"Rejected elements: {rejection_tracker_4.get_total_count()}")

if len(units_4) > len(units_3):
    print(f"\n{len(units_4) - len(units_3)} unnamed plants found!")
    print("These need names added in OpenStreetMap")


# Export Comprehensive Analysis
# =============================
print("\n\n=== EXPORTING ANALYSIS RESULTS ===")

# Why use rejection_tracker_1 instead of rejection_tracker_4?
# -----------------------------------------------------------
# We export data from the FIRST (strictest) iteration because:
#
# 1. It contains ALL issues - The strict configuration identifies every problem:
#    missing names, dates, capacity, technology, invalid formats, etc.
#
# 2. Later iterations hide problems by allowing them:
#    - Iteration 2: Allows missing dates → those rejections disappear
#    - Iteration 3: Enables estimation → capacity rejections disappear
#    - Iteration 4: Allows missing names → name rejections disappear
#
# 3. OSM contributors need to see everything - Even if powerplantmatching can
#    work around missing data (via estimation or relaxed rules), the goal is
#    to improve the actual OSM data quality.
#
# Example: rejection_tracker_1 shows 1077 missing dates, 921 missing names,
# while rejection_tracker_4 would only show ~170 "unfixable" issues.
# We want contributors to add all the missing data, not just the minimum!

# 1. GeoJSON files for mapping tools
os.makedirs("output/rejection_maps", exist_ok=True)
rejection_tracker_1.save_geojson_by_reasons("output/rejection_maps/")
print("✓ Saved GeoJSON files by rejection type to output/rejection_maps/")

# 2. CSV for spreadsheet analysis
report_df = rejection_tracker_1.generate_report()
report_df.to_csv("output/chile_osm_issues.csv", index=False)
print("✓ Saved detailed CSV report to output/chile_osm_issues.csv")

# 3. Summary statistics
summary_path = "output/chile_fix_summary.txt"
with open(summary_path, "w") as f:
    f.write("OSM Power Plant Data Quality Report - Chile\n")
    f.write("==================================================\n\n")

    # Use the first iteration which had all rejections
    total_rejected = rejection_tracker_1.get_total_count()
    total_accepted = len(units_1)
    total_elements = total_rejected + total_accepted

    f.write(f"Total OSM elements analyzed: {total_elements}\n")
    f.write(f"Accepted as valid (strict criteria): {total_accepted}\n")
    f.write(f"Rejected for issues: {total_rejected}\n")

    if total_elements > 0:
        acceptance_rate = total_accepted / total_elements * 100
        f.write(f"Acceptance rate: {acceptance_rate:.1f}%\n\n")

    f.write("Issues by frequency:\n")
    for reason, count in rejection_tracker_1.get_summary().items():
        f.write(f"  {reason}: {count}\n")

    f.write("\n\nProgression through iterations:\n")
    f.write(f"Iteration 1 (strict): {len(units_1)} accepted\n")
    f.write(
        f"Iteration 2 (+dates): {len(units_2)} accepted (+{len(units_2) - len(units_1)})\n"
    )
    f.write(
        f"Iteration 3 (+estimation): {len(units_3)} accepted (+{len(units_3) - len(units_2)})\n"
    )
    f.write(
        f"Iteration 4 (+names): {len(units_4)} accepted (+{len(units_4) - len(units_3)})\n"
    )

    f.write("\n\nRecommended fixes (in priority order):\n")
    f.write("1. Add missing capacity tags (plant:output:electricity=X MW)\n")
    f.write("2. Add missing names to power plants\n")
    f.write("3. Add missing technology tags (plant:method or generator:method)\n")
    f.write("4. Add start dates (start_date=YYYY)\n")

print("✓ Saved summary to output/chile_fix_summary.txt")


# Final Instructions
# ==================
print("\n==================================================")
print("NEXT STEPS FOR OSM CONTRIBUTORS:")
print("==================================================")
print("1. Open output/rejection_maps/ in JOSM or iD editor")
print("2. Use output/chile_osm_issues.csv to prioritize fixes")
print("3. Focus on high-frequency issues first")
print("4. Common fixes:")
print("   - Add capacity: plant:output:electricity=50 MW")
print("   - Add names: name=Central Hidroeléctrica Rapel")
print("   - Add technology: plant:method=water-storage")
print("   - Add dates: start_date=1968")
print("5. Re-run this analysis after fixes to verify improvements")
print("==================================================")

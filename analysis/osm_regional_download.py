#!/usr/bin/env python3

from datetime import datetime

from powerplantmatching.osm.regional import region_download


def example_1_single_region():
    print("=== EXAMPLE 1: Single Region (Lisbon Area) ===\n")

    lisbon_region = {
        "type": "radius",
        "name": "Lisbon Metropolitan Area",
        "center": [38.7223, -9.1393],
        "radius_km": 30,
    }

    print(
        f"Downloading power plants within {lisbon_region['radius_km']}km of Lisbon..."
    )

    results = region_download(regions=lisbon_region)

    if results["success"]:
        region_data = results["results"]["Lisbon Metropolitan Area"]
        print("\n✓ Success!")
        print(f"  - Found {region_data['plants_count']} power plants")
        print(f"  - Found {region_data['generators_count']} generators")
        print(f"  - Added {region_data['elements_added']} new elements to cache")
        print(f"  - Updated {region_data['elements_updated']} existing elements")
    else:
        print("✗ Download failed!")

    return results


def example_2_bounding_box():
    print("\n\n=== EXAMPLE 2: Bounding Box (Northern Portugal) ===\n")

    north_portugal = {
        "type": "bbox",
        "name": "Northern Portugal",
        "bounds": [41.0, -8.8, 41.5, -8.0],
    }

    print("Downloading power plants in northern Portugal (Porto region)...")
    print(f"Bounding box: {north_portugal['bounds']}")

    results = region_download(regions=north_portugal)

    if results["success"]:
        region_data = results["results"]["Northern Portugal"]
        print("\n✓ Success!")
        print(f"  - Found {region_data['plants_count']} power plants")
        print(f"  - Found {region_data['generators_count']} generators")
    else:
        print("✗ Download failed!")

    return results


def example_3_multiple_regions():
    print("\n\n=== EXAMPLE 3: Multiple Regions ===\n")

    regions = [
        {
            "type": "radius",
            "name": "Coimbra Area",
            "center": [40.2033, -8.4103],
            "radius_km": 25,
        },
        {
            "type": "radius",
            "name": "Faro Area",
            "center": [37.0194, -7.9322],
            "radius_km": 40,
        },
    ]

    print("Downloading multiple regions:")
    for r in regions:
        print(f"  - {r['name']}: {r['radius_km']}km radius")

    results = region_download(regions=regions)

    if results["success"]:
        print(f"\n✓ Successfully processed {results['regions_processed']} regions")
        for region_name, data in results["results"].items():
            if data["status"] == "success":
                print(f"\n{region_name}:")
                print(f"  - Plants: {data['plants_count']}")
                print(f"  - Generators: {data['generators_count']}")
    else:
        print("✗ Download failed!")

    return results


def example_4_custom_polygon():
    print("\n\n=== EXAMPLE 4: Custom Polygon ===\n")

    central_triangle = {
        "type": "polygon",
        "name": "Central Portugal Triangle",
        "coordinates": [
            [-8.5, 40.0],
            [-7.5, 40.0],
            [-8.0, 39.0],
            [-8.5, 40.0],
        ],
    }

    print("Downloading triangular region in central Portugal...")

    results = region_download(regions=central_triangle)

    if results["success"]:
        region_data = results["results"]["Central Portugal Triangle"]
        print("\n✓ Success!")
        print(f"  - Found {region_data['plants_count']} power plants")
        print(f"  - Found {region_data['generators_count']} generators")
    else:
        print("✗ Download failed!")

    return results


def example_5_download_options():
    print("\n\n=== EXAMPLE 5: Download Options ===\n")

    region = {
        "type": "radius",
        "name": "Setúbal Industrial Area",
        "center": [38.5244, -8.8882],
        "radius_km": 20,
    }

    print("Example 5a: Download only power plants (no generators)")
    results_plants = region_download(
        regions=region,
        download_type="plants",
        update_country_caches=True,
    )

    if results_plants["success"]:
        data = results_plants["results"]["Setúbal Industrial Area"]
        print(f"✓ Found {data['plants_count']} plants (generators not downloaded)")

    print("\n\nExample 5b: Download without updating country caches")
    results_no_cache = region_download(
        regions=region,
        download_type="both",
        update_country_caches=False,
    )

    if results_no_cache["success"]:
        print("✓ Downloaded but did not update main cache")

    return results_plants, results_no_cache


def main():
    print("=== OSM REGIONAL DOWNLOAD EXAMPLES ===")
    print("Using Portugal as test country")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    example_1_single_region()

    example_2_bounding_box()

    example_3_multiple_regions()

    example_4_custom_polygon()

    example_5_download_options()

    print("\n" + "=" * 50)
    print("All examples completed!")
    print("\nNote: Data is cached, so running this again will be faster.")
    print("Use update=True or force_refresh=True to re-download from OSM.")


if __name__ == "__main__":
    main()

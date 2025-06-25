#!/usr/bin/env python3

import logging
import os
import time
from collections import defaultdict
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from powerplantmatching.core import _data_in, get_config
from powerplantmatching.osm.client import OverpassAPIClient
from powerplantmatching.osm.interface import validate_countries
from powerplantmatching.osm.models import Units
from powerplantmatching.osm.rejection import RejectionTracker
from powerplantmatching.osm.workflow import Workflow

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

plt.style.use("seaborn-v0_8-darkgrid")
sns.set_palette("husl")


def run_scenario(country, scenario_name, config_overrides, cache_dir):
    print(f"\n{'=' * 60}")
    print(f"Running Scenario: {scenario_name}")
    print(f"Country: {country}")
    print(f"{'=' * 60}")

    config = get_config()["OSM"].copy()

    config.update(
        {
            "force_refresh": True,
            "plants_only": True,
            "missing_name_allowed": False,
            "missing_technology_allowed": False,
            "missing_start_date_allowed": True,
        }
    )

    config.update(config_overrides)

    print("\nConfiguration:")
    print(f"- Plants only: {config['plants_only']}")
    print(f"- Reconstruction enabled: {config['units_reconstruction']['enabled']}")
    print(f"- Capacity estimation enabled: {config['capacity_estimation']['enabled']}")
    print(f"- Missing start date allowed: {config['missing_start_date_allowed']}")

    rejection_tracker = RejectionTracker()
    units_collection = Units()

    start_time = time.time()

    with OverpassAPIClient(
        api_url=config["overpass_api"]["url"], cache_dir=cache_dir
    ) as client:
        workflow = Workflow(
            client=client,
            rejection_tracker=rejection_tracker,
            units=units_collection,
            config=config,
        )
        validate_countries([country])
        workflow.process_country_data(country=country)

    processing_time = time.time() - start_time

    stats = units_collection.get_statistics()
    print("\nResults:")
    print(f"- Total plants: {stats['total_units']}")
    print(f"- Total capacity: {stats['total_capacity_mw']:.0f} MW")
    print(f"- Plants with coordinates: {stats['units_with_coordinates']}")
    print(f"- Total rejections: {rejection_tracker.get_total_count()}")
    print(f"- Processing time: {processing_time:.1f} seconds")

    reconstructed_count = 0
    salvaged_count = 0
    for unit in units_collection.units:
        if hasattr(unit, "capacity_source"):
            if unit.capacity_source == "reconstructed_from_generators":
                reconstructed_count += 1
            elif unit.capacity_source == "aggregated_from_orphaned_generators":
                salvaged_count += 1

    if reconstructed_count > 0 or salvaged_count > 0:
        print("\nReconstruction details:")
        print(f"- Plants reconstructed from generators: {reconstructed_count}")
        print(f"- Plants salvaged from orphaned generators: {salvaged_count}")

    return units_collection, rejection_tracker, processing_time


def analyze_reconstruction_impact(results_by_scenario, country, output_dir):
    print(f"\n{'=' * 60}")
    print(f"Analyzing Reconstruction Impact for {country}")
    print(f"{'=' * 60}")

    comparison_data = []

    for scenario_name, (units, rejections, proc_time) in results_by_scenario.items():
        stats = units.get_statistics()

        reconstructed = 0
        salvaged = 0
        estimated = 0
        zero_capacity = 0

        for unit in units.units:
            if unit.Capacity == 0 or unit.Capacity is None:
                zero_capacity += 1
            if hasattr(unit, "capacity_source"):
                if unit.capacity_source == "reconstructed_from_generators":
                    reconstructed += 1
                elif unit.capacity_source == "aggregated_from_orphaned_generators":
                    salvaged += 1
                elif "estimated" in unit.capacity_source:
                    estimated += 1

        comparison_data.append(
            {
                "Scenario": scenario_name,
                "Total Plants": stats["total_units"],
                "Plants with Capacity": stats["total_units"] - zero_capacity,
                "Zero/Missing Capacity": zero_capacity,
                "Total Capacity (MW)": stats["total_capacity_mw"],
                "Average Capacity (MW)": stats["average_capacity_mw"],
                "Reconstructed": reconstructed,
                "Salvaged": salvaged,
                "Estimated": estimated,
                "Total Rejections": rejections.get_total_count(),
                "Processing Time (s)": proc_time,
            }
        )

    df_comparison = pd.DataFrame(comparison_data)
    print("\nComparison Summary:")
    print(df_comparison.to_string(index=False))

    comparison_file = os.path.join(
        output_dir, f"{country.lower()}_reconstruction_comparison.csv"
    )
    df_comparison.to_csv(comparison_file, index=False)
    print(f"\nSaved comparison to: {comparison_file}")

    create_reconstruction_visualizations(
        results_by_scenario, df_comparison, country, output_dir
    )

    print(f"\n{'=' * 60}")
    print("Rejection Analysis")
    print(f"{'=' * 60}")

    for scenario_name, (units, rejections, _) in results_by_scenario.items():
        print(f"\n{scenario_name}:")
        summary = rejections.get_summary()

        if summary:
            print("  Top rejection reasons:")
            for reason, count in sorted(
                summary.items(), key=lambda x: x[1], reverse=True
            )[:5]:
                print(f"    - {reason}: {count}")
        else:
            print("  No rejections!")

    if country in results_by_scenario:
        scenario_with_reconstruction = None
        for scenario_name in [
            "Reconstruction + Estimation",
            "Reconstruction",
        ]:
            if scenario_name in results_by_scenario:
                scenario_with_reconstruction = scenario_name
                break

        if scenario_with_reconstruction:
            units_with_recon = results_by_scenario[scenario_with_reconstruction][0]
            reconstructed_plants = [
                u
                for u in units_with_recon.units
                if hasattr(u, "capacity_source")
                and u.capacity_source == "reconstructed_from_generators"
            ]

            if reconstructed_plants:
                print(f"\n{'=' * 60}")
                print("Example Reconstructed Plants (Top 5 by capacity)")
                print(f"{'=' * 60}")

                reconstructed_plants.sort(key=lambda x: x.Capacity or 0, reverse=True)

                for i, plant in enumerate(reconstructed_plants[:5], 1):
                    print(f"\n{i}. {plant.Name}")
                    print(f"   ID: {plant.projectID}")
                    print(f"   Capacity: {plant.Capacity:.1f} MW")
                    print(f"   Fuel type: {plant.Fueltype}")
                    print(f"   Technology: {plant.Technology}")
                    print(
                        f"   Generator count: {getattr(plant, 'generator_count', 'N/A')}"
                    )
                    print(f"   Location: ({plant.lat:.4f}, {plant.lon:.4f})")


def create_reconstruction_visualizations(
    results_by_scenario, df_comparison, country, output_dir
):
    fig = plt.figure(figsize=(16, 12))
    gs = fig.add_gridspec(3, 3, hspace=0.5, wspace=0.3)

    fig.suptitle(
        f"OSM Plant Reconstruction Analysis - {country}", fontsize=14, fontweight="bold"
    )

    ax1 = fig.add_subplot(gs[0, 0])
    scenarios = df_comparison["Scenario"].values
    plant_counts = df_comparison["Total Plants"].values
    reconstructed = df_comparison["Reconstructed"].values

    x = range(len(scenarios))
    width = 0.35

    bars1 = ax1.bar(
        [i - width / 2 for i in x], plant_counts, width, label="Total Plants", alpha=0.8
    )
    bars2 = ax1.bar(
        [i + width / 2 for i in x],
        reconstructed,
        width,
        label="Reconstructed",
        alpha=0.8,
    )

    ax1.set_xlabel("Scenario")
    ax1.set_ylabel("Count")
    ax1.set_title("Plant Counts by Scenario", fontsize=11)
    ax1.set_xticks(x)
    ax1.set_xticklabels([s.replace(" ", "\n") for s in scenarios], fontsize=9)
    ax1.legend()
    ax1.grid(axis="y", alpha=0.3)

    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax1.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"{int(height)}",
                ha="center",
                va="bottom",
                fontsize=8,
            )

    ax2 = fig.add_subplot(gs[0, 1])
    capacities = df_comparison["Total Capacity (MW)"].values
    bars = ax2.bar(x, capacities, color=["#1f77b4", "#ff7f0e", "#2ca02c"], alpha=0.8)

    ax2.set_xlabel("Scenario")
    ax2.set_ylabel("Total Capacity (MW)")
    ax2.set_title("Total Capacity by Scenario", fontsize=11)
    ax2.set_xticks(x)
    ax2.set_xticklabels([s.replace(" ", "\n") for s in scenarios], fontsize=9)
    ax2.grid(axis="y", alpha=0.3)

    base_capacity = capacities[0]
    for i, bar in enumerate(bars):
        height = bar.get_height()
        ax2.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
            fontsize=8,
        )
        if i > 0:
            change = ((height - base_capacity) / base_capacity) * 100
            ax2.text(
                bar.get_x() + bar.get_width() / 2.0,
                height / 2,
                f"{change:+.1f}%",
                ha="center",
                va="center",
                fontsize=8,
                fontweight="bold",
                color="white",
            )

    ax3 = fig.add_subplot(gs[0, 2])
    rejections = df_comparison["Total Rejections"].values
    bars = ax3.bar(x, rejections, color=["#d62728", "#ff9896", "#ffbb78"], alpha=0.8)

    ax3.set_xlabel("Scenario")
    ax3.set_ylabel("Total Rejections")
    ax3.set_title("Rejections by Scenario", fontsize=11)
    ax3.set_xticks(x)
    ax3.set_xticklabels([s.replace(" ", "\n") for s in scenarios], fontsize=9)
    ax3.grid(axis="y", alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        ax3.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
            fontsize=8,
        )

    for idx, (scenario_name, (units, _, _)) in enumerate(results_by_scenario.items()):
        ax = fig.add_subplot(gs[1, idx])

        capacities = [
            u.Capacity for u in units.units if u.Capacity is not None and u.Capacity > 0
        ]

        if capacities:
            ax.hist(capacities, bins=30, alpha=0.7, edgecolor="black", linewidth=0.5)
            ax.set_xlabel("Capacity (MW)")
            ax.set_ylabel("Count")
            ax.set_title(f"{scenario_name}", fontsize=11)
            ax.set_xlim(0, min(1000, max(capacities)))

            stats_text = (
                f"Count: {len(capacities)}\n"
                f"Total: {sum(capacities):.0f} MW\n"
                f"Mean: {sum(capacities) / len(capacities):.1f} MW\n"
                f"Median: {pd.Series(capacities).median():.1f} MW"
            )
            ax.text(
                0.95,
                0.95,
                stats_text,
                transform=ax.transAxes,
                bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
                verticalalignment="top",
                horizontalalignment="right",
                fontsize=9,
            )
        else:
            ax.text(
                0.5,
                0.5,
                "No plants with\nvalid capacity",
                transform=ax.transAxes,
                ha="center",
                va="center",
                fontsize=9,
            )

        ax.grid(axis="y", alpha=0.3)

    ax5 = fig.add_subplot(gs[2, :2])

    best_scenario = max(
        results_by_scenario.items(),
        key=lambda x: x[1][0].get_statistics()["total_units"],
    )
    scenario_name, (units, _, _) = best_scenario

    fuel_counts = defaultdict(int)
    fuel_capacities = defaultdict(float)

    for unit in units.units:
        if unit.Fueltype:
            fuel_counts[unit.Fueltype] += 1
            if unit.Capacity:
                fuel_capacities[unit.Fueltype] += unit.Capacity

    if fuel_counts:
        sorted_fuels = sorted(fuel_capacities.items(), key=lambda x: x[1], reverse=True)
        fuels = [f[0] for f in sorted_fuels]
        counts = [fuel_counts[f] for f in fuels]
        capacities = [fuel_capacities[f] for f in fuels]

        ax5_twin = ax5.twinx()

        x = range(len(fuels))
        width = 0.35

        bars1 = ax5.bar(
            [i - width / 2 for i in x],
            counts,
            width,
            label="Plant Count",
            alpha=0.8,
            color="steelblue",
        )
        bars2 = ax5_twin.bar(
            [i + width / 2 for i in x],
            capacities,
            width,
            label="Total Capacity",
            alpha=0.8,
            color="darkorange",
        )

        ax5.set_xlabel("Fuel Type")
        ax5.set_ylabel("Plant Count", color="steelblue")
        ax5_twin.set_ylabel("Total Capacity (MW)", color="darkorange")
        ax5.set_title(f"Fuel Type Distribution - {scenario_name}", fontsize=11)
        ax5.set_xticks(x)
        ax5.set_xticklabels(fuels, rotation=45, ha="right")
        ax5.tick_params(axis="y", labelcolor="steelblue")
        ax5_twin.tick_params(axis="y", labelcolor="darkorange")

        ax5.legend(loc="upper left")
        ax5_twin.legend(loc="upper right")

        ax5.grid(axis="y", alpha=0.3)

    ax6 = fig.add_subplot(gs[2, 2])
    scenario_names = df_comparison["Scenario"].values
    proc_times = df_comparison["Processing Time (s)"].values
    x_proc = range(len(scenario_names))
    bars = ax6.bar(
        x_proc, proc_times, color=["#9467bd", "#c5b0d5", "#e377c2"], alpha=0.8
    )

    ax6.set_xlabel("Scenario")
    ax6.set_ylabel("Processing Time (seconds)")
    ax6.set_title("Processing Time Comparison", fontsize=11)
    ax6.set_xticks(x_proc)
    ax6.set_xticklabels([s.replace(" ", "\n") for s in scenario_names], fontsize=9)
    ax6.grid(axis="y", alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        ax6.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{height:.1f}s",
            ha="center",
            va="bottom",
            fontsize=8,
        )

    plt.tight_layout()
    output_file = os.path.join(
        output_dir, f"{country.lower()}_reconstruction_analysis.png"
    )
    plt.savefig(output_file, dpi=150, bbox_inches="tight")
    print(f"\nSaved visualization to: {output_file}")
    plt.close()


def main():
    print("=" * 80)
    print("OSM PLANT RECONSTRUCTION FEATURE DEMONSTRATION")
    print("=" * 80)
    print("\nThis example demonstrates the plant reconstruction feature using:")
    print("- Plant-only mode (plants_only=True)")
    print("- Strict validation (except start_date)")
    print("- Three scenarios: disabled, enabled, enabled+estimation")
    print("\nTest countries: Mexico and Chile")
    print("=" * 80)

    fn = _data_in("osm_data.csv")
    cache_dir = os.path.join(os.path.dirname(fn), "osm_cache")
    os.makedirs(cache_dir, exist_ok=True)

    ppm_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(
        ppm_root,
        "outputs",
        "reconstruction_demo",
        datetime.now().strftime("%Y%m%d_%H%M%S"),
    )
    os.makedirs(output_dir, exist_ok=True)
    print(f"\nOutput directory: {output_dir}")

    scenarios = {
        "Reference": {
            "units_reconstruction": {"enabled": False},
            "capacity_estimation": {"enabled": False},
        },
        "Reconstruction": {
            "units_reconstruction": {
                "enabled": True,
                "min_generators_for_reconstruction": 2,
                "name_similarity_threshold": 0.7,
            },
            "capacity_estimation": {"enabled": False},
        },
        "Reconstruction + Estimation": {
            "units_reconstruction": {
                "enabled": True,
                "min_generators_for_reconstruction": 2,
                "name_similarity_threshold": 0.7,
            },
            "capacity_estimation": {"enabled": True},
        },
    }

    countries = ["Mexico", "Chile"]
    all_results = {}

    for country in countries:
        print(f"\n{'#' * 80}")
        print(f"PROCESSING COUNTRY: {country}")
        print(f"{'#' * 80}")

        country_results = {}

        for scenario_name, config_overrides in scenarios.items():
            units, rejections, proc_time = run_scenario(
                country, scenario_name, config_overrides, cache_dir
            )
            country_results[scenario_name] = (units, rejections, proc_time)

            output_file = os.path.join(
                output_dir,
                f"{country.lower()}_{scenario_name.lower().replace(' ', '_')}.csv",
            )
            units.save_csv(output_file)
            print(f"\nSaved results to: {output_file}")

            if rejections.get_total_count() > 0:
                rejection_file = os.path.join(
                    output_dir,
                    f"{country.lower()}_{scenario_name.lower().replace(' ', '_')}_rejections.csv",
                )
                rejections.generate_report().to_csv(rejection_file, index=False)
                print(f"Saved rejections to: {rejection_file}")

            time.sleep(1)

        analyze_reconstruction_impact(country_results, country, output_dir)
        all_results[country] = country_results

        if country != countries[-1]:
            print("\nWaiting before processing next country...")
            time.sleep(2)

    print("\n" + "=" * 80)
    print("RECONSTRUCTION DEMONSTRATION COMPLETED")
    print("=" * 80)

    print("\nKey Findings:")
    for country in countries:
        print(f"\n{country}:")

        without = all_results[country]["Reference"][0].get_statistics()
        with_recon = all_results[country]["Reconstruction"][0].get_statistics()
        with_est = all_results[country]["Reconstruction + Estimation"][
            0
        ].get_statistics()

        recon_improvement = (
            (
                (with_recon["total_units"] - without["total_units"])
                / without["total_units"]
                * 100
            )
            if without["total_units"] > 0
            else 0
        )

        est_improvement = (
            (
                (with_est["total_units"] - with_recon["total_units"])
                / with_recon["total_units"]
                * 100
            )
            if with_recon["total_units"] > 0
            else 0
        )

        capacity_improvement = (
            (
                (with_est["total_capacity_mw"] - without["total_capacity_mw"])
                / without["total_capacity_mw"]
                * 100
            )
            if without["total_capacity_mw"] > 0
            else 0
        )

        print(f"  - Base plants: {without['total_units']}")
        print(
            f"  - After reconstruction: {with_recon['total_units']} ({recon_improvement:+.1f}%)"
        )
        print(
            f"  - After estimation: {with_est['total_units']} ({est_improvement:+.1f}% additional)"
        )
        print(f"  - Capacity improvement: {capacity_improvement:+.1f}%")

    print(f"\nAll results saved to: {output_dir}")
    print("\nThe reconstruction feature successfully:")
    print("- Completes plant data using information from member generators")
    print("- Aggregates orphaned generators into logical plant units")
    print("- Preserves data quality through configurable thresholds")
    print("- Works particularly well for wind farms and solar parks")


if __name__ == "__main__":
    main()

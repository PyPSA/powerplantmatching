#!/usr/bin/env python3
"""
PPM OSM Analysis Script with Text Capture
===============================================

This script combines all the functionality from the four separate files
and captures all output text instead of printing to console.
All analysis results are stored in text variables that can be saved to files.
"""

import logging
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import powerplantmatching as pm

try:
    import plotly.graph_objects as go
except ImportError:
    raise ImportError("Plotly is not installed. Install it with `pip install plotly`.")

# Configure logging
logging.basicConfig(level=logging.INFO)

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.absolute()
OUTPUT_DIR = SCRIPT_DIR / "osm_eu"


def ensure_output_dir():
    """Create the output directory if it doesn't exist."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    return OUTPUT_DIR


class TextCapture:
    """Class to capture and manage analysis text output."""

    def __init__(self):
        self.analysis_text = ""
        self.step_texts = {}
        self.current_step = None

    def add_text(self, text, step=None):
        """Add text to the analysis output."""
        if step:
            if step not in self.step_texts:
                self.step_texts[step] = ""
            self.step_texts[step] += text + "\n"
        else:
            self.analysis_text += text + "\n"

    def add_header(self, text, level=1, step=None):
        """Add a header with appropriate formatting."""
        if level == 1:
            separator = "=" * 80
            formatted_text = f"{separator}\n{text}\n{separator}"
        elif level == 2:
            separator = "=" * 60
            formatted_text = f"{separator}\n{text}\n{separator}"
        elif level == 3:
            separator = "=" * 40
            formatted_text = f"{separator}\n{text}\n{separator}"
        else:
            separator = "-" * len(text)
            formatted_text = f"{text}\n{separator}"

        self.add_text(formatted_text, step)

    def add_section(self, title, content, step=None):
        """Add a section with title and content."""
        self.add_text(f"\n{title}:", step)
        self.add_text(content, step)

    def get_all_text(self):
        """Get all captured text."""
        full_text = self.analysis_text
        for step, text in self.step_texts.items():
            full_text += f"\n\n{text}"
        return full_text

    def save_to_file(self, filename="osm_analysis_report.txt"):
        """Save all captured text to a file."""
        # Ensure output directory exists
        ensure_output_dir()

        # Create full path
        if not filename.startswith(str(OUTPUT_DIR)):
            filepath = OUTPUT_DIR / filename
        else:
            filepath = Path(filename)

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"OSM Power Plant Analysis Report\nGenerated: {timestamp}\n\n"

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(header)
            f.write(self.get_all_text())

        return str(filepath)


def create_powerplant_datasets(text_capture):
    """
    Create powerplant datasets with and without OSM data.
    Returns dataframes dictionary instead of saving CSV files.
    """
    text_capture.add_header(
        "STEP 1: CREATING POWERPLANT DATASETS", level=2, step="dataset_creation"
    )

    config = pm.get_config(filename=None)

    # Configuration without OSM
    config_without_osm = config.copy()
    config_without_osm["matching_sources"] = [
        source for source in config_without_osm["matching_sources"] if source != "OSM"
    ]
    config_without_osm["fully_included_sources"] = [
        source
        for source in config_without_osm["fully_included_sources"]
        if source != "OSM"
    ]

    # Configuration with OSM
    config_with_osm = config.copy()

    dataframes = {}

    for cfg, label in [
        (config_with_osm, "with_osm"),
        (config_without_osm, "without_osm"),
    ]:
        text_capture.add_text(
            f"Processing {label} configuration...", step="dataset_creation"
        )
        df = pm.powerplants(config=cfg, reduced=True, update=True)

        # Ensure output directory exists and save CSV files
        ensure_output_dir()
        csv_path = OUTPUT_DIR / f"powerplants_eu_reduced_{label}.csv"
        df.to_csv(csv_path, index=False)

        # Store in memory
        dataframes[label] = df

        text_capture.add_text(
            f"Dataset {label}: {len(df):,} plants, {df['Capacity'].sum():,.0f} MW",
            step="dataset_creation",
        )

    text_capture.add_text("Dataset creation completed.", step="dataset_creation")
    text_capture.add_text(
        f"Plants added by OSM: {len(dataframes['with_osm']) - len(dataframes['without_osm']):,}",
        step="dataset_creation",
    )
    text_capture.add_text(
        f"Capacity added by OSM: {dataframes['with_osm']['Capacity'].sum() - dataframes['without_osm']['Capacity'].sum():,.0f} MW",
        step="dataset_creation",
    )

    return dataframes


def load_powerplant_datasets(text_capture):
    """Load powerplant datasets with and without OSM data."""
    text_capture.add_header(
        "STEP 1: LOADING POWERPLANT DATASETS", level=2, step="dataset_loading"
    )

    dataframes = {
        label: pd.read_csv(OUTPUT_DIR / f"powerplants_eu_reduced_{label}.csv")
        for label in ["with_osm", "without_osm"]
    }

    text_capture.add_text("Dataset loading completed.", step="dataset_loading")
    text_capture.add_text(
        f"Plants added by OSM: {len(dataframes['with_osm']) - len(dataframes['without_osm']):,}",
        step="dataset_loading",
    )
    text_capture.add_text(
        f"Capacity added by OSM: {dataframes['with_osm']['Capacity'].sum() - dataframes['without_osm']['Capacity'].sum():,.0f} MW",
        step="dataset_loading",
    )

    return dataframes


def classify_osm_involvement(project_id_str):
    """Classify plants by OSM involvement - same logic as map."""
    if pd.isna(project_id_str) or "OSM" not in str(project_id_str):
        return "no_osm"

    project_id = str(project_id_str)

    if project_id.startswith("OSM_") or (
        project_id.count("{") <= 2
        and project_id.count("'OSM'") == 1
        and project_id.count("',") <= 1
    ):
        return "osm_only"
    else:
        return "osm_mixed"


def prepare_data_for_analysis(dataframes, text_capture):
    """Load and prepare data for analysis using dataframes instead of CSV files."""

    text_capture.add_section("Preparing data for analysis", "", step="data_preparation")

    # Use dataframes instead of loading CSV
    df_ref = dataframes["without_osm"].copy()
    df_combined = dataframes["with_osm"].copy()

    # Add classifications
    df_ref["dataset"] = "Reference (No OSM)"
    df_ref["osm_involvement"] = "no_osm"

    df_combined["dataset"] = "Combined (With OSM)"
    df_combined["osm_involvement"] = df_combined["projectID"].apply(
        classify_osm_involvement
    )

    # Filter for valid coordinates
    df_ref = df_ref[(df_ref["lat"].notna()) & (df_ref["lon"].notna())]
    df_combined = df_combined[
        (df_combined["lat"].notna()) & (df_combined["lon"].notna())
    ]

    # Filter for Europe (same bounds as map)
    europe_bounds = {"lat_min": 35, "lat_max": 71, "lon_min": -25, "lon_max": 45}

    def filter_europe(df):
        return df[
            (df["lat"] >= europe_bounds["lat_min"])
            & (df["lat"] <= europe_bounds["lat_max"])
            & (df["lon"] >= europe_bounds["lon_min"])
            & (df["lon"] <= europe_bounds["lon_max"])
        ]

    df_ref = filter_europe(df_ref)
    df_combined = filter_europe(df_combined)

    text_capture.add_text(
        "After filtering for Europe and valid coordinates:", step="data_preparation"
    )
    text_capture.add_text(f"Reference: {len(df_ref):,} plants", step="data_preparation")
    text_capture.add_text(
        f"Combined: {len(df_combined):,} plants", step="data_preparation"
    )

    # Create separate datasets for analysis
    osm_only = df_combined[df_combined["osm_involvement"] == "osm_only"].copy()
    osm_mixed = df_combined[df_combined["osm_involvement"] == "osm_mixed"].copy()

    text_capture.add_text(
        f"OSM-only plants: {len(osm_only):,}", step="data_preparation"
    )
    text_capture.add_text(
        f"OSM-mixed plants: {len(osm_mixed):,}", step="data_preparation"
    )

    return df_ref, df_combined, osm_only, osm_mixed


def create_osm_involvement_chart(df_ref, df_combined, text_capture):
    """Create OSM involvement comparison chart and save as PNG."""

    text_capture.add_section(
        "Creating OSM involvement comparison chart", "", step="visualizations"
    )

    # Calculate OSM involvement statistics
    involvement_stats = (
        df_combined.groupby("osm_involvement")
        .agg({"Capacity": ["count", "sum"]})
        .round(0)
    )
    involvement_stats.columns = ["Plant_Count", "Total_Capacity_MW"]

    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Colors matching the map
    colors = {"no_osm": "#95A5A6", "osm_only": "#E74C3C", "osm_mixed": "#3498DB"}
    labels = {"no_osm": "No OSM", "osm_only": "OSM Only", "osm_mixed": "OSM + Others"}

    # Plant count pie chart
    plant_counts = [
        involvement_stats.loc[cat, "Plant_Count"]
        for cat in ["no_osm", "osm_only", "osm_mixed"]
    ]
    plant_labels = [labels[cat] for cat in ["no_osm", "osm_only", "osm_mixed"]]
    color_list = [colors[cat] for cat in ["no_osm", "osm_only", "osm_mixed"]]

    wedges1, texts1, autotexts1 = ax1.pie(
        plant_counts,
        labels=plant_labels,
        colors=color_list,
        autopct=lambda pct: f"{pct:.1f}%\n({pct * sum(plant_counts) / 100:,.0f})",
        startangle=90,
        textprops={"fontsize": 10},
    )
    ax1.set_title(
        f"Plants by OSM Involvement (Total: {sum(plant_counts):,} plants)",
        fontsize=12,
        fontweight="bold",
    )

    # Capacity pie chart
    capacity_values = [
        involvement_stats.loc[cat, "Total_Capacity_MW"]
        for cat in ["no_osm", "osm_only", "osm_mixed"]
    ]

    wedges2, texts2, autotexts2 = ax2.pie(
        capacity_values,
        labels=plant_labels,
        colors=color_list,
        autopct=lambda pct: f"{pct:.1f}%\n({pct * sum(capacity_values) / 100:,.0f} MW)",
        startangle=90,
        textprops={"fontsize": 10},
    )
    ax2.set_title(
        f"Capacity by OSM Involvement (Total: {sum(capacity_values):,.0f} MW)",
        fontsize=12,
        fontweight="bold",
    )

    plt.tight_layout()
    ensure_output_dir()
    plot_path = OUTPUT_DIR / "osm_involvement_comparison.png"
    plt.savefig(plot_path, dpi=300, bbox_inches="tight")
    plt.close()

    text_capture.add_text(f"Saved: {plot_path}", step="visualizations")

    # Add statistics to text capture
    text_capture.add_section("OSM Involvement Statistics", "", step="visualizations")
    for category in ["no_osm", "osm_only", "osm_mixed"]:
        if category in involvement_stats.index:
            plants = involvement_stats.loc[category, "Plant_Count"]
            capacity = involvement_stats.loc[category, "Total_Capacity_MW"]
            text_capture.add_text(
                f"{labels[category]}: {plants:,} plants ({capacity:,.0f} MW)",
                step="visualizations",
            )

    return involvement_stats


def create_osm_only_analysis(osm_only, text_capture):
    """Create OSM-only plants analysis and save as PNG."""

    text_capture.add_section(
        "Creating OSM-only plants analysis", "", step="osm_only_analysis"
    )

    # Technology breakdown
    tech_breakdown = (
        osm_only.groupby("Technology").agg({"Capacity": ["count", "sum"]}).round(0)
    )
    tech_breakdown.columns = ["Plant_Count", "Total_Capacity_MW"]
    tech_breakdown = tech_breakdown.sort_values("Total_Capacity_MW", ascending=True)

    # Country breakdown (top 10)
    country_breakdown = (
        osm_only.groupby("Country").agg({"Capacity": ["count", "sum"]}).round(0)
    )
    country_breakdown.columns = ["Plant_Count", "Total_Capacity_MW"]
    country_breakdown = country_breakdown.sort_values(
        "Total_Capacity_MW", ascending=False
    ).head(10)

    # Add detailed breakdown to text
    text_capture.add_section(
        "OSM-Only Plants by Technology", "", step="osm_only_analysis"
    )
    for tech, row in tech_breakdown.iterrows():
        text_capture.add_text(
            f"{tech}: {row['Plant_Count']} plants, {row['Total_Capacity_MW']:,.0f} MW",
            step="osm_only_analysis",
        )

    text_capture.add_section(
        "OSM-Only Plants by Country (Top 10)", "", step="osm_only_analysis"
    )
    for country, row in country_breakdown.iterrows():
        text_capture.add_text(
            f"{country}: {row['Plant_Count']} plants, {row['Total_Capacity_MW']:,.0f} MW",
            step="osm_only_analysis",
        )

    # Create visualization
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

    # Technology breakdown horizontal bar chart
    tech_colors = [
        "#E74C3C",
        "#3498DB",
        "#F39C12",
        "#27AE60",
        "#9B59B6",
        "#E67E22",
        "#16A085",
        "#F1C40F",
    ]
    bars1 = ax1.barh(
        tech_breakdown.index,
        tech_breakdown["Total_Capacity_MW"],
        color=tech_colors[: len(tech_breakdown)],
    )

    ax1.set_xlabel("Capacity (MW)", fontsize=11)
    ax1.set_title(
        "OSM-Only Plants by Technology (Exclusive OSM Data)",
        fontsize=12,
        fontweight="bold",
    )
    ax1.grid(axis="x", alpha=0.3)

    # Add capacity values on bars
    for i, bar in enumerate(bars1):
        width = bar.get_width()
        plants = tech_breakdown.iloc[i]["Plant_Count"]
        ax1.text(
            width + width * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{width:,.0f} MW\n({plants:,} plants)",
            ha="left",
            va="center",
            fontsize=9,
        )

    # Country breakdown
    bars2 = ax2.barh(
        range(len(country_breakdown)),
        country_breakdown["Total_Capacity_MW"],
        color="#E74C3C",
    )

    ax2.set_yticks(range(len(country_breakdown)))
    ax2.set_yticklabels(country_breakdown.index)
    ax2.set_xlabel("Capacity (MW)", fontsize=11)
    ax2.set_title(
        "OSM-Only Plants by Country (Top 10) (Exclusive OSM Data)",
        fontsize=12,
        fontweight="bold",
    )
    ax2.grid(axis="x", alpha=0.3)

    # Add values on bars
    for i, bar in enumerate(bars2):
        width = bar.get_width()
        plants = country_breakdown.iloc[i]["Plant_Count"]
        ax2.text(
            width + width * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{width:,.0f} MW\n({plants:,} plants)",
            ha="left",
            va="center",
            fontsize=9,
        )

    plt.tight_layout()
    ensure_output_dir()
    plot_path = OUTPUT_DIR / "osm_only_plants_analysis.png"
    plt.savefig(plot_path, dpi=300, bbox_inches="tight")
    plt.close()

    text_capture.add_text(f"Saved: {plot_path}", step="osm_only_analysis")

    return tech_breakdown, country_breakdown


def create_summary_visualization(df_combined, text_capture):
    """Create one key summary visualization for the report."""

    text_capture.add_section(
        "Creating technology OSM summary visualization", "", step="visualizations"
    )

    # Create comprehensive comparison by technology
    tech_comparison = (
        df_combined.groupby(["Technology", "osm_involvement"])
        .agg({"Capacity": "sum"})
        .round(0)
        .unstack(fill_value=0)
    )
    tech_comparison.columns = tech_comparison.columns.droplevel(0)
    tech_comparison.columns = ["No OSM", "OSM + Others", "OSM Only"]
    tech_comparison["Total"] = tech_comparison.sum(axis=1)
    tech_comparison = tech_comparison.sort_values("Total", ascending=False).head(8)

    # Calculate percentages for OSM contribution
    tech_comparison["OSM_Total"] = (
        tech_comparison["OSM + Others"] + tech_comparison["OSM Only"]
    )
    tech_comparison["OSM_Share_%"] = (
        tech_comparison["OSM_Total"] / tech_comparison["Total"] * 100
    ).round(1)

    # Add detailed statistics to text
    text_capture.add_section(
        "Technology OSM Share Analysis", "", step="technology_analysis"
    )
    text_capture.add_text(
        "Technology             Total_MW    OSM_MW     OSM_Share_%",
        step="technology_analysis",
    )
    text_capture.add_text("-" * 60, step="technology_analysis")
    for tech, row in tech_comparison.iterrows():
        text_capture.add_text(
            f"{tech:<20} {row['Total']:>8,.0f}  {row['OSM_Total']:>8,.0f}  {row['OSM_Share_%']:>8.1f}%",
            step="technology_analysis",
        )

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

    # Stacked bar chart showing OSM vs Non-OSM capacity
    technologies = tech_comparison.index
    x_pos = np.arange(len(technologies))

    # Create stacked bars
    ax1.bar(
        x_pos, tech_comparison["No OSM"], label="No OSM", color="#95A5A6", alpha=0.8
    )
    ax1.bar(
        x_pos,
        tech_comparison["OSM + Others"],
        bottom=tech_comparison["No OSM"],
        label="OSM + Others",
        color="#3498DB",
        alpha=0.8,
    )
    ax1.bar(
        x_pos,
        tech_comparison["OSM Only"],
        bottom=tech_comparison["No OSM"] + tech_comparison["OSM + Others"],
        label="OSM Only",
        color="#E74C3C",
        alpha=0.8,
    )

    ax1.set_xlabel("Technology", fontsize=11)
    ax1.set_ylabel("Capacity (MW)", fontsize=11)
    ax1.set_title(
        "European Power Plant Capacity by Technology - OSM Contribution Analysis",
        fontsize=12,
        fontweight="bold",
    )
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(technologies, rotation=45, ha="right")
    ax1.legend()
    ax1.grid(axis="y", alpha=0.3)

    # Format y-axis to show values in GW
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x / 1000:.0f} GW"))

    # OSM share percentage chart
    osm_shares = tech_comparison["OSM_Share_%"]
    bars_pct = ax2.barh(technologies, osm_shares, color="#2ECC71", alpha=0.8)

    ax2.set_xlabel("OSM Share (%)", fontsize=11)
    ax2.set_title(
        "OSM Data Coverage by Technology (% of Total Capacity)",
        fontsize=12,
        fontweight="bold",
    )
    ax2.grid(axis="x", alpha=0.3)

    # Add percentage labels on bars
    for i, bar in enumerate(bars_pct):
        width = bar.get_width()
        total_capacity = tech_comparison.iloc[i]["Total"]
        ax2.text(
            width + 1,
            bar.get_y() + bar.get_height() / 2,
            f"{width:.1f}%\n({total_capacity / 1000:.0f} GW total)",
            ha="left",
            va="center",
            fontsize=9,
        )

    plt.tight_layout()
    ensure_output_dir()
    plot_path = OUTPUT_DIR / "technology_osm_summary.png"
    plt.savefig(plot_path, dpi=300, bbox_inches="tight")
    plt.close()

    text_capture.add_text(f"Saved: {plot_path}", step="visualizations")
    return tech_comparison


def generate_corrected_report(df_ref, df_combined, text_capture):
    """Generate corrected statistics matching the map data."""

    text_capture.add_header(
        "STEP 2: OSM ANALYSIS REPORT", level=2, step="main_analysis"
    )

    # Overall statistics
    osm_only = df_combined[df_combined["osm_involvement"] == "osm_only"]
    osm_mixed = df_combined[df_combined["osm_involvement"] == "osm_mixed"]
    no_osm = df_combined[df_combined["osm_involvement"] == "no_osm"]

    total_plants = len(df_combined)
    total_capacity = df_combined["Capacity"].sum()

    text_capture.add_section("OVERALL STATISTICS", "", step="main_analysis")
    text_capture.add_text(
        f"Reference plants: {len(df_ref):,} ({df_ref['Capacity'].sum():,.0f} MW)",
        step="main_analysis",
    )
    text_capture.add_text(
        f"Combined plants: {total_plants:,} ({total_capacity:,.0f} MW)",
        step="main_analysis",
    )
    text_capture.add_text(
        f"Plants added by OSM: {total_plants - len(df_ref):,} ({total_capacity - df_ref['Capacity'].sum():,.0f} MW)",
        step="main_analysis",
    )

    text_capture.add_section("OSM INVOLVEMENT BREAKDOWN", "", step="main_analysis")
    text_capture.add_text(
        f"OSM-Only: {len(osm_only):,} plants ({osm_only['Capacity'].sum():,.0f} MW) - {len(osm_only) / total_plants * 100:.1f}% plants, {osm_only['Capacity'].sum() / total_capacity * 100:.1f}% capacity",
        step="main_analysis",
    )
    text_capture.add_text(
        f"OSM-Mixed: {len(osm_mixed):,} plants ({osm_mixed['Capacity'].sum():,.0f} MW) - {len(osm_mixed) / total_plants * 100:.1f}% plants, {osm_mixed['Capacity'].sum() / total_capacity * 100:.1f}% capacity",
        step="main_analysis",
    )
    text_capture.add_text(
        f"No OSM: {len(no_osm):,} plants ({no_osm['Capacity'].sum():,.0f} MW) - {len(no_osm) / total_plants * 100:.1f}% plants, {no_osm['Capacity'].sum() / total_capacity * 100:.1f}% capacity",
        step="main_analysis",
    )

    return {
        "reference_plants": len(df_ref),
        "reference_capacity": df_ref["Capacity"].sum(),
        "total_plants": total_plants,
        "total_capacity": total_capacity,
        "osm_only_plants": len(osm_only),
        "osm_only_capacity": osm_only["Capacity"].sum(),
        "osm_mixed_plants": len(osm_mixed),
        "osm_mixed_capacity": osm_mixed["Capacity"].sum(),
        "no_osm_plants": len(no_osm),
        "no_osm_capacity": no_osm["Capacity"].sum(),
    }


def analyze_by_category(df, category_col, title, analysis_type="", text_capture=None):
    """Generic function to analyze OSM coverage by any category."""

    text_capture.add_section(f"{title} {analysis_type}", "", step="detailed_analysis")
    text_capture.add_text(
        "-" * (len(title) + len(analysis_type) + 1), step="detailed_analysis"
    )

    # Group by category and OSM classification
    category_analysis = (
        df.groupby([category_col, "osm_classification"])
        .agg({"Capacity": ["count", "sum"]})
        .round(0)
    )

    # Flatten column names
    category_analysis.columns = ["Plant_Count", "Total_Capacity"]

    # Restructure data
    results = {}
    for category in df[category_col].unique():
        if pd.isna(category):
            continue

        category_data = {
            "osm_standalone_plants": 0,
            "osm_standalone_capacity": 0,
            "osm_mixed_plants": 0,
            "osm_mixed_capacity": 0,
            "no_osm_plants": 0,
            "no_osm_capacity": 0,
            "total_plants": 0,
            "total_capacity": 0,
        }

        for osm_type in ["osm_standalone", "osm_mixed", "no_osm"]:
            try:
                plants = int(category_analysis.loc[(category, osm_type), "Plant_Count"])
                capacity = int(
                    category_analysis.loc[(category, osm_type), "Total_Capacity"]
                )
            except KeyError:
                plants = 0
                capacity = 0

            category_data[f"{osm_type}_plants"] = plants
            category_data[f"{osm_type}_capacity"] = capacity
            category_data["total_plants"] += plants
            category_data["total_capacity"] += capacity

        results[category] = category_data

    # Sort by total capacity
    sorted_categories = sorted(
        results.items(), key=lambda x: x[1]["total_capacity"], reverse=True
    )

    # Create formatted output
    header = f"{'Category':<15} {'Tot_MW':<10} {'OSM_Only_MW':<12} {'OSM_Mix_MW':<12} {'No_OSM_MW':<11} {'OSM_Only%':<9} {'OSM_Mix%':<9}"
    text_capture.add_text(header, step="detailed_analysis")
    text_capture.add_text("-" * 95, step="detailed_analysis")

    # Add results
    for category, data in sorted_categories[:15]:  # Top 15
        if data["total_capacity"] == 0:
            continue

        osm_only_pct = (
            (data["osm_standalone_capacity"] / data["total_capacity"] * 100)
            if data["total_capacity"] > 0
            else 0
        )
        osm_mix_pct = (
            (data["osm_mixed_capacity"] / data["total_capacity"] * 100)
            if data["total_capacity"] > 0
            else 0
        )

        row = f"{str(category):<15} {data['total_capacity']:<10,} {data['osm_standalone_capacity']:<12,} {data['osm_mixed_capacity']:<12,} {data['no_osm_capacity']:<11,} {osm_only_pct:<9.1f} {osm_mix_pct:<9.1f}"
        text_capture.add_text(row, step="detailed_analysis")


def classify_osm_sources_detailed(project_id_str):
    """
    Classify OSM involvement for detailed analysis.
    Returns: 'osm_standalone', 'osm_mixed', or 'no_osm'
    """
    if pd.isna(project_id_str) or "OSM" not in str(project_id_str):
        return "no_osm"

    project_id = str(project_id_str)

    # Count source types by looking for pattern like {'SOURCE': {...}}
    # More sophisticated parsing to distinguish standalone vs mixed
    if project_id.startswith("OSM_") or (
        project_id.count("{") <= 2
        and project_id.count("'OSM'") == 1
        and project_id.count("',") <= 1
    ):
        return "osm_standalone"
    else:
        return "osm_mixed"


def separated_osm_analysis(df_combined, text_capture):
    """Main analysis function with clear separation of OSM contributions."""

    text_capture.add_header(
        "STEP 4: DETAILED OSM ANALYSIS", level=2, step="detailed_analysis"
    )

    # Classify OSM involvement with more detailed classification
    df_combined["osm_classification"] = df_combined["projectID"].apply(
        classify_osm_sources_detailed
    )

    # Overall statistics
    text_capture.add_section("OSM CONTRIBUTION BREAKDOWN", "", step="detailed_analysis")
    text_capture.add_text("=" * 40, step="detailed_analysis")

    summary_stats = (
        df_combined.groupby("osm_classification")
        .agg({"Capacity": ["count", "sum"]})
        .round(0)
    )
    summary_stats.columns = ["Plant_Count", "Total_Capacity"]

    total_plants = len(df_combined)
    total_capacity = df_combined["Capacity"].sum()

    for osm_type in ["osm_standalone", "osm_mixed", "no_osm"]:
        if osm_type in summary_stats.index:
            plants = int(summary_stats.loc[osm_type, "Plant_Count"])
            capacity = int(summary_stats.loc[osm_type, "Total_Capacity"])
            plant_pct = plants / total_plants * 100
            capacity_pct = capacity / total_capacity * 100

            type_name = osm_type.replace("_", " ").title()
            text_capture.add_text(
                f"{type_name:<15}: {plants:>7,} plants ({plant_pct:4.1f}%) | {capacity:>9,} MW ({capacity_pct:4.1f}%)",
                step="detailed_analysis",
            )

    text_capture.add_text(
        f"{'Total':<15}: {total_plants:>7,} plants (100.0%) | {int(total_capacity):>9,} MW (100.0%)",
        step="detailed_analysis",
    )

    # Part 1: OSM STANDALONE ANALYSIS
    text_capture.add_header(
        "PART 1: OSM STANDALONE CONTRIBUTION", level=3, step="detailed_analysis"
    )

    osm_standalone = df_combined[
        df_combined["osm_classification"] == "osm_standalone"
    ].copy()

    if len(osm_standalone) == 0:
        text_capture.add_text(
            "No standalone OSM plants found.", step="detailed_analysis"
        )
        return

    standalone_capacity = osm_standalone["Capacity"].sum()
    standalone_plants = len(osm_standalone)

    text_capture.add_section("OSM Standalone Summary", "", step="detailed_analysis")
    text_capture.add_text(
        f"Plants: {standalone_plants:,} ({standalone_plants / total_plants * 100:.1f}% of total)",
        step="detailed_analysis",
    )
    text_capture.add_text(
        f"Capacity: {standalone_capacity:,.0f} MW ({standalone_capacity / total_capacity * 100:.1f}% of total)",
        step="detailed_analysis",
    )

    # OSM Standalone by Technology
    analyze_by_category(
        osm_standalone,
        "Technology",
        "OSM STANDALONE BY TECHNOLOGY",
        "(Exclusive OSM Data)",
        text_capture,
    )

    # OSM Standalone by Fuel Type
    analyze_by_category(
        osm_standalone,
        "Fueltype",
        "OSM STANDALONE BY FUEL TYPE",
        "(Exclusive OSM Data)",
        text_capture,
    )

    # OSM Standalone by Country
    analyze_by_category(
        osm_standalone,
        "Country",
        "OSM STANDALONE BY COUNTRY",
        "(Exclusive OSM Data)",
        text_capture,
    )

    # Part 2: OSM MIXED ANALYSIS
    text_capture.add_header(
        "PART 2: OSM MIXED CONTRIBUTION", level=3, step="detailed_analysis"
    )

    osm_mixed = df_combined[df_combined["osm_classification"] == "osm_mixed"].copy()

    mixed_capacity = osm_mixed["Capacity"].sum() if len(osm_mixed) > 0 else 0
    mixed_plants = len(osm_mixed)

    text_capture.add_section("OSM Mixed Summary", "", step="detailed_analysis")
    text_capture.add_text(
        f"Plants: {mixed_plants:,} ({mixed_plants / total_plants * 100:.1f}% of total)",
        step="detailed_analysis",
    )
    text_capture.add_text(
        f"Capacity: {mixed_capacity:,.0f} MW ({mixed_capacity / total_capacity * 100:.1f}% of total)",
        step="detailed_analysis",
    )

    if mixed_plants > 0:
        # OSM Mixed by Technology
        analyze_by_category(
            osm_mixed,
            "Technology",
            "OSM MIXED BY TECHNOLOGY",
            "(OSM + Other Sources)",
            text_capture,
        )

        # OSM Mixed by Fuel Type
        analyze_by_category(
            osm_mixed,
            "Fueltype",
            "OSM MIXED BY FUEL TYPE",
            "(OSM + Other Sources)",
            text_capture,
        )

        # OSM Mixed by Country
        analyze_by_category(
            osm_mixed,
            "Country",
            "OSM MIXED BY COUNTRY",
            "(OSM + Other Sources)",
            text_capture,
        )

    # Part 3: COMPARATIVE ANALYSIS
    text_capture.add_header(
        "PART 3: COMPARATIVE ANALYSIS", level=3, step="detailed_analysis"
    )

    # Overall comparison across all categories
    analyze_by_category(
        df_combined,
        "Technology",
        "TECHNOLOGY COMPARISON",
        "(Standalone vs Mixed vs None)",
        text_capture,
    )
    analyze_by_category(
        df_combined,
        "Fueltype",
        "FUEL TYPE COMPARISON",
        "(Standalone vs Mixed vs None)",
        text_capture,
    )
    analyze_by_category(
        df_combined,
        "Country",
        "COUNTRY COMPARISON",
        "(Standalone vs Mixed vs None)",
        text_capture,
    )

    # Technology-Fuel combinations for standalone
    text_capture.add_section(
        "TOP OSM STANDALONE TECHNOLOGY-FUEL COMBINATIONS", "", step="detailed_analysis"
    )
    text_capture.add_text("-" * 55, step="detailed_analysis")

    if len(osm_standalone) > 0:
        tech_fuel_combo = (
            osm_standalone.groupby(["Technology", "Fueltype"])
            .agg({"Capacity": ["count", "sum"]})
            .round(0)
        )

        tech_fuel_combo.columns = ["Plant_Count", "Total_Capacity"]
        tech_fuel_combo = tech_fuel_combo.sort_values("Total_Capacity", ascending=False)

        header = (
            f"{'Technology':<15} {'Fueltype':<15} {'Plants':<8} {'Capacity_MW':<12}"
        )
        text_capture.add_text(header, step="detailed_analysis")
        text_capture.add_text("-" * 55, step="detailed_analysis")

        for (tech, fuel), row in tech_fuel_combo.head(15).iterrows():
            line = f"{tech:<15} {fuel:<15} {int(row['Plant_Count']):<8} {int(row['Total_Capacity']):<12,}"
            text_capture.add_text(line, step="detailed_analysis")

    # Data quality assessment
    text_capture.add_section(
        "DATA QUALITY - OSM STANDALONE", "", step="detailed_analysis"
    )
    text_capture.add_text("-" * 35, step="detailed_analysis")

    if len(osm_standalone) > 0:
        missing_data = osm_standalone.isnull().sum()
        text_capture.add_text(
            "Missing values in OSM standalone data:", step="detailed_analysis"
        )
        for col in [
            "Name",
            "Technology",
            "Fueltype",
            "Country",
            "Capacity",
            "lat",
            "lon",
        ]:
            if col in missing_data:
                missing_count = missing_data[col]
                missing_pct = missing_count / len(osm_standalone) * 100
                text_capture.add_text(
                    f"  {col:<12}: {missing_count:>6,} ({missing_pct:4.1f}%)",
                    step="detailed_analysis",
                )

    text_capture.add_text("Detailed analysis complete!", step="detailed_analysis")


def create_color_schemes():
    """Define color schemes for different categories."""

    # Technology colors
    tech_colors = {
        "Steam Turbine": "#FF6B6B",  # Red
        "CCGT": "#4ECDC4",  # Teal
        "Onshore": "#45B7D1",  # Blue
        "PV": "#FFA07A",  # Light Orange
        "Pv": "#FFA07A",  # Light Orange
        "Reservoir": "#98D8C8",  # Light Green
        "Run-Of-River": "#95E1D3",  # Mint
        "Pumped Storage": "#F7DC6F",  # Yellow
        "Offshore": "#3498DB",  # Dark Blue
        "Combustion Engine": "#E74C3C",  # Dark Red
        "CSP": "#F39C12",  # Orange
        "Csp": "#E67E22",  # Different from CSP
        "Marine": "#2E86AB",  # Navy
        "Unknown": "#D7DBDD",  # Gray
        "unknown": "#D7DBDD",  # Gray
        "Not Found": "#D7DBDD",  # Gray
        "not found": "#D7DBDD",  # Gray
    }

    # Fuel type colors
    fuel_colors = {
        "Wind": "#3498DB",  # Blue
        "Solar": "#F1C40F",  # Yellow
        "Hydro": "#16A085",  # Green
        "Natural Gas": "#E67E22",  # Orange
        "Nuclear": "#9B59B6",  # Purple
        "Hard Coal": "#34495E",  # Dark Gray
        "Lignite": "#7F8C8D",  # Gray
        "Oil": "#E74C3C",  # Red
        "Solid Biomass": "#27AE60",  # Green
        "Biogas": "#2ECC71",  # Light Green
        "Waste": "#8E44AD",  # Purple
        "Geothermal": "#D35400",  # Dark Orange
        "Other": "#95A5A6",  # Light Gray
    }

    # OSM involvement colors with better names
    osm_colors = {
        "no_osm": "#202222",  # Gray - "No OSM"
        "osm_only": "#E74C3C",  # Red - "OSM Only"
        "osm_mixed": "#3498DB",  # Blue - "OSM + Others"
    }

    # Better display names for OSM categories
    osm_names = {
        "no_osm": "No OSM",
        "osm_only": "OSM Only",
        "osm_mixed": "OSM + Others",
    }

    return tech_colors, fuel_colors, osm_colors, osm_names


def create_plant_hover_text(df):
    """Create detailed hover text for plants."""

    hover_text = []
    for _, row in df.iterrows():
        capacity_text = (
            f"{row['Capacity']:.1f} MW" if pd.notna(row["Capacity"]) else "Unknown"
        )

        text = (
            f"<b>{row['Name']}</b><br>"
            if pd.notna(row["Name"])
            else "<b>Unnamed Plant</b><br>"
        )
        text += f"Country: {row['Country']}<br>"
        text += f"Technology: {row['Technology']}<br>"
        text += f"Fuel: {row['Fueltype']}<br>"
        text += f"Capacity: {capacity_text}<br>"
        text += f"Dataset: {row['dataset']}<br>"

        # Add OSM involvement info
        if row["osm_involvement"] == "osm_only":
            text += "OSM Status: Exclusive OSM Data"
        elif row["osm_involvement"] == "osm_mixed":
            text += "OSM Status: OSM + Other Sources"
        else:
            text += "OSM Status: No OSM Data"

        hover_text.append(text)

    return hover_text


def create_europe_map(df_ref, df_combined, osm_only, osm_mixed, text_capture):
    """Create the main interactive map with proper legends."""

    text_capture.add_header(
        "STEP 3: CREATING INTERACTIVE EUROPE MAP", level=2, step="map_creation"
    )

    tech_colors, fuel_colors, osm_colors, osm_names = create_color_schemes()

    fig = go.Figure()

    def add_traces_by_category(
        df, color_col, color_scheme, legend_prefix, visible=True, display_names=None
    ):
        """Add separate traces for each category to enable proper legend."""

        # Get unique categories
        categories = sorted([cat for cat in df[color_col].unique() if pd.notna(cat)])

        for category in categories:
            df_cat = df[df[color_col] == category]

            if len(df_cat) == 0:
                continue

            # Create size array (larger for bigger plants)
            sizes = np.where(
                df_cat["Capacity"] > 1000,
                12,
                np.where(
                    df_cat["Capacity"] > 100, 8, np.where(df_cat["Capacity"] > 10, 6, 4)
                ),
            )

            color = color_scheme.get(category, "#95A5A6")
            hover_text = create_plant_hover_text(df_cat)

            # Use display names if provided (for OSM categories)
            display_name = (
                display_names.get(category, category) if display_names else category
            )

            fig.add_trace(
                go.Scattermap(
                    lat=df_cat["lat"],
                    lon=df_cat["lon"],
                    mode="markers",
                    marker=dict(
                        size=sizes, color=color, opacity=0.7, sizemode="diameter"
                    ),
                    text=hover_text,
                    hovertemplate="%{text}<extra></extra>",
                    name=display_name,
                    visible=visible,
                    showlegend=True,
                    legendgroup=legend_prefix,
                )
            )

        return len(categories)

    # Count traces for each view (for dropdown visibility arrays)
    ref_tech_count = add_traces_by_category(
        df_ref, "Technology", tech_colors, "ref_tech", visible=False
    )

    comb_tech_count = add_traces_by_category(
        df_combined, "Technology", tech_colors, "comb_tech", visible=False
    )

    ref_fuel_count = add_traces_by_category(
        df_ref, "Fueltype", fuel_colors, "ref_fuel", visible=False
    )

    comb_fuel_count = add_traces_by_category(
        df_combined, "Fueltype", fuel_colors, "comb_fuel", visible=False
    )

    osm_status_count = add_traces_by_category(
        df_combined,
        "osm_involvement",
        osm_colors,
        "osm_status",
        visible=True,
        display_names=osm_names,
    )

    osm_only_count = 0
    if len(osm_only) > 0:
        osm_only_count = add_traces_by_category(
            osm_only, "Technology", tech_colors, "osm_only_tech", visible=False
        )

    # Create visibility arrays for dropdown menu
    total_traces = (
        ref_tech_count
        + comb_tech_count
        + ref_fuel_count
        + comb_fuel_count
        + osm_status_count
        + osm_only_count
    )

    def create_visibility_array(active_start, active_count, total):
        """Create visibility array with only specified range visible."""
        visibility = [False] * total
        for i in range(active_start, active_start + active_count):
            visibility[i] = True
        return visibility

    # Calculate starting positions
    ref_tech_start = 0
    comb_tech_start = ref_tech_count
    ref_fuel_start = comb_tech_start + comb_tech_count
    comb_fuel_start = ref_fuel_start + ref_fuel_count
    osm_status_start = comb_fuel_start + comb_fuel_count
    osm_only_start = osm_status_start + osm_status_count

    # Calculate summary statistics for annotation
    stats = {
        "reference_plants": len(df_ref),
        "reference_capacity": df_ref["Capacity"].sum(),
        "combined_plants": len(df_combined),
        "combined_capacity": df_combined["Capacity"].sum(),
        "osm_only_plants": len(osm_only),
        "osm_only_capacity": osm_only["Capacity"].sum() if len(osm_only) > 0 else 0,
        "osm_mixed_plants": len(osm_mixed),
        "osm_mixed_capacity": osm_mixed["Capacity"].sum() if len(osm_mixed) > 0 else 0,
    }

    # Calculate additions
    stats["plants_added"] = stats["combined_plants"] - stats["reference_plants"]
    stats["capacity_added"] = stats["combined_capacity"] - stats["reference_capacity"]

    # Add map statistics to text capture
    text_capture.add_section("Interactive Map Statistics", "", step="map_creation")
    text_capture.add_text(
        f"Reference: {stats['reference_plants']:,} plants ({stats['reference_capacity']:,.0f} MW)",
        step="map_creation",
    )
    text_capture.add_text(
        f"Combined: {stats['combined_plants']:,} plants ({stats['combined_capacity']:,.0f} MW)",
        step="map_creation",
    )
    text_capture.add_text(
        f"Added by OSM: {stats['plants_added']:,} plants ({stats['capacity_added']:,.0f} MW)",
        step="map_creation",
    )
    text_capture.add_text(
        f"OSM-Only: {stats['osm_only_plants']:,} plants ({stats['osm_only_capacity']:,.0f} MW)",
        step="map_creation",
    )
    text_capture.add_text(
        f"OSM-Mixed: {stats['osm_mixed_plants']:,} plants ({stats['osm_mixed_capacity']:,.0f} MW)",
        step="map_creation",
    )

    # Update layout with dropdown menu
    fig.update_layout(
        map=dict(
            style="open-street-map",
            center=dict(lat=54, lon=10),  # Center on Europe
            zoom=2.5,
        ),
        font=dict(size=12),
        width=780,  # Reduced width to fit iframe
        height=500,  # Reduced height to fit iframe
        margin=dict(l=5, r=5, t=5, b=5),  # Tight margins
        legend=dict(
            yanchor="top",
            y=0.95,
            xanchor="center",
            x=0.90,
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="Black",
            borderwidth=1,
        ),
        updatemenus=[
            dict(
                buttons=list(
                    [
                        dict(
                            label="OSM Involvement Comparison",
                            method="update",
                            args=[
                                {
                                    "visible": create_visibility_array(
                                        osm_status_start, osm_status_count, total_traces
                                    )
                                }
                            ],
                        ),
                        dict(
                            label="OSM-Only Plants",
                            method="update",
                            args=[
                                {
                                    "visible": create_visibility_array(
                                        osm_only_start, osm_only_count, total_traces
                                    )
                                }
                            ],
                        ),
                        dict(
                            label="Reference - By Technology",
                            method="update",
                            args=[
                                {
                                    "visible": create_visibility_array(
                                        ref_tech_start, ref_tech_count, total_traces
                                    )
                                }
                            ],
                        ),
                        dict(
                            label="Combined - By Technology",
                            method="update",
                            args=[
                                {
                                    "visible": create_visibility_array(
                                        comb_tech_start, comb_tech_count, total_traces
                                    )
                                }
                            ],
                        ),
                        dict(
                            label="Reference - By Fuel",
                            method="update",
                            args=[
                                {
                                    "visible": create_visibility_array(
                                        ref_fuel_start, ref_fuel_count, total_traces
                                    )
                                }
                            ],
                        ),
                        dict(
                            label="Combined - By Fuel",
                            method="update",
                            args=[
                                {
                                    "visible": create_visibility_array(
                                        comb_fuel_start, comb_fuel_count, total_traces
                                    )
                                }
                            ],
                        ),
                    ]
                ),
                direction="down",
                showactive=True,
                x=0.01,
                xanchor="left",
                y=0.99,
                yanchor="top",
            ),
        ],
    )

    # Add annotation with statistics
    annotation_text = f"""
    <b>Dataset Summary:</b><br>
    Reference: {stats["reference_plants"]:,} plants ({stats["reference_capacity"]:,.0f} MW)<br>
    Combined: {stats["combined_plants"]:,} plants ({stats["combined_capacity"]:,.0f} MW)<br>
    Added by OSM: {stats["plants_added"]:,} plants ({stats["capacity_added"]:,.0f} MW)<br>
    <br>
    <b>OSM Breakdown:</b><br>
    OSM-Only: {stats["osm_only_plants"]:,} plants ({stats["osm_only_capacity"]:,.0f} MW)<br>
    OSM-Mixed: {stats["osm_mixed_plants"]:,} plants ({stats["osm_mixed_capacity"]:,.0f} MW)<br>
    <br>
    <b>Size:</b> Marker size represents plant capacity
    """

    fig.add_annotation(
        x=0.01,
        y=0.01,
        xref="paper",
        yref="paper",
        text=annotation_text,
        showarrow=False,
        font=dict(size=10, color="black"),
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor="black",
        borderwidth=1,
    )

    # Save as HTML
    ensure_output_dir()
    output_file = OUTPUT_DIR / "europe_power_plants_osm_comparison.html"
    fig.write_html(output_file, config={"scrollZoom": True})

    text_capture.add_text(
        f"Interactive map saved as: {output_file}", step="map_creation"
    )
    text_capture.add_text("Map Features:", step="map_creation")
    text_capture.add_text(
        "- Use dropdown menu to switch between views", step="map_creation"
    )
    text_capture.add_text(
        "- Legend on right side shows current view categories", step="map_creation"
    )
    text_capture.add_text("- Zoom and pan to explore regions", step="map_creation")
    text_capture.add_text(
        "- Hover over plants for detailed information", step="map_creation"
    )
    text_capture.add_text(
        "- Compare reference vs combined datasets", step="map_creation"
    )
    text_capture.add_text("- Identify OSM-only contributions", step="map_creation")

    return fig


def main():
    """Main function that orchestrates the entire analysis with text capture."""

    # Initialize text capture
    text_capture = TextCapture()

    text_capture.add_header("PPM OSM ANALYSIS", level=1)
    text_capture.add_text("This script combines all PPM OSM analysis functionality:")
    text_capture.add_text("1. Dataset creation (with and without OSM)")
    text_capture.add_text("2. Corrected analysis with visualizations")
    text_capture.add_text("3. Interactive Europe map generation")
    text_capture.add_text("4. Detailed separated OSM analysis")

    create_dataset = (
        input("Create datasets from scratch? (y/n): ").strip().lower() == "y"
    )

    try:
        if create_dataset:
            # Step 1a: Create new datasets
            dataframes = create_powerplant_datasets(text_capture)
        else:
            # Step 1b: Load existing datasets
            dataframes = load_powerplant_datasets(text_capture)

        # Step 2: Prepare data for analysis
        df_ref, df_combined, osm_only, osm_mixed = prepare_data_for_analysis(
            dataframes, text_capture
        )

        # Step 3: Generate analysis report and visualizations
        stats = generate_corrected_report(df_ref, df_combined, text_capture)

        # Create visualizations
        text_capture.add_section("Creating visualizations", "", step="visualizations")
        create_osm_involvement_chart(df_ref, df_combined, text_capture)
        create_osm_only_analysis(osm_only, text_capture)
        create_summary_visualization(df_combined, text_capture)

        # Step 4: Create interactive map
        create_europe_map(df_ref, df_combined, osm_only, osm_mixed, text_capture)

        # Step 5: Detailed OSM analysis
        separated_osm_analysis(df_combined, text_capture)

        # Final summary
        text_capture.add_header(
            "ANALYSIS COMPLETE - FILES CREATED", level=2, step="summary"
        )
        text_capture.add_text(
            f"1. {OUTPUT_DIR / 'powerplants_eu_reduced_with_osm.csv'} - Combined dataset",
            step="summary",
        )
        text_capture.add_text(
            f"2. {OUTPUT_DIR / 'powerplants_eu_reduced_without_osm.csv'} - Reference dataset",
            step="summary",
        )
        text_capture.add_text(
            f"3. {OUTPUT_DIR / 'osm_involvement_comparison.png'} - OSM involvement pie charts",
            step="summary",
        )
        text_capture.add_text(
            f"4. {OUTPUT_DIR / 'osm_only_plants_analysis.png'} - OSM-only plants breakdown",
            step="summary",
        )
        text_capture.add_text(
            f"5. {OUTPUT_DIR / 'technology_osm_summary.png'} - Technology OSM coverage summary",
            step="summary",
        )
        text_capture.add_text(
            f"6. {OUTPUT_DIR / 'europe_power_plants_osm_comparison.html'} - Interactive map",
            step="summary",
        )

        text_capture.add_section("Key insights", "", step="summary")
        text_capture.add_text(
            f"- OSM contributes to {(stats['osm_only_plants'] + stats['osm_mixed_plants']) / stats['total_plants'] * 100:.1f}% of plants",
            step="summary",
        )
        text_capture.add_text(
            f"- OSM capacity share: {(stats['osm_only_capacity'] + stats['osm_mixed_capacity']) / stats['total_capacity'] * 100:.1f}%",
            step="summary",
        )

        # Save all captured text to file
        report_filename = text_capture.save_to_file("osm_analysis_complete_report.txt")
        text_capture.add_text(
            f"7. {report_filename} - Complete text analysis report", step="summary"
        )

        print(f"Analysis complete! All text output saved to: {report_filename}")
        print("Check the report file for detailed analysis results.")

        return text_capture, stats

    except Exception as e:
        error_msg = f"Error during analysis: {e}"
        text_capture.add_text(error_msg, step="errors")
        text_capture.save_to_file("osm_analysis_error_report.txt")
        print(error_msg)
        raise


if __name__ == "__main__":
    text_capture, stats = main()

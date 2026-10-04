# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""Rebuild the configured inventory using an isolated cache and record validation."""

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from powerplantmatching.collection import powerplants
from powerplantmatching.core import _data_out, get_config, package_config


def prepare_cache(
    source: Path, output: Path, jrc_archive: Path | None
) -> dict[str, Any]:
    """Read existing raw archives through symlinks while isolating new outputs."""
    runtime = output / "cache"
    incoming = runtime / "data/in"
    incoming.mkdir(parents=True, exist_ok=True)
    for archive in source.iterdir():
        destination = incoming / archive.name
        if archive.is_file() and not destination.exists():
            destination.symlink_to(archive.resolve())
    package_config["data_dir"] = str(runtime)
    config = get_config()
    if jrc_archive is not None:
        destination = incoming / config["JRC_PPDB_OPEN"]["fn"]
        if not destination.exists():
            destination.symlink_to(jrc_archive.resolve())
    return config


def validate_inventory(plants: pd.DataFrame) -> dict[str, Any]:
    """Check the independent identity, location and capacity contracts."""
    if not plants.index.is_unique:
        raise ValueError("Built inventory has duplicate indexes")
    canonical = plants.EIC.map(
        lambda codes: (
            isinstance(codes, list)
            and all(isinstance(code, str) and bool(code) for code in codes)
            and codes == sorted(set(codes))
        )
    )
    if not canonical.all():
        raise ValueError(
            f"Noncanonical EIC collections in {int((~canonical).sum())} rows"
        )
    known_capacity = plants.Capacity.dropna()
    if not known_capacity.ge(0).all():
        raise ValueError("Built inventory contains negative capacities")
    complete = plants[["lat", "lon"]].notna().all(axis=1)
    provenance = plants.GeopositionSource.map(
        lambda sources: isinstance(sources, list) and bool(sources)
    )
    if not provenance[complete].all():
        raise ValueError("Complete coordinate pairs lack source provenance")
    jrc = plants.GeopositionSource.explode().str.startswith("JRC_PPDB_OPEN@", na=False)
    jrc = jrc.groupby(level=0).any()
    return {
        "rows": len(plants),
        "capacity_gw": float(plants.Capacity.sum() / 1000),
        "rows_with_unknown_capacity": int(plants.Capacity.isna().sum()),
        "all_capacities_known": bool(plants.Capacity.notna().all()),
        "validation_status": "unknown_capacity"
        if plants.Capacity.isna().any()
        else "passed",
        "rows_with_eic": int(plants.EIC.map(bool).sum()),
        "rows_with_complete_coordinates": int(complete.sum()),
        "rows_with_jrc_coordinate_provenance": int(jrc.sum()),
        "countries": int(plants.Country.nunique()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-cache", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--jrc-archive", type=Path)
    parser.add_argument("--baseline-inventory", type=Path)
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()
    args.output_directory.mkdir(parents=True, exist_ok=True)
    config = prepare_cache(args.source_cache, args.output_directory, args.jrc_archive)
    cached = Path(_data_out("matched_data_red.csv", config))
    started = time.monotonic()
    sources = [
        next(iter(source)) if isinstance(source, dict) else source
        for source in config["matching_sources"]
    ]
    print(
        f"Building configured inventory from {len(sources)} matching sources: {sources}",
        flush=True,
    )
    plants = powerplants(
        config=config,
        update=args.rebuild or not cached.exists(),
        fill_geopositions=False,
    )
    summary = validate_inventory(plants)
    summary.update(
        completed_at_utc=datetime.now(UTC).isoformat(),
        elapsed_seconds=round(time.monotonic() - started, 2),
        matching_sources=sources,
        fully_included_sources=config["fully_included_sources"],
        coordinate_enrichment=config["ENTSOE"]["coordinate_source"],
        general_geocoding_enabled=False,
        reused_cached_inventory=cached.exists() and not args.rebuild,
        inventory_scope="Configured source statuses include planned and retired assets; no operational-year filter",
    )
    export = plants.copy()
    export["projectID"] = export.projectID.map(
        lambda identifiers: json.dumps(identifiers, default=sorted, sort_keys=True)
    )
    export.to_parquet(args.output_directory / "validated-powerplant-inventory.parquet")
    plants.groupby(["Country", "Fueltype"]).agg(
        plants=("Capacity", "size"), capacity_mw=("Capacity", "sum")
    ).to_parquet(args.output_directory / "validated-country-fuel-capacities.parquet")
    export.loc[plants.Capacity.isna()].to_parquet(
        args.output_directory / "inventory-records-with-unknown-capacity.parquet"
    )
    if args.baseline_inventory is not None:
        baseline = pd.read_csv(args.baseline_inventory, index_col=0)
        previous = baseline.groupby(["Country", "Fueltype"]).agg(
            baseline_plants=("Capacity", "size"),
            baseline_capacity_mw=("Capacity", "sum"),
        )
        current = plants.groupby(["Country", "Fueltype"]).agg(
            current_plants=("Capacity", "size"), current_capacity_mw=("Capacity", "sum")
        )
        comparison = previous.join(current, how="outer")
        comparison["capacity_delta_mw"] = (
            comparison.current_capacity_mw - comparison.baseline_capacity_mw
        )
        comparison.to_parquet(
            args.output_directory / "previous-snapshot-country-fuel-comparison.parquet"
        )
        summary.update(
            baseline_rows=len(baseline),
            baseline_rows_with_unknown_capacity=int(baseline.Capacity.isna().sum()),
            snapshot_comparison_is_controlled=False,
            snapshot_comparison_note="The previous published snapshot may use different source vintages; differences cannot be attributed solely to matching changes",
        )
    (args.output_directory / "inventory-validation-summary.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()

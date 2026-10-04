# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

from pathlib import Path
from typing import Any
from zipfile import ZipFile

import numpy as np
import pandas as pd
import pytest

from powerplantmatching import core, data, heuristics
from powerplantmatching.collection import collect, powerplants

GAS_EIC = "49W000000000044-"
COAL_EIC = "49W000000000066Q"


def write_archive(path: Path, units: pd.DataFrame) -> None:
    with ZipFile(path, "w") as archive:
        archive.writestr("JRC_OPEN_UNITS.csv", units.to_csv(index=False))


@pytest.fixture
def reference() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "eic_p": [GAS_EIC, GAS_EIC, GAS_EIC, COAL_EIC, COAL_EIC, "HYDRO"],
            "eic_g": ["G1", "G2", "G3", "C1", "C2", "H1"],
            "name_p": ["Eemshaven Gas"] * 3
            + ["Eemshaven Coal"] * 2
            + ["Linth Limmern"],
            "capacity_p": [1410.0] * 3 + [1580.0] * 2 + [100.0],
            "capacity_g": [470.0] * 3 + [790.0] * 2 + [100.0],
            "type_g": ["Fossil Gas"] * 3
            + ["Fossil Hard coal"] * 2
            + ["Hydro Pumped Storage"],
            "lat": [53.0] * 3 + [53.1] * 2 + [47.0],
            "lon": [6.0] * 3 + [6.1] * 2 + [9.0],
            "country": ["Netherlands"] * 5 + ["Switzerland"],
            "status_g": ["DECOMMISSIONED"] + ["COMMISSIONED"] * 5,
            "year_commissioned": [2000.0] * 6,
            "year_decommissioned": [2020.0] + [np.nan] * 5,
        }
    )


@pytest.fixture
def config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reference: pd.DataFrame
) -> dict[str, Any]:
    monkeypatch.setitem(core.package_config, "data_dir", str(tmp_path / "runtime"))
    (tmp_path / "runtime/data/in").mkdir(parents=True)
    archive = tmp_path / "jrc.zip"
    write_archive(archive, reference)
    result = core.get_config()
    result["JRC_PPDB_OPEN"] = {
        "url": str(archive),
        "fn": "jrc.zip",
        "reliability_score": 5,
        "aggregated_units": True,
        "version": "1.00",
    }
    return result


def test_jrc_loader_preserves_plant_capacity_and_unit_identifiers(
    config: dict[str, Any],
) -> None:
    plants = collect("JRC_PPDB_OPEN", config=config).set_index("Name")
    assert len(plants) == 3
    assert plants.loc["Eemshaven Gas", "Capacity"] == 1410.0
    assert plants.loc["Eemshaven Coal", "Capacity"] == 1580.0
    assert plants.loc["Eemshaven Gas", "EIC"] == sorted([GAS_EIC, "G1", "G2", "G3"])
    assert plants.loc["Eemshaven Gas", "Fueltype"] == "Natural Gas"
    assert plants.loc["Eemshaven Coal", "Fueltype"] == "Hard Coal"
    assert plants.loc["Linth Limmern", "Fueltype"] == "Hydro"
    assert plants.loc["Linth Limmern", "Technology"] == "Pumped Storage"
    assert pd.isna(plants.loc["Eemshaven Gas", "DateOut"])


def test_jrc_raw_input_is_unchanged(
    config: dict[str, Any], reference: pd.DataFrame
) -> None:
    pd.testing.assert_frame_equal(
        data.JRC_PPDB_OPEN(raw=True, config=config), reference
    )


def test_jrc_missing_plant_capacity_uses_generation_units(
    config: dict[str, Any], reference: pd.DataFrame
) -> None:
    reference.loc[reference.eic_p.eq(GAS_EIC), "capacity_p"] = np.nan
    write_archive(Path(config["JRC_PPDB_OPEN"]["url"]), reference)
    plants = data.JRC_PPDB_OPEN(update=True, config=config).set_index("Name")
    assert plants.loc["Eemshaven Gas", "Capacity"] == 1410.0


def test_jrc_enrichment_uses_both_identifier_levels_and_preserves_existing_points(
    reference: pd.DataFrame,
) -> None:
    plants = pd.DataFrame(
        {
            "EIC": [[GAS_EIC], ["C1"], [GAS_EIC]],
            "lat": [np.nan, np.nan, 52.0],
            "lon": [np.nan, np.nan, 5.0],
            "GeopositionSource": [[], [], ["Original Registry"]],
        },
        index=[10, 20, 30],
    )
    enriched = heuristics.fill_geopositions_by_eic(
        plants, reference, source="JRC_PPDB_OPEN@1.00"
    )
    assert enriched.index.equals(plants.index)
    assert enriched.loc[10, ["lat", "lon"]].tolist() == [53.0, 6.0]
    assert enriched.loc[20, ["lat", "lon"]].tolist() == [53.1, 6.1]
    assert enriched.loc[30, ["lat", "lon"]].tolist() == [52.0, 5.0]
    assert enriched.loc[10, "GeopositionSource"] == ["JRC_PPDB_OPEN@1.00:eic_p"]
    assert enriched.loc[20, "GeopositionSource"] == ["JRC_PPDB_OPEN@1.00:eic_g"]
    assert enriched.loc[30, "GeopositionSource"] == ["Original Registry"]


def test_jrc_ambiguous_locations_are_not_filled(reference: pd.DataFrame) -> None:
    reference.loc[1, "lat"] = 54.0
    plants = pd.DataFrame(
        {"EIC": [[GAS_EIC], ["G1"], [GAS_EIC, COAL_EIC]], "lat": np.nan, "lon": np.nan}
    )
    enriched = heuristics.fill_geopositions_by_eic(plants, reference, source="JRC")
    assert enriched.loc[[0, 2], ["lat", "lon"]].isna().all().all()
    assert enriched.loc[1, ["lat", "lon"]].tolist() == [53.0, 6.0]


def test_jrc_invalid_coordinates_are_not_filled(reference: pd.DataFrame) -> None:
    reference["lat"] = 95.0
    plants = pd.DataFrame({"EIC": [GAS_EIC], "lat": [np.nan], "lon": [np.nan]})
    enriched = heuristics.fill_geopositions_by_eic(plants, reference, source="JRC")
    assert enriched[["lat", "lon"]].isna().all().all()


def test_entsoe_uses_jrc_coordinates_without_replacing_capacity(
    config: dict[str, Any], tmp_path: Path
) -> None:
    archive = tmp_path / "entsoe.csv"
    pd.DataFrame(
        {
            "Name": ["Eemshaven Gas"],
            "Production Type": ["Fossil Gas"],
            "Installed Capacity [MW]": [1450.0],
        },
        index=[GAS_EIC],
    ).to_csv(archive)
    config["ENTSOE"].update(
        url=str(archive), fn="entsoe.csv", coordinate_source="JRC_PPDB_OPEN"
    )
    plants = data.ENTSOE(config=config)
    assert len(plants) == 1
    assert plants.Capacity.iloc[0] == 1450.0
    assert plants.EIC.iloc[0] == GAS_EIC
    assert plants[["lat", "lon"]].iloc[0].tolist() == [53.0, 6.0]
    assert plants.GeopositionSource.iloc[0] == ["JRC_PPDB_OPEN@1.00:eic_p"]


def test_eic_enrichment_survives_the_combined_pipeline_and_cache(
    config: dict[str, Any], tmp_path: Path
) -> None:
    archive = tmp_path / "entsoe.csv"
    pd.DataFrame(
        {
            "Name": ["Eemshaven Gas"],
            "Production Type": ["Fossil Gas"],
            "Installed Capacity [MW]": [1450.0],
        },
        index=[GAS_EIC],
    ).to_csv(archive)
    config["ENTSOE"].update(
        url=str(archive),
        fn="entsoe.csv",
        reliability_score=6,
        coordinate_source="JRC_PPDB_OPEN",
    )
    config["matching_sources"] = ["ENTSOE", "JRC_PPDB_OPEN"]
    config["fully_included_sources"] = []
    built = powerplants(config=config, update=True, fill_geopositions=False)
    cached = powerplants(config=config, fill_geopositions=False)
    assert len(built) == 1
    assert built.Capacity.iloc[0] == 1450.0
    assert built.EIC.iloc[0] == sorted([GAS_EIC, "G1", "G2", "G3"])
    assert built.GeopositionSource.iloc[0] == ["JRC_PPDB_OPEN@1.00:eic_p"]
    pd.testing.assert_frame_equal(cached, built, check_dtype=False)


@pytest.mark.parametrize("empty_reference", [False, True])
def test_missing_identifiers_or_reference_leave_locations_missing(
    reference: pd.DataFrame, empty_reference: bool
) -> None:
    plants = pd.DataFrame({"EIC": [None, [], np.nan, 42], "lat": np.nan, "lon": np.nan})
    if empty_reference:
        reference = reference.iloc[:0]
    enriched = heuristics.fill_geopositions_by_eic(plants, reference, source="JRC")
    assert enriched[["lat", "lon"]].isna().all().all()

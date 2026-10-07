# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

from zipfile import ZipFile

import pytest

import powerplantmatching as pm
from powerplantmatching import data

config = pm.get_config()
sources = [s if isinstance(s, str) else list(s)[0] for s in config["matching_sources"]]

if not config["entsoe_token"] and "ENTSOE" in sources:
    sources.remove("ENTSOE")


@pytest.mark.parametrize("source", sources)
def test_data_request_raw(source):
    func = getattr(data, source)
    df = func(update=True, raw=True)
    if source == "OPSD":
        assert len(df["DE"])
        assert len(df["EU"])
    elif source == "GEO":
        assert len(df["Units"])
        assert len(df["Plants"])
    else:
        assert len(df)


@pytest.mark.parametrize("source", sources)
def test_data_request_processed(source):
    func = getattr(data, source)
    df = func()
    assert len(df)
    assert df.columns.to_list() == config["target_columns"]


def test_OPSD_VRE():
    df = pm.data.OPSD_VRE()
    assert not df.empty
    assert df.Capacity.sum() > 0


def test_OPSD_VRE_country():
    df = pm.data.OPSD_VRE_country("DE")
    assert not df.empty
    assert df.Capacity.sum() > 0


def test_IRENASTAT():
    df = pm.data.IRENASTAT()
    assert not df.empty
    assert df.Capacity.sum() > 0


@pytest.mark.github_actions
def test_url_retrieval():
    pm.powerplants(from_url=True)


def test_reduced_retrieval():
    config = pm.get_config()
    config["matching_sources"] = ["GEO", "GPD"]
    config["fully_included_sources"] = []
    pm.powerplants(reduced=False, config=config)


def test_jrc_open_linkages_drops_british_gpd_ids(tmp_path, monkeypatch):
    fn = tmp_path / "JRC-PPDB-OPEN.ver1.0.zip"
    with ZipFile(fn, "w") as file:
        file.writestr(
            "JRC_OPEN_LINKAGES.csv",
            "eic_p,eic_g,eprtr_facilityID,WRI_id,GEO_id,fresna_id\n"
            "P1,G1,,WRI1,45146,\n"
            "P2,G2,,GBR1000374,,\n",
        )
    monkeypatch.setattr(data, "get_raw_file", lambda *args, **kwargs: fn)

    df = data.JRC_OPEN_LINKAGES()

    assert df.columns.to_list() == ["EIC", "Source", "projectID"]
    rows = set(df.itertuples(index=False, name=None))
    assert {
        ("P1", "GPD", "WRI1"),
        ("G1", "GPD", "WRI1"),
        ("P1", "GEO", "GEO-45146"),
    } <= rows
    assert not df["projectID"].str.startswith("GBR").any()

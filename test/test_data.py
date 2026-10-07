# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import pandas as pd
import pytest
from deprecation import DeprecatedWarning

import powerplantmatching as pm
from powerplantmatching import data
from powerplantmatching.core import _data_out, package_config

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


@pytest.fixture
def cached_config(tmp_path, monkeypatch):
    monkeypatch.setitem(package_config, "data_dir", tmp_path)
    config = pm.get_config()
    columns = config["target_columns"]
    fn = _data_out("matched_data_red.csv", config)
    plant = {"Name": ["A"], "Fueltype": ["Hydro"], "EIC": ["{}"], "projectID": ["{}"]}
    pd.DataFrame(plant, columns=columns).to_csv(fn)
    config["matched_data_url"] = fn
    vre = pd.DataFrame({"Name": ["B"], "Fueltype": ["Solar"]}, columns=columns)
    monkeypatch.setattr(data, "OPSD_VRE", lambda config: vre)
    return config


@pytest.mark.parametrize("from_url", [False, True])
def test_extend_by_vres_deprecated(cached_config, from_url):
    with pytest.warns(FutureWarning, match="extend_by_vres"):
        with pytest.warns(DeprecatedWarning, match="extend_by_VRE"):
            df = pm.powerplants(
                config=cached_config, from_url=from_url, extend_by_vres=True
            )
    assert df.Fueltype.tolist() == ["Hydro", "Solar"]


def test_extendby_kwargs_deprecated(cached_config):
    with pytest.warns(FutureWarning, match="extendby_kwargs"):
        pm.powerplants(config=cached_config, extendby_kwargs={"query": None})

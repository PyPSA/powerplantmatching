# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import hashlib
import os
from pathlib import Path

import pandas as pd
import pytest
from deprecation import DeprecatedWarning

import powerplantmatching as pm
from powerplantmatching import data
from powerplantmatching.core import _data_in, _data_out, package_config
from powerplantmatching.utils import get_name

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


TAG_URL = (
    "https://raw.githubusercontent.com/PyPSA/powerplantmatching/v0.9.0/powerplants.csv"
)


@pytest.mark.github_actions
def test_url_retrieval(tmp_path, monkeypatch):
    monkeypatch.setitem(package_config, "data_dir", tmp_path)
    url = config["matched_data_url"].format(tag=f"v{pm.latest_release}")
    assert len(pm.powerplants(from_url=True))
    md5 = hashlib.md5(url.encode(), usedforsecurity=False).hexdigest()
    cache = _data_in(os.path.join("releases", md5 + ".csv"))
    assert os.listdir(_data_in("releases")) == [os.path.basename(cache)]

    # update=True replaces a stale cache
    pd.read_csv(cache, index_col=0).head(1).to_csv(cache)
    assert len(pm.powerplants(from_url=True, update=True)) > 1


def test_reduced_retrieval():
    config = pm.get_config()
    config["matching_sources"] = ["GEO", "GPD"]
    config["fully_included_sources"] = []
    pm.powerplants(reduced=False, config=config)


PLANT = {"Name": ["A"], "Fueltype": ["Hydro"], "EIC": ["{}"], "projectID": ["{}"]}


def seed_release_cache(url: str, columns: list[str]) -> None:
    # an existing cache file is read without a download
    md5 = hashlib.md5(url.encode(), usedforsecurity=False).hexdigest()
    os.makedirs(_data_in("releases"))
    pd.DataFrame(PLANT, columns=columns).to_csv(
        _data_in(os.path.join("releases", md5 + ".csv"))
    )


@pytest.fixture
def cached_config(tmp_path, monkeypatch):
    monkeypatch.setitem(package_config, "data_dir", tmp_path)
    config = pm.get_config()
    columns = config["target_columns"]
    fn = _data_out("matched_data_red.csv", config)
    pd.DataFrame(PLANT, columns=columns).to_csv(fn)
    url = config["matched_data_url"].format(tag=f"v{pm.latest_release}")
    seed_release_cache(url, columns)
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


@pytest.mark.parametrize("from_url", ["v0.9.0", TAG_URL])
def test_url_retrieval_cached(tmp_path, monkeypatch, from_url):
    # a release is read from its own cache and never stored as build output
    monkeypatch.setitem(package_config, "data_dir", tmp_path)
    config = pm.get_config()
    seed_release_cache(TAG_URL, config["target_columns"])

    assert pm.powerplants(config=config, from_url=from_url).Name.tolist() == ["A"]
    assert not os.path.exists(_data_out("matched_data_red.csv", config))


def test_url_retrieval_latest_stored(tmp_path, monkeypatch):
    # `from_url=True` also stores the release as build output
    monkeypatch.setitem(package_config, "data_dir", tmp_path)
    config = pm.get_config()
    url = config["matched_data_url"].format(tag=f"v{pm.latest_release}")
    seed_release_cache(url, config["target_columns"])

    pm.powerplants(config=config, from_url=True)
    assert os.path.exists(_data_out("matched_data_red.csv", config))
    assert pm.powerplants(config=config).Name.tolist() == ["A"]


@pytest.mark.parametrize(
    "from_url, config_update, error",
    [
        ("master", None, ValueError),
        ("powerplants.csv", None, ValueError),
        ("ftp://a.b/c.csv", None, ValueError),
        (Path("powerplants.csv"), None, TypeError),
        (True, {"matched_data_url": "{tag}/powerplants.csv"}, ValueError),
    ],
)
def test_url_retrieval_invalid(tmp_path, monkeypatch, from_url, config_update, error):
    # only release tags starting with "v" and http(s) URLs are accepted
    monkeypatch.setitem(package_config, "data_dir", tmp_path)
    monkeypatch.chdir(tmp_path)
    pd.DataFrame(PLANT).to_csv("powerplants.csv")
    with pytest.raises(error, match="from_url"):
        pm.powerplants(from_url=from_url, config_update=config_update)
    assert not os.path.exists(_data_in("releases"))


def test_gem_keeps_source_name(monkeypatch):
    for tracker in ("GBPT", "GGPT", "GCPT", "GGTPT", "GNPT", "GSPT", "GWPT", "GHPT"):
        frame = pd.DataFrame({"Capacity": [1.0]})
        frame.columns.name = tracker
        monkeypatch.setattr(data, tracker, lambda frame=frame, **kwargs: frame)
    assert get_name(data.GEM()) == "GEM"

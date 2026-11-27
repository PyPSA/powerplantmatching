# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import importlib.util
import shutil
from pathlib import Path

import pytest
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files

from powerplantmatching.changes import render_release

DOCS = Path(__file__).parents[1] / "docs"
SPEC = importlib.util.spec_from_file_location("hooks", DOCS / "hooks.py")
hooks = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(hooks)

TAGS = ["v0.9.1", "v0.10.0"]


@pytest.fixture
def config(tmp_path: Path) -> MkDocsConfig:
    docs = tmp_path / "docs"
    for tag in TAGS:
        shutil.copytree(DOCS / "data-changes" / "v0.9.1", docs / "data-changes" / tag)
    config = MkDocsConfig()
    config.load_dict(
        {
            "site_name": "test",
            "docs_dir": str(docs),
            "site_dir": str(tmp_path / "site"),
            "nav": [{"About": [{"Data changes": ["data-changes/index.md"]}]}],
        }
    )
    assert not config.validate()[1]
    return config


def test_on_config_nav_newest_first(config: MkDocsConfig) -> None:
    nav = hooks.on_config(config)["nav"]
    entries = [{tag: f"data-changes/{tag}/index.md"} for tag in reversed(TAGS)]
    assert nav == [{"About": [{"Data changes": ["data-changes/index.md", *entries]}]}]


def test_on_files_adds_release_pages(config: MkDocsConfig) -> None:
    config.plugins._current_plugin = "hooks"
    files = hooks.on_files(Files([]), config)
    assert {file.src_uri for file in files} == {
        f"data-changes/{tag}/{name}"
        for tag in TAGS
        for name in ["index.md", "map.html"]
    }


@pytest.mark.parametrize("folder", hooks.releases(DOCS), ids=lambda f: f.name)
def test_committed_releases_render(folder: Path) -> None:
    assert render_release(folder)["index.md"].startswith("# Data changes from ")

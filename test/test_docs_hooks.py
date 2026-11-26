# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import importlib.util
from pathlib import Path

import pytest
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import File, Files
from mkdocs.structure.pages import Page
from packaging.version import InvalidVersion

SPEC = importlib.util.spec_from_file_location(
    "hooks", Path(__file__).parents[1] / "docs" / "hooks.py"
)
hooks = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(hooks)

RELEASE_NOTES = """\
# Release notes

## [v0.10.0](https://example.org) (date)

* new

## [v0.9.1](https://example.org) (date)

* fix

## [v0.9.0](https://example.org) (date)

* old
"""


@pytest.fixture
def config(tmp_path: Path) -> MkDocsConfig:
    docs = tmp_path / "docs"
    # v0.10.0 compares to v0.9.0, not to the next release notes heading v0.9.1
    for previous, tag in [("v0.9.0", "v0.9.1"), ("v0.9.0", "v0.10.0")]:
        (docs / "data-changes" / tag).mkdir(parents=True)
        (docs / "data-changes" / tag / "index.md").write_text(
            f"# Data changes from {previous} to {tag}\n\n## Overview"
        )
    (docs / "data-changes" / "index.md").write_text("# Data\n\n<!-- releases -->")
    (docs / "release-notes.md").write_text(RELEASE_NOTES)
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


def markdown(config: MkDocsConfig, src_uri: str) -> str:
    file = File(src_uri, config.docs_dir, config.site_dir, config.use_directory_urls)
    text = Path(file.abs_src_path).read_text()
    return hooks.on_page_markdown(text, Page(None, file, config), config, Files([file]))


def test_on_files_adds_tables_script(config: MkDocsConfig) -> None:
    config.plugins._current_plugin = "hooks"  # set by MkDocs when it runs a hook
    files = hooks.on_files(Files([]), config)
    assert files.get_file_from_path("javascripts/tables.js").abs_src_path == str(
        hooks.TABLES_JS
    )
    assert hooks.TABLES_JS.is_file()


def test_on_config_nav_newest_first(config: MkDocsConfig) -> None:
    nav = hooks.on_config(config)["nav"]
    assert nav == [
        {
            "About": [
                {
                    "Data changes": [
                        "data-changes/index.md",
                        {"v0.10.0": "data-changes/v0.10.0/index.md"},
                        {"v0.9.1": "data-changes/v0.9.1/index.md"},
                    ]
                }
            ]
        }
    ]


def test_release_list(config: MkDocsConfig) -> None:
    text = markdown(config, "data-changes/index.md")
    assert text.endswith("- [v0.10.0](v0.10.0/index.md)\n- [v0.9.1](v0.9.1/index.md)")


@pytest.mark.parametrize(
    "previous, tag, following",
    [("v0.9.0", "v0.10.0", "* new"), ("v0.9.0", "v0.9.1", "* fix")],
)
def test_release_notes_buttons(
    config: MkDocsConfig, previous: str, tag: str, following: str
) -> None:
    text = markdown(config, "release-notes.md")
    button = (
        f"[Data changes from {previous} to {tag}](data-changes/{tag}/index.md)"
        '{ .md-button }\n{ style="text-align: center" }'
    )
    assert f") (date)\n\n{button}\n\n{following}" in text
    assert text.count(".md-button") == 2  # none for v0.9.0, it has no folder


def test_malformed_folder_raises(config: MkDocsConfig) -> None:
    (Path(config.docs_dir) / "data-changes" / "latest").mkdir()
    with pytest.raises(InvalidVersion):
        hooks.releases(config.docs_dir)


@pytest.mark.parametrize("title", ["# Report", "# Data changes from v0.9.0 to v0.9.2"])
def test_release_notes_mismatching_title_raises(
    config: MkDocsConfig, title: str
) -> None:
    (Path(config.docs_dir) / "data-changes" / "v0.9.1" / "index.md").write_text(title)
    with pytest.raises(
        ValueError, match="is not '# Data changes from vX.Y.Z to v0.9.1'"
    ):
        markdown(config, "release-notes.md")

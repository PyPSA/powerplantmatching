# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import importlib.util
import json
from pathlib import Path

import pytest
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import File, Files
from mkdocs.structure.pages import Page
from packaging.version import InvalidVersion
from test_changes import NEW, OLD

from powerplantmatching.changes import RELEASE, compare, render_release, write_tables

DOCS = Path(__file__).parents[1] / "docs"
SPEC = importlib.util.spec_from_file_location("hooks", DOCS / "hooks.py")
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

COMPARISON = compare(OLD, NEW, year=2020)


def write_release(docs: Path, previous: str, tag: str) -> Path:
    """A release folder as written by pm.changes.write_release_report."""
    folder = docs / "data-changes" / tag
    write_tables(COMPARISON, folder)
    release = {"old": previous, "new": tag, "year": COMPARISON.year}
    (folder / RELEASE).write_text(json.dumps(release))
    return folder


@pytest.fixture
def config(tmp_path: Path) -> MkDocsConfig:
    docs = tmp_path / "docs"
    # v0.10.0 compares to v0.9.0, not to the next release notes heading v0.9.1
    for previous, tag in [("v0.9.0", "v0.9.1"), ("v0.9.0", "v0.10.0")]:
        write_release(docs, previous, tag)
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


def on_files(config: MkDocsConfig) -> Files:
    config.plugins._current_plugin = "hooks"  # set by MkDocs when it runs a hook
    return hooks.on_files(Files([]), config)


def test_on_files_adds_tables_script(config: MkDocsConfig) -> None:
    files = on_files(config)
    assert files.get_file_from_path("javascripts/tables.js").abs_src_path == str(
        hooks.TABLES_JS
    )
    assert hooks.TABLES_JS.is_file()


def test_on_files_adds_release_pages(config: MkDocsConfig) -> None:
    files = on_files(config)
    for tag in ["v0.9.1", "v0.10.0"]:
        rendered = render_release(Path(config.docs_dir) / "data-changes" / tag)
        assert {"index.md", "map.html", "overview/total.html"} < set(rendered)
        generated = {
            file.src_uri: file.content_string
            for file in files
            if file.src_uri.startswith(f"data-changes/{tag}/")
        }
        assert generated == {f"data-changes/{tag}/{p}": t for p, t in rendered.items()}
        title = generated[f"data-changes/{tag}/index.md"].split("\n", 1)[0]
        assert title == f"# Data changes from v0.9.0 to {tag}"


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


def test_folder_without_metadata_is_no_release(config: MkDocsConfig) -> None:
    write_tables(COMPARISON, Path(config.docs_dir) / "data-changes" / "v0.11.0")
    assert hooks.releases(config.docs_dir) == ["v0.10.0", "v0.9.1"]


def test_malformed_folder_raises(config: MkDocsConfig) -> None:
    (Path(config.docs_dir) / "data-changes" / "latest").mkdir()
    with pytest.raises(InvalidVersion):
        hooks.releases(config.docs_dir)


def test_metadata_of_other_release_raises(config: MkDocsConfig) -> None:
    write_release(Path(config.docs_dir), "v0.9.0", "v0.9.2").rename(
        Path(config.docs_dir) / "data-changes" / "v0.9.3"
    )
    with pytest.raises(ValueError, match="new release 'v0.9.2' is not the folder"):
        hooks.releases(config.docs_dir)


@pytest.mark.parametrize("tag", hooks.releases(DOCS))
def test_committed_releases_render(tag: str) -> None:
    """The committed release folders still read with the current table schema."""
    assert "index.md" in render_release(DOCS / "data-changes" / tag)

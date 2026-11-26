# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""MkDocs hooks: the data-change release pages need no manual edits.

A release folder ``docs/data-changes/vX.Y.Z/`` holds the CSV tables and the
metadata ``release.json`` written by the release workflow. The hooks render
its page (index.md, figures and map) at build time with
``pm.changes.render_release``, so a layout change applies to all release
pages. They derive from the folders the nav entries, the release list on the
data-changes page and the buttons in the release notes. The previous release
of a page is read from its metadata, so the release workflow
(``git describe``) is its only source.
"""

import re
import tempfile
from pathlib import Path
from typing import Any

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import File, Files
from mkdocs.structure.pages import Page
from packaging.version import Version

from powerplantmatching.changes import RELEASE, read_release, render_release
from powerplantmatching.core import _package_data

TABLES_JS = Path(_package_data("tables.js"))
NAV_SECTION = "Data changes"
RELEASES_MARKER = "<!-- releases -->"
RELEASE_HEADING = re.compile(r"^## \[(v\d+\.\d+\.\d+)\].*$", re.MULTILINE)
# The rendered files of the release pages. Files on disk, not in memory, as
# the autolinks plugin needs the source path of each file. Outside docs_dir,
# so `mkdocs serve` does not rebuild on writing them. Removed on exit.
RENDERED = tempfile.TemporaryDirectory(prefix="ppm-data-changes-")
BUTTON = (
    "\n\n[Data changes from {previous} to {tag}](data-changes/{tag}/index.md)"
    '{{ .md-button }}\n{{ style="text-align: center" }}'
)


def _folder(docs_dir: str | Path, tag: str) -> Path:
    """Release folder of `tag`."""
    return Path(docs_dir) / "data-changes" / tag


def releases(docs_dir: str | Path) -> list[str]:
    """Release folders (with metadata) below data-changes, newest first.

    Raises on a folder name that is not a version and on metadata of another
    release than its folder.
    """
    folders = (Path(docs_dir) / "data-changes").glob("*/")
    # Version() raises InvalidVersion for any other folder name
    found = {Version(folder.name): folder for folder in folders if folder.is_dir()}
    tags = []
    for version in sorted(found, reverse=True):
        folder = found[version]
        if not (folder / RELEASE).is_file():
            continue
        if (new := read_release(folder)["new"]) != folder.name:
            raise ValueError(
                f"{folder / RELEASE}: new release {new!r} is not the folder name"
            )
        tags.append(folder.name)
    return tags


def previous_release(docs_dir: str | Path, tag: str) -> str:
    """The old release of the page of `tag`, from its metadata."""
    return read_release(_folder(docs_dir, tag))["old"]


def on_files(files: Files, config: MkDocsConfig) -> Files:
    # The script lives in the package, where report.html inlines it too
    files.append(
        File.generated(config, "javascripts/tables.js", abs_src_path=str(TABLES_JS))
    )
    # The pages of the releases, their CSV files are served from docs_dir
    for tag in releases(config["docs_dir"]):
        for path, text in render_release(_folder(config["docs_dir"], tag)).items():
            src_uri = f"data-changes/{tag}/{path}"
            source = Path(RENDERED.name) / src_uri
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text(text, encoding="utf-8")
            files.append(File.generated(config, src_uri, abs_src_path=str(source)))
    return files


def _find_section(items: list[Any], title: str) -> list[Any] | None:
    """The entries of the nav section `title`, searched recursively."""
    for item in items:
        if not isinstance(item, dict):
            continue
        for key, value in item.items():
            if not isinstance(value, list):
                continue
            found = value if key == title else _find_section(value, title)
            if found is not None:
                return found
    return None


def on_config(config: MkDocsConfig) -> MkDocsConfig:
    # One nav entry per release below the data-changes index, newest first
    section = _find_section(config["nav"], NAV_SECTION)
    if section is None:
        raise ValueError(f"nav section {NAV_SECTION!r} not found")
    section.extend(
        {tag: f"data-changes/{tag}/index.md"} for tag in releases(config["docs_dir"])
    )
    return config


def on_page_markdown(
    markdown: str, page: Page, config: MkDocsConfig, files: Files
) -> str:
    tags = releases(config["docs_dir"])
    if page.file.src_uri == "data-changes/index.md":
        return markdown.replace(
            RELEASES_MARKER, "\n".join(f"- [{tag}]({tag}/index.md)" for tag in tags)
        )
    if page.file.src_uri != "release-notes.md":
        return markdown

    def add_button(match: re.Match[str]) -> str:
        tag = match.group(1)
        if tag not in tags:
            return match.group(0)
        previous = previous_release(config["docs_dir"], tag)
        return match.group(0) + BUTTON.format(previous=previous, tag=tag)

    return RELEASE_HEADING.sub(add_button, markdown)

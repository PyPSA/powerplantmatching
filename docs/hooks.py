# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""MkDocs hooks: the data-change release pages need no manual edits.

A release report is a folder ``docs/data-changes/vX.Y.Z/`` written by the
release workflow. The hooks derive from the folders the nav entries, the
release list on the data-changes page and the buttons in the release notes.
The previous release of a report is read from its title, so the release
workflow (``git describe``) is its only source.
"""

import re
from pathlib import Path
from typing import Any

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import File, Files
from mkdocs.structure.pages import Page
from packaging.version import Version

TABLES_JS = (
    Path(__file__).parents[1] / "powerplantmatching" / "package_data" / "tables.js"
)
NAV_SECTION = "Data changes"
RELEASES_MARKER = "<!-- releases -->"
RELEASE_HEADING = re.compile(r"^## \[(v\d+\.\d+\.\d+)\].*$", re.MULTILINE)
# H1 of a report as written by pm.changes.write_release_report
REPORT_TITLE = re.compile(r"# Data changes from (v\d+\.\d+\.\d+) to (v\d+\.\d+\.\d+)")
BUTTON = (
    "\n\n[Data changes from {previous} to {tag}](data-changes/{tag}/index.md)"
    '{{ .md-button }}\n{{ style="text-align: center" }}'
)


def releases(docs_dir: str | Path) -> list[str]:
    """Release folders with a report below data-changes, newest first.

    Raises on a folder name that is not a version.
    """
    folders = (Path(docs_dir) / "data-changes").glob("*/")
    # Version() raises InvalidVersion for any other folder name
    found = {Version(folder.name): folder for folder in folders if folder.is_dir()}
    return [
        found[version].name
        for version in sorted(found, reverse=True)
        if (found[version] / "index.md").is_file()
    ]


def previous_release(docs_dir: str | Path, tag: str) -> str:
    """The old release of the report of `tag`, from the first line of its index.md.

    Raises if that line is not the title of the report of `tag`.
    """
    index = Path(docs_dir) / "data-changes" / tag / "index.md"
    title = index.read_text().split("\n", 1)[0]
    match = REPORT_TITLE.fullmatch(title)
    if match is None or match[2] != tag:
        raise ValueError(
            f"{index}: first line {title!r} is not "
            f"'# Data changes from vX.Y.Z to {tag}'"
        )
    return match[1]


def on_files(files: Files, config: MkDocsConfig) -> Files:
    # The script lives in the package, where report.html inlines it too
    files.append(
        File.generated(config, "javascripts/tables.js", abs_src_path=str(TABLES_JS))
    )
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

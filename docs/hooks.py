# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

"""MkDocs hooks: render one page per release folder in docs/data-changes."""

import tempfile
from pathlib import Path
from typing import Any

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import File, Files
from packaging.version import Version

from powerplantmatching.changes import render_release

NAV_SECTION = "Data changes"
RENDERED = tempfile.TemporaryDirectory(prefix="ppm-data-changes-")


def releases(docs_dir: str | Path) -> list[Path]:
    """Release folders below data-changes, newest first."""
    folders = (Path(docs_dir) / "data-changes").glob("v*/")
    return sorted(folders, key=lambda folder: Version(folder.name), reverse=True)


def _section(items: list[Any], title: str) -> list[Any] | None:
    for item in items:
        for key, value in item.items() if isinstance(item, dict) else ():
            if key == title:
                return value
            if isinstance(value, list) and (found := _section(value, title)):
                return found
    return None


def on_config(config: MkDocsConfig) -> MkDocsConfig:
    section = _section(config["nav"], NAV_SECTION)
    if section is None:
        raise ValueError(f"nav section {NAV_SECTION!r} not found")
    for folder in releases(config["docs_dir"]):
        section.append({folder.name: f"data-changes/{folder.name}/index.md"})
    return config


def on_files(files: Files, config: MkDocsConfig) -> Files:
    for folder in releases(config["docs_dir"]):
        for name, text in render_release(folder).items():
            src_uri = f"data-changes/{folder.name}/{name}"
            source = Path(RENDERED.name) / src_uri
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text(text, encoding="utf-8")
            files.append(File.generated(config, src_uri, abs_src_path=str(source)))
    return files

# SPDX-FileCopyrightText: 2026 PyPSA Contributors
#
# SPDX-License-Identifier: MIT

"""Tests for IRENASTAT data processing."""

import logging

import pandas as pd

from powerplantmatching import data


def test_irenastat_preserves_regional_aggregates(tmp_path, monkeypatch, caplog):
    """Preserve regional aggregates without country conversion warnings."""
    raw = pd.DataFrame(
        {
            "Country/area": ["Japan", "Eurasia", "Oceania"],
            "Technology": ["Solar photovoltaic"] * 3,
            "Grid connection": ["On-grid"] * 3,
            "Year": [2023] * 3,
            "Electricity capacity statistics": [100.0, 200.0, 300.0],
        }
    )

    raw_file = tmp_path / "irena.csv"
    raw.to_csv(raw_file, index=False)

    monkeypatch.setattr(data, "get_raw_file", lambda *args, **kwargs: raw_file)

    with caplog.at_level(logging.WARNING):
        result = data.IRENASTAT()

    assert len(result) == 3
    assert set(result["Country"]) == {"Japan", "Eurasia", "Oceania"}

    capacities = result.set_index("Country")["Capacity"]
    assert capacities["Japan"] == 100.0
    assert capacities["Eurasia"] == 200.0
    assert capacities["Oceania"] == 300.0

    assert "Eurasia not found" not in caplog.text
    assert "Oceania not found" not in caplog.text

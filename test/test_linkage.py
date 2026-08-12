# SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import pytest

from powerplantmatching import linkage as lk

BASE = {
    "Name": "Alpha Power Station",
    "Fueltype": "Hard Coal",
    "Technology": "Steam Turbine",
    "Country": "Germany",
    "Capacity": 100.0,
    "lat": 51.0,
    "lon": 7.0,
}
KM_IN_DEGREES = 1 / 111.19


def record(**overrides: object) -> dict:
    return {**BASE, **overrides}


def frame(records: list[dict], index: list[int] | None = None) -> pd.DataFrame:
    return pd.DataFrame(records, index=index)


def scores_of(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    return lk.match([left, right], threshold=0.0)


@pytest.fixture
def left() -> pd.DataFrame:
    return frame(
        [
            record(),
            record(
                Name="Beta Hydro", Fueltype="Hydro", Capacity=40.0, lat=48.0, lon=11.0
            ),
        ]
    )


@pytest.fixture
def right(left: pd.DataFrame) -> pd.DataFrame:
    df = left.copy()
    df.loc[0, "Name"] = "Alpha Power Stn"
    df.loc[1, "Capacity"] = 41.0
    return df


def test_links_near_duplicates_and_reports_scores(left, right):
    out = lk.match([left, right], labels=["one", "two"])

    assert list(out.columns) == ["one", "two", "scores"]
    assert out["scores"].between(0, 1).all()
    assert set(zip(out["one"], out["two"])) == {(0, 0), (1, 1)}


@pytest.mark.parametrize(
    "overrides, linked",
    [
        pytest.param({"Name": "Alpha Power Stn"}, True, id="near-duplicate-name"),
        pytest.param(
            {
                "Name": "Gamma Nuclear",
                "Fueltype": "Nuclear",
                "Country": "France",
                "Capacity": 900.0,
                "lat": 45.0,
                "lon": 2.0,
            },
            False,
            id="unrelated-record",
        ),
        pytest.param(
            {"Name": "Alpha Works", "Fueltype": "Nuclear", "Country": "France"},
            False,
            id="fueltype-and-country-differ",
        ),
        pytest.param(
            {"Name": "Delta Works", "Capacity": 5000.0},
            True,
            id="capacity-differs-50x-at-identical-position",
        ),
        pytest.param(
            {"Name": "Alpha Works", "lat": 51.0 + 10 * KM_IN_DEGREES},
            False,
            id="10-km-apart",
        ),
    ],
)
def test_dissimilar_records_are_rejected(overrides, linked):
    out = lk.match([frame([record()]), frame([record(**overrides)])])

    assert out.empty != linked


@pytest.mark.parametrize(
    "first, second",
    [("Kozloduy 1", "Kozloduy 5"), ("Doel 1", "Doel 4"), ("Neurath", "Neurath F")],
)
def test_units_of_one_station_stay_separate(first, second):
    """Units share site, fueltype and technology -- only the designator separates them."""
    unit = record(Name=first, Capacity=440.0)
    other = record(Name=second, Capacity=1040.0)

    assert lk.match(frame([unit, other], index=[0, 1])).empty


def test_linkage_prefers_the_matching_unit():
    """Across sources the 1:1 reduction, not the threshold, resolves sibling units."""
    left = frame([record(Name="Kozloduy 1", Capacity=440.0)])
    right = frame(
        [
            record(Name="Kozloduy 5", Capacity=1040.0),
            record(Name="Kozloduy 1", Capacity=440.0),
        ]
    )

    out = lk.match([left, right], singlematch=True)

    assert list(out["two"]) == [1]


def test_name_matching_ignores_word_order():
    """Reordered names must still link, even 10 km apart -- token set, not plain ratio."""
    reordered = record(Name="Station Alpha Power", lat=51.0 + 10 * KM_IN_DEGREES)

    out = lk.match([frame([record()]), frame([reordered])])

    assert len(out) == 1


def test_field_specs_and_thresholds_are_pinned():
    """Tuned against the GEO/GPD harness in analysis/benchmark_linkage.py."""
    linkage = [(f.column, f.low, f.high) for f in lk.LINKAGE_FIELDS]
    dedup = [(f.column, f.low, f.high) for f in lk.DEDUP_FIELDS]

    assert linkage == [
        ("Name", 0.09, 0.99),
        ("Fueltype", 0.09, 0.7),
        ("Country", 0.0, 0.53),
        ("Capacity", 0.3, 0.75),
        ("geo", 0.1, 0.8),
    ]
    assert dedup == [
        ("Name", 0.09, 0.99),
        ("Fueltype", 0.05, 0.65),
        ("Technology", 0.25, 0.51),
        ("Country", 0.05, 0.51),
        ("Capacity", 0.49, 0.51),
        ("geo", 0.05, 0.75),
    ]
    assert (lk.LINKAGE_THRESHOLD, lk.DEDUP_THRESHOLD, lk.GEO_MAX_DISTANCE_M) == (
        0.85,
        0.96,
        5000.0,
    )


@pytest.mark.parametrize(
    "counterpart", [np.nan, "", "Hard Coal"], ids=["nan", "empty", "present"]
)
def test_missing_categorical_is_neutral_not_a_bonus(counterpart):
    """A field missing on one side must not score better or worse than any counterpart."""
    reference = scores_of(
        frame([record(Fueltype=np.nan)]), frame([record(Name="Alpha Power")])
    )

    out = scores_of(
        frame([record(Fueltype=np.nan)]),
        frame([record(Name="Alpha Power", Fueltype=counterpart)]),
    )

    assert out["scores"].to_list() == reference["scores"].to_list()


@pytest.mark.parametrize(
    "missing",
    [{"lat": np.nan, "lon": np.nan}, {"Capacity": np.nan}],
    ids=["geo", "capacity"],
)
def test_missing_numeric_fields_do_not_block_a_link(missing):
    out = lk.match(
        [frame([record(**missing)]), frame([record(Name="Alpha Power", **missing)])]
    )

    assert len(out) == 1


@pytest.mark.parametrize("singlematch, expected", [(True, 1), (False, 2)])
def test_singlematch_keeps_only_the_best_candidate(singlematch, expected):
    left = frame([record()])
    right = frame(
        [record(Capacity=130.0), record(Name="Alpha Power Stn")], index=[7, 8]
    )

    out = lk.match([left, right], singlematch=singlematch)

    assert len(out) == expected
    assert out.loc[out["scores"].idxmax(), "two"] == 7


def test_linkage_returns_index_labels_not_positions():
    left = frame(
        [record(), record(Name="Beta Hydro", Fueltype="Hydro", lat=48.0, lon=11.0)],
        index=[10, 20],
    )
    right = frame(
        [record(Name="Beta Hydro", Fueltype="Hydro", lat=48.0, lon=11.0), record()],
        index=[7, 8],
    )

    out = lk.match([left, right])

    assert set(zip(out["one"], out["two"])) == {(10, 8), (20, 7)}


def test_dedup_returns_index_labels_not_positions():
    df = frame(
        [
            record(),
            record(Name="Alpha Power Stn"),
            record(Name="Beta Hydro", Fueltype="Hydro", lat=48.0, lon=11.0),
        ],
        index=[10, 20, 30],
    )

    out = lk.match(df)

    assert set(zip(out["one"], out["two"])) == {(10, 20), (20, 10)}


@pytest.mark.parametrize(
    "kwargs", [{"showmatches": True}, {"singlmatch": True}, {"n_jobs": 2}]
)
def test_unknown_keyword_fails_fast(left, kwargs):
    with pytest.raises(TypeError):
        lk.match(left, **kwargs)


def test_geo_contributes_nothing_beyond_the_cutoff():
    near = scores_of(
        frame([record()]), frame([record(lat=51.0 + 4 * KM_IN_DEGREES)])
    ).at[0, "scores"]
    beyond = scores_of(
        frame([record()]), frame([record(lat=51.0 + 5.1 * KM_IN_DEGREES)])
    ).at[0, "scores"]
    far = scores_of(
        frame([record()]), frame([record(lat=51.0 + 50 * KM_IN_DEGREES)])
    ).at[0, "scores"]

    assert beyond == far
    assert beyond < near


def test_dedup_returns_reciprocal_pairs_for_duplicates_only():
    df = frame(
        [
            record(),
            record(),
            record(
                Name="Beta Hydro",
                Fueltype="Hydro",
                Technology="Run-Of-River",
                Capacity=40.0,
                lat=48.0,
                lon=11.0,
            ),
        ]
    )

    out = lk.match(df, labels=["one", "two"])

    assert list(out.columns) == ["one", "two"]
    forward = set(zip(out["one"], out["two"]))
    assert forward == {(0, 1), (1, 0)}


def test_dedup_ignores_technology_that_is_missing_on_both_sides():
    """aggregate_units fills missing strings with "" -- that must not count as agreement."""
    blank = frame(
        [
            record(Technology=""),
            record(
                Name="Delta Works",
                Technology="",
                Capacity=5000.0,
                lat=51.0 + 10 * KM_IN_DEGREES,
            ),
        ]
    )

    out = lk.match(blank)

    assert out.empty


@pytest.mark.parametrize("block_cells", [1, 3, 2_000_000])
def test_results_are_invariant_to_row_blocking(monkeypatch, block_cells):
    df = frame(
        [
            record(),
            record(Name="Alpha Power Stn"),
            record(Name="Alpha Power Station"),
            record(Name="Beta Hydro", Fueltype="Hydro", lat=48.0, lon=11.0),
        ],
        index=[10, 20, 30, 40],
    )
    dedup_reference, linkage_reference = lk.match(df), lk.match([df, df.iloc[::-1]])

    monkeypatch.setattr(lk, "BLOCK_CELLS", block_cells)

    assert lk.match(df).equals(dedup_reference)
    assert lk.match([df, df.iloc[::-1]]).equals(linkage_reference)


def test_empty_input_returns_empty_links(left):
    out = lk.match([left, left.iloc[0:0]], labels=["one", "two"])

    assert out.empty
    assert list(out.columns) == ["one", "two", "scores"]

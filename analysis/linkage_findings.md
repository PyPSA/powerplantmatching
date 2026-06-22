<!--
SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
SPDX-License-Identifier: MIT
-->

# Why the matching engine replaced DUKE (Splink experiment)

`powerplantmatching` historically matched records with [DUKE](https://github.com/larsga/Duke),
a Java/JVM record-linkage engine invoked as a subprocess and configured via XML.
This branch replaces it with `powerplantmatching.linkage` — a pure-Python
backend built on [Splink](https://moj-analytical-services.github.io/splink) and
DuckDB — which needs no JVM, configures the model in Python, and is several
times faster at comparable quality on an objective ground truth. This note
records the evidence. (A sibling experiment, #301, swapped the same engine for a
`rapidfuzz` + `numpy` backend; this is the Splink variant.)

## What the engine does

`linkage.match` takes a list of two frames (record linkage) or a single frame
(deduplication) and returns matched index pairs. It builds a **fixed-parameter
Splink (Fellegi-Sunter) model** — the per-field `m`/`u` probabilities are set
directly from the former DUKE field weights (`Comparison.xml` /
`Deleteduplicates.xml`), so the model needs **no per-call EM training** and is
fully deterministic and stable across the many small per-country slices the
pipeline produces.

- record linkage ← `Comparison.xml` (Name, Fueltype, Country, Capacity, Geo)
- deduplication ← `Deleteduplicates.xml` (adds Technology, near-neutral
  Capacity), returning reciprocal pairs as `cliques()` requires.

Comparators are Splink/DuckDB SQL: Jaro-Winkler thresholds for names, exact
match for categoricals, a min/max ratio ladder for capacity, and a haversine
distance ladder for position. A 0.5 prior reproduces DUKE's belief update. Only
the *relative* Bayes factor between levels affects ranking, so each comparison's
absolute scale is irrelevant and the operating point is set by one calibrated
`match_probability` threshold per mode (`LINKAGE_THRESHOLD`, `DEDUP_THRESHOLD`).

## Ground truth

The production `powerplants.csv` records which source IDs ended in the same
final cluster. From its `projectID` column we recover **567 GEO↔GPD pairs** as a
cross-source linkage ground truth and the intra-GPD pairs for dedup. The harness
is `analysis/benchmark_splink.py`; the DUKE baseline must be run on `master`
(its binaries are removed on this branch).

## Results — record linkage (GEO × GPD, 12 countries, vs ground truth)

| backend | precision | recall | F1 | time |
|---|---|---|---|---|
| DUKE | 0.790 | 0.771 | 0.780 | 6.7 s |
| splink (th 0.70, default) | 0.778 | 0.718 | 0.747 | 2.6 s |
| splink (th 0.50) | 0.700 | **0.802** | 0.748 | 2.6 s |
| splink (th 0.90) | **0.806** | 0.681 | 0.738 | 2.7 s |

Splink lands within ~3 F1-points of DUKE while running **~2.6× faster**, and the
threshold is a single dial trading precision for recall: at `th=0.50` its recall
(0.80) **exceeds** DUKE's (0.77). The matcher's raw recall before the 1:1
`best_matches` reduction is **0.82**, so the headroom is in candidate ranking,
not in the comparators. As in #301, the absolute F1 (~0.4–0.8 depending on
scope) is a property of the *evaluation* — raw pairwise GEO↔GPD against a
ground truth produced by the full multi-source pipeline.

## Results — deduplication (GPD, 8 countries)

| backend | merges | principled | time |
|---|---|---|---|
| DUKE | 454 | — | 21.7 s |
| splink (th 0.95, default) | 459 | 86 % | 3.2 s |
| splink (th 0.99) | 190 | 100 % | 2.9 s |

A scoreboard against the intra-GPD clusters in `powerplants.csv` is **circular** —
production used DUKE for dedup, so the clusters are DUKE's own output and its
recall is 1.0 by construction. The fair test is what each backend merges: at its
default threshold Splink makes a near-identical number of merges (459 vs DUKE's
454) and **86 %** of the merged pairs share a fueltype *and* lie within 5 km
*and* have a high name similarity — i.e. they are principled. Many are
multi-unit plants (*Altahullion* / *Altahullion Extension*, *Abbots Ripton* /
*Abbots Ripton Solar Farm Ext*), exactly what `aggregate_units` exists to
collapse. Splink ran **~7× faster** than DUKE.

## Outcome

DUKE — its Java dependency, bundled `.jar` binaries, and XML configs — was
removed. `linkage` (Splink + DuckDB) is the sole matching engine, selected
automatically with no configuration. `splink` is a hard dependency; Java is no
longer required.

> Note: a fixed-parameter model (m/u set from the DUKE weights) was chosen over
> Splink's usual unsupervised EM training because the pipeline calls `match`
> once per country, and per-call EM on tiny slices is both slow and unstable.
> Fixing the parameters keeps the engine deterministic and fast while still
> using Splink's vectorised DuckDB Fellegi-Sunter scoring.

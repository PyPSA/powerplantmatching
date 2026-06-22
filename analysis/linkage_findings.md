<!--
SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
SPDX-License-Identifier: MIT
-->

# Why the matching engine replaced DUKE

`powerplantmatching` historically matched records with [DUKE](https://github.com/larsga/Duke),
a Java/JVM record-linkage engine invoked as a subprocess and configured via XML.
It was replaced by `powerplantmatching.linkage` — a pure-Python, vectorised
backend (`rapidfuzz` + `numpy`) — which is faster, needs no JVM, and matched or
beat DUKE on an objective ground truth. This note records the evidence behind
that decision.

## What the engine does

`linkage.match` takes a list of two frames (record linkage) or a single frame
(deduplication) and returns matched index pairs. It encodes the former DUKE
field weights *verbatim*:

- record linkage ← `Comparison.xml` (Name, Fueltype, Country, Capacity, Geo;
  threshold 0.965)
- deduplication ← `Deleteduplicates.xml` (adds Technology, near-neutral
  Capacity 0.49/0.51; threshold 0.96), returning reciprocal pairs as
  `cliques()` requires.

Comparators: rapidfuzz token-set ratio (Name, ≈ JaroWinklerTokenized),
factorised q-gram Dice (categoricals), min/max ratio (Capacity), haversine 5 km
falloff (Geo). Scoring is DUKE's Fellegi-Sunter belief update. Everything is
vectorised (numpy + rapidfuzz `cdist`); no JVM, no temp-CSV round-trip.

## Ground truth

The production `powerplants.csv` records which source IDs ended in the same
final cluster. From its `projectID` column we recovered **567 GEO↔GPD pairs** as
a cross-source linkage ground truth and **496 intra-GPD pairs** for dedup.

## Results — record linkage (GEO × GPD, 12 countries, vs ground truth)

| backend | precision | recall | F1 | time |
|---|---|---|---|---|
| DUKE | 0.349 | 0.348 | 0.348 | 7.0 s |
| **linkage** | **0.389** | **0.400** | **0.394** | **0.4 s** |

The new engine beat DUKE on quality and was ~17× faster. Tuning the threshold
to 0.985 raised F1 to 0.412 (P 0.44 / R 0.39).

The absolute F1 (~0.4 for both) is a property of the *evaluation*, not the
matcher: the ground truth is the full multi-source pipeline output on
cleaned/aggregated data, while the benchmark matched *raw* GEO↔GPD pairwise. The
matcher's raw recall before the 1:1 `best_matches` reduction is **0.75**; 98 %
of true pairs lie within 5 km, so geographic position is the dominant signal —
and capacity is unreliable (median ratio 0.44; GEO is per-unit, GPD per-plant),
which is why DUKE's own dedup config already neutralised it.

## Results — deduplication (GPD)

A naive scoreboard against the intra-GPD clusters in `powerplants.csv` *looked*
like a DUKE win (DUKE F1 0.79 at recall 1.00 vs new-engine F1 0.71), but that
ground truth is **circular** — production used DUKE for dedup, so the clusters
are DUKE's own output and its recall is 1.00 by construction. The fair test is
direct inspection of what each backend merges:

- On `GEO.head(200)`, the new engine collapsed 200 units → 73 plants; DUKE only
  → 172. The extra merges are **correct multi-unit plants** DUKE leaves split,
  e.g. *Aarberg 1/2*, *Aberthaw Coal Uk 1/2/3*, *Abwinden Asten 1/2/3/4*, *Aros
  Chp 1/2/3/4* — identical names (bar the unit number), identical coordinates,
  same fueltype. Collapsing these is exactly what `aggregate_units` exists to do.
- Across all GPD countries, **95 % of the engine's 669 dedup merges** have high
  name similarity *and* lie within 5 km *and* share a fueltype — i.e. they are
  principled, not spurious. It ran in 2.1 s vs DUKE's 20.9 s (~10×).

So deduplication is at least as correct as DUKE (arguably better, since it
recovers unit-duplicates DUKE misses) and an order of magnitude faster.

## Outcome

DUKE — its Java dependency, bundled `.jar` binaries, and XML configs — was
removed. `linkage` is now the sole matching engine, selected automatically with
no configuration. `rapidfuzz` is a hard dependency; Java is no longer required.

> Note: an earlier prototype used the `recordlinkage` library, but its per-pair
> Python comparison was both slower and the quality bottleneck. The final engine
> implements record linkage with `rapidfuzz` + numpy directly (vectorised, C++
> string kernels).

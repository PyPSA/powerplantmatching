<!--
SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
SPDX-License-Identifier: MIT
-->

# Why the matching engine replaced DUKE

`powerplantmatching` historically matched records with [DUKE](https://github.com/larsga/Duke),
a Java/JVM record-linkage engine invoked as a subprocess and configured via XML.
It was replaced by `powerplantmatching.linkage` — a pure-Python, vectorised
backend (`rapidfuzz` + `numpy`) — which needs no JVM and is an order of magnitude
faster. This note records the evidence, and is explicit about what the new engine
is *not* and about which of its numbers are unverified.

## This is not a reimplementation of DUKE

`linkage` keeps DUKE's overall shape — per-field similarity mapped onto
`[low, high]` probability bounds, combined by a Fellegi-Sunter belief update over
a 0.5 prior — but the scoring function is **different**. Comparing against
historical DUKE results, these divergences matter:

| | DUKE | `linkage` |
|---|---|---|
| similarity → probability | `sim >= 0.5 ? 0.5 + (high-0.5)*sim² : low` (quadratic, hard floor below 0.5) | `low + sim*(high-low)` (linear ramp) |
| geoposition | `(1 - d/maxdist)*0.5 + 0.5` in `[0.5, 1]`, 0 beyond `maxdist` — i.e. always positive evidence | `clip(1 - d/maxdist, 0, None)` in `[0, 1]` |
| q-gram | overlap coefficient, `|A∩B| / min(|A|,|B|)` | Dice, `2|A∩B| / (|A|+|B|)` |
| numeric, both values 0 | 1.0 | 1.0 (matched after the port initially returned 0.0) |

Measured on the GEO×GPD harness, ~10 % of accepted linkage pairs differ between
DUKE's scoring and this one. Neither the field weights nor the thresholds
therefore transfer, and none of them are inherited on faith: see below.

Name comparison is `rapidfuzz.fuzz.token_set_ratio`, **not** an approximation of
DUKE's `JaroWinklerTokenized` — a different algorithm family with a consequential
property: it returns exactly 100 whenever one token set is a subset of the other,
so *"revin pump 1"* vs *"revin"* scores a perfect match. That is the mechanism
behind the aggressive unit collapsing described under deduplication.

## Ground truth

The production `powerplants.csv` records which source IDs ended in the same final
cluster. From its `projectID` column we recover **567 GEO↔GPD pairs** as a
cross-source linkage ground truth (`analysis/benchmark_linkage.py`). No dedup
target is built from it — see "Deduplication" below for why.

It is not correctness: it is DUKE-era pipeline output. Agreement with it measures
agreement with DUKE-era clusters, so any *precision* cost reported below is an
upper bound.

## Re-tuning the thresholds

Because the scoring curve changed, the inherited constants had no justification.
They were re-tuned against the ground truth with a held-out protocol: the 24
harness countries were sorted by ground-truth pair count (descending, ties broken
alphabetically) and dealt alternately into two folds of 12 — fold A the even
positions (337 pairs), fold B the odd (230) — then the threshold was fitted on
one fold by sweeping 0.50–0.995 in steps of 0.005 and evaluated on the other.

- tune on A → `t* = 0.73` → held-out B **F1 0.845** vs 0.799 at the old 0.965
- tune on B → `t* = 0.86` → held-out A **F1 0.907** vs 0.875 at the old 0.965
- pooled held-out (each fold scored at the threshold fitted on the *other*):
  **F1 0.881 / P 0.915 / R 0.850**, against 0.965's F1 0.844 / P 0.934 / R 0.771

Both directions agree in sign and magnitude. The gain is pure recall
(0.771 → 0.847) for ~1.4 pp of precision. Shipped threshold is **0.85**: it lies
inside both folds' plateaus (0.75–0.86 within 0.003 F1), near its
higher-precision end, and is the more conservative move away from 0.965 — which
sat on the edge of the one genuine cliff. Field bounds were left as they are; no re-weighting
transferred across folds.

Full-harness confirmation at the shipped settings (all 567 pairs):

| threshold | precision | recall | F1 |
|---|---|---|---|
| 0.965 (old) | 0.934 | 0.771 | 0.844 |
| **0.85 (shipped)** | 0.920 | **0.847** | **0.882** |

## Earlier DUKE-vs-`linkage` comparison — historical, unreproduced

The original decision to drop DUKE was taken on an earlier evaluation setup that
reported:

| backend | precision | recall | F1 | time |
|---|---|---|---|---|
| DUKE | 0.349 | 0.348 | 0.348 | 7.0 s |
| `linkage` | 0.389 | 0.400 | 0.394 | 0.4 s |

**These numbers could not be reproduced.** `analysis/benchmark_linkage.py` — which
is bit-exact with the production path (identical pairs to
`compare_two_datasets`) — puts the same shipped config at P 0.934 / R 0.771 /
F1 0.844 on the same 567 pairs, and no production-faithful variant tried
(dropping the `matching_sources` queries, dropping preprocessing, row-level
rather than pair-level scoring) lands anywhere near 0.39. The "12 countries" the
original run cites matches nothing here either: 24 countries have rows in both
frames and 20 carry ground-truth pairs.

The DUKE row cannot be re-measured at all now that the jar is deleted. Treat this
table, and the derived claims that once accompanied it (~17× speedup, raw recall
0.75, "98 % of true pairs lie within 5 km", capacity median ratio 0.44), as
historical and unverified. The speed advantage is not in doubt; the quality
comparison is.

## Deduplication — not validated

A naive scoreboard against the intra-GPD clusters in `powerplants.csv` *looked*
like a DUKE win (DUKE F1 0.79 at recall 1.00 vs new-engine F1 0.71), but that
ground truth is **circular**: production used DUKE for dedup, so the clusters are
DUKE's own output and its recall is 1.00 by construction. No non-circular dedup
ground truth exists, so the dedup weights and threshold **could not be validated**
and were deliberately left untouched at their inherited values — under a scoring
curve they were not fitted for. This is a known, unresolved gap. The comparator
fixes (missing values neutral, `0` vs `0` capacity identical) shift dedup outcomes
slightly, and that shift is likewise unvalidated: GEO 1661 → 1655 and GPD
6640 → 6627 aggregated rows.

What could be inspected directly is what each backend merges:

- On `GEO.head(200)` the new engine collapses 200 units → 73 plants, DUKE only
  → 172. Many extra merges are genuine multi-unit plants DUKE leaves split (e.g.
  *Aarberg 1/2*, *Abwinden Asten 1/2/3/4*) — exactly what `aggregate_units` is for.
  But the collapsing is driven by `token_set_ratio`'s subset rule above, which
  fires on any name that is a prefix-plus-unit-suffix, so it is aggressive by
  construction rather than verified correct case by case.
- Across all GPD countries, 95 % of the engine's 669 dedup merges have high name
  similarity *and* lie within 5 km *and* share a fueltype. It ran in 2.1 s vs
  DUKE's 20.9 s (~10×).

## Outcome

DUKE — its Java dependency, bundled `.jar` binaries, and XML configs — was
removed. `linkage` is the sole matching engine, selected automatically with no
configuration. `rapidfuzz` is a hard dependency; Java is no longer required.

> Note: an earlier prototype used the `recordlinkage` library, but its per-pair
> Python comparison was both slower and the quality bottleneck. The final engine
> implements record linkage with `rapidfuzz` + numpy directly (vectorised, C++
> string kernels).

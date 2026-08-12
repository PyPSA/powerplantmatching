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

Name comparison went the other way: `rapidfuzz.fuzz.token_set_ratio` was tried
first and had to be abandoned. No character-level ratio can resolve a unit
designator — *"Doel 1"* vs *"Doel 4"* differs in one character out of six —
and `token_set_ratio` additionally returns exactly 100 whenever one token set is a
subset of the other, so *"Neurath"* vs *"Neurath F"* is a perfect match. Both
failures merge the units of a station into a single record; see "Deduplication".
The engine therefore aligns token by token like DUKE's `JaroWinklerTokenized`,
scoring the mismatched designator at 0:

| | `token_set_ratio` | mean best-token Jaro-Winkler |
|---|---|---|
| Kozloduy 1 / Kozloduy 5 | 0.90 | 0.50 |
| Neurath / Neurath F | 1.00 | 0.50 |
| Revin Pump 1 / Revin | 1.00 | 0.33 |
| Gravelines 1 / Gravelins 1 (typo) | 0.96 | 0.99 |

The similarity is the mean over one record's tokens of the best Jaro-Winkler
match in the other, divided by the longer token count. Reducing over the shared
token vocabulary rather than over record pairs keeps it as fast as the character
kernel it replaces (6.5 s for a 6000-row dedup, unchanged).

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

Swapping the name comparator moves this operating point (F1 0.856, P 0.948 /
R 0.780 at the same 0.85) and the sweep was repeated: the pooled optimum shifts to
0.675 for +0.008 F1, the two folds fit 0.675 and 0.605, and every held-out gain is
under 0.01 — inside the noise of 567 pairs. **0.85 is kept**, at the
higher-precision end of a flat region, because over-merging is the failure mode
with real cost (below). Lowering the Capacity bound to make a size mismatch veto a
merge was searched and rejected: F1 falls monotonically (0.856 at `low = 0.30`,
0.832 at 0.15) on both folds. DUKE's quadratic curve was re-tested against the new
comparator too, and remains worse than the linear ramp (F1 0.819 vs 0.864).

Note the direction of the bias: this ground truth is DUKE-era output, and the new
comparator is the DUKE-like one, so its scores here flatter it. The independent
check is the statistics comparison below.

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

The DUKE row was never re-measured on this harness. (DUKE itself is still
runnable: `git worktree` at the merge-base restores the jars, which is how the
baseline dataset under "Deduplication" was built.) Treat this
table, and the derived claims that once accompanied it (~17× speedup, raw recall
0.75, "98 % of true pairs lie within 5 km", capacity median ratio 0.44), as
historical and unverified. The speed advantage is not in doubt; the quality
comparison is.

## Deduplication — validated against statistics, not against pairs

A naive scoreboard against the intra-GPD clusters in `powerplants.csv` *looked*
like a DUKE win (DUKE F1 0.79 at recall 1.00 vs new-engine F1 0.71), but that
ground truth is **circular**: production used DUKE for dedup, so the clusters are
DUKE's own output and its recall is 1.00 by construction. No non-circular dedup
ground truth exists, so the dedup weights and threshold **could not be validated**
and were left at their inherited values. `DEDUP_FIELDS` and `DEDUP_THRESHOLD` are
byte-identical to the deleted `Deleteduplicates.xml`, so with the comparator now
matching DUKE's semantics they are back inside the design they were fitted for.

The gap was closed from the other side instead: **ENTSO-E installed capacity**
(`analysis/compare-with-entsoe-stats.py`) is an independent reference, and
aggregating away a station's operating units is visible in it. Summed over the
country × fueltype cells ENTSO-E reports, for 2025, excluding Wind/Solar (extended
separately) and Other (incomparable residual):

| dedup name comparator | signed gap | Σ&#124;gap&#124; | RMSE |
|---|---|---|---|
| DUKE (rebuilt from the merge-base, same inputs) | −17.6 GW | 104.2 GW | 1.234 |
| `token_set_ratio` | −53.2 GW | 123.2 GW | 1.511 |
| **mean best-token Jaro-Winkler (shipped)** | **−5.5 GW** | **101.9 GW** | **1.227** |

`token_set_ratio` collapsed each station's units into one record. Capacity was
conserved, but `AGGREGATION_FUNCTIONS["DateOut"] = "max"` skips NaN, so the merged
station inherited a retired unit's shutdown year and the whole site dropped out of
the operating fleet: Kozloduy 1–6 became one 3840 MW record retired in 2006, and
Belgium, Bulgaria and the Netherlands lost their entire operating nuclear
capacity. Nuclear was 20.3 GW short of the statistics; it is now 2.9 GW over,
against DUKE's 4.8 GW over.

Making `DateOut` aggregate to NaN whenever a unit has no shutdown year was tried
and **rejected**: sources omit `DateOut` for plants that are in fact closed, so it
resurrects them (Σ|gap| 101.9 → 121.1 GW, Polish hard coal 18.6 → 26.6 GW against
19.0 reported). `"max"` is the better estimator once units are no longer merged.

- Across all GPD countries, 95 % of the engine's dedup merges have high name
  similarity *and* lie within 5 km *and* share a fueltype.

## Outcome

DUKE — its Java dependency, bundled `.jar` binaries, and XML configs — was
removed. `linkage` is the sole matching engine, selected automatically with no
configuration. `rapidfuzz` is a hard dependency; Java is no longer required.

> Note: an earlier prototype used the `recordlinkage` library, but its per-pair
> Python comparison was both slower and the quality bottleneck. The final engine
> implements record linkage with `rapidfuzz` + numpy directly (vectorised, C++
> string kernels).

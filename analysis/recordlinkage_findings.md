<!--
SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
SPDX-License-Identifier: MIT
-->

# recordlinkage as a DUKE alternative — prototype findings

Goal: assess whether a pure-Python [`recordlinkage`](https://recordlinkage.readthedocs.io)
backend can replace the JVM-based DUKE engine for the horizontal matching step.

## What was built

- `powerplantmatching/duke_recordlinkage.py` — a `duke()`-signature-compatible
  record-linkage backend that mirrors `package_data/Comparison.xml`:
  - Name → Jaro-Winkler, Fueltype/Country → q-gram, Capacity → min/max ratio
    (DUKE `NumericComparator`), lat/lon → haversine with 5 km linear falloff
    (DUKE `GeopositionComparator`).
  - Scores via DUKE's Fellegi-Sunter belief update (0.5 prior, per-field
    low/high probability bounds from the XML), so the 0.965 threshold is
    nominally comparable. Returns the same `[label1, label2, scores]` links frame.
- `analysis/benchmark_recordlinkage.py` — runs both backends on real GEO×GPD
  country slices, reporting pair-level agreement (treating DUKE as reference)
  and a recordlinkage threshold sweep.

## Results (GEO × GPD, singlematch, 6 countries, threshold 0.965)

| metric | value |
|---|---|
| DUKE matches | 304 |
| recordlinkage matches | 253 |
| agreement (recall vs DUKE) | **0.70** |
| precision vs DUKE | **0.84** |
| wall-clock | DUKE 3.4 s vs RL 12.3 s |

Per-country agreement is high where names are clean (Germany/France/Poland:
Jaccard 0.75–1.0) and drops where DUKE's **tokenized** Jaro-Winkler wins on
word-order/whitespace differences (Spain/Italy: Jaccard ~0.3).

## Takeaways

1. **Feasible, JVM-free.** A faithful port reproduces ~70% of DUKE's matches
   and 84% precision out of the box — no Java, no temp-CSV round-trips.
2. **The gap is the comparators, not the framework.** DUKE's
   `JaroWinklerTokenized`/`QGramComparator` differ from recordlinkage's. A naive
   token-sort *hurt* (over-normalized Name, the highest-weighted field). Closing
   the gap needs a tokenized name comparator (e.g. rapidfuzz `token_set_ratio`).
3. **Hand-ported weights saturate.** Threshold tuning (0.90→0.99 sweep) barely
   moves results — most scores pile up near 1.0. The proper fix is to *train*
   the weights (recordlinkage ECM / supervised) on labelled pairs instead of
   reusing DUKE's hand-set low/high bounds.
4. **Speed is an implementation detail.** The prototype uses a full index and
   rebuilds a `Compare` per field per country. recordlinkage's blocking
   (`Block`/`SortedNeighbourhood` on Fueltype or a capacity band) would cut the
   pair count dramatically and is expected to beat DUKE's in-memory full compare.

## Recommended next steps

- Add a tokenized name comparator and re-benchmark.
- Replace hand-set bounds with ECM-trained weights using DUKE output (or the
  curated `powerplants.csv` projectID groupings) as labels.
- Add blocking and re-measure timing.
- Implement dedup mode (`Deleteduplicates.xml`) for `cleaning.aggregate_units`.

## Reproduce

DUKE needs Java on PATH. Then:

```
uv pip install recordlinkage
uv run python analysis/benchmark_recordlinkage.py
```

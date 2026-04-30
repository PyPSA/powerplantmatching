# OSM changeset draft — Maasstroom (Rijnmond 2)

**OSM object:** relation `power=plant` near 51.8901, 4.352 (Maasstroom Energie, adjacent to Rijnmond 1)

**Tag changes to apply:**
- `plant:output:electricity` → `426 MW` (was 428 MW)

Minor correction — 2 MW delta is within common rounding error, so the changeset comment must be extra clear about the source, otherwise it may be reverted as "noise."

## Changeset comment

<!-- TODO: 1 line, explicit source -->


## Source tag

- `source:plant:output:electricity` =

## Decision point

<!--
2 MW is within typical reporting variance. Options:
  (a) Skip this edit entirely — not worth a changeset
  (b) Bundle it with Rijnmond 1 in a single changeset covering "Maasvlakte gas cluster"
  (c) Solo changeset with very explicit source
-->

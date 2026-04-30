# OSM changeset draft — Hemweg coal unit retirement

**Context:** Hemwegcentrale, Amsterdam. The site historically contained multiple units:
- Hemweg 7 (gas, CCGT) — still operating
- Hemweg 8 (coal, ~650 MW) — closed Dec 2019 (Dutch coal phase-out), demolished 2024

**OSM objects:** search "Hemwegcentrale" in OSM. Likely either:
- One `power=plant` relation for the whole site (must not delete — Hemweg 7 still operates)
- Separate objects per unit (preferred — edit only the coal unit)

---

## Decision point — lifecycle prefix

OSM has three ways to represent a retired asset. You must pick one:

<!--
  (a) disused:power=plant   → structure still standing but not operating
  (b) abandoned:power=plant → structure standing, operator gone, no maintenance
  (c) demolished:power=plant → physically torn down, geometry represents former footprint
  (d) delete the object     → only if geometry is now used for something else

  Your commit says "torn down 2024". That points to (c) demolished.
  BUT: if the cooling tower or smokestack still stands, (a) disused is more accurate.
  Check recent aerial imagery in iD before deciding.
-->

My choice: <!-- TODO -->

## Tag changes

- Remove: `power=plant` (only if demolished)
- Add: `demolished:power=plant=yes` (or `disused:power=plant=yes`)
- Add: `end_date` = `2019` (last generation) or `2024` (physical demolition) — pick the semantically meaningful date for *this* tag
- Keep: name, operator (as historical record)

## Changeset comment

<!-- TODO -->

## Source tag

- `source:end_date` = <!-- e.g. Vattenfall press release, AT5 news article on demolition, aerial imagery date -->

## IMPORTANT

Do NOT edit anything on Hemweg 7 (the gas unit). Verify in iD that you're editing only the coal unit geometry — a wrong click here will retire an active plant in the database.

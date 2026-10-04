# Powerplantmatching PR review implementation tasks

Sources: [#306](https://github.com/PyPSA/powerplantmatching/pull/306), [#301](https://github.com/PyPSA/powerplantmatching/pull/301), [#289](https://github.com/PyPSA/powerplantmatching/pull/289).

## Tasks

- [x] Preserve EIC identifiers as sorted unique lists in unit aggregation and cross-source reduction. Accept scalar strings and existing collections at input boundaries. Verify missing values, repeated identifiers and cache serialization.
- [x] Replace the checkout's greedy EIC matching with isolated one-to-one links. Leave shared scheme identifiers to fuzzy matching. Verify subsets, lists, empty inputs and duplicate index handling.
- [x] Integrate the Python matching engine while preserving the exact-EIC pass. Make name scoring symmetric and verify matching does not depend on source direction or input order.
- [x] Repair the JRC loader and source configuration. Preserve plant capacity once per production unit, normalize fuel types and retain plant/generation identifier provenance. Verify against the actual archive and offline fixtures.
- [x] Add explicit EIC coordinate enrichment with source provenance. Keep historical JRC capacity out of default unmatched-plant inclusion until status handling and coverage are validated.
- [x] Validate the combined pipeline and document capacity, identity and geographic changes. Run focused offline tests before a cached full build. Keep benchmark agreement separate from independently verified plant identity.
- [ ] Prepare separate upstream changes for identifier preservation, matcher integration and JRC enrichment. Update existing user-owned PRs when ready; publication requires the user's instruction.

## Review evidence

- JRC repeats `capacity_p` on generation-unit rows. Summing it changes Eemshaven coal from 1580 to 3160 MW and the 1410 MW Eemshaven gas record to 4230 MW.
- The proposed JRC loader retains only 216 of its 2400 records after the standard country/fuel filter because it skips fuel normalization.
- PR #306 discards list-valued EIC identifiers produced by PR #301.
- PR #301 accepts a synthetic name pair in one source direction at 0.989 and rejects it reversed at 0.841, with a 0.85 threshold.
- Initial review checks: 35 focused tests for #301 and 13 for #289 passed. These were not a full dataset build.

## First implementation batch

- Branch: `codex/powerplantmatching-eic-and-linkage-review-fixes`.
- A shared EIC collector now preserves scalar and collected identifiers as sorted unique lists in unit aggregation and cross-source reduction. Missing identifiers become empty lists.
- Exact matching uses a shared-code join and accepts only degree-one pairs on both sides. Duplicate source indexes raise an explicit error before rows can be collapsed or removed incorrectly.
- Validation: 20 offline matching, cleaning and Java tests passed. Real unit aggregation and CSV cache reload preserve EICs, including empty collections.
- Baseline checks load the original functions in memory without reverting the checkout, proving that the identifier regressions fail before the repair.
- Network-backed source aggregation checks could not complete: ten source cases failed because the sandbox could not resolve download hosts. No full dataset build has been claimed.
- Concurrent GEM loader changes and their separate contribution task list are outside this batch and remain untouched.

## Python matcher integration

- Java matching, bundled jars, XML files and CI Java setup are removed. Pixi and package metadata declare rapidfuzz; Java is no longer a dependency.
- Name scoring uses the lower directional token total. Deduplication and linkage scores are symmetric while separate unit designators still contribute disagreement.
- SciPy sparse bipartite assignment maximizes the total accepted fuzzy score. Canonical source and identifier ordering makes tie resolution independent of source reversal and record order.
- Exact-EIC matching remains before the fuzzy engine. Duplicate source indexes are rejected in both paths.
- Four new regression cases failed against the incoming PR #301 engine. After the repairs, 53 offline linkage, matching and cleaning tests passed.
- The 0.85 linkage threshold is inherited from #301. Its published benchmark metrics do not validate this changed scoring and assignment rule. Real dataset comparison remains required.
- The full inventory comparison below checks integration and source coverage. Independent identity calibration remains outstanding.
- Rename custom `parallel_duke_processes` configuration to `parallel_processes`; obsolete keys raise an explicit error.

## JRC enrichment and full inventory validation

- The optional `JRC_PPDB_OPEN` loader normalizes fuel types, retains production and generation EICs, and counts reported production capacity once. It is excluded from default matching and fully included sources.
- Actual version 1.00 archive: 7117 generation rows become 2972 filtered production records. Eemshaven coal remains 1580 MW and gas remains 1410 MW. One conflicting production capacity is reported and the maximum reported value retained.
- ENTSOE enrichment uses exact EIC identifiers for missing coordinates only. Existing capacities and complete coordinate pairs remain unchanged. Ambiguous identifier coordinates remain missing. The archive contains 48 ambiguous EICs.
- Latitude, longitude and provenance travel together through aggregation and source reduction. CSV reload preserves EIC and provenance lists.
- Validation: 64 focused offline tests pass. The full configured 11-source build completes with 180064 records across 36 countries, including 49 records with JRC coordinate provenance. EIC collection, unique identity, coordinate provenance and nonnegative known-capacity checks pass.
- Capacity quality remains incomplete: 45 records have unknown capacity (35 legacy JRC, 9 EESI and 1 GEM). The previous published snapshot has 35. Unknown values remain visible and validation reports `unknown_capacity`.
- The previous snapshot contains 185296 records. Source vintages differ, so changes in rows and capacities cannot be attributed solely to the matcher. Configured inventories include planned and retired assets; totals are not an operational-year capacity estimate.
- The reproducible runner is `analysis/validate-eic-coordinate-enrichment.py`. Validation artifacts are stored locally in `outputs/2026-oct-04-eic-jrc-validation/`.
- Separate local commits are prepared. The branch contains earlier local history, so extracting changes onto current upstream remains required before publication. No remote PR has been updated.

# Powerplantmatching PR review implementation tasks

Sources: [#306](https://github.com/PyPSA/powerplantmatching/pull/306), [#301](https://github.com/PyPSA/powerplantmatching/pull/301), [#289](https://github.com/PyPSA/powerplantmatching/pull/289).

## Tasks

- [x] Preserve EIC identifiers as sorted unique lists in unit aggregation and cross-source reduction. Accept scalar strings and existing collections at input boundaries. Verify missing values, repeated identifiers and cache serialization.
- [x] Replace the checkout's greedy EIC matching with isolated one-to-one links. Leave shared scheme identifiers to fuzzy matching. Verify subsets, lists, empty inputs and duplicate index handling.
- [x] Integrate the Python matching engine while preserving the exact-EIC pass. Make name scoring symmetric and verify matching does not depend on source direction or input order.
- [ ] Repair the JRC loader and source configuration. Preserve plant capacity once per production unit, normalize fuel types and retain plant/generation identifier provenance. Verify against the actual archive and offline fixtures.
- [ ] Add explicit EIC coordinate enrichment with source provenance. Keep historical JRC capacity out of default unmatched-plant inclusion until status handling and coverage are validated.
- [ ] Validate the combined pipeline and document capacity, identity and geographic changes. Run focused offline tests before a cached full build. Keep benchmark agreement separate from independently verified plant identity.
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
- Rename custom `parallel_duke_processes` configuration to `parallel_processes`; obsolete keys raise an explicit error.

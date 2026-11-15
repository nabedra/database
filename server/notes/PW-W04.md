# PW-W04 table lifecycle integration

Implemented in the existing shared main checkout. No agents, branches, worktrees, commits, pushes, secrets, startup/runtime, AWS, acceptance runner or coordinator plan changes.

## Files owned by this assignment

- `server/src/engine.rs`: validate create schema through injected worker 0; publish a fresh ID with zero rows and an empty Partitioned manifest through one `publish_partitions(candidate, vec![])`. Remove empty legacy Parquet encoding/upload and both raw spawn_blocking calls. Drop uses the same partition-aware publisher with zero objects.
- `server/tests/engine_catalog.rs`: replace empty legacy snapshot expectation with v2 empty-manifest/catalog-only publication assertions; insert a real row before drop and decode its retained partition after drop/recreate; preserve validation, storage-error and stale-ID checks.
- `server/tests/engine_dispatch.rs`: add empty-table get/collect/count/explain assertions to the existing thirteen-operation wire workflow.
- `server/tests/engine_read.rs`: construct the genuine Legacy descriptor and initial entry directly through the existing commit interface. Subsequent legacy updates preserve legacy coverage; no partitioned table is downgraded. The captured-read test polls recreate to pending, then releases the gated worker before awaiting recreate, since create now shares worker 0.
- `server/tests/engine_write.rs`: seed the explicit legacy migration case directly through the commit interface; ordinary fixtures still use production create and now start partitioned.
- `server/tests/api.rs`: mechanically change the cancelled-create expected PUT total from three to two, initialization plus catalog publication.
- `server/notes/PW-W04.md`: this handoff.

## Evidence

All Cargo commands use existing `server/target`, `CARGO_BUILD_JOBS=2`, `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, and `--locked`. No parallel Cargo builds.

- Red: focused create regression failed on the old implementation, showing Legacy snapshot versus expected Partitioned empty files.
- Green: `cargo test --locked --manifest-path server/Cargo.toml --test engine_catalog --test engine_dispatch`: 3 catalog and 2 dispatch tests passed, no warnings.
- Full command run exactly once: `cargo test --locked --manifest-path server/Cargo.toml`. Library 5, API 4, CLI 4, commit 10, config 4, catalog 3 and dispatch 2 passed. The run hung in the captured legacy-read fixture because it gated worker 0 and awaited create validation on that same worker. Terminated only the hung test binary with SIGTERM; Cargo exited 101. Fixed the fixture scheduling as described above.
- Continued with only the repaired and unreached targets: `cargo test --locked --manifest-path server/Cargo.toml --test engine_read --test engine_write --test executor --test health --test mutation --test protocol --test query --test response --test runtime --test snapshot --test startup --test storage --test worker_pool`. Read 4, write 8 and executor 4 passed. Health had 2 passes and 1 unrelated failure; Cargo stopped there.
- Continued unreached targets with `cargo test --locked --manifest-path server/Cargo.toml --test mutation --test protocol --test query --test response --test runtime --test snapshot --test startup --test storage --test worker_pool`: mutation 6, protocol 7, query 5, response 3, runtime 2, snapshot 9, startup 1, storage 7 and worker_pool 2 passed. `cargo test --locked --manifest-path server/Cargo.toml --doc` passed with zero doctests.
- Final coverage across every unit/integration target: **92 passed, 1 unrelated health failure**, excluding repeated focused runs and the terminated read attempt. No compiler warnings. No second full-suite invocation or redundant successful-target reruns.
- Logs: `/tmp/meshdb-pw-w04-cargo-test.log`, `/tmp/meshdb-pw-w04-remaining-tests.log`, `/tmp/meshdb-pw-w04-final-targets.log`, `/tmp/meshdb-pw-w04-doc-tests.log`.
- Only owned Rust files formatted, with rustfmt edition 2024 and skip_children=true. `git diff --check` passed.

## Dispatch and worker audit

Reviewed all thirteen execute arms. Create validates schema on the injected pool. Describe/list/drop do small metadata coordination. Insert, insert_many, update, upsert and delete use worker validation and bounded partition preparation; get uses selected-file worker decoding; collect uses file workers and a worker reduction; count uses file workers and scalar coordination; explain uses metadata only. No new runtime or API was introduced.

`rg -n spawn_blocking server/src/engine.rs server/src/engine` returns no matches. Data decoding remains in partition load_file called by workers, and mutation encoding remains inside bounded map_indexed jobs. No Engine legacy encoder remains. The only production spawn_blocking call elsewhere is WorkerThreads teardown joining, outside this assignment. The executor module has an older documentation reference only.

## Remaining acceptance concerns

The remaining unrelated blocker is `server/tests/health.rs:104`, `readiness_reads_fresh_validated_catalogs_without_writes`. Its unsupported-catalog fixture sets format_version to 2 and expects 503; storage now supports version 2, so it returns 200. This test does not call create/drop and the mismatch follows the earlier catalog-version integration. Left untouched for coordinator/final checks.

No AWS or external acceptance run, benchmark, Clippy or repository-wide format pass performed, per assignment. Existing W02/W03 executor contract remains: admitted operations must be awaited through completion; Engine alone does not shield an abandoned operation future. This assignment preserves the existing validation/conflict/storage and ambiguous-publication behavior. Production commit checks still reject Partitioned-to-Legacy downgrades.

## Coordinator reconciliation

The health failure was a stale fixture from the catalog-version change, within this feature's integration scope. The coordinator changed its unsupported version from 2 to 3 and reran only `cargo test --locked --manifest-path server/Cargo.toml --test health readiness_reads_fresh_validated_catalogs_without_writes` with the same Cargo environment. It passed. Together with the prior successful targets, all 93 unit/integration tests and the zero doctests now pass. The coordinator additionally changed `server/tests/health.rs`; no production change was needed for this correction.

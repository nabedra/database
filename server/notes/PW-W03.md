# PW-W03 partition-aware row mutations

Implemented on the existing shared checkout without agents, branches, worktrees, commits, pushes, secrets, plan changes, or bootstrap/create/drop changes.

## Files

- `server/src/engine/write.rs`: replaces legacy whole-table mutation IO with captured-session routing, worker preparation, complete candidate accounting and one `publish_partitions` call.
- `server/src/engine/partition.rs`: extracts generic `map_indexed` from the reviewed bounded dispatcher; `map_files` delegates to it with unchanged read behavior.
- `server/src/engine/read.rs`: removes the now-unused legacy `load_rows` helper and its imports/error mapper only.
- `server/tests/engine_write.rs`: extends the workflow and no-op/conflict groups and adds legacy migration, gated worker, UTF-8 routing and aggregate accounting coverage.
- `server/notes/PW-W03.md`: this handoff.

Pure `engine/mutation.rs` rules remain unchanged.

## Behavior and APIs

The mutation coordinator retains the global session from fresh catalog resolution through input validation, every admitted worker's completion, candidate accounting and publication. Whole-request schema/type validation and batch duplicate checks complete in a shared worker before file preparation. Empty batches return Count(0) without file GET or PUT. Keyed mutations route to one original partition ordinal; batch rows group by that ordinal in request order. Empty manifests use one synthetic full-range job.

Each preparation worker uses its own runtime through the reviewed `load_file`, then calls pure `mutation::apply` and synchronous `snapshot::encode_partition_range` on the same dedicated native thread. There is no mutation spawn_blocking or nested submission. Unchanged legacy results retain the legacy object. Changed legacy results migrate the entire private candidate to partitions.

The generic helper is:

```rust
map_indexed<I, T, F>(
    pool: &WorkerPool,
    inputs: Vec<(usize, I)>,
    work: F,
) -> Result<Vec<T>, ProtocolError>
// I: Clone + Send + 'static
// T: Send + 'static
// F: Fn(I, &Runtime) -> Result<T, ProtocolError> + Send + Sync + 'static
```

Inputs are ascending original partition indices. Existing windows admit at most worker_count jobs, drain admitted siblings, choose the earliest original-index error and stop later windows. Reads retain their original map_files API.

The coordinator collects all encoded candidates before assigning unique current-session partition ordinal paths or uploading anything. It retains untouched object identities and physical row/payload/byte metadata. Codec splits keep the original range's outer bounds. Removing an empty file extends a neighboring routing bound without rewriting that neighbor; the last deleted row produces an empty manifest. It checks aggregate rows and decoded payload across replacement and retained metadata before one publication. Uploads remain sequential inside the reviewed commit implementation.

Results preserve null, batch original input count, and found booleans independently of changed status. Failed validation, conflict, preparation and aggregate accounting leave catalog and data objects untouched. Commit errors retain the reviewed publication outcome behavior.

## Evidence

All Cargo commands use existing server/target and CARGO_BUILD_JOBS=2, CARGO_PROFILE_DEV_DEBUG=0, CARGO_PROFILE_TEST_DEBUG=0. No parallel Cargo builds.

- Red: extended workflow failed with "changed mutations must publish partitions" on the original legacy-only write path.
- Targeted tests: `cargo test --manifest-path server/Cargo.toml --test engine_write --test mutation --test engine_read`: 8 engine_write, 6 mutation and 4 engine_read tests passed, without warnings.
- Compilation: `cargo check --manifest-path server/Cargo.toml --tests`: passed, without warnings.
- Owned Rust files formatted with edition 2024 and skip_children=true; whitespace checked with git diff --check.

The workflow uses a 1 B soft target to force singleton partitions and splitting. It checks exact production-reader results, changed-owner replacement, unchanged complete file reuse, empty-file range extension with retained physical metadata, deleting the final row, negative keys and reinsertion into an empty manifest. The UTF-8 case includes empty, numeric-looking and accented keys, boolean values, and exactly catalog plus owner GETs for a keyed upsert.

No-op coverage checks missing update/delete, identical update/upsert, empty batch, and empty-table operations with zero PUTs. Cross-partition conflict and request duplicate checks preserve all objects; invalid or duplicate batches fail before file GETs. An unsorted real legacy snapshot stays legacy on no-op and migrates on change.

The gated mutation test runs success and storage-error cases. Both partition GETs must enter before release, with different native thread IDs and worker names. Releasing the first and awaiting a same-worker sentinel proves the operation still waits for its gated sibling and has issued zero PUTs. Error completion preserves all objects and PUT counts. Successful completion returns Count(2) and exact rows through the production reader.

The aggregate-budget test models a large untouched neighbor using valid catalog totals, with deliberately synthetic physical counts/payload for that unaccessed neighbor. It proves retained metadata participates in row and payload candidate rejection without allocating cap-sized data. Selected files use real codec bytes. It is not an end-to-end cap-sized data fixture.

## Remaining work and limits

Bootstrap/create/drop wiring remains with the coordinator; create still emits the existing empty legacy snapshot until that separate assignment changes it. No full suite, Clippy, AWS, cap-sized physical fixture or benchmark ran. The dispatcher retains W02's executor contract: callers must await admitted operations; dropping the complete operation future is not cancellation-shielded here.

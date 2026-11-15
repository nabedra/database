# PW-W02 read/query partition execution

Implemented in the existing shared checkout. No agents, branches, worktrees, commits, pushes, credentials, environment files, mutation behavior changes, or plan edits.

Files changed by this assignment:

- `server/src/engine/read.rs`: captured-catalog get/collect/count/explain using shared workers; full-file accounting before filtering; global reduction and limits. Existing `load_rows` remains unchanged for legacy mutation callers, except the shared storage-error mapper import.
- `server/src/engine/partition.rs`: new reusable worker file loader and bounded indexed dispatcher.
- `server/src/engine/query.rs`: extracted shared row comparator and final collect processing; added local filtering/ordering/counting and global collect reduction. Existing pure evaluator semantics remain.
- `server/src/engine.rs`: `mod partition` and `B: 'static` on `execute` only, plus formatting. Existing constructor/injection changes were already present.
- `server/src/api.rs`: private `execute_request` now has `B: ObjectStore + 'static`, required to compile its call to `Engine::execute`. The router and operations handler already required this lifetime. No API behavior change.
- `server/tests/engine_read.rs`: extended three existing read groups, plus one deterministic gated worker/snapshot/error test.
- `server/tests/query.rs`: existing composed query also verifies partition-local evaluation followed by global reduction against the pure evaluator.
- `server/notes/PW-W02.md`: this handoff.

## APIs for W03

Accessible to sibling engine modules through `super::partition`:

```rust
ReadFile::Legacy { object: String, row_count: u64 }
ReadFile::Partition(PartitionFile)
files(&CatalogEntry) -> Vec<(usize, ReadFile)>
load_file<B: ObjectStore>(
    &tokio::runtime::Runtime, &Storage<B>, &TableDescriptor, &ReadFile,
) -> Result<LoadedRows, ProtocolError>
LoadedRows { rows: Vec<Row>, payload_bytes: usize }
row_key(&TableDescriptor, &Row) -> Key
map_files<T, F>(
    &WorkerPool, Vec<(usize, ReadFile)>, F,
) -> Result<Vec<T>, ProtocolError>
// F: Fn(ReadFile, &Runtime) -> Result<T, ProtocolError> + Send + Sync + 'static
// T: Send + 'static
storage_failure(StorageError) -> ProtocolError
```

Capture owned descriptor/file metadata and an Arc clone of the coordinator in `map_files`' closure. Call `load_file(runtime, coordinator.storage(), &table, &file)` inside the job. It drives bounded storage IO using `runtime.block_on` and performs synchronous decoding/validation afterward. Do not submit nested jobs from the closure. Legacy files allow the existing 80 MiB encoded bound and unsorted rows. Partition files use the decimal 64,000,000-byte hard cap and exact actual length, row count, decoded payload count, strictly increasing typed keys, and range containment.

`map_files` admits windows of at most worker_count jobs and passes each original index to pool.submit. It drains all admitted jobs on either submit or receive failure, selects the lowest original ordinal error, and starts no subsequent window after an observed error. Current callers supply files in ascending original index order. Read operations load exactly one fresh catalog. Workers never reload metadata. The dispatcher does not provide cancellation shielding against dropping the entire operation future; callers retain the existing executor lifecycle contract.

`read::load_rows` remains the legacy-only compatibility path used by writes. W03 should adopt the new synchronous worker loader for partition mutations rather than invoking that asynchronous path inside a worker.

Collect workers retain full rows through filtering and local sorting. A final shared worker combines them using the same comparator, applies one global limit, projects, and enforces the existing result row and serialized body bounds. This implementation globally sorts combined local results rather than implementing a heap merge. Count workers scan and validate complete files, then the caller checked-sums counts and applies one global limit. Checked aggregate row/payload totals use full decoded files, including filtered-out rows. Explain remains metadata-only and preserves its string result, appending partition count and worker execution for partitioned tables.

## Evidence

All Cargo commands used existing server/target with CARGO_BUILD_JOBS=2, CARGO_PROFILE_DEV_DEBUG=0 and CARGO_PROFILE_TEST_DEBUG=0.

- Red: the real codec plus publish_partitions fixture failed `mixed_get_composed_query_count_and_metadata_explain` with StorageUnavailable at partition get before implementation.
- Final `cargo test --manifest-path server/Cargo.toml --test engine_read --test query`: 4 engine read tests and 5 query tests passed, without warnings.
- `cargo check --manifest-path server/Cargo.toml --tests`: passed, without warnings.
- Owned Rust files formatted with rustfmt edition 2024, skip_children=true. `git diff --check` passed.

Coverage includes legacy unsorted get, partition pruning to original worker 1, descending sort with ascending key ties and projection excluding key/sort columns, global collect/count limits, empty partition manifests, metadata-only explain, exact byte/payload/row count and range mismatches, descending stored keys, missing files, and collect/count corruption despite limit zero. The gated test waits for both file GETs in one operation, asserts distinct native thread IDs and original-index worker names, drops the table while the snapshot is captured, and uses a same-worker sentinel after releasing the first job to prove the operation still waits for its gated sibling after a storage error.

No full suite, Clippy, AWS checks, or performance benchmark ran. The compact fixtures do not allocate cap-sized objects or test UTF-8 partition keys separately; the loader uses the shared bounded storage API and explicitly handles both supported typed key variants. Mutation migration remains W03's scope.

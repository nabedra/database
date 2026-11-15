# PW-P02: catalog and partition metadata compatibility

Implemented metadata and wire compatibility only. Partition writing, multi-object publication, query execution, and mutation execution are outside this change.

## Wire format and API

`Catalog::empty()` uses version 2. `Catalog::decode()` accepts frozen version 1 snapshot entries and version 2 explicit layouts. Decoded version 1 retains `format_version: 1` and every snapshot reference. Loading or initializing an existing version 1 catalog performs no PUT and leaves the stored bytes unchanged. `Catalog::encode()` validates the input and always writes version 2, still bounded to 1 MiB. The original version 1 binary's `format_version != 1` guard rejects these version 2 writes. This is a forward-only catalog wire change on the next actual publication, without eager migration or object uploads.

Common entry fields remain `id`, `schema`, `key`, `format`, and `row_count`. Version 2 replaces entry-level `snapshot` with exactly one tagged `data` object:

```json
{"data":{"layout":"legacy","snapshot":"tables/<table-uuid>/<commit-uuid>.parquet"}}
```

```json
{"data":{"layout":"partitioned","files":[{"object":"tables/<table-uuid>/<commit-uuid>/0.parquet","range":{"lower":null,"upper":null},"row_count":1,"byte_size":100,"payload_bytes":8}]}}
```

These snippets show only the layout field. The Rust API is `TableData::Legacy { snapshot: String }` or `TableData::Partitioned { files: Vec<PartitionFile> }`. Each `PartitionFile` has `object: String`, `range: KeyRange`, `row_count: u64`, `byte_size: u64`, and `payload_bytes: u64`. `KeyRange` holds optional `protocol::Key` bounds. `byte_size` describes the actual closed Parquet object length including its footer; `payload_bytes` describes decoded payload. Metadata validation cannot independently check either value against an object. Future writers/readers must supply/check those measurements.

Partition manifests require positive per-file rows and byte lengths, at most `MAX_PARTITION_BYTES` per object. Checked row and payload sums enforce 100,000 rows and 64 MiB decoded payload per table, and the row sum must equal the entry count. An empty file list requires zero table rows and implies zero aggregate payload. Object references must be unique canonical partition paths for the owning table. Ordered ranges must cover the entire key domain with an unbounded first lower bound, an unbounded final upper bound, and equal adjacent endpoints. Interior intervals must be finite and nonempty. Integers compare numerically; strings compare lexically; all finite bounds must match the schema's key type.

`KeyRange::contains(key, column_type)` checks inclusive lower/exclusive upper membership and rejects wrong types. `CatalogEntry::route(key)` returns the matching partition reference, or `None` for Legacy/an empty partitioned table. It checks the key type. These helpers expect validated metadata, as supplied by catalog decoding. `legacy_snapshot()` and `snapshot_key()` return errors for partitioned entries, retaining the single-snapshot compatibility path.

`ObjectKey::partition(table_uuid, commit_uuid, ordinal: u32)` builds canonical decimal ordinals. `parse_partition` rejects noncanonical UUIDs, ordinals, traversal, and wrong owning tables. `parse_relative` accepts catalog, legacy snapshot, and partition paths; both Storage and the S3 adapter use this same guard. Conditional replacement of immutable objects remains rejected.

Strict version-specific entry decoding preserves duplicate/unknown field rejection, including unique table/schema maps. Version 1 cannot contain version 2 layouts, and version 2 cannot carry an entry-level snapshot alongside its data.

## Owned files

- `server/src/storage.rs`: canonical partition keys and shared relative-key guard; reexports metadata from the focused manifest module.
- `server/src/storage/manifest.rs`: catalog compatibility codec, metadata structures, validation, and typed routing.
- `server/tests/storage.rs`: grouped metadata/compatibility cases plus mechanical Legacy changes in existing fixtures.
- `server/src/engine.rs`, `server/src/engine/write.rs`: mechanical Legacy wrappers for existing snapshot creation/replacement.
- `server/src/commit.rs`: mechanical snapshot accessor; candidates/publications use version 2 so a loaded version 1 catalog can publish through the existing single-snapshot path.
- `server/tests/commit.rs`, `server/tests/engine_catalog.rs`, `server/tests/engine_read.rs`, `server/tests/engine_write.rs`: only mechanical Legacy wrappers/accessors in existing fixtures/assertions.
- `server/notes/PW-P02.md`: this note.

Preexisting P01 changes in config/lib/CLI tests and `partition_size.rs` were left intact. `plans/partition-workers.md` was not edited. No agents, branches, commits, pushes, secrets, AWS calls, or partition execution were used.

## Verification

All Cargo commands used the existing `server/target` and the environment prefix `CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`. Cargo builds ran sequentially.

1. `cargo test --manifest-path server/Cargo.toml --test storage frozen_v1_reads_without_migration_and_new_writes_are_v2`: failed before implementation, expected version 2 but observed version 1.
2. `cargo test --manifest-path server/Cargo.toml --test storage`: initial implementation attempt caught a mechanical edit syntax error in the new fixture; corrected it. Subsequent run passed all seven tests.
3. `cargo check --manifest-path server/Cargo.toml --tests`: passed, compiling all source/test fixtures without executing the broader suite.
4. Final `cargo test --manifest-path server/Cargo.toml --test storage`: passed all seven tests after extending compatibility/strictness evidence.
5. `rustfmt --edition 2024 --config skip_children=true --check` on the ten owned Rust files above: passed. `git diff --check`: passed.

Evidence covers frozen version 1 with zero writes, mixed version 2 roundtrip, integer and Unicode boundary routing, wrong input key types, missing domain coverage, gaps/overlaps/empty finite ranges, mistyped bounds, duplicate references, noncanonical/oversized ordinals and paths, mismatched/overflowing row counts, decoded payload overflow, oversized/zero object lengths, empty partitioned tables, ambiguous layouts, duplicate/unknown fields, immutable partition access through Storage, and existing catalog read/write size bounds.

No unresolved implementation blockers. Actual partition object measurements and partition execution remain work for later tasks. Only the focused storage tests were executed; other suites were compile-checked as requested.

## Specification review correction

The reviewer demonstrated that derived Serde struct decoding accepted positional arrays for partition files and ranges. The coordinator added both cases to the existing malformed-manifest group. `cargo test --locked --manifest-path server/Cargo.toml --test storage partition_metadata_rejects_invalid_manifests` failed at the file-array case before the fix. `Catalog::decode` now requires data and file/range objects, and a files array, before typed decoding. After owned-file formatting, `cargo test --locked --manifest-path server/Cargo.toml --test storage` passed all seven tests with the standard disk-saving environment. Independent re-review remains pending.

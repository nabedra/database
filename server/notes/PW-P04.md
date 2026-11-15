# PW-P04 multi-object publication

Changed files for this assignment:

- `server/src/commit.rs`
- `server/tests/commit.rs`
- `server/notes/PW-P04.md`

The shared checkout already contained edits in the first two files and other P01–P03 files. Those edits were preserved. No fixture changes, engine dispatch, splitting, worker pool, branches, commits, pushes, secrets, or coordinator plan edits were made.

## API and behavior

```rust
pub struct PendingObject {
    pub key: ObjectKey,
    pub bytes: Vec<u8>,
}

pub async fn MutationSession::publish_partitions(
    self,
    candidate: Catalog,
    objects: Vec<PendingObject>,
) -> Result<LoadedCatalog, ProtocolError>;
```

`publish(candidate, Option<Vec<u8>>)` remains the Legacy compatibility wrapper. Both methods share candidate validation, immutable uploads, conditional catalog publication, and reconciliation in `publish_objects`. The wrapper's 80 MiB allowance applies only to the temporary Legacy snapshot path. Partition objects have the separate decimal 64,000,000-byte cap and must match their manifest's closed `byte_size` exactly.

Publication requires exactly one changed table and the session's revision/commit UUID and v2 catalog. Existing table ID, schema, primary key and Parquet format cannot change; Partitioned cannot downgrade to Legacy. Legacy can migrate to Partitioned. New partition references must be canonical for the affected table and session commit, and uploads must match those references exactly without duplicates. A retained object must occur in that same table's admitted manifest with unchanged physical row count, byte size and payload bytes. Routing bounds may change, including expansion when an adjacent partition becomes empty. No arbitrary historical object can be newly referenced.

The complete catalog passes authoritative validation and bounded encoding before any PUT. All objects use `WriteCondition::Create`. Sequential awaited uploads stop on failure, retain any orphan objects, return NotApplied, and never PUT the catalog after an upload error, including an ambiguous upload. After all uploads succeed there is exactly one catalog PUT with the admission ETag. The existing one-read commit-UUID reconciliation and Unknown outcome after unresolved catalog ambiguity remain unchanged. The session retains the global mutation lock throughout this work. Empty creates, deletion to empty, changes using only retained files, and drops accept zero uploads as appropriate.

## Checks and evidence

Commands ran sequentially from the repository root using the existing `server/target`:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test --manifest-path server/Cargo.toml --test commit
```

Red: before implementation, exit 101 for the newly added tests because `PendingObject` and `publish_partitions` did not exist. This was an API compilation failure, not an observed runtime assertion failure.

Green: all 9 tests passed after the first implementation. After tightening catalog-bound and immutable-write coverage, all 10 tests passed, including the five original commit tests and five grouped partition publication tests.

The added behavior coverage includes successful ordered multiple uploads followed by one catalog PUT; second-upload unavailable/ambiguous/conflict failures with the old catalog intact; missing, extra and duplicate uploads; mismatched lengths; foreign table/commit references; malformed or duplicated manifest references; revision/UUID and multi-table rejection; retained physical metadata changes; migration, retained-file range expansion, empty transitions/create and drop; downgrade rejection; real stale-ETag catalog conflict; applied/unapplied catalog ambiguity with exactly one reconciliation read; actual over-1-MiB candidate encoding with exactly one changed table and no uploads; and immutable key collision preserving the old object's bytes. Existing tests continue to cover lock retention during reconciliation, failed reconciliation reads and the Legacy 80 MiB allowance.

```sh
rustfmt --edition 2024 --check --config skip_children=true server/src/commit.rs server/tests/commit.rs
git diff --check -- server/src/commit.rs server/tests/commit.rs server/notes/PW-P04.md
```

Owned formatting and whitespace checks passed. No additional `cargo check --tests` was necessary because existing method signatures were preserved and the commit test build compiled the library and its engine callers. No AWS or full checks ran.

## Next integration and limits

The engine still uses the Legacy wrapper. Later integration must convert validated `EncodedPartition` output into manifest files and `PendingObject`s, assign ordinals using this session's table ID and commit UUID, and perform table-wide row/payload accounting. Keep the session alive during decoding/encoding and all worker completion, including client disconnects. This layer trusts closed codec bytes and their row/schema/payload claims; it does not decode them. Uploads are sequential here; future bounded concurrency must await every started upload before releasing the session or returning an error. No storage-task blocker remains.

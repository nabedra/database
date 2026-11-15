# PW-P03 synchronous partition codec

Implemented in `src/snapshot.rs`, `src/snapshot/partition.rs`, and `tests/snapshot.rs`. This note is the fourth owned file. Existing unrelated shared-checkout edits were left alone.

Public API, re-exported from `snapshot`:

```rust
pub fn encode_partition_range(
    table: &TableDescriptor,
    rows: Vec<Row>,
    range: KeyRange,
    target: PartitionSize,
) -> Result<Vec<EncodedPartition>, ProtocolError>;

pub struct EncodedPartition {
    pub range: KeyRange,
    pub row_count: u64,
    pub payload_bytes: u64,
    pub bytes: Vec<u8>,
}
```

The function validates the descriptor and all rows once using `validate_rows`, checks range types and containment, and sorts by integer or UTF-8 primary key. An iterative depth-first split uses the middle row's distinct key. Output order is ascending; the first lower bound and last upper bound are preserved. Empty input returns no files. `payload_bytes` uses the existing private scalar accounting and sums to the validated input payload.

Each attempt writes a complete Parquet file into a private sink that rejects writes beyond 64,000,000 bytes. Exact Vec reservation avoids geometric capacity growth beyond the cap. Files above the soft target, or attempts stopped by the hard cap, split when more than one row remains. A singleton can exceed the soft target but a hard overflow returns `LimitExceeded/NotApplied`. Every successful attempt passes the decoder's envelope and compact-Thrift footer guards, including the strict 1 MiB footer cap. The existing encode/decode signatures and 80 MiB legacy allowance remain intact; both encoders share the original writer settings through `encode_into`.

No runtime, storage calls, storage keys, commit IDs, engine integration, or worker scheduling were added. W01 can move the owned arguments onto its worker and call the function directly.

## Observed checks

Commands run from the repository root, sequentially, using the existing `server/target`:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test --manifest-path server/Cargo.toml --test snapshot partition_
```

Red: exit 101, missing `encode_partition_range` at the new behavior-test call sites, before production implementation.

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test --manifest-path server/Cargo.toml --test snapshot
```

Green: 9 tests passed, including all six existing snapshot tests and three new partition tests. Repeated after extending the existing maximum-schema round trip to use 128 columns with 64-byte names and exercise partition output as well.

Coverage includes negative integer and Unicode primary keys, mixed scalar values, soft-target splitting, ordered decode/concatenation, preserved outer bounds and exact internal boundaries, payload sums, empty input, singleton soft overflow, duplicate keys, missing columns, exclusive upper-bound rejection, and invalid bound types.

One real hard-cap boundary test uses two rows with 32,000,000-byte string values. Their combined attempt exceeds the file cap and successfully splits. The same test then uses one 64,000,000-byte string value: it passes table payload validation but its complete file cannot fit and returns `LimitExceeded/NotApplied`. These inputs fit the existing 64 MiB table payload bound. There is no suite of giant fixtures.

```sh
rustfmt --edition 2024 --check --config skip_children=true server/src/snapshot.rs server/src/snapshot/partition.rs server/tests/snapshot.rs
```

Owned-file formatting check passed. No full suite, Clippy, or AWS checks were run.

## Limits and remaining integration

A generated footer above 1 MiB cannot be constructed through the current valid descriptor and fixed writer profile: at most 128 columns, 64-byte identifiers, one row group, no statistics, dictionaries, or offset indexes, and only the generated Arrow schema metadata. The maximum-width/name schema test confirms readable partition output below the footer cap. There is no artificial public API for injecting unrepresentable metadata. The encoder nevertheless runs the existing decoder footer guard and propagates `LimitExceeded/NotApplied` for an oversized footer; multiple-row limit failures split before eventual singleton rejection.

The sink bounds the output Vec per attempt. It does not bound Arrow/Parquet working memory, the owned input rows, or the aggregate returned files. Allocation failures unrelated to the byte cap retain the existing sanitized writer-error behavior. Caller-side table-wide accounting across separate range calls, storage identity assignment, and worker lifecycle remain outside this task.

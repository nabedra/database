# DN-R02a1: incremental partition production

Implemented only adopted A12 mini 1 on shared main. Read distributed-contracts,
A12 FINAL including the mini 2 helper visibility correction, A06/A09 ownership
and output limits, completed R01b2b1/b2/b3a/b3b/b3c notes, and the actual codec,
prepared receiver, validators, row helper, Spool APIs and focused fixtures.
No applicable AGENTS.md was found in workspace ancestors or owned directories.
Applied the requested test-driven-development and unslop skills.

Owned files are snapshot/partition.rs and private partition/tests.rs, the narrow
snapshot.rs emitter re-export, node/payload/prepared.rs and private
prepared/tests.rs, rows.rs helper visibility, payload.rs exports/documentation,
and this note. Existing unrelated changes were preserved. No delegation,
branch/worktree, commit/push, secret/.env read, cloud call, dependency/download,
target deletion or default AWS build occurred.

## Interfaces and implementation

New crate-private interfaces are:

```rust
snapshot::encode_partition_range_each(
    table: &TableDescriptor,
    rows: Vec<Row>,
    range: KeyRange,
    target: PartitionSize,
    emit: impl FnMut(EncodedPartition) -> Result<(), ProtocolError>,
) -> Result<(), ProtocolError>;

payload::LocalPrepared::new(
    operation: Arc<SpoolOperation>, job: &JobEnvelope,
    effect: Effect, changed: bool,
) -> Result<LocalPrepared<'_>, JobError>;
LocalPrepared::push(&mut self, part: EncodedPartition) -> Result<(), JobError>;
LocalPrepared::finish(self) -> Result<PreparedResult, JobError>;

payload::validate_rows(
    rows: &[Row], result: &ResultEnvelope, job: &JobEnvelope,
) -> Result<(), JobError>;
```

The public collecting encoder keeps its signature and delegates to the emitter.
The algorithm still validates once, sorts typed keys, uses the midpoint stack
with left-before-right contiguous ranges, drops oversized attempts before
children, permits singleton soft-target overflow, and preserves the hard-capped
sink and footer checks. Callback errors propagate immediately. There is no
second codec implementation or change to standalone writes.

LocalPrepared borrows the expected job and copies its exact result identity.
Construction validates the job and mutation effect through validate_for; no
replacement count is declared until consuming completion. Unchanged accepts no
files and returns None; changed with no files returns Some(empty). Finish sets
the exact nullable count and uses validate_for plus the existing
PreparationValidator new/push/finish over retained metadata for effect, ordered
coverage and count checks. Only prepared.rs constructs the opaque parts/result.

Each push computes actual byte length and SHA256, validates PartMeta before
opening its capped writer, checks running file/row/payload/encoded arithmetic,
then writes chunks no larger than 64 KiB. Files and rows are bounded at 110,000
before retaining more output; each file remains at most 64 MiB. The small local
checked additions mirror the existing validator because its count is unknown
until completion and job.rs is unowned. No validator rewrite was needed.

The receiver and producer share private digest formatting and closed-file
verification. The receiver retains its pre-close streaming length/hash checks.
Both paths now check the closed payload length and actual read SHA256 before
Parquet decoding, then use decode with the expected row count and the existing
validate_partition_rows for schema, payload, keys, range and strict order.
Locally generated metadata has no exemption. Encoded input bytes are dropped
before the producer's closed-file read; verification bytes/rows and its reader
are dropped before returning from push. No unchecked part constructor exists.

The row helper body is unchanged and is re-exported crate-privately as
payload::validate_rows. It checks declared semantics but cannot prove that a peer
omitted no matching rows. The captured handler remains responsible for complete
input processing.

## Ownership and caller obligations

Construction, commands, error unwinds, consuming finish, reading and final Drop
must all run synchronously on the bounded worker. No task, channel, runtime or
alternate disk owner is added. Later callers must obtain the same operation
account from registered ownership, deposit completed output there before
leaving the worker, and keep all cleanup on workers. A push rejection is sticky;
finish cannot succeed afterward. Earlier retained files remain owned until
synchronous producer disposal, including error consumption by finish.

The caller must abandon production on any encoder error, map callback JobError
to a fixed ProtocolError without copying its text, check cancellation between
files, and never call finish to accept an interrupted encoding sequence.
Use one existing process Spool and shared operation account across files and
attempts. Quota exhaustion fails without publication. No per-file account,
second spool copy or aggregate 80 MiB prepared-output limit is introduced.

The working set includes source/candidate rows, one encoded attempt, codec
memory, one-file verification bytes/rows, bounded retained metadata and a chunk.
Logical payload caps do not establish peak RSS or distributed concurrency
bounds. Global table/catalog finalization, object naming, PUT and publication
remain outside this module. Item-level dead_code allowances on LocalPrepared
and its impl, and narrow unused-import allowances on future re-exports, have
R02a2 removal comments. No module suppression or public test facade was added.

## Executed evidence

Every Cargo invocation ran sequentially in the existing target with:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Exact commands and results, with the environment above prefixed to each:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib incremental_matches_collecting_and_stops_on_error
Initial red: exit 101, E0425 missing emitter and E0433 missing LocalPrepared.
This was a missing-API compile red, not an assertion failure.
After implementation: exit 0, 1 passed, 67 filtered out, 0.01s.

cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload::prepared
Exit 0, 3 passed, 65 filtered out, 0.02s.

cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload::prepared --test node_prepared --test snapshot
After adding callback/byte-quota coverage: exit 0, 4 private tests passed,
65 filtered out, 0.02s. The shared filter selected zero integration tests;
the following commands ran those intended regressions explicitly.

cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test node_prepared
Exit 0, 4 passed, 0.02s.

cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test snapshot partition_
Exit 0, 3 passed, 6 filtered out, 0.76s.

cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test node_prepared --test snapshot -- -D warnings
Exit 0, no warnings, 9.65s.
```

Owned rustfmt check:

```text
rustfmt --edition 2024 --config skip_children=true --check server/src/snapshot/partition.rs server/src/snapshot/partition/tests.rs server/src/snapshot.rs server/src/node/payload.rs server/src/node/payload/rows.rs server/src/node/payload/prepared.rs server/src/node/payload/prepared/tests.rs
```

Exit 0. Scoped git diff --check and direct whitespace/final-newline checks over
all eight owned files, including untracked contents, passed.

Five new focused private tests cover wrapper/emitter equivalence for ordered
ranges, metadata and decoded rows, tiny forced singleton splits, empty input and
immediate callback rejection; null/empty/multiple prepared parts and consuming
readers; invalid effect, missing range coverage, unchanged-file rejection and
sticky file-quota failure; an independently wrong digest with valid metadata
bounds and otherwise valid Parquet; and real emitter-to-producer byte-quota
failure plus locally incorrect payload metadata. Quota probes establish prior
file retention until synchronous cleanup and reuse afterward. Existing detailed
scalar/Parquet matrices were reused through the affected regressions, without
new matrices or broad suites.

No essential interface or ownership blocker arose. Handler/registered deposit
and runtime wiring remain for later minis. RowMutation::operation visibility is
reserved for mini 2. No captured handler, TCP/auth, runtime, distributed or
peak-RSS guarantee is claimed. Independent specification then quality reviews
remain parent-owned. Cargo/build ownership is released on delivery.

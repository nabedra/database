# DN-R01b2b1: bounded spool-backed Get/Collect rows

Implemented only the row semantics mini of accepted DN-A06, including its
parent reconciliation and `plans/distributed-contracts.md`. Read the reviewed
job/spool implementations and DN-R01a/R01b1/R01b2a, protocol/snapshot row
validation and engine query evaluation. No applicable AGENTS.md was found in
filesystem ancestors or the owned directories. Applied local TDD and unslop
guidance. Existing extensive uncommitted work was preserved.

Owned changes are `node/payload.rs`, `node/payload/rows.rs`,
`tests/node_rows.rs`, this note, the payload module export, and only the
crate-private visibility/re-export of existing `compare_rows` and
`matches_filter` through `engine.rs`. Query evaluation bodies did not change.
No existing job/frame/spool/protocol/snapshot implementations, other tests,
Cargo manifests or lockfiles were edited by this assignment.

## Exact API for the next mini

The public facade is `meshdb_server::node::payload`; its rows module is private.
It exports only these synchronous functions, with the existing DTO/error/spool
and protocol Row types:

```rust
pub fn decode_rows(
    payload: &SpoolPayload,
    result: &ResultEnvelope,
    job: &JobEnvelope,
) -> Result<Vec<Row>, JobError>;

pub fn encode_rows(
    operation: &SpoolOperation,
    rows: &[Row],
    result: &ResultEnvelope,
    job: &JobEnvelope,
) -> Result<SpoolPayload, JobError>;
```

Both validate constructed envelopes with `ResultEnvelope::validate_for`, which
also validates the JobEnvelope, before schema indexing or row comparisons.
They accept matching Get/Collect only. Count, Error and Prepared are rejected.
All failures return ValidationError with the fixed message
`invalid row payload`, discarding parser, payload and OS error text.

Get requires exactly found ? 1 : 0 rows, complete schema, the requested key and
payload within declared source bytes. It reuses `validate_get_rows`. Collect
requires exactly matched_rows complete rows, unique primary keys, true predicate
and the existing local comparator ordering, including ascending primary key
when sort values tie. Snapshot validation remains the final accounting and key
uniqueness authority. Result validators check source counts/payload against the
captured input, including partition metadata. Returned totals fit those source
bounds. Select, limit, the public 10,000-result cap and public 8 MiB body cap do
not apply here. The receiver cannot prove that no matching row was omitted;
the later shared source handler supplies that correctness.

## Byte and memory bounds

Decode checks immutable spool length against MAX_INTERNAL_ROW_BYTES,
402,653,186 bytes, then uses two separately opened bounded readers. Neither
CONTROL's 16 MiB cap nor the public 8 MiB cap substitutes for this bound.

The first pass is a fixed shallow array/object resource scan. It retains no
value strings, encoded token copies or JSON tree. Schema-sized candidate flags
match decoded field-name bytes incrementally without retaining key spellings.
Keys are bounded at 64 decoded bytes, fields at schema size and rows at the
validated result count before decoding allocates rows. Each row precharges
8 bytes plus every known schema cell's 8-byte header and UTF-8 name length.
It then charges UTF-8 scalar bytes, 8 per number and 1 per bool. Escapes and
surrogate pairs count their decoded UTF-8 bytes. This is the exact snapshot
formula for admissible complete rows. Invalid shapes can fail earlier; they
never bypass final snapshot validation. Numbers stop at 128 token bytes before
RawValue allocation. Nested scalar containers, null, unknown fields and
incomplete token states fail. Serde remains the authority for full numeric
syntax and Unicode validity.

The second pass uses sequence/map DeserializeSeeds and Deserializer::end.
It rejects surplus rows before map construction, duplicate/unknown keys before
consuming their values and missing fields at row end. Utf8 goes directly into
String. Other schema types use the existing Scalar decoder only after the
guard has excluded string/container tokens and bounded numeric spellings.
Thus integer/float syntax, i64 bounds, finite f64 and signed-zero bits preserve
existing semantics, while private marker objects remain forbidden. No reserve
uses an untrusted size hint/count. Malformed JSON, a second value and trailing
non-whitespace bytes fail.

Encoding validates identical semantics before opening a writer. It calls
serde_json::to_writer through a 64 KiB BufWriter and a bounded spool adapter
whose append requests are each at most 64 KiB. The spool writer uses the same
internal encoded cap. No aggregate encoded Vec/String or raw escaped string
copy is constructed. Existing conservative spool charging, writer poisoning
and cleanup ownership remain unchanged. Smaller caller scratch quotas may
reject capacity without changing the wire contract.

Decoded rows, one Serde scratch buffer, bounded reader buffers and schema/key
tracking are allowed working memory. The 64 MiB decoded accounting bounds
logical values, not allocator overhead or total resident memory. The caller
must account for this working set when bounding concurrency.

## Focused evidence

Every Cargo command used the existing server/target, with this exact prefix:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Initial red, before implementation:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test node_rows
```

Exit 101, E0432 unresolved import `meshdb_server::node::payload`. This was a
compile-time red for the missing API. Initial three groups subsequently passed;
the tests were extended to five causal groups. A private counted-cursor guard
test initially failed because its expected position after the first escaped
NUL was off by one; correcting the test expectation to 23 bytes passed.

Final commands and results are recorded below. No query suite was needed
because helper extraction only widened visibility and added a re-export;
comparison/filter bodies are unchanged.

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test node_rows
5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; 5.37s. Exit 0.

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload::rows::tests
1 passed; 0 failed; 0 ignored; 0 measured; 7 filtered out; 0.00s. Exit 0.

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test node_rows -- -D warnings
Exit 0, no warnings.

rustfmt --edition 2024 --config skip_children=true --check server/src/node/payload.rs server/src/node/payload/rows.rs server/src/node/mod.rs server/src/engine.rs server/src/engine/query.rs server/tests/node_rows.rs

git diff --check -- server/src/node/payload.rs server/src/node/payload/rows.rs server/src/node/mod.rs server/src/engine.rs server/src/engine/query.rs server/tests/node_rows.rs server/notes/DN-R01b2b1.md
```

Formatting and whitespace checks passed, including a direct whitespace check
of owned untracked files. The focused groups use actual owned Spool directories
and remove their parents after cleanup. They cover full Get/Collect round trips,
ignored local select/limit, ordering with descending sorts and ascending key
ties, default key ordering, predicates, duplicate keys, duplicate/unknown/missing
fields, nested/null/private-marker scalars, malformed/incomplete/trailing JSON,
integer versus float token syntax, both i64 bounds and overflow, nonfinite
floats, signed zero, source/match/count/accounting failures and constructed DTO
validation. Capacity failure releases the owned temporary file so the same tiny
operation can write again. A legal Get with 3 MiB of NUL text serializes directly
into a spool above 16 MiB and round trips, without a giant encoded allocation.
Numeric tokens exactly 128 bytes pass and 129 fail. Private tiny-budget cursor
checks prove exact escaped UTF-8 accounting and early string, row, field-name
and numeric rejection before a decoding pass. No giant dataset matrix or
384 MiB allocation was used.

## Deferred ownership and integration

These functions start no threads/tasks and provide no asynchronous wrapper.
The caller must run construction, parsing, reads, writes and all final
payload/reader/operation/manager drops on the designated bounded CPU/I/O
executor. The returned decoded rows own no spool; decode borrows the caller's
immutable spool, which remains the caller's cleanup obligation. Encode returns
a completed opaque SpoolPayload with the existing retained quota ownership.

The next mini must supply supervised processor ownership, admission and
cancellation/draining integration. Prepared/Parquet validation, hashes, framing,
transport EOF, async adapters, HTTP, runtime wiring and shared local handlers
remain deferred. A successful row decode alone does not accept a framed attempt
or establish complete source execution. Independent specification and quality
reviews remain parent-owned.

No full suite, AWS/default build, dependency fetch, cloud call, .env read,
delegate, branch/worktree, commit/push or target/cache cleanup was used. Disk
checks reported about 10 GiB available at start and after focused checks; no
resource blocker occurred. Cargo ownership is released on delivery.

Independent specification review passed without violations. It inspected the
two-pass allocation paths, exact accounting, typed semantics and unchanged
query helper bodies, including cached Serde behavior. The counted-cursor tests
prove early rejection before decoding; no peak allocator/RSS measurement is
claimed. The reviewer reused recorded checks and ran no tests or probes.
Independent quality review remains pending.

Independent code-quality review subsequently passed without important findings.
It inspected allocation/accounting, strict semantics, bounded writes, cleanup,
actual tests and supporting Serde paths, with no tests/builds/probes repeated.
The row-semantics slice is complete. The earlier memory and framed-attempt
limitations remain unchanged.

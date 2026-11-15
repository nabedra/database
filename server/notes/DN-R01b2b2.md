# DN-R01b2b2: synchronous prepared files

Implemented only the prepared-file semantics and synchronous ownership mini of
accepted DN-A06 and plans/distributed-contracts.md. Read job/frame/spool, their
completion notes, completed payload/rows and its note, snapshot codecs and the
partition loader. No applicable AGENTS.md was found in filesystem ancestors or
owned directories. Applied the local test-driven-development and unslop skills.
Preserved the extensive existing shared uncommitted work.

Owned changes are new node/payload/prepared.rs, tests/node_prepared.rs and this
note; narrow exports/documentation in node/payload.rs; the shared validation
extraction in engine/partition.rs and its crate-private engine.rs re-export;
and only the ownership change in job.rs's PreparationValidator. No row, frame,
spool, protocol, snapshot, storage, config or dependency changes. The existing
direct sha2 0.11.1 dependency is reused. No delegates, branches, worktrees,
commits, pushes, .env reads, cloud calls, new targets or cache cleanup.

## Exact API

Public facade meshdb_server::node::payload exports the following types from a
private prepared module. Existing row functions remain unchanged.

```rust
impl PreparedReceiver {
    pub fn new(
        operation: Arc<SpoolOperation>,
        result: &ResultEnvelope,
        job: &JobEnvelope,
    ) -> Result<Self, JobError>;
    pub fn begin_part(&mut self, meta: PartMeta) -> Result<(), JobError>;
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), JobError>;
    pub fn end_part(&mut self) -> Result<(), JobError>;
    pub fn finish(self) -> Result<PreparedResult, JobError>;
}
impl PreparedResult {
    pub fn envelope(&self) -> &ResultEnvelope;
    pub fn into_parts(self) -> (ResultEnvelope, Option<Vec<ValidatedPart>>);
}
impl ValidatedPart {
    pub fn metadata(&self) -> &PartMeta;
    pub fn into_reader(self) -> Result<(PartMeta, SpoolReader), JobError>;
}
```

new uses PreparationValidator::new, including ResultEnvelope::validate_for and
JobEnvelope::validate. Only matching Prepared controls are admitted. The
receiver retains a cloned descriptor and result envelope, never the mutation
job. PreparationValidator now owns cloned TableDescriptor and KeyRange instead
of references and has no lifetime parameter. Its new/push/finish usage, counters,
metadata validation and poison logic remain unchanged. It does not replay prior
metadata per part. Compile-time Send + 'static assertions cover the receiver,
validator, result and part handles; construction in a nested scope proves that
receiver and validator outlive borrowed job/result inputs.

An Arc shares the exact caller-supplied SpoolOperation across files and attempts.
No manager or per-job operation is created here. One active writer is allowed.
begin_part pushes the existing metadata validator before creating that writer.
The validator retains declared count, contiguous ordered typed coverage, checked
aggregate metadata arithmetic and the 110,000 preparation row/file bound.
Accepted null and zero sequences both call validator.finish. Result envelope
identity and effect are retained exactly. None remains unchanged; Some(empty)
remains changed to empty.

append accepts nonempty chunks of at most MAX_DATA_BYTES, 64 KiB. It checks
actual length with checked addition against meta.byte_size and writes through
the same cap. Standard sha2::{Digest, Sha256} hashes only those exact bytes.
end_part checks actual length and lowercase digest before finishing/decoding.
It reads one bounded completed file into Bytes, calls snapshot::decode with
Some(meta.row_count), then the shared validate_partition_rows helper. This
helper extracts the existing loader's validate_rows payload accounting, typed
range membership and strict ascending-key logic. load_file uses the same helper
with optional metadata, preserving legacy snapshots' unrestricted key ordering
and all previous loading checks/errors. Schema/page/row validation stays in the
existing snapshot decoder. All decoded rows/Bytes disappear before the next
file. No vector of decoded files is retained.

Every invalid transition or metadata, I/O, quota, length, digest or semantic
failure permanently poisons the receiver. Failed finish consumes and drops it.
No implicit completion occurs on drop. Completed parts privately retain metadata
and SpoolPayload, with no live descriptor. Constructors and fields are private;
there is no Clone, raw path, File, raw FD or direct payload accessor. into_reader
consumes the part and transfers its metadata and existing bounded Read + Seek
reader. Quota remains charged until the last reader is dropped. All three new
Debug implementations print only their static type name. Every returned error
uses ValidationError and the fixed message `invalid prepared payload`, discarding
parser, OS, path, ETag and payload text.

## State and caller obligations

Construction, every command, validation and every final drop, including failed
construction/commands, must run on the bounded coordinator CPU/I/O executor.
No thread, task, asynchronous adapter, cancellation mechanism or generic
processor framework is introduced. Values own their state and can move into
later finite executor commands without borrowing a caller stack. The caller
must retain/drain accepted work and arrange synchronous cleanup there; aborting
a spawned task is not cancellation of disk work.

All validated spools remain private until semantic finish. PreparedResult is
semantic output only. The later frame adapter must retain it until STREAM_END
and real EOF, abandoning it on truncation, trailing bytes or cancellation. This
mini cannot prove frame EOF. Later global finalization must combine unaffected
files, handle empty routing and apply global 100,000-row, 64 MiB decoded payload
and catalog limits before any table-object upload. No upload, finalization,
HTTP, handler, worker runtime or supervised scheduling occurs here. R01b2b3
owns frame assembly and supervised command ownership. Row functions remain
available for that common adapter.

Quota cleanup uses existing spool accounting. Partial writers, poisoned
receivers and abandoned completed sequences clean up synchronously; failed
unlink retains conservative charges under the existing spool contract. No new
cleanup thread or scanner exists. Encoded file size is capped by each validated
meta.byte_size, at most 64 MiB. There is no aggregate encoded 80 MiB check:
code inspection confirms the receiver has only per-file length counters and the
unchanged validator's checked aggregate counters. The existing node_job
metadata-stream test exercises aggregates above 80 MiB. No new giant encoded
fixture or 100,000-file fixture was added. Scratch quotas can independently
refuse admission.

## Focused evidence

Every Cargo command used existing server/target and this exact environment:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Initial red command, before production changes:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test node_prepared
```

The first run exited 101 with unresolved prepared imports and a test-helper
LowerHex error for sha2 0.11 output. Corrected only the helper to format bytes,
then repeated the same command. It exited 101 solely with E0432 for the missing
PreparedReceiver/PreparedResult/ValidatedPart API. This was compile-time red.

After implementation the combined focused command passed 6 node_job and the
initial 3 prepared groups. Extending ownership/admission checks passed 6 + 4.
Review found that the schema fixture's count also differed; corrected its
metadata count and payload so the schema failure is independent and reran.

Final commands/results:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test node_prepared --test node_job
6 node_job and 4 node_prepared passed; no warnings; exit 0.

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test engine_read fresh_reads_validate_before_data_and_explain_needs_no_snapshot -- --exact
1 passed, 3 filtered out; exit 0. Covers existing partition corruption and legacy loading behavior.

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test node_prepared --test node_job -- -D warnings
Exit 0; no warnings.

rustfmt --edition 2024 --config skip_children=true --check server/src/node/payload.rs server/src/node/payload/prepared.rs server/src/node/job.rs server/src/engine.rs server/src/engine/partition.rs server/tests/node_prepared.rs

git diff --check -- server/src/node/payload.rs server/src/node/payload/prepared.rs server/src/node/job.rs server/src/engine.rs server/src/engine/partition.rs server/tests/node_prepared.rs server/notes/DN-R01b2b2.md
```

Four grouped actual-Spool/actual-Parquet tests cover unchanged/changed-empty,
one and two contiguous files, exact effect/identity preservation, consuming
reader handoff and quota retention, independent wrong actual length/digest/
schema/row count/decoded payload/range/key order, count and coverage failures,
truncated active and completed sequences, extra files, every illegal command
state, empty/oversized append, per-file cap, declared maximums, invalid metadata
before file admission, permanent poison, byte/file quotas and recovery after
error/drop. Metadata-count and file bounds use small inputs. New types' Debug
and fixed errors are checked. Owned scratch parents are removed after cleanup.

No full suite, default AWS build, fetch or unrelated CRUD suite was run. Owned
rustfmt and direct whitespace checks cover tracked and untracked files.
Independent specification then quality reviews remain parent-owned. Cargo
ownership is released on delivery.

Independent specification review subsequently passed without actionable
findings. It inspected the contract, implementation, supporting APIs and test
bodies, including the independent schema fixture and existing metadata-scale
coverage. The reviewer reused the recorded checks and ran no tests, builds or
probes. Framed EOF acceptance, cancellation supervision, global finalization
and runtime integration remain deferred. Independent quality review follows.

Independent quality review subsequently passed without important findings. It
inspected poisoning, allocation and arithmetic bounds, ownership, shared loading,
reader handoff, quota cleanup and causal test assertions. It reused the recorded
checks without tests, builds or probes. The component is complete within this
synchronous semantic boundary; framing and supervised execution remain separate.

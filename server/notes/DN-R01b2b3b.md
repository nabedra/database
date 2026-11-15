# DN-R01b2b3b: supervised framed receive

Implemented only A09 mini 2 on shared main, preserving existing uncommitted
work. Read distributed-contracts.md, A09, A06 as corrected by A09, A07's caller
boundaries, completed R01 notes and the actual frame, job, spool, row, prepared,
owner and WorkerPool APIs. No applicable AGENTS.md was found. Applied
test-driven-development and unslop. Independent specification review followed
by quality review remains parent-owned.

Owned files are new `src/node/payload_stream.rs`, its private
`src/node/payload_stream/tests.rs`, and this note, with receive-only extensions
in `src/node/payload_owner.rs`, private hooks in
`src/node/payload_owner/tests.rs`, and the private module declaration in
`src/node/mod.rs`. No other repository files were edited by this assignment.
No delegates, branches, worktrees, commits, pushes, dependency changes, secret
or .env reads, cloud calls, target cleanup or full-suite runs were used.

## Private API and execution

`receive_request(reader, lease)` returns `AcceptedRequest { envelope, lease }`.
`receive_result(reader, lease, expected: Box<JobEnvelope>)` returns
`AcceptedResult { envelope, lease }`. Both are crate-private async functions in
a private module. Their reader is the unchanged
`FrameReader<Box<dyn AsyncRead + Unpin + Send>>`; callers box their transport
before constructing the codec. Both consume an already admitted noncloneable
PayloadLease, retaining its original operation account, role admission,
deadline and lifetime guard. They accept either pending or completed initial
owner readiness. Dropping either future unpolled drops its owned lease and
records cancellation without disk work.

A concrete Receive request transfers the codec into the ONE existing registered
supervisor. No additional task, executor, transport queue or processor trait was
introduced. The supervisor performs framing serially and awaits one finite
worker command at a time. BeginRequest parses untrusted CONTROL on the injected
coordinator CPU/I/O WorkerPool. BeginResult parses against the expected job on
that pool. BeginPart parses metadata there. Append, CloseRows, ClosePart and
CompletePrepared use the existing synchronous row/PreparedReceiver APIs there.
No command waits for transport or invokes runtime.block_on or a partition pool.

Disk-backed states remain inside the registered ManuallyDrop cell. Prepared
receiver, pending PreparedResult and accepted PreparedResult never enter a Job
result, Tokio channel or async destructor. The existing PreparedResult retains
unchanged None versus changed Some(empty), the original effect and verified
parts. There is no metadata replay, decoded-file collection, duplicate
validator or aggregate 80 MiB prepared limit. Rows use the original spool cap
MAX_INTERNAL_ROW_BYTES with checked append arithmetic, independent of CONTROL's
16 MiB limit. TakeRows still returns ordinary memory-only Row values.

Accepted async values expose only their memory envelope, whose result tag gives
the logical kind, and their opaque lease. They expose no prepared files,
readers, paths or raw disk owners. Dropping a buffered ticket drops that lease
and signals its original supervisor, using the existing owner mechanism.

## Framing, cancellation and cleanup

Exactly one initial CONTROL is required. Requests, Count and Error then require
STREAM_END and real EOF. Get/Collect require DATA*, PAYLOAD_END, then that same
terminal proof. Zero DATA is rejected as invalid empty JSON; absence/empty rows
still require the actual array `[]`. Prepared null/zero execute semantic finish
before termination. Prepared n consume exactly n metadata/payload groups and
finish the existing receiver before termination. Unexpected frames, missing
terminators, truncation and trailing bytes fail. Only next_frame returning
Ok(None), which consumes both STREAM_END and actual EOF, permits CommitEof.
Partial semantic completion never yields acceptance.

Each transport read selects sticky cancellation and the admitted deadline.
Cancellation abandons the codec; it is never reused. The next read first checks
cancellation, so a cancelled accepted Append cannot start another DATA read.
An accepted command is always retained and awaited to settlement by the
registered supervisor. It then discards on the same pool. No disk/registry lock
spans an await. The EOF handoff mutex and final supervisor cancellation check
still suppress abandoned delivery. A race with external disconnect does not
promise atomic ticket delivery.

An ordinary receive failure explicitly awaits original-owner disposal and
reaping before returning its error. Caller abortion instead leaves that work
with the original registered supervisor. Stable ReapWake broadcasts,
registration of Notify before inspection, notifications after unlocking, and
handle/admission retention through actual reaping are preserved unchanged.
Process/operation anchor disposal and failed-drain behavior remain as in mini 1.

## Executed evidence

All Cargo commands ran sequentially against the existing target with:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Initial red used the requested payload_stream test command before implementation.
After correcting test imports it exited 101 for the missing receive_result API.
This was a compile-time red, not an assertion failure. The first row receive
case then passed. The expanded grammar group exposed a runtime red: Refused on
the next admission after a failed receive, because error delivery preceded
reaping. Awaiting disposal/reaping on that error path corrected it. An
intermediate visibility warning was removed by exposing only run_command rather
than the internal Work enum. Initial strict Clippy found a large enum variant
and manual async functions; boxing the retained PreparedReceiver and using
async fn corrected these without lint allowances.

Final checks:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_stream
4 passed, 0 failed, 16 filtered out; 0.05s; exit 0, no warnings.

cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner
8 passed, 0 failed, 12 filtered out; 0.03s; exit 0, no warnings.

cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
Exit 0, no warnings; 12.39s.
```

Four private test groups use actual Spool, WorkerPool and Tokio duplex. They
compose valid request, Get, Collect, Count, Error and Prepared null/zero/file
streams; missing row JSON; extra CONTROL/DATA/metadata/PAYLOAD_END; truncated
frames; trailing bytes; and bad file digests. The small Parquet fixture follows
node_prepared's snapshot encoding and metadata pattern. No scalar/field matrix
or large escaped Get roundtrip is repeated.

An instrumented duplex reader establishes Pending after valid row semantics and
STREAM_END while its writer remains open. Cancellation yields no accepted
ticket. A worker event separately gates accepted Append, with a second DATA
already available. Byte counts prove that second frame is never read after
cancellation. Actual worker-side writer probes prove file quota remains charged
while Append is gated and becomes reusable after Discard. A post-Discard,
pre-return event proves admission and lifetime remain retained until actual
supervisor reaping. Guard counts return to one after full anchor disposal.
These gates use channels, with timeouts only as failure watchdogs. Tests settle
owned tasks, writers, workers and scratch before their final assertions.

The last group splits a small legal row array inside a JSON escape and a float
number, consumes decoded rows through TakeRows, and drops an unpolled receive
future. The eight existing owner tests also pass, including concurrent disposal
waiters, buffered-ticket disposal and failed-pool retention.

Owned rustfmt uses edition 2024 and skip_children=true. Scoped whitespace checks
include direct inspection of untracked owned files. Only the new private stream
module has a temporary dead_code allowance. Remove it when production receive
runtime callers make that module live; the inherited owner allowance was left
unchanged. No other lint allowance or public test facade was added.

## Deferred work and limits

No blocker required widening ownership. Send composition belongs to mini 3.
R03 must bind exact incoming request/body bytes to authentication headers and
preserve HTTP trailers/body errors/EOF. This receive component makes no auth
claim and does not execute received jobs. Runtime wiring, HTTP/TLS, dependency
admission integration, global prepared-result finalization, uploads, publication
and actual-process/TCP acceptance remain deferred. Local writer finish and
duplex EOF do not prove HTTP trailer validation or TCP delivery. No full suite
or peak-memory measurement is claimed. Cargo ownership is released on delivery.

## Parent correction after specification review

Specification review found that readiness and receive-channel enqueue errors
returned before the original owner was reaped. The normal reply-error branch
already awaited disposal, but early question-mark returns bypassed it.

The parent added one private owner regression with two real supervisor cases:
an initialization failure leaves a buffered readiness error, and an immediately
expired supervisor leaves buffered readiness success with a closed receive
channel. The test observes each actual supervisor result without removing its
registered entry, then calls receive. It records entry removal and semaphore
capacity immediately at error return, before any admission or cleanup call can
hide the defect. Owned anchors, supervisors, pool and scratch are cleaned before
the final assertion.

Before correction both cases returned an error while leaving the entry and its
permit retained: [(true, false, 0), (true, false, 0)]. The parent wrapped readiness,
enqueue and reply observation in one result path that always awaits disposal
on failure. Unexpected reply-variant branches in both public-to-the-crate receive
wrappers also explicitly dispose. Failed disposal still returns FailedDrain and
retains its guards; this is not a promise to release failed cleanup.

Using the same resource-limited Cargo environment documented above:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner::tests::early_receive_errors_reap_before_returning
Before correction: 1 failed, 25 filtered out, 0.01s; exit 101.

cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_
After correction: 13 passed (9 owner + 4 receive), 13 filtered out, 0.06s; exit 0.
```

Owned rustfmt and whitespace checks passed. A fresh specification re-check is
required before quality review; transport/auth/runtime limits remain unchanged.

The parent also confirmed strict no-default library Clippy (`--lib -- -D
warnings`) with the same locked/offline resource-limited environment: exit 0,
1.42s. The earlier invocation's result was unavailable after context truncation;
this repeat recovered its verification evidence without repeating passing tests.

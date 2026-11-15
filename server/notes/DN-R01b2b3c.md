# DN-R01b2b3c: supervised framed send

Implemented only accepted A09 mini 3 on shared main. Read distributed-contracts,
accepted A06/A09, completed R01b2b1/b2/b3a/b3b notes and the actual owner, stream,
frame, job, payload, spool and WorkerPool interfaces. No applicable AGENTS.md was
found in workspace ancestors or owned directories. Applied the requested TDD
and unslop skills. A11 was unrelated to this assignment; the parent adopted its
design during this work, with configuration implementation still deferred.

Changed only payload_stream.rs, new payload_stream/send_tests.rs,
payload_owner.rs, payload_owner/tests.rs, narrow helper visibility in
payload_stream/tests.rs, and this note. All other existing work was preserved.
No delegation, branch/worktree, commit/push, secret/.env read, cloud call,
download, dependency change, target deletion or default AWS build occurred.

## Private API and ownership

The private stream module now provides crate-private async functions:

```rust
send_request(writer: Writer, lease: PayloadLease, job: Box<JobEnvelope>)
    -> Result<Transport, OwnerError>
send_result(writer: Writer, lease: PayloadLease, expected: Box<JobEnvelope>)
    -> Result<Transport, OwnerError>
```

Writer is the unchanged FrameWriter over a boxed Send + Unpin AsyncWrite.
Transport is that underlying boxed writer. Requests take memory-only constructed
DTOs and an admitted lease. Results consume accepted state already registered
in that lease. No raw PreparedResult, ValidatedPart, SpoolPayload or reader
crosses an async input, output, channel or Job result. Dropping either future
unpolled drops its owned lease and signals cancellation.

The SAME registered supervisor handles a concrete Send request. StartSend,
NextPart and ReadNext are finite worker commands. StartSend validates through
encode_job/encode_result before transport output. Accepted rows go directly
through encode_rows into the original operation account, then open one bounded
reader. Accepted prepared output consumes into_parts on the worker and checks
all metadata/counts through PreparationValidator, including mandatory finish
for null/zero, before output. NextPart consumes each immutable validated part
sequentially through into_reader and encodes its metadata with encode_part_meta.
It preserves the envelope, exact effect, nullable count, declared order and
original file bytes without rehashing or a second semantic validator.

ReadNext returns at most 64 KiB of ordinary memory. The supervisor awaits each
DATA write before asking for another read. Each row array, including [], and
each prepared file ends with PAYLOAD_END, followed by FrameWriter::finish.
The final reader stays registered even after its EOF read until worker disposal.
Earlier readers are destroyed on workers when advancing to the next part.

Send readiness, enqueue, reply and output failures all converge on explicit
lease disposal/reaping before ordinary error return. Success also explicitly
disposes before returning the finished transport. Cancellation during output
selects the existing sticky cancellation/deadline, abandons the codec, settles
accepted worker work and disposes through the existing supervisor. Caller
abortion leaves registry ownership intact. No extra supervisor, executor,
fallback thread, cleanup queue or generic command execution was added.

Receive's parent correction is preserved. Its readiness/enqueue/reply errors
and unexpected variants still await disposal; FailedDrain retains guards.
Stable broadcast reaping, registered handles through actual termination/result
consumption, worker-only final disk destruction and failed-drain quarantine
remain unchanged. Runtime must retain process/operation anchors and keep the
injected pool alive through payload drain before WorkerThreads shutdown.

The returned writer's caller must supply HTTP-body EOF or duplex shutdown.
Local finish proves neither peer receipt nor framed acceptance. R02a still owns
handler-side production and worker-side deposit into registered ownership.
There is no new public test facade or raw-anchor deposit API.

## Executed evidence

Every Cargo invocation ran sequentially in the existing target with exactly:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Initial test command, before production implementation:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_stream::send_tests
```

The first run exited 101 with a test-helper visibility/async ordering error and
missing send names. After correcting only test syntax and the transport cast,
the same command exited 101 solely with E0425 for missing send_request. This is
the meaningful compile-time red. Adding the result roundtrip before production
implementation gave another exit 101 for missing send_request, send_result and
the planned Transport alias. No assertion-based red is claimed.

After implementation the same focused command passed 2 tests, 26 filtered out,
0.06s. Adding the large case passed 3, 26 filtered out, 5.80s. Adding causal
cancellation and invalid/unpolled cases passed 5, 26 filtered out, 6.57s.
The unpolled test was then made independent of scheduling by using the two
separate role capacities, followed by the combined affected regression run:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_
18 passed, 0 failed, 13 filtered out; 5.95s; exit 0, no warnings.

cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
Exit 0, no warnings; 9.28s.

rustfmt --edition 2024 --config skip_children=true --check server/src/node/payload_owner.rs server/src/node/payload_owner/tests.rs server/src/node/payload_stream.rs server/src/node/payload_stream/tests.rs server/src/node/payload_stream/send_tests.rs
Exit 0.

git diff --check -- server/src/node/payload_owner.rs server/src/node/payload_owner/tests.rs server/src/node/payload_stream.rs server/src/node/payload_stream/tests.rs server/src/node/payload_stream/send_tests.rs server/notes/DN-R01b2b3c.md
Exit 0.
```

An additional direct Python whitespace check covers all six owned files,
including untracked contents, for trailing whitespace and final newlines.
No full-suite or unrelated tests were run. No lint allowance was added.

## What the tests establish

The composed request and result groups use actual WorkerPool, Spool and Tokio
duplex. Results cover Count/Error, Get absence, empty Collect and Prepared
null/zero/one. Envelope equality preserves exact effects and distinctions.
The small real-Parquet fixture follows the existing pattern, and receiving its
unchanged declared digest validates byte preservation. Existing tests retain
the detailed frame/scalar/Parquet matrices.

A legal 3 MiB NUL-text Get produces over 18 MiB of escaped JSON and traverses
distinct sender and receiver pools/spools through a 257-byte duplex. Input
fragments are generated lazily; neither sender nor receiver collects the
encoded array. Decoded rows are compared after real framed acceptance. This
proves composition, not peak RSS.

A one-byte duplex and real Pending-write events stop output during the first
DATA frame and after the final EOF read. Another case gates an accepted
ReadNext on its actual worker. Read counts remain exactly one, one and two,
respectively, through cancellation and cleanup. A finite worker-side quota
probe on the same pool completes while output/read is held and proves quota
is charged. After cancellation admission remains occupied; after worker
Discard quota is reusable but admission and lifetime guards remain held at a
post-cleanup/pre-return gate. Only releasing the gate and actual reaping lets
operation/process disposal finish. Guard counts return to one. Every normal
case drains supervisors, joins worker threads and removes owned scratch.
Timeouts are event watchdogs, never sleeps used as evidence.

Invalid constructed request output fails before any byte and permits the next
admission. Both request and result futures are also dropped unpolled and their
owners drain. The affected owner tests retain coverage of early receive errors,
concurrent reaping waiters and premature-pool-stop quarantine.

No essential interface blocker arose. HTTP/auth/runtime wiring, cluster
readiness, distributed drain and distributed acceptance remain outside this
mini. Independent specification then quality reviews remain parent-owned.
Cargo ownership is released on delivery.

## Parent correction after specification review

The reviewer found that NextPart opened the next prepared file before assignment
dropped the previous exhausted reader. Two readers could therefore coexist
during the transition. The parent now takes and drops the previous reader on
the worker before opening the next part. The final reader remains registered
until the existing disposal path; no read-next or cancellation behavior changed.

A new two-part roundtrip uses independent real sender/receiver pools and Spool
accounts with exactly two file slots each. A test-only worker hook, immediately
before each into_reader call, tries to create and drop a temporary empty writer
against that same account. Before the first open both prepared files occupy
the quota; before the second open the exhausted first file must already be
deleted and its slot reusable. This observes actual resource lifetime, not an
Option-field assertion. The hook's operation reference is cloned and dropped
only on the worker. Both transports, accepted output, anchors and worker threads
are disposed before the final regression assertion.

The initial hook compile exposed a mutable-borrow conflict, fixed in test-only
setup before the behavioral red. Using the established resource-limited,
locked/offline/no-default Cargo environment:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_stream::send_tests::prepared_transition_releases_exhausted_reader_before_next_open
Before production correction: 1 failed, 41 filtered out, 0.02s; exit 101.
Observed [true, true] instead of [true, false] for quota refusal before each open.

cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_
After correction: 19 passed (9 owner, 4 receive, 6 send), 23 filtered out,
5.89s; exit 0.

cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
Exit 0; 9.94s.
```

Owned rustfmt and direct whitespace checks passed. The earlier one-part/EOF
and cancellation tests remain in the passing affected set. This test establishes
local file/quota release ordering, not OS-wide descriptor exhaustion or a
distributed resource bound. Fresh specification re-check precedes quality review.

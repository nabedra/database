# DN-R02a3: captured Prepare through framed receipt

Implemented A12 mini 3 / R02a3 as one private test case on shared main.
Only the following files are owned by this slice:

- New `server/src/node/payload_owner/composition_tests.rs`.
- One private child-module include at the end of `payload_owner/local_tests.rs`.
- This note.

The parent's file-placement override replaces A12's stream-test location.
Existing fixtures, tests, production behavior, visibility and public APIs are
unchanged. No actual integration defect required production edits. Existing
uncommitted work was preserved. No applicable AGENTS.md was found in workspace
ancestors or the repository. Read the accepted distributed contracts/testing
rubric, A12/A06/A09, R02a2 including its latest harness correction, R01b2b3c,
and the actual owner/stream/handler fixtures and APIs. Applied unslop and TDD
skills subject to the explicit test-only instruction: the initial behavioral
run passed against existing production; no failing regression or production
fix is claimed. No delegation, branches/worktrees, commits/pushes, cloud calls,
secret/.env access, dependency changes/fetch, target removal or AWS build occurred.

## Composition and observations

`captured_split_prepare_roundtrips_original_registered_output` reuses the
private `LocalFixture`, `prepare` and `quota_charged` helpers. Sender and receiver
have independent registered process/operation accounts, registries and original
partition/coordinator pools. The real Fake input contains unsorted rows 3, 1, 2,
all initially true. The captured Prepare updates row 2 to false and uses
`target_bytes=1`. `try_local` and `local_complete` execute the captured handler,
which performs exactly one Fake GET and returns Found(true) with three
replacements. Prepare CONTROL has no source-count fields; source accounting is
observed through that single GET and the exact three surviving complete rows,
rather than inventing new CONTROL fields. A conflicting catalog sentinel from
the existing fixture remains present.

The original validated output lease moves directly into real
`payload_stream::send_result`, using FrameWriter and a 31-byte Tokio duplex.
The returned writer explicitly calls AsyncWriteExt::shutdown to supply true EOF.
The actual `receive_result` uses the same captured JobEnvelope. The test never
fabricates CommitEof for locally produced output and never opens spool paths or
creates unregistered raw anchors. The received envelope must equal both the
handler envelope and an independently constructed identity/result expectation.

The accepted receiver lease is inspected through existing StartSend, NextPart
and ReadNext worker commands. Bounded ordinary bytes, at most 64 KiB per tiny
file, are decoded on the receiver's injected partition pool. Independently
specified expected rows are (1,true), (2,false), (3,true); expected ranges are
[unbounded,2), [2,3), [3,unbounded). Each singleton's complete row, row count,
payload byte count, encoded size and digest must match expected Parquet encoded
from those specified contents. No sender-generated metadata is used as the
expected metadata. This reuses the existing encoder rather than duplicating
Parquet encoding or semantic validation.

Existing supervisor return hooks hold each side after worker disposal and
before actual return/reaping. At those causal boundaries the test observes
scratch release while admission and independent lifetime permits remain held,
including a refused actual admission attempt. Sender scratch is charged after
local completion, free after normal send disposal, and receiver scratch is
initially free then charged after real receipt. Releasing each return gate and
awaiting the real sender/dispose task observes admission and lifetime recovery
through actual reaping. Existing fixture no-PUT/put_keys counters exclude source
seeding and cover both sides.

One ten-second watchdog bounds the complete observation future, including every
gated quota/command observation. On error or timeout, both return gates release,
original retained leases drop/cancel, and every still-owned spawned task is
aborted and joined. Completed handles are removed before propagating their
results, avoiding a second await of a completed handle. Both fixture finish
futures run as owned tasks and are joined before teardown-result assertions, so
one fixture assertion cannot skip the other's teardown. Each fixture attempts
anchor disposal, registry drain, both original worker-pool joins and scratch
removal before its own result/counter assertions. Behavioral observations are
asserted after teardown. No worker gate, sleep-based timing inference, detached
handle, alternate owner or executor is introduced. This cleanup control flow
was inspected; a new failure-injection matrix was not run.

## Verification

Every Cargo command used the existing target and this environment:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Exact commands, with that environment applied to each:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner::local_tests::composition_tests::captured_split_prepare_roundtrips_original_registered_output -- --exact
Initial run: 1 passed, 0 failed, 89 filtered out; 0.05s, exit 0.

cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner::local_tests
After test-only task-settlement/teardown tightening: 7 passed, 0 failed,
83 filtered out; 0.05s, exit 0. Covers the new case and six existing groups
in the parent module receiving the private include; no broader suite repeated.

cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
Passed, no warnings; 1.10s, exit 0.
```

Owned rustfmt with edition 2024 and skip_children=true, scoped git diff --check
and direct whitespace/final-newline checks include both new untracked files.

Evidence is local captured-handler/sender/receiver wiring, verified tiny output
and ownership disposal/reaping. It does not establish process/network/HTTP,
authentication, runtime integration, peak RSS, historical captured provenance,
global finalization/publication, cluster progress or full distributed guarantees.
Existing missing/trailing-EOF and failure matrices are reused without rerunning
them. Independent specification then quality review remain parent-owned.
Cargo/build ownership is released on delivery.

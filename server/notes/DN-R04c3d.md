# DN-R04c3d — local final drain observation

This implements only the final adopted A22 c3 slice. Independent NEW specification
and quality reviews belong to the parent. No rubric or aggregate completion is
claimed. Used test-driven-development and unslop; no applicable AGENTS was found
in the checkout or its ancestors. A09/A20/A21/A22, the distributed contracts and
orchestration/testing rubric, prerequisite delivery notes and actual retained
APIs informed this implementation.

## Changes and ownership

Before edits, the seven existing owned files were copied with repository-relative
paths to `/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-c3d-1lg74y6k/`.
Current unified comparisons are saved beside those copies. These comparisons
establish current content relationships, not historical provenance.

- `admission.rs`: private-field `DrainObservation`, redacted Debug, closed
  `DrainRefusal::{Invalid, Unavailable}`, and final acknowledgement, association,
  evidence and owner checks. The observation owns the original `ReapedProof` and
  exposes its original immutable RequestBinding to the narrow reply caller.
- `drain_observation.rs`: `ProofSlot::matches_reaped(&ReapedProof)` requires the
  exact registered Arc, original retained handle, successful polling/reaping,
  consumed live data, no failure/retirement and completed cancellation handoff.
  No c3c transition or generation behavior changes.
- `payload_owner.rs`: `PayloadOwners::finish_drain(ReapedProof)` and a closed
  typed final classifier. The shared classifier body accepts only an internal
  optional borrowed ReapedProof; the ordinary c3a entry always passes None.
  Final classification additionally requires the original live response hold.
- `auth/wire.rs`: only `DrainReply::from_observation(&DrainObservation)`, visible
  within node. It produces the existing five fields with version 1, barrier true
  and the actual positive acknowledged sequence. Encoding remains bounded and
  occurs outside ReceiverState. Checked decode, signatures and record privacy
  are unchanged; no new wire fields or unchecked constructors.
- `payload_owner/installation_tests.rs`: append-only composition coverage using
  the existing signed receiver, Fake, WorkerPool, gates and teardown fixtures.
- `payload_owner/tests.rs` and `auth/wire/tests.rs`: byte-identical to snapshots.

No other source, Cargo, prior note, contract or rubric was edited. No branch,
worktree, commit, push, network operation, dependency change or target cleanup
was performed. Existing shared modifications and untracked work were preserved.

## Final check and lifetime

`DrainObservation::finish` owns the completion before locking the same actual
ReceiverState mutex. A bounded borrowed check validates the positive real ack's
binding, original sample and sequence; complete current root/high-water,
boot/init, permanent barrier/drain and original authority cell; exact registered
reaped cycle; settled generation evidence; live initialized process and accepted
Drain observer/response; and the complete actual-owner predicate. The evidence
cell is checked before and after classification. Invalid associations and
unsettled/failed ownership refuse. Settled independent history or retained work
can yield only idle false. Matching generation plus ExemptOnly yields idle true.
There is no parsing, hashing, task creation, spawn, I/O or provider work in this
critical section. Proof destruction occurs after unlocking on every return.

Only this exact typed completed cycle is exempted. Ordinary classification,
retention snapshots and whole-process shutdown still include every proof cycle,
result, original handle, process and observer. The observation retains its marker
through reply production until Drop. The existing outside-release/reaper path
then retires the cycle. Ticket cancellation after take remains proof-only and
cannot invalidate a live completed result. No generic exemption flag, numeric
cycle authority, provider certificate, erased future or public facade exists.

The original immutable request association stays in its original OnceLock cell.
The producer never reconstructs a checked request or clones a proof. The signing
test borrows its original Auth, builds the existing outgoing RequestContext and
uses the original request bytes, bounded reply encoding, WorkerSigningKey,
verify_reply and check_reply_content. It constructs no caller idle reply body.
The decoded DrainReply alone is a wire record; it grants no termination authority.

## Causal evidence

The final_drain_receipt groups cover:

- Real signed accepted Drain, original initialized process, real Fake-backed C03c
  ack gated before supervisor return: no completion before reaping; unchanged
  sample succeeds afterward, including ticket Drop before finish. Ordinary
  classification still reports WorkPresent. The resulting typed reply is signed
  and checked against the original sequence and request/content context.
- Operation/self/account owners, DB-publication DeliveryHold, blocked Discard,
  cleanup before return, retained cleanup, terminated-but-unreaped handle and
  pending outside release. Seven phase fixtures consume the final gate at those
  points; each prevents positive evidence. Each independently releases all gates,
  reaps original owners, and proves a new sample/publication becomes idle.
- A real dormant preparation admitted and fully reaped between sample and final
  check leaves empty current owners but yields exactly idle false. A fresh
  publication restores idle true. Live preparation and extra response ownership
  also deny positive evidence before their settlement, then permit fresh proof.
- Unrelated signed readiness control and preparation, retained dependency with
  zero occupancy/no payload account, and a failed original process. These extend
  the existing actual-owner fixtures with final gate consumption. Failed-process
  teardown preserves the corrected original observer/transport reaping before
  test-only quarantine. The failed-process case has its own independent positive
  receipt before failure.
- Two receivers with colliding identities/cycle numbering: finishing A's real
  completion at B refuses; B's original proof and A's fresh proof still succeed.
  Both teardown tasks are independently spawned and both awaited before asserts.
- Cancelled original observer and a complete newly observed root refuse the old
  completion; a current replacement can obtain fresh positive evidence. A separate
  replacement case retains the original publication marker while retiring the
  old observer, refuses taking that cycle with the new exact ControlKey, refuses
  finishing via the retired original key, then accepts a fresh replacement cycle.
- Definite no-ack, ObservedExact, Unknown, supervisor panic and conflict use the
  actual existing C03c/Fake seams. Only the definite no-ack completion is normally
  takeable and final finish refuses it. Ambiguous/failed outcomes retain the
  original slot and cannot produce a receipt. Test quarantine follows completed
  synchronous Fake effects and actual original handle polling, as in c3c.
- Whole-process shutdown times out while a live positive observation retains its
  proof result/marker; dropping it allows ordinary shutdown to resume. Snapshot
  evidence before shutdown includes the original process and observer. Negative
  trait checks establish no Clone/Serialize/Deserialize on DrainObservation;
  Debug prints only its type name.

The c3a/b/c matrices remain unchanged prerequisite evidence for boot/root/drain,
foreign ControlKey, initialization, missing original response, failure ownership,
full protocol shapes, same-waiter shutdown publication and both completion/ticket
Drop orders. The new cases exercise final composition of these classifications;
they do not claim a new protocol matrix or real external provider settlement.

All new gates are finite and released before fixture teardown. Original owners
settle/reap, owned reaper threads join, WorkerThreads shut down and scratch is
removed before outcome assertions. Deliberate failed-provider/process cases use
only existing cfg(test) quarantine after actual fixture effect settlement and
surviving owner cleanup. No production failure recovery was added.

## Test provenance and validation

Every Cargo invocation, including failures, used existing `server/target`, this
prefix, and the exact common flags:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
--manifest-path server/Cargo.toml --locked --offline --no-default-features
```

Actual process exit is awaited before the next Cargo invocation. No build,
Clippy, test or formatting mutation overlaps another active Cargo process.
Logs are `/tmp/meshdb-c3d-*.log`.

The first positive test preceded implementation and failed with missing
finish_drain/from_observation APIs (exit 101): compile-red, not behavioral-red.
Its initial implementation passed (1 test). Later tests are green-only coverage.
Intermediate failures were: wrong test helper names/non-Clone ControlKey use
(compile); noncanonical test ReplyAuth bytes (assertion after teardown);
expecting an active preparation to always return idle false rather than allowing
unavailable while its outside transition was unrecorded (assertion after
teardown); expecting a rejected conflicting drain to replace the accepted
current root (assertion after teardown); and a nonexistent snapshot field in a
new test (compile). These are fixture corrections, not production defect red.
The corrected nine groups passed, then the eleven groups including shutdown and
opacity passed. The final commands and results are recorded below.

## Limits and caller obligations

The caller must obtain a fresh complete GET and install current checked authority
before sampling or finishing; these local components cannot discover a newer
unobserved remote root. Independent work history requires a fresh sample and
serialized publication for a new positive attempt. No positive sequence means
unavailable, never a fabricated zero or ObservedExact acknowledgement.

HTTP/prechecked advanced FrameReader handoff, configured startup and R07 issuance,
periodic scheduling, genuine provider settlement/recovery and C06/controller
proof remain later owners. This local receipt is neither controller authority
nor a process stop grant. C03c/d remain unchanged. No R04/R03a3/runtime aggregate
or distributed idle/termination acceptance is claimed.

Final executed Cargo commands (each with the exact environment prefix above):

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib final_drain_receipt_ -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::wire -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib reply_handoff -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Final runtime results, all exit 0: focused 12 passed (build 39.32s, tests
0.43s); owners 86 passed (0.73s); admission 16 passed (0.53s); wire 8 passed
(0.01s); reply handoff 5 passed (0.03s); heartbeat 41 passed (0.66s). The
purposeful supervisor panic prints its panic hook inside passing lifecycle tests.
These are the final combined affected component runs; earlier prerequisite
passes are not substituted for them.

Intermediate invocation history (log suffix, exit, result):

| Log | Exit | Result |
| --- | --- | --- |
| red | 101 | Missing-API compile-red |
| green | 0 | 1 passed; build 37.63s |
| composition | 101 | Test helper/ControlKey compile errors |
| composition2 | 101 | 1 passed, 1 signing-fixture failure; build 38.40s |
| composition3 | 101 | 6 passed, 3 fixture failures; build 35.10s |
| composition4 | 0 | 9 passed; build 37.04s |
| focused | 101 | Test snapshot field compile error |
| focused2 | 0 | 11 passed; build 47.70s |
| final-focused | 0 | 12 passed; build 39.32s |

The first four invocations used `--lib final_drain_receipt_` without a
`--test-threads` override. The subsequent focused invocations used that same
filter with `-- --test-threads=2`, as listed above. All used the required prefix
and common flags. No behavioral-red claim is made for these fixture failures.

Strict library/tests Clippy passed without diagnostics, exit 0, 25.72s. Scoped
rustfmt `--check --edition 2024 --config skip_children=true` passed on all seven
owned existing Rust files. Direct checks on those files plus this new note reject
trailing whitespace and require exactly one final newline. Scoped `git diff
--check` passed; direct checks include untracked content.

Final snapshot comparisons: admission.rs, drain_observation.rs and
payload_owner.rs contain only the described changes; the entire original
installation_tests.rs and wire.rs files remain exact prefixes; tests.rs and
wire/tests.rs remain byte-identical. Refreshed diffs and `comparison.txt` are in
the snapshot directory named above. This verifies present content only.

All initiated Cargo, test, Clippy, formatting, shell and fixture-owned processes
have exited; the final process check found no remaining Cargo/rustc/test process.
Rust implementation and Cargo/build ownership are explicitly RELEASED to the
parent for NEW independent specification review, fixes/recheck and NEW full
quality review. No rubric was changed and no independent review result is claimed.

## Parent correction: retain cleanup after watchdog expiry

The NEW specification review found one P2 in test teardown, with no production
receipt finding. The shared finish_distributed helper could stop the WorkerPool
after owner drain timed out, then drop the shutdown future holding the original
join handle on its own timeout. The new failed-process fixture repeated the
shutdown pattern. The parent also corrected the identical pattern in the two
preserved failed-owner fixtures that use the same quarantine helper.

Only installation_tests.rs and this appended record changed for the correction.
Pre-correction copies are under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-c3d-teardown-geuk5w_e/`,
using repository-relative names. All six other Rust files in that snapshot are
byte-identical, including every production receipt/lifecycle/authentication file.
The earlier whole-test-prefix statement describes the original delivery. The
parent now changes the shared teardown helper and three existing quarantine/join
blocks, preserving their outcome assertions, and appends two regression tests.
The unchanged PendingProofFake helper remains in its original location.

finish_distributed delegates to the same helper with an explicit watchdog
duration, allowing short real timeouts in the regression. It pins and observes
the original drain future, preserves the timeout result, then awaits that same
future through settlement/reaping while the pool remains available. It likewise
retains and awaits the original shutdown future through actual worker joining
before filesystem cleanup and assertions. Each of the three failed-owner paths
now pins its original quarantine and shutdown futures and continues awaiting
them after recording timeout. These are test-only ownership changes, not a new
production recovery or provider-settlement mechanism.

Two compiling behavioral regressions were observed failing before the fix:

- A real process supervisor is gated after cleanup but before return. The old
  helper stops the pool and removes scratch while that registered handle remains.
  The test releases the gate and reaps the surviving original owner before its
  final failed assertion. After the fix, cleanup remains pending, scratch remains,
  and a real pool job still runs while the owner is gated. The helper reports the
  recorded timeout only after original reaping, worker joining and path removal.
- A real accepted worker job is gated during shutdown. The old helper returns
  and removes scratch before worker exit. The test releases and receives the
  actual job and waits for a worker-runtime destruction witness before reporting
  failure. That red path cannot retroactively recover the already detached join
  future. The fixed helper retains that original future and awaits its actual
  join before returning; the regression observes pending cleanup and retained
  scratch until the gate opens. Both tests expect the helper's eventual timeout
  assertion, captured in an owned and awaited Tokio task, after teardown.

The existing finite test gates are released before the continued waits. A
watchdog records failure rather than granting permission to abandon cleanup.
No injected quarantine-timeout execution is claimed; the three quarantine paths
were inspected and their existing normal/failure tests passed.

Parent commands used the original required Cargo environment/flags and target:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib final_drain_cleanup_watchdog_keeps_owner_drain_before_pool_shutdown -- --nocapture
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib final_drain_cleanup_watchdog -- --nocapture
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner -- --test-threads=2
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Logs are `/tmp/meshdb-c3d-teardown-{red,red-both,green,owners,clippy,final-owners,final-clippy}.log`.
The first red failed one test (build 40.26s); the combined red failed both (build
1m36s, tests 0.03s). The same two groups then passed (build 2m56s, tests 0.22s).
After the remaining quarantine edits and final formatting, all 88 owner groups
passed (build 33.93s, tests 0.95s), including both regressions and all original
receipt groups. Strict library/tests Clippy passed in 21.31s. The parent then
strengthened the worker-exit test to return, retain and await its witness task's
JoinHandle as well as observing destruction at worker runtime exit. That added
assertion has green-only evidence. The final same owner command passed all 88
groups again (build 36.71s, tests 1.35s), followed by strict library/tests Clippy
passing in 23.02s. No additional admission/authentication/heartbeat run is
claimed; those files are unchanged and their implementer-reported passing
evidence is reused.

A mechanical replacement script expected two matching quarantine blocks, found
three and aborted before writing. The parent then collected the formatter's
session exit only after submitting the first focused-green Cargo command. This
was a sequencing deviation; that invocation is not evidence of universal build/
format serialization. Both processes exited. Subsequent edits/formatting finished
before the final owner run, whose exit preceded Clippy. No two Cargo processes
were submitted concurrently. Final scoped formatting, direct whitespace/newline
and snapshot comparisons passed. All initiated processes exited before handback.

A NEW specification re-check is required before full independent quality review.
No aggregate R04c3, runtime, provider or distributed guarantee follows.

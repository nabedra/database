# DN-R04c3c registered proof-heartbeat lifecycle

This implements only the third sequential A22 c3 slice. It exposes private
reaped evidence, with no DrainObservation, typed DrainReply, idle/termination
truth or runtime authority. Parent owns NEW specification and full quality
reviews and rubric updates. No aggregate R04c3 completion is claimed.

Read A09/A20/A21/A22 including the parent execution split, shared distributed
plans/contracts, completed b2/c1/c2/c3a/c3b and C03c/d interfaces and notes.
Applied test-driven-development and unslop. No applicable repository or ancestor
AGENTS.md was found. No delegation occurred.

## Files and preservation

Pre-edit copies use repository-relative paths under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-DN-R04c3c-i6ojx57k/`.

Changed existing files are admission.rs, payload_owner.rs, node/mod.rs and appended
payload_owner/installation_tests.rs, all under server/src/node. New files are
node/drain_observation.rs and this note. payload_owner/tests.rs remains byte
identical. The entire pre-existing installation_tests.rs prefix remains byte
identical, including c3a failed-process teardown and all three c3b regressions.
Snapshot diffs are saved alongside the copies. Node mod.rs adds only the private
module declaration. Admission adds only exact BarrierSample observer comparison.
No contracts, rubric, heartbeat bodies, storage, Fake, WorkerPool, disk/spool,
auth/wire/replay, controller, runtime, HTTP, Cargo or dependencies changed.

Shared modified/untracked work was preserved. No branches/worktrees, commit/push,
downloads, new target, target cleanup, network/cloud/paid calls or protected
secrets/.env/profiles were used. Builds used existing server/target only.

## Private APIs and fixed ownership

PayloadOwners::publish_proof consumes an actual BarrierSample, retains an
Arc<Storage<B>> and takes the original absolute Instant deadline. It checks the
actual sample against the original ReceiverState under its existing mutex.
There are no binding, sequence, occupancy, settlement, acknowledgement or
exemption inputs. Cycle allocation and checked-sample binding allocation happen
outside ReceiverState. The slot remembers the first binding and never resets it.
The sole C03c writer is constructed from that checked binding outside
ReceiverState and installed into the slot's retained async mutex. Later cycles
reuse that same writer, including its original sequence/token history.

The fixed slot contains one optional registered cycle, one original JoinHandle,
one checked u64 allocation position with sticky exhaustion, one writer position
and one outcome position. Busy refuses immediately. No transport/compute permit,
accepted/payload/preparation bound, event collection, task queue or executor
framework was added. The erased Arc contains only retained concrete storage,
with no callable interface. The typed invocation separately borrows that same
retained concrete Storage<B> and calls actual HeartbeatWriter::publish.

The runtime handle is checked before reservation. The locked reservation captures
the fixed cycle/writer references into a private ProofInvocation. After unlocking,
its future is constructed and spawned. The original handle is then synchronously
published under ReceiverState, with no await or callback between spawn and
publication. After unlocking, publication broadcasts to existing reaper waiters
before activation, including waiters that inspected the empty placeholder.
There is no recoverable post-reservation spawn-error API; runtime cancellation
produces a registered failed original handle rather than an empty rollback.
A cfg(test) pre-spawn gate releases ReceiverState while the empty placeholder
remains registered. Other gates occur after original-handle publication only.

| State | Registered ownership | Exit |
| --- | --- | --- |
| Available | sole writer position, pinned binding after first use | checked reservation |
| Registered | exact sample/request/observer, cycle Arc identity, concrete storage, activation and original handle | actual invocation result |
| Settling | same ownership plus full actual C03c outcome | successful original handle poll |
| Reaped | same sample/outcome, returned retained writer, consumed original handle and bounded cycle marker | exact observer takes once; retirement and outside releases |
| Failed | same original storage/writer/sample/result/handle ownership, even after panic or ambiguous return | no production recovery in this slice |

The async mutex owns the writer independently of the task stack. The registry
owns storage independently of the task stack. Supervisor panic therefore cannot
lose either original owner. The original handle remains stored and is polled in
place once to actual termination. is_finished is never reaping evidence.

ProofTicket Drop only signals memory cancellation and broadcasts. The registered
cycle remains counted until its cancellation handoff completes.
It never aborts the supervisor or cancels the accepted publish future. That
invocation runs through its original deadline. Cancellation has a completion
handoff before registry retirement, so reaping cannot race its outside recording.
ReapedProof retains the original sample and full outcome privately. Immutable
sample()/acknowledgement() accessors are reserved for c3d. There is no Clone,
Serde, unchecked constructor or detached-parts API. Exact weak-Slot observer
identity plus the unforgeable ticket Arc and checked cycle ID gate single take.
A different observer at a colliding numeric ID cannot consume the result.

Completion Drop releases its owned sample/outcome before marking retirement.
It never reacquires ReceiverState. The marker remains registered until reaping
can extract the cycle and perform counted outside-lock releases. Even a live
cancelled ticket cannot retain buffered sample/outcome/storage past that release;
only its memory identity remains. Ticket/completion drops belong to this exact
proof-only lifecycle and do not advance WorkEvidence. Actual observer, response
and unrelated delivery changes retain their ordinary work recording.

## Settlement, reaping and shutdown

Only actual AcknowledgedHeartbeat from C03c's timely nonempty-token success is an
acknowledgement. No observation, error or timeout constructs one. Exhausted and
definite InvalidCatalog/LimitExceeded/Conflict error categories can complete
without acknowledgement. All other errors, Unknown, ObservedExact and the
Conflict outcome remain Failed. Conflict is deliberately conservative because
C03c can return it after a deadline-dropped reconciliation read. No later read,
boolean, deadline or test quarantine is a production settlement certificate.
Later storage/runtime work must supply real provider-settlement recovery.

The existing reap_shared polls the proof handle with its existing stable broadcast
waker, then polls ordinary owners. wait_proof enables Notify before inspection,
as wait_reap already does. Cancelling a waiter leaves the handle and broadcast
waker registered for survivors. Actual successful task termination and retained
outcome classification precede Reaped. take_proof also refuses pending outside
releases. Ack buffering or supervisor cleanup alone cannot release ownership.

RetentionSnapshot adds proof_cycles and proof_results and includes original
proof handles in supervisors and proof failure in failed. Empty registration,
buffered outcome, failed cycle, externally held completion and counted outside
release all block successful whole-process drain. close_and_drain cancels every
original owner and proof cycle, with no observer exemption. Cancellation of the
drain future preserves ownership and allows another drain to resume.

Only the concrete proof registration/invocation/reap/release transitions omit
work-generation recording. Ordinary owner mutations and counted releases retain
their original recording. There is no generic exemption switch. Full actual-owner
classification independently reports retained proof ownership as WorkPresent.
An otherwise unchanged original sample remains barrier-matchable while its reaped
completion is live; an independent response DeliveryHold transition invalidates
it even after that hold disappears.

c3d must atomically validate the exact registered reaped marker and original live
observer, full current root/boot/init/drain, generation, positive actual ack and
all actual owners, then compose the final typed receipt. This slice implements
none of those final receipt predicates or encoding. Later periodic publication
must use this same serialized writer owner, with no independent writer or loop.

## Causal evidence and provenance

The original seven appended groups use signed initialized receivers, real accepted Drain
controls, existing Fake, actual C03c and existing worker teardown. They cover
Busy and unchanged charges; terminated-but-unreaped refusal; exact one-use take;
pre-I/O/ack/return gates; two waiter tasks with one cancelled and joined; empty
placeholder shutdown; buffered-ack drain cancellation/resumption; foreign numeric
collisions; same-writer sequence 2 at the final u64 cycle and permanent exhaustion;
independent delivery history; definite no-ack; lost response/exact readback;
Unknown; supervisor panic with buffered ack; and provider work surviving the
original deadline after ticket Drop.

The pending-provider fixture delegates actual storage operations to existing
Fake. It retains the sole provider-effect JoinHandle, releases its finite gate
and joins it before test quarantine. This demonstrates that later successful
effect completion does not clear the failed production slot. Failure quarantine
is cfg(test), requires actual original supervisor termination, and is used only
after all controlled Fake effects have settled. It grants no production recovery.
All gates have finite watchdogs. Tests record fallible observations, release all
gates, join waiter/effect tasks, drop controls/processes, drain/reap originals,
join WorkerThreads and remove scratch before outcome assertions. Two-receiver
cleanup runs in separate independently joined Tokio tasks. No failed ownership
was hidden by production retention changes.

The first baseline test preceded implementation and failed with missing-module
and missing-API compile-red, exit 101. The first implementation then passed it.
Later tests were added incrementally; their first passing runs are green-only
coverage. Two intermediate compile failures were a test delimiter typo and the
local Result alias in and_then, both exit 101 and neither behavioral-red. The
Conflict retention assertion later failed after complete teardown, exit 101;
removing Conflict from normally reapable outcomes made it pass. That is observed
behavioral-red for this conservative classification, not evidence of a real
provider timeout in that particular immediate Fake conflict case. The pending
provider test separately exercises actual deadline ambiguity. The deliberate
supervisor panic prints its panic hook while its lifecycle test passes.

## Validation and handback

Every Cargo invocation used the exact environment prefix below, existing
server/target, and the common manifest/locked/offline/no-default-features flags.
Actual process exit was awaited before starting another Cargo invocation;
session IDs and partial diagnostics were never treated as process completion.

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib proof_cycle_registered_ack_reaps_once
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib proof_cycle_ -- --nocapture
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner -- --test-threads=2
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission -- --test-threads=2
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat -- --test-threads=2
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Sequential execution history, including intermediate failures:

| Invocation | Result |
| --- | --- |
| baseline, first | missing-API compile-red, exit 101 |
| baseline, second | 1 passed, exit 0, build 31.49s |
| lifecycle group, first | 3 passed, exit 0; unused test hook warning |
| lifecycle group, second | delimiter compile error, exit 101 |
| lifecycle group, third | Result alias compile error, exit 101 |
| lifecycle group, fourth | 6 passed, exit 0, build 31.39s |
| lifecycle group, fifth | 7 passed, exit 0, build 31.55s |
| owner group, first | 71 passed, exit 0, build 32.38s; three unused assertion variables |
| lifecycle group, sixth | 6 passed, 1 Conflict-retention behavioral failure, exit 101 |
| owner group, final | 71 passed, exit 0, build 31.85s, tests 0.54s, no warnings |
| admission group | 16 passed, exit 0, tests 0.53s |
| heartbeat group | 41 passed, exit 0, tests 0.64s |
| Clippy, first | needless option dereference and late initialization, exit 101 |
| Clippy, final | passed without diagnostics, exit 0, 20.98s |

The initially unused assertion variables were connected to the outcome assertion
before the final owner run. The final Clippy corrections pass the same context
directly to reap_locked and move a test declaration to its initializer; they
change no behavior. The final runtime passes precede those two style-only edits.
Unchanged auth/protocol/storage matrices reuse reviewed prerequisite evidence;
no broad test rerun is claimed beyond the explicit affected groups above.

Scoped rustfmt used edition 2024 with skip_children=true on owned Rust files,
including a final --check. Direct checks include untracked new files, reject
trailing whitespace and require exactly one final newline. Snapshot comparisons
confirm tests.rs unchanged and the complete old installation-test prefix intact.
Final checks and snapshot diff updates passed.

Self-review covered fixed bounds, checked IDs, sole writer/provider retention,
original handle identity, panic/cancellation paths, exact observer/request/boot
association, outside-release ordering, lock order, precise generation exemption,
full snapshots/shutdown and bounded test teardown. There are no disk owners or
new provider-settlement recovery APIs in this slice. Independent parent reviews
are still required. Configured startup, HTTP/runtime, final idle receipt and
aggregate R04c3 acceptance remain open.

All initiated processes have exited. Rust implementation and Cargo/build
ownership are explicitly RELEASED to the parent for NEW independent reviews.

## Parent specification corrections

Independent review found two P2 defects: proof-only ticket/completion drops
advanced work generation, and task allocation/spawn occurred while ReceiverState
was locked. The parent corrected both. Pre-correction repository-relative copies
and parent diffs are under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r04c3c-correction-s41so9w_/`.

Cycle no longer holds WorkEvidence or starts work transitions for its own
cancellation and completion retirement. Its existing cancellation_done, consumed
and retired flags retain the concrete handoffs. Polling additionally requires
any started cancellation to finish before retiring even a completed result.
Completion retirement still follows dropping its original sample/outcome, and
the registered cycle remains counted until the reaper extracts it. The existing
pending_releases count spans provider/result release outside ReceiverState.
These changes do not exempt observer cancellation or other actual work.

The locked reservation now captures only fixed retained references and test gates
in a private ProofInvocation with private fields. After releasing ReceiverState,
the caller constructs the typed future and performs Tokio task allocation/spawn.
It then publishes the original handle synchronously before activation or return.
There is no await or callback between spawn and publication. The placeholder
remains registered throughout. The typed invocation's async body is byte-identical
to the pre-correction body; only its construction boundary moved.

Two appended regression groups use actual signed observers and Fake-backed
acknowledged publication. The drop group runs both ticket-first and
completion-first orders, retains a separate original checked sample, and gates
the real outside-lock cycle release. It observes retained completion ownership,
refusal during pending release, unchanged work generation after release and
safe late ticket Drop. All fixtures and the reaper thread finish cleanup before
the assertions. The spawn group observes receiver try_lock immediately before
actual runtime.spawn, with successful publication/reaping as a positive control.
The original placeholder-shutdown test remains unchanged and verifies inspection
while preparation is paused outside the lock. No test hook occurs between spawn
and handle publication.

Parent-executed commands used the required environment prefix and existing
target. All process exits were observed before the next Cargo invocation:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib proof_cycle_regression_ -- --nocapture --test-threads=2
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner -- --test-threads=2
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

- Compiling behavioral red: both regression groups failed after teardown. The
  ticket-first sample stopped matching after ticket Drop and remained invalid
  after cycle release. The spawn probe reported locked receiver access despite
  successful acknowledgement/reaping. Both drop-order fixtures had completed
  teardown, but the first failed assertion stopped before the reverse-order
  assertion; no separate behavioral-red claim is made for that second assertion.
- Focused green: both groups passed in 0.05s after a 42.77s build.
- Full affected owner suite: 73 passed in 0.78s after a 1.14s build. This includes
  all nine proof lifecycle groups, independent-delivery invalidation, placeholder
  shutdown and the existing c3a/c3b corrections.
- Strict library/tests Clippy: passed in 27.77s without diagnostics.
- Scoped rustfmt check, direct whitespace/final-newline checks, snapshot
  comparisons and scoped git diff checks passed.

Only drain_observation.rs, payload_owner.rs and appended installation tests
changed in Rust. The complete prior installation-test prefix, admission.rs and
payload_owner/tests.rs match the parent snapshots. This verifies their present
content, not historical execution. The earlier sixteen admission and forty-one
heartbeat passes are reused; no rerun of those unchanged suites is claimed.

All initiated processes exited. Parent Rust/Cargo ownership is released for a
NEW specification re-check, then full independent quality review. R04c3c remains
unchecked. Final drain receipts, provider-settlement recovery and runtime
integration remain later obligations.

## Parent quality correction: wake existing shutdown waiters

Full independent quality review found one P2 defect. A shutdown waiter could
inspect the empty proof placeholder, reap every ordinary owner and sleep before
the proof handle existed. Without a publication notification, no reaper waker was
registered on that handle, so its later termination could leave shutdown asleep.
An already-cancelled cycle's ticket Drop supplied no new notification.

The parent added one shared reaper broadcast after publishing the original handle
and releasing ReceiverState, before activation. The synchronous spawn-to-handle
publication sequence remains uninterrupted. No ownership, outcome classification,
generation, cancellation or reaping condition changed. Pre-correction copies and
parent diffs are under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r04c3c-wake-oih0lcfp/`.

The appended proof_cycle_waiting_drain_survives_handle_publication regression
uses the actual signed observer, Fake-backed writer and existing pre-spawn gate.
It starts one close_and_drain task, waits until that task has reaped all ordinary
owners and releases, and verifies that only the empty proof cycle remains with
zero PUTs. It then releases preparation while retaining the returned ticket and
requires that same shutdown task to finish. It neither calls wait_proof nor
manually reaps or restarts drain during that observation.

If the observation times out, a rescue broadcast runs only after recording the
failure. It permits the same original task to settle for teardown. Its handle is
still joined, or aborted and joined on a second watchdog failure. The test then
completes normal fixture teardown and joins workers/removes scratch before
asserting. The successful path requires no rescue. Its final observations check
one PUT and no retained proof cycle, supervisor or pending release.

Parent-executed commands used the same required environment and existing target,
with actual exit awaited before each next Cargo invocation:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib proof_cycle_waiting_drain_survives_handle_publication -- --nocapture
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner -- --test-threads=2
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

- Behavioral red: the compiling regression failed after 10.03s with the original
  drain waiter missing handle publication. Its rescue and teardown completed,
  and all earlier lifecycle/count assertions passed before the final failure.
- Focused green: 1 passed in 0.03s after a 37.41s build, without rescue.
- Full affected owner suite: 74 passed in 0.65s after a 1.49s build, including all
  ten proof lifecycle groups and prior cancellation/reaping regressions.
- Strict library/tests Clippy: passed in 28.68s without diagnostics.
- Scoped rustfmt, direct whitespace/final-newline checks and scoped git diff
  checks passed. The previous installation-test file remains an exact prefix.
  drain_observation.rs is byte-identical to the correction snapshot. Production
  differences are only the broadcast and its explanatory comment.

Earlier admission/heartbeat passes remain reused evidence. All initiated
processes exited, and Rust/Cargo ownership is released for a NEW quality re-check.
c3c remains unchecked until that review passes; c3d and runtime guarantees remain
open.

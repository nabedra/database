# DN-R04c3b checked work evidence and bound samples

This implements the second bounded c3 slice. Samples are local evidence only.
They confer no idle, termination, publication, authenticated reply or distributed
completion authority. No heartbeat writer is constructed. c3c and c3d remain
separate work. Parent owns NEW independent specification review, then NEW quality
review and rubric status.

## Preservation and scope

Pre-edit copies of all five owned existing files, with repository-relative paths,
are under `/tmp/meshdb-DN-R04c3b-20261112T012702/`. Changed sources are
`server/src/node/admission.rs`, `server/src/node/payload_owner.rs` and appended
`server/src/node/payload_owner/installation_tests.rs`. This note is new.
At original handback, `payload_owner/tests.rs` and `admission/receiver_tests.rs`
were byte-identical. The parent correction below adds one test hook to the former.
Original installation tests, including all classifier assertions and the parent's
failed-process teardown correction, remain intact.

No delegation, branch/worktree, commit/push, dependency modification/download,
alternate target, target cleanup, network/cloud/paid call, secret/profile access,
wire/auth/replay implementation, heartbeat, controller, storage, WorkerPool,
frame/stream/spool, engine, runtime, HTTP or node declaration change occurred.
Existing shared tracked and untracked work was preserved. Used the TDD and unslop
skills. No earlier component PASS is claimed as runtime integration evidence.

## Concrete private APIs and ownership

`LocalAdmission` owns an `Arc<WorkEvidence>` distinct from its allocation
`generation`. Its bounded `PayloadRegistry` clones that same cell and each actual
Slot retains it. Legacy registries get their own cell. The cell contains one
checked u64 generation, a pending count, an unrecorded bit and sticky exhaustion.
It owns no handles, resource guards, request payload, disk or authority. There
is no event collection, waiter queue, task per change or caller notification
endpoint. Concrete internal mutations perform the recording.

`BarrierPreparation::prepare(&Control, Arc<ReceiverIdentity>)` validates a complete
OwnershipObservation and current receiver membership outside ReceiverState. It
requires a verified Draining member with its durable drain and session. Complete
root validation, identity checking, hashing/encoding, verifier String cloning and
HeartbeatBinding construction occur here, outside the receiver lock.

`ReceiverState::sample_barrier(prepared, process_identity, original_control_key)`
retains the preparation, current initialized identity, exact weak-slot key issued
by the actual ControlLease, original immutable authority owning cell, work
generation and a HeartbeatSample derived from LocalAdmission's actual locked
occupancy and permanent drain barrier. It neither clones nor reparses a proof.
The original RequestBinding remains in its original OnceLock cell, including the
accepted request association and sequence. Transfer of the one-use body remains
independent of that cell.

Before recording completed outside changes, sampling checks the original boot,
complete current high-water observation, initialized identity, permanent barrier,
exact durable Drain, original authenticated accepted request cell and c3a actual
owner classifier. Foreign boot/key/init, stale or different-content root and
invalid anchors therefore refuse before sample recording changes history.
Integrity failure remains sticky. The sample requires Verified Draining local
eligibility. It does not update the durable high-water root from supplied input.

`BarrierSample` has private fields, no Clone/Serde/unchecked constructor and
value-free Debug. Borrowed `binding()`, `heartbeat()` and `request_binding()`
provide immutable inputs for later composition. `ReceiverState::barrier_matches`
rechecks current boot/init/root/barrier/drain/request ownership, pending releases,
recorded generation and the actual c3a classifier under the common receiver
mutex. It checks the evidence cell before and after classification, catching
concurrent cancellation or worker changes. InvalidOwner refuses. WorkPresent can
produce truthful samples with actual metrics. ExemptOnly remains owner evidence.
No caller can supply occupancy, idle, settled or proof-exemption flags.

## Mutation inventory

Under ReceiverState or the standalone LocalAdmission test owner:

- Complete durable observation changes, first permanent drain barrier and sticky
  equal-revision integrity invalidation record changes. Identical complete root,
  eligibility and retained coordinator facts do not advance generation.
- Actual operation/self-dependency reservation, partition/dependency-use
  reservation, new dependency open, retained account acquisition and start record
  changes. Capacity and checked generation checks precede the mutation.
- Execution, delivery, cleanup, record reaping, dependency close/reap and account
  release record changes. Accepted Closing uses retain their original behavior.
- `rollback_work` records removal before restoring allocation generation.
  LocalControlReservation rollback records dependency removal while restoring
  allocation generation. Its prepare checks evidence capacity before consume;
  commit records the actual close synchronously and infallibly afterward.
- `complete_initialization` checks evidence room before consuming the actual
  ProcessReady/configured InitializedWorker transition. Only the existing
  explicit cfg(test) initialization fixture bypasses configured issuance.
- Registry preparation, entry/pair and response reservations check room before
  replay.consume. Parent claims check room before incrementing ownership.
  Registration, original handle publication, request installation, work/account
  installation, process-used/boot position and retained request transfer record
  their actual changes. Failed activation records failure without freeing owners.
- Parent reservation rollback records the decrement and retains its existing
  bounded notification handoff. Shutdown records first registry closure and
  changes to dependency closing. Repeated unchanged reaper polls do not record.
- Original task termination, registry removal/position/record completion and
  release completion record changes. Pre-spawn failure retains its counted
  outside-lock guard release and records both removal and completed release.

Outside ReceiverState:

- Slot cancellation and deadline cancellation record only the first actual
  cancellation while holding the existing handoff mutex. Repeated cancelled()
  reads without a new deadline transition do not record.
- ParentHold and DeliveryHold Drop begin a pending transition before decrement,
  retain it through notification, then settle after child/delivery locks release.
  Original response_live changes remain synchronized with delivery count; extra
  holds cannot become original responses.
- New command/receive/send submission and delivery acquisition check room before
  submission/retention. Worker execute/produce, successful initialization,
  production completion, cleanup, failure and dormant abandonment own pending
  transition guards. Worker disk destruction and original guard retention remain
  unchanged. Already accepted worker execution/Discard can settle at exhaustion.

Internal helper admission versus settlement modes concern generation capacity
only. They provide no owner or heartbeat exemption. No production initialized
success shortcut or configured R07 capability was added.

## Handoff, lock order and exhaustion

Outside transitions advance generation and increment pending before mutation.
Their RAII settlement advances generation again, decrements pending and leaves
unrecorded set. A valid sampler or reaper, holding ReceiverState, records completed
changes by clearing unrecorded only when pending is zero. Live matching refuses
pending or unrecorded state. New sampling validates its exact anchors first and
then records completed handoffs. The generation is never reconstructed from
current occupancy, so changes that appear and disappear before an observation
remain visible even after their slots are removed.

The receiver's existing pending_releases and rollback_notifications additionally
cover extracted guards and notifications until the reaper's final relock. A
sample cannot qualify while that release is gated. After release, the generation
still differs. No pending work is erased by allocation-ID rollback.

Evidence mutex guards are short and never survive a slot lock, await, disk work,
notification or acquisition of ReceiverState. Transition tokens contain an Arc,
not a locked mutex. ReceiverState may inspect slot synchronization and the
short evidence mutex. Outside Drop/worker/cancel paths never acquire the
receiver. Cancellation may record under handoff, but no evidence mutex holder
waits for handoff. Disk/handoff/delivery/child locks release before any existing
receiver acquisition. No recursive receiver acquisition was introduced.

Overflow sets permanent exhaustion. New reservations/submissions/delivery holds
refuse, samples refuse, and generation never wraps, resets or recycles. Settlement
recording has no exhaustion refusal: accepted cancellation, worker cleanup,
Drop, original-handle reaping and outside guard release keep progressing with
ownership preserved. All new admission failures remain before replay.consume;
installation after successful consume stays synchronous and infallible.

## Causal tests and provenance

Five appended signed-fixture/real-WorkerPool tests cover exact association and
sequence, actual derived metrics, delivery history after counts return to their
baseline, unchanged root/repeated reap polls, equal-revision differing content,
foreign keys and receivers, wrong initialization/drain, cancelled observer,
fully reaped dormant preparation, blocked worker Discard and pending guard
release. Busy owner sampling succeeds; active cleanup/release sampling refuses.
The old busy sample fails after real work settles, and a fresh settled sample
matches. Existing open/closing dependency, account/publication, failed-owner and
concurrent/cancelled-waiter assertions remain unchanged in the affected group.

Two embedded arithmetic tests cover actual reservation/removal with allocation
rollback, non-rewindable work evidence, exhaustion with accepted work settlement,
unchanged complete root and sticky integrity failure. The signed exhaustion test
prepares a real control, exhausts evidence through a cfg(test) fixture, invokes
the actual admission transaction, and verifies the same signed request sequence
is still consumable by the original replay owner. The test-only replay probe
consumes that unused request without fabricating authority or installing work.
It is not a production acceptance path.

The first signed sample test preceded implementation and produced missing-API
compile-red, exit 101. It is not behavioral-red. Its first implementation passed.
Later assertions are green-only coverage. The first arithmetic run had one
failure because the fixture's full operation capacity refused before reaching
the intended exhaustion check. Increasing that fixture's operation capacity
made the intended branch reachable. That failure is not evidence of a production
bug. No behavioral-red claim is made for this slice. Self-review added the explicit
unrecorded handoff bit after initial green runs; affected tests reran afterward.

All added gates/waits use existing finite watchdogs. Tests release gates, settle
accepted work, dispose/reap originals, join the reaper and WorkerThreads and
remove scratch before final assertions. The original handback incorrectly stated
that two-receiver teardown used separate joined Tokio tasks. Specification review
found a direct join of cleanup futures; the parent correction below fixes it.
No new failed-owner quarantine or production retention relaxation was needed.

## Validation and handback

Every Cargo command used existing server/target and this exact environment:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Every Cargo command included `--manifest-path server/Cargo.toml --locked --offline
--no-default-features`. Actual process exit was awaited before the next Cargo
command, including session-returning calls. Commands, with the required prefix
on the same shell line, were:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib barrier_sample_signed_delivery_history
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib work_evidence_
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib barrier_sample_ -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner -- --test-threads=2
```

Final command results and ownership release follow after validation completes.

c3c must own ONE actual writer/provider and original registered proof-heartbeat
lifecycle, including exact closed-cycle exemption and settlement/reaping checks.
c3d must compose final DrainObservation and typed reply with fresh authority and
all actual owner/generation checks. These samples do not anticipate or stand in
for either obligation. Parent must obtain fresh independent specification and
quality review. Full R04c3, authenticated idle, provider settlement, configured
startup, HTTP/runtime and distributed completion remain unclaimed.

Final validation, all exit 0:

- Final payload-owner group after the unrecorded-bit correction: 61 passed,
  build 31.36s, tests 0.40s. This includes all ten unchanged c3a groups and the
  concurrent/cancelled-waiter regression.
- Final admission group: 16 passed, build 31.66s, tests 0.55s.
- Replay group: 3 passed, build 1.39s, tests 6.64s.
- Reported final focused sample group: 5 passed, build 0.88s, tests 0.03s.
  The handback incorrectly attributed this result to a separate-task teardown
  improvement that was absent from the delivered source. The parent correction
  and validation below supersede that source claim.
- Strict library/tests Clippy: no diagnostics, 21.51s.
- Scoped rustfmt edition2024, skip_children=true, including --check: passed.
- Direct untracked-aware whitespace and exactly-one-final-newline checks on all
  six owned files: passed. git diff --check: passed.
- Snapshot comparisons confirm byte-identical untouched tests.rs and
  receiver_tests.rs and the entire original installation-test prefix. Diffs are
  saved next to the snapshots. The first diff-saving script used Python keyword
  arguments unsupported by the local Python 3.6 and exited 1; the compatible
  retry succeeded. This did not alter repository source or validation results.

Additional exact Cargo commands, using the same required environment and common
arguments above, were:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::replay -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Self-review covered all concrete mutation/cancellation/Drop paths listed above,
checked arithmetic and permanent exhaustion, original authority/weak-slot
identity, bounded handoff recording, no receiver recursion, release ordering,
causal teardown and shared snapshot preservation. All initiated processes have
exited. Rust implementation and Cargo/build ownership are RELEASED to the parent.
No independent review or rubric result is claimed.

## Parent specification corrections

The independent specification review found three defects. Parent snapshots before
correction are under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r04c3b-correction-xqiz6ccl/`,
with repository-relative paths and saved parent diffs. These copies establish
current content relationships, not historical execution.

Pair preparation now begins a checked evidence transition under ReceiverState
before changing `next` or either preparation position. This closes the interval
before supervisor publication and refuses exhausted evidence without inserting
placeholders or spawning either task. A test-only bounded gate pauses that real
interval. The regression observes two placeholders, no published handles and an
invalidated old sample. A second regression checks exhausted refusal, unchanged
identity/positions and returned transport ownership. Both release their original
owners, reap, join workers and remove scratch before asserting.

`WorkEvidence::begin(true)` checks room for both entry and settlement increments.
All three acceptance branches also perform the final checked evidence admission
after their fallible reservations and immediately before replay consumption.
That successful check admits the work; subsequent installation remains
infallible, including if later settlement exhausts evidence. Refused reservations
roll back through the existing mutable state, while accepted cleanup and reaping
continue at exhaustion. No evidence mutex guard spans replay or installation.

The signed control regression reaches actual response reservation before a
test-only hook sets generation to MAX minus one, two or three. The first case
must refuse the two-increment transition. The second can settle that transition
but must refuse at the final admission check. Both leave the same signed sequence
unspent. The third admits and spends it once. Each case retains and then reaps
the original process, observer and prepared owner, restores transport capacity,
joins workers and removes scratch before the combined assertions.

The two-receiver sample test now spawns each `finish_distributed` in its own Tokio
task and awaits both handles before asserting either result. A panic in one
cleanup therefore does not drop the other cleanup future. This is a source fix
with passing coverage in the affected suite; no injected cleanup-panic run is
claimed. The earlier report and rubric statement describing this change before
it existed were inaccurate.

Only the four listed Rust files changed in this correction. `tests.rs` adds one
cfg(test) response-generation hook, and `payload_owner.rs` adds its bounded
pair-preparation gate. The original installation-test prefix from the implementer
snapshot still matches byte-for-byte, including the c3a failed-process cleanup
correction; `admission/receiver_tests.rs` remains byte-identical. No later c3c/d
implementation, authority or provider-settlement shortcut was introduced.

Parent-executed validation used the same required environment and common Cargo
arguments above. Actual Cargo exits were observed before launching the next Cargo
command. One earlier focused invocation's terminal output was unavailable after
context compaction. The parent confirmed no Cargo or test process remained, then
repeated the focused red; no result is claimed for the unavailable output.

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib work_evidence_regression_ -- --nocapture --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::replay -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

- Compiling behavioral red: all three new groups failed after teardown. The pair
  interval observed `(2, 0, true)` instead of `(2, 0, false)`, exhausted pair
  preparation succeeded, and both insufficient response budgets spent replay.
- Focused green: 3 passed in 0.09s after a 33.17s build.
- Affected payload-owner suite: 64 passed in 0.54s, including the corrected
  two-receiver teardown and all prior classifier and association regressions.
- Admission suite: 16 passed in 0.57s. A comment-only clarification was applied
  after its compilation while that test session was still open. The subsequent
  replay build and Clippy used the current source; no Cargo commands overlapped.
- Replay suite: 3 passed in 8.35s after a 33.86s build.
- Strict library/tests Clippy: passed in 22.74s.
- Scoped rustfmt check, direct whitespace/final-newline checks, original-prefix
  and unchanged-file comparisons, and scoped `git diff --check`: passed.

All initiated processes exited. Parent Rust/Cargo ownership is released for a
NEW independent specification re-check, then full quality review. c3b remains
unchecked until both pass. c3c/d and runtime guarantees remain open.

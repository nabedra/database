# DN-R04c3a exact retained-owner classification

This bounded slice implements owner classification only. It grants no drain
receipt, final idle authority, configured startup proof or durable freshness.
Work generation and bound samples, the registered proof-heartbeat lifecycle and
final receipt composition remain c3b/c3c/c3d. Full R04c3 remains incomplete.
Parent owns a NEW independent specification review, then quality review and
rubric updates.

## Scope and snapshots

Pre-edit copies with repository-relative paths are retained under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r04c3a-oj0bckz_/`.
That directory also contains source diffs. Comparisons establish present source
relationships only, not independent edit history.

Changed `server/src/node/payload_owner.rs` and appended tests to
`server/src/node/payload_owner/installation_tests.rs`. The complete original
installation-test prefix is preserved. The snapshotted
`server/src/node/payload_owner/tests.rs` is byte-identical and needed no new hook.
This note is new. No admission/auth, heartbeat, storage, Fake, WorkerPool, frame,
spool, runtime, engine, Cargo/lock or body implementation changed. No rubric,
A22 or DN-R04c3 history edit was made. Shared work was preserved. No delegation,
branch/worktree, commit/push, dependency fetch/edit, network/cloud/paid call,
secret/profile read, alternate target or target cleanup occurred.

Applied test-driven-development and unslop. Consulted distributed-contracts,
A09/A20/A21/A22 owner rules and relevant R04b2/c1/c2 context.

## Exact API and rules

`PayloadRegistry::drain_work_locked(process_identity: Uuid,
observer: &ControlKey, local: &LocalAdmission) -> DrainWork` is private to the
node implementation. Call while holding the common ReceiverState mutex, passing
that state's LocalAdmission and registry. It returns only the static Debug enum
`InvalidOwner`, `WorkPresent`, or `ExemptOnly`. The last variant describes owner
evidence, never idle, termination, authentication or publication authority.
Item-level dead_code allowances cover only the enum/method until c3b consumes it.

The process must be the original boot_reserved entry at payload position zero,
Kind::Process with the exact DistributedOwner::Boot initialization identity,
with successful actual initialization and initialized LocalAdmission. Both it
and the observer must have live unfinished original handles, no cancellation,
failure, completed cleanup or recorded termination. The process must have no
request, account channels, child/production/dependency/delivery ownership or
transport guard. Pending initialization or disposal therefore cannot qualify.

The observer must match the reviewed ControlKey's original Weak<Slot> allocation,
Kind::Control and DistributedOwner::Control. Its original retained authority must
have typed Drain pending checks, the Drain auth scope and the current local boot.
Before transfer, the original stored checked RequestBody must also be Drain with
the same drain ID. After transfer the original retained typed requirements remain
the evidence. No request is copied, consumed, reparsed or reconstructed. Exact
request, receiver and drain association derives from the retained accepted
handoff, not caller IDs or an arbitrary memory-only control lease.

Only this control's original accepted record and sole ordinary transport guard
qualify. The process and control cannot carry dependency or parent guards. All
other entries, controls, preparations/placeholders, accepted records, payload
positions, occupancy, open/closing dependencies, account channels, child or
production ownership, registry failure/closure, rollback notifications and
pending outside-lock releases block ExemptOnly. Unreaped handles remain owned.
An open dependency without any payload account blocks even at zero occupancy.

A narrow helper change distinguishes the original preclaimed response guard
from extra delivery/publication holds. DeliveryHold has a private response-origin
bit; Slot has a response_live atomic maintained under the existing delivery count
mutex. Only reserve_response creates that origin; ordinary retain_delivery does
not. Dropping the original clears it under the same mutex as decrementing the
count. Thus an extra hold cannot replace a dropped response and become exempt.
This changes no charge, release, notification, disk or reaping rule. It adds no
publication slot. All actual holds remain counted by the unchanged full snapshot.

The classifier scans bounded existing tables and reads current synchronization
state. It does no hashing, encoding, parsing, heap allocation, spawning, I/O or
await under ReceiverState. Cloning the existing boot Arc does not allocate.
It locks no Disk and follows receiver-before-slot ordering. retention_locked,
close_and_drain and all reaping algorithms are unchanged. Whole-process shutdown
still counts and cancels both the process and observer.

## Causal coverage and provenance

Ten appended tests compose existing private signed receiver fixtures, real
WorkerPools, actual accepted requests and original registered handles:

- Signed accepted Drain and original response give ExemptOnly, while full
  snapshots count both owners and the response. Its incomplete observer before
  readiness is invalid. An extra delivery blocks and releasing it restores the
  baseline.
- Two signed receiver fixtures have colliding numeric control IDs. Each local
  key succeeds, cross keys and wrong initialization identity refuse. Both
  teardown tasks are joined even if either fails.
- Readiness as observer refuses; its unrelated retained owner and a dormant
  preparation independently block with no compute work.
- Actual operation/account/self/use ownership, DB-publication DeliveryHold,
  blocked Discard, cleanup-complete original supervisor before return, finished
  but unreaped handle and counted outside-lock guard release each block. Only
  after actual release/reap does the live observer baseline return.
- A signed dependency open survives its control reaping without any payload
  account; occupancy is zero and only the process payload entry remains. It
  still blocks.
- Cancellation, cleanup and finished-unreaped boundaries reject both process
  and observer exemptions, starting with a positive control for each.
- A separate hold cannot replace the original response after its drop.
- Intentional original process/observer supervisor aborts remain failed and
  retained. Production retention is unchanged. Tests record refusal and failed
  drain, then recover original process handles using the existing quarantine
  helper. The memory-only failed control's already-polled original guard is
  recovered outside the receiver mutex solely for test teardown.
- A registered ready memory-only control without a checked request cannot be
  an observer and blocks the valid observer's classification.

Observations use finite watchdogs. Gates are released, accepted work is settled,
originals drained/reaped or intentionally quarantined, tasks/reaper threads and
WorkerThreads joined and scratch removed before assertions. The shared live-entry
predicate checks pending initialization for both anchors; normal acceptance
cannot produce an authenticated observer before process initialization. The
incomplete-observer test directly exercises that predicate's initialization arm.

The first signed baseline test was written before the classifier implementation.
A compiling placeholder returning WorkPresent produced a genuine behavioral red
at the expected ExemptOnly assertion, after successful teardown. No missing-API
compile error is counted as that red. Subsequent compilation failures were a
private matches_auth call and a missing bool annotation; both were corrected.
The first expanded run exposed a wrong process-position assumption and reused
API sequence in the test fixture. Both were corrected. An initial lifecycle test
omitted stream reaping while waiting for account Discard; its watchdog outcomes
failed after cleanup, then the test drove the existing reaper causally.

Six tests then passed. The response substitution test was added and run against
a temporary count-only exemption; it failed after teardown because the extra hold
was wrongly exempt. The response-origin check was restored and eight tests
passed. The initial process-failure probe incorrectly expected the ordinary
command panic hook to affect Discard. It did not; the test was corrected to abort
the original supervisor at its existing pre-return gate. The last two tests have
green-only provenance. Other added cases likewise are coverage added after the
initial baseline implementation, not independently demonstrated red/green pairs.
The final focused run passed all ten tests. Earlier incomplete c3 attempt tests
are not evidence for this implementation.

## Verification

Every Cargo invocation used existing server/target, awaited actual exit before
the next command and used this exact prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Commands used the following arguments, with the prefix on the same shell line:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib drain_classifier_ -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Final results and ownership release are recorded below after all processes exit.
No unrelated protocol/storage/heartbeat suites were run. Prior unaffected
component passes belong to their original notes and are not claimed as reruns.

Final results, all exit 0:

- Focused classifier filter: 10 passed, build 36.11s, tests 0.04s.
- Final affected payload-owner filter: 56 passed, build 35.65s, tests 0.40s.
  This includes all ten classifier tests after the final scoped lock/comment
  edits, the R04b2 readiness-failure teardown regression and both R04c2
  receiver-association regressions with their original assertions intact.
- Affected admission filter: 14 passed, build 1.21s, tests 0.69s.
- Strict library/tests Clippy: no diagnostics, 24.30s.
- Scoped rustfmt edition 2024 with skip_children=true passed. Direct checks of
  every owned file, including untracked source and this note, found no trailing
  whitespace and exactly one final newline. git diff --check passed for tracked
  changes. Original installation-test prefix and unchanged tests.rs passed byte
  comparisons; final diffs are retained beside the snapshots.

Self-review checked the exact original handle/Weak identity, process position,
accepted record and typed checked-request association, response origin rather
than count-only exemption, open dependencies without accounts, full retention,
release boundaries and cleanup before assertions. The ten new tests supply local
owner evidence only. Signed fixtures use synthetic initialization and perform no
real R07 configured backend/catalog startup. No HTTP, runtime, distributed or
full R04c3 completion is claimed. Fresh durable authority, permanent barrier and
exact root/sample/generation checks remain c3b/d work, as authorized by A22.

Rust implementation and Cargo/build ownership are RELEASED at handback after all
initiated processes have exited. Parent owns fresh independent specification
review followed by quality review; no review or rubric result is claimed here.

## Parent correction after specification review

The specification reviewer found that the failed-process test could stop
teardown after `close_and_drain` returned `FailedDrain`, leaving the surviving
Drain control's original handle and transport guard unreaped. The existing
quarantine helper recovers payload entries only, so it did not close that gap.

The parent changed only
`payload_owner/installation_tests.rs::drain_classifier_failed_original_process_stays_failed`
and this note. Production and the test hook file are byte-identical to the
pre-correction snapshots under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r04c3a-teardown-axjqcdi_/`.
All other installation-test text is unchanged.

The test now holds the observer at its existing pre-return gate, proves that
global failed drain retains it, then releases the gate and continues polling
the registered handle with the existing stable broadcast waker. Notification
registration precedes inspection. Before quarantining the failed process, it
records empty control storage, a released control record, no pending release
and zero strong references to the original transport guard. The test keeps only
a Weak reference for that last observation.

Observation failures still reach a test-only fallback that cancels and joins
any remaining original memory-only control handles before clearing their records
and dropping their guards outside the receiver lock. Its deadline fallback aborts
and joins the original task; it never detaches it. This recovery does not alter
production retention or make failed drain successful. Process quarantine, pool
join and scratch cleanup precede the final assertions. The regression exercised
the recovery path during its behavioral red; no injected watchdog-timeout run is
claimed.

Parent-executed commands used the same required environment prefix above, with
each process exiting before the next Cargo command:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib drain_classifier_failed_original_process_stays_failed -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib drain_classifier_ -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The first command ran the gated assertion before adding continued reaping. It
failed after the teardown assertions passed: observed control entries, control
records, pending releases and transport strong references were
`(1, 1, 0, Some(1))`, against `(0, 0, 0, Some(0))`. This was a compiling behavioral
red, exit 101, with build 37.71s and test 0.02s. After the correction, all ten
classifier groups passed in 0.13s after a 37.61s build. Strict library/tests
Clippy passed in 22.63s. Scoped formatting, whitespace/newline and snapshot
comparisons passed. No compiler failure occurred during this correction.

The earlier 56 payload-owner and 14 admission passes remain reported prerequisite
evidence, not parent reruns after this test-only change. Full quality review is
still pending after the fresh specification re-check. No runtime or aggregate
R04c3 guarantee follows. Parent Rust/Cargo correction ownership is released;
all initiated processes have exited.

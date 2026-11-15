# DN-R04b2 common receiver ownership and installation

Implemented the remaining adopted A20 mini2 boundary after resuming the same
assignment. This supersedes the explicitly incomplete intermediate handoff;
its source and evidence were preserved, not restarted. Parent fresh independent
specification review followed by quality review is still required. No listener,
final replay/authentication, scheduling, runtime idle or distributed acceptance
claim follows from these local ownership tests.

## Scope, snapshots and preservation

Original owned-source snapshots remain in `/tmp/meshdb-DN-R04b2-before/`.
The passing intermediate migration was additionally snapshotted before resumed
edits in `/tmp/meshdb-DN-R04b2-resume-before/`, with the intermediate note saved
there as `DN-R04b2.intermediate.md`. Both are outside the checkout.

Changed only:

- `server/src/node/admission.rs`, receiver mode/construction, opaque common-state
  access, concrete payload-account/use lifecycle integration and private fixtures.
- `server/src/node/payload_owner.rs`, preparation, reservation, install, boot,
  control, concrete guard retention, lifecycle observation and reaping.
- Existing private `payload_owner/tests.rs`, nested state access, concrete guard
  representation, two finite worker gates and sibling visibility for the existing
  intentional-failure quarantine helper. Existing assertions are preserved.
- Existing private `payload_owner/preparation_tests.rs`, nested access and guard
  construction only, preserving its assertions.
- New private `payload_owner/installation_tests.rs` and its private declaration.
- This note.

`payload_owner/local_tests.rs` and `node/mod.rs` exactly match the original
external snapshots. No other source, rubric, design note, Cargo/lock or dependency
was edited. No delegation, branch/worktree, commit/push, secrets/.env/profiles,
network/cloud/paid call, dependency fetch, alternate target or target cleanup
was used. Existing server/target remained in use, with about 4.0 GiB available.
Applied TDD and unslop and consulted the contracts/rubric, adopted A20 split,
R04a/b1, A06/A09 ownership, A07/A15/A18, A21 and reviewed lifetime/interface
notes. No applicable AGENTS.md was found in scoped repository/ancestor discovery.

## Common state and exact bounds

Pub(super) `ReceiverState` has one opaque pub(super) `payload: PayloadRegistry`
and private closed `mode: ReceiverMode::{Legacy, Distributed(Box<LocalAdmission>)}`.
`PayloadOwners::from_receiver(pool, Arc<Mutex<ReceiverState>>)` retains that SAME
Arc. Shared has no independent admission mutex. The legacy constructor creates
the same container in Legacy mode for reviewed fixtures. Every legacy admission
path is closed in Distributed mode; try_local checks mode before local encoding.
There is no ReplayState fallback or initialized/authority input flag.

`LocalAdmission::new` still validates workers in 1..256 and queue in 0..4096,
including exact local key/coordinator agreement. `ReceiverState::distributed`
consumes that opaque validated local state outside the lock. Its immutable boot
is now held in an Arc so the original identity can also be retained in the boot
receipt without adding a Clone implementation to ReceiverIdentity.

`PayloadRegistry::bounded(&LocalAdmission)` reads its actual validated O/P lengths
and preallocates all storage before the mutex exists:

| Table | Exact bound |
| --- | --- |
| Payload positions | 1 + O + 4096 + 16 + 16, at most 8481 |
| Process position | exactly 1, permanently associated with the first boot attempt |
| Operation account positions | O = operation_workers + queue, at most 4352 |
| Dependency account positions | exactly 4096, at most one lazy account per permit |
| Operation / job stream positions | 16 / 16, nonborrowable |
| Logical accepted records | O + P + 64, at most 8768, P at most 4352 |
| Registered preparation positions | 24 data/boot and 8 control, nonborrowable |
| Memory-only control entry storage | 64, also bounded by logical accepted records and transferred transport |
| Deferred rollback notifications | 32 |
| Outside-lock release batch | fixed capacity 8481 + 64 + 32, plus 32 notifications |

Payload entries and accepted records are separate: a retained dependency account
has a payload position/handle but no operation or partition execution record.
Failed/cleanup-complete/unreaped entries retain their original positions. There
is no unbounded map, scheduler setting or internal admission waiter collection.
Preparation credits never manufacture transport capacity. Control still needs
an already held control exchange; the 64 accepted-record allowance does not
increase A07's upstream eight-control transport budget. Legacy fixtures retain
the fixed global accepted bound and existing injected role permits.

All registry fields, Entry/Preparation, disk cells and guard fields remain
private to payload_owner. Pub(super) opaque types include PreparedEntry,
PreparedPair, EntryReservation, PairReservation, ProcessReady, ControlLease,
ControlKey, DeliveryHold and InstalledPair. Only InstalledPair's account/stream
lease outputs are pub(super); no raw Slot/disk/proof field was exposed. The Slot
type's preexisting sibling visibility is unchanged and all its fields stay
private. Snapshot fields expose derived observations only.

## Reservation, guard transfer and installation

`prepare_pair(role, already_held_transport, deadline)` reserves TWO data/boot
placeholders and two checked IDs atomically before spawning either task. Refusal
with one free placeholder changes neither ID nor table and releases the supplied
charge after unlocking. Runtime availability is checked before creating the pair
so a pre-spawn failure cannot strand its second empty placeholder. Slot,
channels, task futures and transport Arc storage are allocated outside the lock.
Both original handles are published synchronously after their respective spawn,
with no await, caller return or cancellation gap. Prepared tickets own no handle.

`reserve_pair_locked` borrows mutable PayloadRegistry, mutable LocalAdmission
and the EXACT mutable PreparedPair. It checks registry closure/failure, exact
preparation/parent identity and kind, cancellation, settled process initialization,
fixed free payload/accepted positions, original shared transport, complete permit
binding, live local capacity and checked generations. The reviewed core reserves
operation+self atomically, or partition+DependencyUse with no operation slot.
It retains the concrete move-only DependencyUse receipt BEFORE later replay.
The reservation also owns the original move-only WorkTicket for rollback;
a copied live key cannot be supplied to rollback_work.

Parent holds are claimed before the final cancellation check. This prevents a
parent from passing its zero-child disposal check while a child is installed.
Increments use the current locked child count, never overwrite a count sampled
before an outside-lock reaper decrement. Any refusal rolls back tentative work,
use counts, account holds, generation, positions and parent claims through the
same borrowed mutable state. Drop never re-locks ReceiverState. Parent wakeups
are moved to bounded deferred storage and released after unlocking via
release_pending/reaping. No disk cell is locked under ReceiverState.

`PairReservation::install_entry_locked` consumes that same reservation before
unlocking. It moves the ORIGINAL registered handles, concrete WorkKey/use receipt,
transport and parent guards to their accepted positions, sets the already checked
operation identity and sends concrete Activation messages. Both account and
stream install together. Install has no allocation, spawn, parse, hashing,
permit acquisition or recoverable refusal. A gate-send failure retains the
accepted original owner, records failure and cannot pretend to roll back replay.
The private legacy reserve_entry_locked/install_entry_locked follow the same
exact-ticket borrowing rule. LocalProduction future construction occurs outside
the receiver critical section.

The closed Admission enum retains either real legacy permit/lifetime/parent
ownership or concrete distributed owner/use/transport/parent ownership. There
is no arbitrary lifetime trait. A pair wraps ONE already acquired exchange in
one Arc and both registered tasks retain it. No second role acquisition occurs.
Transport and quota can therefore remain conservatively occupied through account
reaping as well as stream reaping. Observers/tickets only signal cancellation.

For dependencies, exact coordinator/node/incarnation/session/key, operation,
admission, digest and permit matching is still performed by LocalAdmission's
reviewed Binding/DependencyKey logic. A retry reuses the existing account; its
unused account preparation is cancelled and reaped on its original memory-only
supervisor. The account retains its request sender and buffered initialization
receiver in Entry, so absence of an account observer cannot cancel it. Streams
wait for the ORIGINAL account's settled initialization watch before borrowing
its disk state. The account creates its quota account on the worker from the
one retained process Spool, with no operation execution charge. Accounts have no
attempt deadline; the stream keeps its bounded transport deadline.

`close_dependency_locked` marks Closing and cancels that exact account, then
returns Pending immediately while accepted uses/account ownership survive.
Accepted children continue their owned commands; Closing blocks new uses.
The account supervisor waits for actual child reaping before worker disposal.
The private DependencyRecord::payload_account hold prevents core removal when
uses reach zero until that account's own successful disposal/termination/reaping.
Implicit self closure is also observed by reaping and signals any lazy account.

## Boot, control and lifetime seams

`start_boot(exact_boot, initialization, path, limits)` checks the retained fixed
boot/init identity and reserves the SOLE process position plus one data/boot
placeholder before any listener/transport. It takes no compute or transport
charge. boot_reserved/process_used prohibit a second owner or a retry around the
unresolved first. A pre-spawn boot failure retains its failed initialization
identity/position and fails drain closed.

Only the original process worker initialization command creates Spool. After
that command settles successfully, the supervisor records initialization and
publishes its original readiness result. `PayloadLease::process_ready` consumes
that result and returns opaque non-Clone ProcessReady retaining exact boot Arc,
initialization UUID and entry ID. `ProcessReady::check_locked` rechecks the
still-retained original initialized/cancellation state. No production method
marks LocalAdmission Initialized or durable Ready. Only cfg(test) fixtures
supply the absent R07 half after validating the real ProcessReady.

`prepare_control` transfers one already held control transport into a reserved
control placeholder. `reserve_control_locked` checks its actual original guard,
kind, identity and accepted-record room. `install_control_locked` moves the same
memory-only supervisor into accepted control ownership. ControlLease offers
ready, retain_response and finish; it cannot submit payload/compute commands.
Control activation/readiness/cleanup does not submit a WorkerPool command or
create a disk slot. Response holds and the original charge survive observer
cancellation until response settlement, cleanup and actual handle reaping.

`PayloadLease::retain_delivery` returns a concrete DeliveryHold for outstanding
coordinator/reply/publication work. Its Drop settles only that hold; it cannot
release WorkKey, transport, account or task ownership. The registered supervisor
waits for these holds before Discard, so an unresolved publication can retain
the original owner. This is a retention seam, not a database publication
algorithm or proof that a caller's storage effect settled. R04c must attach its
actual supervised publication lifecycle to it.

## Reaping, locks and observation

The receiver lock permits bounded lookups/state changes, handle polling and
memory moves only. No parsing/root hashing/RNG, storage/disk call, pool submit,
await, blocking join or task spawn runs under it. Legacy admission try-acquisition
is preparation-only, before a task or final install. Refused guards remain in
outside-lock storage rather than being destroyed by a locked helper.

Original handles are polled in registered storage using the stable broadcast
waker. Successful supervisor termination AND cleanup are required before
finish_entry_locked consumes local capacity/dependency ownership. It validates
exact payload/accepted identities and the retained use before ordered execution,
delivery, cleanup and reap transitions. RunningReserved versus Queued charges
remain unchanged until actual reap. Phase helpers/reap stay private to admission;
registered integration uses the concrete retained-key seam. Failure preserves
original handles/guards/cells, fails closed and never reports successful drain.

After locked local transitions, reaping extracts memory-only guards/notifications,
records bounded pending_releases and unlocks. Parent holds and permits release
outside the receiver lock. The reaper re-locks only after those transitions
finish, clears the marker and broadcasts. Snapshots include queued notifications
and pending releases, preventing a transient false empty sample. Child reaping
precedes parent disposal. Worker disk-to-handoff lock ordering is unchanged;
no receiver lock is taken while holding either lock. Drain cancellation never
takes or drops a sole handle. Wait registration still precedes state inspection.

Pub(super) snapshot APIs are ReceiverState::snapshot_locked,
PayloadRegistry::retention_locked and control_retention_locked(ControlKey).
ReceiverSnapshot retains exact boot/init observation and the derived local and
payload snapshots. LocalSnapshot getters return four occupancy counts,
initialization, permanent drain ID and open/closing dependency counts.
RetentionSnapshot derives accepted/record/position/control/preparation/supervisor,
delivery, cleanup-unreaped, pending-release and failure observations from actual
owners. ControlKey is opaque and only issued by its live ControlLease; its
retention query exposes response-hold/cleanup/termination state. These getters
have no setters and issue no idle or authority receipt. Whole-process drainage
counts every control; no A21 observer exception is implemented here.

Admission's necessary sibling types/methods are pub(super), with all identity,
proof, record and ticket fields still private. The exact additions are the common
receiver factories/parts/local/snapshot/close seam; LocalAdmission constructor,
observation, reserve/open/start and read-only snapshot/key/bounds access; concrete
hold_account/release_account, retain_payload_use/reap_payload_work,
rollback_work and close_payload_dependencies integration. The reviewed core
changes are boot Arc retention and the payload_account removal hold. Other core
admission predicates/capacity semantics remain intact. Item-specific dead-code
allowances document pending R04c/R07 callers; no module-wide suppression or
unrelated lint change was introduced.

## Tests and provenance

Twelve private installation groups cover shared Arc identity and closed legacy
paths; exact configured/max bounds; accepted table exhaustion; incomplete pair
preparation and checked IDs with no task/scratch/charge leak; tentative local
conflict and whole pair rollback; original-handle activation; boot before
transport, wrong boot/init and duplicate refusal, immutable non-Clone receipt
without initialization; all 24 data preparations with eight independently
accepted controls; response/publication holds and pending-release snapshots;
exact dependency account reuse, mismatched binding and no operation charge;
Closing accepted uses; real command/Discard/post-cleanup-pre-return gates;
sixteen real operation streams exhausting their role while job/control ownership
remains available; cancelled/resumed drain, competing waiters and failed-pool
quarantine retaining the original keys/handles/charges.

A physical accepted-table exhaustion fixture shrinks the table before creating
its mutex/owners; it first observes the full production bound. This is a private
fault fixture, not new distributed scheduler configuration. Constructor checks
also cover the exact maximum 8481/8768 production lengths. Private local input
and initialization fixtures do not supply production authentication or R07
storage/catalog authority. Existing reviewed local/stream tests are reused,
not a new frame/Parquet matrix.

New synchronous worker gates have finite ten-second receive watchdogs. Async
waits use ten-second watchdogs, with observations recorded, gates released,
observers dropped, original owners drained/reaped, reaper thread and pools joined,
and temp paths removed before observation assertions. No sleep ordering or
detached test task was added. Existing intentional-failure recovery is reused
on joined teardown threads and is not a production fallback.

Observed red/green history is retained honestly:

1. Intermediate first-turn accepted-storage assertion failed against the original
   constructor: 1 failed, exit 101, build 37.62s. Migration then had six accidental
   Fake `.payload` fixture compile errors; local_tests.rs restoration fixed them.
   Storage green passed in 28.68s; the next two-group green built in 30.62s.
2. The intermediate affected run passed 37/failed two existing expired-deadline
   legacy tests. Removing premature legacy deadline refusal restored all 39,
   build 26.98s/tests 5.68s. Those setup panics preceded existing fixture teardown;
   they are not represented as clean causal runs. The next three-group check
   passed, build 26.77s. An owned let_and_return Clippy diagnostic was corrected.
   Final intermediate evidence was 40 payload tests, 10 admission tests and
   strict Clippy, with build/check times 27.16s/1.44s/21.72s.
3. Resumed distributed-constructor scaffold failed its intended closed-mode
   assertion: exit 101, build 27.42s. Correct configured construction passed,
   build 37.37s. Boot then passed in 31.26s; four/six/seven focused groups passed
   through successive integrations in 33.26s/29.05s/31.73s and 28.64s.
4. One new-test unmatched delimiter compile failure was corrected. Nine focused
   groups passed in 28.61s; eleven admission and 48 affected payload tests passed
   in 29.02s and 28.71s builds. Strict Clippy found one owned collapsible-if,
   fixed without suppression.
5. The focused account-lifetime assertion then failed as intended after complete
   teardown: exit 101, build 29.46s. Account preparation had inherited its stream
   deadline. Removing that inheritance made all twelve focused groups pass,
   build 28.90s/tests 0.07s. Strict Clippy passed in 21.85s. Maximum-bound/opaque
   receipt additions and final receipt-move checks followed; admission passed
   eleven in 32.60s and payload passed 49 in 32.28s/tests 5.95s.

Only those named assertions were observed red. Other additional assertions and
inspection-driven race/guard-transfer corrections have green-only evidence.
Earlier in-progress unused-helper warnings disappeared as their actual callers
were wired. No prior lint backlog is treated as open.

## Commands and final verification

EVERY Cargo invocation, including failures, used existing server/target and:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Exact command forms, repeated as needed for the deltas above:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner::installation_tests -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission::ownership_tests::distributed_container_has_configured_ownership_tables -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner::installation_tests::exact_boot_receipt_needs_no_transport_and_never_initializes -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner::installation_tests::incomplete_preparation_and_checked_ids_refuse_without_extra_task_or_charge -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Every Cargo PROCESS exited before the next command. No default/S3 or needless
broad build ran. Final source results and scoped preservation/format checks are
appended after the last commands below.

## Remaining R04c/R07 and runtime obligations

No mini2 requirement is deferred as missing owned work. R04c still owns the
fresh complete-root/typed route association, ReplayState addition and consume
inside the SAME locked reservation/install transaction, stored original A18
request ownership, authenticated start/result checks and actual route response
composition. No missing replay authorizes a request here. R07 supplies its opaque
configured storage/catalog InitializedWorker for this exact retained boot/init;
R04c combines it with ProcessReady before initialization/readiness completion.

The actual advanced FrameReader/checked prelude handoff, listener/HTTP/TLS,
scheduler/coordinator dispatch and DB publication algorithm remain their later
assignments. A21's exact drain observer and separately retained heartbeat
publication/settlement receipts, work-generation sampling, positive idle and
termination proofs remain R04c/C03/C06 work. DeliveryHold is retention, not an I/O
settlement certificate. Existing snapshots/control identity make those later
checks feasible without excluding an arbitrary caller counter or bool.

No real runtime/idle/distributed guarantee is claimed. Parent assigns NEW
independent specification and quality reviews before checking this slice.

Final verification, all exit 0:

- Final production-source payload/owner/local/stream regression filter: 49 passed,
  0 failed, build 35.49s, tests 6.07s. Includes all twelve installation groups and
  the reviewed preparation, local handler, receive and send regressions.
- Final admission filter: 11 passed, 0 failed, build 1.37s, tests 0.52s.
- Later test-only strengthening explicitly checks new exact uses refused after
  Closing and zero local keys/positions/records/supervisors/pending releases after
  normal drain. Final focused installation run: 12 passed, 0 failed, build 39.42s,
  tests 0.13s. These additional assertions are green-only evidence.
- Strict library/tests Clippy on the final source: exit 0, 17.56s, no diagnostics.
  A prior final production-source strict pass also completed in 25.35s before
  the last test-only assertions.
- Owned rustfmt check, scoped git diff --check, direct trailing-whitespace and
  single-final-newline checks passed, including new/untracked sources and note.
  The final note append was checked again afterward. Exact format command:

```text
rustfmt --check --edition 2024 --config skip_children=true server/src/node/admission.rs server/src/node/payload_owner.rs server/src/node/payload_owner/tests.rs server/src/node/payload_owner/preparation_tests.rs server/src/node/payload_owner/local_tests.rs server/src/node/payload_owner/installation_tests.rs
git diff --check -- server/src/node/admission.rs server/src/node/payload_owner.rs server/src/node/payload_owner/tests.rs server/src/node/payload_owner/preparation_tests.rs server/src/node/payload_owner/local_tests.rs server/src/node/payload_owner/installation_tests.rs server/notes/DN-R04b2.md
```

Snapshot comparisons confirm unchanged local_tests.rs and node/mod.rs. Final
existing-source deltas against the original snapshot are admission 371 added/30
removed, owner 1342 added/152 removed, owner tests 37 added/7 removed and
preparation tests 19 added/15 removed, plus the new installation tests/note.
The reviewable core delta is documented above, rather than claiming the original
admission prefix remains byte-identical after authorized integration. Existing
assertions are preserved. Shared preexisting edits remain uncommitted.

Implementation is ready for the parent's NEW independent specification review,
then quality review. No Cargo process or delegated work remains running.

RELEASED Cargo/build and Rust implementation ownership.

## Parent quality correction: readiness failure teardown

The independent quality review found that the publication-retention test recorded
failed or timed-out account readiness, then unwrapped `retain_delivery()` before
drain, pool join and scratch removal. Cancellation makes that acquisition return
Cancelled, so the test could panic before its teardown.

The parent changed only `payload_owner/installation_tests.rs` and the existing
test-hook field visibility in `payload_owner/tests.rs`. The common observation
helper now retains the fallible hold result, drops any successful hold at the
original release point, and returns observations. Both callers perform complete
`finish_distributed` teardown before checking those observations. Normal-path
rollback, original-handle, pending-drain, capacity and transport assertions remain.

The new regression injects the existing command-panic hook into the dormant
account before installation, making initialization fail on its worker. It catches
observation panics solely to preserve teardown even during the red run. After
registered owners drain/reap, `WorkerThreads::shutdown` joins all workers, and
scratch removal succeeds, it checks that readiness failed and hold acquisition
returned Cancelled without panicking. It also checks path absence and transport
release. No production lifecycle or fault-injection mechanism was changed.

Pre-correction copies of both test files are retained at
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r04b2-teardown-d060932l/`.
Direct comparison shows only the fixture extraction/regression/fallible-hold fix
and `panic` becoming `pub(super)` in the cfg(test) Hooks record.

With the same required four environment settings and locked/offline/no-default
flags, the parent personally ran:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner::installation_tests::publication_initialization_failure_settles_before_observation_assertions -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner::installation_tests -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The regression failed behaviorally before correction: exit 101, build 27.25s,
test 0.01s. It caught the Cancelled unwrap panic and reported the failed
observation only after successful teardown and cleanup assertions. After the
fallible-hold correction, all thirteen installation groups passed: exit 0,
build 27.26s, tests 0.08s. Strict library/test Clippy passed in 15.53s. Each Cargo
process exited before the next command. Scoped rustfmt and direct whitespace/
final-newline checks passed, including the note and parent rubric updates.
The earlier 49 payload and eleven admission passes are reused; no later combined
rerun is claimed for this test-only correction.

A new independent quality re-check remains required. C03d released its ownership
before these edits; its implementation was left untouched. No runtime, idle or
distributed guarantee follows from this regression.

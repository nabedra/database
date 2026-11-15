# DN-R04c2 atomic receiver acceptance and retained request ownership

Implements adopted A22 assignment 2 on shared main, following reviewed c1 and
its retained Sender comparison correction. Applied TDD and unslop. No delegation,
branch/worktree, commit/push, network/cloud/paid action, download, dependency/Cargo
edit, real secret/profile/.env read, new target or target cleanup occurred.
No applicable AGENTS.md was found in scoped repository/ancestor discovery.
C05 provision/reconcile/delivery implementation was excluded.

## Files and snapshots

Existing owned files were copied before edits, with repository-relative paths, to
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r04c2-pa_j9yt4/`.
They are admission.rs, payload_owner.rs, payload_owner/installation_tests.rs and
auth/replay.rs under server/src/node. The snapshot directory also retains direct
unified diffs. New files are node/admission/receiver_tests.rs and this note.
No other source/module declaration was changed. The private test declaration
is in admission.rs. Existing installation tests remain included only once and
retain their original text. New signed-root fixtures reuse the existing private
WorkerPool/scratch fixtures without changing fixture visibility.

## Concrete transaction

ReceiverMode::Distributed contains LocalAdmission and mandatory ReplayState under
the existing ReceiverState mutex. Construction allocates replay outside that
mutex using ReplayState::for_receiver(local.boot()), retaining the SAME original
Arc. ReplayState::new still wraps its owned identity outside locks. Replay's
consume/order/identity bounds and algorithms remain unchanged. Existing parts()
continues to serve reaping; accept_locked borrows the three concrete fields
separately, without a callback or second receiver mutex.

PreparedRoute is Operation(PreparedPair, OperationCandidates), Job(PreparedPair)
or Control(PreparedEntry). Candidates generate only admission/self-permit UUIDs
outside the lock. ReceiverState::accept_locked consumes DurableRequestView and
CheckedContent, borrows the original preparation and process lease, and returns
the closed Operation/Job/Readiness/Open/Close/Drain AcceptedRoute or value-free
Auth/Admission/Owner error categories. Operations derive Binding from retained
local coordinator plus checked operation/digest and candidates. Jobs and controls
use their actual pending/content bindings. Process limits are retained once by
start_boot in its original Slot, so acceptance does not inspect disk state.

Foreign boots are rejected before entering this receiver's root history, matching
LocalAdmission::observe's existing boot check. For this boot, BOTH replay and local
observation methods are called before propagating either ordering refusal. This
preserves sticky complete-root divergence and permanent durable drainage even
when content association, capacity, replay or a later route check refuses.

PairReservation retains all existing tentative capacity, dependency/account,
accepted/payload positions, original process/parent holds and transport claims.
LocalControlReservation borrows LocalAdmission directly. Open reserves its exact
retry or new dependency and generation; Drop restores only that tentative entry
and generation. Open always requires Ready and the permanent open barrier,
including an exact retry. Close prechecks its full binding and plans the closed
observation, with irreversible account cancellation postponed until commit.
Readiness checks initialized exact boot/challenge. Drain checks the checked ID
against both current durable and permanent local drain, granting no idle receipt.

EntryReservation reserves the original control and preclaims its response hold
before replay. Replay failure drops those SAME borrowed reservations. Successful
replay consumes the actual handoff, fills its reserved positions, then installs
and activates the SAME original dormant handles/claims/transport. Commit performs
only bounded prechecked moves and notifications. It does not allocate, reparse,
hash, spawn, await or acquire another transport role. Activation failure keeps
the accepted owner and spent sequence through existing registry failure handling.
PayloadOwners::accept is the concrete locking caller and flushes existing bounded
rollback notifications through release_pending after releasing the mutex.

Payload bounds remain 1+O+4096+32 <= 8481; accepted records O+P+64 <= 8768;
dependencies 4096; preparation 24 data/boot plus 8 controls. Existing bounded
pending releases and 32 rollback notifications are preserved. These are retained
owner bounds, not measured RSS or additional transport capacity.

## Two positions, original transfer and starts

RequestStorage is prepared outside the lock, before spawning the dormant task.
It contains a preallocated Arc<OnceLock<RetainedRequestAuthority>> and a DISTINCT
owner-only Option holding the original Box<[u8]> and RequestBody. Both are checked
empty before replay. RetainedRequestAuthority::from_handoff consumes the actual
into_parts output, retaining only original RequestBinding, complete observation,
checked local identity, PendingLocalChecks, original boot Arc and work association.
The complete RequestHandoff and body never enter shared immutable storage.
Operations install request storage in the account owner, jobs in their work-bearing
stream, controls in their original control entry. No body reconstruction, cloning,
Arc uniqueness or observer disappearance is required.

Only the original move-only PayloadLease can take its registered payload once.
PayloadLease::take_request locks the common receiver and calls take_request_locked;
it refuses an absent/foreign/cancelled owner or second take. The transferred
OwnedRequestPayload carries original bytes/body plus a concrete DeliveryHold.
The registry retains the transferred marker, original handle, authority and charges
through command settlement, cleanup and actual reaping. The continuation must
retain its delivery obligation through the submitted command's settlement and
release the receiver lock before submission or disk/handoff locking. The real
worker test moves the ORIGINAL parsed job Box into existing Command::StartSend,
cancels that command's observer while the accepted WorkerPool command is gated,
releases the gate, and waits for a same-worker FIFO command to settle before
releasing bytes/delivery and disposing/reaping. No executor or stream driver was
introduced; existing worker/disk/supervisor/reaper algorithms are unchanged.

StartSubject retains only WorkKey and immutable authority access. Its check(root,
pin) uses the ORIGINAL binding's check_accepted_authority outside the mutex and
constructs the immutable CheckedStart/StartCheck from actual retained binding,
request, boot and dependency. ReceiverState::start_locked orders both root histories,
checks the exact still-registered authority cell/work/boot and invokes actual
LocalAdmission::next_start. Refusal removes no original record, use, charge or
handle. Verified Draining progress and accepted queued promotion under Closing
remain distinct from fresh-operation/open refusal. control_request_locked supplies
c3 immutable access to an exact registered ControlKey's original request; it is
not an idle/publication receipt.

## Validation and provenance

Every Cargo invocation used the existing server/target, with this environment:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Every invocation included `--manifest-path server/Cargo.toml --locked --offline
--no-default-features`. Actual process exit was awaited before the next Cargo
command, including compile failures. The exact command forms were:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib control_open_rollback -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib receiver_signed -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib receiver_operation -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib receiver_ -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib receiver_original_request -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::replay -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Observed compiling behavioral RED: control_open_rollback failed with one retained
dependency versus expected zero after dropping its concrete reservation. Exit 101,
30.49s build, 0.01s test. The smallest direct rollback correction passed that group,
exit 0, 28.42s build. Later signed transaction/lifetime groups were added green;
there is no claim of an individual observed behavioral RED for each assertion.
Two intervening fixture compilations failed separately: ambiguous Role from a glob
import and missing RequestBody import. Both were corrected with explicit imports;
these were compile failures, not behavioral REDs. Fixture imports/dead-field
warnings were resolved before strict Clippy.

Signed readiness passed one group after its compile correction, 30.36s build.
Operation passed one after its import correction, 30.07s build. Incremental
receiver_ runs passed 6, 8, 10 and 10 groups as new cases and stronger queued/byte
identity checks were added, builds 31.02s, 31.93s, 29.00s and 29.35s respectively.
The filter also includes the existing auth alternate-receiver-boot group.
Affected admission passed 14 groups, 32.32s build/0.58s tests. Affected payload
owners passed 43 groups after concrete outside-lock rollback flushing was added,
32.50s build/0.27s tests. The additional publication/Discard/pre-return request
retention group passed, 32.14s build/0.02s test. Replay passed three groups,
0.86s build/6.65s tests. First strict Clippy passed without diagnostics in 20.04s.
Final strict Clippy and preservation results follow below.

Tests use real signed proofs, content checking and ReplayState, actual registered
workers, original handles, finite gates and private existing fallible-readiness/
teardown helpers. They cover concurrent independently verified same-sequence
operation contenders with one owner and rollback, queue=0 refusal followed by the
same signed higher sequence succeeding after actual reap, response-hold failure
without spend/tentative dependency leakage, foreign boot and independent invocation
association, sticky equal-revision divergence through later roots, first durable
Draining job on an existing permit, queued promotion after Closing versus new-use
refusal, lost/replaced fresh authority with queued owners retained, both open/barrier
lock orders, original byte/parsed-box identities, second-take refusal, surviving
start subjects, actual command cancellation/settlement and request retention through
publication/Discard/cleanup-complete-but-unreaped return. Existing admission and
auth evidence covers registered-draining refusal; the affected owner suite includes
the corrected initialization-failure and lifecycle regressions. Synthetic
fixture_initialize performs NO real R07 configured backend/catalog startup.
All lifecycle observations precede final assertions only after gates release,
leases cancel/drop, tasks drain/reap, WorkerThreads shutdown/join and scratch cleanup.
No listener, distributed, peak-RSS or real-provider acceptance claim is made.

## Open integration obligations and release

c3 still owns exact drain-owner classification, work generation, BarrierSample,
DrainObservation, the single retained proof-heartbeat writer/cycle, acknowledged
publication settlement/reaping and exact observer exclusion. This slice grants no
positive idle or termination receipt. R07 owns configured initialization issuance
and retention of actual startup resources. Later prelude/advanced FrameReader work
must preserve body errors, trailers, STREAM_END and actual HTTP EOF while consuming
the original checked CONTROL once. payload_stream::drive remains unchanged and
cannot yet consume this handoff. Fresh GET, scheduling/dispatch, accepted-result
checks and DB publication continuation/delivery obligations remain separate.
HTTP/runtime/distributed acceptance, aggregate R04/R03a3 and final HTTP safety
remain open. Parent assigns a NEW specification review, then a fresh independent
quality review; this implementation note does not replace either review.

Final strict library/tests Clippy passed without diagnostics, exit 0, 20.19s.
Owned rustfmt check used edition 2024 with skip_children=true on all five Rust
files. Direct trailing-whitespace/single-final-LF checks covered all six owned
files, including untracked files and this note; scoped git diff --check passed.
Snapshot comparisons confirm the complete original installation-test prefix,
unchanged replay algorithms after its constructor, and byte-identical worker/
disk/supervisor implementation from run_command through the existing opaque
implementations. Diffs are retained alongside the preserved pre-edit snapshots.
The final added request-retention test changes no production algorithm and was
run separately after the 43-group owner suite; that broad passing suite was not
repeated. Final counts are 14 admission, 43 owner, 3 replay and the additional
1 request-retention group, with earlier focused receiver evidence above.

Rust implementation and Cargo/build ownership are RELEASED. Every initiated
process has exited; no build/test process remains running. Parent owns the fresh
specification review followed by fresh independent quality review.

## Parent receiver-association corrections, 2026-11-12

The independent specification review found two concrete gaps. A foreign checked
start reached replay root observation before receiver-boot rejection, potentially
setting sticky integrity failure in the wrong receiver. ControlKey contained only
a registry-local numeric ID, so another registry's colliding key could retrieve
this registry's authenticated control authority.

The parent waited for C07a to release exclusive Rust/Cargo ownership, then changed
only admission.rs, payload_owner.rs, appended installation tests and this note.
Pre-correction snapshots, using repository-relative paths, are retained under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r04c2-association-1wsvi31t/`.

start_locked now compares the retained durable boot and checked local identity
with the receiver's boot before either root observation. Valid local starts keep
the existing replay/local observation ordering and final registered-work checks.
No replay algorithm or authority construction changed.

ControlKey now retains a Weak reference to its original Slot as well as the entry
ID. Equality and both control_request_locked and control_retention_locked require
that exact allocation identity. Weak identity does not retain the live Slot or
its owner graph; matching uses pointer identity without dereferencing raw pointers,
allocating a new identity or taking another registry lock. Numeric IDs retain
their existing role and bounds. No control observer or positive idle authority
was added.

Two appended tests use independently initialized real receiver/WorkerPool
fixtures, actual signed/content-checked requests and original registered owners:

- receiver_association_foreign_start_preserves_local_replay_and_start creates
  two accepted operations with different boots and complete roots at the same
  revision. B's actual checked start is refused by A; A then accepts a new signed
  readiness request and starts its retained work. B's own start is a positive
  control. The original faulty code blocked the new local acceptance; its local
  start still succeeded in this fixture. The red is not claimed to demonstrate
  failure of both local operations.
- receiver_association_control_keys_resolve_only_the_original_owner installs
  authenticated controls with equal numeric IDs but different request sequences.
  Each own key resolves the exact original sequence, complete observation, local
  identity and retention entry; both cross-receiver request and retention lookups
  must refuse, and the keys must compare unequal.

All outcomes are captured before dropping accepted owners, preparations and
process leases. Both bounded teardown futures are spawned and joined, so a
reported failure from one teardown cannot skip the other. Registry drain/reaping,
WorkerThreads shutdown/join and scratch removal precede outcome assertions.
Control keys stay alive through teardown. These are in-process receiver identity
checks, not proof of real HTTP or distributed root history.

Parent-executed commands, each with the same required environment recorded above:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib receiver_association_ -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The first command observed both compiling behavioral reds before production
correction: build 32.36s, tests 0.04s, exit 101. Both teardowns passed first.
Foreign-start local acceptance/start were false/true; control keys compared equal
and both cross lookups succeeded. After correction the same filter passed both
tests, build 32.23s, tests 0.04s, exit 0. The affected owner suite then passed all
46 groups in 0.31s (build 0.91s), and admission passed all 14 in 0.60s (build 1.00s).
Strict library/test Clippy passed in 22.34s. Every Cargo session reached actual
exit before the next command; no parent Cargo overlap occurred.

Owned edition-2024 rustfmt with skip_children=true, scoped git diff --check,
direct whitespace/single-final-newline checks and original installation-test
prefix comparison passed. Snapshot diffs show only the early boot check and
ControlKey/lookup association changes in the two production files. Existing
replay tests and earlier other component results remain reused evidence; no new
combined full-node or runtime run is claimed. C07a source was untouched.

Rust/Cargo ownership is released with no parent process running. A NEW independent
specification re-check must close these findings before full quality review.
c3, R07, HTTP/advanced-reader, runtime and aggregate R04/R03a3 remain open.

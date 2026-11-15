# DN-R01b2b3a: bounded payload execution and cleanup

Implemented only the private execution/cleanup owner from accepted DN-A09,
including worker-side anchor initialization and retention through actual reaping.
Read distributed-contracts.md, A06, A09, A07 capacity/drain rules, the completed
R01b1/R01b2a/R01b2b1/R01b2b2 notes and actual WorkerPool/frame/job/spool/row APIs.
A09 supersedes A06's illustrative state-transfer-through-submit wording.
Applied test-driven-development and unslop. No applicable AGENTS.md was found
in the filesystem ancestors or owned directories.

Owned files are payload_owner.rs, its private payload_owner/tests.rs, the narrow
private module declaration in node/mod.rs and this note. Shared work was preserved.
No dependency/configuration, WorkerPool, frame/spool/job, row/prepared, engine,
storage, runtime, cluster/auth, HTTP or Python changes. No delegates, branches,
worktrees, commits, pushes, .env access, cloud calls or target cleanup.

## Actual private API

The node module privately declares payload_owner. Types and caller methods are
crate-private, available to later sibling stream code without a public facade.
PayloadOwners::new injects the coordinator CPU/I/O WorkerPool plus separate
operation-anchor, operation-transport and dependency-transport semaphores.
The runtime must supply a separate coordinator pool and distinct role budgets;
this mini neither starts that pool in production nor selects numeric settings.

try_process(parent, limits) registers the single process anchor. try_operation
borrows its process lease and registers an operation account using injected
limits and a memory-only Arc<OwnedSemaphorePermit> lifetime guard. try_admit
and try_admit_until borrow that operation lease, select Role::Operation or
Role::Dependency and retain the corresponding memory-only lifetime guard.
Operation leases span all attempts, so attempts share the exact account.
All admission uses try-acquire and reaps before trying capacity. Registration
and supervisor spawning occur under the same memory lock as admission close,
with no await gap. Foreign, cancelled or wrongly typed parent leases refuse.
The process registration is permitted once per PayloadOwners lifecycle.

PayloadLease is opaque and noncloneable. ready awaits worker initialization.
command takes &mut self and supports only these implemented finite commands:

- BeginRows receives constructed job/result controls and validates their identity.
- Append accepts one nonempty DATA-sized Vec and appends to the real spool writer.
- CloseRows finishes the immutable spool, invokes existing decode_rows, drops the
  encoded spool on the worker and retains decoded rows plus the exact envelope.
- CommitEof changes logical pending to accepted under the cancellation handoff
  lock and returns a memory-only copy of the envelope.
- TakeRows consumes accepted ordinary memory-owned rows.

Commands are restricted to stream slots. Ready, Accepted(envelope) and Rows are
memory-only replies. Initialization and Discard are internal finite work cases.
No future enum variants, generic executable closures, new processor traits or
stub commands are provided. This mini intentionally does not implement CONTROL
or PartMeta parsing, prepared commands, sending or framed EOF validation. Later
receive/send minis add their concrete cases using existing functions. The caller
of CommitEof must already have established terminal framing and real EOF; a
logical command success in these private tests makes no transport claim.

The consuming dispose signals sticky cancellation immediately, even if its
returned future is never polled, then waits through disposal and actual reaping.
Dropping a lease, or an unpolled/pending ready/command future, signals cancellation
without I/O, spawning or submitting cleanup. A dropped buffered lease ticket has
the same behavior. &mut command borrowing and a single bounded channel prevent
multiple outstanding requests. There is no frame/data read-ahead queue.

## Ownership and draining

Every disk cell starts EMPTY inside a registered slot. Spool::new and operation
creation run inside worker commands. Submitted closures own only registered cell
references and memory-only inputs, and WorkerPool replies never contain disk
owners. Async code never locks or extracts a disk cell. The worker alone accesses
its Mutex<Option<ManuallyDrop<Disk>>>. Rejected initialization captures no raw
anchor. A worker initialization failure unwinds newly created locals there.

Explicit Discard takes the wrapper with safe ManuallyDrop::into_inner and drops
it on the worker. Commands catch unwinding inside the cell boundary, discard
there and never resume a partially mutated processor. Poison is recovered only
for cleanup. Initialization and every pending submit and accepted Job are awaited
to settlement by the registered supervisor, even after abandonment. Deadline
expiry stops optional commands and logical acceptance; it never proves that a
synchronous command has stopped. Replies are checked for cancellation again
before delivery. No command waits on network, submits nested work or holds a
partition permit. Consecutive commands may use different threads; routing uses
the checked admission sequence modulo worker_count.

The registry owns each supervisor JoinHandle and disposal-only admission guards.
After Discard settles the supervisor records cleanup-complete and returns. It
does not remove itself or release capacity. Reaping consumes the actual handle
result before removing the entry and releasing transport/lifetime guards.
Parents retain anchors until child registry entries are reaped, so stream,
operation and process destruction has a defined dependency order. Admission
and drain reap completed entries; history does not accumulate in the registry.

close_and_drain closes admission, cancels registered owners and polls their
JoinHandles in place. Cancelling that waiter leaves every handle registered;
a subsequent call resumes. The reaper registers for task termination itself,
including when cleanup-complete was already recorded before the task returned.
A terminated failed entry is retained without polling its JoinHandle twice.
Registry occupancy includes cleanup-complete but unreaped owners.

If the injected pool stops before Discard can run, drain returns the fixed
OwnerError::FailedDrain, keeps bounded cells, quota, permits and parent lifetime
guards and refuses admission. Both disk and admission wrappers prevent implicit
wrong-thread final destruction even if the outer registry is lost. There is no
production fallback thread, destructor task, retry queue or false successful idle.
The private stopped-pool tests dispose quarantine only on explicitly joined test
cleanup threads, after asserting failed retention.

Normal runtime integration must close ingress, cancel/drain streams and leases,
dispose operation accounts and the process anchor, then shut down and join
WorkerThreads while the coordinator Tokio runtime remains alive. Dropping
WorkerThreads is not the normal shutdown protocol. Runtime/task loss, hung OS
calls, abort/OOM, process death and panicking destructors can prevent completion.
Discard proves destructor execution, not successful unlink, recovered quota or
durability; existing conservative spool unlink accounting is unchanged.

## Focused evidence

All Cargo commands used existing server/target, sequentially, with exactly:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Initial red, before production implementation:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner
```

Exit 101 for missing PayloadOwners/Role and associated owner API/imports in the
initial private test. This was compile-time red. The first implementation check
found test-hook field visibility errors. The expanded six-test run then had five
passes and one failure: the initialization fault hook panicked while holding its
own test mutex, causing disposal instrumentation to panic on poison. Moving the
fault panic outside that test mutex fixed the hook. Six tests passed, followed
by seven after adding unpolled-future abandonment and expired-deadline coverage.

Final checks use the same environment prefix above:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner
7 passed, 0 failed, 8 filtered out; exit 0.

cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
Exit 0, no warnings. Initial strict run found one collapsible-if style issue;
fixed without a lint allowance.

rustfmt --edition 2024 --config skip_children=true --check server/src/node/payload_owner.rs server/src/node/payload_owner/tests.rs server/src/node/mod.rs

git diff --check -- server/src/node/payload_owner.rs server/src/node/payload_owner/tests.rs server/src/node/mod.rs server/notes/DN-R01b2b3a.md
```

Seven private tests use actual small WorkerPool/WorkerThreads and Spool instances.
Events gate accepted Append before caller abortion; admission remains charged and
a real worker-side account writer probe proves the file quota is exhausted.
After release, both ordinary cancellation and caught panic dispose on worker
thread IDs and the same operation quota supports another attempt. A buffered
accepted lease ticket is dropped through a real oneshot; its existing supervisor
cleans it. A post-Discard/pre-return gate proves cleanup-complete still consumes
capacity. Cancelling and resuming drain preserves its original registry entry.
A separate test observes handle termination and verifies admission reclaims the
completed slot. Row commands exercise logical pending/accepted states and empty
Get rows through the existing actual decoder. Unpolled CommitEof abandonment
suppresses acceptance. Expired deadlines refuse optional commands.

Worker-side initialization failure destroys the new root before normal drain
finishes; initialization creation and disposal boundaries record worker thread
IDs. Initialization rejected by an already stopped real pool creates no root.
The saturation test gates one real executing job, occupies its queue, observes
Pending on the supervisor's next submit, stops actual WorkerThreads and verifies
rejection leaves disk and transport guards retained with failed drain. It settles
and joins accepted jobs, supervisors and worker threads; test-only quarantine
threads are joined. Gates use channels, not arbitrary sleeps or simulated pool
rejections. Owned scratch parents are empty after normal cleanup/test teardown.

These tests reuse row/scalar/Parquet/frame behavior through existing functions
and prior suites. No duplicate semantic matrices, full/default AWS build, fetch,
HTTP/TCP, cluster readiness or distributed drain evidence is claimed. The scoped
module-only dead_code allowance supports this intentionally unwired module and
must be removed at caller integration. No other lints are suppressed.

## Parent correction after specification review

Specification review found that multiple disposal/drain waiters could replace
each other's JoinHandle wake registration. A second waiter could reap the first
waiter's slot without waking it. Cancelling the most recently registered waiter
could also leave the handle pointing at a task that would never poll again.

The parent first added a real-pool two-waiter regression. Separate task poll
acknowledgments establish waiter order, and post-cleanup/pre-return gates keep
both supervisors retained. The first supervisor is released while the second
remains gated; the first disposal must then finish independently. A second case
cancels the latest waiter before releasing the first supervisor. Both cases
failed before correction: outcomes were [(false, true), (false, true)] after
the bounded watchdogs. Gates, waiters, registered owners and worker threads were
settled before the failure assertion; watchdog expiry is failure evidence, not
termination proof.

The correction gives every polled supervisor handle one stable broadcast
waker, independent of any individual waiter. Each disposal/drain wait registers
a cancellation-safe Notify future before inspecting registry state. Reaping
notifies all waiters after releasing the registry lock when removal/failure
changes completion state. Admission also notifies if it reaps an entry. Handles
remain in the registry, results are consumed once, and guards are still released
only after observed supervisor termination and successful cleanup. No new
executor, cleanup task, detached handle or disk-owner transfer was introduced.

The parent used the same resource-limited Cargo environment documented above:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner::tests::concurrent_disposal_waiters_survive_reaping_and_waiter_cancellation
Before correction: 1 failed, 15 filtered out, 4.02s; exit 101.

cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner
After correction: 8 passed, 8 filtered out, 0.03s; exit 0.

cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
After correction: exit 0, 13.45s.
```

Owned rustfmt and whitespace checks also passed after correction. Cargo is
released. A fresh
independent specification re-check precedes quality review. Framing, HTTP,
production runtime and distributed drain remain outside these component tests.

Fresh independent specification re-check passed after the concurrent-waiter
correction. It confirmed registration-before-inspection, the stable broadcast
waker and notifications after unlocking, with no scoped ownership regression.
The reviewer inspected both gated cases and cleanup and reused the recorded
checks without rerunning them. Independent code-quality review follows.

Independent code-quality review also passed without critical or important
findings. It inspected all eight test bodies and the complete ownership paths,
including the shared broadcast waker, worker-only disposal, parent retention,
actual reaping and bounded failed retention. It reused recorded checks without
rerunning them. Both review stages are complete for this owner/anchor slice.

## Later integration obligations

Receive must parse controls and metadata on workers, extend concrete processor
states for PreparedResult while preserving None versus Some(empty), and commit
only after validated grammar and actual EOF. Send must retain immutable parts
and readers in registered cells and return only bounded memory chunks. Local
producers must deposit typed disk outputs into an already registered slot while
still executing on a worker; they must never pass a potentially final raw anchor
to an async API. This mini exposes no raw-anchor input for that future hook.

Runtime must retain the process/operation leases, provide actual A07 lifetime
protection and distinct capacities, and use registry ownership through cleanup
when implementing idle accounting. A07 dependency records, auth, HTTP, global
finalization/publication, remote queries and runtime orchestration remain out of
scope. Independent specification and quality reviews follow under the parent.
Cargo ownership is released on delivery.

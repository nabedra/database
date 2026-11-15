# DN-R04b1 registered dormant preparation

Implemented only the parent's selected first half of adopted A20 mini 2 on
shared main. R04a's reviewed state is unchanged. This is intermediate legacy
registry machinery, not completion of R04b or final distributed acceptance.
Applied TDD and unslop, without delegation. No applicable AGENTS.md was found
in the repository or workspace ancestors. Read the distributed contracts and
rubric orchestration/testing/R04/F04 requirements, A20 including the selected
split, A09/A06 ownership rules, R04a, R01b2b3a/b/c, R02a2/a3 and the closed
DN-test-lints note. Inspected the actual owner/private fixtures, WorkerPool
submission/stop behavior and stream receive/send boundaries.

## Files and preservation

Before edits, copied all three existing owned sources outside the checkout to
`/tmp/meshdb-DN-R04b1-before/` with names `payload_owner.rs`, `tests.rs` and
`local_tests.rs`. The assignment changes only:

- `server/src/node/payload_owner.rs`, preparation, legacy activation, close/reap
  and narrow test hooks/module declaration.
- `server/src/node/payload_owner/tests.rs`, one dormant-task observation hook
  and sibling-test visibility for the existing created-thread observations.
- New `server/src/node/payload_owner/preparation_tests.rs`.
- This note.

`local_tests.rs` exactly matches its external snapshot. Existing behavioral
assertions and the quarantine helper are unchanged. No admission, node module,
auth, heartbeat, frame, stream, spool, job, engine, pool, executor, runtime,
manifest or lock changes were made. No branches/worktrees, commits/pushes,
secrets/.env/profiles, network/cloud/paid calls, dependency fetch/download,
alternate target or target cleanup were used. Existing server/target was used.
The filesystem reported approximately 4.1 GiB available during final inspection.

## Private API and lifetimes

`Registry::preparations` preallocates exactly 32 optional positions. Positions
0..24 are data/boot and 24..32 are control, with no borrowing. Accepted legacy
entries remain in the existing separate vector. The full final accepted-entry
bounds belong to R04b2.

`reserve_preparation(Admission, control, legacy_capacity)` reaps first, checks
closure/failure, the selected free share and checked monotonic identity, then
installs the placeholder and owned memory guards under the existing registry
mutex. Legacy capacity uses immediate try-acquire under this lock, before a
placeholder or task exists. Capacity refusal therefore preserves the previous
legacy lifetime behavior. Control supplies its already acquired concrete
OwnedSemaphorePermit, with no second acquisition. Exhaustion changes neither
the ID nor the table. Successful identity allocation never wraps or reuses IDs.

`spawn_preparation(id, kind, deadline)` obtains the runtime handle, allocates an
empty memory Slot and bounded request/readiness/activation channels outside the
mutex, and spawns one dormant task. Immediately after spawn it publishes that
JoinHandle and the original guards into the same placeholder synchronously.
There is no intervening await, caller return or user callback. If runtime lookup
fails before spawn, explicit rollback removes the placeholder, drops only its
memory guards and broadcasts progress. Empty placeholders remain visible to
close/drain while allocation/publication is pending. Publication applies any
cancellation recorded by a concurrent close and wakes surviving reapers.

`PreparedEntry` is private and move-only. It retains the shared registry, Slot,
channel endpoints and activation sender, never a JoinHandle or raw disk anchor.
Drop signals the original Slot and closes its activation channel. The dormant
task waits for activation or sticky cancellation. Abandonment records memory-only
cleanup and returns without submitting a WorkerPool command or touching the
filesystem. The same placeholder, Slot, concrete transport/lifetime guards and
handle remain registered until successful termination is observed and consumed.
An empty Disk representation contains no initialized Spool or operation owner.

`prepare_entry` supplies the private data/boot memory preparation path.
`prepare_control(OwnedSemaphorePermit)` supplies the separate control share with
no compute admission or disk initialization. Its one item-specific dead-code
allowance documents pending R04b2 production wiring. It creates no accepted
control handler. The boot fixture prepares Kind::Process with no transport or
compute guard; full boot identity and ProcessReady are still R04b2 obligations.

Legacy `register_with` reserves/prepares, then `activate_legacy` performs the
existing single-process/parent checks, retains the parent hold, constructs the
existing concrete local production future, moves the original registered Entry
into the accepted vector and sends its activation message. Refusal after
preparation leaves the discarded task registered with its original guards.
Acceptance uses that same task and JoinHandle, with no replacement spawn.
Gate-send failure marks the registry failed and retains ownership. Existing
constructors, operation UUID binding and typed local-production closures remain
reachable. Worker-only initialization/Discard, parent accounting and lifetime,
receive/send/deposit processing remain unchanged.

`reap_entry` centralizes actual handle-result consumption for both preparation
and accepted entries. Cleanup alone never releases a position or guard. Handle
failure, missing cleanup or existing cleanup failure retains the original record
and fails drain closed. `close_and_drain` cancels both empty placeholders and
published entries. `wait_reap` inspects both tables, polls in registry-owned
storage and uses the existing stable broadcast waker. A cancelled drain takes
no handle. ParentHold release still occurs only during actual successful child
reaping, before parent disposal can proceed. No second mutex or recursive-lock
Drop was added.

## Tests and provenance

The first regression ran before production edits. Refusing the second process
left only one registered owner, failing the intended `retained == 2` assertion
with left=1, exit 101, after fixture drainage, worker join and directory removal.
The same red ran a second time when an unavailable `python` command prevented
the edit script from running. The script subsequently ran with python3. Initial
new-test compilation then found private `Hooks::created` visibility; the narrow
sibling visibility change resolved it. That compiler error is not behavioral red.

Five initial preparation groups passed. A sixth adds explicit pre-spawn rollback
outside a Tokio runtime and checked identity exhaustion. These additional cases
are green-only evidence. The first affected payload run passed 35 and failed two
existing stream lifetime assertions with 4 instead of 3 references. It exposed
legacy capacity refusal retaining an extra lifetime clone. Moving legacy
try-acquire into the locked pre-placeholder reservation fixed both assertions
without changing the unowned stream tests. The next affected run passed all 37.

The final focused six-group run also observes that all dormant tasks have been
polled before checking that no work or scratch exists. The observation hook runs
inside the task, never between spawn and publication. Tests cover all 24 data/boot
positions while eight independent controls remain available; immediate bounded
repeated refusal; original task IDs and concrete data/control charges through
post-cleanup/pre-return gates; dropped tickets; cancelled and resumed drain;
close observing an empty placeholder before spawn; boot initialization using the
original registered supervisor before any transport exists; stopped-pool
memory-only abandonment; no phantom scratch; pre-spawn rollback and nonwrapping
identity exhaustion. Existing accepted-command, buffered/unpolled cancellation,
competing-waiter, pool-stop quarantine, worker disposal, parent retention,
local production and framed stream tests are reused.

New async observations use ten-second watchdogs. Gated observations are captured
as results; original tickets/observers are dropped, return gates released,
owners drained/reaped, pools joined and temporary directories removed before
final observation assertions. The outside-runtime thread is joined. No sleeps,
detached task, alternate executor or duplicate frame/Parquet matrix was added.
Existing intentional failed-drain cases retain their reviewed quarantine teardown.

## Executed verification

Every Cargo invocation used the existing target and this exact environment:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Each command below had that environment prefix, including failing invocations:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload_owner::preparation_tests -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::payload -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Results in invocation order:

- Preparation red twice, 1 failed each, exit 101, builds 46.68s and 28.22s.
- Preparation compile failure on private test-hook field, exit 101.
- Preparation green, 5 passed, 35.97s build, 0.01s tests, exit 0.
- Affected payload run, 35 passed/2 failed, 29.48s build, 5.73s tests, exit 101.
- Corrected affected run, 37 passed, 29.01s build, 6.90s tests, exit 0.
- Final preparation check after dormant polling observation, 6 passed,
  27.20s build, 0.01s tests, exit 0.
- Strict production and test Clippy, exit 0, no diagnostics, 21.33s checking.
  Clippy was submitted before the preceding test command had returned and waited
  on Cargo's build-directory lock. Actual compilation was serialized by Cargo;
  this command scheduling overlapped and is recorded rather than claimed as
  fully sequential. No other agent used the Cargo slot.

The final source removes an unnecessary Range::into_iter after the last focused
run; strict Clippy compiled that equivalent iterator change. Owned rustfmt uses
edition 2024 and skip_children=true. Scoped git diff checks and direct whitespace
and final-newline checks cover the untracked owned sources and note as well.
No unrelated lint was changed. DN-test-lints remains closed.

## Remaining boundary

R04b2 must move this machinery behind ReceiverState's one mutex and provide
transaction-bound reserve/install, real WorkKey/dependency/transport transfer,
paired account/stream installation, accepted control retention and exact boot
ProcessReady/initialization receipts. Control and boot fixture preparation here
is not final production acceptance. R04c still owns final authority/replay
composition. R07 still supplies actual worker initialization completion.

No runtime, HTTP, replay, positive idle, safe termination or distributed guarantee
is established. No concrete unowned-interface blocker was found. Fresh independent
specification review followed by quality review remains parent-owned.
Cargo/build and Rust implementation ownership are RELEASED.

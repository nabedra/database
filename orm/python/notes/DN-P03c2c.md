# DN-P03c2c serve runtime reservation

Implemented the private guard and its focused tests on shared main. Read the
applicable ancestor paths and repository for AGENTS.md; none was found. Applied
local test-driven-development and unslop guidance. Read distributed-contracts,
DN-A04 with its parent no-completed-cache override, the reviewed bridge/pool
APIs and notes, and the process helper and DN-P03b notes.

## Owned changes

- New `orm/src/meshdb/_serve_guard.py` owns the single process reservation.
- New `orm/tests/test_serve_guard.py` has five grouped causal tests.
- This note records the private integration contract and evidence.
- Minimal initialization changes in `_callback_bridge.py` and
  `_callback_pool.py` add optional private `_register_owner` hooks. Each hook
  receives its owner after safe cleanup/alive fields exist and before resource
  startup. The bridge retains its pool through `_retain_pool` before any worker
  starts, even when the pool constructor subsequently raises. Existing direct
  constructor calls remain compatible. Callback scheduling, protocol, HTTP,
  observation deadlines and cleanup join budgets are unchanged.

Other shared uncommitted work was preserved. No process/args/CLI/export, public
serve, Rust or dependency changes were made. No delegates, branches/worktrees,
commits/pushes, Cargo/builds, .env reads or cloud calls were used.

## Private integration API for P03d

Import `_reserve` from `meshdb._serve_guard`. It creates only a private state
handle and takes no user arguments. Call it before creating any bridge, pool,
child or other serve resource, including in external mode. Reentrant/concurrent
acquisition raises exactly `RuntimeError('Serve runtime unavailable')` before
any resource construction. The handle and owner default reprs contain no fields.

For managed mode, call
`reservation._create_bridge(validated_callbacks, callback_token, authorize_token)`.
It returns the existing `_CallbackBridge`. Only one bridge may be retained by a
handle, and the guard retains it before listener or thread creation. If bridge
construction raises, including from an inner pool constructor, ownership remains
in the reservation even though the caller never received the constructor result.
Use the existing bridge `url`, `activate` and `stop_admission` operations for the
later startup/process composition. External mode creates no bridge.

After the caller completes the owned child lifetime and establishes child exit
and cleanup, call `reservation._finish()`. This closes the retained original
bridge using its existing one-second ingress and two-second pool join budgets.
The explicit active flag remains set throughout that cleanup, and remains set
if cleanup raises. Cleanup failure becomes the same fixed RuntimeError without
raw cause/context. A later finish may retry failed cleanup. Finishes after a
successful finish or on stale handles do nothing. A finish during construction
fails locally and leaves the reservation active; callers must serialize their
own construction and finish. Concurrent repeated finishes do not start another
cleanup. A duplicate finish returning during cleanup is not exit evidence.

After successful finish, the module still strongly retains the bridge and its
original pool. A later `_reserve()` observes the original `alive` property outside
the transition lock. Only observed exit of every original thread permits
replacement. Failure to establish exit rejects acquisition. An idle or already
closed bridge cannot release an active lifetime. External mode stays active
until finish despite owning no callback threads. Identity checks prevent a stale
finish or competing acquisition from clearing/replacing a newer reservation.

The short module lock protects state transitions and registration only. No
provider code, thread joins, arbitrary cleanup, socket operations or owner exit
checks run under it. There is no monitor thread, timer, replacement pool, weak
reference, GC release, completed-result cache or thread/child enumeration.
Cleanup never invokes stop_node or claims cancellation of entered callbacks.

## Caller obligation and process-helper gap

The guard does not supervise or retain a child. P03d must reserve before calling
`run_process` and must not call `_finish` from its `stop` hook, which runs before
child shutdown/reaping. That hook should stop bridge admission only. P03d must
retain the active reservation whenever child exit remains unestablished.

The current process helper exposes no explicit child-exit-established result or
acknowledgement on exceptional cleanup paths. It normally waits/reaps, but an
exception escaping cleanup is not itself proof of child exit, and a failed
wait can be followed by closing the private process resources. P03d must resolve
that ownership/exit acknowledgement contract with the process owner before
finishing on such paths. This slice deliberately leaves `_serve_process.py`
unchanged and supplies no generic context manager that would finish blindly.

## TDD and focused evidence

Initial red, before the guard module existed:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_serve_guard
Ran 5 tests in 0.004s
FAILED (failures=5)
```

All five failed at the explicit missing-implementation assertion. Initial guard
implementation passed five tests in 10.358s. Then strengthened concurrent
acquisition to construct the winner's real bridge and added cleanup-error
fail-closed assertions. Final focused command:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_serve_guard test_callback_bridge.CallbackBridgeTests.test_partial_startup_failure_reaps_idle_owners_and_sanitizes test_callback_pool.CallbackPoolTests.test_partial_thread_start_failure_joins_original_idle_workers
Ran 7 tests in 10.490s
OK
```

Eight contenders use events to hold the one accepted runtime while all seven
rejections arrive. Only the winner constructs a real bridge. The next external
reservation stays active without a bridge, including after a stale finish.
Other cases prove failure before resource creation, dead-bridge active lifetime,
a real held provider callback authorized over the existing loopback fixture,
bounded finish while that callback lives, rejection until actual thread joins,
and successful acquisition after release. stop_node is never called.

Partial-constructor cases inject original pool/ingress worker entry gates and a
later Thread.start failure. Real original threads survive constructor cleanup;
the bridge and inner pool stay reachable through the guard after the constructor
raises and after finish. Every gate is released and every original thread joined
in finally. No alive-boolean fake supplies the retention evidence. Separate
checks prove cleanup failure keeps the lifetime active and an exception reading
owner exit rejects with sanitized text. Relevant existing partial-start tests
verify direct constructor compatibility and idle-owner cleanup. Full bridge and
pool suites were not repeated.

All four owned Python files were compiled in memory. Those files and this note
passed direct trailing-whitespace and final-newline checks. This is Python
component evidence only. Public serve integration, child startup, Rust fencing
and real-cluster acceptance remain outside this slice. Independent specification
and quality reviews follow; this implementation does not claim those reviews.

Independent specification review subsequently passed without actionable findings.
It inspected the guard, initialization hooks and seven relevant test bodies and
reused recorded verification without tests, probes or builds. Construction/finish
overlap and concurrent duplicate finish were supported by code inspection, not
dedicated tests. Independent code-quality review subsequently passed without
important findings. It inspected atomic reservation, original-owner retention,
stale identity protection, cleanup and exit checks, and reused the recorded
seven-test result without probes or reruns. The exceptional child-exit
acknowledgement gap remains assigned to P03b2; neither review treats an exception
as proof that the API child exited.

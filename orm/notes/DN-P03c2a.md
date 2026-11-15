# P03c2a finite callback owner

Implemented only the three new owned files on shared main:
`orm/src/meshdb/_callback_pool.py`, `orm/tests/test_callback_pool.py`, and this
note. Existing shared work was preserved. Read the distributed contracts,
A04 including its parent override, reviewed P03c1 and its actual protocol APIs,
A03 callback records/idempotency, and accepted A05 lifecycle/authorization
context. Applied test-driven-development and unslop skills.

## Private integration API

Import `_CallbackPool` directly from `meshdb._callback_pool`. No export changes.

- `_CallbackPool(callbacks, token, *, _observation_timeout=30.0)` takes the
  validated synchronous `(start_node, stop_node, list_nodes)` tuple and lifetime
  authorization token. It creates exactly three action daemon threads and one
  inventory daemon thread immediately, once. Admission starts closed. The
  underscore timeout is solely a private component-test timing seam.
- `activate(cluster_id, authorize_url)` binds the prepared, validated cluster
  UUID and exact authorization URL once. Repeated activation or activation
  after stopping raises static `RuntimeError('Callback pool activation rejected')`.
  Startup framing validates these prepared values in the later composition.
- `submit(command: _Command) -> _Call` takes an original validated command,
  strictly decodes its original immutable bytes into a pool-owned snapshot,
  and immediately returns an opaque observation handle.
  The caller must treat both the input command and handle as private protocol
  data. It reserves an idle mailbox or returns a handle with a rejection. It
  never queues behind busy callbacks. Deadline accounting starts on submit,
  before decoding and admission, and includes fresh authorization. The caller's
  mutable decoded dictionary is never used for identity or provider arguments.
- `wait(handle) -> tuple[int, bytes]` returns the HTTP status and complete
  encoded A04 body for that original invocation. It waits only to the admission
  deadline. Multiple waits observe the same terminal response. A handler may
  drop its handle on disconnect without cancelling entered work.
- `stop_admission()` permanently stops admission, prevents unentered callbacks,
  wakes idle workers, and notifies outstanding response waiters. Pending
  observations get `503 bridge_not_active`, or `504 callback_timeout` if already
  expired. Existing terminal responses stay unchanged.
- `close()` also stops admission and joins original threads for at most two
  seconds total per call. Repeated close can finish joining after provider
  release. It never kills, cancels, or replaces a callback thread.
- `alive: bool` reports whether any original worker is alive. Later runtime
  integration must retain this owner and its global runtime guard while true.
  Constructing another pool is the later process-global guard's responsibility.

`_Call` and command reprs exclude their contents. The token is a private pool
field. No credentials or provider output enter SDK diagnostics. If partial
thread startup fails, construction stops and joins the idle original workers,
then raises only `RuntimeError('Callback pool startup failed')`, outside the
original exception context. No fallback pool is created.

## Admission, outcomes and lifetimes

At most four active entries exist. Actions use `(cluster_id, action, intent_id)`;
inventory uses `(cluster_id, 'inventory', invocation_id)`. Sorted compact JSON of
validated target/payload provides deterministic, type-preserving equality:
object insertion order is irrelevant, while booleans, integers and floats keep
their JSON type spellings. Copies and identity representation have memory cost
bounded in proportion to the protocol's 1 MiB command cap and depth 32. They
are prepared outside the short admission lock. Invocation, owner and epoch do
not change stable payload identity. A separate plain payload copy is prepared
before fresh authorization and given only to start. Stop gets the exact resource
string; inventory gets no arguments. Provider mutation cannot alter internal
identity, command or result-validation context.

Identical active delivery returns `409 callback_in_progress/unknown`; changed
immutable target/payload returns `409 identity_conflict/not_invoked`. Inactive
or stopped admission returns `503 bridge_not_active`; a wrong cluster returns
`400 invalid_command`; saturated admission returns `429 callback_capacity`.
These rejections do not authorize or enter the provider. Inventory cannot borrow
an action worker, and actions cannot borrow the inventory worker.

A reserved worker calls existing `_authorize` using exact original bytes and the
earlier admission deadline. The protocol also bounds authorization to two
seconds. Validated denial status/code pairs are preserved; malformed/transport
`authorization_denied` maps to `503 ownership_unavailable/not_invoked`.
The worker checks terminal observation, deadline and stopping under the same
lock that marks entry. A timed out or stopped unentered callback never enters
later. No lock spans authority or provider I/O.

After entry, start/stop failures report `unknown`. Provider exceptions, including
`SystemExit` and other `BaseException`s, become `502 callback_failed`; invalid
and awaitable results become `502 invalid_callback_result`; bounded encoder
limits become `413 limit_exceeded`. The existing `_encode_result` validates the
complete success body. Inventory failures supply no usable inventory and use
`not_invoked`, except an active duplicate's explicit `unknown` response.

Observation timeout returns `504 callback_timeout`, with `unknown` for entered
start/stop. Its slot and identity remain occupied until actual return and result
validation. Late results never overwrite terminal observations or produce
unsolicited completion. Inventory still progresses with three hung actions;
a hung inventory worker degrades reconciliation too. No pending callback queue,
per-call timers, replacement workers, completed-result cache, durable writes or
CAS exist here. On real completion the pool removes the entry and clears the
mailbox and worker's completed-call locals. Caller-owned handles may retain
their bounded observations. Later delivery must authorize and invoke again;
provider durable idempotency supplies duplicate safety.

Stopping cannot revoke an already entered provider effect. Busy callbacks can
survive close as daemons and retain owner state. On actual return they exit if
stopping. Tests explicitly record a late effect under a changed simulated owner
epoch. This is honest component evidence, not Rust ownership or fencing proof.

## TDD and verification

Wrote eight grouped causal behavior tests before implementation. Initial red:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_callback_pool
Ran 8 tests in 0.003s
FAILED (failures=8)
```

All failed with the explicit missing-implementation assertion. Minimal
implementation then passed all eight in 3.042s. An added active-inventory
duplicate assertion was observed red before fixing its outcome classification:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_callback_pool.CallbackPoolTests.test_entered_late_effect_and_hung_inventory_are_not_cancelled
Ran 1 test in 0.188s
FAILED (failures=1)
```

The failure was `not_invoked` instead of the required active duplicate `unknown`.
After correction the full focused group passed in 3.069s. Final verification
repeated the full focused command after clearing completed worker locals and
moving the separate provider payload copy ahead of authorization. Result:
`Ran 8 tests in 2.960s`, `OK`.

The eight groups exercise fresh TCP authorization before entry, zero entries on
ownership denial, malformed-authority rejection, admission/cluster checks,
three blocked actions plus reserved inventory, overload, repeated timeouts with
unchanged original workers, active identity conflicts, JSON type/order behavior,
provider mutation isolation, sequential freshly authorized delivery using one
provider registry resource, authorization gates across deadline/stop, late
entered effects, inventory degradation, static failures, bounded close and live
owner evidence, and cleanup after partial worker startup.

The independent authority fixture uses one bounded server thread, real loopback
TCP, exact command digest/invocation replies and the expected bearer token. It
reuses only read-only command builders/constants from existing protocol tests.
Provider and authority gates use events; waits have failure watchdogs, with
short private observation deadlines. There are no sleep-based overlap checks.
All callback gates are released in finally, pools joined, and authority threads
joined with sockets closed. No hung test workers remain.

In-memory syntax and direct whitespace check command:

```text
orm/.venv/bin/python3.11 - <<'PY'
from pathlib import Path
files = [Path('orm/src/meshdb/_callback_pool.py'), Path('orm/tests/test_callback_pool.py'), Path('orm/notes/DN-P03c2a.md')]
for path in files:
    text = path.read_text()
    if path.suffix == '.py':
        compile(text, str(path), 'exec')
    assert text.endswith('\n'), path
    assert all(line == line.rstrip() for line in text.splitlines()), path
print('In-memory syntax and direct whitespace checks passed for 3 files')
PY
```

## Remaining integration

P03c2b/P03c2c/P03d own bounded HTTP ingress, response-write deadlines, disconnect/socket
cleanup, the process-global runtime guard, startup/child/signal composition and
public serve. They must never release/recreate a runtime while original threads
remain alive. Actual Rust authorization, storage ownership checks, stale
completion CAS rejection and full lifecycle acceptance remain outside this
slice. No listener, runtime global, child process, signal hook, full bridge or
public serve was added. No protocol/process/args/export/existing test changes,
delegation, branches/worktrees, commits/pushes, Cargo, dependencies, cloud or
.env access occurred.

## Specification correction: bind admission to authorized bytes

Independent specification review found that copying the caller's mutable
`command.value` could authorize one stop resource through `original_bytes` and
invoke the provider on a different resource. The same mismatch could affect a
start payload. The parent corrected admission to call the existing strict
`_decode_command(command.original_bytes)`. Active identity, invocation, provider
arguments and result validation now all derive from that pool-owned decoded
snapshot. The separate provider-owned start copy remains in place.

Added one grouped real-loopback regression for both start and stop. Each mutates
the decoded command before submission, holds authorization, then clears the
caller's dictionary after admission. The assertions compare the actual authority
bytes with the original bytes and the provider argument with their decoded
payload. All owned workers and fixture sockets are closed after the scenario.

Before the fix:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_callback_pool.CallbackPoolTests.test_caller_mutation_cannot_change_authorized_arguments
Ran 1 test in 0.210s
FAILED (failures=2)
```

Both failures showed the mutated provider argument while authorization received
the original bytes. After the correction, the affected boundary checks passed:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_callback_pool.CallbackPoolTests.test_caller_mutation_cannot_change_authorized_arguments test_callback_pool.CallbackPoolTests.test_active_identity_mutation_isolation_and_fresh_provider_replay test_callback_pool.CallbackPoolTests.test_json_identity_preserves_types_and_ignores_dictionary_order test_callback_pool.CallbackPoolTests.test_authorization_deadline_and_stop_prevent_late_entry
Ran 4 tests in 0.608s
OK
```

In-memory syntax and direct whitespace checks for the three owned files passed.
The fresh independent specification re-check passed with no remaining findings.
The subsequent independent code-quality review also passed without important
findings. Both inspected the correction and recorded regression evidence without
repeating tests or builds. The quality review inspected all nine current test
bodies; a full nine-method rerun is not claimed. Actual Rust authorization and
runtime integration remain outside this correction.

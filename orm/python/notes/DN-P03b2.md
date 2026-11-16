# P03b2 retained process exit receipt

Implemented the bounded supervisor/guard follow-up on 2026-11-11. No applicable
AGENTS.md was found in the repository or ancestor paths. Read distributed-contracts,
the P03 rubric, DN-A04 including the no-completed-cache override, DN-P03b and
DN-P03c2c, and the actual process/guard tests. Applied test-driven-development and
unslop. No delegation, branches, commits, cloud calls, .env reads, Cargo, builds,
Rust changes or public serve export. Shared unrelated work was preserved.

## Changes and private API

Only `_serve_process.py`, `_serve_guard.py`, new `test_serve_exit.py` and this
note changed. Existing callers and test bodies are unchanged. The independent
`support/serve_child.py` fixture is reused without modification.

Create `_serve_process._ExitReceipt()` and immediately pass it to
`reservation._attach_process(receipt)` before bridge/pool construction or child
startup, including external mode. One reservation accepts one unused receipt,
and a receipt can attach to only one reservation. Existing bridge-only uses of
the guard remain compatible. There is no automatic process discovery.

`run_process(..., prepared=..., stop=..., _receipt=receipt)` consumes that receipt
once. Its `_owner` is strongly retained before `install()` creates the selector,
sockets or handlers. `_Owner.__init__` now initializes fields without external
resource side effects. The same owner retains its actual Popen even if startup,
shutdown, wait/poll or resource close fails and no return value reaches the caller.
A used receipt cannot start another process, including after successful cleanup.

Receipt state separates `_active`, `_exited` and `_cleaned`. `_ready()` requires a
consumed receipt, an inactive run, established exit and completed resource cleanup.
Activity includes saved-handler dispatch after cleanup. A dead/reaped child
cannot release a still-active run. Exit proof is the owned Popen's established
returncode or poll result, or completed no-spawn execution. Shutdown messages,
signal delivery, stopped admission, exceptions and caller return supply no proof.
An already reaped returncode remains proof even if a later OS poll would fail.

`receipt._retry()` is an explicit serialized operation after run_process returns
or raises. It retries emergency reap and resource close on the same owner. For an
unused receipt, it explicitly consumes it as no-spawn completion, covering bridge
construction failure before run_process. During an active run retry rejects with
fixed RuntimeError text. Retry never launches a replacement, enumerates children,
signals a process group or acts on a saved bare PID. Popen owns kill/poll/wait.
Failed observation or cleanup raises sanitized MeshDBError `shutdown_failed`;
there is no success claim while exit is unresolved. Repeated retries are allowed.

The guard's `_finish()` snapshots the receipt outside construction transitions
and checks `_ready()` outside its lock before bridge cleanup. An unresolved or
active receipt leaves the reservation active and raises the fixed
`RuntimeError('Serve runtime unavailable')`. Bridge/pool partial ownership,
construction/finish exclusion, stale identities and later original-thread exit
checks retain their previous behavior. Process observations, shutdown, close,
bridge cleanup and joins all occur outside the guard lock. There are no monitor
threads, weak references, GC release hooks or completed-result caches.

## P03d caller sequence

The composition must use this sequence in both managed and external modes:

```python
reservation = _reserve()
receipt = _ExitReceipt()
reservation._attach_process(receipt)  # before any resource construction
bridge = None
try:
    if managed:
        bridge = reservation._create_bridge(callbacks, callback_token, authorize_token)
    run_process(config_path, startup,
                prepared=install_authorization_and_admission,
                stop=lambda: bridge.stop_admission() if bridge is not None else None,
                _receipt=receipt)
finally:
    try:
        if not receipt._ready():
            receipt._retry()
    finally:
        reservation._finish()
```

The stop hook only stops callback admission. It must never finish the reservation,
because it runs before child shutdown/reaping. Caller-owned hooks must be short.
If retry fails, finish also refuses release. The module's current reservation
still retains the receipt/owner; the caller should retain its local handles for a
later serialized `receipt._retry(); reservation._finish()` recovery. P03d must
choose how to present competing failures using sanitized SDK errors; exception
precedence in this illustrative sequence is not proof of exit. It must not retry
while run_process or another recovery operation is active. Recovery that restores
main-thread signal handlers belongs on the original main thread.

`close()` attempts restoration of each still-owned saved handler and closure of
every resource even if another operation fails. Failed cleanup remains retryable.
The startup 30-second budget, two-second framing budget, wire records, signal
forwarding, caller-replaced handlers and diagnostics cap remain unchanged.
Graceful shutdown still gets ten seconds. Post-kill wait now has a ten-second
observation timeout; an exceptional shutdown may additionally attempt one
emergency kill/wait with a ten-second wait. Thus the ordinary Python exit path
has finite wait budgets, with up to thirty seconds across grace and both waits.
Each explicit retry allows one ten-second reap wait. Existing bridge ingress and
pool join budgets remain one and two seconds. OS calls, hooks or arbitrary close
implementations themselves cannot be given a universal wall-clock guarantee.
Timeout or OS failure retains ownership rather than inventing bounded success.

## TDD and targeted evidence

Initial red before production changes:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_serve_exit.ServeExitTests
Ran 5 tests in 0.003s
FAILED (failures=5)
```

All five failed at the explicit missing-receipt assertion. Initial green passed
five tests in 1.079s. A further red exposed premature release during saved-handler
dispatch:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_serve_exit.ServeExitTests.test_saved_handler_still_owns_active_run
Ran 1 test in 0.246s
FAILED (failures=1)
```

The guard incorrectly accepted finish from that still-running handler. After
moving inactive state past handler dispatch, this affected regression run passed:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_serve_exit.ServeExitTests test_serve_guard test_serve_process.ServeProcessTests.test_selector_failure_stops_admission_and_sanitizes test_serve_process.ServeProcessTests.test_signal_arriving_during_failure_shutdown_is_forwarded test_serve_process.ServeProcessTests.test_second_signal_expedites_owned_kill test_serve_process.ServeProcessTests.test_failed_spawn_closes_both_socket_ends test_serve_process.ServeProcessTests.test_signal_handshake_flood_and_reap test_serve_process.ServeProcessTests.test_stop_interrupt_still_reaps
Ran 17 tests in 16.424s
OK
```

Added resource-close failure and unused-receipt completion coverage passed eight
tests in 1.661s. A final red covered established returncode despite failed later
poll:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_serve_exit.ServeExitTests.test_reaped_returncode_is_exit_proof_even_if_later_poll_fails
Ran 1 test in 0.224s
FAILED (failures=1)
```

It incorrectly replaced `bind_failed` with `shutdown_failed` despite already
having reaped the child. Corrected observation uses established returncode first.
Final targeted green:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_serve_exit.ServeExitTests
Ran 9 tests in 1.948s
OK
```

New tests use actual owned subprocesses and framed readiness. The held child
enters its existing shutdown gate before only Popen.poll is made to fail.
Failed run and retry leave that real child alive, retain the exact original
owner, and reject another reservation. Actual owned kill/wait followed by retry
permits release. A prepared-hook event holds a run after its child was killed and
reaped, proving exit alone cannot finish an active run. All gates release and
owned children reap in finally paths. Further cases distinguish real startup
failure/exit from close failure, no-spawn selector failure, and a failing selector
close from the remaining successfully closed sockets/pipes and restored handlers.
The actual signal host reuses its independent ready-line watchdog and verifies
receipt readiness after normal SIGTERM return. No sleeps establish exit proof.

Unchanged process framing/validation and bridge/pool behavior reuse DN-P03b and
DN-P03c2c passing evidence; no broad Python suite was rerun. The later returncode
change affects only receipt observation, so prior focused guard/process evidence
is reused. In-memory syntax compilation of all three owned Python files and
trailing-whitespace/final-newline checks of all four owned files passed, as did
scoped git diff whitespace checking.

This establishes Python component behavior only. Actual Rust handshake,
authorization, bridge/process composition and real-cluster acceptance remain
P03d and subsequent acceptance work. No concrete implementation blocker remains.
Independent specification and quality reviews are parent follow-ups.

Independent specification review subsequently passed without actionable findings.
It inspected retained owner/Popen identity, active/exit/cleanup separation,
saved-handler lifetime, guard checks, explicit same-owner recovery, retryable
resource cleanup and the finite observation budgets. The reviewer reused the
recorded nine final receipt tests and seventeen earlier affected regressions,
with no tests, probes, builds or edits. Quality review remains pending. Public
serve/Rust composition still must attach the receipt before resources and use
admission-only stop hooks.

## Parent correction after quality review

Quality review found that explicit recovery called reap before observing an
already established returncode. If a previously unresolved child had since been
reaped but a later poll failed, recovery still rejected release. The parent
extended the existing held-child recovery test: after actual kill/wait it keeps
the failing poll injection active while the receipt still has `_exited=False`.
That focused regression failed with `shutdown_failed` before correction (one
test, 0.176s). The real child and reservation were cleaned in its finally path.

`_Owner.reap()` now performs OS observation/termination only while the retained
Popen has no established returncode. `_retry()` then records that existing exit
proof and completes resource cleanup without another poll. The correction adds
no new process handle, guessed PID or exception-based exit inference.

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_serve_exit.ServeExitTests.test_unknown_exit_retains_exact_live_owner_and_recovers
Ran 1 test in 0.176s; FAILED (errors=1), before production correction.

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_serve_exit.ServeExitTests
Ran 9 tests in 1.545s; OK, after correction.

git diff --check -- orm/src/meshdb/_serve_process.py orm/tests/test_serve_exit.py
```

In-memory compilation and direct trailing-whitespace/final-newline checks passed
for both changed Python files. The earlier affected process/guard regressions
remain recorded evidence; no broader suite was repeated. Fresh independent
quality re-check remains pending.

Fresh independent quality re-check passed without a concrete ownership,
exception, cleanup or signal regression. It inspected the real-child regression,
same-owner recovery and finally cleanup, and reused the recorded failing-before
and nine-test passing-after evidence without reruns or edits. This completes
P03b2. Serialized recovery and original-main-thread signal restoration remain
caller obligations; actual public serve/Rust integration is still deferred.

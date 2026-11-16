# P03b private process ownership

Implemented only the process slice on 2026-11-10. No applicable AGENTS.md was
found in the repository ancestors or owned subdirectories. Read the authoritative
`plans/distributed-contracts.md` and all of DN-A04, including its parent override.
No completed callback cache, callback execution, provider ownership, public
serve, exports, Rust edits or real-cluster integration are included.

## Private API for P03d

`_serve_process.run_process(config_path, startup, *, prepared, stop)` blocks and
returns None after signal-driven shutdown, subject to the saved signal handler.
`config_path` is the already-validated operator config path. The helper makes
its argv spelling absolute but never opens or parses the operator config.
`startup` is the exact A04 init dictionary, including the already-bound bridge
URL and two independent canonical 32-byte base64url tokens, or external/null
bridge. This helper validates the wire record, not Rust's config schema. It
creates no tokens, bridge listener, callback pool or worker resources.

`prepared(record)` receives the validated prepared dictionary and must install
the authorization target and open bridge admission synchronously before it
returns. Only then is activate sent. `stop()` closes admission and is called
once on every exit, even validation, resolution and spawn failures. Hooks are
small internal operations and must return promptly. They are not provider
callbacks. Bridge thread joining and runtime guard retention belong to P03d and
the bridge owner. Exceptions from hooks are sanitized; KeyboardInterrupt and
SystemExit are delayed until cleanup. This module imports neither unfinished
serve arguments nor bridge/serve modules.

Only the inherited FD number enters MESHDB_SUPERVISOR_FD. The binary resolver is
reused; argv is binary/api/--config/absolute-path with pass_fds, close_fds and a
new session. Init carries the initial policy and bridge credentials privately.
Prepared and ready require canonical matching UUIDs and identical bound URLs.
Managed authorization URLs require exact numeric loopback HTTP route and a
nonzero port; external authorization must be null. Ready means runnable
machinery, not fleet convergence. Only the bounded ready JSON line is announced.

One monotonic 30-second startup budget covers framed partial I/O and activation.
After ready, idle channel reads have no deadline; a started frame has two seconds
to assemble. Frames reject empty/oversized lengths, duplicates, unsupported
versions, unknown fields, invalid UTF-8, nonfinite numbers and incorrect types.
Unexpected EOF, child exit, malformed traffic and fatal startup codes fail with
static MeshDBError text and bounded static codes, suppressing raw error chains.

A single selector owns the channel, signal wake socket and two child diagnostic
pipes. Each private diagnostic buffer retains at most 1 MiB and discards all
later bytes while continuing to drain. There are no drain threads, log files or
public diagnostic handles. Pipes and socket ends close on success and failure.

Main-thread handlers only record and wake. Teardown closes admission, attempts
shutdown and forwards the first signal only while the owned Popen child is
live. It allows ten seconds before killing that child and waiting for reaping.
A second signal expedites killing. No process group or recorded bare PID is
signalled, and there is only one reaper. Saved handlers are restored only when
still owned. The saved first-signal callable runs once after cleanup; default
SIGINT raises KeyboardInterrupt, SIG_IGN does nothing, and default SIGTERM
permits return. Forced termination is a MeshDBError if the saved handler returns;
a saved handler's own exception, including normal SIGINT KeyboardInterrupt,
retains its usual behavior.

## Focused evidence

Tests were written before the module. The initial supported-interpreter run of
`PYTHONPATH=orm/src python3.11 -m unittest discover -s orm/tests -p test_serve_process.py`
failed all 12 initial subprocess scenarios because `_serve_process` was missing.
An earlier invocation accidentally used the host's Python 3.6, below the package's
3.11 minimum; this was corrected without installing anything. A later added
reaping assertion failed when a stop hook raised KeyboardInterrupt. Cleanup was
corrected to delay that exception until after wait/reap. An exact-route
regression then showed an empty trailing question mark was accepted in a
private URL; strict URL spelling now rejects it before activation.

The final focused command above passes **8 test methods, 22 process scenarios**
including one injected Popen failure. The executable fixture implements its
wire independently and never imports production helpers. It binds an actual
127.0.0.1:0 listener; the prepared hook connects before activate. Private records
contain only events and child PID. Tests check same-child identity, explicit
Popen reaping, signal forwarding/restoration, callable saved handlers and caller
replacement of a handler. They exercise external mode, startup signal ordering,
duplicate JSON, truncated and stalled startup, fatal and invalid URLs, prepared/
ready mismatch, EOF and nonzero exits, stalled lifetime framing, hook and actual
closed-stdout announcement failures, invalid explicit binary and failed spawn.
A permanently held shutdown gate proves forced termination; a stop event orders
the second-signal case. A pre-prepared flood writes 2.5 MiB per child pipe;
private inspection confirms exactly 1 MiB retained per pipe and no raw output
forwarded. Watchdogs only bound failures; no sleeps establish correctness.

Syntax compilation and direct trailing-whitespace checks cover only these new
Python files. No full suite, installation, Cargo command, cloud access or .env
access was performed. Shared uncommitted work was preserved.

## Limits

Evidence is against the tiny Python executable, not Rust's unfinished handshake.
There is no real API/serve claim and no export is authorized by this slice.
P03d still needs actual Rust startup/authorization integration and bridge guard/
thread cleanup. Non-main-thread execution installs no handlers and offers no
cross-thread cancellation guarantee. Hooks and caller-owned adapters must obey
their own bounded-operation contracts. Announcement temporarily makes the
stdout FD nonblocking and restores its prior mode; unrelated concurrent stdout
writers remain the embedding application's responsibility.

After SIGKILL the owner waits to confirm reaping. An OS-uninterruptible child can
therefore keep the call blocked; this cannot honestly guarantee a bounded reap.
Parent SIGKILL cannot execute Python finally blocks. No cleanup promise extends
to provider-created threads/subprocesses or worker resources.

## Quality-review corrections

The independent specification review passed. Quality review found selector
construction outside the protected lifecycle, a signal-forwarding race, and a
second-signal scenario whose short ordinary deadline could satisfy its watchdog.

The coordinator added two regressions before correcting production code. An
injected DefaultSelector failure escaped as raw OSError without calling stop.
A real subprocess scenario injected actual SIGTERM just after the initial
empty-signal snapshot during failure cleanup; the held child received no signal
and required forced termination. Both failed in the initial two-test run
(one error, one assertion failure, 0.825 seconds).

Owner construction now occurs inside the protected lifecycle. A missing owner
still calls stop exactly once and returns a sanitized startup failure. The
forwarded flag starts false and becomes true only after an actual forwarding
attempt. The second-signal scenario now uses a 30-second ordinary exit deadline,
longer than its five-second completion watchdog, making the second signal
necessary for the test to finish successfully.

The focused command was:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests python3.11 -m unittest test_serve_process.ServeProcessTests.test_selector_failure_stops_admission_and_sanitizes test_serve_process.ServeProcessTests.test_signal_arriving_during_failure_shutdown_is_forwarded test_serve_process.ServeProcessTests.test_second_signal_expedites_owned_kill test_serve_process.ServeProcessTests.test_failed_spawn_closes_both_socket_ends test_serve_process.ServeProcessTests.test_signal_handshake_flood_and_reap test_serve_process.ServeProcessTests.test_stop_interrupt_still_reaps
```

All six methods passed in 2.606 seconds. The initial red command used the first
two method names only. Owned Python syntax, direct whitespace and scoped diff
checks also passed. The remaining previously passing scenarios were not rerun.
The fresh quality re-check passed all three corrections without new actionable
findings. No real Rust handshake is claimed.

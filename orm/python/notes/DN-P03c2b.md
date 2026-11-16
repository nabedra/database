# P03c2b bounded authenticated HTTP ingress

Implemented only the three new assigned files on shared main:
`orm/src/meshdb/_callback_bridge.py`, `orm/tests/test_callback_bridge.py`, and
this note. Preserved the other extensive uncommitted work. Read the distributed
contracts, relevant distributed-nodes rubric, A04 including the parent's
no-completed-cache override, reviewed P03c1/P03c2a notes and actual protocol/pool
interfaces. No applicable AGENTS.md was found in the repository or ancestors.
Applied local test-driven-development and unslop skills and causal test guidance.

## Private API and integration obligations

Import `_CallbackBridge` directly from `meshdb._callback_bridge`.

- `_CallbackBridge(callbacks, callback_token, authorize_token)` accepts the
  already validated synchronous callback tuple and independent validated lifetime
  tokens. The callback token authenticates ingress. Only the pool receives the
  authorization token. Neither appears in repr, errors or logs. Construction
  binds an AF_INET socket to `127.0.0.1:0`, listens with backlog eight, creates the
  existing four-worker pool, eight fixed daemon HTTP handlers and one daemon
  accept owner. Pool admission starts closed.
- `url: str` is the actual allocated callback URL, including the numeric port and
  exact `/private/v1/callback` route. P03d must carry it and the two lifetime
  tokens only over the private startup channel. There is no guessed-port step.
- `activate(cluster_id, authorize_url)` delegates to the existing pool. P03d
  must first validate the prepared startup message and exact authority binding.
  It then activates this bridge before sending the child activation message.
  This method does not independently validate startup records or addresses.
- `stop_admission()` is terminal. It stops pool admission and wakes observation
  waits, closes the listener and all registered queued/active sockets, clears
  the accepted queue and wakes idle handlers. An accept racing shutdown closes
  its just-accepted socket after observing the terminal flag. There are no joins
  or waits for provider functions. Repeated calls are safe.
- `close()` calls stop_admission, joins the original ingress threads under one
  absolute one-second deadline, then calls existing pool.close exactly once per
  cleanup attempt. That method joins callback threads for at most two seconds
  total. Thus join budgets total at most three seconds per attempt, plus short
  local socket/lock work and normal scheduler overhead. Repeated close can reap
  owners after their callback returns. It never calls stop_node.
- `alive: bool` checks every original ingress thread and the original pool's
  callback ownership, including after close or partial construction. It does
  not mean that sockets remain open or admission remains possible. P03c2c must
  retain this owner and its runtime reservation while alive is true. P03d must
  preserve ownership through startup failure and teardown and call cleanup.

Construction failure stops and joins already-started idle owners and closes
its socket. It raises only `RuntimeError('Callback bridge startup failed')`
outside the original exception handler, with no cause or exception context.
No constructor callback can enter because pool admission remains closed.
Original thread lists remain available for honest ownership accounting; no
replacement pool or replacement ingress thread is created.

## Wire and resource bounds

The accepted FIFO holds at most eight sockets, in addition to the eight active
handlers, one transient accept-owned socket and kernel backlog eight. Queue
saturation closes immediately without trying to send a response. There is no
second callback queue, executor, per-request thread, timer, retry or result cache.
The pool remains the reviewed three action plus one inventory threads.

Every accepted socket gets one absolute two-second assembly deadline at accept,
including queue residence. Every socket read receives only the remaining budget.
Header reads stop exactly at the terminator without consuming coalesced body
bytes before authentication. Headers allocate at most 8 KiB. Header names and
control characters are validated; all duplicate headers and obs-fold are
rejected. Only exact HTTP/1.1 POST, the exact callback route, and Host naming the
actual numeric loopback address/port are accepted. Bearer bytes are compared
with hmac.compare_digest before body reads, JSON decoding or pool/provider work.

Content-Length is mandatory, canonical unsigned decimal and at most 1 MiB.
Content-Type must be application/json, case insensitive, without parameters.
Transfer/content encoding, upgrade and connection upgrade are rejected. Expect
is unsupported, including 100-continue, and produces a fixed 400 without any
interim response or body read. Wrong bearer produces 401 first. Body reads
allocate only up to Content-Length. A complete request receives its response
without waiting for client EOF. Trailing bytes are discarded when closing;
no second request is parsed or dispatched. Replies always include Connection:
close and a fixed Content-Length. No redirect responses exist.

Original bytes go through reviewed _decode_command and pool.submit/wait. The
pool supplies fresh exact-byte-bound authorization, active identity decisions,
capacity, 30-second observations and complete bounded results. Transport loss
only drops the connection; an entered callback remains owned until actual
return. Before valid command parsing, errors use null invocation_id and fixed
400 invalid_command, 401 unauthorized or 413 limit_exceeded envelopes with
not_invoked. Pool statuses and bodies pass through unchanged. Assembly timeout
maps to fixed 400 if a response remains writable. Socket loss can yield no
response. There are no request logs, input echoes, provider text or tracebacks.

Response writes have a fresh absolute two-second deadline after the result
becomes available. Every partial send gets the remaining budget. The private
module constant `_IO_TIMEOUT` permits short component-test I/O deadlines;
production defaults to two seconds and no public timeout option is added.

## TDD and focused evidence

Seven grouped real-TCP tests were written before the module existed. Initial red:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_callback_bridge
Ran 7 tests in 0.003s
FAILED (failures=7)
```

Every failure was the explicit missing P03c2b implementation assertion. Initial
implementation passed those seven groups. Strengthened absolute-budget tests
and added a blocked-write group. Final focused Python 3.11 command:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python3.11 -m unittest test_callback_bridge
Ran 8 tests in 4.138s
OK
```

The independent bounded authority fixture uses a single server thread, exact
bearer/route checks, bounded TCP framing and SHA256 over actual original bytes.
Only read-only command builders/constants are reused from protocol tests.
Authority events gate denial after request receipt, proving zero callback
entries before the ownership decision. Successful transport preserves original
whitespace and provider arguments.

Three actual callback-entry events establish held action occupancy; inventory
then completes over HTTP while all three remain held. Disconnecting one client
preserves active duplicate and changed-identity rejection. Capacity remains
full. Close returns under its join bound while alive remains true; finally
releases callbacks and joins them, then verifies alive false. Existing pool
tests provide the callback algorithm/observation-expiry evidence, rather than
copying their full suite.

Ingress saturation gates exactly eight handlers. A subsequent excess connection
closing proves the preceding eight FIFO accepts filled the bounded queue.
Stopping closes both queued and active peer sockets. Thread counts stay fixed.
Separate tests trickle header/body bytes until the handler exits under its
absolute deadline and hold a complete queued request past its acceptance budget.
A large valid inventory response, small TCP buffers and a nonreading client
cause an observed socket write timeout within the fresh write deadline. The
authority gate first outlasts the assembly budget, proving response writes get a
new budget. Timing measures deadline contracts or watchdogs; event/counter
assertions establish overlap and ownership. Finally paths release gates, close
owned sockets and join all fixture/provider/ingress owners.

Partial-start failures after zero, two, four, seven and twelve successful starts
exercise pool construction, handler startup and accept-owner startup. Every
started idle thread is reaped, the listener is closed, alive is false and the
raised exception has neither raw context nor cause.

Owned Python files were compiled in memory and all three files checked directly
for trailing whitespace and final newline. No Cargo/build, dependency change,
.env read, cloud call, delegate, branch/worktree, commit or push occurred. Only
focused bridge tests were run. No public export, global guard, process lifecycle,
Rust authority, real cluster lifecycle or serve integration is claimed.

Implementation is ready for independent specification review, then quality
review. These reviews have not been performed by this implementation task.

Independent specification review subsequently passed with no actionable findings.
It inspected the three files, contracts, test causality and recorded evidence;
it ran no probes, tests or builds and changed no files. Independent quality review
is the next step. The deferred global guard and real Rust integration remain
outside this slice.

Independent code-quality review then passed without actionable findings after
inspecting ownership, framing/deadlines, shutdown races, cleanup and test gates.
It reused the eight-group passing evidence and ran no probes, tests or builds.
The bounded HTTP component is complete; process reservation/retention and public
Rust integration remain separate assignments.

# DN-P02c: bounded cluster scale waits

Status: specification review passed; the quality finding was corrected and the
independent quality re-check passed. Real-cluster integration remains pending.

Changed only `orm/src/meshdb/cluster.py`, `orm/src/meshdb/__init__.py`, new
`orm/tests/test_cluster_wait.py`, and this note. Existing `test_cluster.py` needed
no adjustment. Preserved the substantial pre-existing shared-main changes.
No applicable AGENTS.md was found in the repository or its ancestor directories.
Read the TDD and subagent workflow instructions; the explicit assignment overrides
delegation, worktrees and commits. No delegates, branches, commits, cloud calls,
.env access, Cargo, packaging, full Python suite, or large acceptance run occurred.

## Behavior

The package exports `Cluster`. Its only public methods remain `scale`, `autoscale`
and `status`; constructor, autoscale and status signatures remain unchanged.
`scale(nodes, *, wait=False, timeout=300.0)` validates exact bool wait, finite
positive int/float seconds excluding bool, and node bounds before I/O, including
when wait is false. Constructor timeout remains the ordinary per-I/O budget.

Scale generates one submission UUID and never retries the mutation. Without
waiting it returns the same validated acceptance shape. Waiting starts a single
monotonic deadline before submission and passes that deadline to the existing
transport for submission and each status request. Existing acceptance and status
validators are reused through small private helpers. No new public clock or
configuration objects were added; import and construction send no requests.

A matching revision, ready count equal to accepted desired count, and absence of
pending/in_progress/reconciling intents completes the wait and returns the
original acceptance dictionary. Retained completed and failed intents do not
block convergence. A different revision raises local `ConflictError` with
`policy_superseded`, the submission request ID, no HTTP status, and explicitly
cleared outcome. Shared error defaults were not changed.

Expiration after validated acceptance raises `TransportError` with `wait_timeout`,
the original submission request ID, no HTTP status and outcome None. Deadline
checks surround status parsing, polls and sleeps; the fixed 100 ms interval clamps
to remaining time. Neither timeout nor supersession cancels or reverts a policy.
Submission failures propagate unchanged, so a possibly sent unacknowledged write
and invalid acknowledgement remain UnknownOutcomeError while proven pre-send
failures remain TransportError. Non-expired status errors propagate unchanged.
KeyboardInterrupt and SystemExit are not caught by the wait layer.

## Checks

Initial regression, before implementation:

```sh
PYTHONPATH=orm/src:orm/tests python3.11 -B -m unittest test_cluster_wait.ClusterWaitTests.test_final_public_signature_and_export -v
```

Ran 1 test, failed as expected with 1 assertion failure: the existing signature was
`(self, nodes: 'int') -> 'dict'`, missing wait and timeout.

Focused command after implementation and after fixture cleanup correction:

```sh
PYTHONPATH=orm/src:orm/tests python3.11 -B -m unittest test_cluster_wait test_cluster -v
```

Final run passed all 11 tests in 2.629 seconds: 5 new grouped tests and 6 existing
policy/status regressions. The first run also passed 11 tests, but printed an
owned HTTP handler cleanup exception when shutting down a socket already closed
by the deadline. The fixture now tolerates that expected disconnected-socket
shutdown, and the final run emitted no handler exceptions.

The real loopback HTTP scenarios prove acceptance followed by a ready status
with a pending intent requires another poll, then convergence returns the
acceptance record; a stalled status after acceptance expires with correct local
diagnostics and exactly one mutation plus one status request; invalid arguments
send nothing; and a consumed submission with a dropped acknowledgement remains
unknown without retry. Poll sequence counters and an event gate drive these
scenarios. Handler threads are owned, released, and joined through server_close;
the serving thread is joined. No global time-module patch affects socket timers.

The separate scripted transport/clock scenario proves all three incomplete
intent states block completion, a retained failed intent permits completion,
supersession diagnostics, submission budget consumption, post-status-parse
expiration, expired-read remapping, sleep clamping, ordinary read-error
propagation, invalid acknowledgement classification and wait=False behavior.
These scripted cases are deterministic control-flow evidence, not network evidence.
The existing transport timeout/TLS matrix is reused without duplication or rerun.

```sh
python3.11 -m py_compile orm/src/meshdb/cluster.py orm/src/meshdb/__init__.py orm/tests/test_cluster_wait.py
git diff --check -- orm/src/meshdb/cluster.py orm/src/meshdb/__init__.py orm/tests/test_cluster_wait.py orm/tests/test_cluster.py orm/notes/DN-P02c.md
```

Both exited 0. A separate explicit whitespace scan covers the untracked owned
files that git diff does not inspect.

## Limits

These are client contract tests against local HTTP peers, not real cluster
readiness evidence. No serve publication, provisioning, controller reconciliation,
Rust build, real cluster acceptance or independent review is claimed. No transport
defect blocked this slice. Previously reviewed transport behavior and real-CA
HTTPS evidence remain dependencies of the parent integration.

## Quality correction: platform timeout ranges

The independent quality review found that a valid `timeout=1e20` overflowed
the background Timer's native wait. The coordinator added a real TCP regression
which observes native wait arguments and captures thread exceptions. Its initial
run failed on an out-of-range wait. The deadline now owns one cancellable thread
which waits in segments no greater than `threading.TIMEOUT_MAX`, rechecking the
original monotonic deadline after each segment. Cancellation wakes and joins the
same thread; it never creates a chain of replacement timers.

Adding the hostname variant exposed the same float-range problem in the resolver
subprocess's selector timeout. That run had one error and three passing existing
deadline/TLS regressions. Resolver communication now waits in at most 60-second
segments, retaining the original deadline and existing kill/reap cleanup. No DNS
lookup or policy is resubmitted when a communication segment ends.

The new grouped test covers numeric and hostname endpoints with a huge finite
deadline, and an artificially small native-wait ceiling. The latter holds the
real HTTP acknowledgement until the guard has entered a second bounded wait,
proving that a segment boundary does not expire the absolute deadline. Real
threads, sockets and clocks remain in use. Each scenario confirms convergence,
one mutation, no thread exception and complete guard cleanup.

Final affected checks:

```sh
PYTHONPATH=orm/src:orm/tests python3.11 -B -m unittest test_cluster_wait.ClusterWaitTests.test_large_finite_wait_bounds_native_waits_and_rechecks_deadline test_control_transport.ControlTransportTests.test_hostname_deadline_uses_reaped_resolver_and_returns_success -v
```

Two tests passed in 2.392 seconds. The preceding run also passed
`test_tcp_stalled_poll_expires_after_one_accepted_mutation`,
`test_deadline_interrupt_preserves_tls_state`, and
`test_deadline_caps_header_and_trickling_body_and_cleans_up`; these numeric-socket
checks were not repeated after the isolated resolver correction. Syntax checks
for `_control_transport.py` and `test_cluster_wait.py` and scoped whitespace checks
passed. The correction also changes the previously reviewed private transport;
a fresh quality re-check must inspect both cancellation and resolver ownership.

The fresh independent quality re-check passed with no important actionable
defects. It inspected both correction paths and reused the focused test evidence;
no redundant test run or further code change was needed.

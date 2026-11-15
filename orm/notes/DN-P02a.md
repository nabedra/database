# DN-P02a: private control transport

Status: DONE_WITH_CONCERNS

Owned files added:

- `orm/src/meshdb/_control_transport.py`
- `orm/tests/test_control_transport.py`
- `orm/notes/DN-P02a.md`

No exports, data transport, Cluster implementation, Rust, scripts, or existing work were changed. No agents, branch/worktree, commit/push, Cargo, installations, environment-file reads, or cloud-service calls were used.

## Behavior

`_ControlTransport` reuses `_Transport` construction validation, verified TLS configuration, strict JSON hooks, exception classes and known server code/status mappings. It posts exactly one control operation to the configured base path plus `/v1/cluster/operations`. Fresh IDs are UUIDs; callers may supply a UUID to preserve identity. Request and response bodies are capped at 1 MiB. Duplicate JSON fields, nonfinite response numbers, extra envelope fields, mismatched IDs and redirects are rejected. Data/status contents remain opaque for the next assignment. New server error codes use `MeshDBError` while retaining code, request ID, HTTP status and outcome. As in existing transport, error envelopes may have a null request ID, in which case diagnostics retain the client ID. Untrusted server messages are replaced with a generic message to avoid echoing secrets or request data.

DNS, TCP setup and verified TLS handshake complete before a mutation is marked possibly sent. Failure before that point is `TransportError`, not `UnknownOutcomeError`. Lost, malformed or oversized replies after a possible policy send are unknown outcomes; reads retain transport/protocol classification. There are no HTTP operation retries. Trying resolved addresses during connection establishment does not submit requests.

Ordinary timeout remains a socket I/O timeout. Absolute monotonic deadlines additionally shut down the active socket on expiration, including stalled response headers and trickling bodies. Cancellation timers are cancelled and joined before return; sockets and response streams are closed. Hostname resolution with a deadline uses an isolated Python subprocess with a bounded communicate, kill and reap on failure/cancellation. This avoids leaving an uncancellable resolver thread behind. Numeric addresses need no subprocess.

## Exact checks and evidence

Before implementation:

```sh
PYTHONPATH=orm/src:orm/tests python3.11 -m unittest test_control_transport.ControlTransportTests.test_policy_lost_reply_is_unknown_and_not_retried
```

Failed as intended: one failure, `private control transport must handle a lost policy reply`, because the module did not exist. The scenario proceeds to a real peer consuming a policy request and closing without replying, and asserts unknown outcome, retained identity and no retry.

The initial attempted command used the system `python3` and `unittest discover ... -k`; that interpreter rejected `-k`. No tests ran in that attempt. Subsequent commands explicitly used the installed Python 3.11.

After initial implementation:

```sh
PYTHONPATH=orm/src python3.11 -m unittest discover -s orm/tests -p test_control_transport.py
```

Passed 5 tests in 8.923 seconds. After adding collision validation, explicit-ID/configuration checks and real hostname-deadline coverage, ran the same focused command once more. Passed 6 tests in 9.137 seconds.

```sh
python3.11 -m py_compile orm/src/meshdb/_control_transport.py orm/tests/test_control_transport.py
```

Exited 0. Only the new unittest file and syntax checks were run. The real loopback fixture covers all operations, authentication/base path, server rejection, generic new-code mapping, read/policy lost responses, redirects, malformed/duplicate/nonfinite/mismatched/oversized/truncated envelopes, local rejection, refused connection and deadlines. Both stalled-header and continuously trickling-body cases have a 150 ms deadline and must return within 800 ms, with the cancellation thread gone afterward. The hostname deadline success case exercises the isolated resolver process.

## Review concerns and limits

- Whole-call hostname deadlines require the running Python interpreter to be launchable via `sys.executable`. Resolver startup has process overhead. Frozen executables and sandbox policies that forbid subprocesses were not tested. Failure to launch remains a definite pre-send transport failure.
- TLS hostname/certificate verification settings and invalid CA-file rejection are checked, but this focused fixture does not stand up a certificate-backed HTTPS peer. Stalled DNS/connect/TLS paths were not independently fault-injected; stalled headers and trickling bodies were exercised over real TCP.
- HTTP uses direct connections, without urllib's environment-proxy discovery. The existing data transport is unchanged.
- There is no Cluster policy/status validation, waiting, durable replay or retry implementation in this assignment.

## Coordinator correction after specification review

The spec review found that an incomplete status body raised ProtocolError while
the existing data transport raises TransportError. The coordinator changed the
existing truncated-status expectation, observed the incorrect ProtocolError,
then separated Content-Length syntax validation from incomplete-body handling.
Incomplete bodies now use network error classification; possibly sent policy
mutations still raise UnknownOutcomeError.

`PYTHONPATH=orm/src:orm/tests python3.11 -B -m unittest
test_control_transport.ControlTransportTests.test_untrusted_envelopes_and_redirect_do_not_succeed -v`
failed before the fix and passed afterward in 0.650 seconds. Other current
six-scenario evidence remains applicable. Independent spec re-check passed with
no actionable regression after inspecting the correction and nearby strict
envelope/deadline behavior. Quality review is pending; real TLS evidence remains
tracked separately.

Quality review found a TLS cancellation race: `SSLSocket.shutdown` clears its
SSL object before a simultaneous handshake, leaking AttributeError. The
coordinator added a deterministic real-socketpair/TLS-state regression and
observed that exact AttributeError before changing cancellation to the base
`socket.socket.shutdown` operation. This interrupts I/O without discarding TLS
state. The regression and existing stalled-header/trickling-body cleanup scenario
are the focused checks for this correction. Exact command:
`PYTHONPATH=orm/src:orm/tests python3.11 -B -m unittest test_control_transport.ControlTransportTests.test_deadline_interrupt_preserves_tls_state test_control_transport.ControlTransportTests.test_deadline_caps_header_and_trickling_body_and_cleans_up -v`.
Both passed in 0.737 seconds. Independent quality re-check passed without
remaining critical or important findings, confirming TLS state, exception
classification, timer/socket cleanup and resolver reaping. The bounded private
transport slice is complete; real HTTPS evidence remains assigned to T16.

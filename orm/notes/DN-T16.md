# DN-T16: Real TLS control transport evidence

The focused real-TLS contract test passes against the existing Python
`meshdb._control_transport._ControlTransport`. No production defect appeared,
and this assignment changed only the new test and this note.

## Command and result

Run from `/Users/antoniopenapena/Documents/ResearchProjects/AI/meshdb`:

```sh
PYTHONPATH=orm/src python3.11 -B -m unittest discover -s orm/tests -p test_control_tls.py
```

Exit status: 0. Exact unittest output:

```text
.
----------------------------------------------------------------------
Ran 1 test in 1.178s

OK
```

## Topology and ownership

One Python stdlib `HTTPServer` listens on `127.0.0.1`, port 0, in an owned
server thread. Accepted sockets complete a real server-side TLS handshake
before HTTP handling and have a five-second I/O timeout. The client uses
`https://127.0.0.1:<assigned-port>/admin/prefix/`.

The test loads `scripts/test_tls_material.py` by a path derived from the test
file, without requiring the scripts directory to be an installed package or
part of `PYTHONPATH`. It calls `create_test_tls(Path)` inside an owned
`TemporaryDirectory`. That existing helper creates a disposable CA and a
certificate valid for localhost and 127.0.0.1, with protected keys. No permanent
certificates are added. Cleanup shuts down and joins the server thread, closes
the listening socket, and removes the temporary directory, including on failed
assertions. Failed handshake sockets are closed. HTTP logging is disabled and
handler failures are recorded without printing peer-controlled text. Bearer
values and certificate error text are excluded from assertion diagnostics.

## Proven behavior

The single test first sends a real `cluster.scale` POST with an explicit
`ca_file`. The server returns HTTP 200 and the existing matching
`{"request_id": ..., "data": ...}` envelope. Assertions verify the returned
UUID and data, the exact `/admin/prefix/v1/cluster/operations` path, the bearer
header, and the full operation envelope and payload. This successful request
has an explicit whole-call monotonic deadline, exercising verified TLS with
the current deadline guard setup and cancellation on completion.

The test then constructs a client for the same endpoint without `ca_file` and
attempts another mutation. It requires the exact `TransportError` class,
excludes `UnknownOutcomeError`, checks the preserved request UUID and absent
HTTP status, and checks that the outcome is not `unknown`. The suppressed
exception context must actually be an `ssl.SSLCertVerificationError`; only its
type is inspected. The server must observe a TLS handshake rejection, and its
received-policy count must remain one. Together these checks establish a
certificate trust failure before policy submission, rather than a connection
refusal or a lost response after submission.

## Limits

This is real TLS evidence for the Python transport against a local stdlib
peer. It does not establish full distributed runtime acceptance or exercise
the Rust server. The deadline does not expire in this test, so this assignment
does not independently prove interruption of an in-progress TLS exchange.
The fixture includes both DNS and IP subject alternative names; this test
exercises verification for 127.0.0.1 only. No HTTP matrix, fake SSL context
checks, timing assertions, Cargo builds, or package installation were added.

# P03c1 callback protocol primitives

Implemented only P03c1 on shared main. Read `plans/distributed-contracts.md`,
`orm/notes/DN-A04.md`, and the callback/bootstrap portions of
`scripts/notes/DN-A03.md`, plus existing Python validation, errors and control
transport for reusable behavior. The shared contract supersedes historical
`control`/`objects` bootstrap fields with `storage`. The A04 parent override
removes completed-result caching. No completed results or authorizations are
cached here.

Changed files owned by this slice:

- `orm/src/meshdb/_callback_protocol.py`
- `orm/tests/test_callback_protocol.py`
- `orm/notes/DN-P03c1.md`

## Private API for P03c2

Import directly from `meshdb._callback_protocol`; no public exports changed.

- `_decode_command(original_command_bytes: bytes) -> _Command` requires exact
  built-in bytes, returns a private record whose `original_bytes` is the same
  bytes object and whose `value` is the validated JSON dictionary. The record's
  repr excludes both fields. Treat `value` as private, read-only protocol data;
  do not log it or pass the entire envelope to callback code. Decode once at
  ingress, keep the record with the admitted call, and extract only the fixed
  public callback argument in P03c2. Never reserialize `value` for authorization.
- `_authorize(url: str, token: str, original_command_bytes: bytes, *,
  invocation_id: str, deadline: float | None = None) -> None` is synchronous.
  Supply the parsed record's original bytes and invocation UUID. `deadline` is
  an optional absolute `time.monotonic()` observation deadline. The effective
  deadline is the earlier of that value and two seconds after function entry.
  Success returns None; every failure raises a sanitized protocol error.
  This function expects command validation to have occurred already and does
  not parse the command again. It verifies the response invocation and SHA256
  of the exact bytes sent, with `authorized is True`.
- `_encode_result(command: _Command, result: object) -> bytes` validates the
  returned public callback value and returns the complete success envelope
  `{version:1, invocation_id:UUID, result:VALUE}`. The bridge can write these
  bytes directly with its own bounded response-write deadline. It does not
  invoke the callback. Inventory is complete or rejected, never truncated.
- `_CallbackProtocolError` derives from existing `MeshDBError`. Its `code` and
  message are fixed: `invalid_command` / `Invalid callback command`,
  `invalid_callback_result` / `Invalid callback result`, `limit_exceeded` /
  `Callback protocol limit exceeded`, or `authorization_denied` /
  `Callback authorization denied`. Known authorization denials also preserve
  `unauthorized`, `ownership_changed` and `ownership_unavailable` with fixed
  messages. A strictly validated denial carries its matching `status_code`;
  malformed responses and transport failures use `authorization_denied` with
  no status. No input/provider values are included. The bridge owns callback
  entry/outcome classification.

Command decoding enforces the exact A04 envelopes, canonical lowercase UUID
spellings, exact version and u64 epoch, start/bootstrap identity correlation,
separate stop intent, and null inventory intent/target. Strict JSON rejects
nonfinite numbers, duplicate fields, invalid UTF-8 including escaped surrogates,
non-object roots, bodies above 1 MiB and values deeper than 32. Depth counts the
root as one and increases for each child value. Resource IDs reject Unicode Cc
characters and exceed neither 256 characters nor 256 encoded bytes.

Bootstrap validation covers exact documented fields with one generic storage
binding, exact capacity ranges, 1..16 nonempty API address strings, nullable TLS
shape and null provisioning advertisement. Rust remains authoritative for all
address/partition-size syntax, driver/options semantics, credential file
contents and persisted compatibility. Unknown generic storage drivers are
structurally accepted here so this codec does not become another provider
configuration parser. No credential files are opened.

Result containers and strings require exact built-in types. Container fields
also require exact string keys before comparing names, so hostile key equality
methods cannot run. Custom iterables/awaitables/generators are rejected without
iteration or execution. An exact native returned coroutine is closed without
running its body. Inventory enforces resource uniqueness and independently
unique active node and durable request IDs; terminated records do not reserve
active identities. The encoder checks the cap on each emitted chunk and slices
strings before JSON quoting, avoiding an arbitrary giant encoded string or
list allocation. It retains at most a bounded output buffer.

Authorization uses a new AF_INET socket directly against the exact canonical
numeric loopback route, without DNS, proxies, redirects, retries, resolver
threads or timer threads. Every bounded connect/send/receive operation gets the
remaining absolute budget. Headers are capped at 8 KiB and response bodies at
1 MiB. Strict framing requires a single Content-Length, application/json, no
transfer/content encoding and connection EOF after the stated body. The
request sends `Connection: close`; an authority that keeps this private
single-use connection open until the deadline is denied. The owned socket
closes on success and all failure paths.

## TDD and focused evidence

Applied the test-driven-development and unslop skills. Before implementation:

```text
PYTHONPATH=orm/src orm/.venv/bin/python3.11 -m unittest discover -s orm/tests -p test_callback_protocol.py
Ran 6 tests in 0.002s
FAILED (failures=6)
```

All six grouped behavioral tests failed with the explicit missing-implementation
assertion. After minimal implementation they passed. A further adversarial
exact-dictionary key regression was first observed failing because custom
`__eq__` ran; string-key validation then fixed it.

Final focused unittest command:

```text
PYTHONPATH=orm/src orm/.venv/bin/python3.11 -m unittest discover -s orm/tests -p test_callback_protocol.py
Ran 6 tests in 2.607s
OK
```

Groups cover schema/correlation/type/capacity/JSON bounds, valid generic
bootstrap, complete result encoding, duplicate resource/active identities,
foreign-cluster inventory, hostile containers/keys, unexecuted coroutine
closure, and encoded output overflow. The independently authored real TCP
fixture checks exact original request bytes, bearer, route and length; correct
digest success; each known ownership denial status; mismatched digest and
invocation; truthy substitutes; duplicate JSON and unknown response fields;
malformed, duplicate, oversized and truncated HTTP framing; lost response;
held response and paced body trickling against an earlier observation deadline;
and the default absolute two-second budget. Events gate held responses.
Timed waits pace trickling, not overlap assumptions. Elapsed upper bounds are
failure watchdogs. Fixture teardown releases gates, joins every fixture thread,
closes sockets and checks queued connections to detect retries. A held fixture
observes EOF to confirm client closure on timeout.

Final syntax and direct whitespace command:

```text
orm/.venv/bin/python3.11 - <<'PY'
from pathlib import Path
files = [Path('orm/src/meshdb/_callback_protocol.py'), Path('orm/tests/test_callback_protocol.py'), Path('orm/notes/DN-P03c1.md')]
for path in files:
    text = path.read_text()
    if path.suffix == '.py':
        compile(text, str(path), 'exec')
    assert text.endswith('\n'), path
    assert all(line == line.rstrip() for line in text.splitlines()), path
print('In-memory syntax and direct whitespace checks passed for 3 files')
PY
```

Result: `In-memory syntax and direct whitespace checks passed for 3 files`.

## Scope limits

No callback is invoked, pool/listener/runtime started, provider accessed, or
infrastructure executed by this module. Fixture threads exist only in tests.
No admission, active-call deduplication, entry/deadline locking, response writer,
shutdown, startup channel or full bridge is implemented. No public serve,
exports, config parser, Rust, existing Python modules or other shared work was
edited. No branch/worktree, commit/push, Cargo, dependency installation, .env
read or cloud access occurred.

P03c2 owns authorization immediately before callback entry, including the entry
lock/deadline decision and active-call lifecycle. Authorization is evidence of
one fresh ownership observation, never atomic fencing of a later provider side
effect. Actual Rust authorization, control CAS fencing and full cluster
acceptance remain later work; the TCP fixture proves only this Python wire
contract.

## Specification correction: preserve validated authorization denials

Specification review found that collapsing every non-200 response prevented
the bridge from distinguishing ownership changes, unavailable ownership and
authentication rejection. The coordinator changed the existing real-TCP denial
scenario to require each allowlisted code and status. Its initial run failed
eight denial cases before the production correction.

The client now strictly decodes denial envelopes, validates exact version,
fields, status/code pairing and invocation correlation, then raises the fixed
sanitized error with its status. Null invocation is accepted only for the
400/401/413 pre-parse/authentication failures. Ownership denials require the
matching invocation. Unknown fields, mismatched status/code or invocation and
transport/framing errors still fail closed as authorization_denied.

Using `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src:orm/tests python3.11 -m
unittest`, the initial run of
`test_callback_protocol.CallbackProtocolTests.test_authority_denials_and_bad_framing_never_retry`
failed with eight subtest assertion failures in 0.035 seconds. After correction,
that method plus `test_authorization_binds_original_bytes_and_bearer` and
`test_authority_lost_and_absolute_deadline_release_owned_connection` from the
same test class passed: three methods in 2.343 seconds. Owned in-memory syntax
and direct whitespace checks passed. Unchanged command/result groups were not
repeated. Fresh specification re-check and independent code-quality review both
passed without remaining actionable findings. This completes the protocol
primitive slice; callback ownership and HTTP ingress remain separate.

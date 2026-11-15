# DN-C01a specification review

Status: PASS after the coordinator's GET framing correction and independent
specification re-check. The original finding below is retained as history and
is resolved. The subsequent independent quality review also passed.

## Resolved finding

P2: `scripts/local_store.py:167-169` requires Content-Length even for a
bodyless GET, before object lookup or health routing. The agreed contract in
`plans/distributed-nodes.md:142` requires bounded Content-Length for request
bodies. An ordinary bodyless HTTP GET has no such header. Consequently a
normal readiness request returns 411 instead of 200, and an authenticated
missing-object GET returns 411 instead of 404. This affects standard HTTP
clients unless every caller adds a special zero-length header.

The smallest correction is to treat absent Content-Length as zero for GET.
Continue requiring an explicit length for PUT and rejecting Transfer-Encoding,
duplicate/invalid lengths, and a nonzero GET length. Adjust the existing focused
scenario to exercise bodyless GET without injecting Content-Length. Its request
helper currently always injects that header at `scripts/test_local_store.py:64`,
which hides this behavior. Update the wire documentation at
`scripts/local_store.py:4` and `scripts/notes/DN-C01a.md:26-27` accordingly.

## Other required behavior

Independently inspected the implementation, tests, and implementer report.
No other missing requirement was found in this bounded review.

- PUT condition validation at lines 228-233 permits exactly one Create or one
  quoted strong Match condition. SQL uses bound key/body values.
- BEGIN IMMEDIATE at line 246 serializes the condition check, persisted global
  generation increment, and object replacement. FULL synchronous mode and DELETE
  journaling are set at lines 36 and 48. Commit precedes 204 at lines 258-264.
- GET selects complete bytes and generation together at line 217. Versions
  advance even for identical bytes and persist with the object transaction.
- Paths cannot become filesystem paths. Namespace-qualified keys reject empty,
  dot, traversal, encoded, and ambiguous components. Authentication precedes
  explicit body consumption and any 100 Continue invitation.
- The token is required from the process environment. Body size, socket
  lifetime, worker count, queued sockets, and concurrent timer threads are
  bounded. Port-zero announcement and graceful stop/restart are implemented.
- Commit errors and failed response writes escape to socket closure rather
  than a definite-not-applied HTTP response.
- The two-client test at lines 139-153 uses independent TCP connections and
  the same starting ETag, then checks exactly one winner and matching GET
  bytes/token. Source inspection supplies the transaction atomicity evidence.
- Restart assertions at lines 158-165 preserve bytes/token, reject a stale
  token, and advance the next version.
- The response-drop proxy at lines 167-199 consumes the upstream 204/ETag
  before closing downstream. The client observes RemoteDisconnected, and GET
  recovers that exact committed token. This is causal response-loss evidence.

The implementer's reported two passing tests in 1.530 seconds were not rerun.
No Cargo, cloud calls, installs, .env reads, branches, commits, production edits,
or test edits were performed. Only this optional review note was added.

## Review commands and evidence

Read commands included `nl -ba scripts/local_store.py`,
`nl -ba scripts/test_local_store.py`, `nl -ba scripts/notes/DN-C01a.md`,
`nl -ba plans/distributed-contracts.md`, and
`nl -ba plans/distributed-nodes.md | sed -n '138,155p'`.
`git status --short` identified the existing shared work, which was preserved.

One narrow probe was run because of the suspected GET compatibility gap.
Exact command:

```sh
python3.11 -B - <<'PY'
import http.client
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix='dn-c01a-spec-') as directory:
    process = subprocess.Popen(
        [sys.executable, '-B', 'scripts/local_store.py', '--database',
         str(Path(directory) / 'probe.sqlite'), '--port', '0'],
        env={**os.environ, 'MESHDB_STORE_TOKEN': 'spec-probe-token'},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert select.select([process.stdout], [], [], 5)[0], 'startup timeout'
        address = json.loads(process.stdout.readline())
        for path in ('/health/ready', '/v1/objects/spec/missing'):
            for explicit_zero in (False, True):
                headers = {'Authorization': 'Bearer spec-probe-token'}
                if explicit_zero:
                    headers['Content-Length'] = '0'
                connection = http.client.HTTPConnection(address['host'], address['port'], timeout=5)
                try:
                    connection.request('GET', path, headers=headers)
                    response = connection.getresponse()
                    response.read()
                    print(json.dumps({'path': path, 'explicit_zero': explicit_zero, 'status': response.status}))
                finally:
                    connection.close()
    finally:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise
        process.stdout.close()
        process.stderr.close()
        print('shutdown_exit=' + str(process.returncode))
PY
```

Exit 0, output:

```text
{"path": "/health/ready", "explicit_zero": false, "status": 411}
{"path": "/health/ready", "explicit_zero": true, "status": 200}
{"path": "/v1/objects/spec/missing", "explicit_zero": false, "status": 411}
{"path": "/v1/objects/spec/missing", "explicit_zero": true, "status": 404}
shutdown_exit=0
```

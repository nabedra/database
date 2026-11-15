# DN-C01a local conditional object service

Status: DONE. Scope is the development/acceptance TCP object service only.
No cluster runtime, Rust, ORM, dependencies, rubric, existing scripts, branches,
commits, or cloud resources were changed. Parent spec and quality review remain
separate work. No unrelated rubric item is claimed complete.

## Files

- `scripts/local_store.py`
- `scripts/test_local_store.py`
- `scripts/notes/DN-C01a.md`

## Invocation and wire contract

Supply `MESHDB_STORE_TOKEN` through the process environment. There is no default
secret. Missing, empty, or nonprintable/non-ASCII tokens prevent startup.

Run `python3.11 scripts/local_store.py --database /local/path/store.sqlite`.
The parent directory must exist. Defaults are host `127.0.0.1`, port `9000`.
`--port 0` allocates a port and stdout emits one JSON line with the numeric bound
`host` and `port`. It includes no database path, token, key, or object content.
The database must be a persistent file on local disk. Empty paths and `:memory:`
are rejected. The operator must choose a filesystem that honors SQLite syncs.

PUT requires exactly one decimal Content-Length. Bodyless GET and health checks
accept an absent length or explicit `Content-Length: 0`. PUT bodies are capped at
83,886,080 bytes. Transfer-Encoding is rejected, as are duplicate, negative,
invalid, missing, or oversized lengths. Validation and authentication occur
before body reads. Invalid conditions return 400 without consuming the body.
A truncated body returns 400 and never writes a partial object.

Authenticated GET and PUT use `/v1/objects/<namespace>/<relative-key>`.
Keys require at least two slash-separated ASCII unreserved components, each
nonempty and different from `.` and `..`. The key limit is 4096 bytes.
Percent signs, queries, fragments, backslashes, control characters and ambiguous
request targets are rejected. Keys are bound SQL values and never filesystem
paths. No delete or list operation exists.

PUT requires precisely one `If-None-Match: *` or one quoted `If-Match` ETag.
Malformed or duplicate conditions return 400. Condition mismatch returns 412.
Success returns 204 and a quoted ETag only after COMMIT. GET returns 200 with
raw bytes, Content-Length, and ETag, or 404. Clients must treat ETags as opaque.
GET reads the bytes and generation with one SQL SELECT.

Public GET `/health/live` and `/health/ready` return empty responses. Readiness
also checks the generation table and returns 503 if it cannot read the database.
Authentication failures return 401. All responses close the connection.

## Atomicity, durability, resource limits

Each PUT uses a connection with `synchronous=FULL`, a rollback journal in DELETE
mode, and a five-second SQLite busy timeout. `BEGIN IMMEDIATE` covers the
condition check, global generation increment and object upsert. Generation is
persisted in the same transaction and increases on every successful PUT,
including identical-byte writes. Restart preserves acknowledged bytes and ETag.
The signed SQLite integer limit eventually causes transport failure rather than
version reuse.

There are four fixed handler workers, at most eight queued accepted sockets,
and a bounded OS listen backlog. Saturation closes excess accepted sockets.
Each request has a 15-second total socket deadline measured from acceptance,
including queue time, enforced independently of incremental socket progress.
At most four deadline timer threads exist. Header parsing uses the stdlib's
bounded line/header counts; object memory is bounded by the body cap and worker
count. Large requests can still consume hundreds of MiB across four handlers.
SQLite busy waits can last five seconds; OS disk I/O must complete for graceful
shutdown to finish. SIGTERM/SIGINT stop acceptance, discard queued sockets,
finish active workers and close database connections.

An exception during COMMIT, or a failed response after COMMIT, yields transport
loss. It never returns a claim that the write did not apply. The client must
reconcile by GET. The service has no production fault-injection interface.
Diagnostics do not log auth, keys, request bodies, or exception text.

This is a local development service using plaintext HTTP. Loopback is the
default. It does not implement TLS, deletion, listing, or the cluster runtime.
No broad suite, load test, power-loss test, or disk-fault injection was run.

## Test evidence

The tests were written before the implementation. The first run of
`python3.11 -m unittest scripts.test_local_store -v` failed because
`scripts/local_store.py` did not exist, so the child could not announce its TCP
address. The initial helper also repeated that failed-exit assertion during
cleanup; cleanup was subsequently made idempotent.

The focused suite contains two actual-server scenarios with tiny fixtures:

1. Authentication and no-default-token startup refusal; live/ready; missing
   object; malformed conditions, framing, oversized declarations and unsafe
   keys; truncated body; create collision; two independent TCP clients released
   by a deterministic barrier racing one ETag with exactly one 204 and one 412;
   consistent GET bytes/version; same-bytes token advancement; graceful process
   restart preserving acknowledged bytes/version; rejection of a stale ETag;
   successful post-restart update with a fresh ETag.
2. An actual TCP proxy consumes the server's committed 204 and ETag, then closes
   the downstream connection without forwarding the response. The client sees
   RemoteDisconnected, GET recovers the committed bytes/version, and a repeated
   create receives 412. No sleeps or production test hooks are used.

The existing `server_process.py` helper assumes the Rust binary, meshdb launch
arguments, and ORM configuration, so it was read but not reused or modified.
The new helper uses port-zero stdout discovery and a bounded select wait.

Final verification commands and results are recorded below after the final run.

- `python3.11 -m unittest scripts.test_local_store -v`: PASS, 2 tests,
  1.530 seconds, exit 0.
- Python 3.11 `compile(...)` syntax checks for both Python files: PASS.
- `git diff --check -- scripts/local_store.py scripts/test_local_store.py scripts/notes/DN-C01a.md`:
  exit 0. These files are untracked, so an additional direct whitespace check
  below covers their content.
- Direct Python check of all three files for trailing whitespace: PASS.

## Coordinator correction after specification review

The review found ordinary bodyless GET returned 411. The existing TCP helper now
sends ordinary GET without injecting Content-Length, and retains an explicit-zero
health check. Before the fix, `python3.11 -B -m unittest
scripts.test_local_store.LocalStoreTests.test_conditional_tcp_storage_and_restart -v`
failed with `411 != 200`. The coordinator made the small framing correction:
absent length means zero only for GET; PUT and duplicate/invalid/chunked framing
rules remain unchanged. `python3.11 -B -m unittest scripts.test_local_store -v`
then passed both actual-TCP scenarios in 1.506 seconds, exit 0. Independent
specification re-review passed with no remaining findings after inspecting the
corrected framing/auth path and existing regression evidence. Independent quality
review also passed, finding no critical or important issue in socket/queue/timer
ownership, SQLite transaction cleanup, uncertain-write handling or test evidence.
Both reviewers reused the current passing tests rather than repeating them.

# MeshDB v0

MeshDB stores tables as immutable Parquet snapshots in S3. A single Rust server
process exposes HTTP or native HTTPS, and a synchronous Python ORM builds and
sends operations. The ORM requires Python 3.11+ and only the Python standard
library at runtime. It needs the server URL and bearer token, not AWS credentials.
S3 is the server's only durable store. Run one server process per bucket/prefix.

## Start here

- [Hands-on v0 tutorial](TUTORIAL.md): start the server, build a task tracker,
  query data and reopen it after a restart.
- [Python ORM guide](orm/README.md): installation, connection, verified TLS,
  table operations, queries and errors.
- [Server guide](server/README.md): build and serve, environment configuration,
  HTTP requests, native TLS, health and shutdown.
- [ORM v0 specification](plan-v0/orm.md) and
  [server v0 specification](plan-v0/server.md): supported contracts.
- [Implementation and testing rubric](plan-v0/implementation.md): task evidence,
  review and acceptance tracking. The older [design documents](plan/) describe
  broader ideas outside v0.

## Install and use

Run commands from the repository root. Use Python 3.11 or a newer interpreter
in the first command:

```sh
python3.11 -m venv orm/.venv
orm/.venv/bin/python -m pip install -e ./orm
```

The server requires Rust 1.94.1 or newer, as declared in
[Cargo.toml](server/Cargo.toml), with dependencies resolved in
[Cargo.lock](server/Cargo.lock). Follow the
[build and start instructions](server/README.md#build-and-start) to configure
S3 access and run `meshdb-server serve`. See the
[configuration guide](server/README.md#configuration) for HTTP binding and
native HTTPS with `--tls-cert` and `--tls-key`, and the
[HTTP example](server/README.md#health-requests-and-shutdown) for direct requests.

With the server at `http://127.0.0.1:8000`, supply its bearer token privately in
`MESHDB_TOKEN` and run this example with `orm/.venv/bin/python`. It uses a fresh
table name on each run and retains the table:

```python
import os
import uuid
from meshdb import DB, col

db = DB("http://127.0.0.1:8000", os.environ["MESHDB_TOKEN"])
users = db.create_table(
    "guide_users_" + uuid.uuid4().hex,
    {"id": str, "age": int, "active": bool},
    key="id",
)
users.insert_many([
    {"id": "alice", "age": 24, "active": True},
    {"id": "bob", "age": 17, "active": False},
])
rows = users.where(col("age") >= 18).select("id", "age").collect()
assert rows == [{"id": "alice", "age": 24}]
```

For HTTPS, the client verifies certificates and hostnames; use `ca_file` for a
private CA as described in the [ORM connection guide](orm/README.md#install-and-connect).

## Supported behavior and limits

Schemas have 1 to 128 nonnullable columns with exact Python types `str`, `int`,
`float` and `bool`. Integers are signed int64; floats must be finite. There is
no implicit coercion. Primary keys are unique `str` or `int` columns. Full rows
must contain all schema columns and no extras. Updates cannot change the key.

The ORM supports table creation, discovery and drop; row insertion, atomic
batch insertion, lookup, partial update, full-row upsert and deletion. Immutable
query builders support scalar comparisons, Boolean composition, projection,
one-column ordering, limit, collect, count and metadata-only explain. Reads
observe a fresh committed snapshot per call. Ordering defaults to ascending
primary key and uses that order to break explicit sort ties. Count includes an
explicit limit. Dropping and recreating a name creates a new identity; old
handles remain stale. See the [ORM guide](orm/README.md) for full API behavior.

Key lookups and row queries scan full tables. Mutations that change rows rewrite
the complete table snapshot, then conditionally publish the catalog. v0 has
no SQL, joins, indexes, general transactions across calls, garbage collection
or distributed execution. Atomic batches are single operations. Dropping a
table removes its catalog entry but retains snapshots and orphan objects.

Requests and responses are limited to 8 MiB each, including their JSON envelope.
Batches and collected results are limited to 10,000 rows; tables are limited to
100,000 rows and 64 MiB of decoded row payload. Byte limits may be reached first.
Oversized results fail rather than truncate. There is no pagination or streaming.

The ORM never retries automatically. An uncertain write may have committed even
if the caller saw a timeout or `UnknownOutcomeError`. Request IDs do not provide
idempotency. Inspect and reconcile before deciding on another write; an immediate
old read does not prove rollback. See
[uncertain-write handling](orm/README.md#errors-and-uncertain-writes) and
[server storage semantics](server/README.md#storage-behavior-and-limits).

## Local verification

After the editable install, these commands run the ORM tests and local script
guards without using the supplied `.env` or AWS. The guard suite includes local
subprocess/loopback and TLS fixture cleanup checks. OpenSSL is required
for TLS fixtures; AWS CLI is required for live object inspection. Neither tool
is a dependency of normal ORM use.

```sh
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python -m unittest discover -s orm/tests -v
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python -m unittest discover -s scripts -p 'test_*.py' -v
```

Run Cargo checks sequentially from the repository root, using `server/target`
and the checkout's disk-saving settings. These checks do not run live S3
acceptance; dependency installation may need network access on a new machine.

```sh
export CARGO_BUILD_JOBS=2
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
cargo fmt --manifest-path server/Cargo.toml --all -- --check
cargo clippy --locked --manifest-path server/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path server/Cargo.toml
```

## Live acceptance and evidence

Live acceptance needs the installed ORM, a built server binary, AWS CLI and
OpenSSL. To build the debug binary, use the Cargo settings above:

```sh
cargo build --locked --manifest-path server/Cargo.toml
```

The sole allowed acceptance bucket is `meshdb-testing` in `us-east-1`. The
prepared runners load the supplied repository `.env` through
[private configuration helpers](scripts/acceptance_support.py). They require
`BUCKET_NAME`, `AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`,
plus `AWS_SESSION_TOKEN` for temporary credentials. Helpers map `BUCKET_NAME`
to `MESHDB_S3_BUCKET` in private child environments and supply a bearer token.
Do not source, print or commit the dotenv file or credentials.

This strict acceptance configuration ignores ambient AWS profiles. The generic
server uses the standard AWS SDK credential and region chains, including
profiles and roles; it neither loads dotenv nor translates `BUCKET_NAME` itself.

Run these four prepared runners sequentially. HTTP covers types, CRUD, queries
and atomic failures; lifecycle covers actual-process restart and verified native
HTTPS; concurrency covers independent clients, stale handles and retained
objects; latency samples single writes, batches and scans at modest table sizes.

```sh
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python scripts/acceptance_http.py --scenario all --binary server/target/debug/meshdb-server
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python scripts/acceptance_lifecycle.py --scenario all --binary server/target/debug/meshdb-server
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python scripts/acceptance_concurrency.py --binary server/target/debug/meshdb-server
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python scripts/acceptance_latency.py --binary server/target/debug/meshdb-server
```

Each run allocates fresh S3 folders named `test-<number>-<name>-<run-id>/` and
retains fixtures and snapshots. A restart reuses only its scenario's allocated
prefix. Never replay a recorded prefix, delete fixtures or create another bucket.
Each runner creates a fresh sanitized JSON report beneath the Git-ignored
`artifacts/` directory; `--report` accepts an unused alternative path. Keep
reports ignored and record the command, report location, outcome and exact
allocated prefixes in a tracked note under [scripts/notes/](scripts/notes/).
Retain failure reports as well as successful reports, without private logs or
credentials. Latency samples provide no throughput or scaling guarantee.

Recorded live evidence is available for [HTTP/S3 acceptance](scripts/notes/E02.md)
and [restart/native HTTPS acceptance](scripts/notes/E03.md). Consult the
[rubric](plan-v0/implementation.md) for further evidence and final audit status.

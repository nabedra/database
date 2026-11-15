# MeshDB v0 server

The `meshdb-server` Rust binary serves the table and query API over HTTP or
native HTTPS. S3 is its only durable store. Run one process per bucket/prefix;
v0 does not coordinate multiple server processes sharing a namespace.

The [Python ORM guide](../orm/README.md) covers client installation and the full
API. The [partition workers rubric](../plans/partition-workers.md) records current
storage and worker contracts and task status. The [v0 server contract](../plan-v0/server.md)
and [v0 implementation tracker](../plan-v0/implementation.md) are historical
baselines; their single-file and request-only worker descriptions predate this change.

## Build and start

Use Rust 1.94.1 or newer with Cargo. The crate uses edition 2024, pins direct
dependency versions in [Cargo.toml](Cargo.toml), and records resolution in
[Cargo.lock](Cargo.lock). Run these commands from the repository root:

```sh
cargo build --locked --manifest-path server/Cargo.toml
server/target/debug/meshdb-server serve
```

For a release binary:

```sh
cargo build --locked --release --manifest-path server/Cargo.toml
server/target/release/meshdb-server serve
```

Configure the environment before starting either binary. For example, with an
already configured AWS profile and a token supplied in the environment:

```sh
export AWS_PROFILE='<configured-profile>'
export AWS_REGION='<bucket-region>'
export MESHDB_S3_BUCKET='<existing-bucket>'
export MESHDB_S3_PREFIX='meshdb-v0/'
: "${MESHDB_TOKEN:?Set MESHDB_TOKEN to your shared bearer token}"
server/target/debug/meshdb-server serve
```

For development in this shared checkout, keep Cargo commands sequential and
reuse `server/target`. The disk-saving settings are
`CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`.

## Configuration

| Environment variable | Requirement and meaning |
| --- | --- |
| `MESHDB_S3_BUCKET` | Required, nonempty existing S3 general purpose bucket. The server does not create buckets. |
| `MESHDB_TOKEN` | Required, nonempty shared bearer token for operations. Use a header-safe token shared with clients. |
| `MESHDB_S3_PREFIX` | Optional database namespace, default `meshdb-v0/`. An explicit empty prefix is invalid. Trailing slashes normalize to one; absolute paths, traversal, empty internal components and unsafe escaped paths are rejected. |
| `AWS_REGION` | Bucket region, required unless resolved by the standard AWS region chain. |
| `AWS_PROFILE` | Optional named shared AWS profile. Profiles and role-based credentials are valid server configurations. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Optional environment credentials for the standard AWS credential chain; unnecessary when another provider supplies credentials. |
| `AWS_SESSION_TOKEN` | Required with temporary environment credentials; otherwise optional. |
| `AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE` | Optional standard AWS shared-file location overrides. |

The server uses the AWS SDK's standard credential and region chains, including
shared profiles and roles. AWS credentials stay on the server; clients need only
the API URL and bearer token. The configured identity needs access to read and
conditionally write objects beneath the database prefix.

The supplied `.env` uses `BUCKET_NAME`. The server requires `MESHDB_S3_BUCKET`
and does not implicitly load dotenv files or translate `BUCKET_NAME`. Supply
the server variables explicitly through the process environment or your service
manager. The guarded acceptance scripts below perform their own dotenv loading.

`meshdb-server serve` accepts exactly these application flags, plus standard
Clap help/version options:

| Flag | Default | Constraint |
| --- | --- | --- |
| `--host` | `127.0.0.1` | An IPv4 or IPv6 address. Nonloopback binding must be explicit. |
| `--port` | `8000` | Unsigned 16-bit port. |
| `--workers` | `4` | At least 1 persistent local OS worker thread; also sets the separate executing-request budget. |
| `--partition-size`, alias `--partition_size` | `67108864 B` | Positive integer with a supported unit, at most 67,108,864 bytes. |
| `--queue-size` | `64` | Nonnegative queued operation count; zero is valid. |
| `--tls-cert` | Absent | Readable PEM certificate file, supplied with `--tls-key`. |
| `--tls-key` | Absent | Readable matching PEM private key, supplied with `--tls-cert`. |

The checked sum of workers and queue size must fit the executor's semaphore
limit. Authentication and admission precede operation-body reads. At most
`workers + queue-size` operations are admitted and at most `workers` execute;
full or stopped admission returns `429 overloaded`. Request admission and
partition dispatch have separate budgets.

For example:

```sh
server/target/debug/meshdb-server serve --workers 4 --partition_size '64 KB'
```

`--partition-size '64 KB'` is equivalent. Units are mandatory and case-insensitive:
`B`, `KB`, `MB`, `KiB`, `MiB`. Whitespace is optional, so `64kb`, `'64 Kb'` and
`64KiB` are accepted. SI units use powers of 1000; IEC units use powers of 1024.
Only positive integer quantities are valid. Bare numbers, signs, decimal
notation, zero, overflow and values above 64 MiB are rejected. The default and
hard maximum are 64 MiB, exactly 67,108,864 bytes; `64MiB` and `67108864B` are accepted.

Exactly N persistent local OS threads handle partition work for `--workers N`.
The original index in the captured manifest selects worker `index % N`, including
when a keyed operation selects only one file. Each worker fetches its own file,
decodes it and evaluates the operation or encodes replacement files on that
thread. Each worker has one queued-job slot; each operation dispatches at most
N outstanding jobs and awaits every admitted job through errors. More partitions
run in later windows; fewer partitions leave some workers idle. The coordinator
combines results and publishes the catalog. These are local threads in one
process. Changing N on restart is safe and does not change storage partition
count or require rewriting files.

Without TLS flags, the listener serves HTTP. To serve native HTTPS on an
explicit nonloopback address:

```sh
server/target/release/meshdb-server serve --host 0.0.0.0 --port 8443 \
  --workers 4 --queue-size 64 \
  --tls-cert /path/to/server-cert.pem --tls-key /path/to/server-key.pem
```

Clients must trust the certificate's CA and verify its hostname. For a private
CA, use curl's `--cacert /path/to/ca.pem` or the ORM's `ca_file` argument, as
shown in the ORM guide. Invalid configuration, inaccessible or invalid catalog
data, and invalid TLS material fail startup before listening. A definitely
missing catalog is initialized with a conditional create; an unreadable or
unsupported catalog is never replaced with an empty database.

## Health, requests and shutdown

`GET /health/live` returns `200` without storage access. It indicates that the
process can answer HTTP requests. `GET /health/ready` performs one fresh bounded
catalog load, including validation, within five seconds. It returns `200` for a
readable supported catalog and `503` on failure or timeout. Readiness does not
scan snapshots or prove write access. Both routes are unauthenticated, bypass
operation admission, return empty bodies and expose no configuration.

`POST /v1/operations` requires `Authorization: Bearer <token>` and
`Content-Type: application/json`. This lists table descriptors sorted by name:

```sh
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer ${MESHDB_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data '{"request_id":"00000000-0000-4000-8000-000000000001","operation":"table.list","payload":{}}' \
  http://127.0.0.1:8000/v1/operations
```

An empty database returns HTTP `200` with
`{"request_id":"00000000-0000-4000-8000-000000000001","data":[]}`.
Use a fresh request UUID for correlation in actual calls. Errors carry a
non-2xx status and an `error` envelope with `code`, `message` and `outcome`.
The request ID can be null when validation has not recovered a valid UUID.
See the [wire contract](../plan-v0/server.md#http-contract) for operations and
error mappings.

SIGINT or SIGTERM stops admission and ingress, then drains admitted work,
including mutations detached by client disconnects, before exiting. Operation
completion logs contain request IDs, static parsed operation names, elapsed
time, status and error codes. They omit tokens, request payloads, rows and
configuration values.

## Storage behavior and limits

Within the prefix, `catalog.json` references immutable Parquet files. Version 2
catalogs support mixed Legacy entries at `tables/<table_id>/<commit_id>.parquet`
and Partitioned entries whose files are at
`tables/<table_id>/<commit_id>/<ordinal>.parquet`.
A manifest records each file's typed primary-key range, row count, closed byte
size and decoded payload size. Ranges are ordered, contiguous, lower-inclusive
and upper-exclusive, with unbounded outer ends. Empty tables use `files: []`
and create no Parquet object.

Version 1 remains readable without startup or read-time rewriting. New catalog
publications use version 2. A legacy table migrates to partitions only on an
actual data change; reads and no-op mutations retain its legacy file. A catalog
can therefore contain both layouts. Old binaries reject version 2 catalogs.

Each read captures one fresh catalog and retains its referenced files during
writes or drops, without taking the mutation lock. Keyed gets and mutations
route to the owning partition. Changed mutations rewrite only affected files;
unchanged files retain their object references. Queries scan partitions on the
shared workers. The coordinator combines their results with one global ordering,
ascending-primary-key tie-break, limit and projection; count sums matches and
applies the global limit. Explain remains metadata-only.

Mutating requests serialize under the existing in-process coordinator while
their partitions prepare in parallel. After all preparation and aggregate limit
checks succeed, object uploads run sequentially. One conditional catalog PUT
against the previous opaque ETag commits the whole mutation. Preparation or
upload failure publishes no partial table, though uploads can leave orphan
objects. This design makes no throughput or scaling guarantee.

The size target measures closed Parquet bytes, including metadata and footer.
New files split by primary-key range toward the configured target. A singleton
may exceed that soft target but never the 67,108,864-byte hard cap. If no valid
file can fit, the whole mutation fails. Changing the configured size affects
new or rewritten ranges and does not automatically repack unchanged files.
Legacy files retain their existing 80 MiB encoded read allowance.

Table handles retain immutable UUID identities. Dropping and recreating a name
produces a new ID, so old handles return `not_found` instead of targeting the new
table. Old snapshots and orphan uploads remain in S3. Dropping removes only the
catalog entry and deletes no objects. Recovery follows the catalog, never a
listing or the newest-looking file.

Conditional publication conflicts return `conflict`. After an ambiguous catalog
PUT, the server rereads once; a matching commit UUID proves success, otherwise
it reports `outcome_unknown` with outcome `unknown`. Publication is not retried
automatically. Request IDs provide no idempotency or deduplication. After an
unknown outcome, inspect and reconcile data before deciding on another write;
an immediate old read or timeout does not prove rollback. The ORM also performs
no automatic retries.

Schemas contain 1 to 128 columns and exactly four nonnullable scalar types:
`utf8` strings, signed `int64` integers, finite `float64` values and `bool`.
Values must match exactly, without coercion or nulls. Primary keys are unique
`utf8` or `int64` columns and cannot be changed by updates. Full rows must contain
every declared column and no extra columns. Table and column names match
`[A-Za-z_][A-Za-z0-9_]{0,63}` and are case-sensitive. Batches are atomic, including
duplicate-key validation.

| Limit | Maximum |
| --- | --- |
| Encoded JSON request or response, including envelope | 8 MiB each |
| Catalog document | 1 MiB |
| Insert batch or collected row result | 10,000 rows |
| Table | 100,000 rows |
| Decoded logical row payload per table | 64 MiB |
| Closed bytes per new partition file, including metadata | 64 MiB = 67,108,864 bytes |

Partitioning does not multiply table, request, batch or result limits.
Limits produce `limit_exceeded`, without truncation. Only server-generated
Parquet snapshots are supported; this is not a general Parquet file importer.
v0 has no garbage collection, general transactions across calls, SQL, joins,
grouping/aggregations, indexes, streaming or distributed execution. Query
`count` is supported as described in the ORM guide.

## Focused checks and live acceptance

For focused local HTTP, health and admission checks, run from the repository root:

```sh
cargo test --locked --manifest-path server/Cargo.toml --test api --test health --test executor
```

Live acceptance requires a built binary, Python 3.11+, the installed local ORM,
AWS CLI, and OpenSSL for the HTTPS lifecycle scenario. Install the ORM as shown
in its guide. The sole actual acceptance target is `meshdb-testing` in
`us-east-1`. The guarded runners explicitly load the repository `.env`, require
its `BUCKET_NAME`, `AWS_REGION` and supplied environment credentials, and ignore
ambient AWS profiles. This restriction belongs to the acceptance runners; the
server itself supports profiles and roles.

Each runner allocates fresh `test-<number>-<scenario>-<random-run-id>/` prefixes,
starts its own server and retains S3 fixtures. Restart scenarios reuse only
their own allocated namespace. Do not substitute the default database prefix
or manually replay a recorded acceptance prefix. Run the runners sequentially:

```sh
orm/.venv/bin/python scripts/acceptance_http.py --binary server/target/debug/meshdb-server
orm/.venv/bin/python scripts/acceptance_lifecycle.py --binary server/target/debug/meshdb-server
orm/.venv/bin/python scripts/acceptance_concurrency.py --binary server/target/debug/meshdb-server
orm/.venv/bin/python scripts/acceptance_latency.py --binary server/target/debug/meshdb-server
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python scripts/acceptance_partitions.py --binary server/target/debug/meshdb-server
```

The [HTTP runner](../scripts/acceptance_http.py) checks types, CRUD and queries;
the [lifecycle runner](../scripts/acceptance_lifecycle.py) checks restart and
trusted HTTPS; the [concurrency runner](../scripts/acceptance_concurrency.py)
checks concurrent clients and retained storage; the
[latency runner](../scripts/acceptance_latency.py) records single-row writes,
batches and scans as tables grow. Latencies are individual end-to-end samples,
not statistical estimates or scaling guarantees. Runners write fresh sanitized
JSON reports beneath `artifacts/`, or accept `--report` with an unused path.

The prepared [partition runner](../scripts/acceptance_partitions.py) allocates
`test-10-partition-workers-<run-id>/`, checks actual object sizes and manifests,
routed CRUD, global query results, atomic failure and retained objects, then
restarts that namespace with three workers instead of two. Both runs use a
64 KB target. Retain its fixtures and sanitized report, and record the exact
command, report, outcome and allocated prefix under [scripts/notes/](../scripts/notes/).
Never source, print or commit the supplied dotenv file or credentials. Its live
S3 run [passed all 53 checks](../scripts/notes/PW-E02.md); see the
[current rubric](../plans/partition-workers.md) for final audit status.

Historical implementation details are recorded in the notes for
[configuration](notes/R10a.md), [CLI](notes/R10b.md),
[operations HTTP](notes/R09.md), [health](notes/R09b.md),
[S3/catalogs](notes/R02.md) and [publication](notes/R04.md).

## Local API and worker milestone

A local process acceptance runner starts a real HTTP object store, a supervised
API and two manually enrolled worker processes with synthetic credentials. It
checks public data operations, TCP peer/self partition jobs, competing writes,
exact replay after a lost response and API restart, and process shutdown.
Run from the repository root using the existing dependency cache:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo build --manifest-path server/Cargo.toml --locked --offline --no-default-features --bin meshdb-server
python3 scripts/acceptance_nodes.py --binary server/target/debug/meshdb-server
```

The runner needs only Python's standard library and loopback TCP. It creates and
removes its own SQLite store and configuration. It reads no dotenv or cloud
profiles. Its startup exchange demonstrates `meshdb-server api --config PATH`
over the inherited `MESHDB_SUPERVISOR_FD` socket and
`meshdb-server worker --bootstrap PATH`. An API requires that supervisor
exchange; it is not an unsupervised standalone command.

This milestone supports the HTTP storage backend, external provisioning and a
fixed worker policy. Managed provisioning, autoscaling, administration routes,
S3 runtime composition and positive safe-termination authorization remain
unfinished. Signal shutdown stops admission and waits for owned work; it is not
a distributed idle certificate. See [runtime evidence](notes/DN-runtime-integration.md)
for executed checks and limits.

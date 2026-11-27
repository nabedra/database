# Nabedra Database v0 server

The `nabedra-db` Rust binary serves the table and query API over HTTP or
native HTTPS. Durable table data uses the configured object store. `serve`
composes one API with a local worker; `api --config PATH` and
`worker --bootstrap PATH` run separate processes with the same request, receipt,
and atomic publication contracts.

The [Python ORM guide](../../orm/python/docs/reference.md) covers client installation
and the full API. The [execution and recovery plan](../../plans/resource-aware-scheduling.md)
records worker and storage requirements; the
[test contract](../../docs/testing/scheduler-contract.md) defines their acceptance scenarios.

## Worker queue and resource configuration

`POST /v1/operations` waits for the terminal response. `POST /v1/requests` accepts
the same `{request_id, operation, payload}` envelope and returns HTTP 202 with
`request_id`, `run_id`, and `state: "queued"`. `GET /v1/requests/{run_id}/{request_id}`
returns its state, result, and protocol error. Both interfaces use the data bearer
token. A 202 response confirms replication to all ready workers, never successful
mutation publication.

Requests and read snapshots live in replicated worker RAM. Individual failures
retain accepted work while another complete copy survives. Joining workers copy
outstanding requests before readiness; drains transfer copies before removal.
After a fixed/manual cluster's last queue holder stops, use `--cold-start` on
the API or local `serve` to fence old authority and create a fresh run. Old unfinished receipts become abandoned;
durable mutation decisions and committed rows remain available. Heartbeat expiry
can fence a failed incarnation but never authorizes an automatic cold start.
A configured provider can establish a new empty run only after fresh inventory
proves that every retained member stopped, as described in
[provider configuration](../../docs/testing/provisioning-adapters.md).

A brand new namespace creates a fenced format 6 catalog. Format 6 is the only
catalog format: a root of any other format is refused at startup and never
migrated.

For local `serve`, `--execution-config PATH` and `--resources-config PATH` read
strict JSON configuration objects matching
[the scheduling configuration](../../plans/resource-aware-scheduling.md). Legacy
`--workers`, `--queue-size`, and `--partition-size` supply defaults when those
files are absent. Distributed worker templates carry `execution` and each
worker's `resources`; signed enrollment preserves heterogeneous CPU/RAM ceilings.
Each worker reports its actual limits, derived coordination capacity, and bounded
control thread/transport overhead in its startup announcement. The binary uses
two Tokio network threads and at most four Tokio blocking threads; each worker
has two separate control codec threads. Partition, Arrow/codec, and reducer work
share the advertised compute thread limit. Legacy
`operation_slots` no longer serializes queued requests as compute permits. Signed
`operation_slots` reports request coordination capacity; signed `partition_slots`
reports bounded job coordination/prefetch capacity. `resources.max_threads` is the
compute ceiling. A worker reserves at least two job actors per compute thread
plus progress capacity, independently of its legacy partition-slot hint.

Each request exposes a frontier of twice the compute capacity of its allowed
workers. The coordinator validates enough outgoing stream capacity for three
such frontiers plus progress slots before accepting a worker enrollment or
scaling policy. An unsupported topology reports the minimum required
`control_memory_reserve_bytes`; it does not silently restrict the advertised CPU
capacity. Worker listeners reserve separate connection classes for job data,
16 submissions, 8 receipts, 8 ordinary replicas, 4 bootstrap transfers,
8 control requests, and 24 unclassified header handshakes. Signed queue metadata
binds the command to its connection class, which the decoded body must match.
Bootstrap copies and small control replies have reserved memory independent of
request staging and ordinary replica traffic. Startup announcements report these
network bounds and the derived outgoing stream limit.

Queue/result caps are charged for the lifetime of the worker. Active request
bodies, pinned snapshots, partition stages, outputs, and reductions retain
separate memory charges through settlement. Shared compute grants enforce
`max_threads`; coordinator waits, queue copies, storage I/O, and commit waits do
not hold those grants. Memory accounting contains ordinary allocation failures;
it does not provide process isolation from native aborts or an OS OOM kill.

Use [the scheduler test contract](../../docs/testing/scheduler-contract.md) to assess
individual acceptance cases. `scripts/acceptance_scheduler.py` exercises real TCP
queue/recovery gates with a local conditional store. Those assertions do not
establish AWS or Kubernetes behavior.

## Build and start

Install the native server from crates.io with Rust 1.94.1 or newer and Cargo:

```sh
cargo install nabedra-database --version 0.1.1 --locked
nabedra-db --version
```

The package is named `nabedra-database`; its executable is `nabedra-db`
and its Rust library is `nabedra_database_server`.
Version 0.1.1 renames the executable from `nabedra-database-server`.
Update service commands and explicit `NABEDRA_SERVER_BINARY` paths when upgrading.
The default `s3` feature includes the AWS storage backend. The Python CLI and ORM
are separate; use the [repository installer](../../scripts/install_nabedra.py) for
both the Python tools and native server.

The crate uses edition 2024, pins direct
dependency versions in [Cargo.toml](../Cargo.toml), and records resolution in
[Cargo.lock](../Cargo.lock). Run these commands from the repository root:

```sh
cargo build --locked --manifest-path server/Cargo.toml
server/target/debug/nabedra-db serve
```

For a release binary:

```sh
cargo build --locked --release --manifest-path server/Cargo.toml
server/target/release/nabedra-db serve
```

Configure the environment before starting either binary. For example, with an
already configured AWS profile and a token supplied in the environment:

```sh
export AWS_PROFILE='<configured-profile>'
export AWS_REGION='<bucket-region>'
export NABEDRA_S3_BUCKET='<existing-bucket>'
export NABEDRA_S3_PREFIX='nabedra-v0/'
: "${NABEDRA_TOKEN:?Set NABEDRA_TOKEN to your shared bearer token}"
server/target/debug/nabedra-db serve
```

For development in this shared checkout, keep Cargo commands sequential and
reuse `server/target`. The disk-saving settings are
`CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`.

## Configuration

These environment settings configure the standalone S3 `serve` backend.
Supervised `api --config PATH` and `worker --bootstrap PATH` instead read their
explicit HTTP-backend, authentication and worker configuration files; see
[cluster process acceptance](#cluster-process-acceptance).

| Environment variable | Requirement and meaning |
| --- | --- |
| `NABEDRA_S3_BUCKET` | Required, nonempty existing S3 general purpose bucket. The server does not create buckets. |
| `NABEDRA_TOKEN` | Required, nonempty shared bearer token for operations. Use a header-safe token shared with clients. |
| `NABEDRA_S3_PREFIX` | Optional database namespace, default `nabedra-v0/`. An explicit empty prefix is invalid. Trailing slashes normalize to one; absolute paths, traversal, empty internal components and unsafe escaped paths are rejected. |
| `AWS_REGION` | Bucket region, required unless resolved by the standard AWS region chain. |
| `AWS_PROFILE` | Optional named shared AWS profile. Profiles and role-based credentials are valid server configurations. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Optional environment credentials for the standard AWS credential chain; unnecessary when another provider supplies credentials. |
| `AWS_SESSION_TOKEN` | Required with temporary environment credentials; otherwise optional. |
| `AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE` | Optional standard AWS shared-file location overrides. |

The S3 backend uses the AWS SDK's standard credential and region chains, including
shared profiles and roles. AWS credentials stay on the server; clients need only
the API URL and bearer token. The configured identity needs access to read and
conditionally write objects beneath the database prefix.

The supplied `.env` uses `BUCKET_NAME`. The server requires `NABEDRA_S3_BUCKET`
and does not implicitly load dotenv files or translate `BUCKET_NAME`. Supply
the server variables explicitly through the process environment or your service
manager. The guarded acceptance scripts below perform their own dotenv loading.

`nabedra-db serve` accepts exactly these application flags, plus standard
Clap help/version options:

| Flag | Default | Constraint |
| --- | --- | --- |
| `--host` | `127.0.0.1` | An IPv4 or IPv6 address. Nonloopback binding must be explicit. |
| `--port` | `8000` | Unsigned 16-bit port. |
| `--workers` | `4` | Default local compute thread ceiling, in `1..256`; overridden by `resources.max_threads` when a resource file is supplied. |
| `--partition-size`, alias `--partition_size` | `67108864 B` | Positive integer with a supported unit, at most 67,108,864 bytes. |
| `--queue-size` | `64` | Legacy additional pending-request default and ingress coordination backlog; zero is valid. An execution file supplies the request queue limit directly. |
| `--execution-config` | Absent | Readable strict JSON execution limits, including request count, replica/result bytes, retention and partition size. |
| `--resources-config` | Absent | Readable strict JSON compute and memory ceilings; `max_threads` overrides `--workers`. |
| `--cold-start` | `false` | Explicitly fence a previous stopped local run and start an empty request queue while retaining committed data. |
| `--tls-cert` | Absent | Readable PEM certificate file, supplied with `--tls-key`. |
| `--tls-key` | Absent | Readable matching PEM private key, supplied with `--tls-cert`. |

Without an execution file, `workers + queue-size` supplies
`execution.max_pending_requests`; checked configuration arithmetic must fit the
supported limits. Authentication and bounded ingress admission precede body
reads. Acceptance then requires queue-count and replica-byte capacity and copies
on all ready workers. Compute saturation queues accepted work within those caps.
Ingress or queue saturation can return `429 overloaded` before acceptance;
already accepted records retain their identity through terminal settlement.
Coordination and ingress limits are separate from compute grants and the full
worker-memory request queue.

For example:

```sh
server/target/debug/nabedra-db serve --workers 4 --partition_size '64 KB'
```

`--partition-size '64 KB'` is equivalent. Units are mandatory and case-insensitive:
`B`, `KB`, `MB`, `KiB`, `MiB`. Whitespace is optional, so `64kb`, `'64 Kb'` and
`64KiB` are accepted. SI units use powers of 1000; IEC units use powers of 1024.
Only positive integer quantities are valid. Bare numbers, signs, decimal
notation, zero, overflow and values above 64 MiB are rejected. The default and
hard maximum are 64 MiB, exactly 67,108,864 bytes; `64MiB` and `67108864B` are accepted.

`--workers N` supplies N persistent local compute threads unless the resource
file overrides that ceiling. Partition, codec and reducer jobs from concurrent
requests share those slots. A free slot takes the next eligible job; completions
release capacity and refill the frontier without waiting for a dispatch window.
CPU and working-memory reservations apply together, and remote node allocations
respect `max_workers_per_request`, including reduction. Coordination, storage
waits, queue replication and settlement use separate bounded progress capacity.
Changing the compute ceiling on restart preserves partition identities and
committed table contents; worker count does not determine file count.

Without TLS flags, the listener serves HTTP. To serve native HTTPS on an
explicit nonloopback address:

```sh
server/target/release/nabedra-db serve --host 0.0.0.0 --port 8443 \
  --workers 4 --queue-size 64 \
  --tls-cert /path/to/server-cert.pem --tls-key /path/to/server-key.pem
```

Clients must trust the certificate's CA and verify its hostname. For a private
CA, use curl's `--cacert /path/to/ca.pem` or the ORM's `ca_file` argument, as
shown in the ORM guide. Invalid configuration, inaccessible or invalid catalog
data, and invalid TLS material fail startup before HTTP ingress begins. A definitely
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
  -H "Authorization: Bearer ${NABEDRA_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data '{"request_id":"00000000-0000-4000-8000-000000000001","operation":"table.list","payload":{}}' \
  http://127.0.0.1:8000/v1/operations
```

An empty database returns HTTP `200` with
`{"request_id":"00000000-0000-4000-8000-000000000001","data":[]}`.
Use a fresh request UUID for correlation in actual calls. Errors carry a
non-2xx status and an `error` envelope with `code`, `message` and `outcome`.
The request ID can be null when validation has not recovered a valid UUID.
See the [Python API reference](../../orm/python/docs/reference.md) for operation
behavior and error mappings.

SIGINT or SIGTERM stops admission and ingress, then drains admitted work,
including mutations detached by client disconnects, before exiting. Operation
completion logs contain request IDs, static parsed operation names, elapsed
time, status and error codes. They omit tokens, request payloads, rows and
configuration values.

## Storage behavior and limits

Within the prefix, `catalog.json` is the conditional publication root. It uses
format 6, which stores the durable decision head, the root of the immutable
decision index and the `run_id`/scheduler-term fence alongside table references.
The decision index (`decision-index/<sha256>.bin` pages) proves whether a request
ID was already decided in a bounded number of reads, however long the history. An ownership takeover
or cold start updates that same root, so an old attempt's catalog ETag cannot
publish after its fence has been superseded.

Two table layouts are supported inside format 6:
Legacy entries at `tables/<table_id>/<commit_id>.parquet` and Partitioned entries
at `tables/<table_id>/<commit_id>/<ordinal>.parquet`.
A manifest records each file's typed primary-key range, row count, closed byte
size and decoded payload size. Ranges are ordered, contiguous, lower-inclusive
and upper-exclusive, with unbounded outer ends. Empty tables use `files: []`
and create no Parquet object.

A Legacy table becomes partitioned on an actual data change, while reads and
no-op mutations retain its existing file. Mixed table layouts remain valid.

Each read captures one fresh catalog and retains its referenced files during
writes or drops, without taking the mutation lock. Keyed gets and mutations
route to the owning partition. Changed mutations rewrite only affected files;
unchanged files retain their object references. Queries scan partitions on the
shared workers. The coordinator combines their results with one global ordering,
ascending-primary-key tie-break, limit and projection; count sums matches and
applies the global limit. Explain remains metadata-only.

Concurrent mutations prepare private candidates from captured catalogs. Every
affected partition and aggregate limit must pass before one conditional catalog
PUT publishes the complete reference set and durable request decision under the
current fence. A CAS loser recomputes against a fresh catalog within its bounded
retry policy. Preparation or upload failure exposes no partial table. Uploaded
objects that the root never references remain invisible orphans.

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

Accepted requests deduplicate by their exact identity and body. An identical
same-run submission attaches to its existing execution or receipt; a different
body conflicts. Mutations also retain durable decisions, so recovery reconciles
a catalog PUT whose response was lost instead of applying the mutation twice.
An unresolved publication reports `outcome_unknown` with outcome `unknown`;
a timeout or an older read does not prove rollback. Recover the same receipt or
reconcile the durable decision before choosing another logical write. A complete
worker shutdown abandons outstanding memory work rather than resubmitting it.
The ORM performs no automatic mutation resubmission.

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
The server has no garbage collection, general transactions across calls, SQL,
joins, grouping/aggregations, indexes or public result-streaming interface.
Partition jobs can execute on separate worker processes through the configured
cluster runtime. Query `count` is supported as described in the ORM guide.

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
their own allocated namespace. The process helper retains the original child
receipt on that namespace and supplies `--cold-start` only after observing that
child's exit, including when a new helper changes the worker count. It never
silently acknowledges migration of a pre-existing legacy catalog. Do not substitute the default database prefix
or manually replay a recorded acceptance prefix. Run the runners sequentially:

```sh
orm/python/.venv/bin/python scripts/acceptance_http.py --binary server/target/debug/nabedra-db
orm/python/.venv/bin/python scripts/acceptance_lifecycle.py --binary server/target/debug/nabedra-db
orm/python/.venv/bin/python scripts/acceptance_concurrency.py --binary server/target/debug/nabedra-db
orm/python/.venv/bin/python scripts/acceptance_latency.py --binary server/target/debug/nabedra-db
PYTHONDONTWRITEBYTECODE=1 orm/python/.venv/bin/python scripts/acceptance_partitions.py --binary server/target/debug/nabedra-db
```

The [HTTP runner](../../scripts/acceptance_http.py) checks types, CRUD and queries;
the [lifecycle runner](../../scripts/acceptance_lifecycle.py) checks restart and
trusted HTTPS; the [concurrency runner](../../scripts/acceptance_concurrency.py)
checks concurrent clients and retained storage; the
[latency runner](../../scripts/acceptance_latency.py) records single-row writes,
batches and scans as tables grow. Latencies are individual end-to-end samples,
not statistical estimates or scaling guarantees. Runners write fresh sanitized
JSON reports beneath `artifacts/`, or accept `--report` with an unused path.

The prepared [partition runner](../../scripts/acceptance_partitions.py) allocates
`test-10-partition-workers-<run-id>/`, checks actual object sizes and manifests,
routed CRUD, global query results, atomic failure and retained objects, then
restarts that namespace with three workers instead of two. Both runs use a
64 KB target. Retain its fixtures and sanitized report, and record the exact
command, report, outcome and allocated prefix under [scripts/notes/](../../scripts/notes).
Never source, print or commit the supplied dotenv file or credentials. Its historical live S3 run
[passed all 53 partition checks](../../scripts/notes/PW-E02.md). That report predates
the replicated queue and format 4 fences; current scheduler/provider evidence is
listed below.

Historical implementation details are recorded in the notes for
[configuration](../notes/R10a.md), [CLI](../notes/R10b.md),
[operations HTTP](../notes/R09.md), [health](../notes/R09b.md),
[S3/catalogs](../notes/R02.md) and [publication](../notes/R04.md).

## Cluster process acceptance

A local process acceptance runner starts a real HTTP object store, a supervised
API and two manually enrolled worker processes with synthetic credentials. It
checks public data operations, TCP peer/self partition jobs, competing writes,
exact replay after a lost response and API restart, and process shutdown.
Run from the repository root using the existing dependency cache:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo build --manifest-path server/Cargo.toml --locked --offline --no-default-features --bin nabedra-db
python3.11 scripts/acceptance_nodes.py --binary server/target/debug/nabedra-db
```

The runner needs Python 3.11+, its standard library and loopback TCP. It creates and
removes its own SQLite store and configuration. It reads no dotenv or cloud
profiles. Its startup exchange demonstrates `nabedra-db api --config PATH`
over the inherited `NABEDRA_SUPERVISOR_FD` socket and
`nabedra-db worker --bootstrap PATH`. An API requires that supervisor
exchange; it is not an unsupervised standalone command. `api` also accepts
`--cold-start` with the same cold-run confirmation requirement described above.

The supervised cluster uses the HTTP object backend. It supports heterogeneous
workers, replicated in-memory requests, fenced publication, public cluster
administration and configured provider callbacks. The Kubernetes adapter creates
workers and uses fresh drain evidence before termination. The standalone S3
backend remains available through the `s3` feature; supervised S3 composition
requires a separate storage integration.

The scheduling, failure, and provider fixtures are
`scripts/acceptance_scheduler.py`, `scripts/acceptance_scheduler_load.py`, and
`scripts/acceptance_providers.py`. Their case mappings and execution results are
recorded in [scheduler evidence](../../docs/testing/scheduler-evidence.md).
See [provider configuration](../../docs/testing/provisioning-adapters.md) for managed
workers and the live-infrastructure verification boundary. Signal shutdown drains
owned local work; provider termination additionally requires a distributed idle
proof.


## Existing storage and cluster compatibility

The rename keeps catalog and snapshot formats unchanged. Storage identity digests, configuration and template fingerprint domains, persisted worker verifier domains, and the stored `meshdb.callbacks.v1` callback adapter contract retain their original values. Existing committed data does not need a format migration for the rename. The real acceptance bucket `meshdb-testing` also retains its infrastructure name.

The default namespace for a new legacy `serve` process is now `nabedra-v0/`. To reopen data previously written under the default namespace, set `NABEDRA_S3_PREFIX=meshdb-v0/` explicitly, or keep the existing configured namespace. Renaming a namespace selects a different database; it does not move existing objects.

Environment settings now use `NABEDRA_*`, and HTTP headers use `X-Nabedra-Storage` and the internal `X-Nabedra-*` prefix. `X-Nabedra-Database` selects a database at the Python gateway. Restart the cluster with matching renamed API, worker and client versions, since peer authentication domains and headers changed. The public launcher is `nabedra database <command>`; direct Rust commands use `nabedra-db <command>`.

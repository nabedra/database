# MeshDB v0 server

The `meshdb-server` Rust binary serves the table and query API over HTTP or
native HTTPS. S3 is its only durable store. Run one process per bucket/prefix;
v0 does not coordinate multiple server processes sharing a namespace.

The [Python ORM guide](../orm/README.md) covers client installation and the full
API. The [server contract](../plan-v0/server.md) defines the wire protocol, and
the [implementation tracker](../plan-v0/implementation.md) records task status.

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
| `--workers` | `4` | At least 1 executing operation. |
| `--queue-size` | `64` | Nonnegative queued operation count; zero is valid. |
| `--tls-cert` | Absent | Readable PEM certificate file, supplied with `--tls-key`. |
| `--tls-key` | Absent | Readable matching PEM private key, supplied with `--tls-cert`. |

The checked sum of workers and queue size must fit the executor's semaphore
limit. Authentication and admission precede operation-body reads. At most
`workers + queue-size` operations are admitted and at most `workers` execute;
full or stopped admission returns `429 overloaded`.

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

Within the prefix, `catalog.json` references immutable
`tables/<table_id>/<commit_id>.parquet` snapshots. Each read loads one fresh
catalog and uses its referenced snapshot. Mutations serialize in the process,
upload a complete replacement table snapshot, then conditionally publish the
catalog against its previous opaque ETag. That catalog PUT is the commit point.
Reads take no mutation lock and retain their observed snapshot during writes
or drops. Key lookups and row queries scan whole tables; changed tables require
whole-table Parquet rewrites.

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
```

The [HTTP runner](../scripts/acceptance_http.py) checks types, CRUD and queries;
the [lifecycle runner](../scripts/acceptance_lifecycle.py) checks restart and
trusted HTTPS; the [concurrency runner](../scripts/acceptance_concurrency.py)
checks concurrent clients and retained storage; the
[latency runner](../scripts/acceptance_latency.py) records single-row writes,
batches and scans as tables grow. Latencies are individual end-to-end samples,
not statistical estimates or scaling guarantees. Runners write fresh sanitized
JSON reports beneath `artifacts/`, or accept `--report` with an unused path.

Implementation details are recorded in the notes for
[configuration](notes/R10a.md), [CLI](notes/R10b.md),
[operations HTTP](notes/R09.md), [health](notes/R09b.md),
[S3/catalogs](notes/R02.md) and [publication](notes/R04.md).

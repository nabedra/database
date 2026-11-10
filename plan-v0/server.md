Build a terminal-run Rust server to test MeshDB's table and query API in one process, with a bounded executor and S3 as its only durable store.

Adapt [data.md](../plan/data.md), [server.md](../plan/server.md) and [storage.md](../plan/storage.md). This v0 plan owns HTTP(S), execution, metadata and storage. [orm.md](orm.md) owns the pure Python client. The distributed plans remain a separate design.

---

## Process and dependencies

Create a Rust crate at `server/`, with binary name `meshdb-server`. Use `main.rs` for CLI/startup, `api.rs` for transport, `protocol.rs` for wire types, `executor.rs` for admission, `engine.rs` for table/query operations and `storage.rs` for S3/catalog access.

| Dependency | Use |
| --- | --- |
| [clap](https://docs.rs/clap/latest/clap/) | Parse `serve` and its arguments. |
| [Axum](https://docs.rs/axum/latest/axum/), [axum-server](https://docs.rs/axum-server/latest/axum_server/) | Serve HTTP and native HTTPS through the `tls-rustls` feature. |
| [Tokio](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html) | Async network I/O, semaphores, shutdown and blocking compute tasks. |
| [Serde](https://serde.rs/), `serde_json` | Typed JSON requests, responses and catalog documents. |
| [AWS SDK for Rust](https://docs.rs/aws-sdk-s3/latest/aws_sdk_s3/) | Access S3 with `aws-config` and `aws-sdk-s3`. |
| [Arrow/Parquet](https://docs.rs/parquet/latest/parquet/arrow/index.html) | Encode/decode table snapshots with Rust Arrow arrays and the `parquet` crate. |

Pin compatible stable dependencies and commit `server/Cargo.lock` during implementation.

```sh
cargo run --release --manifest-path server/Cargo.toml -- serve \
  --host 127.0.0.1 --port 8000 --workers 4 --queue-size 64

server/target/release/meshdb-server serve --host 0.0.0.0 --port 8443 \
  --workers 4 --queue-size 64 --tls-cert cert.pem --tls-key key.pem
```

The first command shows the defaults for host, port, workers and queue size. Require `workers >= 1` and `queue-size >= 0`. A certificate/key pair selects HTTPS; otherwise serve HTTP. Reject an incomplete pair or an unreadable certificate before listening.

| Environment variable | Meaning |
| --- | --- |
| `MESHDB_S3_BUCKET` | Required existing AWS S3 general purpose bucket. |
| `MESHDB_S3_PREFIX` | Database namespace; default `meshdb-v0/`. Normalize to one trailing slash. |
| `AWS_REGION` | Bucket region, unless supplied by the AWS configuration chain. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Optional inputs to the standard AWS credential chain; profiles and roles also work. |
| `MESHDB_TOKEN` | Required shared bearer token for `/v1/operations`. |

Load the SDK's [region configuration](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/region.html) and [credential providers](https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html). Keep AWS access entirely on the server. Document these variables now; the user will supply their values later.

## Execution and lifecycle

- Run one server process for each bucket/prefix. PostgreSQL, worker processes, cluster membership and autoscaling are outside v0.
- Use one Tokio runtime. S3/HTTP stay async; Parquet encoding/decoding, row validation and query execution use `tokio::task::spawn_blocking`, the Rust equivalent of the proposed thread executor.
- Before reading an operation body, authenticate and try to acquire an admission permit. Allow `workers + queue-size` admitted operations; return HTTP `429` with `overloaded` when full. Acquire a separate execution permit before reading/processing the body, so at most `workers` operations load tables or compute at once.
- Each executing operation awaits at most one blocking task at a time. A global `tokio::sync::Mutex` serializes complete mutations, including catalog publication. Reads take no write lock and use the immutable snapshot they observed.
- Track running operations independently of client connections. Keep their permits until all work finishes; disconnecting does not abort a commit. Tokio documents that a started blocking task [cannot be aborted](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).
- At startup, read and validate `catalog.json`; initialize it conditionally only on a definite `404`. Missing configuration, denied access and unsupported catalog versions fail startup. Never replace an unreadable catalog with an empty database.
- `GET /health/live` returns `200` while the process runs. `GET /health/ready` performs a bounded catalog read and returns `200` only for a readable, supported catalog, otherwise `503`. Both are unauthenticated and return no configuration.
- On SIGINT/SIGTERM, stop accepting operations, drain admitted work and then exit. Log request IDs, operation names, durations and error codes without tokens or row contents.

---

## S3 layout and commits

All paths below are relative to the configured prefix. Memory holds temporary buffers only; restarting needs no local database, journal or saved cache.

```text
catalog.json
tables/<table_id>/<commit_id>.parquet
```

`catalog.json` is the only mutable database object. Its initial contents have `revision: 0` and `tables: {}`. A populated catalog has this shape; IDs are UUID strings and `snapshot` is a relative object key.

```json
{
  "format_version": 1,
  "revision": 1,
  "commit_id": "uuid",
  "tables": {
    "users": {
      "id": "uuid",
      "schema": {"id": "utf8", "age": "int64"},
      "key": "id",
      "format": "parquet",
      "snapshot": "tables/uuid/uuid.parquet",
      "row_count": 1
    }
  }
}
```

Use [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) for publication. Treat ETags as opaque comparison tokens. S3's [strong read-after-write consistency](https://aws.amazon.com/s3/consistency/) lets a subsequent read discover a successful catalog update.

1. Acquire the global mutation lock, then fetch the catalog and its ETag. Creation validates the new name/schema; other mutations resolve the table by immutable ID and validate against its schema.
2. Load the referenced snapshot, or use an empty table for creation. Apply the complete mutation in memory and check primary-key uniqueness. Reject the whole request if any row fails. A missing-row update/delete returns `false` without publishing anything.
3. Generate a new server-side commit UUID for every catalog change. For a data change, encode a complete Parquet snapshot and close its writer before uploading with `If-None-Match: *`. Creating an empty table also writes a valid empty Parquet file. Dropping a table only removes its catalog entry.
4. Build a replacement catalog with `revision + 1`, the new commit UUID and the updated table entries. Publish with `PutObject.if_match(old_etag)`. Initialize a missing catalog with `if_none_match("*")`. These builders are available in the [Rust SDK](https://docs.rs/aws-sdk-s3/latest/aws_sdk_s3/operation/put_object/builders/struct.PutObjectFluentBuilder.html).
5. Return success only after catalog publication succeeds. This conditional PUT is the commit point. Each read fetches the catalog once, then reads exactly its referenced immutable snapshot, so it sees one committed table version.
6. Disable automatic SDK retries for catalog PUTs. A definite `409`/`412` returns `conflict`; never overwrite unconditionally. If a publication response is lost, reread the catalog under the lock. A matching commit UUID proves success; otherwise report `outcome_unknown`. Do not infer rollback from a timeout or an older observed catalog.

An upload without a catalog reference is an orphan. Preserve old snapshots and orphans in v0; `drop()` performs no object deletion. This also keeps snapshots readable during concurrent updates/drops. Recovery follows `catalog.json`, never a prefix listing or the newest-looking filename. Referenced-but-missing objects are storage errors.

Use complete snapshot rewrites and table scans for this experiment. Key lookup is a scan, and every changed table incurs a full Parquet upload plus a catalog PUT. There is no OLTP latency or scaling claim.

---

## HTTP contract

`POST /v1/operations` requires `Authorization: Bearer <token>` and `Content-Type: application/json`. Validate a typed, explicit operation set; reject unknown fields/operators. Success uses HTTP `200` and a `data` envelope. Errors use a non-2xx status and an `error` envelope.

```json
{"request_id":"uuid","operation":"table.insert","payload":{"table_id":"uuid","row":{"id":"alice","age":24}}}
```

```json
{"request_id":"uuid","data":null}
```

```json
{"request_id":"uuid","error":{"code":"conflict","message":"Primary key already exists","outcome":"not_applied"}}
```

`request_id` is a client UUID for correlation, with `null` in errors before a valid ID is available. It provides no deduplication. Error `outcome` is `not_applied` or `unknown`; report `unknown` whenever a mutation may have committed. Never automatically replay a mutation.

| Operation | Payload fields | `data` |
| --- | --- | --- |
| `table.create` | `name`, `schema`, `key`, `format` | Table descriptor. |
| `table.describe` | `name` | Table descriptor. |
| `table.list` | None. | Descriptors sorted by name. |
| `table.drop` | `table_id` | `null`. |
| `table.insert` | `table_id`, `row` | `null`. |
| `table.insert_many` | `table_id`, `rows` | Inserted integer count. |
| `table.get` | `table_id`, `key` | Row or `null`. |
| `table.update` | `table_id`, `key`, `values` | Whether the row existed. |
| `table.upsert` | `table_id`, `row` | `null`. |
| `table.delete` | `table_id`, `key` | Whether the row existed. |
| `query.collect` | `table_id`, `query` | Array of rows. |
| `query.count` | `table_id`, `query` | Integer row count. |
| `query.explain` | `table_id`, `query` | Text describing scan, filter, sort, limit and projection; no data scan. |

A descriptor contains `id`, `name`, `schema`, `key` and `format`, with the schema's wire type names. Handles use the ID, so dropping/recreating a name never redirects an old handle to another table.

## Schema and query rules

- Support `str`, `int`, `float`, `bool` through wire types `utf8`, `int64`, `float64`, `bool`. All columns are required and non-nullable. Require exact types, signed 64-bit integers and finite floats; reject booleans as integers and reject implicit coercion. Only `parquet` is a table format.
- Require a primary key of type `str` or `int`. Inserts reject existing keys; upsert replaces a complete row. Update merges a partial row and rejects changes to the primary key. `insert_many` is one atomic request; duplicate keys within its batch or the existing table reject the whole batch. An empty batch returns `0`.
- Table/column names match `[A-Za-z_][A-Za-z0-9_]{0,63}` and are case-sensitive. Schemas have 1 to 128 columns. Creating an existing name returns `conflict`; unknown names/IDs return `not_found`. Schema changes are outside v0.

Every query has these four fields; `null` selects all columns, omits a predicate/order or applies no explicit limit, respectively.

```json
{
  "select": ["id", "age"],
  "filter": {"op": "gt", "column": "age", "value": 18},
  "order_by": {"column": "age", "desc": false},
  "limit": 100
}
```

Predicates are `{"op":"eq","column":"id","value":"alice"}`, the corresponding `ne`, `gt`, `ge`, `lt`, `le` forms, `{"op":"and","args":[...]}`, `{"op":"or","args":[...]}` or `{"op":"not","arg":...}`. Logical argument lists are nonempty; cap nesting at 32. Comparisons use the column's exact type. Booleans support only `eq`/`ne`; strings use lexical order. No Python code or SQL crosses the wire.

Evaluate filter, order, limit, then projection. Default order is primary-key ascending; an explicit order uses primary-key ascending to break ties. Require a nonempty list of unique, existing selected columns when `select` is not `null`, and a nonnegative integer limit; `limit(0)` returns no rows. Count the resulting row set, including its limit, without materializing a row response.

Cap request/response bodies at 8 MiB, catalog documents at 1 MiB, batches and row results at 10,000 rows, and tables at 100,000 rows and 64 MiB of decoded row payload. Check limits before publication and during decoding. Return `limit_exceeded` rather than truncating; users can reduce a query with `limit()`.

Map `validation_error`/`unsupported_operation` to `400`, `unauthorized` to `401`, `not_found` to `404`, `conflict` to `409`, `limit_exceeded` to `413`, `overloaded` to `429`, `storage_unavailable`/`outcome_unknown` to `503` and unexpected failures to `500`. Preserve the outcome distinction for all mutation failures.

---

## Implementation and acceptance

Implement the protocol/CLI, S3 catalog and conditional publication first. Add the bounded executor and atomic CRUD, then predicates/projection/order/limit/count/explain and HTTP(S). Keep storage and execution inside this crate.

Use Rust unit tests with an S3 fake that models conditional PUTs and injects upload/publication failures. Verify whole-batch rollback, duplicate keys, concurrent writes without lost updates, snapshot reads during a write/drop, stale handles, overload, lost publication responses and recovery after process restart. Require `cargo fmt --check`, `cargo clippy -- -D warnings` and `cargo test` for the crate.

Once credentials are supplied, run the [ORM example](orm.md#minimal-usage) against real S3 in a fresh prefix. Restart the server and read the same data, repeat over HTTPS with a trusted test certificate, and record single-row/batch latency plus scan time as row counts grow.

General transactions, joins, grouping/aggregations, indexes, Arrow IPC/import/export, streaming, analytics/ML helpers, garbage collection and distributed execution remain later work. Return `unsupported_operation` for their wire requests.

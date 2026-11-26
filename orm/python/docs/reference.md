# Nabedra Database Python ORM

Nabedra Database's synchronous Python client creates tables, reads and writes rows, and
builds queries for a separately running Nabedra Database v0 server. It requires Python
3.11+ and has no runtime dependencies beyond the standard library. The client
does not start the server or need AWS credentials; the server owns S3 access.

For shared compute, point this client at your application's endpoint. The
[framework-independent gateway](https://github.com/nabedra/database/blob/main/server/docs/cli-and-serverless.md#host-an-application-endpoint)
lets that endpoint authorize and forward the existing operation with storage
credentials supplied by server middleware.

## Install and connect

Install from PyPI with Python 3.11+:

```sh
python3.11 -m pip install "nabedra[database]"
export NABEDRA_TOKEN='<same-token-configured-on-your-server>'
```

The distribution is named `nabedra`; Python imports also use `nabedra`.
The `database` extra adds no dependencies because the ORM uses the standard
library. `pip install nabedra` includes the same ORM and CLI. Optional
`nabedra[gateway]` and `nabedra[aws]` installs add HTTPX and Boto3 respectively;
use `nabedra[database,gateway,aws]` when you need both integrations.

The Python package does not bundle the Rust server. To run a server locally,
install the published crate separately with Cargo:

```sh
cargo install nabedra-database --version 0.1.1 --locked
nabedra-db --version
```

The published Python 0.1.0 CLI expects the previous server executable name.
To use `nabedra database serve` with `nabedra-db`, install the updated CLI
from this checkout with the [repository installer](../../../scripts/install_nabedra.py).
The Python ORM can connect to the renamed server without a CLI update.

For development, run `python3.11 -m pip install -e ./orm/python` from the repository
root instead of installing from PyPI.

Replace the token placeholder in your environment. The example below assumes
the server is already listening at `http://127.0.0.1:8000`. Use its actual base
URL if different. Start with an unused table name; creating an existing name
raises `ConflictError`.

```python
from nabedra import Database, col

db = Database(url="http://127.0.0.1:8000", mode="olap")
users = db.create_table(
    "guide_users", {"id": str, "age": int, "active": bool}, key="id"
)
users.insert({"id": "alice", "age": 24, "active": True})
inserted = users.insert_many(
    {"id": name, "age": age, "active": True}
    for name, age in [("bob", 17), ("carol", 30)]
)  # 2
alice = users.get("alice")  # complete row, or None if missing
updated = users.update("alice", {"age": 25})  # True
users.upsert({"id": "carol", "age": 31, "active": False})

adults = users.where(
    (col("age") >= 18) & ((col("active") == True) | ~(col("id") == "carol"))
)
query = adults.select("id", "age").order_by("age", desc=True).limit(100)
rows = query.collect()  # [{"id": "alice", "age": 25}]
count = query.count()  # 1, including the limit
plan = query.explain()  # textual plan from metadata, without a row scan

same_table = db.table("guide_users")  # discover by name, retaining its ID
tables = db.tables()  # handles sorted by name
deleted = users.delete("bob")  # True; a second delete returns False
```

`Database(url, token=None, *, mode="oltp", timeout=30.0, ca_file=None, database=None)` validates configuration
without sending a request. Supply an absolute HTTP or HTTPS base URL without
embedded credentials, query or fragment. The client appends `/v1/operations` to
its base path. If omitted, `token` comes from `NABEDRA_TOKEN`. Construction is
allowed without a token, but an operation raises `AuthenticationError` locally
until credentials are supplied through a new client. An explicit token must be
a nonempty safe bearer token string.

`mode` accepts `"olap"` and `"oltp"` and defaults to `"oltp"`. The read-only
`db.mode` property records this workload preference. Both modes currently use
the same server API and execution engine. `DB` remains a convenience alias for
`Database` within `nabedra`.
An optional `database` selector accompanies operations and receipt polling.
The application gateway treats it as untrusted input and authorizes access;
the selector itself grants no storage permissions.

HTTPS verifies certificates and hostnames using the system trust roots. For a
private CA, supply an additional PEM CA file:

```python
import os
from nabedra import Database

db = Database(
    url="https://nabedra.example.com",
    token=os.environ["NABEDRA_TOKEN"],
    mode="oltp",
    timeout=10.0,
    ca_file="/path/to/nabedra-ca.pem",
)
```

`ca_file` adds trust roots and does not disable verification. It defaults to
`None`. `timeout` accepts a positive finite `int` or `float`, excluding `bool`,
and defaults to 30 seconds. It applies to blocking socket I/O, not a whole
operation deadline, cancellation or a server transaction deadline. The client
follows no redirects and performs no automatic retries.

## Command line

The installed console command groups database administration under `database`:

```sh
nabedra database init --help
nabedra database serve --config nabedra-database.json
nabedra database doctor --config nabedra-database.json
nabedra database cluster --help
nabedra database migrate --help
nabedra --version
```

`python -m nabedra database <command>` uses the same command parser.

The CLI uses `Nabedra` as the persisted cluster identifier for new clusters.
To attach to a cluster created by the earlier CLI, preserve its existing name
with `nabedra database serve --config nabedra-database.json --cluster-name MeshDB`.
Use `--cluster-name` for any other existing identifier too. Names must match
`[A-Za-z_][A-Za-z0-9_]{0,63}`. This flag selects the expected durable identity;
it does not rename or migrate stored cluster records. Keep the existing cluster
ID, credentials, storage and compatible configuration when attaching.

Kubernetes provider upgrades retain infrastructure ownership. New launches use
`nabedra.io/*` labels and `nabedra-<launch>` resource names. The provider also
reads existing `meshdb.io/*` labels, lifecycle annotations and
`meshdb-<launch>` resource names so running workers remain in inventory.
Retries reconcile the original launch, and deletion still requires its recorded
Pod UID. Mixed identity namespaces and duplicate launch identities fail rather
than report an empty inventory. Keep retained Secrets until the original workers
have completed their lifecycle.

## Tables and rows

Table and column names must match `[A-Za-z_][A-Za-z0-9_]{0,63}`. A schema has
1 to 128 columns and accepts exactly these Python types:

| Python type | Stored type | Accepted values |
| --- | --- | --- |
| `str` | UTF-8 string | Exact `str` values |
| `int` | Signed int64 | Integers from `-(2**63)` through `2**63 - 1` |
| `float` | Float64 | Finite floats; no NaN or infinity |
| `bool` | Boolean | Exact `True` or `False` |

Values must have the exact declared type. A boolean is not an integer here,
an integer does not fill a float column, and subclasses are not accepted.
Columns are nonnullable. Full rows must contain every schema column, with no
unknown columns or `None` values. The primary key must name a `str` or `int`
column and must be unique. Only `format="parquet"` is supported.

Obtain `Table` handles through `Database`, rather than constructing them directly.
Each handle has immutable `id`, `name`, `key` and `format` metadata and retains
its original schema. Input mappings are copied, and each access to
`table.schema` returns a fresh dictionary. Changing that dictionary does not
change the handle or server schema. Row and query operations use the table ID.

| Method | Result and behavior |
| --- | --- |
| `db.create_table(name, schema, key, *, format="parquet")` | New `Table`; an existing name raises `ConflictError`. |
| `db.table(name)` | One discovered `Table`; a missing name raises `NotFoundError`. |
| `db.tables()` | `list[Table]` sorted by name; `[]` if none exist. |
| `table.insert(row)` | `None`; an existing key raises `ConflictError`. |
| `table.insert_many(rows)` | Inserted `int` count from one atomic request. |
| `table.get(key)` | Complete row dictionary, or `None` for a missing row. |
| `table.update(key, values)` | `bool` indicating whether the row exists; accepts partial values, including `{}`, but forbids the key field even if unchanged. |
| `table.upsert(row)` | `None`; inserts a new row or replaces the entire existing row. |
| `table.delete(key)` | `True` if deleted, `False` for a missing row. |
| `table.query()` | New unfiltered `Query`, without network I/O. |
| `table.where(expr=None, **filters)` | New filtered `Query`, without network I/O. |
| `table.drop()` | `None`; removes the table from the server catalog. |

`insert_many` accepts an iterable, including a generator. It consumes and
validates one bounded batch before sending anything, stopping if it exceeds
10,000 rows or an 8 MiB encoded request. It never splits a batch or sends a
validated prefix. The server applies the whole batch atomically; duplicate keys
within the batch or against existing rows reject the batch. An empty iterable
still sends one request to validate table existence, then returns `0`.

Missing tables raise `NotFoundError`, including through old handles after a
drop. Recreating the same name gives a new ID; existing handles and their queries
remain stale. Obtain a new handle with `db.table(name)`. Dropping a table does
not delete retained S3 snapshots.

## Queries and expressions

Build scalar comparisons with `col(name)` and `==`, `!=`, `>`, `>=`, `<` or
`<=`. Boolean columns support only `==` and `!=`. Comparisons must use a scalar
of the column's exact type; column-to-column comparisons and callbacks are
unsupported. Combine complete comparisons with `&`, `|` and `~`, and put
parentheses around each comparison. Python `and`, `or`, `not` and chained
comparisons raise `TypeError` when they attempt to convert an expression to a
boolean. Predicate nesting is limited to 32 levels, including levels introduced
by repeated `where` calls.

Queries and expressions are immutable. Builders make no network requests and
return new queries, so a base query can be reused:

```python
base = users.where(active=True)
adults = base.where(col("age") >= 18)
youngest = adults.order_by("age").limit(5)
oldest = adults.order_by("age", desc=True).limit(5)
```

Repeated `where(expr=None, **filters)` calls combine the existing predicate,
the expression and keyword equalities with AND. `where()` with no arguments
returns an equivalent query. Repeated `select`, `order_by` and `limit` calls
replace their respective settings.

| Query method | Result and behavior |
| --- | --- |
| `select(*columns)` | New query projecting nonempty, unique, existing columns. The key need not be selected. |
| `where(expr=None, **filters)` | New query with additional AND conditions. |
| `order_by(column, desc=False)` | New query ordering by one column; `desc` must be an exact `bool`. |
| `limit(n)` | New query with a nonnegative exact `int` limit; `bool` is invalid. |
| `collect()` | `list[Row]` containing exactly the selected columns, or all columns by default; `[]` for no matches. |
| `count()` | Integer count of matching rows after the explicit limit. |
| `explain()` | Textual plan using metadata only; no data scan or timing measurements. |

Each terminal call sends a fresh request. `collect` and `count` observe a fresh
committed snapshot each time; a query does not pin data across calls. The server
filters, orders, limits, then projects. Default ordering is primary-key ascending.
Explicit ordering always uses primary-key ascending to break ties, even for
descending order. `limit(0).collect()` returns `[]` and `limit(0).count()` returns
`0`. Projection does not change the count.

`collect()` may return at most 10,000 rows within an 8 MiB response. There is no
implicit limit or automatic truncation. The server returns `LimitExceededError`
for an oversized result; use an explicit smaller limit or projection. `count()`
returns a scalar and can count more than 10,000 matches. Pagination and streaming
results are not available in v0.

## Errors and uncertain writes

All client errors below derive from `NabedraError`, exported by `nabedra`.

| Exception | Meaning |
| --- | --- |
| `ValidationError` | Invalid local input or server validation failure. |
| `AuthenticationError` | Credentials are missing locally or the server rejected authentication. |
| `NotFoundError` | Table does not exist, including a stale table ID. |
| `ConflictError` | Duplicate table/key or a server commit conflict. |
| `LimitExceededError` | Local input or a valid server error exceeds a limit. |
| `OverloadedError` | Server cannot accept more work. |
| `StorageError` | Server storage failed with outcome `not_applied`. |
| `TransportError` | Read-side network failure. |
| `ProtocolError` | Malformed or invalid read response, including an oversized response. |
| `UnknownOutcomeError` | Mutation may have committed despite an unknown server outcome or a lost/invalid response. |
| `NabedraError` | Other valid server error. |

Errors expose `code`, `request_id`, `status_code` and `outcome`. Local errors may
have no request ID or HTTP status. Server errors preserve their diagnostics;
`outcome` can be `"not_applied"` or `"unknown"`, while some local/read failures
have `None`. A request UUID identifies one logical queued operation. Submitting the same UUID
and exact body attaches to that request; a different body conflicts. The client
does not automatically resubmit operations.

After `UnknownOutcomeError`, keep the original identity. A request can still
commit after its response disappears. Use its receipt or durable mutation
decision to reconcile the outcome before submitting another logical mutation.

## Queue receipts

Existing table/query methods remain synchronous. They submit through the same
worker queue as the receipt interface. Acceptance returns a queue identity and
does not mean a mutation has committed.

```python
from nabedra import Database

client = Database(url="http://127.0.0.1:8000", token="data-token", mode="olap")
receipt = client.submit("query.count", {
    "table_id": table.id,
    "query": {"select": None, "filter": None, "order_by": None, "limit": None},
})
print(receipt.run_id, receipt.request_id, receipt.state)
terminal = receipt.wait(timeout=30)
assert terminal.state == "succeeded", terminal.error
print(terminal.result)

# Reconnect through another API using the same database credentials.
receipt = client.receipt(receipt.run_id, receipt.request_id)
```

`RequestReceipt` has `request_id`, `run_id`, `state`, `result`, `error`, and the
`terminal` property. `poll()` returns a fresh receipt. `wait()` polls until a
terminal state or its deadline. A wait timeout releases the client transport and
does not cancel accepted work. To retry an uncertain submission, preserve the
original UUID and operation payload and pass `request_id=` to `submit()`.

Receipt states are `queued`, `running`, `committing`, `reconciling`, `succeeded`,
`failed`, and `abandoned`. Query results expire at the configured retention
interval and then return explicit `NotFoundError`. Durable mutation decisions
remain reconcilable after result expiration or a run change.

Every ready worker holds complete request records in RAM. An API restart or an
individual worker failure can recover accepted work from another holder. A
joining worker copies the queue before readiness. Stopping every worker abandons
outstanding requests. An explicit `--cold-start` fences prior attempts and starts
a new run without reconstructing pending requests from storage. Committed rows
and mutation decisions remain durable. A one-worker deployment has one queue
copy and cannot survive losing that process.

## Limits and scope

Requests and responses are capped at 8 MiB of encoded JSON, including envelopes.
Batches and collected results are capped at 10,000 rows, schemas at 128 columns,
and OLAP tables at 100,000 rows and 64 MiB of decoded row payload. OLTP does
not apply these logical-table limits. Byte limits can be reached before row
limits for bounded requests and results. The server remains authoritative for
stored data and commit validation. `count()` accepts nonnegative counts above
100,000 and still enforces the query's explicit limit. Older ORM releases reject
larger counts and need an upgrade.

The table/query methods remain synchronous; receipt submission and polling let
clients release their connections while work runs. Keyed operations route to one
primary-key-range partition, and changed mutations rewrite only affected files.
Queries scan partitions on persistent local OS worker threads and combine
results with global ordering, ascending-primary-key ties, limit, projection and
count semantics. Worker count is independent of partition count and can change
on restart. A fenced worker scheduler shares compute and memory across requests.
Mutation preparation produces private immutable files, then one conditional
catalog update publishes the full operation and its durable decision.

Partitions do not multiply the limits above. New closed Parquet files, including
metadata, have a 64 MiB hard cap of 67,108,864 bytes. A singleton may exceed a
smaller configured target but never the cap; an impossible file fails the whole
mutation. Empty tables have no files. A new target affects new or rewritten
ranges without repacking unchanged files. Version 1 remains readable; version 2
can mix Legacy and Partitioned tables. Only an actual data change migrates a
legacy table. The shared request runtime uses catalog format 4. Upgrading an
existing format 1/2/3 catalog requires `--migrate-catalog` after stopping every
old writer. Old binaries must remain stopped after migration.
Historical objects remain in S3 with no garbage collection.

The API provides no general transactions across calls, joins, import/export,
schema migrations, model decorators, relationships, indexes, or
data-frame adapters. The atomic batch remains a single operation.

See the [server guide](https://github.com/nabedra/database/blob/main/server/docs/reference.md#configuration)
for size syntax and worker configuration, and the
[execution and recovery plan](https://github.com/nabedra/database/blob/main/plans/resource-aware-scheduling.md)
for storage and worker requirements.

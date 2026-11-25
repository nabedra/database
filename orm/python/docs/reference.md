# Nabedra Database Python ORM

Nabedra Database's synchronous Python client creates tables, reads and writes rows, and
builds queries for a separately running Nabedra Database v0 server. It requires Python
3.11+ and has no runtime dependencies beyond the standard library. The client
does not start the server or need AWS credentials; the server owns S3 access.

For shared compute, point this client at your application's endpoint. The
[framework-independent gateway](https://github.com/zPy52/nabedra-database/blob/main/server/docs/cli-and-serverless.md#host-an-application-endpoint)
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
1 to 128 columns and accepts exactly these Python types, plus the
[vector and list types](#vector-search):

| Python type | Stored type | Accepted values |
| --- | --- | --- |
| `str` | UTF-8 string | Exact `str` values |
| `int` | Signed int64 | Integers from `-(2**63)` through `2**63 - 1` |
| `float` | Float64 | Finite floats; no NaN or infinity |
| `bool` | Boolean | Exact `True` or `False` |

Values must have the exact declared type. A boolean is not an integer here,
an integer does not fill a float column, and subclasses are not accepted.
Columns are nonnullable, except vector columns. Full rows must contain every
schema column, with no unknown columns or other `None` values. The primary key must name a `str` or `int`
column and must be unique. Only `format="parquet"` is supported.

Obtain `Table` handles through `Database`, rather than constructing them directly.
Each handle has immutable `id`, `name`, `key` and `format` metadata and retains
its schema. Only its own `add_column`, `drop_column`, `add_full_text` and
`drop_full_text` calls change that schema. Input mappings are copied, and each access to
`table.schema` returns a fresh dictionary. Changing that dictionary does not
change the handle or server schema. Row and query operations use the table ID.

| Method | Result and behavior |
| --- | --- |
| `db.create_table(name, schema, key, *, format="parquet", partition_by=None, full_text=None, embed=None)` | New `Table`; an existing name raises `ConflictError`. |
| `db.table(name)` | One discovered `Table`; a missing name raises `NotFoundError`. |
| `db.tables()` | `list[Table]` sorted by name; `[]` if none exist. |
| `table.insert(row)` | `None`; an existing key raises `ConflictError`. |
| `table.insert_many(rows)` | Inserted `int` count from one atomic request. |
| `table.get(key)` | Complete row dictionary, or `None` for a missing row. |
| `table.update(key, values)` | `bool` indicating whether the row exists; accepts partial values, including `{}`, but forbids the key field even if unchanged. |
| `table.upsert(row)` | `None`; inserts a new row or replaces the entire existing row. |
| `table.delete(key)` | `True` if deleted, `False` for a missing row. |
| `table.upsert_many(rows, *, wait_for_embedding=False)` | Applied `int` count. See [batches](#batches). |
| `table.delete_many(keys)` | Applied `int` count. A missing key counts as applied. |
| `table.add_column(name, type, *, embed=None)` | `None`; adds a vector column online. |
| `table.drop_column(name)` | `None`; drops a vector column. |
| `table.add_full_text(column, language)` | `None`; indexes a `str` column for `match`. |
| `table.drop_full_text(column)` | `None`; stops indexing it. |
| `table.search_status()` | Dictionary per search column: `state` (`"building"`, `"ready"` or `"failed"`), `lag_ms`, `pending_embeddings`, `failed_embeddings` and `recall`. `lag_ms` and `recall` can be `None`. |
| `table.query()` | New unfiltered `Query`, without network I/O. |
| `table.where(expr=None, **filters)` | New filtered `Query`, without network I/O. |
| `table.drop()` | `None`; removes the table from the server catalog. |

`insert_many` accepts an iterable, including a generator. It consumes and
validates one bounded batch before sending anything, stopping if it exceeds
10,000 rows or an 8 MiB encoded request. It never splits a batch or sends a
validated prefix. The server applies the whole batch atomically; duplicate keys
within the batch or against existing rows reject the batch. An empty iterable
still sends one request to validate table existence, then returns `0`.

### Batches

`upsert_many` and `delete_many` accept any iterable and send it in requests of
at most 10,000 rows and 8 MiB. Each row is atomic and durable. The call is not
atomic across rows. A key that appears twice in one request is a
`ValidationError`; in different requests the later row wins. An empty iterable
sends nothing and returns `0`.

The call stops at the first request that fails and raises `BatchError`:

| Attribute | Value |
| --- | --- |
| `applied` | Rows known to be applied, in the whole call |
| `outcomes` | One dictionary per row of the failed request, in order, with `key`, `outcome` and `error`. `outcome` is `"applied"`, `"not_applied"` or `"outcome_unknown"`. `error` is `None` for an applied row. |
| `request_id` | ID of the failed request |
| `outcome` | `"not_applied"`, `"partial"`, `"unknown"`, or `"applied"` when only the wait for embeddings failed |

A lost response makes every row of its request `"outcome_unknown"`. The client
never sends a request again. Writing the same rows again is safe while no other
writer changes them. Rows after the failed request were not sent. An invalid row
raises `ValidationError` if nothing was sent yet. After that, it or an exception
from the iterable raises `BatchError` with the applied count and no `outcomes`.

`wait_for_embedding=True` polls `search_status()` after the last request until no
embedding of the table is pending, for at most the client's `timeout`. Other
writers' rows count too. If the wait fails, `BatchError` reports every row as
applied. Failed embeddings end the wait; see `failed_embeddings`.

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

## Vector search

Search needs an OLTP server that supports it. Other servers reject these types and
operations with code `unsupported_operation` or `validation_error`. The application
gateway does not forward them.

```python
from nabedra import Database, Vector, col

db = Database(url="http://127.0.0.1:8000")
chunks = db.create_table("chunks", {
    "id": str, "tenant": str, "acl": list[str], "body": str,
    "embedding": Vector(1024),
    "sparse": Vector(sparse=True),
    "tokens": list[Vector(128)],
}, key="id", partition_by="tenant", full_text={"body": "english"},
   embed={"embedding": {"source": "body", "model": "docs-v1"}})

mine = chunks.where((col("tenant") == "acme") & col("acl").contains_any(["staff"]))
hits = mine.rank(col("embedding").cosine("reset my password")).select("id", "body").limit(10).collect()
# [{"id": "doc7#0", "body": "...", "_score": 0.88}]
```

| Column type | Values | Operations |
| --- | --- | --- |
| `Vector(n)` | `n` float32 values, as a list, a tuple or any contiguous float32 buffer such as `array("f")` or a NumPy array. Read as `array("f")`. | `cosine`, `dot`, `l2` |
| `Vector(sparse=True)` | `{index: weight}` with unsigned 32-bit indices. Read as a dictionary in index order. | `cosine`, `dot` |
| `list[Vector(n)]` | A list or tuple of up to 1,024 vectors, for example `list(matrix)`. Read as a list of `array("f")`. | `cosine`, `dot`: for every query vector, its best row vector, summed |
| `list[str]` | A list of strings | `contains`, `contains_any` filters |
| `str` in `full_text` | Text | `match` (BM25) |

Vector values are stored as float32 and must be finite. Vector columns accept
`None`, and those rows never rank on that column. Scalar comparisons and
`order_by` do not accept these types.

| Table option | Meaning |
| --- | --- |
| `partition_by` | A `str` or `int` column. Each of its values gets its own index. |
| `full_text` | `{column: language}` for `str` columns |
| `embed` | `{column: {"source": column, "model": name}}`. The database fills this `Vector(n)` column from a `str` column. Rows omit it, and writing it is a `ValidationError`. |

A table has at most 8 search columns, counting vector and full-text columns. With
any, `_score` is a reserved column name and a row holds at most 1 MiB, counting
vectors as raw float32 bytes.

| Query method | Result and behavior |
| --- | --- |
| `col(name).cosine(query)`, `.dot(query)`, `.l2(query)` | A ranking. A string is embedded with the column's model. `cosine` rejects a zero query vector. |
| `col(name).match(text)` | A ranking by BM25 |
| `col(name).contains(value)`, `.contains_any(values)` | Filters on `list[str]` columns. `contains_any` of nothing matches nothing. |
| `col(name).starts_with(prefix)` | Filter on `str` columns |
| `rank(*rankings, fusion="rrf", candidates=100, recall=0.95, exact=False, consistency="latest")` | New query ordered by one ranking, or by several fused. `fusion` is `"rrf"` or one nonnegative weight per ranking. |
| `rerank(ranking, *, candidates=200)` | New query that scores the best candidates of `rank` again |
| `rank_many(rankings, *, limit, ...)` | One result list per ranking, up to 256, with the options of `rank` |

A ranked query needs a `limit` of at most 1,000 and supports only `collect`. Its rows
gain `_score` and omit vector columns unless selected. `l2` sorts ascending, the
others descending, then by key. `order_by` cannot be combined with `rank`. `recall`
is a target, not a guarantee; `exact=True` is exact. `"latest"` shows every write
acknowledged before the query, and `"snapshot"` reads one cut across ranges.

An operation that a column does not support is a `ValidationError` before any
request. `SearchUnavailableError` (503) means that search cannot answer now.
The query can be sent again later. Streams support only scalar columns.

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
| `SearchUnavailableError` | Search cannot answer now. The query can be sent again later. |
| `BatchError` | `upsert_many` or `delete_many` stopped early. See [batches](#batches). |
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
Requests and collected results are capped at 10,000 rows, schemas at 128 columns,
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

See the [server guide](https://github.com/zPy52/nabedra-database/blob/main/server/docs/reference.md#configuration)
for size syntax and worker configuration, and the
[execution and recovery plan](https://github.com/zPy52/nabedra-database/blob/main/plans/resource-aware-scheduling.md)
for storage and worker requirements.

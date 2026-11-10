Build a pure Python ORM that exposes MeshDB tables and composable queries through the single Rust server's HTTP(S) API.

Keep the `DB`, `Table`, `Query`, `Expr` and `col` approach from [data.md](../plan/data.md). [server.md](server.md) defines v0's wire protocol, supported operations, types, storage and limits. The ORM builds requests and converts results; the Rust process executes operations and owns S3 access.

---

## Package and public interface

Create `orm/pyproject.toml` and an installable `orm/src/meshdb/` package for Python 3.11+. Put transport in `client.py`, table handles in `table.py`, query builders in `query.py`, predicates in `expr.py` and exceptions in `errors.py`; export the public API from `__init__.py`. Install with `python -m pip install -e ./orm`.

Use the Python standard library at runtime. HTTP uses [`urllib.request`](https://docs.python.org/3.11/library/urllib.request.html), JSON uses `json`, and HTTPS uses [`ssl.create_default_context`](https://docs.python.org/3.11/library/ssl.html#ssl.create_default_context). The package needs no Rust extension, local server subprocess, AWS SDK or S3 credentials.

These are interface specifications for the implementation:

```python
from __future__ import annotations

from collections.abc import Iterable
from typing import Literal, TypeAlias

Scalar: TypeAlias = str | int | float | bool
Key: TypeAlias = str | int
Row: TypeAlias = dict[str, Scalar]
Schema: TypeAlias = dict[str, type]

class DB:
    def __init__(self, api_url: str, token: str, *, timeout: float = 30.0,
                 ca_file: str | None = None): ...
    def create_table(self, name: str, schema: Schema, key: str,
                     *, format: Literal["parquet"] = "parquet") -> Table: ...
    def table(self, name: str) -> Table: ...
    def tables(self) -> list[Table]: ...

class Table:
    id: str
    name: str
    schema: Schema
    key: str
    format: Literal["parquet"]

    def insert(self, row: Row) -> None: ...
    def insert_many(self, rows: Iterable[Row]) -> int: ...
    def get(self, key: Key) -> Row | None: ...
    def update(self, key: Key, values: Row) -> bool: ...
    def upsert(self, row: Row) -> None: ...
    def delete(self, key: Key) -> bool: ...
    def query(self) -> Query: ...
    def where(self, expr: Expr | None = None, **filters: Scalar) -> Query: ...
    def drop(self) -> None: ...

class Query:
    def select(self, *columns: str) -> Query: ...
    def where(self, expr: Expr | None = None, **filters: Scalar) -> Query: ...
    def order_by(self, column: str, desc: bool = False) -> Query: ...
    def limit(self, n: int) -> Query: ...
    def collect(self) -> list[Row]: ...
    def count(self) -> int: ...
    def explain(self) -> str: ...

class Expr:
    def __and__(self, other: Expr) -> Expr: ...
    def __or__(self, other: Expr) -> Expr: ...
    def __invert__(self) -> Expr: ...
    def __eq__(self, value: Scalar) -> Expr: ...
    def __ne__(self, value: Scalar) -> Expr: ...
    def __gt__(self, value: Scalar) -> Expr: ...
    def __ge__(self, value: Scalar) -> Expr: ...
    def __lt__(self, value: Scalar) -> Expr: ...
    def __le__(self, value: Scalar) -> Expr: ...
    def __bool__(self) -> bool: ...

def col(name: str) -> Expr: ...
```

`DB.__init__` validates configuration without making a request. `create_table`, `table` and `tables` return handles from server descriptors and translate wire schemas back to Python types. A handle retains its immutable table ID and schema; expose schema copies so caller mutations cannot change validation. All row/query operations send that ID.

## Tables and validation

- Convert schema types explicitly: `{str: "utf8", int: "int64", float: "float64", bool: "bool"}`. Reject unsupported types and formats before transport. Apply the server's identifier, column-count, primary-key and row rules locally; the server remains authoritative.
- Validate exact types rather than `isinstance(value, int)`, because Python booleans are integers under `isinstance`. Check integer range, finite floats, required columns, unknown columns and nulls. Inserts/upserts require full rows; updates accept partial rows and cannot alter the key.
- `insert` raises `ConflictError` for an existing key. `upsert` replaces the complete row. `update`/`delete` return `False` for a missing row, and `get` returns `None`. Missing tables raise `NotFoundError`.
- Consume an `insert_many` iterable into one bounded batch. Stop and raise when it exceeds 10,000 rows or the encoded request exceeds 8 MiB. Send one request and return the inserted count. Never silently split a batch, because its atomicity belongs to one server commit. An empty batch returns `0` after the server validates the table.
- `drop` removes the catalog entry through the server. Old handles then fail by ID even if another caller recreates the same name. No ORM call accesses or deletes S3 objects directly.

---

## Query construction

Store expressions and queries as immutable structured values. Every builder returns a new query. `Table.where(...)` delegates to `Table.query().where(...)`; builders perform no network I/O. Only `collect`, `count` and `explain` execute a query, and each execution observes a fresh committed table snapshot.

Repeated `where` calls combine predicates with `and`. Keyword filters become equality predicates, so `where(active=True)` equals `where(col("active") == True)`. Combine an explicit expression and keyword filters with `and`. An empty `where()` returns an equivalent query. Repeated `select`, `order_by` and `limit` replace their respective settings; `select()` with no columns is invalid.

Use `&`, `|` and `~` for logical composition, with parenthesized comparisons. `Expr.__bool__` raises `TypeError` explaining that Python `and`, `or`, `not` and chained comparisons cannot construct query expressions. Reject comparisons to another expression and arbitrary callables.

```python
adults = users.where((col("age") >= 18) & (col("active") == True))
query = adults.select("id", "age").order_by("age", desc=True).limit(100)
```

Serialize that query as the `query` field of a `query.collect`, `query.count` or `query.explain` payload:

```json
{
  "select": ["id", "age"],
  "filter": {
    "op": "and",
    "args": [
      {"op": "ge", "column": "age", "value": 18},
      {"op": "eq", "column": "active", "value": true}
    ]
  },
  "order_by": {"column": "age", "desc": true},
  "limit": 100
}
```

Default `select`, `filter`, `order_by` and `limit` to `null`. Validate referenced columns and predicate types against the handle schema. Use only the operators in [server.md](server.md#schema-and-query-rules), with nesting limited to 32. Emit JSON data, never SQL, Python source, pickles or serialized callbacks.

The server filters, sorts, limits and then projects. Ordering defaults to primary-key ascending, with the same tie-breaker for explicit ordering. `count()` includes the limit; `limit(0).collect()` returns `[]`. `collect()` receives at most 10,000 rows and 8 MiB; an oversized result raises `LimitExceededError` instead of silently returning a partial result. Pagination and `batches()` remain later work.

## Transport and errors

- Accept an absolute `http://` or `https://` base URL without credentials, a query or a fragment. Append `/v1/operations` to its normalized path. Require a nonempty token and positive finite timeout. Construct a private opener; do not change global `urllib` settings or follow redirects.
- Send UTF-8 JSON with `Content-Type: application/json`, `Accept: application/json` and the bearer header. Generate a fresh `uuid.uuid4()` request ID for each operation. Reject NaN/infinity with `allow_nan=False` and cap request/response reads at 8 MiB.
- Verify HTTPS certificates and hostnames with `ssl.create_default_context()`. If supplied, add `ca_file` with `SSLContext.load_verify_locations(cafile=ca_file)`. The timeout applies to blocking socket I/O; it does not cancel server execution or impose a server transaction deadline.
- Validate status, JSON envelope and operation-specific result shape. Require a matching echoed request ID on success and whenever an error supplies one; allow `null` for errors before body parsing, retaining the client ID for diagnostics. Decode schemas/rows through explicit type mappings and close response objects on success and failure.
- Make no automatic retries in v0. Request IDs are diagnostic and do not imply idempotency. A mutation that loses its response may have committed; expose that ambiguity to the caller.

All exceptions derive from `MeshDBError`, with `code`, `request_id`, `status_code` and `outcome` attributes. Local errors may have no request ID/status. Use these mappings:

| Condition | Exception |
| --- | --- |
| Invalid local input or `validation_error` | `ValidationError` |
| `unauthorized` | `AuthenticationError` |
| `not_found` | `NotFoundError` |
| `conflict` | `ConflictError` |
| `limit_exceeded` | `LimitExceededError` |
| `overloaded` | `OverloadedError` |
| `storage_unavailable` with `outcome: "not_applied"` | `StorageError` |
| Read-side network failure | `TransportError` |
| Malformed read response | `ProtocolError` |
| Server `outcome: "unknown"`, or a mutation with a lost/invalid response | `UnknownOutcomeError` |
| Other valid server error | `MeshDBError` |

Treat an uncertain mutation conservatively, including failures while reading a response body. `UnknownOutcomeError` carries the request ID for diagnostics; callers must inspect current data before deciding whether to submit another write. It does not mean rollback, and v0 has no request-outcome lookup API.

---

## Minimal usage

Run the server described in [server.md](server.md), then install the ORM and use the same token:

```python
import os
from meshdb import DB, col

db = DB(api_url="http://127.0.0.1:8000", token=os.environ["MESHDB_TOKEN"])
users = db.create_table(
    "users", {"id": str, "age": int, "active": bool}, key="id"
)
users.insert({"id": "alice", "age": 24, "active": True})
assert users.insert_many([{"id": "bob", "age": 17, "active": True}]) == 1
assert users.update("alice", {"age": 25}) is True

adults = users.where((col("age") >= 18) & (col("active") == True))
assert adults.select("id", "age").collect() == [{"id": "alice", "age": 25}]
assert adults.count() == 1
assert users.delete("bob") is True

# After restarting the server, reconnect using the same S3 bucket/prefix.
db = DB(api_url="http://127.0.0.1:8000", token=os.environ["MESHDB_TOKEN"])
assert db.table("users").get("alice") == {"id": "alice", "age": 25, "active": True}
```

## Implementation and acceptance

Build schema/predicate serialization and the immutable builders, then transport/error mapping, then table methods and exports. Use standard-library `unittest` with a local HTTP fixture to verify exact payloads, query independence, validation, atomic batch submission, redirect rejection, bounded reads and uncertain write outcomes.

Run the example against the Rust server and real S3 after credentials are supplied. Verify persistence after an actual server restart, HTTPS verification with a trusted test CA, stale handles after drop/recreate and concurrent clients without lost writes. Exercise server errors through the public ORM methods and verify their exception mappings.

Keep v0 synchronous. General transactions, model decorators, migrations, relationships, joins, aggregations, indexes, async APIs, pandas/Arrow/PyTorch adapters, streaming and cloud-management helpers remain outside this package's initial implementation.

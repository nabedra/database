# Nabedra Python ORM

The synchronous Python client creates tables, reads and writes rows, and builds
queries for a running Nabedra Database server. It requires Python 3.11+ and uses
only the standard library at runtime. The package also includes the `nabedra`
CLI and optional application gateway integrations.

## Get started

Start a database with the [server guide](https://github.com/zPy52/nabedra-database/blob/main/server/README.md), then install
the client in a virtual environment:

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install "nabedra[database]"
export NABEDRA_TOKEN='<same-token-configured-on-your-server>'
```

Run this example with that environment's Python. It uses a fresh table name
on each run and retains the table:

```python
import uuid
from nabedra import Database, col

db = Database(url="http://127.0.0.1:8000", mode="oltp")
users = db.create_table(
    "users_" + uuid.uuid4().hex,
    {"id": str, "age": int},
    key="id",
)
users.insert_many([
    {"id": "alice", "age": 24},
    {"id": "bob", "age": 17},
])
adults = users.where(col("age") >= 18).select("id", "age").collect()
assert adults == [{"id": "alice", "age": 24}]
```

The client reads `NABEDRA_TOKEN` unless you pass `token=` explicitly. AWS
credentials belong on the server. For a remote server, use HTTPS; the client
verifies certificates and hostnames and accepts `ca_file=` for a private CA.

`mode` accepts `"oltp"` and `"olap"`; both currently use the same engine.
The client does not retry operations automatically. Preserve the request
identity and follow the [uncertain-write guidance](https://github.com/zPy52/nabedra-database/blob/main/orm/python/docs/reference.md#errors-and-uncertain-writes)
if a write loses its response.

## Develop from this checkout

Run from the repository root:

```sh
python3.11 -m venv orm/python/.venv
orm/python/.venv/bin/python -m pip install -e './orm/python[gateway]'
orm/python/.venv/bin/python -m unittest discover -s orm/python/tests -v
```

The `gateway` extra installs HTTPX for application gateway tests. Normal ORM use
needs no extra dependencies. The Python package does not bundle the Rust server;
the [repository installer](https://github.com/zPy52/nabedra-database/blob/main/scripts/install_nabedra.py) installs both.

## More documentation

- [OLAP query streaming and PyTorch](https://github.com/zPy52/nabedra-database/blob/main/orm/python/docs/streaming.md)
- [Python API, CLI, receipts and limits](https://github.com/zPy52/nabedra-database/blob/main/orm/python/docs/reference.md)
- [Task tracker tutorial](https://github.com/zPy52/nabedra-database/blob/main/server/TUTORIAL.md)
- [Application gateway setup](https://github.com/zPy52/nabedra-database/blob/main/server/docs/cli-and-serverless.md#host-an-application-endpoint)
- [Contribution guide](https://github.com/zPy52/nabedra-database/blob/main/CONTRIBUTION.md)

## Cache

Use a synchronous context manager with a Nabedra Cache endpoint. The client
opens a connection pool and releases it when the context exits.

```python
from nabedra.cache import Cache

with Cache.connect("rediss://cache.example.com:6379", username=user,
                   password=secret, namespace="catalog", ca_file="/path/to/ca.pem") as cache:
    cache.set("sku:42", b"in stock", ttl_ms=30_000, when="absent")
    value = cache.get("sku:42")
```

`Cache` is also exported from `nabedra`. Call `delete`, `expire`, `persist`,
`ttl_ms` and `increment` on the same handle. `get` returns exact bytes or
`None`. Use `cluster=True` with bootstrap endpoints and `allowed_nodes` or
`allowed_node_suffix` when node names differ from the seeds. The
[shared contract](https://github.com/zPy52/nabedra-database/tree/main/orm/cache-contract)
describes deadlines, routing and errors.

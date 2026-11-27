# Nabedra Python ORM

This is the Python client for a running Nabedra Database server. It creates
tables, reads and writes rows, and builds queries. The client is synchronous,
needs Python 3.11 or newer, and uses only the standard library at runtime. The
package also ships the `nabedra` CLI and optional application gateway
integrations.

## Get started

First start a database with the
[server guide](https://github.com/nabedra/database/blob/main/server/README.md).
Then install the client in a virtual environment and export the token you
gave the server:

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install "nabedra[database]"
export NABEDRA_TOKEN='<same-token-configured-on-your-server>'
```

Run this with the virtual environment's Python. It creates a table with a new
random name each time, and it doesn't delete the table afterwards:

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

The client reads the token from `NABEDRA_TOKEN` unless you pass `token=`. It
never needs AWS credentials, because those stay on the server. Use HTTPS for
any server that isn't on your machine. The client checks the certificate and
hostname, and `ca_file=` lets you trust a private CA.

`mode` takes `"oltp"` or `"olap"`. For now it only records which workload you
expect, since both modes use the same server API.

The client never retries an operation on its own. If a write loses its
response, you can't tell whether it was applied. Keep its request identity and
follow the
[guidance on uncertain writes](https://github.com/nabedra/database/blob/main/orm/python/docs/reference.md#errors-and-uncertain-writes).

## Cache

`Cache` connects to a Nabedra Cache server. Use it as a context manager: it
opens a connection pool on entry and closes it on exit.

```python
from nabedra.cache import Cache

with Cache.connect("rediss://cache.example.com:6379", username=user,
                   password=secret, namespace="catalog", ca_file="/path/to/ca.pem") as cache:
    cache.set("sku:42", b"in stock", ttl_ms=30_000, when="absent")
    value = cache.get("sku:42")
```

You can also import `Cache` from `nabedra`. The same handle has `delete`,
`expire`, `persist`, `ttl_ms` and `increment`. `get` returns the stored bytes
unchanged, or `None` when the key is missing.

For a cluster, pass `cluster=True` and a list of seed endpoints. If the cluster
nodes have different hostnames from the seeds, list them in `allowed_nodes` or
give their common DNS suffix in `allowed_node_suffix`. The
[shared Cache contract](https://github.com/nabedra/database/tree/main/orm/cache-contract)
describes deadlines, routing and errors.

## Develop from this checkout

From the repository root:

```sh
python3.11 -m venv orm/python/.venv
orm/python/.venv/bin/python -m pip install -e './orm/python[gateway]'
orm/python/.venv/bin/python -m unittest discover -s orm/python/tests -v
```

The `gateway` extra installs HTTPX, which the application gateway tests need.
Plain ORM use needs no extras. The package doesn't include the Rust server.
To install both, use the
[repository installer](https://github.com/nabedra/database/blob/main/scripts/install_nabedra.py).

## More documentation

- [OLAP query streaming and PyTorch](https://github.com/nabedra/database/blob/main/orm/python/docs/streaming.md)
- [Python API, CLI, receipts and limits](https://github.com/nabedra/database/blob/main/orm/python/docs/reference.md)
- [Task tracker tutorial](https://github.com/nabedra/database/blob/main/server/TUTORIAL.md)
- [Application gateway setup](https://github.com/nabedra/database/blob/main/server/docs/cli-and-serverless.md#host-an-application-endpoint)
- [Contribution guide](https://github.com/nabedra/database/blob/main/CONTRIBUTING.md)

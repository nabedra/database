# Cache ORM contract

Both clients use RESP2 over a pool of persistent connections. Each connection
has one request in flight. An operation uses one monotonic timeout for
connection setup, authentication, command exchange and any redirects. Pool
saturation fails before sending the command. The default pool size is 16 and
the default timeout is 2,000 ms.
Closing a client closes established sockets and sockets still opening or
authenticating. A pending DNS lookup can finish in the background and the
closed client discards its result.

Use `redis://` for a loopback IP or `localhost`. Use `rediss://` everywhere
else. TLS verifies the endpoint name against the platform roots or the
configured CA file. A connection gets credentials only for a configured
endpoint or an authorized node. Standalone mode takes one endpoint. Cluster
mode takes up to 32 seeds and accepts discovered primaries from the seed list,
the explicit node list, or subdomains of the configured private DNS suffix on
the same ports as the seeds. Suffix-based authorization requires TLS.

The cluster client fetches `CLUSTER SLOTS` on connection and can refresh from
its seeds following a transport failure. Normal commands go straight to the
cached primary. A `MOVED` response for the requested slot updates that slot
and retries against the validated owner. Four redirects are allowed within
the original deadline. Command bodies that reached a connection are never
replayed on transport failures.

| Condition | Error type | Code | Outcome |
| --- | --- | --- | --- |
| Missing or expired key | No error | | |
| Invalid argument, `ERR`, `CROSSSLOT` | `ValidationError` | `validation_error` | `not_applied` |
| `NOAUTH`, `WRONGPASS`, `NOPERM` | `AuthenticationError` | `unauthorized` | `not_applied` |
| Full client pool, `BUSY`, `OOM`, `TRYAGAIN` | `OverloadedError` | `overloaded` | `not_applied` |
| `CLUSTERDOWN`, redirect limit | `StorageError` | `storage_unavailable` | `not_applied` |
| Read transport failure or transport failure before sending a write | `TransportError` | `transport_error` | `not_applied` |
| Malformed read reply or invalid discovery or redirection | `ProtocolError` | `protocol_error` | |
| Write may have been sent and response is lost or unrecognized | `UnknownOutcomeError` | `outcome_unknown` | `unknown` |

A binary empty value is a hit. The clients enforce 1 MiB maximum values, valid UTF-8
keys of 1–250 bytes, namespaces of 1–64 characters in `[a-z0-9_-]`, and
safe-integer millisecond expiry inputs. The counter accepts signed 64-bit
integers, represented by Python `int` and TypeScript `bigint`. See
[vectors.json](vectors.json) for the wire key encoding and shared cases.

## Check both installed packages

Run from the repository root with Python ≥3.11, Node ≥22.13 and OpenSSL.

```sh
orm/cache-contract/run.sh
```

The runner installs the wheel and npm tarball into temporary directories.
Its fault peer verifies authentication, TLS hostname checks, pool bounds,
strict error mappings, direct routing after `MOVED`, untrusted redirects,
deadlines, closing during TLS and authentication, and one execution of a
mutation whose reply is lost or malformed.

Give the runner a disposable endpoint for K01 server-backed tests. It sets and
deletes keys in the `catalog` namespace.

```sh
NABEDRA_CACHE_TEST_CONFIG='{"endpoint":"redis://127.0.0.1:6379"}' \
  orm/cache-contract/run.sh
```

Use the same command with the Nabedra and pinned Valkey endpoints. For an
authenticated or clustered endpoint the JSON also accepts `username`,
`password`, `caFile`, `cluster`, `allowedNodes` and `allowedNodeSuffix`.

## Stock driver spike

The RESP2 adapters use Python sockets and Node TCP/TLS. This gives both clients
control of the operation-wide deadline and the boundary after which a lost
response is an unknown write outcome. In the pinned redis-py 6.4.0 cluster
client, a connection or timeout error after command submission can trigger
topology initialization before the error is rethrown. A zero command-retry
setting alone leaves that recovery path active. Node-redis 5.8.2 can decode
RESP2 integers losslessly with explicit string mapping. Its cluster wrapper
runs rediscovery inside the redirection loop. The shared clients enforce their
endpoint authorization before connecting to new owners, bound discovery inside
the same caller deadline and distinguish pre-submit failures from faults after
submission. [redis-py 6.4.0 cluster source](https://github.com/redis/redis-py/blob/v6.4.0/redis/cluster.py) · [node-redis 5.8.2 decoder and cluster loop](https://github.com/redis/node-redis/tree/redis%405.8.2/packages/client/lib)

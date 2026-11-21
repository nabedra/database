# Cache ORM contract

The Python and TypeScript Cache clients must behave the same way. This page
lists the rules they share, and [vectors.json](vectors.json) holds the test
cases both of them run.

## Connections and deadlines

Both clients speak RESP2 over a pool of persistent connections, with one
request in flight per connection. The pool holds 16 connections by default.

Each operation gets a single monotonic deadline, 2,000 ms by default, which
covers connecting, authenticating, exchanging the command and following any
redirects. If the pool is full, the operation fails before the client sends
anything.

Closing a client closes every socket, including ones that are still connecting
or authenticating. A DNS lookup that is already running may finish in the
background, but the closed client throws its result away.

## Endpoints and TLS

Use `redis://` only for a loopback IP or `localhost`, and `rediss://` for
everything else. With TLS, the client checks the endpoint's name against the
platform's root certificates or the CA file you configure.

The client sends credentials only to an endpoint you configured or a node it
has authorized. In standalone mode you give it one endpoint. In cluster mode
you give it up to 32 seeds. A primary it discovers is accepted only if it is in
the seed list, in the explicit node list, or under the configured private DNS
suffix on the same port as a seed. Authorizing nodes by DNS suffix requires
TLS.

## Cluster routing

When it connects, the cluster client fetches `CLUSTER SLOTS`. After a
transport failure it can refresh that map from its seeds. Commands go straight
to the cached primary for their slot.

If a node replies `MOVED` for the requested slot, the client updates that slot
and retries against the new owner, once it has checked that the owner is
authorized. It follows at most four redirects, all within the original
deadline. If a command's bytes reached a connection and the transport then
failed, the client never sends that command again.

## Errors

| Condition | Error type | Code | Outcome |
| --- | --- | --- | --- |
| Missing or expired key | No error | | |
| Invalid argument, `ERR`, `CROSSSLOT` | `ValidationError` | `validation_error` | `not_applied` |
| `NOAUTH`, `WRONGPASS`, `NOPERM` | `AuthenticationError` | `unauthorized` | `not_applied` |
| Full client pool, `BUSY`, `OOM`, `TRYAGAIN` | `OverloadedError` | `overloaded` | `not_applied` |
| `CLUSTERDOWN`, redirect limit | `StorageError` | `storage_unavailable` | `not_applied` |
| Transport failure on a read, or before a write was sent | `TransportError` | `transport_error` | `not_applied` |
| Malformed read reply, or invalid discovery or redirect | `ProtocolError` | `protocol_error` | |
| A write may have been sent, and its reply was lost or unrecognized | `UnknownOutcomeError` | `outcome_unknown` | `unknown` |

## Limits

An empty binary value counts as a hit. Both clients enforce these limits:

- Values are at most 1 MiB.
- Keys are valid UTF-8, from 1 to 250 bytes long.
- Namespaces are 1 to 64 characters from `[a-z0-9_-]`.
- Expiry times in milliseconds are safe integers.
- Counters are signed 64-bit integers, which are `int` in Python and `bigint`
  in TypeScript.

The wire encoding of keys is in [vectors.json](vectors.json).

## Check both installed packages

Run this from the repository root. It needs Python 3.11 or newer, Node 22.13 or
newer, and OpenSSL.

```sh
orm/cache-contract/run.sh
```

The runner installs the wheel and the npm tarball into temporary directories
and runs them against a fault-injecting peer. That peer checks authentication,
TLS hostname checks, pool limits, the error mappings above, direct routing
after `MOVED`, rejection of untrusted redirects, deadlines, closing a client in
the middle of TLS or authentication, and that a mutation whose reply was lost
or malformed runs exactly once.

To also run the K01 tests against a real server, give the runner a disposable
endpoint. These tests set and delete keys in the `catalog` namespace.

```sh
NABEDRA_CACHE_TEST_CONFIG='{"endpoint":"redis://127.0.0.1:6379"}' \
  orm/cache-contract/run.sh
```

Run it once against a Nabedra endpoint and once against the pinned Valkey
version. For an endpoint with authentication or clustering, the JSON also
accepts `username`, `password`, `caFile`, `cluster`, `allowedNodes` and
`allowedNodeSuffix`.

## Why not use redis-py or node-redis

Both clients implement RESP2 directly, on Python sockets and Node's TCP and TLS
modules. That way they control the deadline for the whole operation and know
exactly when a lost reply makes a write's outcome unknown. We tried the stock
drivers first, and neither gave us that control.

- In the cluster client of redis-py 6.4.0, a connection or timeout error
  after a command has been sent can start topology initialization before the
  error reaches the caller. Setting command retries to zero doesn't turn that
  recovery off. See the
  [redis-py 6.4.0 cluster source](https://github.com/redis/redis-py/blob/v6.4.0/redis/cluster.py).
- node-redis 5.8.2 can decode RESP2 integers without losing precision if you
  map them to strings, but its cluster wrapper reruns discovery inside the
  redirect loop. See the
  [node-redis 5.8.2 decoder and cluster loop](https://github.com/redis/node-redis/tree/redis%405.8.2/packages/client/lib).

Our clients check that a new owner is authorized before connecting to it, keep
discovery within the caller's deadline, and tell failures before a command was
sent apart from failures after it.

# Nabedra TypeScript ORM

This is the TypeScript client for the Nabedra Database server. It creates
tables, reads and writes rows, and builds queries. It needs Node.js 22.13 or
newer.

## Get started

To install from this checkout, run `npm ci` and `npm pack` in this directory,
then install the resulting tarball in your application. Set
`NABEDRA_TOKEN` to the server's data token before you connect.

```typescript
import { Database, col } from "nabedra"

const db = new Database({ url: "http://127.0.0.1:8000" })
const users = await db.createTable("users", { id: String, age: BigInt }, "id")
await users.insertMany([{ id: "alice", age: 24n }, { id: "bob", age: 17n }])
const adults = await users.where(col("age").ge(18n)).select("id", "age").collect()
```

A schema maps each column to `String`, `BigInt`, `Number` or `Boolean`, which
stand for UTF-8 strings, signed 64-bit integers, 64-bit floats and booleans.
Integers keep their exact value across the whole signed 64-bit range. When you
get a table handle from `createTable`, its types follow the schema, so the
compiler checks the keys, rows, updates, filter objects and projections you
pass to it.

Building a query is synchronous, and anything that talks to the server returns
a promise. For an HTTPS server with a private CA, pass `caFile`. Timeouts are
in seconds.

The client never retries a write. When it can't authenticate or validate a
write's response, it throws `UnknownOutcomeError`, which carries the request
identity so you can find out what happened. To track accepted work yourself,
use `submit` and `receipt` with the run and request ID.

Queries can also stream their results in batches:

```typescript
const query = users.query().select("id", "age")
await using batches = query.stream({ batchRows: 64 })
for await (const batch of batches) {
  console.log(batch)
}
```

## Cache

`Cache.connect` opens a connection pool to a Nabedra Cache server. Close it
when you are done.

```typescript
import { Cache } from "nabedra/cache"

const cache = await Cache.connect("rediss://cache.example.com:6379", {
  username, password, namespace: "catalog", caFile: "/path/to/ca.pem",
})
try {
  await cache.set("sku:42", new TextEncoder().encode("in stock"), { ttlMs: 30_000, when: "absent" })
  const value = await cache.get("sku:42")
} finally {
  await cache.close()
}
```

You can also import `Cache` from `nabedra`. The same handle has `delete`,
`expire`, `persist`, `ttlMs` and `increment`. `get` returns the stored bytes
unchanged, or `null` when the key is missing. Counters take and return
`bigint`.

For a cluster, pass `cluster: true` and a list of seed endpoints. If the
cluster nodes have different hostnames from the seeds, list them in
`allowedNodes` or give their common DNS suffix in `allowedNodeSuffix`. The
[shared Cache contract](https://github.com/zPy52/nabedra-database/tree/main/orm/cache-contract)
describes deadlines, routing and errors.

## Other entry points

Besides `Database`, the package root exports `Cluster`, `serve`,
`KubernetesProvider` and the application gateway. The Events producer and
subscription client live in `nabedra/events`, and the OLAP sink lives in
`nabedra/events/sink`. To run the sink, save this as `sink.mjs` and start it
with `node sink.mjs --config sink.json`:

```javascript
import { main } from "nabedra/events/sink"

await main()
```

`nabedra/deployment` has the growth and drain adapters, and the Docker, EC2
and EKS adapters. The `nabedra database` CLI runs an installed `nabedra-db`
binary, which must be a compatible version.

### EC2 client settings

The EC2 adapters take an AWS SDK `EC2` client that you configure yourself,
plus a `meta` object that states its retry and timeout settings. The adapter
checks that metadata before it makes any call. Note the units: the SDK takes
milliseconds, while the Nabedra metadata is in seconds. See the
[EC2 client reference](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/ec2)
for the SDK options.

```typescript
import { EC2 } from "@aws-sdk/client-ec2"
import { NodeHttpHandler } from "@smithy/node-http-handler"
import type { Ec2Client } from "nabedra/deployment"

const region = "us-east-1"
const connectTimeout = 2
const readTimeout = 5
const metadata = { connectTimeout, readTimeout, retries: { mode: "standard", totalMaxAttempts: 1 } } as const
const client: Ec2Client = Object.assign(new EC2({
  region,
  maxAttempts: metadata.retries.totalMaxAttempts,
  retryMode: metadata.retries.mode,
  requestHandler: new NodeHttpHandler({
    connectionTimeout: connectTimeout * 1_000,
    socketTimeout: readTimeout * 1_000,
    requestTimeout: (connectTimeout + readTimeout) * 1_000,
    throwOnRequestTimeout: true,
  }),
}), { meta: { config: metadata, regionName: region } })
```

Pass `client` in the constructor options as `Ec2Provider.client`,
`EksProvider.stagingEc2Client` or `EksReplacementVerifier.ec2Client`. Adapters
that accept a deadline want an absolute monotonic time in seconds, for example
`{ deadline: performance.now() / 1_000 + 30 }`. Docker and Kubernetes worker
images keep using the bootstrap commands configured inside their containers.

## Tests

`npm test` runs the type and protocol tests. A few more suites turn on when you
set these variables to absolute paths:

- `NABEDRA_TEST_SERVER_BINARY`, an OLTP server binary, runs the live server
  tests.
- `NABEDRA_TEST_PYTHON`, a Python 3.11 or newer executable, runs the
  differential tests that compare this package with the Python ORM.
- `NABEDRA_TEST_OLAP_BINARY`, an OLAP server binary, adds live row and Arrow
  streaming tests on top of the differential tests.

# Nabedra TypeScript ORM

The TypeScript client creates tables, reads and writes rows, and builds queries
for the Nabedra Database server. It requires Node.js 22.13 or newer.

Run `npm ci` and `npm pack` from this directory, then install the tarball in your
application. Configure `NABEDRA_TOKEN` with the server's data token.

```typescript
import { Database, col } from "nabedra"

const db = new Database({ url: "http://127.0.0.1:8000" })
const users = await db.createTable("users", { id: String, age: BigInt }, "id")
await users.insertMany([{ id: "alice", age: 24n }, { id: "bob", age: 17n }])
const adults = await users.where(col("age").ge(18n)).select("id", "age").collect()
```

Schemas use `String`, `BigInt`, `Number`, and `Boolean` for UTF-8 strings,
signed int64, float64, and booleans. The SDK preserves int64 values throughout
the signed 64-bit range. Table handles inferred from `createTable` type-check
keys, rows, updates, filter objects, and projections.

Builders run synchronously. Await operations that contact the server. Use
`caFile` for an HTTPS endpoint that uses a private CA. Timeout options are in
seconds. An `UnknownOutcomeError` carries the request identity for a write
whose response could not be authenticated or validated. Writes are never
retried automatically. `submit` and `receipt` let callers track accepted work
by run and request ID.

```typescript
const query = users.query().select("id", "age")
await using batches = query.stream({ batchRows: 64 })
for await (const batch of batches) {
  console.log(batch)
}
```

Run `npm test` for the type and protocol tests. Set `NABEDRA_TEST_SERVER_BINARY`
to an absolute path to an OLTP server binary to enable the live fixture.

## Other entry points

The package root also exports `Cluster`, `serve`, `KubernetesProvider`, and the
application gateway. Use `nabedra/events` for the producer and subscription
client and `nabedra/events/sink` for the OLAP sink. To launch the sink, save
the following as `sink.mjs` and run `node sink.mjs --config sink.json`.

```javascript
import { main } from "nabedra/events/sink"

await main()
```

Infrastructure operators can
import the growth, drain, Docker, EC2, and EKS adapters under `nabedra/deployment`.
The `nabedra database` CLI uses a compatible installed `nabedra-db` binary.

For AWS-backed deployment, provide the SDK's aggregated EC2 client with retry
and timeout settings. The adapter checks the explicit metadata before it makes
calls. Use milliseconds for SDK settings and seconds for Nabedra metadata.
[EC2 client configuration](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/ec2).

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

Pass `client` as `Ec2Provider.client`, `EksProvider.stagingEc2Client`, or
`EksReplacementVerifier.ec2Client` in the constructor options. When an adapter
accepts a deadline, pass an absolute monotonic time in seconds, for example
`{ deadline: performance.now() / 1_000 + 30 }`. Existing Docker and Kubernetes
worker images use their configured in-container bootstrap commands.

Set `NABEDRA_TEST_PYTHON` to an absolute Python 3.11+ executable for the
differential tests. Set `NABEDRA_TEST_OLAP_BINARY` as well to test live row and
Arrow streaming with the repository's OLAP process fixture.

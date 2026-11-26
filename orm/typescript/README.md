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

## Vector search

On an OLTP server with search, tables can hold vectors, sparse weights, several
vectors per row and lists of strings. The column type says what the data is, and
the query says how to rank it. Other servers reject these types and operations with
code `unsupported_operation` or `validation_error`. The application gateway does not
forward them.

```typescript
import { col, list, vector } from "nabedra"

const chunks = await db.createTable("chunks", {
  id: String, tenant: String, acl: list(String), body: String,
  embedding: vector(1024), sparse: vector({ sparse: true }), tokens: list(vector(128)),
}, {
  key: "id", partitionBy: "tenant", fullText: { body: "english" },
  embed: { embedding: { source: "body", model: "docs-v1" } },
})
await chunks.upsertMany(rows)
const hits = await chunks.where(col("tenant").eq("acme").and(col("acl").containsAny(["staff"])))
  .rank(col("embedding").cosine("reset my password"), { recall: 0.95 })
  .select("id", "body").limit(10).collect()
// [{ id: "doc7#0", body: "...", _score: 0.88 }]
```

| Column type | Values | Operations |
| --- | --- | --- |
| `vector(n)` | `n` float32 values, as `Float32Array` or `number[]`. Read as `Float32Array`. | `cosine`, `dot`, `l2` |
| `vector({ sparse: true })` | `{ [index]: weight }` with unsigned 32-bit indices | `cosine`, `dot` |
| `list(vector(n))` | An array of up to 1,024 vectors. Read as `Float32Array[]`. | `cosine`, `dot`: for every query vector, its best row vector, summed |
| `list(String)` | An array of strings | `contains`, `containsAny` filters |
| `String` in `fullText` | Text | `match` (BM25) |

Vector values are stored as float32 and must be finite. Vector columns accept
`null`, and those rows never rank on that column. Scalar comparisons and `orderBy`
do not accept these types.

| Table option | Meaning |
| --- | --- |
| `partitionBy` | A `String` or `BigInt` column. Each of its values gets its own index. |
| `fullText` | `{ column: language }` for `String` columns |
| `embed` | `{ column: { source, model } }`. The database fills this `vector(n)` column from a `String` column. Rows omit it, and writing it is a `ValidationError`. |

A table has at most 8 search columns, counting vector and full-text columns. With
any, `_score` is a reserved column name and a row holds at most 1 MiB, counting
vectors as raw float32 bytes.

| Method | Result and behavior |
| --- | --- |
| `col(name).cosine(query)`, `.dot(query)`, `.l2(query)` | A ranking. A string is embedded with the column's model. `cosine` rejects a zero query vector. |
| `col(name).match(text)` | A ranking by BM25 |
| `col(name).contains(value)`, `.containsAny(values)` | Filters on `list(String)` columns. `containsAny` of nothing matches nothing. It sends a `Set` sorted by UTF-8 bytes, as Python sorts a `set`, and any other iterable, `Map` keys included, in its order. |
| `col(name).startsWith(prefix)` | Filter on `String` columns |
| `query.rank(...rankings, { fusion = "rrf", candidates = 100, recall = 0.95, exact = false, consistency = "latest" })` | New query ordered by one ranking, or by several fused. `fusion` is `"rrf"` or one nonnegative weight per ranking. |
| `query.rerank(ranking, { candidates = 200 })` | New query that scores the best candidates of `rank` again |
| `query.rankMany(rankings, { limit, ... })` | One result list per ranking, up to 256, with the options of `rank` |
| `table.addColumn(name, type, { embed })`, `table.dropColumn(name)` | Add or drop a vector column online. The handle then uses the new schema. |
| `table.addFullText(column, language)`, `table.dropFullText(column)` | Start or stop indexing a `String` column for `match` |
| `table.searchStatus()` | Per search column: `state` (`"building"`, `"ready"` or `"failed"`), `lagMs`, `pendingEmbeddings`, `failedEmbeddings` and `recall`. `lagMs` and `recall` can be `null`. |

A ranked query needs a `limit` of at most 1,000 and supports only `collect`. Its rows
gain `_score` and omit vector columns unless selected. `l2` sorts ascending, the
others descending, then by key. `orderBy` cannot be combined with `rank`. `recall`
is a target, not a guarantee; `exact: true` is exact. `"latest"` shows every write
acknowledged before the query, and `"snapshot"` reads one cut across ranges.

An operation that a column does not support is a `ValidationError` before any
request. An options object with an unknown name, or `null` in its place, is a
`TypeError`, like an unknown keyword argument in Python. `SearchUnavailableError`
(503) means that search cannot answer now. The query can be sent again later. Streams support only scalar columns.

## Batches

`upsertMany` and `deleteMany` accept any iterable or async iterable and send it in
requests of at most 10,000 rows and 8 MiB. Each row is atomic and durable. The call is
not atomic across rows. A key that appears twice in one request is a
`ValidationError`; in different requests the later row wins. A missing key counts as
applied for `deleteMany`. An empty iterable sends nothing and returns `0`.

The call stops at the first request that fails and throws `BatchError`:

| Property | Value |
| --- | --- |
| `applied` | Rows known to be applied, in the whole call |
| `outcomes` | One `{ key, outcome, error }` per row of the failed request, in order. `outcome` is `"applied"`, `"not_applied"` or `"outcome_unknown"`. `error` is `null` for an applied row. |
| `requestId` | ID of the failed request |
| `outcome` | `"not_applied"`, `"partial"`, `"unknown"`, or `"applied"` when only the wait for embeddings failed |

The class of a row's `error` follows its outcome, then its code: `"outcome_unknown"`
gives `UnknownOutcomeError`. A `"not_applied"` row maps `validation_error`,
`unauthorized`, `not_found`, `conflict`, `limit_exceeded`, `overloaded`,
`storage_unavailable` and `search_unavailable` to `ValidationError`,
`AuthenticationError`, `NotFoundError`, `ConflictError`, `LimitExceededError`,
`OverloadedError`, `StorageError` and `SearchUnavailableError`, and any other code to
`NabedraError`.

A lost response makes every row of its request `"outcome_unknown"`. The client never
sends a request again. Writing the same rows again is safe while no other writer
changes them. Rows after the failed request were not sent. An invalid row throws
`ValidationError` if nothing was sent yet. After that, it or an error from the
iterable, whatever it throws, throws `BatchError` with the applied count and no `outcomes`.

`{ waitForEmbedding: true }` polls `searchStatus()` after the last request until no
embedding of the table is pending, for at most the client's `timeout`. Other writers'
rows count too. If the wait fails, `BatchError` reports every row as applied. Failed
embeddings end the wait; see `failedEmbeddings`.

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

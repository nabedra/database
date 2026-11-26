import { DB, Database, col, list, vector, type Query, type Table, type Vector } from "nabedra"
import { Cache, type CacheOptions } from "nabedra/cache"

const cacheOptions: CacheOptions = { namespace: "catalog", timeoutMs: 250, poolSize: 4 }
const cache = await Cache.connect("redis://localhost:6379", cacheOptions)
const cacheValue: Uint8Array | null = await cache.get("sku:42")
void cacheValue
const counter: bigint = await cache.increment("count", 1n)
void counter
await cache.set("sku:42", Uint8Array.of(128, 0, 255), { when: "absent", ttlMs: 20_000 })
// @ts-expect-error The cache counter API requires a bigint.
await cache.increment("count", 1)
// @ts-expect-error The cache accepts binary values.
await cache.set("sku:42", "text")
await cache.close()

const db: Database = new DB({ apiUrl: "https://example.test", caFile: "/ca.pem" })
const sameMode: "oltp" | "olap" = new Database("https://example.test", "token", { mode: "olap" }).mode
void sameMode
const users = await db.createTable("users", { id: String, age: BigInt, active: Boolean, score: Number }, "id")
const table: Table<{ id: StringConstructor; age: BigIntConstructor; active: BooleanConstructor; score: NumberConstructor }, "id"> = users
await table.insert({ id: "ada", age: 25n, active: true, score: 100 })
await table.update("ada", { age: 26n })
await table.upsert({ id: "ada", age: 25n, active: true, score: 100 })
const query = table.where(col("age").ge(18n), { active: true }).orderBy("age", true)
const rows: { id: string; age: bigint }[] = await query.select("id", "age").collect()
const stream = query.select("id").stream({ batchRows: 64 })
for await (const batch of stream) {
  const projected: { id: string }[] = batch
  void projected
}

// @ts-expect-error Int64 input must be exact.
await table.insert({ id: "ada", age: 25, active: true, score: 100 })
// @ts-expect-error The primary key is a string.
await table.get(100n)
// @ts-expect-error Updates cannot replace the primary key.
await table.update("ada", { id: "different" })
// @ts-expect-error Predicates must refer to schema columns.
table.where({ city: "Madrid" })
// @ts-expect-error Projection only accepts schema columns.
query.select("missing")
// @ts-expect-error Boolean cannot be a primary key.
await db.createTable("invalid", { active: Boolean }, "active")
// @ts-expect-error Projection returns only selected columns.
void rows[0].active

const embeddingType: Vector<false> = vector(3)
const chunks = await db.createTable("chunks", {
  id: String, tenant: String, acl: list(String), year: BigInt, body: String,
  embedding: embeddingType, weights: vector({ sparse: true }), tokens: list(vector(2)),
}, {
  key: "id", partitionBy: "tenant", fullText: { body: "english" },
  embed: { embedding: { source: "body", model: "docs-v1" } },
})
const chunk = {
  id: "a", tenant: "t", acl: ["x"], year: 2024n, body: "text", weights: { 3: 0.5 }, tokens: [[1, 2], Float32Array.of(1, 2)],
}
const written: number = await chunks.upsertMany([chunk, { ...chunk, weights: null, tokens: null }])
void written
async function* chunkSource() { yield chunk }
await chunks.upsertMany(chunkSource(), { waitForEmbedding: true })
await chunks.deleteMany(["a"])
await chunks.update("a", { weights: null })
const mine = chunks.where(col("tenant").eq("t").and(col("acl").containsAny(["x"])))
const hits: { id: string; body: string; _score: number }[] = await mine
  .rank(col("embedding").cosine([1, 2, 3]), col("body").match("reset"), { recall: 0.95, fusion: [1, 2] })
  .rerank(col("tokens").cosine([[1, 2]]), { candidates: 50 }).select("id", "body").limit(10).collect()
void hits
const [best] = await mine.rank(col("embedding").l2("text")).limit(10).collect()
const bestFields: [bigint, string[], number] = [best.year, best.acl, best._score]
void bestFields
const lists: { id: string; _score: number }[][] = await mine.rankMany([col("embedding").dot([1, 2, 3])], { limit: 5 })
void lists
const stored = await chunks.get("a")
const vectors: [Float32Array | null | undefined, Record<number, number> | null | undefined, Float32Array[] | null | undefined] =
  [stored?.embedding, stored?.weights, stored?.tokens]
void vectors
await chunks.addColumn("extra", vector(8), { embed: { source: "body", model: "docs-v2" } })
const lag: number | null = (await chunks.searchStatus()).embedding.lagMs
void lag

// @ts-expect-error Embedded columns cannot be written.
await chunks.upsert({ ...chunk, embedding: [1, 2, 3] })
// @ts-expect-error Only vector columns accept null.
await chunks.upsert({ ...chunk, acl: null })
// @ts-expect-error Vector columns hold numbers.
await chunks.upsert({ ...chunk, tokens: ["text"] })
// @ts-expect-error Ranked queries omit vector columns unless selected.
void best.embedding
// @ts-expect-error Only ranked rows have a score.
void (await mine.collect())[0]._score
// @ts-expect-error A ranking is not a filter.
chunks.where(col("embedding").cosine([1, 2, 3]))
// @ts-expect-error A filter is not a ranking.
mine.rank(col("year").eq(1n))
// @ts-expect-error Filter mappings compare scalar columns.
chunks.where({ embedding: [1, 2, 3] })
// @ts-expect-error A vector column cannot be the key.
await db.createTable("invalid", { id: vector(2) }, { key: "id" })
// @ts-expect-error Lists hold strings or dense vectors.
list(BigInt)
// @ts-expect-error Added columns have a vector type.
await chunks.addColumn("extra", String)

const baseQuery: Query = (await db.table("users")).query()
void baseQuery

import { KubernetesProvider, serve } from "nabedra"
import { GrowthTarget, type StatefulGrowthAdapter } from "nabedra/deployment"
import { Events, type Event as BrokerEvent } from "nabedra/events"

const provider = new KubernetesProvider({
  apiUrl: "https://kubernetes.example.test", token: "fixture", namespace: "example",
  clusterId: "e1f13e99-cff9-4d33-aa0e-3eeb299b835c", image: "example-worker",
})
await serve("example", [1, 2], { startNode: provider.startNode, stopNode: provider.stopNode, listNodes: provider.listNodes })
const bus = new Events("https://events.example.test", { token: "events-fixture" })
const event: BrokerEvent = await bus.publish("visits", {
  partition: 0, eventId: "a671477f-a07c-4ee7-b761-b3734202e99d", data: { url: "/" },
})
const offset: bigint = event.offset
const adapterTarget: ReturnType<StatefulGrowthAdapter["target"]> = new GrowthTarget({}, "pool", 1)
void offset
void adapterTarget

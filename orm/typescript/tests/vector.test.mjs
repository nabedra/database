// Vector types and values that are particular to TypeScript, and the request comparison with Python.
import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { once } from "node:events"
import { createServer } from "node:http"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"
import { Database, Query, SearchUnavailableError, ValidationError, col, list, vector } from "nabedra"
import { Ranking } from "nabedra/expr"
import { schemaFromWire, schemaToWire, validateRow, validateSchema, validateValue } from "nabedra/validation"

const schema = {
  id: String, vec: vector(2), tokens: list(vector(2)), weights: vector({ sparse: true }), tags: list(String),
}
const context = { id: "table-1", key: "id", schema }

test("types are frozen and round trip", () => {
  assert.throws(() => { vector(3).dim = 4 }, TypeError)
  for (const bad of [undefined, null, 0, 4097, 2.5, NaN, true, "2", 2n, {}, { sparse: false }, { sparse: 1 }, { sparse: true, dim: 2 }]) {
    assert.throws(() => vector(bad), ValidationError)
  }
  for (const bad of [undefined, Number, BigInt, vector({ sparse: true }), list(String), "utf8"]) {
    assert.throws(() => list(bad), ValidationError)
  }
  assert.deepEqual(schemaFromWire(schemaToWire(schema)), schema)
  for (const bad of [{ type: "vector", dim: 2 }, Float32Array, Array, [String], {}]) {
    assert.throws(() => validateSchema({ id: String, x: bad }), ValidationError)
  }
})

test("a Float32Array view is accepted and copied", () => {
  const source = Float32Array.of(0, 1.5, 2.5, 0)
  const copy = validateValue(source.subarray(1, 3), vector(2))
  source[1] = 9
  assert.deepEqual(copy, Float32Array.of(1.5, 2.5))
  for (const bad of [new Float64Array(2), new Uint8Array(8), new ArrayBuffer(8), [1, , 2], [1n, 2n], new Set([1, 2]), undefined]) {
    assert.throws(() => validateValue(bad, vector(2)), ValidationError)
  }
  for (const bad of [new Map([[1, 1]]), { "01": 1 }, { "1.0": 1 }, { "-1": 1 }, { " 1": 1 }, { 4294967296: 1 }, { 1: 1n }, [1]]) {
    assert.throws(() => validateValue(bad, vector({ sparse: true })), ValidationError)
  }
})

test("options objects refuse unknown names and null like Python keyword arguments", async () => {
  const query = new Query(context).limit(1)
  const ranking = col("vec").dot([1, 2])
  for (const call of [
    () => query.rank(ranking, { recal: 0.5 }), () => query.rank(ranking, null),
    () => query.rank(ranking).rerank(ranking, { candidate: 3 }), () => query.rank(ranking).rerank(ranking, null),
    () => query.rankMany([ranking], { limit: 1, exct: true }), () => query.rankMany([ranking], null),
    () => query.rank(ranking, { recall: 0.5, __proto__: null, extra: 1 }),
  ]) {
    await assert.rejects(async () => call(), TypeError)
  }
  assert.doesNotThrow(() => query.rank(ranking, { recall: 0.5 }).rerank(ranking, { candidates: 3 }))
  const db = new Database({ url: "http://127.0.0.1:9", token: "test" })
  await assert.rejects(db.createTable("t", { id: String }, { key: "id", partition: "id" }), TypeError)
  await assert.rejects(db.createTable("t", { id: String }, null), ValidationError)
})

test("rows detach every value", () => {
  const tags = ["a"]
  const tokens = [[1, 2]]
  const weights = { 2: 0.5, 1: 0.25 }
  const checked = validateRow({ id: "x", vec: null, tokens, weights, tags }, schema, { key: "id" })
  tags.push("b")
  tokens[0].push(3)
  delete weights[1]
  assert.deepEqual(checked, {
    id: "x", vec: null, tokens: [Float32Array.of(1, 2)], weights: { 1: 0.25, 2: 0.5 }, tags: ["a"],
  })
})

test("rankings and queries are immutable", () => {
  assert.throws(() => new Ranking(), TypeError)
  const first = col("vec").cosine([1, 2])
  assert.ok(Object.isFrozen(first))
  const query = new Query(context).limit(1)
  assert.throws(() => query.where(first), ValidationError)
  assert.throws(() => query.where({ vec: first }), ValidationError)
  assert.throws(() => col("id").eq("x").and(first), TypeError)
  const ranked = query.rank(first)
  assert.notEqual(ranked, query)
  assert.throws(() => query.rerank(first), ValidationError)
  assert.doesNotThrow(() => ranked.rerank(col("tokens").dot([[1, 2]])))
  assert.equal(new SearchUnavailableError("x").outcome, "not_applied")
})

test("submitted search operations keep Python's float tokens", async (t) => {
  const bodies = []
  const app = createServer(async (request, response) => {
    const parts = []
    for await (const part of request) parts.push(part)
    const raw = Buffer.concat(parts).toString()
    bodies.push(raw.slice(raw.indexOf('"payload":') + '"payload":'.length, -1))
    response.writeHead(202, { "content-type": "application/json" }).end(JSON.stringify({
      request_id: JSON.parse(raw).request_id, run_id: "11111111-1111-4111-8111-111111111111", state: "queued",
    }))
  })
  app.listen(0, "127.0.0.1")
  await once(app, "listening")
  const db = new Database({ url: `http://127.0.0.1:${app.address().port}`, token: "test" })
  t.after(() => new Promise((resolve) => {
    db.close()
    app.close(resolve)
    app.closeAllConnections()
  }))
  await db.submit("table.upsert_many", { rows: [{ id: "a", score: 1 }] })
  await db.submit("query.rank", {
    query: { filter: { op: "eq", column: "score", value: 2 } }, rank: { fusion: [1, 0.5], recall: 1, candidates: 100 },
  })
  await db.submit("query.rank_many", { rank: { fusion: "rrf", recall: 1 } })
  assert.deepEqual(bodies, [
    '{"rows":[{"id":"a","score":1.0}]}',
    '{"query":{"filter":{"op":"eq","column":"score","value":2.0}},"rank":{"fusion":[1.0,0.5],"recall":1.0,"candidates":100}}',
    '{"rank":{"fusion":"rrf","recall":1.0}}',
  ])
})

test("VS18 search requests match Python byte for byte", { timeout: 30_000 }, async (t) => {
  const descriptor = {
    id: "12345678-1234-1234-1234-123456789abc", name: "chunks",
    schema: {
      id: "utf8", tenant: "utf8", acl: { type: "list", item: "utf8" }, year: "int64", body: "utf8",
      embedding: { type: "vector", dim: 3 }, weights: { type: "vector", sparse: true },
      tokens: { type: "list", item: { type: "vector", dim: 2 } },
    },
    key: "id", format: "parquet", partition_by: "tenant", full_text: { body: "english" },
    embed: { embedding: { source: "body", model: "docs-v1" } },
  }
  let current = descriptor
  const requests = []
  const app = createServer(async (request, response) => {
    const parts = []
    for await (const part of request) parts.push(part)
    const raw = Buffer.concat(parts).toString()
    const { request_id: requestId, operation, payload } = JSON.parse(raw)
    requests.push(raw.replace(requestId, "@request_id"))
    if (operation === "table.create" || operation === "table.describe") current = descriptor
    if (operation === "table.add_column") {
      current = { ...current, schema: { ...current.schema, [payload.name]: payload.type },
        embed: { ...current.embed, [payload.name]: payload.embed } }
    }
    if (operation === "table.drop_column") {
      const { [payload.name]: _, ...rest } = current.schema
      const { [payload.name]: __, ...embed } = current.embed
      current = { ...current, schema: rest, embed }
    }
    if (operation === "table.drop_full_text") ({ full_text: {}, ...current } = { ...current, full_text: {} })
    if (operation === "table.add_full_text") current = { ...current, full_text: { [payload.column]: payload.language } }
    const data = {
      "table.upsert_many": () => ({ outcomes: payload.rows.map(() => ({ o: "applied" })) }),
      "table.delete_many": () => ({ outcomes: payload.keys.map(() => ({ o: "applied" })) }),
      "table.upsert": () => null,
      "table.search_status": () => ({ columns: {} }),
      "query.rank": () => [],
      "query.rank_many": () => payload.batch.map(() => []),
    }[operation] ?? (() => current)
    response.writeHead(200, { "content-type": "application/json" })
      .end(JSON.stringify({ request_id: requestId, data: data() }))
  })
  app.listen(0, "127.0.0.1")
  await once(app, "listening")
  const url = `http://127.0.0.1:${app.address().port}`
  const db = new Database({ url, token: "test" })
  t.after(() => new Promise((resolve) => {
    db.close()
    app.close(resolve)
    app.closeAllConnections()
  }))

  const chunks = await db.createTable("chunks", {
    id: String, tenant: String, acl: list(String), year: BigInt, body: String,
    embedding: vector(3), weights: vector({ sparse: true }), tokens: list(vector(2)),
  }, { key: "id", partitionBy: "tenant", fullText: { body: "english" }, embed: { embedding: { source: "body", model: "docs-v1" } } })
  const row = {
    id: "é", tenant: "t", acl: ["x"], year: 2024n, body: "b",
    weights: { 9: 0.1, 3: 1e-7, 10: 2 }, tokens: [[0.1, 0.2], Float32Array.of(0.3, 0.4)],
  }
  assert.equal(await chunks.upsertMany([row]), 1)
  await chunks.upsert({ ...row, weights: null, tokens: [] })
  assert.equal(await chunks.deleteMany(["a", "é"]), 2)
  const q = [0.1, -0.2, 1 / 3]
  const mine = chunks.where(col("tenant").eq("t").and(col("acl").containsAny(["x", "y"])).and(col("year").ge(2024n)))
  assert.deepEqual(await mine.rank(col("embedding").cosine(q)).select("id", "body").limit(10).collect(), [])
  await mine.rank(col("embedding").dot("text"), col("body").match("reset"), col("weights").dot({ 5: 0.1 }),
    { fusion: [1, 0.25, 1e-7], candidates: 50, recall: 1, exact: true, consistency: "snapshot" })
    .rerank(col("tokens").cosine([[0.1, 0.2]])).limit(10).collect()
  await chunks.where(col("id").startsWith("acme/")).rankMany(
    [col("embedding").l2(q), col("tokens").dot([[1, 2], [3, 4]])], { limit: 10, recall: 0.99 })
  await chunks.addColumn("extra", vector(2), { embed: { source: "body", model: "docs-v2" } })
  await chunks.dropColumn("extra")
  await chunks.dropFullText("body")
  await chunks.addFullText("body", "spanish")
  assert.deepEqual(await chunks.searchStatus(), {})
  const typescript = requests.splice(0)
  assert.equal(typescript.length, 12)

  const pythonBinary = process.env.NABEDRA_TEST_PYTHON
  await t.test("matches the Python operation envelopes", { skip: !pythonBinary }, async () => {
    const workload = `
import sys
from array import array
from nabedra import Database, Vector, col
db = Database(sys.argv[1], "test")
chunks = db.create_table("chunks", {
    "id": str, "tenant": str, "acl": list[str], "year": int, "body": str,
    "embedding": Vector(3), "weights": Vector(sparse=True), "tokens": list[Vector(2)],
}, key="id", partition_by="tenant", full_text={"body": "english"}, embed={"embedding": {"source": "body", "model": "docs-v1"}})
row = {"id": "é", "tenant": "t", "acl": ["x"], "year": 2024, "body": "b",
       "weights": {9: 0.1, 3: 1e-7, 10: 2.0}, "tokens": [[0.1, 0.2], array("f", [0.3, 0.4])]}
assert chunks.upsert_many([row]) == 1
chunks.upsert({**row, "weights": None, "tokens": []})
assert chunks.delete_many(["a", "é"]) == 2
q = [0.1, -0.2, 1 / 3]
mine = chunks.where((col("tenant") == "t") & col("acl").contains_any(["x", "y"]) & (col("year") >= 2024))
assert mine.rank(col("embedding").cosine(q)).select("id", "body").limit(10).collect() == []
(mine.rank(col("embedding").dot("text"), col("body").match("reset"), col("weights").dot({5: 0.1}),
           fusion=[1.0, 0.25, 1e-7], candidates=50, recall=1.0, exact=True, consistency="snapshot")
     .rerank(col("tokens").cosine([[0.1, 0.2]])).limit(10).collect())
chunks.where(col("id").starts_with("acme/")).rank_many(
    [col("embedding").l2(q), col("tokens").dot([[1.0, 2.0], [3.0, 4.0]])], limit=10, recall=0.99)
chunks.add_column("extra", Vector(2), embed={"source": "body", "model": "docs-v2"})
chunks.drop_column("extra")
chunks.drop_full_text("body")
chunks.add_full_text("body", "spanish")
assert chunks.search_status() == {}
`
    await promisify(execFile)(pythonBinary, ["-c", workload, url], {
      env: { ...process.env, PYTHONPATH: fileURLToPath(new URL("../../python/src/", import.meta.url)) },
      timeout: 15_000,
    })
    assert.deepEqual(requests.splice(0), typescript)
  })
})

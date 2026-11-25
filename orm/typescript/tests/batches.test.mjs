// Batch writes against a fake server: chunk bounds, partial failure, lost replies and waiting.
import assert from "node:assert/strict"
import { once } from "node:events"
import { createServer } from "node:http"
import { test } from "node:test"
import { BatchError, Database, LimitExceededError, TransportError, UnknownOutcomeError, ValidationError } from "nabedra"

const limit = 8 * 1024 * 1024
const applied = { o: "applied" }
const rejected = { o: "not_applied", error: { code: "conflict", message: "no", outcome: "not_applied" } }
const unknown = { o: "outcome_unknown", error: { code: "outcome_unknown", message: "lost", outcome: "unknown" } }
const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/

const allApplied = (_, items) => ({ outcomes: items.map(() => applied) })
const status = (pending) => ({ columns: { vec: {
  state: "ready", lag_ms: 1, pending_embeddings: pending, failed_embeddings: 0, recall: null,
} } })
const row = (id, note = "") => ({ id: BigInt(id), note })

async function setUp(t) {
  const server = {
    chunks: [], answer: allApplied, statuses: [status(0)],
    descriptor: {
      id: "12345678-1234-4234-8234-123456789abc", name: "users", schema: { id: "int64", note: "utf8" },
      key: "id", format: "parquet",
    },
    sizes: () => server.chunks.map(({ items }) => items.length),
    async table(timeout = 10) {
      const db = new Database({ url, token: "test", timeout })
      t.after(() => db.close())
      return db.table("users")
    },
  }
  const app = createServer(async (request, response) => {
    const parts = []
    for await (const part of request) parts.push(part)
    const raw = Buffer.concat(parts)
    const { request_id: requestId, operation, payload } = JSON.parse(raw)
    let data
    if (operation === "table.describe") data = server.descriptor
    else if (operation === "table.search_status") {
      data = server.statuses.length > 1 ? server.statuses.shift() : server.statuses[0]
    } else {
      const items = payload.rows ?? payload.keys
      server.chunks.push({ requestId, items, size: raw.byteLength })
      data = server.answer(server.chunks.length, items)
    }
    if (data === null) return request.socket.destroy()
    response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ request_id: requestId, data }))
  })
  app.listen(0, "127.0.0.1")
  await once(app, "listening")
  const url = `http://127.0.0.1:${app.address().port}`
  t.after(() => new Promise((resolve) => {
    app.close(resolve)
    app.closeAllConnections()
  }))
  return server
}

async function batchError(promise) {
  let caught
  await assert.rejects(promise, (error) => {
    caught = error
    return error instanceof BatchError
  })
  return caught
}

function* numbers(end) {
  for (let number = 0; number < end; number += 1) yield number
}

function* rows(end) {
  for (const number of numbers(end)) yield row(number)
}

test("chunks hold at most 10000 rows in order with one call ID each", async (t) => {
  const server = await setUp(t)
  const table = await server.table()
  assert.equal(await table.upsertMany(rows(25_000)), 25_000)
  assert.deepEqual(server.sizes(), [10_000, 10_000, 5_000])
  assert.deepEqual(server.chunks.flatMap(({ items }) => items.map(({ id }) => id)), [...numbers(25_000)])
  assert.equal(new Set(server.chunks.map(({ requestId }) => requestId)).size, 3)
  for (const { requestId } of server.chunks) assert.match(requestId, uuid)
  server.chunks = []
  async function* keys() {
    for (const number of numbers(10_001)) yield BigInt(number)
  }
  assert.equal(await table.deleteMany(keys()), 10_001)
  assert.deepEqual(server.sizes(), [10_000, 1])
})

test("chunks fill 8 MiB exactly and never exceed it", async (t) => {
  const server = await setUp(t)
  const table = await server.table()
  assert.equal(await table.upsertMany([row(1)]), 1)
  const overhead = server.chunks.pop().size
  // Two rows and the comma between them fill one request to the last byte.
  const second = '{"id":2,"note":""},'.length
  const half = Math.floor((limit - overhead - second) / 2)
  const rest = limit - overhead - second - half
  const pair = [row(1, "a".repeat(half)), row(2, "é".repeat(Math.floor(rest / 2)) + "a".repeat(rest % 2))]
  assert.equal(await table.upsertMany(pair), 2)
  assert.deepEqual(server.chunks.map(({ size }) => size), [limit])
  server.chunks = []
  pair[1].note += "a"
  assert.equal(await table.upsertMany(pair), 2)
  assert.deepEqual(server.sizes(), [1, 1])
  server.chunks = []
  assert.equal(await table.upsertMany(Array.from({ length: 20 }, (_, id) => row(id, "a".repeat(900_000)))), 20)
  assert.deepEqual(server.sizes(), [9, 9, 2])
  assert.ok(server.chunks.every(({ size }) => size <= limit))
  server.chunks = []
  await assert.rejects(table.upsertMany([row(1, "a".repeat(limit))]), LimitExceededError)
  assert.deepEqual(server.chunks, [])
})

test("a failed chunk stops an endless iterable and is not sent again", async (t) => {
  const server = await setUp(t)
  const table = await server.table()
  let consumed = 0
  let closed = false
  function* endless() {
    try {
      for (;;) yield row(consumed++)
    } finally {
      closed = true
    }
  }
  server.answer = (number, items) => {
    const outcomes = items.map(() => applied)
    if (number === 2) [outcomes[3], outcomes[5]] = [rejected, unknown]
    return { outcomes }
  }
  const error = await batchError(table.upsertMany(endless()))
  assert.deepEqual(server.sizes(), [10_000, 10_000])
  assert.ok(consumed <= 20_001)
  assert.equal(closed, true)
  assert.deepEqual([error.applied, error.outcome, error.code], [19_998, "unknown", "batch_error"])
  assert.equal(error.requestId, server.chunks[1].requestId)
  assert.deepEqual(error.outcomes.map(({ key }) => key), [...numbers(10_000)].map((number) => BigInt(number + 10_000)))
  assert.deepEqual(error.outcomes.filter(({ outcome }) => outcome !== "applied").map(({ key, outcome }) => [key, outcome]),
    [[10_003n, "not_applied"], [10_005n, "outcome_unknown"]])
  assert.equal(error.cause, error.outcomes[3].error)
})

test("a lost reply is unknown for every row of that chunk only", async (t) => {
  const server = await setUp(t)
  const table = await server.table()
  server.answer = (number, items) => number === 2 ? null : allApplied(number, items)
  const error = await batchError(table.deleteMany([...numbers(30_000)].map(BigInt)))
  assert.deepEqual(server.sizes(), [10_000, 10_000])
  assert.deepEqual([error.applied, error.outcome], [10_000, "unknown"])
  assert.equal(error.outcomes.length, 10_000)
  assert.equal(error.outcomes[0].key, 10_000n)
  assert.ok(error.outcomes.every(({ outcome }) => outcome === "outcome_unknown"))
  assert.ok(error.cause instanceof UnknownOutcomeError)
})

test("a short outcome list is unknown", async (t) => {
  const server = await setUp(t)
  server.answer = (_, items) => ({ outcomes: items.slice(1).map(() => applied) })
  const error = await batchError((await server.table()).deleteMany([1n, 2n]))
  assert.deepEqual([error.applied, error.outcome], [0, "unknown"])
})

test("later input errors keep the applied count", async (t) => {
  const server = await setUp(t)
  const table = await server.table()
  function* input(count, last) {
    yield* rows(count)
    if (last instanceof Error) throw last
    yield last
  }
  const source = new RangeError("source")
  for (const [last, cause] of [[{ id: "x", note: "" }, ValidationError], [source, RangeError], [row(10_000), ValidationError]]) {
    server.chunks = []
    const error = await batchError(table.upsertMany(input(10_001, last)))
    assert.deepEqual(server.sizes(), [10_000])
    assert.deepEqual([error.applied, error.outcomes, error.outcome], [10_000, [], "partial"])
    assert.ok(error.cause instanceof cause)
  }
  server.chunks = []
  // Nothing was sent, so the original error is the whole story.
  await assert.rejects(table.upsertMany(input(1, { id: "x", note: "" })), ValidationError)
  await assert.rejects(table.upsertMany(input(1, source)), (error) => error === source)
  assert.deepEqual(server.chunks, [])
})

test("a key may repeat in a later chunk", async (t) => {
  const server = await setUp(t)
  assert.equal(await (await server.table()).deleteMany([...numbers(10_000), 0].map(BigInt)), 10_001)
  assert.deepEqual(server.sizes(), [10_000, 1])
})

function embedded(server) {
  server.descriptor.schema.vec = { type: "vector", dim: 2 }
  server.descriptor.embed = { vec: { source: "note", model: "m" } }
}

test("waitForEmbedding polls until nothing is pending", async (t) => {
  const server = await setUp(t)
  embedded(server)
  const table = await server.table()
  server.statuses = [status(2), { columns: {} }, status(0)]
  assert.equal(await table.upsertMany([row(1, "a")], { waitForEmbedding: true }), 1)
  assert.equal(server.statuses.length, 1)
  server.statuses = [status(2)]
  assert.equal(await table.upsertMany([], { waitForEmbedding: true }), 0)
})

test("waitForEmbedding reports applied rows when it times out or fails", async (t) => {
  const server = await setUp(t)
  embedded(server)
  const table = await server.table(0.3)
  for (const [answer, cause] of [[status(1), undefined], [null, TransportError]]) {
    server.statuses = [answer]
    const error = await batchError(table.upsertMany([row(1, "a")], { waitForEmbedding: true }))
    assert.deepEqual([error.applied, error.outcomes, error.outcome], [1, [], "applied"])
    assert.equal(error.message, "1 rows were applied and their embeddings can still be pending")
    assert.equal(error.cause?.constructor, cause)
  }
})

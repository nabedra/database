// Replays the shared vector search cases through the public TypeScript ORM.
import assert from "node:assert/strict"
import { once } from "node:events"
import { readFileSync } from "node:fs"
import { createServer } from "node:http"
import { test } from "node:test"
import { BatchError, Database, col, list, vector } from "nabedra"

const contract = JSON.parse(readFileSync(new URL("../../contract/vector/vectors.json", import.meta.url), "utf8"))
const requestIdMarker = "@request_id"
const scalars = { utf8: String, int64: BigInt, float64: Number, bool: Boolean }
const methods = {
  insert_many: "insertMany", upsert_many: "upsertMany", delete_many: "deleteMany", add_column: "addColumn",
  drop_column: "dropColumn", add_full_text: "addFullText", drop_full_text: "dropFullText",
  search_status: "searchStatus", contains_any: "containsAny", starts_with: "startsWith",
}

/** Build a public column type from its wire form, without the decoder under test. */
function columnType(wire) {
  if (typeof wire === "string") return scalars[wire]
  if (wire.type === "list") return list(columnType(wire.item))
  return vector(Object.hasOwn(wire, "dim") ? wire.dim : { sparse: wire.sparse })
}

function schemaOf(wire) {
  return Object.fromEntries(Object.entries(wire).map(([column, type]) => [column, columnType(type)]))
}

function valueOf(item) {
  if (Array.isArray(item)) return item.map(valueOf)
  if (typeof item !== "object" || item === null) return item
  const [[kind, content] = [""], ...rest] = Object.entries(item)
  if (rest.length > 0 || !kind.startsWith("$")) {
    return Object.fromEntries(Object.entries(item).map(([name, child]) => [name, valueOf(child)]))
  }
  const number = (value) => value === "nan" ? NaN : value === "inf" ? Infinity : value === "-inf" ? -Infinity : value
  switch (kind) {
    case "$int": return BigInt(content)
    case "$float": return number(content)
    case "$list": return content.map(valueOf)
    case "$float32": return Float32Array.from(content, number)
    case "$float64": return Float64Array.from(content)
    case "$bytes": return Uint8Array.from(content)
    case "$sparse":
    case "$object": return Object.fromEntries(content.map(([index, weight]) => [index, valueOf(weight)]))
    case "$set": return new Set(content)
    case "$repeat": return Array.from({ length: content[1] }, () => valueOf(content[0]))
    case "$text": return content[0].repeat(content[1])
    case "$type": return columnType(content)
    default: throw new Error(kind)
  }
}

/** Describe a result without language types. Float32 bits stay exact as float64 values. */
function normal(value) {
  if (value instanceof Float32Array) return { $float32: Array.from(value) }
  if (Array.isArray(value)) return value.map(normal)
  if (typeof value === "bigint") return { $int: String(value) }
  if (typeof value === "number") return { $float: value }
  if (typeof value !== "object" || value === null) return value ?? null
  const entries = Object.entries(value)
  if (entries.every(([key]) => /^\d+$/.test(key))) {
    return { $sparse: entries.map(([index, weight]) => [Number(index), normal(weight)]) }
  }
  return Object.fromEntries(entries.map(([key, item]) => [key, normal(item)]))
}

function predicate([op, ...args]) {
  if (op === "and" || op === "or") return predicate(args[0])[op](predicate(args[1]))
  if (op === "not") return predicate(args[0]).not()
  const column = Array.isArray(args[0]) ? predicate(args[0]) : col(args[0])
  return column[methods[op] ?? op](valueOf(args[1]))
}

function ranking([column, op, query]) {
  if (column === "filter") return predicate(op)
  return (Array.isArray(column) ? predicate(column) : col(column))[op](valueOf(query))
}

async function runQuery(table, options) {
  let query = table.query()
  const rankOptions = Object.hasOwn(options, "rank_options") ? [valueOf(options.rank_options)] : []
  if ("where" in options) query = query.where(predicate(options.where))
  if ("where_ranking" in options) query = query.where(ranking(options.where_ranking))
  if ("order_by_first" in options) query = query.orderBy(options.order_by_first)
  if ("rank" in options) query = query.rank(...options.rank.map(ranking), ...rankOptions)
  if ("then_rank" in options) query = query.rank(...options.then_rank.map(ranking))
  if ("rerank" in options) {
    const [node, ...candidates] = options.rerank
    query = query.rerank(ranking(node), ...candidates.map((value) => ({ candidates: valueOf(value) })))
  }
  if ("order_by" in options) query = query.orderBy(options.order_by)
  if (options.select != null) query = query.select(...options.select)
  if (options.limit != null) query = query.limit(options.limit)
  if (options.terminal === "rank_many") {
    const nodes = options.rankings
    const rankings = Array.isArray(nodes) ? nodes.map(ranking)
      : Array.from({ length: nodes.$repeat[1] }, () => ranking(nodes.$repeat[0]))
    return query.rankMany(rankings, { limit: options.limit_many, ...rankOptions[0] })
  }
  if (options.terminal === "stream_snapshot") return query.streamSnapshot({ shards: 1 })
  return query[options.terminal]()
}

async function runCall(table, { method, args, options }) {
  if (method === "query") return runQuery(table, options)
  const named = options === undefined ? [] : [valueOf(
    Object.hasOwn(options, "wait_for_embedding") ? { waitForEmbedding: options.wait_for_embedding } : options)]
  const result = await table[methods[method] ?? method](...args.map(valueOf), ...named)
  if (method !== "search_status") return result
  return Object.fromEntries(Object.entries(result).map(([column, status]) => {
    assert.deepEqual(Object.keys(status), ["state", "lagMs", "pendingEmbeddings", "failedEmbeddings", "recall"])
    const { state, lagMs, pendingEmbeddings, failedEmbeddings, recall } = status
    return [column, {
      state, lag_ms: lagMs, pending_embeddings: pendingEmbeddings, failed_embeddings: failedEmbeddings, recall,
    }]
  }))
}

function assertError(error, expected) {
  const actual = { type: error.constructor.name, message: error.message }
  if ("code" in expected) actual.code = error.code
  if ("status_code" in expected) actual.status_code = error.statusCode
  if ("outcome" in expected) actual.outcome = error.outcome
  if (error instanceof BatchError) {
    actual.applied = error.applied
    actual.outcomes = error.outcomes.map(({ key, outcome, error: cause }) =>
      [key, outcome, cause && cause.constructor.name, cause && cause.outcome])
    assert.notEqual(error.cause, undefined)
    for (const row of error.outcomes) assert.ok(row.error === null || row.error.requestId === error.requestId)
  }
  const message = typeof expected.message === "string" ? expected.message : expected.message.typescript
  assert.deepEqual(actual, { ...expected, message })
}

test("VS18 vector search shared contract", { timeout: 60_000 }, async (t) => {
  let described = null
  let reply
  let requests = []
  const server = createServer(async (request, response) => {
    const parts = []
    for await (const part of request) parts.push(part)
    const raw = Buffer.concat(parts).toString("utf8")
    const { request_id: requestId, operation } = JSON.parse(raw)
    let answer = reply
    if (operation === "table.describe" && described !== null) {
      answer = { data: described }
      described = null
    } else {
      requests.push(raw.replace(requestId, requestIdMarker))
    }
    if (answer.drop) return request.socket.destroy()
    const id = JSON.stringify(requestId)
    const body = answer.raw ?? (answer.error
      ? `{"request_id":${id},"error":${JSON.stringify(answer.error)}}`
      : `{"request_id":${id},"data":${answer.data}}`)
    response.writeHead(answer.status ?? 200, {
      "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body),
    }).end(body)
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const db = new Database({ url: `http://127.0.0.1:${server.address().port}`, token: "fixture", timeout: 5 })
  t.after(() => new Promise((resolve) => {
    db.close()
    server.close(resolve)
    server.closeAllConnections()
  }))

  async function exchange(vectorCase, run, expected) {
    requests = []
    reply = vectorCase.response ?? { data: vectorCase.data ?? "null" }
    if (vectorCase.error === undefined) {
      assert.deepEqual(await run(), expected)
    } else {
      await assert.rejects(run, (error) => {
        assertError(error, vectorCase.error)
        return true
      })
    }
    const { operation, payload } = vectorCase
    assert.deepEqual(requests, operation === undefined ? []
      : [`{"request_id":"${requestIdMarker}","operation":"${operation}","payload":${payload}}`])
  }

  for (const vectorCase of contract.create) {
    await t.test(`create: ${vectorCase.name}`, () => {
      const { partition_by: partitionBy, full_text: fullText, embed } = valueOf(vectorCase.options)
      const operation = vectorCase.payload === undefined ? {} : { operation: "table.create" }
      return exchange({ ...vectorCase, ...operation }, async () => {
        const table = await db.createTable("made", schemaOf(vectorCase.schema), { key: "id", partitionBy, fullText, embed })
        return table.schema
      }, vectorCase.error === undefined ? schemaOf(vectorCase.schema) : undefined)
    })
  }

  for (const vectorCase of contract.describe) {
    await t.test(`describe: ${vectorCase.name}`, () => {
      described = null
      return exchange({
        operation: "table.describe", payload: '{"name":"made"}', response: { data: vectorCase.data },
        ...(vectorCase.error ? { error: { type: "ProtocolError", message: "Invalid operation result" } } : {}),
      }, async () => (await db.table("made")).schema, vectorCase.error ? undefined : schemaOf(vectorCase.result))
    })
  }

  for (const vectorCase of contract.calls) {
    await t.test(vectorCase.name, async () => {
      described = contract.tables[vectorCase.table]
      const table = await db.table(vectorCase.table)
      const raw = vectorCase.call.method === "search_status"
      await exchange(vectorCase, async () => {
        const result = await runCall(table, vectorCase.call)
        return raw || typeof result === "number" ? result : normal(result)
      }, vectorCase.result)
      if (vectorCase.then !== undefined) assert.deepEqual(table.schema, schemaOf(vectorCase.then.$schema))
    })
  }
})

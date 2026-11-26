// Runs the adversarial scenarios of orm/contract/vector/adversarial.json through the public TypeScript ORM.
// Run as a script to print one JSON transcript, which the differential test compares with the Python one.
import { createHash } from "node:crypto"
import { readFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import { BatchError, Database, NabedraError, col, list, vector } from "nabedra"
import { Fake, tables } from "./fakeServer.mjs"

const scenariosUrl = new URL("../../../contract/vector/adversarial.json", import.meta.url)
const scalars = { utf8: String, int64: BigInt, float64: Number, bool: Boolean }
const methods = {
  insert_many: "insertMany", upsert_many: "upsertMany", delete_many: "deleteMany", add_column: "addColumn",
  drop_column: "dropColumn", add_full_text: "addFullText", drop_full_text: "dropFullText",
  search_status: "searchStatus", contains_any: "containsAny", starts_with: "startsWith",
}

function columnType(wire) {
  if (typeof wire === "string") return scalars[wire]
  if (wire.type === "list") return list(columnType(wire.item))
  return vector(Object.hasOwn(wire, "dim") ? wire.dim : { sparse: wire.sparse })
}

const special = (value) => value === "nan" ? NaN : value === "inf" ? Infinity : value === "-inf" ? -Infinity : value
const fill = (text, index) => index === undefined ? text : text.replaceAll("{i}", String(index))

export function valueOf(item, index) {
  if (Array.isArray(item)) return item.map((child) => valueOf(child, index))
  if (typeof item === "string") return fill(item, index)
  if (typeof item !== "object" || item === null) return item
  const [[kind, content] = [""], ...rest] = Object.entries(item)
  if (rest.length > 0 || !kind.startsWith("$")) {
    return Object.fromEntries(Object.entries(item).map(([name, child]) => [name, valueOf(child, index)]))
  }
  switch (kind) {
    case "$int": return BigInt(fill(content, index))
    case "$float": return special(content)
    case "$list": return content.map((child) => valueOf(child, index))
    case "$float32": return Float32Array.from(content, special)
    case "$sparse":
    case "$object": return Object.fromEntries(content.map(([key, weight]) => [key, valueOf(weight, index)]))
    case "$repeat": return Array.from({ length: content[1] }, () => valueOf(content[0], index))
    case "$text": return content[0].repeat(content[1])
    case "$concat": return content.flatMap((part) => valueOf(part, index))
    case "$cat": return content.map((child) => valueOf(child, index)).join("")
    case "$seq": return Array.from({ length: content[0] }, (_, number) => valueOf(content[1], number))
    case "$type": return columnType(content)
    default: throw new Error(kind)
  }
}

function hex(number) {
  // Match Python's float.hex() so that transcripts compare as text.
  if (Number.isNaN(number)) return "nan"
  if (number === 0) return Object.is(number, -0) ? "-0x0.0p+0" : "0x0.0p+0"
  if (!Number.isFinite(number)) return number > 0 ? "inf" : "-inf"
  const sign = number < 0 ? "-" : ""
  const view = new DataView(new ArrayBuffer(8))
  view.setFloat64(0, Math.abs(number))
  const bits = view.getBigUint64(0)
  let exponent = Number((bits >> 52n) & 0x7ffn)
  let mantissa = bits & ((1n << 52n) - 1n)
  let lead = "1"
  if (exponent === 0) {
    lead = "0"
    exponent = -1022
  } else {
    exponent -= 1023
  }
  const digits = mantissa.toString(16).padStart(13, "0")
  return `${sign}0x${lead}.${digits}p${exponent >= 0 ? "+" : "-"}${Math.abs(exponent)}`
}

function normal(value) {
  if (value instanceof Float32Array) return { $float32: Array.from(value, hex) }
  if (Array.isArray(value)) return value.map(normal)
  if (typeof value === "bigint") return { $int: String(value) }
  if (typeof value === "number") return { $float: hex(value) }
  if (typeof value !== "object" || value === null) return value ?? null
  const entries = Object.entries(value)
  if (entries.length > 0 && entries.every(([key]) => /^\d+$/.test(key))) {
    return { $sparse: entries.map(([index, weight]) => [Number(index), normal(weight)]) }
  }
  return entries.length === 0 ? { $sparse: [] } : Object.fromEntries(entries.map(([key, item]) => [key, normal(item)]))
}

const keyText = (key) => typeof key === "bigint" ? { $int: String(key) } : key

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
  for (const [node, ...candidates] of options.reranks ?? []) {
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
  return query[options.terminal]()
}

async function runCall(table, call, db) {
  const { method, args, options } = call
  if (method === "create_table") {
    const schema = Object.fromEntries(Object.entries(call.schema).map(([name, wire]) => [name, columnType(wire)]))
    const given = call.options ?? {}
    await db.createTable("made", schema, {
      key: call.key ?? "id", partitionBy: given.partition_by, fullText: given.full_text, embed: given.embed,
    })
    return null
  }
  if (method === "noop") return null
  if (method === "query") return runQuery(table, options)
  const named = options === undefined ? [] : [valueOf(
    Object.hasOwn(options, "wait_for_embedding") ? { waitForEmbedding: options.wait_for_embedding } : options)]
  const result = await table[methods[method] ?? method](...(args ?? []).map((arg) => valueOf(arg)), ...named)
  if (method !== "search_status") return result
  return Object.fromEntries(Object.entries(result).map(([column, status]) => [column, {
    state: status.state, lag_ms: status.lagMs, pending_embeddings: status.pendingEmbeddings,
    failed_embeddings: status.failedEmbeddings, recall: status.recall,
  }]))
}

function mask(raw) {
  const text = raw.toString("utf8").replace(/[0-9a-f]{8}-[0-9a-f-]{27}/, "ID")
  return text.length > 4000 ? { len: raw.length, sha: createHash("sha256").update(text).digest("hex") } : text
}

function describeError(error) {
  const known = error instanceof NabedraError
  const summary = { type: known ? error.constructor.name : `RAW:${error?.constructor?.name}`, message: String(error?.message) }
  if (known) Object.assign(summary, { code: error.code, outcome: error.outcome, status: error.statusCode ?? null })
  if (error instanceof BatchError) {
    summary.applied = error.applied
    summary.outcomes = error.outcomes.map(({ key, outcome, error: cause }) =>
      [keyText(key), outcome, cause && cause.constructor.name, cause && cause.outcome])
    summary.cause = error.cause ? error.cause.constructor.name : null
  }
  return summary
}

function shape({ raw, request }) {
  const { payload } = request
  return { operation: request.operation, items: (payload.rows ?? payload.keys ?? []).length, bytes: raw.length }
}

export async function runOne(fake, db, scenario) {
  let { table } = scenario
  if (typeof table === "object") {
    const raw = table.$nest === undefined ? table.$raw : '{"id":"00000000-0000-0000-0000-000000000031","name":"custom",'
      + '"key":"id","format":"parquet","schema":{"id":"utf8","x":' + '{"type":"list","item":'.repeat(table.$nest)
      + '"utf8"' + "}".repeat(table.$nest) + "}}"
    tables.custom = raw ?? { name: "custom", ...table }
    table = "custom"
  }
  fake.reset(...(scenario.script ?? []))
  let result
  try {
    const handle = scenario.call.method === "create_table" ? null : await db.table(table)
    const value = await runCall(handle, scenario.call, db)
    result = { ok: scenario.call.method === "search_status" ? value : typeof value === "number" ? { $int: String(value) } : normal(value) }
  } catch (error) {
    result = { error: describeError(error) }
  }
  const cut = (item) => item.request.operation === "cut"
  return {
    requests: fake.requests.map((item) => cut(item) ? "CUT" : mask(item.raw)),
    shapes: fake.requests.map((item) => cut(item) ? { operation: "cut", items: 0, bytes: 0 } : shape(item)),
    result,
  }
}

export function loadScenarios() {
  return JSON.parse(readFileSync(scenariosUrl, "utf8"))
}

export async function runAll(only) {
  const scenarios = loadScenarios()
  const fake = await Fake.start()
  // 8 MiB requests must not time out on a loaded machine. A scenario whose reply sleeps tests
  // the timeout itself, so it gets a client that gives up after 2 s.
  const db = new Database({ url: fake.url, token: "fixture", timeout: 30 })
  const short = new Database({ url: fake.url, token: "fixture", timeout: 2 })
  const client = (scenario) => scenario.script.some((step) => "sleep" in step) ? short : db
  const transcript = {}
  try {
    for (const scenario of scenarios) {
      if (only === undefined || only.has(scenario.name)) {
        transcript[scenario.name] = await runOne(fake, client(scenario), scenario)
      }
    }
  } finally {
    db.close()
    short.close()
    await fake.close()
  }
  return { scenarios, transcript }
}

function sorted(value) {
  if (Array.isArray(value)) return value.map(sorted)
  if (typeof value !== "object" || value === null) return value
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sorted(value[key])]))
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  process.stdout.write(JSON.stringify(sorted((await runAll()).transcript)))
}

/** Compare one run with the scenario's expectations. Returns a list of messages. */
export function problems(scenario, run) {
  const { expect } = scenario
  const found = []
  const { error } = run.result
  const kinds = Array.isArray(expect.type) ? expect.type : [expect.type]
  if (error?.type.startsWith("RAW:") && !kinds.includes(error.type)) found.push(`raw exception ${error.type}: ${error.message}`)
  if (expect.type !== undefined) {
    const allowed = Array.isArray(expect.type) ? expect.type : [expect.type]
    const actual = error === undefined ? "ok" : error.type
    if (!allowed.includes(actual)) found.push(`result ${actual}, expected ${allowed}: ${error?.message}`)
  }
  const same = (left, right) => JSON.stringify(left) === JSON.stringify(right)
  const items = run.shapes.map((shape) => shape.items)
  const bytes = run.shapes.map((shape) => shape.bytes)
  if (expect.requests !== undefined && run.requests.length !== expect.requests) {
    found.push(`${run.requests.length} requests, expected ${expect.requests}`)
  }
  if (expect.items !== undefined && !same(items, expect.items)) found.push(`items ${items}, expected ${expect.items}`)
  if (expect.bytes !== undefined && !same(bytes, expect.bytes)) found.push(`bytes ${bytes}, expected ${expect.bytes}`)
  if (expect.bytes === undefined && bytes.some((size) => size > 8 * 1024 * 1024)) found.push("a request over 8 MiB was sent")
  if (expect.result_count !== undefined && !same(run.result.ok, { $int: String(expect.result_count) })) {
    found.push(`count ${JSON.stringify(run.result.ok)}, expected ${expect.result_count}`)
  }
  if (expect.result_len !== undefined && (run.result.ok?.length ?? -1) !== expect.result_len) {
    found.push(`${run.result.ok?.length} rows, expected ${expect.result_len}`)
  }
  const texts = run.requests.map((item) => typeof item === "string" ? item : "")
  for (const needle of expect.request_contains ?? []) {
    if (!texts.some((text) => text.includes(needle))) found.push(`no request contains ${needle}`)
  }
  for (const needle of expect.request_lacks ?? []) {
    if (texts.some((text) => text.includes(needle))) found.push(`a request contains ${needle}`)
  }
  if (error?.type === "BatchError") {
    for (const field of ["applied", "outcome"]) {
      if (expect[field] !== undefined && error[field] !== expect[field]) {
        found.push(`${field} ${error[field]}, expected ${expect[field]}`)
      }
    }
    const names = error.outcomes.map((row) => row[1])
    if (expect.outcomes !== undefined && !same(names, expect.outcomes)) found.push(`outcomes ${names}, expected ${expect.outcomes}`)
    if (expect.outcomes_all !== undefined && !names.every((name) => name === expect.outcomes_all)) {
      found.push(`outcomes ${[...new Set(names)]}, expected only ${expect.outcomes_all}`)
    }
    if (expect.keys !== undefined && !same(error.outcomes.map((row) => row[0]), expect.keys)) {
      found.push(`keys ${JSON.stringify(error.outcomes.map((row) => row[0]))}`)
    }
  }
  return found
}

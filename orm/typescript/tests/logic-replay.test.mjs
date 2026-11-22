// Replay the Nabedra logic tests (server/tests/logic/*.nlt) through the TypeScript ORM.
//
// The format and rendering rules are in server/tests/logic/README.md. This replay must render
// results exactly as the Rust runner (server/tests/olap_logic.rs) and the Python replay do. The
// server-backed test runs against NABEDRA_LOGIC_URL and skips with
// "not-run: NABEDRA_LOGIC_URL unset" otherwise. The parser, renderer and sensitivity tests need
// no server.
import assert from "node:assert/strict"
import { createHash, randomBytes } from "node:crypto"
import { readdirSync, readFileSync, statSync } from "node:fs"
import { join, relative, sep } from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { NabedraError, ValidationError } from "../dist/errors.js"
import { Transport, parseJson } from "../dist/transport.js"

const corpus = fileURLToPath(new URL("../../../server/tests/logic/", import.meta.url))
const missingTableId = "ffffffff-ffff-4fff-bfff-ffffffffffff"
const uuidPattern = /[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}/g
// Operations whose integer results the transport decodes as bigint, so a JS number is a float.
const strictIntegers = new Set(["table.get", "query.collect", "query.count", "table.insert_many"])

export class FormatError extends Error {}

export function parse(text) {
  const lines = text.split(/\r?\n/)
  if (lines.at(-1) === "") lines.pop()
  if (!lines[0]?.startsWith("# name:")) throw new FormatError("line 1: the file must start with `# name: <name>`")
  const name = lines[0].slice("# name:".length).trim()
  let engines = ["olap", "oltp"]
  const records = []
  let skip = null
  let i = 1
  while (i < lines.length) {
    const line = lines[i].trimEnd()
    if (line === "" || line.startsWith("#")) {
      i++
      continue
    }
    const fail = (message, at = i) => { throw new FormatError(`line ${at + 1}: ${message}`) }
    if (line.startsWith("require ")) {
      const words = line.slice("require ".length).split(/\s+/).filter(Boolean)
      if (records.length > 0 || skip !== null) fail("require must come before the first record")
      if (words.length === 0 || words.some(w => w !== "olap" && w !== "oltp")) fail("require takes olap and/or oltp")
      engines = words
      i++
      continue
    }
    if (line.startsWith("skipif")) {
      const reason = line.slice("skipif".length).trim()
      if (reason === "" || skip !== null) fail("skipif needs a reason and one record")
      skip = { at: i, reason }
      i++
      continue
    }
    const words = line.split(/\s+/).filter(Boolean)
    let kind
    if (words.length === 2 && words[0] === "statement" && words[1] === "ok") {
      kind = { type: "ok" }
    } else if (words[0] === "statement" && words[1] === "error") {
      const pattern = line.slice("statement error".length).trim()
      if (pattern === "") fail("statement error needs a code or message substring")
      kind = { type: "error", pattern }
    } else if (words[0] === "query" && words.length >= 2) {
      if (words[1] !== "rowsort" && words[1] !== "nosort") fail(`unknown sort mode ${JSON.stringify(words[1])}`)
      kind = { type: "query", sort: words[1], hash: false, label: null }
      for (const word of words.slice(2)) {
        if (word === "hash" && !kind.hash) kind.hash = true
        else if (word.startsWith("label-") && kind.label === null && word.length > 6) kind.label = word.slice(6)
        else fail(`unknown query option ${JSON.stringify(word)}`)
      }
    } else if (words.length === 1 && words[0] === "explain") {
      kind = { type: "explain" }
    } else {
      fail(`unknown directive ${JSON.stringify(line)}`)
    }
    if (skip !== null && skip.at + 1 !== i) fail("skipif must be on the line right before its record")
    const header = i
    i++
    const start = i
    const takesResult = kind.type === "query" || kind.type === "explain"
    while (i < lines.length && lines[i].trim() !== "" && !(takesResult && lines[i] === "----")) {
      if (lines[i] === "----") fail("statements have no ---- block")
      i++
    }
    if (i === start) fail("record has no operation", header)
    const command = lines.slice(start, i).join("\n")
    let expected = null
    if (i < lines.length && lines[i] === "----") {
      i++
      const first = i
      while (i < lines.length && lines[i] !== "") i++
      expected = lines.slice(first, i)
    }
    if (expected === null && (kind.type === "explain" || (kind.type === "query" && kind.label === null))) {
      fail("query needs a ---- block unless it has a label", header)
    }
    if (kind.hash && expected !== null && expected.length !== 1) {
      fail("hash expects one `N values hashing to <hex>` line", header)
    }
    records.push({ kind, line: header + 1, skip: skip?.reason ?? null, command, expected })
    skip = null
  }
  if (skip !== null) throw new FormatError("skipif at the end of the file has no record")
  return { name, engines, records }
}

export function renderString(value) {
  if (value === "") return "(empty)"
  const named = { "\\": "\\\\", "\t": "\\t", "\n": "\\n", "\r": "\\r" }
  return value.replace(/[\\\x00-\x1f\x7f]/g, c => named[c] ?? "\\x" + c.charCodeAt(0).toString(16).padStart(2, "0"))
}

function formatFloat(sign, digits, exp) {
  digits = digits.replace(/0+$/, "") || "0"
  if (-7 < exp && exp < 21) {
    if (exp >= 0) {
      const intLen = exp + 1
      const [whole, frac] = digits.length > intLen
        ? [digits.slice(0, intLen), digits.slice(intLen)]
        : [digits.padEnd(intLen, "0"), "0"]
      return `${sign}${whole}.${frac}`
    }
    return `${sign}0.${"0".repeat(-exp - 1)}${digits}`
  }
  const rest = digits.length > 1 ? "." + digits.slice(1) : ""
  return `${sign}${digits[0]}${rest}e${exp}`
}

export function canonicalFloat(value) {
  if (value === 0) return "0.0"
  const sign = value < 0 ? "-" : ""
  // toExponential() without an argument gives the shortest round-trip digits.
  const [mantissa, exp] = Math.abs(value).toExponential().split("e")
  return formatFloat(sign, mantissa.replace(".", ""), Number(exp))
}

export function renderValue(value, strict) {
  if (value === null || value === undefined) return "NULL"
  if (value === true) return "true"
  if (value === false) return "false"
  if (typeof value === "bigint") return value.toString()
  if (typeof value === "number") {
    return !strict && Number.isInteger(value) ? String(value) : canonicalFloat(value)
  }
  if (typeof value === "string") return renderString(value)
  throw new FormatError(`cannot render ${String(value)}`)
}

const utf8Compare = (a, b) => Buffer.compare(Buffer.from(a, "utf8"), Buffer.from(b, "utf8"))

function ordered(rows, sort) {
  return sort === "rowsort" ? [...rows].sort((a, b) => utf8Compare(a.join("\t"), b.join("\t"))) : [...rows]
}

export function hashLine(rows, sort) {
  const values = ordered(rows, sort).flat()
  const digest = createHash("sha256").update(values.map(v => v + "\n").join(""), "utf8").digest("hex")
  return `${values.length} values hashing to ${digest}`
}

export class Translator {
  constructor(prefix) {
    this.prefix = prefix
    this.tables = new Map()
  }

  translate(command) {
    let payload
    try {
      // Keep every number as its source token, so 2.0 stays a float on the wire.
      payload = parseJson(command, { numberToken: token => JSON.rawJSON(token) })
    } catch (error) {
      throw new FormatError(`invalid operation JSON: ${error.message}`)
    }
    if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
      throw new FormatError("operation must be a JSON object")
    }
    const op = payload.op
    if (typeof op !== "string") throw new FormatError("operation needs a string `op`")
    delete payload.op
    if (typeof payload.table === "string") {
      const table = payload.table
      delete payload.table
      payload.table_id = this.tables.get(table) ?? missingTableId
    } else {
      delete payload.table
    }
    if ((op === "table.create" || op === "table.describe") && typeof payload.name === "string") {
      payload.name = this.prefix + payload.name
    }
    if (op.startsWith("query.") && !Object.hasOwn(payload, "query")) {
      const query = {}
      for (const key of ["select", "filter", "order_by", "limit"]) {
        query[key] = Object.hasOwn(payload, key) ? payload[key] : null
        delete payload[key]
      }
      payload.query = query
    }
    return [op, payload]
  }

  observe(op, data) {
    if (op === "table.create" && typeof data?.name === "string" && typeof data?.id === "string") {
      this.tables.set(this.logical(data.name), data.id)
    }
  }

  logical(name) {
    return name.startsWith(this.prefix) ? name.slice(this.prefix.length) : name
  }

  descriptorRow(d) {
    const schema = Object.keys(d.schema).sort(utf8Compare).map(c => `${c}:${d.schema[c]}`).join(",")
    return [renderString(this.logical(d.name)), d.key, schema]
  }

  render(op, payload, data) {
    const strict = strictIntegers.has(op)
    const row = (object) => {
      if (object === null || typeof object !== "object" || Array.isArray(object)) {
        throw new FormatError("result row is not an object")
      }
      const select = op === "query.collect" ? payload.query?.select : null
      const columns = Array.isArray(select) ? select : Object.keys(object).sort(utf8Compare)
      return columns.map(c => renderValue(Object.hasOwn(object, c) ? object[c] : null, strict))
    }
    if (op === "table.create" || op === "table.describe") return [this.descriptorRow(data)]
    if (op === "table.list") {
      return data.filter(d => d.name.startsWith(this.prefix)).map(d => this.descriptorRow(d))
        .sort((a, b) => utf8Compare(a[0], b[0]))
    }
    if (op === "query.collect") return data.map(row)
    if (op === "table.get") return data === null ? [] : [row(data)]
    if (op === "query.explain") {
      const text = (this.prefix ? data.replaceAll(this.prefix, "") : data).replace(uuidPattern, "<uuid>")
      return text.split("\n").map(line => [renderString(line)])
    }
    if (data === null) return []
    return [[renderValue(data, strict)]]
  }
}

/** execute(op, payload) resolves to data or rejects with a NabedraError. */
export async function runFile(path, parsed, execute, prefix) {
  const tr = new Translator(prefix)
  const labels = new Map()
  let assertions = 0
  const skipped = []
  const failures = []
  for (const record of parsed.records) {
    const at = `${path}:${record.line}`
    if (record.skip !== null) {
      skipped.push(`${at} skipif ${record.skip}`)
      continue
    }
    const [op, payload] = tr.translate(record.command)
    let data = null
    let error = null
    try {
      data = await execute(op, payload)
    } catch (caught) {
      if (!(caught instanceof NabedraError)) throw caught
      error = caught
    }
    if (error === null) tr.observe(op, data)
    assertions++
    const { kind } = record
    if (kind.type === "ok") {
      if (error !== null) failures.push(`${at}: expected success\n  actual error: ${error.code}: ${error.message}`)
      continue
    }
    if (kind.type === "error") {
      if (error === null) {
        failures.push(`${at}: expected error ${JSON.stringify(kind.pattern)}\n  actual: success`)
      } else if (error.code !== kind.pattern && !error.message.includes(kind.pattern)) {
        failures.push(`${at}: expected error ${JSON.stringify(kind.pattern)}\n  actual error: ${error.code}: ${error.message}`)
      }
      continue
    }
    if (error !== null) {
      failures.push(`${at}: expected success\n  actual error: ${error.code}: ${error.message}`)
      continue
    }
    const rows = tr.render(op, payload, data)
    const sort = kind.sort ?? "nosort"
    const actual = kind.hash ? [hashLine(rows, sort)] : ordered(rows, sort).map(r => r.join("\t"))
    if (kind.label) {
      const first = labels.get(kind.label)
      if (first === undefined) labels.set(kind.label, { line: record.line, actual })
      else if (JSON.stringify(first.actual) !== JSON.stringify(actual)) {
        failures.push(`${at}: label-${kind.label} disagrees with line ${first.line}\n`
          + `  expected: ${JSON.stringify(first.actual)}\n  actual: ${JSON.stringify(actual)}`)
      }
    }
    let expected = record.expected
    if (expected !== null && sort === "rowsort" && !kind.hash) expected = [...expected].sort(utf8Compare)
    if (expected !== null && JSON.stringify(expected) !== JSON.stringify(actual)) {
      failures.push(`${at}: wrong result\n  expected: ${JSON.stringify(expected)}\n  actual: ${JSON.stringify(actual)}`)
    }
  }
  return { assertions, skipped, failures }
}

function corpusFiles(dir = corpus) {
  const out = []
  for (const entry of readdirSync(dir).sort()) {
    const path = join(dir, entry)
    if (statSync(path).isDirectory()) out.push(...corpusFiles(path))
    else if (entry.endsWith(".nlt")) out.push(path)
  }
  return out.sort((a, b) => utf8Compare(a, b))
}

function fakeExecute(responses) {
  const queue = [...responses]
  return async (op) => {
    const [expectedOp, result] = queue.shift()
    assert.equal(op, expectedOp)
    if (result instanceof NabedraError) throw result
    return result
  }
}

const cannedFile = `# name: canned
require olap

statement ok
{"op":"table.create","name":"t","schema":{"id":"int64","v":"utf8","f":"float64"},"key":"id","format":"parquet"}

query nosort
{"op":"query.collect","table":"t","order_by":{"column":"v","desc":false}}
----
1.5\t2\ta
2.0\t1\tb
`
const descriptor = {
  id: "11111111-1111-4111-8111-111111111111", name: "t", key: "id", format: "parquet",
  schema: { id: "int64", v: "utf8", f: "float64" },
}

test("corpus parses", () => {
  const files = corpusFiles()
  assert.ok(files.length >= 25, `${files.length} files`)
  for (const path of files) parse(readFileSync(path, "utf8"))
})

test("canonical floats match the Rust runner", () => {
  const cases = [[0, "0.0"], [-0, "0.0"], [2, "2.0"], [-0.5, "-0.5"], [1.5, "1.5"], [100, "100.0"],
    [0.001, "0.001"], [1e-7, "1e-7"], [1.5e-7, "1.5e-7"], [1e-6, "0.000001"],
    [1e20, "100000000000000000000.0"], [1e21, "1e21"], [123456.789, "123456.789"],
    [0.1 + 0.2, "0.30000000000000004"], [1.7976931348623157e308, "1.7976931348623157e308"],
    [2.2250738585072014e-308, "2.2250738585072014e-308"]]
  for (const [value, text] of cases) assert.equal(canonicalFloat(value), text, String(value))
})

test("rendering and translation", () => {
  assert.equal(renderString(""), "(empty)")
  assert.equal(renderString("a\tb\\c\nd"), "a\\tb\\\\c\\nd")
  assert.equal(renderString("\x00x\x1f\x7f"), "\\x00x\\x1f\\x7f")
  assert.equal(renderValue(3n, true), "3")
  assert.equal(renderValue(3, true), "3.0")
  assert.equal(renderValue(3, false), "3")
  const tr = new Translator("p_")
  tr.observe("table.create", { name: "p_t", id: "abc" })
  const [op, payload] = tr.translate('{"op":"query.count","table":"t","limit":3,"x":2.0}')
  assert.equal(op, "query.count")
  assert.equal(JSON.stringify(payload),
    '{"x":2.0,"table_id":"abc","query":{"select":null,"filter":null,"order_by":null,"limit":3}}')
  assert.equal(tr.translate('{"op":"table.get","table":"x","key":1}')[1].table_id, missingTableId)
  assert.equal(tr.translate('{"op":"table.describe","name":"t"}')[1].name, "p_t")
  assert.deepEqual(tr.render("query.explain", {}, "scan p_t (11111111-1111-4111-8111-111111111111)"),
    [["scan t (<uuid>)"]])
})

test("canned correct order passes", async () => {
  const execute = fakeExecute([["table.create", descriptor],
    ["query.collect", [{ id: 2n, v: "a", f: 1.5 }, { id: 1n, v: "b", f: 2 }]]])
  const { assertions, failures } = await runFile("canned.nlt", parse(cannedFile), execute, "")
  assert.deepEqual([assertions, failures], [2, []])
})

test("planted sort-order difference fails", async () => {
  // The server answer is right; this replay planted a row-order difference.
  const execute = fakeExecute([["table.create", descriptor],
    ["query.collect", [{ id: 1n, v: "b", f: 2 }, { id: 2n, v: "a", f: 1.5 }]]])
  const { failures } = await runFile("canned.nlt", parse(cannedFile), execute, "")
  assert.equal(failures.length, 1, failures.join("\n"))
  assert.match(failures[0], /canned\.nlt:7: wrong result/)
})

test("label disagreement and error mismatch fail", async () => {
  const text = '# name: x\n\nquery rowsort label-a\n{"op":"query.count","table":"t"}\n\n'
    + 'query rowsort label-a\n{"op":"query.count","table":"t"}\n\n'
    + 'statement error not_found\n{"op":"query.count","table":"t"}\n'
  const execute = fakeExecute([["query.count", 1n], ["query.count", 2n],
    ["query.count", new ValidationError("bad", { code: "validation_error" })]])
  const { failures } = await runFile("x.nlt", parse(text), execute, "")
  assert.equal(failures.length, 2, failures.join("\n"))
})

test("parser rejects malformed files", () => {
  for (const bad of ["statement ok\n{}\n", "# name: x\nquery nosort\n{}\n", "# name: x\nquery sideways\n{}\n----\n",
    "# name: x\nstatement ok\n{}\n----\n", "# name: x\nskipif\nstatement ok\n{}\n", "# name: x\nstatement ok\n\n",
    "# name: x\nquery nosort hash\n{}\n----\na\nb\n", "# name: x\nstatement ok\n{}\n\nrequire olap\n",
    "# name: x\nbogus\n"]) {
    assert.throws(() => parse(bad), FormatError, JSON.stringify(bad))
  }
})

test("replay the Rust runner's recorded server responses", async (t) => {
  const root = process.env.NABEDRA_LOGIC_TRANSCRIPTS
  if (!root) {
    t.skip("not-run: NABEDRA_LOGIC_TRANSCRIPTS unset")
    return
  }
  let total = 0
  const failures = []
  for (const path of corpusFiles()) {
    const rel = relative(corpus, path).split(sep).join("/")
    const parsed = parse(readFileSync(path, "utf8"))
    if (!parsed.engines.includes("olap")) continue
    const log = JSON.parse(readFileSync(join(root, rel + ".json"), "utf8"))
    const execute = async (op) => {
      const entry = log.shift()
      assert.equal(entry.op, op)
      if (entry.error) throw new NabedraError(entry.error.message, { code: entry.error.code })
      // Decode as the transport does: bigint integers for the strict operations.
      return parseJson(entry.data, { integers: strictIntegers.has(op) ? "bigint" : undefined })
    }
    const result = await runFile("tests/logic/" + rel, parsed, execute, "")
    assert.deepEqual(log, [], rel)
    total += result.assertions
    failures.push(...result.failures)
  }
  t.diagnostic(`logic transcripts (typescript): ${total - failures.length} passed; ${failures.length} failed`)
  assert.ok(total > 0)
  assert.deepEqual(failures, [], failures.join("\n"))
})

test("replay corpus over HTTP", async (t) => {
  const url = process.env.NABEDRA_LOGIC_URL
  if (!url) {
    t.skip("not-run: NABEDRA_LOGIC_URL unset")
    return
  }
  const token = process.env.NABEDRA_LOGIC_TOKEN || process.env.NABEDRA_TOKEN
  const engine = process.env.NABEDRA_LOGIC_ENGINE || "olap"
  const transport = new Transport(url, token)
  const execute = (op, payload) => transport.request(op, payload)
  let total = 0
  const skipped = []
  const failures = []
  try {
    for (const path of corpusFiles()) {
      const rel = relative(corpus, path).split(sep).join("/")
      const parsed = parse(readFileSync(path, "utf8"))
      if (!parsed.engines.includes(engine)) {
        skipped.push(`${rel} requires ${parsed.engines.join(" ")}`)
        continue
      }
      const prefix = "lt" + randomBytes(4).toString("hex") + "_"
      const result = await runFile("tests/logic/" + rel, parsed, execute, prefix)
      total += result.assertions
      skipped.push(...result.skipped)
      failures.push(...result.failures)
    }
  } finally {
    transport.close()
  }
  for (const line of skipped) t.diagnostic(`skipped: ${line}`)
  t.diagnostic(`logic replay (typescript): ${total - failures.length} passed; ${failures.length} failed; ${skipped.length} skipped`)
  assert.deepEqual(failures, [], failures.join("\n"))
})

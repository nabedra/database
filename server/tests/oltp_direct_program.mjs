// D01 program driver for the TypeScript ORM (T9). Same protocol and canonical output as
// `oltp_direct_program.py`; see that file. Run after `npm run build` in orm/typescript.
import { createInterface } from "node:readline"
import { readFile } from "node:fs/promises"
import { parseArgs } from "node:util"

const { values: args } = parseArgs({
  options: {
    url: { type: "string" },
    program: { type: "string" },
    table: { type: "string" },
    database: { type: "string", default: "test" },
    "ca-file": { type: "string" },
    orm: { type: "string" },
  },
})
const { Database, NabedraError } = await import(args.orm)

function decode(value) {
  if (Array.isArray(value)) return value.map(decode)
  if (value !== null && typeof value === "object") {
    const keys = Object.keys(value)
    if (keys.length === 1 && keys[0] === "$i") return BigInt(value.$i)
    if (keys.length === 1 && keys[0] === "$f") return Number(value.$f)
    return Object.fromEntries(keys.map(key => [key, decode(value[key])]))
  }
  return value
}

function canonical(value) {
  if (value === undefined || value === null) return null
  if (typeof value === "boolean" || typeof value === "string") return value
  if (typeof value === "bigint") return { $i: value.toString() }
  if (typeof value === "number") {
    if (Object.is(value, -0)) return { $f: "-0" }
    if (Number.isInteger(value) && Math.abs(value) < 2 ** 53) return { $f: String(value) }
    return { $f: String(value) }
  }
  if (Array.isArray(value)) return value.map(canonical)
  if (typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]))
  }
  throw new TypeError(`unexpected ORM value ${typeof value}`)
}

async function step(table, operation, argv) {
  switch (operation) {
    case "insert": return table.insert(argv[0])
    case "insert_many": return table.insertMany(argv[0])
    case "get": return table.get(argv[0])
    case "update": return table.update(argv[0], argv[1])
    case "upsert": return table.upsert(argv[0])
    case "delete": return table.delete(argv[0])
    default: throw new Error(`unknown program operation ${operation}`)
  }
}

const program = JSON.parse(await readFile(args.program, "utf8"))
const db = new Database({
  url: args.url, token: process.env.NABEDRA_TEST_TOKEN, database: args.database,
  caFile: args["ca-file"] ?? null, timeout: 30,
})
const table = await db.table(args.table)
const lines = createInterface({ input: process.stdin })[Symbol.asyncIterator]()
console.log(JSON.stringify({ ready: true }))
const results = []
for (const section of program.sections) {
  await lines.next()
  for (const [operation, ...raw] of section.steps) {
    let outcome
    try {
      const value = await step(table, operation, decode(raw))
      outcome = { ok: operation === "insert_many" ? { $count: String(value) } : canonical(value) }
    } catch (error) {
      if (!(error instanceof NabedraError)) throw error
      outcome = { error: error.constructor.name, code: error.code, outcome: error.outcome }
    }
    results.push({ section: section.name, operation, ...outcome })
  }
  console.log(JSON.stringify({ checkpoint: section.name }))
}
console.log(JSON.stringify({ results }))
process.exit(0)

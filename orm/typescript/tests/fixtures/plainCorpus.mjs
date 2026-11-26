// Records the request bytes of every operation that existed before vector search.
//   node plainCorpus.mjs                      prints the corpus of the package that "nabedra" resolves to
//   NABEDRA_SDK=/path/dist/index.js node ...  records another build
// plainCorpus.test.mjs compares it with fixtures/plainCorpus.json, recorded on the base branch.
import { fileURLToPath, pathToFileURL } from "node:url"
import { Fake, ok } from "./fakeServer.mjs"

const { Database, col } = await import(process.env.NABEDRA_SDK ? pathToFileURL(process.env.NABEDRA_SDK).href : "nabedra")

const id = "00000000-0000-0000-0000-000000000022"
const plain = { id, name: "plain", key: "id", format: "parquet", schema: { id: "utf8", n: "int64", f: "float64", b: "bool" } }
const floats = [1.0, -0.0, 0.1, 1e16, 1e-7, 5e-324, 1.7976931348623157e308, 123456789.123456789, 2.5e-5, 1e22, 1e21, 1e15]
const ints = [0n, -1n, 2n ** 53n, 2n ** 53n + 1n, 2n ** 63n - 1n, -(2n ** 63n)]
const texts = ["", "a", "é", "é", "\u{1f600}", "\u0000", "\u001f", "\u007f", " ", "\"\\/\b\f\n\r\t", "x".repeat(5000)]

const descriptorFor = (request) => ok(JSON.stringify({
  id: "00000000-0000-0000-0000-000000000040", name: request.payload.name, schema: request.payload.schema,
  key: request.payload.key, format: request.payload.format,
}))

function* calls(db, table) {
  yield ["create all scalars", () => db.createTable("t", { id: String, n: BigInt, f: Number, b: Boolean }, "id")]
  yield ["create int key", () => db.createTable("t", { id: BigInt, name: String }, "id")]
  yield ["create parquet", () => db.createTable("t", { id: String }, "id", { format: "parquet" })]
  yield ["create unicode column", () => db.createTable("t", { id: String, "café": String }, "id")]
  yield ["table", () => db.table("plain")]
  yield ["tables", () => db.tables()]
  yield ["drop", () => table.drop()]
  for (const [index, text] of texts.entries()) {
    const row = { id: text, n: 1n, f: 1.0, b: true }
    yield [`insert text ${index}`, () => table.insert(row)]
    yield [`upsert text ${index}`, () => table.upsert(row)]
    yield [`get text ${index}`, () => table.get(text)]
    yield [`delete text ${index}`, () => table.delete(text)]
    yield [`update text ${index}`, () => table.update(text, { n: 2n })]
  }
  for (const [index, number] of floats.entries()) {
    const row = { id: "a", n: 1n, f: number, b: false }
    yield [`insert float ${index}`, () => table.insert(row)]
    yield [`upsert float ${index}`, () => table.upsert(row)]
    yield [`update float ${index}`, () => table.update("a", { f: number })]
    yield [`where float ${index}`, () => table.where(col("f").gt(number)).collect()]
  }
  for (const [index, number] of ints.entries()) {
    const row = { id: "a", n: number, f: 1.0, b: false }
    yield [`insert int ${index}`, () => table.insert(row)]
    yield [`upsert int ${index}`, () => table.upsert(row)]
    yield [`update int ${index}`, () => table.update("a", { n: number })]
    yield [`where int ${index}`, () => table.where(col("n").eq(number)).collect()]
  }
  const rows = Array.from({ length: 50 }, (_, i) => ({ id: `k${i}`, n: BigInt(i), f: i / 3, b: i % 2 === 0 }))
  yield ["insert_many", () => table.insertMany(rows)]
  yield ["insert_many one", () => table.insertMany(rows.slice(0, 1))]
  yield ["insert_many unicode", () => table.insertMany(texts.map((text) => ({ id: text, n: 1n, f: 1.0, b: true })))]
  yield ["insert_many floats", () => table.insertMany(floats.map((number, i) => ({ id: `f${i}`, n: 1n, f: number, b: true })))]
  yield ["insert_many generator", () => table.insertMany((function* () { for (const row of rows) yield { ...row } })())]
  yield ["update many fields", () => table.update("a", { n: 2n, f: 3.5, b: false })]
  yield ["collect", () => table.query().collect()]
  yield ["collect where", () => table.where(col("n").ge(3n)).collect()]
  yield ["collect kwargs", () => table.where(col("n").eq(3n).and(col("b").eq(true))).collect()]
  yield ["collect and or not", () => table.where(col("n").gt(1n).and(col("f").lt(2.5)).or(col("b").eq(true).not())).collect()]
  for (const op of ["eq", "ne", "gt", "ge", "lt", "le"]) yield [`where ${op}`, () => table.where(col("n")[op](5n)).collect()]
  yield ["where string", () => table.where(col("id").eq("café")).collect()]
  yield ["where bool", () => table.where(col("b").ne(false)).collect()]
  yield ["select", () => table.query().select("id", "f").collect()]
  yield ["order asc", () => table.query().orderBy("n").collect()]
  yield ["order desc", () => table.query().orderBy("n", true).limit(7).collect()]
  yield ["limit 0", () => table.query().limit(0).collect()]
  yield ["count", () => table.where(col("n").gt(1n)).count()]
  yield ["count limit", () => table.query().limit(5).count()]
  yield ["explain", () => table.where(col("n").gt(1n)).orderBy("n").explain()]
  yield ["deep", () => table.where(col("n").gt(1n).and(col("n").lt(9n)).and(col("f").gt(0.5).and(col("b").eq(true)))).collect()]
}

function reply(name) {
  if (name.startsWith("create")) return descriptorFor
  if (name === "table") return ok(JSON.stringify(plain))
  if (name === "tables") return ok(JSON.stringify([plain]))
  if (name.startsWith("drop")) return ok("null")
  if (name.startsWith("insert_many")) return (request) => ok(String(request.payload.rows.length))
  if (name.startsWith("insert") || name.startsWith("upsert")) return ok("null")
  if (name.startsWith("update") || name.startsWith("delete")) return ok("true")
  if (name.startsWith("get")) return ok("null")
  if (name.startsWith("count")) return ok("0")
  if (name.startsWith("explain")) return ok('"plan"')
  return ok("[]")
}

export async function record() {
  const fake = await Fake.start()
  const db = new Database({ url: fake.url, token: "fixture", timeout: 3 })
  const corpus = {}
  try {
    const table = await db.table("plain")
    for (const [name, call] of calls(db, table)) {
      fake.reset(reply(name))
      let failure = null
      try {
        await call()
      } catch (error) {
        failure = `${error.constructor.name}: ${error.message}`
      }
      corpus[name] = {
        requests: fake.requests.map(({ raw }) => raw.toString("utf8").replace(/[0-9a-f]{8}-[0-9a-f-]{27}/, "ID")),
        failure,
      }
    }
  } finally {
    db.close()
    await fake.close()
  }
  return corpus
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const corpus = await record()
  process.stdout.write(JSON.stringify(Object.fromEntries(Object.keys(corpus).sort().map((key) => [key, corpus[key]]))))
}

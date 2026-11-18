import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { createServer } from "node:http"
import { once } from "node:events"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"
import { Database, Expr, LimitExceededError, ProtocolError, Query, UnknownOutcomeError, ValidationError, col } from "nabedra"
import { parseJson } from "../dist/transport.js"

test("T06/U02 ORM wire contract, validation and batch bounds", { timeout: 30_000 }, async (t) => {
  const requests = []
  const tableId = "12345678-1234-1234-1234-123456789aBC"
  const schema = { id: String, age: BigInt, score: Number, active: Boolean }
  const descriptor = { id: tableId, name: "users", schema: { id: "utf8", age: "int64", score: "float64", active: "bool" }, key: "id", format: "parquet" }
  const row = { id: "é", age: 9223372036854775807n, score: -0, active: true }
  const rowJson = '{"id":"é","age":9223372036854775807,"score":-0.0,"active":true}'
  let override
  const app = createServer(async (req, res) => {
    const parts = []
    for await (const chunk of req) parts.push(chunk)
    const raw = Buffer.concat(parts)
    const { request_id, operation, payload } = parseJson(raw, { integers: "bigint" })
    requests.push({ operation, payload, bytes: raw.byteLength })
    const data = override ?? ({
      "table.create": JSON.stringify(descriptor),
      "table.describe": JSON.stringify(descriptor),
      "table.list": JSON.stringify([descriptor]),
      "table.insert": "null",
      "table.insert_many": String(payload.rows?.length),
      "table.get": rowJson,
      "table.update": "true",
      "table.upsert": "null",
      "table.delete": "false",
      "query.collect": '[{"id":"é","age":9223372036854775807}]',
      "query.count": "1",
      "query.explain": '"plan"',
      "table.drop": "null",
    })[operation]
    const body = `{"request_id":"${request_id}","data":${data}}`
    res.writeHead(200, { "content-type": "application/json", "content-length": Buffer.byteLength(body) })
    res.end(body)
  })
  app.listen(0, "127.0.0.1")
  await once(app, "listening")
  const url = `http://127.0.0.1:${app.address().port}`
  t.after(async () => {
    app.close()
    await once(app, "close")
  })
  const db = new Database({ url, token: "test" })
  const users = await db.createTable("users", schema, "id")
  assert.equal((await db.table("users")).id, tableId)
  assert.deepEqual((await db.tables()).map((value) => value.id), [tableId])
  assert.equal(await users.insert(row), undefined)
  assert.equal(await users.insertMany([row]), 1)
  assert.deepEqual(await users.get("é"), row)
  assert.equal(await users.update("é", { score: 3 }), true)
  assert.equal(await users.upsert(row), undefined)
  assert.equal(await users.delete("é"), false)
  const query = users.where(col("age").ge(18n), { active: true }).select("id", "age").orderBy("age", true).limit(2)
  assert.deepEqual(await query.collect(), [{ id: "é", age: 9223372036854775807n }])
  assert.equal(await query.count(), 1)
  assert.equal(await query.explain(), "plan")
  assert.equal(await users.drop(), undefined)
  const ts = requests.splice(0)

  assert.deepEqual(ts.map(({ operation }) => operation), [
    "table.create", "table.describe", "table.list", "table.insert", "table.insert_many", "table.get",
    "table.update", "table.upsert", "table.delete", "query.collect", "query.count", "query.explain", "table.drop",
  ])
  assert.deepEqual(ts[6].payload.values, { score: 3 })
  const pythonBinary = process.env.NABEDRA_TEST_PYTHON
  await t.test("matches the Python operation envelopes", { skip: !pythonBinary }, async () => {
    const workload = `
import sys
from nabedra import Database, col
db = Database(url=sys.argv[1], token="test")
users = db.create_table("users", {"id": str, "age": int, "score": float, "active": bool}, "id")
row = {"id": "é", "age": 9223372036854775807, "score": -0.0, "active": True}
assert db.table("users").id == users.id
assert [t.id for t in db.tables()] == [users.id]
assert users.insert(row) is None
assert users.insert_many([row]) == 1
assert users.get("é") == row
assert users.update("é", {"score": 3.0}) is True
assert users.upsert(row) is None
assert users.delete("é") is False
query = users.where(col("age") >= 18, active=True).select("id", "age").order_by("age", True).limit(2)
assert query.collect() == [{"id": "é", "age": 9223372036854775807}]
assert query.count() == 1
assert query.explain() == "plan"
assert users.drop() is None
`
    await promisify(execFile)(pythonBinary, ["-c", workload, url], {
      env: { ...process.env, PYTHONPATH: fileURLToPath(new URL("../../python/src/", import.meta.url)) },
      timeout: 15_000,
    })
    assert.deepEqual(requests.splice(0), ts)
  })

  const original = users.schema
  original.age = String
  assert.equal(users.schema.age, BigInt)
  assert.throws(() => { users.id = "changed" }, TypeError)
  assert.throws(() => new Expr(), TypeError)
  assert.throws(() => col("id\n"), ValidationError)
  assert.throws(() => users.where(col("missing").eq("bad")), ValidationError)
  assert.throws(() => users.where(col("active").ge(true)), ValidationError)
  assert.throws(() => users.where(col("age").eq(1)), ValidationError)
  assert.throws(() => users.insert({ ...row, age: 2n ** 63n }), ValidationError)
  assert.throws(() => users.insert({ ...row, score: Infinity }), ValidationError)
  assert.throws(() => users.update("é", { id: "changed" }), ValidationError)
  assert.throws(() => users.query().limit(2 ** 53), ValidationError)
  const safe = users.query().limit(2n ** 53n)
  assert.equal(await safe.count(), 1)
  assert.equal(requests.pop().payload.query.limit, 2n ** 53n)
  for (const bad of [true, NaN, -1, 0.5]) assert.throws(() => users.query().limit(bad), ValidationError)
  const base = new Query(users).where({ active: true })
  const branch = base.where(col("age").ge(0n), { id: "é" }).select("age")
  await base.explain()
  await branch.explain()
  assert.deepEqual(requests.splice(0).map(({ payload }) => payload.query), [
    { select: null, filter: { op: "eq", column: "active", value: true }, order_by: null, limit: null },
    { select: ["age"], filter: { op: "and", args: [
      { op: "eq", column: "active", value: true },
      { op: "ge", column: "age", value: 0n },
      { op: "eq", column: "id", value: "é" },
    ] }, order_by: null, limit: null },
  ])
  let deep = col("active").eq(true)
  for (let i = 0; i < 31; ++i) deep = deep.not()
  assert.doesNotThrow(() => users.where(deep))
  assert.throws(() => users.where(deep.not()), ValidationError)
  assert.throws(() => users.where(deep).where({ active: true }), ValidationError)
  assert.equal(requests.length, 0)

  override = '{"id":"é","age":9223372036854775807,"score":3,"active":true}'
  await assert.rejects(users.get("é"), ProtocolError)
  override = '{"id":"é","age":1.0,"score":3.0,"active":true}'
  await assert.rejects(users.get("é"), ProtocolError)
  override = "1.0"
  await assert.rejects(users.insertMany([row]), UnknownOutcomeError)
  await assert.rejects(users.query().count(), ProtocolError)
  override = "[{\"id\":\"a\",\"age\":1,\"score\":1.0,\"active\":true}]"
  await assert.rejects(users.query().limit(0).collect(), ProtocolError)
  override = undefined
  assert.equal(requests.splice(0).length, 5)

  assert.equal(await users.insertMany([]), 0)
  assert.deepEqual(requests.pop().payload.rows, [])
  assert.equal(await users.insertMany(Array.from({ length: 10_000 }, () => row)), 10_000)
  assert.equal(requests.pop().payload.rows.length, 10_000)
  let consumed = 0
  function* forever() {
    while (true) {
      ++consumed
      yield row
    }
  }
  assert.throws(() => users.insertMany(forever()), LimitExceededError)
  assert.equal(consumed, 10_001)
  const byteLimit = 8 * 1024 * 1024
  assert.equal(await users.insertMany([{ ...row, id: "" }]), 1)
  const budget = byteLimit - requests.pop().bytes
  const text = "é".repeat(Math.floor(budget / 2)) + "a".repeat(budget % 2)
  assert.equal(await users.insertMany([{ ...row, id: text }]), 1)
  assert.equal(requests.pop().bytes, byteLimit)
  assert.throws(() => users.insertMany([{ ...row, id: text + "a" }]), LimitExceededError)
  assert.equal(requests.length, 0)
})

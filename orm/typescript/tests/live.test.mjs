import assert from "node:assert/strict"
import { execFile, spawn } from "node:child_process"
import { once } from "node:events"
import { mkdtemp, rm } from "node:fs/promises"
import { createServer } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { test } from "node:test"
import { setTimeout as sleep } from "node:timers/promises"
import { promisify } from "node:util"
import { randomUUID } from "node:crypto"
import { AuthenticationError, ConflictError, Database, NotFoundError, col } from "nabedra"

const binary = process.env.NABEDRA_TEST_SERVER_BINARY
const python = process.env.NABEDRA_TEST_PYTHON

async function ephemeralPort() {
  const server = createServer()
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const { port } = server.address()
  server.close()
  await once(server, "close")
  return port
}

async function startServer(t) {
  const data = await mkdtemp(join(tmpdir(), "nabedra-typescript-"))
  const port = await ephemeralPort()
  const token = randomUUID() + randomUUID()
  const database = "typescript_test"
  const url = `http://127.0.0.1:${port}`
  const child = spawn(binary, ["serve", "--data-dir", data, "--database", database,
    "--port", String(port), "--workers", "2"], {
    stdio: ["ignore", "ignore", "pipe"],
    env: { ...process.env, NABEDRA_TOKEN: token },
  })
  let diagnostic = ""
  child.stderr.setEncoding("utf8").on("data", chunk => { diagnostic = (diagnostic + chunk).slice(-2048) })
  const exit = new Promise(resolve => {
    child.once("close", resolve)
  })
  let startupError
  child.once("error", error => { startupError = error })
  let stopping
  const stop = () => stopping ??= (async () => {
    child.kill("SIGTERM")
    const kill = setTimeout(() => child.kill("SIGKILL"), 5_000)
    try {
      await exit
    } finally {
      clearTimeout(kill)
      await rm(data, { recursive: true, force: true })
    }
  })()
  t.after(stop)
  const deadline = performance.now() + 30_000
  while (performance.now() < deadline) {
    if (startupError || child.exitCode !== null) {
      throw new Error(`OLTP fixture stopped during startup. ${startupError?.message ?? diagnostic}`)
    }
    try {
      const response = await fetch(`${url}/health/ready`, { signal: AbortSignal.timeout(1_000) })
      await response.body?.cancel()
      if (response.ok) return { url, token, database, stop }
    } catch {
      // The server may still be opening the process-local store.
    }
    await sleep(50)
  }
  throw new Error(`OLTP fixture was never ready. ${diagnostic}`)
}

test("O01/T06 live ORM and Q04–Q06 receipt identity", { skip: !binary, timeout: 180_000 }, async t => {
  const { stop, ...connection } = await startServer(t)
  const db = new Database({ ...connection, mode: "olap", timeout: 15 })
  const schema = { id: String, age: BigInt, score: Number, active: Boolean }
  const users = await db.createTable("typescript_users", schema, "id")
  assert.deepEqual(users.schema, schema)
  assert.equal(users.key, "id")
  assert.equal(users.format, "parquet")
  assert.equal((await db.table(users.name)).id, users.id)
  assert.deepEqual((await db.tables()).map(table => table.id), [users.id])

  const alice = { id: "alice", age: (1n << 63n) - 1n, score: -0, active: true }
  const bob = { id: "bob", age: -(1n << 63n), score: 0.5, active: false }
  const carol = { id: "carol", age: 24n, score: 3, active: true }
  assert.equal(await users.insert(alice), undefined)
  assert.equal(await users.insertMany([bob, carol]), 2)
  assert.equal(await users.insertMany([]), 0)
  assert.deepEqual(await users.get("alice"), alice)
  assert.deepEqual(await users.get("bob"), bob)
  assert.equal(await users.get("missing"), null)
  const adults = users.where(col("age").ge(18n).and(col("active").eq(false).not()))
  assert.deepEqual(await adults.orderBy("age", true).select("id").limit(2).collect(),
    [{ id: "alice" }, { id: "carol" }])
  assert.equal(await adults.limit(1).count(), 1)
  assert.equal(await adults.count(), 2)
  assert.match(await adults.explain(), /\S/)
  assert.equal(await users.update("bob", { age: 18n, active: true }), true)
  assert.equal(await users.update("missing", { age: 1n }), false)
  assert.equal(await users.upsert({ ...alice, age: 25n }), undefined)
  assert.equal((await users.get("alice")).age, 25n)
  assert.equal(await users.delete("missing"), false)
  assert.equal(await users.delete("carol"), true)
  await assert.rejects(users.insertMany([{ ...carol, id: "new" }, alice]), ConflictError)
  assert.equal(await users.get("new"), null)
  assert.equal((await users.get("alice")).age, 25n)

  const requestId = randomUUID()
  const row = { id: "receipt", age: 42n, score: 2, active: true }
  const payload = { table_id: users.id, row }
  const accepted = await db.submit("table.upsert", payload, { requestId })
  const settled = await accepted.wait({ timeout: 5 })
  assert.equal(settled.requestId, requestId)
  assert.equal(settled.state, "succeeded")
  assert.equal(settled.result, null)
  assert.equal((await db.receipt(settled.runId, requestId)).state, "succeeded")
  const replay = await (await db.submit("table.upsert", payload, { requestId })).wait({ timeout: 5 })
  assert.equal(replay.requestId, requestId)
  assert.equal(replay.runId, settled.runId)
  assert.equal(replay.result, null)
  await assert.rejects(db.submit("table.upsert", { ...payload, row: { ...row, age: 43n } }, { requestId }), ConflictError)
  assert.deepEqual(await users.get(row.id), row)
  await assert.rejects(new Database({ ...connection, token: "invalid" }).tables(), AuthenticationError)

  const perBatch = []
  for (const size of [1, 100, 1_000]) {
    const table = await db.createTable(`typescript_batch_${size}`, { id: BigInt, value: String }, "id")
    const rows = Array.from({ length: size }, (_, index) => ({ id: BigInt(index), value: `row-${index}-é` }))
    const start = performance.now()
    assert.equal(await table.insertMany(rows), size)
    perBatch.push({ rows: size, insertManyMs: Number((performance.now() - start).toFixed(2)) })
    assert.equal(await table.query().count(), size)
    assert.deepEqual(await table.query().orderBy("id").collect(), rows)
    assert.deepEqual(await table.get(BigInt(size - 1)), rows.at(-1))
    await table.drop()
  }
  await users.drop()
  await assert.rejects(users.get("alice"), NotFoundError)
  const replacement = await db.createTable(users.name, schema, "id")
  assert.notEqual(replacement.id, users.id)
  await replacement.drop()
  t.diagnostic(JSON.stringify(perBatch))

  if (python) {
    await stop()
    const reference = await startServer(t)
    const { stdout } = await promisify(execFile)(python, [
      "../../server/tests/oltp_orm_contract.py", "--url", reference.url, "--database", reference.database,
    ], { env: { ...process.env, NABEDRA_TEST_TOKEN: reference.token }, timeout: 120_000 })
    const observation = JSON.parse(stdout)
    assert.equal(observation.operations.length, 13)
    assert.deepEqual(observation.scales, [1, 100, 1_000])
    assert.equal(observation.stale_handle, "not_found")
  }
})

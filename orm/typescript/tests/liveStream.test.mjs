import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { once } from "node:events"
import { createInterface } from "node:readline"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { Database } from "nabedra"

const binary = process.env.NABEDRA_TEST_OLAP_BINARY
const python = process.env.NABEDRA_TEST_PYTHON
const script = String.raw`
import json, pathlib, signal, sys, tempfile
sys.path.insert(0, sys.argv[2])
from acceptance_olap_streaming import StreamCluster

def stop(_signal, _frame):
    raise SystemExit(1)

signal.signal(signal.SIGTERM, stop)
with tempfile.TemporaryDirectory(prefix='nabedra-typescript-stream-') as directory:
    cluster = StreamCluster(pathlib.Path(sys.argv[1]).resolve(), directory)
    try:
        for worker in (10, 20, 30):
            cluster.start_worker(worker, 1)
        cluster.ready_run()
        print(json.dumps({'url': cluster.api_door.url, 'token': cluster.auth['data_token']}), flush=True)
        sys.stdin.read()
    except Exception:
        cluster.diagnostics.flush()
        sys.stderr.write((pathlib.Path(directory) / 'diagnostics').read_text()[-3000:])
        raise
    finally:
        cluster.close()
`

async function fixture(t) {
  const scripts = fileURLToPath(new URL("../../../scripts", import.meta.url))
  const child = spawn(python, ["-c", script, binary, scripts], { stdio: ["pipe", "pipe", "pipe"] })
  const lines = createInterface({ input: child.stdout })
  let diagnostics = ""
  child.stderr.setEncoding("utf8").on("data", chunk => { diagnostics = (diagnostics + chunk).slice(-4_000) })
  const closed = new Promise(resolve => child.once("close", (code, signal) => resolve({ code, signal })))
  t.after(async () => {
    child.stdin.end()
    const terminate = setTimeout(() => child.kill("SIGTERM"), 20_000)
    const kill = setTimeout(() => child.kill("SIGKILL"), 40_000)
    try {
      await closed
    } finally {
      clearTimeout(terminate)
      clearTimeout(kill)
      lines.close()
    }
  })
  child.stdin.on("error", () => {})
  const startup = await Promise.race([
    once(lines, "line", { signal: AbortSignal.timeout(135_000) }).then(([line]) => ({ line })),
    closed,
    once(child, "error").then(([error]) => ({ error })),
  ])
  if (!startup.line) throw new Error(`OLAP fixture did not start. ${startup.error?.message ?? diagnostics}`)
  return JSON.parse(startup.line)
}

test("ST03/ST05 live OLAP row and Arrow streams over a shared sharded snapshot",
  { skip: !binary || !python, timeout: 180_000 }, async t => {
    const connection = await fixture(t)
    const db = new Database({ ...connection, mode: "olap", timeout: 30 })
    const table = await db.createTable("typescript_stream", { id: BigInt, text: String }, "id")
    const expected = Array.from({ length: 23 }, (_, index) => ({ id: BigInt(index), text: `value-${index}-é` }))
    assert.equal(await table.insertMany(expected), expected.length)
    const query = table.query().select("id", "text")
    const snapshot = await query.streamSnapshot({ shards: 2 }).open()
    try {
      const seen = new Map()
      for (const shard of [0, 1]) {
        const rows = query.stream({ batchRows: 4, prefetchBatches: 2, snapshot: snapshot.id, shard })
        try {
          for await (const batch of rows) {
            assert(batch.length > 0 && batch.length <= 4)
            for (const row of batch) {
              assert(!seen.has(row.id))
              seen.set(row.id, row)
            }
          }
        } finally {
          await rows[Symbol.asyncDispose]()
        }
      }
      assert.deepEqual(expected.map(row => seen.get(row.id)), expected)
    } finally {
      await snapshot[Symbol.asyncDispose]()
    }
    const arrow = query.orderBy("id").stream({ batchRows: 6, format: "arrow", dropLast: true })
    try {
      const decoded = []
      for await (const batch of arrow) {
        assert.equal(batch.numRows, 6)
        const ids = batch.getChild("id")
        const texts = batch.getChild("text")
        for (let index = 0; index < batch.numRows; ++index) decoded.push({ id: ids.get(index), text: texts.get(index) })
      }
      assert.deepEqual(decoded, expected.slice(0, 18))
    } finally {
      await arrow[Symbol.asyncDispose]()
    }
    await table.drop()
  })

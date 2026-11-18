import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { createHash, randomBytes, randomUUID } from "node:crypto"
import { once } from "node:events"
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises"
import { createServer, request } from "node:http"
import { tmpdir } from "node:os"
import { delimiter, dirname, join } from "node:path"
import { createInterface } from "node:readline"
import { test } from "node:test"
import { setTimeout as delay } from "node:timers/promises"
import { CallbackBridge } from "../dist/callbackBridge.js"
import { Database } from "nabedra/client"
import { ValidationError } from "nabedra/errors"
import { serve } from "nabedra/serve"

function within(promise, ms, label) {
  let timer
  return Promise.race([promise, new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(label)), ms)
  })]).finally(() => clearTimeout(timer))
}

function send(url, token, body) {
  return new Promise((resolve, reject) => {
    const outgoing = request(url, {
      method: "POST", agent: false,
      headers: {
        Authorization: "Bearer " + token, "Content-Type": "application/json", "Content-Length": body.length,
      },
    }, incoming => {
      const chunks = []
      incoming.on("data", chunk => chunks.push(chunk))
      incoming.on("error", reject)
      incoming.on("end", () => resolve({
        status: incoming.statusCode, value: JSON.parse(Buffer.concat(chunks).toString("utf8")),
      }))
    })
    outgoing.on("error", reject)
    outgoing.end(body)
  })
}

function launch(clusterId, ownerId) {
  const requestId = randomUUID()
  const nodeId = randomUUID()
  const keys = { cluster_id: clusterId, node_id: nodeId, request_id: requestId }
  return {
    version: 1, invocation_id: randomUUID(), cluster_id: clusterId, owner_id: ownerId,
    epoch: 1, intent_id: requestId, action: "start",
    target: { node_id: nodeId, request_id: requestId, incarnation_id: null },
    payload: {
      version: 1, cluster_name: "Fixture", ...keys,
      bootstrap: {
        version: 1, ...keys, api_urls: ["http://127.0.0.1:8000"], listen: "0.0.0.0:9000",
        advertise_url: null, storage: { driver: "http", options: { floatIntegral: 1 } },
        enrollment_token: "fixture-enrollment", tls: null, operation_slots: 4, partition_slots: 4,
        queue_size: 64, partition_size: "64 MiB",
      },
    },
  }
}

test("A01 local serve validation rejects implicit autoscaling before launching", async () => {
  await assert.rejects(serve("Fixture", [1, 2]), {
    name: "ValidationError", message: "Tuple nodes require managed callbacks",
  })
  await assert.rejects(serve("Fixture", true), ValidationError)
  await assert.rejects(serve("Fixture", 1, { startNode() { throw new Error("unexpected entry") } }), ValidationError)
  await assert.rejects(serve("Fixture", 1, { coldStart: "true" }), ValidationError)
})

test("A02 callback admission verifies fresh ownership, identity and independent inventory capacity", { timeout: 15_000 }, async t => {
  const clusterId = randomUUID()
  const ownerId = randomUUID()
  const token = randomBytes(32).toString("base64url")
  const authorizeToken = randomBytes(32).toString("base64url")
  const seen = []
  const entered = new Set()
  const slots = new Map()
  let allow = false
  let inventory = 0
  const authority = createServer(async (request, response) => {
    assert.equal(request.url, "/private/v1/authorize")
    assert.equal(request.headers.authorization, "Bearer " + authorizeToken)
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    const raw = Buffer.concat(chunks)
    seen.push(raw)
    const command = JSON.parse(raw)
    const result = allow ? {
      version: 1, invocation_id: command.invocation_id,
      command_sha256: createHash("sha256").update(raw).digest("hex"), authorized: true,
    } : { version: 1, invocation_id: command.invocation_id, error: { code: "ownership_changed" } }
    const bytes = Buffer.from(JSON.stringify(result))
    response.writeHead(allow ? 200 : 409, {
      "Content-Length": bytes.length, "Content-Type": "application/json", Connection: "close",
    })
    response.end(bytes)
  })
  authority.listen(0, "127.0.0.1")
  await once(authority, "listening")
  t.after(() => new Promise(resolve => authority.close(resolve)))

  const bridge = await CallbackBridge.open({
    async startNode(payload) {
      assert.equal(payload.bootstrap.operation_slots, 4)
      entered.add(payload.request_id)
      const slot = slots.get(payload.request_id)
      slot?.entered.resolve()
      await slot?.finish.promise
      payload.bootstrap.operation_slots = 17
      return "worker/" + payload.node_id
    },
    stopNode() { return },
    listNodes() { inventory++; return [] },
  }, token, authorizeToken)
  t.after(async () => {
    for (const slot of slots.values()) slot.finish.resolve()
    await bridge.close()
  })
  const commands = Array.from({ length: 4 }, () => launch(clusterId, ownerId))
  const original = Buffer.from(JSON.stringify(commands[0], null, 1).replace(
    '"floatIntegral": 1', '"floatIntegral": 1.0',
  ))
  const invoke = command => send(bridge.url, token, Buffer.from(JSON.stringify(command)))

  let reply = await send(bridge.url, "wrong-fixture-token", original)
  assert.equal(reply.status, 401)
  assert.equal(reply.value.error.code, "unauthorized")
  reply = await send(bridge.url, token, original)
  assert.equal(reply.status, 503)
  assert.equal(reply.value.error.code, "bridge_not_active")
  assert.equal(seen.length, 0)

  bridge.activate(clusterId, "http://127.0.0.1:" + authority.address().port + "/private/v1/authorize")
  reply = await send(bridge.url, token, original)
  assert.equal(reply.status, 409)
  assert.deepEqual(reply.value.error, { code: "ownership_changed", outcome: "not_invoked" })
  assert.deepEqual(seen[0], original)
  assert.equal(entered.size, 0)

  allow = true
  for (const command of commands.slice(0, 3)) {
    slots.set(command.intent_id, { entered: Promise.withResolvers(), finish: Promise.withResolvers() })
  }
  const calls = [
    send(bridge.url, token, original), ...commands.slice(1, 3).map(invoke),
  ]
  await within(Promise.all(Array.from(slots.values(), slot => slot.entered.promise)), 3_000, "callbacks did not enter")
  assert.equal(entered.size, 3)

  // Numeric type identity survives JSON normalization, even for an integral float.
  reply = await invoke(commands[0])
  assert.equal(reply.status, 409)
  assert.equal(reply.value.error.code, "identity_conflict")
  reply = await send(bridge.url, token, original)
  assert.deepEqual(reply.value.error, { code: "callback_in_progress", outcome: "unknown" })
  reply = await invoke(commands[3])
  assert.equal(reply.status, 429)
  assert.equal(reply.value.error.code, "callback_capacity")

  const list = {
    version: 1, invocation_id: randomUUID(), cluster_id: clusterId, owner_id: ownerId, epoch: 1,
    intent_id: null, action: "inventory", target: null, payload: {},
  }
  reply = await invoke(list)
  assert.equal(reply.status, 200)
  assert.deepEqual(reply.value.result, [])
  assert.equal(inventory, 1)
  assert.equal(seen.length, 5)

  for (const slot of slots.values()) slot.finish.resolve()
  for (const finished of await Promise.all(calls)) {
    assert.equal(finished.status, 200)
    assert.match(finished.value.result, /^worker\//)
  }
  reply = await send(bridge.url, token, original)
  assert.equal(reply.status, 200)
  assert.deepEqual(seen.at(-1), original)
  assert.equal(seen.length, 6)

  const duplicate = Buffer.from(original.toString().replace('"version": 1', '"version": 1, "version": 1'))
  reply = await send(bridge.url, token, duplicate)
  assert.equal(reply.status, 400)
  assert.equal(seen.length, 6)
})

test("managed serve negotiates the provider bridge and reaps startup failures", { timeout: 15_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), "nabedra-ts-managed-"))
  const binary = join(directory, "fixture.mjs")
  const config = join(directory, "api.json")
  const marker = join(directory, "provider.json")
  const children = new Set()
  t.after(async () => {
    await Promise.all(Array.from(children, async child => {
      const exit = once(child, "exit")
      child.kill("SIGTERM")
      const kill = setTimeout(() => child.kill("SIGKILL"), 5_000)
      try { await exit } finally { clearTimeout(kill) }
    }))
    await rm(directory, { recursive: true, force: true })
  })
  const source = await readFile(new URL("./fixtures/serve-peer.mjs", import.meta.url))
  await writeFile(binary, Buffer.concat([Buffer.from("#!/usr/bin/env node\n"), source]), { mode: 0o700 })
  await writeFile(config, "{}")
  function run(mode) {
    const module = new URL("../dist/serve.js", import.meta.url).href
    const script = "import { serve } from " + JSON.stringify(module)
      + "; try { await serve('Fixture', [1, 3], { startNode: () => 'resource', stopNode: () => {},"
      + " listNodes: async () => [], coldStart: true, migrateCatalog: true }); }"
      + " catch (error) { console.log(JSON.stringify({code:error.code})); process.exitCode = 1; }"
    const child = spawn(process.execPath, ["--input-type=module", "--eval", script], {
      env: { ...process.env, PATH: dirname(process.execPath) + delimiter + (process.env.PATH ?? ""),
        NABEDRA_CONFIG: config, NABEDRA_SERVER_BINARY: binary, NABEDRA_TEST_SERVE_MODE: mode,
        NABEDRA_TEST_SERVE_MARKER: marker },
      stdio: ["ignore", "pipe", "pipe"],
    })
    children.add(child)
    child.once("exit", () => children.delete(child))
    const finished = once(child, "exit")
    const lines = createInterface({ input: child.stdout })
    child.stderr.resume()
    return { child, finished, lines }
  }
  const managed = run("ready")
  const [line] = await within(Promise.race([
    once(managed.lines, "line"), managed.finished.then(() => { throw new Error("managed fixture exited early") }),
  ]), 15_000, "managed fixture did not become ready")
  assert.equal(JSON.parse(line).event, "nabedra.serve.ready")
  managed.lines.close()
  const invoked = await within((async () => {
    while (managed.child.exitCode === null && managed.child.signalCode === null) {
      try { return JSON.parse(await readFile(marker, "utf8")) }
      catch (error) { if (error.code !== "ENOENT") throw error }
      await delay(10)
    }
    throw new Error("managed fixture exited before invoking provider")
  })(), 15_000, "managed fixture did not invoke provider")
  assert.deepEqual(invoked, { providerInvoked: true })
  managed.child.kill("SIGTERM")
  assert.deepEqual(await within(managed.finished, 15_000, "managed fixture did not exit"), [0, null])

  const failed = run("fatal")
  const [failureLine] = await within(once(failed.lines, "line"), 15_000, "fatal fixture did not report")
  assert.deepEqual(JSON.parse(failureLine), { code: "bind_failed" })
  failed.lines.close()
  assert.deepEqual(await within(failed.finished, 15_000, "fatal fixture did not exit"), [1, null])
})

const liveBinary = process.env.NABEDRA_TEST_SERVER_BINARY
test("U01/R04 supervised OLTP serves ORM operations, reaps and cold-starts committed data", {
  skip: !liveBinary, timeout: 45_000,
}, async t => {
  const directory = await mkdtemp(join(tmpdir(), "nabedra-ts-serve-"))
  const dataDir = join(directory, "data")
  const authFile = join(directory, "auth.json")
  const configFile = join(directory, "api.json")
  const token = randomBytes(32).toString("base64url")
  await mkdir(dataDir)
  await writeFile(authFile, JSON.stringify({
    admin_token: randomBytes(32).toString("base64url"),
    data_token: token, enrollment_token: randomBytes(32).toString("base64url"),
  }), { mode: 0o600 })
  await writeFile(configFile, JSON.stringify({
    version: 1, engine: "oltp", bind: "127.0.0.1:0", database: "serve_test", auth_file: authFile,
    topology: { mode: "local", data_dir: dataDir },
  }))
  const running = []
  t.after(async () => {
    await Promise.all(running.map(stop => stop()))
    await rm(directory, { recursive: true, force: true })
  })

  async function start(coldStart) {
    const module = new URL("../dist/serve.js", import.meta.url).href
    const script = "import { serve } from " + JSON.stringify(module)
      + "; await serve('Fixture', 1, { coldStart: " + coldStart + " });"
    const child = spawn(process.execPath, ["--input-type=module", "--eval", script], {
      env: { ...process.env, NABEDRA_CONFIG: configFile, NABEDRA_SERVER_BINARY: liveBinary },
      stdio: ["ignore", "pipe", "pipe"],
    })
    let diagnostic = ""
    child.stderr.setEncoding("utf8").on("data", chunk => { diagnostic = (diagnostic + chunk).slice(-1500) })
    const finished = once(child, "exit")
    async function stop() {
      if (child.exitCode !== null || child.signalCode !== null) {
        return await finished
      }
      child.kill("SIGTERM")
      const force = setTimeout(() => child.kill("SIGKILL"), 12_000)
      try { return await finished }
      finally { clearTimeout(force) }
    }
    running.push(stop)
    const lines = createInterface({ input: child.stdout })
    const announce = await within(Promise.race([
      once(lines, "line").then(([line]) => JSON.parse(line)),
      finished.then(([code, signal]) => { throw new Error("serve exited: " + (signal ?? code) + " " + diagnostic) }),
    ]), 15_000, "serve did not announce readiness")
    lines.close()
    assert.equal(announce.event, "nabedra.serve.ready")
    const health = await fetch(announce.bound_url + "/health/ready", {
      headers: { Authorization: "Bearer " + token, Connection: "close" },
      signal: AbortSignal.timeout(3_000),
    })
    assert.equal(health.status, 200)
    const info = await health.json()
    assert.equal(info.engine, "oltp")
    return {
      db: new Database({ url: announce.bound_url, token, database: "serve_test", timeout: 5 }),
      url: announce.bound_url, run: info.run_id, stop,
    }
  }

  const first = await start(false)
  const table = await first.db.createTable("serve_records", { id: String, value: BigInt }, "id")
  const row = { id: "retained", value: 9_223_372_036_854_775_807n }
  const receipt = await (await first.db.submit("table.upsert", { table_id: table.id, row })).wait({ timeout: 5 })
  assert.equal(receipt.state, "succeeded")
  assert.equal(receipt.runId, first.run)
  assert.deepEqual(await table.get(row.id), row)
  assert.deepEqual(await first.stop(), [0, null])
  await assert.rejects(fetch(first.url + "/health/ready", { signal: AbortSignal.timeout(1_000) }))

  const restarted = await start(true)
  assert.notEqual(restarted.run, first.run)
  const retained = await restarted.db.table("serve_records")
  assert.equal(retained.id, table.id)
  assert.deepEqual(await retained.get(row.id), row)
  const old = await restarted.db.receipt(receipt.runId, receipt.requestId)
  assert.equal(old.state, "succeeded")
  assert.equal(old.result, null)
  assert.deepEqual(await restarted.stop(), [0, null])
})

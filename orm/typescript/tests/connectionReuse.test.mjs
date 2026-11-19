import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { once } from "node:events"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { createServer } from "node:https"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { test } from "node:test"
import { setTimeout as sleep } from "node:timers/promises"
import { Database } from "../dist/client.js"
import { ProtocolError, TransportError, UnknownOutcomeError } from "../dist/errors.js"
import { Transport, encodeJson, parseJson } from "../dist/transport.js"

// One local TLS server per test. Actions shape the next responses; the default is a keep-alive reply.
async function keepAliveServer(t) {
  const work = await mkdtemp(join(tmpdir(), "nabedra-reuse-"))
  t.after(() => rm(work, { recursive: true, force: true }))
  const key = join(work, "key")
  const cert = join(work, "cert")
  const config = join(work, "openssl.cnf")
  await writeFile(config, "[req]\nprompt = no\ndistinguished_name = dn\nx509_extensions = v3\n" +
    "[dn]\nCN = local-nabedra\n[v3]\nsubjectAltName = IP:127.0.0.1\n")
  const generated = spawnSync("openssl", [
    "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-config", config, "-keyout", key, "-out", cert,
  ], { encoding: "utf8" })
  if (generated.error?.code === "ENOENT") return null
  assert.equal(generated.status, 0, generated.stderr)

  const state = { accepted: 0, seen: [], actions: [], closed: [] }
  const numbers = new WeakMap()
  const server = createServer({ key: await readFile(key), cert: await readFile(cert) }, async (request, response) => {
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    const { request_id, operation } = parseJson(Buffer.concat(chunks))
    state.seen.push({ connection: numbers.get(request.socket), operation, requestId: request_id })
    const action = state.actions.shift() ?? "ok"
    if (action === "hangup") return request.socket.destroy()
    if (action === "malformed") {
      request.socket.end("HTTP/1.1 invalid\r\nContent-Length: 0\r\n\r\n")
      return
    }
    const body = encodeJson({ request_id, data: [] })
    const headers = { "content-type": "application/json", "content-length": String(body.length + (action === "truncate" ? 5 : 0)) }
    if (action === "close") headers.connection = "close"
    response.writeHead(200, headers)
    if (action === "truncate") {
      response.write(body)
      setImmediate(() => request.socket.destroy())
      return
    }
    response.end(body)
    if (action === "ok_then_close") response.once("finish", () => request.socket.end())
  })
  server.on("secureConnection", (socket) => {
    numbers.set(socket, ++state.accepted)
    socket.once("close", () => state.closed.push(numbers.get(socket)))
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(() => new Promise((resolve) => {
    server.close(resolve)
    server.closeAllConnections()
  }))
  return { url: "https://127.0.0.1:" + server.address().port + "/api", cert, state }
}

test("O01 sequential calls reuse one TLS connection", async (t) => {
  const fixture = await keepAliveServer(t)
  if (fixture === null) return t.skip("openssl is needed to generate the local TLS fixture")
  const { url, cert, state } = fixture
  const transport = new Transport(url, "token", { caFile: cert })
  const operations = ["table.get", "table.insert", "query.count", "table.update", "table.delete"]
  for (let round = 0; round < 4; round++) {
    for (const operation of operations) assert.deepEqual(await transport.request(operation, {}), [])
  }
  const db = new Database(url, "token", { caFile: cert })
  for (let call = 0; call < 3; call++) assert.deepEqual(await db.tables(), [])
  // One connection for the Transport and one for the Database's own Transport.
  assert.equal(state.accepted, 2)
  assert.equal(state.seen.length, 23)
  assert.deepEqual(new Set(state.seen.slice(0, 20).map(seen => seen.connection)), new Set([1]))
  assert.equal(new Set(state.seen.map(seen => seen.requestId)).size, 23)
})

test("O01 server-requested close and agent: false do not reuse", async (t) => {
  const fixture = await keepAliveServer(t)
  if (fixture === null) return t.skip("openssl is needed to generate the local TLS fixture")
  const { url, cert, state } = fixture
  state.actions.push("close", "close")
  const transport = new Transport(url, "token", { caFile: cert })
  for (let call = 0; call < 3; call++) await transport.request("table.get", {})
  assert.equal(state.accepted, 3)
  const single = new Transport(url, "token", { caFile: cert, agent: false })
  for (let call = 0; call < 2; call++) await single.request("table.get", {})
  assert.equal(state.accepted, 5)
})

test("O01 an idle connection closed by the server is replaced", async (t) => {
  const fixture = await keepAliveServer(t)
  if (fixture === null) return t.skip("openssl is needed to generate the local TLS fixture")
  const { url, cert, state } = fixture
  const transport = new Transport(url, "token", { caFile: cert })
  state.actions.push("ok_then_close")
  await transport.request("table.get", {})
  while (!state.closed.includes(1)) await sleep(10)
  await sleep(50)
  assert.deepEqual(await transport.request("table.insert", {}), [])
  assert.deepEqual(state.seen.map(seen => [seen.connection, seen.operation]), [[1, "table.get"], [2, "table.insert"]])
})

test("O01 a failure on a reused connection surfaces once and drops that connection", async (t) => {
  const fixture = await keepAliveServer(t)
  if (fixture === null) return t.skip("openssl is needed to generate the local TLS fixture")
  const { url, cert, state } = fixture
  const transport = new Transport(url, "token", { caFile: cert })
  const cases = [
    ["hangup", "table.insert", UnknownOutcomeError], ["hangup", "table.get", TransportError],
    ["truncate", "table.upsert", UnknownOutcomeError], ["truncate", "table.get", TransportError],
    ["malformed", "table.delete", UnknownOutcomeError], ["malformed", "table.get", ProtocolError],
  ]
  for (const [action, operation, expected] of cases) {
    await transport.request("table.get", {})
    const reused = state.accepted
    const before = state.seen.length
    state.actions.push(action)
    await assert.rejects(transport.request(operation, {}), (error) => {
      assert.equal(error.constructor, expected, `${action} ${operation}`)
      assert.equal(error.requestId, state.seen.at(-1).requestId)
      return true
    })
    assert.equal(state.seen.length, before + 1, "a failed request was resent")
    assert.equal(state.seen.at(-1).connection, reused, "the failed request used the pooled connection")
    await transport.request("table.get", {})
    assert.equal(state.accepted, reused + 1, `${action} left its connection in the pool`)
  }
})

test("O01 the default agent closes connections idle for two seconds", async (t) => {
  const fixture = await keepAliveServer(t)
  if (fixture === null) return t.skip("openssl is needed to generate the local TLS fixture")
  const { url, cert, state } = fixture
  const transport = new Transport(url, "token", { caFile: cert })
  await transport.request("table.get", {})
  await sleep(2_400)
  assert.deepEqual(state.closed, [1])
  await transport.request("table.get", {})
  assert.equal(state.accepted, 2)
})

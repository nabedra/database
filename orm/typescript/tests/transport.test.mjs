import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { randomUUID } from "node:crypto"
import { once } from "node:events"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import { createServer as createTlsServer } from "node:https"
import { createServer as createTcpServer } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import {
  AuthenticationError, ConflictError, LimitExceededError, NotFoundError, ProtocolError,
  TransportError, UnknownOutcomeError, ValidationError,
} from "../dist/errors.js"
import { Transport, encodeJson, exchangeMayHaveSent, floatToken, parseJson } from "../dist/transport.js"

async function loopback(t, server, protocol = "http") {
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(() => new Promise((resolve) => {
    server.close(resolve)
    server.closeAllConnections?.()
  }))
  return protocol + "://127.0.0.1:" + server.address().port
}

function reply(response, status, envelope) {
  response.writeHead(status, { "content-type": "application/json" }).end(encodeJson(envelope))
}

test("strict JSON preserves numeric syntax and rejects ambiguous inputs", () => {
  const wire = '{"id":-9223372036854775808,"score":2.0,"negative":-0.0,"note":"🦬"}'
  assert.equal(encodeJson({
    id: -(2n ** 63n), score: floatToken(2), negative: floatToken(-0), note: "🦬",
  }).toString(), wire)
  assert.equal(encodeJson([floatToken(1e20), floatToken(1e-6), floatToken(0.1)]).toString(),
    "[1e+20,1e-06,0.1]")
  assert.deepEqual(parseJson(wire, { integers: "bigint" }), {
    id: -(2n ** 63n), score: 2, negative: -0, note: "🦬",
  })
  assert.deepEqual(parseJson('{"small":1,"large":9223372036854775807,"__proto__":{"valid":true}}'), {
    small: 1, large: 2n ** 63n - 1n, ["__proto__"]: { valid: true },
  })
  for (const raw of ['{"x":1,"\\u0078":2}', '{ "x": [{"n":NaN}] }', "1e999", "[1,]", '{"x":1,}']) {
    assert.throws(() => parseJson(raw), SyntaxError)
  }
  assert.throws(() => parseJson(Buffer.from([34, 0xff, 34])), TypeError)
  for (const value of [undefined, "\ud800", { nested: NaN }, { nested: new Date() }, Array(1)]) {
    assert.throws(() => encodeJson(value), TypeError)
  }
})

test("receipt identity and uncertain writes survive client and HTTP boundaries", { timeout: 8_000 }, async (t) => {
  const runId = randomUUID()
  const records = new Map()
  const seen = []
  let fault
  const address = await loopback(t, createServer(async (request, response) => {
    const { authorization, "x-nabedra-database": selector } = request.headers
    seen.push({ route: request.url, authorization, selector })
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    const raw = Buffer.concat(chunks).toString()
    const parsed = raw ? parseJson(raw) : null
    const requestId = parsed?.request_id ?? request.url.split("/").at(-1)
    const outcomeError = (status, code, message) => reply(response, status, {
      request_id: requestId, error: { code, message, outcome: "not_applied" },
    })
    if (fault === "disconnect") return request.socket.destroy()
    if (fault === "timeout") return
    if (fault === "duplicate") {
      return response.writeHead(200, { "content-type": "application/json" }).end(
        '{"request_id":"' + requestId + '","data":null,"\\u0064ata":[]}',
      )
    }
    if (fault === "redirect") return response.writeHead(307, { location: "/elsewhere" }).end()
    if (fault === "oversize") {
      return response.writeHead(200, { "content-type": "application/json", "content-length": 8 * 1024 * 1024 + 1 }).end()
    }
    if (fault === "invalid-acceptance") return reply(response, 202, { request_id: requestId, state: "queued" })
    if (request.url.endsWith("/operations")) {
      return reply(response, 200, { request_id: requestId, data: { raw, id: 9223372036854775807n, score: floatToken(2) } })
    }
    if (request.method === "POST") {
      if (records.has(requestId) && records.get(requestId) !== raw) return outcomeError(409, "conflict", "identity conflict")
      records.set(requestId, raw)
      return reply(response, 202, { request_id: requestId, run_id: runId, state: "queued" })
    }
    if (fault === "expire") return outcomeError(404, "not_found", "receipt expired")
    if (authorization !== "Bearer token") return outcomeError(401, "unauthorized", "scope mismatch")
    const state = fault === "bad-state" ? "running" : "succeeded"
    return reply(response, 200, { request_id: requestId, run_id: runId, state, result: ["ok"], error: null })
  }))

  const endpoint = address + "/mount///"
  const transport = new Transport(endpoint, "token", { database: "tenant-a" })
  const row = await transport.request("table.get", { key: 9223372036854775807n })
  assert.equal(row.id, 9223372036854775807n)
  assert.equal(row.score, 2)
  assert.match(row.raw, /"key":9223372036854775807/)
  assert.deepEqual(seen[0], { route: "/mount/v1/operations", authorization: "Bearer token", selector: "tenant-a" })

  const preflight = seen.length
  assert.throws(() => new Transport(endpoint, "unsafe\n"), ValidationError)
  assert.throws(() => new Transport(endpoint, "token", { database: "unsafe\n" }), ValidationError)
  await assert.rejects(new Transport(endpoint, null).request("table.list", {}), AuthenticationError)
  await assert.rejects(transport.request("bad.op", {}), ValidationError)
  await assert.rejects(transport.request("table.list", { value: "x".repeat(8 * 1024 * 1024) }), LimitExceededError)
  await assert.rejects(transport.submit("table.list", {}, "not-a-uuid"), ValidationError)
  assert.equal(seen.length, preflight)

  const requestId = randomUUID()
  const accepted = await transport.submit("table.list", {}, requestId)
  assert.equal(accepted.state, "queued")
  const terminal = await new Transport(endpoint, "token").receipt(accepted.runId, accepted.requestId)
  assert.equal(terminal.requestId, requestId)
  assert.deepEqual(terminal.result, ["ok"])
  assert.equal((await transport.submit("table.list", {}, requestId)).runId, runId)
  await assert.rejects(transport.submit("table.describe", { name: "different" }, requestId), ConflictError)
  await assert.rejects(new Transport(endpoint, "different").receipt(runId, requestId), AuthenticationError)
  fault = "bad-state"
  await assert.rejects(transport.receipt(runId, requestId), ProtocolError)
  fault = "expire"
  await assert.rejects(transport.receipt(runId, requestId), NotFoundError)
  assert.equal(records.size, 1)

  for (const [mode, kind] of [
    ["duplicate", ProtocolError], ["disconnect", TransportError], ["redirect", ProtocolError], ["oversize", ProtocolError],
  ]) {
    fault = mode
    const count = seen.length
    await assert.rejects(transport.request("table.list", {}), kind)
    await assert.rejects(transport.request("table.insert", {}), UnknownOutcomeError)
    assert.equal(seen.length, count + 2, mode)
  }
  fault = "invalid-acceptance"
  await assert.rejects(transport.submit("table.insert", {}), UnknownOutcomeError)

  const malformedAddress = await loopback(t, createTcpServer((socket) => {
    socket.once("data", () => socket.end("HTTP/1.1 invalid\r\n\r\n"))
  }))
  await assert.rejects(new Transport(malformedAddress, "token").request("table.list", {}), ProtocolError)
  fault = "timeout"
  const deadline = performance.now() / 1000 + 0.15
  await assert.rejects(transport.exchange("GET", "/hold", { deadline }), (error) =>
    error instanceof TransportError && exchangeMayHaveSent(error),
  )
  await assert.rejects(transport.exchange("GET", "/hold", { deadline: performance.now() / 1000 - 1 }), (error) =>
    error instanceof TransportError && !exchangeMayHaveSent(error),
  )
})

test("OD-3: an unknown mutation retries only its own identity", { timeout: 8_000 }, async (t) => {
  // D12 after OD-3: a full leader answers a mutation 503 outcome_unknown. The client
  // rejects at once (no resend of its own); retry() sends the same ID and bytes.
  const runId = randomUUID()
  const seen = []
  let refuse = true
  const address = await loopback(t, createServer(async (request, response) => {
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    const raw = Buffer.concat(chunks).toString()
    const requestId = parseJson(raw).request_id
    seen.push({ route: request.url, raw, requestId })
    if (refuse) {
      return reply(response, 503, { request_id: requestId, error: {
        code: "outcome_unknown", outcome: "unknown",
        message: "direct request outcome unknown after overloaded: direct in-flight budget is full; "
          + "retry the same request_id and body",
      } })
    }
    if (request.url.endsWith("/operations")) return reply(response, 200, { request_id: requestId, data: true })
    return reply(response, 202, { request_id: requestId, run_id: runId, state: "queued" })
  }))
  const transport = new Transport(address, "token")
  const caught = await transport.request("table.update", { table_id: "t", key: 1, values: { v: 2 } }, (data) => data)
    .then(() => assert.fail("a full leader answered"), (error) => error)
  assert.ok(caught instanceof UnknownOutcomeError)
  assert.deepEqual([caught.code, caught.statusCode, caught.outcome], ["outcome_unknown", 503, "unknown"])
  assert.match(caught.message, /after overloaded: direct in-flight budget is full/)
  assert.equal(seen.length, 1, "the client does not resend by itself")
  assert.equal(caught.retryable, true)
  refuse = false
  assert.equal(await caught.retry(), true)
  assert.equal(seen.length, 2)
  assert.deepEqual([seen[1].raw, seen[1].requestId], [seen[0].raw, seen[0].requestId])
  assert.equal(caught.requestId, seen[0].requestId)

  refuse = true
  const failedAgain = await caught.retry().then(() => assert.fail("refused"), (error) => error)
  assert.ok(failedAgain instanceof UnknownOutcomeError && failedAgain.retryable)
  assert.equal(seen[2].raw, seen[0].raw)

  // A read has no identity to retry.
  const read = await transport.request("table.get", { table_id: "t", key: 1 }).then(() => assert.fail(), (error) => error)
  assert.ok(read instanceof UnknownOutcomeError)
  assert.equal(read.retryable, false)
  await assert.rejects(read.retry(), ValidationError)

  const submitted = await transport.submit("table.insert", { table_id: "t", row: { id: 1 } })
    .then(() => assert.fail("a full leader accepted"), (error) => error)
  assert.ok(submitted instanceof UnknownOutcomeError && submitted.retryable)
  refuse = false
  const receipt = await submitted.retry()
  assert.deepEqual([receipt.requestId, receipt.runId, receipt.state], [submitted.requestId, runId, "queued"])
  const [first, again] = seen.slice(-2)
  assert.ok(again.route.endsWith("/v1/requests"))
  assert.deepEqual([again.raw, again.requestId], [first.raw, first.requestId])
})

test("private CA enables HTTPS while unknown certificates remain rejected", { timeout: 8_000 }, async (t) => {
  assert.throws(() => new Transport("https://example.com", "token", { caFile: fileURLToPath(import.meta.url) }), ValidationError)
  const work = await mkdtemp(join(tmpdir(), "nabedra-transport-"))
  t.after(() => rm(work, { recursive: true, force: true }))
  const key = join(work, "key")
  const cert = join(work, "cert")
  const config = join(work, "openssl.cnf")
  await writeFile(config, "[req]\nprompt = no\ndistinguished_name = dn\nx509_extensions = v3\n" +
    "[dn]\nCN = local-nabedra\n[v3]\nsubjectAltName = IP:127.0.0.1\n")
  const generated = spawnSync("openssl", [
    "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-config", config, "-keyout", key, "-out", cert,
  ], { encoding: "utf8" })
  if (generated.error?.code === "ENOENT") return t.skip("openssl is needed to generate the local TLS fixture")
  assert.equal(generated.status, 0, generated.stderr)
  const endpoint = await loopback(t, createTlsServer({ key: await readFile(key), cert: await readFile(cert) }, async (request, response) => {
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    const { request_id } = parseJson(Buffer.concat(chunks))
    reply(response, 200, { request_id, data: [] })
  }), "https")
  await assert.rejects(new Transport(endpoint, "token").request("table.list", {}), TransportError)
  assert.deepEqual(await new Transport(endpoint, "token", { caFile: cert }).request("table.list", {}), [])
})

test("ordinary timeout reaches a progressing response and an explicit deadline still bounds it", async t => {
  const server = createServer(async (request, response) => {
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    const { request_id } = JSON.parse(Buffer.concat(chunks))
    response.writeHead(200, { "content-type": "application/json" })
    response.write(`{"request_id":"${request_id}","data":`)
    const emitting = setInterval(() => { response.write(" ") }, 30)
    const finish = setTimeout(() => response.end("[]}"), 420)
    response.once("close", () => { clearInterval(emitting); clearTimeout(finish) })
  })
  const base = await loopback(t, server)
  const transport = new Transport(base, "test", { timeout: 0.2 })
  assert.deepEqual(await transport.request("table.list", {}, value => value), [])
  await assert.rejects(transport.exchange("POST", "/v1/operations", {
    body: encodeJson({ request_id: randomUUID() }), deadline: performance.now() / 1_000 + 0.15,
  }), TransportError)
})

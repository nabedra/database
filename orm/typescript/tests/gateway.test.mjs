import { strict as assert } from "node:assert"
import { execFile } from "node:child_process"
import { once } from "node:events"
import { createServer } from "node:http"
import { test } from "node:test"
import { inspect, promisify } from "node:util"
import { AwsCredentials } from "nabedra/aws"
import { Gateway, GatewayContext, HTTPResponse, Operation, Receipt, S3 } from "nabedra/gateway"

const requestId = "11111111-1111-4111-8111-111111111111"
const tableId = "22222222-2222-4222-8222-222222222222"
const runId = "33333333-3333-4333-8333-333333333333"
const headers = [["Content-Type", "application/json"], ["Authorization", "Bearer caller-token"]]
const body = Buffer.from(' { "payload" : {"table_id":"' + tableId + '", "row":{"n":1e0}}, "operation":"table.insert", "request_id":"' + requestId + '" } ')
const operation = { method: "POST", path: "/nabedra/v1/operations", headers, body }
const credentials = new AwsCredentials("key-sentinel", "secret-sentinel", {
  sessionToken: "session-sentinel", expiresAt: new Date("2099-01-01T00:00:01.999Z"),
})

function authorize(context) {
  context.principal = "fixture-user"
  context.storage = new S3("fixture-bucket", { region: "eu-west-1", prefix: "app///", credentials })
}

function json(response) {
  return JSON.parse(Buffer.from(response.body).toString())
}

async function fixture(t, serve, options = {}) {
  const received = []
  const server = createServer(async (request, response) => {
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    const incoming = { url: request.url, method: request.method, headers: request.headers, body: Buffer.concat(chunks) }
    received.push(incoming)
    serve(response, incoming)
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const gateway = new Gateway({
    clusterUrl: `http://127.0.0.1:${server.address().port}`, token: "compute-token",
    mountPath: "/nabedra", ...options,
  })
  t.after(async () => {
    gateway.close()
    server.closeAllConnections()
    await new Promise(resolve => server.close(resolve))
  })
  return { gateway, received }
}

function send(response, status = 200, raw = Buffer.from(JSON.stringify({ request_id: requestId, data: null }))) {
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": raw.byteLength,
    "set-cookie": "cluster-secret",
    location: "https://compute.internal/hidden",
  })
  response.end(raw)
}

test("GW01 preserves exact HTTP bytes, immutable context, middleware order and trusted headers", async t => {
  const steps = []
  const { gateway, received } = await fixture(t, response => {
    steps.push("compute")
    send(response)
  })
  const first = context => {
    steps.push("first")
    assert(context instanceof GatewayContext)
    assert(context.operation instanceof Operation)
    assert.equal(context.receipt, null)
    assert.equal(context.method, "POST")
    assert.equal(context.path, "/v1/operations")
    assert.equal(context.database, "tenant-a")
    assert.deepEqual(context.headers.getAll("x-extra"), ["a", "b"])
    assert.equal(context.headers.get("X-EXTRA"), "a, b")
    assert.equal(context.headers.get("missing", null), null)
    assert.equal(context.operation.payload.row.n, 1)
    assert.throws(() => { context.operation = null }, TypeError)
    assert.throws(() => { context.operation.payload.row.n = 2 }, TypeError)
    assert.throws(() => { context.headers.pairs[0][0] = "forged" }, TypeError)
    context.rawBody.fill(0)
    assert.deepEqual(context.rawBody, body)
    for (const value of [context, context.headers, context.operation, gateway, credentials]) {
      assert.match(inspect(value), /redacted/)
      assert.doesNotMatch(inspect(value), /sentinel|caller-token|compute-token/)
    }
    authorize(context)
  }
  assert.equal(gateway.middleware(first), first)
  gateway.middleware(() => { steps.push("second") })
  const response = await gateway.handle({
    ...operation, headers: [...headers, ["X-Nabedra-Database", "tenant-a"],
      ["Cookie", "sentinel"], ["X-Extra", "a"], ["x-extra", "b"]],
  })
  assert(response instanceof HTTPResponse)
  assert.equal(response.status, 200, JSON.stringify(json(response)))
  assert.deepEqual(json(response), { request_id: requestId, data: null })
  assert.deepEqual(steps, ["first", "second", "compute"])
  assert.equal(received.length, 1)
  assert.deepEqual(received[0].body, body)
  assert.equal(received[0].headers.authorization, "Bearer compute-token")
  for (const name of ["cookie", "x-extra", "x-nabedra-database"]) assert.equal(received[0].headers[name], undefined)
  assert.deepEqual(JSON.parse(Buffer.from(received[0].headers["x-nabedra-storage"], "base64url").toString()), {
    version: 1, bucket: "fixture-bucket", region: "eu-west-1", prefix: "app/",
    credentials: {
      access_key_id: "key-sentinel", secret_access_key: "secret-sentinel",
      session_token: "session-sentinel", expires_at: "2099-01-01T00:00:01Z",
    },
  })
  assert.deepEqual(response.headers, [
    ["content-type", "application/json; charset=utf-8"], ["cache-control", "no-store"],
  ])
  response.body.fill(0)
  assert.notEqual(response.body[0], 0)
  assert.throws(() => gateway.middleware(authorize), TypeError)
})

test("GW02 preserves receipt and conflict envelopes, and a lost mutation is sent once", async t => {
  const queued = Buffer.from(JSON.stringify({ request_id: requestId, run_id: runId, state: "queued" }, null, 2))
  const terminal = Buffer.from(JSON.stringify({ request_id: requestId, run_id: runId, state: "succeeded", result: null, error: null }))
  const conflict = Buffer.from(JSON.stringify({ request_id: requestId, error: { code: "conflict", message: "conflict", outcome: "not_applied" } }))
  const statuses = [202, 200, 409]
  let upstream = 0
  const { gateway, received } = await fixture(t, response => {
    if (upstream === 3) {
      ++upstream
      response.socket.destroy()
    } else {
      const index = upstream++
      send(response, statuses[index], [queued, terminal, conflict][index])
    }
  })
  const observed = []
  gateway.middleware(async context => {
    observed.push(context)
    await Promise.resolve()
    authorize(context)
  })
  const accepted = await gateway.handleAsync({ ...operation, path: "/nabedra/v1/requests" })
  assert.equal(accepted.status, 202)
  assert.deepEqual(accepted.body, queued)
  assert.equal(Object.fromEntries(accepted.headers).location, `/nabedra/v1/requests/${runId}/${requestId}`)
  const receipt = await gateway.handleAsync({
    method: "GET", path: `/nabedra/v1/requests/${runId}/${requestId}`, headers: [], body: Buffer.alloc(0),
  })
  assert.equal(receipt.status, 200)
  assert.deepEqual(receipt.body, terminal)
  assert(observed[1].receipt instanceof Receipt)
  assert.equal(observed[1].receipt.runId, runId)
  assert.equal(observed[1].operation, null)
  assert.equal(received[1].method, "GET")
  assert.equal(received[1].url, `/v1/requests/${runId}/${requestId}`)
  const rejected = await gateway.handleAsync(operation)
  assert.equal(rejected.status, 409)
  assert.deepEqual(rejected.body, conflict)
  const lost = await gateway.handleAsync(operation)
  assert.equal(lost.status, 503)
  assert.deepEqual(json(lost), {
    request_id: requestId,
    error: { code: "outcome_unknown", message: "Compute response unavailable; do not resubmit", outcome: "unknown" },
  })
  assert.equal(received.length, 4)
  assert.equal(new Set(observed).size, 4)
})

test("SL06 and U02 reject malformed bodies, spoofed identities, and oversize requests before forwarding", async t => {
  const { gateway, received } = await fixture(t, response => send(response))
  let middleware = 0
  gateway.middleware(context => {
    middleware++
    authorize(context)
  })
  const malformed = [
    ["{", null],
    [body.toString().replace("1e0", "null"), requestId],
    [body.toString().replace("1e0", "9223372036854775808"), requestId],
    [body.toString().replace('"n":1e0', '"n":1,"n":2'), null],
    [body.toString().replace("1e0", '"\\ud800"'), null],
    [body.toString().replace(tableId, "invalid"), requestId],
    ["[".repeat(97) + "]".repeat(97), null],
  ]
  for (const [raw, correlation] of malformed) {
    const response = await gateway.handle({ ...operation, body: Buffer.from(raw) })
    assert.equal(response.status, 400, raw)
    assert.equal(json(response).request_id, correlation)
  }
  const spoofed = await gateway.handle({ ...operation, headers: [...headers, ["X-Nabedra-Storage", "forged"]] })
  assert.equal(spoofed.status, 401)
  for (const request of [
    { ...operation, headers: [...headers, ["authorization", "dup"]] },
    { ...operation, headers: [...headers, ["x-nabedra-database", "../invalid"]] },
    { ...operation, headers: [...headers, ["transfer-encoding", "chunked"]] },
    { ...operation, path: "/nabedra/v1/operations?x=1" },
    { ...operation, path: "/other/v1/operations" },
  ]) assert.equal((await gateway.handle(request)).status, 400)
  assert.equal((await gateway.handle({ ...operation, body: Buffer.alloc(8 * 1024 * 1024 + 1) })).status, 413)
  assert.equal((await gateway.handle({ ...operation, headers: [...headers, ["X-Large", "x".repeat(32 * 1024)]] })).status, 413)
  assert.equal(await gateway.handle(null).then(item => item.status), 400)
  assert.equal(middleware, 0)
  assert.equal(received.length, 0)
  assert.throws(() => new Gateway({ clusterUrl: "http://2130706433", token: "private" }), TypeError)
  assert.throws(() => new Gateway({ clusterUrl: "http://localhost", token: "private" }), TypeError)
})

test("GW03 bounds waiting middleware and retains capacity until its callback settles", async t => {
  const { gateway, received } = await fixture(t, response => send(response), { maxConcurrency: 1, middlewareTimeout: 0.05 })
  let entered
  let release
  const started = new Promise(resolve => { entered = resolve })
  const pending = new Promise(resolve => { release = resolve })
  let calls = 0
  gateway.middleware(async context => {
    if (++calls === 1) {
      entered()
      await pending
    }
    authorize(context)
  })
  const first = gateway.handleAsync(operation)
  await started
  const unparsed = { ...operation, get body() { assert.fail("overload read the body") } }
  assert.deepEqual(json(await gateway.handleAsync(unparsed)), {
    request_id: null,
    error: { code: "overloaded", message: "Gateway capacity exhausted", outcome: "not_applied" },
  })
  assert.equal((await first).status, 400)
  assert.equal((await gateway.handleAsync(unparsed)).status, 429)
  assert.equal(received.length, 0)
  release()
  await new Promise(resolve => setImmediate(resolve))
  assert.equal((await gateway.handle(operation)).status, 400)
  assert.equal(calls, 1)
  assert.equal((await gateway.handleAsync(operation)).status, 200)
  assert.equal(received.length, 1)
  await gateway.aclose()
  assert.equal((await gateway.handleAsync(unparsed)).status, 503)
})

test("S3 validates records and resolves optional AWS discovery lazily without a network call", async t => {
  for (const bucket of ["192.168.1.1", "bucket--x-s3", "bucket..name", "amzn-s3-demo-fixture"]) {
    assert.throws(() => new S3(bucket, { region: "eu-west-1", credentials }), TypeError)
  }
  for (const prefix of ["/", "a//b/", "a/../b/", "a\n/", "a\u0085b/", "x".repeat(1024)]) {
    assert.throws(() => new S3("fixture-bucket", { region: "eu-west-1", prefix, credentials }), TypeError)
  }
  assert.throws(() => new AwsCredentials("key", "secret", { expiresAt: new Date(NaN) }), TypeError)
  assert.throws(() => new AwsCredentials("key", "secret", { sessionToken: "x".repeat(8193) }), TypeError)
  assert.throws(() => new S3("fixture-bucket", { region: "us-iso-east-1", credentials }), TypeError)
  const expiry = credentials.expiresAt
  expiry.setUTCFullYear(2000)
  assert.equal(credentials.expiresAt.getUTCFullYear(), 2099)

  const { gateway, received } = await fixture(t, response => send(response))
  gateway.middleware(ctx => {
    ctx.principal = "fixture"
    ctx.storage = new S3("fixture-bucket", { region: "eu-west-1", credentials: async () => {
      throw new Error("provider-secret-sentinel")
    } })
  })
  const response = await gateway.handleAsync(operation)
  assert.equal(response.status, 401)
  assert.doesNotMatch(Buffer.from(response.body).toString(), /sentinel/)
  assert.equal(received.length, 0)

  try {
    import.meta.resolve("@aws-sdk/credential-provider-node")
  } catch {
    t.diagnostic("Optional AWS SDK absent")
    return
  }
  const script = "import { defaultCredentials } from 'nabedra/aws';"
    + "import assert from 'node:assert/strict';"
    + "const provider = defaultCredentials();"
    + "process.env.AWS_ACCESS_KEY_ID = 'fixture-access'; process.env.AWS_SECRET_ACCESS_KEY = 'fixture-secret';"
    + "const [first, second] = await Promise.all([provider(), provider()]);"
    + "assert.equal(first.accessKeyId, 'fixture-access'); assert.equal(second.secretAccessKey, 'fixture-secret');"
  await promisify(execFile)(process.execPath, ["--input-type=module", "-e", script], {
    cwd: import.meta.dirname + "/..",
    env: { AWS_EC2_METADATA_DISABLED: "true" },
    timeout: 5_000,
  })
})

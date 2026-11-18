import assert from "node:assert/strict"
import { createHash, randomUUID } from "node:crypto"
import { once } from "node:events"
import { rename, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import { Socket } from "node:net"

// Independent, native peer for the Rust supervisor and provider wire protocols.
assert.deepEqual(process.argv.slice(2, 4), ["api", "--config"])
assert.deepEqual(process.argv.slice(5), ["--cold-start", "--migrate-catalog"])
const env = process.env
const channel = new Socket({ fd: Number(env.NABEDRA_SUPERVISOR_FD), readable: true, writable: true })
const messages = channel[Symbol.asyncIterator]()
let buffer = Buffer.alloc(0)
async function exact(length) {
  while (buffer.length < length) {
    const chunk = await messages.next()
    if (chunk.done) throw new Error("Supervisor disconnected")
    buffer = Buffer.concat([buffer, chunk.value])
  }
  const result = buffer.subarray(0, length)
  buffer = buffer.subarray(length)
  return result
}
async function receive() {
  const length = (await exact(4)).readUInt32BE()
  return JSON.parse((await exact(length)).toString())
}
function send(value) {
  const bytes = Buffer.from(JSON.stringify(value))
  const prefix = Buffer.alloc(4)
  prefix.writeUInt32BE(bytes.length)
  channel.write(Buffer.concat([prefix, bytes]))
}

const init = await receive()
assert.deepEqual(init.initial_policy, { mode: "autoscale", desired: 1, min_nodes: 1, max_nodes: 3 })
assert.equal(init.name, "Fixture")
assert.equal(init.version, 1)
assert.equal(init.type, "init")
assert.equal(init.provisioning, "managed")
assert.match(init.bridge.url, /^http:\/\/127\.0\.0\.1:[0-9]+\/private\/v1\/callback$/)
assert.notEqual(init.bridge.callback_token, init.bridge.authorize_token)

if (env.NABEDRA_TEST_SERVE_MODE === "fatal") {
  send({ version: 1, type: "fatal", code: "bind_failed" })
  assert.deepEqual(await receive(), { version: 1, type: "shutdown" })
  channel.destroy()
} else {
  const clusterId = randomUUID()
  const invocationId = randomUUID()
  const inventory = Buffer.from(JSON.stringify({
    version: 1, invocation_id: invocationId, cluster_id: clusterId, owner_id: randomUUID(),
    epoch: 1, intent_id: null, action: "inventory", target: null, payload: {},
  }))
  let authorityReads = 0
  const authority = createServer(async (request, response) => {
    ++authorityReads
    assert.equal(request.url, "/private/v1/authorize")
    assert.equal(request.headers.authorization, "Bearer " + init.bridge.authorize_token)
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    const raw = Buffer.concat(chunks)
    assert.deepEqual(raw, inventory)
    const data = Buffer.from(JSON.stringify({
      version: 1, invocation_id: invocationId, authorized: true, command_sha256: createHash("sha256").update(raw).digest("hex"),
    }))
    response.writeHead(200, { "Content-Type": "application/json", "Content-Length": data.length, Connection: "close" })
    response.end(data)
  })
  function shutdown(signal) {
    // Wait for the supervisor to send shutdown, even after the forwarded signal.
    assert.equal(signal, "SIGTERM")
  }
  process.on("SIGTERM", shutdown)
  authority.listen(0, "127.0.0.1")
  await once(authority, "listening")
  const bound = "http://127.0.0.1:" + authority.address().port
  send({ version: 1, type: "prepared", cluster_id: clusterId, bound_url: bound, authorize_url: bound + "/private/v1/authorize" })
  assert.deepEqual(await receive(), { version: 1, type: "activate" })
  send({ version: 1, type: "ready", cluster_id: clusterId, bound_url: bound })
  const response = await fetch(init.bridge.url, {
    method: "POST",
    headers: { Authorization: "Bearer " + init.bridge.callback_token, "Content-Type": "application/json" },
    body: inventory, signal: AbortSignal.timeout(3_000),
  })
  assert.equal(response.status, 200)
  assert.deepEqual(await response.json(), { version: 1, invocation_id: invocationId, result: [] })
  assert.equal(authorityReads, 1)
  const marker = env.NABEDRA_TEST_SERVE_MARKER
  await writeFile(marker + ".tmp", JSON.stringify({ providerInvoked: true }))
  await rename(marker + ".tmp", marker)
  assert.deepEqual(await receive(), { version: 1, type: "shutdown" })
  authority.close()
  channel.destroy()
}

import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { once } from "node:events"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { inspect } from "node:util"
import { Decimal } from "decimal.js"
import { Events, EventsError, EventsProtocolError, EventsUnknownOutcomeError, PublishRecord } from "nabedra/events"

const topicId = "00000000-0000-0000-0000-000000000001"
const eventId = "00000000-0000-0000-0000-000000000002"
const anotherId = "00000000-0000-0000-0000-000000000003"
const tableId = "00000000-0000-0000-0000-000000000004"
const runId = "00000000-0000-0000-0000-000000000005"
const publishedAt = "2026-11-17T12:00:00Z"
const precise = "1.2345678901234567890123456789"
const offset = 9_007_199_254_740_993n
const eventPath = "/v1/topics/orders/partitions/0"
const subscriptionPath = eventPath + "/subscriptions/audit"

function event({ id = eventId, partition = 0, at = offset, data = true } = {}) {
  return { topic_id: topicId, partition, offset: String(at), event_id: id, published_at: publishedAt, data }
}

function reply(response, value, status = 200, headers = {}) {
  const body = value === undefined ? "" : typeof value === "string" ? value : JSON.stringify(value)
  response.writeHead(status, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body), ...headers })
  response.end(body)
}

async function fixture(t, handler) {
  const observed = []
  let failure
  const server = createServer(async (request, response) => {
    try {
      const chunks = []
      for await (const chunk of request) chunks.push(chunk)
      const entry = { method: request.method, path: request.url, headers: request.headers, body: Buffer.concat(chunks).toString() }
      observed.push(entry)
      await handler(entry, response)
    } catch (error) {
      failure ??= error
      response.destroy()
    }
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(async () => {
    server.closeAllConnections()
    await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
    if (failure) throw failure
  })
  return { url: `http://127.0.0.1:${server.address().port}`, observed }
}

async function scripted(t, replies) {
  const host = await fixture(t, (request, response) => {
    assert.equal(request.headers.authorization, "Bearer fixture")
    assert.ok(replies.length > 0, `unexpected request ${request.method} ${request.path}`)
    const next = replies.shift()
    if (next === null) response.destroy()
    else reply(response, ...next)
  })
  return { ...host, client: new Events(host.url, { token: "fixture", allowInsecure: true }) }
}

test("Events publish preserves exact data and identity and leaves ambiguous writes to the caller", async t => {
  const exactEvent = JSON.stringify(event({ data: null })).replace('"data":null', `"data":${precise}`)
  const { client, observed, url } = await scripted(t, [
    null, [exactEvent, 201],
    [{ events: [event({ at: 2n }), event({ id: anotherId, at: 3n })] }],
    [{ events: [event({ at: 2n }), event({ id: anotherId, at: 2n })] }],
  ])
  assert.throws(() => new Events(url, { token: "fixture" }), /HTTPS/)
  assert.throws(() => new Events(url + "?secret=hidden", { token: "fixture", allowInsecure: true }), /HTTPS/)
  assert.throws(() => new Events(url, { token: "header\r\ninjection", allowInsecure: true }), /bearer token/)
  const options = { partition: 0, eventId, data: new Decimal(precise) }
  await assert.rejects(client.publish("orders", options), { name: "EventsUnknownOutcomeError", outcome: "unknown" })
  assert.equal(observed.length, 1)
  const published = await client.publish("orders", options)
  assert.equal(observed[1].body, observed[0].body)
  assert.equal(observed[1].path, eventPath + "/events")
  assert.equal(observed[1].body, `{"data":${precise},"event_id":"${eventId}"}`)
  assert.equal(published.offset, offset)
  assert.equal(published.eventId, eventId)
  assert.equal(published.data.toString(), precise)
  assert.doesNotMatch(inspect(published), new RegExp(precise))

  const events = [new PublishRecord(eventId, { z: 10n, a: ["é", -0, true] }), { eventId: anotherId, data: 1n }]
  const receipts = await client.publishBatch("orders", { partition: 0, events })
  assert.deepEqual(receipts.map(event => [event.eventId, event.offset]), [[eventId, 2n], [anotherId, 3n]])
  assert.equal(observed[2].body, `{"events":[{"data":{"a":["é",-0.0,true],"z":10},"event_id":"${eventId}"},{"data":1,"event_id":"${anotherId}"}]}`)
  await assert.rejects(client.publishBatch("orders", { partition: 0, events }), EventsUnknownOutcomeError)
  assert.throws(() => client.publishBatch("orders", {
    partition: 0, events: [events[0], { eventId, data: false }],
  }), /Duplicate event ID/)
  assert.throws(() => client.publish("orders", { ...options, data: "é".repeat(32_768) }), /byte limit/)
  assert.throws(() => client.publish("orders", { ...options, data: 1n << 64n }), /uint64/)
  assert.throws(() => client.publish("orders", { ...options, data: Number.POSITIVE_INFINITY }), /finite/)
  assert.equal(observed.length, 4)
})

test("Events leases retry only a lost claim and serialize checkpoint, renewal, and release", async t => {
  const firstLease = { lease: "fixture.lease", expires_at: "2035-01-20T01:02:03.123456Z" }
  const renewed = { lease: "renewed.lease", expires_at: "2035-01-20T01:02:12Z" }
  const { client, observed } = await scripted(t, [
    null, [firstLease], [{ ...firstLease, lease: "other.lease" }],
    [{ after: String(offset - 1n), events: [event(), event({ id: anotherId, at: offset + 1n })], delivery: "fixture.delivery" }],
    [renewed], [{ offset: String(offset) }], [undefined, 204],
    [{ code: "outcome_unknown", message: "Retry after the lease settles" }, 504, { "Retry-After": "2" }],
  ])
  const subscription = client.subscription("orders", "audit", { partition: 0 })
  const lease = await subscription.claim()
  const foreign = await subscription.claim()
  assert.equal(observed[0].body, observed[1].body)
  assert.equal(observed[0].path, subscriptionPath + "/claim")
  assert.match(JSON.parse(observed[0].body).claim_id, /^[a-f\d-]{36}$/)
  assert.equal(lease.expiresAt.toISOString(), "2035-01-20T01:02:03.123Z")
  const batch = await lease.poll({ limit: 2 })
  assert.equal(batch.after, offset - 1n)
  assert.deepEqual(batch.events.map(event => event.offset), [offset, offset + 1n])
  assert.doesNotMatch(inspect(batch), /fixture\.delivery/)
  assert.throws(() => foreign.ack(batch, { through: offset }), /from this lease/)
  assert.throws(() => lease.ack(batch, { through: offset - 1n }), /from this lease/)
  const [expiresAt, checkpoint] = await Promise.all([lease.renew(), lease.ack(batch, { through: offset })])
  assert.equal(expiresAt.toISOString(), "2035-01-20T01:02:12.000Z")
  assert.equal(checkpoint, offset)
  assert.equal(observed[4].headers["x-events-lease"], "fixture.lease")
  assert.equal(observed[5].headers["x-events-lease"], "renewed.lease")
  assert.deepEqual(JSON.parse(observed[5].body), { delivery: "fixture.delivery", through: String(offset) })
  await Promise.all([lease.close(), lease.close()])
  assert.equal(observed[6].path, subscriptionPath + "/release")
  assert.equal(observed[6].headers["x-events-lease"], "renewed.lease")
  await assert.rejects(lease.poll(), { name: "EventsError", code: "lease_lost" })
  await assert.rejects(subscription.claim({ claimId: anotherId }), {
    name: "EventsUnknownOutcomeError", retryAfter: 2, claimId: anotherId,
  })
  assert.equal(observed.length, 8)
})

test("Events discovery advances through pages and rejects malformed wire fields", async t => {
  const { client, observed } = await scripted(t, [
    [{ topic_id: topicId, partitions: [0, 4], next_after: 4 }],
    [{ topic_id: topicId, partitions: [8], next_after: null }],
    [{ topic_id: topicId, partitions: [0], next_after: 0 }],
    [{ topic_id: anotherId, partitions: [1], next_after: null }],
    [{ lease: "fixture.lease", expires_at: publishedAt }],
    [{ code: "checkpoint_expired", message: "Expired", partition: 0, earliest_offset: String(offset) }, 410],
    ['{"after":"0","events":[],"delivery":null,"after":"1"}'],
    ['{"after":"0","events":[{"topic_id":"' + topicId + '","partition":0.0,"offset":"1","event_id":"' + eventId
      + '","published_at":"' + publishedAt + '","data":true}],"delivery":"fixture.delivery"}'],
  ])
  const partitions = client.partitions("orders")
  assert.equal(observed.length, 0)
  assert.deepEqual(await partitions.next(), { value: 0, done: false })
  assert.deepEqual(await partitions.next(), { value: 4, done: false })
  assert.equal(observed.length, 1)
  assert.deepEqual(await Array.fromAsync(partitions), [8])
  assert.equal(observed[1].path, "/v1/topics/orders/partitions?limit=100&after=4")
  await assert.rejects(Array.fromAsync(client.partitions("orders")), EventsProtocolError)
  const lease = await client.subscription("orders", "audit", { partition: 0 }).claim()
  await assert.rejects(lease.poll(), error => error instanceof EventsError && error.code === "checkpoint_expired"
    && error.statusCode === 410 && error.partition === 0 && error.earliestOffset === offset)
  await assert.rejects(lease.poll(), EventsProtocolError)
  await assert.rejects(lease.poll(), EventsProtocolError)
})

function createSignal() {
  let resolve
  const promise = new Promise(done => { resolve = done })
  return { promise, resolve }
}

async function until(promise) {
  const timeout = AbortSignal.timeout(5_000)
  return Promise.race([promise, once(timeout, "abort").then(() => { throw new Error("Timed out waiting for fixture barrier") })])
}

async function sinkChild(t, config, source, destination) {
  const dir = await mkdtemp(join(tmpdir(), "nabedra-events-"))
  const path = join(dir, "sink.json")
  await writeFile(path, typeof config === "string" ? config : JSON.stringify(config))
  const child = spawn(process.execPath, [fileURLToPath(new URL("../dist/events/sink.js", import.meta.url)), "--config", path], {
    env: { ...process.env, NABEDRA_EVENTS_TEST_SOURCE: source, NABEDRA_EVENTS_TEST_DESTINATION: destination },
    stdio: ["ignore", "ignore", "pipe"],
  })
  const closed = once(child, "close")
  let diagnostic = ""
  child.stderr.setEncoding("utf-8").on("data", chunk => { diagnostic += chunk })
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL")
    await closed
    await rm(dir, { recursive: true, force: true })
  })
  return { child, closed, diagnostic: () => diagnostic }
}

test("Events sink waits for a receipt and reconciles the same delivery before acknowledging", { timeout: 15_000 }, async t => {
  const pending = createSignal()
  const resume = createSignal()
  const renewed = createSignal()
  const acked = createSignal()
  let ackAttempts = 0
  let committed = false
  let submissions = 0
  let inserted
  let identity
  const source = await fixture(t, (request, response) => {
    assert.equal(request.headers.authorization, "Bearer source")
    if (request.path.endsWith("/claim")) {
      reply(response, { lease: "fixture.lease", expires_at: "2035-01-20T01:02:03Z" })
    } else {
      assert.equal(request.headers["x-events-lease"], "fixture.lease")
      if (request.path.includes("/events?")) {
        if (ackAttempts >= 2) {
          reply(response, { after: String(offset), events: [], delivery: null })
        } else {
          const value = JSON.stringify({
            after: String(offset - 1n), events: [event({ data: { z: null, a: "x" } })], delivery: "fixture.delivery",
          }).replace('"z":null', `"z":${precise}00`)
          reply(response, value)
        }
      } else if (request.path.endsWith("/renew")) {
        renewed.resolve()
        reply(response, { lease: "fixture.lease", expires_at: "2035-01-20T01:02:03Z" })
      } else if (request.path.endsWith("/checkpoint")) {
        assert.equal(committed, true)
        assert.deepEqual(JSON.parse(request.body), { delivery: "fixture.delivery", through: String(offset) })
        if (++ackAttempts === 1) response.destroy()
        else {
          reply(response, { offset: String(offset) })
          acked.resolve()
        }
      } else {
        assert.ok(request.path.endsWith("/release"))
        reply(response, undefined, 204)
      }
    }
  })
  const schema = { event_key: "utf8", partition: "int64", offset: "utf8", published_at: "utf8", data: "utf8" }
  const destination = await fixture(t, async (request, response) => {
    assert.equal(request.headers.authorization, "Bearer destination")
    assert.equal(request.headers["x-nabedra-database"], "analytics-v1")
    const body = request.body ? JSON.parse(request.body) : {}
    if (request.path === "/v1/operations") {
      let data
      if (body.operation === "table.get") {
        assert.equal(body.payload.key, `${topicId}:0:${eventId}`)
        assert.equal(committed, true)
        data = inserted
      } else {
        assert.equal(body.operation, "table.describe")
        data = { id: tableId, name: "events", key: "event_key", format: "parquet", schema }
      }
      reply(response, { request_id: body.request_id, data })
    } else if (request.path === "/v1/requests") {
      assert.equal(body.operation, "table.insert_many")
      assert.equal(body.payload.table_id, tableId)
      assert.equal(body.payload.rows.length, 1)
      inserted = body.payload.rows[0]
      identity = body.request_id
      ++submissions
      reply(response, { request_id: identity, run_id: runId, state: "queued" }, 202)
    } else {
      assert.equal(request.path, `/v1/requests/${runId}/${identity}`)
      pending.resolve()
      await resume.promise
      committed = true
      reply(response, { request_id: identity, run_id: runId, state: "succeeded", result: 1, error: null })
    }
  })
  const config = {
    events: { url: source.url, token_env: "NABEDRA_EVENTS_TEST_SOURCE", topic: "orders", subscription: "audit", allow_insecure: true },
    olap: { url: destination.url, token_env: "NABEDRA_EVENTS_TEST_DESTINATION", table: "events", table_id: tableId,
      database: "analytics-v1", allow_insecure: true },
    partitions: { start: 0, stop: 1 }, poll_interval: 0.05, receipt_wait: 3, renew_interval: 0.05,
  }
  const worker = await sinkChild(t, config, "source", "destination")
  await until(pending.promise)
  await until(renewed.promise)
  assert.equal(ackAttempts, 0)
  assert.equal(submissions, 1)
  resume.resolve()
  await until(acked.promise)
  assert.equal(submissions, 1)
  assert.equal(ackAttempts, 2)
  assert.deepEqual(inserted, {
    event_key: `${topicId}:0:${eventId}`, partition: 0, offset: String(offset), published_at: publishedAt,
    data: `{"a":"x","z":${precise}00}`,
  })
  worker.child.kill("SIGTERM")
  assert.deepEqual(await worker.closed, [0, null], worker.diagnostic())
})


test("Events sink rejects fractional partition tokens and invalid selectors before opening connections", async t => {
  const config = {
    events: { url: "https://source.example", token_env: "NABEDRA_EVENTS_TEST_SOURCE", topic: "orders", subscription: "audit" },
    olap: { url: "https://destination.example", token_env: "NABEDRA_EVENTS_TEST_DESTINATION", table: "events", table_id: tableId },
    partitions: { start: 0, stop: 1 },
  }
  for (const invalid of [
    JSON.stringify(config).replace('"start":0', '"start":0.0'),
    { ...config, olap: { ...config.olap, database: null } },
  ]) {
    const worker = await sinkChild(t, invalid, "source", "destination")
    assert.deepEqual(await worker.closed, [1, null], worker.diagnostic())
    assert.match(worker.diagnostic(), /Check the endpoint, credentials and configuration/)
  }
})

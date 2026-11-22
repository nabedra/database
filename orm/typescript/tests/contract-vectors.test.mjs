import assert from "node:assert/strict"
import { once } from "node:events"
import { readFileSync } from "node:fs"
import { createServer } from "node:http"
import { test } from "node:test"
import { Database, NabedraError, col } from "nabedra"
import { Events, EventsError } from "nabedra/events"

const checkedHeaders = ["authorization", "content-type", "x-nabedra-database", "x-events-lease"]
const requestIdMarker = "@request_id"

async function runOlap(url, action, input) {
  const db = new Database({ url, token: "fixture", mode: "olap", database: "analytics-v1", timeout: 2 })
  try {
    const table = await db.table(input.table)
    if (action === "insert") {
      assert.equal(await table.insert({ id: input.insert_id, age: BigInt(input.insert_age), active: true }), undefined)
      return null
    }
    const query = table.where(col("age").ge(BigInt(input.minimum_age)), { active: true })
      .select("id", "age").orderBy("age", true).limit(input.limit)
    if (action === "collect") return (await query.collect()).map(row => ({ ...row, age: bigintText(row.age) }))
    if (action === "count") {
      const count = await query.count()
      assert.ok(Number.isSafeInteger(count))
      return String(count)
    }
    if (action === "explain") return await query.explain()
    throw new Error(`Unknown OLAP operation ${action}`)
  } finally {
    db.close()
  }
}

function eventResult(event) {
  return {
    topic_id: event.topicId, partition: event.partition, offset: bigintText(event.offset),
    event_id: event.eventId, published_at: event.publishedAt, data: event.data,
  }
}

function bigintText(value) {
  assert.equal(typeof value, "bigint")
  return String(value)
}

async function runEvents(url, action, input) {
  const client = new Events(url, { token: "fixture", allowInsecure: true, timeout: 2 })
  if (action === "publish") {
    return eventResult(await client.publish(input.topic, {
      partition: input.partition, eventId: input.event_id, data: input.data,
    }))
  }
  const lease = await client.subscription(input.topic, input.subscription, { partition: input.partition })
    .claim({ claimId: input.claim_id })
  const page = await lease.poll({ limit: input.limit })
  if (action === "poll") {
    return { after: bigintText(page.after), events: page.events.map(eventResult), delivery: page.delivery }
  }
  if (action === "ack") return bigintText(await lease.ack(page, { through: BigInt(input.through) }))
  throw new Error(`Unknown Events operation ${action}`)
}

function assertError(error, expected) {
  const actual = { type: error.constructor.name, code: error.code, status_code: error.statusCode, outcome: error.outcome }
  if ("message" in expected) actual.message = error.message
  if (error instanceof EventsError) {
    Object.assign(actual, {
      partition: error.partition, retry_after: error.retryAfter,
      earliest_offset: error.earliestOffset === null ? null : bigintText(error.earliestOffset),
    })
    expected = { partition: null, retry_after: null, earliest_offset: null, ...expected }
  }
  assert.deepEqual(actual, expected)
}

for (const [module, run, errorType] of [["olap", runOlap, NabedraError], ["events", runEvents, EventsError]]) {
  test(`${module} shared contract vectors`, { timeout: 20_000 }, async t => {
    const { input, operations, cases } = JSON.parse(
      readFileSync(new URL(`../../contract/${module}/vectors.json`, import.meta.url), "utf8"),
    )
    let requests, responses, requestIds
    const server = createServer(async (request, response) => {
      const chunks = []
      for await (const chunk of request) chunks.push(chunk)
      let body = Buffer.concat(chunks).toString("utf8")
      const headers = Object.fromEntries(
        checkedHeaders.filter(name => name in request.headers).map(name => [name, request.headers[name]]),
      )
      const requestId = request.url === "/v1/operations" ? JSON.parse(body).request_id : undefined
      if (requestId !== undefined) body = body.replace(JSON.stringify(requestId), JSON.stringify(requestIdMarker))
      const reply = responses[requests.length]
      requests.push({ method: request.method, path: request.url, body, headers })
      requestIds.push(requestId)
      if (!reply) {
        response.writeHead(500).end()
        return
      }
      const raw = reply.body.replaceAll(requestIdMarker, requestId ?? requestIdMarker)
      response.writeHead(reply.status, {
        "Content-Type": "application/json", "Content-Length": Buffer.byteLength(raw), ...reply.headers,
      }).end(raw)
    })
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    t.after(() => new Promise(resolve => server.close(resolve)))
    const url = `http://127.0.0.1:${server.address().port}`

    for (const vector of cases) {
      await t.test(vector.name, async () => {
        const operation = { ...operations[vector.operation], ...vector }
        const exchanges = [...operation.setup.map(name => operations[name]), operation]
        requests = []
        requestIds = []
        responses = exchanges.map(step => step.response)
        if (vector.error) {
          await assert.rejects(run(url, vector.operation, input), error => {
            assert.ok(error instanceof errorType)
            assertError(error, vector.error)
            if (module === "olap") assert.equal(error.requestId, requestIds.at(-1))
            return true
          })
        } else {
          assert.deepEqual(await run(url, vector.operation, input), operation.result)
        }
        assert.deepEqual(requests, exchanges.map(step => step.request))
        if (module === "olap") {
          assert.equal(new Set(requestIds).size, exchanges.length)
          for (const id of requestIds) assert.match(id, /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/)
        }
      })
    }
  })
}

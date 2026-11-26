// Adversarial tests of the vector search SDK: VEC-G12 (parity), VEC-G09 (outcomes) and VEC-G11 (limits).
// The scenarios live in orm/contract/vector/adversarial.json, written by orm/python/tests/support/gen_adversarial.py.
import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { test } from "node:test"
import { BatchError, Database, ValidationError, col, vector } from "nabedra"
import { Fake, ok } from "./fixtures/fakeServer.mjs"
import { loadScenarios, problems, runAll, runOne } from "./fixtures/scenarioRunner.mjs"
import { record } from "./fixtures/plainCorpus.mjs"

const row = (over = {}) => ({ id: "a", n: 1n, f: 1, b: true, vec: [1, 2], sp: {}, mu: [], tags: [], ...over })

// Scenarios that fail on the current code, each with its own named test below.
const defects = new Set([])

test("every scenario meets its expectations", { timeout: 300_000 }, async (t) => {
  const { scenarios, transcript } = await runAll()
  assert.ok(scenarios.length > 500)
  for (const scenario of scenarios) {
    if (defects.has(scenario.name)) continue
    await t.test(scenario.name, () => assert.deepEqual(problems(scenario, transcript[scenario.name]), []))
  }
})

test("a row is never applied without proof", { timeout: 300_000 }, async (t) => {
  const { scenarios, transcript } = await runAll()
  for (const scenario of scenarios) {
    const run = transcript[scenario.name]
    const { error } = run.result
    if (error?.type !== "BatchError" || error.outcomes.length === 0) continue
    // With no request recorded there is no reply, so nothing can have been applied.
    const reply = run.requests.length === 0 ? {} : scenario.script[Math.min(run.requests.length, scenario.script.length) - 1]
    const wellFormed = reply.applied !== undefined
      || (reply.data !== undefined && reply.data.includes('"outcomes":[') && !reply.data.includes('"outcomes":{'))
    if (!wellFormed) {
      await t.test(scenario.name, () => assert.deepEqual(error.outcomes.filter((item) => item[1] === "applied"), []))
    }
  }
})

test("every pre-search operation keeps its request bytes", { timeout: 120_000 }, async (t) => {
  // Recorded on nabedra/oltp-vector: 160 operations, twelve kinds of float, big ints, Unicode.
  const golden = JSON.parse(readFileSync(new URL("./fixtures/plainCorpus.json", import.meta.url), "utf8"))
  const corpus = await record()
  assert.deepEqual(Object.keys(corpus).sort(), Object.keys(golden).sort())
  assert.ok(Object.keys(golden).length > 150)
  for (const name of Object.keys(golden)) await t.test(name, () => assert.deepEqual(corpus[name], golden[name]))
})

async function harness(t) {
  const fake = await Fake.start()
  const db = new Database({ url: fake.url, token: "fixture", timeout: 3 })
  t.after(async () => {
    db.close()
    await fake.close()
  })
  return { fake, db, docs: await db.table("docs"), chunks: await db.table("chunks") }
}

const sent = (fake, index = 0) => fake.requests[index].request.payload

test("values are copied when the row is checked", async (t) => {
  const { fake, docs } = await harness(t)
  const vec = new Float32Array([1, 2])
  const tags = ["x"]
  const sparse = { 1: 1 }
  const multi = [new Float32Array([1, 2])]
  function* rows() {
    for (let number = 0; number < 3; number++) {
      yield row({ id: `k${number}`, vec, tags, sp: sparse, mu: multi })
      vec[0] = 99
      tags.push("y")
      sparse[2] = 2
      multi[0][0] = 77
      multi.push(new Float32Array([0, 0]))
    }
  }
  fake.reset({ applied: 3 })
  assert.equal(await docs.upsertMany(rows()), 3)
  const [first, second] = sent(fake).rows
  assert.deepEqual(first.tags, ["x"])
  assert.deepEqual(first.sp, { sparse: { i: "AQAAAA==", f32: "AACAPw==" } })
  assert.equal(first.mu.n, 1)
  assert.deepEqual(second.tags, ["x", "y"])
})

test("a ranking copies its query vector", async (t) => {
  const { fake, chunks } = await harness(t)
  const query = new Float32Array([1, 0, 0])
  const ranking = col("emb").cosine(query)
  query[0] = 0
  query[1] = 1
  fake.reset(ok("[]"))
  await chunks.query().rank(ranking).limit(1).collect()
  assert.deepEqual(sent(fake).rank.rankings[0].value, { f32: "AACAPwAAAAAAAAAA" })
})

test("a one-shot iterator is consumed once", async (t) => {
  const { fake, docs } = await harness(t)
  const iterator = [row({ id: "a" }), row({ id: "b" })][Symbol.iterator]()
  fake.reset({ applied: 2 })
  assert.equal(await docs.upsertMany({ [Symbol.iterator]: () => iterator }), 2)
  assert.equal(await docs.upsertMany({ [Symbol.iterator]: () => iterator }), 0)
  assert.equal(fake.requests.length, 1)
})

test("an async iterable is accepted and a throwing one reports the applied rows", async (t) => {
  const { fake, docs } = await harness(t)
  async function* slow() {
    for (let number = 0; number < 3; number++) {
      await new Promise((resolve) => setTimeout(resolve, 5))
      yield row({ id: `a${number}` })
    }
  }
  fake.reset({ applied: 3 })
  assert.equal(await docs.upsertMany(slow()), 3)
  async function* boom() {
    for (let number = 0; number < 10_005; number++) yield row({ id: `k${number}` })
    throw new RangeError("boom")
  }
  fake.reset({ applied: 10_000 })
  await assert.rejects(docs.upsertMany(boom()), (error) => {
    assert.ok(error instanceof BatchError)
    assert.deepEqual([error.applied, error.outcomes], [10_000, []])
    assert.ok(error.cause instanceof RangeError)
    return true
  })
  assert.equal(fake.requests.length, 1)
})

test("a generator that raises in the first chunk sends nothing", async (t) => {
  const { fake, docs } = await harness(t)
  function* rows() {
    yield row()
    throw new RangeError("boom")
  }
  fake.reset({ applied: 1 })
  await assert.rejects(docs.upsertMany(rows()), RangeError)
  assert.equal(fake.requests.length, 0)
})

test("a handle serves sixteen concurrent calls with distinct request ids", async (t) => {
  const { fake, docs } = await harness(t)
  fake.reset({ applied: 100 })
  const counts = await Promise.all(Array.from({ length: 16 }, (_, number) =>
    docs.upsertMany(Array.from({ length: 100 }, (_, index) => row({ id: `t${number}-${index}` })))))
  assert.deepEqual(counts, Array(16).fill(100))
  assert.equal(new Set(fake.requests.map((item) => item.request.request_id)).size, 16)
  assert.deepEqual(new Set(fake.requests.map((item) => item.request.payload.rows.length)), new Set([100]))
})

test("a sparse reply of half a million entries decodes in linear time", async (t) => {
  // Time grows with size: 20 times the entries may take at most 60 times as long, plus one second.
  // Quadratic decoding takes 400 times as long. Load slows both replies alike.
  const { fake, chunks } = await harness(t)
  async function decode(count) {
    const indices = Buffer.from(Uint32Array.from({ length: count }, (_, index) => index).buffer).toString("base64")
    const weights = Buffer.from(new Float32Array(count).fill(1).buffer).toString("base64")
    fake.reset(ok(`[{"id":"a","sp":{"sparse":{"i":"${indices}","f32":"${weights}"}},"_score":0.5}]`))
    const started = performance.now()
    const hits = await chunks.query().rank(col("sp").dot({ 1: 1 })).select("id", "sp").limit(1).collect()
    const elapsed = performance.now() - started
    assert.equal(Object.keys(hits[0].sp).length, count)
    return elapsed
  }
  const small = await decode(25_000)
  const large = await decode(500_000)
  assert.ok(large < 60 * small + 1_000, `${large} ms for 500,000 entries, ${small} ms for 25,000`)
})

test("a float that rounds up to the largest float32 is kept", async (t) => {
  const { fake, docs } = await harness(t)
  fake.reset({ applied: 1 })
  await docs.upsertMany([row({ vec: [3.4028235677973362e38, -3.4028235677973362e38] })])
  const largest = new Float32Array([3.4028234663852886e38, -3.4028234663852886e38])
  assert.deepEqual(sent(fake).rows[0].vec, { f32: Buffer.from(largest.buffer).toString("base64") })
})

test("dense values equal their float32 buffer", async (t) => {
  const { fake, docs } = await harness(t)
  for (const number of [0.1, 1 / 3, 1e-45, 16777217, -0, 5e-324]) {
    fake.reset({ applied: 2 })
    await docs.upsertMany([row({ id: "a", vec: [number, 0] }), row({ id: "b", vec: new Float32Array([number, 0]) })])
    const [first, second] = sent(fake).rows
    assert.deepEqual(first.vec, second.vec, String(number))
  }
})

test("a ranking built from a typed array view sends only the view", async (t) => {
  const { fake, chunks } = await harness(t)
  const view = new Float32Array([9, 1, 0, 0, 9]).subarray(1, 4)
  fake.reset(ok("[]"))
  await chunks.query().rank(col("emb").cosine(view)).limit(1).collect()
  assert.deepEqual(sent(fake).rank.rankings[0].value, { f32: "AACAPwAAAAAAAAAA" })
})

test("the README example runs", async (t) => {
  const { fake, db } = await harness(t)
  const text = readFileSync(new URL("../README.md", import.meta.url), "utf8")
  const section = text.split("## Vector search")[1].split(/\n## /)[0]
  const source = /```typescript\n([\s\S]*?)```/.exec(section)[1].replace(/^import .*\n/m, "")
  const run = new (Object.getPrototypeOf(async () => {}).constructor)("db", "rows", "col", "list", "vector", `${source}\nreturn hits`)
  const { list } = await import("nabedra")
  fake.reset({ echo_create: true }, ok('[{"id":"doc7#0","body":"text","_score":0.88}]'))
  const hits = await run(db, [], col, list, vector)
  assert.deepEqual(hits, [{ id: "doc7#0", body: "text", _score: 0.88 }])
  assert.deepEqual(fake.requests.map((item) => item.request.operation), ["table.create", "query.rank"])
  assert.equal(sent(fake, 0).partition_by, "tenant")
  assert.equal(sent(fake, 1).rank.rankings[0].value, "reset my password")
})

test("errors are Nabedra errors, never raw", async (t) => {
  const { docs } = await harness(t)
  const calls = [
    () => docs.upsertMany([null]), () => docs.upsertMany([row({ vec: {} })]),
    () => docs.upsertMany([row({ sp: { 1: {} } })]), () => docs.upsertMany([row({ mu: [{}] })]),
    () => docs.upsertMany([row({ tags: [1] })]), () => docs.deleteMany([{}]),
    () => docs.upsertMany([row({ n: 2n ** 64n })]), () => docs.upsertMany([row({ f: NaN })]),
    () => docs.upsertMany([row({ vec: new Float32Array([NaN, 1]) })]),
    () => docs.upsertMany([row({ vec: [1, 2], extra: 1 })]),
  ]
  for (const call of calls) await assert.rejects(call, (error) => error.constructor.name.endsWith("Error") && error.code !== undefined)
})

// Former defects, fixed in b0cab01b. Each test failed before the fix.

test("rank, rerank, rankMany and upsertMany refuse an option they do not know, as Python does", async (t) => {
  const { fake, chunks } = await harness(t)
  const ranking = col("emb").cosine([1, 0, 0])
  fake.reset(ok("[]"), ok("[]"), ok("[[]]"), { applied: 0 })
  const typos = [
    async () => chunks.query().rank(ranking, { recal: 0.5 }).limit(1).collect(),
    async () => chunks.query().rank(ranking).rerank(col("mu").cosine([[1, 0]]), { candidate: 3 }).limit(1).collect(),
    async () => chunks.query().rankMany([ranking], { limit: 1, exct: true }),
    async () => chunks.upsertMany([], { waitForEmbeding: true }),
  ]
  for (const call of typos) {
    await assert.rejects(call, (error) => error instanceof ValidationError || error instanceof TypeError)
  }
  assert.equal(fake.requests.length, 0, "a request with a misspelled option was sent")
})

test("a not_applied row whose error code is outcome_unknown is not an UnknownOutcomeError", async (t) => {
  // Python maps the unknown code to NabedraError. The row says not_applied, so its class must not say unknown.
  const { fake, db } = await harness(t)
  const scenario = loadScenarios().find((item) => item.name === "not_applied with the unknown code")
  const run = await runOne(fake, db, scenario)
  assert.equal(run.result.error.type, "BatchError")
  const [key, outcome, errorClass] = run.result.error.outcomes[0]
  assert.deepEqual([key, outcome], ["a", "not_applied"])
  assert.notEqual(errorClass, "UnknownOutcomeError")
})

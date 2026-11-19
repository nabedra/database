import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { createRequire } from "node:module"
import { setTimeout as sleep } from "node:timers/promises"
import { dirname, resolve } from "node:path"
import * as nabedra from "nabedra"
import { Cache } from "nabedra/cache"

const vectors = JSON.parse(readFileSync(new URL("./vectors.json", import.meta.url), "utf8"))
const config = JSON.parse(process.env.NABEDRA_CACHE_TEST_CONFIG)
const require = createRequire(import.meta.url)
const manifest = resolve(dirname(require.resolve("nabedra")), "..", "package.json")
assert.equal(JSON.parse(readFileSync(manifest, "utf8")).version, vectors.version)
assert.equal(nabedra.Cache, Cache)
assert.equal(typeof Cache.connect, "function")
assert.deepEqual(Object.getOwnPropertyNames(Cache.prototype).sort(),
  ["close", "constructor", "delete", "expire", "get", "increment", "persist", "set", "ttlMs"])

async function expectError(name, command, code, outcome) {
  await assert.rejects(command, error => error.constructor.name === name &&
    (!code || error.code === code) && (!outcome || error.outcome === outcome))
}

async function awaitFixture(observer, event) {
  const deadline = performance.now() + 2_000
  while (!binary(await observer.get(event)).equals(Buffer.from("1"))) {
    assert(performance.now() < deadline, event)
    await sleep(5)
  }
}

function binary(value) {
  assert(value instanceof Uint8Array)
  return Buffer.from(value)
}

async function semantics(cache) {
  for (const [command, key, ...args] of vectors.operations) {
    switch (command) {
      case "get": {
        const value = await cache.get(key)
        if (args[0] === null) assert.equal(value, null)
        else assert.deepEqual(binary(value), Buffer.from(args[0], "hex"))
        break
      }
      case "set": {
        const [data, ttlMs, when, expected] = args
        assert.equal(await cache.set(key, Buffer.from(data, "hex"), { ttlMs: ttlMs ?? undefined, when }), expected)
        break
      }
      case "ttl": {
        const value = await cache.ttlMs(key)
        const expected = args[0]
        if (Array.isArray(expected)) assert(value >= expected[0] && value <= expected[1], `unexpected TTL ${value}`)
        else assert.equal(value, expected)
        break
      }
      case "expire":
        assert.equal(await cache.expire(key, args[0]), args[1])
        break
      case "persist":
        assert.equal(await cache.persist(key), args[0])
        break
      case "delete": {
        const result = await cache.delete(key)
        if (args.length) assert.equal(result, args[0])
        break
      }
      case "increment":
        assert.equal(await cache.increment(key, BigInt(args[0])), BigInt(args[1]))
        break
      case "incrementError":
        await expectError("ValidationError", () => cache.increment(key, BigInt(args[0])))
        break
      case "waitMiss": {
        const deadline = performance.now() + 2000
        while (await cache.get(key) !== null) {
          assert(performance.now() < deadline, key)
          await sleep(5)
        }
        assert.equal(await cache.ttlMs(key), -2)
        break
      }
      default:
        assert.fail(command)
    }
  }
  const large = Buffer.alloc(1024 * 1024, Buffer.from([0, 255]))
  assert.equal(await cache.set("large", large), true)
  assert.deepEqual(binary(await cache.get("large")), large)
  assert.equal(await cache.delete("large"), true)
}

async function fixture() {
  const endpoint = config.fixture
  const auth = { username: "test", password: "secret", namespace: vectors.namespace }
  await expectError("AuthenticationError", () => Cache.connect(endpoint, { username: "test", password: "bad" }))
  const cache = await Cache.connect(endpoint, { ...auth, timeoutMs: 500 })
  try {
    for (const [key, encoded] of vectors.keys) assert.deepEqual(binary(await cache.get(key)), Buffer.from(encoded))
    for (const [key, error, code, outcome] of vectors.serviceErrors) {
      await expectError(error, () => cache.set(key, Buffer.from("x")), code, outcome)
    }
    const invalid = [
      () => cache.get(""), () => cache.get("é".repeat(126)), () => cache.get("\ud800"),
      () => cache.set("k", Buffer.from("a"), { ttlMs: 0 }),
      () => cache.set("k", Buffer.from("a"), { ttlMs: 2 ** 53 }),
      () => cache.set("k", Buffer.from("a"), { ttlMs: true }),
      () => cache.set("k", Buffer.from("a"), { when: "maybe" }),
      () => cache.set("k", new Uint8Array((1 << 20) + 1)), () => cache.expire("k", -1),
      () => cache.increment("k", 1n << 63n), () => cache.increment("k", true),
    ]
    for (const command of invalid) await expectError("ValidationError", command, "validation_error", "not_applied")
    await expectError("UnknownOutcomeError", () => cache.increment("drop", 1n), "outcome_unknown", "unknown")
    assert.deepEqual(binary(await cache.get("drop")), Buffer.from("1"))
    await expectError("UnknownOutcomeError", () => cache.increment("garbled", 1n))
    assert.deepEqual(binary(await cache.get("garbled")), Buffer.from("1"))
    await expectError("ProtocolError", () => cache.get("badframe"))
    await expectError("TransportError", () => cache.get("stall"))
    const start = performance.now()
    await expectError("UnknownOutcomeError", () => cache.increment("deadline", 1n))
    assert(performance.now() - start < 1500)
    assert.deepEqual(binary(await cache.get("deadline")), Buffer.from("1"))
  } finally {
    await cache.close()
  }
  await cache.close()
  await expectError("TransportError", () => cache.get("closed"), "transport_error", "not_applied")

  await expectError("ValidationError", () => Cache.connect("redis://192.0.2.1"))
  await expectError("ValidationError", () => Cache.connect(endpoint, { namespace: "UPPER" }))
  await expectError("ValidationError", () => Cache.connect(endpoint, { namespace: "ok\n" }))
  await expectError("TransportError", () => Cache.connect(config.badTls, { ...auth, caFile: config.fixtureCa }))
  const secure = await Cache.connect(config.tls, { ...auth, caFile: config.fixtureCa })
  try {
    assert.deepEqual(binary(await secure.get("sku:42")), Buffer.from("nb:catalog:c2t1OjQy"))
  } finally {
    await secure.close()
  }

  const closureObserver = await Cache.connect(endpoint, auth)
  try {
    for (const [phase, target] of [["auth", config.closeAuth], ["tls", config.closeTls]]) {
      const closing = await Cache.connect(config.tls, { ...auth, cluster: true, allowedNodes: [target],
        caFile: config.fixtureCa, timeoutMs: 10_000, poolSize: 1 })
      const write = () => closing.increment(`close-${phase}`, 1n)
      const pending = write().then(() => null, error => error)
      try {
        await awaitFixture(closureObserver, `${phase}-entered`)
        await expectError("OverloadedError", () => closing.get("full"), "overloaded", "not_applied")
      } finally {
        await closing.close()
      }
      await awaitFixture(closureObserver, `${phase}-closed`)
      const error = await pending
      assert.equal(error?.constructor.name, "TransportError")
      assert.equal(error.code, "transport_error")
      assert.equal(error.outcome, "not_applied")
      await expectError("TransportError", write, "transport_error", "not_applied")
    }
  } finally {
    await closureObserver.close()
  }

  const limited = await Cache.connect(endpoint, { ...auth, poolSize: 1 })
  const observer = await Cache.connect(endpoint, auth)
  try {
    const pending = limited.get("held")
    try {
      const deadline = performance.now() + 1_000
      while (!binary(await observer.get("held-entered")).equals(Buffer.from("1"))) {
        assert(performance.now() < deadline)
        await sleep(5)
      }
      await expectError("OverloadedError", () => limited.get("anything"), "overloaded", "not_applied")
    } finally {
      assert.deepEqual(binary(await observer.get("held-release")), Buffer.from("1"))
    }
    assert.deepEqual(binary(await pending), Buffer.from("released"))
  } finally {
    await observer.close()
    await limited.close()
  }

  const seed = config.seed
  const redirected = await Cache.connect(seed, { ...auth, cluster: true, allowedNodes: [config.target] })
  try {
    assert.equal(await redirected.increment("move", 11n), 11n)
    assert.equal(await redirected.increment("move", 2n), 13n)
    assert.deepEqual(binary(await redirected.get("move-count")), Buffer.from("2"))
    assert.deepEqual(binary(await redirected.get("move-seed-count")), Buffer.from("1"))
    await expectError("ProtocolError", () => redirected.increment("wrong-slot", 1n))
    await expectError("ProtocolError", () => redirected.increment("untrusted", 1n))
  } finally {
    await redirected.close()
  }
  const allowSeedOnly = await Cache.connect(seed, { ...auth, cluster: true })
  try {
    await expectError("ProtocolError", () => allowSeedOnly.increment("move", 1n))
  } finally {
    await allowSeedOnly.close()
  }
}

if (config.fixture) await fixture()
if (config.endpoint) {
  const cache = await Cache.connect(config.endpoint, { namespace: vectors.namespace,
    username: config.username, password: config.password, caFile: config.caFile, cluster: config.cluster,
    allowedNodes: config.allowedNodes, allowedNodeSuffix: config.allowedNodeSuffix })
  try {
    await semantics(cache)
  } finally {
    await cache.close()
  }
}
console.log("TypeScript installed Cache contract passed")

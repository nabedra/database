import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { createRequire } from "node:module"
import { createClient, RESP_TYPES } from "redis"

const require = createRequire(import.meta.url)
assert.equal(require("redis/package.json").version, "5.8.2")
const settings = JSON.parse(process.env.NABEDRA_STOCK_CONFIG)
const fixtures = JSON.parse(readFileSync(new URL("./standalone_stock.json", import.meta.url)))
const client = createClient({
  RESP: 2,
  socket: {
    host: "127.0.0.1",
    port: settings.port,
    connectTimeout: 2000,
    reconnectStrategy: false,
    ...(settings.caFile ? {
      tls: true,
      servername: "localhost",
      ca: readFileSync(settings.caFile),
    } : {}),
  },
  username: settings.username,
  password: settings.password,
})
// Use RESP2's exact decimal text for int64 replies. The driver's default
// Number representation rounds some well-formed integers.
const options = { typeMapping: { [RESP_TYPES.BLOB_STRING]: Buffer, [RESP_TYPES.NUMBER]: String } }
client.on("error", () => {})
await client.connect()
try {
  for (const { command, reply } of fixtures) {
    const args = command.map(argument => typeof argument === "object" ?
      Buffer.from(argument.hex, "hex") : String(argument))
    if (reply?.error) {
      await assert.rejects(client.sendCommand(args, options), error => error.message.includes(reply.error))
    } else {
      compare(await client.sendCommand(args, options), reply, command[0])
    }
  }
  const hello = await client.sendCommand(["HELLO", "2"], options)
  assert.equal(hello[hello.findIndex(value => Buffer.isBuffer(value) && value.toString() === "proto") + 1], "2")
} finally {
  client.destroy()
}

function compare(actual, expected, operation) {
  if (Array.isArray(expected)) {
    assert.equal(actual.length, expected.length, operation)
    expected.forEach((value, i) => compare(actual[i], value, operation))
  } else if (expected?.range) {
    assert(actual >= expected.range[0] && actual <= expected.range[1], operation)
  } else if (expected?.integer) {
    assert.equal(actual, expected.integer, operation)
  } else if (expected?.hex !== undefined) {
    assert.deepEqual(actual, Buffer.from(expected.hex, "hex"), operation)
  } else if (typeof expected === "number") {
    assert.equal(actual, String(expected), operation)
  } else {
    assert.equal(actual, expected?.simple ?? expected, operation)
  }
}

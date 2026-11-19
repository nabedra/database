import { existsSync } from "node:fs"
import { setTimeout as sleep } from "node:timers/promises"
import { Cache } from "nabedra/cache"

const config = JSON.parse(process.env.NABEDRA_CACHE_REPLAY)
const options = {
  username: config.username, password: config.password, caFile: config.caFile,
  allowedNodes: config.allowed_nodes, cluster: true, namespace: config.namespace, timeoutMs: 1500
}
const value = Buffer.from("00ff400a", "hex")
const ttlMs = config.limitSeconds * 1000 + 300000
const cache = await Cache.connect(config.seeds, options)
for (let index = 0; index < 64; index++) {
  if (!await cache.set(`provider-ts-entry-${index}`, value, { ttlMs })) {
    throw new Error("provider replay preload failed")
  }
}
const started = process.hrtime.bigint()
let number = 0
while (!existsSync(config.stop) && process.hrtime.bigint() - started < BigInt(config.limitSeconds) * 1000000000n) {
  const scheduled = started + BigInt(number) * 50000000n
  const waitMs = Number(scheduled - process.hrtime.bigint()) / 1e6
  if (waitMs > 0) await sleep(waitMs)
  const key = `provider-ts-entry-${number % 64}`
  const command = number % 5 ? "get" : "set"
  let outcome = "ok"
  try {
    if (command === "set") {
      if (!await cache.set(key, value, { ttlMs })) outcome = "incorrect_reply"
    } else {
      const result = await cache.get(key)
      if (result === null) {
        outcome = "origin_refill_miss"
      } else if (!Buffer.from(result).equals(value)) outcome = "incorrect_reply"
    }
  } catch (error) {
    outcome = error.code ?? "driver_error"
  }
  console.log(JSON.stringify({
    arrivalNs: Number(scheduled - started), completedNs: Number(process.hrtime.bigint() - started),
    command, outcome
  }))
  number++
}
await cache.close()

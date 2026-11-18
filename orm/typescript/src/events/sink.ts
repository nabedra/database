import { open } from "node:fs/promises"
import { pathToFileURL } from "node:url"
import { parseArgs } from "node:util"
import { Decimal } from "decimal.js"
import { Database } from "../client.js"
import { NabedraError, OverloadedError, StorageError, TransportError } from "../errors.js"
import { isJsonObject, type JsonObject } from "../transport.js"
import { validateIdentifier } from "../validation.js"
import { Events, eventName, wireUuid, type Lease } from "./client.js"
import {
  EventsError, EventsTransportError, EventsUnknownOutcomeError, boundedInt, endpoint, jsonLoads,
  maxPageCount, maxPartition, positiveSeconds, type EventsOptions,
} from "./http.js"
import { RetryLater, SinkPaused, Writer, waitOrStop, type Permits } from "./ingest.js"

const configBytes = 64 * 1024
const maxWorkers = 32
const maxRange = 10_000

interface Endpoint extends EventsOptions {
  url: string
  timeout: number
}

interface Config {
  events: Endpoint
  olap: Endpoint
  database: string | null
  table: string
  tableId: string
  topic: string
  subscription: string
  start: number
  stop: number
  workers: number
  maxInFlight: number
  batchSize: number
  pollInterval: number
  receiptWait: number
  renewInterval: number
}

function duration(value: unknown): number {
  const result = positiveSeconds(value instanceof Decimal ? value.toNumber() : value)
  if (result < 0.05 || result > 30) throw new RangeError("Sink duration must be between 0.05 and 30 seconds")
  return result
}

function fields(value: unknown, required: readonly string[], optional: readonly string[] = []): JsonObject {
  if (!isJsonObject(value) || required.some(key => !Object.hasOwn(value, key))
    || Object.keys(value).some(key => !required.includes(key) && !optional.includes(key))) {
    throw new TypeError("Invalid configuration object with required fields " + [...required].sort().join(", "))
  }
  return value
}

function fieldOr(value: JsonObject, key: string, fallback: unknown): unknown {
  return Object.hasOwn(value, key) ? value[key] : fallback
}

function endpointFromConfig(value: JsonObject): Endpoint {
  const env = value.token_env
  if (typeof env !== "string" || !/^[A-Za-z_][A-Za-z0-9_]{0,127}(?![\s\S])/.test(env)) {
    throw new TypeError("Endpoint token_env must name an environment variable")
  }
  const token = process.env[env]
  if (!token) throw new TypeError("Endpoint token environment variable is empty or unset")
  const allowInsecure = fieldOr(value, "allow_insecure", false)
  const url = endpoint(value.url, allowInsecure)
  const caFile = fieldOr(value, "ca_file", null)
  if (caFile !== null && (typeof caFile !== "string" || !caFile)) throw new TypeError("Endpoint ca_file must be a nonempty path")
  return { url, token, caFile, allowInsecure: allowInsecure as boolean, timeout: duration(fieldOr(value, "timeout", 3)) }
}

async function loadConfig(path: string): Promise<Config> {
  const file = await open(path)
  let raw: Buffer
  try {
    const buffer = Buffer.alloc(configBytes + 1)
    let size = 0
    while (size < buffer.length) {
      const { bytesRead } = await file.read(buffer, size, buffer.length - size, size)
      if (!bytesRead) break
      size += bytesRead
    }
    if (size > configBytes) throw new RangeError("Sink configuration exceeds 64 KiB")
    raw = buffer.subarray(0, size)
  } finally {
    await file.close()
  }
  const config = fields(jsonLoads(raw, true), ["events", "olap", "partitions"],
    ["workers", "max_in_flight", "batch_size", "poll_interval", "receipt_wait", "renew_interval"])
  const required = ["url", "token_env"]
  const optional = ["ca_file", "allow_insecure", "timeout"]
  const source = fields(config.events, [...required, "topic", "subscription"], optional)
  const destination = fields(config.olap, [...required, "table", "table_id"], [...optional, "database"])
  const partitions = fields(config.partitions, ["start", "stop"])
  const start = boundedInt(partitions.start, maxPartition)
  const stop = boundedInt(partitions.stop, Math.min(maxPartition + 1, start + maxRange), start + 1)
  const workers = boundedInt(fieldOr(config, "workers", Math.min(4, stop - start)), Math.min(maxWorkers, stop - start), 1)
  const maxInFlight = boundedInt(fieldOr(config, "max_in_flight", workers), workers, 1)
  const batchSize = boundedInt(fieldOr(config, "batch_size", maxPageCount), maxPageCount, 1)
  let database = null
  if (Object.hasOwn(destination, "database")) {
    if (typeof destination.database !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}(?![\s\S])/.test(destination.database)) {
      throw new TypeError("Invalid OLAP database selector")
    }
    database = destination.database
  }
  return {
    events: endpointFromConfig(source), olap: endpointFromConfig(destination), database,
    table: validateIdentifier(destination.table), tableId: wireUuid(destination.table_id),
    topic: eventName(source.topic), subscription: eventName(source.subscription),
    start, stop, workers, maxInFlight, batchSize,
    pollInterval: duration(fieldOr(config, "poll_interval", 0.5)),
    receiptWait: duration(fieldOr(config, "receipt_wait", 2)),
    renewInterval: duration(fieldOr(config, "renew_interval", 5)),
  }
}

class Heartbeat {
  readonly #lease: Lease
  readonly #stop: AbortSignal
  readonly #done = new AbortController()
  readonly #loop: Promise<void>
  #renewal: Promise<Date> | null = null
  #failure: unknown = null

  constructor(lease: Lease, stop: AbortSignal, interval: number) {
    this.#lease = lease
    this.#stop = stop
    this.#loop = this.#run(interval)
  }

  async #run(interval: number): Promise<void> {
    const stop = AbortSignal.any([this.#stop, this.#done.signal])
    try {
      while (!stop.aborted) {
        const remaining = (this.#lease.expiresAt.getTime() - Date.now()) / 1_000
        const delay = Math.min(interval, Math.max(0.05, remaining / 3))
        if (await waitOrStop(delay, stop)) return
        this.#renewal = this.#lease.renew()
        await this.#renewal
        this.#renewal = null
      }
    } catch (error) {
      this.#failure = error
    }
  }

  async check(): Promise<void> {
    if (this.#stop.aborted) throw new RetryLater()
    await this.#renewal
    if (this.#failure !== null) throw this.#failure
    if (this.#lease.expiresAt.getTime() <= Date.now()) throw new RetryLater()
  }

  async close(): Promise<void> {
    this.#done.abort()
    await this.#loop
  }
}

async function consumePartition(config: Config, events: Events, writer: Writer, stop: AbortSignal,
  partition: number): Promise<boolean> {
  if (!await writer.ready(partition)) return false
  const sub = events.subscription(config.topic, config.subscription, { partition })
  const lease = await sub.claim()
  let failed = false
  try {
    const batch = await lease.poll({ limit: config.batchSize })
    if (!batch.events.length) return false
    const heartbeat = new Heartbeat(lease, stop, config.renewInterval)
    try {
      if (!await writer.commit(partition, batch.events, () => heartbeat.check())) return false
      await writer.destination()
      await heartbeat.check()
      await lease.ack(batch, { through: batch.events.at(-1)!.offset })
      writer.acknowledged(partition)
      return true
    } finally {
      await heartbeat.close()
    }
  } catch (error) {
    failed = true
    throw error
  } finally {
    try {
      await lease.close()
    } catch (error) {
      if (!failed || !(error instanceof EventsError)) throw error
    }
  }
}

function retryable(error: EventsError): boolean {
  return error instanceof EventsTransportError || error instanceof EventsUnknownOutcomeError
    || error.statusCode === 429 || error.statusCode === 503 || ["lease_busy", "lease_held", "lease_lost"].includes(error.code)
}

async function consume(config: Config, events: Events, writer: Writer, stop: AbortSignal, partitions: number[]): Promise<boolean> {
  const active = new Set(partitions)
  const retryAt = new Map<number, number>()
  try {
    while (active.size && !stop.aborted) {
      let progressed = false
      for (const partition of partitions) {
        if (stop.aborted) break
        if (!active.has(partition) || performance.now() < (retryAt.get(partition) ?? 0)) continue
        try {
          if (await consumePartition(config, events, writer, stop, partition)) {
            retryAt.delete(partition)
            progressed = true
          } else {
            retryAt.set(partition, performance.now() + config.pollInterval * 1_000)
          }
        } catch (error) {
          if (error instanceof EventsError && retryable(error)) {
            retryAt.set(partition, performance.now() + Math.max(config.pollInterval, error.retryAfter ?? 0) * 1_000)
          } else if (error instanceof RetryLater || error instanceof OverloadedError
            || error instanceof StorageError || error instanceof TransportError) {
            retryAt.set(partition, performance.now() + config.pollInterval * 1_000)
          } else {
            if (error instanceof EventsError) console.error(`ERROR Partition ${partition} paused after Events error ${error.code}`)
            else if (error instanceof SinkPaused) console.error(`ERROR Partition ${partition} paused. ${error.message}`)
            else if (error instanceof NabedraError) console.error(`ERROR Partition ${partition} paused after Database error`)
            else throw error
            active.delete(partition)
          }
        }
      }
      if (!progressed) await waitOrStop(config.pollInterval, stop)
    }
    return active.size > 0
  } finally {
    writer.close()
  }
}

async function run(config: Config, stop: AbortController): Promise<number> {
  let available = config.maxInFlight
  const permits: Permits = {
    acquire() {
      if (available === 0) return false
      --available
      return true
    },
    release() { ++available },
  }
  const lanes = Array.from({ length: config.workers }, (_, lane) => {
    const events = new Events(config.events.url, config.events)
    const { url, token, timeout, caFile } = config.olap
    const database = new Database({ url, token, mode: "olap", database: config.database, timeout, caFile })
    const writer = new Writer(database, config.table, config.tableId, permits, stop.signal, config)
    const partitions = []
    for (let p = config.start + lane; p < config.stop; p += config.workers) partitions.push(p)
    return { events, writer, partitions }
  })
  const jobs = lanes.map(async ({ events, writer, partitions }) => {
    try {
      return await consume(config, events, writer, stop.signal, partitions)
    } catch (error) {
      stop.abort()
      throw error
    }
  })
  const results = await Promise.allSettled(jobs)
  stop.abort()
  for (const result of results) if (result.status === "rejected") throw result.reason
  return results.some(result => result.status === "fulfilled" && result.value) ? 0 : 1
}

export async function main(): Promise<void> {
  let configPath: string
  try {
    const { values } = parseArgs({ options: { config: { type: "string" }, help: { type: "boolean", short: "h" } } })
    if (values.help) {
      console.log("Ingest Events into an append-only OLAP table. Usage: sink --config PATH")
      return
    }
    if (!values.config) throw new TypeError("Missing sink config")
    configPath = values.config
  } catch {
    console.error("ERROR Usage: sink --config PATH")
    process.exitCode = 2
    return
  }

  const stop = new AbortController()
  const requestStop = () => stop.abort()
  process.on("SIGINT", requestStop)
  process.on("SIGTERM", requestStop)
  try {
    process.exitCode = await run(await loadConfig(configPath), stop)
  } catch {
    console.error("ERROR Could not start the Events sink. Check the endpoint, credentials and configuration.")
    process.exitCode = 1
  } finally {
    process.off("SIGINT", requestStop)
    process.off("SIGTERM", requestStop)
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main()

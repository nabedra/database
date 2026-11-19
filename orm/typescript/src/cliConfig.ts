import { randomUUID } from "node:crypto"
import { closeSync, constants, fstatSync, openSync, readSync, writeFileSync } from "node:fs"
import { isIP } from "node:net"
import { dirname, resolve } from "node:path"
import { encodeJson, integerNumberToken, isJsonObject, parseJson, type JsonObject } from "./transport.js"
import { validateIdentifier } from "./validation.js"

export class ConfigError extends Error {}

export function readFile(path: string, { secret = false, limit = 131_072 } = {}): Buffer {
  const descriptor = openSync(path, constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW)
  try {
    const info = fstatSync(descriptor)
    if (!info.isFile() || (secret && (info.mode & 0o077) !== 0)) {
      throw new ConfigError("Secret files must be regular files with mode 0600 or stricter")
    }
    const data = Buffer.allocUnsafe(limit + 1)
    let total = 0
    while (total < data.length) {
      const count = readSync(descriptor, data, total, data.length - total, null)
      if (count === 0) break
      total += count
    }
    if (total > limit) throw new ConfigError("Configuration or secret exceeds size limit")
    return data.subarray(0, total)
  } finally {
    closeSync(descriptor)
  }
}

export function readJson(path: string, secret = false): unknown {
  const data = readFile(path, { secret })
  try {
    return parseJson(data, { numberToken: integerNumberToken })
  } catch {
    throw new ConfigError("Invalid configuration JSON")
  }
}

const maxInt64 = (1n << 63n) - 1n
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?![\s\S])/

export function fields(value: unknown, required: readonly string[], optional: readonly string[] = []): JsonObject {
  if (!isJsonObject(value) || required.some(field => !Object.hasOwn(value, field))
    || Object.keys(value).some(field => !required.includes(field) && !optional.includes(field))) {
    throw new ConfigError("Missing or unknown configuration fields")
  }
  return value
}

function integer(value: unknown, low: number, high: number | bigint): number | bigint {
  if ((typeof value !== "bigint" && !Number.isSafeInteger(value)) || (value as number) < low || (value as number) > high) {
    throw new ConfigError("Configuration integer outside permitted range")
  }
  return value as number | bigint
}

function text(value: unknown): string {
  if (typeof value !== "string" || !value || [...value].length > 2_048 || /[\x00-\x1f]/.test(value)) {
    throw new ConfigError("Invalid configuration text")
  }
  return value
}

function loopback(host: string): boolean {
  const unbracketed = host.replace(/^\[|\]$/g, "")
  return unbracketed === "localhost" || /^127\./.test(unbracketed) && isIP(unbracketed) === 4
    || isIP(unbracketed) === 6 && new URL("http://[" + unbracketed + "]").hostname === "[::1]"
}

export function origin(value: unknown): URL {
  const source = text(value)
  if (!/^https?:\/\/[^/?#@\\]+\/?(?![\s\S])/.test(source) || /\s/.test(source)) {
    throw new ConfigError("Links require HTTPS except loopback fixtures")
  }
  const parsed = new URL(source)
  if (parsed.protocol !== "https:" && !(parsed.protocol === "http:" && loopback(parsed.hostname))) {
    throw new ConfigError("Links require HTTPS except loopback fixtures")
  }
  return parsed
}

function listen(value: unknown, tls: unknown): void {
  const match = /^(.*):([0-9]+)(?![\s\S])/.exec(text(value))
  if (!match) throw new ConfigError("Invalid listener")
  integer(Number(match[2]), 0, 65_535)
  if (!tls && !loopback(match[1])) throw new ConfigError("External listeners require TLS")
}

export function s3Storage(uri: unknown, region: unknown): JsonObject {
  const match = /^s3:\/\/([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])(?:\/([^?#%\\]*))?(?![\s\S])/.exec(text(uri))
  if (!match || match[1].includes("..") || isIP(match[1])) {
    throw new ConfigError("Expected a general-purpose s3://bucket/prefix/ URI")
  }
  const prefix = (match[2] ?? "").replace(/^\/+/, "")
  if (prefix && prefix.replace(/\/+$/, "").split("/").some(item => [".", "..", ""].includes(item))) {
    throw new ConfigError("Invalid storage prefix")
  }
  if (typeof region !== "string" || !/^[a-z]{2}(?:-[a-z0-9]+)+-\d+(?![\s\S])/.test(region)) {
    throw new ConfigError("A valid AWS region is required")
  }
  return { driver: "s3", options: { bucket: match[1], namespace: prefix, region } }
}

function validateUuid(value: unknown, allowZero = true): void {
  if (typeof value !== "string" || !uuid.test(value) || (!allowZero && !/[1-9a-f]/.test(value))) {
    throw new ConfigError("Invalid configuration UUID")
  }
}

function enrollment(value: unknown): JsonObject {
  const data = fields(value, ["worker_id", "incarnation", "certificate_sha256", "endpoint", "server_name", "enrollment_revision"])
  for (const key of ["worker_id", "incarnation"]) validateUuid(data[key], false)
  const fingerprint = data.certificate_sha256
  if (typeof fingerprint !== "string" || !/^[0-9a-f]{64}(?![\s\S])/i.test(fingerprint)) {
    throw new ConfigError("Invalid certificate fingerprint")
  }
  integer(data.enrollment_revision, 1, maxInt64)
  if (/\s/.test(text(data.endpoint) + text(data.server_name))) throw new ConfigError("Invalid enrollment endpoint")
  return data
}

function workerLimits(worker: JsonObject): void {
  if (Object.hasOwn(worker, "resources")) {
    const bytes = ["memory_limit_bytes", "task_memory_limit_bytes", "control_memory_reserve_bytes"]
    const resources = fields(worker.resources, ["max_threads", ...bytes])
    integer(resources.max_threads, 1, 256)
    const [total, task, reserve] = bytes.map(key => BigInt(integer(resources[key], 1, maxInt64)))
    if (task + reserve > total) throw new ConfigError("Worker memory limits contradict each other")
  }
  if (Object.hasOwn(worker, "execution")) {
    const limits = ["max_workers_per_request", "max_pending_requests", "max_queue_bytes_per_replica",
      "max_retained_result_bytes_per_worker", "result_retention_ms"]
    const execution = fields(worker.execution, [...limits, "queue_replication", "queue_lifetime", "partition_size"])
    for (const key of limits) integer(execution[key], 1, maxInt64)
    if (execution.queue_replication !== "all_ready_workers" || execution.queue_lifetime !== "live_cluster") {
      throw new ConfigError("Unsupported queue policy")
    }
    text(execution.partition_size)
  }
}

function validateOltp(value: unknown): JsonObject {
  const config = fields(value, ["version", "engine", "bind", "database", "auth_file", "topology"], ["tls", "worker", "cluster_name"])
  integer(config.version, 1, 1)
  if (typeof config.database !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}(?![\s\S])/.test(config.database)) {
    throw new ConfigError("Invalid OLTP database selector")
  }
  if (Object.hasOwn(config, "cluster_name")) validateIdentifier(config.cluster_name)
  text(config.auth_file)
  if (config.tls != null) {
    for (const path of Object.values(fields(config.tls, ["cert_file", "key_file"], ["ca_file"]))) {
      if (path !== null) text(path)
    }
  }
  listen(config.bind, config.tls)

  const basic = fields(config.topology, ["mode"], ["data_dir", "cluster_id", "control_group_id", "enrollment", "worker_tls", "control_peers", "scoped_oltp"])
  if (basic.mode === "local") {
    text(fields(basic, ["mode", "data_dir"]).data_dir)
  } else if (basic.mode === "remote") {
    const topology = fields(basic, ["mode", "cluster_id", "control_group_id", "enrollment", "worker_tls", "control_peers"], ["scoped_oltp"])
    if ("scoped_oltp" in topology && typeof topology.scoped_oltp !== "boolean") {
      throw new ConfigError("scoped_oltp must be a boolean")
    }
    for (const key of ["cluster_id", "control_group_id"]) validateUuid(topology[key], false)
    for (const path of Object.values(fields(topology.worker_tls, ["cert_file", "key_file", "ca_file"]))) text(path)
    const api = fields(topology.enrollment, ["database", "enrollment"])
    if (api.database !== config.database) throw new ConfigError("API enrollment database differs")
    enrollment(api.enrollment)
    if (!Array.isArray(topology.control_peers) || topology.control_peers.length < 1 || topology.control_peers.length > 128) {
      throw new ConfigError("Invalid Control bootstrap peers")
    }
    const identities = topology.control_peers.map(peer => enrollment(peer).worker_id)
    if (new Set(identities).size !== identities.length) throw new ConfigError("Duplicate Control bootstrap peer")
  } else {
    throw new ConfigError("Unsupported OLTP topology")
  }
  const worker = fields(Object.hasOwn(config, "worker") ? config.worker : {},
    [], ["resources", "execution", "queue_size"])
  if (Object.hasOwn(worker, "queue_size")) integer(worker.queue_size, 1, 65_536)
  workerLimits(worker)
  return config
}

export function validate(value: unknown): JsonObject {
  if (isJsonObject(value) && value.engine === "oltp") return validateOltp(value)
  const config = fields(value, ["version", "cluster_id", "bind", "advertise_url", "api_urls", "storage", "auth_file", "tls", "worker"],
    ["mode", "lifecycle"])
  integer(config.version, 1, 2)
  if (config.version === 2 && config.mode !== "fixed" && config.mode !== "serverless") {
    throw new ConfigError("Version 2 requires mode fixed or serverless")
  }
  if (config.version === 1 && Object.hasOwn(config, "mode")) throw new ConfigError("Version 1 has no mode field")
  validateUuid(config.cluster_id)
  if (config.tls !== null) {
    for (const path of Object.values(fields(config.tls, ["cert_file", "key_file", "ca_file"]))) text(path)
  }
  listen(config.bind, config.tls)
  if (!Array.isArray(config.api_urls) || config.api_urls.length < 1 || config.api_urls.length > 64) {
    throw new ConfigError("Invalid API URL list")
  }
  for (const endpoint of [...config.api_urls, config.advertise_url]) {
    if ((origin(endpoint).protocol === "https:") !== Boolean(config.tls)) {
      throw new ConfigError("TLS and API URL schemes must agree")
    }
  }
  text(config.auth_file)
  const storage = fields(config.storage, ["driver", "options"])
  if (storage.driver === "s3") {
    const options = fields(storage.options, ["bucket", "namespace"], ["region"])
    if (typeof options.namespace !== "string") throw new ConfigError("Invalid storage prefix")
    s3Storage(`s3://${text(options.bucket)}/${options.namespace}`, options.region ?? "us-east-1")
  } else if (storage.driver === "http") {
    const options = fields(storage.options, ["base_url", "namespace", "token_file", "ca_file"])
    const base = text(options.base_url)
    origin(/^https?:\/\/[^/]+/.exec(base)?.[0])
    text(options.token_file)
  } else {
    throw new ConfigError("Unsupported storage driver")
  }
  const worker = fields(config.worker, ["listen", "operation_slots", "partition_slots", "queue_size", "partition_size"], ["execution", "resources"])
  listen(worker.listen, config.tls)
  for (const key of ["operation_slots", "partition_slots", "queue_size"]) integer(worker[key], 1, 65_536)
  text(worker.partition_size)
  workerLimits(worker)
  if (Object.hasOwn(config, "lifecycle")) {
    const policy = fields(config.lifecycle, [], ["heartbeat_ms", "expiry_ms", "readiness_ms", "drain_ms",
      "lease_ms", "renew_ms", "stabilization_ms", "cooldown_ms", "retry_initial_ms", "retry_max_ms", "max_attempts"])
    for (const value of Object.values(policy)) integer(value, 1, maxInt64)
  }
  return config
}

export function load(path: string): JsonObject {
  try {
    return validate(readJson(path))
  } catch {
    throw new ConfigError("Invalid API configuration; see nabedra database init --help")
  }
}

export function protectedWrite(path: string, value: unknown): void {
  writeFileSync(path, Buffer.concat([encodeJson(value), Buffer.from("\n")]), { flag: "wx", mode: 0o600 })
}

export function resolvePaths(config: JsonObject, source: string): JsonObject {
  const result = structuredClone(config)
  const base = dirname(resolve(source))
  const absolute = (record: JsonObject, key: string) => {
    if (typeof record[key] === "string") record[key] = resolve(base, record[key])
  }
  absolute(result, "auth_file")
  if (isJsonObject(result.tls)) for (const key of Object.keys(result.tls)) absolute(result.tls, key)
  if (result.engine === "oltp") {
    const topology = result.topology as JsonObject
    if (topology.mode === "local") absolute(topology, "data_dir")
    else for (const key of ["cert_file", "key_file", "ca_file"]) absolute(topology.worker_tls as JsonObject, key)
  } else {
    const storage = result.storage as JsonObject
    if (storage.driver === "http") for (const key of ["token_file", "ca_file"]) absolute(storage.options as JsonObject, key)
  }
  return result
}

export function example(mode: string, storage: JsonObject, authFile: string, threads = 4): JsonObject {
  const bind = "127.0.0.1:8000"
  const endpoint = "http://" + bind
  return {
    version: 2, mode, cluster_id: randomUUID(), bind, advertise_url: endpoint, api_urls: [endpoint],
    storage, auth_file: authFile, tls: null,
    worker: {
      listen: "127.0.0.1:0", operation_slots: 4, partition_slots: threads, queue_size: 64, partition_size: "64 MiB",
      resources: defaultResources(threads),
    },
  }
}

export function defaultResources(threads: number): JsonObject {
  return { max_threads: threads, memory_limit_bytes: 8 * 2 ** 30, task_memory_limit_bytes: 512 * 2 ** 20,
    control_memory_reserve_bytes: 256 * 2 ** 20 }
}

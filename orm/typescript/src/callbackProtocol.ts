import { createHash } from "node:crypto"
import { createConnection, type Socket } from "node:net"
import { encodeJson, floatToken, hasFields, isJsonObject, parseJson, type JsonObject } from "./transport.js"

export const callbackMaxBody = 1 << 20
const maxHeaders = 8 << 10
const maxInt64 = (1n << 63n) - 1n
const maxUint64 = (1n << 64n) - 1n
const uuidPattern = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?![\s\S])/
const identifier = /^[A-Za-z_][A-Za-z0-9_]{0,63}(?![\s\S])/
const headerToken = /^[!#$%&'*+.^_\x60|~0-9A-Za-z-]+(?![\s\S])/
const tokenPattern = /^[A-Za-z0-9_-]{43}(?![\s\S])/
const authorityUrl = /^http:\/\/127\.0\.0\.1:([1-9][0-9]{0,4})\/private\/v1\/authorize(?![\s\S])/
const resourceKeys = ["max_threads", "memory_limit_bytes", "task_memory_limit_bytes", "control_memory_reserve_bytes"]
const executionIntKeys = [
  "max_workers_per_request", "max_pending_requests", "max_queue_bytes_per_replica",
  "max_retained_result_bytes_per_worker", "result_retention_ms",
]
const authDenials = new Map([
  [400, "invalid_command"], [401, "unauthorized"], [409, "ownership_changed"],
  [413, "limit_exceeded"], [503, "ownership_unavailable"],
])

export class CallbackProtocolError extends Error {
  constructor(readonly code: string = "invalid_command", readonly status?: number) {
    super("Nabedra callback protocol failed.")
  }
}

export interface StartNodeRequest {
  version: 1
  cluster_id: string
  cluster_name: string
  request_id: string
  node_id: string
  bootstrap: JsonObject
}

type Action = "start" | "stop" | "inventory"

export interface CallbackCommand {
  raw: Buffer
  invocationId: string
  clusterId: string
  intentId: string | null
  action: Action
  payload: JsonObject
  identity: string
}

function requireCondition(condition: unknown, code?: string): asserts condition {
  if (!condition) throw new CallbackProtocolError(code)
}

function validUuid(value: unknown): value is string {
  return typeof value === "string" && uuidPattern.test(value)
}

function isInt(value: unknown, low: bigint, high: bigint): value is bigint {
  return typeof value === "bigint" && value >= low && value <= high
}

function hasText(value: unknown): value is string {
  return typeof value === "string" && value.length > 0
}

function validResource(value: unknown): value is string {
  return hasText(value) && value.isWellFormed() && Buffer.byteLength(value) <= 256 && !/\p{Cc}/u.test(value)
}

function validateTree(value: unknown, depth = 1): void {
  requireCondition(depth <= 32)
  if (isJsonObject(value)) {
    for (const [key, item] of Object.entries(value)) {
      requireCondition(key.isWellFormed())
      validateTree(item, depth + 1)
    }
  } else if (Array.isArray(value)) {
    for (const item of value) validateTree(item, depth + 1)
  } else if (typeof value === "string") {
    requireCondition(value.isWellFormed())
  }
}

function load(raw: Buffer): JsonObject {
  requireCondition(raw.length <= callbackMaxBody, "limit_exceeded")
  try {
    const value = parseJson(raw, { integers: "bigint" })
    requireCondition(isJsonObject(value))
    validateTree(value)
    return value
  } catch {
    throw new CallbackProtocolError()
  }
}

function validateBootstrap(value: unknown, payload: JsonObject): void {
  requireCondition(isJsonObject(value))
  const fields = [
    "version", "cluster_id", "node_id", "request_id", "api_urls", "listen", "advertise_url",
    "storage", "enrollment_token", "tls", "operation_slots", "partition_slots", "queue_size",
    "partition_size",
  ]
  for (const optional of ["resources", "execution"]) {
    if (Object.hasOwn(value, optional)) fields.push(optional)
  }
  requireCondition(hasFields(value, fields))
  if (Object.hasOwn(value, "resources")) {
    const r = value.resources
    requireCondition(hasFields(r, resourceKeys) && isInt(r.max_threads, 1n, 256n))
    const { memory_limit_bytes: limit, task_memory_limit_bytes: task, control_memory_reserve_bytes: reserve } = r
    requireCondition(isInt(limit, 1n, maxInt64) && isInt(task, 1n, maxInt64) && isInt(reserve, 1n, maxInt64)
      && reserve < limit && task <= limit - reserve)
  }
  if (Object.hasOwn(value, "execution")) {
    const e = value.execution
    requireCondition(hasFields(e, [...executionIntKeys, "queue_replication", "queue_lifetime", "partition_size"])
      && executionIntKeys.every(key => isInt(e[key], 1n, maxInt64))
      && e.queue_replication === "all_ready_workers" && e.queue_lifetime === "live_cluster"
      && hasText(e.partition_size))
  }
  requireCondition(value.version === 1n && value.advertise_url === null)
  for (const key of ["cluster_id", "node_id", "request_id"]) requireCondition(value[key] === payload[key])
  requireCondition(Array.isArray(value.api_urls) && value.api_urls.length >= 1 && value.api_urls.length <= 16
    && value.api_urls.every(hasText))
  for (const key of ["listen", "partition_size", "enrollment_token"]) requireCondition(hasText(value[key]))
  requireCondition(isInt(value.operation_slots, 1n, 256n) && isInt(value.partition_slots, 1n, 256n)
    && isInt(value.queue_size, 0n, 4096n))
  const { storage, tls } = value
  requireCondition(hasFields(storage, "driver", "options") && hasText(storage.driver) && isJsonObject(storage.options))
  if (tls !== null) {
    requireCondition(hasFields(tls, "cert_file", "key_file", "ca_file") && Object.values(tls).every(hasText))
  }
}

/** Walk a decoded tree before producing either the identity or the provider copy. */
function mapTree(value: unknown, convert: (value: unknown) => unknown, sorted: boolean): unknown {
  if (Array.isArray(value)) return value.map(item => mapTree(item, convert, sorted))
  if (isJsonObject(value)) {
    const keys = Object.keys(value)
    if (sorted) keys.sort()
    return Object.fromEntries(keys.map(key => [key, mapTree(value[key], convert, sorted)]))
  }
  return convert(value)
}

export function decodeCallback(raw: Buffer): CallbackCommand {
  const value = load(raw)
  requireCondition(hasFields(value, "version", "invocation_id", "cluster_id", "owner_id", "epoch", "intent_id",
    "action", "target", "payload"))
  const { invocation_id: invocationId, cluster_id: clusterId, intent_id: intentId, action, target, payload } = value
  requireCondition(value.version === 1n && isInt(value.epoch, 1n, maxUint64)
    && validUuid(invocationId) && validUuid(clusterId) && validUuid(value.owner_id))
  requireCondition(action === "start" || action === "stop" || action === "inventory")
  if (action === "inventory") {
    requireCondition(intentId === null && target === null && hasFields(payload))
  } else {
    requireCondition(validUuid(intentId) && hasFields(target, "node_id", "request_id", "incarnation_id")
      && validUuid(target.node_id) && validUuid(target.request_id)
      && (target.incarnation_id === null || validUuid(target.incarnation_id)))
    if (action === "start") {
      requireCondition(intentId === target.request_id && target.incarnation_id === null
        && hasFields(payload, "version", "cluster_id", "cluster_name", "request_id", "node_id", "bootstrap")
        && payload.version === 1n && payload.cluster_id === clusterId && payload.request_id === target.request_id
        && payload.node_id === target.node_id && typeof payload.cluster_name === "string" && identifier.test(payload.cluster_name))
      validateBootstrap(payload.bootstrap, payload)
    } else {
      requireCondition(intentId !== target.request_id && hasFields(payload, "resource_id") && validResource(payload.resource_id))
    }
  }
  const identity = encodeJson(mapTree([target, payload], item => typeof item === "number" ? floatToken(item) : item,
    true)).toString("utf8")
  const providerPayload = mapTree(payload, item =>
    typeof item === "bigint" && Number.isSafeInteger(Number(item)) ? Number(item) : item, false)
  return { raw, invocationId, clusterId, intentId, action, payload: providerPayload as JsonObject, identity }
}

export function callbackSuccess(command: CallbackCommand, value: unknown): Buffer {
  const code = "invalid_callback_result"
  let result: unknown = null
  if (command.action === "start") {
    requireCondition(validResource(value), code)
    result = value
  } else if (command.action === "stop") {
    requireCondition(value === undefined || value === null, code)
  } else {
    requireCondition(Array.isArray(value), code)
    requireCondition(value.length <= 4096, "limit_exceeded")
    const resources = new Set<string>()
    const nodes = new Set<string>()
    const requests = new Set<string>()
    const records: JsonObject[] = []
    for (const item of value) {
      requireCondition(hasFields(item, "resource_id", "cluster_id", "node_id", "request_id", "state"), code)
      const descriptors = Object.getOwnPropertyDescriptors(item)
      requireCondition(Object.values(descriptors).every(descriptor => Object.hasOwn(descriptor, "value")), code)
      const resourceId: unknown = descriptors.resource_id.value
      const clusterId: unknown = descriptors.cluster_id.value
      const nodeId: unknown = descriptors.node_id.value
      const requestId: unknown = descriptors.request_id.value
      const state: unknown = descriptors.state.value
      requireCondition(validResource(resourceId) && clusterId === command.clusterId
        && validUuid(nodeId) && validUuid(requestId)
        && (state === "starting" || state === "running" || state === "stopping" || state === "terminated"), code)
      requireCondition(!resources.has(resourceId), code)
      resources.add(resourceId)
      if (state !== "terminated") {
        requireCondition(!nodes.has(nodeId) && !requests.has(requestId), code)
        nodes.add(nodeId)
        requests.add(requestId)
      }
      records.push({ resource_id: resourceId, cluster_id: clusterId, node_id: nodeId, request_id: requestId, state })
    }
    result = records
  }
  const bytes = encodeJson({ version: 1, invocation_id: command.invocationId, result })
  requireCondition(bytes.length <= callbackMaxBody, "limit_exceeded")
  return bytes
}

export function parseHeaders(head: Buffer): [string, ReadonlyMap<string, string>] {
  const [start, ...lines] = head.toString("latin1").split("\r\n")
  const headers = new Map<string, string>()
  for (const line of lines) {
    const colon = line.indexOf(":")
    requireCondition(colon > 0)
    const field = line.slice(0, colon).toLowerCase()
    const value = line.slice(colon + 1)
    requireCondition(headerToken.test(field) && !headers.has(field) && !/[\x00-\x08\x0a-\x1f\x7f]/.test(value))
    headers.set(field, value.replace(/^[ \t]+|[ \t]+$/g, ""))
  }
  return [start, headers]
}

export function contentLength(headers: ReadonlyMap<string, string>): number {
  const value = headers.get("content-length")
  requireCondition(value !== undefined && /^(?:0|[1-9][0-9]{0,6})(?![\s\S])/.test(value))
  const length = Number(value)
  requireCondition(length <= callbackMaxBody, "limit_exceeded")
  return length
}

/** Read one bounded packet. An authority reply additionally requires EOF and rejects extra bytes. */
export function readPacket(socket: Socket, deadline: number, length: (head: Buffer) => number,
  requireClose: boolean): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const headers = Buffer.allocUnsafe(maxHeaders)
    let scanned = 0
    let body: Buffer | undefined
    let received = 0
    let done = false
    const timer = setTimeout(() => finish(new CallbackProtocolError()), Math.max(0, deadline - performance.now()))

    function finish(error?: unknown): void {
      if (done) return
      done = true
      clearTimeout(timer)
      socket.pause()
      socket.off("data", read)
      socket.off("end", ended)
      socket.off("close", lost)
      socket.off("error", lost)
      if (error === undefined) resolve(body!)
      else reject(error)
    }

    function lost(): void { finish(new CallbackProtocolError()) }
    function ended(): void {
      if (body && received === body.length && performance.now() < deadline) finish()
      else lost()
    }

    function read(chunk: Buffer): void {
      try {
        requireCondition(performance.now() < deadline)
        let offset = 0
        if (body === undefined) {
          for (; offset < chunk.length; offset++) {
            requireCondition(scanned < maxHeaders, "limit_exceeded")
            headers[scanned++] = chunk[offset]
            if (scanned >= 4 && headers.readUInt32BE(scanned - 4) === 0x0d0a0d0a) {
              body = Buffer.allocUnsafe(length(headers.subarray(0, scanned - 4)))
              ++offset
              break
            }
          }
          if (body === undefined) {
            requireCondition(scanned < maxHeaders, "limit_exceeded")
            return
          }
        }
        if (requireClose) requireCondition(chunk.length - offset <= body.length - received)
        received += chunk.copy(body, received, offset, offset + Math.min(chunk.length - offset, body.length - received))
        if (received === body.length && !requireClose) finish()
      } catch (error) {
        finish(error)
      }
    }

    socket.on("data", read)
    socket.once("end", ended)
    socket.once("close", lost)
    socket.once("error", lost)
    if (socket.destroyed || socket.readableEnded) lost()
    else socket.resume()
  })
}

export async function authorizeCallback(url: string, token: string, command: CallbackCommand, deadline: number): Promise<void> {
  let denial: CallbackProtocolError | undefined
  let socket: Socket | undefined
  try {
    const match = authorityUrl.exec(url)
    requireCondition(match && Number(match[1]) <= 65535 && tokenPattern.test(token)
      && Buffer.from(token, "base64url").toString("base64url") === token)
    const port = Number(match[1])
    const end = Math.min(deadline, performance.now() + 2_000)
    requireCondition(performance.now() < end)
    socket = createConnection({ port, host: "127.0.0.1" })
    socket.on("error", () => {})
    let status = 0
    const body = readPacket(socket, end, head => {
      const [start, headers] = parseHeaders(head)
      const statusLine = /^HTTP\/1\.[01] ([0-9]{3}) [\x20-\x7e]*(?![\s\S])/.exec(start)
      requireCondition(statusLine && headers.get("content-type")?.toLowerCase() === "application/json"
        && !headers.has("transfer-encoding") && !headers.has("content-encoding"))
      status = Number(statusLine[1])
      const length = contentLength(headers)
      requireCondition(length > 0)
      return length
    }, true)
    const header = "POST /private/v1/authorize HTTP/1.1\r\nHost: 127.0.0.1:" + port
      + "\r\nAuthorization: Bearer " + token + "\r\nContent-Type: application/json\r\nContent-Length: "
      + command.raw.length + "\r\nConnection: close\r\n\r\n"
    socket.write(header, "ascii")
    socket.write(command.raw)
    const response = load(await body)
    requireCondition(response.version === 1n)
    if (status === 200) {
      const digest = createHash("sha256").update(command.raw).digest("hex")
      requireCondition(hasFields(response, "version", "invocation_id", "command_sha256", "authorized")
        && response.invocation_id === command.invocationId && response.command_sha256 === digest
        && response.authorized === true)
    } else {
      const code = authDenials.get(status)
      requireCondition(code && hasFields(response, "version", "invocation_id", "error")
        && (response.invocation_id === command.invocationId
          || (response.invocation_id === null && (status === 400 || status === 401 || status === 413)))
        && hasFields(response.error, "code") && response.error.code === code)
      denial = new CallbackProtocolError(code, status)
    }
    requireCondition(performance.now() < end)
  } catch {
    throw new CallbackProtocolError("authorization_denied")
  } finally {
    socket?.destroy()
  }
  if (denial) throw denial
}

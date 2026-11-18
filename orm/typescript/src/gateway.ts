import { Agent as HttpAgent } from "node:http"
import { Agent as HttpsAgent } from "node:https"
import { isIP } from "node:net"
import { inspect, types } from "node:util"
import { AwsCredentials, type AwsCredentialsProvider } from "./aws.js"
import {
  fields,
  freeze,
  identity,
  load,
  maxBody,
  mutations,
  operationPayload,
  requireProtocol,
  type GatewayRecord,
} from "./gatewayProtocol.js"
import { encodeJson, isJsonObject, Transport, validDuration, type HttpResponse } from "./transport.js"

export { AwsCredentials } from "./aws.js"
export type { GatewayRecord, GatewayValue } from "./gatewayProtocol.js"

export type HeaderPair = readonly [name: string, value: string]

export class HTTPResponse {
  readonly status: number
  readonly headers: readonly HeaderPair[]
  readonly #body: Buffer

  constructor(status: number, headers: readonly HeaderPair[], body: Uint8Array) {
    this.status = status
    this.headers = copyHeaders(headers)
    this.#body = Buffer.from(body)
    Object.freeze(this)
  }

  get body(): Uint8Array {
    return Buffer.from(this.#body)
  }
}

export interface S3Options {
  region: string
  prefix?: string
  credentials: AwsCredentials | AwsCredentialsProvider
}

export class S3 {
  readonly bucket: string
  readonly region: string
  readonly prefix: string
  readonly credentials: AwsCredentials | AwsCredentialsProvider

  constructor(bucket: string, { region, prefix = "nabedra/", credentials }: S3Options) {
    requireProtocol(typeof bucket === "string" && /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](?![\s\S])/.test(bucket)
      && !bucket.includes("..") && !/^(xn--|sthree-|amzn-s3-demo-)/.test(bucket)
      && !/(-s3alias|--ol-s3|\.mrap|--x-s3|--table-s3)$/.test(bucket) && !isIP(bucket))
    requireProtocol(typeof region === "string" && region.length <= 64 &&
      /^(?:(?:af|ap|ca|cn|eu|il|me|mx|sa|us)-(?:east|west|north|south|central|northeast|northwest|southeast|southwest)|us-gov-(?:east|west))-[1-9]\d*(?![\s\S])/.test(region))
    requireProtocol(typeof prefix === "string" && prefix.isWellFormed() && Buffer.byteLength(prefix) <= 1024)
    const normalized = prefix.replace(/\/+$/, "")
    requireProtocol(normalized && !/[\\:%?#\p{Cc}]/u.test(normalized)
      && normalized.split("/").every(part => part && part !== "." && part !== "..")
      && Buffer.byteLength(normalized + "/") <= 1024)
    requireProtocol(credentials instanceof AwsCredentials || typeof credentials === "function")
    this.bucket = bucket
    this.region = region
    this.prefix = normalized + "/"
    this.credentials = credentials
    Object.freeze(this)
  }

  [inspect.custom](): string {
    return "S3(<redacted>)"
  }
}

export class Operation {
  readonly requestId: string
  readonly name: string
  readonly payload: GatewayRecord

  constructor(requestId: string, name: string, payload: GatewayRecord) {
    this.requestId = requestId
    this.name = name
    this.payload = freeze(payload) as GatewayRecord
    Object.freeze(this)
  }

  [inspect.custom](): string {
    return "Operation(<redacted>)"
  }
}

export class Receipt {
  constructor(readonly runId: string, readonly requestId: string) {
    Object.freeze(this)
  }
}

function copyHeaders(pairs: readonly HeaderPair[]): readonly HeaderPair[] {
  return Object.freeze(pairs.map(([key, value]) => Object.freeze([key, value] as HeaderPair)))
}

export class Headers implements Iterable<string> {
  readonly #pairs: readonly HeaderPair[]

  constructor(pairs: Iterable<HeaderPair>) {
    this.#pairs = copyHeaders([...pairs])
    Object.freeze(this)
  }

  get(key: string): string | undefined
  get<T>(key: string, defaultValue: T): string | T
  get<T>(key: string, defaultValue?: T): string | T | undefined {
    const values = this.getAll(key)
    return values.length === 0 ? defaultValue : values.join(", ")
  }

  getAll(key: string): readonly string[] {
    const name = key.toLowerCase()
    return Object.freeze(this.#pairs.filter(([item]) => item.toLowerCase() === name).map(([, value]) => value))
  }

  get pairs(): readonly HeaderPair[] {
    return this.#pairs
  }

  [Symbol.iterator](): IterableIterator<string> {
    return new Set(this.#pairs.map(([name]) => name.toLowerCase())).values()
  }

  get size(): number {
    return [...this].length
  }

  [inspect.custom](): string {
    return "Headers(<redacted>)"
  }
}

class Rejected extends Error {
  constructor(readonly code: string, readonly publicMessage: string) {
    super("Gateway request rejected")
  }
}

export interface GatewayContextValues {
  method: string
  path: string
  headers: Headers
  rawBody: Uint8Array
  database: string | null
  operation: Operation | null
  receipt: Receipt | null
}

export class GatewayContext {
  readonly #values: Readonly<GatewayContextValues>
  principal: string | null = null
  storage: S3 | null = null

  constructor(values: GatewayContextValues) {
    this.#values = Object.freeze({ ...values, rawBody: Buffer.from(values.rawBody) })
    Object.seal(this)
  }

  get method(): string { return this.#values.method }
  get path(): string { return this.#values.path }
  get headers(): Headers { return this.#values.headers }
  get rawBody(): Uint8Array { return Buffer.from(this.#values.rawBody) }
  get database(): string | null { return this.#values.database }
  get operation(): Operation | null { return this.#values.operation }
  get receipt(): Receipt | null { return this.#values.receipt }

  reject(code: string, message: string): never {
    const approved = code === "unauthorized" || code === "validation_error" || code === "not_found" || code === "overloaded"
    throw new Rejected(approved ? code : "unauthorized", "Request rejected by application middleware")
  }

  [inspect.custom](): string {
    return "GatewayContext(<redacted>)"
  }
}

const statusCodes = new Map([
  ["validation_error", 400], ["unauthorized", 401], ["not_found", 404], ["limit_exceeded", 413],
  ["overloaded", 429], ["storage_unavailable", 503], ["outcome_unknown", 503], ["conflict", 409],
  ["unsupported_operation", 400], ["internal_error", 500],
])
const receiptStates = new Set(["queued", "running", "committing", "reconciling", "succeeded", "failed", "abandoned"])
const headerName = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+(?![\s\S])/
const maxTimer = 2_147_483_647
const now = () => performance.now() / 1_000

export interface GatewayOptions {
  clusterUrl: string
  token: string
  mountPath?: string
  maxConcurrency?: number
  timeout?: number
  middlewareTimeout?: number
}

export interface GatewayRequest {
  method: string
  path: string
  headers: readonly HeaderPair[]
  body: Uint8Array
}

export type GatewayMiddleware = (context: GatewayContext) => void | PromiseLike<void>

function rejectedResponse(requestId: string | null, code: string, message: string, unknown = false): HTTPResponse {
  return new HTTPResponse(statusCodes.get(code) ?? 503,
    [["content-type", "application/json"], ["cache-control", "no-store"]],
    encodeJson({ request_id: requestId, error: { code, message, outcome: unknown ? "unknown" : "not_applied" } }))
}

function selector(value: string): string {
  requireProtocol(/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}(?![\s\S])/.test(value))
  return value
}

function checkError(value: unknown): asserts value is { code: string; message: string; outcome: "not_applied" | "unknown" } {
  fields(value, ["code", "message", "outcome"])
  requireProtocol(typeof value.code === "string" && value.code && typeof value.message === "string"
    && (value.outcome === "not_applied" || value.outcome === "unknown"))
  requireProtocol(value.code !== "outcome_unknown" || value.outcome === "unknown")
}

function storageHeader(storage: S3, credentials: unknown): string {
  requireProtocol(credentials instanceof AwsCredentials)
  const expiresAt = credentials.expiresAt
  requireProtocol(expiresAt === null || expiresAt.getTime() > Date.now())
  const raw = encodeJson({
    version: 1,
    bucket: storage.bucket,
    region: storage.region,
    prefix: storage.prefix,
    credentials: {
      access_key_id: credentials.accessKeyId,
      secret_access_key: credentials.secretAccessKey,
      session_token: credentials.sessionToken,
      expires_at: expiresAt?.toISOString().replace(/\.\d{3}Z$/, "Z") ?? null,
    },
  })
  requireProtocol(raw.length <= 16 * 1024)
  const header = raw.toString("base64url")
  requireProtocol(header.length <= 24 * 1024)
  return header
}

function contentType(value: string): string {
  return value.split(";", 1)[0].trim().toLowerCase()
}

function deadlineExceeded(): Error {
  return new Error("Middleware deadline exceeded")
}

function thenable(value: unknown): value is PromiseLike<unknown> {
  return value !== null && (typeof value === "object" || typeof value === "function")
    && typeof (value as PromiseLike<unknown>).then === "function"
}

function withinDeadline<T>(promise: Promise<T>, deadline: number): Promise<T> {
  return new Promise((resolve, reject) => {
    let timer: NodeJS.Timeout | undefined
    const check = () => {
      const remaining = deadline - now()
      if (remaining <= 0) {
        reject(deadlineExceeded())
      } else {
        timer = setTimeout(check, Math.min(maxTimer, Math.ceil(remaining * 1_000)))
      }
    }
    void promise.then(value => {
      clearTimeout(timer)
      if (now() >= deadline) reject(deadlineExceeded())
      else resolve(value)
    }, error => {
      clearTimeout(timer)
      reject(error)
    })
    check()
  })
}

export class Gateway {
  readonly #transport: Transport
  readonly #agent: HttpAgent | HttpsAgent
  readonly #mount: string
  readonly #timeout: number
  readonly #middlewareTimeout: number
  readonly #maxConcurrency: number
  readonly #callbacks: GatewayMiddleware[] = []
  #active = 0
  #started = false
  #closed = false

  constructor({
    clusterUrl,
    token,
    mountPath = "",
    maxConcurrency = 16,
    timeout = 30,
    middlewareTimeout = 10,
  }: GatewayOptions) {
    try {
      requireProtocol(typeof clusterUrl === "string" && !/[^!-~]|\\/.test(clusterUrl))
      const origin = /^(https?):\/\/(\[[^\]]+\]|[^:/@?#]+)(?::[0-9]+)?\/?(?![\s\S])/i.exec(clusterUrl)
      requireProtocol(origin !== null)
      const rawHost = origin[2].replace(/^\[|\]$/g, "")
      const url = new URL(clusterUrl)
      if (url.protocol === "http:") {
        const family = isIP(rawHost)
        requireProtocol((family === 4 && rawHost.startsWith("127.")) || (family === 6 && url.hostname === "[::1]"))
      }
      requireProtocol(typeof token === "string" && /^[A-Za-z0-9._~+/\-]+=*(?![\s\S])/.test(token))
      requireProtocol(typeof mountPath === "string" && (mountPath === "" || mountPath === "/"
        || /^(?:\/[A-Za-z0-9_-]+)+(?![\s\S])/.test(mountPath)))
      requireProtocol(Number.isInteger(maxConcurrency) && maxConcurrency > 0 && maxConcurrency <= 1024)
      requireProtocol(validDuration(timeout) && validDuration(middlewareTimeout))
      this.#transport = new Transport(clusterUrl, token, { timeout })
      const options = { keepAlive: true, maxSockets: maxConcurrency, maxFreeSockets: maxConcurrency }
      this.#agent = url.protocol === "https:" ? new HttpsAgent({ ...options, rejectUnauthorized: true }) : new HttpAgent(options)
    } catch {
      throw new TypeError("Invalid gateway configuration")
    }
    this.#mount = mountPath.replace(/\/$/, "")
    this.#timeout = timeout
    this.#middlewareTimeout = middlewareTimeout
    this.#maxConcurrency = maxConcurrency
  }

  [inspect.custom](): string {
    return "Gateway(<redacted>)"
  }

  middleware<T extends GatewayMiddleware>(callback: T): T {
    if (this.#started || this.#closed || typeof callback !== "function") {
      throw new TypeError("Register middleware before serving requests")
    }
    this.#callbacks.push(callback)
    return callback
  }

  #parse({ method, path: target, headers, body }: GatewayRequest,
    correlation: { id: string | null }): { context: GatewayContext; raw: Buffer } {
    requireProtocol(typeof method === "string" && typeof target === "string" && body instanceof Uint8Array
      && Array.isArray(headers))
    if (body.byteLength > maxBody) throw new Rejected("limit_exceeded", "Request body exceeds 8 MiB")
    requireProtocol(target.length <= 1024 && target.startsWith(this.#mount + "/") && !/[?#%\\]/.test(target))
    const path = target.slice(this.#mount.length)
    let size = 0
    const names = new Map<string, number>()
    for (const pair of headers) {
      requireProtocol(Array.isArray(pair) && pair.length === 2)
      const [key, value] = pair
      requireProtocol(typeof key === "string" && typeof value === "string" && headerName.test(key)
        && value.isWellFormed() && !/[\x00-\x08\x0a-\x1f\x7f]/.test(value))
      size += key.length + Buffer.byteLength(value) + 4
      if (size > 32 * 1024) throw new Rejected("limit_exceeded", "Request headers exceed 32 KiB")
      const lower = key.toLowerCase()
      names.set(lower, (names.get(lower) ?? 0) + 1)
      if (lower.startsWith("x-nabedra-") && lower !== "x-nabedra-database") {
        throw new Rejected("unauthorized", "Untrusted internal header")
      }
    }
    requireProtocol(["authorization", "x-nabedra-database", "content-type", "content-length", "transfer-encoding"]
      .every(key => (names.get(key) ?? 0) <= 1))
    requireProtocol(!names.has("transfer-encoding"))
    const input = new Headers(headers)
    const length = input.get("content-length")
    requireProtocol(length === undefined || (/^[0-9]+(?![\s\S])/.test(length) && Number(length) === body.byteLength))
    const selected = input.get("x-nabedra-database")
    const database = selected === undefined ? null : selector(selected)
    const raw = Buffer.from(body)
    let operation: Operation | null = null
    let receipt: Receipt | null = null
    if (method === "POST" && (path === "/v1/operations" || path === "/v1/requests")) {
      requireProtocol(contentType(input.get("content-type", "")) === "application/json")
      const envelope = load(raw)
      fields(envelope, ["request_id", "operation", "payload"])
      correlation.id = identity(envelope.request_id)
      requireProtocol(typeof envelope.operation === "string")
      operationPayload(envelope.operation, envelope.payload)
      operation = new Operation(correlation.id, envelope.operation, envelope.payload as GatewayRecord)
    } else if (method === "GET" && path.startsWith("/v1/requests/")) {
      const parts = path.split("/")
      requireProtocol(parts.length === 5 && raw.length === 0)
      receipt = new Receipt(identity(parts[3]), identity(parts[4]))
    } else {
      throw new TypeError("Unsupported route")
    }
    return { raw, context: new GatewayContext({
      method, path, headers: input, rawBody: raw, database,
      operation, receipt,
    }) }
  }

  #response(context: GatewayContext, { status, headers, body }: HttpResponse): HTTPResponse {
    const type = headers["content-type"]
    requireProtocol(typeof type === "string" && contentType(type) === "application/json")
    requireProtocol((headers["content-encoding"] ?? "identity") === "identity")
    const length = headers["content-length"]
    requireProtocol(length === undefined || (/^[0-9]+(?![\s\S])/.test(length) && Number(length) === body.length))
    const value = load(body)
    const id = (context.operation ?? context.receipt)!.requestId
    const failed = status >= 400 && status <= 599
    requireProtocol(isJsonObject(value) && (value.request_id === id || (failed && value.request_id === null)))
    const safe: HeaderPair[] = [["content-type", type], ["cache-control", "no-store"]]
    if (failed) {
      fields(value, ["request_id", "error"])
      checkError(value.error)
      requireProtocol(status === (statusCodes.get(value.error.code) ?? 500))
    } else if (status === 202) {
      fields(value, ["request_id", "run_id", "state"])
      const runId = identity(value.run_id)
      requireProtocol(context.path === "/v1/requests" && value.state === "queued")
      safe.push(["location", `${this.#mount}/v1/requests/${runId}/${id}`])
    } else if (status === 200 && context.receipt) {
      fields(value, ["request_id", "run_id", "state", "result", "error"])
      const { state, error, result } = value
      requireProtocol(value.run_id === context.receipt.runId && typeof state === "string" && receiptStates.has(state))
      if (error !== null) checkError(error)
      if (state === "failed") requireProtocol(error !== null)
      else requireProtocol(error === null && (state === "succeeded" || result === null))
    } else {
      requireProtocol(status === 200 && context.path === "/v1/operations")
      fields(value, ["request_id", "data"])
    }
    return new HTTPResponse(status, safe, body)
  }

  #failure(context: GatewayContext | undefined, sent: boolean, error: unknown, correlationId: string | null): HTTPResponse {
    const id = context?.operation?.requestId ?? context?.receipt?.requestId ?? correlationId
    if (sent && context?.operation && mutations.has(context.operation.name)) {
      return rejectedResponse(id, "outcome_unknown", "Compute response unavailable; do not resubmit", true)
    }
    if (error instanceof Rejected) return rejectedResponse(id, error.code, error.publicMessage)
    return sent
      ? rejectedResponse(id, "storage_unavailable", "Compute response unavailable")
      : rejectedResponse(id, "validation_error", "Gateway validation or middleware failed")
  }

  handle(request: GatewayRequest): Promise<HTTPResponse> {
    return this.#handle(request, false)
  }

  handleAsync(request: GatewayRequest): Promise<HTTPResponse> {
    return this.#handle(request, true)
  }

  async #handle(request: GatewayRequest, allowAsync: boolean): Promise<HTTPResponse> {
    let context: GatewayContext | undefined
    let sent = false
    let entered = false
    let pending: Promise<unknown> | undefined
    const correlation = { id: null as string | null }
    try {
      this.#started = true
      if (this.#closed) throw new Rejected("storage_unavailable", "Gateway is closed")
      if (this.#active >= this.#maxConcurrency) throw new Rejected("overloaded", "Gateway capacity exhausted")
      this.#active++
      entered = true
      const parsed = this.#parse(request, correlation)
      context = parsed.context
      if (!allowAsync && this.#callbacks.some(callback => types.isAsyncFunction(callback))) {
        throw new TypeError("Async middleware requires handleAsync")
      }
      const deadline = now() + this.#middlewareTimeout
      for (const callback of this.#callbacks) {
        if (now() >= deadline) throw deadlineExceeded()
        const result = callback(context)
        if (thenable(result)) {
          pending = Promise.resolve(result)
          if (!allowAsync) throw new TypeError("Async middleware requires handleAsync")
          await withinDeadline(pending, deadline)
          pending = undefined
        }
      }
      if (now() >= deadline) throw deadlineExceeded()
      if (!(context.storage instanceof S3) || typeof context.principal !== "string" || !context.principal
        || [...context.principal].length > 1024) {
        throw new Rejected("unauthorized", "Middleware must authorize identity and storage")
      }
      const storage = context.storage
      let header: string
      try {
        const provider = storage.credentials
        const credentials = typeof provider === "function" ? provider() : provider
        if (thenable(credentials)) {
          pending = Promise.resolve(credentials)
          const value = await withinDeadline(pending, deadline)
          pending = undefined
          header = storageHeader(storage, value)
        } else {
          if (now() >= deadline) throw deadlineExceeded()
          header = storageHeader(storage, credentials)
        }
      } catch {
        if (now() >= deadline) throw deadlineExceeded()
        throw new Rejected("unauthorized", "Storage credentials unavailable")
      }
      requireProtocol(!this.#closed)
      sent = true
      const response = await this.#transport.exchange(context.method, context.path, {
        body: parsed.raw,
        headers: {
          "X-Nabedra-Storage": header,
          "Content-Type": "application/json",
          Accept: "application/json",
          "Accept-Encoding": "identity",
        },
        timeout: this.#timeout,
        maxBytes: maxBody,
        agent: this.#agent,
      })
      return this.#response(context, response)
    } catch (error) {
      return this.#failure(context, sent, error, correlation.id)
    } finally {
      if (entered) {
        const release = () => { this.#active-- }
        if (pending) void pending.then(release, release)
        else release()
      }
    }
  }

  close(): void {
    this.#closed = true
    this.#agent.destroy()
  }

  async aclose(): Promise<void> {
    this.close()
  }

  [Symbol.dispose](): void {
    this.close()
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.aclose()
  }
}

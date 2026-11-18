import { randomUUID, X509Certificate } from "node:crypto"
import { readFileSync } from "node:fs"
import * as http from "node:http"
import * as https from "node:https"
import * as tls from "node:tls"
import {
  AuthenticationError,
  ConflictError,
  LimitExceededError,
  NabedraError,
  NotFoundError,
  OverloadedError,
  ProtocolError,
  StorageError,
  TransportError,
  UnknownOutcomeError,
  ValidationError,
} from "./errors.js"

export type JsonObject = Record<string, unknown>

export interface HttpResponse {
  status: number
  headers: http.IncomingHttpHeaders
  rawHeaders: readonly string[]
  body: Buffer
}

export interface ReceiptError {
  code: string
  message: string
  outcome: "not_applied" | "unknown"
}

export type ReceiptState =
  | "queued"
  | "running"
  | "committing"
  | "reconciling"
  | "succeeded"
  | "failed"
  | "abandoned"

interface ReceiptData {
  requestId: string
  runId: string
  state: ReceiptState
  result: unknown
  error: ReceiptError | null
}

interface ConnectionOptions {
  timeout?: number
  caFile?: string | null
  database?: string | null
}

interface ExchangeOptions {
  body?: Uint8Array
  headers?: Record<string, string>
  authorization?: string
  timeout?: number
  maxBytes?: number
  deadline?: number
  agent?: http.Agent
  readResponse?: (response: http.IncomingMessage) => Promise<Buffer>
}

const maxBody = 8 * 1024 * 1024
const maxTimerMs = 2_147_483_647
const mutations = new Set([
  "table.create", "table.drop", "table.insert", "table.insert_many", "table.update",
  "table.upsert", "table.delete",
])
const reads = new Set([
  "table.describe", "table.list", "table.get", "query.collect", "query.count", "query.explain",
])
const strictIntegers = new Set(["table.get", "query.collect", "query.count", "table.insert_many"])

export const errorMappings: ReadonlyMap<string, { status: number; type: typeof NabedraError }> = new Map([
  ["validation_error", { status: 400, type: ValidationError }],
  ["unsupported_operation", { status: 400, type: NabedraError }],
  ["unauthorized", { status: 401, type: AuthenticationError }],
  ["not_found", { status: 404, type: NotFoundError }],
  ["conflict", { status: 409, type: ConflictError }],
  ["limit_exceeded", { status: 413, type: LimitExceededError }],
  ["overloaded", { status: 429, type: OverloadedError }],
  ["storage_unavailable", { status: 503, type: StorageError }],
  ["outcome_unknown", { status: 503, type: UnknownOutcomeError }],
])
const receiptStates: ReadonlySet<string> = new Set<ReceiptState>([
  "queued", "running", "committing", "reconciling", "succeeded", "failed", "abandoned",
])
const uuidPattern = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/
const loneSurrogate = /[\uD800-\uDFFF]/u
const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true })
const sentFailures = new WeakSet<NabedraError>()
const rawJson = JSON as typeof JSON & {
  rawJSON(text: string): object
  isRawJSON(value: unknown): boolean
}

export function isJsonObject(value: unknown): value is JsonObject {
  if (typeof value !== "object" || value === null || rawJson.isRawJSON(value)) return false
  const prototype: unknown = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

export function hasFields(value: unknown, fields: readonly string[]): value is JsonObject
export function hasFields(value: unknown, ...fields: string[]): value is JsonObject
export function hasFields(value: unknown, ...args: (string | readonly string[])[]): value is JsonObject {
  const fields = typeof args[0] === "string" || args[0] === undefined
    ? args as readonly string[]
    : args[0]
  return isJsonObject(value)
    && Object.keys(value).length === fields.length
    && fields.every((field) => Object.hasOwn(value, field))
}

/** Parse JSON without discarding duplicate fields or rounding large integers. */
export function parseJson(
  raw: Uint8Array | string,
  options: { integers?: "bigint"; numberToken?: (token: string) => unknown } = {},
): unknown {
  const source = typeof raw === "string" ? raw : utf8.decode(raw)
  const whitespace = /[ \t\r\n]*/y
  const number = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y
  const stringContents = /[^"\\\x00-\x1f]*/y
  const { integers, numberToken } = options
  let position = 0

  function fail(): never {
    throw new SyntaxError("Invalid JSON")
  }

  function skipWhitespace(): void {
    whitespace.lastIndex = position
    whitespace.exec(source)
    position = whitespace.lastIndex
  }

  function string(): string {
    if (source[position] !== '"') return fail()
    const start = position
    position++
    let escaped = false
    for (;;) {
      stringContents.lastIndex = position
      stringContents.exec(source)
      position = stringContents.lastIndex
      const next = source[position++]
      if (next === '"') {
        return escaped
          ? JSON.parse(source.slice(start, position)) as string
          : source.slice(start + 1, position - 1)
      }
      if (next !== "\\") return fail()
      escaped = true
      position++
    }
  }

  function value(depth: number): unknown {
    if (depth > 1_000) throw new SyntaxError("JSON exceeds the maximum nesting depth")
    skipWhitespace()
    switch (source[position]) {
      case '"':
        return string()
      case "{": {
        const object: JsonObject = {}
        position++
        skipWhitespace()
        if (source[position] === "}") {
          position++
          return object
        }
        for (;;) {
          const key = string()
          if (Object.hasOwn(object, key)) throw new SyntaxError("Duplicate JSON field")
          skipWhitespace()
          if (source[position++] !== ":") return fail()
          const property = value(depth + 1)
          if (key === "__proto__") {
            Object.defineProperty(object, key, {
              value: property, writable: true, enumerable: true, configurable: true,
            })
          } else {
            object[key] = property
          }
          skipWhitespace()
          const separator = source[position++]
          if (separator === "}") return object
          if (separator !== ",") return fail()
          skipWhitespace()
        }
      }
      case "[": {
        const array: unknown[] = []
        position++
        skipWhitespace()
        if (source[position] === "]") {
          position++
          return array
        }
        for (;;) {
          array.push(value(depth + 1))
          skipWhitespace()
          const separator = source[position++]
          if (separator === "]") return array
          if (separator !== ",") return fail()
        }
      }
      case "t":
        if (!source.startsWith("true", position)) return fail()
        position += 4
        return true
      case "f":
        if (!source.startsWith("false", position)) return fail()
        position += 5
        return false
      case "n":
        if (!source.startsWith("null", position)) return fail()
        position += 4
        return null
      default: {
        number.lastIndex = position
        const token = number.exec(source)?.[0]
        if (token === undefined) return fail()
        position = number.lastIndex
        if (numberToken !== undefined) return numberToken(token)
        const parsed = Number(token)
        if (/[.eE]/.test(token)) {
          if (!Number.isFinite(parsed)) throw new SyntaxError("Nonfinite JSON number")
          return parsed
        }
        if (integers !== "bigint" && Number.isSafeInteger(parsed)) {
          return parsed === 0 ? 0 : parsed
        }
        if (token.length - Number(token.startsWith("-")) > 4_300) {
          throw new SyntaxError("JSON integer exceeds 4,300 digits")
        }
        return BigInt(token)
      }
    }
  }

  const result = value(0)
  skipWhitespace()
  if (position !== source.length) return fail()
  return result
}

export function floatString(value: number): string {
  if (Object.is(value, -0)) return "-0.0"
  if (value !== 0 && (Math.abs(value) < 1e-4 || Math.abs(value) >= 1e16)) {
    return value.toExponential().replace(/e([+-])(\d)$/, "e$10$2")
  }
  return Number.isInteger(value) ? value + ".0" : String(value)
}

/** Preserve Python's float token so receipt replays hash identical bytes. */
export function floatToken(value: number): number | object {
  if (!Number.isFinite(value)) return value
  const token = floatString(value)
  return token === String(value) ? value : rawJson.rawJSON(token)
}

export function encodeJson(value: unknown): Buffer {
  const raw = JSON.stringify(value, function (this: JsonObject, key, item: unknown): unknown {
    if (loneSurrogate.test(key)) throw new TypeError("Invalid Unicode in JSON property")
    const original: unknown = Object.getOwnPropertyDescriptor(this, key)?.value
    if (!Object.is(original, item)) throw new TypeError("Unsupported JSON value")
    if (typeof item === "bigint") {
      const token = item.toString()
      if (token.length - Number(token.startsWith("-")) > 4_300) {
        throw new TypeError("JSON integer exceeds 4,300 digits")
      }
      return rawJson.rawJSON(token)
    }
    if (item === null || typeof item === "boolean") return item
    if (typeof item === "number" && Number.isFinite(item)) return item
    if (typeof item === "string" && !loneSurrogate.test(item)) return item
    if (Array.isArray(item) || isJsonObject(item) || rawJson.isRawJSON(item)) return item
    throw new TypeError("Value must encode as finite UTF-8 JSON")
  })
  return Buffer.from(raw, "utf8")
}

/** Match the canonical JSON used for retained provider resource identities. */
export function encodeCanonicalJson(value: unknown): string {
  const seen = new Set<object>()
  function encode(item: unknown): string {
    if (rawJson.isRawJSON(item) || (!isJsonObject(item) && !Array.isArray(item))) {
      return encodeJson(item).toString("utf8")
    }
    if (seen.has(item)) throw new TypeError("Cyclic JSON value")
    seen.add(item)
    try {
      if (Array.isArray(item)) return "[" + Array.from(item, encode).join(",") + "]"
      const byteKeys = Object.keys(item).map(key => ({ key, bytes: Buffer.from(key) }))
      byteKeys.sort((left, right) => Buffer.compare(left.bytes, right.bytes))
      const fields = byteKeys.map(({ key }) => {
        const entry = Object.getOwnPropertyDescriptor(item, key)?.value
        return encodeJson(key).toString("utf8") + ":" + encode(entry)
      })
      return "{" + fields.join(",") + "}"
    } finally {
      seen.delete(item)
    }
  }
  return encode(value).replace(/[\u007f-\uffff]/g, char =>
    "\\u" + char.charCodeAt(0).toString(16).padStart(4, "0"))
}

export function integerNumberToken(token: string): number | bigint {
  if (/[.eE]/.test(token)) throw new SyntaxError("Expected a JSON integer token")
  if (token.length - Number(token.startsWith("-")) > 4_300) throw new SyntaxError("JSON integer exceeds 4,300 digits")
  const number = Number(token)
  return Number.isSafeInteger(number) ? (number === 0 ? 0 : number) : BigInt(token)
}

export function validDuration(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
}

function canonicalUuid(value: unknown, type: typeof NabedraError = ValidationError): string {
  if (typeof value !== "string" || value.length !== 36 || !uuidPattern.test(value)) {
    throw new type("Receipt identity must be a canonical UUID")
  }
  return value
}

function isReceiptError(error: unknown): error is ReceiptError & JsonObject {
  return hasFields(error, "code", "message", "outcome")
    && typeof error.code === "string" && typeof error.message === "string"
    && (error.outcome === "not_applied" || error.outcome === "unknown")
}

function operationBody(operation: string, payload: JsonObject, requestId: string): Buffer {
  if (!isJsonObject(payload)) throw new ValidationError("Operation payload must be a dictionary")
  let body: Buffer
  try {
    body = encodeJson({ request_id: requestId, operation, payload })
  } catch {
    throw new ValidationError("Operation payload must encode as finite UTF-8 JSON")
  }
  if (body.byteLength > maxBody) throw new LimitExceededError("Encoded request exceeds 8 MiB")
  return body
}

function validateServerError(
  value: JsonObject,
  requestId: string,
  statusCode: number,
  failure: (message: string) => NabedraError,
): never {
  if (statusCode < 400 || statusCode > 599
      || !hasFields(value, "request_id", "error")
      || (value.request_id !== null && value.request_id !== requestId)) {
    throw failure("Invalid error response envelope")
  }
  const error = value.error
  if (!isReceiptError(error) || error.code.length === 0) throw failure("Invalid error details")
  const known = errorMappings.get(error.code)
  if ((known?.status ?? 500) !== statusCode
      || (error.code === "outcome_unknown" && error.outcome !== "unknown")) {
    throw failure("Error code/status/outcome mismatch")
  }
  const kind = error.outcome === "unknown" ? UnknownOutcomeError : (known?.type ?? NabedraError)
  throw new kind(error.message, { code: error.code, outcome: error.outcome, requestId, statusCode })
}

async function readBoundedResponse(response: http.IncomingMessage, maxBytes: number): Promise<Buffer> {
  const length = response.headers["content-length"]
  const statusCode = response.statusCode ?? null
  if (length !== undefined) {
    if (!/^\d+$/.test(length)) throw new ProtocolError("Invalid response Content-Length", { statusCode })
    if (Number(length) > maxBytes) throw new ProtocolError("Response exceeds byte limit", { statusCode })
  }
  let size = 0
  const chunks: Buffer[] = []
  for await (const chunk of response) {
    const buffer = chunk as Buffer
    size += buffer.byteLength
    if (size > maxBytes) throw new ProtocolError("Response exceeds byte limit", { statusCode })
    chunks.push(buffer)
  }
  if (!response.complete || (length !== undefined && Number(length) !== size)) {
    throw new TransportError("Incomplete response body", { statusCode })
  }
  return Buffer.concat(chunks, size)
}

export function exchangeMayHaveSent(error: unknown): boolean {
  return error instanceof NabedraError && sentFailures.has(error)
}

export class Transport {
  readonly timeout: number
  readonly #url: URL
  readonly #path: string
  readonly #token: string | null
  readonly #database: string | null
  readonly #tls?: tls.SecureContext

  constructor(url: string | null | undefined, token: string | null | undefined, options: ConnectionOptions = {}) {
    try {
      if (typeof url !== "string" || !/^https?:\/\/[^/]/i.test(url) || /[^\x21-\x7e]|[?#\\]/.test(url)) {
        throw new TypeError()
      }
      const authority = url.slice(url.indexOf("://") + 3).split("/", 1)[0]
      this.#url = new URL(url)
      if (!this.#url.hostname || authority.includes("@")) throw new TypeError()
      this.#path = url.slice(url.indexOf("://") + 3 + authority.length).replace(/\/+$/, "")
    } catch {
      throw new ValidationError("API URL must be absolute HTTP(S), without credentials, query or fragment")
    }
    if (token != null && (typeof token !== "string" || !/^[A-Za-z0-9._~+/\-]+=*(?![\s\S])/.test(token))) {
      throw new ValidationError("Token must be a nonempty safe bearer token")
    }
    const { timeout = 30, caFile = null, database = null } = options
    if (!validDuration(timeout)) throw new ValidationError("Timeout must be positive and finite")
    if (caFile !== null) {
      if (typeof caFile !== "string" || !caFile || caFile.includes("\0")) {
        throw new ValidationError("CA file must be a nonempty path string")
      }
      try {
        const caBundle = readFileSync(caFile, "utf8")
        const certificates = caBundle.match(/-----BEGIN CERTIFICATE-----[^]*?-----END CERTIFICATE-----/g)
        if (certificates === null) throw new TypeError()
        for (const certificate of certificates) new X509Certificate(certificate)
        const defaults = typeof tls.getCACertificates === "function"
          ? tls.getCACertificates("default")
          : tls.rootCertificates
        this.#tls = tls.createSecureContext({ ca: [...defaults, caBundle] })
      } catch {
        throw new ValidationError("Could not load TLS trust configuration")
      }
    }
    if (database !== null
        && (typeof database !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}(?![\s\S])/.test(database))) {
      throw new ValidationError("Invalid database selector")
    }
    this.#token = token ?? null
    this.#database = database
    this.timeout = timeout
  }

  createAgent(): http.Agent {
    const options = { keepAlive: true, maxSockets: 1, maxFreeSockets: 1 }
    return this.#url.protocol === "https:"
      ? new https.Agent({ ...options, secureContext: this.#tls, rejectUnauthorized: true })
      : new http.Agent(options)
  }

  async exchange(method: string, route: string, options: ExchangeOptions = {}): Promise<HttpResponse> {
    const token = this.#token
    if (token === null && options.authorization === undefined) {
      throw new AuthenticationError("Set NABEDRA_TOKEN or pass token before making a request")
    }
    const { body, timeout = this.timeout, maxBytes = maxBody, deadline, agent, readResponse } = options
    const deadlineBudget = deadline === undefined ? Infinity : deadline - performance.now() / 1000
    if (deadlineBudget <= 0) throw new TransportError("HTTP deadline expired before sending")
    const headers = {
      ...options.headers,
      ...(body === undefined ? {} : { "Content-Length": String(body.byteLength) }),
      ...(this.#database === null ? {} : { "X-Nabedra-Database": this.#database }),
      Authorization: options.authorization ?? "Bearer " + token,
    }
    const path = this.#path + route
    const request = this.#url.protocol === "https:" ? https.request : http.request
    const security = this.#url.protocol === "https:"
      ? { secureContext: this.#tls, rejectUnauthorized: true }
      : {}

    return new Promise((resolve, reject) => {
      const milliseconds = (seconds: number) => Math.min(maxTimerMs, Math.max(1, Math.ceil(seconds * 1000)))
      let statusCode: number | null = null
      let mayHaveSent = false
      let completed = false
      let connectionTimer: NodeJS.Timeout
      let deadlineTimer: NodeJS.Timeout | undefined
      const finish = () => {
        completed = true
        clearTimeout(connectionTimer)
        clearTimeout(deadlineTimer)
      }
      const fail = (error: NabedraError) => {
        if (completed) return
        finish()
        if (mayHaveSent) sentFailures.add(error)
        reject(error)
      }
      const req = request(this.#url, { method, path, headers, agent: agent ?? false, ...security }, (response) => {
        mayHaveSent = true
        statusCode = response.statusCode ?? null
        const read = readResponse ?? ((message) => readBoundedResponse(message, maxBytes))
        void Promise.resolve().then(() => read(response)).then(
          (responseBody) => {
            if (completed) return
            finish()
            resolve({
              status: response.statusCode ?? 0,
              headers: response.headers,
              rawHeaders: response.rawHeaders,
              body: responseBody,
            })
          },
          (error: unknown) => {
            fail(error instanceof NabedraError
              ? error
              : new TransportError("Response lost during HTTP I/O", { statusCode }))
            response.destroy()
            req.destroy()
          },
        )
      })
      const expire = () => {
        if (completed) return
        fail(new TransportError("Response lost during HTTP I/O", { statusCode }))
        req.destroy()
      }
      connectionTimer = setTimeout(expire, milliseconds(Math.min(timeout, deadlineBudget)))
      if (deadline !== undefined) deadlineTimer = setTimeout(expire, milliseconds(deadlineBudget))
      req.setTimeout(milliseconds(timeout), expire)
      req.on("socket", (socket) => {
        const markConnected = () => {
          mayHaveSent = true
          clearTimeout(connectionTimer)
        }
        if (socket instanceof tls.TLSSocket) {
          if (socket.authorized) markConnected()
          else socket.once("secureConnect", () => { if (socket.authorized) markConnected() })
        } else if (socket.connecting) {
          socket.once("connect", markConnected)
        } else {
          markConnected()
        }
      })
      req.on("upgrade", (response, socket) => {
        mayHaveSent = true
        fail(new ProtocolError("Unexpected HTTP upgrade", { statusCode: response.statusCode ?? null }))
        socket.destroy()
      })
      req.on("error", (error: NodeJS.ErrnoException) => {
        if (error instanceof NabedraError) fail(error)
        else if (error.code?.startsWith("HPE_")) fail(new ProtocolError("Malformed HTTP response", { statusCode }))
        else fail(new TransportError("Response lost during HTTP I/O", { statusCode }))
      })
      req.end(body)
    })
  }

  async #envelope(
    method: string, route: string, options: {
      body?: Buffer
      requestId: string
      mutation: boolean
      timeout?: number
      integers?: "bigint"
    },
  ): Promise<{ status: number; value: JsonObject }> {
    const { body, requestId, mutation, timeout, integers } = options
    const failure = (message: string, statusCode: number | null, network = false): NabedraError => {
      const kind = mutation ? UnknownOutcomeError : (network ? TransportError : ProtocolError)
      return new kind(message, { requestId, statusCode })
    }
    let response: HttpResponse
    try {
      response = await this.exchange(method, route, {
        body,
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        timeout,
      })
    } catch (error) {
      if (!(error instanceof ProtocolError || error instanceof TransportError)) throw error
      throw failure(error.message, error.statusCode, error instanceof TransportError)
    }

    const { status, headers } = response
    if (headers["content-type"]?.split(";", 1)[0].trim().toLowerCase() !== "application/json") {
      throw failure("Response is not application/json", status)
    }
    let value: unknown
    try {
      value = parseJson(response.body, { integers })
    } catch {
      throw failure("Malformed response JSON", status)
    }
    if (!isJsonObject(value)) throw failure("Invalid response envelope", status)
    return { status, value }
  }

  async request<T = unknown>(
    operation: string,
    payload: JsonObject,
    decode?: (data: unknown) => T,
  ): Promise<T> {
    const mutation = mutations.has(operation)
    if (!mutation && !reads.has(operation)) throw new ValidationError("Unsupported v0 operation")
    if (decode !== undefined && typeof decode !== "function") {
      throw new ValidationError("Result decoder must be callable")
    }
    const requestId = randomUUID()
    const body = operationBody(operation, payload, requestId)
    const { status, value } = await this.#envelope("POST", "/v1/operations", {
      body, requestId, mutation, integers: strictIntegers.has(operation) ? "bigint" : undefined,
    })
    const failure = (message: string): NabedraError => {
      const kind = mutation ? UnknownOutcomeError : ProtocolError
      return new kind(message, { requestId, statusCode: status })
    }
    if (status !== 200) validateServerError(value, requestId, status, failure)
    if (!hasFields(value, "request_id", "data") || value.request_id !== requestId) {
      throw failure("Invalid success envelope or request ID")
    }
    if (decode === undefined) return value.data as T
    try {
      return decode(value.data)
    } catch (error) {
      if (error instanceof ValidationError || error instanceof LimitExceededError || error instanceof ProtocolError) {
        throw failure("Invalid operation result")
      }
      throw error
    }
  }

  async submit(operation: string, payload: JsonObject, requestId?: string | null): Promise<ReceiptData> {
    const mutation = mutations.has(operation)
    if (!mutation && !reads.has(operation)) throw new ValidationError("Unsupported operation")
    requestId = requestId == null ? randomUUID() : canonicalUuid(requestId)
    const body = operationBody(operation, payload, requestId)
    const { status, value } = await this.#envelope("POST", "/v1/requests", { body, requestId, mutation })
    const failure = (message: string): NabedraError => {
      const kind = mutation ? UnknownOutcomeError : ProtocolError
      return new kind(message, { requestId, statusCode: status })
    }
    if (status !== 200 && status !== 202) validateServerError(value, requestId, status, failure)
    if (status !== 202 || !hasFields(value, "request_id", "run_id", "state")
        || value.request_id !== requestId || value.state !== "queued") {
      throw failure("Invalid acceptance receipt")
    }
    let runId: string
    try {
      runId = canonicalUuid(value.run_id, ProtocolError)
    } catch {
      throw failure("Invalid acceptance run identity")
    }
    return { requestId, runId, state: "queued", result: null, error: null }
  }

  async receipt(runId: string, requestId: string, timeout?: number): Promise<ReceiptData> {
    runId = canonicalUuid(runId)
    requestId = canonicalUuid(requestId)
    const { status, value } = await this.#envelope("GET", `/v1/requests/${runId}/${requestId}`, {
      requestId, mutation: false, timeout: timeout === undefined ? undefined : Math.min(timeout, this.timeout),
    })
    const failure = (message: string): ProtocolError => new ProtocolError(message, { requestId, statusCode: status })
    if (status !== 200 && status !== 202) validateServerError(value, requestId, status, failure)
    if (status !== 200 || !hasFields(value, "request_id", "run_id", "state", "result", "error")
        || value.request_id !== requestId || value.run_id !== runId
        || typeof value.state !== "string" || !receiptStates.has(value.state)) {
      throw failure("Invalid request receipt")
    }
    const { state, result, error } = value
    if (error !== null && !isReceiptError(error)) throw failure("Invalid receipt error")
    if ((state === "failed" && error === null)
        || (state === "succeeded" && error !== null)
        || (state !== "succeeded" && state !== "failed" && (result !== null || error !== null))) {
      throw failure("Invalid receipt state")
    }
    return { requestId, runId, state: state as ReceiptState, result, error }
  }
}

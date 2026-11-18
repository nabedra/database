import { floatString } from "../transport.js"
import { Decimal } from "decimal.js"
import { NabedraError, ProtocolError } from "../errors.js"
import { Transport, isJsonObject, parseJson } from "../transport.js"

export const maxEventBytes = 64 * 1024
export const maxBodyBytes = 1024 * 1024
export const maxPageCount = 100
export const maxOffset = (1n << 64n) - 1n
export const maxPartition = 2 ** 32 - 1
const tokenPattern = /^[A-Za-z0-9._~+/\-]+=*(?![\s\S])/
const codePattern = /^[a-z][a-z0-9_]{0,63}(?![\s\S])/
const offsetPattern = /^(?:0|[1-9][0-9]{0,19})(?![\s\S])/
const minInteger = -(1n << 63n)
const decimalTokens = new WeakMap<Decimal, string>()

export type EventData = string | boolean | number | bigint | Decimal | null
  | readonly EventData[] | { readonly [key: string]: EventData }

export interface EventsErrorOptions {
  code: string
  statusCode?: number | null
  retryAfter?: number | null
  partition?: number | null
  earliestOffset?: bigint | null
  outcome?: "not_applied" | "unknown" | null
}

export class EventsError extends Error {
  code: string
  statusCode: number | null
  retryAfter: number | null
  partition: number | null
  earliestOffset: bigint | null
  outcome: "not_applied" | "unknown" | null

  constructor(message: string, { code, statusCode = null, retryAfter = null, partition = null,
    earliestOffset = null, outcome = null }: EventsErrorOptions) {
    super(message)
    this.name = new.target.name
    this.code = code
    this.statusCode = statusCode
    this.retryAfter = retryAfter
    this.partition = partition
    this.earliestOffset = earliestOffset
    this.outcome = outcome
  }
}

export class EventsProtocolError extends EventsError {}
export class EventsTransportError extends EventsError {}
export class EventsUnknownOutcomeError extends EventsError {
  claimId: string | null = null
}

export function boundedInt(value: unknown, max: number, min = 0): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new RangeError(`Expected an integer between ${min} and ${max}`)
  }
  return value
}

export function boundedOffset(value: unknown, min = 0n): bigint {
  if (typeof value !== "bigint" || value < min || value > maxOffset) {
    throw new RangeError(`Expected a bigint between ${min} and ${maxOffset}`)
  }
  return value
}

export function offsetString(value: unknown, min = 0n): bigint {
  if (typeof value !== "string" || !offsetPattern.test(value)) {
    throw new TypeError("Expected a canonical decimal offset string")
  }
  return boundedOffset(BigInt(value), min)
}

export function positiveSeconds(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw new RangeError("Expected a positive finite duration")
  }
  return value
}

// Retain the coefficient's scale and signed zero when decoding a broker token.
function decimalString(token: string): string {
  const [mantissa, exponentText = "0"] = token.toLowerCase().split("e")
  const sign = mantissa.startsWith("-") ? "-" : ""
  const [whole, fraction = ""] = mantissa.slice(sign.length).split(".")
  const digits = (whole + fraction).replace(/^0+(?=\d)/, "")
  const exponent = BigInt(exponentText) - BigInt(fraction.length)
  const adjusted = exponent + BigInt(digits.length - 1)
  if (exponent === 0n) return sign + digits
  if (exponent < 0n && adjusted >= -6n) {
    const point = digits.length + Number(exponent)
    return point > 0
      ? sign + digits.slice(0, point) + "." + digits.slice(point)
      : sign + "0." + "0".repeat(-point) + digits
  }
  const coefficient = digits.length === 1 ? digits : digits[0] + "." + digits.slice(1)
  return sign + coefficient + "E" + (adjusted >= 0n ? "+" : "") + adjusted
}

export function jsonLoads(raw: Uint8Array, exactNumbers = false): unknown {
  return parseJson(raw, {
    numberToken(token) {
      const number = Number(token)
      if (/[.eE]/.test(token)) {
        if (!Number.isFinite(number)) throw new RangeError("JSON number is outside the supported range")
        if (!exactNumbers) return number
        const exponent = token.split(/[eE]/)[1]
        if (exponent !== undefined && exponent.replace(/^[+-]?0*/, "").length > 18) {
          throw new RangeError("JSON number is outside the supported range")
        }
        const decimal = new Decimal(token)
        decimalTokens.set(decimal, decimalString(token))
        return decimal
      }
      if (Number.isSafeInteger(number)) return number === 0 ? 0 : number
      if (token.length - Number(token.startsWith("-")) > 4_300) {
        throw new RangeError("JSON integer exceeds 4,300 digits")
      }
      return BigInt(token)
    },
  })
}

function compareCodePoints(left: string, right: string): number {
  const l = Array.from(left, char => char.codePointAt(0)!)
  const r = Array.from(right, char => char.codePointAt(0)!)
  for (let index = 0; index < Math.min(l.length, r.length); ++index) {
    if (l[index] !== r[index]) return l[index] - r[index]
  }
  return l.length - r.length
}

export function canonicalJson(data: unknown, limit = maxBodyBytes): Buffer {
  const parts: string[] = []
  let size = 0

  function append(text: string): void {
    size += Buffer.byteLength(text)
    if (size > limit) throw new RangeError("Encoded JSON exceeds the byte limit")
    parts.push(text)
  }

  function string(value: string): void {
    if (value.length > limit - size) throw new RangeError("Encoded JSON exceeds the byte limit")
    if (!value.isWellFormed()) throw new TypeError("JSON strings must encode as UTF-8")
    append(JSON.stringify(value))
  }

  function write(value: unknown, depth: number): void {
    if (Array.isArray(value) || isJsonObject(value)) {
      if (depth >= 64) throw new RangeError("JSON nesting exceeds 64 container levels")
      if (Array.isArray(value)) {
        if (value.length > limit - size) throw new RangeError("Encoded JSON exceeds the byte limit")
        append("[")
        for (const [index, child] of value.entries()) {
          if (index) append(",")
          write(child, depth + 1)
        }
        append("]")
      } else {
        const keys = Object.keys(value)
        if (keys.length > limit - size) throw new RangeError("Encoded JSON exceeds the byte limit")
        if (Object.getOwnPropertySymbols(value).some(key => Object.prototype.propertyIsEnumerable.call(value, key))) {
          throw new TypeError("JSON object keys must be strings")
        }
        keys.sort(compareCodePoints)
        append("{")
        for (const [index, key] of keys.entries()) {
          if (index) append(",")
          string(key)
          append(":")
          write(value[key], depth + 1)
        }
        append("}")
      }
    } else if (typeof value === "string") {
      string(value)
    } else if (typeof value === "bigint") {
      if (value < minInteger || value > maxOffset) throw new RangeError("JSON integer does not fit an int64 or uint64")
      append(String(value))
    } else if (typeof value === "number") {
      if (!Number.isFinite(value)) throw new RangeError("JSON numbers must be finite")
      append(Number.isSafeInteger(value) && !Object.is(value, -0) ? String(value) : floatString(value))
    } else if (value instanceof Decimal) {
      if (!value.isFinite() || !Number.isFinite(value.toNumber())) throw new RangeError("JSON number is outside the supported range")
      append(decimalTokens.get(value) ?? decimalString(value.valueOf()))
    } else if (typeof value === "boolean" || value === null) {
      append(String(value))
    } else {
      throw new TypeError("Use JSON values made of objects, arrays and scalars")
    }
  }

  write(data, 0)
  return Buffer.from(parts.join(""))
}

function retryAfter(header: string | undefined): number | null {
  if (!header || header.length > 128) return null
  if (/^[0-9]+(?![\s\S])/.test(header)) return Number(header)
  // A stray date number is not an HTTP-date.
  if (!/[a-z]{3}/i.test(header)) return null
  const when = Date.parse(header)
  return Number.isFinite(when) ? Math.max(0, (when - Date.now()) / 1000) : null
}

export function endpoint(url: unknown, allowInsecure: unknown): string {
  const message = "Events URL must use HTTPS without credentials, query or fragment (HTTP requires allowInsecure=true)"
  if (typeof url !== "string" || typeof allowInsecure !== "boolean" || !/^[!-~]+(?![\s\S])/.test(url)
    || /[?#\\]/.test(url) || !/^https?:\/\/[^/@]+(?:\/|$)/.test(url)) throw new TypeError(message)
  try {
    const parts = new URL(url)
    if ((!allowInsecure && parts.protocol !== "https:") || !parts.hostname || parts.username || parts.password) {
      throw new TypeError()
    }
    return url.replace(/\/+$/, "")
  } catch {
    throw new TypeError(message)
  }
}

export interface EventsOptions {
  token: string
  timeout?: number
  caFile?: string | null
  allowInsecure?: boolean
}

interface RequestOptions<T> {
  decode: (value: unknown) => T
  body?: Uint8Array
  lease?: string
  expected?: readonly number[]
}

export class EventsHttp {
  readonly #transport: Transport

  constructor(url: string, { token, timeout = 10, caFile = null, allowInsecure = false }: EventsOptions) {
    const origin = endpoint(url, allowInsecure)
    if (typeof token !== "string" || token.length > 8192 || !tokenPattern.test(token)) {
      throw new TypeError("Expected a nonempty, header-safe bearer token")
    }
    positiveSeconds(timeout)
    if (caFile !== null && (typeof caFile !== "string" || !caFile || caFile.includes("\0"))) {
      throw new TypeError("CA file must be a nonempty path string")
    }
    try {
      this.#transport = new Transport(origin, token, { timeout, caFile })
    } catch {
      throw new TypeError("Could not load Events TLS trust configuration")
    }
  }

  async request<T>(method: string, path: string, { decode, body, lease, expected = [200] }: RequestOptions<T>): Promise<T> {
    const writing = method !== "GET"
    let status: number | null = null
    const invalid = (message: string, network = false): EventsError => {
      if (writing) return new EventsUnknownOutcomeError(message, { code: "outcome_unknown", statusCode: status, outcome: "unknown" })
      if (network) return new EventsTransportError(message, { code: "transport_error", statusCode: status })
      return new EventsProtocolError(message, { code: "protocol_error", statusCode: status })
    }
    let response
    try {
      response = await this.#transport.exchange(method, path, {
        body,
        maxBytes: maxBodyBytes,
        headers: { Accept: "application/json",
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
          ...(lease === undefined ? {} : { "X-Events-Lease": lease }) },
      })
    } catch (error) {
      if (error instanceof NabedraError) status = error.statusCode
      if (error instanceof ProtocolError && error.message.includes("exceeds")) {
        throw invalid("Events response exceeds the byte limit")
      }
      throw invalid("Events response lost during HTTP I/O", true)
    }
    status = response.status
    const length = response.headers["content-length"]
    if (length !== undefined && (!/^[0-9]{1,10}(?![\s\S])/.test(length) || Number(length) !== response.body.length)) {
      throw invalid("Invalid or incomplete Events response length", true)
    }
    if (status === 204 && expected.includes(status) && !response.body.length) return decode(null)
    if (response.headers["content-type"]?.split(";", 1)[0].trim().toLowerCase() !== "application/json") {
      throw invalid("Events response is not application/json")
    }
    try {
      const value = jsonLoads(response.body, true)
      if (expected.includes(status)) return decode(value)
      if (status >= 400 && status <= 599) {
        throw serverError(value, status, retryAfter(response.headers["retry-after"]), writing)
      }
    } catch (error) {
      if (error instanceof EventsError) throw error
      throw invalid("Malformed Events response JSON or fields")
    }
    throw invalid("Unexpected Events response status")
  }
}

function serverError(value: unknown, status: number, retryAfter: number | null, writing: boolean): EventsError {
  if (!isJsonObject(value)) throw new TypeError("Expected an Events error object")
  const { code, message } = value
  const outcome = value.outcome ?? null
  if (typeof code !== "string" || !codePattern.test(code) || typeof message !== "string"
    || (outcome !== null && outcome !== "not_applied" && outcome !== "unknown")) {
    throw new TypeError("Invalid Events error details")
  }
  const partition = value.partition == null ? null : boundedInt(value.partition, maxPartition)
  const earliestOffset = value.earliest_offset == null ? null : offsetString(value.earliest_offset, 1n)
  const ambiguous = writing && (outcome === "unknown" || code === "outcome_unknown" || code === "unknown_outcome"
    || ((status === 502 || status === 504) && outcome !== "not_applied"))
  const ErrorType = ambiguous ? EventsUnknownOutcomeError : EventsError
  return new ErrorType(message, { code, statusCode: status, retryAfter, partition, earliestOffset, outcome: ambiguous ? "unknown" : outcome })
}

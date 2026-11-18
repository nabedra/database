import { hasFields, isJsonObject, parseJson, type JsonObject } from "./transport.js"

export const maxBody = 8 * 1024 * 1024
export const mutations = new Set([
  "table.create", "table.drop", "table.insert", "table.insert_many", "table.update", "table.upsert", "table.delete",
])

const fieldsByOperation: ReadonlyMap<string, readonly string[]> = new Map([
  ["table.create", ["name", "schema", "key", "format"]],
  ["table.describe", ["name"]],
  ["table.list", []],
  ["table.drop", ["table_id"]],
  ["table.insert", ["table_id", "row"]],
  ["table.insert_many", ["table_id", "rows"]],
  ["table.get", ["table_id", "key"]],
  ["table.update", ["table_id", "key", "values"]],
  ["table.upsert", ["table_id", "row"]],
  ["table.delete", ["table_id", "key"]],
  ["query.collect", ["table_id", "query"]],
  ["query.count", ["table_id", "query"]],
  ["query.explain", ["table_id", "query"]],
])

export type GatewayValue = string | number | bigint | boolean | null
  | readonly GatewayValue[] | GatewayRecord
export interface GatewayRecord {
  readonly [key: string]: GatewayValue
}

export function requireProtocol(ok: unknown): asserts ok {
  if (!ok) throw new TypeError("Invalid Nabedra Database protocol")
}

export function identity(value: unknown): string {
  requireProtocol(typeof value === "string" && /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?![\s\S])/.test(value))
  return value
}

function identifier(value: unknown): asserts value is string {
  requireProtocol(typeof value === "string" && /^[A-Za-z_][A-Za-z0-9_]{0,63}(?![\s\S])/.test(value))
}

export function fields(value: unknown, expected: readonly string[]): asserts value is JsonObject {
  requireProtocol(hasFields(value, expected))
}

function scalar(value: unknown): void {
  if (typeof value === "bigint") {
    requireProtocol(value >= -(1n << 63n) && value < 1n << 63n)
    return
  }
  requireProtocol(typeof value === "string" || typeof value === "boolean" || Number.isFinite(value))
}

function row(value: unknown): void {
  requireProtocol(isJsonObject(value) && Object.keys(value).length <= 128)
  for (const [key, item] of Object.entries(value)) {
    identifier(key)
    scalar(item)
  }
}

function predicate(value: unknown, depth: number): void {
  requireProtocol(depth <= 32 && isJsonObject(value))
  const { op } = value
  switch (op) {
    case "and":
    case "or":
      fields(value, ["op", "args"])
      requireProtocol(Array.isArray(value.args) && value.args.length >= 1)
      for (const arg of value.args) predicate(arg, depth + 1)
      return
    case "not":
      fields(value, ["op", "arg"])
      predicate(value.arg, depth + 1)
      return
    default:
      fields(value, ["op", "column", "value"])
      requireProtocol(op === "eq" || op === "ne" || op === "gt" || op === "ge" || op === "lt" || op === "le")
      identifier(value.column)
      scalar(value.value)
      requireProtocol(typeof value.value !== "boolean" || op === "eq" || op === "ne")
  }
}

export function operationPayload(name: unknown, value: unknown): asserts value is JsonObject {
  requireProtocol(typeof name === "string")
  const expected = fieldsByOperation.get(name)
  requireProtocol(expected)
  fields(value, expected)
  if (Object.hasOwn(value, "table_id")) identity(value.table_id)
  if (Object.hasOwn(value, "name")) identifier(value.name)
  if (name === "table.create") {
    const { schema, key } = value
    requireProtocol(isJsonObject(schema))
    const columns = Object.entries(schema)
    requireProtocol(columns.length > 0 && columns.length <= 128)
    for (const [column, type] of columns) {
      identifier(column)
      requireProtocol(type === "utf8" || type === "int64" || type === "float64" || type === "bool")
    }
    identifier(key)
    requireProtocol(Object.hasOwn(schema, key) && (schema[key] === "utf8" || schema[key] === "int64")
      && value.format === "parquet")
  } else if (Object.hasOwn(value, "key")) {
    requireProtocol(typeof value.key === "string" || typeof value.key === "bigint")
    scalar(value.key)
  }
  for (const key of ["row", "values"]) {
    if (Object.hasOwn(value, key)) row(value[key])
  }
  if (Object.hasOwn(value, "rows")) {
    requireProtocol(Array.isArray(value.rows) && value.rows.length > 0 && value.rows.length <= 10_000)
    for (const item of value.rows) row(item)
  }
  if (Object.hasOwn(value, "query")) {
    const query = value.query
    fields(query, ["select", "filter", "order_by", "limit"])
    if (query.select !== null) {
      const columns = query.select
      requireProtocol(Array.isArray(columns) && columns.length > 0 && columns.length <= 128)
      for (const column of columns) identifier(column)
      requireProtocol(new Set(columns).size === columns.length)
    }
    if (query.filter !== null) predicate(query.filter, 1)
    if (query.order_by !== null) {
      const order = query.order_by
      fields(order, ["column", "desc"])
      identifier(order.column)
      requireProtocol(typeof order.desc === "boolean")
    }
    requireProtocol(query.limit === null || (typeof query.limit === "bigint" && query.limit >= 0n))
  }
}

export function load(raw: Uint8Array): unknown {
  requireProtocol(raw instanceof Uint8Array && raw.byteLength <= maxBody)
  let depth = 0
  let quoted = false
  let escaped = false
  for (const byte of raw) {
    if (quoted) {
      if (escaped) escaped = false
      else if (byte === 92) escaped = true
      else if (byte === 34) quoted = false
    } else if (byte === 34) {
      quoted = true
    } else if (byte === 91 || byte === 123) {
      requireProtocol(++depth <= 96)
    } else if (byte === 93 || byte === 125) {
      --depth
    }
  }
  const value = parseJson(raw, { integers: "bigint" })
  checkStrings(value)
  return value
}

function checkStrings(value: unknown): void {
  if (typeof value === "string") {
    requireProtocol(value.isWellFormed())
  } else if (Array.isArray(value)) {
    for (const item of value) checkStrings(item)
  } else if (isJsonObject(value)) {
    for (const [key, item] of Object.entries(value)) {
      requireProtocol(key.isWellFormed())
      checkStrings(item)
    }
  }
}

export function freeze(value: unknown): GatewayValue {
  if (typeof value === "bigint") {
    const number = Number(value)
    return Number.isSafeInteger(number) ? number : value
  }
  if (Array.isArray(value)) return Object.freeze(value.map(freeze))
  if (isJsonObject(value)) {
    return Object.freeze(Object.fromEntries(Object.entries(value).map(([key, item]) => [key, freeze(item)])))
  }
  requireProtocol(value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number")
  return value
}

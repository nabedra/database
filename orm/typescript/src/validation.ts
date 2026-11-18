import { LimitExceededError, ProtocolError, ValidationError } from "./errors.js"
import { floatToken, isJsonObject, type JsonObject } from "./transport.js"

export type Scalar = string | bigint | number | boolean
export type Key = string | bigint
export type Row = Record<string, Scalar>
export type ScalarType = StringConstructor | BigIntConstructor | NumberConstructor | BooleanConstructor
export type Schema = Record<string, ScalarType>
export type Column<S extends Schema> = keyof S & string
export type RowOf<S extends Schema> = { -readonly [C in keyof S]: ReturnType<S[C]> }
export type KeyColumn<S extends Schema> = {
  [C in Column<S>]-?: Extract<S[C], StringConstructor | BigIntConstructor> extends never ? never : C
}[Column<S>]
export type KeyOf<S extends Schema, K extends Column<S>> = Extract<RowOf<S>[K], Key>
export type Update<S extends Schema, K extends Column<S>> =
  string extends K ? Partial<RowOf<S>> : Partial<Omit<RowOf<S>, K>> & { [C in K]?: never }
export type Filters<S extends Schema> = Partial<RowOf<S>>

type WireType = "utf8" | "int64" | "float64" | "bool"

const wireTypes = new Map<ScalarType, WireType>([
  [String, "utf8"], [BigInt, "int64"], [Number, "float64"], [Boolean, "bool"],
])
const scalarTypes = new Map<string, ScalarType>(Array.from(wireTypes, ([kind, wire]) => [wire, kind]))
export const int64Min = -(2n ** 63n)
export const int64Max = 2n ** 63n - 1n
export const maxColumns = 128
const identifier = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/

export function isRecord(value: unknown): value is JsonObject {
  return isJsonObject(value) && Object.getOwnPropertySymbols(value).length === 0
}

function mappingCopy(value: unknown, label: string): JsonObject {
  if (!isRecord(value)) throw new ValidationError(`${label} must be a mapping`)
  return { ...value }
}

export function validateIdentifier(value: unknown): string {
  if (typeof value !== "string" || !identifier.test(value)) {
    throw new ValidationError("Identifier must match [A-Za-z_][A-Za-z0-9_]{0,63}")
  }
  return value
}

export function validateFormat(value: unknown): "parquet" {
  if (value !== "parquet") throw new ValidationError("Only parquet format is supported")
  return value
}

export function validateSchema(schema: unknown, key?: string): Schema {
  const result = mappingCopy(schema, "Schema")
  const columns = Object.entries(result)
  if (columns.length === 0) throw new ValidationError("Schema must contain at least one column")
  if (columns.length > maxColumns) throw new LimitExceededError(`Schema exceeds ${maxColumns} columns`)
  for (const [column, kind] of columns) {
    validateIdentifier(column)
    if (!wireTypes.has(kind as ScalarType)) {
      throw new ValidationError(`Unsupported type for column '${column}'`)
    }
  }
  if (key !== undefined) {
    validateIdentifier(key)
    if (!Object.hasOwn(result, key) || (result[key] !== String && result[key] !== BigInt)) {
      throw new ValidationError("Primary key must name a string or bigint column")
    }
  }
  return result as Schema
}

export function schemaToWire(schema: Schema): Record<string, WireType> {
  return Object.fromEntries(Object.entries(validateSchema(schema)).map(([column, kind]) => [
    column, wireTypes.get(kind)!,
  ]))
}

export function schemaFromWire(schema: unknown): Schema {
  const source = mappingCopy(schema, "Wire schema")
  const result = Object.fromEntries(Object.entries(source).map(([column, wire]) => {
    const kind = typeof wire === "string" ? scalarTypes.get(wire) : undefined
    if (kind === undefined) throw new ValidationError("Unsupported wire schema type")
    return [column, kind]
  }))
  return validateSchema(result)
}

export function validateValue(value: unknown, expectedType: ScalarType, column = "value"): Scalar {
  if (!wireTypes.has(expectedType)) throw new ValidationError("Unsupported scalar type")
  if (expectedType === String && typeof value === "string") return value
  if (expectedType === Boolean && typeof value === "boolean") return value
  if (expectedType === BigInt && typeof value === "bigint") {
    if (value < int64Min || value > int64Max) {
      throw new ValidationError(`Column '${column}' requires a signed int64 value`)
    }
    return value
  }
  if (expectedType === Number && typeof value === "number") {
    if (!Number.isFinite(value)) throw new ValidationError(`Column '${column}' requires a finite float`)
    return value
  }
  throw new ValidationError(`Column '${column}' requires exact ${expectedType.name.toLowerCase()} values`)
}

export function validateKey(value: unknown, schema: Schema, key: string): Key {
  validateIdentifier(key)
  const checked = validateSchema(schema, key)
  return validateValue(value, checked[key], key) as Key
}

export function validateRow(row: unknown, schema: Schema, key: string, partial = false): Row {
  validateIdentifier(key)
  const checked = validateSchema(schema, key)
  const result = mappingCopy(row, "Row")
  for (const column of Object.keys(result)) {
    validateIdentifier(column)
    if (!Object.hasOwn(checked, column)) throw new ValidationError(`Unknown column '${column}'`)
  }
  if (partial) {
    if (Object.hasOwn(result, key)) throw new ValidationError("Updates cannot include the primary key")
  } else if (Object.keys(checked).length !== Object.keys(result).length) {
    throw new ValidationError("Full rows must include every schema column")
  }
  for (const [column, value] of Object.entries(result)) validateValue(value, checked[column], column)
  return result as Row
}

export function scalarToWire(value: Scalar): unknown {
  return typeof value === "number" ? floatToken(value) : value
}

export function rowToWire(row: Row): JsonObject {
  return Object.fromEntries(Object.entries(row).map(([column, value]) => [column, scalarToWire(value)]))
}

export function decodeRow(data: unknown, schema: Schema): Row {
  const columns = Object.entries(schema)
  if (!isRecord(data) || Object.keys(data).length !== columns.length
    || columns.some(([column]) => !Object.hasOwn(data, column))) {
    throw new ProtocolError("Row must contain exactly the projected columns")
  }
  return Object.fromEntries(columns.map(([column, kind]) => [
    column, validateValue(data[column], kind, column),
  ]))
}

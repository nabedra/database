import { LimitExceededError, ProtocolError, ValidationError } from "./errors.js"
import { floatToken, isJsonObject, type JsonObject } from "./transport.js"
import {
  checkValue, columnKind, typeFromWire, typeToWire, vectorFromWire,
  type List, type SparseVector, type Vector, type VectorInput, type VectorValue,
} from "./vector.js"

export type { VectorValue } from "./vector.js"
export type Scalar = string | bigint | number | boolean
export type Key = string | bigint
export type Row = Record<string, Scalar | readonly string[] | VectorValue | null>
export type ScalarType = StringConstructor | BigIntConstructor | NumberConstructor | BooleanConstructor
export type ColumnType = ScalarType | Vector | List
export type Schema = Record<string, ColumnType>
export type Column<S extends Schema> = keyof S & string
type ValueOf<T> =
  T extends ScalarType ? ReturnType<T>
    : T extends Vector<true> ? SparseVector | null
      : T extends Vector<false> ? Float32Array | null
        : T extends Vector ? SparseVector | Float32Array | null
          : T extends List<StringConstructor> ? string[]
            : T extends List<Vector<false>> ? Float32Array[] | null
              : string[] | Float32Array[] | null
type InputOf<T> =
  T extends ScalarType ? ReturnType<T>
    : T extends Vector<true> ? Readonly<SparseVector> | null
      : T extends Vector<false> ? VectorInput | null
        : T extends Vector ? Readonly<SparseVector> | VectorInput | null
          : T extends List<StringConstructor> ? readonly string[]
            : T extends List<Vector<false>> ? readonly VectorInput[] | null
              : readonly string[] | readonly VectorInput[] | null
export type RowOf<S extends Schema> = { -readonly [C in keyof S]: ValueOf<S[C]> }
/** A full row to write. It omits the columns E that the database embeds. */
export type RowInput<S extends Schema, E extends Column<S> = never> = {
  -readonly [C in keyof S as C extends E ? never : C]: InputOf<S[C]>
}
export type KeyColumn<S extends Schema> = {
  [C in Column<S>]-?: Extract<S[C], StringConstructor | BigIntConstructor> extends never ? never : C
}[Column<S>]
export type KeyOf<S extends Schema, K extends Column<S>> = Extract<RowOf<S>[K], Key>
export type Update<S extends Schema, K extends Column<S>, E extends Column<S> = never> =
  string extends K ? Partial<RowInput<S, E>> : Partial<Omit<RowInput<S, E>, K>> & { [C in K]?: never }
export type Filters<S extends Schema> = {
  [C in keyof S as Extract<S[C], ScalarType> extends never ? never : C]?: ReturnType<Extract<S[C], ScalarType>>
}
/** The columns that a ranked query returns by default: every selected column, or every nonvector column. */
export type RankedColumn<S extends Schema, C extends Column<S>> = [Column<S>] extends [C]
  ? string extends C ? C : { [N in C]: S[N] extends ScalarType | List<StringConstructor> ? N : never }[C]
  : C

type WireType = "utf8" | "int64" | "float64" | "bool" | JsonObject

const wireTypes = new Map<ColumnType, WireType>([
  [String, "utf8"], [BigInt, "int64"], [Number, "float64"], [Boolean, "bool"],
])
const scalarTypes = new Map<unknown, ColumnType>(Array.from(wireTypes, ([kind, wire]) => [wire, kind]))
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
    if (columnKind(kind) === null && !wireTypes.has(kind as ScalarType)) {
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
    column, wireTypes.get(kind) ?? typeToWire(kind),
  ]))
}

export function schemaFromWire(schema: unknown): Schema {
  return decodeSchema(schema, false)
}

export function decodeSchema(schema: unknown, response: boolean): Schema {
  const source = mappingCopy(schema, "Wire schema")
  return validateSchema(Object.fromEntries(Object.entries(source).map(([column, wire]) => [
    column, scalarTypes.get(wire) ?? typeFromWire(wire, response),
  ])))
}

/** Validate one value for rows, keys and predicate builders, copying vectors and lists. */
export function validateValue(value: unknown, expectedType: ColumnType, column = "value"): Row[string] {
  return columnKind(expectedType) === null
    ? validateScalar(value, expectedType as ScalarType, column)
    : checkValue(value, expectedType, column) as Row[string]
}

export function validateScalar(value: unknown, expectedType: ScalarType, column = "value"): Scalar {
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
  for (const [column, value] of Object.entries(result)) {
    result[column] = validateValue(value, checked[column], column)
  }
  return result as Row
}

export function scalarToWire(value: Scalar): unknown {
  return typeof value === "number" ? floatToken(value) : value
}

export function decodeRow(data: unknown, schema: Schema): Row {
  const columns = Object.entries(schema)
  if (!isRecord(data) || Object.keys(data).length !== columns.length
    || columns.some(([column]) => !Object.hasOwn(data, column))) {
    throw new ProtocolError("Row must contain exactly the projected columns")
  }
  return Object.fromEntries(columns.map(([column, kind]) => [
    column, columnKind(kind) === null
      ? validateValue(data[column], kind, column)
      : vectorFromWire(data[column], kind, column) as Row[string],
  ]))
}

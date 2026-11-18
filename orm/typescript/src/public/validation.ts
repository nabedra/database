import {
  validateRow as row, validateSchema as schema, validateValue as value, type ScalarType, type Schema,
} from "../validation.js"

export {
  validateIdentifier, validateFormat, schemaToWire, schemaFromWire, validateKey, int64Min, int64Max, maxColumns,
  type Scalar, type Key, type Row, type Schema, type RowOf, type ScalarType,
} from "../validation.js"

export function validateSchema(input: unknown, { key }: { key?: string | null } = {}): Schema {
  return schema(input, key ?? undefined)
}

export function validateValue(input: unknown, expectedType: ScalarType, { column }: { column?: string } = {}) {
  return value(input, expectedType, column)
}

export function validateRow(input: unknown, rowSchema: Schema, { key, partial }: { key: string; partial?: boolean }) {
  return row(input, rowSchema, key, partial)
}

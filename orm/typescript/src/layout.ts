import { LimitExceededError, ValidationError } from "./errors.js"
import { hasFields, type JsonObject } from "./transport.js"
import { columnKind, vectorToWire } from "./vector.js"
import { isRecord, scalarToWire, schemaToWire, validateIdentifier, validateSchema, type Schema } from "./validation.js"

export const scoreColumn = "_score"
const maxSearchColumns = 8
const optionFields = ["partition_by", "full_text", "embed"]

export interface EmbedOptions {
  source: string
  model: string
}

/** A table's columns and search options. Absent options are null or empty. */
export interface Layout {
  readonly schema: Schema
  readonly partitionBy: string | null
  readonly fullText: Readonly<Record<string, string>>
  readonly embed: Readonly<Record<string, EmbedOptions>>
}

const layouts = new WeakMap<object, Layout>()

export function tableLayout(table: object): Layout | undefined {
  return layouts.get(table)
}

export function setTableLayout(table: object, layout: Layout): void {
  layouts.set(table, layout)
}

function text(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) throw new ValidationError(`${label} must be a nonempty string`)
  return value
}

function options(value: unknown, label: string): JsonObject {
  if (!isRecord(value)) throw new ValidationError(`${label} must be a mapping`)
  return { ...value }
}

function columnType(schema: Schema, column: unknown): unknown {
  const name = validateIdentifier(column)
  return Object.hasOwn(schema, name) ? schema[name] : undefined
}

function isVector(type: unknown): boolean {
  const kind = columnKind(type)
  return kind !== null && kind !== "tags"
}

/** Check search options against a validated schema. */
export function buildLayout(schema: Schema, partitionBy?: unknown, fullText?: unknown, embed?: unknown): Layout {
  if (partitionBy != null) {
    const type = columnType(schema, partitionBy)
    if (type !== String && type !== BigInt) {
      throw new ValidationError("partition_by must name a string or integer column")
    }
  }
  const texts = options(fullText ?? {}, "full_text")
  for (const [column, language] of Object.entries(texts)) {
    if (columnType(schema, column) !== String) {
      throw new ValidationError(`Column '${column}' must be a string column for full text`)
    }
    text(language, "Language")
  }
  const embedded = Object.fromEntries(Object.entries(options(embed ?? {}, "embed")).map(([column, value]) => {
    if (columnKind(columnType(schema, column)) !== "dense") {
      throw new ValidationError(`Column '${column}' must be a dense vector column for embed`)
    }
    return [column, checkEmbed(value, schema)]
  }))
  const searched = Object.entries(schema).filter(([column, type]) => isVector(type) || Object.hasOwn(texts, column))
  if (searched.length > maxSearchColumns) {
    throw new LimitExceededError(`Table exceeds ${maxSearchColumns} search columns`)
  }
  if (searched.length > 0 && Object.hasOwn(schema, scoreColumn)) {
    throw new ValidationError("Column '_score' is reserved on tables with search columns")
  }
  return {
    schema: Object.freeze({ ...schema }),
    partitionBy: partitionBy as string | null | undefined ?? null,
    fullText: texts as Record<string, string>,
    embed: embedded,
  }
}

/** Decode the optional descriptor fields. Absent options are omitted, never null or empty. */
export function layoutFromDescriptor(schema: Schema, descriptor: JsonObject): Layout {
  for (const field of optionFields) {
    if (Object.hasOwn(descriptor, field) && (descriptor[field] === null || hasFields(descriptor[field]))) {
      throw new ValidationError("Descriptor options must be omitted when absent")
    }
  }
  return buildLayout(schema, descriptor.partition_by, descriptor.full_text, descriptor.embed)
}

export function hasOnlyOptions(fields: readonly string[]): boolean {
  return fields.every((field) => optionFields.includes(field))
}

export function checkEmbed(value: unknown, schema: Schema): EmbedOptions {
  const checked = options(value, "Embed options")
  if (!hasFields(checked, "source", "model")) {
    throw new ValidationError("Embed options must contain exactly source and model")
  }
  if (columnType(schema, checked.source) !== String) {
    throw new ValidationError("Embed source must name a string column")
  }
  return { source: checked.source as string, model: text(checked.model, "Embed model") }
}

/** Check a new vector column against the table it joins. */
export function extendLayout(layout: Layout, name: string, type: unknown, embed: EmbedOptions | null): Layout {
  validateIdentifier(name)
  if (Object.hasOwn(layout.schema, name)) throw new ValidationError(`Column '${name}' already exists`)
  if (!isVector(type)) throw new ValidationError("Added columns must have a vector type")
  const schema = validateSchema({ ...layout.schema, [name]: type })
  return buildLayout(schema, layout.partitionBy, layout.fullText,
    embed === null ? layout.embed : { ...layout.embed, [name]: embed })
}

export function hasSearch({ schema, fullText }: Layout): boolean {
  return Object.keys(fullText).length > 0 || Object.values(schema).some(isVector)
}

/** The columns that a full row contains. */
export function writtenSchema({ schema, embed }: Layout): Schema {
  return Object.fromEntries(Object.entries(schema).filter(([column]) => !Object.hasOwn(embed, column)))
}

/** The columns that a ranked query returns unless others are selected. */
export function rankedColumns({ schema }: Layout): string[] {
  return Object.keys(schema).filter((column) => !isVector(schema[column]))
}

export function wireOptions({ partitionBy, fullText, embed }: Layout): JsonObject {
  return {
    ...(partitionBy === null ? {} : { partition_by: partitionBy }),
    ...(Object.keys(fullText).length === 0 ? {} : { full_text: { ...fullText } }),
    ...(Object.keys(embed).length === 0 ? {} : { embed: { ...embed } }),
  }
}

/** Compare two layouts without regard to the order of columns or options. */
export function sameLayout(left: Layout, right: Layout): boolean {
  return canonical(left) === canonical(right)
}

function canonical(layout: Layout): string {
  const sorted = (value: object) => Object.entries(value).sort(([a], [b]) => a < b ? -1 : 1)
  return JSON.stringify([
    sorted(schemaToWire(layout.schema)), layout.partitionBy, sorted(layout.fullText),
    sorted(layout.embed).map(([column, { source, model }]) => [column, source, model]),
  ])
}

export function rowToWire(row: Record<string, unknown>, { schema }: Layout): JsonObject {
  return Object.fromEntries(Object.entries(row).map(([column, value]) => [
    column, columnKind(schema[column]) === null ? scalarToWire(value as never) : vectorToWire(value, schema[column]),
  ]))
}

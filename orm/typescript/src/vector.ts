import { endianness } from "node:os"
import { LimitExceededError, ProtocolError, ValidationError } from "./errors.js"
import { hasFields, isJsonObject, type JsonObject } from "./transport.js"

export const maxDimensions = 4096
export const maxVectors = 1024
const maxIndex = 2 ** 32 - 1
const bigEndian = endianness() === "BE"
const indexPattern = /^(?:0|[1-9]\d{0,9})$/

/** A float32 vector column type. Create one with vector(). */
export class Vector<Sparse extends boolean = boolean> {
  readonly dim: number | null
  readonly sparse: Sparse

  constructor(dim: number | null, sparse: Sparse) {
    this.dim = dim
    this.sparse = sparse
    Object.freeze(this)
  }
}

/** A column type that holds several strings or several vectors. Create one with list(). */
export class List<Item extends StringConstructor | Vector<false> = StringConstructor | Vector<false>> {
  readonly item: Item

  constructor(item: Item) {
    this.item = item
    Object.freeze(this)
  }
}

export type SparseVector = Record<number, number>
export type VectorInput = readonly number[] | Float32Array
export type VectorValue = VectorInput | readonly VectorInput[] | Readonly<SparseVector>
export type QueryValue = string | Float32Array | Float32Array[] | SparseVector
export type Kind = "dense" | "sparse" | "multi" | "tags"

export function vector(dim: number): Vector<false>
export function vector(options: { sparse: true }): Vector<true>
export function vector(type: unknown): Vector {
  if (typeof type === "number" && Number.isInteger(type) && type >= 1 && type <= maxDimensions) {
    return new Vector(type, false)
  }
  if (hasFields(type, "sparse") && type.sparse === true) return new Vector(null, true)
  throw new ValidationError(`A vector needs 1 to ${maxDimensions} dimensions, or sparse`)
}

export function list<Item extends StringConstructor | Vector<false>>(item: Item): List<Item> {
  if (item !== String && !(item instanceof Vector && !item.sparse)) {
    throw new ValidationError("A list needs String or a dense vector type")
  }
  return new List(item)
}

/** Classify a nonscalar column type; null means a scalar or an unsupported type. */
export function columnKind(type: unknown): Kind | null {
  if (type instanceof Vector) return type.sparse ? "sparse" : "dense"
  if (type instanceof List) return type.item === String ? "tags" : "multi"
  return null
}

function itemDim(type: unknown): number {
  return ((type as List).item as Vector<false>).dim!
}

export function typeToWire(type: unknown): JsonObject {
  if (type instanceof Vector) {
    return type.sparse ? { type: "vector", sparse: true } : { type: "vector", dim: type.dim }
  }
  const { item } = type as List
  return { type: "list", item: item === String ? "utf8" : typeToWire(item) }
}

/** Decode one nonscalar column type. Unknown or extra fields are errors. Responses carry bigint dimensions. */
export function typeFromWire(wire: unknown, response: boolean): Vector | List {
  if (hasFields(wire, "type", "dim") && wire.type === "vector" && (typeof wire.dim === "bigint") === response) {
    return vector(response ? Number(wire.dim) : wire.dim as number)
  }
  if (hasFields(wire, "type", "sparse") && wire.type === "vector" && wire.sparse === true) {
    return vector({ sparse: true })
  }
  if (hasFields(wire, "type", "item") && wire.type === "list") {
    if (wire.item === "utf8") return list(String)
    const item = typeFromWire(wire.item, response)
    if (columnKind(item) === "dense") return list(item as Vector<false>)
  }
  throw new ValidationError("Unsupported wire schema type")
}

function invalidVector(column: string): ValidationError {
  return new ValidationError(`Column '${column}' requires a float32 vector`)
}

function float32(values: Iterable<number> & { length: number }, column: string): Float32Array {
  const result = new Float32Array(values)
  if (!result.every(Number.isFinite)) throw new ValidationError(`Column '${column}' requires finite float32 values`)
  return result
}

export function dense(value: unknown, column: string, dim: number | null = null): Float32Array {
  if (!(value instanceof Float32Array)) {
    // A hole in a sparse array reads as undefined here.
    if (!Array.isArray(value) || Array.from(value as unknown[]).some((item) => typeof item !== "number")) {
      throw invalidVector(column)
    }
  }
  const result = float32(value, column)
  if (dim !== null && result.length !== dim) {
    throw new ValidationError(`Column '${column}' requires ${dim} dimensions, got ${result.length}`)
  }
  return result
}

export function sparse(value: unknown, column: string): SparseVector {
  if (!isJsonObject(value) || Object.getOwnPropertySymbols(value).length > 0) {
    throw new ValidationError(`Column '${column}' requires a mapping of index to weight`)
  }
  // JavaScript lists integer keys in ascending order. Only the largest index is not one, and it is last.
  const entries = Object.entries(value)
  if (entries.some(([index]) => !indexPattern.test(index) || Number(index) > maxIndex)) {
    throw new ValidationError(`Column '${column}' requires unsigned 32-bit indices`)
  }
  if (entries.some(([, weight]) => typeof weight !== "number")) {
    throw new ValidationError(`Column '${column}' requires a mapping of index to weight`)
  }
  const weights = float32(entries.map(([, weight]) => weight as number), column)
  return Object.fromEntries(entries.map(([index], position) => [index, weights[position]]))
}

export function multi(value: unknown, column: string, dim: number | null = null): Float32Array[] {
  if (!Array.isArray(value)) {
    throw new ValidationError(`Column '${column}' requires a sequence of float32 vectors`)
  }
  if (value.length > maxVectors) throw new LimitExceededError(`Column '${column}' exceeds ${maxVectors} vectors`)
  return Array.from(value as unknown[], (item) => dense(item, column, dim))
}

export function tags(value: unknown, column: string): string[] {
  if (!Array.isArray(value) || Array.from(value as unknown[]).some((item) => typeof item !== "string")) {
    throw new ValidationError(`Column '${column}' requires a list of strings`)
  }
  return [...value as string[]]
}

/** Copy one row value of a nonscalar column. Only vector columns accept null. */
export function checkValue(value: unknown, type: unknown, column: string): unknown {
  const kind = columnKind(type)
  if (kind === "tags") return tags(value, column)
  if (value === null) return null
  if (kind === "dense") return dense(value, column, (type as Vector).dim)
  if (kind === "sparse") return sparse(value, column)
  return multi(value, column, itemDim(type))
}

/** Detach a query value before its column type is known. */
export function queryValue(value: unknown, column: string): QueryValue {
  if (typeof value === "string") return value
  if (isJsonObject(value)) return sparse(value, column)
  if (Array.isArray(value) && value.length > 0 && typeof value[0] !== "number" && typeof value[0] !== "boolean") {
    return multi(value, column)
  }
  return dense(value, column)
}

/** Match a detached query value against its column's type. */
export function checkQuery(value: QueryValue, type: unknown, column: string): void {
  const kind = columnKind(type)
  if (kind === "dense") {
    if (!(value instanceof Float32Array)) throw invalidVector(column)
    dense(value, column, (type as Vector).dim)
  } else if (kind === "sparse") {
    if (!isJsonObject(value)) {
      throw new ValidationError(`Column '${column}' requires a mapping of index to weight`)
    }
  } else if (!Array.isArray(value)) {
    throw new ValidationError(`Column '${column}' requires a sequence of float32 vectors`)
  } else {
    multi(value, column, itemDim(type))
  }
}

export function hasZeroVector(value: QueryValue): boolean {
  if (Array.isArray(value)) return value.some(hasZeroVector)
  return !(value instanceof Float32Array ? value : Object.values(value)).some(Boolean)
}

function encode(values: Float32Array | Uint32Array): string {
  const raw = Buffer.from(values.buffer, values.byteOffset, values.byteLength)
  return (bigEndian ? Buffer.from(raw).swap32() : raw).toString("base64")
}

/** Encode a checked row or query value. Lists of strings are already JSON. */
export function vectorToWire(value: unknown, type: unknown): unknown {
  const kind = columnKind(type)
  if (value === null || kind === "tags") return value
  if (kind === "dense") return { f32: encode(value as Float32Array) }
  if (kind === "sparse") {
    const weights = value as SparseVector
    return { sparse: {
      i: encode(Uint32Array.from(Object.keys(weights), Number)),
      f32: encode(Float32Array.from(Object.values(weights))),
    } }
  }
  const vectors = value as Float32Array[]
  const joined = new Float32Array(vectors.length * itemDim(type))
  vectors.forEach((item, index) => joined.set(item, index * item.length))
  return { f32s: encode(joined), n: vectors.length }
}

function decodeRaw(text: unknown): ArrayBuffer {
  if (typeof text !== "string") throw new ProtocolError("Invalid vector encoding")
  const raw = Buffer.from(text, "base64")
  if (raw.byteLength % 4 !== 0 || raw.toString("base64") !== text) throw new ProtocolError("Invalid vector encoding")
  if (bigEndian) raw.swap32()
  return Uint8Array.from(raw).buffer
}

function decodeFloats(text: unknown): Float32Array {
  const result = new Float32Array(decodeRaw(text))
  if (!result.every(Number.isFinite)) throw new ProtocolError("Nonfinite vector value")
  return result
}

/** Decode one value of a nonscalar column exactly. */
export function vectorFromWire(data: unknown, type: unknown, column: string): unknown {
  const kind = columnKind(type)
  if (kind === "tags") {
    if (!Array.isArray(data)) throw new ProtocolError("Invalid list value")
    return tags(data, column)
  }
  if (data === null) return null
  if (kind === "dense") {
    if (!hasFields(data, "f32")) throw new ProtocolError("Invalid vector value")
    const result = decodeFloats(data.f32)
    if (result.length !== (type as Vector).dim) throw new ProtocolError("Invalid vector dimensions")
    return result
  }
  if (kind === "sparse") {
    if (!hasFields(data, "sparse") || !hasFields(data.sparse, "i", "f32")) {
      throw new ProtocolError("Invalid vector value")
    }
    const indices = new Uint32Array(decodeRaw(data.sparse.i))
    const weights = decodeFloats(data.sparse.f32)
    if (indices.length !== weights.length
        || indices.some((index, position) => position > 0 && indices[position - 1] >= index)) {
      throw new ProtocolError("Invalid sparse vector")
    }
    return Object.fromEntries(Array.from(indices, (index, position) => [index, weights[position]]))
  }
  if (!hasFields(data, "f32s", "n")) throw new ProtocolError("Invalid vector value")
  const values = decodeFloats(data.f32s)
  const dim = itemDim(type)
  const count = typeof data.n === "bigint" ? Number(data.n) : -1
  if (count < 0 || count > maxVectors || values.length !== count * dim) throw new ProtocolError("Invalid vector list")
  return Array.from({ length: count }, (_, index) => values.slice(index * dim, (index + 1) * dim))
}

/** Encoded minus raw bytes of one wire value. */
export function base64Overhead(wire: unknown): number {
  if (!isJsonObject(wire)) return 0
  const parts = isJsonObject(wire.sparse) ? Object.values(wire.sparse) : [wire.f32 ?? "", wire.f32s ?? ""]
  return (parts as string[]).reduce((total, text) => total + text.length - Buffer.byteLength(text, "base64"), 0)
}

import { ValidationError } from "./errors.js"
import type { JsonObject } from "./transport.js"
import {
  scalarToWire, validateIdentifier, validateScalar,
  type Scalar, type ScalarType, type Schema, type VectorValue,
} from "./validation.js"
import { checkQuery, columnKind, hasZeroVector, queryValue, text, vectorToWire, type QueryValue } from "./vector.js"
import type { Layout } from "./layout.js"

type Comparison = "eq" | "ne" | "gt" | "ge" | "lt" | "le"
type Logical = "and" | "or"
type Operation = "cosine" | "dot" | "l2" | "match"
type Node =
  | { op: "column"; column: string }
  | { op: Comparison; column: string; value: Scalar }
  | { op: Logical; args: readonly Node[] }
  | { op: "not"; arg: Node }
  | { op: "contains" | "starts_with"; column: string; value: string }
  | { op: "contains_any"; column: string; values: readonly string[] }

const nodes = new WeakMap<Expr, Node>()
const rankings = new WeakMap<Ranking, { column: string; op: Operation; value: QueryValue }>()
const operations = new Map<unknown, readonly Operation[]>([
  ["dense", ["cosine", "dot", "l2"]], ["sparse", ["cosine", "dot"]], ["multi", ["cosine", "dot"]],
])
const scalarTypes: ReadonlyMap<string, ScalarType> = new Map<string, ScalarType>([
  ["string", String], ["bigint", BigInt], ["number", Number], ["boolean", Boolean],
])
const maxDepth = 32

function fromNode(node: Node): Expr {
  const expression: Expr = Object.create(Expr.prototype)
  nodes.set(expression, node)
  Object.freeze(expression)
  return expression
}

function compare(expression: Expr, op: Comparison, value: unknown): Expr {
  if (value instanceof Expr || typeof value === "function") {
    throw new TypeError("Compare columns to scalar values")
  }
  const node = nodes.get(expression)
  if (node?.op !== "column") throw new TypeError("Only a col() reference can be compared to a scalar")
  const kind = scalarTypes.get(typeof value)
  if (kind === undefined) throw new ValidationError("Unsupported scalar type")
  return fromNode({ op, column: node.column, value: validateScalar(value, kind, node.column) })
}

function columnName(expression: Expr, method: string): string {
  const node = nodes.get(expression)
  if (node?.op !== "column") throw new TypeError(`Only a col() reference supports ${method}`)
  return node.column
}

function stringValue(value: unknown, column: string): string {
  if (typeof value !== "string") throw new ValidationError(`Column '${column}' requires string values`)
  return text(value, column)
}

function rankingOf(expression: Expr, op: Operation, query: unknown): Ranking {
  const column = columnName(expression, op)
  const ranking: Ranking = Object.create(Ranking.prototype)
  const value = op === "match" ? stringValue(query, column) : queryValue(query, column)
  rankings.set(ranking, { column, op, value })
  Object.freeze(ranking)
  return ranking
}

function combine(op: Logical, expressions: readonly Expr[]): Expr {
  const args = expressions.map((expression) => {
    const node = nodes.get(expression)
    if (node === undefined) throw new TypeError("Logical operators require another Expr")
    return node
  })
  return fromNode({ op, args })
}

/** An operation that ranks rows. Build one with col() and pass it to Query.rank(). */
export class Ranking {
  // Keeps other types from being accepted as rankings.
  declare private readonly kind: "ranking"

  constructor() {
    throw new TypeError("Construct rankings with col() and its ranking methods")
  }
}

/** An immutable comparison or logical predicate. Create one with col(). */
export class Expr {
  constructor() {
    throw new TypeError("Construct expressions with col() and comparison methods")
  }

  eq(value: Scalar): Expr { return compare(this, "eq", value) }
  ne(value: Scalar): Expr { return compare(this, "ne", value) }
  gt(value: Scalar): Expr { return compare(this, "gt", value) }
  ge(value: Scalar): Expr { return compare(this, "ge", value) }
  lt(value: Scalar): Expr { return compare(this, "lt", value) }
  le(value: Scalar): Expr { return compare(this, "le", value) }

  /** Rank rows by cosine similarity, highest first. */
  cosine(query: VectorValue | string): Ranking { return rankingOf(this, "cosine", query) }
  /** Rank rows by dot product, highest first. */
  dot(query: VectorValue | string): Ranking { return rankingOf(this, "dot", query) }
  /** Rank rows by Euclidean distance, lowest first. */
  l2(query: VectorValue | string): Ranking { return rankingOf(this, "l2", query) }
  /** Rank rows of a full-text column by BM25, highest first. */
  match(text: string): Ranking { return rankingOf(this, "match", text) }

  /** Keep rows whose list(String) column holds the value. */
  contains(value: string): Expr {
    const column = columnName(this, "contains")
    return fromNode({ op: "contains", column, value: stringValue(value, column) })
  }

  /** Keep rows whose list(String) column holds at least one of the values. */
  containsAny(values: Iterable<string>): Expr {
    const column = columnName(this, "contains_any")
    if (values == null || typeof values !== "object" || typeof values[Symbol.iterator] !== "function") {
      throw new ValidationError(`Column '${column}' requires a list of strings`)
    }
    const checked = Array.from(values, (value) => stringValue(value, column))
    // A Set keeps insertion order, but Python's set has none. Sort by UTF-8 bytes in both SDKs.
    // Other iterables, Map keys included, keep their order, as in Python.
    if (values instanceof Set) checked.sort((left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right)))
    return fromNode({ op: "contains_any", column, values: checked })
  }

  /** Keep rows whose String column begins with the prefix. */
  startsWith(prefix: string): Expr {
    const column = columnName(this, "starts_with")
    return fromNode({ op: "starts_with", column, value: stringValue(prefix, column) })
  }

  and(other: Expr): Expr { return combine("and", [this, other]) }
  or(other: Expr): Expr { return combine("or", [this, other]) }
  not(): Expr { return fromNode({ op: "not", arg: nodes.get(this)! }) }
}

export function col(name: string): Expr {
  return fromNode({ op: "column", column: validateIdentifier(name) })
}

export function andExpressions(expressions: readonly Expr[]): Expr {
  return combine("and", expressions)
}

export function expressionToWire(expression: Expr, schema: Schema): JsonObject {
  const node = nodes.get(expression)
  if (node === undefined) throw new ValidationError("Malformed predicate")
  return predicateToWire(node, schema, 1)
}

/** Check a ranking against its column and return JSON data. */
export function rankingToWire(ranking: Ranking, { schema, fullText, embed }: Layout): JsonObject {
  const state = rankings.get(ranking)
  if (state === undefined) throw new ValidationError("Malformed ranking")
  const { column, op, value } = state
  const type = schema[knownColumn(column, schema)]
  const supported = Object.hasOwn(fullText, column) ? ["match"] : operations.get(columnKind(type)) ?? []
  if (!supported.includes(op)) throw new ValidationError(`Column '${column}' does not support ${op}`)
  if (op === "match") return { column, op, value }
  if (typeof value === "string") {
    if (!Object.hasOwn(embed, column)) throw new ValidationError(`Column '${column}' has no model to embed text with`)
    return { column, op, value }
  }
  checkQuery(value, type, column)
  if (op === "cosine" && hasZeroVector(value)) {
    throw new ValidationError(`Column '${column}' requires nonzero query vectors for cosine`)
  }
  return { column, op, value: vectorToWire(value, type) }
}

function knownColumn(column: string, schema: Schema): string {
  if (!Object.hasOwn(schema, column)) throw new ValidationError(`Unknown column '${column}'`)
  return column
}

function predicateToWire(node: Node, schema: Schema, depth: number): JsonObject {
  if (depth > maxDepth) throw new ValidationError(`Predicate nesting exceeds ${maxDepth}`)
  switch (node.op) {
    case "column":
      throw new ValidationError("A filter must be a complete comparison or logical predicate")
    case "and":
    case "or":
      return { op: node.op, args: node.args.map((child) => predicateToWire(child, schema, depth + 1)) }
    case "not":
      return { op: node.op, arg: predicateToWire(node.arg, schema, depth + 1) }
    case "contains":
    case "contains_any":
    case "starts_with": {
      const { op, column } = node
      const type = schema[knownColumn(column, schema)]
      if (op === "starts_with" ? type !== String : columnKind(type) !== "tags") {
        throw new ValidationError(`Column '${column}' does not support ${op}`)
      }
      return op === "contains_any"
        ? { op, column, values: [...node.values] }
        : { op, column, value: node.value }
    }
    default: {
      const { op, column } = node
      const kind = schema[knownColumn(column, schema)]
      if (columnKind(kind) !== null) throw new ValidationError(`Column '${column}' does not support ${op}`)
      const value = validateScalar(node.value, kind as ScalarType, column)
      if (kind === Boolean && op !== "eq" && op !== "ne") {
        throw new ValidationError("Boolean columns support only eq and ne")
      }
      return { op, column, value: scalarToWire(value) }
    }
  }
}

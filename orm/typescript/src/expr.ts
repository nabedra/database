import { ValidationError } from "./errors.js"
import type { JsonObject } from "./transport.js"
import {
  scalarToWire, validateIdentifier, validateValue,
  type Scalar, type ScalarType, type Schema,
} from "./validation.js"

type Comparison = "eq" | "ne" | "gt" | "ge" | "lt" | "le"
type Logical = "and" | "or"
type Node =
  | { op: "column"; column: string }
  | { op: Comparison; column: string; value: Scalar }
  | { op: Logical; args: readonly Node[] }
  | { op: "not"; arg: Node }

const nodes = new WeakMap<Expr, Node>()
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
  return fromNode({ op, column: node.column, value: validateValue(value, kind, node.column) })
}

function combine(op: Logical, expressions: readonly Expr[]): Expr {
  const args = expressions.map((expression) => {
    const node = nodes.get(expression)
    if (node === undefined) throw new TypeError("Logical operators require another Expr")
    return node
  })
  return fromNode({ op, args })
}

/** An immutable scalar comparison or logical predicate. Create one with col(). */
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
    default: {
      const { op, column } = node
      if (!Object.hasOwn(schema, column)) throw new ValidationError(`Unknown column '${column}'`)
      const kind = schema[column]
      const value = validateValue(node.value, kind, column)
      if (kind === Boolean && op !== "eq" && op !== "ne") {
        throw new ValidationError("Boolean columns support only eq and ne")
      }
      return { op, column, value: scalarToWire(value) }
    }
  }
}

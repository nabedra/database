import type { RecordBatch } from "apache-arrow"
import { ProtocolError, ValidationError } from "./errors.js"
import { Expr, andExpressions, col, expressionToWire } from "./expr.js"
import {
  batchStream, streamSnapshot, type BatchStream, type StreamOptions, type StreamSnapshot,
} from "./stream.js"
import { tableTransport, type Table } from "./table.js"
import type { JsonObject } from "./transport.js"
import {
  decodeRow, isRecord, validateIdentifier, validateSchema,
  type Column, type Filters, type RowOf, type Scalar, type Schema,
} from "./validation.js"

type QueryLimit = number | bigint

interface QueryState {
  readonly table: { id: string }
  readonly schema: Schema
  readonly selection: string[] | null
  readonly predicate: Expr | null
  readonly order: { column: string; desc: boolean } | null
  readonly limit: QueryLimit | null
}

const states = new WeakMap<object, QueryState>()
const maxResultRows = 10_000

function stateOf(query: object): QueryState {
  const state = states.get(query)
  if (state === undefined) throw new TypeError("Expected a query")
  return state
}

function replace<S extends Schema, C extends Column<S>>(
  state: QueryState, changes: Partial<QueryState>,
): Query<S, C> {
  const query: Query<S, C> = Object.create(Query.prototype)
  states.set(query, { ...state, ...changes })
  Object.freeze(query)
  return query
}

function checkColumn(schema: Schema, column: string): void {
  validateIdentifier(column)
  if (!Object.hasOwn(schema, column)) throw new ValidationError(`Unknown column '${column}'`)
}

function toWire(state: QueryState): JsonObject {
  return {
    select: state.selection === null ? null : [...state.selection],
    filter: state.predicate === null ? null : expressionToWire(state.predicate, state.schema),
    order_by: state.order === null ? null : { ...state.order },
    limit: state.limit,
  }
}

function projectedSchema(state: QueryState): Schema {
  if (state.selection === null) return state.schema
  return Object.fromEntries(state.selection.map((column) => [column, state.schema[column]]))
}

/** An immutable query. Each terminal operation observes a fresh snapshot. */
export class Query<S extends Schema = Schema, C extends Column<S> = Column<S>> {
  constructor(table: Pick<Table<S>, "id" | "key" | "schema">) {
    states.set(this, {
      table,
      schema: validateSchema(table.schema, table.key),
      selection: null,
      predicate: null,
      order: null,
      limit: null,
    })
    Object.freeze(this)
  }

  select<P extends Column<S>>(...columns: P[]): Query<S, P> {
    if (columns.length === 0) throw new ValidationError("Select requires at least one column")
    const state = stateOf(this)
    for (const column of columns) checkColumn(state.schema, column)
    if (new Set(columns).size !== columns.length) {
      throw new ValidationError("Selected columns must be unique")
    }
    return replace(state, { selection: columns })
  }

  where(expr: Expr | Filters<S> | null = null, filters: Filters<S> = {}): Query<S, C> {
    const state = stateOf(this)
    if (!isRecord(filters)) throw new ValidationError("Filters must be a mapping")
    const predicates = state.predicate === null ? [] : [state.predicate]
    if (expr instanceof Expr) {
      predicates.push(expr)
    } else if (expr !== null) {
      if (!isRecord(expr) || Object.keys(filters).length > 0) {
        throw new ValidationError("Filter must be an Expr or a mapping")
      }
      filters = expr
    }
    for (const [column, value] of Object.entries(filters)) predicates.push(col(column).eq(value as Scalar))
    const predicate = predicates.length > 1 ? andExpressions(predicates) : predicates[0] ?? null
    if (predicate !== null) expressionToWire(predicate, state.schema)
    return replace(state, { predicate })
  }

  orderBy(column: Column<S>, desc = false): Query<S, C> {
    const state = stateOf(this)
    checkColumn(state.schema, column)
    if (typeof desc !== "boolean") throw new ValidationError("Order desc must be an exact bool")
    return replace(state, { order: { column, desc } })
  }

  limit(n: QueryLimit): Query<S, C> {
    if (!(typeof n === "bigint" ? n >= 0n : Number.isSafeInteger(n) && n >= 0)) {
      throw new ValidationError("Limit must be a nonnegative integer")
    }
    return replace(stateOf(this), { limit: n })
  }

  stream(options?: StreamOptions & { format?: "rows" }): BatchStream<Pick<RowOf<S>, C>[]>
  stream(options: StreamOptions & { format: "arrow" }): BatchStream<RecordBatch>
  stream(options: StreamOptions): BatchStream<Pick<RowOf<S>, C>[] | RecordBatch>
  stream(options: StreamOptions = {}): BatchStream<Pick<RowOf<S>, C>[] | RecordBatch> {
    const state = stateOf(this)
    return batchStream<Pick<RowOf<S>, C>>(
      tableTransport(state.table), state.table.id, toWire(state), projectedSchema(state),
      { ...options, limit: state.limit },
    )
  }

  streamSnapshot({ shards }: { shards: number }): StreamSnapshot {
    const state = stateOf(this)
    return streamSnapshot(tableTransport(state.table), state.table.id, toWire(state), shards)
  }

  collect(): Promise<Pick<RowOf<S>, C>[]> {
    const state = stateOf(this)
    const schema = projectedSchema(state)
    return tableTransport(state.table).request(
      "query.collect", { table_id: state.table.id, query: toWire(state) }, (data) => {
        if (!Array.isArray(data)) throw new ProtocolError("Expected a row list")
        if (data.length > maxResultRows || (state.limit !== null && data.length > state.limit)) {
          throw new ProtocolError("Row result exceeds query or response row limit")
        }
        return data.map((row) => decodeRow(row, schema)) as Pick<RowOf<S>, C>[]
      },
    )
  }

  count(): Promise<number> {
    const state = stateOf(this)
    return tableTransport(state.table).request(
      "query.count", { table_id: state.table.id, query: toWire(state) }, (data) => {
        if (typeof data !== "bigint" || data < 0n || data > Number.MAX_SAFE_INTEGER
          || (state.limit !== null && data > state.limit)) {
          throw new ProtocolError("Count must be a nonnegative integer within the query limit")
        }
        return Number(data)
      },
    )
  }

  explain(): Promise<string> {
    const state = stateOf(this)
    return tableTransport(state.table).request(
      "query.explain", { table_id: state.table.id, query: toWire(state) }, (data) => {
        if (typeof data !== "string") throw new ProtocolError("Expected query plan text")
        return data
      },
    )
  }
}

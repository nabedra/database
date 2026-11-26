import type { RecordBatch } from "apache-arrow"
import { LimitExceededError, ProtocolError, ValidationError } from "./errors.js"
import { Expr, Ranking, andExpressions, col, expressionToWire, rankingToWire } from "./expr.js"
import { buildLayout, rankedColumns, scoreColumn, tableLayout, type Layout } from "./layout.js"
import {
  batchStream, integer, streamSnapshot, type BatchStream, type StreamOptions, type StreamSnapshot,
} from "./stream.js"
import { tableTransport, type Table } from "./table.js"
import { checkOptions, floatToken, isJsonObject, type JsonObject } from "./transport.js"
import {
  decodeRow, isRecord, validateIdentifier, validateSchema,
  type Column, type Filters, type RankedColumn, type RowOf, type Scalar, type Schema,
} from "./validation.js"
import { columnKind } from "./vector.js"

type QueryLimit = number | bigint
type Scored = { _score: number }
const rankNames = ["fusion", "candidates", "recall", "exact", "consistency"]

export interface RankOptions {
  fusion?: "rrf" | readonly number[]
  candidates?: number
  recall?: number
  exact?: boolean
  consistency?: "latest" | "snapshot"
}

interface QueryState {
  readonly table: { id: string }
  readonly schema: Schema
  readonly layout: Layout
  readonly selection: string[] | null
  readonly predicate: Expr | null
  readonly order: { column: string; desc: boolean } | null
  readonly limit: QueryLimit | null
  readonly rank: JsonObject | null
  readonly rerank: JsonObject | null
}

const states = new WeakMap<object, QueryState>()
const maxResultRows = 10_000
const maxRankedRows = 1_000
const maxRankings = 256
const maxCandidates = 2 ** 32 - 1

function stateOf(query: object): QueryState {
  const state = states.get(query)
  if (state === undefined) throw new TypeError("Expected a query")
  return state
}

function replace<S extends Schema, C extends Column<S>, X = unknown>(
  state: QueryState, changes: Partial<QueryState>,
): Query<S, C, X> {
  const query: Query<S, C, X> = Object.create(Query.prototype)
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

function rankOptions(options: RankOptions, count: number): JsonObject {
  checkOptions(options, rankNames)
  const { fusion = "rrf", candidates = 100, recall = 0.95, exact = false, consistency = "latest" } = options
  const weighted = Array.isArray(fusion) && fusion.length === count
    && Array.from(fusion).every((weight) => typeof weight === "number" && Number.isFinite(weight) && weight >= 0)
  if (fusion !== "rrf" && !weighted) {
    throw new ValidationError("fusion must be 'rrf' or one nonnegative finite weight per ranking")
  }
  integer(candidates, 1, maxCandidates, "candidates")
  if (typeof recall !== "number" || !(recall > 0 && recall <= 1)) {
    throw new ValidationError("recall must be greater than 0 and at most 1")
  }
  if (typeof exact !== "boolean") throw new ValidationError("exact must be a bool")
  if (consistency !== "latest" && consistency !== "snapshot") {
    throw new ValidationError("consistency must be latest or snapshot")
  }
  return {
    fusion: fusion === "rrf" ? fusion : fusion.map(floatToken),
    candidates, recall: floatToken(recall), exact, consistency,
  }
}

function checkRanking(ranking: unknown, layout: Layout): JsonObject {
  if (!(ranking instanceof Ranking)) throw new ValidationError("Rank requires rankings")
  return rankingToWire(ranking, layout)
}

function checkRankable(state: QueryState): void {
  if (state.order !== null) throw new ValidationError("order_by cannot be combined with rank")
}

function checkUnranked(state: QueryState): void {
  if (state.rank !== null) throw new ValidationError("Ranked queries support only collect")
}

function rankedSchema(state: QueryState): Schema {
  const columns = state.selection ?? rankedColumns(state.layout)
  return Object.fromEntries(columns.map((column) => [column, state.schema[column]]))
}

function rankedWire(state: QueryState, schema: Schema): JsonObject {
  if (state.limit === null) throw new ValidationError("Ranked queries require a limit")
  if (state.limit > maxRankedRows) {
    throw new LimitExceededError(`Ranked queries return at most ${maxRankedRows} rows`)
  }
  const { filter, limit } = toWire(state)
  return { select: Object.keys(schema), filter, limit }
}

function rankedRows(data: unknown, schema: Schema, limit: QueryLimit): JsonObject[] {
  if (!Array.isArray(data) || data.length > limit) {
    throw new ProtocolError("Expected a row list within the query limit")
  }
  return data.map((row) => decodeRow(row, { ...schema, [scoreColumn]: Number }))
}

/** An immutable query. Each terminal operation observes a fresh snapshot. */
export class Query<S extends Schema = Schema, C extends Column<S> = Column<S>, X = unknown> {
  constructor(table: Pick<Table<S>, "id" | "key" | "schema">) {
    const layout = tableLayout(table) ?? buildLayout(validateSchema(table.schema, table.key))
    states.set(this, {
      table,
      schema: layout.schema,
      layout,
      selection: null,
      predicate: null,
      order: null,
      limit: null,
      rank: null,
      rerank: null,
    })
    Object.freeze(this)
  }

  select<P extends Column<S>>(...columns: P[]): Query<S, P, X> {
    if (columns.length === 0) throw new ValidationError("Select requires at least one column")
    const state = stateOf(this)
    for (const column of columns) checkColumn(state.schema, column)
    if (new Set(columns).size !== columns.length) {
      throw new ValidationError("Selected columns must be unique")
    }
    return replace(state, { selection: columns })
  }

  where(expr: Expr | Filters<S> | null = null, filters: Filters<S> = {}): Query<S, C, X> {
    const state = stateOf(this)
    if ((expr as unknown) instanceof Ranking) throw new ValidationError("A ranking cannot be used as a filter")
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

  orderBy(column: Column<S>, desc = false): Query<S, C, X> {
    const state = stateOf(this)
    checkColumn(state.schema, column)
    if (typeof desc !== "boolean") throw new ValidationError("Order desc must be an exact bool")
    if (columnKind(state.schema[column]) !== null) {
      throw new ValidationError(`Column '${column}' does not support order_by`)
    }
    if (state.rank !== null) throw new ValidationError("order_by cannot be combined with rank")
    return replace(state, { order: { column, desc } })
  }

  /** Order the matching rows by one operation, or by several fused operations. */
  rank(...rankings: Ranking[]): Query<S, RankedColumn<S, C>, Scored>
  rank(...rankings: [...Ranking[], RankOptions]): Query<S, RankedColumn<S, C>, Scored>
  rank(...values: (Ranking | RankOptions)[]): Query<S, RankedColumn<S, C>, Scored> {
    const state = stateOf(this)
    const last = values.at(-1)
    const given = last === null || isJsonObject(last)
    const options = given ? last as RankOptions : {}
    const rankings = given ? values.slice(0, -1) : values
    if (rankings.length === 0) throw new ValidationError("Rank requires at least one ranking")
    if (rankings.length > maxRankings) throw new LimitExceededError(`rank exceeds ${maxRankings} rankings`)
    const checked = rankOptions(options, rankings.length)
    const wire = rankings.map((ranking) => checkRanking(ranking, state.layout))
    checkRankable(state)
    return replace(state, { rank: { rankings: wire, ...checked } })
  }

  /** Score the best candidates of a ranked query again with another ranking. */
  rerank(ranking: Ranking, options?: { candidates?: number }): Query<S, C, X> {
    const { candidates = 200 } = checkOptions(options, ["candidates"])
    const state = stateOf(this)
    if (state.rank === null) throw new ValidationError("rerank requires rank")
    integer(candidates, 1, maxCandidates, "candidates")
    return replace(state, { rerank: { ranking: checkRanking(ranking, state.layout), candidates } })
  }

  /** Run one ranked query per ranking and return a result list for each, in order. */
  async rankMany(
    rankings: readonly Ranking[], options: RankOptions & { limit: QueryLimit },
  ): Promise<(Pick<RowOf<S>, RankedColumn<S, C>> & Scored)[][]> {
    const { limit, ...rankSettings } = checkOptions(options, ["limit", ...rankNames])
    const state = stateOf(this)
    if (state.rank !== null) throw new ValidationError("rank_many cannot be combined with rank")
    const checked = rankOptions(rankSettings, 1)
    if (!Array.isArray(rankings) || rankings.length === 0) {
      throw new ValidationError("rank_many requires a sequence of rankings")
    }
    if (rankings.length > maxRankings) throw new LimitExceededError(`rank_many exceeds ${maxRankings} rankings`)
    const batch = Array.from(rankings, (ranking) => checkRanking(ranking, state.layout))
    const limited = stateOf(this.limit(limit))
    checkRankable(limited)
    const schema = rankedSchema(limited)
    return tableTransport(state.table).request("query.rank_many", {
      table_id: state.table.id, query: rankedWire(limited, schema),
      rank: { rankings: [], ...checked, rerank: null }, batch,
    }, (data) => {
      if (!Array.isArray(data) || data.length !== batch.length) {
        throw new ProtocolError("Expected one row list for each ranking")
      }
      return data.map((rows) => rankedRows(rows, schema, limit)) as (Pick<RowOf<S>, RankedColumn<S, C>> & Scored)[][]
    })
  }

  limit(n: QueryLimit): Query<S, C, X> {
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
    checkUnranked(state)
    const schema = projectedSchema(state)
    if (Object.values(schema).some((type) => columnKind(type) !== null)) {
      throw new ValidationError("Streams support only scalar columns")
    }
    return batchStream<Pick<RowOf<S>, C>>(
      tableTransport(state.table), state.table.id, toWire(state), schema,
      { ...options, limit: state.limit },
    )
  }

  streamSnapshot({ shards }: { shards: number }): StreamSnapshot {
    const state = stateOf(this)
    checkUnranked(state)
    return streamSnapshot(tableTransport(state.table), state.table.id, toWire(state), shards)
  }

  async collect(): Promise<(Pick<RowOf<S>, C> & X)[]> {
    const state = stateOf(this)
    if (state.rank !== null) {
      const ranked = rankedSchema(state)
      return tableTransport(state.table).request("query.rank", {
        table_id: state.table.id, query: rankedWire(state, ranked), rank: { ...state.rank, rerank: state.rerank },
      }, (data) => rankedRows(data, ranked, state.limit!) as (Pick<RowOf<S>, C> & X)[])
    }
    const schema = projectedSchema(state)
    return tableTransport(state.table).request(
      "query.collect", { table_id: state.table.id, query: toWire(state) }, (data) => {
        if (!Array.isArray(data)) throw new ProtocolError("Expected a row list")
        if (data.length > maxResultRows || (state.limit !== null && data.length > state.limit)) {
          throw new ProtocolError("Row result exceeds query or response row limit")
        }
        return data.map((row) => decodeRow(row, schema)) as (Pick<RowOf<S>, C> & X)[]
      },
    )
  }

  async count(): Promise<number> {
    const state = stateOf(this)
    checkUnranked(state)
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

  async explain(): Promise<string> {
    const state = stateOf(this)
    checkUnranked(state)
    return tableTransport(state.table).request(
      "query.explain", { table_id: state.table.id, query: toWire(state) }, (data) => {
        if (typeof data !== "string") throw new ProtocolError("Expected query plan text")
        return data
      },
    )
  }
}

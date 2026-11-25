import { randomUUID } from "node:crypto"
import { setTimeout as delay } from "node:timers/promises"
import {
  AuthenticationError, BatchError, ConflictError, LimitExceededError, NabedraError, NotFoundError, OverloadedError,
  ProtocolError, SearchUnavailableError, StorageError, UnknownOutcomeError, ValidationError, type RowOutcome,
} from "./errors.js"
import type { Expr } from "./expr.js"
import {
  buildLayout, checkEmbed, extendLayout, hasOnlyOptions, hasSearch, layoutFromDescriptor, rowToWire,
  setTableLayout, tableLayout, writtenSchema, type EmbedOptions, type Layout,
} from "./layout.js"
import { Query } from "./query.js"
import {
  checkOptions, encodeJson, hasFields, isReceiptError, type JsonObject, type Transport,
} from "./transport.js"
import {
  decodeRow, decodeSchema, isRecord, validateFormat, validateIdentifier, validateKey, validateRow, validateSchema,
  type Column, type ColumnType, type Filters, type Key, type KeyOf, type RowInput, type RowOf, type Schema,
  type Update,
} from "./validation.js"
import { base64Overhead, columnKind, typeToWire, type List, type Vector } from "./vector.js"

export interface SearchStatus {
  state: "building" | "ready" | "failed"
  lagMs: number | null
  pendingEmbeddings: number
  failedEmbeddings: number
  recall: number | null
}

type Check = (item: unknown) => [Key, unknown]
type Chunk = Map<Key, unknown>

const maxRows = 10_000
const maxBody = 8 * 1024 * 1024
const maxRow = 1024 * 1024
// The class of a row's error follows its outcome, then its code. A not_applied row is never unknown.
const rowErrors = new Map<string, typeof NabedraError>([
  ["validation_error", ValidationError], ["unauthorized", AuthenticationError], ["not_found", NotFoundError],
  ["conflict", ConflictError], ["limit_exceeded", LimitExceededError], ["overloaded", OverloadedError],
  ["storage_unavailable", StorageError], ["search_unavailable", SearchUnavailableError],
])
const descriptorKeys = ["id", "name", "schema", "key", "format"]
const tableIdPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const transports = new WeakMap<object, Transport>()

function nullResult(data: unknown): void {
  if (data !== null) throw new ProtocolError("Expected a null result")
}

function boolResult(data: unknown): boolean {
  if (typeof data !== "boolean") throw new ProtocolError("Expected a boolean result")
  return data
}

function encode(value: unknown): Uint8Array {
  try {
    return encodeJson(value)
  } catch {
    throw new ValidationError("Row must encode as finite UTF-8 JSON")
  }
}

function layoutOf(table: object): Layout {
  return tableLayout(table)!
}

/** Check a row against the writable columns and return its wire form. */
function checkedRow(table: { key: string }, row: unknown, partial = false): JsonObject {
  const layout = layoutOf(table)
  if (isRecord(row)) {
    for (const column of Object.keys(layout.embed)) {
      if (Object.hasOwn(row, column)) {
        throw new ValidationError(`Column '${column}' is embedded and cannot be written`)
      }
    }
  }
  const wire = rowToWire(validateRow(row, writtenSchema(layout), table.key, partial), layout)
  if (hasSearch(layout)) {
    const overhead = Object.values(wire).reduce<number>((total, value) => total + base64Overhead(value), 0)
    if (encode(wire).byteLength - overhead > maxRow) throw new LimitExceededError("Row exceeds 1 MiB")
  }
  return wire
}

function count(value: unknown): value is bigint {
  return typeof value === "bigint" && value >= 0n
}

function statusResult(data: unknown): Record<string, SearchStatus> {
  if (!hasFields(data, "columns") || !isRecord(data.columns)) throw new ProtocolError("Invalid search status")
  return Object.fromEntries(Object.entries(data.columns).map(([column, status]) => {
    validateIdentifier(column)
    if (!hasFields(status, "state", "lag_ms", "pending_embeddings", "failed_embeddings", "recall")
        || (status.state !== "building" && status.state !== "ready" && status.state !== "failed")
        || (status.lag_ms !== null && !count(status.lag_ms))
        || !count(status.pending_embeddings) || !count(status.failed_embeddings)
        || (status.recall !== null && typeof status.recall !== "number")) {
      throw new ProtocolError("Invalid search status")
    }
    return [column, {
      state: status.state,
      lagMs: status.lag_ms === null ? null : Number(status.lag_ms),
      pendingEmbeddings: Number(status.pending_embeddings),
      failedEmbeddings: Number(status.failed_embeddings),
      recall: status.recall,
    }]
  }))
}

function rowOutcome(data: unknown, key: Key, requestId: string): RowOutcome {
  if (hasFields(data, "o") && data.o === "applied") return { key, outcome: "applied", error: null }
  if (!hasFields(data, "o", "error") || (data.o !== "not_applied" && data.o !== "outcome_unknown")) {
    throw new ProtocolError("Invalid row outcome")
  }
  const { o: outcome, error } = data
  const unknown = outcome === "outcome_unknown"
  if (!isReceiptError(error) || error.code.length === 0 || error.outcome !== (unknown ? "unknown" : "not_applied")) {
    throw new ProtocolError("Invalid row outcome")
  }
  const kind = unknown ? UnknownOutcomeError : rowErrors.get(error.code) ?? NabedraError
  return { key, outcome, error: new kind(error.message, { code: error.code, requestId, outcome: error.outcome }) }
}

async function* chunksOf(
  table: { id: string }, operation: string, field: "rows" | "keys", items: unknown, check: Check,
): AsyncGenerator<Chunk, undefined> {
  // The empty chunk already accounts for both brackets and the entire envelope.
  const base = encode({
    request_id: "0".repeat(36), operation, payload: { table_id: table.id, [field]: [] },
  }).byteLength
  const iterable = items as Iterable<unknown> & AsyncIterable<unknown> | null | undefined
  if (iterable == null || (typeof iterable[Symbol.iterator] !== "function"
      && typeof iterable[Symbol.asyncIterator] !== "function")) {
    throw new ValidationError(field === "rows" ? "Rows must be iterable" : "Keys must be iterable")
  }
  let chunk: Chunk = new Map()
  let size = base
  for await (const item of iterable) {
    const [key, checked] = check(item)
    const encoded = encode(checked).byteLength
    if (base + encoded > maxBody) throw new LimitExceededError("Encoded request exceeds 8 MiB")
    if (chunk.size === maxRows || size + encoded + Number(chunk.size > 0) > maxBody) {
      yield chunk
      chunk = new Map()
      size = base
    }
    if (chunk.has(key)) throw new ValidationError("A key appears more than once")
    size += encoded + Number(chunk.size > 0)
    chunk.set(key, checked)
  }
  if (chunk.size > 0) yield chunk
}

async function sendChunk(
  table: { id: string }, operation: string, field: "rows" | "keys", chunk: Chunk, before: number,
): Promise<number> {
  const requestId = randomUUID()
  const keys = [...chunk.keys()]
  let outcomes: RowOutcome[]
  let cause: unknown
  try {
    outcomes = await tableTransport(table).request(
      operation, { table_id: table.id, [field]: [...chunk.values()] }, (data) => {
        if (!hasFields(data, "outcomes") || !Array.isArray(data.outcomes) || data.outcomes.length !== keys.length) {
          throw new ProtocolError("Expected one outcome for each row")
        }
        return data.outcomes.map((item: unknown, index: number) => rowOutcome(item, keys[index], requestId))
      }, requestId,
    )
  } catch (error) {
    if (!(error instanceof NabedraError)) throw error
    cause = error
    const outcome = error.outcome === "not_applied" ? "not_applied" : "outcome_unknown"
    outcomes = keys.map((key) => ({ key, outcome, error }))
  }
  const rejected = outcomes.filter(({ outcome }) => outcome === "not_applied")
  const unknown = outcomes.filter(({ outcome }) => outcome === "outcome_unknown")
  const done = outcomes.length - rejected.length - unknown.length
  if (done === outcomes.length) return done
  const applied = before + done
  throw new BatchError(
    `Batch stopped after ${applied} applied rows: ${rejected.length} not applied, ${unknown.length} unknown`, {
      applied, outcomes, requestId, cause: cause ?? [...rejected, ...unknown][0].error,
      outcome: unknown.length > 0 ? "unknown" : applied > 0 ? "partial" : "not_applied",
    })
}

async function writeMany(
  table: { id: string }, operation: string, field: "rows" | "keys", items: unknown, check: Check,
): Promise<number> {
  const chunks = chunksOf(table, operation, field, items, check)
  let applied = 0
  for (;;) {
    let chunk: Chunk | undefined
    try {
      chunk = (await chunks.next()).value
    } catch (error) {
      if (applied === 0 || !(error instanceof Error)) throw error
      throw new BatchError(`Batch stopped after ${applied} applied rows: ${error.message}`,
        { applied, outcome: "partial", cause: error })
    }
    if (chunk === undefined) return applied
    try {
      applied += await sendChunk(table, operation, field, chunk, applied)
    } catch (error) {
      await chunks.return(undefined)
      throw error
    }
  }
}

function knownColumn(table: object, column: string): ColumnType {
  validateIdentifier(column)
  const { schema } = layoutOf(table)
  if (!Object.hasOwn(schema, column)) throw new ValidationError(`Unknown column '${column}'`)
  return schema[column]
}

/** A handle to one table ID and its original schema. */
export class Table<
  S extends Schema = Schema, K extends Column<S> = Column<S>, E extends Column<S> = never,
> {
  readonly id: string
  readonly name: string
  readonly key: K
  readonly format: "parquet"

  constructor(transport: Transport, descriptor: unknown) {
    if (!isRecord(descriptor) || descriptorKeys.some((key) => !Object.hasOwn(descriptor, key))
      || !hasOnlyOptions(Object.keys(descriptor).filter((key) => !descriptorKeys.includes(key)))) {
      throw new ProtocolError("Invalid table descriptor fields")
    }
    const id = descriptor.id
    if (typeof id !== "string" || id.length !== 36) {
      throw new ProtocolError("Table ID must be a hyphenated UUID string")
    }
    if (!tableIdPattern.test(id)) throw new ProtocolError("Invalid table UUID")
    this.id = id
    this.name = validateIdentifier(descriptor.name)
    this.key = validateIdentifier(descriptor.key) as K
    this.format = validateFormat(descriptor.format)
    const schema = validateSchema(decodeSchema(descriptor.schema, true), this.key)
    setTableLayout(this, layoutFromDescriptor(schema, descriptor))
    transports.set(this, transport)
    Object.freeze(this)
  }

  get schema(): S {
    return { ...layoutOf(this).schema } as S
  }

  insert(row: RowInput<S, E>): Promise<void> {
    const checked = checkedRow(this, row)
    return tableTransport(this).request("table.insert", { table_id: this.id, row: checked }, nullResult)
  }

  insertMany(rows: Iterable<RowInput<S, E>>): Promise<number> {
    const envelope = {
      request_id: "0".repeat(36),
      operation: "table.insert_many",
      payload: { table_id: this.id, rows: [] },
    }
    let size = encodeJson(envelope).byteLength
    const batch: JsonObject[] = []
    if (rows == null || typeof rows[Symbol.iterator] !== "function") {
      throw new ValidationError("Rows must be iterable")
    }
    for (const row of rows) {
      if (batch.length === maxRows) throw new LimitExceededError("Batch exceeds 10000 rows")
      const checked = checkedRow(this, row)
      size += encode(checked).byteLength + Number(batch.length > 0)
      if (size > maxBody) throw new LimitExceededError("Encoded request exceeds 8 MiB")
      batch.push(checked)
    }

    return tableTransport(this).request("table.insert_many", { table_id: this.id, rows: batch }, (data) => {
      if (data !== BigInt(batch.length)) throw new ProtocolError("Inserted count must match submitted batch")
      return batch.length
    })
  }

  get(key: KeyOf<S, K>): Promise<RowOf<S> | null> {
    const { schema } = layoutOf(this)
    const checked = validateKey(key, schema, this.key)
    return tableTransport(this).request("table.get", { table_id: this.id, key: checked }, (data) =>
      data === null ? null : decodeRow(data, schema) as RowOf<S>,
    )
  }

  update(key: KeyOf<S, K>, values: Update<S, K, E>): Promise<boolean> {
    const checkedKey = validateKey(key, layoutOf(this).schema, this.key)
    const checkedValues = checkedRow(this, values, true)
    return tableTransport(this).request(
      "table.update", { table_id: this.id, key: checkedKey, values: checkedValues }, boolResult,
    )
  }

  upsert(row: RowInput<S, E>): Promise<void> {
    const checked = checkedRow(this, row)
    return tableTransport(this).request("table.upsert", { table_id: this.id, row: checked }, nullResult)
  }

  delete(key: KeyOf<S, K>): Promise<boolean> {
    const checked = validateKey(key, layoutOf(this).schema, this.key)
    return tableTransport(this).request("table.delete", { table_id: this.id, key: checked }, boolResult)
  }

  /**
   * Write rows in bounded requests and return how many were applied. Each row is atomic and durable.
   * The call is not atomic across rows. A failure stops the call with BatchError, and nothing is sent again.
   */
  async upsertMany(
    rows: Iterable<RowInput<S, E>> | AsyncIterable<RowInput<S, E>>,
    options?: { waitForEmbedding?: boolean },
  ): Promise<number> {
    const { waitForEmbedding = false } = checkOptions(options, ["waitForEmbedding"])
    if (typeof waitForEmbedding !== "boolean") throw new ValidationError("wait_for_embedding must be a bool")
    if (waitForEmbedding && Object.keys(layoutOf(this).embed).length === 0) {
      throw new ValidationError("Waiting for embeddings requires an embedded column")
    }
    const applied = await writeMany(this, "table.upsert_many", "rows", rows, (row) => {
      const checked = checkedRow(this, row)
      return [checked[this.key] as Key, checked]
    })
    if (waitForEmbedding && applied > 0) await this.#waitForEmbedding(applied)
    return applied
  }

  /** Delete keys in bounded requests. A missing key counts as applied. */
  deleteMany(keys: Iterable<KeyOf<S, K>> | AsyncIterable<KeyOf<S, K>>): Promise<number> {
    return writeMany(this, "table.delete_many", "keys", keys, (key) => {
      const checked = validateKey(key, layoutOf(this).schema, this.key)
      return [checked, checked]
    })
  }

  async #waitForEmbedding(applied: number): Promise<void> {
    const deadline = performance.now() + tableTransport(this).timeout * 1000
    let pause = 50
    let cause: unknown
    for (let remaining = deadline - performance.now(); remaining > 0; remaining = deadline - performance.now()) {
      let status: Record<string, SearchStatus>
      try {
        status = await this.searchStatus()
      } catch (error) {
        if (!(error instanceof NabedraError)) throw error
        cause = error
        break
      }
      if (Object.keys(layoutOf(this).embed).every((column) => status[column]?.pendingEmbeddings === 0)) return
      await delay(Math.min(pause, remaining))
      pause = Math.min(pause * 2, 1000)
    }
    throw new BatchError(`${applied} rows were applied and their embeddings can still be pending`,
      { applied, outcome: "applied", cause })
  }

  /** Add a vector column online. This handle then uses the new schema. */
  async addColumn(
    name: string, type: Vector | List<Vector<false>>, settings?: { embed?: EmbedOptions | null },
  ): Promise<void> {
    const { embed = null } = checkOptions(settings, ["embed"])
    const options = embed === null ? null : checkEmbed(embed, layoutOf(this).schema)
    const layout = extendLayout(layoutOf(this), name, type, options)
    const added = typeToWire(layout.schema[name])
    await this.#change("table.add_column", {
      table_id: this.id, name, type: added, ...(options === null ? {} : { embed: options }),
    }, ({ schema, embed: embedded }) => {
      const received = Object.hasOwn(embedded, name) ? embedded[name] : null
      return Object.hasOwn(schema, name) && columnKind(schema[name]) !== null
        && JSON.stringify(typeToWire(schema[name])) === JSON.stringify(added)
        && received?.source === options?.source && received?.model === options?.model
    })
  }

  /** Drop a vector column. This handle then uses the new schema. */
  async dropColumn(name: string): Promise<void> {
    const kind = columnKind(knownColumn(this, name))
    if (kind === null || kind === "tags") throw new ValidationError(`Column '${name}' is not a vector column`)
    await this.#change("table.drop_column", { table_id: this.id, name }, ({ schema }) => !Object.hasOwn(schema, name))
  }

  /** Index a String column for match(), in the given language. */
  async addFullText(column: string, language: string): Promise<void> {
    knownColumn(this, column)
    const { schema, partitionBy, fullText, embed } = layoutOf(this)
    if (Object.hasOwn(fullText, column)) throw new ValidationError(`Column '${column}' already has full text`)
    buildLayout(schema, partitionBy, { ...fullText, [column]: language }, embed)
    await this.#change("table.add_full_text", { table_id: this.id, column, language },
      (changed) => Object.hasOwn(changed.fullText, column) && changed.fullText[column] === language)
  }

  /** Stop indexing a String column for match(). */
  async dropFullText(column: string): Promise<void> {
    knownColumn(this, column)
    if (!Object.hasOwn(layoutOf(this).fullText, column)) {
      throw new ValidationError(`Column '${column}' has no full text`)
    }
    await this.#change("table.drop_full_text", { table_id: this.id, column },
      ({ fullText }) => !Object.hasOwn(fullText, column))
  }

  async #change(operation: string, payload: JsonObject, applied: (layout: Layout) => boolean): Promise<void> {
    const layout = await tableTransport(this).request(operation, payload, (data) => {
      const table = new Table(tableTransport(this), data)
      if (table.id.toLowerCase() !== this.id.toLowerCase() || table.name !== this.name || table.key !== this.key
          || !applied(layoutOf(table))) {
        throw new ProtocolError("Changed descriptor does not match the request")
      }
      return layoutOf(table)
    })
    setTableLayout(this, layout)
  }

  /** Report state, lag, embedding counts and recall of each search column. */
  searchStatus(): Promise<Record<string, SearchStatus>> {
    return tableTransport(this).request("table.search_status", { table_id: this.id }, statusResult)
  }

  query(): Query<S> {
    return new Query(this)
  }

  where(expr: Expr | Filters<S> | null = null, filters: Filters<S> = {}): Query<S> {
    return this.query().where(expr, filters)
  }

  drop(): Promise<void> {
    return tableTransport(this).request("table.drop", { table_id: this.id }, nullResult)
  }
}

export function tableFromDescriptor<
  S extends Schema = Schema, K extends Column<S> = Column<S>, E extends Column<S> = never,
>(transport: Transport, descriptor: unknown): Table<S, K, E> {
  return new Table<S, K, E>(transport, descriptor)
}

export function tableTransport(table: object): Transport {
  const transport = transports.get(table)
  if (transport === undefined) throw new TypeError("Expected a table handle")
  return transport
}

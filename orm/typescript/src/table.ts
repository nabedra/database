import { LimitExceededError, ProtocolError, ValidationError } from "./errors.js"
import type { Expr } from "./expr.js"
import { Query } from "./query.js"
import { encodeJson, type JsonObject, type Transport } from "./transport.js"
import {
  isRecord, rowToWire, schemaFromWire, validateFormat, validateIdentifier,
  validateKey, validateRow, validateSchema,
  type Column, type Filters, type KeyOf, type RowOf, type Schema, type Update,
} from "./validation.js"

const maxRows = 10_000
const maxBody = 8 * 1024 * 1024
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

/** A handle to one table ID and its original schema. */
export class Table<S extends Schema = Schema, K extends Column<S> = Column<S>> {
  readonly id: string
  readonly name: string
  readonly key: K
  readonly format: "parquet"
  readonly #schema: S

  constructor(transport: Transport, descriptor: unknown) {
    if (!isRecord(descriptor) || Object.keys(descriptor).length !== descriptorKeys.length
      || descriptorKeys.some((key) => !Object.hasOwn(descriptor, key))) {
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
    this.#schema = Object.freeze(validateSchema(schemaFromWire(descriptor.schema), this.key)) as S
    transports.set(this, transport)
    Object.freeze(this)
  }

  get schema(): S {
    return { ...this.#schema }
  }

  insert(row: RowOf<S>): Promise<void> {
    const checked = rowToWire(validateRow(row, this.#schema, this.key))
    return tableTransport(this).request("table.insert", { table_id: this.id, row: checked }, nullResult)
  }

  insertMany(rows: Iterable<RowOf<S>>): Promise<number> {
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
      const checked = rowToWire(validateRow(row, this.#schema, this.key))
      let encoded: Uint8Array
      try {
        encoded = encodeJson(checked)
      } catch {
        throw new ValidationError("Row must encode as finite UTF-8 JSON")
      }
      size += encoded.byteLength + Number(batch.length > 0)
      if (size > maxBody) throw new LimitExceededError("Encoded request exceeds 8 MiB")
      batch.push(checked)
    }

    return tableTransport(this).request("table.insert_many", { table_id: this.id, rows: batch }, (data) => {
      if (data !== BigInt(batch.length)) throw new ProtocolError("Inserted count must match submitted batch")
      return batch.length
    })
  }

  get(key: KeyOf<S, K>): Promise<RowOf<S> | null> {
    const checked = validateKey(key, this.#schema, this.key)
    return tableTransport(this).request("table.get", { table_id: this.id, key: checked }, (data) =>
      data === null ? null : validateRow(data, this.#schema, this.key) as RowOf<S>,
    )
  }

  update(key: KeyOf<S, K>, values: Update<S, K>): Promise<boolean> {
    const checkedKey = validateKey(key, this.#schema, this.key)
    const checkedValues = rowToWire(validateRow(values, this.#schema, this.key, true))
    return tableTransport(this).request(
      "table.update", { table_id: this.id, key: checkedKey, values: checkedValues }, boolResult,
    )
  }

  upsert(row: RowOf<S>): Promise<void> {
    const checked = rowToWire(validateRow(row, this.#schema, this.key))
    return tableTransport(this).request("table.upsert", { table_id: this.id, row: checked }, nullResult)
  }

  delete(key: KeyOf<S, K>): Promise<boolean> {
    const checked = validateKey(key, this.#schema, this.key)
    return tableTransport(this).request("table.delete", { table_id: this.id, key: checked }, boolResult)
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

export function tableFromDescriptor<S extends Schema = Schema, K extends Column<S> = Column<S>>(
  transport: Transport, descriptor: unknown,
): Table<S, K> {
  return new Table<S, K>(transport, descriptor)
}

export function tableTransport(table: object): Transport {
  const transport = transports.get(table)
  if (transport === undefined) throw new TypeError("Expected a table handle")
  return transport
}

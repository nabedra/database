import { setTimeout as delay } from "node:timers/promises"
import { ProtocolError, TransportError, ValidationError } from "./errors.js"
import { tableFromDescriptor, type Table } from "./table.js"
import {
  floatToken,
  isJsonObject,
  Transport,
  validDuration,
  type JsonObject,
  type ReceiptError,
  type ReceiptState,
} from "./transport.js"
import {
  schemaToWire,
  validateFormat,
  validateIdentifier,
  validateSchema,
  type KeyColumn,
  type Schema,
} from "./validation.js"

export type { ReceiptError, ReceiptState } from "./transport.js"

export interface DatabaseOptions {
  url?: string | null
  token?: string | null
  mode?: "olap" | "oltp"
  timeout?: number
  caFile?: string | null
  database?: string | null
  apiUrl?: string | null
}

export interface SubmitOptions {
  requestId?: string | null
}

export interface RequestReceiptWaitOptions {
  timeout?: number
  pollInterval?: number
}

const terminalStates: ReadonlySet<ReceiptState> = new Set(["succeeded", "failed", "abandoned"])

function scalarToken(value: unknown): unknown {
  return typeof value === "number" ? floatToken(value) : value
}

function rowTokens(row: unknown): unknown {
  return isJsonObject(row) ? Object.fromEntries(Object.entries(row).map(([key, value]) => [key, scalarToken(value)])) : row
}

function predicateTokens(predicate: unknown, depth = 0): unknown {
  if (!isJsonObject(predicate) || depth >= 32) return predicate
  if (Object.hasOwn(predicate, "value")) return { ...predicate, value: scalarToken(predicate.value) }
  if (Array.isArray(predicate.args)) return { ...predicate, args: predicate.args.map(value => predicateTokens(value, depth + 1)) }
  if (Object.hasOwn(predicate, "arg")) return { ...predicate, arg: predicateTokens(predicate.arg, depth + 1) }
  return predicate
}

function submitPayload(operation: string, payload: JsonObject): JsonObject {
  if (!isJsonObject(payload)) return payload
  switch (operation) {
    case "table.insert":
    case "table.upsert":
      return Object.hasOwn(payload, "row") ? { ...payload, row: rowTokens(payload.row) } : payload
    case "table.insert_many":
      return Array.isArray(payload.rows) ? { ...payload, rows: payload.rows.map(rowTokens) } : payload
    case "table.update":
      return Object.hasOwn(payload, "values") ? { ...payload, values: rowTokens(payload.values) } : payload
    case "query.collect":
    case "query.count":
    case "query.explain":
      return isJsonObject(payload.query) && Object.hasOwn(payload.query, "filter")
        ? { ...payload, query: { ...payload.query, filter: predicateTokens(payload.query.filter) } }
        : payload
    default:
      return payload
  }
}

export class RequestReceipt {
  readonly #transport: Transport

  constructor(
    readonly requestId: string,
    readonly runId: string,
    readonly state: ReceiptState,
    readonly result: unknown,
    readonly error: ReceiptError | null,
    transport: Transport,
  ) {
    this.#transport = transport
    Object.freeze(this)
  }

  get terminal(): boolean {
    return terminalStates.has(this.state)
  }

  async poll(): Promise<RequestReceipt> {
    return this.#observe()
  }

  async wait({ timeout = 30, pollInterval = 0.05 }: RequestReceiptWaitOptions = {}): Promise<RequestReceipt> {
    if (!validDuration(timeout) || !validDuration(pollInterval)) {
      throw new ValidationError("Receipt wait durations must be positive and finite")
    }
    const deadline = performance.now() / 1000 + timeout
    const expired = () => new TransportError("Receipt wait timed out; work remains queued", {
      requestId: this.requestId,
    })
    let receipt: RequestReceipt = this
    while (!receipt.terminal) {
      const remaining = deadline - performance.now() / 1000
      if (remaining <= 0) throw expired()
      const waitMs = Math.min(2_147_483_647, Math.ceil(Math.min(pollInterval, remaining) * 1000))
      await delay(waitMs)
      const budget = deadline - performance.now() / 1000
      if (budget <= 0) throw expired()
      receipt = await this.#observe(budget)
    }
    return receipt
  }

  async #observe(timeout?: number): Promise<RequestReceipt> {
    const { requestId, runId, state, result, error } = await this.#transport.receipt(
      this.runId, this.requestId, timeout,
    )
    return new RequestReceipt(requestId, runId, state, result, error, this.#transport)
  }
}

export class Database {
  readonly #transport: Transport
  readonly #mode: "olap" | "oltp"

  constructor(options?: DatabaseOptions)
  constructor(url: string | null | undefined, token?: string | null, options?: Omit<DatabaseOptions, "url" | "token">)
  constructor(urlOrOptions?: DatabaseOptions | string | null, suppliedToken?: string | null,
    connection: Omit<DatabaseOptions, "url" | "token"> = {}) {
    const options = urlOrOptions !== null && typeof urlOrOptions === "object"
      ? urlOrOptions : { ...connection, url: urlOrOptions, token: suppliedToken }
    const { url, token, mode = "oltp", timeout, caFile, database, apiUrl } = options
    if (mode !== "olap" && mode !== "oltp") throw new ValidationError("Mode must be olap or oltp")
    if (apiUrl != null && url != null) throw new ValidationError("Supply url only once")
    this.#transport = new Transport(apiUrl ?? url, token ?? process.env.NABEDRA_TOKEN, {
      timeout, caFile, database,
    })
    this.#mode = mode
  }

  get mode(): "olap" | "oltp" {
    return this.#mode
  }

  async submit(operation: string, payload: JsonObject, options: SubmitOptions = {}): Promise<RequestReceipt> {
    const { requestId, runId, state, result, error } = await this.#transport.submit(operation, submitPayload(operation, payload), options.requestId)
    return new RequestReceipt(requestId, runId, state, result, error, this.#transport)
  }

  async receipt(runId: string, requestId: string): Promise<RequestReceipt> {
    const { state, result, error } = await this.#transport.receipt(runId, requestId)
    return new RequestReceipt(requestId, runId, state, result, error, this.#transport)
  }

  async createTable<S extends Schema, K extends KeyColumn<S>>(
    name: string,
    schema: S,
    key: K,
    { format = "parquet" }: { format?: "parquet" } = {},
  ): Promise<Table<S, K>> {
    name = validateIdentifier(name)
    validateIdentifier(key)
    const checked = validateSchema(schema, key)
    format = validateFormat(format)
    const wireSchema = schemaToWire(checked)
    const payload = { name, schema: wireSchema, key, format }
    return this.#transport.request("table.create", payload, (data) => {
      const table = tableFromDescriptor<S, K>(this.#transport, data)
      const received = schemaToWire(table.schema)
      if (table.name !== name || table.key !== key || table.format !== format
          || Object.keys(received).length !== Object.keys(wireSchema).length
          || Object.entries(wireSchema).some(([column, type]) => received[column] !== type)) {
        throw new ProtocolError("Created descriptor does not match submitted metadata")
      }
      return table
    })
  }

  async table(name: string): Promise<Table> {
    name = validateIdentifier(name)
    return this.#transport.request("table.describe", { name }, (data) => {
      const table = tableFromDescriptor(this.#transport, data)
      if (table.name !== name) throw new ProtocolError("Described table name does not match requested name")
      return table
    })
  }

  async tables(): Promise<Table[]> {
    return this.#transport.request("table.list", {}, (data) => {
      if (!Array.isArray(data)) throw new ProtocolError("Expected a table descriptor list")
      const tables = data.map((item: unknown) => tableFromDescriptor(this.#transport, item))
      const names = tables.map((table) => table.name)
      const ids = tables.map((table) => table.id.toLowerCase())
      if (names.some((name, index) => index > 0 && names[index - 1] >= name)
          || new Set(ids).size !== ids.length) {
        throw new ProtocolError("Table list must have sorted unique names and unique IDs")
      }
      return tables
    })
  }
}

export const DB = Database

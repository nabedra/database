import { setTimeout as sleep } from "node:timers/promises"
import type { Database, RequestReceipt } from "../client.js"
import { ConflictError, NotFoundError, OverloadedError, StorageError, TransportError, UnknownOutcomeError } from "../errors.js"
import type { Table } from "../table.js"
import type { Row, RowOf } from "../validation.js"
import type { Event } from "./client.js"
import { canonicalJson } from "./http.js"

const rowKey = "event_key"
const rowSchema = {
  event_key: String,
  partition: BigInt,
  offset: String,
  published_at: String,
  data: String,
} as const

type EventRow = RowOf<typeof rowSchema>
type Settlement = "pending" | "reconcile" | "committed"

export class SinkPaused extends Error {}
export class RetryLater extends Error {}

interface Pending {
  receipt: RequestReceipt
  rowCount: number
}

export interface Permits {
  acquire(): boolean
  release(): void
}

export async function waitOrStop(seconds: number, stop: AbortSignal): Promise<boolean> {
  try {
    await sleep(seconds * 1_000, undefined, { signal: stop })
    return stop.aborted
  } catch (error) {
    if (!stop.aborted) throw error
    return true
  }
}

function row(event: Event): EventRow {
  let data: string
  try {
    data = canonicalJson(event.data).toString("utf-8")
  } catch {
    throw new SinkPaused("Event contains JSON that cannot be written to OLAP")
  }
  return {
    event_key: [event.topicId, event.partition, event.eventId].join(":"),
    partition: BigInt(event.partition),
    offset: String(event.offset),
    published_at: event.publishedAt,
    data,
  }
}

function matches(expected: EventRow, existing: Row): boolean {
  const fields = Object.entries(expected)
  return Object.keys(existing).length === fields.length && fields.every(([key, value]) => existing[key] === value)
}

export class Writer {
  readonly #database: Database
  readonly #tableName: string
  readonly #tableId: string
  readonly #permits: Permits
  readonly #stop: AbortSignal
  readonly #receiptWait: number
  readonly #pollInterval: number
  readonly #pending = new Map<number, Pending>()
  readonly #needsReconciliation = new Set<number>()

  constructor(database: Database, tableName: string, tableId: string, permits: Permits, stop: AbortSignal,
    { receiptWait, pollInterval }: { receiptWait: number; pollInterval: number }) {
    this.#database = database
    this.#tableName = tableName
    this.#tableId = tableId
    this.#permits = permits
    this.#stop = stop
    this.#receiptWait = receiptWait
    this.#pollInterval = pollInterval
  }

  async destination(): Promise<Table> {
    let table
    try {
      table = await this.#database.table(this.#tableName)
    } catch (error) {
      if (error instanceof NotFoundError) throw new SinkPaused("Configured destination table is missing")
      throw error
    }
    if (table.id.toLowerCase() !== this.#tableId) throw new SinkPaused("Destination table UUID changed")
    const schema = table.schema
    if (table.key !== rowKey || Object.keys(schema).length !== Object.keys(rowSchema).length
      || Object.entries(rowSchema).some(([key, type]) => schema[key] !== type)) {
      throw new SinkPaused("Destination table key or schema changed")
    }
    return table
  }

  async #settle(partition: number, wait: boolean): Promise<Settlement> {
    const pending = this.#pending.get(partition)!
    let receipt: RequestReceipt | null
    try {
      receipt = wait
        ? await pending.receipt.wait({ timeout: this.#receiptWait, pollInterval: this.#pollInterval })
        : await pending.receipt.poll()
    } catch (error) {
      if (error instanceof TransportError || error instanceof OverloadedError || error instanceof StorageError) return "pending"
      if (!(error instanceof NotFoundError)) throw error
      receipt = null
    }
    if (receipt !== null && !receipt.terminal) return "pending"

    this.#pending.delete(partition)
    this.#permits.release()
    if (receipt === null || receipt.state === "abandoned") return "reconcile"
    if (receipt.state === "succeeded") {
      if (receipt.result !== pending.rowCount) throw new SinkPaused("Database receipt has an unexpected inserted count")
      return "committed"
    }
    const error = receipt.error
    if (error !== null) {
      if (error.code === "conflict" || error.outcome === "unknown") return "reconcile"
      if (error.code === "storage_unavailable" || error.code === "overloaded") throw new RetryLater()
      if (error.code === "not_found") await this.destination()
    }
    throw new SinkPaused("Database rejected the event batch")
  }

  async ready(partition: number): Promise<boolean> {
    return !this.#pending.has(partition) || await this.#settle(partition, false) !== "pending"
  }

  async #missing(expected: readonly EventRow[], checkLease: () => Promise<void>): Promise<EventRow[]> {
    const table = await this.destination()
    const missing: EventRow[] = []
    for (const row of expected) {
      await checkLease()
      let existing
      try {
        existing = await table.get(row[rowKey])
      } catch (error) {
        if (error instanceof NotFoundError) throw new SinkPaused("Destination table disappeared during reconciliation")
        throw error
      }
      if (existing === null) missing.push(row)
      else if (!matches(row, existing)) {
        throw new SinkPaused("Destination already has different data for this event identity")
      }
    }
    return missing
  }

  async commit(partition: number, events: readonly Event[], checkLease: () => Promise<void>): Promise<boolean> {
    const expected = events.map(row)
    let rows = this.#needsReconciliation.has(partition) ? await this.#missing(expected, checkLease) : expected
    for (let attempt = 0; attempt < 3; ++attempt) {
      if (!rows.length) return true
      if (!this.#permits.acquire()) return false
      let keepPermit = false
      try {
        const table = await this.destination()
        await checkLease()
        try {
          const receipt = await this.#database.submit("table.insert_many", { table_id: table.id, rows })
          this.#pending.set(partition, { receipt, rowCount: rows.length })
          this.#needsReconciliation.add(partition)
          keepPermit = true
        } catch (error) {
          if (error instanceof ConflictError || error instanceof UnknownOutcomeError) {
            this.#needsReconciliation.add(partition)
          } else if (error instanceof NotFoundError) {
            await this.destination()
            throw new SinkPaused("Database rejected the configured table UUID")
          } else {
            throw error
          }
        }
      } finally {
        if (!keepPermit) this.#permits.release()
      }

      if (keepPermit) {
        const current = await this.#settle(partition, true)
        if (current === "committed") return true
        if (current === "pending") return false
      }
      rows = await this.#missing(expected, checkLease)
      if (rows.length && await waitOrStop(this.#pollInterval, this.#stop)) return false
    }
    return !rows.length
  }

  acknowledged(partition: number): void {
    this.#needsReconciliation.delete(partition)
  }

  close(): void {
    for (const _pending of this.#pending) this.#permits.release()
    this.#pending.clear()
  }
}

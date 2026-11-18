import { randomUUID } from 'node:crypto'
import { createRequire } from 'node:module'

import type { RecordBatch } from 'apache-arrow'

import {
  LimitExceededError, NabedraError, ProtocolError, StreamUnsupportedError, ValidationError,
} from './errors.js'
import { StreamChannel, streamJson } from './streamTransport.js'
import { hasFields, type JsonObject, type Transport } from './transport.js'
import { decodeRow, type ScalarType, type Schema } from './validation.js'

const maxBatchRows = 10_000
const maxBatchBytes = 8 * 1024 * 1024
const maxU64 = (1n << 64n) - 1n
const maxU32 = 2 ** 32 - 1
const arrowContent = 'application/vnd.apache.arrow.stream'
const jsonContent = 'application/json'
const arrowEnd = Buffer.from([255, 255, 255, 255, 0, 0, 0, 0])
const snapshotPattern = /^[A-Za-z0-9][A-Za-z0-9._~-]{0,127}(?![\s\S])/
const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const require = createRequire(import.meta.url)

export interface StreamOptions {
  batchRows?: number
  prefetchBatches?: number
  format?: 'rows' | 'arrow'
  dropLast?: boolean
  snapshot?: string | null
  shard?: number
}

function integer(value: unknown, min: number, max: number, name: string): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > max) {
    throw new ValidationError(`${name} must be an integer between ${min} and ${max}`)
  }
  return value
}

function wireInteger(value: unknown, min: bigint, max: bigint): value is bigint {
  return typeof value === 'bigint' && value >= min && value <= max
}

function canonicalUuid(value: unknown): string {
  if (typeof value !== 'string' || value.length !== 36 || !uuidPattern.test(value)) {
    throw new ProtocolError('Expected a canonical stream UUID')
  }
  return value
}

function snapshotId(value: unknown): value is string {
  return typeof value === 'string' && snapshotPattern.test(value)
}

function responseObject(contentType: string, raw: Buffer): JsonObject {
  if (contentType !== jsonContent) throw new ProtocolError('Expected application/json')
  return streamJson(raw, 'bigint')
}

class StreamLease {
  readonly #transport: Transport
  readonly #route: string
  readonly #idField: 'stream_id' | 'snapshot_id'
  readonly #parameters: JsonObject
  #pending: Promise<unknown> = Promise.resolve()
  #started = false
  closed = false
  channel: StreamChannel | null = null
  id: string | null = null
  #runId: string | null = null

  constructor(transport: Transport, route: string, idField: 'stream_id' | 'snapshot_id', parameters: JsonObject) {
    this.#transport = transport
    this.#route = route
    this.#idField = idField
    this.#parameters = parameters
  }

  run<T>(action: () => Promise<T>): Promise<T> {
    const result = this.#pending.then(action)
    this.#pending = result.then(() => undefined, () => undefined)
    return result
  }

  async open(): Promise<number> {
    if (this.closed || this.#started) throw new ValidationError('Stream handles can only be opened once')
    this.#started = true
    try {
      return await this.#open()
    } catch (error) {
      await this.close()
      throw error
    }
  }

  async #open(): Promise<number> {
    const channel = new StreamChannel(this.#transport)
    this.channel = channel
    const capabilityReply = await channel.exchange('GET', '/v1/streams/capabilities', {
      expected: [200], capabilities: true,
    })
    const { version, run_id: rawRunId, max_batch_rows: batchRows, max_batch_bytes: batchBytes,
      max_prefetch_batches: prefetch, ...extra } = responseObject(capabilityReply.contentType, capabilityReply.raw)
    if (version !== 1n) {
      if (typeof version === 'bigint' && version > 1n) {
        throw new StreamUnsupportedError('Server stream protocol version is unsupported')
      }
      throw new ProtocolError('Invalid stream protocol version')
    }
    if (Object.keys(extra).length !== 0 || rawRunId === undefined ||
        batchRows === undefined || batchBytes === undefined || prefetch === undefined) {
      throw new ProtocolError('Invalid streaming capabilities')
    }
    const runId = canonicalUuid(rawRunId)
    if (!wireInteger(batchRows, 1n, maxU64) || !wireInteger(batchBytes, 1n, maxU64) ||
        !wireInteger(prefetch, 0n, 2n)) {
      throw new ProtocolError('Invalid streaming capability limits')
    }
    const requestedRows = this.#parameters.batch_rows
    if (typeof requestedRows === 'number' && requestedRows > batchRows) {
      throw new LimitExceededError('batch_rows exceeds the server stream limit')
    }

    const requestId = randomUUID()
    const openReply = await channel.exchange('POST', this.#route, {
      expected: [201], requestId,
      value: { ...this.#parameters, request_id: requestId, expected_run_id: runId },
    })
    const opened = responseObject(openReply.contentType, openReply.raw)
    if (canonicalUuid(opened.run_id) !== runId) throw new ProtocolError('Stream open returned a different run ID')
    const id = opened[this.#idField]
    if (this.#idField === 'stream_id') {
      this.id = canonicalUuid(id)
    } else if (snapshotId(id)) {
      this.id = id
    } else {
      throw new ProtocolError('Invalid stream snapshot ID')
    }
    this.#runId = runId
    const fields = [this.#idField, 'run_id', 'expires_at']
    if (this.#idField === 'stream_id') {
      fields.push('prefetch_batches')
      const maxPrefetch = Math.min(Number(prefetch), Number(this.#parameters.prefetch_batches))
      if (!wireInteger(opened.prefetch_batches, 0n, BigInt(maxPrefetch))) {
        throw new ProtocolError('Invalid negotiated prefetch depth')
      }
    }
    if (!hasFields(opened, ...fields) || !wireInteger(opened.expires_at, 1n, maxU64)) {
      throw new ProtocolError('Invalid stream open response')
    }
    return batchBytes > maxBatchBytes ? maxBatchBytes : Number(batchBytes)
  }

  path(): string {
    return `${this.#route}/${this.#runId}/${this.id}`
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    const channel = this.channel
    this.channel = null
    if (!channel) return
    try {
      if (this.id !== null) {
        await channel.exchange('DELETE', this.path(), { expected: [204], cleanup: true })
      }
    } catch (error) {
      if (!(error instanceof NabedraError)) throw error
      // The server expires the lease if it cannot receive cleanup.
    } finally {
      channel.close()
    }
  }
}

type BatchDecoder<T> = (raw: Buffer) => [T, number] | Promise<[T, number]>

interface BatchOptions<T> {
  batchRows: number
  dropLast: boolean
  limit: number | bigint | null
  contentType: string
  decode: BatchDecoder<T>
}

export class BatchStream<T> implements AsyncIterableIterator<T, undefined>, AsyncDisposable {
  readonly #lease: StreamLease
  readonly #options: BatchOptions<T>
  #nextSequence = 0n
  #deliveredRows = 0n
  #shortTail = false
  #maxBytes = 0

  constructor(lease: StreamLease, options: BatchOptions<T>) {
    this.#lease = lease
    this.#options = options
  }

  async open(): Promise<this> {
    return this.#lease.run(async () => {
      this.#maxBytes = await this.#lease.open()
      return this
    })
  }

  [Symbol.asyncIterator](): this {
    return this
  }

  next(): Promise<IteratorResult<T, undefined>> {
    return this.#lease.run(async () => {
      if (this.#lease.closed) return { done: true, value: undefined }
      if (this.#lease.channel === null) this.#maxBytes = await this.#lease.open()
      try {
        if (this.#nextSequence > maxU64) throw new ProtocolError('Stream sequence exceeds its wire limit')
        const channel = this.#lease.channel!
        const { status, contentType, raw } = await channel.exchange(
          'POST', `${this.#lease.path()}/batches/${this.#nextSequence}`,
          { expected: [200, 204], maxBytes: this.#maxBytes },
        )
        if (status === 204) {
          await this.#lease.close()
          return { done: true, value: undefined }
        }
        if (contentType !== this.#options.contentType) throw new ProtocolError('Unexpected stream batch content type')
        if (this.#shortTail) throw new ProtocolError('Stream produced a batch after its short tail')
        const { batchRows, dropLast, limit, decode } = this.#options
        const [batch, rows] = await decode(raw)
        const deliveredRows = this.#deliveredRows + BigInt(rows)
        if (rows <= 0 || rows > batchRows || (dropLast && rows !== batchRows) ||
            (limit !== null && deliveredRows > limit)) {
          throw new ProtocolError('Stream batch violates its row limit')
        }
        this.#nextSequence += 1n
        this.#deliveredRows = deliveredRows
        this.#shortTail = rows < batchRows
        return { done: false, value: batch }
      } catch (error) {
        await this.#lease.close()
        throw error
      }
    })
  }

  async return(): Promise<IteratorResult<T, undefined>> {
    await this.close()
    return { done: true, value: undefined }
  }

  close(): Promise<void> {
    return this.#lease.run(() => this.#lease.close())
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close()
  }
}

export class StreamSnapshot implements AsyncDisposable {
  readonly #lease: StreamLease

  constructor(lease: StreamLease) {
    this.#lease = lease
  }

  get id(): string {
    if (this.#lease.id === null) throw new ValidationError('Open the snapshot before accessing its ID')
    return this.#lease.id
  }

  async open(): Promise<this> {
    return this.#lease.run(async () => {
      await this.#lease.open()
      return this
    })
  }

  close(): Promise<void> {
    return this.#lease.run(() => this.#lease.close())
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close()
  }
}

function rowsDecoder<R>(schema: Schema, batchRows: number): BatchDecoder<R[]> {
  return raw => {
    const result = streamJson(raw, 'bigint')
    const rows = result.rows
    if (!hasFields(result, 'rows') || !Array.isArray(rows)) throw new ProtocolError('Expected a rows batch')
    if (rows.length === 0 || rows.length > batchRows) throw new ProtocolError('Invalid stream batch row count')
    try {
      return [rows.map(row => decodeRow(row, schema) as R), rows.length]
    } catch (error) {
      if (!(error instanceof ValidationError)) throw error
      throw new ProtocolError('Invalid stream row scalar')
    }
  }
}

function arrowDecoder(schema: Schema, batchRows: number): BatchDecoder<RecordBatch> {
  try {
    require.resolve('apache-arrow')
  } catch {
    throw new StreamUnsupportedError('Arrow streaming requires the apache-arrow package')
  }
  const columns = Object.entries(schema)
  return async raw => {
    try {
      const arrow = await import('apache-arrow')
      if (!raw.subarray(-arrowEnd.length).equals(arrowEnd)) {
        throw new ProtocolError('Arrow stream batch is missing its end marker')
      }
      validateArrowMessages(raw, arrow)
      const table = arrow.tableFromIPC(raw)
      const fields = table.schema.fields
      const types = new Map<ScalarType, string>([
        [String, new arrow.Utf8().toString()],
        [BigInt, new arrow.Int64().toString()],
        [Number, new arrow.Float64().toString()],
        [Boolean, new arrow.Bool().toString()],
      ])
      if (fields.length !== columns.length ||
          fields.some((field, index) =>
            field.name !== columns[index][0] || field.nullable || field.type.toString() !== types.get(columns[index][1]))) {
        throw new ProtocolError('Arrow batch does not match the projected schema')
      }
      if (table.batches.length !== 1) throw new ProtocolError('Arrow stream must contain exactly one record batch')
      const [batch] = table.batches
      if (batch.numRows <= 0 || batch.numRows > batchRows) throw new ProtocolError('Invalid Arrow batch row count')
      for (const [index, [, kind]] of columns.entries()) {
        validateArrowColumn(batch, index, kind)
      }
      return [batch, batch.numRows]
    } catch (error) {
      if (error instanceof ProtocolError) throw error
      throw new ProtocolError('Malformed Arrow stream batch')
    }
  }
}

function validateArrowMessages(raw: Buffer, arrow: typeof import('apache-arrow')): void {
  let offset = 0
  const endOffset = raw.length - arrowEnd.length
  for (const headerType of [arrow.MessageHeader.Schema, arrow.MessageHeader.RecordBatch]) {
    let length = raw.readInt32LE(offset)
    offset += 4
    if (length === -1) {
      length = raw.readInt32LE(offset)
      offset += 4
    }
    const metadataEnd = offset + length
    if (length <= 0 || metadataEnd > endOffset) throw new ProtocolError('Malformed Arrow stream batch')
    const message = arrow.Message.decode(raw.subarray(offset, metadataEnd))
    if (message.headerType !== headerType || message.bodyLength < 0 || message.bodyLength > endOffset - metadataEnd) {
      throw new ProtocolError('Arrow stream must contain one schema and one record batch')
    }
    offset = metadataEnd + message.bodyLength
  }
  if (offset !== endOffset) {
    throw new ProtocolError('Unexpected bytes after the Arrow batch')
  }
}

function validateArrowColumn(batch: RecordBatch, index: number, kind: Schema[string]): void {
  const data = batch.data.children[index]
  const length = batch.numRows
  if (!data || data.length !== length || data.nullCount !== 0) {
    throw new ProtocolError('Arrow batch contains invalid or null stream values')
  }
  const values = data.values
  if (kind === String) {
    const offsets = data.valueOffsets
    if (!(values instanceof Uint8Array) || !(offsets instanceof Int32Array) ||
        offsets.length < length + 1 || offsets[0] < 0) {
      throw new ProtocolError('Malformed Arrow UTF-8 buffers')
    }
    const utf8 = new TextDecoder('utf-8', { fatal: true })
    for (let i = 0; i < length; i += 1) {
      if (offsets[i + 1] < offsets[i] || offsets[i + 1] > values.length) {
        throw new ProtocolError('Malformed Arrow UTF-8 offsets')
      }
      utf8.decode(values.subarray(offsets[i], offsets[i + 1]))
    }
  } else if (kind === Boolean) {
    if (!(values instanceof Uint8Array) || values.length < Math.ceil(length / 8)) {
      throw new ProtocolError('Malformed Arrow boolean buffers')
    }
  } else if (kind === Number) {
    if (!(values instanceof Float64Array) || values.length < length) {
      throw new ProtocolError('Malformed Arrow float64 buffers')
    }
    if (!values.subarray(0, length).every(Number.isFinite)) {
      throw new ProtocolError('Arrow batch contains nonfinite stream values')
    }
  } else if (!(values instanceof BigInt64Array) || values.length < length) {
    throw new ProtocolError('Malformed Arrow int64 buffers')
  }
}

export function batchStream<R>(
  transport: Transport, tableId: string, query: JsonObject, schema: Schema,
  { batchRows = 64, prefetchBatches = 1, format = 'rows', dropLast = false,
    snapshot = null, shard = 0, limit }: StreamOptions & { limit: number | bigint | null },
): BatchStream<R[] | RecordBatch> {
  integer(batchRows, 1, maxBatchRows, 'batchRows')
  integer(prefetchBatches, 0, 2, 'prefetchBatches')
  integer(shard, 0, maxU32, 'shard')
  if (format !== 'rows' && format !== 'arrow') throw new ValidationError('Stream format must be rows or arrow')
  if (typeof dropLast !== 'boolean') throw new ValidationError('dropLast must be a bool')
  if (snapshot === null) {
    if (shard !== 0) throw new ValidationError('A nonzero shard requires a shared snapshot ID')
  } else if (!snapshotId(snapshot)) {
    throw new ValidationError('Invalid stream snapshot ID')
  }

  const lease = new StreamLease(transport, '/v1/streams', 'stream_id', {
    table_id: tableId, query, batch_rows: batchRows, prefetch_batches: prefetchBatches,
    format, drop_last: dropLast, snapshot_id: snapshot, shard,
  })
  return new BatchStream<R[] | RecordBatch>(lease, {
    batchRows, dropLast, limit,
    contentType: format === 'arrow' ? arrowContent : jsonContent,
    decode: format === 'arrow' ? arrowDecoder(schema, batchRows) : rowsDecoder<R>(schema, batchRows),
  })
}

export function streamSnapshot(transport: Transport, tableId: string, query: JsonObject, shards: number): StreamSnapshot {
  integer(shards, 1, maxU32, 'shards')
  return new StreamSnapshot(new StreamLease(transport, '/v1/stream-snapshots', 'snapshot_id', {
    table_id: tableId, query, shards,
  }))
}

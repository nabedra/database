import type { IncomingMessage } from 'node:http'
import { setTimeout as sleep } from 'node:timers/promises'

import {
  LimitExceededError, NabedraError, OverloadedError, ProtocolError, StorageError,
  StreamAbandonedError, StreamExpiredError,
  StreamUnsupportedError, TransportError, UnknownOutcomeError, ValidationError,
} from './errors.js'
import {
  encodeJson, errorMappings, hasFields, isJsonObject, parseJson, type JsonObject, type Transport,
} from './transport.js'

const requestBytes = 128 * 1024
const controlBytes = 64 * 1024
const attempts = 14
const cleanupTimeout = 1
const initialBackoff = 0.25
const maxBackoff = 2

const streamErrors = new Map(errorMappings)
streamErrors.set('unsupported_operation', { status: 400, type: StreamUnsupportedError })
streamErrors.set('stream_expired', { status: 410, type: StreamExpiredError })
streamErrors.set('stream_abandoned', { status: 410, type: StreamAbandonedError })

export function streamJson(raw: Uint8Array, integers?: 'bigint'): JsonObject {
  let value: unknown
  try {
    value = parseJson(raw, { integers })
  } catch {
    throw new ProtocolError('Malformed stream JSON')
  }
  if (!isJsonObject(value)) throw new ProtocolError('Expected a stream JSON object')
  return value
}

interface StreamExchangeOptions {
  expected: readonly number[]
  value?: JsonObject
  requestId?: string
  maxBytes?: number
  capabilities?: boolean
  cleanup?: boolean
}

export class StreamChannel {
  readonly #transport: Transport
  readonly #agent
  readonly #headers = {
    Accept: 'application/json, application/vnd.apache.arrow.stream',
    'Accept-Encoding': 'identity',
    'Cache-Control': 'no-store',
  }

  constructor(transport: Transport) {
    this.#transport = transport
    this.#agent = transport.createAgent()
  }

  close(): void {
    this.#agent.destroy()
  }

  async exchange(
    method: string, route: string,
    { expected, value, requestId, maxBytes = controlBytes, capabilities, cleanup }: StreamExchangeOptions,
  ): Promise<{ status: number; contentType: string; raw: Buffer }> {
    let body: Buffer | undefined
    let headers: Record<string, string> = this.#headers
    if (value !== undefined) {
      try {
        body = encodeJson(value)
      } catch {
        throw new ValidationError('Stream request must encode as finite UTF-8 JSON')
      }
      if (body.length > requestBytes) throw new LimitExceededError('Encoded stream request exceeds 128 KiB')
      headers = { ...headers, 'Content-Type': 'application/json' }
    }

    const attemptLimit = cleanup ? 1 : attempts
    const timeout = cleanup ? Math.min(this.#transport.timeout, cleanupTimeout) : this.#transport.timeout
    const deadline = performance.now() / 1000 + timeout
    let remaining = timeout
    let backoff = initialBackoff
    for (let attempt = 1; ; attempt += 1) {
      let hint = 0
      try {
        const { status, headers: responseHeaders, body: raw } = await this.#transport.exchange(method, route, {
          body, headers, timeout: remaining, agent: this.#agent,
          readResponse: response => {
            const accepted = expected.includes(response.statusCode ?? 0)
            return readBody(response, accepted ? maxBytes : controlBytes, accepted, requestId)
          },
        })
        const contentType = responseHeaders['content-type']?.split(';', 1)[0].trim().toLowerCase() ?? ''
        if (expected.includes(status)) return { status, contentType, raw }
        if (capabilities && status === 404 && !hasErrorEnvelope(contentType, raw)) {
          throw new StreamUnsupportedError('Server does not expose OLAP streaming capabilities', { statusCode: status })
        }
        const retryAfter = responseHeaders['retry-after']
        if (typeof retryAfter === 'string' && /^[0-9]{1,10}$/.test(retryAfter)) {
          hint = Math.min(Number(retryAfter), maxBackoff)
        }
        raiseResponseError(status, contentType, raw, requestId)
      } catch (error) {
        if (error instanceof ProtocolError) this.close()
        if (!(error instanceof TransportError || error instanceof OverloadedError || error instanceof StorageError)) {
          throw error
        }
        if (error instanceof TransportError) this.close()
        const delay = Math.max(hint, backoff)
        if (attempt >= attemptLimit || delay >= deadline - performance.now() / 1000) throw error
        await sleep(delay * 1000)
        remaining = deadline - performance.now() / 1000
        if (remaining <= 0) throw error
        backoff = Math.min(2 * backoff, maxBackoff)
      }
    }
  }
}

async function readBody(
  response: IncomingMessage, maxBytes: number, requireFrame: boolean, requestId?: string,
): Promise<Buffer> {
  const statusCode = response.statusCode
  const headers = response.headersDistinct
  const lengths = headers['content-length'] ?? []
  const transfers = headers['transfer-encoding'] ?? []
  const encodings = headers['content-encoding'] ?? []
  if ((headers['content-type']?.length ?? 0) > 1) {
    throw new ProtocolError('Duplicate stream response Content-Type', { requestId, statusCode })
  }
  if (lengths.length > 1 ||
      (transfers.length > 0 && (lengths.length > 0 || transfers.length !== 1 || transfers[0].toLowerCase() !== 'chunked')) ||
      (encodings.length > 0 && (encodings.length !== 1 || encodings[0].toLowerCase() !== 'identity'))) {
    throw new ProtocolError('Invalid stream response framing', { requestId, statusCode })
  }

  let length: bigint | undefined
  if (lengths.length) {
    if (!/^[0-9]{1,20}$/.test(lengths[0])) {
      throw new ProtocolError('Invalid stream response Content-Length', { requestId, statusCode })
    }
    length = BigInt(lengths[0])
  }
  if (statusCode === 204) {
    if ((length !== undefined && length !== 0n) || transfers.length > 0) {
      throw new ProtocolError('A stream EOF response must have an empty body', { requestId, statusCode })
    }
  } else {
    if (length !== undefined && length > maxBytes) {
      throw new ProtocolError('Stream response exceeds its byte limit', { requestId, statusCode })
    }
    if (requireFrame && length === undefined && transfers.length === 0) {
      throw new ProtocolError('Stream response requires a Content-Length or chunk framing', { requestId, statusCode })
    }
  }

  const chunks: Buffer[] = []
  let total = 0
  try {
    for await (const chunk of response) {
      const bytes = chunk as Buffer
      total += bytes.length
      if (total > maxBytes) throw new ProtocolError('Stream response exceeds its byte limit', { requestId, statusCode })
      chunks.push(bytes)
    }
  } catch (error) {
    if (error instanceof ProtocolError) throw error
    throw new TransportError('Lost stream response during HTTP I/O', { requestId, statusCode })
  }
  if (!response.complete || (length !== undefined && length !== BigInt(total))) {
    throw new TransportError('Lost stream response during HTTP I/O', { requestId, statusCode })
  }
  if (transfers.length) {
    let trailerBytes = 2
    for (let i = 0; i < response.rawTrailers.length; i += 2) {
      trailerBytes += Buffer.byteLength(response.rawTrailers[i], 'latin1') +
        Buffer.byteLength(response.rawTrailers[i + 1], 'latin1') + 3
    }
    if (trailerBytes > 8192) {
      throw new ProtocolError('Stream response trailers exceed 8 KiB', { requestId, statusCode })
    }
  }
  return Buffer.concat(chunks, total)
}

function hasErrorEnvelope(contentType: string, raw: Buffer): boolean {
  if (contentType !== 'application/json') return false
  try {
    return hasFields(streamJson(raw), 'request_id', 'error')
  } catch {
    return false
  }
}

function raiseResponseError(status: number, contentType: string, raw: Buffer, requestId?: string): never {
  const malformed = (message: string) => new ProtocolError(message, { requestId, statusCode: status })
  if (status < 400 || status > 599 || contentType !== 'application/json') {
    throw malformed('Unexpected stream response status or content type')
  }

  let envelope: JsonObject
  try {
    envelope = streamJson(raw)
  } catch (error) {
    throw malformed(error instanceof ProtocolError ? error.message : 'Malformed stream JSON')
  }
  const envelopeId = envelope.request_id
  if (!hasFields(envelope, 'request_id', 'error') ||
      (envelopeId !== null && envelopeId !== requestId)) {
    throw malformed('Invalid stream error envelope')
  }
  const error = envelope.error
  if (!hasFields(error, 'code', 'message', 'outcome') ||
      typeof error.code !== 'string' || !error.code || typeof error.message !== 'string' ||
      (error.outcome !== 'not_applied' && error.outcome !== 'unknown')) {
    throw malformed('Invalid stream error details')
  }
  const { code, message, outcome } = error
  const { status: expectedStatus, type: ErrorType } = streamErrors.get(code) ?? { status: 500, type: NabedraError }
  if (expectedStatus !== status || (code === 'outcome_unknown' && outcome !== 'unknown')) {
    throw malformed('Stream error code/status/outcome mismatch')
  }
  if ((code === 'stream_expired' || code === 'stream_abandoned') && outcome !== 'not_applied') {
    throw malformed('Invalid stream lease error outcome')
  }
  const ErrorClass = outcome === 'unknown' ? UnknownOutcomeError : ErrorType
  throw new ErrorClass(message, { code, outcome, requestId, statusCode: status })
}

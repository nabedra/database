import { randomUUID } from "node:crypto"
import { inspect } from "node:util"
import { hasFields, type JsonObject } from "../transport.js"
import {
  EventsError, EventsHttp, EventsProtocolError, EventsUnknownOutcomeError,
  boundedInt, boundedOffset, canonicalJson, maxBodyBytes, maxEventBytes, maxPageCount,
  maxPartition, offsetString, type EventData, type EventsOptions,
} from "./http.js"

const namePattern = /^[A-Za-z0-9_-]{1,63}(?![\s\S])/
const credentialPattern = /^[!-~]{1,8192}(?![\s\S])/
const timestampPattern = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?(?:Z|[+-](\d{2}):(\d{2}))(?![\s\S])/
const subscriptionConnections = new WeakMap<Subscription, { http: EventsHttp; path: string }>()
const batchLeases = new WeakMap<Batch, Lease>()

export function uuid(value: unknown): string {
  if (typeof value !== "string") throw new TypeError("Expected a UUID")
  const hex = value.replaceAll("urn:", "").replaceAll("uuid:", "").replace(/^[{}]+|[{}]+$/g, "").replaceAll("-", "").toLowerCase()
  if (!/^[0-9a-f]{32}(?![\s\S])/.test(hex)) throw new TypeError("Expected a UUID")
  return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join("-")
}

export function wireUuid(value: unknown): string {
  const id = uuid(value)
  if (id !== value) throw new TypeError("Expected a canonical UUID string")
  return id
}

function timestamp(value: unknown): Date {
  const parts = typeof value === "string" ? timestampPattern.exec(value) : null
  if (!parts) throw new TypeError("Expected an RFC3339 timestamp")
  const [year, month, day, hour, minute, second, timezoneHour, timezoneMinute] = parts.slice(1).map(Number)
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0)
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
  const expires = new Date(value as string)
  if (year < 1 || month < 1 || month > 12 || day < 1 || day > days[month - 1] || hour > 23 || minute > 59
    || second > 59 || timezoneHour > 23 || timezoneMinute > 59 || !Number.isFinite(expires.valueOf())) {
    throw new RangeError("Invalid RFC3339 timestamp")
  }
  return expires
}

export function eventName(value: unknown): string {
  if (typeof value !== "string" || !namePattern.test(value)) throw new TypeError("Events names must match [A-Za-z0-9_-]{1,63}")
  return value
}

function object(value: unknown, ...keys: string[]): JsonObject {
  if (!hasFields(value, ...keys)) throw new TypeError("Unexpected Events response fields")
  return value
}

function opaque(value: unknown): string {
  if (typeof value !== "string" || !credentialPattern.test(value)) throw new TypeError("Expected an opaque credential")
  return value
}

function topicPath(topic: string, partition?: number): string {
  const prefix = "/v1/topics/" + eventName(topic) + "/partitions"
  return partition === undefined ? prefix : prefix + "/" + boundedInt(partition, maxPartition)
}

function publishBody(eventId: string, data: EventData): Buffer {
  const prefix = Buffer.from('{"data":')
  const suffix = Buffer.from(`,"event_id":"${eventId}"}`)
  return Buffer.concat([prefix, canonicalJson(data, maxEventBytes - prefix.length - suffix.length), suffix])
}

export class PublishRecord {
  constructor(readonly eventId: string, readonly data: EventData) {
    Object.freeze(this)
  }

  [inspect.custom](): object {
    return { eventId: this.eventId }
  }
}

export class Event {
  constructor(readonly topicId: string, readonly partition: number, readonly offset: bigint,
    readonly eventId: string, readonly publishedAt: string, readonly data: EventData) {
    Object.freeze(this)
  }

  [inspect.custom](): object {
    const { topicId, partition, offset, eventId, publishedAt } = this
    return { topicId, partition, offset, eventId, publishedAt }
  }
}

function decodeEvent(value: unknown, partition: number, expectedId?: string): Event {
  const fields = object(value, "topic_id", "partition", "offset", "event_id", "published_at", "data")
  const wirePartition = boundedInt(fields.partition, maxPartition)
  const topicId = wireUuid(fields.topic_id)
  const eventId = wireUuid(fields.event_id)
  if (wirePartition !== partition || (expectedId !== undefined && eventId !== expectedId)) {
    throw new TypeError("Event receipt does not match its submitted identity")
  }
  timestamp(fields.published_at)
  return new Event(topicId, partition, offsetString(fields.offset, 1n), eventId,
    fields.published_at as string, fields.data as EventData)
}

export class Batch {
  readonly events: readonly Event[]

  constructor(readonly after: bigint, events: readonly Event[], readonly delivery: string | null, lease: Lease) {
    this.events = Object.freeze([...events])
    batchLeases.set(this, lease)
    Object.freeze(this)
  }

  [inspect.custom](): object {
    return { after: this.after, events: this.events }
  }
}

export interface PublishOptions {
  partition: number
  eventId: string
  data: EventData
}

export interface PublishBatchOptions {
  partition: number
  events: Iterable<PublishRecord | { eventId: string; data: EventData }>
}

export interface SubscriptionOptions {
  partition: number
}

export class Events {
  readonly #http: EventsHttp

  constructor(url: string, options: EventsOptions) {
    this.#http = new EventsHttp(url, options)
  }

  async *partitions(topic: string): AsyncGenerator<number, void, undefined> {
    const path = topicPath(topic)
    let after: number | null = null
    let topicId: string | null = null
    do {
      const cursor: number | null = after
      const query: string = "?limit=" + maxPageCount + (cursor === null ? "" : "&after=" + cursor)
      const current = await this.#http.request<{ topicId: string; items: number[]; next: number | null }>("GET", path + query, {
        decode(value) {
          const page = object(value, "topic_id", "partitions", "next_after")
          if (!Array.isArray(page.partitions) || page.partitions.length > maxPageCount) {
            throw new TypeError("Invalid partition page size")
          }
          const items = page.partitions.map(item => boundedInt(item, maxPartition))
          if (items.some((item, index) => index > 0 && item <= items[index - 1])
            || (cursor !== null && items.length > 0 && items[0] <= cursor)) {
            throw new TypeError("Partition discovery did not advance")
          }
          const next = page.next_after === null ? null : boundedInt(page.next_after, maxPartition)
          if (next !== null && next !== items.at(-1)) throw new TypeError("Invalid partition discovery cursor")
          return { topicId: wireUuid(page.topic_id), items, next }
        },
      })
      if (topicId !== null && topicId !== current.topicId) {
        throw new EventsProtocolError("Topic identity changed during discovery", { code: "protocol_error" })
      }
      topicId = current.topicId
      after = current.next
      yield* current.items
    } while (after !== null)
  }

  publish(topic: string, { partition, eventId, data }: PublishOptions): Promise<Event> {
    boundedInt(partition, maxPartition)
    const path = topicPath(topic, partition)
    const id = uuid(eventId)
    return this.#http.request("POST", path + "/events", {
      body: publishBody(id, data), expected: [200, 201],
      decode: value => decodeEvent(value, partition, id),
    })
  }

  publishBatch(topic: string, { partition, events }: PublishBatchOptions): Promise<readonly Event[]> {
    boundedInt(partition, maxPartition)
    const path = topicPath(topic, partition)
    let size = Buffer.byteLength('{"events":[]}')
    const parts: Buffer[] = [Buffer.from('{"events":[')]
    const ids = new Set<string>()
    for (const event of events) {
      if (ids.size === maxPageCount) throw new RangeError(`Events batch exceeds ${maxPageCount} records`)
      if (!(event instanceof PublishRecord) && !hasFields(event, "eventId", "data")) {
        throw new TypeError("Each event must have eventId and data")
      }
      const id = uuid(event.eventId)
      if (ids.has(id)) throw new TypeError("Duplicate event ID in publish batch")
      const body = publishBody(id, event.data)
      size += body.length + Number(ids.size > 0)
      if (size > maxBodyBytes) throw new RangeError("Encoded Events batch exceeds the byte limit")
      if (ids.size) parts.push(Buffer.from(","))
      parts.push(body)
      ids.add(id)
    }
    if (!ids.size) throw new RangeError("Events batch must contain at least one record")
    parts.push(Buffer.from("]}"))

    return this.#http.request("POST", path + "/batches", {
      body: Buffer.concat(parts, size),
      decode(value) {
        const receipts = object(value, "events").events
        if (!Array.isArray(receipts) || receipts.length !== ids.size) throw new TypeError("Incorrect number of batch receipts")
        const result = [...ids].map((id, index) => decodeEvent(receipts[index], partition, id))
        if (new Set(result.map(event => event.topicId)).size !== 1
          || new Set(result.map(event => event.offset)).size !== result.length) {
          throw new TypeError("Inconsistent topic or offset in batch receipts")
        }
        return Object.freeze(result)
      },
    })
  }

  subscription(topic: string, name: string, { partition }: SubscriptionOptions): Subscription {
    boundedInt(partition, maxPartition)
    return new Subscription(this.#http, topicPath(topic, partition) + "/subscriptions/" + eventName(name), partition)
  }
}

function leaseResponse(value: unknown): { credential: string; expiresAt: Date } {
  const fields = object(value, "lease", "expires_at")
  return { credential: opaque(fields.lease), expiresAt: timestamp(fields.expires_at) }
}

export interface ClaimOptions {
  claimId?: string | null
}

export class Subscription {
  constructor(http: EventsHttp, path: string, readonly partition: number) {
    subscriptionConnections.set(this, { http, path })
    Object.freeze(this)
  }

  async claim({ claimId }: ClaimOptions = {}): Promise<Lease> {
    const id = claimId == null ? randomUUID() : uuid(claimId)
    const body = canonicalJson({ claim_id: id })
    const { http, path } = subscriptionConnections.get(this)!
    const request = () => http.request("POST", path + "/claim", { body, decode: leaseResponse })
    let claim
    try {
      claim = await request()
    } catch (error) {
      if (!(error instanceof EventsUnknownOutcomeError)) throw error
      error.claimId = id
      if (error.retryAfter) throw error
      try {
        claim = await request()
      } catch (second) {
        if (!(second instanceof EventsError)) throw second
        Object.defineProperty(error, "cause", { value: second, configurable: true })
        throw error
      }
    }
    return new Lease(this, claim.credential, claim.expiresAt)
  }
}

export interface PollOptions {
  limit?: number
}

export interface AckOptions {
  through: bigint
}

export class Lease implements AsyncDisposable {
  readonly #subscription: Subscription
  #credential: string
  #expiresAt: Date
  #closed = false
  #tail: Promise<unknown> = Promise.resolve()

  constructor(subscription: Subscription, credential: string, expiresAt: Date) {
    this.#subscription = subscription
    this.#credential = credential
    this.#expiresAt = new Date(expiresAt)
  }

  get expiresAt(): Date {
    return new Date(this.#expiresAt)
  }

  poll({ limit = maxPageCount }: PollOptions = {}): Promise<Batch> {
    boundedInt(limit, maxPageCount, 1)
    return this.#locked(() => this.#request("GET", "/events?limit=" + limit, value => {
      const fields = object(value, "after", "events", "delivery")
      const after = offsetString(fields.after)
      if (!Array.isArray(fields.events) || fields.events.length > limit) throw new TypeError("Invalid Events page size")
      const events = fields.events.map(event => decodeEvent(event, this.#subscription.partition))
      let delivery: string | null = null
      if (events.length) {
        if (events.some((event, index) => event.offset !== after + 1n + BigInt(index))
          || new Set(events.map(event => event.topicId)).size !== 1
          || new Set(events.map(event => event.eventId)).size !== events.length) {
          throw new TypeError("Poll must return a contiguous page for one topic")
        }
        delivery = opaque(fields.delivery)
      } else if (fields.delivery !== null) {
        delivery = opaque(fields.delivery)
      }
      return new Batch(after, events, delivery, this)
    }))
  }

  renew(): Promise<Date> {
    return this.#locked(async () => {
      const claim = await this.#request("POST", "/renew", leaseResponse, Buffer.from("{}"))
      this.#credential = claim.credential
      this.#expiresAt = claim.expiresAt
      return this.expiresAt
    })
  }

  ack(batch: Batch, { through }: AckOptions): Promise<bigint> {
    boundedOffset(through, 1n)
    if (!(batch instanceof Batch) || batchLeases.get(batch) !== this || !batch.events.length
      || batch.delivery === null || through < batch.events[0].offset || through > batch.events.at(-1)!.offset) {
      throw new TypeError("Checkpoint must acknowledge a prefix of a page from this lease")
    }
    const body = canonicalJson({ delivery: batch.delivery, through: String(through) })
    return this.#locked(() => this.#request("PUT", "/checkpoint", value => {
      const offset = offsetString(object(value, "offset").offset)
      if (offset < through) throw new RangeError("Checkpoint receipt precedes the acknowledged prefix")
      return offset
    }, body))
  }

  close(): Promise<void> {
    return this.#locked(async () => {
      if (this.#closed) return
      try {
        await this.#request("POST", "/release", () => undefined, Buffer.from("{}"), [200, 204])
      } catch (error) {
        if (!(error instanceof EventsError) || error.code !== "lease_lost") throw error
      }
      this.#credential = ""
      this.#closed = true
    })
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close()
  }

  #locked<T>(work: () => Promise<T>): Promise<T> {
    const current = this.#tail.then(work, work)
    this.#tail = current.then(() => undefined, () => undefined)
    return current
  }

  #request<T>(method: string, suffix: string, decode: (value: unknown) => T,
    body?: Buffer, expected?: readonly number[]): Promise<T> {
    if (this.#closed) throw new EventsError("Events lease is closed", { code: "lease_lost" })
    const { http, path } = subscriptionConnections.get(this.#subscription)!
    return http.request(method, path + suffix, { body, decode, lease: this.#credential, expected })
  }
}

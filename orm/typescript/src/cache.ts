import { readFile } from "node:fs/promises"
import { isIP } from "node:net"
import { createSecureContext, type SecureContext } from "node:tls"
import {
  Connection, ErrorReply, frame, isLoopback, MAX_VALUE, remaining,
  type Endpoint, type Reply,
} from "./cacheWire.js"
import {
  AuthenticationError, OverloadedError, ProtocolError, StorageError,
  TransportError, UnknownOutcomeError, ValidationError,
} from "./errors.js"

export type CacheSetCondition = "always" | "absent" | "present"

export interface CacheSetOptions {
  ttlMs?: number
  when?: CacheSetCondition
}

export interface CacheOptions {
  username?: string
  password?: string
  namespace?: string
  cluster?: boolean
  poolSize?: number
  timeoutMs?: number
  caFile?: string
  allowedNodes?: readonly string[]
  allowedNodeSuffix?: string
}

const MAX_TTL = Number.MAX_SAFE_INTEGER
const SLOT_COUNT = 16_384
const MAX_REDIRECTS = 4
const MIN_I64 = -(2n ** 63n)
const MAX_I64 = 2n ** 63n - 1n
const NAMESPACE = /^[a-z0-9_-]{1,64}$/u
const LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/u
const URI = /^(rediss?):\/\/([^/?#]+)\/?$/u
const AUTHORITY = /^(\[[a-fA-F0-9:.]+\]|[a-zA-Z0-9.-]+)(?::([0-9]{1,5}))?$/u
const MOVED = /^MOVED ([0-9]{1,5}) ([-a-zA-Z0-9.:\[\]]+)$/u
const CRC_TABLE = Uint16Array.from({ length: 256 }, (_, byte) => {
  let crc = byte << 8
  for (let bit = 0; bit < 8; bit++) crc = (crc << 1) ^ ((crc & 0x8000) ? 0x1021 : 0)
  return crc
})

function keySlot(bytes: Buffer): number {
  let crc = 0
  for (const byte of bytes) crc = (crc << 8) ^ CRC_TABLE[((crc >>> 8) ^ byte) & 0xff]
  return crc & (SLOT_COUNT - 1)
}

function checkedNumber(value: number, low: number, high: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < low || value > high) {
    throw new ValidationError(`${name} must be an integer from ${low} through ${high}`)
  }
  return value
}

function hostname(host: string): string {
  const lower = host.toLowerCase()
  if (isIP(lower) === 6) return new URL(`https://[${lower}]`).hostname.slice(1, -1)
  if (lower.length <= 253 && lower.split(".").every(label => LABEL.exec(label)?.[0] === label)) return lower
  throw new ValidationError("invalid cache hostname")
}

function authority(address: string, tls: boolean, requirePort = false): Endpoint {
  const parsed = AUTHORITY.exec(address)
  if (!parsed || parsed[0] !== address || (requirePort && parsed[2] === undefined)) {
    throw new ValidationError("invalid cache endpoint authority")
  }
  const bracketed = parsed[1].startsWith("[")
  const name = bracketed ? parsed[1].slice(1, -1) : parsed[1]
  if (bracketed && isIP(name) !== 6) throw new ValidationError("invalid cache IPv6 address")
  const host = hostname(name)
  if (!tls && host !== "localhost" && !isLoopback(host)) {
    throw new ValidationError("cache plaintext requires a loopback endpoint")
  }
  const port = checkedNumber(Number(parsed[2] ?? "6379"), 1, 65535, "port")
  return { tls, host, port, id: `${tls}/${host}/${port}` }
}

function endpoints(values: string | readonly string[], maximum: number): Endpoint[] {
  const input = typeof values === "string" ? [values] : values
  if (!Array.isArray(input) || input.length > maximum) throw new ValidationError("invalid cache endpoint list")
  const nodes = new Map<string, Endpoint>()
  for (const value of input) {
    const match = typeof value === "string" && value.length <= 512 ? URI.exec(value) : null
    if (!match || match[0] !== value) throw new ValidationError("cache endpoint must be a redis:// or rediss:// URI")
    const node = authority(match[2], match[1] === "rediss")
    nodes.set(node.id, node)
  }
  return [...nodes.values()]
}

function credential(value: string, name: string): Buffer {
  if (typeof value === "string" && value.length >= 1 && value.length <= 4096 && value.isWellFormed()) {
    const bytes = Buffer.from(value)
    if (bytes.length <= 4096) return bytes
  }
  throw new ValidationError(`invalid cache ${name}`)
}

function failure(reply: ErrorReply): Error {
  switch (reply.text.split(" ", 1)[0]) {
    case "NOAUTH":
    case "WRONGPASS":
    case "NOPERM":
      return new AuthenticationError("cache authentication or permission denied")
    case "BUSY":
    case "OOM":
    case "TRYAGAIN":
      return new OverloadedError("cache operation rejected before execution")
    case "CLUSTERDOWN":
      return new StorageError("cache slot unavailable")
    case "ERR":
    case "CROSSSLOT":
      return new ValidationError("cache command rejected")
    default:
      return new ProtocolError("unrecognized cache error reply")
  }
}

function booleanReply(reply: Reply): boolean {
  if (reply === 0n || reply === 1n) return reply !== 0n
  throw new ProtocolError("invalid cache boolean reply")
}

class Pool {
  #closed = false
  #opening = new Set<Connection>()
  #live = new Set<Connection>()
  #idle = new Map<Connection, string>()

  constructor(readonly size: number, readonly context?: SecureContext, readonly auth?: Buffer) {}

  async take(endpoint: Endpoint, deadline: number): Promise<Connection> {
    remaining(deadline)
    if (this.#closed) throw new TransportError("cache is closed", { outcome: "not_applied" })
    for (const [connection, owner] of this.#idle) {
      if (connection.open && owner === endpoint.id) {
        this.#idle.delete(connection)
        return connection
      }
      if (!connection.open) this.release(endpoint, connection, false)
    }
    if (this.#opening.size + this.#live.size >= this.size) {
      const discarded = this.#idle.keys().next().value
      if (!discarded) throw new OverloadedError("cache connection pool is busy")
      this.release(endpoint, discarded, false)
    }
    let connection: Connection | undefined
    try {
      const opened = await Connection.open(endpoint, this.context, deadline, opening => {
        connection = opening
        this.#opening.add(opening)
      })
      if (this.#closed) throw new TransportError("cache is closed", { outcome: "not_applied" })
      if (this.auth) {
        const reply = await opened.exchange(this.auth, deadline)
        if (reply instanceof ErrorReply) throw failure(reply)
        if (reply !== "OK") throw new ProtocolError("invalid cache AUTH reply")
      }
      if (this.#closed) throw new TransportError("cache is closed", { outcome: "not_applied" })
      this.#live.add(opened)
      return opened
    } catch (error) {
      connection?.close()
      if (error instanceof TransportError || error instanceof ProtocolError) error.outcome = "not_applied"
      throw error
    } finally {
      if (connection) this.#opening.delete(connection)
    }
  }

  release(endpoint: Endpoint, connection: Connection, reusable: boolean): void {
    if (!this.#live.has(connection)) return
    if (reusable && connection.open) {
      this.#idle.set(connection, endpoint.id)
    } else {
      this.#idle.delete(connection)
      this.#live.delete(connection)
      connection.close()
    }
  }

  close(): void {
    this.#closed = true
    for (const connection of this.#opening) connection.close()
    for (const connection of this.#live) connection.close()
    this.#opening.clear()
    this.#live.clear()
    this.#idle.clear()
  }
}

/** A binary Cache client with one connection pool and namespace. */
export class Cache {
  #seeds: readonly Endpoint[]
  #allowed: ReadonlySet<string>
  #suffix?: string
  #ports: ReadonlySet<number>
  #prefix: string
  #cluster: boolean
  #timeout: number
  #pool: Pool
  #slots: (Endpoint | undefined)[] = Array(SLOT_COUNT)

  private constructor(seeds: readonly Endpoint[], allowed: ReadonlySet<string>, suffix: string | undefined,
    namespace: string, cluster: boolean, timeout: number, pool: Pool) {
    this.#seeds = seeds
    this.#allowed = allowed
    this.#suffix = suffix
    this.#ports = new Set(seeds.map(seed => seed.port))
    this.#prefix = `nb:${namespace}:`
    this.#cluster = cluster
    this.#timeout = timeout
    this.#pool = pool
  }

  /** Open a verified connection and discover slot owners in cluster mode. */
  static async connect(bootstrap: string | readonly string[], options: CacheOptions = {}): Promise<Cache> {
    const {
      username, password, namespace = "default", cluster = false, poolSize = 16,
      timeoutMs = 2_000, caFile, allowedNodes = [], allowedNodeSuffix,
    } = options
    const seeds = endpoints(bootstrap, 32)
    if (!seeds.length || typeof cluster !== "boolean" || (!cluster && seeds.length !== 1)) {
      throw new ValidationError("cache requires seeds, or one endpoint in standalone mode")
    }
    if (typeof namespace !== "string" || NAMESPACE.exec(namespace)?.[0] !== namespace) {
      throw new ValidationError("cache namespace must contain 1–64 characters in [a-z0-9_-]")
    }
    const size = checkedNumber(poolSize, 1, 1024, "poolSize")
    const timeout = checkedNumber(timeoutMs, 1, 60_000, "timeoutMs")
    const nodes = [...endpoints(allowedNodes, 1024), ...seeds]
    if (nodes.some(node => node.tls !== seeds[0].tls)) {
      throw new ValidationError("cache endpoints must use the same transport")
    }
    let suffix: string | undefined
    if (allowedNodeSuffix !== undefined) {
      if (!seeds[0].tls || typeof allowedNodeSuffix !== "string") {
        throw new ValidationError("cache node suffix requires TLS and a DNS name")
      }
      suffix = hostname(allowedNodeSuffix)
      if (!suffix.includes(".") || /[0-9]$/u.test(suffix)) throw new ValidationError("invalid cache node DNS suffix")
    }
    if (username !== undefined && password === undefined) throw new ValidationError("cache username requires a password")
    let auth: Buffer | undefined
    if (password !== undefined) {
      const user = username === undefined ? [] : [credential(username, "username")]
      auth = frame(["AUTH", ...user, credential(password, "password")])
    }
    if (caFile !== undefined && (!seeds[0].tls || typeof caFile !== "string")) {
      throw new ValidationError("cache TLS CA file requires TLS")
    }
    let context: SecureContext | undefined
    if (seeds[0].tls && caFile !== undefined) {
      try {
        context = createSecureContext({ ca: await readFile(caFile) })
      } catch {
        throw new ValidationError("invalid cache TLS CA file")
      }
    }
    const cache = new Cache(seeds, new Set(nodes.map(node => node.id)), suffix, namespace,
      cluster, timeout, new Pool(size, context, auth))
    const deadline = performance.now() + timeout
    try {
      if (cluster) await cache.#refresh(deadline)
      else {
        const reply = await cache.#call(seeds[0], frame(["PING"]), deadline)
        if (reply instanceof ErrorReply) throw failure(reply)
        if (reply !== "PONG") throw new ProtocolError("invalid cache PING reply")
      }
      return cache
    } catch (error) {
      await cache.close()
      throw error
    }
  }

  #key(key: string): Buffer {
    if (typeof key === "string" && key.length >= 1 && key.length <= 250 && key.isWellFormed()) {
      const bytes = Buffer.from(key)
      if (bytes.length <= 250) return Buffer.from(this.#prefix + bytes.toString("base64url"))
    }
    throw new ValidationError("cache key must contain 1–250 UTF-8 bytes")
  }

  #authorize(endpoint: Endpoint): Endpoint {
    if (this.#allowed.has(endpoint.id) || (this.#suffix &&
      endpoint.host.endsWith("." + this.#suffix) && this.#ports.has(endpoint.port))) return endpoint
    throw new ProtocolError("cache discovery advertised an untrusted endpoint")
  }

  #address(host: Buffer, port: Reply): Endpoint {
    if (typeof port !== "bigint" || port < 1n || port > 65535n) throw new ProtocolError("invalid cache owner port")
    try {
      const name = host.toString("latin1")
      return this.#authorize(authority(name.includes(":") ? `[${name}]:${port}` : `${name}:${port}`,
        this.#seeds[0].tls, true))
    } catch (error) {
      if (error instanceof ValidationError) throw new ProtocolError("invalid cache owner address")
      throw error
    }
  }

  #topology(reply: Reply): (Endpoint | undefined)[] {
    if (!Array.isArray(reply) || !reply.length || reply.length > SLOT_COUNT) throw new ProtocolError("invalid cache slot map")
    const slots = Array<Endpoint | undefined>(SLOT_COUNT)
    for (const row of reply) {
      if (!Array.isArray(row) || row.length < 3) throw new ProtocolError("invalid cache slot range")
      const [first, last, owner] = row
      if (typeof first !== "bigint" || typeof last !== "bigint" || first < 0n || first > last ||
          last >= SLOT_COUNT || !Array.isArray(owner) || owner.length < 2 || !Buffer.isBuffer(owner[0])) {
        throw new ProtocolError("invalid cache slot range")
      }
      const endpoint = this.#address(owner[0], owner[1])
      for (let slot = Number(first); slot <= Number(last); slot++) {
        if (slots[slot]) throw new ProtocolError("overlapping cache slot ranges")
        slots[slot] = endpoint
      }
    }
    return slots
  }

  async #refresh(deadline: number): Promise<void> {
    let failureSeen: unknown
    for (const seed of this.#seeds) {
      try {
        const reply = await this.#call(seed, frame(["CLUSTER", "SLOTS"]), deadline)
        if (reply instanceof ErrorReply) throw failure(reply)
        this.#slots = this.#topology(reply)
        return
      } catch (error) {
        if (!(error instanceof TransportError || error instanceof ProtocolError ||
              error instanceof StorageError || error instanceof OverloadedError)) throw error
        failureSeen = error
      }
    }
    throw failureSeen
  }

  #moved(reply: ErrorReply, slot: number): Endpoint {
    const match = MOVED.exec(reply.text)
    if (!match || match[0] !== reply.text || Number(match[1]) !== slot) {
      throw new ProtocolError("cache MOVED does not identify the requested slot")
    }
    let target: Endpoint
    try {
      target = this.#authorize(authority(match[2], this.#seeds[0].tls, true))
    } catch (error) {
      if (error instanceof ValidationError) throw new ProtocolError("invalid cache MOVED address")
      throw error
    }
    this.#slots[slot] = target
    return target
  }

  async #call(endpoint: Endpoint, payload: Buffer, deadline: number): Promise<Reply>
  async #call<T>(endpoint: Endpoint, payload: Buffer, deadline: number, write: boolean,
    check: (reply: Reply) => T): Promise<T | ErrorReply>
  async #call<T>(endpoint: Endpoint, payload: Buffer, deadline: number, write = false,
    check?: (reply: Reply) => T): Promise<Reply | T> {
    const connection = await this.#pool.take(endpoint, deadline)
    let submitted = false
    let reusable = false
    try {
      const reply = await connection.exchange(payload, deadline, () => { submitted = true })
      const decoded = reply instanceof ErrorReply || !check ? reply : check(reply)
      reusable = true
      return decoded
    } catch (error) {
      if (write && submitted) throw new UnknownOutcomeError("cache mutation may have executed")
      if (error instanceof ProtocolError) throw new ProtocolError("invalid cache reply")
      throw new TransportError("cache read failed before a complete reply", { outcome: "not_applied" })
    } finally {
      this.#pool.release(endpoint, connection, reusable)
    }
  }

  async #run<T>(command: string, key: string, args: readonly (Buffer | string)[], write: boolean,
    check: (reply: Reply) => T): Promise<T> {
    const deadline = performance.now() + this.#timeout
    const encoded = this.#key(key)
    const payload = frame([command, encoded, ...args])
    const slot = this.#cluster ? keySlot(encoded) : -1
    let endpoint = this.#slots[slot] ?? this.#seeds[0]
    let refreshed = false
    for (let attempt = 0; attempt <= MAX_REDIRECTS; attempt++) {
      let reply: T | ErrorReply
      try {
        reply = await this.#call(endpoint, payload, deadline, write, check)
      } catch (error) {
        // A write-side transport error can only occur before submission.
        if (!(error instanceof TransportError) || !this.#cluster || refreshed) throw error
        refreshed = true
        await this.#refresh(deadline)
        endpoint = this.#slots[slot] ?? this.#seeds[0]
        continue
      }
      if (!(reply instanceof ErrorReply)) return reply
      if (this.#cluster && reply.text.startsWith("MOVED ")) {
        if (attempt === MAX_REDIRECTS) throw new StorageError("cache redirect limit reached")
        endpoint = this.#moved(reply, slot)
        continue
      }
      const error = failure(reply)
      if (write && error instanceof ProtocolError) {
        throw new UnknownOutcomeError("cache mutation received an unrecognized reply")
      }
      throw error
    }
    throw new StorageError("cache redirect limit reached")
  }

  /** Read exact bytes, or null for a miss. */
  async get(key: string): Promise<Uint8Array | null> {
    return this.#run("GET", key, [], false, reply => {
      if (reply === null || Buffer.isBuffer(reply)) return reply
      throw new ProtocolError("invalid cache GET reply")
    })
  }

  /** Write bytes, optionally conditioned on the presence of a live key. */
  async set(key: string, value: Uint8Array, options: CacheSetOptions = {}): Promise<boolean> {
    if (!(value instanceof Uint8Array) || value.byteLength > MAX_VALUE) {
      throw new ValidationError("cache value must be bytes and at most 1 MiB")
    }
    const { ttlMs, when = "always" } = options
    if (when !== "always" && when !== "absent" && when !== "present") throw new ValidationError("invalid cache SET condition")
    const args: (Buffer | string)[] = [Buffer.from(value.buffer, value.byteOffset, value.byteLength)]
    if (ttlMs !== undefined) args.push("PX", String(checkedNumber(ttlMs, 1, MAX_TTL, "ttlMs")))
    if (when !== "always") args.push(when === "absent" ? "NX" : "XX")
    return this.#run("SET", key, args, true, reply => {
      if (reply === "OK") return true
      if (reply === null && when !== "always") return false
      throw new ProtocolError("invalid cache SET reply")
    })
  }

  /** Delete a live key. */
  async delete(key: string): Promise<boolean> {
    return this.#run("DEL", key, [], true, booleanReply)
  }

  /** Set a relative TTL in milliseconds. Zero deletes the key. */
  async expire(key: string, ttlMs: number): Promise<boolean> {
    return this.#run("PEXPIRE", key, [String(checkedNumber(ttlMs, 0, MAX_TTL, "ttlMs"))], true, booleanReply)
  }

  /** Remove a live key's expiry. */
  async persist(key: string): Promise<boolean> {
    return this.#run("PERSIST", key, [], true, booleanReply)
  }

  /** Return remaining whole milliseconds, -1 for persistent or -2 for missing. */
  async ttlMs(key: string): Promise<number> {
    return this.#run("PTTL", key, [], false, reply => {
      if (typeof reply === "bigint" && reply >= -2n && reply <= MAX_TTL) return Number(reply)
      throw new ProtocolError("invalid cache PTTL reply")
    })
  }

  /** Atomically add a signed 64-bit integer and return the exact result. */
  async increment(key: string, delta: bigint): Promise<bigint> {
    if (typeof delta !== "bigint" || delta < MIN_I64 || delta > MAX_I64) {
      throw new ValidationError("delta must be a signed 64-bit bigint")
    }
    return this.#run("INCRBY", key, [String(delta)], true, reply => {
      if (typeof reply === "bigint") return reply
      throw new ProtocolError("invalid cache INCRBY reply")
    })
  }

  /** Close established and opening connections. Repeated calls succeed. */
  async close(): Promise<void> {
    this.#pool.close()
  }
}

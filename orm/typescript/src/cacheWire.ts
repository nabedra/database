import { connect as tcpConnect, isIP, type Socket } from "node:net"
import { checkServerIdentity, connect as tlsConnect, type SecureContext } from "node:tls"
import { ProtocolError, TransportError } from "./errors.js"

export const MAX_VALUE = 1024 * 1024
const MAX_REPLY = 8 * MAX_VALUE
const MAX_LINE = 4096
const MAX_ITEMS = 262_144
const MIN_I64 = -(2n ** 63n)
const MAX_I64 = 2n ** 63n - 1n
const CRLF = Buffer.from("\r\n")
const DIGITS = /^-?[0-9]{1,19}$/

export interface Endpoint {
  tls: boolean
  host: string
  port: number
  id: string
}

export class ErrorReply {
  constructor(readonly text: string) {}
}

export type Reply = Buffer | string | bigint | Reply[] | ErrorReply | null

export function isLoopback(host: string): boolean {
  return host === "::1" || (isIP(host) === 4 && host.startsWith("127."))
}

export function remaining(deadline: number): number {
  const elapsed = deadline - performance.now()
  if (elapsed <= 0) throw new TransportError("cache deadline expired", { outcome: "not_applied" })
  return elapsed
}

export function frame(args: readonly (Buffer | string)[]): Buffer {
  const parts: Buffer[] = [Buffer.from(`*${args.length}\r\n`)]
  for (const arg of args) {
    const bytes = typeof arg === "string" ? Buffer.from(arg) : arg
    parts.push(Buffer.from(`$${bytes.length}\r\n`), bytes, CRLF)
  }
  return Buffer.concat(parts)
}

class ReplyDecoder {
  #received = 0
  #items = 0
  #header: Buffer = Buffer.alloc(0)
  #groups: { remaining: number; values: Reply[] }[] = []
  #body?: Buffer
  #bodyOffset = 0

  push(chunk: Buffer): Reply | undefined {
    this.#received += chunk.length
    if (this.#received > MAX_REPLY) throw new ProtocolError("cache reply exceeds byte limit")
    let bytes = this.#header.length ? Buffer.concat([this.#header, chunk]) : chunk
    this.#header = Buffer.alloc(0)
    let offset = 0
    while (offset < bytes.length) {
      let value: Reply
      if (this.#body) {
        const length = bytes.copy(this.#body, this.#bodyOffset, offset)
        offset += length
        this.#bodyOffset += length
        if (this.#bodyOffset < this.#body.length) return
        value = this.#bulk(this.#body, 0, this.#body.length - 2)
        this.#body = undefined
        this.#bodyOffset = 0
      } else {
        const end = bytes.indexOf(CRLF, offset)
        if (end === -1) {
          if (bytes.length - offset > MAX_LINE + 1) throw new ProtocolError("cache reply line exceeds byte limit")
          this.#header = bytes.subarray(offset)
          return
        }
        if (end <= offset || end - offset > MAX_LINE || ++this.#items > MAX_ITEMS) {
          throw new ProtocolError("invalid cache reply line or item count")
        }
        const type = bytes[offset]
        const text = bytes.toString("latin1", offset + 1, end)
        offset = end + CRLF.length
        if (type === 43 && !/[^\x00-\x7f]/u.test(text)) value = text
        else if (type === 45 && this.#groups.length === 0) value = new ErrorReply(text)
        else {
          if (!DIGITS.test(text)) throw new ProtocolError("malformed cache RESP2 reply")
          switch (type) {
            case 58: {
              value = BigInt(text)
              if (value < MIN_I64 || value > MAX_I64) throw new ProtocolError("cache reply integer exceeds signed 64 bits")
              break
            }
            case 36: {
              const length = Number(text)
              if (length === -1) {
                value = null
                break
              }
              if (length < 0 || length > MAX_VALUE) throw new ProtocolError("invalid cache bulk size")
              if (offset + length + CRLF.length > bytes.length) {
                this.#body = Buffer.allocUnsafe(length + CRLF.length)
                this.#bodyOffset = bytes.copy(this.#body, 0, offset)
                return
              }
              value = this.#bulk(bytes, offset, offset + length)
              offset += length + CRLF.length
              break
            }
            case 42: {
              const length = Number(text)
              if (length < 0 || length > MAX_ITEMS - this.#items || this.#groups.length >= 6) {
                throw new ProtocolError("invalid cache array size or nesting")
              }
              if (length === 0) {
                value = []
                break
              }
              this.#groups.push({ remaining: length, values: [] })
              continue
            }
            default:
              throw new ProtocolError("unsupported cache RESP2 reply")
          }
        }
      }

      while (this.#groups.length) {
        const parent = this.#groups.at(-1)!
        parent.values.push(value)
        if (--parent.remaining > 0) break
        this.#groups.pop()
        value = parent.values
        if (this.#groups.length === 0) {
          if (offset !== bytes.length) throw new ProtocolError("extra bytes after cache reply")
          return value
        }
      }
      if (this.#groups.length === 0) {
        if (offset !== bytes.length) throw new ProtocolError("extra bytes after cache reply")
        return value
      }
    }
  }

  #bulk(bytes: Buffer, start: number, end: number): Buffer {
    if (bytes[end] !== 13 || bytes[end + 1] !== 10) throw new ProtocolError("malformed cache bulk reply")
    return bytes.subarray(start, end)
  }
}

interface Exchange {
  decoder: ReplyDecoder
  finish: (result: Reply | Error) => void
}

export class Connection {
  #pending?: Exchange

  private constructor(readonly socket: Socket) {
    socket.setNoDelay(true)
    socket.on("data", (bytes: Buffer) => {
      const pending = this.#pending
      if (!pending) {
        this.close()
        return
      }
      try {
        const result = pending.decoder.push(bytes)
        if (result !== undefined) pending.finish(result)
      } catch (error) {
        pending.finish(error as Error)
        this.close()
      }
    })
    socket.on("error", () => this.#failed())
    socket.on("end", () => this.#failed())
    socket.on("close", () => this.#failed())
  }

  static async open(endpoint: Endpoint, secureContext: SecureContext | undefined, deadline: number,
    track: (connection: Connection) => void): Promise<Connection> {
    const timeout = remaining(deadline)
    return new Promise((resolve, reject) => {
      const options = { host: endpoint.host, port: endpoint.port }
      const socket = endpoint.tls ? tlsConnect({
        ...options,
        servername: isIP(endpoint.host) ? undefined : endpoint.host,
        secureContext,
        rejectUnauthorized: true,
        checkServerIdentity: (_host, certificate) => checkServerIdentity(endpoint.host, certificate),
      }) : tcpConnect(options)
      const connection = new Connection(socket)
      const ready = endpoint.tls ? "secureConnect" : "connect"
      const cleanup = () => {
        clearTimeout(timer)
        socket.off("error", fail).off("close", fail).off(ready, connected)
      }
      const fail = () => {
        cleanup()
        connection.close()
        reject(new TransportError("cache connection or TLS verification failed", { outcome: "not_applied" }))
      }
      const connected = () => {
        if (!endpoint.tls && (!socket.remoteAddress || !isLoopback(socket.remoteAddress))) {
          fail()
          return
        }
        cleanup()
        resolve(connection)
      }
      const timer = setTimeout(fail, timeout)
      socket.once("error", fail).once("close", fail).once(ready, connected)
      track(connection)
    })
  }

  get open(): boolean {
    return !this.socket.destroyed && !this.socket.readableEnded
  }

  exchange(payload: Buffer, deadline: number, submit?: () => void): Promise<Reply> {
    const timeout = remaining(deadline)
    if (!this.open) throw new TransportError("cache connection closed", { outcome: "not_applied" })
    return new Promise((resolve, reject) => {
      const finish = (result: Reply | Error) => {
        clearTimeout(timer)
        this.#pending = undefined
        if (result instanceof Error) reject(result)
        else resolve(result)
      }
      const timer = setTimeout(() => {
        finish(new TransportError("cache deadline expired"))
        this.close()
      }, timeout)
      this.#pending = { decoder: new ReplyDecoder(), finish }
      try {
        submit?.()
        this.socket.write(payload)
      } catch {
        finish(new TransportError("cache write failed"))
        this.close()
      }
    })
  }

  #failed(): void {
    this.#pending?.finish(new TransportError("cache peer closed before a complete reply"))
    this.close()
  }

  close(): void {
    this.socket.destroy()
  }
}

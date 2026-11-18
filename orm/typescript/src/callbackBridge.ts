import { timingSafeEqual } from "node:crypto"
import { createServer, type Server, type Socket } from "node:net"
import {
  authorizeCallback, callbackSuccess, CallbackProtocolError, contentLength, decodeCallback,
  parseHeaders, readPacket, type CallbackCommand, type StartNodeRequest,
} from "./callbackProtocol.js"
import type { ProviderNode } from "./providers.js"
import { encodeJson } from "./transport.js"

const maxHandlers = 8
const maxPending = 8
const sideEffectSlots = 3
const ioMs = 2_000
const observationMs = 30_000

type Awaitable<T> = T | PromiseLike<T>

export interface ProviderCallbacks {
  startNode: (payload: StartNodeRequest) => Awaitable<string>
  stopNode: (resourceId: string) => Awaitable<void | null>
  listNodes: () => Awaitable<ProviderNode[]>
}

interface Reply {
  status: number
  bytes: Buffer
}

interface Call {
  command: CallbackCommand
  deadline: number
  entered: boolean
  completed: boolean
  resolve: (reply: Reply) => void
  timer: NodeJS.Timeout
}

interface Peer { socket: Socket; deadline: number }

function rejectReply(status: number, code: string, command?: CallbackCommand, entered = false): Reply {
  const unknown = code === "callback_in_progress" || (entered && command?.action !== "inventory")
  return {
    status,
    bytes: encodeJson({
      version: 1, invocation_id: command?.invocationId ?? null,
      error: { code, outcome: unknown ? "unknown" : "not_invoked" },
    }),
  }
}

export class CallbackBridge {
  readonly #server: Server
  readonly #sockets = new Set<Socket>()
  readonly #pending: Peer[] = []
  readonly #calls = new Map<string, Call>()
  readonly #idle = new Set<() => void>()
  readonly #callbacks: ProviderCallbacks
  readonly #credential: Buffer
  readonly #authorizeToken: string
  #host = ""
  #binding: { clusterId: string; url: string } | undefined
  #stopped = false
  #handlers = 0
  #listenerClose: Promise<void> | undefined

  private constructor(callbacks: ProviderCallbacks, callbackToken: string, authorizeToken: string) {
    this.#callbacks = callbacks
    this.#credential = Buffer.from("Bearer " + callbackToken, "ascii")
    this.#authorizeToken = authorizeToken
    this.#server = createServer({ pauseOnConnect: true, allowHalfOpen: true }, socket => this.#accept(socket))
  }

  static async open(callbacks: ProviderCallbacks, callbackToken: string, authorizeToken: string): Promise<CallbackBridge> {
    const bridge = new CallbackBridge(callbacks, callbackToken, authorizeToken)
    try {
      await new Promise<void>((resolve, reject) => {
        const server = bridge.#server
        server.once("error", reject)
        server.listen({ host: "127.0.0.1", port: 0, backlog: 8 }, () => {
          server.off("error", reject)
          resolve()
        })
      })
      const address = bridge.#server.address()
      if (!address || typeof address === "string") throw new Error()
      bridge.#host = "127.0.0.1:" + address.port
      bridge.#server.on("error", () => bridge.stopAdmission())
      return bridge
    } catch {
      await bridge.close()
      throw new Error("Callback bridge startup failed")
    }
  }

  get url(): string {
    return "http://" + this.#host + "/private/v1/callback"
  }

  get alive(): boolean {
    return this.#calls.size !== 0
  }

  activate(clusterId: string, url: string): void {
    if (this.#stopped || this.#binding) throw new Error("Callback pool activation rejected")
    this.#binding = { clusterId, url }
  }

  stopAdmission(): void {
    if (this.#stopped) return
    this.#stopped = true
    for (const call of this.#calls.values()) {
      const expired = performance.now() >= call.deadline
      this.#finish(call, rejectReply(expired ? 504 : 503,
        expired ? "callback_timeout" : "bridge_not_active", call.command, call.entered))
    }
    this.#listenerClose = new Promise(resolve => this.#server.close(() => resolve()))
    for (const socket of this.#sockets) socket.destroy()
    this.#pending.length = 0
  }

  async close(): Promise<void> {
    this.stopAdmission()
    await this.#listenerClose
    if (!this.alive) return
    await new Promise<void>(resolve => {
      const finish = () => {
        clearTimeout(timer)
        this.#idle.delete(finish)
        resolve()
      }
      const timer = setTimeout(finish, 2_000)
      this.#idle.add(finish)
    })
  }

  #accept(socket: Socket): void {
    socket.on("error", () => {})
    if (this.#stopped || socket.remoteAddress !== "127.0.0.1" || this.#pending.length >= maxPending) {
      socket.destroy()
      return
    }
    this.#sockets.add(socket)
    socket.once("close", () => this.#sockets.delete(socket))
    this.#pending.push({ socket, deadline: performance.now() + ioMs })
    this.#dispatch()
  }

  #dispatch(): void {
    while (!this.#stopped && this.#handlers < maxHandlers && this.#pending.length > 0) {
      const peer = this.#pending.shift()!
      if (peer.socket.destroyed) continue
      this.#handlers += 1
      void this.#handle(peer).finally(() => {
        peer.socket.destroy()
        this.#handlers -= 1
        this.#dispatch()
      })
    }
  }

  #readLength(head: Buffer): number {
    const [start, headers] = parseHeaders(head)
    if (start !== "POST /private/v1/callback HTTP/1.1") throw new CallbackProtocolError()
    const authorization = Buffer.from(headers.get("authorization") ?? "", "latin1")
    if (authorization.length !== this.#credential.length || !timingSafeEqual(authorization, this.#credential)) {
      throw new CallbackProtocolError("unauthorized")
    }
    const connection = headers.get("connection")?.toLowerCase() ?? "close"
    if (headers.get("host") !== this.#host || headers.get("content-type")?.toLowerCase() !== "application/json"
      || ["transfer-encoding", "content-encoding", "expect", "upgrade"].some(field => headers.has(field))
      || (connection !== "close" && connection !== "keep-alive")) throw new CallbackProtocolError()
    return contentLength(headers)
  }

  async #handle({ socket, deadline }: Peer): Promise<void> {
    try {
      let reply: Reply
      try {
        const bytes = await readPacket(socket, deadline, head => this.#readLength(head), false)
        if (performance.now() >= deadline) throw new CallbackProtocolError()
        reply = await this.#submit(decodeCallback(bytes))
      } catch (error) {
        const code = error instanceof CallbackProtocolError ? error.code : "invalid_command"
        reply = rejectReply(code === "unauthorized" ? 401 : code === "limit_exceeded" ? 413 : 400, code)
      }
      if (socket.destroyed) return
      const head = "HTTP/1.1 " + reply.status + " Reply\r\nContent-Type: application/json\r\n"
        + "Content-Length: " + reply.bytes.length + "\r\nConnection: close\r\n\r\n"
      await new Promise<void>(resolve => {
        const finish = () => {
          clearTimeout(timer)
          socket.off("close", finish)
          resolve()
        }
        const timer = setTimeout(() => {
          socket.destroy()
          finish()
        }, ioMs)
        socket.once("close", finish)
        socket.end(Buffer.concat([Buffer.from(head, "ascii"), reply.bytes]), finish)
      })
    } catch {
      // Losing the inbound client cannot cancel an entered callback.
    }
  }

  #submit(command: CallbackCommand): Promise<Reply> {
    const binding = this.#binding
    if (this.#stopped || !binding) return Promise.resolve(rejectReply(503, "bridge_not_active", command))
    if (binding.clusterId !== command.clusterId) return Promise.resolve(rejectReply(400, "invalid_command", command))
    const inventory = command.action === "inventory"
    const key = command.action + ":" + (inventory ? command.invocationId : command.intentId)
    const existing = this.#calls.get(key)
    if (existing) {
      const code = existing.command.identity === command.identity ? "callback_in_progress" : "identity_conflict"
      return Promise.resolve(rejectReply(409, code, command))
    }
    const occupied = Array.from(this.#calls.values()).filter(call => (call.command.action === "inventory") === inventory).length
    if (occupied >= (inventory ? 1 : sideEffectSlots)) {
      return Promise.resolve(rejectReply(429, "callback_capacity", command))
    }
    const { promise, resolve } = Promise.withResolvers<Reply>()
    const call: Call = {
      command, deadline: performance.now() + observationMs, entered: false, completed: false, resolve,
      timer: setTimeout(() => this.#finish(call, rejectReply(504, "callback_timeout", command, call.entered)), observationMs),
    }
    this.#calls.set(key, call)
    void this.#execute(call, binding.url).then(reply => {
      this.#finish(call, performance.now() >= call.deadline
        ? rejectReply(504, "callback_timeout", command, call.entered) : reply)
    }).finally(() => {
      this.#calls.delete(key)
      if (!this.alive) for (const finish of this.#idle) finish()
    })
    return promise
  }

  #finish(call: Call, reply: Reply): void {
    if (call.completed) return
    call.completed = true
    clearTimeout(call.timer)
    call.resolve(reply)
  }

  async #execute(call: Call, url: string): Promise<Reply> {
    const { command, deadline } = call
    try {
      await authorizeCallback(url, this.#authorizeToken, command, deadline)
    } catch (error) {
      return error instanceof CallbackProtocolError && error.status !== undefined
        ? rejectReply(error.status, error.code, command) : rejectReply(503, "ownership_unavailable", command)
    }
    if (performance.now() >= deadline || call.completed || this.#stopped) {
      return rejectReply(503, "bridge_not_active", command)
    }
    call.entered = true
    let result: unknown
    try {
      const { startNode, stopNode, listNodes } = this.#callbacks
      if (command.action === "start") {
        result = await startNode(command.payload as unknown as StartNodeRequest)
      } else if (command.action === "stop") {
        result = await stopNode(command.payload.resource_id as string)
      } else {
        result = await listNodes()
      }
    } catch {
      return rejectReply(502, "callback_failed", command, true)
    }
    try {
      return { status: 200, bytes: callbackSuccess(command, result) }
    } catch (error) {
      return error instanceof CallbackProtocolError && error.code === "limit_exceeded"
        ? rejectReply(413, "limit_exceeded", command, true)
        : rejectReply(502, "invalid_callback_result", command, true)
    }
  }
}

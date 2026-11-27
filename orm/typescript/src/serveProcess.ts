import { spawn, type ChildProcess } from "node:child_process"
import { resolve } from "node:path"
import type { Duplex, Writable } from "node:stream"
import { resolveServerBinary } from "./binary.js"
import { NabedraError } from "./errors.js"
import { encodeJson, hasFields, parseJson } from "./transport.js"

interface StartupPolicy {
  mode: "fixed" | "autoscale"
  desired: number
  min_nodes: number
  max_nodes: number
}

export type ServeStartup = { version: 1; type: "init"; name: string; initial_policy: StartupPolicy } & (
  { provisioning: "external"; bridge: null } |
  { provisioning: "managed"; bridge: { url: string; callback_token: string; authorize_token: string } }
)

export interface ReadyMessage {
  version: 1
  type: "ready"
  cluster_id: string
  bound_url: string
}

interface PreparedMessage extends Omit<ReadyMessage, "type"> {
  type: "prepared"
  authorize_url: string | null
}

export interface SuperviseOptions {
  binary?: string
  coldStart?: boolean
  announcement?: string
  onPrepared?: (message: PreparedMessage) => void
  onReady?: (message: ReadyMessage) => void | Promise<void>
  onStop?: (signal: NodeJS.Signals | null) => void | Promise<void>
  failureSignal?: AbortSignal
}

type Exit = { code: number | null; signal: NodeJS.Signals | null }

const maxFrame = 16 * 1024
const startupMs = 30_000
const frameMs = 2_000
const exitMs = 10_000
const fatalCodes = new Set([
  "invalid_config", "incompatible_cluster", "storage_unavailable", "bind_failed", "protocol_error", "startup_failed",
])
const callbackPort = /^http:\/\/127\.0\.0\.1:([1-9][0-9]{0,4})\/private\/v1\/(authorize|callback)$/
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?![\s\S])/
const signals = ["SIGINT", "SIGTERM"] as const
const interrupted = Symbol("serve interrupted")
let running = false
let unresolvedExit: Promise<Exit> | undefined

export function ensureSupervisorAvailable(): void {
  if (running || unresolvedExit) throw new Error("Serve runtime unavailable")
}

function failure(code = "protocol_error"): NabedraError {
  return new NabedraError("Nabedra Database API process failed.", { code })
}

function validEndpoint(value: unknown, privatePath?: "authorize" | "callback"): value is string {
  if (typeof value !== "string" || !value || Buffer.byteLength(value) > 4096
    || !value.isWellFormed() || /[\x00-\x20\x7f\\#]/.test(value)) return false
  if (privatePath !== undefined) {
    const match = callbackPort.exec(value)
    return match !== null && match[2] === privatePath && Number(match[1]) <= 65535
  }
  try {
    const parsed = new URL(value)
    const authority = /^(?:http|https):\/\/([^/?#]+)/.exec(value)?.[1]
    return authority !== undefined && !authority.includes("@") && Boolean(parsed.hostname) && parsed.port !== "0"
  } catch {
    return false
  }
}

function validToken(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9_-]{43}$/.test(value)
    && Buffer.from(value, "base64url").toString("base64url") === value
}

function validateStartup(value: ServeStartup): void {
  if (!hasFields(value, "version", "type", "name", "initial_policy", "provisioning", "bridge")
      || value.version !== 1 || value.type !== "init" || typeof value.name !== "string"
      || !/^[A-Za-z_][A-Za-z0-9_]{0,63}(?![\s\S])/.test(value.name)) throw failure()
  const policy = value.initial_policy
  if (!hasFields(policy, "mode", "desired", "min_nodes", "max_nodes")
      || ![policy.desired, policy.min_nodes, policy.max_nodes].every(count => Number.isInteger(count) && count >= 0 && count <= 1024)
      || policy.desired !== policy.min_nodes
      || (policy.mode === "fixed" ? policy.desired !== policy.max_nodes
        : policy.mode !== "autoscale" || policy.min_nodes < 1 || policy.desired > policy.max_nodes)) throw failure()
  if (value.provisioning === "external") {
    if (value.bridge !== null || policy.mode !== "fixed") throw failure()
  } else if (value.provisioning === "managed") {
    const bridge = value.bridge
    if (!hasFields(bridge, "url", "callback_token", "authorize_token")
      || !validEndpoint(bridge.url, "callback") || !validToken(bridge.callback_token)
      || !validToken(bridge.authorize_token) || bridge.callback_token === bridge.authorize_token) throw failure()
  } else {
    throw failure()
  }
}

function validateMessage(value: unknown, expected: "prepared", managed: boolean): PreparedMessage
function validateMessage(value: unknown, expected: "ready", managed: boolean): ReadyMessage
function validateMessage(value: unknown, expected: "prepared" | "ready" | "fatal", managed: boolean): PreparedMessage | ReadyMessage
function validateMessage(value: unknown, expected: "prepared" | "ready" | "fatal", managed: boolean): PreparedMessage | ReadyMessage {
  if (hasFields(value, "version", "type", "code") && value.version === 1 && value.type === "fatal"
    && typeof value.code === "string" && fatalCodes.has(value.code)) throw failure(value.code)
  if (expected === "fatal") throw failure()
  const keys = ["version", "type", "cluster_id", "bound_url"]
  if (expected === "prepared") keys.push("authorize_url")
  if (!hasFields(value, keys) || value.version !== 1 || value.type !== expected
    || typeof value.cluster_id !== "string" || !uuid.test(value.cluster_id) || !validEndpoint(value.bound_url)) throw failure()
  if (expected === "prepared" && (managed ? !validEndpoint(value.authorize_url, "authorize") : value.authorize_url !== null)) {
    throw failure()
  }
  return value as unknown as PreparedMessage | ReadyMessage
}

function write(stream: Writable, bytes: Buffer | string): Promise<void> {
  return new Promise((resolve, reject) => {
    stream.write(bytes, error => error ? reject(error) : resolve())
  })
}

function frame(value: object): Buffer {
  const bytes = encodeJson(value)
  if (!bytes.length || bytes.length > maxFrame) throw failure()
  const prefix = Buffer.allocUnsafe(4)
  prefix.writeUInt32BE(bytes.length)
  return Buffer.concat([prefix, bytes], 4 + bytes.length)
}

function readFrame(channel: Duplex, deadline: number | undefined, signal: AbortSignal): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let length: number | undefined
    let done = false
    let timer: NodeJS.Timeout | undefined

    function finish(error?: unknown, record?: unknown): void {
      if (done) return
      done = true
      clearTimeout(timer)
      signal.removeEventListener("abort", cancelled)
      channel.off("readable", read)
      channel.off("end", closed)
      channel.off("close", closed)
      if (error !== undefined) reject(error)
      else resolve(record)
    }

    function cancelled(): void { finish(signal.reason) }
    function closed(): void { finish(failure()) }
    function expire(): void { finish(failure(deadline === undefined ? "protocol_error" : "startup_timeout")) }

    function read(): void {
      try {
        if (deadline !== undefined && deadline <= performance.now()) return expire()
        if (length === undefined) {
          if (deadline === undefined && timer === undefined && channel.readableLength > 0) {
            timer = setTimeout(expire, frameMs)
          }
          const header: Buffer | null = channel.read(4)
          if (header === null) return
          length = header.readUInt32BE()
          if (!length || length > maxFrame) throw failure()
        }
        const bytes: Buffer | null = channel.read(length)
        if (bytes === null) return
        const value = parseJson(bytes, { integers: "bigint" })
        // Frame protocol numeric fields are small integers. Reject float tokens.
        if (typeof value !== "object" || value === null || !("version" in value) || value.version !== 1n) throw failure()
        finish(undefined, { ...value, version: 1 })
      } catch {
        finish(failure())
      }
    }

    if (signal.aborted) return cancelled()
    signal.addEventListener("abort", cancelled, { once: true })
    channel.on("readable", read)
    channel.once("end", closed)
    channel.once("close", closed)
    if (deadline !== undefined) timer = setTimeout(expire, Math.max(0, deadline - performance.now()))
    read()
    if (channel.destroyed || channel.readableEnded) closed()
  })
}

function bounded<T>(operation: Promise<T>, deadline: number, error: unknown, signal?: AbortSignal): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => finish(error), Math.max(0, deadline - performance.now()))
    function finish(reason: unknown, result?: T): void {
      clearTimeout(timeout)
      signal?.removeEventListener("abort", cancelled)
      if (reason !== undefined) reject(reason)
      else resolve(result as T)
    }
    function cancelled(): void { finish(signal?.reason) }
    if (signal?.aborted) cancelled()
    else signal?.addEventListener("abort", cancelled, { once: true })
    operation.then(value => finish(undefined, value), reason => {
      finish(reason ?? error)
    })
  })
}

async function announce(event: string, url: string, deadline: number, signal: AbortSignal): Promise<void> {
  const stdout = process.stdout
  // A closed consumer must fail startup without an unhandled stdout error.
  const ignoreError = () => {}
  const releaseError = () => {
    setImmediate(() => stdout.off("error", ignoreError)).unref()
  }
  stdout.on("error", ignoreError)
  const writing = write(stdout, JSON.stringify({ event, bound_url: url }) + "\n")
  void writing.then(releaseError, releaseError)
  try {
    await bounded(writing, Math.min(deadline, performance.now() + frameMs), failure("announcement_failed"), signal)
  } catch (error) {
    if (error === interrupted || error instanceof NabedraError) throw error
    throw failure("announcement_failed")
  }
}

function watchExit(child: ChildProcess): Promise<Exit> {
  return new Promise(resolve => {
    child.once("exit", (code, signal) => resolve({ code, signal }))
    child.on("error", () => {
      if (child.pid === undefined) resolve({ code: null, signal: null })
    })
  })
}

export async function superviseApi(configPath: string, startup: ServeStartup, options: SuperviseOptions = {}):
  Promise<{ signal: NodeJS.Signals | null }> {
  const {
    binary, coldStart = false, announcement = "nabedra.serve.ready",
    onPrepared, onReady, onStop, failureSignal,
  } = options
  ensureSupervisorAvailable()
  running = true
  const interrupt = new AbortController()
  const received: NodeJS.Signals[] = []
  let child: ChildProcess | undefined
  let channel: Duplex | undefined
  let exit: Promise<Exit> | undefined
  let exitProof: Exit | undefined
  let failed: NabedraError | undefined
  let stopping = false
  let forced = false
  let forwarded = false

  function forward(): void {
    if (!stopping || !child || !child.pid || exitProof) return
    if (received.length > 1) {
      if (!forced) child.kill("SIGKILL")
      forced = true
    } else if (!forwarded && received.length === 1) {
      child.kill(received[0])
      forwarded = true
    }
  }

  function receiveSignal(signal: NodeJS.Signals): void {
    if (received.length < 2) received.push(signal)
    interrupt.abort(interrupted)
    forward()
  }

  const handleFailure = () => interrupt.abort(failure("child_exited"))
  for (const signal of signals) process.on(signal, receiveSignal)
  if (failureSignal?.aborted) handleFailure()
  else failureSignal?.addEventListener("abort", handleFailure, { once: true })
  try {
    validateStartup(startup)
    if (interrupt.signal.aborted) throw interrupt.signal.reason
    const deadline = performance.now() + startupMs
    const args = ["api", "--config", resolve(configPath)]
    if (coldStart) args.push("--cold-start")
    child = spawn(binary ?? resolveServerBinary(), args, {
      env: { ...process.env, NABEDRA_SUPERVISOR_FD: "3" },
      detached: true, stdio: ["ignore", "pipe", "pipe", "pipe"],
    })
    exit = watchExit(child).then(result => {
      exitProof = result
      // An incomplete frame, even with a normal child exit, remains a protocol failure.
      if (child?.pid === undefined) interrupt.abort(failure("startup_failed"))
      return result
    })
    channel = child.stdio[3] as Duplex
    channel.on("error", () => interrupt.abort(failure(child?.pid === undefined ? "startup_failed" : "protocol_error")))
    child.stdout?.resume()
    child.stderr?.resume()

    const managed = startup.provisioning === "managed"
    await bounded(write(channel, frame(startup)), deadline, failure("startup_timeout"), interrupt.signal)
    const prepared = validateMessage(await readFrame(channel, deadline, interrupt.signal), "prepared", managed)
    onPrepared?.(prepared)
    await bounded(write(channel, frame({ version: 1, type: "activate" })), deadline, failure("startup_timeout"), interrupt.signal)
    const ready = validateMessage(await readFrame(channel, deadline, interrupt.signal), "ready", managed)
    if (ready.cluster_id !== prepared.cluster_id || ready.bound_url !== prepared.bound_url) throw failure()
    await onReady?.(ready)
    if (interrupt.signal.aborted) throw interrupt.signal.reason
    await announce(announcement, ready.bound_url, deadline, interrupt.signal)

    validateMessage(await readFrame(channel, undefined, interrupt.signal), "fatal", managed)
    throw failure()
  } catch (error) {
    if (error !== interrupted) {
      failed = error instanceof NabedraError ? failure(error.code) : failure("startup_failed")
    }
  } finally {
    try {
      try {
        await onStop?.(received[0] ?? null)
      } catch {
        failed = failure("shutdown_failed")
      }
      if (child && exit && !exitProof) {
        const deadline = performance.now() + exitMs
        if (channel && !channel.destroyed) {
          // Queue the protocol shutdown before forwarding the OS signal.
          void write(channel, frame({ version: 1, type: "shutdown" })).catch(() => {})
        }
        stopping = true
        forward()
        try {
          await bounded(exit, deadline, failure("forced_termination"))
        } catch {
          forced = true
          if (!exitProof) child.kill("SIGKILL")
          await bounded(exit, performance.now() + exitMs, failure("shutdown_failed"))
        }
      }
      if (forced) failed = failure("forced_termination")
      else if (exitProof && exitProof.code !== 0 && exitProof.signal !== received[0]) {
        failed ??= failure("child_exited")
      }
    } catch {
      failed = failure("shutdown_failed")
    } finally {
      if (exit && !exitProof) {
        // Refuse replacement while the original child has no observed exit.
        unresolvedExit = exit
        void exit.then(() => { unresolvedExit = undefined })
      }
      // Keep the original process observable even if bounded cleanup timed out.
      channel?.destroy()
      child?.stdout?.destroy()
      child?.stderr?.destroy()
      for (const signal of signals) process.off(signal, receiveSignal)
      failureSignal?.removeEventListener("abort", handleFailure)
      running = false
    }
  }
  if (failed) throw failed
  return { signal: received[0] ?? null }
}

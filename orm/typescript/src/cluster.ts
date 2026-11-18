import { randomUUID } from "node:crypto"
import { setTimeout as sleep } from "node:timers/promises"
import { ControlTransport } from "./controlTransport.js"
import { ConflictError, NabedraError, ProtocolError, TransportError, UnknownOutcomeError, ValidationError } from "./errors.js"
import { isJsonObject, type JsonObject } from "./transport.js"
import { validateIdentifier } from "./validation.js"

export type ControlInteger = number | bigint
export interface ClusterPolicy {
  revision: ControlInteger
  mode: "fixed" | "autoscale"
  desired: number
  min_nodes: number
  max_nodes: number
}

export interface WorkerResources {
  max_threads: number
  memory_limit_bytes: ControlInteger
  task_memory_limit_bytes: ControlInteger
  control_memory_reserve_bytes: ControlInteger
}

export interface ClusterNode {
  node_id: string
  incarnation: string | null
  resource_id: string | null
  ownership: "managed" | "external"
  state: "starting" | "ready" | "draining" | "lost" | "stopped"
  operation_slots: number
  partition_slots: number
  running_operations: number
  running_partitions: number
  queued_operations: number
  queued_partitions: number
  heartbeat_age_seconds: ControlInteger | null
  resources?: WorkerResources | null
  available_threads?: number | null
  available_memory_bytes?: ControlInteger | null
}

export interface ClusterIntent {
  request_id: string
  node_id: string
  resource_id: string | null
  action: "start" | "stop"
  state: "pending" | "in_progress" | "reconciling" | "completed" | "failed"
  attempts: ControlInteger
  error_code: string | null
}

export interface ClusterStatus extends ClusterPolicy {
  version: 1
  cluster_id: string
  name: string
  capabilities: Record<"provision" | "terminate" | "autoscale", boolean>
  controller: { epoch: ControlInteger; active: boolean; degraded: boolean }
  counts: Record<"ready" | "starting" | "draining" | "lost", number>
  nodes: ClusterNode[]
  intents: ClusterIntent[]
  metrics_fresh: boolean
}

export interface ClusterOptions {
  timeout?: number
  caFile?: string | null
}

export interface ScaleOptions {
  wait?: boolean
  timeout?: number
}

export interface AutoscaleOptions {
  minNodes: number
  maxNodes: number
}

const policyKeys = ["revision", "mode", "desired", "min_nodes", "max_nodes"]
const statusKeys = [
  ...policyKeys, "version", "cluster_id", "name", "capabilities", "controller", "counts",
  "nodes", "intents", "metrics_fresh",
]
const nodeKeys = [
  "node_id", "incarnation", "resource_id", "ownership", "state", "operation_slots", "partition_slots",
  "running_operations", "running_partitions", "queued_operations", "queued_partitions", "heartbeat_age_seconds",
]
const nodeOptionalKeys = ["resources", "available_threads", "available_memory_bytes"]
const intentKeys = ["request_id", "node_id", "resource_id", "action", "state", "attempts", "error_code"]
const countKeys = ["ready", "starting", "draining", "lost"] as const
const resourceKeys = ["memory_limit_bytes", "task_memory_limit_bytes", "control_memory_reserve_bytes"]
const pendingIntents = new Set(["pending", "in_progress", "reconciling"])
const canonicalUuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/
const maxInt64 = (1n << 63n) - 1n

function record(value: unknown, keys: readonly string[], label: string, optional: string[] = []): JsonObject {
  if (!isJsonObject(value) || keys.some(key => !Object.hasOwn(value, key))
      || Object.keys(value).some(key => !keys.includes(key) && !optional.includes(key))) {
    throw new ValidationError(`${label} must have exactly the specified fields`)
  }
  return value
}

function integer(value: unknown, label: string, low: ControlInteger = 0,
  high: ControlInteger = Infinity): ControlInteger {
  if (typeof value !== "bigint" || value < low || value > high) {
    throw new ValidationError(`${label} is outside its integer range`)
  }
  return value <= Number.MAX_SAFE_INTEGER ? Number(value) : value
}

function nodeCount(value: unknown, label: string, low: number): void {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < low || value > 1024) {
    throw new ValidationError(`${label} is outside its integer range`)
  }
}

function boolean(value: unknown, label: string): void {
  if (typeof value !== "boolean") throw new ValidationError(`${label} must be a boolean`)
}

function choice(value: unknown, choices: readonly string[], label: string): void {
  if (typeof value !== "string" || !choices.includes(value)) throw new ValidationError(`${label} is unsupported`)
}

function uuid(value: unknown, label: string): void {
  if (typeof value !== "string" || !canonicalUuid.test(value)) {
    throw new ValidationError(`${label} must be a canonical UUID`)
  }
}

function resource(value: unknown): void {
  if (value === null) return
  if (typeof value !== "string" || !value) throw new ValidationError("Resource ID must be a nonempty string or null")
  if (!value.isWellFormed() || Buffer.byteLength(value) > 256 || /\p{Cc}/u.test(value)) {
    throw new ValidationError("Resource ID exceeds its bound or contains controls")
  }
}

function policyFields(data: JsonObject): asserts data is JsonObject & ClusterPolicy {
  data.revision = integer(data.revision, "Revision", 1)
  choice(data.mode, ["fixed", "autoscale"], "Policy mode")
  const low = data.mode === "fixed" ? 0 : 1
  const desired = data.desired = integer(data.desired, "desired", low, 1024)
  const min = data.min_nodes = integer(data.min_nodes, "min_nodes", low, 1024)
  const max = data.max_nodes = integer(data.max_nodes, "max_nodes", low, 1024)
  if (min > desired || desired > max) throw new ValidationError("Desired count must be within policy bounds")
  if (data.mode === "fixed" && (min !== desired || desired !== max)) {
    throw new ValidationError("Fixed policy bounds must equal desired count")
  }
}

function node(value: unknown): ClusterNode {
  const data = record(value, nodeKeys, "Node", nodeOptionalKeys)
  uuid(data.node_id, "Node ID")
  if (data.incarnation !== null) uuid(data.incarnation, "Incarnation")
  resource(data.resource_id)
  choice(data.ownership, ["managed", "external"], "Ownership")
  choice(data.state, ["starting", "ready", "draining", "lost", "stopped"], "Node state")
  const hasResources = data.resources != null
  for (const [kind, slotKey] of [["operations", "operation_slots"], ["partitions", "partition_slots"]]) {
    const slots = integer(data[slotKey], slotKey, 1, kind === "partitions" && hasResources ? 1024 : 256)
    data[slotKey] = slots
    data[`running_${kind}`] = integer(data[`running_${kind}`], `Running ${kind}`, 0, slots)
    data[`queued_${kind}`] = integer(data[`queued_${kind}`], `Queued ${kind}`, 0, 4096)
  }
  let working: bigint | undefined
  let ceiling: ControlInteger | undefined
  if (hasResources) {
    const resources = record(data.resources, ["max_threads", ...resourceKeys], "Worker resources")
    ceiling = resources.max_threads = integer(resources.max_threads, "Worker threads", 1, 256)
    if (ceiling > (data.partition_slots as ControlInteger)) {
      throw new ValidationError("Worker job coordination slots must cover its compute threads")
    }
    const [limit, task, reserve] = resourceKeys.map(key => BigInt(resources[key] = integer(resources[key], key, 1, maxInt64)))
    working = limit - reserve
    if (working <= 0n || task > working) throw new ValidationError("Worker memory limits contradict each other")
  }
  const hasThreads = Object.hasOwn(data, "available_threads")
  if (hasThreads !== Object.hasOwn(data, "available_memory_bytes")) {
    throw new ValidationError("Worker availability fields must appear together")
  }
  if (hasThreads) {
    const threads = data.available_threads
    const memory = data.available_memory_bytes
    if ((threads === null) !== (memory === null)) {
      throw new ValidationError("Worker availability fields must both be known or null")
    }
    if (threads !== null) {
      if (working === undefined || ceiling === undefined) {
        throw new ValidationError("Worker availability requires validated resource ceilings")
      }
      data.available_threads = integer(threads, "Available worker threads", 0, ceiling)
      data.available_memory_bytes = integer(memory, "Available worker memory", 0, working)
    }
  }
  const age = data.heartbeat_age_seconds
  if (age !== null && (typeof age === "bigint" ? age < 0n : typeof age !== "number" || !Number.isFinite(age) || age < 0)) {
    throw new ValidationError("Heartbeat age must be finite nonnegative seconds or null")
  }
  if (typeof age === "bigint" && age <= Number.MAX_SAFE_INTEGER) data.heartbeat_age_seconds = Number(age)
  return data as unknown as ClusterNode
}

function intent(value: unknown): ClusterIntent {
  const data = record(value, intentKeys, "Intent")
  uuid(data.request_id, "Intent request ID")
  uuid(data.node_id, "Intent node ID")
  resource(data.resource_id)
  choice(data.action, ["start", "stop"], "Intent action")
  choice(data.state, [...pendingIntents, "completed", "failed"], "Intent state")
  data.attempts = integer(data.attempts, "Intent attempts")
  const code = data.error_code
  if (code !== null && (typeof code !== "string" || !/^[A-Za-z0-9_.:-]{1,256}$/.test(code))) {
    throw new ValidationError("Intent error code must be a bounded machine token or null")
  }
  return data as unknown as ClusterIntent
}

function unique<T extends object>(records: T[], key: keyof T): void {
  const identities = records.map(record => record[key]).filter(value => value !== null)
  if (new Set(identities).size !== identities.length) {
    throw new ValidationError(`Duplicate ${String(key)} in status records`)
  }
}

function status(value: unknown): ClusterStatus {
  const data = record(value, statusKeys, "Status")
  if (data.version !== 1n) throw new ValidationError("Unsupported status version")
  data.version = 1
  uuid(data.cluster_id, "Cluster ID")
  validateIdentifier(data.name)
  policyFields(data)
  const capabilities = record(data.capabilities, ["provision", "terminate", "autoscale"], "Capabilities")
  for (const [key, enabled] of Object.entries(capabilities)) boolean(enabled, key)
  const controller = record(data.controller, ["epoch", "active", "degraded"], "Controller")
  controller.epoch = integer(controller.epoch, "Controller epoch")
  boolean(controller.active, "active")
  boolean(controller.degraded, "degraded")
  const counts = record(data.counts, countKeys, "Counts")
  for (const key of countKeys) counts[key] = integer(counts[key], key)
  boolean(data.metrics_fresh, "Metrics freshness")
  if (!Array.isArray(data.nodes) || !Array.isArray(data.intents)) {
    throw new ValidationError("Nodes and intents must be arrays")
  }
  if (data.nodes.length + data.intents.length > 4096) {
    throw new ValidationError("Combined node/intent count exceeds 4096")
  }
  const nodes = data.nodes.map(node)
  const intents = data.intents.map(intent)
  for (const key of ["node_id", "incarnation", "resource_id"] as const) unique(nodes, key)
  unique(intents, "request_id")
  const actual = { ready: 0, starting: 0, draining: 0, lost: 0 }
  for (const node of nodes) if (node.state !== "stopped") ++actual[node.state]
  if (countKeys.some(key => counts[key] !== actual[key])) {
    throw new ValidationError("Counts do not match node membership states")
  }
  nodes.sort((left, right) => left.node_id.localeCompare(right.node_id))
  intents.sort((left, right) => left.request_id.localeCompare(right.request_id))
  return { ...data, nodes, intents } as unknown as ClusterStatus
}

function remaining(deadline: number, requestId: string): number {
  const seconds = deadline - performance.now() / 1_000
  if (seconds <= 0) {
    throw new TransportError("Accepted policy did not converge before the deadline", {
      code: "wait_timeout", requestId, outcome: null,
    })
  }
  return seconds
}

export class Cluster {
  readonly #transport: ControlTransport

  constructor(apiUrl: string, adminToken: string, options: ClusterOptions = {}) {
    this.#transport = new ControlTransport(apiUrl, adminToken, options)
  }

  async scale(nodes: number, { wait = false, timeout = 300 }: ScaleOptions = {}): Promise<ClusterPolicy> {
    nodeCount(nodes, "Nodes", 0)
    boolean(wait, "Wait")
    if (!Number.isFinite(timeout) || timeout <= 0) throw new ValidationError("Timeout must be finite positive seconds")
    const deadline = wait ? performance.now() / 1_000 + timeout : undefined
    const requestId = randomUUID()
    const accepted = await this.#submit("cluster.scale", { nodes }, { mode: "fixed", desired: nodes }, requestId, deadline)
    if (deadline !== undefined) await this.#wait(accepted, requestId, deadline)
    return accepted
  }

  async autoscale({ minNodes, maxNodes }: AutoscaleOptions): Promise<ClusterPolicy> {
    nodeCount(minNodes, "Minimum nodes", 1)
    nodeCount(maxNodes, "Maximum nodes", 1)
    if (minNodes > maxNodes) throw new ValidationError("Minimum nodes must not exceed maximum nodes")
    const bounds = { min_nodes: minNodes, max_nodes: maxNodes }
    return this.#submit("cluster.autoscale", bounds, { mode: "autoscale", ...bounds })
  }

  status(): Promise<ClusterStatus> {
    return this.#readStatus()
  }

  async #wait(accepted: ClusterPolicy, requestId: string, deadline: number): Promise<void> {
    while (true) {
      remaining(deadline, requestId)
      let current: ClusterStatus
      try {
        current = await this.#readStatus(deadline)
      } catch (error) {
        if (error instanceof NabedraError) remaining(deadline, requestId)
        throw error
      }
      remaining(deadline, requestId)
      if (current.revision !== accepted.revision) {
        const error = new ConflictError("Accepted policy was superseded", { code: "policy_superseded", requestId })
        error.outcome = null
        throw error
      }
      if (current.counts.ready === accepted.desired && !current.counts.starting && !current.counts.draining
          && !current.intents.some(intent => pendingIntents.has(intent.state))) return
      await sleep(Math.min(0.1, remaining(deadline, requestId)) * 1_000)
    }
  }

  async #submit(operation: string, payload: JsonObject, expected: JsonObject, requestId?: string,
    deadline?: number): Promise<ClusterPolicy> {
    const [id, value] = await this.#transport.request(operation, payload, { requestId, deadline })
    try {
      const data = record(value, policyKeys, "Policy acceptance")
      policyFields(data)
      if (Object.entries(expected).some(([key, value]) => data[key] !== value)) {
        throw new ValidationError("Policy acceptance does not match requested policy")
      }
      return data
    } catch (error) {
      if (!(error instanceof ValidationError)) throw error
      throw new UnknownOutcomeError(error.message, { requestId: id, statusCode: 200 })
    }
  }

  async #readStatus(deadline?: number): Promise<ClusterStatus> {
    const [requestId, value] = await this.#transport.request("cluster.status", {}, { deadline })
    try {
      return status(value)
    } catch (error) {
      if (!(error instanceof ValidationError)) throw error
      throw new ProtocolError(error.message, { requestId, statusCode: 200 })
    }
  }
}

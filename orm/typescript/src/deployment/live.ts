import { execFile } from "node:child_process"
import { isAbsolute } from "node:path"
import { setTimeout as sleep } from "node:timers/promises"
import { isDeepStrictEqual as same } from "node:util"
import { NabedraError } from "../errors.js"
import { ProviderError } from "../providers.js"
import { encodeJson, parseJson, type JsonObject } from "../transport.js"
import { advanceGrowth, growthEndpoint, growthOperation, growthRead, type GrowthController } from "./controller.js"
import { array, authority, InventoryUnavailable, list, max, metadata, plannerWorker, remaining,
  rosterMatches, spareMove, splitDemand, splitMoveId, uint, workerIdentity, type InventorySamples } from "./growthPlanning.js"
import type { GrowthTarget, ProviderObservation, StatefulGrowthAdapter } from "./growthTypes.js"
import { canonicalJson, equalWire, identity, monotonic, trustedEndpoint, uuid5 } from "./shared.js"

export { GrowthTarget, HostObservation, ProviderObservation, type StatefulGrowthAdapter } from "./growthTypes.js"

type Endpoints = Readonly<Record<string, GrowthController>>

interface WorkerAdapter extends StatefulGrowthAdapter {
  bindEndpoints?(endpoints: Endpoints): StatefulGrowthAdapter
  provisionWorker?(intent: JsonObject, target: GrowthTarget, options: { deadline: number; authorize: () => Promise<void> }):
    JsonObject | Promise<JsonObject>
  registerWorker?(intent: JsonObject, target: GrowthTarget, record: JsonObject, options: {
    controller: GrowthController; pin: JsonObject | null; deadline: number
  }): JsonObject | Promise<JsonObject>
}

export interface AutomaticGrowthOptions {
  controller: GrowthController
  workers: Endpoints
  adapter: StatefulGrowthAdapter | null
  planner?: readonly string[] | null
  timeout?: number
}

type ReconcileResult = JsonObject & { state: string }

const moving = (sample: JsonObject) => Object.hasOwn(sample, "split_replica_move")

function runPlanner(command: readonly string[], lines: JsonObject[], deadline: number): Promise<unknown> {
  const body = Buffer.concat(lines.flatMap(line => [encodeJson(line), Buffer.from("\n")]))
  return new Promise((resolve, reject) => {
    const child = execFile(command[0], [...command.slice(1), "growth-plan"], {
      timeout: Math.max(1, Math.ceil(remaining(deadline) * 1_000)), killSignal: "SIGKILL",
      maxBuffer: 32 * 1024, encoding: "buffer",
    }, (error, output) => {
      if (error) { reject(error); return }
      try {
        resolve(parseJson(output))
      } catch (parseError) {
        reject(parseError)
      }
    })
    child.stdin?.on("error", () => { /* The child exit callback reports failures. */ })
    child.stdin?.end(body)
  })
}

export class AutomaticGrowth {
  readonly controller: GrowthController
  readonly workers: Endpoints
  readonly adapter: WorkerAdapter | null
  readonly planner: readonly string[]
  readonly timeout: number

  constructor({ controller, workers, adapter, planner, timeout = 60 }: AutomaticGrowthOptions) {
    this.controller = controller
    this.workers = Object.freeze(Object.fromEntries(Object.entries(workers).map(([key, endpoint]) => [identity(key), endpoint])))
    this.planner = Object.freeze([...(planner ?? [])])
    this.timeout = timeout
    if (adapter === null) { this.adapter = null; return }
    if (!Array.isArray(planner) || !this.planner.length || typeof this.planner[0] !== "string"
      || !isAbsolute(this.planner[0]) || !Number.isFinite(timeout) || timeout < 1 || timeout > 120
      || Object.keys(this.workers).length > 4_096
      || ![controller, ...Object.values(this.workers)].every(endpoint => trustedEndpoint(growthEndpoint(endpoint)))) {
      throw new ProviderError("Automatic growth requires bounded local planner and authenticated worker URLs")
    }
    const provider = adapter as WorkerAdapter
    this.adapter = provider.bindEndpoints?.(this.workers) ?? provider
  }

  async iterate(): Promise<ReconcileResult> {
    if (this.adapter === null) return { state: "unsupported", reason: "adapter_unavailable" }
    const deadline = monotonic() + this.timeout
    try {
      const snapshot = await this.#snapshot(deadline)
      const policy = metadata(snapshot.policy)
      const pin = metadata(snapshot.pin)
      const auto = policy.automatic == null ? null : metadata(policy.automatic)
      if (auto === null || auto.adapter_id !== this.adapter.adapterId) {
        return { state: "unsupported", reason: "adapter_policy_unavailable" }
      }
      const active = snapshot.active === null ? null : metadata(snapshot.active)
      if (active && !snapshot.active_automatic) return { state: "manual_pending", claim: active }
      const clusterId = identity(snapshot.cluster_id)
      const enrollments = await this.#roster(pin, Number(uint(auto.max_workers)), deadline)
      const first = await this.#samples(pin, enrollments, clusterId, deadline, active)
      if (active) return await this.#reconcile(snapshot, metadata(active.intent), first, deadline)

      const moves = Object.entries(first).filter(([, sample]) => moving(sample))
      if (moves.length) {
        if (moves.length !== 1) throw new InventoryUnavailable("Several workers report the placement owner")
        const [[worker, sample]] = moves
        const record = metadata(sample.split_replica_move)
        const request = metadata(record.request)
        const currentAuthority = authority(metadata(pin.ready))
        if (!same(record.owner, enrollments[worker]) || !same(record.authority, currentAuthority)
          || record.id !== splitMoveId(policy, currentAuthority, request)) {
          return { state: "placement_pending", reason: "existing_replica_move" }
        }
        if (!same((await this.#snapshot(deadline)).pin, pin)) throw new InventoryUnavailable("Control inventory changed before placement resume")
        return await this.#move(worker, policy, currentAuthority, request, deadline)
      }

      const observeSeconds = Number(uint(auto.sample_seconds))
      if (remaining(deadline) <= observeSeconds) throw new InventoryUnavailable("Deadline is shorter than the configured observation window")
      await sleep(observeSeconds * 1_000)
      const second = await this.#samples(pin, enrollments, clusterId, deadline)
      let provider: ProviderObservation
      try {
        provider = await this.adapter.observe(second, { policy, deadline })
      } catch (error) {
        if (!(error instanceof ProviderError)) throw error
        throw new InventoryUnavailable("Provider inventory unavailable")
      }
      if (!rosterMatches(provider.hosts, enrollments)) {
        throw new InventoryUnavailable("Provider inventory does not cover the pinned worker roster")
      }
      const rows = Object.entries(second).map(([worker, current]) => plannerWorker(first[worker], current, provider.hosts[worker], auto))
      if (Object.values(second).some(moving)) return { state: "placement_pending", reason: "existing_replica_move" }

      const demand = splitDemand(pin, second, provider, policy)
      if (!demand && Object.values(first).some(sample => Object.hasOwn(sample, "split_deficit"))) {
        throw new InventoryUnavailable("Exact split observation expired during sampling")
      }
      if (demand) {
        if (!demand.needs.length) return { state: "idle" }
        const move = spareMove(demand, second, rows, policy)
        if (move) {
          const [fresh, samples] = await this.#refreshSplit(demand, snapshot, enrollments, provider, deadline,
            [identity(metadata(move.worker).worker_id)])
          if (!same(spareMove(fresh, samples, rows, policy), move)) {
            throw new InventoryUnavailable("Spare pool or split source changed")
          }
          return await this.#move(demand.owner, policy, authority(metadata(pin.ready)), move, deadline)
        }
      }

      const planned = await this.#plan(snapshot, provider, rows, deadline, demand)
      if (!same((await this.#snapshot(deadline)).pin, pin)) throw new InventoryUnavailable("Control inventory changed during planning")
      if (Object.hasOwn(planned, "refusal")) return { state: "refused", reason: planned.refusal }
      const plan = metadata(planned.plan)
      const [action] = list(plan.actions)
      if (!action) return { state: "idle" }
      const claimIdentity = demand ? [pin, action, demand.record.generation] : [pin, action]
      const claimId = uuid5(identity(policy.policy_id), canonicalJson(claimIdentity))
      const target = await this.#target(claimId, action, second)
      if (!target) return { state: "unsupported", reason: "target_unavailable", action }
      if (action.kind === "add_worker" && !Object.hasOwn(this.workers, identity(target.worker.worker_id))) {
        return { state: "unsupported", reason: "worker_inventory_endpoint_unavailable", action }
      }
      if (demand) {
        const [fresh] = await this.#refreshSplit(demand, snapshot, enrollments, provider, deadline)
        const previous = Object.fromEntries(demand.needs.map(need => [need.pool_id, need.free_bytes]))
        const current = Object.fromEntries(fresh.needs.map(need => [need.pool_id, need.free_bytes]))
        if (!rosterMatches(previous, current) || Object.entries(current).some(([pool, available]) => available < previous[pool])) {
          throw new InventoryUnavailable("Exact split capacity changed during planning")
        }
      }
      const intent = {
        policy_id: policy.policy_id, claim_id: claimId, action, worker: target.worker, pool_id: target.poolId,
        pool_generation: target.poolGeneration, quoted_monthly_cost: array(plan.action_monthly_costs)[0],
        monthly_spend_ceiling: policy.monthly_spend_ceiling, max_attempts: policy.max_attempts, cooldown_seconds: policy.cooldown_seconds,
      }
      const owner = this.#owner(intent)
      await growthOperation(owner, claimId, "claim-automatic", "storage.growth.claim_automatic", { intent, pin }, { deadline })
      return await this.#reconcile(snapshot, intent, second, deadline, target)
    } catch (error) {
      if (error instanceof InventoryUnavailable) return { state: "incomplete", reason: "live_inventory_unavailable" }
      if (error instanceof NabedraError) return error.code === "unsupported_operation"
        ? { state: "unsupported", reason: "automatic_policy_unavailable" }
        : { state: "incomplete", reason: error.code }
      if (error instanceof ProviderError) return { state: "pending", reason: "provider_reconciliation_required" }
      throw error
    }
  }

  async #snapshot(deadline: number): Promise<JsonObject> {
    const snapshot = await growthRead(this.controller, "storage.growth.snapshot", {}, { deadline })
    if (snapshot.cluster_id !== this.adapter!.clusterId) {
      throw new InventoryUnavailable("Control and provider cluster identities differ")
    }
    return snapshot
  }

  async #roster(pin: JsonObject, maximum: number, deadline: number): Promise<InventorySamples> {
    const expected = uint(metadata(pin.ready).ready_count)
    if (!expected || expected > maximum || maximum > 4_096) {
      throw new InventoryUnavailable("Ready roster exceeds the automatic policy ceiling")
    }
    const enrollments: InventorySamples = {}
    let after: string | null = null
    for (let index = 0; index <= maximum; ++index) {
      const page: JsonObject = await growthRead(this.controller, "storage.growth.workers", { pin, after }, { deadline })
      const workers = list(page.workers)
      if (!same(page.pin, pin) || workers.length > 32 || (!workers.length && page.done !== true)) {
        throw new InventoryUnavailable("Worker inventory page is invalid or stale")
      }
      for (const enrollment of workers) {
        const worker = workerIdentity(enrollment)
        const current = identity(worker.worker_id)
        if ((after !== null && current <= after) || Object.keys(enrollments).length >= expected) {
          throw new InventoryUnavailable("Worker roster order or count differs")
        }
        enrollments[current] = worker
        after = current
      }
      if (page.after !== after) throw new InventoryUnavailable("Worker inventory cursor differs")
      if (page.done === true) {
        if (BigInt(Object.keys(enrollments).length) !== expected) throw new InventoryUnavailable("Worker roster size differs")
        return enrollments
      }
    }
    throw new InventoryUnavailable("Worker inventory page limit reached")
  }

  async #samples(pin: JsonObject, enrollments: InventorySamples, clusterId: string, deadline: number,
    active: JsonObject | null = null): Promise<InventorySamples> {
    const samples: InventorySamples = {}
    for (const [workerId, worker] of Object.entries(enrollments)) {
      const endpoint = this.workers[workerId]
      if (!endpoint) throw new InventoryUnavailable("A ready worker has no configured authenticated endpoint")
      const sample = await growthRead(endpoint, "storage.growth.sample", { pin, worker }, { deadline })
      const inventory = metadata(sample.inventory)
      const pools = list(inventory.pools)
      const limit = uint(inventory.local_open_pool_limit)
      if (!same(sample.pin, pin) || !same(inventory.worker, worker) || inventory.ready !== true
        || !pools.length || pools.length > limit || limit > 8n) {
        throw new InventoryUnavailable("Live worker inventory differs from the Control enrollment")
      }
      for (const pool of pools) {
        const mount = metadata(pool.expected)
        const pending = active?.phase === "awaiting_mount" && pool.growth_claim === metadata(active.intent).claim_id
          && pool.pool_id === metadata(active.intent).pool_id
        const identity = metadata(mount.identity)
        if (!same(mount.worker, worker) || (pool.placement_eligible !== true && !pending)
          || identity.cluster_id !== clusterId || identity.pool_id !== pool.pool_id || !pool.mount_id) {
          throw new InventoryUnavailable("A registered pool is not verified on its owning worker")
        }
      }
      samples[workerId] = sample
    }
    return samples
  }

  async #plan(snapshot: JsonObject, provider: ProviderObservation, rows: JsonObject[], deadline: number,
    demand: ReturnType<typeof splitDemand>): Promise<JsonObject> {
    const policy = metadata(snapshot.policy)
    const pin = metadata(snapshot.pin)
    const excess = max(0n, uint(provider.monthlySpendFloor) - uint(policy.initial_monthly_spend))
    const revision = pin.growth_revision
    const head: JsonObject = { policy, now: Math.floor(Date.now() / 1_000), revision, last_attempt: snapshot.last_attempt,
      monthly_spend: uint(uint(pin.monthly_spend) + excess), retained_workers: uint(provider.retainedWorkers),
      unavailable_worker_zones: array(policy.zones).filter(zone => !provider.workerZones.has(String(zone))) }
    if (demand) head.split_needs = demand.needs.map(({ free_bytes: _, ...need }) => need)
    const lines = [head]
    for (let index = 0; index < rows.length; index += 32) {
      lines.push({ revision, workers: rows.slice(index, index + 32), done: index + 32 >= rows.length })
    }
    try {
      return metadata(await runPlanner(this.planner, lines, deadline))
    } catch {
      throw new InventoryUnavailable("Local growth planner failed")
    }
  }

  async #refreshSplit(demand: NonNullable<ReturnType<typeof splitDemand>>, snapshot: JsonObject,
    enrollments: InventorySamples, provider: ProviderObservation, deadline: number, extra: string[] = []) {
    const pin = metadata(snapshot.pin)
    const workers = new Set([demand.owner, ...extra, ...demand.members.map(member => identity(member.worker_id))])
    const samples = await this.#samples(pin, Object.fromEntries([...workers].map(worker => [worker, enrollments[worker]])),
      identity(snapshot.cluster_id), deadline)
    const fresh = splitDemand(pin, samples, provider, metadata(snapshot.policy))
    if (!fresh || fresh.owner !== demand.owner || !same(fresh.anchor, demand.anchor)
      || Object.values(samples).some(moving) || !same((await this.#snapshot(deadline)).pin, pin)) {
      throw new InventoryUnavailable("Exact split source changed during planning")
    }
    return [fresh, samples] as const
  }

  async #move(worker: string, policy: JsonObject, authority: JsonObject, request: JsonObject, deadline: number): Promise<ReconcileResult> {
    const id = splitMoveId(policy, authority, request)
    const record = await growthRead(this.workers[worker], "storage.replica.move", request, { requestId: id, deadline })
    if (record.id !== id || !equalWire(record.request, request) || !same(record.authority, authority)) {
      throw new InventoryUnavailable("Replica move result differs from the durable request")
    }
    const state = metadata(record.phase).state
    if (state === "cancelled") return { state: "refused", reason: "replica_move_cancelled", move: record }
    return { state: state === "retired" ? "resample" : "placement_pending", move: record }
  }

  #owner(intent: JsonObject): GrowthController {
    if (metadata(intent.action).kind === "add_worker") return this.controller
    const endpoint = this.workers[identity(metadata(intent.worker).worker_id)]
    if (!endpoint) throw new InventoryUnavailable("Growth owner has no authenticated endpoint")
    return endpoint
  }

  async #target(claimId: string, action: JsonObject, inventories: InventorySamples): Promise<GrowthTarget | null> {
    const target = await this.adapter!.target(claimId, action, inventories)
    if (target === null) return null
    identity(target.poolId)
    if (uint(target.poolGeneration) === 0n) throw new InventoryUnavailable("Growth target generation is missing")
    if (action.kind === "add_worker") {
      if (Object.hasOwn(inventories, identity(target.worker.worker_id))) {
        throw new InventoryUnavailable("The growth worker already belongs to the ready set")
      }
      return target
    }
    const inventory = metadata(metadata(inventories[identity(action.worker)]).inventory)
    if (!same(inventory.worker, target.worker)) throw new InventoryUnavailable("Growth target identity differs from the pinned worker")
    const pool = list(inventory.pools).find(row => row.pool_id === target.poolId)
    if (action.kind === "expand") {
      if (!pool || target.poolId !== action.pool || uint(target.poolGeneration) !== uint(metadata(pool.expected).generation)
        || target.resourceId !== metadata(pool.expected).provider_volume_id) {
        throw new InventoryUnavailable("Expansion target differs from the observed mounted pool")
      }
    } else if (pool && (pool.growth_claim !== claimId || uint(metadata(pool.expected).generation) !== uint(target.poolGeneration)
      || (target.resourceId !== null && metadata(pool.expected).provider_volume_id !== target.resourceId))) {
      throw new InventoryUnavailable("Pool addition would reuse an active pool")
    }
    return target
  }

  async #reconcile(snapshot: JsonObject, intent: JsonObject, inventories: InventorySamples, deadline: number,
    chosen?: GrowthTarget): Promise<ReconcileResult> {
    const claimId = identity(intent.claim_id)
    const action = metadata(intent.action)
    const worker = metadata(intent.worker)
    const workerId = identity(worker.worker_id)
    const adapter = this.adapter!
    const target = chosen ?? await this.#target(claimId, action, inventories)
    if (!target) return { state: "pending", reason: "target_unavailable", claim: snapshot.active }
    if (!same(target.worker, worker) || target.poolId !== intent.pool_id || uint(target.poolGeneration) !== uint(intent.pool_generation)) {
      throw new InventoryUnavailable("Provider target differs from the durable claim")
    }
    const owner = this.#owner(intent)
    const original = metadata(snapshot.pin)
    const registerWorker = action.kind === "add_worker" ? adapter.registerWorker?.bind(adapter) : undefined
    const provisionWorker = action.kind === "add_worker" ? adapter.provisionWorker?.bind(adapter) : undefined
    let registrationPin: JsonObject | null = null
    let provisioningAttempts: number | undefined
    const authorize = async (phase: "provisioning" | "awaiting_mount", attempts: number) => {
      const current = await this.#snapshot(deadline)
      const pin = metadata(current.pin)
      const active = metadata(current.active)
      if (["policy_id", "ready", "enrollment_revision", "group_revision"].some(key => !same(pin[key], original[key]))
        || !current.active_automatic || !equalWire(active.intent, intent) || active.phase !== phase || active.attempts !== attempts) {
        throw new InventoryUnavailable("The current Control claim or worker roster changed")
      }
      if (action.kind !== "add_worker") {
        const fresh = await this.#samples(pin, { [workerId]: worker }, identity(current.cluster_id), deadline, active)
        const updated = await this.#target(claimId, action, fresh)
        if (!updated || !same(updated.worker, target.worker) || updated.poolId !== target.poolId
          || uint(updated.poolGeneration) !== uint(target.poolGeneration) || updated.resourceId !== target.resourceId) {
          throw new InventoryUnavailable("Growth target mount or provider identity changed")
        }
      }
      if (phase === "provisioning") {
        const policy = metadata(current.policy)
        const provider = await adapter.observe(inventories, { policy, deadline, ...(registerWorker && { claimId }) })
        if (!rosterMatches(provider.hosts, inventories)) {
          throw new InventoryUnavailable("Provider inventory no longer covers the pinned roster")
        }
        const extra = max(0n, uint(provider.monthlySpendFloor) - uint(policy.initial_monthly_spend))
        if (uint(pin.monthly_spend) + extra > uint(policy.monthly_spend_ceiling)) {
          throw new ProviderError("Retained resource spend exceeds the current growth ceiling")
        }
        if (action.kind === "add_worker") {
          if (BigInt(Object.keys(inventories).length) + uint(provider.retainedWorkers) >= uint(metadata(policy.automatic).max_workers)
            || !provider.workerZones.has(String(action.zone))) throw new ProviderError("Provider worker ceiling or zone capability changed")
        } else {
          const host = provider.hosts[workerId]
          if (!host || !same(host.worker, worker)) throw new InventoryUnavailable("Provider worker incarnation changed")
          if ((action.kind === "add_pool" && host.attachmentSlots <= 0)
            || (action.kind === "expand" && !host.expandableResources.has(target.resourceId!))) {
            throw new ProviderError("Provider attachment or expansion headroom changed")
          }
        }
        const latest = await this.#snapshot(deadline)
        if (!same(latest.pin, pin) || !same(latest.active, active)) {
          throw new InventoryUnavailable("Control inventory changed during provider inspection")
        }
        provisioningAttempts = attempts
      } else if (registerWorker) {
        registrationPin = pin
      }
    }

    let registration: JsonObject | undefined
    const record = await advanceGrowth(owner, intent, async intent => {
      const observed = provisionWorker
        ? await provisionWorker(intent, target, { deadline, authorize: () => authorize("provisioning", provisioningAttempts!) })
        : await adapter.provision(intent, target, { deadline })
      if (target.resourceId !== null && observed.resource_id !== target.resourceId) {
        throw new ProviderError("Provider acknowledged another resource")
      }
      return observed
    }, {
      deadline, authorize,
      async register(intent, record) {
        if (target.resourceId !== null && record.provider_resource !== target.resourceId) {
          throw new ProviderError("Claim names another provider resource")
        }
        if (registerWorker) {
          registration = await registerWorker(intent, target, record, { controller: owner, pin: registrationPin, deadline })
          return null
        }
        return adapter.register(intent, target, record, { controller: owner, deadline })
      },
    })
    const result = { state: "pending", ...registration }
    if (record.phase === "complete") result.state = registerWorker || action.kind === "expand" ? "mounted" : "awaiting_placement"
    return { ...result, claim: record }
  }
}

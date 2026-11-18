import { isDeepStrictEqual as same } from "node:util"
import { ProviderError } from "../providers.js"
import { isJsonObject, type JsonObject } from "../transport.js"
import type { HostObservation, ProviderObservation } from "./growthTypes.js"
import { canonicalJson, identity, monotonic, uuid5 } from "./shared.js"

const nanoseconds = 1_000_000_000n
const maxNodeId = (1n << 63n) - 1n

export class InventoryUnavailable extends ProviderError {}

export type InventorySamples = Record<string, JsonObject>

interface SplitNeed extends JsonObject {
  worker_id: string
  pool_id: string
  zone: string
  required_bytes: bigint
  replacement_bytes: bigint
  can_move_to_local_pool: boolean
  can_move_to_new_worker: boolean
  free_bytes: bigint
}

interface SplitDemand {
  owner: string
  record: JsonObject
  anchor: JsonObject
  members: JsonObject[]
  needs: SplitNeed[]
}

export function metadata(value: unknown): JsonObject {
  if (!isJsonObject(value)) throw new InventoryUnavailable("Invalid inventory record")
  return value
}

export function array(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new InventoryUnavailable("Expected an inventory array")
  return value
}

export function list(value: unknown): JsonObject[] {
  return array(value).map(metadata)
}

export function uint(value: unknown): bigint {
  if (typeof value !== "bigint" && !Number.isSafeInteger(value)) {
    throw new InventoryUnavailable("Inventory counter is missing or out of range")
  }
  const integer = BigInt(value as number | bigint)
  if (integer < 0n || integer >= 1n << 64n) {
    throw new InventoryUnavailable("Inventory counter is missing or out of range")
  }
  return integer
}

export function remaining(deadline: number): number {
  const seconds = deadline - monotonic()
  if (seconds <= 0) throw new InventoryUnavailable("Automatic growth iteration deadline expired")
  return seconds
}

export function rosterMatches(records: object, enrolled: object): boolean {
  return same(Object.keys(records).sort(), Object.keys(enrolled).sort())
}

export function workerIdentity(enrollment: JsonObject): JsonObject {
  return Object.fromEntries(["worker_id", "incarnation", "certificate_sha256", "enrollment_revision"]
    .map(key => [key, enrollment[key]]))
}

function min(left: bigint, right: bigint): bigint {
  return left < right ? left : right
}

export function max(left: bigint, right: bigint): bigint {
  return left > right ? left : right
}

export function authority(ready: JsonObject): JsonObject {
  const view = metadata(ready.view)
  return { run_id: view.run_id, owner_term: view.owner_term }
}

export function splitMoveId(policy: JsonObject, authority: JsonObject, request: JsonObject): string {
  return uuid5(identity(policy.policy_id), "automatic-split-replica-move/" + canonicalJson([authority, request]))
}

export function splitDemand(pin: JsonObject, samples: InventorySamples, provider: ProviderObservation,
  policy: JsonObject): SplitDemand | null {
  const reported = Object.entries(samples).filter(([, sample]) => Object.hasOwn(sample, "split_deficit"))
  if (reported.length === 0) return null
  if (reported.length !== 1) throw new InventoryUnavailable("Several workers report an exact split deficit")
  const [[owner, sample]] = reported
  const deficit = metadata(sample.split_deficit)
  const source = metadata(deficit.source)
  const record = metadata(source.record)
  identity(metadata(record.identity).group_id)
  identity(record.generation)
  const voters = list(source.voters)
  const missing = list(deficit.pools)
  if (!same(deficit.ready, pin.ready) || record.replica_count !== 3 || voters.length !== 3 || missing.length < 1 || missing.length > 3) {
    throw new InventoryUnavailable("Split deficit differs from the pinned RF3 source")
  }
  const required = uint(deficit.required_bytes)
  const committed = uint(deficit.observed_committed_bytes)
  if (required <= committed * 3n) throw new InventoryUnavailable("Split deficit is missing its copy and settlement reserve")

  const members: JsonObject[] = []
  const mounts = new Map<string, { pool: JsonObject; worker: JsonObject; zone: string }>()
  const zones = new Set<string>()
  const initial = metadata(policy.initial_replica_zones ?? {})
  for (const voter of voters) {
    const member = metadata(voter.member)
    const worker = metadata(voter.worker)
    const workerId = identity(worker.worker_id)
    const poolId = identity(member.pool_id)
    const inventory = samples[workerId] ? metadata(samples[workerId].inventory) : null
    const host = provider.hosts[workerId]
    if (!inventory || !host || !same(inventory.worker, worker) || !same(host.worker, worker)
      || !array(policy.zones).includes(host.zone) || zones.has(host.zone)
      || (initial[workerId] ?? host.zone) !== host.zone || member.worker_id !== workerId
      || mounts.has(poolId) || members.some(old => old.worker_id === workerId || old.node_id === member.node_id)) {
      throw new InventoryUnavailable("Source voter or failure domain changed")
    }
    const pool = list(inventory.pools).find(row => row.pool_id === poolId)
    if (!pool || (voter.registered_pool_generation !== null && voter.registered_pool_generation !== metadata(pool.expected).generation)) {
      throw new InventoryUnavailable("Source pool generation changed")
    }
    members.push(member)
    mounts.set(poolId, { pool, worker, zone: host.zone })
    zones.add(host.zone)
  }

  const needs: SplitNeed[] = []
  const seen = new Set<string>()
  for (const entry of missing) {
    const poolId = identity(entry.pool)
    const mount = mounts.get(poolId)
    if (seen.has(poolId) || !mount) throw new InventoryUnavailable("Split deficit names another pool")
    seen.add(poolId)
    const { pool, worker, zone } = mount
    const expected = metadata(pool.expected)
    const needed = required * (isJsonObject(expected.profile) ? 3n : 1n)
    if (!same(entry.worker, worker) || entry.pool_generation !== expected.generation
      || uint(entry.required_bytes) !== needed || uint(entry.available_bytes) >= needed) {
      throw new InventoryUnavailable("Split deficit differs from the exact native mount")
    }
    const free = uint(pool.observed_available_bytes)
    if (free >= needed) continue
    const id = identity(worker.worker_id)
    needs.push({ worker_id: id, pool_id: poolId, zone, required_bytes: needed, replacement_bytes: uint(required + committed),
      can_move_to_local_pool: id === owner, can_move_to_new_worker: id === owner && source.domains !== null, free_bytes: free })
  }
  const anchor = Object.fromEntries(["ready", "source", "epoch", "required_bytes"].map(key => [key, deficit[key]]))
  anchor.mounts = Object.fromEntries([...mounts].map(([poolId, { pool }]) => [poolId, metadata(pool.expected).generation]))
  return { owner, record, anchor, members, needs }
}

export function spareMove(demand: SplitDemand, samples: InventorySamples, rows: JsonObject[],
  policy: JsonObject): JsonObject | null {
  const workers = Object.fromEntries(rows.map(worker => [identity(worker.worker_id), worker]))
  const initial = metadata(policy.initial_replica_zones ?? {})
  const auto = metadata(policy.automatic)
  const reserve = uint(auto.settlement_reserve_bytes) + uint(auto.compaction_reserve_bytes)
  const domains = metadata(demand.anchor.source).domains
  for (const need of demand.needs) {
    const { worker_id: workerId, pool_id: poolId, zone } = need
    const source = demand.members.find(member => member.worker_id === workerId)
    const old = list(metadata(samples[workerId].inventory).pools).find(pool => pool.pool_id === poolId)
    if (!source || !old) throw new InventoryUnavailable("Exact split source is unavailable")
    const retained = demand.members.filter(member => member !== source)
    for (const targetId of Object.keys(samples).sort()) {
      const sample = samples[targetId]
      const target = workers[targetId]
      if (!target || target.zone !== zone || retained.some(member => member.worker_id === targetId)
        || uint(target.cpu_used) >= uint(target.cpu_limit) || uint(target.io_used) >= uint(target.io_limit)
        || uint(target.memory_used) >= uint(target.memory_limit)
        || uint(target.resident_shards) + (targetId === workerId ? 2n : 3n) > uint(target.resident_shard_limit)) continue
      const inventory = metadata(sample.inventory)
      for (const pool of list(inventory.pools).sort((a, b) => String(a.pool_id).localeCompare(String(b.pool_id)))) {
        const targetPool = identity(pool.pool_id)
        const expected = metadata(pool.expected)
        if (targetPool === poolId || retained.some(member => member.pool_id === targetPool)) continue
        const docker = isJsonObject(expected.profile)
        if ((targetId === workerId && same(pool.device, old.device)) || (docker && targetId !== workerId)) continue
        if (workerId !== demand.owner) {
          if (targetId !== demand.owner || Object.hasOwn(initial, workerId) || initial[targetId] !== zone
            || pool.growth_claim == null || domains === null) continue
        } else if (targetId !== demand.owner && (Object.hasOwn(initial, targetId) || domains === null)) continue
        const planned = list(target.pools).find(item => item.pool_id === targetPool)
        if (!planned) throw new InventoryUnavailable("Spare pool is missing from the planner observation")
        const required = need.replacement_bytes + reserve + uint(planned.write_bytes_per_second) * uint(auto.provisioning_seconds)
          + (docker ? uint(demand.anchor.required_bytes) * 2n : 0n)
        if (planned.healthy !== true || uint(pool.observed_available_bytes) < required) continue
        const generation = identity(demand.record.generation)
        const node = BigInt("0x" + uuid5(generation, targetId + "/" + targetPool).replaceAll("-", "")) & maxNodeId
        if (node === 0n || demand.members.some(member => uint(member.node_id) === node)) continue
        return { group: demand.record.identity, expected_generation: generation, remove_node: source.node_id,
          worker: inventory.worker, target: { node_id: node, worker_id: targetId, pool_id: targetPool }, pool_generation: expected.generation }
      }
    }
  }
  return null
}

export function plannerWorker(first: JsonObject, second: JsonObject, host: HostObservation, auto: JsonObject): JsonObject {
  const inventory = metadata(second.inventory)
  const before = metadata(first.usage)
  const after = metadata(second.usage)
  if (!same(host.worker, inventory.worker)) throw new InventoryUnavailable("Provider observation names another worker incarnation")
  const delta = (key: string) => uint(uint(after[key]) - uint(before[key]))
  const elapsed = delta("monotonic_ns")
  if (elapsed === 0n || elapsed < uint(auto.sample_seconds) * nanoseconds || elapsed > uint(auto.observation_max_age_seconds) * nanoseconds) {
    throw new InventoryUnavailable("Native measurement interval is stale or incomplete")
  }
  let cpu = delta("cpu_time_ns") * 1_000n / elapsed
  const write = delta("write_bytes") * nanoseconds / elapsed
  const read = delta("read_bytes") * nanoseconds / elapsed
  let cpuLimit = uint(auto.cpu_limit_millicores)
  if (host.cpuLimitMillicores !== null) cpuLimit = min(cpuLimit, uint(host.cpuLimitMillicores))
  if (before.oldest_queued != null && same(before.oldest_queued, after.oldest_queued)
    && [before, after].every(usage => uint(usage.queued_count) >= uint(auto.queue_pressure_count)
      && uint(usage.allocated_query_threads) >= uint(usage.query_thread_limit))) cpu = max(cpu, cpuLimit)

  const backing = new Map<string, { total_bytes: bigint; free_bytes: bigint; expandable: boolean }>()
  const pools = list(inventory.pools).map(pool => {
    const device = canonicalJson(array(pool.device))
    const capacity = { pool_id: pool.pool_id, total_bytes: uint(pool.observed_total_bytes), free_bytes: uint(pool.observed_available_bytes),
      reserved_bytes: 0, write_bytes_per_second: write, expandable: false, healthy: true }
    const prior = backing.get(device)
    if (!prior) {
      capacity.expandable = host.expandableResources.has(String(metadata(pool.expected).provider_volume_id))
      backing.set(device, capacity)
    } else {
      prior.total_bytes = min(prior.total_bytes, capacity.total_bytes)
      prior.free_bytes = min(prior.free_bytes, capacity.free_bytes)
      prior.expandable = false
      capacity.healthy = false
    }
    return capacity
  })
  const slots = min(uint(host.attachmentSlots), uint(uint(inventory.local_open_pool_limit) - BigInt(pools.length)))
  let memory = min(uint(before.memory_limit_bytes), uint(after.memory_limit_bytes))
  if (host.memoryLimitBytes !== null) memory = min(memory, uint(host.memoryLimitBytes))
  return { worker_id: metadata(inventory.worker).worker_id, zone: host.zone, ready: true,
    observed_at: uint(inventory.observed_at), attachment_slots_free: slots, cpu_used: cpu, cpu_limit: cpuLimit,
    io_used: read + write, io_limit: auto.io_limit_bytes_per_second,
    memory_used: max(uint(before.charged_memory_bytes), uint(after.charged_memory_bytes)), memory_limit: memory,
    resident_shards: max(uint(before.resident_group_count), uint(after.resident_group_count)),
    resident_shard_limit: min(uint(before.resident_group_limit), uint(after.resident_group_limit)), pools }
}

import { closeSync, constants, fstatSync, lstatSync, openSync, readSync, realpathSync, statSync } from "node:fs"
import { dirname, isAbsolute, join } from "node:path"
import { isDeepStrictEqual as equal } from "node:util"
import { ProviderError } from "../providers.js"
import { hasFields, parseJson, type JsonObject } from "../transport.js"
import { ec2WorkerTags, type NativeTarget } from "./cloudControl.js"
import { growthEndpoint, growthMatchesCredentials, growthOperation, growthRead, type GrowthController } from "./controller.js"
import { eksInternals, positiveInteger, quantityBytes, resource, zoneLabel, type EksProvider, type KubeRecord } from "./eks.js"
import { GrowthTarget, HostObservation, ProviderObservation, type StatefulGrowthAdapter } from "./growthTypes.js"
import { digest, identity } from "./shared.js"

const gibibyte = 1n << 30n
const labels = "nabedra.io/"
type EksTarget = Extract<NativeTarget, { kind: "eks" }>
type Samples = Readonly<Record<string, JsonObject>>
type Endpoints = Readonly<Record<string, GrowthController>>
type Records = Map<string, KubeRecord>
type Worker = [zone: string, stateful: KubeRecord, volumes: Record<string, string>, mounts: Record<string, string>]
type StagedPool = [pvc: KubeRecord, pv: KubeRecord, volumeId: string]
type Authorization = () => void | Promise<void>
type Enrollment = KubeRecord & { worker_id: string; incarnation: string }
const max = (left: bigint, right: bigint) => left > right ? left : right
const sizeGib = (size: bigint) => (size + gibibyte - 1n) / gibibyte
const secretName = (claimId: string) => "nabedra-bootstrap-" + claimId

export class EksWorkerTemplate {
  constructor(
    readonly zone: string,
    readonly poolId: string,
    readonly mountPath: string,
    readonly cpuMillicores: number,
    readonly memoryBytes: number | bigint,
    readonly workerMonthlyCost: number | bigint,
    readonly maxRequestBytes: number | bigint,
    readonly filesystemReserveBytes: number | bigint,
    readonly mountPublicKey: string,
    readonly enrollmentFile: string,
    readonly adminUrl: string,
    readonly adminTokenFile: string,
    readonly handoffDirectory: string,
  ) { Object.freeze(this) }
}

function privatePath(path: string): boolean {
  try {
    const info = lstatSync(path)
    return isAbsolute(path) && realpathSync(path) === path && info.isDirectory() && (info.mode & 0o077) === 0
  } catch { return false }
}

function privateInput(path: string, maxBytes: number): Buffer {
  const parent = dirname(path)
  if (!privatePath(parent)) throw new ProviderError("EKS staging inputs require an isolated private directory")
  let fd: number | undefined
  try {
    if (!isAbsolute(path) || realpathSync(path) !== path) throw new Error()
    fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
    const info = fstatSync(fd)
    if (!info.isFile() || info.uid !== statSync(parent).uid || (info.mode & 0o077) !== 0 || info.nlink !== 1
      || info.size < 1 || info.size > maxBytes) throw new Error()
    const data = Buffer.alloc(info.size + 1)
    if (readSync(fd, data) !== info.size) throw new Error()
    return data.subarray(0, info.size)
  } catch {
    throw new ProviderError("EKS staging input must be a bounded private regular file")
  } finally {
    if (fd !== undefined) closeSync(fd)
  }
}

function enrolledWorker(enrollment: Enrollment): JsonObject {
  const { worker_id, incarnation, certificate_sha256, enrollment_revision } = enrollment
  return { worker_id, incarnation, certificate_sha256, enrollment_revision }
}

function inputs(template: EksWorkerTemplate, namespace: string): [Enrollment, string] {
  try {
    const enrollment = resource(parseJson(privateInput(template.enrollmentFile, 4096))) as Enrollment
    const tokenBytes = privateInput(template.adminTokenFile, 4097)
    if (tokenBytes.some(byte => byte > 127)) throw new Error()
    const token = tokenBytes.toString("utf8").replace(/\n(?![\s\S])/u, "")
    ec2WorkerTags(enrollment)
    const name = "nabedra-worker-" + enrollment.worker_id
    const peer = `${name}-0.${name}.${namespace}.svc.`
    const server = enrollment.server_name
    const admin = /^https:\/\/([a-z0-9-]+(?:\.[a-z0-9-]+)+)(?::([0-9]+))?(?![\s\S])/u.exec(template.adminUrl)
    if (!hasFields(enrollment, "worker_id", "incarnation", "enrollment_revision", "certificate_sha256", "endpoint", "server_name")
      || enrollment.enrollment_revision !== 1 || typeof server !== "string" || !server.startsWith(peer)
      || !/^[a-z0-9-]+(?:\.[a-z0-9-]+)*(?![\s\S])/u.test(server.slice(peer.length))
      || server.length > 253 || enrollment.endpoint !== server + ":9000" || admin === null
      || (admin[2] !== undefined && (Number(admin[2]) > 65535 || String(Number(admin[2])) !== admin[2]))
      || token.length < 43 || token.length > 4096 || !/^[A-Za-z0-9._~+/-]+=*(?![\s\S])/u.test(token)
      || template.enrollmentFile === template.adminTokenFile) throw new Error()
    return [enrollment, token]
  } catch (error) {
    if (error instanceof ProviderError) throw error
    throw new ProviderError("EKS staging requires exact worker enrollment and protected admin credentials")
  }
}

interface Slot {
  template: EksWorkerTemplate
  fingerprint: string
}
interface AdapterState {
  byZone: Map<string, Slot>
  byWorker: Map<string, Slot>
  endpoints: Readonly<Record<string, GrowthController>>
}
const states = new WeakMap<EksGrowthAdapter, AdapterState>()
const state = (adapter: EksGrowthAdapter) => states.get(adapter)!

export interface EksGrowthAdapterOptions {
  provider: EksProvider
  adapterId: string
  workers: Readonly<Record<string, readonly [statefulResourceId: string, podUid: string]>>
  pvcs: Readonly<Record<string, string>>
  maxVolumeGib: number
  templates?: readonly EksWorkerTemplate[]
}

export class EksGrowthAdapter implements StatefulGrowthAdapter {
  readonly provider: EksProvider
  readonly clusterId: string
  readonly adapterId: string
  readonly workers: EksGrowthAdapterOptions["workers"]
  readonly pvcs: EksGrowthAdapterOptions["pvcs"]
  readonly maxVolumeBytes: number

  constructor({ provider, adapterId, workers, pvcs, maxVolumeGib, templates = [] }: EksGrowthAdapterOptions) {
    if (!Number.isInteger(maxVolumeGib) || maxVolumeGib < 1 || maxVolumeGib > 65536) {
      throw new ProviderError("Explicit EKS volume ceiling is required")
    }
    this.provider = provider
    this.clusterId = identity(provider.clusterId)
    this.adapterId = identity(adapterId)
    this.workers = Object.freeze(Object.fromEntries(Object.entries(workers).map(([id, binding]) => {
      if (!Array.isArray(binding) || binding.length !== 2 || !binding.every(value => typeof value === "string" && value)) {
        throw new ProviderError("EKS workers require exact StatefulSet identity and Pod UID")
      }
      return [identity(id), Object.freeze([binding[0], binding[1]] as const)]
    })))
    this.pvcs = Object.freeze({ ...pvcs })
    if (new Set(Object.values(this.pvcs)).size !== Object.keys(this.pvcs).length) {
      throw new ProviderError("EKS growth PVC bindings must be distinct")
    }
    this.maxVolumeBytes = maxVolumeGib * Number(gibibyte)
    const byZone = new Map<string, Slot>()
    const byWorker = new Map<string, Slot>()
    const pools = new Set<string>()
    const sources = new Set<string>()
    const identities = [new Set(), new Set(), new Set(), new Set()]
    const operations = eksInternals(provider)
    for (const template of templates) {
      const region = operations.stagingRegion
      if (Object.getPrototypeOf(template) !== EksWorkerTemplate.prototype || byWorker.size >= 64 || !region
        || !/^\S+@sha256:[a-f0-9]{64}(?![\s\S])/u.test(operations.image) || typeof template.zone !== "string"
        || !template.zone.startsWith(region) || !/^[a-z](?![\s\S])/u.test(template.zone.slice(region.length))
        || byZone.has(template.zone) || pools.has(identity(template.poolId))
        || Object.values(pvcs).some(binding => operations.parts(binding)[1] === "nabedra-pool-" + template.poolId)
        || typeof template.mountPath !== "string" || Buffer.byteLength(template.mountPath) > 1024
        || !/^(?:\/[A-Za-z0-9_][A-Za-z0-9_.-]*)+(?![\s\S])/u.test(template.mountPath)
        || ["dev", "proc", "sys", "nabedra"].includes(template.mountPath.split("/")[1])
        || !Number.isInteger(template.cpuMillicores) || template.cpuMillicores < 1000 || template.cpuMillicores >= 2 ** 32
        || !positiveInteger(template.memoryBytes) || template.memoryBytes >= 1n << 63n
        || !positiveInteger(template.workerMonthlyCost) || template.workerMonthlyCost >= 1n << 64n
        || !positiveInteger(template.maxRequestBytes) || template.maxRequestBytes > this.maxVolumeBytes
        || !positiveInteger(template.filesystemReserveBytes) || template.filesystemReserveBytes >= gibibyte
        || typeof template.mountPublicKey !== "string" || !/^[a-f0-9]{64}(?![\s\S])/u.test(template.mountPublicKey)
        || template.mountPublicKey === "0".repeat(64) || typeof template.handoffDirectory !== "string"
        || !privatePath(template.handoffDirectory)) {
        throw new ProviderError("EKS requires a bounded protected worker template with a distinct zone and pool")
      }
      const [enrollment, token] = inputs(template, provider.namespace)
      const workerId = enrollment.worker_id
      const files = [template.enrollmentFile, template.adminTokenFile]
      const values = [enrollment.incarnation, enrollment.certificate_sha256, template.adminUrl, template.mountPublicKey]
      if (Object.hasOwn(this.workers, workerId) || byWorker.has(workerId) || files.some(file => sources.has(file))
        || values.some((value, index) => identities[index].has(value))) {
        throw new ProviderError("EKS staging requires distinct enrollment, keys and private inputs")
      }
      const slot = { template, fingerprint: fingerprint(this, template, enrollment, token) }
      byZone.set(template.zone, slot)
      byWorker.set(workerId, slot)
      pools.add(template.poolId)
      files.forEach(file => sources.add(file))
      values.forEach((value, index) => identities[index].add(value))
    }
    states.set(this, { byZone, byWorker, endpoints: Object.freeze({}) })
  }

  bindEndpoints(endpoints: Endpoints): EksGrowthAdapter {
    const current = state(this)
    const urls = Object.values(endpoints).map(growthEndpoint)
    const matched: Record<string, GrowthController> = {}
    for (const [workerId, slot] of current.byWorker) {
      const endpoint = endpoints[workerId]
      if (endpoint === undefined) continue
      const [, token] = exactInputs(this, slot)
      const url = slot.template.adminUrl
      if (urls.filter(candidate => candidate === url).length === 1 && growthMatchesCredentials(endpoint, url, token)) {
        matched[workerId] = endpoint
      }
    }
    const copy = Object.assign(Object.create(Object.getPrototypeOf(this)) as EksGrowthAdapter, this)
    states.set(copy, { ...current, endpoints: Object.freeze(matched) })
    return copy
  }

  async observe(inventories: Samples, { policy: raw, deadline, claimId = null }: {
    policy: JsonObject; deadline: number; claimId?: string | null
  }): Promise<ProviderObservation> {
    const policy = resource(raw)
    const operations = eksInternals(this.provider)
    return operations.limit(deadline, async () => {
      const [stateful, pvcs] = await operations.retainedInventory()
      const hosts: Record<string, HostObservation> = {}
      const readyUids = new Set<string>()
      const { byWorker, endpoints } = state(this)
      for (const [workerId, sample] of Object.entries(inventories)) {
        const inventory = resource(sample.inventory)
        const worker = resource(inventory.worker)
        if (!Object.hasOwn(this.workers, workerId)) {
          const { zone, uid, template } = await readyStaged(this, workerId, inventory, stateful, pvcs)
          if (readyUids.has(uid)) throw new ProviderError("Ready EKS worker shares a retained StatefulSet")
          readyUids.add(uid)
          hosts[workerId] = new HostObservation(worker, zone, 0, new Set(), template.cpuMillicores, template.memoryBytes)
          continue
        }
        const [zone, owner, volumes, mounts] = await fixedWorker(this, worker)
        const uid = owner.metadata.uid
        if (!stateful.has(uid) || readyUids.has(uid)) throw new ProviderError("Ready EKS worker is missing from retained inventory")
        readyUids.add(uid)
        const expandable = new Set<string>()
        for (const row of inventory.pools) {
          const expected = resource(row.expected)
          const pvc = await mountedPvc(this, expected, worker, zone, volumes, mounts)
          if (!pvcs.has(pvc.metadata.uid)) throw new ProviderError("Mounted EKS PVC is missing from retained inventory")
          const capacity = quantityBytes(pvc.status?.capacity?.storage)
          if (row.observed_total_bytes > capacity) throw new ProviderError("Mounted bytes exceed observed EKS volume capacity")
          if (row.observed_total_bytes < this.maxVolumeBytes) expandable.add(expected.provider_volume_id)
        }
        hosts[workerId] = new HostObservation(worker, zone, 0, expandable)
      }
      const zones = new Set<string>()
      let reservedPvc: string | undefined
      for (const [workerId, slot] of byWorker) {
        const { template } = slot
        if (!Object.hasOwn(endpoints, workerId) || Object.hasOwn(inventories, workerId) || !policy.zones.includes(template.zone)
          || template.workerMonthlyCost > policy.worker_cost) continue
        exactInputs(this, slot)
        const [compute, storage] = slotRecords(slot, workerId, stateful, pvcs)
        if (compute !== null) continue
        if (storage !== null) {
          const meta = resource(storage.metadata)
          if (claimId === null || meta.labels?.[labels + "claim"] !== claimId) continue
          const size = quantityBytes(storage.spec?.resources?.requests?.storage)
          const exact = await stagePvc(this, slot, workerId, claimId, size, true)
          if (reservedPvc !== undefined || !validSize(template, size) || operations.parts(exact.resource_id)[2] !== meta.uid) {
            throw new ProviderError("Reserved EKS PVC differs from its exact priced slot")
          }
          const capacity = storage.status?.capacity?.storage
          if (capacity != null && quantityBytes(capacity) > sizeGib(size) * gibibyte) {
            throw new ProviderError("Reserved EKS capacity exceeds its priced slot")
          }
          reservedPvc = meta.uid
        }
        zones.add(template.zone)
      }
      return new ProviderObservation(hosts, [...stateful.keys()].filter(uid => !readyUids.has(uid)).length,
        spend(stateful, pvcs, policy, undefined, reservedPvc), zones)
    })
  }

  target(claimId: string, raw: JsonObject, inventories: Samples): GrowthTarget | null {
    const action = resource(raw)
    const { byWorker, byZone, endpoints } = state(this)
    if (action.kind === "add_worker") {
      const desired = byZone.get(action.zone)
      if (desired === undefined || !validSize(desired.template, action.bytes)) return null
      for (const [workerId, slot] of byWorker) {
        if (slot === desired && Object.hasOwn(endpoints, workerId) && !Object.hasOwn(inventories, workerId)) {
          const [enrollment] = exactInputs(this, slot)
          return new GrowthTarget(enrolledWorker(enrollment), slot.template.poolId, 1)
        }
      }
      return null
    }
    if (action.kind !== "expand" || action.target_bytes > this.maxVolumeBytes) return null
    const inventory = resource(inventories[action.worker]?.inventory)
    for (const row of inventory.pools) {
      if (row.pool_id === action.pool && row.expected?.profile === "ebs") {
        const expected = resource(row.expected)
        const volumeId = expected.provider_volume_id
        if (Object.hasOwn(this.pvcs, volumeId)) return new GrowthTarget(inventory.worker, row.pool_id, expected.generation, volumeId)
      }
    }
    return null
  }

  async provisionWorker(raw: JsonObject, target: GrowthTarget, { authorize, deadline }:
    { authorize: Authorization; deadline: number }): Promise<JsonObject> {
    const [slot, enrollment, size, intent] = workerIntent(this, raw, target)
    const workerId = enrollment.worker_id
    const claimId = identity(intent.claim_id)
    const operations = eksInternals(this.provider)
    return operations.limit(deadline, async () => {
      const [stateful, pvcs] = await operations.retainedInventory()
      const [compute, retained] = slotRecords(slot, workerId, stateful, pvcs, claimId)
      if (compute !== null) throw new ProviderError("EKS compute exists before this claim proved physical storage")
      const staged = await stagePvc(this, slot, workerId, claimId, size, retained !== null, authorize)
      if (retained !== null && operations.parts(staged.resource_id)[2] !== retained.metadata.uid) {
        throw new ProviderError("EKS staging PVC changed during observation")
      }
      if (staged.state !== "Bound") throw new ProviderError("EKS WaitForFirstConsumer PVC is awaiting the protected storage handoff")
      const [, , volumeId] = await stagedStorage(this, slot.template, staged.resource_id, workerId, claimId, size)
      const volume = await operations.stagedEbs(volumeId, slot.template.zone, Number(sizeGib(size)))
      if (volume.State !== "available" || volume.Attachments.length !== 0) {
        throw new ProviderError("EKS staging awaits exclusive ownership after the first consumer")
      }
      await authorize()
      return { resource_id: volumeId, state: "awaiting_worker_staging" }
    })
  }

  async registerWorker(raw: JsonObject, target: GrowthTarget, record: JsonObject, { controller, pin: authority, deadline }:
    { controller: GrowthController; pin: JsonObject | null; deadline: number }): Promise<JsonObject> {
    const [slot, enrollment, size, intent] = workerIntent(this, raw, target)
    const { template } = slot
    const workerId = enrollment.worker_id
    const claimId = identity(intent.claim_id)
    const volumeId = resource(record).provider_resource
    const pin = resource(authority)
    const snapshot = async () => resource(await growthRead(controller, "storage.growth.snapshot", {}, { deadline }))
    const before = await snapshot()
    const policy = resource(before.policy)
    const auto = resource(policy.automatic ?? {})
    const quote = BigInt(policy.worker_cost) + sizeGib(size) * BigInt(policy.volume_cost_per_gib)
    if (!equal(before.pin, pin) || before.cluster_id !== this.clusterId || !before.active_automatic
      || auto.adapter_id !== this.adapterId || policy.policy_id !== intent.policy_id || !policy.zones.includes(template.zone)
      || policy.worker_cost < template.workerMonthlyCost || quote !== BigInt(intent.quoted_monthly_cost) || !equal(before.active, record)
      || record.phase !== "awaiting_mount" || !equal(record.intent, raw)) {
      throw new ProviderError("The current Control claim does not authorize EKS staging")
    }
    const operations = eksInternals(this.provider)
    const reobserve = async (): Promise<[string | null, string, StagedPool]> => {
      const [stateful, pvcs] = await operations.retainedInventory()
      const [compute, storage] = slotRecords(slot, workerId, stateful, pvcs, claimId)
      if (storage === null) throw new ProviderError("The durable EKS staging PVC disappeared")
      const pvcUid = identity(storage.metadata.uid)
      const computeUid = compute === null ? null : identity(compute.metadata.uid)
      const floor = spend(stateful, pvcs, policy, computeUid ?? undefined, pvcUid)
      const extra = max(0n, floor - BigInt(policy.initial_monthly_spend))
      const count = max(BigInt(stateful.size + (compute === null ? 1 : 0)), BigInt(pin.ready.ready_count) + 1n)
      if (count > auto.max_workers || BigInt(pin.monthly_spend) + extra > policy.monthly_spend_ceiling) {
        throw new ProviderError("Retained EKS resources exceed the current growth ceiling")
      }
      const { resource_id: resourceId } = await stagePvc(this, slot, workerId, claimId, size, true)
      if (operations.parts(resourceId)[2] !== pvcUid) throw new ProviderError("EKS staging PVC changed during observation")
      return [computeUid, resourceId, await stagedStorage(this, template, resourceId, workerId, claimId, size, volumeId)]
    }

    return operations.limit(deadline, async () => {
      const [previousUid, resourceId, [pvc, pv]] = await reobserve()
      let computeUid = previousUid
      const handoff = readHandoff(template, claimId, pvc, pv, volumeId)
      const stage: JsonObject = { zone: template.zone, pvc_resource_id: resourceId, pv_uid: pv.metadata.uid,
        volume_id: volumeId, bootstrap_secret: secretName(claimId) }
      if (handoff === null) return { state: "awaiting_filesystem_identity", stage }
      const expected = nativeTarget(this.provider, handoff)
      const authorize = async () => {
        const [currentUid, currentResource, [currentPvc, currentPv]] = await reobserve()
        if (currentResource !== resourceId || currentPv.metadata.uid !== stage.pv_uid
          || !equal(readHandoff(template, claimId, currentPvc, currentPv, volumeId), handoff)
          || currentUid !== computeUid || !equal(await snapshot(), before)) {
          throw new ProviderError("Control or the exact EKS staging resources changed")
        }
      }
      if (computeUid === null) {
        const volume = await operations.stagedEbs(volumeId, template.zone, Number(sizeGib(size)))
        if (volume.State !== "available" || volume.Attachments.length !== 0) {
          throw new ProviderError("EKS can create the exact worker only after first-consumer detachment")
        }
      }
      const staged = await stageWorker(this, slot, enrollment, claimId, resourceId, handoff,
        computeUid !== null || expected !== null, authorize)
      computeUid = identity(staged.stateful.metadata.uid)
      if (expected !== null) await authorize()
      const observed = await operations.stagedPod(staged, { zone: template.zone, pvc, pv, sizeGib: Number(sizeGib(size)), expected })
      stage.statefulset_resource_id = staged.resource_id
      if (observed === null) return { state: "awaiting_pod_attachment", stage }
      stage.pod_uid = observed.pod_uid
      if (expected === null) return { state: "awaiting_pod_identity", stage }
      const preparation = { claim_id: claimId, run_id: identity(pin.ready.view.run_id), enrollment,
        mount: mount(this, template, target.worker, volumeId, handoff.filesystem_uuid),
        mount_public_key: template.mountPublicKey, native_target: observed }
      if (!equal(await snapshot(), before)) throw new ProviderError("Control changed before EKS worker preparation")
      const prepared = await growthOperation(controller, claimId, "prepare-worker", "storage.growth.prepare_worker", preparation, { deadline })
      const after = await snapshot()
      if (!equal(prepared, { identity: target.worker }) || !equal(after.active, record) || !after.active_automatic
        || after.cluster_id !== this.clusterId || !equal(after.policy, policy)
        || Object.entries(pin).some(([key, value]) => key !== "enrollment_revision" && !equal(after.pin[key], value))
        || ![BigInt(pin.enrollment_revision), BigInt(pin.enrollment_revision) + 1n].includes(BigInt(after.pin.enrollment_revision))) {
        throw new ProviderError("Control changed before the EKS bootstrap handoff")
      }
      return { state: "awaiting_bootstrap", stage: { ...stage, bootstrap_intent: preparation } }
    })
  }

  async provision(intent: JsonObject, target: GrowthTarget, { deadline }: { deadline: number }): Promise<JsonObject> {
    const operations = eksInternals(this.provider)
    return operations.limit(deadline, async () => {
      const [zone, , volumes, mounts] = await fixedWorker(this, target.worker)
      const pvc = pvcId(this, target.resourceId)
      const [, name] = operations.parts(pvc)
      if (!Object.values(volumes).includes(name) || !Object.values(mounts).some(key => volumes[key] === name)) {
        throw new ProviderError("EKS worker no longer mounts the expansion target PVC")
      }
      const bytes = resource(intent.action).target_bytes
      if (bytes > this.maxVolumeBytes) throw new ProviderError("EKS growth exceeds the configured volume ceiling")
      return this.provider.expandPool({ resourceId: pvc, targetBytes: bytes, worker: target.worker,
        poolId: target.poolId, zone, volumeId: target.resourceId! })
    })
  }

  async register(_intent: JsonObject, target: GrowthTarget, _record: JsonObject, { controller, deadline }:
    { controller: GrowthController; deadline: number }): Promise<null> {
    const inventory = resource(await growthRead(controller, "storage.growth.inventory", {}, { deadline }))
    const row = inventory.pools.find((entry: KubeRecord) => entry.pool_id === target.poolId)
    if (!equal(inventory.worker, target.worker) || inventory.ready !== true || row === undefined
      || row.expected?.generation !== target.poolGeneration || row.expected?.provider_volume_id !== target.resourceId) {
      throw new ProviderError("EKS growth target differs from the current native mount")
    }
    await eksInternals(this.provider).limit(deadline, async () => {
      const [zone, , volumes, mounts] = await fixedWorker(this, target.worker)
      await mountedPvc(this, row.expected, target.worker, zone, volumes, mounts)
    })
    return null
  }
}

function fingerprint(adapter: EksGrowthAdapter, template: EksWorkerTemplate, enrollment: Enrollment, token: string): string {
  const operations = eksInternals(adapter.provider)
  const spec = { zone: template.zone, pool_id: template.poolId, mount_path: template.mountPath, cpu_millicores: template.cpuMillicores,
    memory_bytes: template.memoryBytes, worker_monthly_cost: template.workerMonthlyCost, max_request_bytes: template.maxRequestBytes,
    filesystem_reserve_bytes: template.filesystemReserveBytes, mount_public_key: template.mountPublicKey, admin_url: template.adminUrl }
  return digest([adapter.clusterId, adapter.adapterId, operations.origin, adapter.provider.namespace,
    adapter.provider.storageClass, operations.image, spec, enrollment, digest(token)])
}

function exactInputs(adapter: EksGrowthAdapter, slot: Slot): [Enrollment, string] {
  const [enrollment, token] = inputs(slot.template, adapter.provider.namespace)
  if (fingerprint(adapter, slot.template, enrollment, token) !== slot.fingerprint) throw new ProviderError("Protected EKS launch inputs changed")
  return [enrollment, token]
}

function validSize(template: EksWorkerTemplate, size: unknown): size is number | bigint {
  return positiveInteger(size) && size <= template.maxRequestBytes
    && sizeGib(BigInt(size)) * gibibyte - BigInt(size) >= template.filesystemReserveBytes
}

function slotRecords(slot: Slot, workerId: string, stateful: Records, pvcs: Records, claimId?: string): [KubeRecord | null, KubeRecord | null] {
  const find = (set: Records, name: string) => {
    const matches = [...set.values()].filter(record => {
      const meta = resource(record.metadata)
      return meta.name === name || meta.labels?.[labels + "worker"] === workerId
        || (claimId !== undefined && meta.labels?.[labels + "claim"] === claimId)
    })
    if (matches.length > 1) throw new ProviderError("Several EKS resources occupy one protected worker or pool slot")
    return matches[0] ?? null
  }
  return [find(stateful, "nabedra-worker-" + workerId), find(pvcs, "nabedra-pool-" + slot.template.poolId)]
}

function stagePvc(adapter: EksGrowthAdapter, slot: Slot, workerId: string, claimId: string, bytes: bigint, existingOnly: boolean,
  authorize?: Authorization) {
  return eksInternals(adapter.provider).ensurePool({ claimId, workerId, poolId: slot.template.poolId, zone: slot.template.zone,
    bytes, staging: slot.fingerprint, authorize, existingOnly })
}

async function stagedStorage(adapter: EksGrowthAdapter, template: EksWorkerTemplate, resourceId: string, workerId: string,
  claimId: string, size: bigint, volume?: string): Promise<StagedPool> {
  const operations = eksInternals(adapter.provider)
  const [, pvc] = await operations.pvc(resourceId, { workerId, zone: template.zone, poolId: template.poolId })
  const capacity = pvc.status?.capacity?.storage
  if (pvc.metadata.labels?.[labels + "claim"] !== claimId || capacity == null
    || quantityBytes(pvc.spec.resources.requests.storage) !== size || size > quantityBytes(capacity)
    || quantityBytes(capacity) > sizeGib(size) * gibibyte) {
    throw new ProviderError("Bound EKS staging PVC capacity or claim differs")
  }
  const pv = await operations.boundVolume(pvc, { zone: template.zone, expected: volume })
  identity(pvc.metadata.uid)
  identity(pv.metadata.uid)
  return [pvc, pv, pv.spec.csi.volumeHandle]
}

function readHandoff(template: EksWorkerTemplate, claimId: string, pvc: KubeRecord, pv: KubeRecord, volumeId: string): KubeRecord | null {
  const path = join(template.handoffDirectory, identity(claimId) + ".json")
  try { lstatSync(path) } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null
    throw error
  }
  try {
    const handoff = parseJson(privateInput(path, 4096))
    const required = ["filesystem_uuid", "pvc_uid", "pv_uid", "volume_id"]
    if (!(hasFields(handoff, ...required) || hasFields(handoff, ...required, "statefulset_uid", "pod_uid"))
      || handoff.volume_id !== volumeId || handoff.pvc_uid !== pvc.metadata.uid || handoff.pv_uid !== pv.metadata.uid) throw new Error()
    for (const [key, value] of Object.entries(handoff)) if (key !== "volume_id") identity(value)
    return handoff
  } catch (error) {
    if (error instanceof ProviderError) throw error
    throw new ProviderError("Protected EKS handoff differs from the exact claim storage or Pod")
  }
}

function nativeTarget(provider: EksProvider, handoff: KubeRecord): EksTarget | null {
  if (!Object.hasOwn(handoff, "pod_uid")) return null
  const uids = [handoff.pod_uid, handoff.statefulset_uid, handoff.pvc_uid, handoff.pv_uid].map(identity)
  if (new Set(uids).size !== 4) throw new ProviderError("EKS native target requires distinct exact UIDs")
  const [pod_uid, statefulset_uid, pvc_uid, pv_uid] = uids
  return { kind: "eks", namespace: provider.namespace, pod_uid, statefulset_uid, pvc_uid, pv_uid }
}

function stageWorker(adapter: EksGrowthAdapter, slot: Slot, enrollment: Enrollment, claimId: string, resourceId: string,
  handoff: KubeRecord, existingOnly: boolean, authorize?: Authorization) {
  const { template } = slot
  return eksInternals(adapter.provider).ensureWorker({ claimId, workerId: enrollment.worker_id, incarnation: enrollment.incarnation,
    zone: template.zone, bootstrapSecret: secretName(claimId), pools: [{ resource_id: resourceId,
      mount_path: template.mountPath, filesystem_uuid: handoff.filesystem_uuid }], cpuMillicores: template.cpuMillicores,
    memoryBytes: template.memoryBytes, enrollment, staging: slot.fingerprint, existingOnly, authorize })
}

function spend(stateful: Records, pvcs: Records, policy: KubeRecord, reservedWorker?: string, reservedPvc?: string): bigint {
  let gib = 0n
  for (const [uid, pvc] of pvcs) {
    if (uid === reservedPvc) continue
    const requested = quantityBytes(pvc.spec.resources.requests.storage)
    const capacity = pvc.status?.capacity?.storage
    gib += sizeGib(capacity == null ? requested : max(requested, quantityBytes(capacity)))
  }
  return BigInt(stateful.size - (reservedWorker !== undefined && stateful.has(reservedWorker) ? 1 : 0)) * BigInt(policy.worker_cost)
    + gib * BigInt(policy.volume_cost_per_gib)
}

function mount(adapter: EksGrowthAdapter, template: EksWorkerTemplate, worker: JsonObject, volumeId: string, filesystem: string): JsonObject {
  return { profile: "ebs", worker, generation: 1, mount_path: template.mountPath, provider_volume_id: volumeId,
    filesystem_uuid: filesystem, identity: { engine: "oltp", pool_format_version: 2, cluster_id: adapter.clusterId,
      worker_id: worker.worker_id, pool_id: template.poolId } }
}

async function readyStaged(adapter: EksGrowthAdapter, workerId: string, inventory: KubeRecord, stateful: Records, pvcs: Records):
  Promise<{ zone: string; uid: string; template: EksWorkerTemplate }> {
  const { byWorker, endpoints } = state(adapter)
  const slot = byWorker.get(workerId)
  if (slot === undefined || !Object.hasOwn(endpoints, workerId) || inventory.pools?.length !== 1) {
    throw new ProviderError("Ready EKS worker has no protected launch slot and native pool")
  }
  const { template } = slot
  const [enrollment] = exactInputs(adapter, slot)
  const row = resource(inventory.pools[0])
  const claimId = identity(row.growth_claim)
  const [compute, storage] = slotRecords(slot, workerId, stateful, pvcs, claimId)
  if (compute === null || storage === null || !equal(enrolledWorker(enrollment), inventory.worker)) {
    throw new ProviderError("Ready EKS worker no longer matches the enrolled slot")
  }
  const size = quantityBytes(storage.spec.resources.requests.storage)
  if (!validSize(template, size)) throw new ProviderError("Ready EKS staging PVC changed its priced size")
  const { resource_id: resourceId } = await stagePvc(adapter, slot, workerId, claimId, size, true)
  const [pvc, pv, volumeId] = await stagedStorage(adapter, template, resourceId, workerId, claimId, size, row.expected?.provider_volume_id)
  const handoff = readHandoff(template, claimId, pvc, pv, volumeId)
  if (handoff === null || !Object.hasOwn(handoff, "pod_uid") || row.pool_id !== template.poolId || row.primary !== true
    || !equal(row.expected, mount(adapter, template, inventory.worker, volumeId, handoff.filesystem_uuid))
    || row.observed_total_bytes < size || row.observed_total_bytes > sizeGib(size) * gibibyte
    || pvc.metadata.uid !== storage.metadata.uid) {
    throw new ProviderError("Ready EKS worker differs from its pinned mount and handoff")
  }
  const staged = await stageWorker(adapter, slot, enrollment, claimId, resourceId, handoff, true)
  if (staged.stateful.metadata.uid !== compute.metadata.uid || await eksInternals(adapter.provider).stagedPod(staged,
    { zone: template.zone, pvc, pv, sizeGib: Number(sizeGib(size)), expected: nativeTarget(adapter.provider, handoff), ready: true }) === null) {
    throw new ProviderError("Ready EKS worker no longer owns its exact Pod and EBS attachment")
  }
  return { zone: template.zone, uid: identity(compute.metadata.uid), template }
}

function workerIntent(adapter: EksGrowthAdapter, value: JsonObject, target: GrowthTarget): [Slot, Enrollment, bigint, KubeRecord] {
  const intent = resource(value)
  const claimId = identity(intent.claim_id)
  const action = resource(intent.action)
  const slot = state(adapter).byWorker.get(identity(target.worker.worker_id))
  if (action.kind !== "add_worker" || slot === undefined || !equal(adapter.target(claimId, action, {}), target)
    || !equal(intent.worker, target.worker) || intent.pool_id !== target.poolId || intent.pool_generation !== target.poolGeneration
    || intent.quoted_monthly_cost < slot.template.workerMonthlyCost || intent.quoted_monthly_cost > intent.monthly_spend_ceiling) {
    throw new ProviderError("EKS staging target differs from the exact priced worker claim")
  }
  const [enrollment] = exactInputs(adapter, slot)
  return [slot, enrollment, BigInt(action.bytes), intent]
}

async function fixedWorker(adapter: EksGrowthAdapter, worker: JsonObject): Promise<Worker> {
  const binding = adapter.workers[identity(worker.worker_id)]
  if (binding === undefined) throw new ProviderError("Ready EKS worker has no fixed StatefulSet/Pod binding")
  const [stateful, pod] = await eksInternals(adapter.provider).inspectLiveWorker({ resourceId: binding[0], podUid: binding[1], worker })
  const spec = resource(pod.spec)
  const containers: KubeRecord[] = (spec.containers ?? []).filter((container: KubeRecord) => container.name === "worker")
  if (containers.length !== 1) throw new ProviderError("EKS Pod has no exact worker container")
  const volumes = Object.fromEntries((spec.volumes ?? []).filter((entry: KubeRecord) =>
    entry.persistentVolumeClaim && entry.persistentVolumeClaim.readOnly !== true)
    .map((entry: KubeRecord) => [entry.name, entry.persistentVolumeClaim.claimName]))
  const mounts = Object.fromEntries((containers[0].volumeMounts ?? []).filter((entry: KubeRecord) =>
    entry.readOnly !== true && !entry.subPath && !entry.subPathExpr).map((entry: KubeRecord) => [entry.mountPath, entry.name]))
  return [spec.nodeSelector[zoneLabel], stateful, volumes, mounts]
}

function pvcId(adapter: EksGrowthAdapter, volumeId: string | null): string {
  const pvc = volumeId === null ? undefined : adapter.pvcs[volumeId]
  if (pvc === undefined) throw new ProviderError("Native EBS mount has no fixed PVC UID binding")
  return pvc
}

async function mountedPvc(adapter: EksGrowthAdapter, expected: KubeRecord, worker: JsonObject, zone: string,
  volumes: Record<string, string>, mounts: Record<string, string>): Promise<KubeRecord> {
  const resourceId = expected.provider_volume_id
  if (expected.profile !== "ebs" || !equal(expected.worker, worker)) {
    throw new ProviderError("EKS inventory contains a mount from another worker or provider")
  }
  const binding = pvcId(adapter, resourceId)
  const operations = eksInternals(adapter.provider)
  if (volumes[mounts[expected.mount_path]] !== operations.parts(binding)[1]) {
    throw new ProviderError("EKS Pod no longer mounts the exact PVC at the registered path")
  }
  const [, pvc] = await operations.pvc(binding, { workerId: identity(worker.worker_id), zone, poolId: identity(expected.identity?.pool_id) })
  await operations.boundVolume(pvc, { zone, expected: resourceId })
  return pvc
}

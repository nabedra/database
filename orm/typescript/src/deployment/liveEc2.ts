import { closeSync, constants, fstatSync, openSync, readSync, realpathSync, statSync } from "node:fs"
import { dirname, posix } from "node:path"
import { isDeepStrictEqual } from "node:util"
import { ProviderError } from "../providers.js"
import { hasFields, parseJson, type JsonObject } from "../transport.js"
import { ec2WorkerTags } from "./cloudControl.js"
import { growthEndpoint, growthMatchesCredentials, growthOperation, growthRead, type GrowthController } from "./controller.js"
import { ec2Attachment, ec2SecondaryDevice, ec2Size, ec2Tags, ownedEc2, stagedEc2Volume, stagedEc2Worker,
  type Ec2Authorize, type Ec2Bytes, type Ec2Instance, type Ec2InventoryRecord,
  type Ec2Provider, type Ec2StagedWorkerClaim, type Ec2Volume } from "./ec2.js"
import { GrowthTarget, HostObservation, ProviderObservation, type StatefulGrowthAdapter } from "./growthTypes.js"
import { digest, identity, requireRecord } from "./shared.js"

const gib = 1n << 30n
const maximumUint64 = (1n << 64n) - 1n
const subnet = /^subnet-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/
const fingerprint = /^[a-f0-9]{64}(?![\s\S])/
const ec2InstanceId = /^i-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/
const tlsName = /^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?![\s\S])/
const exactEnrollmentKeys = ["worker_id", "incarnation", "certificate_sha256", "enrollment_revision", "endpoint", "server_name"]

type Inventory = Readonly<Record<string, JsonObject>>
type Endpoints = Readonly<Record<string, GrowthController>>
type Instances = Record<string, Ec2Instance>
type Volumes = Record<string, Ec2Volume>
type PoolClaim = Parameters<typeof stagedEc2Volume>[2]

export interface Ec2GrowthOptions {
  provider: Ec2Provider
  adapterId: string
  instances: Readonly<Record<string, string>>
  poolSlots?: Readonly<Record<string, Ec2PoolSlot>> | null
  templates?: Iterable<Ec2WorkerTemplate>
}

interface State {
  templates: Map<string, Ec2WorkerTemplate>
  byWorker: Map<string, Ec2WorkerTemplate>
  fingerprints: Map<string, string>
  endpoints: Endpoints
}

export class Ec2PoolSlot {
  constructor(
    readonly poolId: string,
    readonly device: string,
    readonly mountPath: string,
    readonly filesystemUuid: string,
    readonly kmsKeyId: string,
  ) {
    identity(poolId)
    identity(filesystemUuid)
    if (typeof device !== "string" || !ec2SecondaryDevice.test(device)
      || typeof mountPath !== "string" || !posix.isAbsolute(mountPath) || mountPath === "/" || mountPath.endsWith("/")
      || posix.normalize(mountPath) !== mountPath || mountPath.split("/").includes("..")
      || typeof kmsKeyId !== "string" || !kmsKeyId) {
      throw new ProviderError("A reserved EC2 secondary device, mount path and KMS key are required")
    }
    Object.freeze(this)
  }
}

export class Ec2WorkerTemplate {
  constructor(
    readonly zone: string,
    readonly subnetId: string,
    readonly pool: Ec2PoolSlot,
    readonly enrollmentFile: string,
    readonly adminUrl: string,
    readonly adminTokenFile: string,
    readonly mountPublicKey: string,
    readonly filesystemReserveBytes: Ec2Bytes,
    readonly maxRequestBytes: Ec2Bytes,
    readonly monthlyCost: number | bigint,
  ) { Object.freeze(this) }
}

export interface Ec2WorkerStage {
  instance_id: string
  zone: string
  subnet_id: string
  device: string
  admin_url: string
  bootstrap_intent: JsonObject
}

function quantity(value: unknown): bigint | null {
  if (typeof value === "bigint") return value
  return typeof value === "number" && Number.isSafeInteger(value) ? BigInt(value) : null
}
function bounded(value: unknown, min: bigint, max: bigint): boolean {
  const exact = quantity(value)
  return exact !== null && exact >= min && exact <= max
}
function asQuantity(value: unknown): bigint {
  const exact = quantity(value)
  if (exact === null) throw new ProviderError("Expected an exact EC2 inventory or growth counter")
  return exact
}
function compareQuantity(left: unknown, right: unknown): boolean {
  const lhs = quantity(left)
  return lhs !== null && lhs === quantity(right)
}
function getString(value: unknown): string {
  if (typeof value !== "string") throw new ProviderError("Invalid EC2 resource or endpoint identity")
  return value
}
function dataOf(sample: unknown): JsonObject {
  return requireRecord(requireRecord(sample).inventory)
}
function poolsOf(inventory: JsonObject): JsonObject[] {
  if (!Array.isArray(inventory.pools)) throw new ProviderError("Mounted pool inventory is invalid")
  return inventory.pools.map(requireRecord)
}
function policyZones(policy: JsonObject): string[] {
  if (!Array.isArray(policy.zones) || policy.zones.some(zone => typeof zone !== "string")) {
    throw new ProviderError("EC2 growth policy has an invalid zone list")
  }
  return policy.zones as string[]
}
function enrollmentWorker(enrollment: JsonObject): JsonObject {
  return {
    worker_id: enrollment.worker_id, incarnation: enrollment.incarnation,
    certificate_sha256: enrollment.certificate_sha256, enrollment_revision: enrollment.enrollment_revision,
  }
}
function workerZone(record: Ec2Instance, worker: JsonObject): string {
  const tags = ec2Tags(record)
  if (record.State?.Name !== "running" || typeof record.Placement?.AvailabilityZone !== "string"
    || Object.entries(ec2WorkerTags(worker)).some(([key, value]) => tags[key] !== value)) {
    throw new ProviderError("EC2 instance identity differs from the current worker")
  }
  return record.Placement.AvailabilityZone
}

function privateFile(filename: string, limit: number): Buffer {
  let descriptor: number | undefined
  try {
    if (!posix.isAbsolute(filename) || realpathSync(filename) !== filename) throw new Error()
    const parent = statSync(dirname(filename))
    descriptor = openSync(filename, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
    const file = fstatSync(descriptor)
    if ((parent.mode & 0o077) !== 0 || file.uid !== parent.uid || !file.isFile() || (file.mode & 0o077) !== 0
      || file.nlink !== 1 || file.size < 1 || file.size > limit) {
      throw new Error()
    }
    const bytes = Buffer.allocUnsafe(file.size + 1)
    let offset = 0
    while (offset < bytes.length) {
      const count = readSync(descriptor, bytes, offset, bytes.length - offset, null)
      if (count === 0) break
      offset += count
    }
    if (offset !== file.size) throw new Error()
    return bytes.subarray(0, offset)
  } catch {
    throw new ProviderError("EC2 staging requires isolated private enrollment and admin credentials")
  } finally {
    if (descriptor !== undefined) closeSync(descriptor)
  }
}

function rawInputs(template: Ec2WorkerTemplate): [JsonObject, string] {
  const file = privateFile(template.enrollmentFile, 4096)
  const secret = privateFile(template.adminTokenFile, 4097)
  try {
    const enrollment = requireRecord(parseJson(file))
    if (secret.some(byte => byte > 127)) throw new Error()
    const token = secret.toString("ascii").replace(/\n(?![\s\S])/, "")
    ec2WorkerTags(enrollment)
    const endpoint = getString(enrollment.endpoint)
    const delimiter = endpoint.lastIndexOf(":")
    const name = delimiter < 0 ? "" : endpoint.slice(0, delimiter)
    const port = delimiter < 0 ? "" : endpoint.slice(delimiter + 1)
    const address = /^https:\/\/([^:\/?#]+)(?::([0-9]+))?(?![\s\S])/.exec(template.adminUrl)
    const adminPort = address?.[2] == null ? null : Number(address[2])
    if (!hasFields(enrollment, ...exactEnrollmentKeys) || enrollment.enrollment_revision !== 1
      || !tlsName.test(name) || enrollment.server_name !== name || !/^[1-9][0-9]{0,4}(?![\s\S])/.test(port)
      || Number(port) > 65535 || address?.[1] !== name || adminPort === Number(port)
      || (adminPort !== null && (adminPort > 65535 || address?.[2] !== String(adminPort)))
      || token.length < 43 || token.length > 4096 || !/^[A-Za-z0-9._~+/-]+=*(?![\s\S])/.test(token)
      || template.enrollmentFile === template.adminTokenFile) {
      throw new Error()
    }
    return [enrollment, token]
  } catch {
    throw new ProviderError("EC2 data worker slot has an invalid enrollment or protected admin endpoint")
  }
}

const state = new WeakMap<Ec2GrowthAdapter, State>()

function launchFingerprint(adapter: Ec2GrowthAdapter, template: Ec2WorkerTemplate, enrollment: JsonObject, token: string): string {
  const { pool } = template
  const spec = {
    zone: template.zone, subnet_id: template.subnetId, admin_url: template.adminUrl, mount_public_key: template.mountPublicKey,
    filesystem_reserve_bytes: template.filesystemReserveBytes, max_request_bytes: template.maxRequestBytes,
    monthly_cost: template.monthlyCost, pool: {
      pool_id: pool.poolId, device: pool.device, mount_path: pool.mountPath, filesystem_uuid: pool.filesystemUuid,
      kms_key_id: pool.kmsKeyId,
    },
  }
  return digest([adapter.clusterId, adapter.adapterId, adapter.provider.launchTemplate, spec, enrollment, digest(token)])
}
function currentInputs(adapter: Ec2GrowthAdapter, template: Ec2WorkerTemplate): [JsonObject, string] {
  const [enrollment, token] = rawInputs(template)
  if (launchFingerprint(adapter, template, enrollment, token) !== state.get(adapter)!.fingerprints.get(template.zone)) {
    throw new ProviderError("Protected EC2 launch inputs changed")
  }
  return [enrollment, token]
}
function templateData(adapter: Ec2GrowthAdapter, template: Ec2WorkerTemplate, deadline: number): Promise<JsonObject> {
  return adapter.provider.stagingTemplate({ zone: template.zone, subnetId: template.subnetId, device: template.pool.device, deadline })
}

function slotRecord<T extends Ec2InventoryRecord>(records: Record<string, T>, workerId: string,
  poolId?: string, claimId?: string | null): T | null {
  const matches = Object.values(records).filter(record => {
    const tags = ec2Tags(record)
    return tags["nabedra:worker"] === workerId || (poolId !== undefined && tags["nabedra:pool"] === poolId)
      || (claimId != null && tags["nabedra:claim"] === claimId)
  })
  if (matches.length > 1) throw new ProviderError("Several EC2 resources match one reserved worker or pool")
  return matches[0] ?? null
}
function volumeClaim(adapter: Ec2GrowthAdapter, template: Ec2WorkerTemplate, workerId: string, claimId: string, size: number): PoolClaim {
  return {
    claimId, workerId, size, poolId: template.pool.poolId, zone: template.zone, kmsKeyId: template.pool.kmsKeyId,
    stagingDigest: state.get(adapter)!.fingerprints.get(template.zone)!,
  }
}
function stagedResources(adapter: Ec2GrowthAdapter, workers: Instances, volumes: Volumes,
  template: Ec2WorkerTemplate, workerId: string, claimId: string, size: number): [Ec2Instance | null, Ec2Volume | null] {
  const instance = slotRecord(workers, workerId, undefined, claimId)
  const volume = slotRecord(volumes, workerId, template.pool.poolId, claimId)
  if (volume) {
    stagedEc2Volume(adapter.provider, volume, volumeClaim(adapter, template, workerId, claimId, size))
    if (volume.Size !== size || !["creating", "available", "in-use"].includes(volume.State)) {
      throw new ProviderError("EBS staging volume changed size or is unavailable")
    }
    if (!instance && (volume.Attachments?.length || volume.State === "in-use")) {
      throw new ProviderError("EBS staging volume is attached without the exact retained worker")
    }
  } else if (instance) {
    throw new ProviderError("EC2 staging worker has no exact retained volume")
  }
  return [instance, volume]
}

async function instanceClaim(adapter: Ec2GrowthAdapter, template: Ec2WorkerTemplate, enrollment: JsonObject,
  claimId: string, volumeId: string, size: number, deadline: number): Promise<Ec2StagedWorkerClaim> {
  return { claimId, enrollment, zone: template.zone, subnetId: template.subnetId, volumeId, size,
    stagingDigest: state.get(adapter)!.fingerprints.get(template.zone)!, template: await templateData(adapter, template, deadline) }
}

function mountExpectation(adapter: Ec2GrowthAdapter, worker: JsonObject, volumeId: string, slot: Ec2PoolSlot,
  generation: number | bigint = 1): JsonObject {
  return {
    profile: "ebs", worker, generation, mount_path: slot.mountPath, provider_volume_id: volumeId, filesystem_uuid: slot.filesystemUuid,
    identity: { engine: "oltp", pool_format_version: 1, cluster_id: adapter.clusterId, worker_id: worker.worker_id, pool_id: slot.poolId },
  }
}

function stagedAttachment(adapter: Ec2GrowthAdapter, template: Ec2WorkerTemplate, instance: Ec2Instance, volume: Ec2Volume): boolean {
  const attachment = ec2Attachment(adapter.provider, volume, instance, template.pool.device)
  const mappings = (instance.BlockDeviceMappings ?? []).filter(row => row.DeviceName === template.pool.device).map(row => row.Ebs ?? {})
  if (mappings.length > 1 || (mappings.length === 1 && (mappings[0].VolumeId !== volume.VolumeId
    || mappings[0].DeleteOnTermination !== false))) {
    throw new ProviderError("EC2 instance maps the staging device to a different or unretained volume")
  }
  return attachment?.State === "attached" && mappings.length === 1 && mappings[0].Status === "attached" && volume.State === "in-use"
}

async function stagedReady(adapter: Ec2GrowthAdapter, template: Ec2WorkerTemplate, worker: JsonObject, record: Ec2Instance,
  volumes: Volumes, inventory: JsonObject, deadline: number): Promise<void> {
  const [enrollment] = currentInputs(adapter, template)
  const tags = ec2Tags(record)
  const claimId = identity(tags["nabedra:claim"])
  const size = tags["nabedra:volume-gib"]
  const volumeId = tags["nabedra:volume"]
  const primary = poolsOf(inventory).filter(row => row.primary)
  if (!isDeepStrictEqual(worker, enrollmentWorker(enrollment)) || typeof size !== "string" || !/^[1-9][0-9]{0,4}(?![\s\S])/.test(size)
    || !Object.hasOwn(volumes, volumeId) || primary.length !== 1 || primary[0].growth_claim !== claimId
    || primary[0].pool_id !== template.pool.poolId || !isDeepStrictEqual(primary[0].expected, mountExpectation(adapter, worker, volumeId, template.pool))) {
    throw new ProviderError("Ready EC2 worker does not match its staged claim and native primary mount")
  }
  const originalSize = Number(size)
  stagedEc2Volume(adapter.provider, volumes[volumeId], volumeClaim(adapter, template, identity(worker.worker_id), claimId, originalSize))
  stagedEc2Worker(adapter.provider, record, await instanceClaim(adapter, template, enrollment, claimId, volumeId, originalSize, deadline))
  if (!stagedAttachment(adapter, template, record, volumes[volumeId])) {
    throw new ProviderError("Ready EC2 worker lost the exact retained mount device")
  }
}

function floor(workers: Instances, volumes: Volumes, policy: JsonObject, reservedWorker: string | null, reservedVolume: string | null): bigint {
  let size = 0n
  for (const [resource, volume] of Object.entries(volumes)) {
    if (resource === reservedVolume) continue
    if (!Number.isSafeInteger(volume.Size) || volume.Size <= 0) throw new ProviderError("EBS inventory has an invalid retained volume size")
    size += BigInt(volume.Size)
  }
  const workersCount = Object.keys(workers).length - Number(reservedWorker !== null && Object.hasOwn(workers, reservedWorker))
  return BigInt(workersCount) * asQuantity(policy.worker_cost) + size * asQuantity(policy.volume_cost_per_gib)
}
function validSize(adapter: Ec2GrowthAdapter, template: Ec2WorkerTemplate, size: unknown): boolean {
  return bounded(size, 1n, asQuantity(template.maxRequestBytes))
    && BigInt(ec2Size(adapter.provider, size)) * gib - asQuantity(size) >= asQuantity(template.filesystemReserveBytes)
}
function verifyAttachment(adapter: Ec2GrowthAdapter, volume: Ec2Volume, worker: Ec2Instance, expected: JsonObject): void {
  ownedEc2(adapter.provider, volume, {
    "nabedra:pool": identity(requireRecord(expected.identity).pool_id),
    "nabedra:worker": identity(requireRecord(expected.worker).worker_id),
  })
  if (ec2Attachment(adapter.provider, volume, worker)?.State !== "attached") {
    throw new ProviderError("EBS does not exclusively belong to the current worker")
  }
}
function workerIntent(adapter: Ec2GrowthAdapter, intent: JsonObject, target: GrowthTarget):
  [Ec2WorkerTemplate, JsonObject, PoolClaim] {
  const claimId = identity(intent.claim_id)
  const action = requireRecord(intent.action)
  const template = state.get(adapter)!.templates.get(getString(action.zone))
  if (!template || !isDeepStrictEqual(adapter.target(claimId, action, {}), target)
    || !isDeepStrictEqual(intent.worker, target.worker) || intent.pool_id !== target.poolId
    || !compareQuantity(intent.pool_generation, target.poolGeneration)
    || !bounded(intent.quoted_monthly_cost, asQuantity(template.monthlyCost), asQuantity(intent.monthly_spend_ceiling))) {
    throw new ProviderError("EC2 staging template differs from the exact priced claim")
  }
  const [enrollment] = currentInputs(adapter, template)
  const size = ec2Size(adapter.provider, action.bytes)
  return [template, enrollment, volumeClaim(adapter, template, identity(enrollment.worker_id), claimId, size)]
}

export class Ec2GrowthAdapter implements StatefulGrowthAdapter {
  readonly provider: Ec2Provider
  readonly clusterId: string
  readonly adapterId: string
  readonly instances: Readonly<Record<string, string>>
  readonly poolSlots: Readonly<Record<string, Ec2PoolSlot>>

  constructor({ provider, adapterId, instances, poolSlots, templates = [] }: Ec2GrowthOptions) {
    this.provider = provider
    this.clusterId = identity(provider.clusterId)
    this.adapterId = identity(adapterId)
    this.instances = Object.freeze(Object.fromEntries(Object.entries(instances).map(([id, instance]) => [identity(id), instance])))
    const slots = Object.entries(poolSlots ?? {})
    if (slots.some(([id, slot]) => !Object.hasOwn(this.instances, id) || Object.getPrototypeOf(slot) !== Ec2PoolSlot.prototype)
      || new Set(slots.map(([, slot]) => slot.poolId)).size !== slots.length) {
      throw new ProviderError("EC2 pool slots must belong to fixed instances and have distinct pool identities")
    }
    this.poolSlots = Object.freeze(Object.fromEntries(slots))
    const inner: State = { templates: new Map(), byWorker: new Map(), fingerprints: new Map(), endpoints: {} }
    state.set(this, inner)
    const pools = new Set(slots.map(([, slot]) => slot.poolId))
    const files = new Set<string>()
    const names = new Set<unknown>()
    const certificates = new Set<unknown>()
    const incarnations = new Set<unknown>()
    const mountKeys = new Set<string>()
    for (const template of templates) {
      if (Object.getPrototypeOf(template) !== Ec2WorkerTemplate.prototype || !provider.zones.includes(template.zone)
        || inner.templates.has(template.zone) || Object.getPrototypeOf(template.pool) !== Ec2PoolSlot.prototype
        || pools.has(template.pool.poolId) || Buffer.byteLength(template.pool.mountPath) > 1024 || template.pool.mountPath.includes("\0")
        || typeof template.subnetId !== "string" || !subnet.test(template.subnetId)
        || typeof template.mountPublicKey !== "string" || !fingerprint.test(template.mountPublicKey)
        || !/[1-9a-f]/.test(template.mountPublicKey) || mountKeys.has(template.mountPublicKey)
        || !bounded(template.filesystemReserveBytes, 1n, gib - 1n)
        || !bounded(template.maxRequestBytes, 1n, BigInt(provider.maxVolumeGib) * gib)
        || !bounded(template.monthlyCost, 1n, maximumUint64)) {
        throw new ProviderError("EC2 requires one bounded prospective worker slot per configured zone")
      }
      const region = provider.client.meta.regionName
      const kms = /^arn:aws(?:-[a-z]+)*:kms:([^:]+):[0-9]{12}:key\/(?:mrk-[a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})(?![\s\S])/.exec(template.pool.kmsKeyId)
      if (!region || kms?.[1] !== region) throw new ProviderError("EC2 staging requires a KMS key ARN in the configured client region")
      const [enrollment, token] = rawInputs(template)
      const workerId = identity(enrollment.worker_id)
      const name = enrollment.server_name
      const certificate = enrollment.certificate_sha256
      const incarnation = enrollment.incarnation
      if (Object.hasOwn(this.instances, workerId) || inner.byWorker.has(workerId) || names.has(name)
        || certificates.has(certificate) || incarnations.has(incarnation) || files.has(template.enrollmentFile) || files.has(template.adminTokenFile)) {
        throw new ProviderError("EC2 staging templates must have distinct worker identities and private inputs")
      }
      inner.templates.set(template.zone, template)
      inner.byWorker.set(workerId, template)
      inner.fingerprints.set(template.zone, launchFingerprint(this, template, enrollment, token))
      pools.add(template.pool.poolId)
      names.add(name)
      certificates.add(certificate)
      incarnations.add(incarnation)
      files.add(template.enrollmentFile)
      files.add(template.adminTokenFile)
      mountKeys.add(template.mountPublicKey)
    }
  }

  bindEndpoints(endpoints: Endpoints): Ec2GrowthAdapter {
    const inner = state.get(this)!
    const matched: Record<string, GrowthController> = {}
    const urls = Object.values(endpoints).map(growthEndpoint)
    for (const [workerId, template] of inner.byWorker) {
      const endpoint = endpoints[workerId]
      if (!endpoint) continue
      const [, token] = currentInputs(this, template)
      const connected = growthEndpoint(endpoint)
      if (urls.filter(url => url === connected).length === 1 && growthMatchesCredentials(endpoint, template.adminUrl, token)) {
        matched[workerId] = endpoint
      }
    }
    const bound = Object.assign(Object.create(Object.getPrototypeOf(this)) as Ec2GrowthAdapter, this)
    state.set(bound, { ...inner, endpoints: Object.freeze(matched) })
    return bound
  }

  async observe(inventories: Inventory, { policy, deadline, claimId }: {
    policy: JsonObject; deadline: number; claimId?: string | null
  }): Promise<ProviderObservation> {
    const inner = state.get(this)!
    const [workers, volumes] = await this.provider.retainedInventory({ deadline })
    const hosts: Record<string, HostObservation> = {}
    const ready = new Set<string>()
    for (const [workerId, sample] of Object.entries(inventories)) {
      const inventory = dataOf(sample)
      const worker = requireRecord(inventory.worker)
      let instanceId: string | undefined = this.instances[workerId]
      if (instanceId === undefined && Object.hasOwn(inner.endpoints, workerId)) {
        const record = slotRecord(workers, workerId)
        if (record) {
          await stagedReady(this, inner.byWorker.get(workerId)!, worker, record, volumes, inventory, deadline)
          instanceId = record.InstanceId
        }
      }
      if (instanceId === undefined || !Object.hasOwn(workers, instanceId) || ready.has(instanceId)) {
        throw new ProviderError("Ready worker has no exact EC2 provider instance")
      }
      const record = workers[instanceId]
      const zone = workerZone(record, worker)
      const cpu = requireRecord(record.CpuOptions ?? {})
      const { CoreCount: cores, ThreadsPerCore: threads } = cpu
      if (typeof cores !== "number" || !Number.isSafeInteger(cores) || cores <= 0
        || typeof threads !== "number" || !Number.isSafeInteger(threads) || threads <= 0) {
        throw new ProviderError("EC2 did not report the current worker CPU limit")
      }
      const mountedPools = poolsOf(inventory)
      const expandable = new Set<string>()
      for (const row of mountedPools) {
        const expected = requireRecord(row.expected)
        const resource = getString(expected.provider_volume_id)
        if (!Object.hasOwn(volumes, resource)) throw new ProviderError("Mounted EBS volume is missing from retained inventory")
        const volume = volumes[resource]
        verifyAttachment(this, volume, record, expected)
        const mounted = asQuantity(row.observed_total_bytes)
        if (BigInt(volume.Size) * gib < mounted) throw new ProviderError("Mounted bytes exceed the observed provider volume size")
        if (Object.hasOwn(this.instances, workerId) && mounted < BigInt(this.provider.maxVolumeGib) * gib) expandable.add(resource)
      }
      const slot = this.poolSlots[workerId]
      const freeSlot = slot !== undefined && !(record.BlockDeviceMappings ?? []).some(entry => entry.DeviceName === slot.device)
        && !mountedPools.some(row => row.pool_id === slot.poolId || requireRecord(row.expected).mount_path === slot.mountPath)
      hosts[workerId] = new HostObservation(worker, zone, Number(freeSlot), expandable, cores * threads * 1000)
      ready.add(instanceId)
    }

    const zones = new Set<string>()
    let reservedWorker: string | null = null
    let reservedVolume: string | null = null
    const allowedZones = policyZones(policy)
    for (const [workerId, template] of inner.byWorker) {
      if (!Object.hasOwn(inner.endpoints, workerId) || Object.hasOwn(inventories, workerId) || !allowedZones.includes(template.zone)) continue
      const [enrollment] = currentInputs(this, template)
      const instance = slotRecord(workers, workerId)
      const volume = slotRecord(volumes, workerId, template.pool.poolId)
      if (instance && (claimId == null || ec2Tags(instance)["nabedra:claim"] !== claimId)) continue
      if (volume) {
        if (claimId == null || ec2Tags(volume)["nabedra:claim"] !== claimId) continue
        const size = volume.Size
        const [stagedInstance, stagedVolume] = stagedResources(this, workers, volumes, template, workerId, claimId, size)
        if (reservedVolume !== null) throw new ProviderError("More than one staging volume uses the current EC2 claim")
        if (!stagedVolume) throw new ProviderError("Retained EC2 staging volume is missing")
        reservedVolume = stagedVolume.VolumeId
        if (stagedInstance) {
          stagedEc2Worker(this.provider, stagedInstance, await instanceClaim(this, template, enrollment, claimId, reservedVolume, size, deadline))
          reservedWorker = stagedInstance.InstanceId
        }
      } else if (instance) continue
      const baseline = requireRecord(policy.automatic).pool_increment_bytes
      if (!bounded(baseline, 1n, asQuantity(template.maxRequestBytes))) continue
      if (asQuantity(policy.worker_cost) + BigInt(ec2Size(this.provider, baseline)) * asQuantity(policy.volume_cost_per_gib)
        < asQuantity(template.monthlyCost)) continue
      await templateData(this, template, deadline)
      zones.add(template.zone)
    }
    const spend = floor(workers, volumes, policy, reservedWorker, reservedVolume)
    const retained = Object.keys(workers).filter(instance => !ready.has(instance) && instance !== reservedWorker).length
    return new ProviderObservation(hosts, retained, spend <= BigInt(Number.MAX_SAFE_INTEGER) && spend >= BigInt(Number.MIN_SAFE_INTEGER) ? Number(spend) : spend, zones)
  }

  target(claimId: string, action: JsonObject, inventories: Inventory): GrowthTarget | null {
    void claimId
    if (action.kind === "add_worker") {
      const inner = state.get(this)!
      const template = inner.templates.get(getString(action.zone))
      if (!template || !validSize(this, template, action.bytes)) return null
      const [enrollment] = currentInputs(this, template)
      const worker = enrollmentWorker(enrollment)
      const workerId = identity(worker.worker_id)
      if (!Object.hasOwn(inner.endpoints, workerId) || Object.hasOwn(inventories, workerId)
        || Object.values(inventories).some(sample => {
          const current = requireRecord(dataOf(sample).worker)
          return current.incarnation === worker.incarnation || current.certificate_sha256 === worker.certificate_sha256
        })) return null
      return new GrowthTarget(worker, template.pool.poolId, 1)
    }
    const max = BigInt(this.provider.maxVolumeGib) * gib
    if (action.kind === "add_pool") {
      const workerId = identity(action.worker)
      const slot = this.poolSlots[workerId]
      if (!slot || asQuantity(action.bytes) > max) return null
      return new GrowthTarget(requireRecord(dataOf(inventories[workerId]).worker), slot.poolId, 1)
    }
    if (action.kind !== "expand" || !Object.hasOwn(this.instances, getString(action.worker)) || asQuantity(action.target_bytes) > max) return null
    const inventory = dataOf(inventories[getString(action.worker)])
    const row = poolsOf(inventory).find(row => row.pool_id === action.pool)
    if (!row) return null
    const expected = requireRecord(row.expected)
    if (expected.profile !== "ebs") return null
    const generation = asQuantity(expected.generation)
    return new GrowthTarget(requireRecord(inventory.worker), identity(row.pool_id),
      generation <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(generation) : generation, getString(expected.provider_volume_id))
  }

  async provision(intent: JsonObject, target: GrowthTarget, { deadline }: { deadline: number }): Promise<JsonObject> {
    const action = requireRecord(intent.action)
    if (action.kind === "add_worker") throw new ProviderError("EC2 worker provisioning requires a current automatic Control attempt")
    const workerId = identity(target.worker.worker_id)
    const record = await this.provider.inspectWorker(this.instances[workerId], { deadline })
    const zone = workerZone(record, target.worker)
    if (action.kind === "add_pool") {
      const slot = this.poolSlots[workerId]
      if (action.zone !== zone || target.poolId !== slot?.poolId) throw new ProviderError("Reserved EBS slot and growth claim differ")
      return this.provider.ensurePool({ claimId: identity(intent.claim_id), poolId: target.poolId, workerId, zone,
        bytes: asQuantity(action.bytes), kmsKeyId: slot.kmsKeyId, deadline })
    }
    const volumeId = getString(target.resourceId)
    const volume = await this.provider.inspectVolume(volumeId, { deadline })
    verifyAttachment(this, volume, record, { identity: { pool_id: target.poolId }, worker: target.worker })
    return this.provider.expandPool({ volumeId, targetBytes: asQuantity(action.target_bytes), deadline })
  }

  async provisionWorker(intent: JsonObject, target: GrowthTarget, { authorize, deadline }: {
    authorize: Ec2Authorize; deadline: number
  }): Promise<JsonObject> {
    const [template, enrollment, claim] = workerIntent(this, intent, target)
    const { claimId, workerId, size } = claim
    const [workers, volumes] = await this.provider.retainedInventory({ deadline })
    let [instance, volume] = stagedResources(this, workers, volumes, template, workerId, claimId, size)
    if (!volume) {
      const created = await this.provider.ensurePool({ ...claim, bytes: asQuantity(requireRecord(intent.action).bytes), authorize, deadline })
      volume = await this.provider.inspectVolume(created.resource_id, { deadline })
      stagedEc2Volume(this.provider, volume, claim)
      if (volume.Size !== size || volume.Attachments?.length) throw new ProviderError("EBS staged volume size or attachment changed")
    }
    if (volume.State === "creating") throw new ProviderError("The exact encrypted EBS volume is still being created")
    if (!instance && volume.State !== "available") throw new ProviderError("The exact encrypted EBS volume is unavailable for an instance")
    const volumeId = volume.VolumeId
    const options = await instanceClaim(this, template, enrollment, claimId, volumeId, size, deadline)
    instance = instance ? stagedEc2Worker(this.provider, instance, options) : await this.provider.stageWorker({ ...options, authorize, deadline })
    if (instance.State?.Name !== "running") throw new ProviderError("The exact staged EC2 instance is pending or unavailable")
    const instanceId = instance.InstanceId
    if (!stagedAttachment(this, template, instance, volume)) {
      await this.provider.attachPool({ claimId, poolId: target.poolId, worker: target.worker, volumeId,
        instanceId, device: template.pool.device, authorize, deadline })
      instance = await this.provider.inspectWorker(instanceId, { deadline })
      volume = await this.provider.inspectVolume(volumeId, { deadline })
      stagedEc2Worker(this.provider, instance, options)
      stagedEc2Volume(this.provider, volume, claim)
    }
    if (!stagedAttachment(this, template, instance, volume)) {
      throw new ProviderError("The exact EC2 and EBS attachment is pending or unavailable")
    }
    return { resource_id: volumeId, state: "attached" }
  }

  async registerWorker(intent: JsonObject, target: GrowthTarget, record: JsonObject, { controller, pin, deadline }: {
    controller: GrowthController; pin: JsonObject; deadline: number
  }): Promise<{ state: "awaiting_bootstrap"; stage: Ec2WorkerStage }> {
    const [template, enrollment, claim] = workerIntent(this, intent, target)
    const { claimId, workerId, size } = claim
    const volumeId = getString(record.provider_resource)
    const snapshot = () => growthRead(controller, "storage.growth.snapshot", {}, { deadline })
    const before = await snapshot()
    const policy = requireRecord(before.policy)
    const auto = requireRecord(policy.automatic ?? {})
    const quote = asQuantity(policy.worker_cost) + BigInt(size) * asQuantity(policy.volume_cost_per_gib)
    if (!isDeepStrictEqual(before.pin, pin) || before.cluster_id !== this.clusterId || !before.active_automatic
      || auto.adapter_id !== this.adapterId || policy.policy_id !== intent.policy_id || !policyZones(policy).includes(template.zone)
      || !compareQuantity(quote, intent.quoted_monthly_cost) || !isDeepStrictEqual(before.active, record)
      || record.phase !== "awaiting_mount" || !isDeepStrictEqual(record.intent, intent)) {
      throw new ProviderError("The current Control claim does not authorize EC2 staging")
    }
    const authorize = async () => {
      if (!isDeepStrictEqual(await snapshot(), before)) throw new ProviderError("Control changed during EC2 staging")
    }

    const [workers, volumes] = await this.provider.retainedInventory({ deadline })
    const [instance, volume] = stagedResources(this, workers, volumes, template, workerId, claimId, size)
    if (!instance || !volume || volume.VolumeId !== volumeId) throw new ProviderError("The durable EC2 staging resources are absent or changed")
    const options = await instanceClaim(this, template, enrollment, claimId, volumeId, size, deadline)
    stagedEc2Worker(this.provider, instance, options)
    const instanceId = instance.InstanceId
    const retainedCost = floor(workers, volumes, policy, instanceId, volumeId) - asQuantity(policy.initial_monthly_spend)
    const extra = retainedCost > 0n ? retainedCost : 0n
    const ready = requireRecord(pin.ready)
    const ceiling = asQuantity(auto.max_workers)
    if (asQuantity(pin.monthly_spend) + extra > asQuantity(policy.monthly_spend_ceiling)
      || Object.keys(workers).length > ceiling || asQuantity(ready.ready_count) > ceiling) {
      throw new ProviderError("Retained EC2 resources exceed the current growth ceiling")
    }
    if (instance.State?.Name !== "running" || !stagedAttachment(this, template, instance, volume)) {
      throw new ProviderError("EC2 bootstrap requires the running instance and exact retained EBS attachment")
    }
    if (!ec2InstanceId.test(instanceId)) throw new ProviderError("Exact EC2 instance identity is required")
    const preparation = {
      claim_id: claimId, run_id: identity(requireRecord(ready.view).run_id), enrollment,
      mount: mountExpectation(this, target.worker, volumeId, template.pool), mount_public_key: template.mountPublicKey,
      native_target: { kind: "ec2", instance_id: instanceId },
    }
    await authorize()
    const prepared = await growthOperation(controller, claimId, "prepare-worker", "storage.growth.prepare_worker", preparation, { deadline })
    const after = await snapshot()
    const afterPin = requireRecord(after.pin)
    const revision = asQuantity(pin.enrollment_revision)
    if (!isDeepStrictEqual(prepared, { identity: target.worker }) || !isDeepStrictEqual(after.active, record)
      || !after.active_automatic || after.cluster_id !== this.clusterId || !isDeepStrictEqual(after.policy, policy)
      || Object.entries(pin).some(([key, value]) => key !== "enrollment_revision" && !isDeepStrictEqual(afterPin[key], value))
      || !bounded(afterPin.enrollment_revision, revision, revision + 1n)) {
      throw new ProviderError("Control changed before the EC2 bootstrap handoff")
    }
    return { state: "awaiting_bootstrap", stage: {
      instance_id: instanceId, zone: template.zone, subnet_id: template.subnetId, device: template.pool.device,
      admin_url: template.adminUrl, bootstrap_intent: preparation,
    } }
  }

  async register(intent: JsonObject, target: GrowthTarget, record: JsonObject, { controller, deadline }: {
    controller: GrowthController; deadline: number
  }): Promise<JsonObject | null> {
    const instanceId = this.instances[identity(target.worker.worker_id)]
    const action = requireRecord(intent.action)
    if (action.kind === "add_pool") {
      const slot = this.poolSlots[identity(target.worker.worker_id)]
      const volumeId = getString(record.provider_resource)
      const attachment = await this.provider.attachPool({ claimId: identity(intent.claim_id), poolId: target.poolId,
        worker: target.worker, volumeId, instanceId, device: slot.device, deadline })
      if (attachment.state !== "attached") return null
      return mountExpectation(this, target.worker, volumeId, slot, target.poolGeneration)
    }
    if (this.provider.localHost?.instanceId === instanceId && this.provider.localHost !== null) {
      const inventory = await growthRead(controller, "storage.growth.inventory", {}, { deadline })
      const mounted = poolsOf(inventory).find(row => row.pool_id === target.poolId)
      const expected = mounted ? requireRecord(mounted.expected) : null
      if (!isDeepStrictEqual(inventory.worker, target.worker) || inventory.ready !== true || !mounted || !expected
        || !compareQuantity(expected.generation, target.poolGeneration) || expected.provider_volume_id !== target.resourceId
        || expected.profile !== "ebs") {
        throw new ProviderError("EBS grow target no longer matches the verified mounted pool")
      }
      if (asQuantity(mounted.observed_total_bytes) < asQuantity(action.target_bytes)) {
        await this.provider.growFilesystem({ instanceId, expected, targetBytes: asQuantity(action.target_bytes), deadline })
      }
    }
    // The worker's native mount observation completes the claim.
    return null
  }
}

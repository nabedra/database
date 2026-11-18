import { ProviderError } from "../providers.js"
import { isJsonObject, type JsonObject } from "../transport.js"
import { CloudLifecycleControl, closedPool, closedWorker, ec2WorkerTags } from "./cloudControl.js"
import { EbsLinuxHost } from "./ebsHost.js"
import { digest, identity, monotonic, requireRecord } from "./shared.js"

export type Ec2Bytes = bigint | number
export type Ec2Authorize = () => void | Promise<void>
type Tags = Record<string, string>
type Tag = { Key: string; Value: string }
type Filter = { Name: string; Values: string[] }
type LaunchTemplate = { LaunchTemplateId: string; Version: string }
type Page = { Filters?: Filter[]; MaxResults?: number; NextToken?: string }
type Placement = { AvailabilityZone: string }
type CredentialsConfig = { connectTimeout: number; readTimeout: number; retries: { mode: "standard"; totalMaxAttempts: 1 } }

interface Requests {
  describeVolumes: Page & { VolumeIds?: string[] }
  createVolume: { ClientToken: string; AvailabilityZone: string; Size: number; VolumeType: "gp3";
    Encrypted: true; KmsKeyId: string; TagSpecifications: { ResourceType: "volume"; Tags: Tag[] }[] }
  describeVolumesModifications: { VolumeIds: string[] }
  modifyVolume: { VolumeId: string; Size: number }
  describeInstances: Page & { InstanceIds?: string[] }
  runInstances: { ClientToken: string; MinCount: number; MaxCount: number; LaunchTemplate: LaunchTemplate;
    SubnetId: string; Placement: Placement; UserData?: string; TagSpecifications: { ResourceType: "instance"; Tags: Tag[] }[] }
  describeLaunchTemplateVersions: { LaunchTemplateId: string; Versions: string[] }
  describeSubnets: { SubnetIds: string[] }
  describeInstanceTypes: { InstanceTypes: string[] }
  attachVolume: { VolumeId: string; InstanceId: string; Device: string }
  stopInstances: { InstanceIds: string[]; Force: false; SkipOsShutdown: false; Hibernate: false }
  detachVolume: { VolumeId: string; InstanceId: string; Device: string; Force: false }
}

export type Ec2Client = {
  readonly meta: { readonly config: CredentialsConfig; readonly regionName?: string }
} & { [K in keyof Requests]: (request: Requests[K]) => unknown }

export interface Ec2ProviderOptions {
  client: Ec2Client
  clusterId: string
  zones: readonly string[]
  launchTemplate: LaunchTemplate
  maxVolumeGib: number
  lifecycleAuthority?: CloudLifecycleControl | null
  localHost?: EbsLinuxHost | null
}

interface Deadline { deadline?: number | null }
interface RequiredDeadline { deadline: number }
interface PoolClaim {
  claimId: string
  poolId: string
  workerId: string
  zone: string
  kmsKeyId: string
  stagingDigest?: string | null
}
export interface EnsureEc2PoolOptions extends PoolClaim, Deadline {
  bytes: Ec2Bytes
  authorize?: Ec2Authorize | null
}
export interface EnsureEc2WorkerOptions extends Deadline {
  claimId: string
  workerId: string
  incarnation: string
  zone: string
  subnetId: string
  userData: string
  enrollment?: JsonObject | null
}
export interface AttachEc2PoolOptions extends Deadline {
  claimId: string
  poolId: string
  worker: JsonObject
  volumeId: string
  instanceId: string
  device: string
  authorize?: Ec2Authorize | null
}
export interface GrowEc2FilesystemOptions extends RequiredDeadline {
  instanceId: string
  expected: JsonObject
  targetBytes: Ec2Bytes
}
export interface Ec2StagingTemplateOptions extends RequiredDeadline {
  zone: string
  subnetId: string
  device: string
}
export interface Ec2StagedWorkerClaim {
  claimId: string
  enrollment: JsonObject
  zone: string
  subnetId: string
  volumeId: string
  size: number
  stagingDigest: string
  template: JsonObject
}
export interface StageEc2WorkerOptions extends Ec2StagedWorkerClaim, RequiredDeadline {
  authorize: Ec2Authorize
}
export interface Ec2Resource extends JsonObject { resource_id: string; state: string }
export interface Ec2InventoryRecord extends JsonObject { Tags?: Tag[] }
export interface Ec2Volume extends Ec2InventoryRecord {
  VolumeId: string
  State: string
  Size: number
  AvailabilityZone?: string
  KmsKeyId?: string
  VolumeType?: string
  Encrypted?: boolean
  SnapshotId?: string | null
  MultiAttachEnabled?: boolean
  Attachments?: Ec2Attachment[]
}
export interface Ec2Attachment extends JsonObject {
  InstanceId?: string
  VolumeId?: string
  DeleteOnTermination?: boolean
  Device?: string
  State?: string
}
interface BlockDeviceMapping {
  DeviceName?: string
  Ebs?: { VolumeId?: string; DeleteOnTermination?: boolean; Status?: string }
}
export interface Ec2Instance extends Ec2InventoryRecord {
  InstanceId: string
  State?: { Name?: string }
  Placement?: Placement
  BlockDeviceMappings?: BlockDeviceMapping[]
  RootDeviceName?: string
  RootDeviceType?: string
}

const gib = 1n << 30n
const volumePattern = /^vol-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/
const instancePattern = /^i-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/
const subnetPattern = /^subnet-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/
const templatePattern = /^lt-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/
const imagePattern = /^ami-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/
export const ec2SecondaryDevice = /^\/dev\/sd[f-p](?![\s\S])/

function matches(pattern: RegExp, value: unknown): value is string {
  return typeof value === "string" && pattern.test(value)
}
function present(value: unknown): boolean {
  if (Array.isArray(value)) return value.length > 0
  if (isJsonObject(value)) return Object.keys(value).length > 0
  return Boolean(value)
}
function duration(value: unknown, maximum: number): value is number {
  return typeof value === "number" && value > 0 && value <= maximum
}
function volumeTags(claimId: string, poolId: string, workerId: string): Tags {
  return { "nabedra:claim": identity(claimId), "nabedra:pool": identity(poolId), "nabedra:worker": identity(workerId) }
}
export function ec2Tags(record: Ec2InventoryRecord): Tags {
  return Object.fromEntries((record.Tags ?? []).map(({ Key, Value }) => [Key, Value]))
}
function tagsFor(provider: Ec2Provider, claimId: string, content: unknown): Tags {
  return { "nabedra:cluster": provider.clusterId, "nabedra:claim": identity(claimId), "nabedra:content": digest(content) }
}
function toAwsTags(tags: Tags): Tag[] {
  return Object.entries(tags).map(([Key, Value]) => ({ Key, Value }))
}
export function ownedEc2<T extends Ec2InventoryRecord>(provider: Ec2Provider, record: T, expected: Tags = {}): T {
  const tags = ec2Tags(record)
  if (tags["nabedra:cluster"] !== provider.clusterId
    || Object.entries(expected).some(([key, value]) => tags[key] !== value)) {
    throw new ProviderError("EC2 resource ownership or immutable claim differs")
  }
  return record
}
export function ec2Size(provider: Ec2Provider, bytes: unknown): number {
  if ((typeof bytes !== "bigint" && !(typeof bytes === "number" && Number.isSafeInteger(bytes)))
    || (bytes as Ec2Bytes) <= 0) {
    throw new ProviderError("Positive storage bytes are required")
  }
  const size = (BigInt(bytes as Ec2Bytes) + gib - 1n) / gib
  if (size > provider.maxVolumeGib) throw new ProviderError("EC2 volume exceeds configured ceiling")
  return Number(size)
}
function poolTags(provider: Ec2Provider,
  { claimId, poolId, workerId, zone, kmsKeyId, stagingDigest, size }: PoolClaim & { size: number }): Tags {
  const content: JsonObject = { pool_id: identity(poolId), worker_id: identity(workerId), zone, gib: size, kms_key_id: kmsKeyId }
  if (stagingDigest != null) content.staging_digest = stagingDigest
  return { ...tagsFor(provider, claimId, content), ...volumeTags(claimId, poolId, workerId) }
}
export function stagedEc2Volume(provider: Ec2Provider, record: Ec2Volume, claim: PoolClaim & { size: number }): Ec2Volume {
  ownedEc2(provider, record, poolTags(provider, claim))
  if (!matches(volumePattern, record.VolumeId) || record.AvailabilityZone !== claim.zone || record.KmsKeyId !== claim.kmsKeyId
    || record.VolumeType !== "gp3" || !Number.isSafeInteger(record.Size) || record.Size < claim.size || record.Encrypted !== true
    || record.MultiAttachEnabled !== false || (record.SnapshotId != null && record.SnapshotId !== "")) {
    throw new ProviderError("EBS staging volume differs from the exact encrypted disk claim")
  }
  return record
}
function stagingTags(provider: Ec2Provider, claim: Omit<Ec2StagedWorkerClaim, "template">): Tags {
  const { claimId, enrollment, zone, subnetId, volumeId, size, stagingDigest } = claim
  return { ...tagsFor(provider, claimId, { enrollment, zone, subnet_id: subnetId,
    volume_id: volumeId, size, template: provider.launchTemplate, staging_digest: stagingDigest }),
    ...ec2WorkerTags(enrollment), "nabedra:volume": volumeId, "nabedra:volume-gib": String(size) }
}

function list<T>(record: JsonObject, key: string): T[] {
  const values = record[key]
  if (values === undefined) return []
  if (!Array.isArray(values)) throw new ProviderError("EC2 returned malformed inventory")
  return values as T[]
}
function instances(response: JsonObject): Ec2Instance[] {
  return list<JsonObject>(response, "Reservations").flatMap(row => list<Ec2Instance>(row, "Instances"))
}
export function stagedEc2Worker(provider: Ec2Provider, record: Ec2Instance, claim: Ec2StagedWorkerClaim): Ec2Instance {
  ownedEc2(provider, record, stagingTags(provider, claim))
  const security = new Set(list<string>(claim.template, "SecurityGroupIds"))
  const groups = new Set(list<{ GroupId: string }>(record, "SecurityGroups").map(group => group.GroupId))
  const root = list<BlockDeviceMapping>(claim.template, "BlockDeviceMappings")[0]
  if (!matches(instancePattern, record.InstanceId) || record.ClientToken !== claim.claimId
    || record.Placement?.AvailabilityZone !== claim.zone || record.SubnetId !== claim.subnetId
    || record.ImageId !== claim.template.ImageId || record.InstanceType !== claim.template.InstanceType || record.PublicIpAddress
    || groups.size !== security.size || [...groups].some(group => !security.has(group)) || record.RootDeviceType !== "ebs"
    || record.RootDeviceName !== root?.DeviceName) {
    throw new ProviderError("EC2 staging instance differs from the exact launch claim")
  }
  return record
}
export function ec2Attachment(provider: Ec2Provider, volume: Ec2Volume, worker: Ec2Instance, device?: string): Ec2Attachment | null {
  const zone = worker.Placement?.AvailabilityZone
  const attachments = volume.Attachments ?? []
  if (!zone || !provider.zones.includes(zone) || volume.AvailabilityZone !== zone) {
    throw new ProviderError("EBS volume and worker are outside the same configured zone")
  }
  if (!volume.Encrypted || volume.MultiAttachEnabled || attachments.length > 1) {
    throw new ProviderError("EBS attachments require an encrypted single-writer volume")
  }
  const attachment = attachments[0]
  if (!attachment) return null
  if (attachment.InstanceId !== worker.InstanceId || attachment.VolumeId !== volume.VolumeId
    || attachment.DeleteOnTermination !== false || (device !== undefined && attachment.Device !== device)) {
    throw new ProviderError("EBS attachment owner, retained state or device differs")
  }
  return attachment
}

export class Ec2Provider {
  readonly client: Ec2Client
  readonly clusterId: string
  readonly zones: readonly string[]
  readonly launchTemplate: Readonly<LaunchTemplate>
  readonly maxVolumeGib: number
  readonly lifecycleAuthority: CloudLifecycleControl | null
  readonly localHost: EbsLinuxHost | null
  readonly #callWindow: number

  constructor({ client, clusterId, zones, launchTemplate, maxVolumeGib, lifecycleAuthority = null, localHost = null }: Ec2ProviderOptions) {
    this.client = client
    this.clusterId = identity(clusterId)
    const { connectTimeout, readTimeout, retries } = client?.meta?.config ?? {}
    if (!duration(connectTimeout, 10) || !duration(readTimeout, 20)
      || retries?.mode !== "standard" || retries?.totalMaxAttempts !== 1) {
      throw new ProviderError("EC2 client requires finite connect/read timeouts and standard totalMaxAttempts=1")
    }
    this.#callWindow = connectTimeout + readTimeout
    if (!Array.isArray(zones) || !zones.length || new Set(zones).size !== zones.length || zones.length > 16
      || zones.some(zone => typeof zone !== "string" || !zone || [...zone].length > 128)
      || !launchTemplate || Object.keys(launchTemplate).sort().join(",") !== "LaunchTemplateId,Version"
      || !Number.isSafeInteger(maxVolumeGib) || maxVolumeGib < 1 || maxVolumeGib > 65536) {
      throw new ProviderError("Explicit EC2 zones, pinned launch template and volume ceiling are required")
    }
    if ((lifecycleAuthority !== null && Object.getPrototypeOf(lifecycleAuthority) !== CloudLifecycleControl.prototype)
      || (localHost !== null && Object.getPrototypeOf(localHost) !== EbsLinuxHost.prototype)) {
      throw new ProviderError("EC2 maintenance requires an authenticated Control endpoint and configured Linux host")
    }
    this.zones = Object.freeze([...zones])
    this.launchTemplate = Object.freeze({ ...launchTemplate })
    this.maxVolumeGib = maxVolumeGib
    this.lifecycleAuthority = lifecycleAuthority
    this.localHost = localHost
  }

  async #call<M extends keyof Requests>(method: M, args: Requests[M], deadline?: number | null): Promise<JsonObject> {
    if (deadline != null && (!Number.isFinite(deadline) || deadline - monotonic() < this.#callWindow)) {
      throw new ProviderError("EC2 inventory deadline is shorter than the configured SDK timeout")
    }
    try {
      const invoke = this.client[method] as (request: Requests[M]) => unknown
      return requireRecord(await invoke.call(this.client, args))
    } catch {
      throw new ProviderError("EC2 operation failed; reconcile the same durable claim")
    }
  }

  async inspectVolume(volumeId: string, { deadline }: Deadline = {}): Promise<Ec2Volume> {
    if (!matches(volumePattern, volumeId)) throw new ProviderError("Exact EBS volume identity is required")
    const records = list<Ec2Volume>(await this.#call("describeVolumes", { VolumeIds: [volumeId] }, deadline), "Volumes")
    if (records.length !== 1 || records[0].VolumeId !== volumeId) throw new ProviderError("EBS volume identity changed")
    return ownedEc2(this, records[0])
  }

  async ensurePool(options: EnsureEc2PoolOptions): Promise<Ec2Resource> {
    const { claimId, zone, bytes, kmsKeyId, deadline, stagingDigest, authorize } = options
    if (!this.zones.includes(zone) || typeof kmsKeyId !== "string" || !kmsKeyId) {
      throw new ProviderError("Authorized EBS zone and explicit encryption key are required")
    }
    const size = ec2Size(this, bytes)
    const tags = poolTags(this, { ...options, size })
    await authorize?.()
    const created = await this.#call("createVolume", { ClientToken: claimId, AvailabilityZone: zone,
      Size: size, VolumeType: "gp3", Encrypted: true, KmsKeyId: kmsKeyId,
      TagSpecifications: [{ ResourceType: "volume", Tags: toAwsTags(tags) }] }, deadline)
    const record = await this.inspectVolume(created.VolumeId as string, { deadline })
    if (stagingDigest != null) stagedEc2Volume(this, record, { ...options, size })
    ownedEc2(this, record, tags)
    if (record.AvailabilityZone !== zone || !Number.isSafeInteger(record.Size) || record.Size < size
      || !record.Encrypted || record.MultiAttachEnabled) {
      throw new ProviderError("EBS volume parameters differ from its claim")
    }
    return { resource_id: record.VolumeId, state: record.State }
  }

  async expandPool({ volumeId, targetBytes, deadline }: Deadline & {
    volumeId: string; targetBytes: Ec2Bytes
  }): Promise<Ec2Resource & { requested_gib: number }> {
    const record = await this.inspectVolume(volumeId, { deadline })
    const size = ec2Size(this, targetBytes)
    if (!record.Encrypted || record.MultiAttachEnabled) {
      throw new ProviderError("EBS growth requires an encrypted single-writer volume")
    }
    if (size > record.Size) {
      const change = await this.#latestModification(volumeId, deadline)
      if (change && (change.ModificationState === "modifying" || change.ModificationState === "optimizing")) {
        if (change.TargetSize !== size) throw new ProviderError("EBS volume has another unfinished growth request")
      } else {
        await this.#call("modifyVolume", { VolumeId: volumeId, Size: size }, deadline)
      }
    }
    return { resource_id: volumeId, state: "awaiting_filesystem", requested_gib: size }
  }

  async #latestModification(volumeId: string, deadline?: number | null): Promise<JsonObject | null> {
    const response = await this.#call("describeVolumesModifications", { VolumeIds: [volumeId] }, deadline)
    const changes = list<JsonObject>(response, "VolumesModifications")
    if (response.NextToken || changes.length > 1 || (changes.length === 1 && changes[0].VolumeId !== volumeId)) {
      throw new ProviderError("EBS volume modification inventory is ambiguous")
    }
    return changes[0] ?? null
  }

  async growFilesystem({ instanceId, expected, targetBytes, deadline }: GrowEc2FilesystemOptions): Promise<void> {
    if (this.localHost?.instanceId !== instanceId) return
    const volumeId = expected.provider_volume_id as string
    const worker = ownedEc2(this, await this.inspectWorker(instanceId, { deadline }), ec2WorkerTags(expected.worker))
    const volume = await this.inspectVolume(volumeId, { deadline })
    const attachment = ec2Attachment(this, volume, worker)
    if (worker.State?.Name !== "running" || attachment?.State !== "attached") {
      throw new ProviderError("EBS filesystem is outside the exact running worker")
    }
    const size = ec2Size(this, targetBytes)
    const change = await this.#latestModification(volumeId, deadline)
    if (volume.Size >= size && (!change || change.ModificationState === "optimizing" || change.ModificationState === "completed")) {
      await this.localHost.grow(instanceId, expected, { deadline })
    }
  }

  async inspectWorker(instanceId: string, { deadline }: Deadline = {}): Promise<Ec2Instance> {
    if (!matches(instancePattern, instanceId)) throw new ProviderError("Exact EC2 instance identity is required")
    const records = instances(await this.#call("describeInstances", { InstanceIds: [instanceId] }, deadline))
    if (records.length !== 1 || records[0].InstanceId !== instanceId) throw new ProviderError("EC2 instance identity changed")
    return ownedEc2(this, records[0])
  }

  async retainedInventory({ deadline }: RequiredDeadline): Promise<[Record<string, Ec2Instance>, Record<string, Ec2Volume>]> {
    const collect = async <T extends Ec2InventoryRecord>(
      method: "describeInstances" | "describeVolumes", readRows: (record: JsonObject) => T[], idField: string,
    ): Promise<Record<string, T>> => {
      const records: Record<string, T> = {}
      const parameters: Page = { Filters: [{ Name: "tag:nabedra:cluster", Values: [this.clusterId] }], MaxResults: 128 }
      let count = 0
      for (let page = 0; page < 32; page++) {
        const result = await this.#call(method, parameters, deadline)
        for (const record of readRows(result)) {
          const resource = record[idField]
          if (typeof resource !== "string" || !resource || Object.hasOwn(records, resource) || count >= 4096) {
            throw new ProviderError("EC2 retained inventory changed during paging")
          }
          Object.defineProperty(records, resource, { value: ownedEc2(this, record), enumerable: true })
          count++
        }
        const token = result.NextToken
        if (!token) return records
        if (typeof token !== "string" || token === parameters.NextToken || token.length > 4096) {
          throw new ProviderError("EC2 retained inventory cursor is invalid")
        }
        parameters.NextToken = token
      }
      throw new ProviderError("EC2 retained inventory page limit reached")
    }
    return [await collect("describeInstances", instances, "InstanceId"),
      await collect("describeVolumes", reply => list<Ec2Volume>(reply, "Volumes"), "VolumeId")]
  }

  async ensureWorker({ claimId, workerId, incarnation, zone, subnetId, userData, enrollment, deadline }:
    EnsureEc2WorkerOptions): Promise<Ec2Resource> {
    if (!this.zones.includes(zone) || typeof userData !== "string" || !userData || !userData.isWellFormed() || Buffer.byteLength(userData) > 16384) {
      throw new ProviderError("Bounded protected EC2 bootstrap and authorized zone are required")
    }
    const tags = { ...tagsFor(this, claimId, {
      worker_id: identity(workerId), incarnation: identity(incarnation), zone, subnet_id: subnetId,
      bootstrap_digest: digest(userData), template: this.launchTemplate,
    }), "nabedra:worker": workerId, "nabedra:incarnation": incarnation }
    if (enrollment != null) {
      const exact = ec2WorkerTags(enrollment)
      if (exact["nabedra:worker"] !== workerId || exact["nabedra:incarnation"] !== incarnation) {
        throw new ProviderError("EC2 bootstrap and enrollment worker identities differ")
      }
      Object.assign(tags, exact)
    }
    const result = await this.#call("runInstances", { ClientToken: claimId, MinCount: 1, MaxCount: 1,
      LaunchTemplate: { ...this.launchTemplate }, SubnetId: subnetId, Placement: { AvailabilityZone: zone },
      UserData: Buffer.from(userData, "utf8").toString("base64"), TagSpecifications: [{ ResourceType: "instance", Tags: toAwsTags(tags) }] }, deadline)
    const launched = list<Ec2Instance>(result, "Instances")
    if (launched.length !== 1) throw new ProviderError("EC2 launch did not return one instance")
    const record = ownedEc2(this, await this.inspectWorker(launched[0].InstanceId, { deadline }), tags)
    if (record.Placement?.AvailabilityZone !== zone) throw new ProviderError("EC2 launched in another failure domain")
    if (typeof record.State?.Name !== "string") throw new ProviderError("EC2 launch did not return the current state")
    return { resource_id: record.InstanceId, state: record.State.Name }
  }

  async stagingTemplate({ zone, subnetId, device, deadline }: Ec2StagingTemplateOptions): Promise<JsonObject> {
    const { LaunchTemplateId: templateId, Version: version } = this.launchTemplate
    if (!matches(templatePattern, templateId) || !matches(/^[1-9][0-9]{0,18}(?![\s\S])/, version)
      || !this.zones.includes(zone) || !matches(subnetPattern, subnetId)) {
      throw new ProviderError("EC2 staging requires a numeric template version and exact subnet")
    }
    let reply = await this.#call("describeLaunchTemplateVersions", { LaunchTemplateId: templateId, Versions: [version] }, deadline)
    const rows = list<JsonObject>(reply, "LaunchTemplateVersions")
    const versionNumber = rows[0]?.VersionNumber
    if (reply.NextToken || rows.length !== 1 || rows[0].LaunchTemplateId !== templateId
      || !(typeof versionNumber === "bigint" || (typeof versionNumber === "number" && Number.isSafeInteger(versionNumber)))
      || BigInt(versionNumber) !== BigInt(version)) {
      throw new ProviderError("EC2 staging launch template identity differs")
    }
    const data = requireRecord(rows[0].LaunchTemplateData ?? {})
    const root = list<BlockDeviceMapping & { Ebs?: { Encrypted?: boolean } }>(data, "BlockDeviceMappings")
    const metadata = requireRecord(data.MetadataOptions ?? {})
    if (!matches(imagePattern, data.ImageId) || typeof data.InstanceType !== "string" || !data.InstanceType
      || list<string>(data, "SecurityGroupIds").length === 0
      || ["UserData", "NetworkInterfaces", "Placement", "TagSpecifications", "InstanceMarketOptions", "KeyName"]
        .some(key => present(data[key]))
      || metadata.HttpTokens !== "required" || metadata.HttpEndpoint !== "enabled"
      || root.length !== 1 || !root[0].DeviceName || root[0].DeviceName === device
      || root[0].Ebs?.Encrypted !== true || root[0].Ebs?.DeleteOnTermination !== true) {
      throw new ProviderError("EC2 staging template requires private networking, IMDSv2 and one encrypted root disk")
    }
    reply = await this.#call("describeSubnets", { SubnetIds: [subnetId] }, deadline)
    const subnets = list<JsonObject>(reply, "Subnets")
    if (reply.NextToken || subnets.length !== 1 || subnets[0].SubnetId !== subnetId || subnets[0].AvailabilityZone !== zone
      || subnets[0].State !== "available" || subnets[0].MapPublicIpOnLaunch !== false || subnets[0].AssignIpv6AddressOnCreation !== false) {
      throw new ProviderError("EC2 staging subnet is not the exact private subnet in the requested zone")
    }
    reply = await this.#call("describeInstanceTypes", { InstanceTypes: [data.InstanceType] }, deadline)
    const types = list<JsonObject>(reply, "InstanceTypes")
    if (reply.NextToken || types.length !== 1 || types[0].InstanceType !== data.InstanceType
      || types[0].Hypervisor !== "nitro" || !list<string>(types[0], "SupportedRootDeviceTypes").includes("ebs")) {
      throw new ProviderError("EC2 staging requires the exact Nitro instance type with an EBS root")
    }
    return data
  }

  async stageWorker(options: StageEc2WorkerOptions): Promise<Ec2Instance> {
    const { claimId, subnetId, zone, authorize, deadline } = options
    const tags = stagingTags(this, options)
    await authorize()
    const result = await this.#call("runInstances", { ClientToken: claimId, MinCount: 1, MaxCount: 1,
      LaunchTemplate: { ...this.launchTemplate }, SubnetId: subnetId, Placement: { AvailabilityZone: zone },
      TagSpecifications: [{ ResourceType: "instance", Tags: toAwsTags(tags) }] }, deadline)
    const launched = list<Ec2Instance>(result, "Instances")
    if (launched.length !== 1) throw new ProviderError("EC2 staging launch did not return one instance")
    return stagedEc2Worker(this, await this.inspectWorker(launched[0].InstanceId, { deadline }), options)
  }

  async attachPool({ claimId, poolId, worker, volumeId, instanceId, device, deadline, authorize }:
    AttachEc2PoolOptions): Promise<Ec2Resource> {
    if (!matches(ec2SecondaryDevice, device)) throw new ProviderError("EBS attachment requires an explicit secondary slot")
    const record = ownedEc2(this, await this.inspectWorker(instanceId, { deadline }), ec2WorkerTags(worker))
    let volume = ownedEc2(this, await this.inspectVolume(volumeId, { deadline }), volumeTags(claimId, poolId, worker.worker_id as string))
    if (record.State?.Name !== "running") throw new ProviderError("EBS attachment requires the exact running worker")
    let attachment = ec2Attachment(this, volume, record, device)
    if (!attachment && volume.State === "available") {
      if (record.BlockDeviceMappings?.some(entry => entry.DeviceName === device)) {
        throw new ProviderError("EBS attachment slot is already in use")
      }
      await authorize?.()
      await this.#call("attachVolume", { VolumeId: volumeId, InstanceId: instanceId, Device: device }, deadline)
      volume = await this.inspectVolume(volumeId, { deadline })
      attachment = ec2Attachment(this, volume, record, device)
    }
    return { resource_id: volumeId, state: attachment?.State === "attached" ? "attached" : "attaching" }
  }

  async stopWorker({ claimId, instanceId, deadline }: Deadline & { claimId: string; instanceId: string }):
    Promise<Ec2Resource & { retained_volume_id: string }> {
    if (!this.lifecycleAuthority) throw new ProviderError("Authenticated Control is required before stopping an EC2 worker")
    const cutoff = deadline ?? monotonic() + 120
    const [record, zone] = await closedWorker(this.lifecycleAuthority, claimId, this.clusterId, "ec2", cutoff)
    const intent = requireRecord(record.intent)
    if (requireRecord(intent.native_target).instance_id !== instanceId) {
      throw new ProviderError("EC2 stop permit names a different instance")
    }
    const expected = requireRecord(requireRecord(requireRecord(requireRecord(record.phase).closed).mount).expected)
    const enrollment = requireRecord(intent.worker)
    const claim = identity(record.growth_claim)
    const workerTags = { ...ec2WorkerTags(enrollment), "nabedra:claim": claim }
    const volumeId = expected.provider_volume_id as string
    const tags = volumeTags(claim, identity(requireRecord(expected.identity).pool_id), identity(enrollment.worker_id))

    const state = async () => {
      const worker = ownedEc2(this, await this.inspectWorker(instanceId, { deadline: cutoff }), workerTags)
      const volume = ownedEc2(this, await this.inspectVolume(volumeId, { deadline: cutoff }), tags)
      const attachment = ec2Attachment(this, volume, worker)
      if (worker.Placement?.AvailabilityZone !== zone || attachment?.State !== "attached" || volume.State !== "in-use") {
        throw new ProviderError("EC2 worker lacks the exact retained same-zone database attachment")
      }
      const root = worker.RootDeviceName
      const data = (worker.BlockDeviceMappings ?? []).filter(entry => entry.DeviceName !== root)
      if (worker.RootDeviceType !== "ebs" || !root || attachment.Device === root || data.length !== 1
        || data[0].DeviceName !== attachment.Device || data[0].Ebs?.VolumeId !== volumeId
        || data[0].Ebs.DeleteOnTermination !== false) {
        throw new ProviderError("EC2 worker block device mappings differ from the stopped enrollment")
      }
      return worker.State?.Name
    }

    let observed = await state()
    if (observed === "running") {
      await this.#call("stopInstances", { InstanceIds: [instanceId], Force: false, SkipOsShutdown: false, Hibernate: false }, cutoff)
      observed = await state()
    }
    if (observed !== "stopping" && observed !== "stopped") {
      throw new ProviderError("EC2 worker stop is unresolved; reconcile the same drain")
    }
    return { resource_id: instanceId, state: observed, retained_volume_id: volumeId }
  }

  async detachPool({ claimId, volumeId, instanceId, deadline }: Deadline & {
    claimId: string; volumeId: string; instanceId: string
  }): Promise<Ec2Resource> {
    if (!this.lifecycleAuthority) {
      throw new ProviderError("Authenticated exact-owner Control endpoint is required before EBS detach")
    }
    const cutoff = deadline ?? monotonic() + 120
    const permit = await closedPool(this.lifecycleAuthority, claimId, this.clusterId, volumeId, cutoff)
    const mount = requireRecord(permit.mount)
    const expected = requireRecord(mount.expected)
    const enrollment = requireRecord(expected.worker)
    const worker = ownedEc2(this, await this.inspectWorker(instanceId, { deadline: cutoff }), ec2WorkerTags(enrollment))
    const tags = volumeTags(identity(permit.growth_claim), identity(requireRecord(expected.identity).pool_id), identity(enrollment.worker_id))
    let volume = ownedEc2(this, await this.inspectVolume(volumeId, { deadline: cutoff }), tags)
    let attachment = ec2Attachment(this, volume, worker)
    if (attachment?.State === "attached") {
      if (this.localHost?.instanceId !== instanceId || !await this.localHost.unmounted(instanceId, mount, { deadline: cutoff })) {
        return { resource_id: volumeId, state: "awaiting_unmount" }
      }
      const device = attachment.Device
      if (!matches(ec2SecondaryDevice, device)) {
        throw new ProviderError("Closed EBS volume is outside a configured secondary device slot")
      }
      await this.#call("detachVolume", { VolumeId: volumeId, InstanceId: instanceId, Device: device, Force: false }, cutoff)
      volume = ownedEc2(this, await this.inspectVolume(volumeId, { deadline: cutoff }), tags)
      attachment = ec2Attachment(this, volume, worker)
    }
    return { resource_id: volumeId, state: attachment === null && volume.State === "available" ? "detached" : "detaching" }
  }
}

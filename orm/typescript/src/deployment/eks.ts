import { AsyncLocalStorage } from "node:async_hooks"
import { isDeepStrictEqual } from "node:util"
import { Decimal } from "decimal.js"
import { KubernetesProvider, ProviderError, type KubernetesProviderOptions, type ProviderLaunch, type ProviderNode } from "../providers.js"
import type { JsonObject } from "../transport.js"
import { CloudLifecycleControl, closedWorker, closedWorkerMounts, ec2WorkerTags, type NativeTarget } from "./cloudControl.js"
import { EksReplacementVerifier, audience, reconcileReplacement, replacementMatches, serviceAccount, tokenDirectory,
  boundedEc2, type EksEc2Client, type ReplaceWorkerMountsOptions } from "./eksReplacement.js"
import { digest, identity, monotonic, requireRecord } from "./shared.js"

// Kubernetes wire records have different layouts for each resource kind.
export type KubeRecord = Record<string, any>
type Target = Extract<NativeTarget, { kind: "eks" }>
type Awaitable = void | Promise<void>

export const zoneLabel = "topology.kubernetes.io/zone"
const zoneKeys = new Set([zoneLabel, "topology.ebs.csi.aws.com/zone"])
const writer = ["ReadWriteOncePod"]
const driver = "ebs.csi.aws.com"
const retain = { whenDeleted: "Retain", whenScaled: "Retain" }
const resourceName = /^[a-z0-9][.a-z0-9-]{0,252}(?![\s\S])/u
const volumeId = /^vol-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/u
const prefix = "nabedra.io/"

const privateConfig = `import os,shutil,uuid
source='/secret';target='/private'
os.chown(target,1000,1000);os.chmod(target,0o700)
names=[name for name in os.listdir(source) if not name.startswith('.')]
assert 0<len(names)<=64,'bounded bootstrap Secret required'
for name in names:
    assert '/' not in name and name not in ('.','..')
    path=source+'/'+name
    assert os.path.isfile(path) and os.stat(path).st_size<=1048576,'oversized bootstrap item'
    temporary=target+'/.copy-'+str(uuid.uuid4())
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    os.fchown(fd,1000,1000)
    with open(path,'rb') as incoming,os.fdopen(fd,'wb') as outgoing:
        shutil.copyfileobj(incoming,outgoing,65536)
    os.replace(temporary,target+'/'+name)
`

export function resource(value: unknown): KubeRecord { return requireRecord(value) }

export function positiveInteger(value: unknown): value is number | bigint {
  return typeof value === "bigint" ? value > 0n : Number.isSafeInteger(value) && (value as number) > 0
}

function base32(bytes: Uint8Array): string {
  const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
  let shift = 0
  let value = 0
  let result = ""
  for (const byte of bytes) {
    value = (value << 8) | byte
    shift += 8
    while (shift >= 5) {
      shift -= 5
      result += alphabet[(value >>> shift) & 31]
    }
  }
  if (shift) result += alphabet[(value << (5 - shift)) & 31]
  return result
}

export function quantityBytes(value: unknown): bigint {
  const match = /^([0-9]+)(?:\.([0-9]+))?([KMGTPE]i?|m)?$/u.exec(String(value))
  if (match === null || match[0] !== String(value)) throw new ProviderError("Unsupported PVC storage quantity")
  const [, integer, decimal = "", suffix = ""] = match
  const whole = integer.replace(/^0+/u, "")
  const fraction = decimal.replace(/0+$/u, "")
  if (whole.length > 22 || fraction.length > 60) throw new ProviderError("PVC storage quantity is outside its byte range")
  const magnitude = suffix && suffix !== "m" ? "KMGTPE".indexOf(suffix[0]) + 1 : 0
  const base = suffix.endsWith("i") ? 1024n : 1000n
  const amount = BigInt(whole + fraction) * base ** BigInt(magnitude)
  const denominator = 10n ** BigInt(fraction.length + (suffix === "m" ? 3 : 0))
  if (amount % denominator || amount < denominator || amount / denominator > (1n << 63n) - 1n) {
    throw new ProviderError("PVC storage quantity is outside its byte range")
  }
  return amount / denominator
}

function millicores(value: string): Decimal {
  return value.endsWith("m") ? new Decimal(value.slice(0, -1)) : new Decimal(value).mul(1000)
}

function matches(actual: unknown, expected: unknown): boolean {
  if (Array.isArray(expected)) {
    return Array.isArray(actual) && actual.length === expected.length && expected.every((value, i) => matches(actual[i], value))
  }
  if (expected !== null && typeof expected === "object") {
    if (actual === null || typeof actual !== "object" || Array.isArray(actual)) return false
    const left = resource(actual)
    const right = resource(expected)
    if (Object.keys(right).length === 2 && Object.hasOwn(right, "cpu") && Object.hasOwn(right, "memory")) {
      try {
        return Object.keys(left).length === 2 && millicores(left.cpu).eq(millicores(right.cpu))
          && quantityBytes(left.memory) === quantityBytes(right.memory)
      } catch { return false }
    }
    return Object.entries(right).every(([key, value]) => matches(left[key], value))
  }
  return actual === expected
}

function stagingSpec(actual: KubeRecord, expected: KubeRecord): boolean {
  if (!matches(actual, expected)
      || ["ephemeralContainers", "hostNetwork", "hostPID", "hostIPC", "hostAliases", "shareProcessNamespace"]
        .some(key => present(actual[key]))) return false
  for (const kind of ["containers", "initContainers"]) {
    const containers: KubeRecord[] = actual[kind]
    if (containers.some((container, index) => {
      const template = expected[kind][index]
      return ["args", "env", "envFrom", "volumeDevices", "lifecycle", "workingDir"].some(key => present(container[key]))
        || !isDeepStrictEqual(container.securityContext, template.securityContext)
        || (container.volumeMounts as KubeRecord[]).some((mount, j) =>
          mount.subPath || mount.subPathExpr || (mount.mountPropagation != null && mount.mountPropagation !== "None")
          || Boolean(mount.readOnly) !== Boolean(template.volumeMounts[j].readOnly))
    })) return false
  }
  return isDeepStrictEqual(actual.securityContext, expected.securityContext)
    && (actual.serviceAccountName ?? "default") === "default"
    && (actual.volumes as KubeRecord[]).every((volume, index) =>
      !volume.secret?.optional && !present(volume.secret?.items) && !volume.persistentVolumeClaim?.readOnly
        && sameKeys(volume, expected.volumes[index]))
}

export function present(value: unknown): boolean {
  if (Array.isArray(value)) return value.length > 0
  if (value !== null && typeof value === "object") return Object.keys(value).length > 0
  return Boolean(value)
}

function sameKeys(left: KubeRecord, right: KubeRecord): boolean {
  return Object.keys(left).length === Object.keys(right).length && Object.keys(right).every(key => Object.hasOwn(left, key))
}

function enrollmentLabels(worker: JsonObject): Record<string, string> {
  const tags = ec2WorkerTags(worker)
  return { [prefix + "worker"]: tags["nabedra:worker"], [prefix + "incarnation"]: tags["nabedra:incarnation"],
    [prefix + "enrollment-revision"]: tags["nabedra:enrollment-revision"] }
}

interface KubeRequestOptions { missing?: boolean; conflict?: boolean; absolute?: boolean; deadline?: number }
export interface EksProviderOptions extends KubernetesProviderOptions {
  storageClass: string
  lifecycleAuthority?: CloudLifecycleControl | null
  replacementVerifier?: EksReplacementVerifier | null
  stagingEc2Client?: EksEc2Client | null
}

interface PoolOptions {
  claimId: string
  poolId: string
  workerId: string
  zone: string
  bytes: number | bigint
}

interface StageOptions { staging?: string | null; authorize?: () => Awaitable; existingOnly?: boolean }
interface WorkerOptions {
  claimId: string
  workerId: string
  incarnation: string
  zone: string
  bootstrapSecret: string
  pools: readonly { resource_id: string; mount_path: string; filesystem_uuid: string }[]
  cpuMillicores: number | bigint
  memoryBytes: number | bigint
  enrollment?: JsonObject | null
}

export interface EksPoolResult extends JsonObject { resource_id: string; pvc_name: string; state: string }
export interface EksWorkerResult extends JsonObject { resource_id: string; state: "awaiting_database_ready" }
type RetainedStorage = readonly [mount: KubeRecord, pvcName: string]

interface Internals {
  origin: string
  image: string
  stagingRegion: string | null
  limit<T>(deadline: number, work: () => Promise<T>): Promise<T>
  request(method: string, path: string, body?: JsonObject, options?: KubeRequestOptions): Promise<KubeRecord | null>
  storageClass(): Promise<void>
  parts(value: unknown): [namespace: string, name: string, uid: string]
  pvc(value: unknown, options: { workerId: string; zone: string; poolId?: string }): Promise<[string, KubeRecord]>
  boundVolume(pvc: KubeRecord, options: { zone: string; expected?: string | null }): Promise<KubeRecord>
  ensurePool(options: PoolOptions & StageOptions): Promise<EksPoolResult>
  ensureWorker(options: WorkerOptions & StageOptions & { replacement?: string | null }): Promise<EksWorkerResult & KubeRecord>
  stagedEbs(volumeId: string, zone: string, sizeGib: number): Promise<KubeRecord>
  stagedPod(staged: KubeRecord, options: { zone: string; pvc: KubeRecord; pv: KubeRecord; sizeGib: number; expected?: Target | null; ready?: boolean }): Promise<Target | null>
  inspectLiveWorker(options: { resourceId: string; podUid: string; worker: JsonObject }): Promise<[KubeRecord, KubeRecord]>
  retainedInventory(): Promise<[Map<string, KubeRecord>, Map<string, KubeRecord>]>
}
const internals = new WeakMap<EksProvider, Internals>()

export function eksInternals(provider: EksProvider): Internals {
  const value = internals.get(provider)
  if (!value) throw new ProviderError("EKS provider is unavailable")
  return value
}

export class EksProvider extends KubernetesProvider {
  readonly storageClass: string
  readonly lifecycleAuthority: CloudLifecycleControl | null
  readonly replacementVerifier: EksReplacementVerifier | null
  readonly #apps: string
  readonly #image: string
  readonly #timeout: number
  readonly #calls = new AsyncLocalStorage<number>()
  readonly #staging: { client: EksEc2Client; region: string; window: number } | null

  constructor({ storageClass, lifecycleAuthority = null, replacementVerifier = null, stagingEc2Client = null,
    ...config }: EksProviderOptions) {
    super(config)
    if (typeof storageClass !== "string" || !storageClass || storageClass.length > 253) {
      throw new ProviderError("Explicit retained expandable EBS CSI StorageClass is required")
    }
    if (lifecycleAuthority !== null && Object.getPrototypeOf(lifecycleAuthority) !== CloudLifecycleControl.prototype) {
      throw new ProviderError("EKS worker lifecycle requires authenticated Control")
    }
    if (replacementVerifier !== null && (lifecycleAuthority === null
        || !replacementMatches(replacementVerifier, config.apiUrl, this.clusterId, this.namespace))) {
      throw new ProviderError("EKS replacement verifier must use the same explicit Kubernetes deployment")
    }
    this.storageClass = storageClass
    this.lifecycleAuthority = lifecycleAuthority
    this.replacementVerifier = replacementVerifier
    this.#apps = "/apis/apps/v1/namespaces/" + encodeURIComponent(this.namespace)
    this.#image = config.image
    this.#timeout = config.timeout ?? 10
    this.#staging = stagingEc2Client === null ? null : boundedEc2(stagingEc2Client,
      "EKS staging requires a region-pinned EC2 client with a single bounded attempt")
    internals.set(this, {
      origin: config.apiUrl.replace(/\/$/u, ""), image: this.#image, stagingRegion: this.#staging?.region ?? null,
      limit: (deadline, work) => this.#limit(deadline, work),
      request: (method, path, body, options) => this.#kube(method, path, body, options ?? {}),
      storageClass: () => this.#storageClass(),
      parts: value => this.#parts(value),
      pvc: (value, options) => this.#pvc(value, options),
      boundVolume: (pvc, options) => this.#boundVolume(pvc, options),
      ensurePool: options => this.#ensurePool(options),
      ensureWorker: options => this.#ensureWorker(options),
      stagedEbs: (volumeId, zone, sizeGib) => this.#stagedEbs(volumeId, zone, sizeGib),
      stagedPod: (staged, options) => this.#stagedPod(staged, options),
      inspectLiveWorker: options => this.#limited(() => this.#inspectLiveWorker(options)),
      retainedInventory: () => this.#limited(() => this.#retainedInventory()),
    })
  }

  #limit<T>(deadline: number, work: () => Promise<T>): Promise<T> {
    return this.#calls.run(Math.min(this.#calls.getStore() ?? Infinity, deadline), work)
  }

  #limited<T>(work: () => Promise<T>): Promise<T> {
    return this.#limit(monotonic() + this.#timeout, work)
  }

  #deadline(): number {
    return this.#calls.getStore() ?? monotonic() + this.#timeout
  }

  #kube(method: string, path: string, body?: JsonObject,
    options?: KubeRequestOptions & { missing?: false; conflict?: false }): Promise<KubeRecord>
  #kube(method: string, path: string, body: JsonObject | undefined, options: KubeRequestOptions): Promise<KubeRecord | null>
  async #kube(method: string, path: string, body?: JsonObject, options: KubeRequestOptions = {}): Promise<KubeRecord | null> {
    return this.request(method, path, body, {
      ...options, deadline: Math.min(this.#deadline(), options.deadline ?? Infinity), absolute: options.absolute ?? path.startsWith("/apis/")
    })
  }

  #labels(claimId: string, content: JsonObject): Record<string, string> {
    return { [prefix + "cluster"]: this.clusterId, [prefix + "claim"]: identity(claimId),
      [prefix + "content"]: base32(Buffer.from(digest(content), "hex")) }
  }

  async #storageClass(): Promise<void> {
    const storage = await this.#kube("GET", "/apis/storage.k8s.io/v1/storageclasses/" + encodeURIComponent(this.storageClass))
    if (storage.provisioner !== driver || storage.reclaimPolicy !== "Retain"
        || storage.volumeBindingMode !== "WaitForFirstConsumer" || storage.allowVolumeExpansion !== true
        || storage.parameters?.encrypted !== "true") {
      throw new ProviderError("EKS storage requires retained expandable standard EBS CSI volumes and topology binding")
    }
  }

  #owned(record: KubeRecord, labels: Readonly<Record<string, string>>): KubeRecord {
    const meta = resource(record.metadata)
    if (meta.namespace !== this.namespace || !Object.entries(labels).every(([key, value]) => meta.labels?.[key] === value)) {
      throw new ProviderError("EKS stateful ownership or immutable claim differs")
    }
    if (typeof meta.uid !== "string" || !meta.uid || typeof meta.resourceVersion !== "string" || !meta.resourceVersion) {
      throw new ProviderError("EKS stateful resource has no exact identity")
    }
    return meta
  }

  ensurePool(options: PoolOptions & StageOptions): Promise<EksPoolResult> {
    return this.#limited(() => this.#ensurePool(options))
  }

  async #ensurePool({ claimId, poolId, workerId, zone, bytes, staging = null, authorize, existingOnly = false }:
    PoolOptions & StageOptions): Promise<EksPoolResult> {
    if (!positiveInteger(bytes) || typeof zone !== "string" || !zone) {
      throw new ProviderError("Explicit positive EKS volume size and zone are required")
    }
    await this.#storageClass()
    const name = "nabedra-pool-" + identity(poolId)
    const labels = { ...this.#labels(claimId, { pool_id: poolId, worker_id: identity(workerId),
      zone, bytes, storage_class: this.storageClass, ...(staging === null ? {} : { staging }) }),
    [prefix + "worker"]: workerId, [prefix + "pool"]: poolId }
    const path = "/persistentvolumeclaims/" + name
    let record = await this.#kube("GET", path, undefined, { missing: true })
    if (record === null) {
      if (existingOnly) throw new ProviderError("The retained EKS staging PVC disappeared")
      await authorize?.()
      await this.#kube("POST", "/persistentvolumeclaims", {
        apiVersion: "v1", kind: "PersistentVolumeClaim",
        metadata: { name, namespace: this.namespace, labels, annotations: { [prefix + "retain"]: "true", [prefix + "zone"]: zone } },
        spec: { accessModes: writer, volumeMode: "Filesystem", storageClassName: this.storageClass,
          resources: { requests: { storage: String(bytes) } } },
      }, { conflict: true })
      record = await this.#kube("GET", path)
    }
    const meta = this.#owned(record, labels)
    this.#retainedPvc(record, { name, workerId, zone, poolId })
    if (staging !== null && quantityBytes(record.spec.resources.requests.storage) !== BigInt(bytes)) {
      throw new ProviderError("EKS staging PVC size differs from the durable claim")
    }
    return { resource_id: [this.namespace, name, meta.uid].join("/"), pvc_name: name, state: record.status?.phase ?? "Pending" }
  }

  expandPool({ resourceId, targetBytes, worker, poolId, zone, volumeId }: {
    resourceId: string; targetBytes: number | bigint; worker: JsonObject; poolId: string; zone: string; volumeId: string
  }): Promise<{ resource_id: string; state: "awaiting_filesystem" }> {
    return this.#limited(async () => {
      if (!positiveInteger(targetBytes)) throw new ProviderError("Positive EKS target bytes are required")
      await this.#storageClass()
      const [path, record] = await this.#pvc(resourceId, { workerId: identity(worker.worker_id), zone, poolId })
      await this.#boundVolume(record, { zone, expected: volumeId })
      if (BigInt(targetBytes) > quantityBytes(record.spec.resources.requests.storage)) {
        record.spec.resources.requests.storage = String(targetBytes)
        await this.#kube("PUT", path, record)
      }
      return { resource_id: volumeId, state: "awaiting_filesystem" }
    })
  }

  #retainedPvc(record: KubeRecord, { name, workerId, zone, poolId }: {
    name: string; workerId?: string; zone?: string; poolId?: string
  }): void {
    const labels = { [prefix + "cluster"]: this.clusterId,
      ...(workerId === undefined ? {} : { [prefix + "worker"]: identity(workerId) }),
      ...(poolId === undefined ? {} : { [prefix + "pool"]: identity(poolId) }) }
    const meta = this.#owned(record, labels)
    const spec = resource(record.spec)
    if (meta.name !== name || meta.deletionTimestamp || present(meta.ownerReferences)
      || meta.annotations?.[prefix + "retain"] !== "true" || (zone !== undefined && meta.annotations?.[prefix + "zone"] !== zone)
      || spec.storageClassName !== this.storageClass || !isDeepStrictEqual(spec.accessModes, writer)
      || (spec.volumeMode ?? "Filesystem") !== "Filesystem") {
      throw new ProviderError("EKS PVC does not satisfy retained single-Pod storage ownership")
    }
  }

  #parts(value: unknown): [string, string, string] {
    const fields = typeof value === "string" ? value.split("/") : []
    const [namespace, name, uid] = fields
    if (fields.length !== 3) throw new ProviderError("Exact namespace/name/UID is required")
    if (namespace !== this.namespace || !/^nabedra-[a-z0-9][-a-z0-9]{0,240}(?![\s\S])/u.test(name) || !uid) {
      throw new ProviderError("EKS stateful resource identity differs")
    }
    return [namespace, name, uid]
  }

  #exact(record: KubeRecord, uid: string): void {
    const meta = this.#owned(record, { [prefix + "cluster"]: this.clusterId })
    if (meta.uid !== uid || meta.deletionTimestamp) throw new ProviderError("EKS stateful UID or cluster changed")
  }

  async #pvc(resourceId: unknown, { workerId, zone, poolId }: {
    workerId: string; zone: string; poolId?: string
  }): Promise<[string, KubeRecord]> {
    const [, name, uid] = this.#parts(resourceId)
    if (!name.startsWith("nabedra-pool-") || (poolId !== undefined && name !== "nabedra-pool-" + poolId)) {
      throw new ProviderError("EKS PVC name differs from the registered pool")
    }
    const path = "/persistentvolumeclaims/" + name
    const record = await this.#kube("GET", path)
    this.#exact(record, uid)
    this.#retainedPvc(record, { name, workerId, zone, poolId })
    return [path, record]
  }

  async #boundVolume(pvc: KubeRecord, { zone, expected }: { zone: string; expected?: string | null }): Promise<KubeRecord> {
    const name: unknown = pvc.spec?.volumeName
    if (pvc.status?.phase !== "Bound" || typeof name !== "string" || !resourceName.test(name)) {
      throw new ProviderError("Exact bound EKS PVC is required")
    }
    const volume = await this.#kube("GET", "/api/v1/persistentvolumes/" + name, undefined, { absolute: true })
    const meta = resource(volume.metadata)
    const spec = resource(volume.spec)
    const claim = resource(pvc.metadata)
    const id: unknown = expected ?? spec.csi?.volumeHandle
    const zones: unknown = spec.nodeAffinity?.required?.nodeSelectorTerms
    const zoneBound = Array.isArray(zones) && zones.length !== 0 && zones.every(term =>
      Array.isArray(term?.matchExpressions) && term.matchExpressions.some((expr: KubeRecord) =>
        zoneKeys.has(expr.key) && expr.operator === "In" && isDeepStrictEqual(expr.values, [zone])))
    if (meta.name !== name || !meta.uid || meta.deletionTimestamp || !["namespace", "name", "uid"].every(key => spec.claimRef?.[key] === claim[key])
      || volume.status?.phase !== "Bound" || spec.storageClassName !== this.storageClass || !isDeepStrictEqual(spec.accessModes, writer)
      || (spec.volumeMode ?? "Filesystem") !== "Filesystem" || spec.persistentVolumeReclaimPolicy !== "Retain" || !zoneBound
      || typeof id !== "string" || !volumeId.test(id) || spec.csi?.driver !== driver || spec.csi?.volumeHandle !== id
      || spec.csi?.readOnly === true) {
      throw new ProviderError("EKS PV no longer binds the exact retained same-zone EBS volume")
    }
    return volume
  }

  ensureWorker(options: WorkerOptions): Promise<EksWorkerResult> {
    return this.#limited(() => this.#ensureWorker(options))
  }

  async #ensureWorker({ claimId, workerId, incarnation, zone, bootstrapSecret, pools, cpuMillicores,
    memoryBytes, enrollment, replacement = null, staging = null, authorize, existingOnly = false }:
    WorkerOptions & StageOptions & { replacement?: string | null }): Promise<EksWorkerResult & KubeRecord> {
    identity(workerId)
    identity(incarnation)
    if (!Array.isArray(pools) || pools.length < 1 || pools.length > 64
      || !positiveInteger(cpuMillicores) || !positiveInteger(memoryBytes) || typeof bootstrapSecret !== "string"
      || !resourceName.test(bootstrapSecret) || typeof zone !== "string" || !zone) {
      throw new ProviderError("Bounded worker pools, resources and bootstrap Secret are required")
    }
    const name = "nabedra-worker-" + workerId
    const labels = { ...this.#labels(claimId, {
      worker_id: workerId, incarnation, zone, bootstrap_secret: bootstrapSecret,
      pools, cpu: cpuMillicores, memory: memoryBytes, image: this.#image,
      ...(replacement === null ? {} : { replacement: identity(replacement) }),
      ...(staging === null ? {} : { staging }),
    }), [prefix + "worker"]: workerId, [prefix + "incarnation"]: incarnation }
    const annotations: Record<string, string> = {}
    if (enrollment != null) {
      const enrolled = enrollmentLabels(enrollment)
      if (enrolled[prefix + "worker"] !== workerId || enrolled[prefix + "incarnation"] !== incarnation) {
        throw new ProviderError("EKS bootstrap and enrollment worker identities differ")
      }
      Object.assign(labels, enrolled)
      annotations[prefix + "certificate"] = String(enrollment.certificate_sha256)
    }
    const volumes: JsonObject[] = [
      { name: "bootstrap-source", secret: { secretName: bootstrapSecret, defaultMode: 256 } },
      { name: "bootstrap", emptyDir: { medium: "Memory", sizeLimit: "16Mi" } },
    ]
    const mounts: JsonObject[] = [{ name: "bootstrap", mountPath: "/nabedra", readOnly: true }]
    if (replacement !== null) {
      annotations[prefix + "replacement"] = replacement
      volumes.push({ name: "replacement-identity", projected: { defaultMode: 0o440,
        sources: [{ serviceAccountToken: { audience: audience(this.clusterId), expirationSeconds: 600, path: "token" } }] } })
      mounts.push({ name: "replacement-identity", mountPath: tokenDirectory, readOnly: true })
    }
    const paths = new Set<string>()
    const claims = new Set<string>()
    const uuids = new Set<string>()
    for (const [index, pool] of pools.entries()) {
      let pvc: string, path: string, uuid: string
      try {
        if (!sameKeys(resource(pool), { resource_id: 0, mount_path: 0, filesystem_uuid: 0 })) throw new Error()
        pvc = this.#parts(pool.resource_id)[1]
        path = pool.mount_path
        uuid = identity(pool.filesystem_uuid)
        if (claims.has(pvc) || uuids.has(uuid) || !path.startsWith("/") || paths.has(path)
            || path.slice(1).split("/").some(part => ["", ".", ".."].includes(part))
            || ["dev", "proc", "sys", "nabedra"].includes(path.split("/")[1])) throw new Error()
      } catch {
        throw new ProviderError("EKS requires distinct retained PVC UIDs, filesystem UUIDs and mount paths")
      }
      await this.#pvc(pool.resource_id, { workerId, zone })
      paths.add(path)
      claims.add(pvc)
      uuids.add(uuid)
      const storageName = "pool-" + index
      const deviceName = "pool-device-" + index
      const devicePath = "/dev/disk/by-uuid/" + uuid
      volumes.push({ name: storageName, persistentVolumeClaim: { claimName: pvc } },
        { name: deviceName, hostPath: { path: devicePath, type: "BlockDevice" } })
      mounts.push({ name: storageName, mountPath: path }, { name: deviceName, mountPath: devicePath, readOnly: true })
    }
    const path = this.#apps + "/statefulsets/" + name
    let record = await this.#kube("GET", path, undefined, { missing: true })
    if (record === null && existingOnly) throw new ProviderError("The retained EKS staging StatefulSet disappeared")
    const selector = { [prefix + "worker"]: workerId }
    if (record === null || staging !== null) {
      const servicePath = "/services/" + name
      const serviceSpec = { clusterIP: "None", publishNotReadyAddresses: true,
        selector, ports: [{ name: "peer", port: 9000, targetPort: 9000 }] }
      let service = await this.#kube("GET", servicePath, undefined, { missing: true })
      if (service === null) {
        if (record !== null) throw new ProviderError("The retained EKS staging peer Service disappeared")
        await authorize?.()
        await this.#kube("POST", "/services", {
          apiVersion: "v1", kind: "Service", metadata: { name, namespace: this.namespace, labels }, spec: serviceSpec,
        }, { conflict: true })
        service = await this.#kube("GET", servicePath)
      }
      if (replacement === null) {
        const meta = this.#owned(service, labels)
        if (staging !== null && (meta.name !== name || meta.deletionTimestamp || present(meta.ownerReferences)
          || !matches(service.spec, serviceSpec) || !isDeepStrictEqual(service.spec.selector, selector))) {
          throw new ProviderError("EKS staging requires the exact independent peer Service")
        }
      } else {
        const meta = this.#owned(service, { [prefix + "cluster"]: this.clusterId,
          [prefix + "claim"]: claimId, [prefix + "worker"]: workerId })
        if (meta.name !== name || meta.deletionTimestamp || present(meta.ownerReferences)
          || service.spec?.clusterIP !== "None" || service.spec?.publishNotReadyAddresses !== true
          || !isDeepStrictEqual(service.spec?.selector, selector)) {
          throw new ProviderError("EKS replacement requires the exact retained peer Service")
        }
      }
    }
    const limit = { cpu: String(cpuMillicores) + "m", memory: String(memoryBytes) }
    const podSpec = {
      terminationGracePeriodSeconds: 120, automountServiceAccountToken: false,
      securityContext: { runAsUser: 1000, runAsGroup: 1000, fsGroup: 1000 },
      initContainers: [{
        name: "private-bootstrap", image: this.#image, command: ["python3", "-c", privateConfig],
        securityContext: { runAsUser: 0, runAsGroup: 0 },
        resources: { requests: { cpu: "100m", memory: "32Mi" }, limits: { cpu: "250m", memory: "64Mi" } },
        volumeMounts: [{ name: "bootstrap-source", mountPath: "/secret", readOnly: true }, { name: "bootstrap", mountPath: "/private" }],
      }],
      nodeSelector: { [zoneLabel]: zone },
      affinity: { podAntiAffinity: { requiredDuringSchedulingIgnoredDuringExecution: [{
        labelSelector: { matchLabels: { [prefix + "cluster"]: this.clusterId } }, topologyKey: "kubernetes.io/hostname",
      }] } },
      volumes,
      containers: [{
        name: "worker", image: this.#image,
        command: ["nabedra-db", staging === null ? "worker" : "data-worker", "--bootstrap", "/nabedra/bootstrap.json"],
        securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } },
        volumeMounts: mounts, resources: { requests: limit, limits: limit },
      }],
      ...(replacement === null ? {} : { serviceAccountName: serviceAccount }),
    }
    const spec = { replicas: 1, serviceName: name, updateStrategy: { type: "OnDelete" as const },
      persistentVolumeClaimRetentionPolicy: retain, selector: { matchLabels: selector },
      template: { metadata: { labels, annotations }, spec: podSpec } }
    if (record === null) {
      await authorize?.()
      await this.#kube("POST", this.#apps + "/statefulsets", {
        apiVersion: "apps/v1", kind: "StatefulSet", metadata: { name, namespace: this.namespace, labels, annotations }, spec,
      }, { conflict: true })
      record = await this.#kube("GET", path)
    }
    const meta = this.#owned(record, labels)
    this.#workerSpec(record, name, labels, annotations)
    const result: EksWorkerResult = {
      resource_id: [this.namespace, name, meta.uid].join("/"), state: "awaiting_database_ready",
    }
    if (staging === null) return result
    if (present(meta.ownerReferences) || (record.spec.ordinals?.start ?? 0) !== 0
      || !isDeepStrictEqual(record.spec.selector, spec.selector) || !matches(record.spec, spec)
      || !stagingSpec(record.spec.template.spec, podSpec)) {
      throw new ProviderError("EKS staged StatefulSet differs from the exact worker template")
    }
    return { ...result, stateful: record, labels, annotations, pod_spec: podSpec }
  }

  #workerSpec(record: KubeRecord, name: string, labels: Record<string, string>, annotations: Record<string, unknown>,
    deleting = false): void {
    const meta = resource(record.metadata)
    const spec = resource(record.spec)
    const template = spec.template?.metadata
    if (meta.name !== name || (meta.deletionTimestamp && !deleting) || spec.updateStrategy?.type !== "OnDelete"
      || spec.replicas !== 1 || !isDeepStrictEqual(spec.persistentVolumeClaimRetentionPolicy, retain)
      || present(spec.volumeClaimTemplates) || !matches(template?.labels, labels)
      || !Object.entries(annotations).every(([key, value]) => meta.annotations?.[key] === value
        && template?.annotations?.[key] === value)) {
      throw new ProviderError("EKS worker identity or fixed retained StatefulSet configuration changed")
    }
  }

  async #stagedEbs(volumeId: string, zone: string, sizeGib: number): Promise<KubeRecord> {
    const deadline = this.#deadline()
    const aws = this.#staging
    if (aws === null || !zone.startsWith(aws.region) || !/^[a-z](?![\s\S])/u.test(zone.slice(aws.region.length))
      || deadline - monotonic() < aws.window) {
      throw new ProviderError("EKS staging requires live EBS observation in the configured region")
    }
    try {
      const reply = resource(await aws.client.describeVolumes({ VolumeIds: [volumeId] }))
      const volume = reply.Volumes?.[0]
      if (!Array.isArray(reply.Volumes) || reply.Volumes.length !== 1 || reply.NextToken
        || volume.VolumeId !== volumeId || volume.AvailabilityZone !== zone || volume.Encrypted !== true
        || (volume.MultiAttachEnabled ?? false) !== false || volume.Size !== sizeGib
        || !["available", "in-use"].includes(volume.State) || !Array.isArray(volume.Attachments)
        || volume.Attachments.some((entry: unknown) => !isRecord(entry)) || monotonic() >= deadline) throw new Error()
      return volume
    } catch {
      throw new ProviderError("EKS staging could not observe the exact encrypted EBS volume and size")
    }
  }

  async #stagedPod(staged: KubeRecord, { zone, pvc, pv, sizeGib, expected = null, ready = false }:
    { zone: string; pvc: KubeRecord; pv: KubeRecord; sizeGib: number; expected?: Target | null; ready?: boolean }): Promise<Target | null> {
    const owner = resource(staged.stateful?.metadata)
    const name = owner.name as string
    const uid = identity(owner.uid)
    const podPath = "/pods/" + name + "-0"
    const pod = await this.#kube("GET", podPath, undefined, { missing: true })
    if (pod === null) return null
    const meta = this.#owned(pod, staged.labels)
    const podUid = identity(meta.uid)
    const reference = { apiVersion: "apps/v1", kind: "StatefulSet", name, uid, controller: true }
    const spec = resource(pod.spec)
    if (meta.name !== name + "-0" || meta.deletionTimestamp || !matches(meta.ownerReferences, [reference])
      || !matches(meta.annotations, staged.annotations) || !stagingSpec(spec, staged.pod_spec)) {
      throw new ProviderError("EKS candidate Pod differs from the exact staged worker")
    }
    const observed: Target = { kind: "eks", namespace: this.namespace, statefulset_uid: uid, pod_uid: podUid,
      pvc_uid: identity(pvc.metadata?.uid), pv_uid: identity(pv.metadata?.uid) }
    if (new Set([observed.statefulset_uid, observed.pod_uid, observed.pvc_uid, observed.pv_uid]).size !== 4
      || (expected !== null && !isDeepStrictEqual(observed, expected))) {
      throw new ProviderError("EKS candidate Pod or storage UID changed")
    }
    const phase = pod.status?.phase
    if (!["Pending", "Running"].includes(phase) || (ready && phase !== "Running")) {
      if (phase == null || phase === "Pending") return null
      throw new ProviderError("EKS candidate Pod is unavailable")
    }
    const nodeName = spec.nodeName
    if (!nodeName) return null
    if (typeof nodeName !== "string" || !resourceName.test(nodeName)) {
      throw new ProviderError("EKS candidate Pod has an invalid Node name")
    }
    const nodePath = "/api/v1/nodes/" + encodeURIComponent(nodeName)
    const node = await this.#kube("GET", nodePath, undefined, { absolute: true })
    const nodeMeta = resource(node.metadata)
    const instance = exactInstance(node.spec?.providerID, zone)
    if (nodeMeta.name !== nodeName || nodeMeta.deletionTimestamp || nodeMeta.labels?.[zoneLabel] !== zone || !instance) {
      throw new ProviderError("EKS candidate is not on the exact same-zone EC2 Node")
    }
    identity(nodeMeta.uid)
    const volumeId = pv.spec?.csi?.volumeHandle
    const volume = await this.#stagedEbs(volumeId, zone, sizeGib)
    const attachments = volume.Attachments
    if (attachments.length === 0) return null
    if (attachments.length !== 1 || attachments[0].InstanceId !== instance || attachments[0].VolumeId !== volumeId) {
      throw new ProviderError("EKS candidate EBS volume is attached outside the exact Node")
    }
    if (volume.State !== "in-use" || attachments[0].State !== "attached") return null
    if (attachments[0].DeleteOnTermination !== false) {
      throw new ProviderError("EKS candidate EBS volume is not retained on Node termination")
    }
    const confirmations = [[podPath, meta], [this.#apps + "/statefulsets/" + name, owner], [nodePath, nodeMeta]] as const
    for (const [path, previous] of confirmations) {
      const final = await this.#kube("GET", path, undefined, { missing: true, absolute: path.startsWith("/api") })
      const current = final?.metadata
      if (!previous.resourceVersion || current?.uid !== previous.uid || current?.resourceVersion !== previous.resourceVersion || current?.deletionTimestamp) {
        throw new ProviderError("EKS Pod, StatefulSet or Node changed during EBS observation")
      }
    }
    return observed
  }

  stopWorker(options: { claimId: string; resourceId: string; podUid: string }): Promise<{
    resource_id: string; pod_uid: string; state: "stopping" | "stopped"; retained_volume_id: string; retained_volume_ids?: string[]
  }> {
    return this.#limited(async () => {
      const { claimId, resourceId, podUid } = options
      if (this.lifecycleAuthority === null) throw new ProviderError("Authenticated Control is required before stopping an EKS worker")
      const [raw, zone] = await closedWorker(this.lifecycleAuthority, claimId, this.clusterId, "eks", this.#deadline())
      const record = resource(raw)
      const worker = resource(record.intent?.worker)
      const target: Target = record.intent.native_target
      const [namespace, name, uid] = this.#parts(resourceId)
      if (namespace !== target.namespace || uid !== target.statefulset_uid
        || name !== "nabedra-worker-" + worker.worker_id || podUid !== target.pod_uid) {
        throw new ProviderError("EKS stop permit names a different StatefulSet or Pod")
      }
      const pools = closedWorkerMounts(record, this.clusterId, target)
      const storage: RetainedStorage[] = pools.map(([mount]) => [mount, "nabedra-pool-" + resource(mount.identity).pool_id])
      const labels = { ...enrollmentLabels(worker), [prefix + "cluster"]: this.clusterId,
        [prefix + "claim"]: identity(record.growth_claim) }
      const annotations = { [prefix + "certificate"]: worker.certificate_sha256 }
      const owner = { apiVersion: "apps/v1", kind: "StatefulSet", name, uid, controller: true }
      const statefulPath = this.#apps + "/statefulsets/" + name
      const podName = name + "-0"
      const podPath = "/pods/" + podName
      await this.#storageClass()

      const checkRetained = async () => {
        for (const [i, [expected, claim, pvcUid, pvUid]] of pools.entries()) {
          const [, pvcName] = storage[i]
          const [, pvc] = await this.#pvc([namespace, pvcName, pvcUid].join("/"), {
            workerId: worker.worker_id, zone, poolId: resource(expected.identity).pool_id,
          })
          const pv = await this.#boundVolume(pvc, { zone, expected: String(expected.provider_volume_id) })
          if (pvc.metadata.labels[prefix + "claim"] !== claim || pv.metadata.uid !== pvUid) {
            throw new ProviderError("EKS stop permit differs from the exact retained PVC or PV")
          }
        }
      }

      await checkRetained()
      const stateful = await this.#kube("GET", statefulPath, undefined, { missing: true })
      const pod = await this.#kube("GET", podPath, undefined, { missing: true })
      if (stateful !== null) {
        const meta = this.#owned(stateful, labels)
        this.#workerSpec(stateful, name, labels, annotations, true)
        if (meta.uid !== uid || present(meta.ownerReferences) || (stateful.spec.ordinals?.start ?? 0) !== 0
          || !isDeepStrictEqual(stateful.spec.selector, { matchLabels: { [prefix + "worker"]: worker.worker_id } })) {
          throw new ProviderError("EKS stop requires the exact independent StatefulSet")
        }
        checkStorageSet(resource(stateful.spec.template.spec), storage, zone)
      }
      let owners: KubeRecord[] = []
      if (pod !== null) {
        const meta = this.#owned(pod, labels)
        if (meta.ownerReferences != null && !Array.isArray(meta.ownerReferences)) {
          throw new ProviderError("EKS stop requires the exact enrolled StatefulSet Pod")
        }
        owners = meta.ownerReferences ?? []
        if (meta.uid !== podUid || meta.name !== podName || meta.annotations?.[prefix + "certificate"] !== worker.certificate_sha256
          || (stateful !== null && owners.length === 0 && !stateful.metadata.deletionTimestamp)
          || (owners.length !== 0 && !matches(owners, [owner]))
          || (pod.status?.phase === "Running" && !pod.spec?.nodeName)) {
          throw new ProviderError("EKS stop requires the exact enrolled StatefulSet Pod")
        }
        checkStorageSet(resource(pod.spec), storage, zone)
      }
      let state: "stopping" | "stopped" = "stopping"
      if (stateful !== null) {
        if (!stateful.metadata.deletionTimestamp) await this.#kube("DELETE", statefulPath, deleteExact(stateful))
      } else if (pod !== null) {
        if (owners.length === 0 && !pod.metadata.deletionTimestamp) await this.#kube("DELETE", podPath, deleteExact(pod))
      } else state = "stopped"
      await checkRetained()
      return { resource_id: resourceId, pod_uid: podUid, state,
        retained_volume_id: String(pools[0][0].provider_volume_id),
        ...(pools.length === 1 ? {} : { retained_volume_ids: record.phase.closed.native_mounts.map((mount: KubeRecord) =>
          mount.expected.provider_volume_id as string) }) }
    })
  }

  async #inspectLiveWorker({ resourceId, podUid, worker }: { resourceId: string; podUid: string; worker: JsonObject }):
    Promise<[KubeRecord, KubeRecord]> {
    const [, name, uid] = this.#parts(resourceId)
    const labels = { ...enrollmentLabels(worker), [prefix + "cluster"]: this.clusterId }
    const annotations = { [prefix + "certificate"]: worker.certificate_sha256 }
    if (name !== "nabedra-worker-" + worker.worker_id || !podUid) {
      throw new ProviderError("EKS requires exact StatefulSet and Pod UIDs for the current worker")
    }
    const stateful = await this.#kube("GET", this.#apps + "/statefulsets/" + name)
    this.#exact(stateful, uid)
    this.#owned(stateful, labels)
    this.#workerSpec(stateful, name, labels, annotations)
    const podName = name + "-0"
    const pod = await this.#kube("GET", "/pods/" + podName)
    this.#exact(pod, podUid)
    const meta = this.#owned(pod, labels)
    const controllers = (meta.ownerReferences ?? []).filter((owner: KubeRecord) => owner.controller === true)
    const zone = pod.spec?.nodeSelector?.[zoneLabel]
    if (meta.name !== podName || !matches(controllers, [{ apiVersion: "apps/v1", kind: "StatefulSet", name, uid }])
      || meta.annotations?.[prefix + "certificate"] !== worker.certificate_sha256 || pod.status?.phase !== "Running"
      || !pod.spec?.nodeName || !zone || zone !== stateful.spec.template.spec.nodeSelector?.[zoneLabel]) {
      throw new ProviderError("EKS Pod no longer binds the expected worker generation and zone")
    }
    return [stateful, pod]
  }

  async #retainedInventory(): Promise<[Map<string, KubeRecord>, Map<string, KubeRecord>]> {
    const collect = async (path: string) => {
      const records = new Map<string, KubeRecord>()
      const continuations = new Set<string>()
      const query = new URLSearchParams({ labelSelector: prefix + "cluster=" + this.clusterId, limit: "128" })
      for (let page = 0; page < 32; page++) {
        const result = await this.#kube("GET", path + "?" + query)
        if (!Array.isArray(result.items)) throw new ProviderError("EKS retained inventory is invalid")
        for (const record of result.items) {
          const meta = this.#owned(record, { [prefix + "cluster"]: this.clusterId })
          if (records.has(meta.uid) || records.size >= 4096) {
            throw new ProviderError("EKS retained inventory changed during paging")
          }
          records.set(meta.uid, record)
        }
        const cursor = result.metadata?.continue
        if (!cursor) return records
        if (typeof cursor !== "string" || continuations.has(cursor) || cursor.length > 4096) {
          throw new ProviderError("EKS retained inventory cursor is invalid")
        }
        query.set("continue", cursor)
        continuations.add(cursor)
      }
      throw new ProviderError("EKS retained inventory page limit reached")
    }
    await this.#storageClass()
    return [await collect(this.#apps + "/statefulsets"), await collect("/persistentvolumeclaims")]
  }

  replaceWorkerMounts(options: ReplaceWorkerMountsOptions): Promise<unknown> {
    return this.#limited(() => reconcileReplacement(this, options, this.#deadline()))
  }

  override async startNode(_payload: ProviderLaunch): Promise<string> {
    throw new ProviderError("OLTP EKS requires ensureWorker with retained explicit pools and a database growth claim")
  }
  override async stopNode(_resourceId: string): Promise<void> {
    throw new ProviderError("OLTP EKS worker removal requires the database drain controller and exact Pod UID")
  }
  override async listNodes(): Promise<ProviderNode[]> {
    throw new ProviderError("OLTP EKS worker state requires exact StatefulSet and enrolled Pod inventory")
  }
}

function isRecord(value: unknown): value is JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value)
}

export function exactInstance(value: unknown, zone: string): string | null {
  const prefix = "aws:///" + zone + "/"
  if (typeof value !== "string" || !value.startsWith(prefix)) return null
  const id = value.slice(prefix.length)
  return /^i-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/u.test(id) ? id : null
}

function deleteExact(record: KubeRecord): JsonObject {
  const { uid, resourceVersion } = record.metadata
  return { apiVersion: "v1", kind: "DeleteOptions", propagationPolicy: "Orphan", preconditions: { uid, resourceVersion } }
}

export function checkStorageSet(spec: KubeRecord, retained: RetainedStorage[], zone: string): void {
  const volumes: KubeRecord[] = spec.volumes ?? []
  const claims = volumes.filter(volume => Object.hasOwn(volume, "persistentVolumeClaim"))
  const devices = volumes.filter(volume => Object.hasOwn(volume, "hostPath"))
  if (claims.length !== retained.length || devices.length !== retained.length
    || new Set(volumes.map(volume => volume.name)).size !== volumes.length) {
    throw new ProviderError("EKS Pod does not have the exact retained PVC and filesystem set")
  }
  for (const [mount, pvc] of retained) {
    const devicePath = "/dev/disk/by-uuid/" + identity(mount.filesystem_uuid)
    const claimsForMount = claims.filter(volume => volume.persistentVolumeClaim?.claimName === pvc)
    const devicesForMount = devices.filter(volume => isDeepStrictEqual(volume.hostPath, { path: devicePath, type: "BlockDevice" }))
    const claim = claimsForMount[0]
    const device = devicesForMount[0]
    const worker = spec.containers?.[0]
    if (claimsForMount.length !== 1 || devicesForMount.length !== 1 || spec.containers?.length !== 1
      || worker.name !== "worker" || present(spec.ephemeralContainers) || !positiveInteger(spec.terminationGracePeriodSeconds)
      || spec.nodeSelector?.[zoneLabel] !== zone || volumes.some(volume => Object.hasOwn(volume, "ephemeral"))
      || claim.persistentVolumeClaim.readOnly === true || !claim.name || !device.name) {
      throw new ProviderError("EKS worker does not have the exact retained claim and filesystem projection")
    }
    const mounts: KubeRecord[] = worker.volumeMounts ?? []
    const mounted = mounts.filter(entry => entry.name === claim.name || entry.mountPath === mount.mount_path)
    const deviceMounts = mounts.filter(entry => entry.name === device.name || entry.mountPath === devicePath)
    const first = mounted[0]
    const second = deviceMounts[0]
    if (mounted.length !== 1 || first.name !== claim.name || first.mountPath !== mount.mount_path
      || first.readOnly || first.subPath || first.subPathExpr || (first.mountPropagation != null && first.mountPropagation !== "None")
      || deviceMounts.length !== 1 || second.name !== device.name || second.mountPath !== devicePath || second.readOnly !== true
      || present(worker.volumeDevices) || (spec.initContainers ?? []).some((init: KubeRecord) =>
        present(init.volumeDevices) || (init.volumeMounts ?? []).some((entry: KubeRecord) => [claim.name, device.name].includes(entry.name)))) {
      throw new ProviderError("EKS Pod mount and filesystem projection differ from the closed mount")
    }
  }
}

import { createHash, createPrivateKey, createPublicKey, sign as edSign, type KeyObject } from "node:crypto"
import { closeSync, constants, fstatSync, openSync, readSync } from "node:fs"
import { isDeepStrictEqual } from "node:util"
import { KubernetesProvider, ProviderError } from "../providers.js"
import { hasFields, parseJson, type JsonObject } from "../transport.js"
import { applyEksReplacement, ebsMount, ec2WorkerTags, eksReplacementStatus, readLifecycle, type NativeTarget } from "./cloudControl.js"
import { checkStorageSet, eksInternals, exactInstance, resource, zoneLabel, type EksProvider, type KubeRecord } from "./eks.js"
import { digest, identity, monotonic, uuid5 } from "./shared.js"

export const serviceAccount = "nabedra-oltp-worker-replacement"
export const tokenDirectory = "/var/run/nabedra-replacement"
const labels = "nabedra.io/"
const nodeNamePattern = /^[a-z0-9][.a-z0-9-]{0,252}(?![\s\S])/u
type EksTarget = Extract<NativeTarget, { kind: "eks" }>

export function audience(clusterId: string): string {
  return "nabedra:" + identity(clusterId) + ":eks-replacement"
}

export interface EksEc2Client {
  readonly meta?: { readonly config?: { readonly connectTimeout?: number; readonly readTimeout?: number;
    readonly retries?: { readonly mode?: string; readonly totalMaxAttempts?: number }; readonly regionName?: string }; readonly regionName?: string }
  describeVolumes(input: { VolumeIds: string[] }): unknown | Promise<unknown>
}

export function boundedEc2(client: EksEc2Client, diagnostic: string): { client: EksEc2Client; region: string; window: number } {
  const config = client?.meta?.config
  const connect = config?.connectTimeout
  const read = config?.readTimeout
  const region = config?.regionName ?? client?.meta?.regionName
  if (typeof client?.describeVolumes !== "function" || typeof connect !== "number" || !(connect > 0 && connect <= 10)
    || typeof read !== "number" || !(read > 0 && read <= 20) || config?.retries?.mode !== "standard"
    || config.retries.totalMaxAttempts !== 1 || typeof region !== "string" || !/^[a-z][a-z0-9-]+-[1-9][0-9]*(?![\s\S])/u.test(region)) {
    throw new ProviderError(diagnostic)
  }
  return { client, region, window: connect + read }
}

function protectedSeed(path: string): Buffer {
  let fd: number | undefined
  try {
    fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
    const metadata = fstatSync(fd)
    if (!metadata.isFile() || metadata.uid !== process.geteuid?.() || (metadata.mode & 0o077) !== 0
      || metadata.nlink !== 1 || metadata.size !== 32) throw new Error()
    const data = Buffer.alloc(33)
    if (readSync(fd, data) !== 32) throw new Error()
    return data.subarray(0, 32)
  } catch {
    throw new ProviderError("EKS replacement signing seed must be a private 32-byte regular file")
  } finally {
    if (fd !== undefined) closeSync(fd)
  }
}

class KubeReader extends KubernetesProvider {
  query(method: string, path: string, deadline: number, body?: JsonObject, missing = false): Promise<KubeRecord | null> {
    return this.request(method, path, body, { absolute: true, deadline, missing })
  }
}

export interface EksReplacementVerifierOptions {
  apiUrl: string
  token: string
  namespace: string
  clusterId: string
  ec2Client: EksEc2Client
  signingSeedFile: string
  caFile?: string | null
  timeout?: number
}

interface Excluded {
  expected: KubeRecord
  target: EksTarget
  pvc: string
  zone: string
  claim: string
}
type MountProof = readonly [resource: string, mount: KubeRecord, pvUid: string, claim: string]

interface Candidate {
  worker: JsonObject
  zone: string
  pools: MountProof[]
}
interface VerifierInternal {
  origin: string
  clusterId: string
  namespace: string
  signer: { publicKey: string; sign: (message: Uint8Array) => [string, string] }
  exclusion(provider: EksProvider, resourceId: string, podUid: string, retained: Excluded, deadline: number): Promise<JsonObject | null>
  candidate(provider: EksProvider, resourceId: string, token: string, expected: Candidate, deadline: number): Promise<[KubeRecord, JsonObject]>
}
const verifiers = new WeakMap<EksReplacementVerifier, VerifierInternal>()

export function replacementMatches(verifier: EksReplacementVerifier, apiUrl: string, clusterId: string, namespace: string): boolean {
  if (Object.getPrototypeOf(verifier) !== EksReplacementVerifier.prototype) return false
  const internal = verifiers.get(verifier)
  return internal !== undefined && internal.origin === apiUrl.replace(/\/$/u, "")
    && internal.clusterId === clusterId && internal.namespace === namespace
}

export class EksReplacementVerifier {
  readonly publicKey: string
  readonly #key: KeyObject
  readonly #kube: KubeReader
  readonly #aws: ReturnType<typeof boundedEc2>

  constructor({ apiUrl, token, namespace, clusterId, ec2Client, signingSeedFile, caFile, timeout = 10 }: EksReplacementVerifierOptions) {
    this.#kube = new KubeReader({ apiUrl, token, namespace, clusterId, image: "unused", caFile, timeout })
    this.#aws = boundedEc2(ec2Client, "EKS verifier requires a pinned EC2 region and bounded SDK attempts")
    const seed = protectedSeed(signingSeedFile)
    const der = Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), seed])
    try {
      this.#key = createPrivateKey({ key: der, format: "der", type: "pkcs8" })
    } finally {
      der.fill(0)
      seed.fill(0)
    }
    this.publicKey = createPublicKey(this.#key).export({ format: "der", type: "spki" }).subarray(-32).toString("hex")
    verifiers.set(this, {
      origin: apiUrl.replace(/\/$/u, ""), clusterId, namespace,
      signer: { publicKey: this.publicKey, sign: message => this.#sign(message) },
      exclusion: (provider, resourceId, podUid, retained, deadline) => this.#exclusion(provider, resourceId, podUid, retained, deadline),
      candidate: (provider, resourceId, projectedToken, expected, deadline) => this.#candidate(provider, resourceId, projectedToken, expected, deadline),
    })
  }

  #sign(message: Uint8Array): [string, string] {
    const signature = edSign(null, message, this.#key)
    return [signature.subarray(0, 32).toString("hex"), signature.subarray(32).toString("hex")]
  }

  async #attachmentsAbsent(pvName: string, deadline: number): Promise<string | null> {
    let revision: string | undefined
    let continuation: string | undefined
    const seen = new Set<string>()
    for (let page = 0; page < 64; page++) {
      const query = new URLSearchParams({ limit: "128" })
      if (continuation) query.set("continue", continuation)
      const result = await this.#kube.query("GET", "/apis/storage.k8s.io/v1/volumeattachments?" + query, deadline)
      try {
        const value = resource(result)
        const current = exactRevision(value.metadata?.resourceVersion)
        if (revision !== undefined && revision !== current) throw new Error()
        revision = current
        if (!Array.isArray(value.items) || value.items.length > 128) throw new Error()
        for (const attachment of value.items) {
          const source = resource(attachment?.spec?.source)
          const name = source.persistentVolumeName
          const inline = source.inlineVolumeSpec
          if (Boolean(name) === Boolean(inline)) throw new Error()
          if (name === pvName) return null
        }
        continuation = value.metadata?.continue
        if (!continuation) return revision
        if (typeof continuation !== "string" || continuation.length > 4096 || seen.has(continuation)) throw new Error()
        seen.add(continuation)
      } catch {
        throw new ProviderError("Kubernetes attachment inventory changed or is unreadable")
      }
    }
    throw new ProviderError("Kubernetes attachment inventory exceeds the reconciliation bound")
  }

  async #exactEbs(volumeId: string, zone: string, deadline: number): Promise<[KubeRecord, string]> {
    const { client, region, window } = this.#aws
    if (!zone.startsWith(region) || deadline - monotonic() < window) {
      throw new ProviderError("EKS EBS observation requires the exact region and a live deadline")
    }
    try {
      const response = resource(await client.describeVolumes({ VolumeIds: [volumeId] }))
      const rows = response.Volumes
      if (!Array.isArray(rows) || rows.length !== 1 || response.NextToken || deadline <= monotonic()) throw new Error()
      const volume = resource(rows[0])
      if (volume.VolumeId !== volumeId || volume.AvailabilityZone !== zone || volume.Encrypted !== true
        || (volume.MultiAttachEnabled ?? false) !== false || !Array.isArray(volume.Attachments)) throw new Error()
      return [volume, identity(response.$metadata?.requestId ?? response.ResponseMetadata?.RequestId)]
    } catch {
      throw new ProviderError("EKS verifier could not observe the exact encrypted EBS volume")
    }
  }

  async #exclusion(provider: EksProvider, resourceId: string, podUid: string, retained: Excluded, deadline: number): Promise<JsonObject | null> {
    const [namespace, name, uid] = eksInternals(provider).parts(resourceId)
    if (namespace !== this.#kube.namespace || retained.target.statefulset_uid !== uid) {
      throw new ProviderError("EKS verifier received another worker lifecycle")
    }
    const paths = [statefulPath(namespace, name), podPath(namespace, name)]
    for (const [index, path] of paths.entries()) {
      const record = await this.#kube.query("GET", path, deadline, undefined, true)
      if (record !== null) {
        if (record.metadata?.uid !== [uid, podUid][index]) {
          throw new ProviderError("Another EKS compute identity occupies the retired worker name")
        }
        return null
      }
    }
    const { pvc, expected, target, zone, claim } = retained
    const pv = await exactPv(provider, pvc, expected, target.pv_uid, zone, claim)
    const nameOfPv: string = pv.metadata.name
    const revision = await this.#attachmentsAbsent(nameOfPv, deadline)
    if (revision === null) return null
    const [volume, requestId] = await this.#exactEbs(expected.provider_volume_id, zone, deadline)
    if (volume.State !== "available" || volume.Attachments.length !== 0) return null
    for (const path of paths) {
      if (await this.#kube.query("GET", path, deadline, undefined, true) !== null) {
        throw new ProviderError("Retired EKS compute identity changed during attachment reconciliation")
      }
    }
    const confirmed = await exactPv(provider, pvc, expected, target.pv_uid, zone, claim)
    if (confirmed.metadata.name !== nameOfPv) throw new ProviderError("Retained EKS persistent volume binding changed")
    return { attachment_revision: revision, describe_volumes_request_id: requestId }
  }

  async #candidate(provider: EksProvider, resourceId: string, projectedToken: string, expected: Candidate, deadline: number):
    Promise<[KubeRecord, JsonObject]> {
    try {
      if (typeof projectedToken !== "string" || !/^[A-Za-z0-9_.-]{32,8192}(?![\s\S])/u.test(projectedToken)) throw new Error()
      const operations = eksInternals(provider)
      const [namespace, name, statefulUid] = operations.parts(resourceId)
      const tokenAudience = audience(provider.clusterId)
      const path = podPath(namespace, name)
      const pod = resource(await this.#kube.query("GET", path, deadline))
      const uid = identity(pod.metadata?.uid)
      const [stateful, verifiedPod] = await operations.inspectLiveWorker({ resourceId, podUid: uid, worker: expected.worker })
      if (pod.metadata.resourceVersion !== verifiedPod.metadata.resourceVersion) throw new Error()
      for (const spec of [resource(pod.spec), resource(stateful.spec?.template?.spec)]) {
        checkStorageSet(spec, expected.pools.map(([resource, mount]) => [mount, operations.parts(resource)[1]]), expected.zone)
        checkProjection(spec, tokenAudience)
      }
      const review = resource(await this.#kube.query("POST", "/apis/authentication.k8s.io/v1/tokenreviews", deadline, {
        apiVersion: "authentication.k8s.io/v1", kind: "TokenReview",
        spec: { token: projectedToken, audiences: [tokenAudience] },
      }))
      const status = resource(review.status)
      const user = resource(status.user)
      if (status.authenticated !== true || status.error || !isDeepStrictEqual(status.audiences, [tokenAudience])
        || user.username !== `system:serviceaccount:${namespace}:${serviceAccount}`) throw new Error()
      const extra = resource(user.extra)
      const nodeName = pod.spec.nodeName
      if (!isDeepStrictEqual(extra["authentication.kubernetes.io/pod-name"], [name + "-0"])
        || !isDeepStrictEqual(extra["authentication.kubernetes.io/pod-uid"], [uid])
        || !isDeepStrictEqual(extra["authentication.kubernetes.io/node-name"], [nodeName])
        || typeof nodeName !== "string" || !nodeNamePattern.test(nodeName)) throw new Error()
      const node = resource(await this.#kube.query("GET", "/api/v1/nodes/" + encodeURIComponent(nodeName), deadline))
      const nodeUid = identity(node.metadata?.uid)
      const zone = expected.zone
      const instance = exactInstance(node.spec?.providerID, zone)
      if (!isDeepStrictEqual(extra["authentication.kubernetes.io/node-uid"], [nodeUid]) || node.metadata.name !== nodeName
        || node.metadata.deletionTimestamp || node.metadata.labels?.[zoneLabel] !== zone || instance === null) throw new Error()
      for (const [pvc, mount, pvUid, claim] of expected.pools) {
        await exactPv(provider, pvc, mount, pvUid, zone, claim)
        const [volume] = await this.#exactEbs(mount.provider_volume_id, zone, deadline)
        const attachment = volume.Attachments[0]
        if (volume.State !== "in-use" || volume.Attachments.length !== 1 || attachment.State !== "attached"
          || attachment.InstanceId !== instance || attachment.VolumeId !== mount.provider_volume_id
          || attachment.DeleteOnTermination !== false) throw new Error()
      }
      const final = resource((await this.#kube.query("GET", path, deadline))?.metadata)
      if (final.uid !== uid || final.deletionTimestamp || final.resourceVersion !== pod.metadata.resourceVersion) throw new Error()
      const parts = projectedToken.split(".")
      if (parts.length !== 3) throw new Error()
      const expiry: unknown = resource(parseJson(Buffer.from(parts[1], "base64url"))).exp
      if ((typeof expiry !== "number" || !Number.isSafeInteger(expiry)) && typeof expiry !== "bigint") throw new Error()
      if (expiry <= Math.floor(Date.now() / 1000) || expiry >= 1n << 64n) throw new Error()
      return [{ namespace, statefulset_uid: statefulUid, pod_uid: uid },
        { pod_revision: exactRevision(final.resourceVersion), service_account_uid: identity(user.uid), node_uid: nodeUid,
          projected_token_sha256: createHash("sha256").update(projectedToken, "ascii").digest("hex"), projected_token_expires_at: expiry }]
    } catch (error) {
      if (error instanceof ProviderError) throw error
      throw new ProviderError("Projected EKS identity, retained pools or native attachment differs")
    }
  }
}

function exactRevision(value: unknown): string {
  if (typeof value !== "string" || !/^[!-~]{1,128}(?![\s\S])/u.test(value)) throw new Error("Invalid Kubernetes revision")
  return value
}

function podPath(namespace: string, name: string): string {
  return `/api/v1/namespaces/${encodeURIComponent(namespace)}/pods/${name}-0`
}

function statefulPath(namespace: string, name: string): string {
  return `/apis/apps/v1/namespaces/${encodeURIComponent(namespace)}/statefulsets/${name}`
}

async function exactPv(provider: EksProvider, pvcResource: string, mount: KubeRecord, uid: string, zone: string,
  claim: string): Promise<KubeRecord> {
  const operations = eksInternals(provider)
  const [, pvc] = await operations.pvc(pvcResource, { workerId: mount.worker.worker_id, zone, poolId: mount.identity.pool_id })
  const pv = await operations.boundVolume(pvc, { zone, expected: mount.provider_volume_id })
  if (pvc.metadata.labels?.[labels + "claim"] !== claim || pv.metadata.uid !== uid) {
    throw new ProviderError("EKS replacement PVC/PV identity or original growth claim changed")
  }
  return pv
}

function checkProjection(spec: KubeRecord, tokenAudience: string): void {
  const expected = { sources: [{ serviceAccountToken: { audience: tokenAudience, expirationSeconds: 600, path: "token" } }],
    defaultMode: 0o440 }
  const name = "replacement-identity"
  const volumes: KubeRecord[] = spec.volumes.filter((item: KubeRecord) => item.name === name)
  const projected: KubeRecord[] = spec.containers[0].volumeMounts.filter((item: KubeRecord) =>
    item.name === name || item.mountPath === tokenDirectory)
  if (spec.serviceAccountName !== serviceAccount || spec.automountServiceAccountToken !== false
    || !isDeepStrictEqual(volumes, [{ name, projected: expected }])
    || !isDeepStrictEqual(projected, [{ name, mountPath: tokenDirectory, readOnly: true }])) {
    throw new ProviderError("EKS candidate lacks the dedicated projected Pod credential")
  }
}

export interface ReplaceWorkerMountsOptions {
  claimId: string
  resourceId: string
  expectedPodUid: string
  template: JsonObject
  projectedToken?: string | null
}

export async function reconcileReplacement(provider: EksProvider, { claimId, resourceId, expectedPodUid, template,
  projectedToken = null }: ReplaceWorkerMountsOptions, deadline: number): Promise<unknown> {
  const control = provider.lifecycleAuthority
  const verifier = provider.replacementVerifier
  const observer = verifier === null ? undefined : verifiers.get(verifier)
  if (control === null || verifier === null || observer === undefined) {
    throw new ProviderError("Controlled EKS replacement requires protected Control and a dedicated verifier")
  }
  const operations = eksInternals(provider)
  const drainId = identity(claimId)
  const record = resource(await readLifecycle(control, "worker.drain.status", drainId, {}, deadline))
  const current = await eksReplacementStatus(control, drainId, deadline)
  let durable = current === null ? null : resource(current)
  let launchDigest: string, oldWorker: KubeRecord, target: EksTarget, previous: KubeRecord, growthId: string, original: string
  let oldStatefulUid: string, next: KubeRecord, added: KubeRecord, worker: KubeRecord, oldPvc: string, addedPvc: string
  try {
    if (Object.keys(template).sort().join(",") !== "added,bootstrap_secret,cpu_millicores,memory_bytes,next_intent") throw new Error()
    launchDigest = digest(template)
    const drain = resource(record.intent)
    oldWorker = resource(drain.worker)
    const native = resource(drain.native_target)
    if (!hasFields(native, "kind", "namespace", "statefulset_uid", "pod_uid", "pvc_uid", "pv_uid")
      || new Set([native.statefulset_uid, native.pod_uid, native.pvc_uid, native.pv_uid].map(identity)).size !== 4) throw new Error()
    target = native as EksTarget
    previous = ebsMount(record.phase?.closed?.mount, provider.clusterId, oldWorker)
    growthId = identity(drain.replacement_growth_claim)
    original = identity(record.growth_claim)
    const [namespace, name, uid] = operations.parts(resourceId)
    oldStatefulUid = uid
    next = resource(template.next_intent)
    added = resource(template.added)
    worker = Object.fromEntries(Object.keys(oldWorker).map(key => [key, next.enrollment[key]]))
    ec2WorkerTags(worker)
    const generation = typeof previous.generation === "bigint" ? previous.generation + 1n : previous.generation + 1
    if (record.id !== drainId || target.kind !== "eks" || target.namespace !== namespace
      || name !== "nabedra-worker-" + oldWorker.worker_id || oldStatefulUid !== identity(target.statefulset_uid)
      || identity(expectedPodUid) !== identity(target.pod_uid) || next.native_target != null || next.claim_id !== original
      || next.run_id !== drain.run_id || next.eks_replacement_key !== verifier.publicKey
      || !isDeepStrictEqual(next.mount.worker, worker) || worker.worker_id !== oldWorker.worker_id
      || worker.incarnation === oldWorker.incarnation || worker.certificate_sha256 === oldWorker.certificate_sha256
      || worker.enrollment_revision <= oldWorker.enrollment_revision
      || !isDeepStrictEqual(next.mount, { ...previous, worker, generation })
      || !isDeepStrictEqual(added.mount.worker, worker) || added.mount.profile !== "ebs"
      || added.mount.identity.cluster_id !== provider.clusterId || added.mount.identity.worker_id !== worker.worker_id
      || added.mount.identity.pool_id === previous.identity.pool_id || added.mount.filesystem_uuid === previous.filesystem_uuid
      || added.mount.provider_volume_id === previous.provider_volume_id
      || identity(added.pvc_uid) === identity(target.pvc_uid) || identity(added.pv_uid) === identity(target.pv_uid)
      || (durable !== null && (durable.drain_id !== drainId || durable.add_pool_claim !== growthId
        || durable.exclusion?.launch_digest !== launchDigest))) throw new Error()
    const pvc = (mount: KubeRecord, uid: string) => [namespace, "nabedra-pool-" + identity(mount.identity.pool_id), identity(uid)].join("/")
    oldPvc = pvc(previous, target.pvc_uid)
    addedPvc = pvc(added.mount, added.pvc_uid)
  } catch {
    throw new ProviderError("EKS replacement launch differs from the closed worker or durable operation")
  }
  if (durable !== null) {
    if (!Object.hasOwn(durable, "authorization")) throw new ProviderError("Control returned an invalid EKS replacement record")
    if (durable.authorization !== null) return durable
  }
  const growth = resource(await readLifecycle(control, "storage.growth.status", growthId, { claim_id: growthId }, deadline))
  const intent = resource(growth.intent)
  const zone = intent.action?.zone
  if (typeof zone !== "string" || !zone || growth.phase !== "awaiting_mount" || intent.action.kind !== "add_pool"
    || intent.claim_id !== growthId || !isDeepStrictEqual(intent.worker, oldWorker) || intent.pool_id !== added.mount.identity.pool_id
    || intent.pool_generation !== added.mount.generation || growth.provider_resource !== added.mount.provider_volume_id) {
    throw new ProviderError("EKS added mount differs from the original pending growth claim")
  }
  await operations.storageClass()
  await exactPv(provider, addedPvc, added.mount, added.pv_uid, zone, growthId)
  if (durable === null) {
    const stopped = await provider.stopWorker({ claimId: drainId, resourceId, podUid: expectedPodUid })
    if (stopped.state !== "stopped") return { state: "stopping" }
    const proof = await observer.exclusion(provider, resourceId, expectedPodUid,
      { expected: previous, target, pvc: oldPvc, zone, claim: original }, deadline)
    if (proof === null) return { state: "awaiting_provider_exclusion" }
    durable = resource(await applyEksReplacement(control, uuid5(drainId, "eks-provider-exclusion-v1"), {
      action: "exclude", evidence: { ...proof, drain_id: drainId, launch_digest: launchDigest },
    }, observer.signer, deadline))
  }
  if (durable.drain_id !== drainId || durable.exclusion?.launch_digest !== launchDigest
    || durable.zone !== zone || durable.add_pool_claim !== growthId) {
    throw new ProviderError("EKS replacement has no matching committed exclusion")
  }
  await exactPv(provider, oldPvc, previous, target.pv_uid, zone, original)
  const mounts: MountProof[] = [[oldPvc, previous, target.pv_uid, original], [addedPvc, added.mount, added.pv_uid, growthId]]
  const staged = await operations.ensureWorker({ claimId: original, workerId: worker.worker_id, incarnation: worker.incarnation,
    zone, bootstrapSecret: template.bootstrap_secret as string, cpuMillicores: template.cpu_millicores as number,
    memoryBytes: template.memory_bytes as number, enrollment: worker, replacement: identity(durable.exclusion_operation),
    pools: mounts.map(([pvc, mount]) => ({ resource_id: pvc, mount_path: mount.mount_path, filesystem_uuid: mount.filesystem_uuid })) })
  if (projectedToken === null) return { state: "awaiting_projected_pod_identity", resource_id: staged.resource_id }
  const [newTarget, proof] = await observer.candidate(provider, staged.resource_id, projectedToken, { worker, zone, pools: mounts }, deadline)
  if (newTarget.statefulset_uid === oldStatefulUid || newTarget.pod_uid === expectedPodUid) {
    throw new ProviderError("EKS candidate still has the retired compute identity")
  }
  const nativeTarget = { ...newTarget, kind: "eks", pvc_uid: target.pvc_uid, pv_uid: target.pv_uid }
  return applyEksReplacement(control, uuid5(drainId, "eks-replacement-authorization-v1"), { action: "authorize",
    evidence: { ...proof, drain_id: drainId, exclusion_operation: durable.exclusion_operation, launch_digest: launchDigest,
      next: { ...next, native_target: nativeTarget }, added } }, observer.signer, deadline)
}

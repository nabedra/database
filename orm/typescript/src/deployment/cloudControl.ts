import { isDeepStrictEqual } from "node:util"
import { ControlTransport } from "../controlTransport.js"
import { ProviderError } from "../providers.js"
import { hasFields, type JsonObject } from "../transport.js"
import { identity, monotonic, requireRecord, trustedEndpoint } from "./shared.js"

const operations = {
  mutations: new Set(["worker.drain", "worker.replacement.apply"]),
  reads: new Set(["storage.pool.detach_permit", "worker.drain.status", "worker.stop_permit",
    "worker.replacement.check", "worker.replacement.status", "worker.replacement.challenge", "storage.growth.status"]),
}
const instanceId = /^i-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/
const volumeId = /^vol-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/
const certificate = /^[a-f0-9]{64}(?![\s\S])/
const uidFields = ["statefulset_uid", "pod_uid", "pvc_uid", "pv_uid"]

export type NativeTarget =
  | { kind: "ec2"; instance_id: string }
  | { kind: "eks"; namespace: string; statefulset_uid: string; pod_uid: string; pvc_uid: string; pv_uid: string }

export interface CloudLifecycleOptions {
  apiUrl: string
  adminToken: string
  caFile?: string | null
  timeout?: number
}

export interface WorkerDrainOptions {
  worker: JsonObject
  runId: string
  nativeTarget: NativeTarget
  replacementGrowthClaim?: string | null
  retainedReplacementDrain?: string | null
}

function positiveInteger(value: unknown): boolean {
  return typeof value === "bigint" ? value > 0n : Number.isSafeInteger(value) && (value as number) > 0
}

function nonnegativeInteger(value: unknown): boolean {
  return typeof value === "bigint" ? value >= 0n : Number.isSafeInteger(value) && (value as number) >= 0
}

function nativeTarget(value: unknown): NativeTarget {
  try {
    const target = requireRecord(value)
    if (target.kind === "ec2") {
      if (hasFields(target, "kind", "instance_id") && typeof target.instance_id === "string" && instanceId.test(target.instance_id)) {
        return target as NativeTarget
      }
    } else if (
      target.kind === "eks" && hasFields(target, "kind", "namespace", ...uidFields) &&
      typeof target.namespace === "string" && /^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?(?![\s\S])/.test(target.namespace)
      && new Set(uidFields.map(key => identity(target[key]))).size === uidFields.length
    ) {
      return target as NativeTarget
    }
  } catch {}
  throw new ProviderError("An exact native EC2 instance or EKS StatefulSet, Pod, PVC and PV is required")
}

export function ec2WorkerTags(value: unknown): Record<string, string> {
  try {
    const worker = requireRecord(value)
    const { enrollment_revision: revision, certificate_sha256: fingerprint } = worker
    if (!positiveInteger(revision) || typeof fingerprint !== "string" || !certificate.test(fingerprint)) throw new Error()
    return { "nabedra:worker": identity(worker.worker_id), "nabedra:incarnation": identity(worker.incarnation),
      "nabedra:enrollment-revision": String(revision), "nabedra:certificate": fingerprint }
  } catch {
    throw new ProviderError("Exact cloud worker enrollment is required")
  }
}

export function ebsMount(value: unknown, clusterId: string, worker?: unknown): JsonObject {
  const mount = requireRecord(value)
  const expected = requireRecord(mount.expected)
  const pool = requireRecord(expected.identity)
  const mountedWorker = requireRecord(expected.worker)
  const path = expected.mount_path
  if (expected.profile !== "ebs" || pool.cluster_id !== clusterId || pool.engine !== "oltp" || pool.pool_format_version !== 1
    || pool.worker_id !== mountedWorker.worker_id || (worker !== undefined && !isDeepStrictEqual(worker, mountedWorker))
    || typeof expected.provider_volume_id !== "string" || !volumeId.test(expected.provider_volume_id)
    || typeof path !== "string" || !path.startsWith("/") || path.includes("\0")
    || path.slice(1).split("/").some(part => part === "" || part === "." || part === "..")
    || !positiveInteger(expected.generation)
    || !positiveInteger(mount.mount_id) || !positiveInteger(mount.root_inode)
    || !nonnegativeInteger(mount.device_major) || !nonnegativeInteger(mount.device_minor)) {
    throw new ProviderError("Invalid EBS mount proof")
  }
  identity(pool.pool_id)
  identity(expected.filesystem_uuid)
  ec2WorkerTags(mountedWorker)
  return expected
}

type PoolProof = [mount: JsonObject, claimId: string, pvcUid: string | null, pvUid: string | null]

export function closedWorkerMounts(record: JsonObject, clusterId: string, target: NativeTarget): PoolProof[] {
  const closed = requireRecord(requireRecord(record.phase).closed)
  const intent = requireRecord(record.intent)
  const primary = closed.mount
  const primaryMount = ebsMount(primary, clusterId, intent.worker)
  const claim = identity(record.growth_claim)
  const primaryPool: PoolProof = [primaryMount, claim, target.kind === "eks" ? target.pvc_uid : null,
    target.kind === "eks" ? target.pv_uid : null]
  if (!Object.hasOwn(closed, "native_mounts") && !Object.hasOwn(record, "retained_eks_pool")
    && !Object.hasOwn(intent, "retained_replacement_drain")) return [primaryPool]
  const mounts = closed.native_mounts
  const retained = requireRecord(record.retained_eks_pool)
  const pool = requireRecord(retained.pool)
  const retainedClaim = identity(retained.claim_id)
  if (target.kind !== "eks" || Object.hasOwn(intent, "replacement_growth_claim")
    || identity(intent.retained_replacement_drain) === record.id || !Array.isArray(mounts) || mounts.length !== 2
    || mounts.filter(mount => isDeepStrictEqual(mount, primary)).length !== 1 || !hasFields(retained, "claim_id", "pool")
    || !hasFields(pool, "mount", "pvc_uid", "pv_uid") || retainedClaim === claim) {
    throw new ProviderError("Invalid retained EKS pool proof")
  }
  const [first, second] = mounts.map(mount => ebsMount(mount, clusterId, intent.worker))
  const [firstProof, secondProof] = mounts.map(requireRecord)
  if (identity(requireRecord(first.identity).pool_id) >= identity(requireRecord(second.identity).pool_id)
    || firstProof.mount_id === secondProof.mount_id
    || (firstProof.device_major === secondProof.device_major && firstProof.device_minor === secondProof.device_minor)
    || ["provider_volume_id", "filesystem_uuid", "mount_path"].some(key => first[key] === second[key])
    || String(first.mount_path).startsWith(String(second.mount_path) + "/")
    || String(second.mount_path).startsWith(String(first.mount_path) + "/")) {
    throw new ProviderError("Invalid EKS native mount set")
  }
  const added = isDeepStrictEqual(firstProof, primary) ? second : first
  const pvc = identity(pool.pvc_uid)
  const pv = identity(pool.pv_uid)
  if (!isDeepStrictEqual(added, pool.mount) || new Set([pvc, pv, ...uidFields.map(key => target[key as keyof typeof target])]).size !== 6) {
    throw new ProviderError("Invalid EKS retained mount identity")
  }
  return [primaryPool, [added, retainedClaim, pvc, pv]]
}

const controls = new WeakMap<CloudLifecycleControl, ControlTransport>()
const durations = new WeakMap<CloudLifecycleControl, number>()

export async function readLifecycle(control: CloudLifecycleControl, operation: string, requestId: string,
  payload: JsonObject, deadline: number): Promise<unknown> {
  const [, response] = await controls.get(control)!.request(operation, payload, { requestId: identity(requestId), deadline })
  return response
}

export async function eksReplacementStatus(control: CloudLifecycleControl, drainId: string, deadline: number): Promise<unknown> {
  const response = await readLifecycle(control, "worker.replacement.status", drainId, {}, deadline)
  if (!hasFields(response, "replacement")) throw new ProviderError("Control returned an invalid EKS replacement record")
  return response.replacement
}

export async function applyEksReplacement(control: CloudLifecycleControl, operationId: string, action: JsonObject,
  signer: { publicKey: string; sign: (message: Uint8Array) => string | [string, string] | Promise<string | [string, string]> }, deadline: number): Promise<unknown> {
  const challenge = requireRecord(await readLifecycle(control, "worker.replacement.challenge", operationId, action, deadline))
  if (challenge.verifier_key !== signer.publicKey || typeof challenge.digest !== "string" || !certificate.test(challenge.digest)) {
    throw new ProviderError("Control challenge differs from the enrolled EKS verifier key")
  }
  const signature = await signer.sign(Buffer.from(challenge.digest, "hex"))
  return readLifecycle(control, "worker.replacement.apply", operationId, { action, signature }, deadline)
}

export async function closedWorker(control: CloudLifecycleControl, drainId: string, clusterId: string, kind: NativeTarget["kind"],
  deadline: number): Promise<[JsonObject, string]> {
  const reply = await readLifecycle(control, "worker.stop_permit", drainId, {}, deadline)
  try {
    const { record: raw, native_zone: zone } = requireRecord(reply)
    const record = requireRecord(raw)
    const intent = requireRecord(record.intent)
    const phase = requireRecord(record.phase)
    const target = nativeTarget(intent.native_target)
    const ready = requireRecord(requireRecord(phase.closed).ready)
    if (record.id !== drainId || !hasFields(phase, "closed") || target.kind !== kind
      || Object.hasOwn(intent, "docker_container_id") || typeof zone !== "string" || !zone || [...zone].length > 128
      || requireRecord(ready.view).run_id !== identity(intent.run_id)
      || !positiveInteger(ready.ready_count) || (ready.ready_count as number | bigint) < 3) throw new Error()
    closedWorkerMounts(record, clusterId, target)
    return [record, zone]
  } catch {
    throw new ProviderError("Control returned no exact closed cloud worker permit")
  }
}

export async function closedPool(control: CloudLifecycleControl, drainId: string, clusterId: string, volume: string,
  deadline: number): Promise<JsonObject> {
  const value = await readLifecycle(control, "storage.pool.detach_permit", drainId, {}, deadline)
  try {
    const record = requireRecord(value)
    const expected = ebsMount(record.mount, clusterId)
    if (record.id !== drainId || record.phase !== "closed" || expected.provider_volume_id !== volume) throw new Error()
    identity(record.growth_claim)
    return record
  } catch {
    throw new ProviderError("Control returned no exact closed EBS pool permit")
  }
}

export class CloudLifecycleControl {
  constructor({ apiUrl, adminToken, caFile, timeout = 20 }: CloudLifecycleOptions) {
    if (!trustedEndpoint(apiUrl)) throw new ProviderError("Cloud lifecycle requires authenticated HTTPS or numeric loopback")
    controls.set(this, new ControlTransport(apiUrl, adminToken, { caFile, timeout }, operations))
    durations.set(this, timeout)
  }

  async beginWorkerDrain(drainId: string, { worker, runId, nativeTarget: raw, replacementGrowthClaim,
    retainedReplacementDrain }: WorkerDrainOptions): Promise<unknown> {
    ec2WorkerTags(worker)
    const target = nativeTarget(raw)
    const payload: JsonObject = { worker, run_id: identity(runId), native_target: target }
    if (replacementGrowthClaim != null) {
      if (target.kind !== "eks") throw new ProviderError("A replacement growth claim requires an EKS worker")
      payload.replacement_growth_claim = identity(replacementGrowthClaim)
    }
    if (retainedReplacementDrain != null) {
      if (target.kind !== "eks" || replacementGrowthClaim != null) {
        throw new ProviderError("A retained replacement drain requires an EKS stop without an AddPool claim")
      }
      payload.retained_replacement_drain = identity(retainedReplacementDrain)
    }
    return readLifecycle(this, "worker.drain", drainId, payload, monotonic() + durations.get(this)!)
  }

  workerDrainStatus(drainId: string): Promise<unknown> {
    return readLifecycle(this, "worker.drain.status", drainId, {}, monotonic() + durations.get(this)!)
  }

  checkWorkerReplacement(drainId: string, { nextIntent }: { nextIntent: JsonObject }): Promise<unknown> {
    return readLifecycle(this, "worker.replacement.check", drainId, nextIntent, monotonic() + durations.get(this)!)
  }

  workerReplacementStatus(drainId: string): Promise<unknown> {
    return eksReplacementStatus(this, drainId, monotonic() + durations.get(this)!)
  }
}

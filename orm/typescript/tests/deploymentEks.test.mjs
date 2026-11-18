import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { once } from "node:events"
import { mkdtemp, mkdir, realpath, rm, symlink, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import { join } from "node:path"
import test from "node:test"
import { ProviderError } from "../dist/providers.js"
import { CloudLifecycleControl } from "../dist/deployment/cloudControl.js"
import { GrowthController } from "../dist/deployment/controller.js"
import { EksProvider } from "../dist/deployment/eks.js"
import { EksReplacementVerifier } from "../dist/deployment/eksReplacement.js"
import { EksGrowthAdapter, EksWorkerTemplate } from "../dist/deployment/liveEks.js"

const zone = "us-east-1a"
const namespace = "storage"
const storageClass = "retained-ebs"
const volumeId = "vol-00000001"
const kubeUrl = "https://kubernetes.example"
const second = 1000
const gib = 2 ** 30
const timeout = 5
const providerImage = "registry.example/worker@sha256:" + "a".repeat(64)
const now = () => performance.now() / second
const ec2Client = {
  meta: { config: { connectTimeout: 0.2, readTimeout: 0.2, retries: { mode: "standard", totalMaxAttempts: 1 },
    regionName: "us-east-1" } },
  describeVolumes() { assert.fail("EC2 is outside this fixture's operation boundary") },
}

class KubeFixture extends EksProvider {
  constructor(clusterId, options = {}) {
    super({ apiUrl: kubeUrl, token: "fixture-token", clusterId, namespace, image: providerImage, storageClass, timeout, ...options })
    this.resources = new Map()
    this.writes = []
    this.retention = "Retain"
  }

  async request(method, path, value, options = {}) {
    const route = path.split("?")[0]
    if (route.includes("/storageclasses/")) return { provisioner: "ebs.csi.aws.com", reclaimPolicy: this.retention,
      volumeBindingMode: "WaitForFirstConsumer", allowVolumeExpansion: true, parameters: { encrypted: "true" } }
    if (method === "GET") {
      if (path.includes("?")) {
        return { items: [...this.resources.entries()].filter(([key]) => key.startsWith(route + "/"))
          .map(([, value]) => structuredClone(value)), metadata: {} }
      }
      const record = this.resources.get(path)
      if (record === undefined) {
        assert(options.missing, "Missing Kubernetes fixture resource " + path)
        return null
      }
      return structuredClone(record)
    }
    this.writes.push({ method, path, value: structuredClone(value) })
    if (method === "POST") {
      const record = structuredClone(value)
      record.metadata.uid = randomUUID()
      record.metadata.resourceVersion = "7"
      this.resources.set(path + "/" + record.metadata.name, record)
    } else if (method === "PUT") {
      assert(this.resources.has(path))
      this.resources.set(path, structuredClone(value))
    } else if (method === "DELETE") {
      const record = this.resources.get(path)
      assert.deepEqual(value, { apiVersion: "v1", kind: "DeleteOptions", propagationPolicy: "Orphan",
        preconditions: { uid: record.metadata.uid, resourceVersion: record.metadata.resourceVersion } })
      this.resources.delete(path)
      if (path.includes("statefulsets")) this.resources.get("/pods/" + record.metadata.name + "-0").metadata.ownerReferences = []
    }
    return {}
  }
}

function mountState(clusterId, worker, poolId) {
  return { profile: "ebs", worker, generation: 1, mount_path: "/data/primary", provider_volume_id: volumeId,
    filesystem_uuid: randomUUID(), identity: { engine: "oltp", pool_format_version: 1, cluster_id: clusterId,
      worker_id: worker.worker_id, pool_id: poolId } }
}

async function storage(provider, claimId, worker, poolId) {
  const pool = await provider.ensurePool({ claimId, poolId, workerId: worker.worker_id, zone, bytes: gib })
  const pvcPath = "/persistentvolumeclaims/" + pool.pvc_name
  const pvc = provider.resources.get(pvcPath)
  pvc.status = { phase: "Bound", capacity: { storage: "1Gi" } }
  pvc.spec.volumeName = "pv-test"
  const pv = {
    metadata: { name: "pv-test", uid: randomUUID() }, status: { phase: "Bound" },
    spec: { claimRef: { namespace, name: pool.pvc_name, uid: pvc.metadata.uid }, csi: { driver: "ebs.csi.aws.com", volumeHandle: volumeId },
      storageClassName: storageClass, accessModes: ["ReadWriteOncePod"], persistentVolumeReclaimPolicy: "Retain",
      nodeAffinity: { required: { nodeSelectorTerms: [{ matchExpressions: [{ key: "topology.ebs.csi.aws.com/zone", operator: "In",
        values: [zone] }] }] } } },
  }
  provider.resources.set("/api/v1/persistentvolumes/pv-test", pv)
  return [pool, pvc, pv]
}

function enrollment() {
  return { worker_id: randomUUID(), incarnation: randomUUID(), certificate_sha256: "c".repeat(64), enrollment_revision: 1 }
}

async function loopback(t, authorize) {
  const server = createServer(async (req, res) => {
    const chunks = []
    for await (const chunk of req) chunks.push(chunk)
    const request = JSON.parse(Buffer.concat(chunks))
    const data = authorize(request)
    res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify({ request_id: request.request_id, data }))
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(() => new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve())))
  return "http://127.0.0.1:" + server.address().port
}

test("A02 EKS expands only the exact retained same-zone bound claim", async () => {
  const provider = new KubeFixture(randomUUID())
  const claimId = randomUUID()
  const poolId = randomUUID()
  const worker = enrollment()
  provider.retention = "Delete"
  await assert.rejects(provider.ensurePool({ claimId, poolId, workerId: worker.worker_id, zone, bytes: gib }), ProviderError)
  assert.equal(provider.writes.length, 0)
  provider.retention = "Retain"
  const [pool, pvc, pv] = await storage(provider, claimId, worker, poolId)
  assert.deepEqual(pvc.spec.accessModes, ["ReadWriteOncePod"])
  assert.match(pvc.metadata.labels["nabedra.io/content"], /^[a-z2-7]{52}$/)
  assert.equal((await provider.ensurePool({ claimId, poolId, workerId: worker.worker_id, zone, bytes: gib })).state, "Bound")
  assert.equal(provider.writes.length, 1)
  const options = { resourceId: pool.resource_id, targetBytes: 2 * gib, poolId, worker, zone, volumeId }
  await assert.rejects(provider.expandPool({ ...options, volumeId: "vol-00000002" }), /exact retained same-zone EBS volume/)
  await assert.rejects(provider.expandPool({ ...options, resourceId: pool.resource_id.replace(pvc.metadata.uid, randomUUID()) }), /UID or cluster changed/)
  pv.spec.nodeAffinity.required.nodeSelectorTerms.push({ matchExpressions: [] })
  await assert.rejects(provider.expandPool(options), /exact retained same-zone EBS volume/)
  pv.spec.nodeAffinity.required.nodeSelectorTerms.pop()
  assert.equal(provider.writes.length, 1)
  assert.deepEqual(await provider.expandPool(options), { resource_id: volumeId, state: "awaiting_filesystem" })
  assert.equal(provider.writes.at(-1).value.metadata.resourceVersion, "7")
  assert.equal(provider.writes.at(-1).value.spec.resources.requests.storage, String(2 * gib))
  await provider.expandPool(options)
  assert.equal(provider.writes.length, 2)
})

test("A02 EKS stops only with a closed permit and orphans the exact Pod before deleting it", async t => {
  const clusterId = randomUUID()
  const drain = randomUUID()
  const run = randomUUID()
  const claimId = randomUUID()
  const poolId = randomUUID()
  const worker = enrollment()
  const expected = mountState(clusterId, worker, poolId)
  const requests = []
  let permit
  const control = new CloudLifecycleControl({ apiUrl: await loopback(t, request => {
    requests.push(request)
    return permit
  }), adminToken: "fixture-admin" })
  const provider = new KubeFixture(clusterId, { lifecycleAuthority: control })
  const [pool, pvc, pv] = await storage(provider, claimId, worker, poolId)
  const create = { claimId, workerId: worker.worker_id, incarnation: worker.incarnation, zone,
    bootstrapSecret: "bootstrap-workers", cpuMillicores: 1500, memoryBytes: gib, enrollment: worker,
    pools: [{ resource_id: pool.resource_id, mount_path: expected.mount_path, filesystem_uuid: expected.filesystem_uuid }] }
  const launched = await provider.ensureWorker(create)
  assert.equal(launched.state, "awaiting_database_ready")
  assert.deepEqual(await provider.ensureWorker(create), launched)
  const stateful = [...provider.resources.values()].find(record => record.kind === "StatefulSet")
  assert.equal(stateful.spec.updateStrategy.type, "OnDelete")
  assert.deepEqual(stateful.spec.persistentVolumeClaimRetentionPolicy, { whenDeleted: "Retain", whenScaled: "Retain" })
  const podUid = randomUUID()
  const pod = { metadata: { ...stateful.metadata, name: stateful.metadata.name + "-0", uid: podUid, ownerReferences: [
    { apiVersion: "apps/v1", kind: "StatefulSet", name: stateful.metadata.name, uid: stateful.metadata.uid, controller: true },
  ] }, spec: { ...stateful.spec.template.spec, nodeName: "node-a" }, status: { phase: "Running" } }
  provider.resources.set("/pods/" + pod.metadata.name, pod)
  const target = { kind: "eks", namespace, statefulset_uid: stateful.metadata.uid, pod_uid: podUid,
    pvc_uid: pvc.metadata.uid, pv_uid: pv.metadata.uid }
  permit = { native_zone: zone, record: { id: drain, growth_claim: claimId, intent: { worker, native_target: target, run_id: run },
    phase: { closed: { ready: { view: { run_id: run }, ready_count: 3 },
      mount: { expected, mount_id: 7, root_inode: 8, device_major: 1, device_minor: 2 } } } } }
  const stop = { claimId: drain, resourceId: launched.resource_id, podUid }
  permit.record.phase.closed.ready.ready_count = 2
  await assert.rejects(provider.stopWorker(stop), /no exact closed cloud worker permit/)
  permit.record.phase.closed.ready.ready_count = 3
  await assert.rejects(provider.stopWorker({ ...stop, podUid: randomUUID() }), /different StatefulSet or Pod/)
  pv.metadata.uid = randomUUID()
  await assert.rejects(provider.stopWorker(stop), /exact retained PVC or PV/)
  pv.metadata.uid = target.pv_uid
  assert.equal(provider.writes.filter(entry => entry.method === "DELETE").length, 0)
  assert.equal((await provider.stopWorker(stop)).state, "stopping")
  assert(provider.resources.has("/pods/" + pod.metadata.name), "StatefulSet deletion is orphan-only")
  assert.equal((await provider.stopWorker(stop)).state, "stopping")
  assert.equal((await provider.stopWorker(stop)).state, "stopped")
  assert.equal(provider.writes.filter(entry => entry.method === "DELETE").length, 2)
  assert(provider.resources.has("/persistentvolumeclaims/" + pool.pvc_name))
  assert(provider.resources.has("/api/v1/persistentvolumes/pv-test"))
  assert(requests.every(request => request.operation === "worker.stop_permit" && request.request_id === drain))
})

test("A02 EKS staging accepts only one exact protected endpoint for its worker slot", async t => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "nabedra-eks-")))
  t.after(() => rm(root, { recursive: true, force: true }))
  const handoff = join(root, "handoff")
  await mkdir(handoff, { mode: 0o700 })
  const worker = enrollment()
  const name = "nabedra-worker-" + worker.worker_id
  const serverName = `${name}-0.${name}.${namespace}.svc.cluster.local`
  const enrollmentFile = join(root, "enrollment.json")
  const tokenFile = join(root, "admin-token")
  const token = "a".repeat(43)
  const endpoint = "https://worker-admin.storage.example"
  await writeFile(enrollmentFile, JSON.stringify({ ...worker, server_name: serverName, endpoint: serverName + ":9000" }), { mode: 0o600 })
  await writeFile(tokenFile, token + "\n", { mode: 0o600 })
  let ebs = { VolumeId: volumeId, AvailabilityZone: zone, Encrypted: true, Size: 1,
    State: "available", Attachments: [] }
  const provider = new KubeFixture(randomUUID(), { stagingEc2Client: { ...ec2Client, describeVolumes(params) {
    assert.deepEqual(params, { VolumeIds: [volumeId] })
    return { Volumes: [structuredClone(ebs)] }
  } } })
  const poolId = randomUUID()
  const template = new EksWorkerTemplate(zone, poolId, "/data/primary", 1000, gib, 4, gib, 1024, "b".repeat(64),
    enrollmentFile, endpoint, tokenFile, handoff)
  const adapter = new EksGrowthAdapter({ provider, adapterId: randomUUID(), pvcs: {}, workers: {},
    maxVolumeGib: 2, templates: [template] })
  const claim = randomUUID()
  const action = { kind: "add_worker", zone, bytes: gib - 1024 }
  const exact = new GrowthController({ apiUrl: endpoint, adminToken: token })
  const wrong = new GrowthController({ apiUrl: endpoint, adminToken: "x".repeat(43) })
  assert.equal(adapter.target(claim, action, {}), null)
  assert.equal(adapter.bindEndpoints({ [worker.worker_id]: wrong }).target(claim, action, {}), null)
  assert.equal(adapter.bindEndpoints({ [worker.worker_id]: exact, [randomUUID()]: exact }).target(claim, action, {}), null)
  const bound = adapter.bindEndpoints({ [worker.worker_id]: exact })
  const target = bound.target(claim, action, {})
  assert.deepEqual(target.worker, worker)
  assert.equal(bound.target(claim, { ...action, bytes: gib }, {}), null)
  const observation = await bound.observe({}, { policy: { zones: [zone], worker_cost: 5, volume_cost_per_gib: 1 }, deadline: now() + timeout })
  assert.deepEqual([...observation.workerZones], [zone])
  const intent = { claim_id: claim, action, worker, pool_id: poolId, pool_generation: 1,
    quoted_monthly_cost: 5, monthly_spend_ceiling: 10 }
  const updates = []
  const stage = () => bound.provisionWorker(intent, target, { deadline: now() + timeout,
    authorize: () => { updates.push("authorized") } })
  await assert.rejects(stage(), /WaitForFirstConsumer PVC/)
  assert.deepEqual(updates, ["authorized"])
  const pvcName = "nabedra-pool-" + poolId
  const pvc = provider.resources.get("/persistentvolumeclaims/" + pvcName)
  pvc.status = { phase: "Bound", capacity: { storage: "1Gi" } }
  pvc.spec.volumeName = "staged-pv"
  provider.resources.set("/api/v1/persistentvolumes/staged-pv", {
    metadata: { name: "staged-pv", uid: randomUUID() }, status: { phase: "Bound" },
    spec: { claimRef: { namespace, name: pvcName, uid: pvc.metadata.uid }, csi: { driver: "ebs.csi.aws.com", volumeHandle: volumeId },
      storageClassName: storageClass, accessModes: ["ReadWriteOncePod"], persistentVolumeReclaimPolicy: "Retain",
      nodeAffinity: { required: { nodeSelectorTerms: [{ matchExpressions: [{ key: "topology.ebs.csi.aws.com/zone",
        operator: "In", values: [zone] }] }] } } },
  })
  ebs = { ...ebs, State: "in-use", Attachments: [{ InstanceId: "i-00000001" }] }
  await assert.rejects(stage(), /awaits exclusive ownership/)
  assert.equal(updates.length, 1)
  ebs = { ...ebs, State: "available", Attachments: [] }
  assert.deepEqual(await stage(), { resource_id: volumeId, state: "awaiting_worker_staging" })
  assert.equal(updates.length, 2)
  assert.deepEqual(provider.writes.map(({ method, path }) => [method, path]), [["POST", "/persistentvolumeclaims"]])
  await writeFile(tokenFile, "z".repeat(43))
  assert.throws(() => bound.target(claim, action, {}), /Protected EKS launch inputs changed/)
})

test("EKS verifier derives the exact Ed25519 public key from a protected seed", async t => {
  const root = await mkdtemp(join(tmpdir(), "nabedra-eks-key-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  const file = join(root, "seed")
  const seed = Buffer.from("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60", "hex")
  await writeFile(file, seed, { mode: 0o600 })
  const clusterId = randomUUID()
  const options = { apiUrl: kubeUrl, clusterId, namespace, token: "fixture-verifier", ec2Client, signingSeedFile: file }
  assert.equal(new EksReplacementVerifier(options).publicKey,
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
  const link = join(root, "linked")
  await symlink(file, link)
  assert.throws(() => new EksReplacementVerifier({ ...options, signingSeedFile: link }), /private 32-byte regular file/)
  assert.throws(() => new EksReplacementVerifier({ ...options, ec2Client: { ...ec2Client, meta: {} } }), /pinned EC2 region/)
})

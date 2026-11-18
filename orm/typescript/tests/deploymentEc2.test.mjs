import assert from "node:assert/strict"
import { mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import { join } from "node:path"
import test from "node:test"
import { CloudLifecycleControl } from "../dist/deployment/cloudControl.js"
import { GrowthController } from "../dist/deployment/controller.js"
import { EbsLinuxHost } from "../dist/deployment/ebsHost.js"
import { Ec2Provider } from "../dist/deployment/ec2.js"
import { Ec2GrowthAdapter, Ec2PoolSlot, Ec2WorkerTemplate } from "../dist/deployment/liveEc2.js"
import { ProviderError } from "../dist/providers.js"

const clusterId = "00000000-0000-0000-0000-000000000001"
const adapterId = "00000000-0000-0000-0000-000000000002"
const workerId = "00000000-0000-0000-0000-000000000003"
const incarnation = "00000000-0000-0000-0000-000000000004"
const poolId = "00000000-0000-0000-0000-000000000005"
const claimId = "00000000-0000-0000-0000-000000000006"
const filesystemUuid = "00000000-0000-0000-0000-000000000007"
const drainId = "00000000-0000-0000-0000-000000000008"
const runId = "00000000-0000-0000-0000-000000000009"
const policyId = "00000000-0000-0000-0000-000000000010"
const volumeId = "vol-0123456789abcdef0"
const instanceId = "i-0123456789abcdef0"
const subnetId = "subnet-0123456789abcdef0"
const zone = "us-east-1a"
const token = "unit-token-" + "c".repeat(37)
const device = "/dev/sdf"
const gib = 2 ** 30
const worker = { worker_id: workerId, incarnation, enrollment_revision: 1, certificate_sha256: "a".repeat(64) }
const launchTemplate = { LaunchTemplateId: "lt-0123456789abcdef0", Version: "2" }
const kmsKeyId = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000008"
const templateData = {
  ImageId: "ami-0123456789abcdef0", InstanceType: "m6i.large", SecurityGroupIds: ["sg-test"],
  BlockDeviceMappings: [{ DeviceName: "/dev/sda1", Ebs: { Encrypted: true, DeleteOnTermination: true } }],
  MetadataOptions: { HttpTokens: "required", HttpEndpoint: "enabled" },
}
const stamp = () => performance.now() / 1000 + 10

function fixture(options = {}) {
  const calls = []
  const state = { volume: null, instance: null, modification: null, subnetPublic: false }
  const aws = {
    meta: { config: { connectTimeout: 0.01, readTimeout: 0.01,
      retries: { mode: "standard", totalMaxAttempts: 1 } }, regionName: "us-east-1" },
    describeVolumes: () => ({ Volumes: state.volume ? [state.volume] : [] }),
    describeInstances: () => ({ Reservations: [{ Instances: state.instance ? [state.instance] : [] }] }),
    describeVolumesModifications: () => ({ VolumesModifications: state.modification ? [state.modification] : [] }),
    createVolume(args) {
      state.volume ??= { VolumeId: volumeId, State: "available", Size: args.Size, AvailabilityZone: args.AvailabilityZone,
        KmsKeyId: args.KmsKeyId, VolumeType: args.VolumeType, Encrypted: args.Encrypted, MultiAttachEnabled: false,
        SnapshotId: "", Attachments: [], Tags: args.TagSpecifications[0].Tags }
      return { VolumeId: volumeId }
    },
    modifyVolume(args) {
      state.modification = { VolumeId: volumeId, TargetSize: args.Size, ModificationState: "modifying" }
      return {}
    },
    describeLaunchTemplateVersions: () => ({ LaunchTemplateVersions: [{ LaunchTemplateId: launchTemplate.LaunchTemplateId,
      VersionNumber: 2, LaunchTemplateData: templateData }] }),
    describeSubnets: () => ({ Subnets: [{ SubnetId: subnetId, AvailabilityZone: zone, State: "available",
      MapPublicIpOnLaunch: state.subnetPublic, AssignIpv6AddressOnCreation: false }] }),
    describeInstanceTypes: () => ({ InstanceTypes: [{ InstanceType: templateData.InstanceType, Hypervisor: "nitro",
      SupportedRootDeviceTypes: ["ebs"] }] }),
    runInstances(args) {
      state.instance ??= { InstanceId: instanceId, State: { Name: "running" }, Placement: args.Placement, SubnetId: args.SubnetId,
        ImageId: templateData.ImageId, InstanceType: templateData.InstanceType, RootDeviceName: "/dev/sda1", RootDeviceType: "ebs",
        SecurityGroups: [{ GroupId: "sg-test" }], ClientToken: args.ClientToken, BlockDeviceMappings: [
          { DeviceName: "/dev/sda1", Ebs: { VolumeId: "vol-root", DeleteOnTermination: true } },
        ], CpuOptions: { CoreCount: 2, ThreadsPerCore: 2 }, Tags: args.TagSpecifications[0].Tags }
      return { Instances: [{ InstanceId: instanceId }] }
    },
    attachVolume(args) {
      state.volume.State = "in-use"
      state.volume.Attachments = [{ InstanceId: instanceId, VolumeId: volumeId, Device: args.Device,
        DeleteOnTermination: false, State: "attached" }]
      state.instance.BlockDeviceMappings.push({ DeviceName: args.Device, Ebs: { VolumeId: volumeId,
        DeleteOnTermination: false, Status: "attached" } })
      return {}
    },
    stopInstances() {
      state.instance.State = { Name: "stopping" }
      return {}
    },
    detachVolume() {
      state.volume.State = "available"
      state.volume.Attachments = []
      return {}
    },
  }
  for (const [name, invoke] of Object.entries(aws)) {
    if (typeof invoke === "function") {
      aws[name] = args => {
        calls.push([name, structuredClone(args)])
        return structuredClone(invoke(args))
      }
    }
  }
  const provider = new Ec2Provider({ client: aws, clusterId, zones: [zone], launchTemplate,
    maxVolumeGib: 16, ...options })
  return { provider, calls, state }
}

async function control(t, respond) {
  const requests = []
  const server = createServer(async (req, res) => {
    try {
      assert.equal(req.url, "/v1/cluster/operations")
      assert.equal(req.headers.authorization, "Bearer " + token)
      const chunks = []
      for await (const chunk of req) chunks.push(chunk)
      const body = JSON.parse(Buffer.concat(chunks).toString())
      requests.push(body)
      const data = respond(body.operation, body.payload)
      res.writeHead(200, { "content-type": "application/json" })
      res.end(JSON.stringify({ request_id: body.request_id, data }))
    } catch (error) {
      res.writeHead(500)
      res.end(String(error))
    }
  })
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve))
  t.after(() => new Promise(resolve => server.close(resolve)))
  return { options: { apiUrl: "http://127.0.0.1:" + server.address().port, adminToken: token }, requests }
}

test("EC2 volume retries retain the same claim and remain pending until filesystem observation", async () => {
  const { provider, calls, state } = fixture()
  const args = { claimId, poolId, workerId, zone, bytes: BigInt(gib + 1), kmsKeyId, deadline: stamp() }
  const initial = await provider.ensurePool(args)
  const replayed = await provider.ensurePool(args)
  assert.deepEqual(initial, { resource_id: volumeId, state: "available" })
  assert.deepEqual(replayed, initial)
  const creates = calls.filter(([operation]) => operation === "createVolume")
  assert.equal(creates.length, 2)
  assert.deepEqual(creates[0], creates[1])
  assert.equal(creates[0][1].ClientToken, claimId)
  assert.equal(creates[0][1].Size, 2)
  assert.equal(creates[0][1].Encrypted, true)
  assert.equal(creates[0][1].TagSpecifications[0].Tags.find(tag => tag.Key === "nabedra:pool").Value, poolId)
  await assert.rejects(provider.ensurePool({ ...args, kmsKeyId: kmsKeyId + "-other" }), /immutable claim/)
  const expanded = await provider.expandPool({ volumeId, targetBytes: 3n * BigInt(gib), deadline: stamp() })
  assert.deepEqual(expanded, { resource_id: volumeId, state: "awaiting_filesystem", requested_gib: 3 })
  assert.deepEqual(await provider.expandPool({ volumeId, targetBytes: 3 * gib, deadline: stamp() }), expanded)
  await assert.rejects(provider.expandPool({ volumeId, targetBytes: 4 * gib, deadline: stamp() }), /unfinished growth/)
  assert.equal(calls.filter(([name]) => name === "modifyVolume").length, 1)
  const allCalls = calls.length
  await assert.rejects(provider.inspectVolume(volumeId, { deadline: performance.now() / 1000 }), /shorter/)
  assert.equal(calls.length, allCalls)
  state.volume.Tags = []
  await assert.rejects(provider.inspectVolume(volumeId), /ownership/)
})

test("A02 EC2 stop needs closed Control evidence and detach waits for exact-host unmount", async t => {
  const expected = { profile: "ebs", worker, generation: 1, provider_volume_id: volumeId, mount_path: "/pool",
    filesystem_uuid: filesystemUuid, identity: { pool_id: poolId, worker_id: workerId, cluster_id: clusterId,
      engine: "oltp", pool_format_version: 1 } }
  const mount = { expected, mount_id: 1, root_inode: 2, device_major: 3, device_minor: 4 }
  const drain = { id: drainId, growth_claim: claimId,
    intent: { worker, run_id: runId, native_target: { kind: "ec2", instance_id: instanceId } },
    phase: { closed: { mount, ready: { view: { run_id: runId }, ready_count: 2 } } } }
  const endpoint = await control(t, operation => {
    if (operation === "worker.stop_permit") return { record: drain, native_zone: zone }
    if (operation === "storage.pool.detach_permit") return { id: drainId, phase: "closed", mount, growth_claim: claimId }
    assert.fail("Unexpected operation " + operation)
  })
  const localHost = new EbsLinuxHost({ instanceId, mountPaths: ["/pool"] })
  let unmounted = false
  Object.defineProperty(localHost, "unmounted", { value: async (id, proof) => {
    assert.equal(id, instanceId)
    assert.deepEqual(proof, mount)
    return unmounted
  } })
  const { provider, calls } = fixture({ lifecycleAuthority: new CloudLifecycleControl(endpoint.options), localHost })
  await provider.ensureWorker({ claimId, workerId, incarnation, zone, subnetId, userData: "sealed", enrollment: worker })
  assert.equal(calls.find(([operation]) => operation === "runInstances")[1].UserData, Buffer.from("sealed").toString("base64"))
  await provider.ensurePool({ claimId, poolId, workerId, zone, bytes: gib, kmsKeyId })
  await provider.attachPool({ claimId, poolId, worker, volumeId, instanceId, device })

  await assert.rejects(provider.stopWorker({ claimId: drainId, instanceId }), /exact closed/)
  assert.equal(calls.filter(([name]) => name === "stopInstances").length, 0)
  drain.phase.closed.ready.ready_count = 3
  assert.deepEqual(await provider.stopWorker({ claimId: drainId, instanceId }), {
    resource_id: instanceId, state: "stopping", retained_volume_id: volumeId,
  })
  assert.deepEqual(calls.find(([name]) => name === "stopInstances")[1], {
    InstanceIds: [instanceId], Force: false, SkipOsShutdown: false, Hibernate: false,
  })
  assert.equal((await provider.detachPool({ claimId: drainId, volumeId, instanceId })).state, "awaiting_unmount")
  assert.equal(calls.filter(([name]) => name === "detachVolume").length, 0)
  unmounted = true
  assert.equal((await provider.detachPool({ claimId: drainId, volumeId, instanceId })).state, "detached")
  assert.equal(calls.find(([name]) => name === "detachVolume")[1].Force, false)
  assert.ok(endpoint.requests.every(request => request.request_id === drainId))
})

test("A02 staged growth requires protected enrollment and reuses the exact private disk and instance", async t => {
  const dir = mkdtempSync(join(realpathSync(tmpdir()), "nabedra-ec2-"))
  t.after(() => rmSync(dir, { recursive: true, force: true }))
  const enrollmentFile = join(dir, "enrollment.json")
  const adminTokenFile = join(dir, "admin.token")
  const adminUrl = "https://worker.example:8443"
  const enrollment = { ...worker, endpoint: "worker.example:9443", server_name: "worker.example" }
  writeFileSync(enrollmentFile, JSON.stringify(enrollment), { mode: 0o600 })
  writeFileSync(adminTokenFile, token + "\n", { mode: 0o600 })

  const { provider, calls, state } = fixture()
  const slot = new Ec2PoolSlot(poolId, device, "/data/pool", filesystemUuid, kmsKeyId)
  const bytes = gib - 1024
  const template = new Ec2WorkerTemplate(zone, subnetId, slot, enrollmentFile, adminUrl, adminTokenFile,
    "b".repeat(64), 1024, bytes, 12)
  const base = new Ec2GrowthAdapter({ provider, adapterId, instances: {}, templates: [template] })
  const action = { kind: "add_worker", zone, bytes }
  assert.equal(base.target(claimId, action, {}), null)
  const wrong = base.bindEndpoints({ [workerId]: new GrowthController({ apiUrl: adminUrl, adminToken: "b".repeat(43) }) })
  assert.equal(wrong.target(claimId, action, {}), null)
  const bound = base.bindEndpoints({ [workerId]: new GrowthController({ apiUrl: adminUrl, adminToken: token }) })
  const target = bound.target(claimId, action, {})
  assert.deepEqual(target.worker, worker)
  assert.equal(target.poolId, poolId)
  const policy = { policy_id: policyId, automatic: { adapter_id: adapterId, pool_increment_bytes: bytes, max_workers: 4 },
    zones: [zone], worker_cost: 10, volume_cost_per_gib: 2, monthly_spend_ceiling: 50, initial_monthly_spend: 0 }
  state.subnetPublic = true
  await assert.rejects(bound.observe({}, { policy, deadline: stamp() }), /private subnet/)
  state.subnetPublic = false
  assert.deepEqual([...(await bound.observe({}, { policy, deadline: stamp() })).workerZones], [zone])

  const intent = { claim_id: claimId, policy_id: policyId, action, worker, pool_id: poolId, pool_generation: 1,
    quoted_monthly_cost: 12, monthly_spend_ceiling: 50 }
  let authorizations = 0
  const opts = { authorize: () => { authorizations++ }, deadline: stamp() }
  assert.deepEqual(await bound.provisionWorker(intent, target, opts), { resource_id: volumeId, state: "attached" })
  assert.deepEqual(await bound.provisionWorker(intent, target, opts), { resource_id: volumeId, state: "attached" })
  assert.equal(authorizations, 3)
  assert.deepEqual(calls.filter(([operation]) => ["createVolume", "runInstances", "attachVolume"].includes(operation))
    .map(([operation]) => operation), ["createVolume", "runInstances", "attachVolume"])
  assert.equal(Object.hasOwn(calls.find(([operation]) => operation === "runInstances")[1], "UserData"), false)

  const record = { intent, phase: "awaiting_mount", provider_resource: volumeId }
  const pin = { ready: { ready_count: 3, view: { run_id: runId } }, monthly_spend: 12, enrollment_revision: 1 }
  let revision = 1
  const endpoint = await control(t, (operation, payload) => {
    if (operation === "storage.growth.snapshot") return { cluster_id: clusterId,
      pin: { ...pin, enrollment_revision: revision }, policy, active_automatic: true, active: record }
    assert.equal(operation, "storage.growth.prepare_worker")
    assert.equal(payload.claim_id, claimId)
    assert.equal(payload.mount.provider_volume_id, volumeId)
    assert.deepEqual(payload.native_target, { kind: "ec2", instance_id: instanceId })
    revision = 2
    return { identity: worker }
  })
  const handoff = await bound.registerWorker(intent, target, record, {
    controller: new GrowthController(endpoint.options), pin, deadline: stamp(),
  })
  assert.equal(handoff.state, "awaiting_bootstrap")
  assert.equal(handoff.stage.admin_url, adminUrl)
  assert.equal(handoff.stage.bootstrap_intent.enrollment.certificate_sha256, worker.certificate_sha256)
  assert.equal(JSON.stringify(handoff).includes(token), false)
  assert.deepEqual(endpoint.requests.map(request => request.operation), [
    "storage.growth.snapshot", "storage.growth.snapshot", "storage.growth.prepare_worker", "storage.growth.snapshot",
  ])
  writeFileSync(adminTokenFile, "b".repeat(43) + "\n")
  assert.throws(() => bound.target(claimId, action, {}), /Protected EC2 launch inputs changed/)
  assert.ok(calls.every(([name]) => ["describeVolumes", "describeInstances", "describeLaunchTemplateVersions", "describeSubnets",
    "describeInstanceTypes", "createVolume", "runInstances", "attachVolume"].includes(name)))
})

import assert from "node:assert/strict"
import { execFileSync } from "node:child_process"
import { createHash, randomUUID, X509Certificate } from "node:crypto"
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs"
import { createServer } from "node:http"
import { once } from "node:events"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { test } from "node:test"
import { ProviderError } from "../dist/providers.js"
import { GrowthController } from "../dist/deployment/controller.js"
import { DockerProvider, DockerWorker, UnsupportedVolumeExpansion } from "../dist/deployment/docker.js"
import { DockerGrowthAdapter, DockerWorkerTemplate } from "../dist/deployment/liveDocker.js"
import { DockerWorkerDrain } from "../dist/deployment/workerDrain.js"

const image = "sha256:" + "a".repeat(64)
const network = "isolated-worker-network"
const retainedId = "d".repeat(64)
const fixture = String.raw`#!/usr/bin/env node
const assert = require("node:assert/strict")
const fs = require("node:fs")
const file = process.env.NABEDRA_TEST_DOCKER_STATE
const [group, command, ...args] = process.argv.slice(2)
const data = JSON.parse(fs.readFileSync(file))
const result = value => console.log(JSON.stringify(value))
const option = name => args[args.indexOf(name) + 1]
const options = name => args.flatMap((value, index) => value === name ? [args[index + 1]] : [])
const name = args.at(-1)
const labels = () => Object.fromEntries(options("--label").map(item => {
  const separator = item.indexOf("=")
  return [item.slice(0, separator), item.slice(separator + 1)]
}))
data.calls.push([group, command])
try {
  if (group === "volume" && command === "ls") {
    for (const volume of Object.values(data.volumes)) {
      const filters = options("--filter")
      if (filters.every(filter => filter.startsWith("name=") ? volume.Name.includes(filter.slice(5))
        : filter.startsWith("label=") && Object.entries(volume.Labels).some(([key, value]) => filter.slice(6) === key + "=" + value))) {
        console.log(volume.Name)
      }
    }
  } else if (group === "volume" && command === "create") {
    assert(!data.volumes[name])
    data.volumes[name] = { Name: name, Labels: labels(), Driver: "local", Scope: "local", Options: null }
    console.log(name)
  } else if (group === "volume" && command === "inspect") {
    result(data.volumes[name] ? [data.volumes[name]] : [])
  } else if (group === "image" && command === "inspect") {
    result(args.includes("--format") ? "" : [{ Id: name }])
  } else if (group === "network" && command === "inspect") {
    result([{ Name: name, Driver: "bridge", Scope: "local" }])
  } else if (group === "run") {
    assert.equal(option("--entrypoint"), "python3")
    assert.equal(option("--network"), "none")
    const marker = JSON.parse(option("-c") === undefined ? "" : args[args.indexOf("-c") + 2])
    assert(data.volumes[marker.volume_name])
    assert.equal(marker.volume_marker, data.volumes[marker.volume_name].Labels["io.nabedra.pool"])
    if (name === "existing") assert(options("--mount").some(mount => mount.endsWith("target=/data,readonly")))
    result({ ...marker, observed_free_bytes: 3 * 2 ** 30 })
  } else if (group === "container" && command === "ls") {
    const filter = option("--filter")
    for (const worker of Object.values(data.workers)) {
      if (filter.startsWith("name=") ? new RegExp(filter.slice(5)).test(worker.Name)
        : Object.entries(worker.Config.Labels).some(([key, value]) => filter.slice(6) === key + "=" + value)) console.log(worker.Id)
    }
  } else if (group === "container" && command === "create") {
    const name = "/" + option("--name")
    const imageAt = args.indexOf("--entrypoint") + 2
    const mounts = options("--mount").map(item => {
      const [type, source, dest, readonly] = item.split(",")
      return { Type: type.slice(5), Name: source.slice(7), Source: source.slice(7), Destination: dest.slice(7), RW: readonly !== "readonly" }
    })
    const port = args.includes("--publish") ? option("--publish").split(":") : null
    data.workers[data.id] = {
      Id: data.id, Name: name, Image: args[imageAt], Path: option("--entrypoint"), Args: args.slice(imageAt + 1),
      Config: { Labels: labels() }, Mounts: mounts,
      HostConfig: { NanoCpus: Number(option("--cpus")) * 1e9, Memory: Number(option("--memory")),
        PidsLimit: Number(option("--pids-limit")), PortBindings: port ? { [port[2] + "/tcp"]: [{ HostIp: port[0], HostPort: port[1] }] } : {},
        RestartPolicy: { Name: "no" } },
      NetworkSettings: { Networks: { [option("--network")]: { Aliases: [option("--network-alias")] } } },
      State: { Status: "created", Running: false },
    }
    console.log(data.id)
  } else if (group === "container" && command === "inspect") {
    const worker = data.workers[name] ?? Object.values(data.workers).find(item => item.Name === "/" + name)
    result(worker ? [worker] : [])
  } else if (group === "container" && (command === "start" || command === "stop")) {
    const worker = data.workers[name]
    assert(worker)
    const running = command === "start"
    worker.State = { Status: running ? "running" : "exited", Running: running }
    console.log(worker.Id)
  } else {
    assert.fail("Unrecognized Docker fixture operation")
  }
} finally {
  fs.writeFileSync(file, JSON.stringify(data))
}
`

function dockerFixture(t) {
  const temp = mkdtempSync(join(realpathSync(tmpdir()), "nabedra-docker-ts-"))
  const path = join(temp, "state.json")
  const before = { PATH: process.env.PATH, NABEDRA_TEST_DOCKER_STATE: process.env.NABEDRA_TEST_DOCKER_STATE }
  const original = { calls: [], volumes: {}, workers: {}, id: retainedId }
  writeFileSync(path, JSON.stringify(original))
  writeFileSync(join(temp, "docker"), fixture, { mode: 0o700 })
  process.env.PATH = temp + ":" + process.env.PATH
  process.env.NABEDRA_TEST_DOCKER_STATE = path
  t.after(() => {
    for (const [key, value] of Object.entries(before)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
    rmSync(temp, { recursive: true, force: true })
  })
  const state = () => JSON.parse(readFileSync(path))
  const update = callback => {
    const data = state()
    callback(data)
    writeFileSync(path, JSON.stringify(data))
  }
  return { state, update, temp }
}

async function authority(t, adminToken, handler) {
  const calls = []
  const server = createServer(async (request, response) => {
    assert.equal(request.method, "POST")
    assert.equal(request.url, "/v1/cluster/operations")
    assert.equal(request.headers.authorization, "Bearer " + adminToken)
    const body = []
    for await (const chunk of request) body.push(chunk)
    const envelope = JSON.parse(Buffer.concat(body))
    calls.push(envelope)
    const data = handler(envelope)
    response.setHeader("Content-Type", "application/json")
    response.end(JSON.stringify({ request_id: envelope.request_id, data }))
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(() => new Promise(resolve => server.close(resolve)))
  return { apiUrl: "http://127.0.0.1:" + server.address().port, calls }
}

test("Docker pool provisioning retains volume identity and never relabels an existing owner", async t => {
  const { state, update, temp } = dockerFixture(t)
  const provider = new DockerProvider({ clusterId: randomUUID(), image, network })
  const claimId = randomUUID()
  const mountSeed = join(temp, "seed.bin")
  writeFileSync(mountSeed, Buffer.alloc(32, 15), { mode: 0o600 })
  const options = { claimId, poolId: randomUUID(), workerId: randomUUID(), zone: "zone-one", mountSeed, templateDigest: "b".repeat(64) }

  await assert.rejects(provider.ensurePool({ ...options, existingOnly: true }), /retained Docker volume is missing/)
  const pool = await provider.ensurePool(options)
  assert.equal(pool.resource_id, "docker-volume:" + provider.poolName(claimId))
  assert.equal(pool.volume_marker, options.poolId)
  assert.equal(pool.observed_free_bytes, 3 * 2 ** 30)
  assert.deepEqual(await provider.ensurePool({ ...options, existingOnly: true }), pool)
  assert.equal(state().calls.filter(([kind, action]) => kind === "volume" && action === "create").length, 1)
  await assert.rejects(provider.expandPool({ resourceId: pool.resource_id, targetBytes: 10n ** 12n }), UnsupportedVolumeExpansion)
  update(data => { data.volumes[pool.volume_name].Labels["io.nabedra.pool"] = randomUUID() })
  await assert.rejects(provider.ensurePool(options), /immutable claim differs/)
  assert.equal(state().calls.filter(([kind, action]) => kind === "volume" && action === "create").length, 1)
})

test("Docker launches once and a drain requires an exact retained container and mount", async t => {
  const { state, update, temp } = dockerFixture(t)
  const clusterId = randomUUID()
  const workerId = randomUUID()
  const incarnation = randomUUID()
  const claimId = randomUUID()
  const drainId = randomUUID()
  const runId = randomUUID()
  const certificate = "c".repeat(64)
  const mountSeed = join(temp, "seed.bin")
  writeFileSync(mountSeed, Buffer.alloc(32, 15), { mode: 0o600 })
  const provider = new DockerProvider({ clusterId, image, network })
  const poolId = randomUUID()
  const pool = await provider.ensurePool({ claimId, poolId, workerId, zone: "zone", mountSeed, templateDigest: "e".repeat(64) })
  const bootstrap = join(temp, "bootstrap.json")
  writeFileSync(bootstrap, JSON.stringify({ resources: { max_threads: 2, memory_limit_bytes: 2 ** 30 },
    node: { intent: { enrollment: { worker_id: workerId, incarnation, certificate_sha256: certificate, enrollment_revision: 1 } } } }),
  { mode: 0o600 })
  const worker = new DockerWorker(workerId, incarnation, bootstrap, "fixture-worker", 2_000, 2 ** 30,
    [[pool.volume_name, "/data"]], undefined, "zone", [19998, 19999])
  assert.equal(worker.validate(), bootstrap)
  assert.equal(await provider.ensureWorker({ claimId, worker, inspectOnly: true }), null)
  const launched = await provider.ensureWorker({ claimId, worker })
  assert.equal(launched.started, true)
  assert.equal(launched.state, "running")
  assert.equal((await provider.ensureWorker({ claimId, worker })).started, false)
  assert.equal(state().calls.filter(([kind, action]) => kind === "container" && action === "start").length, 1)

  const growth = new DockerGrowthAdapter({ provider, adapterId: randomUUID(), workers: { [workerId]: launched.resource_id },
    monthlySpendFloor: 1 })
  const enrolled = { worker_id: workerId, incarnation, certificate_sha256: certificate, enrollment_revision: 1 }
  const mount = { identity: { cluster_id: clusterId, worker_id: workerId, pool_id: poolId }, worker: enrolled,
    provider_volume_id: pool.resource_id, mount_path: "/data", profile: { docker: { volume_marker: poolId } } }
  const reports = { [workerId]: { inventory: { worker: enrolled, pools: [{ expected: mount }] } } }
  const survey = await growth.observe(reports, { policy: {}, deadline: performance.now() / 1_000 + 10 })
  assert.equal(survey.hosts[workerId].attachmentSlots, 0)
  assert.equal(survey.hosts[workerId].cpuLimitMillicores, 2_000)
  assert.equal(survey.retainedWorkers, 0)
  assert.equal(growth.target(claimId, { kind: "add_pool" }, reports), null)

  const record = { id: drainId, intent: { worker: enrolled, run_id: runId, docker_container_id: retainedId },
    phase: { closed: { mount: { expected: mount } } }, growth_claim: claimId }
  const adminToken = "control-test-token"
  const { apiUrl, calls } = await authority(t, adminToken, envelope => {
    if (envelope.operation === "worker.drain") assert.deepEqual(envelope.payload, record.intent)
    return envelope.operation === "worker.stop_permit" ? { record } : record
  })
  const drain = new DockerWorkerDrain({ provider, apiUrl, adminToken })
  assert.deepEqual(await drain.begin(drainId, { worker: enrolled, runId, resourceId: launched.resource_id }), record)
  assert.deepEqual(await drain.status(drainId), record)
  update(data => { data.workers[retainedId].Mounts.unshift({ Type: "volume", RW: true, Name: "extra", Destination: "/extra" }) })
  await assert.rejects(drain.stop(drainId), /no longer owns the retained mount/)
  assert.equal(state().calls.filter(([kind, action]) => kind === "container" && action === "stop").length, 0)
  update(data => { data.workers[retainedId].Mounts.shift() })
  const expected = { resource_id: launched.resource_id, state: "stopped", retained_volume_id: pool.resource_id }
  assert.deepEqual(await drain.stop(drainId), expected)
  assert.deepEqual(await drain.stop(drainId), expected)
  assert.equal(state().calls.filter(([kind, action]) => kind === "container" && action === "stop").length, 1)
  assert(state().volumes[pool.volume_name])
  assert.equal((await provider.ensureWorker({ claimId, worker })).state, "exited")
  assert.equal(state().calls.filter(([kind, action]) => kind === "container" && action === "start").length, 1)
  assert(calls.every(envelope => envelope.request_id === drainId))
})

test("Automatic Docker worker slots require exact credentials and a fresh Control pin", async t => {
  try {
    execFileSync("openssl", ["version"], { stdio: "ignore" })
  } catch {
    t.skip("OpenSSL unavailable")
    return
  }
  const { state, temp } = dockerFixture(t)
  const clusterId = randomUUID(), workerId = randomUUID(), incarnation = randomUUID(), poolId = randomUUID()
  const adapterId = randomUUID(), claimId = randomUUID(), runId = randomUUID()
  const provider = new DockerProvider({ clusterId, image, network })
  const key = join(temp, "worker.key")
  const certificate = join(temp, "worker.pem")
  execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=fixture-worker",
    "-keyout", key, "-out", certificate], { stdio: "ignore" })
  const certificateBytes = readFileSync(certificate)
  const authFile = join(temp, "auth.json")
  const seedFile = join(temp, "seed.bin")
  const templatePath = join(temp, "template.json")
  const adminToken = "a".repeat(43)
  writeFileSync(certificate, certificateBytes, { mode: 0o600 })
  const { chmodSync } = await import("node:fs")
  chmodSync(certificate, 0o600)
  writeFileSync(join(temp, "ca.pem"), certificateBytes, { mode: 0o600 })
  writeFileSync(authFile, JSON.stringify({ admin_token: adminToken, data_token: "b".repeat(43) }), { mode: 0o600 })
  writeFileSync(seedFile, Buffer.alloc(32, 15), { mode: 0o600 })
  const worker = { worker_id: workerId, incarnation,
    certificate_sha256: createHash("sha256").update(new X509Certificate(certificateBytes).raw).digest("hex"),
    enrollment_revision: 1 }
  const enrollment = { ...worker, endpoint: "fixture-worker:19001", server_name: "fixture-worker" }
  writeFileSync(templatePath, JSON.stringify({ version: 1, bind: "0.0.0.0:19002", auth_file: "/config/auth.json",
    execution: {}, resources: { max_threads: 2, memory_limit_bytes: 2 ** 30 },
    node: { intent: { enrollment }, bind: "0.0.0.0:19001", certificate: "/config/worker.pem",
      private_key: "/config/worker.key", authority_ca: "/config/ca.pem", scheduler_node_id: 1,
      control: { identity: { cluster_id: clusterId } }, control_enrollments: [] },
  }), { mode: 0o600 })
  const temporary = createServer().listen(0, "127.0.0.1")
  await once(temporary, "listening")
  const hostPort = temporary.address().port
  await new Promise(resolve => temporary.close(resolve))
  const adapter = new DockerGrowthAdapter({ provider, adapterId, workers: {}, monthlySpendFloor: 0,
    templates: [new DockerWorkerTemplate("zone", poolId, templatePath, seedFile, hostPort, 2_000, 2 ** 30, 2 ** 30, 1)] })
  const action = { kind: "add_worker", zone: "zone", bytes: 2 ** 30 }
  assert.equal(adapter.target(claimId, action, {}), null)
  const wrong = new GrowthController({ apiUrl: "http://127.0.0.1:" + hostPort, adminToken: "z".repeat(43) })
  assert.equal(adapter.bindEndpoints({ [workerId]: wrong }).target(claimId, action, {}), null)
  const endpoint = new GrowthController({ apiUrl: "http://127.0.0.1:" + hostPort, adminToken })
  const bound = adapter.bindEndpoints({ [workerId]: endpoint })
  const target = bound.target(claimId, action, {})
  assert.equal(target.poolId, poolId)
  const intent = { claim_id: claimId, action, worker, pool_id: poolId, pool_generation: 1,
    quoted_monthly_cost: 1, monthly_spend_ceiling: 2 }
  const deadline = performance.now() / 1_000 + 20
  const retained = await bound.provision(intent, target, { deadline })
  const record = { phase: "awaiting_mount", intent, provider_resource: retained.resource_id }
  const pin = { ready: { view: { run_id: runId } }, enrollment_revision: 1 }
  const policy = { automatic: { adapter_id: adapterId } }
  let prepared = false
  const { apiUrl, calls } = await authority(t, adminToken, ({ operation, payload }) => {
    if (operation === "storage.growth.prepare_worker") {
      assert.equal(payload.run_id, runId)
      assert.deepEqual(payload.enrollment, enrollment)
      assert.match(payload.mount_public_key, /^[a-f0-9]{64}$/)
      assert.equal(payload.mount.provider_volume_id, retained.resource_id)
      prepared = true
      return { identity: worker }
    }
    assert.equal(operation, "storage.growth.snapshot")
    return { pin: { ...pin, enrollment_revision: prepared ? 2 : 1 }, cluster_id: clusterId, active_automatic: true,
      active: record, policy }
  })
  const controller = new GrowthController({ apiUrl, adminToken })
  await assert.rejects(bound.registerWorker(intent, target, record, { controller, pin: { enrollment_revision: 0 }, deadline }),
    /does not authorize a Docker launch/)
  assert(!state().workers[retainedId])
  const started = await bound.registerWorker(intent, target, record, { controller, pin, deadline })
  assert.deepEqual(started, { state: "launched", container_id: "docker-container:" + retainedId })
  assert.equal(calls.filter(call => call.operation === "storage.growth.prepare_worker").length, 1)
  assert.equal(state().workers[retainedId].HostConfig.PortBindings["19002/tcp"][0].HostIp, "127.0.0.1")
  writeFileSync(seedFile, Buffer.alloc(32, 16))
  assert.throws(() => bound.target(claimId, action, {}), ProviderError)
})

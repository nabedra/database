import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { once } from "node:events"
import { createServer } from "node:http"
import { test } from "node:test"
import { UnknownOutcomeError } from "nabedra"
import { GrowthController } from "../dist/deployment/controller.js"
import { CloudLifecycleControl, closedWorker } from "../dist/deployment/cloudControl.js"
import { AutomaticGrowth } from "../dist/deployment/live.js"

async function controlFixture(t, route) {
  const server = createServer(async (request, response) => {
    assert.equal(request.url, "/v1/cluster/operations")
    assert.equal(request.headers.authorization, "Bearer admin")
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    const command = JSON.parse(Buffer.concat(chunks))
    const value = route(command)
    if (value === undefined) {
      request.socket.destroy()
      return
    }
    response.writeHead(200, { "content-type": "application/json" })
    response.end(JSON.stringify({ request_id: command.request_id, data: value }))
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(() => {
    server.closeAllConnections()
    return new Promise(resolve => server.close(resolve))
  })
  return { apiUrl: `http://127.0.0.1:${server.address().port}`, adminToken: "admin" }
}

test("growth reconciliation preserves a claim and does not repeat a settled provider callback", async t => {
  const intent = { claim_id: randomUUID(), action: { kind: "expand" }, cooldown_seconds: 0, max_attempts: 3 }
  const operations = new Map()
  let provisions = 0
  let phase = "claimed"
  const options = await controlFixture(t, command => {
    const { operation, request_id: requestId, payload } = command
    const ids = operations.get(operation) ?? new Set()
    ids.add(requestId)
    operations.set(operation, ids)
    assert.equal(ids.size, 1, `Identity changed for ${operation}`)
    const report = () => ({ intent, phase, attempts: phase === "claimed" ? 0 : 1 })
    switch (operation) {
      case "storage.growth.claim":
        assert.deepEqual(payload, { intent })
        return report()
      case "storage.growth.status":
        return report()
      case "storage.growth.attempt":
        phase = "provisioning"
        return report()
      case "storage.growth.provisioned":
        assert.equal(payload.resource_id, "fixture-volume")
        phase = "awaiting_mount"
        return undefined
      case "storage.growth.mounted":
        phase = "complete"
        return report()
      default:
        assert.fail(operation)
    }
  })
  const growth = new GrowthController(options)
  const reconcile = () => growth.reconcile({ intent, provision: record => {
    assert.deepEqual(record, intent)
    ++provisions
    return { resource_id: "fixture-volume" }
  } })
  await assert.rejects(reconcile(), UnknownOutcomeError)
  assert.equal((await reconcile()).phase, "complete")
  assert.equal((await reconcile()).phase, "complete")
  assert.equal(provisions, 1)
  assert.equal(operations.size, 5)
  assert.deepEqual(await new AutomaticGrowth({ controller: growth, workers: {}, adapter: null }).iterate(),
    { state: "unsupported", reason: "adapter_unavailable" })
})

test("closed worker permit requires the retained EBS mount and same-run quorum", async t => {
  const drainId = randomUUID()
  const runId = randomUUID()
  const clusterId = randomUUID()
  const worker = { worker_id: randomUUID(), incarnation: randomUUID(), enrollment_revision: 1,
    certificate_sha256: "a".repeat(64) }
  const nativeTarget = { kind: "ec2", instance_id: "i-12345678" }
  const expected = {
    profile: "ebs", worker, generation: 1, mount_path: "/storage/pool",
    provider_volume_id: "vol-a1234567", filesystem_uuid: randomUUID(),
    identity: { cluster_id: clusterId, engine: "oltp", pool_format_version: 1, worker_id: worker.worker_id, pool_id: randomUUID() },
  }
  const record = {
    id: drainId, growth_claim: randomUUID(), intent: { worker, run_id: runId, native_target: nativeTarget },
    phase: { closed: { ready: { ready_count: 3, view: { run_id: runId } },
      mount: { expected, mount_id: 1, root_inode: 2, device_major: 259, device_minor: 3 } } },
  }
  const lifecycle = new CloudLifecycleControl(await controlFixture(t, command => {
    assert.equal(command.request_id, drainId)
    if (command.operation === "worker.drain") {
      assert.deepEqual(command.payload, record.intent)
      return record
    }
    assert.equal(command.operation, "worker.stop_permit")
    return { record, native_zone: "test-1a" }
  }))
  assert.deepEqual(await lifecycle.beginWorkerDrain(drainId, { worker, runId, nativeTarget }), record)
  const permit = () => closedWorker(lifecycle, drainId, clusterId, "ec2", performance.now() / 1_000 + 1)
  assert.deepEqual(await permit(), [record, "test-1a"])
  record.phase.closed.ready.ready_count = 2
  await assert.rejects(permit(), { name: "ProviderError", message: "Control returned no exact closed cloud worker permit" })
  record.phase.closed.ready.ready_count = JSON.rawJSON("3.0")
  await assert.rejects(permit(), { name: "ProtocolError" })
  record.phase.closed.ready.ready_count = 3
  record.phase.closed.mount.expected.identity.cluster_id = randomUUID()
  await assert.rejects(permit(), { name: "ProviderError", message: "Control returned no exact closed cloud worker permit" })
})

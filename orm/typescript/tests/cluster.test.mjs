import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { once } from "node:events"
import { createServer } from "node:http"
import { test } from "node:test"
import { Cluster, ConflictError, KubernetesProvider, ProviderError, ProtocolError,
  TransportError, UnknownOutcomeError, ValidationError } from "nabedra"

async function startHttp(t, callback) {
  const server = createServer(async (request, response) => {
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    callback(request, response, JSON.parse(Buffer.concat(chunks)))
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  t.after(() => {
    server.closeAllConnections()
    return new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
  })
  return `http://127.0.0.1:${server.address().port}`
}

function state(revision, count) {
  return {
    version: 1, cluster_id: "e1f13e99-cff9-4d33-aa0e-3eeb299b835c", name: "test",
    revision, mode: "fixed", desired: count, min_nodes: count, max_nodes: count,
    capabilities: { provision: false, terminate: false, autoscale: false },
    controller: { epoch: 1, active: true, degraded: false },
    counts: { ready: count, starting: 0, draining: 0, lost: 0 }, intents: [], metrics_fresh: true,
    nodes: Array.from({ length: count }, () => ({
      node_id: randomUUID(), incarnation: randomUUID(), resource_id: null, ownership: "external", state: "ready",
      operation_slots: 4, partition_slots: 8, running_operations: 0, running_partitions: 0,
      queued_operations: 0, queued_partitions: 0, heartbeat_age_seconds: 1,
      resources: { max_threads: 8, memory_limit_bytes: 10_000, task_memory_limit_bytes: 5_000, control_memory_reserve_bytes: 2_000 },
      available_threads: 8, available_memory_bytes: 8_000,
    })),
  }
}

test("Cluster separates acceptance, convergence, rejected autoscale and unknown writes", async t => {
  let behavior = "normal"
  let scaleCalls = 0
  let scaleId
  const url = await startHttp(t, (request, response, envelope) => {
    assert.equal(request.url, "/v1/cluster/operations")
    assert.equal(request.headers.authorization, "Bearer admin-fixture")
    const { operation, request_id } = envelope
    const send = (status, fields) => {
      response.writeHead(status, { "content-type": "application/json" })
      response.end(JSON.stringify({ request_id, ...fields }))
    }
    if (operation === "cluster.scale") {
      assert.equal(envelope.payload.nodes, 2)
      ++scaleCalls
      scaleId = request_id
      if (behavior === "lost") {
        request.socket.destroy()
        return
      }
      send(200, { data: { revision: 1, mode: "fixed", desired: 2, min_nodes: 2, max_nodes: 2 } })
    } else if (operation === "cluster.autoscale") {
      assert.deepEqual(envelope.payload, { min_nodes: 1, max_nodes: 2 })
      send(409, { error: { code: "conflict", message: "external", outcome: "not_applied" } })
    } else {
      assert.equal(operation, "cluster.status")
      assert.deepEqual(envelope.payload, {})
      if (behavior === "timeout") return
      const status = state(behavior === "superseded" ? 2 : 1, 2)
      if (behavior === "invalid_status") status.nodes[0].available_memory_bytes = 8_001
      if (behavior === "float_integer") status.nodes[0].operation_slots = JSON.rawJSON("4.0")
      send(200, { data: status })
    }
  })
  const cluster = new Cluster(url, "admin-fixture", { timeout: 1 })
  const policy = await cluster.scale(2, { wait: true })
  assert.equal(policy.revision, 1)
  assert.equal(scaleCalls, 1)
  const status = await cluster.status()
  assert.equal(status.counts.ready, 2)
  const nodeIds = status.nodes.map(node => node.node_id)
  assert.deepEqual(nodeIds, nodeIds.toSorted())
  await assert.rejects(cluster.autoscale({ minNodes: 1, maxNodes: 2 }), {
    name: "ConflictError", code: "conflict", outcome: "not_applied",
  })
  await assert.rejects(cluster.autoscale({ minNodes: 3, maxNodes: 2 }), ValidationError)
  behavior = "superseded"
  await assert.rejects(cluster.scale(2, { wait: true }), error => error instanceof ConflictError
    && error.code === "policy_superseded" && error.requestId === scaleId && error.outcome === null)
  behavior = "invalid_status"
  await assert.rejects(cluster.status(), ProtocolError)
  behavior = "float_integer"
  await assert.rejects(cluster.status(), ProtocolError)
  behavior = "timeout"
  await assert.rejects(cluster.scale(2, { wait: true, timeout: 0.1 }), error => error instanceof TransportError
    && error.code === "wait_timeout" && error.requestId === scaleId)
  behavior = "lost"
  await assert.rejects(cluster.scale(2), error => error instanceof UnknownOutcomeError
    && error.requestId === scaleId)
  assert.equal(scaleCalls, 4)
})

test("Kubernetes callback preserves exact Pod UID and durable Secret ownership", async () => {
  const clusterId = randomUUID()
  const nodeId = randomUUID()
  const requestId = randomUUID()
  const resourceName = "nabedra-" + requestId
  const uid = randomUUID()
  const records = new Map()
  class Fixture extends KubernetesProvider {
    async request(method, path, body, options = {}) {
      const [resource, recordName] = path.split("?")[0].slice(1).split("/")
      if (method === "GET" && !recordName) {
        const selector = new URLSearchParams(path.split("?")[1]).get("labelSelector")
        const prefix = selector.split("/")[0]
        return { items: [...records].filter(([key, value]) =>
          key.startsWith("/secrets/") && value.metadata.labels[prefix + "/cluster"] === clusterId).map(([, value]) => value) }
      }
      if (method === "GET") {
        const record = records.get(path)
        if (!record && !options.missing) throw new Error("Fixture expected a present record")
        return record ?? null
      }
      if (method === "POST") {
        const key = path + "/" + body.metadata.name
        assert(!records.has(key))
        if (resource === "pods") {
          body.metadata.uid = uid
          body.status = { phase: "Running" }
        }
        records.set(key, body)
        return body
      }
      if (method === "PUT") {
        records.set(path, body)
        return body
      }
      assert.equal(method, "DELETE")
      assert.deepEqual(body.preconditions, { uid })
      records.delete(path)
      return {}
    }
  }
  const provider = new Fixture({
    apiUrl: "https://kube.example.test", token: "private-token",
    namespace: "development", clusterId, image: "test-image",
  })
  const launch = { cluster_id: clusterId, node_id: nodeId, request_id: requestId,
    bootstrap: { partition_slots: 4, listen: "0.0.0.0:9000" } }
  const { startNode, stopNode, listNodes } = provider
  const resourceId = await startNode(launch)
  assert.equal(resourceId, `development/${resourceName}/${uid}`)
  assert.equal(await startNode(launch), resourceId)
  assert.deepEqual(await listNodes(), [{ resource_id: resourceId, cluster_id: clusterId,
    node_id: nodeId, request_id: requestId, state: "running" }])
  const pod = records.get("/pods/" + resourceName)
  pod.metadata.uid = randomUUID()
  await assert.rejects(stopNode(resourceId), ProviderError)
  assert(records.has("/pods/" + resourceName))
  pod.metadata.uid = uid
  await stopNode(resourceId)
  assert(!records.has("/pods/" + resourceName))
  assert(records.has("/secrets/" + resourceName))
  assert.deepEqual((await listNodes()).map(node => node.state), ["terminated"])
  await assert.rejects(startNode(launch), ProviderError)
})

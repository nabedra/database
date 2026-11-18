import assert from "node:assert/strict"
import { execFileSync } from "node:child_process"
import { fileURLToPath } from "node:url"
import { test } from "node:test"
import * as orm from "nabedra"
import * as events from "nabedra/events"
import * as deployment from "nabedra/deployment"
import * as aws from "nabedra/aws"
import * as gateway from "nabedra/gateway"
import * as cli from "nabedra/cli"
import * as sink from "nabedra/events/sink"

const root = ["AuthenticationError", "AwsCredentials", "BatchStream", "Cluster", "ConflictError", "DB", "Database",
  "Expr", "Gateway", "GatewayContext", "HTTPResponse", "KubernetesProvider", "LimitExceededError",
  "NabedraError", "NotFoundError", "OverloadedError", "ProtocolError", "ProviderError", "Query",
  "RequestReceipt", "S3", "StorageError", "StreamAbandonedError", "StreamExpiredError", "StreamSnapshot",
  "StreamUnsupportedError", "Table", "TransportError", "UnknownOutcomeError", "ValidationError", "col", "serve"]

const eventValues = ["Batch", "Event", "Events", "EventsError", "EventsProtocolError", "EventsTransportError",
  "EventsUnknownOutcomeError", "Lease", "PublishRecord", "Subscription"]

function classMembers(prototype) {
  return Object.getOwnPropertyNames(prototype).filter(name => name !== "constructor").sort()
}

test("package has the Python ORM and Events exports with camelCase public members", () => {
  assert.deepEqual(Object.keys(orm).sort(), root)
  assert.deepEqual(Object.keys(events).sort(), eventValues)
  assert.deepEqual(Object.keys(deployment).sort(), [
    "AutomaticGrowth", "CloudLifecycleControl", "DockerGrowthAdapter", "DockerProvider", "DockerWorker",
    "EbsLinuxHost", "Ec2GrowthAdapter", "Ec2PoolSlot", "Ec2Provider", "EksGrowthAdapter", "EksProvider", "EksReplacementVerifier",
    "GrowthController", "GrowthTarget", "HostObservation", "ProviderObservation",
  ])
  assert.deepEqual(Object.keys(aws).sort(), ["AwsCredentials", "defaultCredentials"])
  assert.deepEqual(Object.keys(gateway).sort(), ["AwsCredentials", "Gateway", "GatewayContext", "HTTPResponse", "Headers", "Operation", "Receipt", "S3"])
  assert.deepEqual(Object.keys(cli).sort(), ["main"])
  assert.deepEqual(Object.keys(sink).sort(), ["main"])
  assert.equal(orm.DB, orm.Database)
  assert.equal(new orm.Database("http://localhost", "test", { mode: "olap" }).mode, "olap")
  assert.deepEqual(classMembers(orm.Database.prototype),
    ["createTable", "mode", "receipt", "submit", "table", "tables"])
  assert.deepEqual(classMembers(orm.Table.prototype),
    ["delete", "drop", "get", "insert", "insertMany", "query", "schema", "update", "upsert", "where"])
  assert.deepEqual(classMembers(orm.Query.prototype),
    ["collect", "count", "explain", "limit", "orderBy", "select", "stream", "streamSnapshot", "where"])
  assert.deepEqual(classMembers(orm.Expr.prototype),
    ["and", "eq", "ge", "gt", "le", "lt", "ne", "not", "or"])
  assert.deepEqual(classMembers(orm.Cluster.prototype), ["autoscale", "scale", "status"])
  assert.deepEqual(classMembers(orm.RequestReceipt.prototype), ["poll", "terminal", "wait"])
  assert.deepEqual(classMembers(orm.Gateway.prototype), ["aclose", "close", "handle", "handleAsync", "middleware"])
})

test("compares exports with the Python package", { skip: !process.env.NABEDRA_TEST_PYTHON }, () => {
  const source = fileURLToPath(new URL("../../python/src", import.meta.url))
  const script = [
    "import json, sys",
    "sys.path.insert(0, sys.argv[1])",
    "import nabedra, nabedra.deployment, nabedra.events",
    "print(json.dumps([nabedra.__all__, nabedra.events.__all__, nabedra.deployment.__all__]))",
  ].join("\n")
  const [rootNames, eventNames, deploymentNames] = JSON.parse(execFileSync(process.env.NABEDRA_TEST_PYTHON,
    ["-c", script, source], { encoding: "utf8", timeout: 10_000 }))
  const aliases = new Set(["Key", "Row", "Scalar", "Schema"])
  assert.deepEqual(rootNames.filter(name => !aliases.has(name)).sort(), Object.keys(orm).sort())
  assert.deepEqual(eventNames.sort(), Object.keys(events).sort())
  assert.deepEqual(deploymentNames.filter(name => name !== "StatefulGrowthAdapter").sort(), Object.keys(deployment).sort())
})

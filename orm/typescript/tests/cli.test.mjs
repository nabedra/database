import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdtemp, readFile, stat, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import { test } from "node:test"
import { promisify } from "node:util"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../dist/cli.js", import.meta.url))

test("CLI preserves grouped commands, version output and protected init", async t => {
  const directory = await mkdtemp(join(tmpdir(), "nabedra-typescript-cli-"))
  t.after(() => rm(directory, { force: true, recursive: true }))
  const binary = join(directory, "nabedra-db")
  await writeFile(binary, "#!/bin/sh\nprintf 'nabedra-db 0.1.1 oltp\\n'\n", { mode: 0o700 })
  const options = { cwd: directory, env: { ...process.env, NABEDRA_SERVER_BINARY: binary } }
  const launch = async (...args) => run(process.execPath, [cli, ...args], options)
  for (const flags of [["--version", "--json"], ["database", "--version", "--json"]]) {
    assert.deepEqual(JSON.parse((await launch(...flags)).stdout),
      { cli: "0.2.3", binary: "0.1.1", config: 2, protocol: 1 })
  }
  for (const command of ["init", "serve", "doctor", "worker", "cluster", "migrate"]) {
    assert.match((await launch("database", command, "--help")).stdout, new RegExp("database " + command))
  }
  await assert.rejects(launch("init"), { code: 2 })
  const args = ["database", "init", "--mode", "fixed", "--storage", "s3://fixture-bucket/checks/",
    "--region", "us-east-1", "--non-interactive", "--threads-per-worker", "3"]
  const { config, auth_file: auth } = JSON.parse((await launch(...args)).stdout)
  const configBytes = await readFile(config)
  assert.equal(JSON.parse(configBytes).worker.resources.max_threads, 3)
  assert.notEqual(JSON.parse(configBytes).auth_file, "")
  const credentials = Object.values(JSON.parse(await readFile(auth)))
  assert.equal(new Set(credentials).size, 3)
  assert(credentials.every(token => token.length === 43))
  for (const path of [config, auth]) assert.equal((await stat(path)).mode & 0o777, 0o600)
  await assert.rejects(launch(...args), { code: 2 })
  assert.deepEqual(await readFile(config), configBytes)
  const doctor = await launch("database", "--json", "doctor", "--config", config)
  assert.equal(doctor.stdout.trim(), "nabedra-db 0.1.1 oltp")
})

test("D01/T9 OLTP config accepts the direct_oltp switch", async () => {
  // T9 finding: `direct_oltp` (plan "Rollout") must reach the server through the CLI.
  const { validate } = await import(new URL("../dist/cliConfig.js", import.meta.url).href)
  const execution = {
    max_workers_per_request: 1, max_pending_requests: 8, max_queue_bytes_per_replica: 1 << 20,
    max_retained_result_bytes_per_worker: 1 << 20, result_retention_ms: 1000,
    queue_replication: "all_ready_workers", queue_lifetime: "live_cluster", partition_size: "64 MiB",
  }
  const local = {
    version: 1, engine: "oltp", bind: "127.0.0.1:0", database: "application", auth_file: "auth.json",
    topology: { mode: "local", data_dir: "volume" }, worker: { execution: { ...execution, direct_oltp: false } },
  }
  validate(local)
  const peer = {
    worker_id: crypto.randomUUID(), incarnation: crypto.randomUUID(), certificate_sha256: "a".repeat(64),
    endpoint: "127.0.0.1:9000", server_name: "localhost", enrollment_revision: 1,
  }
  const remote = {
    version: 1, engine: "oltp", bind: "127.0.0.1:0", database: "application", auth_file: "auth.json",
    topology: {
      mode: "remote", cluster_id: crypto.randomUUID(), control_group_id: crypto.randomUUID(),
      enrollment: { database: "application", enrollment: peer },
      worker_tls: { cert_file: "c.pem", key_file: "k.pem", ca_file: "ca.pem" },
      control_peers: [peer], direct_oltp: false,
    },
  }
  validate(remote)
  assert.throws(() => validate({ ...remote, topology: { ...remote.topology, direct_oltp: "false" } }))
  assert.throws(() => validate({ ...remote, worker: { execution: { ...execution, direct_oltp: 0 } } }))
  assert.throws(() => validate({ ...local, topology: { ...local.topology, direct_oltp: false } }))
})

import { createPrivateKey, createPublicKey, X509Certificate } from "node:crypto"
import { closeSync, constants, fsyncSync, linkSync, mkdtempSync, openSync, rmSync, statSync, writeFileSync } from "node:fs"
import { createServer } from "node:net"
import { basename, dirname, join } from "node:path"
import { createSecureContext } from "node:tls"
import { isDeepStrictEqual as same } from "node:util"
import { ProviderError } from "../providers.js"
import { hasFields, isJsonObject, parseJson, type JsonObject } from "../transport.js"
import { growthMatchesCredentials, growthOperation, growthRead, type GrowthController } from "./controller.js"
import { boundedInteger, dockerLabels, dockerRecords, DockerWorker, prefix, privateBytes, sha256, unsignedInteger,
  type DockerMount, type DockerPoolResult, type DockerProvider, type DockerWorkerResult } from "./docker.js"
import { GrowthTarget, HostObservation, ProviderObservation, type StatefulGrowthAdapter } from "./growthTypes.js"
import { canonicalJson, digest, identity, requireRecord } from "./shared.js"

const gib = 1n << 30n
const ed25519Pkcs8 = Buffer.from("302e020100300506032b657004220420", "hex")
const protectedTarget = /^\/config\/[A-Za-z0-9][A-Za-z0-9_.-]{0,100}(?![\s\S])/
const plainToken = /^[!-~]{43,4096}(?![\s\S])/

export class DockerWorkerTemplate {
  constructor(
    readonly zone: string,
    readonly poolId: string,
    readonly bootstrapFile: string,
    readonly mountSeedFile: string,
    readonly hostPort: number,
    readonly cpuMillicores: number,
    readonly memoryBytes: number,
    readonly maxRequestBytes: number | bigint,
    readonly monthlyCost: number | bigint,
  ) { Object.freeze(this) }
}

interface LaunchInputs {
  bootstrap: JsonObject
  worker: JsonObject
  alias: string
  httpPort: number
  adminToken: string
  publicKey: string
  mounts: DockerMount[]
  fingerprint: string
}

export interface DockerGrowthOptions {
  provider: DockerProvider
  adapterId: string
  workers: Readonly<Record<string, string>>
  monthlySpendFloor: number | bigint
  templates?: readonly DockerWorkerTemplate[]
}

type Inventories = Readonly<Record<string, JsonObject>>
type Endpoints = Readonly<Record<string, GrowthController>>
type Registration = { controller: GrowthController; pin: JsonObject | null; deadline: number }

function launchInputs(template: DockerWorkerTemplate): LaunchInputs {
  const path = template.bootstrapFile
  try {
    const bytes = privateBytes(path, 65_536)
    if (statSync(dirname(path)).mode & 0o077) throw new Error()
    const bootstrap = parseJson(bytes)
    if (!hasFields(bootstrap, "version", "bind", "auth_file", "node", "resources", "execution") || bootstrap.version !== 1) {
      throw new Error()
    }
    const node = bootstrap.node
    if (!hasFields(node, "intent", "bind", "certificate", "private_key", "authority_ca", "scheduler_node_id",
      "control", "control_enrollments") || !hasFields(node.intent, "enrollment") || !unsignedInteger(node.scheduler_node_id, 1n)) {
      throw new Error()
    }
    const enrollment = requireRecord(node.intent.enrollment)
    const worker = {
      worker_id: identity(enrollment.worker_id),
      incarnation: identity(enrollment.incarnation),
      certificate_sha256: enrollment.certificate_sha256,
      enrollment_revision: enrollment.enrollment_revision,
    }
    identity(template.poolId)
    identity(requireRecord(requireRecord(node.control).identity).cluster_id)
    const endpoint = typeof enrollment.endpoint === "string"
      ? /^([a-z0-9][a-z0-9-]{0,62}):([1-9][0-9]{0,4})(?![\s\S])/.exec(enrollment.endpoint)
      : null
    const listener = typeof bootstrap.bind === "string"
      ? /^0\.0\.0\.0:([1-9][0-9]{0,4})(?![\s\S])/.exec(bootstrap.bind) : null
    if (!endpoint || !listener || endpoint[1] !== enrollment.server_name || Number(endpoint[2]) > 65_535
      || Number(listener[1]) > 65_535 || listener[1] === endpoint[2] || node.bind !== "0.0.0.0:" + endpoint[2]
      || typeof worker.certificate_sha256 !== "string" || !/^[a-f0-9]{64}(?![\s\S])/.test(worker.certificate_sha256)
      || worker.enrollment_revision !== 1) throw new Error()
    const resources = requireRecord(bootstrap.resources)
    if (!boundedInteger(resources.max_threads, 1, Math.floor(template.cpuMillicores / 1_000))
      || !boundedInteger(resources.memory_limit_bytes, 1, template.memoryBytes)) {
      throw new Error()
    }
    const destinations = [bootstrap.auth_file, node.certificate, node.private_key, node.authority_ca]
    if (new Set(destinations).size !== 4 || !destinations.every((destination): destination is string =>
      typeof destination === "string" && protectedTarget.test(destination))) throw new Error()
    const mounts = destinations.map(destination => [join(dirname(path), basename(destination)), destination] as const)
    if (mounts.some(([source]) => source === path || source === template.mountSeedFile)) throw new Error()
    const [authBytes, certificate, key, ca] = mounts.map(([source], index) => privateBytes(source, index === 0 ? 16_384 : 65_536))
    const auth = parseJson(authBytes)
    if (!hasFields(auth, "admin_token", "data_token") || typeof auth.admin_token !== "string" || typeof auth.data_token !== "string"
      || !plainToken.test(auth.admin_token) || !plainToken.test(auth.data_token) || auth.admin_token === auth.data_token) {
      throw new Error()
    }
    if (sha256(new X509Certificate(certificate).raw) !== worker.certificate_sha256) throw new Error()
    const caCertificates = ca.toString("ascii").match(/-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g)
    if (!caCertificates?.length) throw new Error()
    for (const caCertificate of caCertificates) new X509Certificate(caCertificate)
    createSecureContext({ cert: certificate, key, ca, passphrase: "" })
    const seed = privateBytes(template.mountSeedFile, 32)
    if (seed.length !== 32) throw new Error()
    const privateKey = createPrivateKey({ key: Buffer.concat([ed25519Pkcs8, seed]), type: "pkcs8", format: "der" })
    const publicKey = createPublicKey(privateKey).export({ type: "spki", format: "der" }).subarray(-32).toString("hex")
    const templateWire = {
      zone: template.zone, pool_id: template.poolId, bootstrap_file: path, mount_seed_file: template.mountSeedFile,
      host_port: template.hostPort, cpu_millicores: template.cpuMillicores, memory_bytes: template.memoryBytes,
      max_request_bytes: template.maxRequestBytes, monthly_cost: template.monthlyCost,
    }
    const fingerprint = digest([templateWire, ...[bytes, authBytes, certificate, key, ca, seed].map(sha256)])
    return { bootstrap, worker, alias: endpoint[1], httpPort: Number(listener[1]), adminToken: auth.admin_token,
      publicKey, mounts, fingerprint }
  } catch {
    throw new ProviderError("Docker data worker template has invalid private launch configuration")
  }
}

function writeBootstrap(template: DockerWorkerTemplate, claimId: string, bootstrap: JsonObject): string {
  const parent = dirname(template.bootstrapFile)
  const destination = join(parent, "launch-" + identity(claimId) + ".json")
  const body = Buffer.from(canonicalJson(bootstrap))
  if (body.length > 65_536) throw new ProviderError("Docker launch bootstrap exceeds the worker limit")

  let directory: string | undefined
  try {
    directory = mkdtempSync(join(parent, ".nabedra-launch-"))
    const temporary = join(directory, "bootstrap.json")
    const file = openSync(temporary, "wx", 0o600)
    try {
      writeFileSync(file, body)
      fsyncSync(file)
    } finally {
      closeSync(file)
    }
    try {
      linkSync(temporary, destination)
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error
    }
  } catch {
    throw new ProviderError("Cannot retain the private Docker launch bootstrap")
  } finally {
    if (directory) rmSync(directory, { force: true, recursive: true })
  }
  if (!privateBytes(destination, 65_536).equals(body)) {
    throw new ProviderError("Docker launch bootstrap differs from the retained claim")
  }
  const fd = openSync(parent, constants.O_RDONLY | constants.O_DIRECTORY)
  try {
    fsyncSync(fd)
  } finally {
    closeSync(fd)
  }
  return destination
}

function portAvailable(port: number): Promise<boolean> {
  return new Promise(resolve => {
    const listener = createServer()
    listener.once("error", () => resolve(false))
    listener.listen({ host: "127.0.0.1", port, exclusive: true }, () => listener.close(error => resolve(!error)))
  })
}

function price(policy: JsonObject, size: number | bigint): bigint {
  if (!unsignedInteger(policy.worker_cost) || !unsignedInteger(policy.volume_cost_per_gib)) {
    throw new ProviderError("Invalid Docker growth cost policy")
  }
  return BigInt(policy.worker_cost) + ((BigInt(size) + gib - 1n) / gib) * BigInt(policy.volume_cost_per_gib)
}

export class DockerGrowthAdapter implements StatefulGrowthAdapter {
  readonly provider: DockerProvider
  readonly clusterId: string
  readonly adapterId: string
  readonly workers: Readonly<Record<string, string>>
  readonly monthlySpendFloor: number | bigint
  #slots = new Map<string, DockerWorkerTemplate>()
  #workersBySlot = new Map<string, DockerWorkerTemplate>()
  #fingerprints = new Map<string, string>()
  #endpoints = new Map<string, GrowthController>()

  constructor({ provider, adapterId, workers, monthlySpendFloor, templates = [] }: DockerGrowthOptions) {
    this.provider = provider
    this.clusterId = identity(provider.clusterId)
    this.adapterId = identity(adapterId)
    this.workers = Object.freeze(Object.fromEntries(Object.entries(workers).map(([key, resource]) => [identity(key), resource])))
    if (!unsignedInteger(monthlySpendFloor)) throw new ProviderError("Explicit retained Docker resource costs are required")
    this.monthlySpendFloor = monthlySpendFloor
    const poolIds = new Set<string>()
    const aliases = new Set<string>()
    const ports = new Set<number>()
    const schedulerIds = new Set<unknown>()
    for (const template of templates) {
      if (!(template instanceof DockerWorkerTemplate) || this.#slots.size >= 16 || typeof template.zone !== "string"
        || !template.zone || [...template.zone].length > 128 || this.#slots.has(template.zone)
        || !boundedInteger(template.cpuMillicores, 1, 64_000) || !boundedInteger(template.memoryBytes, 64 * 2 ** 20, 2 ** 40)
        || !boundedInteger(template.hostPort, 1, 65_535) || !unsignedInteger(template.maxRequestBytes, 1n)
        || !unsignedInteger(template.monthlyCost)
        || [template.bootstrapFile, template.mountSeedFile].some(path => typeof path !== "string" || /[,\0]/.test(path))
        || !/^sha256:[a-f0-9]{64}(?![\s\S])/.test(provider.image)) throw new ProviderError("Invalid bounded Docker data worker template")
      const inputs = launchInputs(template)
      const workerId = identity(inputs.worker.worker_id)
      const node = requireRecord(inputs.bootstrap.node)
      const schedulerId = node.scheduler_node_id
      const controlId = requireRecord(requireRecord(node.control).identity).cluster_id
      if (Object.hasOwn(this.workers, workerId) || this.#workersBySlot.has(workerId) || poolIds.has(template.poolId)
        || aliases.has(inputs.alias) || ports.has(template.hostPort) || schedulerIds.has(schedulerId) || controlId !== this.clusterId) {
        throw new ProviderError("Docker launch template reuses an identity, endpoint or slot")
      }
      this.#slots.set(template.zone, template)
      this.#workersBySlot.set(workerId, template)
      this.#fingerprints.set(template.zone, this.#fingerprint(inputs))
      poolIds.add(template.poolId)
      aliases.add(inputs.alias)
      ports.add(template.hostPort)
      schedulerIds.add(schedulerId)
    }
  }

  #fingerprint(inputs: LaunchInputs): string {
    return digest([this.clusterId, this.adapterId, this.provider.image, this.provider.network, inputs.fingerprint])
  }

  #inputs(template: DockerWorkerTemplate): LaunchInputs {
    const inputs = launchInputs(template)
    if (this.#fingerprint(inputs) !== this.#fingerprints.get(template.zone)) {
      throw new ProviderError("Protected Docker launch template changed")
    }
    return inputs
  }

  bindEndpoints(endpoints: Endpoints): DockerGrowthAdapter {
    const bound = new DockerGrowthAdapter({
      provider: this.provider, adapterId: this.adapterId, workers: this.workers, monthlySpendFloor: this.monthlySpendFloor,
    })
    bound.#slots = this.#slots
    bound.#workersBySlot = this.#workersBySlot
    bound.#fingerprints = this.#fingerprints
    for (const [workerId, template] of this.#workersBySlot) {
      const endpoint = endpoints[workerId]
      if (!endpoint) continue
      const token = this.#inputs(template).adminToken
      if (growthMatchesCredentials(endpoint, "http://127.0.0.1:" + template.hostPort, token)) {
        bound.#endpoints.set(workerId, endpoint)
      }
    }
    return bound
  }

  async observe(inventories: Inventories, { policy, deadline, claimId = null }: {
    policy: JsonObject; deadline: number; claimId?: string | null
  }): Promise<ProviderObservation> {
    const hosts: Record<string, HostObservation> = {}
    const observedIds = new Set<string>()
    const retained = await this.provider.workerIds({ deadline })
    const launchRecords = new Map<string, JsonObject>()
    if (this.#slots.size) {
      const staticIds = new Set(Object.values(this.workers).map(id => id.startsWith("docker-container:") ? id.slice(17) : id))
      for (const id of retained) {
        if (staticIds.has(id)) continue
        const record = await this.provider.inspectWorker("docker-container:" + id, { deadline })
        const workerId = dockerLabels(record)[prefix + "worker"]
        if (typeof workerId === "string" && this.#workersBySlot.has(workerId)) {
          if (launchRecords.has(workerId)) throw new ProviderError("Several Docker containers use one launch slot")
          launchRecords.set(workerId, record)
        }
      }
    }
    for (const [workerId, sample] of Object.entries(inventories)) {
      const inventory = requireRecord(sample.inventory)
      const worker = requireRecord(inventory.worker)
      let record: JsonObject
      if (Object.hasOwn(this.workers, workerId)) {
        record = await this.provider.inspectWorker(this.workers[workerId], { deadline })
      } else {
        const launched = launchRecords.get(workerId)
        const template = this.#workersBySlot.get(workerId)
        if (!launched || !template) throw new ProviderError("Ready worker has no exact Docker container binding")
        record = launched
        this.#checkLaunchedBinding(template, worker, record, inventory)
      }
      const labels = dockerLabels(record)
      const { Id: id } = record
      const zone = labels[prefix + "zone"]
      if (typeof id !== "string" || !retained.has(id) || observedIds.has(id)
        || requireRecord(record.State).Running !== true || typeof zone !== "string" || !zone
        || labels[prefix + "worker"] !== workerId || labels[prefix + "incarnation"] !== worker.incarnation) {
        throw new ProviderError("Docker provider inventory differs from the current worker")
      }
      const mounts = dockerRecords(record.Mounts ?? []).filter(mount => mount.Type === "volume" && mount.RW === true)
      for (const row of dockerRecords(inventory.pools)) {
        const expected = requireRecord(row.expected)
        const resource = expected.provider_volume_id
        if (typeof resource !== "string" || !resource.startsWith("docker-volume:") || !mounts.some(mount =>
          mount.Name === resource.slice(14) && mount.Destination === expected.mount_path)) {
          throw new ProviderError("Docker container no longer mounts the exact observed volume")
        }
      }
      const { NanoCpus: cpu, Memory: memory } = requireRecord(record.HostConfig)
      if (!boundedInteger(cpu, 1_000_000, Number.MAX_SAFE_INTEGER) || !unsignedInteger(memory, 1n)) {
        throw new ProviderError("Docker worker requires finite enforced CPU and memory")
      }
      hosts[workerId] = new HostObservation(worker, zone, 0, new Set(), Math.floor(cpu / 1_000_000), memory)
      observedIds.add(id)
    }
    const retainedCount = retained.size - observedIds.size
    if (claimId === null && Object.values(inventories).some(sample => Object.hasOwn(sample, "split_deficit"))) {
      return new ProviderObservation(hosts, retainedCount, this.monthlySpendFloor)
    }
    const zones = new Set<string>()
    for (const [workerId, template] of this.#workersBySlot) {
      if (!this.#endpoints.has(workerId) || Object.hasOwn(inventories, workerId) || launchRecords.has(workerId)
        || !Array.isArray(policy.zones) || !policy.zones.includes(template.zone)) continue
      const size = requireRecord(policy.automatic).pool_increment_bytes
      if (!unsignedInteger(size) || size > template.maxRequestBytes || price(policy, size) < template.monthlyCost
        || !await portAvailable(template.hostPort)) continue
      this.#inputs(template)
      const volumes = await this.provider.workerPoolNames(workerId, { deadline })
      if (volumes.size && (claimId === null || volumes.size !== 1 || !volumes.has(this.provider.poolName(claimId)))) continue
      zones.add(template.zone)
    }
    if (zones.size) await this.provider.inspectLaunchEnvironment({ deadline })
    return new ProviderObservation(hosts, retainedCount, this.monthlySpendFloor, zones)
  }

  #checkLaunchedBinding(template: DockerWorkerTemplate, worker: JsonObject, record: JsonObject, inventory: JsonObject): void {
    const inputs = this.#inputs(template)
    const labels = dockerLabels(record)
    const claimId = identity(labels[prefix + "claim"])
    const primary = dockerRecords(inventory.pools).filter(pool => pool.primary)
    const configured = requireRecord(record.HostConfig)
    const networks = requireRecord(requireRecord(record.NetworkSettings).Networks)
    const network = requireRecord(networks[this.provider.network])
    if (!same(worker, inputs.worker) || labels[prefix + "zone"] !== template.zone
      || labels[prefix + "certificate"] !== worker.certificate_sha256
      || labels[prefix + "enrollment-revision"] !== String(worker.enrollment_revision)
      || record.Name !== "/nabedra-worker-" + claimId || record.Image !== this.provider.image
      || configured.NanoCpus !== template.cpuMillicores * 1_000_000 || configured.Memory !== template.memoryBytes
      || primary.length !== 1 || primary[0].pool_id !== template.poolId || primary[0].growth_claim !== claimId
      || requireRecord(primary[0].expected).provider_volume_id !== "docker-volume:" + this.provider.poolName(claimId)
      || !Array.isArray(network.Aliases) || !network.Aliases.includes(inputs.alias)) {
      throw new ProviderError("Docker data worker does not match the enrolled launch slot")
    }
  }

  target(claimId: string, action: JsonObject, inventories: Inventories): GrowthTarget | null {
    const template = typeof action.zone === "string" ? this.#slots.get(action.zone) : undefined
    if (action.kind !== "add_worker" || !template || !unsignedInteger(action.bytes, 1n) || action.bytes > template.maxRequestBytes) {
      return null
    }
    const worker = this.#inputs(template).worker
    const workerId = identity(worker.worker_id)
    if (Object.hasOwn(inventories, workerId) || !this.#endpoints.has(workerId)) return null
    return new GrowthTarget(worker, template.poolId, 1, "docker-volume:" + this.provider.poolName(claimId))
  }

  async #volume(intent: JsonObject, target: GrowthTarget, existingOnly: boolean, deadline: number):
    Promise<[DockerWorkerTemplate, DockerPoolResult]> {
    const action = requireRecord(intent.action)
    const template = typeof action.zone === "string" ? this.#slots.get(action.zone) : undefined
    const claimId = identity(intent.claim_id)
    const cost = intent.quoted_monthly_cost
    const ceiling = intent.monthly_spend_ceiling
    if (!template || !same(this.target(claimId, action, {}), target) || !same(intent.worker, target.worker)
      || intent.pool_id !== target.poolId || intent.pool_generation !== target.poolGeneration
      || !unsignedInteger(cost) || !unsignedInteger(ceiling) || cost < template.monthlyCost || cost > ceiling) {
      throw new ProviderError("Docker launch template differs from the exact priced claim")
    }
    const observed = await this.provider.ensurePool({
      claimId, poolId: target.poolId, workerId: identity(target.worker.worker_id), zone: template.zone,
      mountSeed: template.mountSeedFile, templateDigest: this.#fingerprints.get(template.zone), existingOnly, deadline,
    })
    if (observed.resource_id !== target.resourceId || observed.volume_marker !== target.poolId
      || !unsignedInteger(observed.observed_free_bytes) || !unsignedInteger(action.bytes) || observed.observed_free_bytes < action.bytes) {
      throw new ProviderError("Docker retained volume identity or shared filesystem headroom differs")
    }
    return [template, observed]
  }

  async provision(intent: JsonObject, target: GrowthTarget, { deadline }: { deadline: number }): Promise<JsonObject> {
    const [, observed] = await this.#volume(intent, target, false, deadline)
    return observed
  }

  register(intent: JsonObject, target: GrowthTarget, record: JsonObject,
    options: { controller: GrowthController; deadline: number }): never {
    throw new ProviderError("Live Docker filesystem provisioning is unavailable")
  }

  async registerWorker(intent: JsonObject, target: GrowthTarget, record: JsonObject,
    { controller, pin, deadline }: Registration): Promise<JsonObject> {
    const claimId = identity(intent.claim_id)
    const before = await growthRead(controller, "storage.growth.snapshot", {}, { deadline })
    const beforePin = requireRecord(before.pin)
    const automatic = requireRecord(before.policy).automatic
    if (!same(before.pin, pin) || before.cluster_id !== this.clusterId || !before.active_automatic
      || !isJsonObject(automatic) || automatic.adapter_id !== this.adapterId || !same(before.active, record)
      || !same(record.intent, intent) || record.phase !== "awaiting_mount" || record.provider_resource !== target.resourceId) {
      throw new ProviderError("The current Control claim does not authorize a Docker launch")
    }
    const [template, volume] = await this.#volume(intent, target, true, deadline)
    const { bootstrap, worker, ...inputs } = this.#inputs(template)
    const node = requireRecord(bootstrap.node)
    const enrollment = requireRecord(node.intent).enrollment
    const runId = identity(requireRecord(requireRecord(beforePin.ready).view).run_id)
    const mount = {
      profile: { docker: { volume_marker: target.poolId } },
      identity: { engine: "oltp", pool_format_version: 1, cluster_id: this.clusterId,
        worker_id: worker.worker_id, pool_id: target.poolId },
      worker, generation: target.poolGeneration, mount_path: "/data", provider_volume_id: target.resourceId, filesystem_uuid: "",
    }
    const preparation = { claim_id: claimId, run_id: runId, enrollment, mount, mount_public_key: inputs.publicKey }
    node.intent = preparation
    const config = writeBootstrap(template, claimId, bootstrap)
    const launch = new DockerWorker(identity(worker.worker_id), identity(worker.incarnation), config, inputs.alias,
      template.cpuMillicores, template.memoryBytes, [[volume.volume_name, "/data"]],
      undefined, template.zone, [template.hostPort, inputs.httpPort])
    const options = { claimId, worker: launch, additionalMounts: inputs.mounts, deadline }
    const existing = await this.provider.ensureWorker({ ...options, inspectOnly: true })
    if (existing && existing.state !== "created") return this.#launchResult(existing)
    if (!await portAvailable(template.hostPort)) throw new ProviderError("Docker data worker loopback port is occupied")
    if (!same(await growthRead(controller, "storage.growth.snapshot", {}, { deadline }), before)) {
      throw new ProviderError("Control changed before Docker data worker preparation")
    }
    const prepared = await growthOperation(controller, claimId, "prepare-worker", "storage.growth.prepare_worker", preparation, { deadline })
    const after = await growthRead(controller, "storage.growth.snapshot", {}, { deadline })
    const afterPin = requireRecord(after.pin)
    const beforeRevision = beforePin.enrollment_revision
    const afterRevision = afterPin.enrollment_revision
    if (!same(prepared, { identity: worker }) || !same(after.active, record) || !after.active_automatic
      || after.cluster_id !== this.clusterId || !same(after.policy, before.policy)
      || Object.entries(beforePin).some(([key, value]) => key !== "enrollment_revision" && !same(afterPin[key], value))
      || !unsignedInteger(beforeRevision) || !unsignedInteger(afterRevision)
      || (BigInt(afterRevision) !== BigInt(beforeRevision) && BigInt(afterRevision) !== BigInt(beforeRevision) + 1n)) {
      throw new ProviderError("Control changed before Docker data worker launch")
    }
    return this.#launchResult(await this.provider.ensureWorker(options))
  }

  #launchResult(observed: DockerWorkerResult): JsonObject {
    if (observed.state !== "running") throw new ProviderError("Docker data worker did not stay running; its resources were retained")
    return { state: observed.started ? "launched" : "awaiting_ready", container_id: observed.resource_id }
  }
}

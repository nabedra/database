import { isDeepStrictEqual } from "node:util"
import { ControlTransport } from "../controlTransport.js"
import { ProviderError } from "../providers.js"
import { hasFields, type JsonObject } from "../transport.js"
import { dockerLabels, dockerOwned, dockerRecords, prefix, runDocker, unsignedInteger, volumeName, type DockerProvider } from "./docker.js"
import { identity, monotonic, requireRecord, trustedEndpoint } from "./shared.js"

const operations = {
  mutations: new Set(["worker.drain"]),
  reads: new Set(["worker.drain.status", "worker.stop_permit"]),
}

export interface DockerWorkerDrainOptions {
  provider: DockerProvider
  apiUrl: string
  adminToken: string
  caFile?: string | null
}

export interface BeginDockerDrainOptions {
  worker: JsonObject
  runId: string
  resourceId: string
}

export class DockerWorkerDrain {
  readonly #provider: DockerProvider
  readonly #control: ControlTransport

  constructor({ provider, apiUrl, adminToken, caFile }: DockerWorkerDrainOptions) {
    if (!trustedEndpoint(apiUrl)) {
      throw new ProviderError("Docker stop authority requires authenticated HTTPS or a numeric loopback address")
    }
    this.#provider = provider
    this.#control = new ControlTransport(apiUrl, adminToken, { timeout: provider.timeout, caFile }, operations)
  }

  async begin(drainId: string, { worker, runId, resourceId }: BeginDockerDrainOptions): Promise<JsonObject> {
    const id = identity(drainId)
    await this.#inspect(worker, resourceId, monotonic() + this.#provider.timeout)
    return this.#read("worker.drain", id, { worker, run_id: identity(runId), docker_container_id: resourceId.slice(17) })
  }

  status(drainId: string): Promise<JsonObject> {
    return this.#read("worker.drain.status", identity(drainId), {})
  }

  async stop(drainId: string): Promise<JsonObject> {
    const id = identity(drainId)
    const cutoff = monotonic() + this.#provider.timeout
    const permit = await this.#read("worker.stop_permit", id, {}, cutoff)
    let worker: JsonObject
    let resourceId: string
    let volume: string
    let claimId: string
    let poolId: string
    let destination: string
    let providerVolume: string
    try {
      const record = requireRecord(permit.record)
      const phase = record.phase
      if (record.id !== id || !hasFields(phase, "closed")) throw new Error()
      const intent = requireRecord(record.intent)
      worker = requireRecord(intent.worker)
      resourceId = "docker-container:" + intent.docker_container_id
      const mount = requireRecord(requireRecord(requireRecord(phase.closed).mount).expected)
      const pool = requireRecord(mount.identity)
      const profile = requireRecord(requireRecord(mount.profile).docker)
      if (pool.cluster_id !== this.#provider.clusterId || pool.worker_id !== worker.worker_id
        || !isDeepStrictEqual(mount.worker, worker) || typeof mount.provider_volume_id !== "string"
        || !mount.provider_volume_id.startsWith("docker-volume:") || profile.volume_marker !== pool.pool_id
        || typeof mount.mount_path !== "string") throw new Error()
      providerVolume = mount.provider_volume_id
      volume = providerVolume.slice(14)
      if (!volumeName.test(volume)) throw new Error()
      claimId = identity(record.growth_claim)
      poolId = identity(pool.pool_id)
      destination = mount.mount_path
    } catch {
      throw new ProviderError("Control returned no exact retained Docker stop permit")
    }
    const container = await this.#inspect(worker, resourceId, cutoff)
    const restart = requireRecord(requireRecord(container.HostConfig).RestartPolicy)
    if (dockerLabels(container)[prefix + "claim"] !== claimId || (restart.Name !== "" && restart.Name !== "no")) {
      throw new ProviderError("Docker launch claim or fixed restart policy differs")
    }
    const all = dockerRecords(container.Mounts ?? [])
    const chosen = all.filter(mount => mount.Destination === destination)
    if (all.some(mount => mount.Type === "volume" && mount.RW === true && mount.Destination !== destination)
      || chosen.length !== 1 || chosen[0].Type !== "volume" || chosen[0].Name !== volume || chosen[0].RW !== true) {
      throw new ProviderError("Docker worker no longer owns the retained mount from the permit")
    }
    const workerId = identity(worker.worker_id)
    await this.#retainedVolume(volume, claimId, workerId, poolId, cutoff)
    let status = requireRecord(container.State).Status
    if (status === "running") {
      await runDocker(this.#provider, ["container", "stop", "--time", "5", resourceId.slice(17)], { deadline: cutoff })
      status = requireRecord((await this.#inspect(worker, resourceId, cutoff)).State).Status
    }
    if (status !== "exited") throw new ProviderError("Exact Docker container stop is unresolved; reconcile the same drain")
    await this.#retainedVolume(volume, claimId, workerId, poolId, cutoff)
    return { resource_id: resourceId, state: "stopped", retained_volume_id: providerVolume }
  }

  async #retainedVolume(name: string, claim: string, worker: string, pool: string, deadline: number): Promise<void> {
    const volume = dockerRecords(await runDocker(this.#provider, ["volume", "inspect", name], { jsonResult: true, deadline }))
    if (volume.length !== 1 || volume[0].Name !== name) throw new ProviderError("Docker volume inventory changed")
    dockerOwned(volume[0].Labels, { [prefix + "cluster"]: this.#provider.clusterId, [prefix + "claim"]: claim,
      [prefix + "worker"]: worker, [prefix + "pool"]: pool })
  }

  async #inspect(worker: JsonObject, resourceId: string, deadline: number): Promise<JsonObject> {
    let labels: Record<string, string>
    try {
      const revision = worker.enrollment_revision
      const certificate = worker.certificate_sha256
      if (!unsignedInteger(revision, 1n) || typeof certificate !== "string" || !/^[a-f0-9]{64}(?![\s\S])/.test(certificate)) {
        throw new Error()
      }
      labels = { [prefix + "worker"]: identity(worker.worker_id), [prefix + "incarnation"]: identity(worker.incarnation),
        [prefix + "enrollment-revision"]: String(revision), [prefix + "certificate"]: certificate }
    } catch {
      throw new ProviderError("Exact Docker worker enrollment is required")
    }
    const container = await this.#provider.inspectWorker(resourceId, { deadline })
    dockerOwned(dockerLabels(container), labels)
    return container
  }

  async #read(operation: string, requestId: string, payload: JsonObject, deadline = monotonic() + this.#provider.timeout):
    Promise<JsonObject> {
    const [, record] = await this.#control.request(operation, payload, { requestId, deadline })
    return requireRecord(record)
  }
}

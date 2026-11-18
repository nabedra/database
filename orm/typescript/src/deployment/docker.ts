import { spawn } from "node:child_process"
import { createHash } from "node:crypto"
import { closeSync, constants, existsSync, fstatSync, openSync, readSync, realpathSync } from "node:fs"
import { isAbsolute } from "node:path"
import { isDeepStrictEqual } from "node:util"
import { ProviderError } from "../providers.js"
import { encodeJson, hasFields, isJsonObject, parseJson, type JsonObject } from "../transport.js"
import { digest, identity, monotonic, requireRecord, UnsupportedVolumeExpansion } from "./shared.js"

export { digest, identity, UnsupportedVolumeExpansion } from "./shared.js"

export const prefix = "io.nabedra."
export const volumeName = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}(?![\s\S])/
export const containerId = /^[a-f0-9]{64}(?![\s\S])/
const imageId = /^sha256:[a-f0-9]{64}(?![\s\S])/
const maxOutput = 8 * 2 ** 20
const maxBootstrap = 2 ** 20
const utf8 = new TextDecoder("utf-8", { fatal: true })

// Use the image's existing volume initializer and mount file format.
const initialize = String.raw`import fcntl,json,os,sys,pwd,grp,stat
expected=json.loads(sys.argv[1])
user,_,group=(sys.argv[2] or '0').partition(':')
account=pwd.getpwuid(int(user)) if user.isdigit() else pwd.getpwnam(user)
uid=account.pw_uid
gid=(int(group) if group.isdigit() else grp.getgrnam(group).gr_gid) if group else account.pw_gid
directory=os.open('/data',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
existing_only=len(sys.argv)>3 and sys.argv[3]=='existing'
lock_flags=os.O_RDONLY if existing_only else os.O_RDWR|os.O_CREAT
lock=os.open('.nabedra-provider.lock',lock_flags|os.O_NOFOLLOW,0o600,dir_fd=directory)
fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
def publish(name,data):
    temporary=name+'.pending'
    try:
        os.unlink(temporary,dir_fd=directory)
    except FileNotFoundError:
        pass
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=directory)
    with os.fdopen(fd,'wb') as target:
        os.fchown(target.fileno(),uid,gid)
        target.write(data);target.flush();os.fsync(target.fileno())
    os.replace(temporary,name,src_dir_fd=directory,dst_dir_fd=directory)
    os.fsync(directory)
name='.nabedra-docker-volume.json'
try:
    fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
except FileNotFoundError:
    assert not existing_only,'missing retained volume marker'
    # A provider never adopts an existing database merely because its path matches.
    assert set(os.listdir('/data'))<={'.nabedra-provider.lock',name+'.pending'},'nonempty unlabelled volume'
    os.fchown(directory,uid,gid);os.fchmod(directory,0o700)
    publish(name,json.dumps(expected,sort_keys=True).encode())
else:
    assert os.fstat(directory).st_uid==uid and os.fstat(directory).st_gid==gid,'volume owner changed'
    info=os.fstat(fd)
    assert (info.st_uid,info.st_gid)==(uid,gid),'marker owner changed'
    assert stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_mode&0o077==0 and info.st_size<=1024
    with os.fdopen(fd) as source:
        assert json.load(source)==expected,'volume marker mismatch'
if len(sys.argv)>3:
    with open('/seed','rb') as source:
        seed=source.read(33)
    assert len(seed)==32,'invalid mounted key'
    name='.nabedra-data-worker-key'
    try:
        fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
    except FileNotFoundError:
        assert not existing_only,'missing retained mount key'
        publish(name,seed)
    else:
        with os.fdopen(fd,'rb') as key:
            info=os.fstat(key.fileno())
            assert stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_mode&0o077==0
            assert (info.st_uid,info.st_gid)==(uid,gid) and key.read(33)==seed,'mount key changed'
    space=os.fstatvfs(directory)
    expected['observed_free_bytes']=space.f_bavail*space.f_frsize
print(json.dumps(expected))
`

export type DockerMount = readonly [nameOrSource: string, target: string]

export interface DockerProviderOptions {
  clusterId: string
  image: string
  network: string
  timeout?: number
}

export interface DockerPoolOptions {
  claimId: string
  poolId: string
  workerId: string
  zone: string
  mountSeed?: string | null
  templateDigest?: string | null
  existingOnly?: boolean
  deadline?: number | null
}

export interface DockerWorkerOptions {
  claimId: string
  worker: DockerWorker
  additionalMounts?: readonly DockerMount[]
  deadline?: number | null
  inspectOnly?: boolean
}

export interface DockerPoolResult extends JsonObject {
  resource_id: string
  volume_name: string
  volume_marker: string
  state: "provisioned"
  observed_free_bytes?: number | bigint
}

export interface DockerWorkerResult extends JsonObject {
  resource_id: string
  worker_id: string
  incarnation: string
  state: string
  started?: boolean
}

export function boundedInteger(value: unknown, minimum: number, maximum: number): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum && value <= maximum
}

export function unsignedInteger(value: unknown, minimum = 0n): value is number | bigint {
  return (typeof value === "bigint" || (typeof value === "number" && Number.isSafeInteger(value)))
    && value >= minimum && value < (1n << 64n)
}

export function sha256(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex")
}

function fileBytes(path: string, maximum: number, oneLink: boolean): Buffer {
  if (typeof path !== "string" || !isAbsolute(path) || (oneLink && realpathSync(path) !== path)) {
    throw new ProviderError("Docker bootstrap must be an existing absolute regular file")
  }
  const handle = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
  try {
    const stat = fstatSync(handle)
    if (!stat.isFile() || (oneLink && stat.nlink !== 1)) {
      throw new ProviderError("Docker bootstrap must be an existing absolute regular file")
    }
    if (stat.mode & 0o077) throw new ProviderError("Docker bootstrap must be private to its owner")
    if (stat.size <= 0 || stat.size > maximum) throw new ProviderError("Docker bootstrap exceeds its size bound")
    const data = Buffer.allocUnsafe(stat.size + 1)
    let total = 0
    while (total < data.length) {
      const count = readSync(handle, data, total, data.length - total, null)
      if (count === 0) break
      total += count
    }
    if (total !== stat.size) throw new ProviderError("Docker bootstrap changed while being read")
    return data.subarray(0, total)
  } finally {
    closeSync(handle)
  }
}

export function privateBytes(path: string, maximum: number): Buffer {
  try {
    return fileBytes(path, maximum, true)
  } catch {
    throw new ProviderError("Docker launch requires a bounded private regular file")
  }
}

function bootstrapBytes(path: string): Buffer {
  try {
    return fileBytes(path, maxBootstrap, false)
  } catch (error) {
    if (error instanceof ProviderError) throw error
    throw new ProviderError("Docker bootstrap must be an existing absolute regular file")
  }
}

function validTarget(target: unknown): target is string {
  return typeof target === "string" && target.startsWith("/") && target !== "/" && !target.split("/").includes("..")
    && !target.includes("\0")
}

export class DockerWorker {
  constructor(
    readonly workerId: string,
    readonly incarnation: string,
    readonly bootstrapFile: string,
    readonly networkAlias: string,
    readonly cpuMillicores: number,
    readonly memoryBytes: number,
    readonly volumes: readonly DockerMount[],
    readonly binary: string = "/usr/local/bin/nabedra-db",
    readonly zone: string | null = null,
    readonly httpPorts: readonly [host: number, internal: number] | null = null,
  ) {
    Object.freeze(this)
  }

  validate(): string {
    identity(this.workerId)
    identity(this.incarnation)
    if (typeof this.networkAlias !== "string" || !/^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}(?![\s\S])/.test(this.networkAlias)
      || !boundedInteger(this.cpuMillicores, 1, 64_000) || !boundedInteger(this.memoryBytes, 64 * 2 ** 20, 2 ** 40)
      || typeof this.binary !== "string" || !this.binary.startsWith("/") || !Array.isArray(this.volumes)
      || this.volumes.length < 1 || this.volumes.length > 64) {
      throw new ProviderError("Invalid bounded Docker worker configuration")
    }
    if (this.zone !== null && (typeof this.zone !== "string" || !this.zone || [...this.zone].length > 128
      || !Array.isArray(this.httpPorts) || this.httpPorts.length !== 2
      || this.httpPorts.some(port => !boundedInteger(port, 1, 65_535)))) {
      throw new ProviderError("Docker data workers require a zone and exact loopback port binding")
    }
    if (this.zone === null && this.httpPorts !== null) {
      throw new ProviderError("A Docker launch port is supported only for data workers")
    }
    const raw = bootstrapBytes(this.bootstrapFile)
    try {
      const bootstrap = requireRecord(parseJson(raw))
      const resources = requireRecord(bootstrap.resources)
      const node = requireRecord(bootstrap.node)
      const enrollment = requireRecord(this.zone === null ? node.enrollment : requireRecord(node.intent).enrollment)
      if (!boundedInteger(resources.max_threads, Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER)
        || !boundedInteger(resources.memory_limit_bytes, Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER)
        || resources.max_threads * 1_000 > this.cpuMillicores || resources.memory_limit_bytes > this.memoryBytes
        || enrollment.worker_id !== this.workerId || enrollment.incarnation !== this.incarnation) {
        throw new Error()
      }
    } catch {
      throw new ProviderError("Docker bootstrap worker identity or advertised resources contradict the enforced limits")
    }
    const paths = new Set<string>()
    for (const mount of this.volumes) {
      if (!Array.isArray(mount) || mount.length !== 2) throw new ProviderError("Invalid or duplicate Docker volume mount")
      const [name, path] = mount
      if (typeof name !== "string" || !volumeName.test(name) || !validTarget(path)
        || path === "/config" || path.startsWith("/config/") || paths.has(path)) {
        throw new ProviderError("Invalid or duplicate Docker volume mount")
      }
      paths.add(path)
    }
    return this.bootstrapFile
  }
}

export function runDocker(provider: DockerProvider, args: readonly string[],
  options: { jsonResult: true; deadline?: number | null }): Promise<unknown>
export function runDocker(provider: DockerProvider, args: readonly string[],
  options?: { jsonResult?: false; deadline?: number | null }): Promise<string>
export function runDocker(provider: DockerProvider, args: readonly string[],
  { jsonResult = false, deadline }: { jsonResult?: boolean; deadline?: number | null } = {}): Promise<unknown> {
  const remaining = Math.min(provider.timeout, (deadline ?? Infinity) - monotonic())
  if (!Number.isFinite(remaining) || remaining <= 0) {
    return Promise.reject(new ProviderError("Docker operation unavailable or timed out; reconcile the same claim"))
  }
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    let length = 0
    let failure: string | undefined
    const unavailable = "Docker operation unavailable or timed out; reconcile the same claim"
    let process
    try {
      process = spawn("docker", [...args], { stdio: ["ignore", "pipe", "ignore"] })
    } catch {
      reject(new ProviderError(unavailable))
      return
    }
    const timer = setTimeout(() => {
      failure ??= unavailable
      process.kill("SIGKILL")
    }, Math.max(1, Math.floor(remaining * 1_000)))
    process.stdout.on("data", (buffer: Buffer) => {
      if (failure) return
      length += buffer.length
      if (length > maxOutput) {
        failure = "Docker inventory exceeds its bound"
        process.kill("SIGKILL")
      } else {
        chunks.push(buffer)
      }
    })
    process.on("error", () => { failure = unavailable })
    process.on("close", code => {
      clearTimeout(timer)
      if (failure || code !== 0) {
        reject(new ProviderError(failure ?? "Docker operation failed; reconcile the same claim"))
        return
      }
      try {
        const body = Buffer.concat(chunks, length)
        resolve(jsonResult ? parseJson(body) : utf8.decode(body).trim())
      } catch {
        reject(new ProviderError("Invalid Docker response"))
      }
    })
  })
}

export function dockerOwned(labels: unknown, expected: Readonly<Record<string, string>>): void {
  if (!isJsonObject(labels) || Object.entries(expected).some(([key, value]) => labels[key] !== value)) {
    throw new ProviderError("Docker resource identity or immutable claim differs")
  }
}

export function dockerLabels(record: JsonObject): JsonObject {
  const config = isJsonObject(record.Config) ? record.Config : {}
  return isJsonObject(config.Labels) ? config.Labels : {}
}

export function dockerRecords(value: unknown): JsonObject[] {
  if (!Array.isArray(value)) throw new ProviderError("Invalid Docker response")
  return value.map(requireRecord)
}

function labelArguments(labels: Readonly<Record<string, string>>): string[] {
  return Object.entries(labels).flatMap(([key, value]) => ["--label", key + "=" + value])
}

function mountArgument(type: "volume" | "bind", source: string, target: string, readonly = false): string {
  const escape = (field: string) => /[,"\r\n]/.test(field) ? '"' + field.replaceAll('"', '""') + '"' : field
  const fields = ["type=" + type, "source=" + source, "target=" + target].map(escape)
  if (readonly) fields.push("readonly")
  return fields.join(",")
}

function lines(value: string): string[] {
  return value ? value.split(/\r?\n/) : []
}

function workerWire(worker: DockerWorker): JsonObject {
  const data: JsonObject = {
    worker_id: worker.workerId, incarnation: worker.incarnation, bootstrap_file: worker.bootstrapFile,
    network_alias: worker.networkAlias, cpu_millicores: worker.cpuMillicores, memory_bytes: worker.memoryBytes,
    volumes: worker.volumes, binary: worker.binary,
  }
  if (worker.zone !== null) Object.assign(data, { zone: worker.zone, http_ports: worker.httpPorts })
  return data
}

export class DockerProvider {
  readonly clusterId: string
  readonly image: string
  readonly network: string
  readonly timeout: number

  constructor({ clusterId, image, network, timeout = 20 }: DockerProviderOptions) {
    this.clusterId = identity(clusterId)
    if (typeof image !== "string" || !image || image.startsWith("-") || image.includes("\0")
      || typeof network !== "string" || !volumeName.test(network) || !Number.isFinite(timeout) || !(timeout > 0 && timeout <= 30)) {
      throw new ProviderError("Explicit Docker image, network and bounded timeout are required")
    }
    this.image = image
    this.network = network
    this.timeout = timeout
  }

  #labels(claimId: string, content: JsonObject): Record<string, string> {
    return { [prefix + "cluster"]: this.clusterId, [prefix + "claim"]: identity(claimId), [prefix + "content"]: digest(content) }
  }

  poolName(claimId: string): string {
    return "nabedra-" + this.clusterId + "-" + identity(claimId)
  }

  async ensurePool({ claimId, poolId, workerId, zone, mountSeed = null, templateDigest = null, existingOnly = false,
    deadline }: DockerPoolOptions): Promise<DockerPoolResult> {
    const content: JsonObject = { pool_id: identity(poolId), worker_id: identity(workerId), zone }
    if (typeof zone !== "string" || !zone || [...zone].length > 128) throw new ProviderError("Explicit placement zone is required")
    if (mountSeed !== null) {
      if (typeof templateDigest !== "string" || !containerId.test(templateDigest)
        || typeof mountSeed !== "string" || mountSeed.includes(",")
        || privateBytes(mountSeed, 32).length !== 32) {
        throw new ProviderError("Docker data worker requires a bound private mount seed")
      }
      content.template = templateDigest
    }
    const labels = { ...this.#labels(claimId, content), [prefix + "pool"]: poolId,
      [prefix + "worker"]: workerId, [prefix + "zone"]: zone }
    const name = this.poolName(claimId)
    const names = lines(await runDocker(this, ["volume", "ls", "--filter", "name=" + name, "--format", "{{.Name}}"], { deadline }))
    if (!names.includes(name)) {
      if (existingOnly) throw new ProviderError("The retained Docker volume is missing")
      await runDocker(this, ["volume", "create", ...labelArguments(labels), name], { deadline })
    }
    const observed = dockerRecords(await runDocker(this, ["volume", "inspect", name], { jsonResult: true, deadline }))
    if (observed.length !== 1 || observed[0].Name !== name) throw new ProviderError("Docker volume inventory changed")
    dockerOwned(observed[0].Labels, labels)
    const options = observed[0].Options
    if (mountSeed !== null && (observed[0].Driver !== "local" || observed[0].Scope !== "local"
      || (options != null && (!isJsonObject(options) || Object.keys(options).length)))) {
      throw new ProviderError("Docker launch requires a local named volume without driver options")
    }
    const marker = { format_version: 1, volume_name: name, volume_marker: poolId }
    const imageUser = await runDocker(this, ["image", "inspect", "--format", "{{json .Config.User}}", this.image], { jsonResult: true, deadline })
    if (typeof imageUser !== "string") throw new ProviderError("Invalid Docker image user")
    const extra = mountSeed === null ? [] : ["--mount", mountArgument("bind", mountSeed, "/seed", true)]
    const mode = mountSeed === null ? [] : [existingOnly ? "existing" : "initialize"]
    const initialized = requireRecord(await runDocker(this, ["run", "--rm", "--network", "none", "--cpus", ".25",
      "--memory", "128m", "--pids-limit", "32", "--user", "0:0", "--mount", mountArgument("volume", name, "/data",
        existingOnly && mountSeed !== null), ...extra, "--entrypoint", "python3", this.image, "-c", initialize,
      encodeJson(marker).toString("utf8"), imageUser, ...mode], { jsonResult: true, deadline }))
    const { observed_free_bytes: free, ...observedMarker } = initialized
    if (!isDeepStrictEqual(observedMarker, marker)) throw new ProviderError("Docker volume marker was not observed")
    const result: DockerPoolResult = { resource_id: "docker-volume:" + name, volume_name: name, volume_marker: poolId, state: "provisioned" }
    if (mountSeed !== null) {
      if (!unsignedInteger(free)) throw new ProviderError("Docker backing filesystem free space was not observed")
      result.observed_free_bytes = free
    }
    return result
  }

  async ensureWorker(options: DockerWorkerOptions & { inspectOnly: true }): Promise<DockerWorkerResult | null>
  async ensureWorker(options: DockerWorkerOptions & { inspectOnly?: false }): Promise<DockerWorkerResult>
  async ensureWorker(options: DockerWorkerOptions): Promise<DockerWorkerResult | null>
  async ensureWorker({ claimId, worker, additionalMounts = [], deadline, inspectOnly = false }: DockerWorkerOptions):
    Promise<DockerWorkerResult | null> {
    const config = worker.validate()
    const dataWorker = worker.zone !== null
    const volumes = worker.volumes.map(([source, path]) => [source, path] as const)
    const auxiliary = additionalMounts.map(([source, target]) => {
      if (typeof source !== "string" || !isAbsolute(source) || !existsSync(source) || source.includes(",")
        || !validTarget(target) || target.includes(",") || volumes.some(([, path]) => path === target)) {
        throw new ProviderError("Invalid Docker auxiliary mount")
      }
      return [source, target] as const
    })
    const content: JsonObject = { worker: workerWire(worker), bootstrap_sha256: sha256(bootstrapBytes(config)),
      image: this.image, network: this.network, auxiliary }
    const labels: Record<string, string> = { [prefix + "worker"]: worker.workerId, [prefix + "incarnation"]: worker.incarnation }
    if (dataWorker) {
      if (!imageId.test(this.image)) throw new ProviderError("Automatic Docker launch requires an immutable image ID")
      const launch = requireRecord(parseJson(privateBytes(config, maxBootstrap)))
      const enrollment = requireRecord(requireRecord(requireRecord(launch.node).intent).enrollment)
      content.auxiliary_sha256 = auxiliary.map(([path]) => sha256(privateBytes(path, maxBootstrap)))
      Object.assign(labels, { [prefix + "zone"]: worker.zone, [prefix + "certificate"]: enrollment.certificate_sha256,
        [prefix + "enrollment-revision"]: String(enrollment.enrollment_revision) })
    }
    Object.assign(labels, this.#labels(claimId, content))
    const name = "nabedra-worker-" + identity(claimId)
    const ids = lines(await runDocker(this, ["container", "ls", "-a", "--filter", "name=^/" + name + "$",
      "--format", "{{.ID}}"], { deadline }))
    if (!ids.length) {
      if (inspectOnly) return null
      for (const [volume] of volumes) {
        const observed = dockerRecords(await runDocker(this, ["volume", "inspect", volume], { jsonResult: true, deadline }))
        if (observed.length !== 1 || observed[0].Name !== volume) throw new ProviderError("Worker data volume is not owned by this cluster")
        dockerOwned(observed[0].Labels, { [prefix + "cluster"]: this.clusterId, [prefix + "worker"]: worker.workerId })
      }
      const args = ["container", "create", "--name", name, "--network", this.network, "--network-alias", worker.networkAlias,
        "--cpus", String(worker.cpuMillicores / 1_000), "--memory", String(worker.memoryBytes), "--pids-limit", "128"]
      if (dataWorker) {
        const [host, internal] = worker.httpPorts!
        args.push("--publish", `127.0.0.1:${host}:${internal}`)
      }
      args.push(...labelArguments(labels))
      for (const [source, target] of volumes) args.push("--mount", mountArgument("volume", source, target))
      for (const [source, target] of [[config, "/bootstrap.json"], ...auxiliary]) {
        args.push("--mount", mountArgument("bind", source, target, true))
      }
      args.push("--entrypoint", worker.binary, this.image, dataWorker ? "data-worker" : "worker", "--bootstrap", "/bootstrap.json")
      await runDocker(this, args, { deadline })
    }
    const observed = dockerRecords(await runDocker(this, ["container", "inspect", name], { jsonResult: true, deadline }))
    if (observed.length !== 1) throw new ProviderError("Docker worker identity is ambiguous")
    let [record] = observed
    dockerOwned(dockerLabels(record), labels)
    const hostConfig = requireRecord(record.HostConfig)
    if (hostConfig.NanoCpus !== worker.cpuMillicores * 1_000_000 || hostConfig.Memory !== worker.memoryBytes) {
      throw new ProviderError("Docker worker CPU or memory ceiling changed")
    }
    const mounts = dockerRecords(record.Mounts ?? [])
    const expectedVolumes = new Set(volumes.map(volume => JSON.stringify(volume)))
    const actualVolumes = new Set(mounts.filter(mount => mount.Type === "volume" && (!dataWorker || mount.RW === true))
      .map(mount => JSON.stringify([mount.Name, mount.Destination])))
    if (!isDeepStrictEqual(actualVolumes, expectedVolumes)) throw new ProviderError("Docker worker mounted volume identity changed")
    if (dataWorker) {
      const expectedBinds = new Set([[config, "/bootstrap.json"], ...auxiliary].map(bind => JSON.stringify(bind)))
      const actualBinds = new Set(mounts.filter(mount => mount.Type === "bind" && mount.RW === false)
        .map(mount => JSON.stringify([mount.Source, mount.Destination])))
      const [host, internal] = worker.httpPorts!
      const ports = { [internal + "/tcp"]: [{ HostIp: "127.0.0.1", HostPort: String(host) }] }
      const network = requireRecord(requireRecord(record.NetworkSettings).Networks)
      const aliases = hasFields(network, this.network) && requireRecord(network[this.network]).Aliases
      if (record.Name !== "/" + name || record.Image !== this.image || typeof record.Id !== "string" || !containerId.test(record.Id)
        || record.Path !== worker.binary || !isDeepStrictEqual(record.Args, ["data-worker", "--bootstrap", "/bootstrap.json"])
        || !Array.isArray(aliases) || !aliases.includes(worker.networkAlias) || hostConfig.PidsLimit !== 128
        || !isDeepStrictEqual(hostConfig.PortBindings, ports) || mounts.length !== expectedBinds.size + expectedVolumes.size
        || !isDeepStrictEqual(actualBinds, expectedBinds)) throw new ProviderError("Docker data worker launch binding changed")
    }
    const started = requireRecord(record.State).Status === "created" && !inspectOnly
    const id = record.Id
    if (typeof id !== "string" || !containerId.test(id)) throw new ProviderError("Exact Docker container identity is required")
    if (started) {
      await runDocker(this, ["container", "start", id], { deadline })
      if (dataWorker) record = await this.inspectWorker("docker-container:" + id, { deadline })
      else {
        const updated = dockerRecords(await runDocker(this, ["container", "inspect", id], { jsonResult: true, deadline }))
        if (updated.length !== 1 || updated[0].Id !== id) throw new ProviderError("Docker worker identity is ambiguous")
        record = updated[0]
      }
    }
    const state = requireRecord(record.State).Status
    if (typeof state !== "string") throw new ProviderError("Invalid Docker worker status")
    return { resource_id: "docker-container:" + id, worker_id: worker.workerId, incarnation: worker.incarnation, state,
      ...(dataWorker && { started }) }
  }

  async workerPoolNames(workerId: string, { deadline }: { deadline: number }): Promise<ReadonlySet<string>> {
    const names = lines(await runDocker(this, ["volume", "ls", "--filter", "label=" + prefix + "cluster=" + this.clusterId,
      "--filter", "label=" + prefix + "worker=" + identity(workerId), "--format", "{{.Name}}"], { deadline }))
    if (names.length > 64 || names.some(name => !volumeName.test(name))) {
      throw new ProviderError("Docker retained volume inventory exceeds its bound")
    }
    return new Set(names)
  }

  async inspectLaunchEnvironment({ deadline }: { deadline: number }): Promise<void> {
    const image = dockerRecords(await runDocker(this, ["image", "inspect", this.image], { jsonResult: true, deadline }))
    const network = dockerRecords(await runDocker(this, ["network", "inspect", this.network], { jsonResult: true, deadline }))
    if (image.length !== 1 || image[0].Id !== this.image || network.length !== 1 || network[0].Name !== this.network
      || network[0].Driver !== "bridge" || network[0].Scope !== "local") {
      throw new ProviderError("Automatic Docker launch requires the installed image and local bridge network")
    }
  }

  async inspectWorker(resourceId: string, { deadline }: { deadline?: number | null } = {}): Promise<JsonObject> {
    const id = typeof resourceId === "string" && resourceId.startsWith("docker-container:") ? resourceId.slice(17) : ""
    if (!containerId.test(id)) throw new ProviderError("Exact Docker container identity is required")
    const observed = dockerRecords(await runDocker(this, ["container", "inspect", id], { jsonResult: true, deadline }))
    if (observed.length !== 1 || observed[0].Id !== id) throw new ProviderError("Docker worker identity is ambiguous")
    if (dockerLabels(observed[0])[prefix + "cluster"] !== this.clusterId) throw new ProviderError("Docker worker belongs to another cluster")
    return observed[0]
  }

  async workerIds({ deadline }: { deadline: number }): Promise<ReadonlySet<string>> {
    const rows = lines(await runDocker(this, ["container", "ls", "-a", "--no-trunc", "--filter",
      "label=" + prefix + "cluster=" + this.clusterId, "--format", "{{.ID}}"], { deadline }))
    if (rows.length > 4_096 || rows.some(id => !containerId.test(id))) throw new ProviderError("Docker cluster inventory exceeds its bound")
    return new Set(rows)
  }

  async expandPool({ resourceId, targetBytes }: { resourceId: string; targetBytes: number | bigint }): Promise<never> {
    const name = typeof resourceId === "string" && resourceId.startsWith("docker-volume:") ? resourceId.slice(14) : ""
    if (!volumeName.test(name) || !unsignedInteger(targetBytes, 1n)) {
      throw new ProviderError("Exact Docker volume and positive target size are required")
    }
    const records = dockerRecords(await runDocker(this, ["volume", "inspect", name], { jsonResult: true }))
    if (records.length !== 1 || records[0].Name !== name || !isJsonObject(records[0].Labels)
      || records[0].Labels[prefix + "cluster"] !== this.clusterId) throw new ProviderError("Docker volume identity or cluster changed")
    throw new UnsupportedVolumeExpansion(resourceId)
  }
}

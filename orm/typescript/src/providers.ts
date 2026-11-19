import { createHash } from "node:crypto"
import { AsyncLocalStorage } from "node:async_hooks"
import { ValidationError } from "./errors.js"
import { encodeCanonicalJson, encodeJson, isJsonObject, parseJson, Transport, type JsonObject } from "./transport.js"

export class ProviderError extends Error {
  constructor(message: string) {
    super(message)
    this.name = new.target.name
  }
}

export interface KubernetesProviderOptions {
  apiUrl: string
  token: string
  namespace: string
  clusterId: string
  image: string
  caFile?: string | null
  podSpec?: JsonObject | null
  volumeMounts?: JsonObject[] | null
  timeout?: number
}

export interface ProviderLaunch {
  cluster_id: string
  node_id: string
  request_id: string
  bootstrap: JsonObject
}

export interface ProviderNode {
  resource_id: string
  cluster_id: string
  node_id: string
  request_id: string
  state: "starting" | "running" | "stopping" | "terminated"
}

interface KubernetesMetadata {
  name: string
  namespace: string
  labels: Record<string, string>
  annotations?: Record<string, string>
  uid?: string
  deletionTimestamp?: string
}

const podRunner = `import json, os
with open('/nabedra/bootstrap.json') as source:
    bootstrap = json.load(source)
address = os.environ['NABEDRA_POD_IP']
if ':' in address:
    address = '[' + address + ']'
port = int(bootstrap['listen'].rsplit(':', 1)[1])
bootstrap['advertise_url'] = ('https' if bootstrap['tls'] else 'http') + '://' + address + ':' + str(port)
path = '/tmp/nabedra-worker-bootstrap.json'
with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as target:
    json.dump(bootstrap, target)
os.execvp('nabedra-db', ['nabedra-db', 'worker', '--bootstrap', path])
`

const prefixes = ["nabedra.io", "meshdb.io"] as const
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?![\s\S])/
const now = () => performance.now() / 1_000

function object(value: unknown): JsonObject {
  if (!isJsonObject(value)) throw new ProviderError("Invalid Kubernetes record")
  return value
}

export class KubernetesProvider {
  readonly clusterId: string
  readonly namespace: string
  readonly #image: string
  readonly #podSpec: JsonObject
  readonly #volumeMounts: JsonObject[]
  readonly #base: string
  readonly #transport: Transport
  readonly #authorization: string
  readonly #calls = new AsyncLocalStorage<number>()

  constructor({ apiUrl, token, namespace, clusterId, image, caFile, podSpec, volumeMounts,
    timeout = 10 }: KubernetesProviderOptions) {
    if (typeof apiUrl !== "string" || !/^https:\/\/[^/?#@\\]+\/?(?![\s\S])/.test(apiUrl)) {
      throw new ValidationError("Kubernetes apiUrl must be an HTTPS origin")
    }
    if (typeof namespace !== "string" || !/^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?(?![\s\S])/.test(namespace)) {
      throw new ValidationError("Invalid Kubernetes namespace")
    }
    if (typeof clusterId !== "string" || !uuid.test(clusterId) || typeof token !== "string" || !token || /\s/.test(token)) {
      throw new ValidationError("Explicit cluster identity and Kubernetes token are required")
    }
    if (typeof image !== "string" || !image || !Number.isFinite(timeout) || timeout <= 0 || timeout > 30) {
      throw new ValidationError("Kubernetes image and timeout are required")
    }
    if ((podSpec != null && !isJsonObject(podSpec)) || (volumeMounts != null && !Array.isArray(volumeMounts))) {
      throw new ValidationError("Kubernetes podSpec and volumeMounts must be an object and a list")
    }
    this.clusterId = clusterId
    this.namespace = namespace
    this.#image = image
    this.#podSpec = structuredClone(podSpec ?? {})
    this.#volumeMounts = structuredClone(volumeMounts ?? [])
    if (["containers", "initContainers", "restartPolicy"].some(key => Object.hasOwn(this.#podSpec, key))) {
      throw new ValidationError("podSpec cannot override worker containers or restart policy")
    }
    const volumes = Object.hasOwn(this.#podSpec, "volumes") ? this.#podSpec.volumes : []
    if (!Array.isArray(volumes) || volumes.some(v => !isJsonObject(v) || v.name === "bootstrap")
      || this.#volumeMounts.some(v => !isJsonObject(v) || v.name === "bootstrap" || typeof v.mountPath !== "string"
        || v.mountPath === "/" || v.mountPath === "/nabedra" || v.mountPath.startsWith("/nabedra/"))) {
      throw new ValidationError("Credential mounts cannot override the bootstrap volume")
    }
    this.#transport = new Transport(apiUrl, null, { timeout, caFile, agent: false })
    this.#authorization = "Bearer " + token
    this.#base = "/api/v1/namespaces/" + encodeURIComponent(namespace)
    this.startNode = this.startNode.bind(this)
    this.stopNode = this.stopNode.bind(this)
    this.listNodes = this.listNodes.bind(this)
  }

  protected request(method: string, path: string, body?: JsonObject, options?: { missing?: false; conflict?: false; absolute?: boolean; deadline?: number }): Promise<JsonObject>
  protected request(method: string, path: string, body: JsonObject | undefined,
    options: { missing?: boolean; conflict?: boolean; absolute?: boolean; deadline?: number }): Promise<JsonObject | null>
  protected async request(method: string, path: string, body?: JsonObject,
    { missing = false, conflict = false, absolute = false, deadline: requested }: {
      missing?: boolean; conflict?: boolean; absolute?: boolean; deadline?: number
    } = {}): Promise<JsonObject | null> {
    const payload = body === undefined ? undefined : encodeJson(body)
    if (payload && payload.length > 1 << 20) throw new ProviderError("Kubernetes request exceeds bootstrap limit")
    const deadline = Math.min(requested ?? Infinity, this.#calls.getStore() ?? now() + this.#transport.timeout)
    if (deadline <= now()) throw new ProviderError("Kubernetes operation deadline exceeded")
    try {
      const response = await this.#transport.exchange(method, absolute ? path : this.#base + path, {
        body: payload, maxBytes: 8 << 20, deadline, authorization: this.#authorization,
        headers: { "Content-Type": "application/json", Accept: "application/json" },
      })
      if ((missing && response.status === 404) || (conflict && response.status === 409)) return null
      if (response.status < 200 || response.status >= 300) throw new ProviderError("Kubernetes request failed")
      if (deadline <= now()) throw new ProviderError("Kubernetes operation deadline exceeded")
      return object(parseJson(response.body))
    } catch (error) {
      if (error instanceof ProviderError) throw error
      throw new ProviderError("Kubernetes request failed")
    }
  }

  #bounded<T>(work: () => Promise<T>): Promise<T> {
    return this.#calls.run(Math.min(this.#calls.getStore() ?? Infinity, now() + this.#transport.timeout), work)
  }

  #labels(node: string, launch: string, prefix = "nabedra.io"): Record<string, string> {
    return { [prefix + "/cluster"]: this.clusterId, [prefix + "/node"]: node, [prefix + "/launch"]: launch }
  }

  #identityPrefix(meta: KubernetesMetadata): typeof prefixes[number] {
    const present = prefixes.filter(prefix => Object.keys(meta.labels ?? {}).some(key => key.startsWith(prefix + "/")))
    if (present.length !== 1) throw new ProviderError("Kubernetes resource ownership mismatch")
    const [prefix] = present
    const other = prefixes.find(candidate => candidate !== prefix)
    if (Object.keys(meta.annotations ?? {}).some(key => key.startsWith(other + "/"))) {
      throw new ProviderError("Kubernetes resource lifecycle namespace mismatch")
    }
    return prefix
  }

  #annotation(meta: KubernetesMetadata, key: string): string | undefined {
    return meta.annotations?.[this.#identityPrefix(meta) + "/" + key]
  }

  #owned(record: JsonObject, node?: string, launch?: string): [KubernetesMetadata, string, string] {
    const meta = object(record.metadata) as unknown as KubernetesMetadata
    const prefix = this.#identityPrefix(meta)
    const n = meta.labels[prefix + "/node"]
    const l = meta.labels[prefix + "/launch"]
    if (typeof n !== "string" || !uuid.test(n) || typeof l !== "string" || !uuid.test(l)
      || meta.namespace !== this.namespace || meta.labels[prefix + "/cluster"] !== this.clusterId
      || (node !== undefined && node !== n) || (launch !== undefined && launch !== l)) {
      throw new ProviderError("Kubernetes resource ownership mismatch")
    }
    if (meta.name !== (prefix === "nabedra.io" ? "nabedra-" : "meshdb-") + l) {
      throw new ProviderError("Kubernetes resource identity mismatch")
    }
    return [meta, n, l]
  }

  async #retainUid(secret: JsonObject, uid: string, terminated = false): Promise<void> {
    const [meta] = this.#owned(secret)
    const prefix = this.#identityPrefix(meta)
    const annotations = { ...meta.annotations }
    const podUid = prefix + "/pod-uid"
    const ended = prefix + "/terminated"
    const previous = annotations[podUid]
    if (previous !== undefined && previous !== uid) throw new ProviderError("Kubernetes launch UID changed")
    if (previous === uid && (!terminated || annotations[ended] === "true")) return
    annotations[podUid] = uid
    if (terminated) annotations[ended] = "true"
    await this.request("PUT", "/secrets/" + encodeURIComponent(meta.name), { ...secret, metadata: { ...meta, annotations } })
  }

  startNode(payload: ProviderLaunch): Promise<string> {
    return this.#bounded(async () => {
      if (payload.cluster_id !== this.clusterId) throw new ProviderError("Kubernetes launch cluster mismatch")
      const { node_id: node, request_id: launch, bootstrap } = payload
      if (typeof node !== "string" || typeof launch !== "string" || !uuid.test(node) || !uuid.test(launch)) {
        throw new ProviderError("Invalid Kubernetes launch identity")
      }
      const port = typeof bootstrap?.listen === "string" && /:([0-9]+)(?![\s\S])/.exec(bootstrap.listen)
      if (!port || Number(port[1]) < 1 || Number(port[1]) > 65535) {
        throw new ProviderError("Kubernetes workers require an explicit nonzero listen port")
      }

      let name = "nabedra-" + launch
      const secretPath = "/secrets/" + name
      const bytes = Buffer.from(encodeCanonicalJson(bootstrap))
      const digest = createHash("sha256").update(bytes).digest("hex")
      const encoded = bytes.toString("base64")
      let secret = await this.request("GET", secretPath, undefined, { missing: true })
      const legacyName = "meshdb-" + launch
      const legacy = await this.request("GET", "/secrets/" + legacyName, undefined, { missing: true })
      if (legacy) {
        if (secret) throw new ProviderError("Kubernetes launch has multiple resource identities")
        name = legacyName
        secret = legacy
      }
      if (!secret) {
        await this.request("POST", "/secrets", {
          apiVersion: "v1", kind: "Secret", type: "Opaque",
          metadata: { name, namespace: this.namespace, labels: this.#labels(node, launch),
            annotations: { "nabedra.io/bootstrap-sha256": digest } },
          data: { "bootstrap.json": encoded },
        }, { conflict: true })
        secret = await this.request("GET", secretPath)
      }
      const [meta] = this.#owned(secret, node, launch)
      if (this.#annotation(meta, "bootstrap-sha256") !== digest || object(secret.data)["bootstrap.json"] !== encoded
        || this.#annotation(meta, "terminated") === "true") throw new ProviderError("Kubernetes launch identity conflict")

      let pod = await this.request("GET", "/pods/" + name, undefined, { missing: true })
      if (!pod) {
        if (this.#annotation(meta, "pod-uid")) throw new ProviderError("Kubernetes launch was already removed")
        const { volumes = [], ...spec } = this.#podSpec
        const resources = bootstrap.resources ? object(bootstrap.resources)
          : { max_threads: bootstrap.partition_slots, memory_limit_bytes: 8 * 2 ** 30 }
        const quantity = { cpu: String(resources.max_threads), memory: String(resources.memory_limit_bytes) }
        await this.request("POST", "/pods", {
          apiVersion: "v1", kind: "Pod",
          metadata: { name, namespace: this.namespace, labels: this.#labels(node, launch, this.#identityPrefix(meta)) },
          spec: {
            ...spec, restartPolicy: "Never",
            volumes: [{ name: "bootstrap", secret: { secretName: name, defaultMode: 256 } }, ...(volumes as JsonObject[])],
            containers: [{ name: "worker", image: this.#image, command: ["python3", "-c", podRunner],
              env: [{ name: "NABEDRA_POD_IP", valueFrom: { fieldRef: { fieldPath: "status.podIP" } } }],
              volumeMounts: [{ name: "bootstrap", mountPath: "/nabedra", readOnly: true }, ...this.#volumeMounts],
              resources: { requests: quantity, limits: quantity } }],
          },
        }, { conflict: true })
        pod = await this.request("GET", "/pods/" + name)
      }
      const [podMeta] = this.#owned(pod, node, launch)
      const uid = podMeta.uid
      if (!uid || typeof uid !== "string" || uid.includes("/")) throw new ProviderError("Kubernetes Pod has no UID")
      await this.#retainUid(secret, uid)
      return [this.namespace, name, uid].join("/")
    })
  }

  stopNode(resourceId: string): Promise<void> {
    return this.#bounded(async () => {
      const parts = typeof resourceId === "string" ? resourceId.split("/") : []
      const [namespace, name, uid] = parts
      if (parts.length !== 3) throw new ProviderError("Invalid Kubernetes resource identity")
      if (namespace !== this.namespace || !/^(nabedra|meshdb)-/.test(name) || !uid) {
        throw new ProviderError("Kubernetes resource ownership mismatch")
      }
      const secret = await this.request("GET", "/secrets/" + encodeURIComponent(name))
      const [meta, node, launch] = this.#owned(secret)
      if (meta.name !== name || this.#annotation(meta, "pod-uid") !== uid) {
        throw new ProviderError("Kubernetes resource ownership mismatch")
      }
      const path = "/pods/" + encodeURIComponent(name)
      const pod = await this.request("GET", path, undefined, { missing: true })
      if (pod) {
        const [podMeta] = this.#owned(pod, node, launch)
        if (podMeta.uid !== uid) throw new ProviderError("Kubernetes Pod UID changed")
        await this.request("DELETE", path, { apiVersion: "v1", kind: "DeleteOptions",
          preconditions: { uid } }, { missing: true })
      }
      if (await this.request("GET", path, undefined, { missing: true }) === null) {
        await this.#retainUid(secret, uid, true)
      }
    })
  }

  listNodes(): Promise<ProviderNode[]> {
    return this.#bounded(async () => {
      const result: ProviderNode[] = []
      const launches = new Set<string>()
      for (const prefix of prefixes) {
        const query = new URLSearchParams({ labelSelector: prefix + "/cluster=" + this.clusterId, limit: "64" })
        const continuations = new Set<string>()
        while (true) {
          const page = await this.request("GET", "/secrets?" + query.toString())
          if (!Array.isArray(page.items)) throw new ProviderError("Invalid Kubernetes inventory")
          for (const entry of page.items) {
            const secret = object(entry)
            const [meta, node, launch] = this.#owned(secret)
            if (this.#identityPrefix(meta) !== prefix || launches.has(launch)) {
              throw new ProviderError("Kubernetes inventory identity conflict")
            }
            launches.add(launch)
            const pod = await this.request("GET", "/pods/" + encodeURIComponent(meta.name), undefined, { missing: true })
            let uid = this.#annotation(meta, "pod-uid")
            let state: ProviderNode["state"] = "terminated"
            if (pod) {
              const [podMeta] = this.#owned(pod, node, launch)
              if (this.#identityPrefix(podMeta) !== prefix) throw new ProviderError("Kubernetes resource ownership mismatch")
              if (uid !== undefined && podMeta.uid !== uid) throw new ProviderError("Kubernetes Pod UID changed")
              uid = podMeta.uid
              if (!uid || typeof uid !== "string" || uid.includes("/")) throw new ProviderError("Kubernetes Pod has no UID")
              const phase = isJsonObject(pod.status) ? pod.status.phase : undefined
              const ended = phase === "Failed" || phase === "Succeeded"
              if (this.#annotation(meta, "terminated") === "true" && !ended) {
                throw new ProviderError("Kubernetes terminated resource reappeared")
              }
              state = ended ? "terminated" : podMeta.deletionTimestamp ? "stopping" : phase === "Running" ? "running" : "starting"
              if (this.#annotation(meta, "pod-uid") === undefined) await this.#retainUid(secret, uid)
            } else if (uid) {
              await this.#retainUid(secret, uid, true)
            } else {
              continue
            }
            result.push({ resource_id: [this.namespace, meta.name, uid].join("/"),
              cluster_id: this.clusterId, node_id: node, request_id: launch, state })
            if (result.length > 4096) throw new ProviderError("Kubernetes inventory exceeds callback limit")
          }
          const continuation = isJsonObject(page.metadata) ? page.metadata.continue : null
          if (!continuation) break
          if (typeof continuation !== "string" || continuations.has(continuation) || continuations.size >= 63) {
            throw new ProviderError("Kubernetes inventory pagination exceeded its bound")
          }
          continuations.add(continuation)
          query.set("continue", continuation)
        }
      }
      return result
    })
  }
}

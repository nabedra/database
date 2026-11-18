import { execFile } from "node:child_process"
import { createReadStream } from "node:fs"
import { access, opendir, readFile, readlink, realpath, stat } from "node:fs/promises"
import { posix } from "node:path"
import { createInterface } from "node:readline"
import { promisify } from "node:util"
import { ProviderError } from "../providers.js"
import type { JsonObject } from "../transport.js"
import { identity, monotonic, requireRecord } from "./shared.js"

const runFile = promisify(execFile)
const configuredInstance = /^i-(?:[a-f0-9]{8}|[a-f0-9]{17})(?![\s\S])/

export interface EbsLinuxHostOptions {
  instanceId: string
  mountPaths: Iterable<string>
}

function deviceNumber(number: bigint): string {
  const major = ((number >> 8n) & 0xfffn) | ((number >> 32n) & 0xfffff000n)
  const minor = (number & 0xffn) | ((number >> 12n) & 0xffffff00n)
  return `${major}:${minor}`
}

function removed(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | null)?.code
  return code === "ENOENT" || code === "ESRCH"
}

export class EbsLinuxHost {
  readonly #instanceId: string
  readonly #mountPaths: Set<string>

  constructor({ instanceId, mountPaths }: EbsLinuxHostOptions) {
    if (typeof instanceId !== "string" || !configuredInstance.test(instanceId)) {
      throw new ProviderError("Exact local EC2 instance ID is required")
    }
    const paths = new Set(mountPaths)
    if (paths.size === 0 || paths.size > 64 || [...paths].some(path => typeof path !== "string" || path === "/"
      || !path.startsWith("/") || path.endsWith("/") || path.includes("\0") || posix.normalize(path) !== path)) {
      throw new ProviderError("Explicit EBS host maintenance mount paths are required")
    }
    this.#instanceId = instanceId
    this.#mountPaths = paths
  }

  get instanceId(): string { return this.#instanceId }
  get mountPaths(): ReadonlySet<string> { return new Set(this.#mountPaths) }

  async #requireHost(instanceId: string, deadline: number): Promise<void> {
    if (instanceId !== this.instanceId || process.platform !== "linux" || process.geteuid?.() !== 0
      || !Number.isFinite(deadline) || monotonic() >= deadline) {
      throw new ProviderError("EBS maintenance requires the configured root Linux host and a live deadline")
    }
    try {
      const uid = (await readFile("/sys/class/dmi/id/board_asset_tag", "utf8")).trim()
      for (const name of ["mnt", "pid"]) {
        if (await readlink("/proc/1/ns/" + name) !== await readlink("/proc/self/ns/" + name)) throw new Error()
      }
      if (uid !== instanceId) throw new Error()
    } catch {
      throw new ProviderError("EBS maintenance is not on the expected Nitro instance")
    }
  }

  async #device(expected: JsonObject): Promise<{ link: string; device: string }> {
    if (typeof expected.mount_path !== "string" || !this.#mountPaths.has(expected.mount_path)) {
      throw new ProviderError("EBS mount path is outside host maintenance configuration")
    }
    try {
      const link = "/dev/disk/by-uuid/" + identity(expected.filesystem_uuid)
      const block = await stat(link, { bigint: true })
      if (!block.isBlockDevice()) throw new Error()
      const device = deviceNumber(block.rdev)
      const sysfs = await realpath("/sys/dev/block/" + device)
      const serial = (await readFile(sysfs + "/device/serial", "utf8")).trim()
      const model = (await readFile(sysfs + "/device/model", "utf8")).trim()
      const partition = await access(sysfs + "/partition").then(() => true, error => {
        if (!removed(error)) throw error
        return false
      })
      if (partition || typeof expected.provider_volume_id !== "string"
        || serial !== expected.provider_volume_id.replace("-", "") || model !== "Amazon Elastic Block Store") throw new Error()
      return { link, device }
    } catch {
      throw new ProviderError("EBS maintenance volume and whole-device filesystem UUID differ")
    }
  }

  async grow(instanceId: string, expected: JsonObject, { deadline }: { deadline: number }): Promise<void> {
    await this.#requireHost(instanceId, deadline)
    const { link, device } = await this.#device(expected)
    const path = String(expected.mount_path)
    try {
      if (await realpath(path) !== path || deviceNumber((await stat(path, { bigint: true })).dev) !== device) throw new Error()
      const escaped = path.replaceAll("\\", "\\134").replaceAll(" ", "\\040").replaceAll("\t", "\\011")
      const rows = (await readFile("/proc/self/mountinfo", "utf8")).split("\n")
        .map(line => line.split(" ")).filter(line => line[4] === escaped)
      const [row] = rows
      if (rows.length !== 1 || row[2] !== device || row[3] !== "/" || !row[5].split(",").includes("rw")) throw new Error()
      const type = row[row.indexOf("-") + 1]
      const command = type === "xfs" ? ["/usr/sbin/xfs_growfs", path]
        : type === "ext4" ? ["/usr/sbin/resize2fs", link]
        : null
      const remaining = deadline - monotonic()
      if (!command || remaining <= 0) throw new Error()
      await runFile(command[0], [command[1]], { timeout: Math.max(1, Math.min(2 ** 31 - 1, remaining * 1_000)),
        killSignal: "SIGKILL", maxBuffer: 64 * 1024 })
    } catch {
      throw new ProviderError("EBS host filesystem growth is unresolved")
    }
  }

  async unmounted(instanceId: string, mount: JsonObject, { deadline }: { deadline: number }): Promise<boolean> {
    await this.#requireHost(instanceId, deadline)
    const { device } = await this.#device(requireRecord(mount.expected))
    if (device !== `${mount.device_major}:${mount.device_minor}`) {
      throw new ProviderError("Closed EBS mount device has changed")
    }
    const namespaces = new Set<string>()
    try {
      let visited = 0
      for await (const entry of await opendir("/proc")) {
        if (++visited > 32_768 || monotonic() >= deadline) {
          throw new ProviderError("EBS mount inventory exceeded the maintenance bound")
        }
        if (!/^\d+$/.test(entry.name)) continue
        try {
          const dir = "/proc/" + entry.name
          const namespace = await readlink(dir + "/ns/mnt")
          if (namespaces.has(namespace)) continue
          const table = createReadStream(dir + "/mountinfo")
          const reader = createInterface({ input: table, crlfDelay: Infinity })
          try {
            for await (const row of reader) {
              if (monotonic() >= deadline) throw new ProviderError("EBS mount inventory exceeded the maintenance bound")
              if (row.split(" ", 3)[2] === device) return false
            }
          } finally {
            reader.close()
            table.destroy()
          }
          namespaces.add(namespace)
        } catch (error) {
          if (!removed(error)) throw error
        }
      }
    } catch (error) {
      if (error instanceof ProviderError) throw error
      throw new ProviderError("EBS mount inventory could not verify unmount")
    }
    return true
  }
}

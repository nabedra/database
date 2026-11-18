import { spawnSync } from "node:child_process"
import { accessSync, constants, statSync } from "node:fs"
import { delimiter, dirname, join, resolve } from "node:path"

export const cliVersion = "0.1.0"
export const configVersion = 2
export const protocolVersion = 1
export const binaryFailure =
  "Cannot launch compatible nabedra-db. Set NABEDRA_SERVER_BINARY to an executable file path or put nabedra-db on PATH."

export function binaryVersion(path: string): string {
  try {
    const result = spawnSync(path, ["--version"], {
      stdio: ["ignore", "pipe", "ignore"], timeout: 5_000, killSignal: "SIGKILL", maxBuffer: 257, encoding: "utf8",
    })
    const match = /^nabedra-db (0\.1\.\d+)(?: (?:olap|oltp))?\s*$/.exec(result.stdout ?? "")
    if (!result.error && result.status === 0 && match) return match[1]
  } catch {
    // Only the sanitized failure is part of the public CLI response.
  }
  throw new Error(binaryFailure)
}

export function resolveServerBinary({ check = false }: { check?: boolean } = {}): string {
  const explicit = process.env.NABEDRA_SERVER_BINARY
  const path = process.env.PATH ?? ["/usr/bin", "/bin"].join(delimiter)
  const candidates = explicit === undefined
    ? [dirname(process.execPath), ...(path ? path.split(delimiter) : [])].map(directory => join(directory, "nabedra-db"))
    : [explicit]
  for (const candidate of candidates) {
    if (!candidate) continue
    try {
      const path = resolve(candidate)
      if (!statSync(path).isFile()) continue
      accessSync(path, constants.X_OK)
      if (check) binaryVersion(path)
      return path
    } catch {
      // An explicit override has no fallback.
    }
  }
  throw new Error(binaryFailure)
}

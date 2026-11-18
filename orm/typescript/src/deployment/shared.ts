import { createHash } from "node:crypto"
import { isIP } from "node:net"
import { isDeepStrictEqual } from "node:util"
import { ProviderError } from "../providers.js"
import { encodeCanonicalJson, isJsonObject, type JsonObject } from "../transport.js"

export class UnsupportedVolumeExpansion extends ProviderError {
  constructor(readonly resourceId: string) {
    super("Docker named volumes do not expose filesystem expansion; add a pool or worker")
  }
}

export function identity(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?![\s\S])/.test(value)
    || !/[1-9a-f]/.test(value)) {
    throw new ProviderError("A canonical nonzero identity is required")
  }
  return value
}

export function uuid5(namespace: string, name: string): string {
  const bytes = createHash("sha1").update(Buffer.from(identity(namespace).replaceAll("-", ""), "hex")).update(name).digest()
  bytes[6] = (bytes[6] & 0x0f) | 0x50
  bytes[8] = (bytes[8] & 0x3f) | 0x80
  const hex = bytes.subarray(0, 16).toString("hex")
  return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join("-")
}

export const canonicalJson = encodeCanonicalJson

export function equalWire(left: unknown, right: unknown): boolean {
  if (isDeepStrictEqual(left, right)) return true
  try {
    return canonicalJson(left) === canonicalJson(right)
  } catch {
    return false
  }
}

export function digest(value: unknown): string {
  return createHash("sha256").update(canonicalJson(value)).digest("hex")
}

export function requireRecord(value: unknown): JsonObject {
  if (!isJsonObject(value)) throw new ProviderError("Database or provider reply is not a metadata record")
  return value
}

export function isNumericLoopback(value: string): boolean {
  const hostname = value.replace(/^\[|\]$/g, "")
  return (isIP(hostname) === 4 && hostname.startsWith("127.")) || hostname === "::1"
}

export function trustedEndpoint(value: string): boolean {
  try {
    const endpoint = new URL(value)
    const host = /^http:\/\/(\[[^\]]+\]|[^:/]+)(?=[:/]|$)/.exec(value)?.[1]
    return endpoint.protocol === "https:" || (host !== undefined && isNumericLoopback(host))
  } catch {
    return false
  }
}

export const monotonic = () => performance.now() / 1_000

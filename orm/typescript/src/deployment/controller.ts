import { createHash, timingSafeEqual } from "node:crypto"
import { ControlTransport } from "../controlTransport.js"
import { StorageError } from "../errors.js"
import { ProviderError } from "../providers.js"
import { isJsonObject, type JsonObject } from "../transport.js"
import { equalWire, identity, requireRecord, UnsupportedVolumeExpansion, uuid5 } from "./shared.js"

const operations = {
  mutations: new Set([
    ...["configure", "claim", "claim_automatic", "attempt", "provisioned", "mounted", "prepare_worker"].map(name => "storage.growth." + name),
    "storage.replica.move",
  ]),
  reads: new Set(["status", "inventory", "snapshot", "workers", "sample"].map(name => "storage.growth." + name)),
}

export interface GrowthControllerOptions {
  apiUrl: string
  adminToken: string
  timeout?: number
  caFile?: string | null
}

export interface DeadlineOptions { deadline?: number }
export interface MountOptions extends DeadlineOptions { mount?: JsonObject | null }
export type GrowthProvision = (intent: JsonObject) => JsonObject | Promise<JsonObject>
export interface ReconcileGrowthOptions {
  intent: JsonObject
  provision: GrowthProvision
  mount?: JsonObject | null
}

interface AdvanceOptions extends MountOptions {
  register?: (intent: JsonObject, record: JsonObject) => JsonObject | null | Promise<JsonObject | null>
  authorize?: (phase: "provisioning" | "awaiting_mount", attempt: number) => void | Promise<void>
}

const endpoints = new WeakMap<GrowthController, string>()
const control = new WeakMap<GrowthController, ControlTransport>()
const credentials = new WeakMap<GrowthController, Buffer>()

export function growthMatchesCredentials(controller: GrowthController, apiUrl: string, adminToken: string): boolean {
  const expected = credentials.get(controller)
  return growthEndpoint(controller) === apiUrl && expected !== undefined
    && timingSafeEqual(expected, createHash("sha256").update(adminToken).digest())
}

export function growthEndpoint(controller: GrowthController): string {
  return endpoints.get(controller)!
}

export async function growthRead(controller: GrowthController, operation: string, payload: JsonObject,
  options: DeadlineOptions & { requestId?: string } = {}): Promise<JsonObject> {
  const [, value] = await control.get(controller)!.request(operation, payload, options)
  return requireRecord(value)
}

export function growthOperation(controller: GrowthController, claimId: string, step: string, operation: string,
  payload: JsonObject, { deadline }: DeadlineOptions = {}): Promise<JsonObject> {
  return growthRead(controller, operation, payload, { requestId: uuid5(identity(claimId), step), deadline })
}

export async function advanceGrowth(controller: GrowthController, intent: JsonObject, provision: GrowthProvision,
  { mount, register, authorize, deadline }: AdvanceOptions = {}): Promise<JsonObject> {
  const claimId = identity(intent.claim_id)
  let record = await controller.status(claimId, { deadline })
  if (!equalWire(record.intent, intent) || !["claimed", "provisioning", "awaiting_mount", "complete"].includes(String(record.phase))) {
    throw new ProviderError("Database growth reply conflicts with its immutable intent")
  }
  if (record.phase === "complete") return record

  const action = requireRecord(intent.action)
  if (record.phase !== "awaiting_mount") {
    const current = Math.floor(Date.now() / 1_000)
    const previous = record.last_attempt
    if (previous != null && current - Number(previous) < Number(intent.cooldown_seconds)) return record
    const attempts = record.attempts
    if (typeof attempts !== "number" || !Number.isInteger(attempts) || attempts < 0 || attempts >= Number(intent.max_attempts)) {
      throw new ProviderError("Growth retry ceiling reached; existing claim and resources remain retained")
    }
    const attempt = attempts + 1
    await growthOperation(controller, claimId, "attempt-" + attempt, "storage.growth.attempt", { claim_id: claimId }, { deadline })
    await authorize?.("provisioning", attempt)
    let observed: JsonObject
    try {
      observed = await provision(intent)
    } catch (error) {
      if (!(error instanceof UnsupportedVolumeExpansion) || action.kind !== "expand") throw error
      return growthOperation(controller, claimId, "provisioned", "storage.growth.provisioned",
        { claim_id: claimId, resource_id: error.resourceId }, { deadline })
    }
    if (!isJsonObject(observed) || typeof observed.resource_id !== "string") {
      throw new ProviderError("Provider returned no exact resource identity")
    }
    record = await growthOperation(controller, claimId, "provisioned", "storage.growth.provisioned",
      { claim_id: claimId, resource_id: observed.resource_id }, { deadline })
  }
  if (register) {
    await authorize?.("awaiting_mount", Number(record.attempts))
    mount = await register(intent, record)
    if (mount === null && action.kind !== "expand") return controller.status(claimId, { deadline })
  }
  try {
    return await controller.mounted(claimId, { mount, deadline })
  } catch (error) {
    if (!(error instanceof StorageError)) throw error
    const current = await controller.status(claimId, { deadline })
    if (equalWire(current.intent, intent) && current.phase === "awaiting_mount" && current.mounted_total_bytes == null) {
      return current
    }
    throw error
  }
}

export class GrowthController {
  constructor({ apiUrl, adminToken, timeout = 20, caFile }: GrowthControllerOptions) {
    control.set(this, new ControlTransport(apiUrl, adminToken, { timeout, caFile }, operations))
    endpoints.set(this, apiUrl.replace(/\/+$/, ""))
    credentials.set(this, createHash("sha256").update(adminToken).digest())
  }

  status(claimId: string, { deadline }: DeadlineOptions = {}): Promise<JsonObject> {
    return growthOperation(this, claimId, "status", "storage.growth.status", { claim_id: claimId }, { deadline })
  }

  inventory(): Promise<JsonObject> {
    return growthRead(this, "storage.growth.inventory", {})
  }

  configure(policy: JsonObject): Promise<JsonObject> {
    return growthOperation(this, identity(policy.policy_id), "configure", "storage.growth.configure", { policy })
  }

  async reconcile({ intent, provision, mount }: ReconcileGrowthOptions): Promise<JsonObject> {
    if (typeof provision !== "function") {
      throw new ProviderError("Provisioning is unavailable without a configured stateful adapter")
    }
    const claimId = identity(intent.claim_id)
    await growthOperation(this, claimId, "claim", "storage.growth.claim", { intent })
    return advanceGrowth(this, intent, provision, { mount })
  }

  mounted(claimId: string, { mount, deadline }: MountOptions = {}): Promise<JsonObject> {
    const payload: JsonObject = { claim_id: claimId }
    if (mount != null) payload.mount = mount
    return growthOperation(this, claimId, "mounted", "storage.growth.mounted", payload, { deadline })
  }
}

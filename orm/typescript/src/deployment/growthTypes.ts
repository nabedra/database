import type { JsonObject } from "../transport.js"
import type { GrowthController } from "./controller.js"

export class HostObservation {
  constructor(
    readonly worker: JsonObject,
    readonly zone: string,
    readonly attachmentSlots: number,
    readonly expandableResources: ReadonlySet<string> = new Set(),
    readonly cpuLimitMillicores: number | bigint | null = null,
    readonly memoryLimitBytes: number | bigint | null = null,
  ) { Object.freeze(this) }
}

export class ProviderObservation {
  constructor(
    readonly hosts: Readonly<Record<string, HostObservation>>,
    readonly retainedWorkers: number,
    readonly monthlySpendFloor: number | bigint,
    readonly workerZones: ReadonlySet<string> = new Set(),
  ) { Object.freeze(this) }
}

export class GrowthTarget {
  constructor(
    readonly worker: JsonObject,
    readonly poolId: string,
    readonly poolGeneration: number | bigint,
    readonly resourceId: string | null = null,
  ) { Object.freeze(this) }
}

export interface StatefulGrowthAdapter {
  readonly clusterId: string
  readonly adapterId: string
  observe(inventories: Readonly<Record<string, JsonObject>>, options: {
    policy: JsonObject; deadline: number; claimId?: string | null
  }): ProviderObservation | Promise<ProviderObservation>
  target(claimId: string, action: JsonObject, inventories: Readonly<Record<string, JsonObject>>):
    GrowthTarget | null | Promise<GrowthTarget | null>
  provision(intent: JsonObject, target: GrowthTarget, options: { deadline: number }): JsonObject | Promise<JsonObject>
  register(intent: JsonObject, target: GrowthTarget, record: JsonObject, options: {
    controller: GrowthController; deadline: number
  }): JsonObject | null | Promise<JsonObject | null>
}

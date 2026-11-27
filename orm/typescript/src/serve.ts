import { randomBytes } from "node:crypto"
import { accessSync, constants, statSync } from "node:fs"
import { resolve } from "node:path"
import { CallbackBridge, type ProviderCallbacks } from "./callbackBridge.js"
import { ValidationError } from "./errors.js"
import { ensureSupervisorAvailable, superviseApi, type ServeStartup } from "./serveProcess.js"
import { validateIdentifier } from "./validation.js"

export type { StartNodeRequest } from "./callbackProtocol.js"

export interface ServeOptions {
  startNode?: ProviderCallbacks["startNode"] | null
  stopNode?: ProviderCallbacks["stopNode"] | null
  listNodes?: ProviderCallbacks["listNodes"] | null
  coldStart?: boolean
}

interface Reservation { active: boolean; bridge?: CallbackBridge }
let reservation: Reservation | undefined

function configPath(): string {
  try {
    const configured = process.env.NABEDRA_CONFIG
    if (configured) {
      const path = resolve(configured)
      if (statSync(path).isFile()) {
        accessSync(path, constants.R_OK)
        return path
      }
    }
  } catch {
    // The path can contain private configuration names.
  }
  throw new ValidationError("NABEDRA_CONFIG must name an explicit readable regular file.")
}

/** Own a configured POSIX API child until SIGINT or SIGTERM. Managed mode also owns the loopback callback bridge. */
export async function serve(
  name: string, nodes: number | readonly [number, number] = 1, options: ServeOptions = {},
): Promise<void> {
  const { startNode, stopNode, listNodes, coldStart = false } = options
  if (typeof coldStart !== "boolean") {
    throw new ValidationError("coldStart must be a boolean")
  }
  name = validateIdentifier(name)
  const managed = startNode != null || stopNode != null || listNodes != null
  let callbacks: ProviderCallbacks | undefined
  if (managed) {
    if (typeof startNode !== "function" || typeof stopNode !== "function" || typeof listNodes !== "function") {
      throw new ValidationError("Callbacks must be all callable or all absent")
    }
    callbacks = { startNode, stopNode, listNodes }
  }
  let policy: ServeStartup["initial_policy"]
  if (typeof nodes === "number" && Number.isInteger(nodes) && nodes >= 0 && nodes <= 1024) {
    policy = { mode: "fixed", desired: nodes, min_nodes: nodes, max_nodes: nodes }
  } else if (Array.isArray(nodes) && nodes.length === 2 && nodes.every(Number.isInteger)
    && nodes[0] >= 1 && nodes[0] <= nodes[1] && nodes[1] <= 1024) {
    if (!managed) throw new ValidationError("Tuple nodes require managed callbacks")
    policy = { mode: "autoscale", desired: nodes[0], min_nodes: nodes[0], max_nodes: nodes[1] }
  } else {
    throw new ValidationError("Nodes must be an integer in 0..1024 or a two-integer tuple with 1 <= min <= max <= 1024")
  }
  const config = configPath()
  if (reservation?.active || reservation?.bridge?.alive) throw new Error("Serve runtime unavailable")
  ensureSupervisorAvailable()
  const owner: Reservation = { active: true }
  reservation = owner
  const initial = { version: 1, type: "init", name, initial_policy: policy } as const
  try {
    let startup: ServeStartup
    if (callbacks) {
      const callbackToken = randomBytes(32).toString("base64url")
      const authorizeToken = randomBytes(32).toString("base64url")
      const bridge = await CallbackBridge.open(callbacks, callbackToken, authorizeToken)
      owner.bridge = bridge
      startup = {
        ...initial, provisioning: "managed",
        bridge: { url: bridge.url, callback_token: callbackToken, authorize_token: authorizeToken },
      }
    } else {
      startup = { ...initial, provisioning: "external", bridge: null }
    }
    await superviseApi(config, startup, {
      coldStart,
      onPrepared: ({ cluster_id, authorize_url }) => {
        if (owner.bridge && authorize_url !== null) owner.bridge.activate(cluster_id, authorize_url)
      },
      onStop: () => owner.bridge?.stopAdmission(),
    })
  } finally {
    try {
      await owner.bridge?.close()
    } finally {
      owner.active = false
    }
  }
}

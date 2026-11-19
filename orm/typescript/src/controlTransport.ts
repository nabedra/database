import { randomUUID } from "node:crypto"
import { LimitExceededError, NabedraError, ProtocolError, TransportError, UnknownOutcomeError, ValidationError } from "./errors.js"
import { encodeJson, errorMappings, exchangeMayHaveSent, hasFields, integerNumberToken, isJsonObject, parseJson, Transport,
  type HttpResponse, type JsonObject } from "./transport.js"

const maxBody = 1024 * 1024
const clusterOperations = {
  mutations: new Set(["cluster.scale", "cluster.autoscale"]),
  reads: new Set(["cluster.status"]),
}
const canonicalUuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i

export class ControlTransport {
  readonly #transport: Transport
  readonly #decodeIntegers: "bigint" | undefined
  readonly #operations: { mutations: ReadonlySet<string>; reads: ReadonlySet<string> }

  constructor(apiUrl: string, adminToken: string, options: { timeout?: number; caFile?: string | null } = {},
    operations: { mutations: ReadonlySet<string>; reads: ReadonlySet<string> } = clusterOperations) {
    if (typeof adminToken !== "string") throw new ValidationError("Token must be a nonempty safe bearer token")
    this.#transport = new Transport(apiUrl, adminToken, { ...options, agent: false })
    this.#operations = operations
    this.#decodeIntegers = operations === clusterOperations ? "bigint" : undefined
  }

  async request(operation: string, payload: JsonObject, { requestId = randomUUID(), deadline }: {
    requestId?: string
    deadline?: number
  } = {}): Promise<[string, unknown]> {
    const mutation = this.#operations.mutations.has(operation)
    if (!mutation && !this.#operations.reads.has(operation)) throw new ValidationError("Unsupported control operation")
    if (!isJsonObject(payload)) throw new ValidationError("Operation payload must be a dictionary")
    if (typeof requestId !== "string" || !canonicalUuid.test(requestId)) {
      throw new ValidationError("Request ID must be a UUID string")
    }
    let body: Buffer
    try {
      body = encodeJson({ request_id: requestId, operation, payload })
    } catch {
      throw new ValidationError("Operation payload must encode as finite UTF-8 JSON", { requestId })
    }
    if (body.length > maxBody) throw new LimitExceededError("Encoded control request exceeds 1 MiB", { requestId })

    let response: HttpResponse
    try {
      response = await this.#transport.exchange("POST", "/v1/cluster/operations", {
        body, headers: { "Content-Type": "application/json", Accept: "application/json" },
        deadline, maxBytes: maxBody,
      })
    } catch (error) {
      if (!(error instanceof TransportError || error instanceof ProtocolError)) throw error
      const type = mutation && exchangeMayHaveSent(error) ? UnknownOutcomeError
        : error instanceof TransportError ? TransportError : ProtocolError
      throw new type(error instanceof TransportError ? "Control response lost during HTTP I/O" : "Malformed control HTTP response",
        { requestId, statusCode: error.statusCode })
    }
    const { status, headers } = response
    const failure = (message: string): NabedraError => {
      const type = mutation ? UnknownOutcomeError : ProtocolError
      return new type(message, { requestId, statusCode: status })
    }
    if (headers["content-type"]?.split(";", 1)[0].trim().toLowerCase() !== "application/json") {
      throw failure("Control response is not application/json")
    }
    let envelope: unknown
    try {
      envelope = parseJson(response.body, this.#decodeIntegers ? { integers: this.#decodeIntegers } : { numberToken: integerNumberToken })
    } catch {
      throw failure("Malformed control response JSON")
    }
    if (status === 200) {
      if (!hasFields(envelope, "request_id", "data") || envelope.request_id !== requestId) {
        throw failure("Invalid control success envelope or request ID")
      }
      return [requestId, envelope.data]
    }
    if (status < 400 || status > 599) throw failure("Invalid control response status")
    if (!hasFields(envelope, "request_id", "error") || (envelope.request_id !== null && envelope.request_id !== requestId)) {
      throw failure("Invalid control error envelope or request ID")
    }
    const details = envelope.error
    if (!hasFields(details, "code", "message", "outcome") || typeof details.code !== "string" || !details.code
        || typeof details.message !== "string" || (details.outcome !== "not_applied" && details.outcome !== "unknown")) {
      throw failure("Invalid control error details")
    }
    const { code, outcome } = details
    const known = errorMappings.get(code)
    if ((known && status !== known.status) || (code === "outcome_unknown" && outcome !== "unknown")) {
      throw failure("Control error code/status/outcome mismatch")
    }
    const type = outcome === "unknown" ? UnknownOutcomeError : known?.type ?? NabedraError
    throw new type("Control operation rejected", { code, outcome, requestId, statusCode: status })
  }
}

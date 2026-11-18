export interface NabedraErrorOptions {
  code?: string | null
  requestId?: string | null
  statusCode?: number | null
  outcome?: string | null
}

export class NabedraError extends Error {
  static readonly code: string = "nabedra_error"
  protected static readonly defaultOutcome: string | null = null

  code: string
  requestId: string | null
  statusCode: number | null
  outcome: string | null

  constructor(message: string, options: NabedraErrorOptions = {}) {
    super(message)
    this.name = new.target.name
    this.code = options.code ?? new.target.code
    this.requestId = options.requestId ?? null
    this.statusCode = options.statusCode ?? null
    this.outcome = options.outcome ?? new.target.defaultOutcome
  }
}

export class ValidationError extends NabedraError {
  static override readonly code = "validation_error"
  protected static override readonly defaultOutcome = "not_applied"
}

export class AuthenticationError extends NabedraError {
  static override readonly code = "unauthorized"
  protected static override readonly defaultOutcome = "not_applied"
}

export class NotFoundError extends NabedraError {
  static override readonly code = "not_found"
  protected static override readonly defaultOutcome = "not_applied"
}

export class ConflictError extends NabedraError {
  static override readonly code = "conflict"
  protected static override readonly defaultOutcome = "not_applied"
}

export class LimitExceededError extends NabedraError {
  static override readonly code = "limit_exceeded"
  protected static override readonly defaultOutcome = "not_applied"
}

export class OverloadedError extends NabedraError {
  static override readonly code = "overloaded"
  protected static override readonly defaultOutcome = "not_applied"
}

export class StorageError extends NabedraError {
  static override readonly code = "storage_unavailable"
  protected static override readonly defaultOutcome = "not_applied"
}

export class TransportError extends NabedraError {
  static override readonly code = "transport_error"
}

export class ProtocolError extends NabedraError {
  static override readonly code = "protocol_error"
}

export class UnknownOutcomeError extends NabedraError {
  static override readonly code = "outcome_unknown"
  protected static override readonly defaultOutcome = "unknown"
}

export class StreamUnsupportedError extends NabedraError {
  static override readonly code = "unsupported_operation"
  protected static override readonly defaultOutcome = "not_applied"
}

export class StreamExpiredError extends NabedraError {
  static override readonly code = "stream_expired"
  protected static override readonly defaultOutcome = "not_applied"
}

export class StreamAbandonedError extends NabedraError {
  static override readonly code = "stream_abandoned"
  protected static override readonly defaultOutcome = "not_applied"
}

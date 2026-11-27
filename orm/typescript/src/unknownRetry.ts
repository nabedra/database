/**
 * Internal: the same-identity resend of a mutation whose outcome is unknown,
 * keyed by its error. The transport attaches it; `UnknownOutcomeError.retry` runs it.
 */
const retries = new WeakMap<object, () => Promise<unknown>>()

export function attachRetry(error: object, retry: () => Promise<unknown>): void {
  retries.set(error, retry)
}

export function retryOf(error: object): (() => Promise<unknown>) | undefined {
  return retries.get(error)
}

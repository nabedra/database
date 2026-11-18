import { inspect } from "node:util"

export interface AwsCredentialsOptions {
  sessionToken?: string | null
  expiresAt?: Date | null
}

export type AwsCredentialsProvider = () => AwsCredentials | Promise<AwsCredentials>

function printable(value: unknown, limit: number): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= limit && !/[^ -~]/.test(value)
}

export class AwsCredentials {
  readonly accessKeyId: string
  readonly secretAccessKey: string
  readonly sessionToken: string | null
  readonly #expiresAt: number | null

  constructor(accessKeyId: string, secretAccessKey: string, {
    sessionToken = null,
    expiresAt = null,
  }: AwsCredentialsOptions = {}) {
    if (!printable(accessKeyId, 4096) || !printable(secretAccessKey, 4096)
      || (sessionToken !== null && !printable(sessionToken, 8192))) {
      throw new TypeError("Invalid AWS credentials")
    }
    if (expiresAt !== null && (!(expiresAt instanceof Date) || !Number.isFinite(expiresAt.getTime())
      || expiresAt.getUTCFullYear() < 1 || expiresAt.getUTCFullYear() > 9999)) {
      throw new TypeError("Credential expiry must be a valid Date")
    }
    this.accessKeyId = accessKeyId
    this.secretAccessKey = secretAccessKey
    this.sessionToken = sessionToken
    this.#expiresAt = expiresAt?.getTime() ?? null
    Object.freeze(this)
  }

  get expiresAt(): Date | null {
    return this.#expiresAt === null ? null : new Date(this.#expiresAt)
  }

  [inspect.custom](): string {
    return "AwsCredentials(<redacted>)"
  }
}

export function defaultCredentials(profile?: string | null): () => Promise<AwsCredentials> {
  type Provider = ReturnType<typeof import("@aws-sdk/credential-provider-node").defaultProvider>
  let provider: Promise<Provider> | undefined

  return async () => {
    try {
      provider ??= import("@aws-sdk/credential-provider-node").then(({ defaultProvider }) =>
        defaultProvider(profile == null ? {} : { profile }))
      const resolve = await provider
      const credentials = await resolve()
      return new AwsCredentials(credentials.accessKeyId, credentials.secretAccessKey, {
        sessionToken: credentials.sessionToken,
        expiresAt: credentials.expiration,
      })
    } catch {
      throw new TypeError("AWS credential discovery failed")
    }
  }
}

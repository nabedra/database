#!/usr/bin/env node
import { spawn, type ChildProcess } from "node:child_process"
import { randomBytes, randomUUID } from "node:crypto"
import { once } from "node:events"
import { existsSync, mkdtempSync, realpathSync, rmSync, unlinkSync } from "node:fs"
import { createServer } from "node:net"
import { constants, tmpdir } from "node:os"
import { extname, join, resolve } from "node:path"
import { createInterface } from "node:readline/promises"
import { fileURLToPath } from "node:url"
import { parseArgs, type ParseArgsConfig } from "node:util"
import { binaryVersion, resolveServerBinary } from "./binary.js"
import { ControlTransport } from "./controlTransport.js"
import { ConfigError, defaultResources, example, fields, load, origin, protectedWrite,
  readFile, readJson, resolvePaths, s3Storage, validate } from "./cliConfig.js"
import { NabedraError, ValidationError } from "./errors.js"
import { superviseApi } from "./serveProcess.js"
import { encodeJson, isJsonObject, type JsonObject } from "./transport.js"
import { validateIdentifier } from "./validation.js"

const configName = "nabedra-database.json"
const cliVersion = "0.2.1"
const binaryFailure = "Cannot launch compatible nabedra-db. Set NABEDRA_SERVER_BINARY to an executable file path or put nabedra-db on PATH."
const tokenKeys = ["admin_token", "data_token", "enrollment_token"] as const
const signaled = ["SIGINT", "SIGTERM"] as const
const commandNames = ["init", "serve", "doctor", "worker", "cluster", "migrate"] as const
type Command = typeof commandNames[number]
type Auth = Record<typeof tokenKeys[number], string>
type Values = Record<string, string | boolean | (string | boolean)[] | undefined>

function options(strings: readonly string[], booleans: readonly string[] = []): NonNullable<ParseArgsConfig["options"]> {
  return Object.fromEntries([
    ...strings.map(key => [key, { type: "string" }]),
    ...booleans.map(key => [key, { type: "boolean" }]),
  ])
}

const commandOptions: Record<Command, NonNullable<ParseArgsConfig["options"]>> = {
  init: options(["mode", "storage", "control-storage", "region", "auth-file", "output", "threads-per-worker",
    "data-token-file", "admin-token-file", "enrollment-token-file"], ["non-interactive"]),
  serve: options(["cluster-name", "config", "mode", "storage", "region", "local-workers", "threads-per-worker"], ["cold-start"]),
  doctor: options(["config"], ["json"]),
  worker: options(["bootstrap"]),
  cluster: options(["url", "admin-token-file", "ca-file"], ["json"]),
  migrate: options(["storage", "region", "cluster-id"], ["confirm-writers-stopped"]),
}

function string(values: Values, key: string): string | undefined {
  return typeof values[key] === "string" ? values[key] : undefined
}

function required(values: Values, key: string): string {
  const value = string(values, key)
  if (!value) throw new ConfigError("Missing argument")
  return value
}

function count(value: string | undefined, defaultValue: number, minimum: number, maximum: number): number {
  if (value === undefined) return defaultValue
  const parsed = Number(value)
  if (!/^[+-]?\d+(?![\s\S])/.test(value.trim()) || parsed < minimum || parsed > maximum) {
    throw new ConfigError("Invalid count")
  }
  return parsed
}

function token(value: unknown): string {
  if (typeof value !== "string" || !/^[!-~]{43,4096}(?![\s\S])/.test(value)) {
    throw new ConfigError("Invalid protected token file")
  }
  return value
}

function readToken(path: string, key?: keyof Auth): string {
  const bytes = readFile(path, { secret: true, limit: key ? 16_384 : 4096 })
  if (bytes.some(byte => byte > 127)) throw new ConfigError("Invalid protected token file")
  const data = bytes.toString("ascii").trim()
  return token(key && data.startsWith("{") ? fields(readJson(path, true), tokenKeys)[key] : data)
}

function auth(config: JsonObject): Auth {
  const record = fields(readJson(String(config.auth_file), true), tokenKeys)
  const parsed = Object.fromEntries(tokenKeys.map(key => [key, token(record[key])])) as Auth
  if (new Set(Object.values(parsed)).size !== 3) throw new ConfigError("Admin, data and enrollment tokens must differ")
  return parsed
}

function printJson(value: unknown, pretty = false): void {
  if (!pretty) {
    process.stdout.write(encodeJson(value).toString("utf8") + "\n")
    return
  }
  const json = JSON as typeof JSON & { rawJSON(value: string): object }
  process.stdout.write(JSON.stringify(value, (_, item: unknown) =>
    typeof item === "bigint" ? json.rawJSON(item.toString()) : item, 2) + "\n")
}

async function init(values: Values): Promise<number> {
  const interactive = !values["non-interactive"] && process.stdin.isTTY
    ? createInterface({ input: process.stdin, output: process.stdout }) : undefined
  const ask = async (key: string, prompt: string): Promise<string> => {
    const value = string(values, key)
    if (value) return value
    if (!interactive) throw new ConfigError("Missing settings")
    return (await interactive.question(prompt + ": ")).trim()
  }
  try {
    const mode = await ask("mode", "Mode (fixed/serverless)")
    if ((mode !== "fixed" && mode !== "serverless")
      || (mode === "fixed" && values["control-storage"]) || (mode === "serverless" && values.storage)) {
      throw new ConfigError("Invalid mode or storage")
    }
    const storage = s3Storage(await ask(mode === "fixed" ? "storage" : "control-storage", "S3 storage URI"),
      await ask("region", "AWS region"))
    const output = resolve(string(values, "output") ?? configName)
    const authFlag = string(values, "auth-file")
    const extension = extname(output)
    const authFile = authFlag ? resolve(authFlag) : output.slice(0, output.length - extension.length) + ".auth.json"
    if (existsSync(output) || (!authFlag && existsSync(authFile))) throw new ConfigError("Refusing to overwrite configuration or credentials")
    const config = validate(example(mode, storage, authFile, count(string(values, "threads-per-worker"), 4, 1, 256)))
    const flags = ["admin-token-file", "data-token-file", "enrollment-token-file"]
    if (authFlag) {
      if (flags.some(flag => values[flag])) throw new ConfigError("Use auth-file or individual token files")
      auth(config)
    } else {
      const credentials = Object.fromEntries(tokenKeys.map((key, index) => {
        const path = string(values, flags[index])
        return [key, path ? readToken(path) : randomBytes(32).toString("base64url")]
      }))
      if (new Set(Object.values(credentials)).size !== 3) throw new ConfigError("Admin, data and enrollment tokens must differ")
      protectedWrite(authFile, credentials)
    }
    try {
      protectedWrite(output, config)
    } catch (error) {
      if (!authFlag) unlinkSync(authFile)
      throw error
    }
    printJson({ config: output, auth_file: authFile })
    return 0
  } finally {
    interactive?.close()
  }
}

async function ephemeralPort(): Promise<number> {
  const server = createServer()
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const address = server.address()
  server.close()
  await once(server, "close")
  if (address === null || typeof address === "string") throw new Error("Worker port unavailable")
  return address.port
}

function observe(child: ChildProcess): Promise<{ code: number; signal: NodeJS.Signals | null }> {
  return new Promise(resolve => {
    child.once("close", (code, signal) => resolve({ code: code ?? 1, signal }))
    child.once("error", () => resolve({ code: 1, signal: null }))
  })
}

async function supervise(binary: string, config: JsonObject, workers: number, name: string, coldStart: boolean): Promise<number> {
  const embeddedOltp = config.engine === "oltp"
  if (embeddedOltp && workers !== 1) throw new ConfigError("OLTP local-workers must be 1")
  const credentials = auth(config)
  const directory = mkdtempSync(join(tmpdir(), "nabedra-"))
  const path = join(directory, "api.json")
  const children: { process: ChildProcess; exited: ReturnType<typeof observe> }[] = []
  const workerFailure = new AbortController()
  let workerExit: number | undefined
  let stopping = false
  try {
    protectedWrite(path, config)
    const state = await superviseApi(path, {
      version: 1, type: "init", name,
      initial_policy: { mode: "fixed", desired: workers, min_nodes: workers, max_nodes: workers },
      provisioning: "external", bridge: null,
    }, {
      binary, coldStart, announcement: "nabedra.serve.launched", failureSignal: workerFailure.signal,
      async onReady() {
        for (let index = 0; index < (embeddedOltp ? 0 : workers); ++index) {
          const id = randomUUID()
          const bind = "127.0.0.1:" + await ephemeralPort()
          const file = join(directory, id + ".json")
          protectedWrite(file, {
            ...(config.worker as JsonObject),
            version: config.version,
            ...(config.version === 2 ? { mode: config.mode } : {}),
            cluster_id: config.cluster_id, node_id: id, request_id: null, api_urls: config.api_urls,
            storage: config.storage, enrollment_token: credentials.enrollment_token, tls: config.tls,
            listen: bind, advertise_url: `${config.tls ? "https" : "http"}://${bind}`,
          })
          const child = spawn(binary, ["worker", "--bootstrap", file], { stdio: "ignore", detached: true })
          const exited = observe(child)
          children.push({ process: child, exited })
          void exited.then(({ code }) => {
            if (!stopping) {
              workerExit ??= code || 1
              workerFailure.abort()
            }
          })
        }
      },
      async onStop(signal) {
        stopping = true
        const lifecycle = config.lifecycle
        const drain = isJsonObject(lifecycle) ? Number(lifecycle.drain_ms ?? 300_000) : 300_000
        await drainChildren(children, signal ?? "SIGTERM", drain)
      },
    })
    return state.signal ? 128 + constants.signals[state.signal] : workerExit ?? 0
  } catch (error) {
    if (workerExit !== undefined) return workerExit
    throw error
  } finally {
    stopping = true
    await drainChildren(children, "SIGKILL", 1_000)
    rmSync(directory, { recursive: true, force: true })
  }
}

async function drainChildren(children: { process: ChildProcess; exited: ReturnType<typeof observe> }[],
  signal: NodeJS.Signals, milliseconds: number): Promise<void> {
  for (const child of children) child.process.kill(signal)
  const kill = setTimeout(() => {
    for (const child of children) child.process.kill("SIGKILL")
  }, Math.min(milliseconds, 2 ** 31 - 1))
  try {
    await Promise.all(children.map(child => child.exited))
  } finally {
    clearTimeout(kill)
  }
}

async function start(values: Values): Promise<number> {
  const workers = count(string(values, "local-workers"), 1, 0, 1_024)
  const threads = values["threads-per-worker"] === undefined ? undefined
    : count(string(values, "threads-per-worker"), 4, 1, 256)
  const name = validateIdentifier(string(values, "cluster-name") ?? "Nabedra")
  const storage = string(values, "storage")
  const mode = string(values, "mode")
  const path = string(values, "config")
  const region = string(values, "region")
  const coldStart = values["cold-start"] === true
  if (storage) {
    if (path || (mode && mode !== "fixed")) throw new ConfigError("Direct storage requires fixed mode and no config")
    const settings = s3Storage(storage, region)
    const dataToken = token(process.env.NABEDRA_TOKEN)
    const directory = mkdtempSync(join(tmpdir(), "nabedra-fixed-"))
    const authFile = join(directory, "auth.json")
    try {
      protectedWrite(authFile, { data_token: dataToken, admin_token: randomBytes(32).toString("base64url"),
        enrollment_token: randomBytes(32).toString("base64url") })
      const config = validate(example("fixed", settings, authFile, threads ?? 4))
      return await supervise(resolveServerBinary({ check: true }), config, workers, name, coldStart)
    } finally {
      rmSync(directory, { recursive: true, force: true })
    }
  }
  if (mode || region) throw new ConfigError("Mode and region require storage")
  const source = path ?? configName
  const config = resolvePaths(load(source), source)
  if (threads !== undefined) {
    const worker = isJsonObject(config.worker) ? config.worker : (config.worker = {})
    if (isJsonObject(worker.resources)) worker.resources.max_threads = threads
    else worker.resources = defaultResources(threads)
    if (config.engine !== "oltp") worker.partition_slots = threads
    validate(config)
  }
  auth(config)
  return supervise(resolveServerBinary({ check: true }), config, workers, name, coldStart)
}

async function forward(args: string[]): Promise<number> {
  const child = spawn(resolveServerBinary({ check: true }), args, { stdio: "inherit" })
  const listeners = signaled.map(signal => {
    const handler = () => { child.kill(signal) }
    process.on(signal, handler)
    return { signal, handler }
  })
  try {
    const { code, signal } = await observe(child)
    return signal ? 128 + constants.signals[signal] : code
  } finally {
    for (const { signal, handler } of listeners) process.off(signal, handler)
  }
}

function usage(command?: Command): void {
  const base = "Usage: nabedra [--version] [--json] | nabedra database <command>"
  if (!command) {
    process.stdout.write(base + "\nDatabase commands: " + commandNames.join(", ") + "\n")
    return
  }
  const flags = Object.entries(commandOptions[command]).map(([key, option]) =>
    "  --" + key + (option.type === "string" ? " <value>" : "")).join("\n")
  process.stdout.write("Usage: nabedra database " + command
    + (command === "cluster" ? " <status|scale> [nodes]" : "") + " [options]\n" + flags + "\n")
}

export async function main(argv: readonly string[] = process.argv.slice(2)): Promise<number> {
  try {
    const scope = argv.indexOf("database")
    const prefix = argv.slice(0, scope === -1 ? undefined : scope)
    const groupOptions = options([], ["version", "json", "help"])
    const global = parseArgs({ args: prefix, options: groupOptions }).values
    const argumentsAfterScope = scope === -1 ? [] : argv.slice(scope + 1)
    const commandIndex = argumentsAfterScope.findIndex(argument => !argument.startsWith("-"))
    const groupArguments = argumentsAfterScope.slice(0, commandIndex === -1 ? undefined : commandIndex)
    const group = parseArgs({ args: groupArguments, options: groupOptions }).values
    const [commandArgument, ...commandArgs] = commandIndex < 0 ? [] : argumentsAfterScope.slice(commandIndex)
    if (global.help || group.help) { usage(); return 0 }
    const json = group.json || global.json
    if (global.version || group.version) {
      if (commandArgument !== undefined) throw new ConfigError("Version does not accept a command")
      const server = binaryVersion(resolveServerBinary())
      const report = { cli: cliVersion, binary: server, config: 2, protocol: 1 }
      if (json) printJson(report)
      else process.stdout.write(`nabedra ${cliVersion} (binary ${server}, config 2, protocol 1)\n`)
      return 0
    }
    if (!commandNames.includes(commandArgument as Command)) throw new ConfigError("Unsupported command")
    const command = commandArgument as Command
    const argumentsForParsing = commandArgs[0] === "status" || commandArgs[0] === "scale" ? commandArgs.slice(1) : commandArgs
    const { values, positionals }: { values: Values; positionals: string[] } = parseArgs({
      args: command === "cluster" ? argumentsForParsing : commandArgs,
      options: { ...commandOptions[command], help: { type: "boolean" } }, allowPositionals: command === "cluster",
    })
    if (values.help) { usage(command); return 0 }
    if (command === "init") return await init(values)
    if (command === "serve") return await start(values)
    if (command === "worker") return await forward(["worker", "--bootstrap", required(values, "bootstrap")])
    if (command === "cluster") {
      const url = required(values, "url")
      origin(url)
      const actions = { status: 0, scale: 1 } as const
      const action = commandArgs[0]
      if ((action !== "status" && action !== "scale") || positionals.length !== actions[action]) {
        throw new ConfigError("Invalid cluster action")
      }
      const control = new ControlTransport(url, readToken(required(values, "admin-token-file"), "admin_token"),
        { caFile: string(values, "ca-file") })
      const payload = action === "scale" ? { nodes: count(positionals[0], 0, 0, 1024) } : {}
      const [, result] = await control.request("cluster." + action, payload, { deadline: performance.now() / 1_000 + 30 })
      printJson(result, !values.json && !json)
      return 0
    }
    if (command === "migrate") {
      const storage = required(values, "storage")
      const region = required(values, "region")
      const clusterId = required(values, "cluster-id")
      s3Storage(storage, region)
      if (!values["confirm-writers-stopped"] || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(clusterId)) {
        throw new ConfigError("Migration requires valid cluster identity and explicit confirmation")
      }
      return await forward(["database", "migrate", "--storage", storage, "--region", region, "--cluster-id", clusterId,
        "--confirm-writers-stopped"])
    }
    const source = string(values, "config") ?? configName
    const config = resolvePaths(load(source), source)
    auth(config)
    if (isJsonObject(config.tls)) {
      for (const [key, path] of Object.entries(config.tls)) {
        if (typeof path === "string") readFile(path, { secret: key === "key_file" })
      }
    }
    if (!values.json && !json) {
      process.stdout.write("Config and secret permissions checked. Storage writes remain unverified.\n")
    }
    return await forward(["doctor", "--config", resolve(source), ...(values.json || json ? ["--json"] : [])])
  } catch (error) {
    if (error instanceof ConfigError || error instanceof ValidationError || error instanceof TypeError
      || (error as NodeJS.ErrnoException | null)?.code === "ENOENT") {
      process.stderr.write("nabedra: invalid configuration or arguments; see --help\n")
      return 2
    }
    if (error instanceof NabedraError) {
      process.stderr.write("nabedra: operation failed; outcome may be unknown. No automatic retry was attempted.\n")
      return 1
    }
    process.stderr.write(binaryFailure + "\n")
    return 1
  }
}

if (process.argv[1] && existsSync(process.argv[1]) && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = await main()
}

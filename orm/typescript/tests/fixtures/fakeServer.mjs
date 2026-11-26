// A scripted fake Nabedra server for adversarial SDK tests.
import { once } from "node:events"
import { createServer } from "node:http"

export const docs = {
  id: "00000000-0000-0000-0000-000000000021", name: "docs", key: "id", format: "parquet",
  schema: {
    id: "utf8", n: "int64", f: "float64", b: "bool", vec: { type: "vector", dim: 2 },
    sp: { type: "vector", sparse: true },
    mu: { type: "list", item: { type: "vector", dim: 2 } }, tags: { type: "list", item: "utf8" },
  },
}
export const plain = {
  id: "00000000-0000-0000-0000-000000000022", name: "plain", key: "id", format: "parquet",
  schema: { id: "utf8", n: "int64", f: "float64", b: "bool" },
}
export const chunks = {
  id: "00000000-0000-0000-0000-000000000023", name: "chunks", key: "id", format: "parquet",
  schema: {
    id: "utf8", body: "utf8", note: "utf8", year: "int64", emb: { type: "vector", dim: 3 },
    sp: { type: "vector", sparse: true }, mu: { type: "list", item: { type: "vector", dim: 2 } },
    tags: { type: "list", item: "utf8" },
  },
  full_text: { body: "english" }, embed: { emb: { source: "body", model: "m1" } },
}
export const lean = {
  id: "00000000-0000-0000-0000-000000000024", name: "lean", key: "id", format: "parquet",
  schema: { id: "utf8", body: "utf8", emb: { type: "vector", dim: 3 } },
}
export const numbered = {
  id: "00000000-0000-0000-0000-000000000025", name: "numbered", key: "id", format: "parquet",
  schema: { id: "int64", vec: { type: "vector", dim: 1 } },
}
export const tables = Object.fromEntries([docs, plain, chunks, lean, numbered].map((table) => [table.name, table]))
const statuses = {
  validation_error: 400, unauthorized: 401, not_found: 404, conflict: 409, limit_exceeded: 413, overloaded: 429,
  storage_unavailable: 503, search_unavailable: 503, outcome_unknown: 503,
}

export const ok = (data) => ({ data })
export const failure = (code, message = "m", outcome = "not_applied", status = undefined) => ({
  status: status ?? statuses[code] ?? 500, error: { code, message, outcome },
})

/** Replies come from a script: one entry per non-describe request, the last repeating. */
export class Fake {
  requests = []
  script = []
  #server

  static async start() {
    const fake = new Fake()
    fake.#server = createServer((request, response) => fake.#handle(request, response))
    fake.#server.listen(0, "127.0.0.1")
    await once(fake.#server, "listening")
    fake.url = `http://127.0.0.1:${fake.#server.address().port}`
    return fake
  }

  reset(...script) {
    this.requests = []
    this.script = script
  }

  close() {
    this.#server.closeAllConnections()
    return new Promise((resolve) => this.#server.close(resolve))
  }

  async #handle(request, response) {
    const planned = this.script[Math.min(this.requests.length, this.script.length - 1)]
    if (planned?.cut && request.headers["x-nabedra-describe"] === undefined) {
      // Read the start of the body, which tells whether this is a describe, then cut the connection.
      const [first] = await once(request, "data")
      if (!first.toString("utf8").includes('"operation":"table.describe"')) {
        this.requests.push({ raw: first, request: { operation: "cut", payload: {} } })
        return request.socket.destroy()
      }
      request.unshift(first)
    }
    const parts = []
    for await (const part of request) parts.push(part)
    const raw = Buffer.concat(parts)
    const parsed = JSON.parse(raw.toString("utf8"))
    const id = JSON.stringify(parsed.request_id)
    const reply = (status, body, type = "application/json") => {
      const encoded = Buffer.isBuffer(body) ? body : Buffer.from(body ?? "", "utf8")
      response.writeHead(status, { ...(type ? { "Content-Type": type } : {}), "Content-Length": encoded.length })
      response.end(encoded)
    }
    if (parsed.operation === "table.describe") {
      const table = tables[parsed.payload.name]
      return reply(200, `{"request_id":${id},"data":${typeof table === "string" ? table : JSON.stringify(table)}}`)
    }
    this.requests.push({ raw, request: parsed })
    const index = this.requests.length - 1
    let answer = this.script.length > 0 ? this.script[Math.min(index, this.script.length - 1)] : ok("null")
    if (typeof answer === "function") answer = answer(parsed)
    if (answer.drop) return request.socket.destroy()
    if (answer.sleep) await new Promise((resolve) => setTimeout(resolve, answer.sleep * 1000))
    if (answer.partial) {
      response.writeHead(200, { "Content-Type": "application/json", "Content-Length": 500 })
      response.write('{"request_id":')
      return setTimeout(() => request.socket.destroy(), 20)
    }
    let body = answer.raw?.replaceAll("$ID", id)
    if (answer.echo_create) {
      body = `{"request_id":${id},"data":${JSON.stringify({ id: "00000000-0000-0000-0000-000000000040", ...parsed.payload })}}`
    }
    if (answer.repeat) body = `{"request_id":${id},"data":[${Array(answer.repeat[1]).fill(answer.repeat[0]).join(",")}]}`
    if (answer.applied !== undefined) {
      const outcomes = [...Array.from({ length: answer.applied }, () => ({ o: "applied" })), ...(answer.tail ?? [])]
      body = `{"request_id":${id},"data":${JSON.stringify({ outcomes })}}`
    }
    if (answer.fill !== undefined) body = `{"request_id":${id},"data":{"outcomes":[],"pad":"${"x".repeat(answer.fill)}"}}`
    if (body === undefined && answer.error) body = `{"request_id":${id},"error":${JSON.stringify(answer.error).replace("$FILL", "m".repeat(1_000_000))}}`
    else if (body === undefined && answer.data !== undefined) body = `{"request_id":${id},"data":${answer.data}}`
    if (answer.rawhex !== undefined) {
      body = Buffer.from(answer.rawhex.replaceAll("$ID", Buffer.from(id).toString("hex")), "hex")
    }
    reply(answer.status ?? 200, body, answer.type ?? "application/json")
  }
}

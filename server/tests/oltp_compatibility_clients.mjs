import assert from "node:assert/strict"
import { AuthenticationError, Database, NotFoundError } from "../../orm/typescript/dist/index.js"

const input = []
for await (const chunk of process.stdin) input.push(chunk)
const { url, token, database, forbiddenDatabase, foreignToken, name, id, total, limit } =
  JSON.parse(Buffer.concat(input))
const options = { url, token, database, timeout: 120 }
const db = new Database(options)
await assert.rejects(() => new Database({ ...options, token: "invalid-o01-token" }).tables(),
  error => error instanceof AuthenticationError && error.outcome === "not_applied")
if (forbiddenDatabase) {
  await assert.rejects(() => new Database({ ...options, database: forbiddenDatabase }).tables(),
    error => error instanceof AuthenticationError && error.outcome === "not_applied")
}
if (foreignToken) {
  await assert.rejects(() => new Database({ ...options, token: foreignToken }).tables(),
    error => error instanceof AuthenticationError && error.outcome === "not_applied")
}
if (total) {
  const table = await db.table(name)
  assert.equal(await table.query().count(), total)
  assert.equal(await table.query().limit(limit).count(), limit)
  assert.deepEqual(await table.get(BigInt(total - 1)), { id: BigInt(total - 1) })
} else {
  assert.ok(!(await db.tables()).some(table => table.name === name || table.id === id))
  await assert.rejects(() => db.table(name),
    error => error instanceof NotFoundError && error.outcome === "not_applied")
}

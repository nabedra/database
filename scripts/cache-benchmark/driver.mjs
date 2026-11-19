import { readFile, writeFile } from "node:fs/promises";
import { setImmediate, setTimeout } from "node:timers/promises";
import { gzip } from "node:zlib";
import { promisify } from "node:util";
import { Cache } from "nabedra/cache";

const [specPath, processIndexText, outputPath] = process.argv.slice(2);
const spec = JSON.parse(await readFile(specPath, "utf8"));
const processIndex = Number(processIndexText);
const value = Buffer.from(spec.value_b64, "base64");
const warmValue = Buffer.from(value);
warmValue[0] ^= 0xff;
const ns = 1_000_000_000n;
const timeoutNs = BigInt(spec.timeout_ms) * 1_000_000n;
const endpoint = spec.endpoints ?? `${spec.tls ? "rediss" : "redis"}://127.0.0.1:${spec.port}`;

async function connect() {
  const cache = await Cache.connect(endpoint, {
    namespace: "bench",
    username: spec.username,
    password: spec.password,
    cluster: spec.cluster ?? false,
    caFile: spec.tls ? spec.certificate : undefined,
    poolSize: spec.cluster ? spec.endpoints.length : 1,
    timeoutMs: spec.timeout_ms,
  });
  try {
    for (const key of spec.warm_keys ?? []) await cache.get(key);
    return cache;
  } catch (error) {
    await cache.close();
    throw error;
  }
}

function operation(sequence) {
  let index = (Math.imul((sequence + spec.seed) >>> 0, 0x9e3779b1) >>> 0) % spec.keys;
  const mode = (sequence + spec.seed) % 10;
  if (spec.distribution === "hot") index = 0;
  if (spec.distribution === "skew" && mode !== 0) index %= Math.max(1, Math.floor(spec.keys / 100));

  if (spec.workload === "miss" || (spec.workload === "mixed" && mode === 1)) {
    return { key: `missing:${index}`, write: false, expected: null };
  }
  if (spec.workload === "set-px" || (spec.workload === "mixed" && mode === 0)) {
    return { key: `key:${index}`, write: true, expected: true };
  }
  return { key: `key:${index}`, write: false, expected: value };
}

async function waitUntil(due) {
  for (;;) {
    const remaining = due - process.hrtime.bigint();
    if (remaining <= 0n) return;
    if (remaining >= 2_000_000n) {
      await setTimeout(Number(remaining / 1_000_000n) - 1);
    } else {
      await setImmediate();
    }
  }
}

async function runLane(lane, client, [warmStart, measureStart]) {
  const result = [];

  async function phase(count, origin, measure) {
    const payload = !measure && spec.workload === "set-px" ? warmValue : value;
    for (let sequence = lane; sequence < count; sequence += spec.lanes) {
      const turn = operation(sequence);
      const due = origin + BigInt(sequence) * ns / BigInt(spec.rate);
      await waitUntil(due);
      const started = process.hrtime.bigint();
      let outcome = "arrival-deadline";
      if (started - due <= timeoutNs) {
        let connection;
        try {
          connection = client ?? await connect();
          const reply = turn.write ?
            await connection.set(turn.key, payload, { ttlMs: spec.ttl_ms }) :
            await connection.get(turn.key);
          const correct = turn.expected === reply ||
            (turn.expected === value && reply instanceof Uint8Array && value.equals(reply));
          outcome = correct ? "ok" : "incorrect-reply";
        } catch (error) {
          outcome = (error?.constructor?.name ?? "Error").replace(/[^a-zA-Z0-9_-]/g, "_");
        } finally {
          if (connection && client === null) await connection.close();
        }
      }
      if (measure) {
        result.push(`${sequence},${due},${started},${process.hrtime.bigint()},${outcome}\n`);
      }
    }
  }

  try {
    await phase(spec.warmup, warmStart, false);
    await phase(spec.samples, measureStart, true);
  } finally {
    if (client) await client.close();
  }
  return result.join("");
}

const laneIds = [];
for (let lane = processIndex; lane < spec.lanes; lane += spec.client_processes) {
  laneIds.push(lane);
}
const clients = await Promise.all(laneIds.map(() => (
  spec.transport === "cold" ? null : connect()
)));
await writeFile(outputPath + ".ready", "");
let starts;
const waitStartThrough = process.hrtime.bigint() + 10n * ns;
while (!starts) {
  try {
    starts = JSON.parse(await readFile(spec.start_path, "utf8")).map(BigInt);
  } catch (error) {
    if (error?.code !== "ENOENT" || process.hrtime.bigint() > waitStartThrough) throw error;
    await setTimeout(5);
  }
}
const rows = (await Promise.all(laneIds.map((lane, index) => (
  runLane(lane, clients[index], starts)
)))).join("");
const compress = promisify(gzip);
await writeFile(outputPath, await compress(
  "sequence,scheduled_ns,started_ns,completed_ns,outcome\n" + rows,
));

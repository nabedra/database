import assert from 'node:assert/strict'
import { once } from 'node:events'
import http from 'node:http'
import test from 'node:test'

import { Database } from '../dist/client.js'
import {
  AuthenticationError, LimitExceededError, ProtocolError, StreamAbandonedError, StreamExpiredError,
  StreamUnsupportedError, ValidationError,
} from '../dist/errors.js'

const runId = '10000000-0000-0000-0000-000000000001'
const tableId = '20000000-0000-0000-0000-000000000002'
const streamId = '30000000-0000-0000-0000-000000000003'
const snapshotId = `${runId}.40000000-0000-0000-0000-000000000004`
const streamPath = `/root/v1/streams/${runId}/${streamId}`
const arrowType = 'application/vnd.apache.arrow.stream'
const caps = {
  version: 1, run_id: runId, max_batch_rows: 100, max_batch_bytes: 2048, max_prefetch_batches: 2,
}

function response(status, body, type = 'application/json', framing = 'length') {
  return { status, body: Buffer.isBuffer(body) ? body : Buffer.from(JSON.stringify(body)), type, framing }
}

function writeResponse(res, { status, body, type, framing }) {
  if (status === 204) {
    res.writeHead(status).end()
    return
  }
  if (framing !== 'length') {
    const header = framing === 'truncated' ? `Content-Length: ${body.length}` : 'Transfer-Encoding: chunked'
    const partial = framing === 'truncated'
      ? body.subarray(0, Math.floor(body.length / 2))
      : Buffer.concat([Buffer.from(`${body.length.toString(16)}\r\n`), body, Buffer.from('\r\n0\r\n')])
    res.socket.end(Buffer.concat([
      Buffer.from(`HTTP/1.1 ${status} OK\r\nContent-Type: ${type}\r\n${header}\r\nConnection: close\r\n\r\n`),
      partial,
    ]))
    return
  }
  res.writeHead(status, { 'Content-Type': type, 'Content-Length': body.length }).end(body)
}

function errorResponse(status, code, outcome = 'not_applied') {
  return response(status, { request_id: null, error: { code, outcome, message: 'stream test failure' } })
}

async function fixture(t) {
  const calls = []
  const batches = []
  const openFramings = []
  const config = { caps: response(200, caps) }
  const server = http.createServer(async (req, res) => {
    const chunks = []
    for await (const chunk of req) chunks.push(chunk)
    const raw = Buffer.concat(chunks)
    const data = raw.length ? JSON.parse(raw.toString()) : null
    const { method, url: path } = req
    calls.push({
      method, path, port: req.socket.remotePort, data,
      authorization: req.headers.authorization, database: req.headers['x-nabedra-database'],
    })
    let reply
    if (path === '/root/v1/operations') {
      reply = response(200, { request_id: data.request_id, data: {
        id: tableId, name: 'samples', key: 'id', format: 'parquet',
        schema: { id: 'int64', score: 'float64', visible: 'bool' },
      } })
    } else if (path === '/root/v1/streams/capabilities') {
      reply = config.caps
    } else if (path === '/root/v1/stream-snapshots') {
      reply = response(201, { run_id: runId, snapshot_id: snapshotId, expires_at: 123456789 })
    } else if (path === '/root/v1/streams') {
      reply = response(201, {
        run_id: runId, stream_id: streamId, expires_at: 123456789, prefetch_batches: data.prefetch_batches,
      }, 'application/json', openFramings.shift())
    } else if (path.startsWith(`${streamPath}/batches/`) && method === 'POST') {
      reply = batches.shift() ?? response(204, null)
    } else if (method === 'DELETE') {
      reply = response(204, null)
    } else {
      reply = response(404, null)
    }
    writeResponse(res, reply)
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  t.after(() => new Promise(resolve => {
    server.close(resolve)
    server.closeAllConnections()
  }))
  const database = new Database({
    url: `http://127.0.0.1:${server.address().port}/root`, database: 'training',
    token: 'stream-test-token', timeout: 3,
  })
  const table = await database.table('samples')
  return {
    config, batches, openFramings, table,
    streamCalls: () => calls.filter(call => call.path.startsWith('/root/v1/stream')),
  }
}

async function collect(stream) {
  const result = []
  for await (const batch of stream) result.push(batch)
  return result
}

test('ST05 lazily opens leased streams, keeps one socket per lease and closes early', async t => {
  const { table, batches, streamCalls } = await fixture(t)
  const query = table.where({ visible: true }).select('id')
  const snapshot = query.streamSnapshot({ shards: 2 })
  assert.throws(() => snapshot.id, ValidationError)
  assert.deepEqual(streamCalls(), [])
  assert.equal(await snapshot.open(), snapshot)
  assert.equal(snapshot.id, snapshotId)
  batches.push(response(200, Buffer.from('{"rows":[{"id":9223372036854775807},{"id":-9223372036854775808}]}')))
  const stream = query.stream({ batchRows: 2, snapshot: snapshot.id, shard: 1 })
  assert.equal(streamCalls().length, 2)
  let count = 0
  for await (const rows of stream) {
    assert.deepEqual(rows, [{ id: 9223372036854775807n }, { id: -9223372036854775808n }])
    count += 1
    break
  }
  assert.equal(count, 1)
  await stream.close()
  await snapshot[Symbol.asyncDispose]()
  await assert.rejects(snapshot.open(), ValidationError)
  assert.deepEqual(await collect(stream), [])

  const [snapCaps, snapshotOpen, streamCaps, streamOpen, batch, close, snapshotClose] = streamCalls()
  assert.equal(streamCalls().length, 7)
  assert.deepEqual(
    [snapCaps.path, snapshotOpen.path, streamCaps.path, streamOpen.path],
    ['/root/v1/streams/capabilities', '/root/v1/stream-snapshots', '/root/v1/streams/capabilities', '/root/v1/streams'],
  )
  assert.equal(streamOpen.data.table_id, tableId)
  assert.equal(streamOpen.data.snapshot_id, snapshotId)
  assert.equal(streamOpen.data.expected_run_id, runId)
  assert.equal(streamOpen.data.shard, 1)
  assert.equal(streamOpen.data.prefetch_batches, 1)
  assert.deepEqual(streamOpen.data.query, snapshotOpen.data.query)
  assert.equal(batch.path, `${streamPath}/batches/0`)
  assert.deepEqual([close.method, close.path], ['DELETE', streamPath])
  assert.deepEqual([snapshotClose.method, snapshotClose.path], ['DELETE', `/root/v1/stream-snapshots/${runId}/${snapshotId}`])
  assert.equal(new Set([snapCaps, snapshotOpen, snapshotClose].map(call => call.port)).size, 1)
  assert.equal(new Set([streamCaps, streamOpen, batch, close].map(call => call.port)).size, 1)
  assert.notEqual(streamCaps.port, snapCaps.port)
  assert.ok(streamCalls().every(call => call.authorization === 'Bearer stream-test-token' && call.database === 'training'))
})

test('ST04 retries incomplete frames without changing request identity or batch sequence', async t => {
  const { table, batches, openFramings, streamCalls } = await fixture(t)
  openFramings.push('truncated')
  const full = { rows: [{ id: 4 }, { id: 8 }] }
  const tail = { rows: [{ id: 12 }] }
  batches.push(
    errorResponse(503, 'storage_unavailable'),
    response(200, full, 'application/json', 'chunk-close'),
    response(200, full),
    response(200, tail, 'application/json', 'truncated'),
    response(200, tail),
  )
  const stream = table.query().select('id').stream({ batchRows: 2 })
  // Promise-based calls share the iterator state and the same ordered batch sequence.
  const [first, second] = await Promise.all([stream.next(), stream.next()])
  assert.deepEqual(first, { done: false, value: [{ id: 4n }, { id: 8n }] })
  assert.deepEqual(second, { done: false, value: [{ id: 12n }] })
  assert.deepEqual(await stream.next(), { done: true, value: undefined })
  const calls = streamCalls()
  const opens = calls.filter(call => call.path === '/root/v1/streams')
  assert.equal(opens.length, 2)
  assert.deepEqual(opens[0].data, opens[1].data)
  assert.notEqual(opens[0].port, opens[1].port)
  assert.equal(calls.filter(call => call.path.endsWith('/capabilities')).length, 1)
  assert.deepEqual(calls.filter(call => call.path.includes('/batches/')).map(call => call.path.split('/').at(-1)),
    ['0', '0', '0', '1', '1', '2'])
  assert.equal(calls.at(-1).method, 'DELETE')
})

test('ST02 and U02 reject bad rows, framing, and limits before acknowledging another batch', async t => {
  const { table, config, batches, streamCalls } = await fixture(t)
  const query = table.query().select('id')
  const cases = [
    response(200, Buffer.from('{"rows":[{"id":3,"id":4}]}')),
    response(200, Buffer.from('{"rows":[{"id":3.0},{"id":4}]}')),
    response(200, { rows: [{ id: false }] }),
    response(200, { rows: [{ id: 3, visible: true }] }),
    response(200, { rows: [{ id: 3 }, { id: 4 }, { id: 5 }] }),
    response(200, { rows: [{ id: 3 }] }),
    response(200, Buffer.alloc(2049, 0x20)),
  ]
  for (const malformed of cases) {
    const before = streamCalls().length
    batches.push(malformed)
    await assert.rejects(collect(query.stream({ batchRows: 2, dropLast: true })), ProtocolError)
    const calls = streamCalls().slice(before)
    assert.deepEqual(calls.filter(call => call.path.includes('/batches/')).map(call => call.path), [`${streamPath}/batches/0`])
    assert.equal(calls.at(-1).method, 'DELETE')
  }

  const before = streamCalls().length
  assert.throws(() => query.stream({ shard: 1 }), ValidationError)
  assert.throws(() => query.stream({ snapshot: `${snapshotId}\n` }), ValidationError)
  assert.throws(() => query.stream({ prefetchBatches: 3 }), ValidationError)
  assert.throws(() => query.streamSnapshot({ shards: 0 }), ValidationError)
  assert.equal(streamCalls().length, before)
  config.caps = response(200, { ...caps, max_batch_rows: 1 })
  await assert.rejects(query.stream({ batchRows: 2 }).next(), LimitExceededError)
  assert.equal(streamCalls().length, before + 1)
})

test('ST05 preserves typed capability and lease errors', async t => {
  const { table, config, batches, streamCalls } = await fixture(t)
  const query = table.query().select('id')
  const cases = [
    [response(404, Buffer.from('route missing'), 'text/plain'), StreamUnsupportedError],
    [response(200, { ...caps, version: 2 }), StreamUnsupportedError],
    [errorResponse(401, 'unauthorized'), AuthenticationError],
  ]
  for (const [reply, ErrorType] of cases) {
    const before = streamCalls().length
    config.caps = reply
    await assert.rejects(collect(query.stream()), ErrorType)
    assert.equal(streamCalls().length, before + 1)
  }
  config.caps = response(200, caps)
  for (const [code, ErrorType] of [
    ['stream_expired', StreamExpiredError], ['stream_abandoned', StreamAbandonedError],
  ]) {
    const before = streamCalls().length
    batches.push(errorResponse(410, code))
    await assert.rejects(collect(query.stream()), ErrorType)
    const calls = streamCalls().slice(before)
    assert.equal(calls.filter(call => call.path.includes('/batches/')).length, 1)
    assert.equal(calls.at(-1).method, 'DELETE')
  }
})

test('ST02 returns native Apache Arrow batches and rejects invalid IPC', async t => {
  let arrow
  try {
    arrow = await import('apache-arrow')
  } catch (error) {
    if (error.code !== 'ERR_MODULE_NOT_FOUND') throw error
    t.skip('apache-arrow optional dependency is unavailable')
    return
  }
  const { table, batches, streamCalls } = await fixture(t)
  const query = table.query().select('id')
  function ipc(type, values, copies = 1, nullable = false, name = 'id') {
    const input = arrow.tableFromArrays({ [name]: arrow.vectorFromArray(values, type) })
    const schema = new arrow.Schema([new arrow.Field(name, type, nullable)])
    const record = new arrow.RecordBatch(schema, input.batches[0].data)
    return Buffer.from(arrow.tableToIPC(new arrow.Table(schema, Array(copies).fill(record)), 'stream'))
  }
  const good = ipc(new arrow.Int64(), [9n, 14n])
  batches.push(response(200, good, arrowType))
  const [batch] = await collect(query.stream({ format: 'arrow', batchRows: 2 }))
  assert.ok(batch instanceof arrow.RecordBatch)
  assert.deepEqual([...batch.getChild('id')], [9n, 14n])

  const badFrames = [
    good.subarray(0, -8),
    ipc(new arrow.Int32(), [9, 14]),
    ipc(new arrow.Int64(), [9n, 14n], 2),
    ipc(new arrow.Int64(), [9n, 14n], 1, true),
    ipc(new arrow.Int64(), [9n, null], 1, false),
    Buffer.concat([good, good]),
  ]
  for (const malformed of badFrames) {
    const before = streamCalls().length
    batches.push(response(200, malformed, arrowType))
    await assert.rejects(collect(query.stream({ format: 'arrow', batchRows: 2 })), ProtocolError)
    const calls = streamCalls().slice(before)
    assert.equal(calls.filter(call => call.path.includes('/batches/')).length, 1)
    assert.equal(calls.at(-1).method, 'DELETE')
  }

  batches.push(response(200, ipc(new arrow.Float64(), [Infinity, NaN], 1, false, 'score'), arrowType))
  await assert.rejects(collect(table.query().select('score').stream({ format: 'arrow' })), ProtocolError)
})

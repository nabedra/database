"""ST02/ST04/ST05 and U02 assertions at the SDK's HTTP stream boundary."""
from collections import deque
from dataclasses import dataclass
from functools import partial
import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import threading
import unittest
from unittest.mock import patch
import uuid

from nabedra import (
    AuthenticationError,
    Database,
    ProtocolError,
    StreamAbandonedError,
    StreamExpiredError,
    StreamUnsupportedError,
    col,
)


@dataclass
class Reply:
    status: int
    body: bytes = b''
    mime: str = 'application/json'
    framing: str = 'length'

    @classmethod
    def json(cls, status, value, framing='length'):
        return cls(status, json.dumps(value).encode(), framing=framing)


class StreamFixture:
    def __init__(self):
        self.run_id = str(uuid.uuid4())
        self.stream_id = str(uuid.uuid4())
        self.table_id = str(uuid.uuid4())
        self.snapshot_id = f'{self.run_id}.{uuid.uuid4()}'
        self.calls = []
        self.opens = deque()
        self.batches = deque()
        self.capability_failure = None
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def handle_request(self):
                body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                value = json.loads(body) if body else None
                fixture.calls.append((self.command, self.path, self.client_address[1],
                                      self.headers.get('Authorization'),
                                      self.headers.get('X-Nabedra-Database'), value))
                reply = fixture.reply(self.path, self.command, value)
                self.send_response(reply.status)
                self.send_header('Content-Type', reply.mime)
                if reply.framing == 'chunk-close':
                    self.send_header('Transfer-Encoding', 'chunked')
                elif reply.status != 204:
                    self.send_header('Content-Length', str(len(reply.body)))
                self.end_headers()
                if reply.framing == 'truncated':
                    self.wfile.write(reply.body[: len(reply.body) // 2])
                elif reply.framing == 'chunk-close':
                    prefix = f'{len(reply.body):x}'.encode()
                    self.wfile.write(prefix + b'\r\n' + reply.body + b'\r\n0\r\n')
                else:
                    self.wfile.write(reply.body)
                if reply.framing != 'length':
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_WR)

            do_GET = handle_request
            do_POST = handle_request
            do_DELETE = handle_request

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(
            target=partial(self.server.serve_forever, poll_interval=0.02),
            daemon=True,
        )
        self.thread.start()

    def query(self):
        url = f'http://127.0.0.1:{self.server.server_port}'
        db = Database(url, 'streams-test-token', database='training', timeout=3)
        return db.table('examples').where((col('active') == True) & (col('id') >= 0))

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def reply(self, path, method, value):
        if path == '/v1/operations':
            return Reply.json(200, {'request_id': value['request_id'], 'data': {
                'id': self.table_id, 'name': 'examples', 'key': 'id', 'format': 'parquet',
                'schema': {'id': 'int64', 'score': 'float64', 'active': 'bool'},
            }})
        if path == '/v1/streams/capabilities':
            if self.capability_failure:
                return self.capability_failure
            return Reply.json(200, {
                'version': 1, 'run_id': self.run_id, 'max_batch_rows': 100,
                'max_batch_bytes': 2048, 'max_prefetch_batches': 2,
            })
        if method == 'DELETE':
            return Reply(204)
        if path == '/v1/stream-snapshots':
            return Reply.json(201, {
                'snapshot_id': self.snapshot_id, 'run_id': self.run_id, 'expires_at': 1234567890000,
            })
        if path == '/v1/streams':
            return Reply.json(201, {
                'stream_id': self.stream_id, 'run_id': self.run_id,
                'prefetch_batches': value['prefetch_batches'], 'expires_at': 1234567890000,
            }, self.opens.popleft() if self.opens else 'length')
        return self.batches.popleft() if self.batches else Reply(204)

    def stream_calls(self):
        return [entry for entry in self.calls if entry[1].startswith('/v1/stream')]


class StreamingTests(unittest.TestCase):
    def setUp(self):
        self.fixture = StreamFixture()
        self.addCleanup(self.fixture.close)
        self.query = self.fixture.query().select('id')

    def test_st05_snapshot_scope_keep_alive_early_close(self):
        fixture = self.fixture
        fixture.batches.append(Reply.json(200, {'rows': [{'id': 3}, {'id': 7}]}))
        with self.query.stream_snapshot(shards=2) as snapshot:
            self.assertEqual(snapshot.id, fixture.snapshot_id)
            with self.query.stream(batch_rows=2, snapshot=snapshot.id, shard=1) as stream:
                self.assertEqual(next(stream), [{'id': 3}, {'id': 7}])
            self.assertEqual(list(stream), [])
            stream.close()

        caps, snapshot, stream_caps, open_call, batch, close, close_snapshot = fixture.stream_calls()
        self.assertEqual(
            [event[1] for event in [caps, snapshot, stream_caps, open_call]],
            ['/v1/streams/capabilities', '/v1/stream-snapshots',
             '/v1/streams/capabilities', '/v1/streams'],
        )
        self.assertEqual(snapshot[2], caps[2])
        self.assertEqual({event[2] for event in (stream_caps, open_call, batch, close)}, {close[2]})
        self.assertEqual(close_snapshot[2], snapshot[2])
        self.assertEqual(open_call[-1]['table_id'], fixture.table_id)
        self.assertEqual(open_call[-1]['snapshot_id'], fixture.snapshot_id)
        self.assertEqual(open_call[-1]['shard'], 1)
        self.assertEqual(open_call[-1]['expected_run_id'], fixture.run_id)
        self.assertEqual(open_call[-1]['query'], snapshot[-1]['query'])
        self.assertEqual(batch[1], f'/v1/streams/{fixture.run_id}/{fixture.stream_id}/batches/0')
        self.assertEqual(close[:2], ('DELETE', f'/v1/streams/{fixture.run_id}/{fixture.stream_id}'))
        self.assertEqual(close_snapshot[:2],
                         ('DELETE', f'/v1/stream-snapshots/{fixture.run_id}/{fixture.snapshot_id}'))
        self.assertTrue(all(call[3:5] == ('Bearer streams-test-token', 'training')
                            for call in fixture.stream_calls()))

    def test_st04_retries_preserve_open_identity_and_batch_sequence(self):
        self.fixture.opens.append('truncated')
        error = {'request_id': None, 'error': {
            'code': 'storage_unavailable', 'message': 'owner unavailable', 'outcome': 'not_applied',
        }}
        full = {'rows': [{'id': 4}, {'id': 8}]}
        self.fixture.batches.extend([
            Reply.json(503, error),
            Reply.json(200, full, 'chunk-close'),
            Reply.json(200, full),
            Reply(200, b'{"rows":[{"id":12}]}', framing='truncated'),
            Reply.json(200, {'rows': [{'id': 12}]}),
        ])
        actual = list(self.query.stream(batch_rows=2))
        self.assertEqual(actual, [[{'id': 4}, {'id': 8}], [{'id': 12}]])
        calls = self.fixture.stream_calls()
        opens = [call for call in calls if call[1] == '/v1/streams']
        self.assertEqual(len(opens), 2)
        self.assertEqual(opens[0][-1], opens[1][-1])
        self.assertNotEqual(opens[0][2], opens[1][2])
        self.assertEqual(sum(call[1].endswith('/capabilities') for call in calls), 1)
        self.assertEqual([call[1].rsplit('/', 1)[1] for call in calls if '/batches/' in call[1]],
                         ['0', '0', '0', '1', '1', '2'])
        self.assertEqual(calls[-1][0], 'DELETE')

    def test_st02_rejects_invalid_rows_and_closes_without_ack(self):
        bad_frames = [
            Reply(200, b'{"rows":[{"id":3,"id":4}]}'),
            Reply.json(200, {'rows': [{'id': False}]}),
            Reply.json(200, {'rows': [{'id': 3, 'active': True}]}),
            Reply.json(200, {'rows': [{'id': 3}, {'id': 4}, {'id': 5}]}),
            Reply.json(200, {'rows': [{'id': 3}]}),
            Reply(200, b' ' * 2049),
        ]
        for frame in bad_frames:
            with self.subTest(frame=frame.body[:80]):
                start = len(self.fixture.stream_calls())
                self.fixture.batches.append(frame)
                with self.assertRaises(ProtocolError):
                    list(self.query.stream(batch_rows=2, drop_last=True))
                calls = self.fixture.stream_calls()[start:]
                self.assertEqual([call[1].rsplit('/', 1)[1] for call in calls if '/batches/' in call[1]],
                                 ['0'])
                self.assertEqual(calls[-1][0], 'DELETE')

    def test_st05_typed_stream_errors_and_no_arrow_install_io(self):
        with patch.dict('sys.modules', {'pyarrow': None}):
            with self.assertRaises(StreamUnsupportedError):
                self.query.stream(format='arrow')
        self.assertEqual(self.fixture.stream_calls(), [])

        cases = [
            (Reply(404, b'route missing', mime='text/plain'), StreamUnsupportedError),
            (Reply.json(401, {'request_id': None, 'error': {
                'code': 'unauthorized', 'message': 'denied', 'outcome': 'not_applied',
            }}), AuthenticationError),
        ]
        for reply, error in cases:
            with self.subTest(error=error):
                self.fixture.capability_failure = reply
                with self.assertRaises(error):
                    list(self.query.stream())
        self.fixture.capability_failure = None

        for code, error in (('stream_expired', StreamExpiredError),
                            ('stream_abandoned', StreamAbandonedError)):
            with self.subTest(code=code):
                self.fixture.batches.append(Reply.json(410, {'request_id': None, 'error': {
                    'code': code, 'message': 'worker run ended', 'outcome': 'not_applied',
                }}))
                before = len(self.fixture.stream_calls())
                with self.assertRaises(error):
                    list(self.query.stream())
                calls = self.fixture.stream_calls()[before:]
                self.assertEqual(sum('/batches/' in call[1] for call in calls), 1)
                self.assertEqual(calls[-1][0], 'DELETE')

    @unittest.skipUnless(importlib.util.find_spec('pyarrow'), 'requires the arrow extra')
    def test_st02_arrow_rejects_bad_ipc_schema_and_payload(self):
        import pyarrow as pa

        mime = 'application/vnd.apache.arrow.stream'

        def ipc(type_, values, *, copies=1):
            schema = pa.schema([pa.field('id', type_, nullable=False)])
            record = pa.RecordBatch.from_arrays([pa.array(values, type=type_)], schema=schema)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, schema) as writer:
                for _ in range(copies):
                    writer.write_batch(record)
            return sink.getvalue().to_pybytes()

        good = ipc(pa.int64(), [9, 14])
        self.fixture.batches.append(Reply(200, good, mime))
        (batch,) = list(self.query.stream(format='arrow', batch_rows=2))
        self.assertEqual(batch.to_pylist(), [{'id': 9}, {'id': 14}])
        for name, frame in (
            ('missing_end_marker', good[:-8]),
            ('wrong_schema', ipc(pa.int32(), [9, 14])),
            ('two_batches', ipc(pa.int64(), [9, 14], copies=2)),
            ('trailing_frame', good + good),
        ):
            with self.subTest(name=name):
                start = len(self.fixture.stream_calls())
                self.fixture.batches.append(Reply(200, frame, mime))
                with self.assertRaises(ProtocolError):
                    list(self.query.stream(format='arrow', batch_rows=2))
                calls = self.fixture.stream_calls()[start:]
                self.assertEqual(sum('/batches/' in call[1] for call in calls), 1)
                self.assertEqual(calls[-1][0], 'DELETE')

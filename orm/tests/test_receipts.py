"""Q04-Q06/U01 receipt transport keeps identity across API connections."""
import json
import threading
import unittest
import uuid
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from meshdb import (DB, RequestReceipt, ConflictError, NotFoundError, ProtocolError,
                    ValidationError, AuthenticationError, TransportError, UnknownOutcomeError)


class ReceiptsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                value = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                cls.seen.append((self.path, self.headers['Authorization'], value))
                identity = value['request_id']
                if identity in cls.requests and cls.requests[identity] != value:
                    return self.respond(409, {'request_id': identity, 'error': {
                        'code': 'conflict', 'message': 'identity conflict', 'outcome': 'not_applied'}})
                cls.requests[identity] = value
                if cls.malformed_acceptance:
                    return self.respond(202, {'request_id': identity, 'state': 'queued'})
                self.respond(202, {'request_id': identity, 'run_id': cls.run_id, 'state': 'queued'})

            def do_GET(self):
                cls.seen.append((self.path, self.headers['Authorization'], None))
                run, identity = self.path.rsplit('/', 2)[-2:]
                if self.headers['Authorization'] != 'Bearer scope-token':
                    return self.respond(401, {'request_id': identity, 'error': {
                        'code': 'unauthorized', 'message': 'unauthorized', 'outcome': 'not_applied'}})
                if cls.receipt_gate is not None:
                    cls.receipt_gate.wait(2)
                if identity not in cls.requests or cls.expired:
                    return self.respond(404, {'request_id': identity, 'error': {
                        'code': 'not_found', 'message': 'receipt unavailable or expired', 'outcome': 'not_applied'}})
                self.respond(200, {'request_id': identity, 'run_id': run, 'state': cls.state,
                                   'result': cls.result, 'error': None})

            def respond(self, status, value):
                body = json.dumps(value).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *args):
                pass
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f'http://127.0.0.1:{cls.server.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        type(self).run_id = str(uuid.uuid4())
        type(self).requests = {}
        type(self).seen = []
        type(self).state = 'queued'
        type(self).result = None
        type(self).expired = False
        type(self).malformed_acceptance = False
        type(self).receipt_gate = None

    def test_q04_reconnect_uses_receipt_without_resubmission(self):
        receipt = DB(self.url, 'scope-token').submit('table.list', {})
        self.assertIsInstance(receipt, RequestReceipt)
        self.assertFalse(receipt.terminal)
        type(self).state, type(self).result = 'succeeded', [{'name': 'test'}]
        another_api = DB(self.url, 'scope-token')
        terminal = another_api.receipt(receipt.run_id, receipt.request_id)
        self.assertEqual(terminal.result, [{'name': 'test'}])
        self.assertTrue(terminal.terminal)
        self.assertEqual(sum(body is not None for _, _, body in self.seen), 1)
        self.assertTrue(all(auth == 'Bearer scope-token' for _, auth, _ in self.seen))

    def test_q05_same_identity_and_body_attach_conflicting_body_fails(self):
        client = DB(self.url, 'scope-token')
        identity = str(uuid.uuid4())
        first = client.submit('table.list', {}, request_id=identity)
        duplicate = client.submit('table.list', {}, request_id=identity)
        self.assertEqual(first.request_id, duplicate.request_id)
        self.assertEqual(first.run_id, duplicate.run_id)
        with self.assertRaises(ConflictError):
            client.submit('table.describe', {'name': 'other'}, request_id=identity)
        self.assertEqual(len(self.requests), 1)

    def test_q06_expiration_is_explicit_and_never_resubmits(self):
        receipt = DB(self.url, 'scope-token').submit('table.list', {})
        type(self).expired = True
        with self.assertRaises(NotFoundError) as caught:
            receipt.poll()
        self.assertIn('expired', str(caught.exception))
        self.assertEqual(sum(body is not None for _, _, body in self.seen), 1)

    def test_wait_polls_same_identity(self):
        receipt = DB(self.url, 'scope-token').submit('table.list', {})
        type(self).state, type(self).result = 'succeeded', []
        terminal = receipt.wait(timeout=1, poll_interval=0.001)
        self.assertEqual(terminal.result, [])
        self.assertEqual(terminal.request_id, receipt.request_id)
        self.assertEqual(len(self.seen), 2)

    def test_reject_result_in_nonterminal_state(self):
        receipt = DB(self.url, 'scope-token').submit('table.list', {})
        type(self).result = 'fabricated'
        with self.assertRaises(ProtocolError):
            receipt.poll()

    def test_receipt_scope_is_authenticated_on_reconnect(self):
        receipt = DB(self.url, 'scope-token').submit('table.list', {})
        with self.assertRaises(AuthenticationError):
            DB(self.url, 'other-token').receipt(receipt.run_id, receipt.request_id)
        self.assertEqual(len(self.requests), 1)

    def test_invalid_identity_or_payload_creates_no_submission(self):
        client = DB(self.url, 'scope-token')
        for identity in ('not-a-uuid', str(uuid.uuid4()).upper(), 1):
            with self.subTest(identity=identity), self.assertRaises(ValidationError):
                client.submit('table.list', {}, request_id=identity)
        for run, request in (('bad', str(uuid.uuid4())), (str(uuid.uuid4()), 'bad')):
            with self.assertRaises(ValidationError):
                client.receipt(run, request)
        with self.assertRaises(ValidationError):
            client.submit('query.unknown', {})
        with self.assertRaises(ValidationError):
            client.submit('table.list', {'bad': float('nan')})
        self.assertEqual(self.seen, [])

    def test_mutation_acceptance_without_run_is_unknown_and_never_retried(self):
        type(self).malformed_acceptance = True
        with self.assertRaises(UnknownOutcomeError) as caught:
            DB(self.url, 'scope-token').submit('table.insert', {})
        self.assertIsNotNone(caught.exception.request_id)
        self.assertEqual(len(self.seen), 1)

    def test_wait_deadline_bounds_receipt_http_io_without_cancelling_work(self):
        receipt = DB(self.url, 'scope-token', timeout=5).submit('table.list', {})
        gate = threading.Event()
        type(self).receipt_gate = gate
        started = time.monotonic()
        try:
            with self.assertRaises(TransportError):
                receipt.wait(timeout=0.05, poll_interval=0.001)
            self.assertLess(time.monotonic() - started, 1)
            self.assertEqual(len(self.requests), 1)
        finally:
            gate.set()

    def test_wait_rejects_invalid_or_overflowing_durations(self):
        receipt = DB(self.url, 'scope-token').submit('table.list', {})
        before = len(self.seen)
        for duration in (0, -1, True, float('nan'), float('inf'), 10 ** 1000):
            with self.subTest(duration=type(duration).__name__), self.assertRaises(ValidationError):
                receipt.wait(timeout=duration)
        self.assertEqual(len(self.seen), before)

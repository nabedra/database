"""Private control transport behavior against a real loopback peer."""
import json
import socket
import ssl
import threading
import time
import unittest
import uuid

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from meshdb import errors

LIMIT = 1024 * 1024


class ControlTransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                cls.seen.append((self.path, self.headers['Authorization'], request))
                cls.reply(self, request)

            def log_message(self, *args):
                pass
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f'http://127.0.0.1:{cls.server.server_port}/prefix/'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        type(self).seen = []
        self.set_reply(lambda r: {'request_id': r['request_id'], 'data': {'revision': 3}})

    def transport(self, **kwargs):
        from meshdb._control_transport import _ControlTransport
        return _ControlTransport(self.url, 'private-token', **kwargs)

    def set_reply(self, envelope, status=200, truncate=0):
        def reply(handler, request):
            data = envelope(request) if callable(envelope) else envelope
            raw = data if isinstance(data, bytes) else json.dumps(data).encode()
            handler.send_response(status)
            handler.send_header('Content-Type', 'application/json')
            handler.send_header('Content-Length', str(len(raw) + truncate))
            handler.send_header('Location', self.url)
            handler.end_headers()
            try:
                handler.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass
            handler.close_connection = True
        type(self).reply = staticmethod(reply)

    def test_policy_lost_reply_is_unknown_and_not_retried(self):
        def lost(handler, request):
            handler.connection.shutdown(socket.SHUT_RDWR)
            handler.close_connection = True
        type(self).reply = staticmethod(lost)
        request_id = str(uuid.uuid4())
        with self.assertRaises(errors.UnknownOutcomeError) as caught:
            self.transport().request('cluster.scale', {'nodes': 2}, request_id=request_id)
        self.assertEqual(caught.exception.request_id, request_id)
        self.assertEqual(caught.exception.outcome, 'unknown')
        self.assertEqual(len(self.seen), 1)
        with self.assertRaises(errors.TransportError):
            self.transport().request('cluster.status', {})
        self.assertEqual(len(self.seen), 2)

    def test_wire_success_ids_and_rejection(self):
        transport = self.transport()
        ids = []
        for operation in ('cluster.scale', 'cluster.autoscale', 'cluster.status'):
            request_id, data = transport.request(operation, {})
            self.assertEqual(data, {'revision': 3})
            self.assertEqual(str(uuid.UUID(request_id)), request_id)
            ids.append(request_id)
        self.assertEqual(len(set(ids)), 3)
        explicit_id = str(uuid.uuid4())
        self.assertEqual(transport.request('cluster.scale', {}, request_id=explicit_id)[0], explicit_id)
        for path, auth, request in self.seen:
            self.assertEqual(path, '/prefix/v1/cluster/operations')
            self.assertEqual(auth, 'Bearer private-token')
            self.assertEqual(set(request), {'request_id', 'operation', 'payload'})
        for code, status, expected in [('conflict', 409, errors.ConflictError),
                                       ('unsupported_capability', 400, errors.MeshDBError)]:
            self.set_reply(lambda r: {'request_id': r['request_id'], 'error': {
                'code': code, 'message': 'rejected', 'outcome': 'not_applied'}}, status)
            with self.assertRaises(expected) as caught:
                transport.request('cluster.autoscale', {})
            self.assertEqual(caught.exception.code, code)
            self.assertEqual(caught.exception.status_code, status)
            self.assertEqual(caught.exception.outcome, 'not_applied')

    def test_untrusted_envelopes_and_redirect_do_not_succeed(self):
        cases = [
            (lambda r: {'request_id': r['request_id'], 'data': {}, 'extra': 1}, 200, 0),
            ({'request_id': str(uuid.uuid4()), 'data': {}}, 200, 0),
            (lambda r: ('{"request_id":"' + r['request_id'] + '","data":1,"data":2}').encode(), 200, 0),
            (lambda r: ('{"request_id":"' + r['request_id'] + '","data":NaN}').encode(), 200, 0),
            (b'x' * (LIMIT + 1), 200, 0),
            (lambda r: {'request_id': r['request_id'], 'data': {}}, 200, 7),
            ({}, 307, 0),
        ]
        transport = self.transport()
        for envelope, status, truncate in cases:
            with self.subTest(status=status, truncate=truncate, envelope=type(envelope).__name__):
                self.set_reply(envelope, status, truncate)
                for operation, expected in [('cluster.scale', errors.UnknownOutcomeError),
                                            ('cluster.status', errors.TransportError if truncate
                                             else errors.ProtocolError)]:
                    with self.assertRaises(expected):
                        transport.request(operation, {})
        self.assertEqual(len(self.seen), 2 * len(cases))

    def test_local_validation_and_refused_connection_are_not_unknown(self):
        from meshdb._control_transport import _ControlTransport
        for api_url, token, kwargs in [('http://user:pass@localhost', 'token', {}),
                                       (self.url, 'unsafe\r\ntoken', {}),
                                       (self.url, 'token', {'timeout': True}),
                                       (self.url, 'token', {'ca_file': '/missing/meshdb-test-ca.pem'})]:
            with self.assertRaises(errors.ValidationError):
                _ControlTransport(api_url, token, **kwargs)
        tls = _ControlTransport('https://localhost', 'token')
        self.assertTrue(tls._context.check_hostname)
        self.assertEqual(tls._context.verify_mode, ssl.CERT_REQUIRED)
        transport = self.transport()
        for kwargs in [{'operation': 'table.get', 'payload': {}},
                       {'operation': 'cluster.scale', 'payload': {'x': float('nan')}},
                       {'operation': 'cluster.scale', 'payload': {1: 1, '1': 2}},
                       {'operation': 'cluster.scale', 'payload': {}, 'request_id': 'invalid'},
                       {'operation': 'cluster.scale', 'payload': {}, 'deadline': float('inf')}]:
            with self.assertRaises(errors.ValidationError):
                transport.request(**kwargs)
        with self.assertRaises(errors.LimitExceededError):
            transport.request('cluster.scale', {'x': 'x' * LIMIT})
        with self.assertRaises(errors.TransportError):
            transport.request('cluster.scale', {}, deadline=time.monotonic() - 1)
        self.assertEqual(self.seen, [])
        # The socket is bound but never listens: connect fails before any send.
        with socket.socket() as refused:
            refused.bind(('127.0.0.1', 0))
            from meshdb._control_transport import _ControlTransport
            transport = _ControlTransport(f'http://127.0.0.1:{refused.getsockname()[1]}', 'token')
            with self.assertRaises(errors.TransportError):
                transport.request('cluster.scale', {})

    def test_hostname_deadline_uses_reaped_resolver_and_returns_success(self):
        from meshdb._control_transport import _ControlTransport
        transport = _ControlTransport(self.url.replace('127.0.0.1', 'localhost'), 'token')
        request_id, data = transport.request('cluster.status', {}, deadline=time.monotonic() + 2)
        self.assertEqual(data, {'revision': 3})
        self.assertEqual(request_id, self.seen[0][2]['request_id'])
        self.assertFalse(any(t.name.startswith('meshdb-control-deadline')
                             for t in threading.enumerate()))

    def test_deadline_interrupt_preserves_tls_state(self):
        from meshdb._control_transport import _Deadline
        raw, peer = socket.socketpair()
        with raw, peer:
            with ssl.create_default_context().wrap_socket(
                    raw, server_hostname='localhost', do_handshake_on_connect=False) as tls:
                guard = _Deadline(None)
                try:
                    guard.attach(tls)
                    guard._expire()
                    with self.assertRaises(OSError):
                        tls.do_handshake()
                finally:
                    guard.close()

    def test_deadline_caps_header_and_trickling_body_and_cleans_up(self):
        transport = self.transport(timeout=2)
        for phase in ('header', 'body'):
            release = threading.Event()
            finished = threading.Event()
            def slow(handler, request):
                try:
                    if phase == 'header':
                        release.wait(2)
                        return
                    raw = json.dumps({'request_id': request['request_id'], 'data': {}}).encode()
                    handler.send_response(200)
                    handler.send_header('Content-Type', 'application/json')
                    handler.send_header('Content-Length', str(len(raw)))
                    handler.end_headers()
                    for byte in raw:
                        handler.wfile.write(bytes([byte]))
                        handler.wfile.flush()
                        if release.wait(0.025):
                            break
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    handler.close_connection = True
                    finished.set()
            type(self).reply = staticmethod(slow)
            baseline = {t.ident for t in threading.enumerate()}
            start = time.monotonic()
            try:
                with self.assertRaises(errors.UnknownOutcomeError):
                    transport.request('cluster.scale', {}, deadline=start + 0.15)
                self.assertLess(time.monotonic() - start, 0.8)
            finally:
                release.set()
                self.assertTrue(finished.wait(2))
            self.assertFalse(any(t.name.startswith('meshdb-control-deadline')
                                 and t.ident not in baseline for t in threading.enumerate()))


if __name__ == '__main__':
    unittest.main()

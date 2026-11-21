"""O01 connection reuse: keep-alive over TLS without resending any request."""
import importlib.util
import json
from pathlib import Path
import os
import socket
import ssl
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from nabedra import Database
from nabedra import _http_pool
from nabedra.client import _Transport
from nabedra.errors import ProtocolError, TransportError, UnknownOutcomeError

RUN_ID = '22222222-2222-4222-8222-222222222222'


def _create_test_tls(directory):
    helper = Path(__file__).resolve().parents[3] / 'scripts' / 'test_tls_material.py'
    name = '_nabedra_pool_tls_fixture'
    spec = importlib.util.spec_from_file_location(name, helper)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        return module.create_test_tls(directory)
    finally:
        sys.modules.pop(name, None)


class _KeepAliveTLSServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, context):
        self.context = context
        self.lock = threading.Lock()
        self.accepted = 0
        self.seen = []          # (connection number, operation, request ID)
        self.sockets = []       # server-side TLS sockets, in accept order
        self.actions = []       # per-request behavior, default 'ok'
        self.finished = threading.Semaphore(0)
        super().__init__(('127.0.0.1', 0), _Handler)

    def get_request(self):
        raw, address = super().get_request()
        raw.settimeout(5)
        tls = self.context.wrap_socket(raw, server_side=True)
        with self.lock:
            self.accepted += 1
            self.sockets.append(tls)
        return tls, address

    def shutdown_request(self, request):
        super().shutdown_request(request)
        self.finished.release()   # the socket is closed, not just the handler

    def handle_error(self, request, client_address):
        pass


class _Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def setup(self):
        super().setup()
        with self.server.lock:
            self.number = self.server.accepted

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        with self.server.lock:
            self.server.seen.append((self.number, body['operation'], body['request_id']))
            action = self.server.actions.pop(0) if self.server.actions else 'ok'
        if action == 'hangup':
            self.close_connection = True
            return
        if action == 'malformed':
            self.wfile.write(b'HTTP/1.1 invalid\r\nContent-Length: 0\r\n\r\n')
            self.close_connection = True
            return
        if self.path.endswith('/v1/requests'):
            self.reply(202, {'request_id': body['request_id'], 'run_id': RUN_ID, 'state': 'queued'})
            return
        reply = json.dumps({'request_id': body['request_id'], 'data': None}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(reply) + (5 if action == 'truncate' else 0)))
        if action == 'close':
            self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(reply)
        if action in ('truncate', 'close', 'ok_then_close'):
            self.close_connection = True

    def do_GET(self):
        run_id, request_id = self.path.rsplit('/', 2)[-2:]
        with self.server.lock:
            self.server.seen.append((self.number, 'receipt', request_id))
        self.reply(200, {'request_id': request_id, 'run_id': run_id, 'state': 'succeeded',
                         'result': None, 'error': None})

    def reply(self, status, value):
        reply = json.dumps(value).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)

    def log_message(self, *args):
        pass


class ConnectionReuseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix='nabedra-pool-tls-')
        cls.material = _create_test_tls(Path(cls.directory.name) / 'tls')

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def setUp(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.material.server_cert, self.material.server_key)
        self.server = _KeepAliveTLSServer(context)
        self.thread = threading.Thread(target=self.server.serve_forever, name='nabedra-test-pool')
        self.thread.start()
        self.url = f'https://127.0.0.1:{self.server.server_port}/api'
        self.transport = _Transport(self.url, 'test-token', ca_file=str(self.material.ca_cert), timeout=5)
        self.pool = self.transport._opener
        self.assertIsInstance(self.pool, _http_pool.ConnectionPool)

    def tearDown(self):
        self.pool.close()
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()

    def wait_for_server_close(self):
        self.assertTrue(self.server.finished.acquire(timeout=5), 'server kept the connection')
        time.sleep(0.05)   # let the FIN cross loopback

    def test_o01_sequential_calls_reuse_one_tls_connection(self):
        operations = ['table.get', 'table.insert', 'query.count', 'table.update', 'table.delete'] * 4
        for operation in operations:
            self.assertIsNone(self.transport.request(operation, {}))
        self.assertEqual(self.server.accepted, 1)
        self.assertEqual([seen[1] for seen in self.server.seen], operations)
        self.assertEqual({seen[0] for seen in self.server.seen}, {1})
        self.assertEqual(len({seen[2] for seen in self.server.seen}), len(operations))

    def test_o01_database_close_and_context_manager_close_idle_connections(self):
        with Database(self.url, 'test-token', ca_file=str(self.material.ca_cert), timeout=5) as db:
            db._transport.request('table.list', {})
            self.assertEqual(self.server.accepted, 1)
            db.close()
            self.wait_for_server_close()
            db._transport.request('table.list', {})   # still usable after close
            self.assertEqual(self.server.accepted, 2)
        self.wait_for_server_close()
        self.assertEqual(len(db._transport._opener._idle), 0)

    def test_o01_database_calls_reuse_one_connection(self):
        db = Database(self.url, 'test-token', ca_file=str(self.material.ca_cert), timeout=5)
        try:
            for _ in range(5):
                db._transport.request('table.list', {})
            self.assertEqual(self.server.accepted, 1)
        finally:
            db._transport._opener.close()

    def test_o01_receipts_share_the_operation_connection(self):
        receipt = self.transport.submit('table.insert', {})
        self.assertEqual(receipt.wait(timeout=5).state, 'succeeded')
        self.transport.request('table.get', {})
        self.assertEqual([op for _, op, _ in self.server.seen], ['table.insert', 'receipt', 'table.get'])
        self.assertEqual(self.server.accepted, 1)

    def test_o01_server_requested_close_is_not_reused(self):
        self.server.actions[:] = ['close', 'close']
        for _ in range(3):
            self.transport.request('table.get', {})
        self.assertEqual(self.server.accepted, 3)

    def test_o01_idle_close_by_server_is_detected_before_sending(self):
        self.server.actions[:] = ['ok_then_close']
        self.transport.request('table.get', {})
        self.wait_for_server_close()
        self.assertIsNone(self.transport.request('table.insert', {}))
        self.assertEqual(self.server.accepted, 2)
        self.assertEqual([(n, op) for n, op, _ in self.server.seen], [(1, 'table.get'), (2, 'table.insert')])

    def test_o01_perf2e_reuse_bound_stays_under_the_server_idle_bound(self):
        # The OLTP API keeps an idle connection 30 s (server/src/network.rs).
        self.assertEqual(_http_pool.MAX_IDLE_SECONDS, 25.0)
        self.transport.request('table.get', {})
        time.sleep(2.5)   # past the former 2 s bound
        self.transport.request('table.insert', {})
        self.assertEqual(self.server.accepted, 1)
        self.assertEqual([(n, op) for n, op, _ in self.server.seen], [(1, 'table.get'), (1, 'table.insert')])

    def test_o01_perf2e_connection_evicted_while_idle_is_replaced_before_sending(self):
        # A full OLTP listener evicts its oldest idle connection: the server closes
        # it cleanly some time after the response, without reading another byte.
        self.transport.request('table.get', {})
        time.sleep(0.2)
        self.server.sockets[0].shutdown(socket.SHUT_RDWR)
        self.wait_for_server_close()
        self.assertIsNone(self.transport.request('table.insert', {}))
        self.assertEqual(self.server.accepted, 2)
        # The mutation was sent once, on the new connection.
        self.assertEqual([(n, op) for n, op, _ in self.server.seen], [(1, 'table.get'), (2, 'table.insert')])

    def test_o01_hangup_after_send_surfaces_without_retry_and_drops_connection(self):
        for operation, expected in [('table.insert', UnknownOutcomeError), ('table.get', TransportError)]:
            with self.subTest(operation=operation):
                self.server.seen.clear()
                self.transport.request('table.get', {})   # leaves a pooled connection
                accepted = self.server.accepted
                self.server.actions[:] = ['hangup']
                with self.assertRaises(expected) as caught:
                    self.transport.request(operation, {})
                self.assertIs(type(caught.exception), expected)
                # The request reached the server once, on the reused connection.
                self.assertEqual([op for _, op, _ in self.server.seen], ['table.get', operation])
                self.assertEqual(self.server.seen[1][0], accepted)
                self.assertEqual(caught.exception.request_id, self.server.seen[1][2])
                self.assertEqual(self.pool._idle, [])
                self.transport.request('table.get', {})
                self.assertEqual(self.server.accepted, accepted + 1)

    def test_o01_truncated_and_malformed_responses_drop_the_connection(self):
        for action, operation, expected in [('truncate', 'table.get', TransportError),
                                            ('truncate', 'table.upsert', UnknownOutcomeError),
                                            ('malformed', 'table.get', ProtocolError),
                                            ('malformed', 'table.delete', UnknownOutcomeError)]:
            with self.subTest(action=action, operation=operation):
                self.transport.request('table.get', {})
                accepted = self.server.accepted
                before = len(self.server.seen)
                self.server.actions[:] = [action]
                with self.assertRaises(expected):
                    self.transport.request(operation, {})
                self.assertEqual(len(self.server.seen), before + 1, 'failed request was resent')
                self.assertEqual(self.pool._idle, [])
                self.transport.request('table.get', {})
                self.assertEqual(self.server.accepted, accepted + 1)

    def test_o01_connections_idle_past_the_limit_are_replaced(self):
        self.pool._max_idle_seconds = 0.05
        self.transport.request('table.get', {})
        time.sleep(0.1)
        self.transport.request('table.get', {})
        self.assertEqual(self.server.accepted, 2)
        self.assertEqual(len(self.server.seen), 2)

    def test_o01_concurrent_callers_keep_a_bounded_idle_pool(self):
        barrier = threading.Barrier(8)
        failures = []

        def call():
            try:
                barrier.wait(5)
                for _ in range(5):
                    self.transport.request('table.get', {})
            except BaseException as error:  # reported by the main thread
                failures.append(error)

        threads = [threading.Thread(target=call) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertEqual(failures, [])
        self.assertEqual(len(self.server.seen), 40)
        self.assertLessEqual(len(self.pool._idle), _http_pool.MAX_IDLE_CONNECTIONS)
        self.assertLessEqual(self.server.accepted, 8)

    def test_o01_forked_child_does_not_reuse_parent_connections(self):
        self.transport.request('table.get', {})
        with patch('nabedra._http_pool.os.getpid', return_value=-1):
            self.transport.request('table.get', {})
        self.assertEqual(self.server.accepted, 2)

    @unittest.skipUnless(hasattr(os, 'fork') and hasattr(os, 'register_at_fork'), 'needs fork')
    def test_o01_fork_while_the_pool_lock_is_held_does_not_block_the_child(self):
        # Another thread holding the lock at fork time must not deadlock the child.
        with self.pool._lock:
            pid = os.fork()
            if pid == 0:
                os._exit(0 if self.pool._lock.acquire(timeout=2) else 1)
        _, status = os.waitpid(pid, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)


class ProxyFallbackTests(unittest.TestCase):
    def test_o01_configured_proxy_keeps_urllib_opener(self):
        with patch('nabedra.client.getproxies', return_value={'https': 'http://proxy.invalid:3128'}):
            transport = _Transport('https://example.com', 'token')
        self.assertNotIsInstance(transport._opener, _http_pool.ConnectionPool)
        with patch('nabedra.client.getproxies', return_value={}):
            transport = _Transport('https://example.com', 'token')
        self.assertIsInstance(transport._opener, _http_pool.ConnectionPool)

    def test_o01_idle_socket_check_sees_eof_and_unread_data(self):
        left, right = socket.socketpair()
        try:
            self.assertTrue(_http_pool._idle_socket_quiet(left))
            right.sendall(b'x')
            time.sleep(0.01)
            self.assertFalse(_http_pool._idle_socket_quiet(left))
            left.recv(1)
            right.close()
            time.sleep(0.01)
            self.assertFalse(_http_pool._idle_socket_quiet(left))
        finally:
            left.close()
        self.assertFalse(_http_pool._idle_socket_quiet(None))


if __name__ == '__main__':
    unittest.main()

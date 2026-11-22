"""Focused tests against actual local_store TCP processes, using tiny objects."""
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest


SCRIPT = Path(__file__).with_name('local_store.py')
TOKEN = 'local-test-token'


class StoreProcess:
    def __init__(self, database, extra_args=()):
        self.database = database
        self.extra_args = list(extra_args)
        self.process = None

    def start(self):
        self.process = subprocess.Popen(
            [sys.executable, str(SCRIPT), '--database', str(self.database), '--port', '0',
             *self.extra_args],
            env={**os.environ, 'NABEDRA_STORE_TOKEN': TOKEN},
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        if not select.select([self.process.stdout], [], [], 5)[0]:
            self.stop()
            raise AssertionError('Store did not announce its TCP address')
        line = self.process.stdout.readline()
        if not line:
            self.stop()
            raise AssertionError('Store exited before announcing its TCP address')
        address = json.loads(line)
        self.port = address['port']
        return self

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise AssertionError('Store did not shut down gracefully')
            finally:
                self.process.stdout.close()
                self.process.stderr.close()
            returncode = self.process.returncode
            self.process = None
            if returncode != 0:
                raise AssertionError('Store exited unsuccessfully')

    def request(self, method, key='/v1/objects/test/item', body=b'', headers=None):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=5)
        try:
            request_headers = {'Authorization': f'Bearer {TOKEN}'}
            if method != 'GET' or body:
                request_headers['Content-Length'] = str(len(body))
            request_headers.update(headers or {})
            connection.request(method, key, body=body if method != 'GET' or body else None,
                               headers=request_headers)
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def raw(self, target, headers):
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as sock:
            sock.sendall((f'PUT {target} HTTP/1.1\r\nHost: localhost\r\n'
                          f'Authorization: Bearer {TOKEN}\r\n{headers}\r\n\r\n').encode())
            response = http.client.HTTPResponse(sock)
            response.begin()
            response.read()
            return response.status


class LocalStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = StoreProcess(Path(self.temp.name) / 'store.sqlite')
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.store.stop)
        self.store.start()

    def test_conditional_tcp_storage_and_restart(self):
        store = self.store
        for route in ('/health/live', '/health/ready'):
            self.assertEqual(store.request('GET', route, headers={'Authorization': ''})[0], 200)
        self.assertEqual(store.request('GET', '/health/ready',
                                       headers={'Content-Length': '0'})[0], 200)
        self.assertEqual(store.request('GET', headers={'Authorization': 'Bearer wrong'})[0], 401)
        self.assertEqual(store.request('PUT', body=b'x', headers={'Authorization': ''})[0], 401)
        self.assertEqual(store.request('GET')[0], 404)
        no_token = {k: v for k, v in os.environ.items() if k != 'NABEDRA_STORE_TOKEN'}
        refused = subprocess.run(
            [sys.executable, str(SCRIPT), '--database', str(store.database), '--port', '0'],
            env=no_token, capture_output=True, timeout=5,
        )
        self.assertEqual(refused.returncode, 2)
        self.assertEqual(refused.stdout, b'')
        bad_headers = [
            'Content-Length: 0',
            'Content-Length: 1\r\nIf-Match: unquoted',
            'Content-Length: 0\r\nIf-Match: "g1"\r\nIf-None-Match: *',
            'Content-Length: 0\r\nIf-None-Match: *\r\nIf-None-Match: *',
            'If-None-Match: *',
            'Content-Length: 0\r\nContent-Length: 0\r\nIf-None-Match: *',
            'Content-Length: -1\r\nIf-None-Match: *',
            'Content-Length: 83886081\r\nIf-None-Match: *',
            'Content-Length: 1\r\nTransfer-Encoding: chunked\r\nIf-None-Match: *',
        ]
        for headers in bad_headers:
            with self.subTest(headers=headers):
                self.assertIn(store.raw('/v1/objects/test/item', headers), (400, 411, 413))
        for key in ('test/../a', 'test/./a', 'test//a', 'test/a%2fb', 'test/a?b',
                    'test/a\\b', 'test/a\x01', 'solo', 'test/', 'test/' + 'a' * 4096):
            with self.subTest(key=key):
                self.assertIn(store.raw('/v1/objects/' + key,
                                        'Content-Length: 0\r\nIf-None-Match: *'), (400, 414))
        # Declaring more bytes than supplied must not create a partial object.
        with socket.create_connection(('127.0.0.1', store.port), timeout=5) as sock:
            sock.sendall((f'PUT /v1/objects/test/item HTTP/1.1\r\nHost: localhost\r\n'
                          f'Authorization: Bearer {TOKEN}\r\nContent-Length: 2\r\n'
                          'If-None-Match: *\r\n\r\nx').encode())
            sock.shutdown(socket.SHUT_WR)
            response = http.client.HTTPResponse(sock)
            response.begin()
            self.assertEqual(response.status, 400)
            response.read()
        self.assertEqual(store.request('GET')[0], 404)
        status, headers, _ = store.request('PUT', body=b'original', headers={'If-None-Match': '*'})
        self.assertEqual(status, 204)
        first = headers['ETag']
        self.assertRegex(first, r'^"[^"\x00-\x20]+"$')
        self.assertEqual(store.request('PUT', body=b'collision', headers={'If-None-Match': '*'})[0], 412)
        barrier = threading.Barrier(2)

        def update(body):
            barrier.wait(timeout=5)
            return body, store.request('PUT', body=body, headers={'If-Match': first})

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(update, (b'left', b'right')))
        self.assertEqual(sorted(result[0] for _, result in results), [204, 412])
        winner, response = next(pair for pair in results if pair[1][0] == 204)
        second = response[1]['ETag']
        self.assertNotEqual(first, second)
        status, headers, body = store.request('GET')
        self.assertEqual((status, headers['ETag'], body, headers['Content-Length']),
                         (200, second, winner, str(len(winner))))
        status, headers, _ = store.request('PUT', body=winner, headers={'If-Match': second})
        self.assertEqual(status, 204)
        third = headers['ETag']
        self.assertNotIn(third, (first, second))
        store.stop()
        store.start()
        status, headers, body = store.request('GET')
        self.assertEqual((status, headers['ETag'], body), (200, third, winner))
        self.assertEqual(store.request('PUT', body=b'ABA', headers={'If-Match': second})[0], 412)
        status, headers, _ = store.request('PUT', body=b'after restart', headers={'If-Match': third})
        self.assertEqual(status, 204)
        self.assertNotIn(headers['ETag'], (first, second, third))

    def test_post_commit_response_loss_is_recovered_by_get(self):
        # A TCP proxy consumes the upstream committed response, then drops it.
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen(1)
            listener.settimeout(5)
            def discard_response():
                with listener.accept()[0] as downstream:
                    downstream.settimeout(5)
                    request = b''
                    while b'\r\n\r\n' not in request:
                        request += downstream.recv(4096)
                    with socket.create_connection(('127.0.0.1', self.store.port), timeout=5) as upstream:
                        upstream.sendall(request)
                        response = http.client.HTTPResponse(upstream)
                        response.begin()
                        response.read()
                        return response.status, response.getheader('ETag')
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                dropped = pool.submit(discard_response)
                connection = http.client.HTTPConnection('127.0.0.1', listener.getsockname()[1], timeout=5)
                try:
                    connection.request('PUT', '/v1/objects/test/item', body=b'', headers={
                        'Authorization': f'Bearer {TOKEN}', 'If-None-Match': '*', 'Content-Length': '0'})
                    with self.assertRaises(http.client.RemoteDisconnected):
                        connection.getresponse()
                finally:
                    connection.close()
                status, acknowledged_to_proxy = dropped.result(timeout=5)
                self.assertEqual(status, 204)
        status, headers, body = self.store.request('GET')
        self.assertEqual((status, headers['ETag'], body), (200, acknowledged_to_proxy, b''))
        self.assertEqual(self.store.request('PUT', headers={'If-None-Match': '*'})[0], 412)


FAULT = 'X-Nabedra-Test-Fault'


class TestFaultTests(unittest.TestCase):
    """--test-faults: X-Nabedra-Test-Fault reaches HttpStore over real HTTP."""

    def start(self, *args):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        store = StoreProcess(Path(temp.name) / 'store.sqlite', args)
        self.addCleanup(store.stop)
        return store.start()

    def disconnected(self, store, method, key, body=b'', headers=None):
        try:
            store.request(method, key, body=body, headers=headers)
        except (http.client.RemoteDisconnected, ConnectionResetError):
            return True
        return False

    def test_header_is_ignored_without_the_flag(self):
        store = self.start()
        status, _, _ = store.request('PUT', body=b'x', headers={'If-None-Match': '*', FAULT: 'status=503'})
        self.assertEqual(status, 204)
        self.assertEqual(store.request('GET', headers={FAULT: 'status=503'})[0], 200)

    def test_put_faults(self):
        store = self.start('--test-faults')
        create = {'If-None-Match': '*'}
        key = '/v1/objects/test/lost'
        self.assertTrue(self.disconnected(store, 'PUT', key, b'lost', {**create, FAULT: 'lost-response'}))
        status, _, body = store.request('GET', key)
        self.assertEqual((status, body), (200, b'lost'))
        for code in (412, 409, 503, 404):
            with self.subTest(code=code):
                key = f'/v1/objects/test/status{code}'
                status, _, _ = store.request('PUT', key, b'x', {**create, FAULT: f'status={code}'})
                self.assertEqual(status, code)
                self.assertEqual(store.request('GET', key)[0], 404)
        key = '/v1/objects/test/embedded'
        status, headers, body = store.request('PUT', key, b'x', {**create, FAULT: 'status=200'})
        self.assertEqual(status, 200)
        self.assertIn(b'<Error>', body)
        self.assertNotIn('ETag', headers)
        self.assertEqual(store.request('GET', key)[0], 404)
        for bad in ('nonsense', 'status=9999', 'delay-apply=', 'status=200,lost-response'):
            with self.subTest(bad=bad):
                key = '/v1/objects/test/bad'
                self.assertEqual(store.request('PUT', key, b'x', {**create, FAULT: bad})[0], 400)
                self.assertEqual(store.request('GET', key)[0], 404)

    def test_delayed_apply_checks_its_condition_late(self):
        store = self.start('--test-faults')
        key = '/v1/objects/test/late'
        self.assertTrue(self.disconnected(store, 'PUT', key, b'late',
                                          {'If-None-Match': '*', FAULT: 'delay-apply=300'}))
        self.assertEqual(store.request('GET', key)[0], 404)
        deadline = time.monotonic() + 5
        while store.request('GET', key)[0] == 404 and time.monotonic() < deadline:
            time.sleep(0.05)
        status, headers, body = store.request('GET', key)
        self.assertEqual((status, body), (200, b'late'))
        # A late If-Match whose version was replaced meanwhile must not apply.
        stale = headers['ETag']
        self.assertTrue(self.disconnected(store, 'PUT', key, b'stale',
                                          {'If-Match': stale, FAULT: 'delay-apply=300'}))
        status, headers, _ = store.request('PUT', key, b'fresh', {'If-Match': stale})
        self.assertEqual(status, 204)
        time.sleep(0.8)
        self.assertEqual(store.request('GET', key)[2], b'fresh')

    def test_get_faults(self):
        store = self.start('--test-faults')
        key = '/v1/objects/test/item'
        self.assertEqual(store.request('PUT', key, b'x', {'If-None-Match': '*'})[0], 204)
        self.assertEqual(store.request('GET', key, headers={FAULT: 'status=503'})[0], 503)
        self.assertEqual(store.request('GET', key, headers={FAULT: 'status=404'})[0], 404)
        self.assertTrue(self.disconnected(store, 'GET', key, headers={FAULT: 'lost-response'}))
        self.assertEqual(store.request('GET', key, headers={FAULT: 'delay-apply=5'})[0], 400)
        self.assertEqual(store.request('GET', key)[2], b'x')


if __name__ == '__main__':
    unittest.main()

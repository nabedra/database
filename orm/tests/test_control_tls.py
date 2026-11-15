"""Real TLS evidence for the private cluster administration transport."""

import importlib.util
import json
from pathlib import Path
import ssl
import sys
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

from meshdb._control_transport import _ControlTransport
from meshdb.errors import TransportError, UnknownOutcomeError


def _create_test_tls(directory):
    # Resolve the repository helper independently of cwd and installed packages.
    helper = Path(__file__).resolve().parents[2] / 'scripts' / 'test_tls_material.py'
    name = '_meshdb_control_tls_fixture'
    spec = importlib.util.spec_from_file_location(name, helper)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        return module.create_test_tls(directory)
    finally:
        sys.modules.pop(name, None)


class _TLSServer(HTTPServer):
    def __init__(self, context, handler):
        self.context = context
        self.seen = []
        self.handshake_rejected = threading.Event()
        self.handler_failed = False
        super().__init__(('127.0.0.1', 0), handler)

    def get_request(self):
        raw, address = super().get_request()
        try:
            raw.settimeout(5)
            return self.context.wrap_socket(raw, server_side=True), address
        except BaseException as error:
            raw.close()
            if isinstance(error, ssl.SSLError):
                self.handshake_rejected.set()
            raise

    def handle_error(self, request, client_address):
        # Do not print peer-controlled errors or request credentials.
        self.handler_failed = True


class ControlTLSTests(unittest.TestCase):
    def test_verified_policy_then_untrusted_handshake_is_pre_send(self):
        temporary = tempfile.TemporaryDirectory(prefix='meshdb-control-tls-')
        self.addCleanup(temporary.cleanup)
        material = _create_test_tls(Path(temporary.name) / 'tls')
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(material.server_cert, material.server_key)
        response_data = {'revision': 3, 'nodes': 2}

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                self.server.seen.append((self.path, self.headers['Authorization'], request))
                body = json.dumps({
                    'request_id': request['request_id'], 'data': response_data,
                }).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                self.close_connection = True

            def log_message(self, *args):
                pass

        server = _TLSServer(context, Handler)
        self.addCleanup(server.server_close)
        thread = threading.Thread(target=server.serve_forever, name='meshdb-test-https')
        thread.start()

        def stop_server():
            server.shutdown()
            thread.join()

        self.addCleanup(stop_server)
        url = f'https://127.0.0.1:{server.server_port}/admin/prefix/'
        token = 'disposable-test-admin-token'
        request_id = str(uuid.uuid4())
        payload = {'nodes': 2}
        trusted = _ControlTransport(url, token, timeout=5, ca_file=str(material.ca_cert))
        result = trusted.request('cluster.scale', payload, request_id=request_id,
                                 deadline=time.monotonic() + 10)
        self.assertEqual(result, (request_id, response_data))
        self.assertEqual(len(server.seen), 1)
        path, authorization, envelope = server.seen[0]
        self.assertEqual(path, '/admin/prefix/v1/cluster/operations')
        # A boolean assertion keeps credentials out of failure output.
        self.assertTrue(authorization == 'Bearer ' + token, 'bearer authorization changed')
        self.assertEqual(envelope, {
            'request_id': request_id, 'operation': 'cluster.scale', 'payload': payload,
        })
        self.assertFalse(server.handshake_rejected.is_set())

        untrusted_id = str(uuid.uuid4())
        untrusted = _ControlTransport(url, token, timeout=5)
        with self.assertRaises(TransportError) as caught:
            untrusted.request('cluster.scale', payload, request_id=untrusted_id)
        error = caught.exception
        self.assertIs(type(error), TransportError)
        self.assertNotIsInstance(error, UnknownOutcomeError)
        self.assertEqual(error.request_id, untrusted_id)
        self.assertIsNone(error.status_code)
        self.assertNotEqual(error.outcome, 'unknown')
        # Inspect only the type of the suppressed cause, never its untrusted text.
        self.assertTrue(isinstance(error.__context__, ssl.SSLCertVerificationError),
                        'expected a real certificate verification failure')
        self.assertTrue(server.handshake_rejected.wait(5), 'peer did not reject the handshake')
        self.assertEqual(len(server.seen), 1, 'untrusted call delivered a policy')
        self.assertFalse(server.handler_failed, 'HTTPS request handler failed')


if __name__ == '__main__':
    unittest.main()

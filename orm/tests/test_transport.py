"""Transport contract tests using one local HTTP fixture and bounded fake streams."""
import json
import ssl
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from meshdb import errors
from meshdb.client import _Transport

LIMIT = 8 * 1024 * 1024


class TransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                request = json.loads(body)
                cls.seen.append((self.path, dict(self.headers), body, request))
                if cls.raw_response is not None:
                    self.wfile.write(cls.raw_response)
                    self.close_connection = True
                    return
                status, headers, response, truncate = cls.reply(request)
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header('Content-Length', str(len(response) + truncate))
                self.end_headers()
                self.wfile.write(response)
                self.close_connection = True

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
        type(self).seen = []
        type(self).raw_response = None
        self.set_reply(lambda r: {'request_id': r['request_id'], 'data': None})
        self.transport = _Transport(self.url + '/prefix///', 'test-token', timeout=2)

    def set_reply(self, envelope, status=200, content_type='application/json', truncate=0):
        def reply(request):
            data = envelope(request) if callable(envelope) else envelope
            body = data if isinstance(data, bytes) else json.dumps(data).encode()
            return status, {'Content-Type': content_type, 'Location': self.url + '/redirect'}, body, truncate
        type(self).reply = staticmethod(reply)

    def test_exact_wire_and_fresh_ids_and_decoder(self):
        self.set_reply(lambda r: {'request_id': r['request_id'], 'data': {'value': 'é'}})
        for _ in range(2):
            self.assertEqual(self.transport.request('table.get', {'table_id': 'id', 'key': 'é'}, decode=lambda d: d['value']), 'é')
        ids = []
        for path, headers, body, request in self.seen:
            self.assertEqual(path, '/prefix/v1/operations')
            self.assertEqual(headers['Authorization'], 'Bearer test-token')
            self.assertEqual(headers['Content-Type'], 'application/json')
            self.assertEqual(headers['Accept'], 'application/json')
            self.assertIn('é'.encode(), body)
            self.assertEqual(request['operation'], 'table.get')
            self.assertEqual(request['payload'], {'table_id': 'id', 'key': 'é'})
            self.assertEqual(set(request), {'request_id', 'operation', 'payload'})
            ids.append(uuid.UUID(request['request_id']))
        self.assertNotEqual(*ids)

    def test_server_error_mapping_and_diagnostics(self):
        cases = [('validation_error', 400, errors.ValidationError), ('unsupported_operation', 400, errors.MeshDBError),
                 ('unauthorized', 401, errors.AuthenticationError), ('not_found', 404, errors.NotFoundError),
                 ('conflict', 409, errors.ConflictError), ('limit_exceeded', 413, errors.LimitExceededError),
                 ('overloaded', 429, errors.OverloadedError), ('storage_unavailable', 503, errors.StorageError),
                 ('internal_error', 500, errors.MeshDBError)]
        for code, status, expected in cases:
            with self.subTest(code=code):
                self.set_reply({'request_id': None, 'error': {'code': code, 'message': 'failed', 'outcome': 'not_applied'}}, status)
                with self.assertRaises(expected) as caught:
                    self.transport.request('table.insert', {})
                error = caught.exception
                self.assertIs(type(error), expected)
                self.assertEqual((error.code, error.status_code, error.outcome), (code, status, 'not_applied'))
                self.assertEqual(error.request_id, self.seen[-1][3]['request_id'])
        self.set_reply(lambda r: {'request_id': r['request_id'], 'error': {'code': 'storage_unavailable', 'message': 'lost', 'outcome': 'unknown'}}, 503)
        with self.assertRaises(errors.UnknownOutcomeError):
            self.transport.request('table.get', {})

    def test_invalid_responses_respect_mutation_boundary(self):
        cases = [(b'not json', 200, 'application/json'),
                 (lambda r: {'request_id': r['request_id'], 'data': None}, 201, 'application/json'),
                 ({'request_id': str(uuid.uuid4()), 'data': None}, 200, 'application/json'),
                 ({'request_id': None, 'data': None}, 200, 'application/json'),
                 (lambda r: {'request_id': r['request_id'], 'data': None}, 200, 'text/html'),
                 (b'{"request_id":null,"data":NaN}', 200, 'application/json'),
                 ({'request_id': None, 'error': {'code': 'conflict', 'message': 'x', 'outcome': 'not_applied'}}, 500, 'application/json'),
                 (b'x' * (LIMIT + 1), 503, 'application/json')]
        for body, status, content_type in cases:
            self.set_reply(body, status, content_type)
            for operation, expected in [('table.get', errors.ProtocolError), ('table.insert', errors.UnknownOutcomeError)]:
                with self.subTest(status=status, operation=operation), self.assertRaises(expected) as caught:
                    self.transport.request(operation, {})
                self.assertEqual(caught.exception.request_id, self.seen[-1][3]['request_id'])

    def test_redirect_is_not_followed(self):
        self.set_reply(b'', 307)
        with self.assertRaises(errors.UnknownOutcomeError):
            self.transport.request('table.insert', {})
        self.assertEqual(len(self.seen), 1)

    def test_malformed_http_differs_from_disconnects(self):
        cases = [
            (b'HTTP/1.1 invalid\r\nContent-Length: 0\r\n\r\n', errors.ProtocolError),
            (b'HTTP/1.1 200 OK\r\nX: ' + b'a' * 65537 + b'\r\n\r\n', errors.ProtocolError),
            (b'', errors.TransportError),
        ]
        for response, read_error in cases:
            type(self).raw_response = response
            for operation, expected in [('table.get', read_error), ('table.insert', errors.UnknownOutcomeError)]:
                with self.subTest(response_size=len(response), operation=operation):
                    with self.assertRaises(expected) as caught:
                        self.transport.request(operation, {})
                    self.assertIs(type(caught.exception), expected)
                    self.assertEqual(caught.exception.request_id, self.seen[-1][3]['request_id'])

    def test_truncated_response_and_decoder_failure(self):
        self.set_reply(lambda r: {'request_id': r['request_id'], 'data': None}, truncate=20)
        for operation, expected in [('table.get', errors.TransportError), ('table.delete', errors.UnknownOutcomeError)]:
            with self.assertRaises(expected):
                self.transport.request(operation, {})
        self.set_reply(lambda r: {'request_id': r['request_id'], 'data': {'invalid': True}})
        def invalid(data):
            raise errors.ValidationError('invalid result')
        for operation, expected in [('table.get', errors.ProtocolError), ('table.create', errors.UnknownOutcomeError)]:
            with self.assertRaises(expected):
                self.transport.request(operation, {}, decode=invalid)
        def bug(data):
            raise RuntimeError('caller bug')
        with self.assertRaisesRegex(RuntimeError, 'caller bug'):
            self.transport.request('table.insert', {}, decode=bug)

    def test_local_preflight_never_opens_connection(self):
        bad_urls = ['relative', 'ftp://host', 'https://user:pass@host', 'https://host/?x', 'https://host/#x',
                    'http://', 'http://host:bad', 'http://host/\npath', 'http://host/?', 'http://host/#']
        for url in bad_urls:
            with self.subTest(url=url), self.assertRaises(errors.ValidationError):
                _Transport(url, 'token')
        for token in ['', ' ', 'abc\r\nInjected: yes', 'é', 'a b']:
            with self.subTest(token=token), self.assertRaises(errors.ValidationError):
                _Transport(self.url, token)
        for timeout in [0, -1, float('nan'), float('inf'), True, '2']:
            with self.subTest(timeout=timeout), self.assertRaises(errors.ValidationError):
                _Transport(self.url, 'token', timeout=timeout)
        with patch.object(self.transport._opener, 'open') as opened:
            for payload, expected in [({'x': float('nan')}, errors.ValidationError), ({'x': object()}, errors.ValidationError),
                                      ({'x': '\ud800'}, errors.ValidationError), ({'x': 'x' * LIMIT}, errors.LimitExceededError)]:
                with self.assertRaises(expected):
                    self.transport.request('table.insert', payload)
            with self.assertRaises(errors.ValidationError):
                self.transport.request('not.an.operation', {})
            opened.assert_not_called()

    def test_private_ssl_context_adds_ca_without_network(self):
        with patch('meshdb.client.ssl.create_default_context', wraps=ssl.create_default_context) as create, patch.object(ssl.SSLContext, 'load_verify_locations') as load:
            with patch('urllib.request.OpenerDirector.open') as opened:
                _Transport('https://example.test', 'token', ca_file='extra.pem')
                opened.assert_not_called()
            create.assert_called_once_with()
            load.assert_called_once_with(cafile='extra.pem')
        transport = _Transport('https://example.test', 'token')
        contexts = [h._context for h in transport._opener.handlers if hasattr(h, '_context')]
        self.assertEqual(len(contexts), 1)
        self.assertTrue(contexts[0].check_hostname)
        self.assertEqual(contexts[0].verify_mode, ssl.CERT_REQUIRED)

    def test_bounded_reads_close_success_http_error_and_read_failure(self):
        for mode in ['success', 'http_error', 'read_failure', 'http_error_read_failure', 'malformed', 'decoder_failure']:
            with self.subTest(mode=mode):
                response = Mock()
                response.status = 200
                response.headers = {'Content-Type': 'application/json'}
                def read(size):
                    self.assertEqual(size, LIMIT + 1)
                    if mode in ('read_failure', 'http_error_read_failure'):
                        raise OSError('lost')
                    if mode == 'malformed':
                        return b'not JSON'
                    request = opened.call_args.args[0]
                    request_id = json.loads(request.data)['request_id']
                    if mode == 'http_error':
                        return json.dumps({'request_id': request_id, 'error': {'code': 'conflict', 'message': 'x', 'outcome': 'not_applied'}}).encode()
                    return json.dumps({'request_id': request_id, 'data': None}).encode()
                response.read.side_effect = read
                with patch.object(self.transport._opener, 'open') as opened:
                    if mode in ('http_error', 'http_error_read_failure'):
                        opened.side_effect = HTTPError(self.url, 409, 'conflict', response.headers, response)
                    else:
                        opened.return_value = response
                    def decode(data):
                        if mode == 'decoder_failure':
                            raise errors.ValidationError('bad shape')
                        return data
                    if mode == 'success':
                        self.assertIsNone(self.transport.request('table.insert', {}, decode=decode))
                    else:
                        expected = errors.ConflictError if mode == 'http_error' else errors.UnknownOutcomeError
                        with self.assertRaises(expected):
                            self.transport.request('table.insert', {}, decode=decode)
                    self.assertEqual(opened.call_args.kwargs, {'timeout': 2.0})
                response.close.assert_called_once()
        with patch.object(self.transport._opener, 'open', side_effect=URLError('lost')) as opened:
            with self.assertRaises(errors.TransportError):
                self.transport.request('table.list', {})
            with self.assertRaises(errors.UnknownOutcomeError):
                self.transport.request('table.upsert', {})
            self.assertEqual(opened.call_count, 2)


if __name__ == '__main__':
    unittest.main()

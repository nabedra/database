"""Transport contract tests using one local HTTP fixture and bounded fake streams."""
import json
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from nabedra import errors
from nabedra.client import _Transport

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
        cases = [('validation_error', 400, errors.ValidationError), ('unsupported_operation', 400, errors.NabedraError),
                 ('unauthorized', 401, errors.AuthenticationError), ('not_found', 404, errors.NotFoundError),
                 ('conflict', 409, errors.ConflictError), ('limit_exceeded', 413, errors.LimitExceededError),
                 ('overloaded', 429, errors.OverloadedError), ('storage_unavailable', 503, errors.StorageError),
                 ('internal_error', 500, errors.NabedraError)]
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

    def test_strict_json_rejects_ambiguous_and_nonfinite_successes(self):
        # Each envelope has the correct ID, so its data must cause rejection.
        for data in (b'0,"data":1', b'{"value":0,"value":1}', b'NaN',
                     b'Infinity', b'-Infinity', b'1e999', b'"\xff"'):
            def response(request):
                return (b'{"request_id":' + json.dumps(request['request_id']).encode()
                        + b',"data":' + data + b'}')

            self.set_reply(response)
            for operation, expected in [('table.get', errors.ProtocolError),
                                        ('table.insert', errors.UnknownOutcomeError)]:
                with self.subTest(data=data, operation=operation):
                    before = len(self.seen)
                    with self.assertRaises(expected) as caught:
                        self.transport.request(operation, {})
                    self.assertEqual(len(self.seen), before + 1, 'invalid replies must not retry')
                    self.assertEqual(caught.exception.request_id, self.seen[-1][3]['request_id'])
                    self.assertEqual(caught.exception.status_code, 200)

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

    def test_od3_unknown_mutation_retries_only_its_own_identity(self):
        # D12 after OD-3: a full leader answers a mutation 503 outcome_unknown. The
        # client raises at once (no resend of its own); retry() sends the same request
        # ID and bytes, and answers that identity's result.
        overloaded = {'code': 'outcome_unknown', 'outcome': 'unknown',
                      'message': 'direct request outcome unknown after overloaded: direct in-flight budget '
                                 'is full; retry the same request_id and body'}
        answers = []

        def reply(request):
            answers.append(request['request_id'])
            if len(answers) == 1:
                return {'request_id': request['request_id'], 'error': overloaded}
            return {'request_id': request['request_id'], 'data': True}
        self.set_reply(reply, 503)
        with self.assertRaises(errors.UnknownOutcomeError) as caught:
            self.transport.request('table.update', {'table_id': 't', 'key': 1, 'values': {'v': 2}},
                                   decode=lambda data: data)
        error = caught.exception
        self.assertEqual((error.code, error.status_code, error.outcome), ('outcome_unknown', 503, 'unknown'))
        self.assertIn('after overloaded: direct in-flight budget is full', str(error))
        self.assertEqual(len(self.seen), 1, 'the client does not resend by itself')
        self.assertTrue(error.retryable)
        self.set_reply(reply)
        self.assertIs(error.retry(), True)
        self.assertEqual(len(self.seen), 2)
        (_, _, first, sent), (_, _, again, resent) = self.seen
        self.assertEqual(again, first, 'the retry sends the exact body')
        self.assertEqual(resent['request_id'], sent['request_id'])
        self.assertEqual(error.request_id, sent['request_id'])
        # A read has no identity to retry; a later call is a new request.
        self.set_reply(lambda r: {'request_id': r['request_id'], 'error': overloaded}, 503)
        with self.assertRaises(errors.UnknownOutcomeError) as caught:
            self.transport.request('table.get', {})
        self.assertFalse(caught.exception.retryable)
        with self.assertRaises(errors.ValidationError):
            caught.exception.retry()

    def test_od3_unknown_submit_retries_its_own_identity(self):
        overloaded = {'code': 'outcome_unknown', 'outcome': 'unknown',
                      'message': 'direct request outcome unknown after overloaded: worker handler lane full; '
                                 'retry the same request_id and body'}
        run_id = str(uuid.uuid4())
        self.set_reply(lambda r: {'request_id': r['request_id'], 'error': overloaded}, 503)
        with self.assertRaises(errors.UnknownOutcomeError) as caught:
            self.transport.submit('table.insert', {'table_id': 't', 'row': {'id': 1}})
        self.assertTrue(caught.exception.retryable)
        self.set_reply(lambda r: {'request_id': r['request_id'], 'run_id': run_id, 'state': 'queued'}, 202)
        receipt = caught.exception.retry()
        self.assertEqual((receipt.request_id, receipt.run_id), (caught.exception.request_id, run_id))
        (_, _, first, sent), (path, _, again, resent) = self.seen
        self.assertTrue(path.endswith('/v1/requests'))
        self.assertEqual((again, resent['request_id']), (first, sent['request_id']))


if __name__ == '__main__':
    unittest.main()

"""Behavioral contract for the private callback codec and authority client."""
import base64
import hashlib
import json
import socket
import threading
import time
import unittest
from pathlib import Path

_MODULE = Path(__file__).parents[1] / 'src/meshdb/_callback_protocol.py'
if _MODULE.exists():
    from meshdb import _callback_protocol as p
else:
    p = None

C, N, R, I, O, S = [f'00000000-0000-4000-8000-{n:012d}' for n in range(1, 7)]
TOKEN = base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip('=')


def command(action='start'):
    bootstrap = dict(version=1, cluster_id=C, node_id=N, request_id=R,
                     api_urls=['https://api.example'], listen='rust validates this',
                     advertise_url=None, storage={'driver': 'custom', 'options': {'secret': 'private'}},
                     enrollment_token='private', tls=None, operation_slots=4,
                     partition_slots=4, queue_size=0, partition_size='rust validates this')
    value = dict(version=1, invocation_id=I, cluster_id=C, owner_id=O, epoch=1,
                 intent_id=R, action=action,
                 target=dict(node_id=N, request_id=R, incarnation_id=None),
                 payload=dict(version=1, cluster_id=C, cluster_name='cluster_1',
                              request_id=R, node_id=N, bootstrap=bootstrap))
    if action == 'stop':
        value.update(intent_id=S, payload={'resource_id': 'resource'})
    if action == 'inventory':
        value.update(intent_id=None, target=None, payload={})
    return value


def wire(value):
    return json.dumps(value, ensure_ascii=False).encode()


def record(resource='resource', state='running'):
    return dict(resource_id=resource, cluster_id=C, node_id=N, request_id=R, state=state)


class Authority:
    """Independent HTTP wire fixture, one accepted connection, no production codec."""
    def __init__(self, response=None, mode=None):
        self.response, self.mode = response, mode
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(4)
        self.listener.settimeout(3)
        self.url = f'http://127.0.0.1:{self.listener.getsockname()[1]}/private/v1/authorize'
        self.received = None
        self.count = 0
        self.error = None
        self.entered = threading.Event()
        self.release = threading.Event()
        self.peer_closed = threading.Event()
        self.thread = threading.Thread(target=self.run)

    def __enter__(self):
        self.thread.start()
        return self

    def run(self):
        try:
            with self.listener.accept()[0] as peer:
                self.count += 1
                peer.settimeout(3)
                data = b''
                while b'\r\n\r\n' not in data:
                    data += peer.recv(4096)
                headers, body = data.split(b'\r\n\r\n', 1)
                length = int(next(line.split(b':', 1)[1] for line in headers.split(b'\r\n')
                                  if line.lower().startswith(b'content-length:')))
                while len(body) < length:
                    body += peer.recv(4096)
                self.received = (headers, body)
                self.entered.set()
                if self.mode == 'lost':
                    return
                if self.mode == 'held':
                    self.release.wait(3)
                    if peer.recv(1) != b'':
                        raise AssertionError('client socket was not closed')
                    self.peer_closed.set()
                    return
                if self.mode == 'trickle':
                    # Keep sending until the client's absolute deadline closes it.
                    peer.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n')
                    while not self.release.wait(0.02):
                        peer.sendall(b' ')
                    return
                response = self.response
                if response is None:
                    payload = json.dumps({'version': 1, 'invocation_id': I,
                                          'command_sha256': hashlib.sha256(body).hexdigest(),
                                          'authorized': True}).encode()
                    response = (b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '
                                + str(len(payload)).encode() + b'\r\n\r\n' + payload)
                peer.sendall(response)
        except (BrokenPipeError, ConnectionResetError):
            self.peer_closed.set()
        except BaseException as exc:
            self.error = exc

    def __exit__(self, *args):
        self.release.set()
        self.thread.join(4)
        if self.thread.is_alive():
            raise AssertionError('authority did not exit')
        self.listener.setblocking(False)
        try:
            while True:
                peer, _ = self.listener.accept()
                peer.close()
                self.count += 1
        except BlockingIOError:
            pass
        finally:
            self.listener.close()
        if self.error:
            raise self.error


class CallbackProtocolTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(p, 'P03c1 callback protocol implementation is missing')

    def reject(self, fn, code):
        with self.assertRaises(p._CallbackProtocolError) as caught:
            fn()
        self.assertEqual(caught.exception.code, code)
        self.assertNotIn('private', str(caught.exception))
        self.assertNotIn('private', repr(caught.exception))
        self.assertIsNone(caught.exception.__cause__)
        return caught.exception

    def test_commands_preserve_bytes_and_correlate_identities(self):
        for action in ('start', 'stop', 'inventory'):
            raw = b' \n' + wire(command(action)) + b'\n'
            parsed = p._decode_command(raw)
            self.assertIs(parsed.original_bytes, raw)
            self.assertEqual(parsed.value['action'], action)
            self.assertNotIn('private', repr(parsed))
        mutations = [lambda c: c.update(epoch=True), lambda c: c.update(version=1.0),
                     lambda c: c.update(epoch=2**64), lambda c: c.update(extra=1),
                     lambda c: c.update(owner_id=O.upper().replace('00000000', 'ABCDEFAB', 1)),
                     lambda c: c.update(intent_id=S),
                     lambda c: c['target'].update(incarnation_id=S),
                     lambda c: c['payload'].update(cluster_name='bad-name'),
                     lambda c: c['payload']['bootstrap'].update(node_id=S),
                     lambda c: c['payload']['bootstrap'].update(storage={'driver': 'x', 'options': []}),
                     lambda c: c['payload']['bootstrap'].update(operation_slots=0),
                     lambda c: c['payload']['bootstrap'].update(partition_slots=257),
                     lambda c: c['payload']['bootstrap'].update(queue_size=4097),
                     lambda c: c['payload']['bootstrap'].update(api_urls=[]),
                     lambda c: c['payload']['bootstrap'].update(api_urls=['x'] * 17),
                     lambda c: c['payload']['bootstrap'].update(advertise_url='https://worker'),
                     lambda c: c['payload']['bootstrap'].update(tls={'cert_file': 'private'})]
        for mutate in mutations:
            c = command(); mutate(c)
            with self.subTest(mutation=mutations.index(mutate)):
                self.reject(lambda: p._decode_command(wire(c)), 'invalid_command')
        c = command(); c['epoch'] = 2**64-1
        p._decode_command(wire(c))
        for resource in ('', 'private\x00', 'é'*129):
            c = command('stop'); c['payload']['resource_id'] = resource
            self.reject(lambda: p._decode_command(wire(c)), 'invalid_command')
        c = command('stop'); c['intent_id'] = R
        self.reject(lambda: p._decode_command(wire(c)), 'invalid_command')
        c = command('inventory'); c['target'] = command()['target']
        self.reject(lambda: p._decode_command(wire(c)), 'invalid_command')

    def test_strict_json_depth_size_and_generic_bootstrap(self):
        for raw in (b'[]', b'\xff', b'{"a":1,"a":2}', b'{"x":NaN}',
                    b'{"x":1e999}', b'{"version":1,"version":1}', b'{"x":"\\ud800"}', b'{' + b'"x":[' * 33 + b'0' + b']' * 33 + b'}'):
            self.reject(lambda: p._decode_command(raw), 'invalid_command')
        self.reject(lambda: p._decode_command(b' ' * (1024*1024+1)), 'limit_exceeded')
        c = command(); b = c['payload']['bootstrap']
        b.update(operation_slots=256, partition_slots=1, queue_size=4096,
                 tls=dict(cert_file='private', key_file='private', ca_file='private'))
        p._decode_command(wire(c))
        nested = 0
        for _ in range(33):
            nested = [nested]
        b['storage']['options']['nested'] = nested
        self.reject(lambda: p._decode_command(wire(c)), 'invalid_command')

    def test_results_complete_strict_and_unique_without_custom_execution(self):
        for action, result in [('start', 'resource'), ('stop', None), ('inventory', [record()])]:
            encoded = p._encode_result(p._decode_command(wire(command(action))), result)
            self.assertEqual(json.loads(encoded), dict(version=1, invocation_id=I, result=result))
        inventory = p._decode_command(wire(command('inventory')))
        bad = [[record(), record()], [record(), record('other')],
               [record(), dict(record('other'), request_id=S)],
               [record(), dict(record('other'), node_id=S)], [dict(record(), cluster_id=S)],
               [dict(record(), state='ready')], [dict(record(), extra=1)],
               [dict(record(), resource_id='x\x85')], (record(),)]
        for result in bad:
            self.reject(lambda: p._encode_result(inventory, result), 'invalid_callback_result')
        p._encode_result(inventory, [record('old', 'terminated'), record('new')])
        class Hostile(list):
            def __iter__(self):
                raise AssertionError('custom iterator executed')
            def __await__(self):
                raise AssertionError('custom await executed')
        self.reject(lambda: p._encode_result(inventory, Hostile()), 'invalid_callback_result')
        class HostileKey:
            def __hash__(self):
                return hash('state')
            def __eq__(self, other):
                raise AssertionError('custom key equality executed')
        hostile_record = record()
        del hostile_record['state']
        hostile_record[HostileKey()] = 'running'
        self.reject(lambda: p._encode_result(inventory, [hostile_record]), 'invalid_callback_result')
        async def coroutine():
            raise AssertionError('coroutine executed')
        result = coroutine()
        self.reject(lambda: p._encode_result(inventory, result), 'invalid_callback_result')
        self.assertIsNone(result.cr_frame)
        start = p._decode_command(wire(command()))
        for result in ('', 'é' * 129, 'x\n', 1, 'x' * (2*1024*1024)):
            self.reject(lambda: p._encode_result(start, result), 'invalid_callback_result')
        self.reject(lambda: p._encode_result(p._decode_command(wire(command('stop'))), False),
                    'invalid_callback_result')
        self.reject(lambda: p._encode_result(inventory, [record()] * 4097), 'limit_exceeded')
        large = [dict(resource_id=str(n)+'x'*245, cluster_id=C,
                      node_id=f'00000000-0000-4000-8000-{n:012d}',
                      request_id=f'00000001-0000-4000-8000-{n:012d}', state='running') for n in range(4096)]
        self.reject(lambda: p._encode_result(inventory, large), 'limit_exceeded')

    def test_authorization_binds_original_bytes_and_bearer(self):
        raw = b'\n ' + wire(command()) + b' '
        with Authority() as server:
            self.assertIsNone(p._authorize(server.url, TOKEN, raw, invocation_id=I))
        headers, sent = server.received
        self.assertEqual(sent, raw)
        self.assertIn(b'Authorization: Bearer ' + TOKEN.encode(), headers)
        self.assertIn(b'Content-Type: application/json', headers)
        self.assertIn(b'Content-Length: ' + str(len(raw)).encode(), headers)
        self.assertTrue(headers.startswith(b'POST /private/v1/authorize HTTP/1.1\r\n'))
        self.assertEqual(server.count, 1)

    def test_authority_denials_and_bad_framing_never_retry(self):
        bodies = [dict(version=1, invocation_id=S, command_sha256='0'*64, authorized=True),
                  dict(version=1, invocation_id=I, command_sha256='0'*64, authorized=True),
                  dict(version=1, invocation_id=I, command_sha256='0'*64, authorized=1)]
        replies = []
        for body in bodies:
            b = wire(body)
            replies.append(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '+str(len(b)).encode()+b'\r\n\r\n'+b)
        good = dict(version=1, invocation_id=I,
                    command_sha256=hashlib.sha256(wire(command())).hexdigest(), authorized=True)
        for b in (wire(good)[:-1]+b', "authorized":true}', wire(dict(good, private='provider-secret'))):
            replies.append(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '
                           + str(len(b)).encode() + b'\r\n\r\n' + b)
        for status, code in [(400,'invalid_command'),(401,'unauthorized'),(409,'ownership_changed'),
                             (413,'limit_exceeded'),(503,'ownership_unavailable')]:
            for invocation in ([I, None] if status in (400, 401, 413) else [I]):
                b = wire(dict(version=1, invocation_id=invocation, error={'code':code}))
                response = f'HTTP/1.1 {status} Denied\r\nContent-Type: application/json\r\nContent-Length: {len(b)}\r\n\r\n'.encode()+b
                with self.subTest(status=status, invocation=invocation), Authority(response) as server:
                    error = self.reject(
                        lambda: p._authorize(server.url, TOKEN, wire(command()), invocation_id=I), code)
                    self.assertEqual(error.status_code, status)
                self.assertEqual(server.count, 1)
        for status, invocation, error in [
                (409, I, {'code': 'ownership_unavailable'}),
                (409, None, {'code': 'ownership_changed'}),
                (503, S, {'code': 'ownership_unavailable'}),
                (401, I, {'code': 'unauthorized', 'private': 'provider-secret'})]:
            b = wire(dict(version=1, invocation_id=invocation, error=error))
            replies.append(f'HTTP/1.1 {status} Denied\r\nContent-Type: application/json\r\nContent-Length: {len(b)}\r\n\r\n'.encode()+b)
        for extra in (b'Content-Length: 2\r\nContent-Length: 2', b'Content-Length: -1',
                      b'Content-Length: 1048577', b'Transfer-Encoding: chunked',
                      b'Content-Length: 2\r\nContent-Encoding: gzip',
                      b'Content-Length: 2\r\nX: '+b'x'*8192):
            replies.append(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n'+extra+b'\r\n\r\n{}')
        replies += [b'HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\n{}',
                    b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 30\r\n\r\n{}']
        for response in replies:
            with Authority(response) as server:
                self.reject(lambda: p._authorize(server.url, TOKEN, wire(command()), invocation_id=I),
                            'authorization_denied')
            self.assertEqual(server.count, 1)

    def test_authority_lost_and_absolute_deadline_release_owned_connection(self):
        for mode in ('lost', 'held', 'trickle'):
            with Authority(mode=mode) as server:
                begin = time.monotonic()
                self.reject(lambda: p._authorize(server.url, TOKEN, wire(command()), invocation_id=I,
                                                  deadline=begin+0.15), 'authorization_denied')
                self.assertTrue(server.entered.wait(1))
                elapsed = time.monotonic()-begin
                self.assertLess(elapsed, 1.5)  # failure watchdog only
                if mode in ('held', 'trickle'):
                    self.assertGreaterEqual(elapsed, 0.12)  # timed out, not a framing rejection
            self.assertEqual(server.count, 1)
            if mode == 'held':
                self.assertTrue(server.peer_closed.is_set())
        with Authority(mode='held') as server:
            begin = time.monotonic()
            self.reject(lambda: p._authorize(server.url, TOKEN, wire(command()), invocation_id=I),
                        'authorization_denied')
            self.assertGreaterEqual(time.monotonic()-begin, 1.8)
            self.assertLess(time.monotonic()-begin, 3.5)
        self.assertTrue(server.peer_closed.is_set())
        self.assertEqual(server.count, 1)
        for token in (TOKEN+'=', 'A'*42, 'A'*42+'B', 'private\n'):
            self.reject(lambda: p._authorize('http://127.0.0.1:1/private/v1/authorize', token,
                                             wire(command()), invocation_id=I), 'authorization_denied')
        for url in ('http://localhost:12/private/v1/authorize',
                    'http://127.0.0.1:0/private/v1/authorize',
                    'http://127.0.0.1:12/private/v1/authorize?x',
                    'http://127.0.0.1:12/private/v1/authorize/',
                    'http://x@127.0.0.1:12/private/v1/authorize'):
            self.reject(lambda: p._authorize(url, TOKEN, wire(command()), invocation_id=I), 'authorization_denied')


if __name__ == '__main__':
    unittest.main()

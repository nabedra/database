"""Bounded ingress contracts over real TCP; provider overlap is event-gated."""
import base64
import hashlib
import json
from pathlib import Path
import socket
import threading
import time
import unittest
from unittest.mock import patch

from test_callback_protocol import C, I, TOKEN, command, wire

if (Path(__file__).parents[1] / 'src/nabedra/_callback_bridge.py').exists():
    from nabedra import _callback_bridge as implementation
else:
    implementation = None

CALLBACK_TOKEN = base64.urlsafe_b64encode(b'c' * 32).decode().rstrip('=')


class Authority:
    """Independent single-owner authority, bounded sockets, no production codec."""
    def __enter__(self):
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(8)
        self.listener.settimeout(.05)
        self.url = f'http://127.0.0.1:{self.listener.getsockname()[1]}/private/v1/authorize'
        self.stop = threading.Event()
        self.gate = threading.Event()
        self.gate.set()
        self.received = threading.Event()
        self.requests = []
        self.denial = None
        self.error = None
        self.thread = threading.Thread(target=self.run)
        self.thread.start()
        return self

    def run(self):
        try:
            while not self.stop.is_set():
                try:
                    peer, _ = self.listener.accept()
                except socket.timeout:
                    continue
                with peer:
                    peer.settimeout(2)
                    data = bytearray()
                    while b'\r\n\r\n' not in data:
                        part = peer.recv(min(4096, 8192-len(data)))
                        assert part, 'authority header cap or EOF'
                        data.extend(part)
                    head, body = bytes(data).split(b'\r\n\r\n', 1)
                    assert head.startswith(b'POST /private/v1/authorize HTTP/1.1\r\n')
                    assert b'Authorization: Bearer ' + TOKEN.encode() + b'\r\n' in head + b'\r\n'
                    length = int(next(line.split(b':', 1)[1] for line in head.split(b'\r\n')[1:]
                                      if line.lower().startswith(b'content-length:')))
                    assert 0 < length <= 1024*1024
                    while len(body) < length:
                        part = peer.recv(min(65536, length-len(body)))
                        assert part, 'authority truncated command'
                        body += part
                    self.requests.append(body)
                    self.received.set()
                    assert self.gate.wait(5), 'authority gate watchdog'
                    invocation = json.loads(body)['invocation_id']
                    status = 200
                    result = dict(version=1, invocation_id=invocation,
                                  command_sha256=hashlib.sha256(body).hexdigest(), authorized=True)
                    if self.denial:
                        status, code = self.denial
                        result = dict(version=1, invocation_id=invocation, error={'code': code})
                    raw = wire(result)
                    peer.sendall(f'HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {len(raw)}\r\nConnection: close\r\n\r\n'.encode() + raw)
        except BaseException as exc:
            self.error = exc

    def __exit__(self, *args):
        self.gate.set()
        self.stop.set()
        self.thread.join(3)
        self.listener.close()
        assert not self.thread.is_alive(), 'authority owner leaked'
        if self.error:
            raise self.error


def response(peer):
    data = bytearray()
    while b'\r\n\r\n' not in data:
        part = peer.recv(4096)
        if not part:
            raise AssertionError('missing response')
        data.extend(part)
        assert len(data) <= 1024*1024+8192
    head, body = bytes(data).split(b'\r\n\r\n', 1)
    assert b'Connection: close' in head
    length = int(next(line.split(b':', 1)[1] for line in head.split(b'\r\n')
                      if line.lower().startswith(b'content-length:')))
    while len(body) < length:
        part = peer.recv(min(65536, length-len(body)))
        assert part
        body += part
    return int(head.split(b' ')[1]), json.loads(body)


class CallbackBridgeTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(implementation, 'P03c2b callback bridge implementation is missing')

    def bridge(self, authority=None, start=lambda p: 'resource', inventory=lambda: []):
        bridge = implementation._CallbackBridge((start, lambda r: None, inventory), CALLBACK_TOKEN, TOKEN)
        self.addCleanup(bridge.close)
        if authority:
            bridge.activate(C, authority.url)
        return bridge

    def connect(self, bridge):
        peer = socket.create_connection(('127.0.0.1', int(bridge.url.split(':')[2].split('/')[0])), 3)
        self.addCleanup(peer.close)
        return peer

    def head(self, bridge, length, token=CALLBACK_TOKEN, extra=b''):
        port = bridge.url.split(':')[2].split('/')[0]
        return (f'POST /private/v1/callback HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {length}\r\n'.encode() + extra + b'\r\n')

    def request(self, bridge, raw=None):
        raw = wire(command()) if raw is None else raw
        with self.connect(bridge) as peer:
            peer.sendall(self.head(bridge, len(raw)) + raw)
            return response(peer)

    def error(self, result, status, code, invocation=None):
        self.assertEqual(result[0], status)
        self.assertEqual(result[1], dict(version=1, invocation_id=invocation,
                                        error=dict(code=code, outcome='not_invoked')))

    def test_actual_url_inactive_and_authentication_before_held_body(self):
        entries = []
        with Authority() as authority:
            bridge = self.bridge(start=lambda p: entries.append(p) or 'resource')
            self.assertRegex(bridge.url, r'^http://127\.0\.0\.1:[1-9][0-9]*/private/v1/callback$')
            self.error(self.request(bridge), 503, 'bridge_not_active', I)
            bridge.activate(C, authority.url)
            for extra in (b'', b'Expect: 100-continue\r\n'):
                with self.connect(bridge) as peer:
                    peer.sendall(self.head(bridge, 999, token='wrong', extra=extra))
                    self.error(response(peer), 401, 'unauthorized')
            self.assertEqual(entries, [])
            self.assertEqual(authority.requests, [])
            self.assertNotIn(TOKEN, repr(bridge))
            self.assertNotIn(CALLBACK_TOKEN, repr(bridge))

    def test_strict_framing_bounds_and_single_request_without_eof(self):
        bridge = self.bridge()
        good = self.head(bridge, 2)
        cases = [(good.replace(b'POST ', b'GET '), 400),
                 (good.replace(b'/private/v1/callback ', b'/private/v1/callback?x '), 400),
                 (good.replace(b'127.0.0.1:', b'localhost:'), 400),
                 (good.replace(b'Content-Length: 2', b'Content-Length: +2'), 400),
                 (good.replace(b'Content-Length: 2', b'Content-Length: 02'), 400),
                 (good.replace(b'Content-Length: 2', b'Content-Length: 1048577'), 413),
                 (good.replace(b'Content-Length: 2\r\n', b''), 400),
                 (good.replace(b'application/json', b'text/plain'), 400)]
        for extra in (b'Content-Length: 2\r\n', b'Authorization: x\r\n',
                      b'Transfer-Encoding: chunked\r\n', b'Content-Encoding: identity\r\n',
                      b' folded\r\n', b'Expect: other\r\n', b'Upgrade: h2c\r\n',
                      b'Connection: upgrade\r\n'):
            cases.append((self.head(bridge, 2, extra=extra), 400))
        cases.append((self.head(bridge, 2, extra=b'X: '+b'x'*8192+b'\r\n'), 413))
        for header, status in cases:
            with self.subTest(header=header[:40]), self.connect(bridge) as peer:
                peer.sendall(header)
                self.error(response(peer), status, 'limit_exceeded' if status == 413 else 'invalid_command')
        for raw in (b'{}', b'null', b'{"x":1,"x":2}', b'\xff'):
            self.error(self.request(bridge, raw), 400, 'invalid_command')
        with self.connect(bridge) as peer:
            raw = wire(command())
            peer.sendall(self.head(bridge, len(raw)) + raw + self.head(bridge, len(raw)) + raw)
            self.error(response(peer), 503, 'bridge_not_active', I)
            try:
                self.assertEqual(peer.recv(1), b'')
            except ConnectionResetError:
                pass

    def test_original_bytes_authority_gate_and_ownership_denial(self):
        entries = []
        with Authority() as authority:
            bridge = self.bridge(authority, lambda p: entries.append(p) or 'resource')
            raw = b'\n ' + wire(command()) + b' \n'
            authority.gate.clear()
            with self.connect(bridge) as peer:
                peer.sendall(self.head(bridge, len(raw)) + raw)
                self.assertTrue(authority.received.wait(1))
                self.assertEqual(entries, [])
                authority.denial = (409, 'ownership_changed')
                authority.gate.set()
                self.error(response(peer), 409, 'ownership_changed', I)
            self.assertEqual(entries, [])
            authority.denial = None
            self.assertEqual(self.request(bridge, raw), (200, dict(version=1, invocation_id=I, result='resource')))
            self.assertEqual(authority.requests, [raw, raw])
            self.assertEqual(entries, [command()['payload']])

    def test_three_entered_actions_inventory_disconnect_and_live_close(self):
        release = threading.Event()
        entered = [threading.Event() for _ in range(3)]
        inventory = threading.Event()
        def start(payload):
            entered[int(payload['request_id'][-1])-1].set()
            assert release.wait(8), 'provider watchdog'
            return 'resource'
        with Authority() as authority:
            bridge = self.bridge(authority, start, lambda: inventory.set() or [])
            peers = []
            try:
                for number in range(1, 4):
                    value = command()
                    request = f'00000001-0000-4000-8000-{number:012d}'
                    value['intent_id'] = value['target']['request_id'] = request
                    value['payload']['request_id'] = value['payload']['bootstrap']['request_id'] = request
                    peer = self.connect(bridge)
                    raw = wire(value)
                    peer.sendall(self.head(bridge, len(raw)) + raw)
                    peers.append(peer)
                    self.assertTrue(entered[number-1].wait(1))
                peers[0].close()
                duplicate = command()
                request = '00000001-0000-4000-8000-000000000001'
                duplicate['intent_id'] = duplicate['target']['request_id'] = request
                duplicate['payload']['request_id'] = duplicate['payload']['bootstrap']['request_id'] = request
                result = self.request(bridge, wire(duplicate))
                self.assertEqual((result[0], result[1]['error']), (409, dict(code='callback_in_progress', outcome='unknown')))
                duplicate['payload']['bootstrap']['storage']['options']['x'] = 1
                self.error(self.request(bridge, wire(duplicate)), 409, 'identity_conflict', I)
                self.error(self.request(bridge), 429, 'callback_capacity', I)
                self.assertEqual(self.request(bridge, wire(command('inventory')))[0], 200)
                self.assertTrue(inventory.is_set())
                self.assertEqual(len(bridge._pool._threads), 4)
                self.assertEqual(len(bridge._threads), 9)
                begin = time.monotonic()
                bridge.stop_admission()
                self.assertLess(time.monotonic()-begin, .5)
                begin = time.monotonic()
                bridge.close()
                self.assertLess(time.monotonic()-begin, 3.3)
                self.assertTrue(bridge.alive)
                self.assertTrue(all(not t.is_alive() for t in bridge._threads))
            finally:
                release.set()
                bridge.close()
            self.assertFalse(bridge.alive)

    def test_fixed_ingress_queue_saturation_and_terminal_socket_cleanup(self):
        bridge = self.bridge()
        release = threading.Event()
        condition = threading.Condition()
        held = []
        original = implementation._CallbackBridge._handle
        def handle(owner, peer, deadline):
            with condition:
                held.append(peer)
                condition.notify_all()
            assert release.wait(4), 'handler gate watchdog'
            original(owner, peer, deadline)
        peers = []
        try:
            with patch.object(implementation._CallbackBridge, '_handle', handle):
                for _ in range(8):
                    peers.append(self.connect(bridge))
                with condition:
                    self.assertTrue(condition.wait_for(lambda: len(held) == 8, 2))
                # FIFO accept proves all preceding eight sockets reached the queue.
                for _ in range(8):
                    peers.append(self.connect(bridge))
                with self.connect(bridge) as excess:
                    self.assertEqual(excess.recv(1), b'')
                self.assertEqual(len(bridge._pending), 8)
                self.assertEqual(len(bridge._threads), 9)
                self.assertTrue(all(t.daemon for t in bridge._threads))
                bridge.stop_admission()
                for peer in peers:
                    self.assertEqual(peer.recv(1), b'')
                self.assertEqual(len(bridge._pending), 0)
                release.set()
                bridge.close()
                self.assertFalse(bridge.alive)
                with self.assertRaises(RuntimeError):
                    bridge.activate(C, 'unused')
        finally:
            release.set()
            bridge.close()

    def test_absolute_header_body_budgets_include_queued_time(self):
        with patch.object(implementation, '_IO_TIMEOUT', .15):
            bridge = self.bridge()
            for prefix in (b'POST /private', self.head(bridge, 100)+b'{'):
                done = threading.Event()
                original = implementation._CallbackBridge._handle
                def handle(owner, peer, deadline):
                    try:
                        original(owner, peer, deadline)
                    finally:
                        done.set()
                with patch.object(implementation._CallbackBridge, '_handle', handle):
                    with self.connect(bridge) as peer:
                        begin = time.monotonic()
                        peer.sendall(prefix)
                        # Pace bytes until the handler finishes, rather than ending
                        # the trickle and allowing a per-byte timeout to pass.
                        while not done.wait(.025):
                            self.assertLess(time.monotonic()-begin, .4)
                            try:
                                peer.sendall(b' ')
                            except OSError:
                                break
                        self.assertTrue(done.wait(.1))
                        self.assertLess(time.monotonic()-begin, .3)
                        self.error(response(peer), 400, 'invalid_command')
            release = threading.Event()
            condition = threading.Condition()
            held = []
            def hold(owner, peer, deadline):
                with condition:
                    held.append(peer)
                    condition.notify_all()
                assert release.wait(3)
                original(owner, peer, deadline)
            try:
                with patch.object(implementation._CallbackBridge, '_handle', hold):
                    blockers = [self.connect(bridge) for _ in range(8)]
                    with condition:
                        self.assertTrue(condition.wait_for(lambda: len(held) == 8, 1))
                    with self.connect(bridge) as queued:
                        raw = wire(command())
                        queued.sendall(self.head(bridge, len(raw)) + raw)
                        # Wait out the contractual assembly budget while queued.
                        threading.Event().wait(.2)
                        release.set()
                        self.error(response(queued), 400, 'invalid_command')
                    for peer in blockers:
                        peer.close()
            finally:
                release.set()

    def test_blocked_response_write_has_fresh_absolute_budget(self):
        writing, finished, timed_out = threading.Event(), threading.Event(), threading.Event()
        elapsed = []
        original = implementation._CallbackBridge._write
        def write(owner, peer, status, raw):
            peer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            begin = time.monotonic()
            writing.set()
            try:
                self.assertEqual(status, 200)
                self.assertGreater(len(raw), 700000)
                original(owner, peer, status, raw)
            except TimeoutError:
                timed_out.set()
                raise
            finally:
                elapsed.append(time.monotonic()-begin)
                finished.set()
        records = [dict(resource_id='resource-'+str(n)+'x'*180, cluster_id=C,
                        node_id=f'00000001-0000-4000-8000-{n:012d}',
                        request_id=f'00000002-0000-4000-8000-{n:012d}', state='running')
                   for n in range(2000)]
        with Authority() as authority, patch.object(implementation, '_IO_TIMEOUT', .15):
            authority.gate.clear()
            bridge = self.bridge(authority, inventory=lambda: records)
            with patch.object(implementation._CallbackBridge, '_write', write):
                with self.connect(bridge) as peer:
                    peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
                    raw = wire(command('inventory'))
                    peer.sendall(self.head(bridge, len(raw)) + raw)
                    self.assertTrue(authority.received.wait(1))
                    # Result arrives after the assembly budget; writes get a new one.
                    threading.Event().wait(.2)
                    authority.gate.set()
                    self.assertTrue(writing.wait(2))
                    self.assertTrue(finished.wait(.6))
                    self.assertTrue(timed_out.is_set())
                    self.assertLess(elapsed[0], .4)
                    self.assertTrue(bridge.alive)

    def test_partial_startup_failure_reaps_idle_owners_and_sanitizes(self):
        real_start = threading.Thread.start
        for count in (0, 2, 4, 7, 12):
            with self.subTest(started_before_failure=count):
                started = []
                def start(thread):
                    if len(started) == count:
                        raise OSError('secret-token-and-os-detail')
                    real_start(thread)
                    started.append(thread)
                owner = implementation._CallbackBridge.__new__(implementation._CallbackBridge)
                with patch.object(threading.Thread, 'start', start):
                    with self.assertRaisesRegex(RuntimeError, '^Callback bridge startup failed$') as caught:
                        owner.__init__((lambda p: 'r', lambda r: None, lambda: []), CALLBACK_TOKEN, TOKEN)
                self.assertIsNone(caught.exception.__cause__)
                self.assertIsNone(caught.exception.__context__)
                self.assertTrue(all(not t.is_alive() for t in started))
                self.assertFalse(owner.alive)
                self.assertEqual(owner._listener.fileno(), -1)


if __name__ == '__main__':
    unittest.main()

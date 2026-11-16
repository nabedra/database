"""Causal tests of finite callback ownership, using real TCP authorization."""
import hashlib
import json
import socket
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from nabedra import _callback_protocol as protocol
from test_callback_protocol import C, I, O, R, S, TOKEN, command, wire

if (Path(__file__).parents[1] / 'src/nabedra/_callback_pool.py').exists():
    from nabedra import _callback_pool as implementation
else:
    implementation = None


def parsed(action='start', number=None, **changes):
    value = command(action)
    if number is not None:
        request = f'00000001-0000-4000-8000-{number:012d}'
        value['intent_id'] = request
        value['target']['request_id'] = request
        value['payload']['request_id'] = request
        value['payload']['bootstrap']['request_id'] = request
    value.update(changes)
    return protocol._decode_command(wire(value))


class Authority:
    """One bounded server thread, with a gate before the authority decision."""
    def __init__(self):
        self.socket = socket.socket()
        self.socket.bind(('127.0.0.1', 0))
        self.socket.listen(8)
        self.socket.settimeout(.1)
        self.url = f'http://127.0.0.1:{self.socket.getsockname()[1]}/private/v1/authorize'
        self.gate = threading.Event()
        self.gate.set()
        self.received = threading.Event()
        self.stopping = threading.Event()
        self.requests = []
        self.denial = None
        self.malformed = False
        self.error = None
        self.thread = threading.Thread(target=self.run)

    def __enter__(self):
        self.thread.start()
        return self

    def run(self):
        try:
            while not self.stopping.is_set():
                try:
                    peer, _ = self.socket.accept()
                except socket.timeout:
                    continue
                with peer:
                    peer.settimeout(2)
                    data = b''
                    while b'\r\n\r\n' not in data:
                        part = peer.recv(4096)
                        if not part:
                            raise AssertionError('truncated request')
                        data += part
                        assert len(data) <= 1024*1024 + 8192
                    head, body = data.split(b'\r\n\r\n', 1)
                    length = int(next(h.split(b':', 1)[1] for h in head.split(b'\r\n')
                                      if h.lower().startswith(b'content-length:')))
                    while len(body) < length:
                        part = peer.recv(min(65536, length-len(body)))
                        if not part:
                            raise AssertionError('truncated body')
                        body += part
                    assert b'Authorization: Bearer ' + TOKEN.encode() in head
                    value = json.loads(body)
                    self.requests.append(body)
                    self.received.set()
                    assert self.gate.wait(5), 'authority gate watchdog'
                    status = 200
                    response = dict(version=1, invocation_id=value['invocation_id'],
                                    command_sha256=hashlib.sha256(body).hexdigest(), authorized=True)
                    if self.denial:
                        status, code = self.denial
                        response = dict(version=1, invocation_id=value['invocation_id'], error={'code': code})
                    if self.malformed:
                        response = {'private': 'provider-secret'}
                    raw = wire(response)
                    try:
                        peer.sendall(f'HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {len(raw)}\r\n\r\n'.encode() + raw)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
        except BaseException as exc:
            self.error = exc

    def __exit__(self, *args):
        self.gate.set()
        self.stopping.set()
        self.thread.join(3)
        self.socket.close()
        assert not self.thread.is_alive(), 'authority thread leaked'
        if self.error:
            raise self.error


class CallbackPoolTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(implementation, 'P03c2a callback pool implementation is missing')

    def pool(self, authority, start=lambda payload: 'resource', stop=lambda resource: None,
             inventory=lambda: [], timeout=.3):
        pool = implementation._CallbackPool((start, stop, inventory), TOKEN,
                                             _observation_timeout=timeout)
        pool.activate(C, authority.url)
        return pool

    def error(self, pool, ticket, status, code, outcome='not_invoked'):
        response_status, body = pool.wait(ticket)
        self.assertEqual(response_status, status)
        value = json.loads(body)
        self.assertEqual(value['error'], dict(code=code, outcome=outcome))
        self.assertNotIn('provider-secret', body.decode())
        return value

    def test_admission_fresh_authorization_and_denial_before_entry(self):
        entries = []
        with Authority() as authority:
            pool = implementation._CallbackPool((lambda p: entries.append(p) or 'resource',
                                                  lambda r: None, lambda: []), TOKEN)
            try:
                self.error(pool, pool.submit(parsed()), 503, 'bridge_not_active')
                pool.activate(C, authority.url)
                wrong = command('inventory'); wrong['cluster_id'] = S
                self.error(pool, pool.submit(protocol._decode_command(wire(wrong))), 400, 'invalid_command')
                authority.gate.clear()
                call = pool.submit(parsed())
                self.assertTrue(authority.received.wait(1))
                self.assertEqual(entries, [])
                authority.denial = (409, 'ownership_changed')
                authority.gate.set()
                self.error(pool, call, 409, 'ownership_changed')
                self.assertEqual(entries, [])
                authority.denial = None
                self.assertEqual(pool.wait(pool.submit(parsed()))[0], 200)
                self.assertEqual(len(entries), 1)
                self.assertEqual(len(authority.requests), 2)
                authority.malformed = True
                self.error(pool, pool.submit(parsed()), 503, 'ownership_unavailable')
                self.assertEqual(len(entries), 1)
            finally:
                authority.gate.set()
                pool.close()
            self.assertFalse(pool.alive)

    def test_three_hung_actions_reserve_inventory_and_keep_original_threads(self):
        release = threading.Event()
        entered = [threading.Event() for _ in range(3)]
        inventory_entered = threading.Event()
        def start(payload):
            index = int(payload['request_id'][-12:])-1
            entered[index].set()
            if not release.wait(5):
                raise AssertionError('provider watchdog')
            return 'resource'
        def inventory():
            inventory_entered.set()
            return []
        with Authority() as authority:
            pool = self.pool(authority, start, inventory=inventory, timeout=.08)
            originals = tuple(pool._threads)
            try:
                calls = [pool.submit(parsed(number=n+1)) for n in range(3)]
                for event in entered:
                    self.assertTrue(event.wait(1))
                self.error(pool, pool.submit(parsed(number=4)), 429, 'callback_capacity')
                self.assertEqual(pool.wait(pool.submit(parsed('inventory')))[0], 200)
                self.assertTrue(inventory_entered.is_set())
                for call in calls:
                    for _ in range(2):
                        self.error(pool, call, 504, 'callback_timeout', 'unknown')
                self.assertEqual(tuple(pool._threads), originals)
                self.assertEqual(len(originals), 4)
                self.assertTrue(all(t.daemon and t.is_alive() for t in originals))
                self.error(pool, pool.submit(parsed(number=4)), 429, 'callback_capacity')
                begin = time.monotonic()
                pool.close()
                self.assertLess(time.monotonic()-begin, 2.3)
                self.assertTrue(pool.alive)
            finally:
                release.set()
                pool.close()
            self.assertFalse(pool.alive)

    def test_active_identity_mutation_isolation_and_fresh_provider_replay(self):
        release, entered = threading.Event(), threading.Event()
        registry, entries = {}, []
        def start(payload):
            entries.append(payload['request_id'])
            resource = registry.setdefault(payload['request_id'], 'durable-resource')
            payload['bootstrap']['storage']['options'].clear()
            payload['cluster_id'] = S
            entered.set()
            assert release.wait(5), 'provider watchdog'
            return resource
        with Authority() as authority:
            pool = self.pool(authority, start, timeout=2)
            first = parsed()
            try:
                call = pool.submit(first)
                self.assertTrue(entered.wait(1))
                retry = command(); retry.update(invocation_id=S, owner_id=I, epoch=2)
                retry['payload'] = dict(reversed(list(retry['payload'].items())))
                duplicate = pool.submit(protocol._decode_command(wire(retry)))
                echo = self.error(pool, duplicate, 409, 'callback_in_progress', 'unknown')
                self.assertEqual(echo['invocation_id'], S)
                retry['payload']['bootstrap']['storage']['options']['secret'] = True
                self.error(pool, pool.submit(protocol._decode_command(wire(retry))), 409, 'identity_conflict')
                self.assertEqual(first.value['payload']['cluster_id'], C)
                self.assertEqual(first.value['payload']['bootstrap']['storage']['options']['secret'], 'private')
                release.set()
                self.assertEqual(json.loads(pool.wait(call)[1])['result'], 'durable-resource')
                self.assertEqual(json.loads(pool.wait(pool.submit(parsed()))[1])['result'], 'durable-resource')
                self.assertEqual(entries, [R, R])
                self.assertEqual(len(authority.requests), 2)
            finally:
                release.set()
                pool.close()

    def test_caller_mutation_cannot_change_authorized_arguments(self):
        for action in ('start', 'stop'):
            with self.subTest(action=action), Authority() as authority:
                entries = []
                pool = self.pool(authority, start=lambda p: entries.append(p) or 'resource',
                                 stop=lambda r: entries.append(r), timeout=2)
                original = parsed(action)
                authorized = json.loads(original.original_bytes)
                if action == 'start':
                    original.value['payload']['bootstrap']['storage']['options']['secret'] = 'changed'
                    expected = authorized['payload']
                else:
                    original.value['payload']['resource_id'] = 'different-resource'
                    expected = authorized['payload']['resource_id']
                authority.gate.clear()
                try:
                    call = pool.submit(original)
                    self.assertTrue(authority.received.wait(1))
                    self.assertEqual(entries, [])
                    original.value.clear()
                    authority.gate.set()
                    self.assertEqual(pool.wait(call)[0], 200)
                    self.assertEqual(authority.requests, [original.original_bytes])
                    self.assertEqual(entries, [expected])
                finally:
                    authority.gate.set()
                    pool.close()
                self.assertFalse(pool.alive)

    def test_json_identity_preserves_types_and_ignores_dictionary_order(self):
        release, entered = threading.Event(), threading.Event()
        def start(payload):
            entered.set()
            assert release.wait(5)
            return 'resource'
        with Authority() as authority:
            pool = self.pool(authority, start)
            value = command(); value['payload']['bootstrap']['storage']['options'] = {'x': True, 'y': None}
            try:
                call = pool.submit(protocol._decode_command(wire(value)))
                self.assertTrue(entered.wait(1))
                value['payload']['bootstrap']['storage']['options'] = {'y': None, 'x': True}
                self.error(pool, pool.submit(protocol._decode_command(wire(value))), 409, 'callback_in_progress', 'unknown')
                value['payload']['bootstrap']['storage']['options']['x'] = 1
                self.error(pool, pool.submit(protocol._decode_command(wire(value))), 409, 'identity_conflict')
                value['payload']['bootstrap']['storage']['options']['x'] = 1.0
                self.error(pool, pool.submit(protocol._decode_command(wire(value))), 409, 'identity_conflict')
            finally:
                release.set()
                pool.close()

    def test_authorization_deadline_and_stop_prevent_late_entry(self):
        for stopping in (False, True):
            with self.subTest(stopping=stopping), Authority() as authority:
                entries = []
                authority.gate.clear()
                pool = self.pool(authority, lambda p: entries.append(p) or 'resource', timeout=.08)
                try:
                    call = pool.submit(parsed())
                    self.assertTrue(authority.received.wait(1))
                    if stopping:
                        pool.stop_admission()
                        self.error(pool, call, 503, 'bridge_not_active')
                    else:
                        self.error(pool, call, 504, 'callback_timeout')
                    authority.gate.set()
                finally:
                    authority.gate.set()
                    pool.close()
                self.assertEqual(entries, [])
                self.assertFalse(pool.alive)

    def test_entered_late_effect_and_hung_inventory_are_not_cancelled(self):
        release, entered, effect = threading.Event(), threading.Event(), threading.Event()
        inventory_entered = threading.Event()
        owner_epoch, effect_epochs = [1], []
        def start(payload):
            entered.set()
            assert release.wait(5)
            effect_epochs.append(owner_epoch[0])
            effect.set()
            return 'resource'
        def inventory():
            inventory_entered.set()
            assert release.wait(5)
            return []
        with Authority() as authority:
            pool = self.pool(authority, start, inventory=inventory, timeout=.08)
            try:
                call = pool.submit(parsed())
                self.assertTrue(entered.wait(1))
                authority.denial = (409, 'ownership_changed')
                owner_epoch[0] = 2
                self.error(pool, call, 504, 'callback_timeout', 'unknown')
                authority.denial = None
                inv = pool.submit(parsed('inventory'))
                self.assertTrue(inventory_entered.wait(1))
                self.error(pool, pool.submit(parsed('inventory')), 409, 'callback_in_progress', 'unknown')
                self.error(pool, inv, 504, 'callback_timeout')
                self.error(pool, pool.submit(parsed('inventory', invocation_id=S)), 429, 'callback_capacity')
                pool.stop_admission()
                self.error(pool, pool.submit(parsed()), 503, 'bridge_not_active')
                release.set()
                pool.close()
                self.assertTrue(effect.is_set())
                self.assertEqual(effect_epochs, [2])
                self.error(pool, call, 504, 'callback_timeout', 'unknown')
            finally:
                release.set()
                pool.close()
            self.assertFalse(pool.alive)

    def test_sanitized_failures_and_exact_callback_signatures(self):
        async def awaitable():
            raise AssertionError('must not execute')
        def failing(payload):
            raise SystemExit('provider-secret')
        with Authority() as authority:
            for start, code in ((failing, 'callback_failed'), (lambda p: awaitable(), 'invalid_callback_result'),
                                (lambda p: 'provider-secret\n', 'invalid_callback_result')):
                pool = self.pool(authority, start)
                try:
                    self.error(pool, pool.submit(parsed()), 502, code, 'unknown')
                    self.assertTrue(all(t.is_alive() for t in pool._threads))
                finally:
                    pool.close()
            stopped = []
            pool = self.pool(authority, stop=lambda resource: stopped.append(resource), inventory=lambda: [None]*4097)
            try:
                self.assertEqual(pool.wait(pool.submit(parsed('stop')))[0], 200)
                self.assertEqual(stopped, ['resource'])
                self.error(pool, pool.submit(parsed('inventory')), 413, 'limit_exceeded')
            finally:
                pool.close()

    def test_partial_thread_start_failure_joins_original_idle_workers(self):
        real_start = threading.Thread.start
        started = []
        def start(thread):
            if len(started) == 2:
                raise RuntimeError('provider-secret')
            real_start(thread)
            started.append(thread)
        with patch.object(threading.Thread, 'start', start):
            with self.assertRaisesRegex(RuntimeError, '^Callback pool startup failed$') as caught:
                implementation._CallbackPool((lambda p: 'r', lambda r: None, lambda: []), TOKEN)
        self.assertIsNone(caught.exception.__cause__)
        self.assertEqual(len(started), 2)
        self.assertTrue(all(not t.is_alive() for t in started))


if __name__ == '__main__':
    unittest.main()

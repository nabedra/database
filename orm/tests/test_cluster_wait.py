"""Wait contract checks; real TCP peers and separate deterministic clock cases."""
import inspect
import json
import socket
import threading
import time
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from meshdb import errors
from meshdb.cluster import Cluster
from test_cluster import policy, status


def ready(intent='completed', revision=1):
    data = status()
    data.update(policy(1), revision=revision)
    data['nodes'][0]['state'] = 'ready'
    data['counts'].update(ready=1, starting=0)
    data['intents'][0]['state'] = intent
    return data


@contextmanager
def peer(respond, hostname='127.0.0.1'):
    seen = []
    release = threading.Event()
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            seen.append(request)
            data = respond(request, len(seen), release)
            if data is None:
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass  # The deadline may already have closed the peer.
                self.close_connection = True
                return
            raw = json.dumps(dict(request_id=request['request_id'], data=data)).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield Cluster(f'http://{hostname}:{server.server_port}', 'token'), seen
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join()


class ClusterWaitTests(unittest.TestCase):
    def test_final_public_signature_and_export(self):
        self.assertEqual(str(inspect.signature(Cluster.scale)),
                         "(self, nodes: 'int', *, wait: 'bool' = False, timeout: 'float' = 300.0) -> 'dict'")
        from meshdb import Cluster as PublicCluster
        self.assertIs(PublicCluster, Cluster)
        self.assertEqual([n for n in vars(Cluster) if not n.startswith('_')],
                         ['scale', 'autoscale', 'status'])

    def test_tcp_pending_intent_requires_another_poll_and_returns_acceptance(self):
        def respond(request, count, release):
            if count == 1:
                return policy(1)
            return ready('pending' if count == 2 else 'completed')
        with peer(respond) as (client, seen):
            self.assertEqual(seen, [])
            self.assertEqual(client.scale(1, wait=True, timeout=2), policy(1))
            self.assertEqual([r['operation'] for r in seen],
                             ['cluster.scale', 'cluster.status', 'cluster.status'])

    def test_tcp_stalled_poll_expires_after_one_accepted_mutation(self):
        polled = threading.Event()
        def respond(request, count, release):
            if count == 1:
                return policy(1)
            polled.set()
            release.wait(5)
            return None
        with peer(respond) as (client, seen):
            start = time.monotonic()
            with self.assertRaises(errors.TransportError) as caught:
                client.scale(1, wait=True, timeout=0.3)
            self.assertTrue(polled.is_set())
            self.assertLess(time.monotonic() - start, 2)
            self.assertEqual([r['operation'] for r in seen], ['cluster.scale', 'cluster.status'])
            error = caught.exception
            self.assertEqual((error.code, error.outcome, error.request_id, error.status_code),
                             ('wait_timeout', None, seen[0]['request_id'], None))

    def test_large_finite_wait_bounds_native_waits_and_rechecks_deadline(self):
        native_wait = threading.Event.wait
        for limit, hostname in ((threading.TIMEOUT_MAX, '127.0.0.1'),
                                (threading.TIMEOUT_MAX, 'localhost'), (0.01, '127.0.0.1')):
            with self.subTest(platform_wait_limit=limit, hostname=hostname):
                budgets = []
                failures = []
                waiting = threading.Event()
                required = 2 if limit == 0.01 else 1

                def observe_wait(event, timeout=None):
                    # Observe real native waits without replacing the clock or timer.
                    if threading.current_thread().name == 'meshdb-control-deadline':
                        budgets.append(timeout)
                        if len(budgets) >= required:
                            waiting.set()
                    return native_wait(event, timeout)

                def respond(request, count, release):
                    if not waiting.wait(2):
                        return None
                    return policy(1) if count == 1 else ready()

                with patch.object(threading, 'TIMEOUT_MAX', limit), \
                        patch.object(threading.Event, 'wait', observe_wait), \
                        patch.object(threading, 'excepthook',
                                     lambda args: failures.append(args.exc_type)):
                    with peer(respond, hostname) as (client, seen):
                        self.assertEqual(client.scale(1, wait=True, timeout=1e20), policy(1))
                        self.assertEqual([r['operation'] for r in seen],
                                         ['cluster.scale', 'cluster.status'])
                self.assertGreaterEqual(len(budgets), required)
                self.assertTrue(all(0 < value <= limit for value in budgets),
                                'deadline exceeded the native wait limit')
                self.assertEqual(failures, [], 'deadline thread raised an exception')
                self.assertFalse(any(t.name == 'meshdb-control-deadline'
                                     for t in threading.enumerate()))

    def test_invalid_inputs_no_io_and_pre_ack_failures_keep_classification(self):
        with peer(lambda *args: None) as (client, seen):
            for args in [dict(wait=v) for v in (0, 1, None, 'yes')]+[
                    dict(timeout=v) for v in (True, False, 0, -1, float('inf'),
                                               float('nan'), '1', None, 10**1000)]+[
                    dict(nodes=v, wait=True) for v in (True, -1, 1025, 1.0)]:
                with self.subTest(args=args), self.assertRaises(errors.ValidationError):
                    client.scale(**dict(dict(nodes=1), **args))
            self.assertEqual(seen, [])
            with self.assertRaises(errors.UnknownOutcomeError) as caught:
                client.scale(1, wait=True, timeout=1)
            self.assertEqual(caught.exception.request_id, seen[0]['request_id'])
            self.assertEqual(caught.exception.outcome, 'unknown')
            self.assertEqual(len(seen), 1)
        client = Cluster('http://127.0.0.1:1', 'token')
        with patch.object(client._transport, 'request', side_effect=errors.TransportError('pre-send')):
            with self.assertRaises(errors.TransportError) as caught:
                client.scale(1, wait=True, timeout=1)
            self.assertNotIsInstance(caught.exception, errors.UnknownOutcomeError)

    def test_scripted_progression_supersession_and_whole_call_budget(self):
        # Patch only cluster's private clock functions, never the shared time module.
        class Clock:
            now = 100.0
            sleeps = []
            def monotonic(self):
                return self.now
            def sleep(self, seconds):
                self.sleeps.append(seconds)
                self.now += seconds
        for scenario in ('progress', 'superseded', 'submission_budget', 'parse_budget',
                         'poll_budget', 'sleep_budget', 'read_failure', 'invalid_ack', 'no_wait'):
            with self.subTest(scenario=scenario):
                clock = Clock()
                clock.sleeps = []
                calls = []
                client = Cluster('http://127.0.0.1:1', 'token')
                failure = errors.ProtocolError('bad status')
                def request(operation, payload, *, request_id=None, deadline=None):
                    calls.append((operation, request_id, deadline))
                    if operation == 'cluster.scale':
                        clock.now += (1.0 if scenario == 'submission_budget' else
                                      0.95 if scenario == 'sleep_budget' else 0.2)
                        return request_id, None if scenario == 'invalid_ack' else policy(1)
                    if scenario == 'read_failure':
                        raise failure
                    if scenario == 'poll_budget':
                        clock.now = 101.0
                        raise errors.TransportError('expired read')
                    states = ['pending', 'in_progress', 'reconciling', 'failed']
                    return '00000000-0000-0000-0000-000000000004', ready(
                        states[min(len(calls)-2, 3)], 2 if scenario == 'superseded' else 1)
                from meshdb.cluster import _status
                def parse(value):
                    result = _status(value)
                    if scenario == 'parse_budget':
                        clock.now = 101.0
                    return result
                with patch('meshdb.cluster._monotonic', clock.monotonic), \
                        patch('meshdb.cluster._sleep', clock.sleep), \
                        patch('meshdb.cluster._status', parse), \
                        patch.object(client._transport, 'request', side_effect=request):
                    if scenario in ('progress', 'no_wait'):
                        self.assertEqual(client.scale(1, wait=scenario != 'no_wait', timeout=1), policy(1))
                    else:
                        expected = (errors.ConflictError if scenario == 'superseded' else
                                    errors.ProtocolError if scenario == 'read_failure' else
                                    errors.UnknownOutcomeError if scenario == 'invalid_ack' else
                                    errors.TransportError)
                        with self.assertRaises(expected) as caught:
                            client.scale(1, wait=True, timeout=1)
                        error = caught.exception
                        if scenario in ('superseded', 'submission_budget', 'parse_budget', 'poll_budget', 'sleep_budget'):
                            self.assertEqual(error.code, 'policy_superseded' if scenario == 'superseded' else 'wait_timeout')
                            self.assertEqual(error.request_id, calls[0][1])
                            self.assertIsNone(error.outcome)
                            self.assertIsNone(error.status_code)
                        if scenario == 'read_failure':
                            self.assertIs(error, failure)
                self.assertEqual(sum(c[0] == 'cluster.scale' for c in calls), 1)
                if scenario != 'no_wait':
                    self.assertTrue(all(c[2] == 101.0 for c in calls))
                if scenario == 'progress':
                    self.assertEqual(len(calls), 5)
                    self.assertEqual(len(clock.sleeps), 3)
                    self.assertTrue(all(0 < s <= 0.8 for s in clock.sleeps))
                if scenario == 'sleep_budget':
                    self.assertEqual(len(calls), 2)
                    self.assertEqual(len(clock.sleeps), 1)
                    self.assertAlmostEqual(clock.sleeps[0], 0.05)
                if scenario == 'no_wait':
                    self.assertEqual(len(calls), 1)
                    self.assertIsNone(calls[0][2])

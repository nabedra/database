"""The harness never hands one port to two workers and notices a worker that is not the one answering.

Run 45484607482 (O28, direct_oltp off): a worker lost its HTTP port and exited, another worker answered at
its URL, and `wait_workers` took that answer as the dead worker's. The workers here are small Python
processes that register, bind and answer readiness in the product's order. The harness code is the real one.
"""
import contextlib
import io
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness_ports
from oltp_direct_cluster import PortLost, Rf3Cluster, WORKERS

FAKE_WORKER = '''\
#!%s
import http.server, json, os, pathlib, sys, threading, time

path = pathlib.Path(sys.argv[sys.argv.index('--bootstrap') + 1])
config = json.loads(path.read_text())
mode = config.get('fake', 'serve')
print('queue active', mode, flush=True)
if mode == 'crash':
    print('pool open failed', flush=True)
    os._exit(3)
# The product registers the worker and activates its queue before it binds its HTTP listener.
path.with_suffix('.registered').touch()
started = time.monotonic()


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, status, body):
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        ready = len(list(path.parent.glob('*.registered')))
        if mode == 'late' and time.monotonic() - started < 1.5:
            return self.reply(503, {})
        self.reply(200, dict(replication_factor=3, ready_workers=ready, pid=os.getpid()))
        if mode == 'leave' and ready == 3:
            threading.Timer(0.3, os._exit, (7,)).start()

    def do_POST(self):
        identity = config['node']['placements']['control']['identity']
        self.reply(200, dict(data=dict(cluster_id=identity['cluster_id'], control_peers=[1, 2, 3],
                                       control_group_id='g')))


time.sleep({'slow': 1, 'silent': 60}.get(mode, 0))
host, port = config['bind'].rsplit(':', 1)
try:
    server = http.server.ThreadingHTTPServer((host, int(port)), Handler)
except OSError:
    print('worker HTTP bind failed', flush=True)
    os._exit(101)
server.serve_forever()
''' % sys.executable


def free_ports(count):
    """Distinct free ports, without harness_ports."""
    held = [socket.socket() for _ in range(count)]
    try:
        for sock in held:
            sock.bind(('127.0.0.1', 0))
        return [sock.getsockname()[1] for sock in held]
    finally:
        for sock in held:
            sock.close()


@contextlib.contextmanager
def kernel_answers(ports):
    """The kernel's next answers to a bind to port 0. It can repeat a port whose socket is closed."""
    script = list(ports)
    real = socket.socket

    class Scripted(real):
        def getsockname(self):
            address = super().getsockname()
            return (address[0], script.pop(0)) if script else address

    with mock.patch.object(harness_ports.socket, 'socket', Scripted):
        yield


class Case(unittest.TestCase):
    def setUp(self):
        self.stderr = io.StringIO()
        redirect = contextlib.redirect_stderr(self.stderr)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)
        getattr(harness_ports, '_picked', set()).clear()


class PickPort(Case):
    def test_a_repeated_kernel_answer_is_not_handed_out_twice(self):
        first, second = free_ports(2)
        with kernel_answers([first, first, first, second]):
            picks = [harness_ports.pick_port(), harness_ports.pick_port()]
        self.assertEqual(picks, [first, second])

    def test_thousands_of_picks_are_distinct(self):
        picks = [harness_ports.pick_port() for _ in range(3000)]
        self.assertEqual(len(set(picks)), len(picks))

    def test_concurrent_picks_are_distinct(self):
        picks = []

        def pick():
            picks.extend([harness_ports.pick_port() for _ in range(250)])

        threads = [threading.Thread(target=pick) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(set(picks)), 2000)

    def test_every_pick_is_logged(self):
        port = harness_ports.pick_port()
        self.assertRegex(self.stderr.getvalue(), rf'harness_ports: pick \d+ is port {port}\n')

    def test_the_collision_hook_still_holds_the_selected_pick(self):
        with mock.patch.dict(os.environ, NABEDRA_TEST_PORT_COLLISIONS='2'), \
                mock.patch.object(harness_ports, '_picks', 0):
            free, held = harness_ports.pick_port(), harness_ports.pick_port()
        self.addCleanup(harness_ports.release_held)
        self.assertFalse(harness_ports.port_taken(free))
        self.assertTrue(harness_ports.port_taken(held))


class Workers(Case):
    def setUp(self):
        super().setUp()
        directory = tempfile.TemporaryDirectory(prefix='nabedra-harness-ports-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.binary = self.root / 'fake-worker'
        self.binary.write_text(FAKE_WORKER)
        self.binary.chmod(0o700)

    def cluster(self, cls=Rf3Cluster):
        cluster = cls(self.binary, self.root / 'cluster', api_direct=())
        self.addCleanup(cluster.close)
        return cluster

    def prepared(self, **modes):
        cluster = self.cluster()
        cluster.prepare_workers()
        for name, mode in modes.items():
            cluster.config[name]['fake'] = mode
        return cluster

    def launch(self, cluster, names=WORKERS):
        for name in names:
            cluster.launch_worker(name)
            self.bound(cluster, name)

    def bound(self, cluster, name):
        """Wait until the worker's address has a listener, so a later worker on that address loses it."""
        if cluster.config[name].get('fake') in ('crash', 'slow', 'silent'):
            return
        host, port = cluster.config[name]['bind'].rsplit(':', 1)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and cluster.workers[name].alive():
            with contextlib.suppress(OSError), socket.create_connection((host, int(port)), timeout=1):
                return
            time.sleep(0.02)

    def alive(self, cluster):
        return [name for name in WORKERS if cluster.workers[name].alive()]

    def wait_exit(self, cluster, name):
        return cluster.workers[name].popen.wait(timeout=20)

    def assert_names(self, error, *expected):
        for text in expected:
            self.assertIn(text, str(error))

    def test_distinct_live_workers_are_ready(self):
        cluster = self.prepared()
        self.launch(cluster)
        views = cluster.wait_workers(deadline_seconds=30)
        self.assertEqual({name: view['pid'] for name, view in views.items()},
                         {name: cluster.workers[name].pid for name in WORKERS})

    def test_a_kernel_that_repeats_a_port_does_not_give_it_to_two_workers(self):
        cluster = self.cluster()
        ports = free_ports(6)
        # Picks 1 to 3 are the node ports, 4 to 6 the HTTP ports. The kernel repeats w1's HTTP port for w2.
        with kernel_answers([*ports[:4], ports[3], *ports[4:]]):
            cluster.prepare_workers()
        self.launch(cluster)
        cluster.wait_workers(deadline_seconds=30)
        self.assertEqual(self.alive(cluster), list(WORKERS))
        used = [bind for config in cluster.config.values() for bind in (config['bind'], config['node']['bind'])]
        self.assertEqual(len(set(used)), 6)

    def test_a_worker_that_exits_on_bind_fails_with_its_exit_and_log(self):
        cluster = self.prepared()
        cluster.config['w2']['bind'] = cluster.config['w1']['bind']
        self.launch(cluster)
        self.assertEqual(self.wait_exit(cluster, 'w2'), 101)
        with self.assertRaises(PortLost) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assert_names(raised.exception, f'w2 (pid {cluster.workers["w2"].pid})', 'status 101',
                          'worker HTTP bind failed', cluster.config['w1']['bind'].rsplit(':', 1)[1])

    def test_a_worker_that_loses_its_port_after_it_registered_fails(self):
        cluster = self.prepared(w2='slow')
        cluster.config['w2']['bind'] = cluster.config['w1']['bind']
        self.launch(cluster)
        with self.assertRaises(PortLost):
            # w1 answers at w2's URL in the second before w2 tries its port and exits.
            cluster.wait_workers(deadline_seconds=30)
        self.assertEqual(self.wait_exit(cluster, 'w2'), 101)

    def test_an_answer_from_another_worker_does_not_count(self):
        cluster = self.prepared(w2='silent')
        cluster.config['w2']['bind'] = cluster.config['w1']['bind']
        self.launch(cluster)
        with self.assertRaises(PortLost) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assert_names(raised.exception, f'w2 (pid {cluster.workers["w2"].pid})',
                          f'w1 (pid {cluster.workers["w1"].pid})', cluster.config['w2']['bind'], 'queue active silent')

    def test_an_unready_answer_from_another_worker_fails_before_the_deadline(self):
        cluster = self.prepared(w2='silent')
        cluster.config['w2']['bind'] = cluster.config['w1']['bind']
        # w3 never registers, so the answering worker never becomes ready.
        self.launch(cluster, names=('w1', 'w2'))
        with self.assertRaises(PortLost) as raised:
            cluster.wait_workers(names=('w1', 'w2'), deadline_seconds=10)
        self.assert_names(raised.exception, f'w1 (pid {cluster.workers["w1"].pid})')

    def test_a_worker_that_exits_after_its_answer_fails_and_is_not_a_lost_port(self):
        cluster = self.prepared(w1='leave', w3='late')
        self.launch(cluster)
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w1 (pid {cluster.workers["w1"].pid})', 'status 7', 'queue active leave')

    def test_another_startup_failure_reports_its_exit_and_is_not_a_lost_port(self):
        cluster = self.prepared(w2='crash')
        self.launch(cluster)
        self.wait_exit(cluster, 'w2')
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w2 (pid {cluster.workers["w2"].pid})', 'status 3', 'pool open failed')

    def test_the_first_start_reports_a_lost_port_and_retries_with_fresh_ports(self):
        class Duplicate(Rf3Cluster):
            attempts = []

            def prepare_workers(self):
                super().prepare_workers()
                for registered in self.root.glob('*.registered'):
                    registered.unlink()
                if not self.attempts:
                    self.config['w2'].update(bind=self.config['w1']['bind'], fake='slow')
                self.attempts.append({name: config['bind'] for name, config in self.config.items()})

        cluster = self.cluster(Duplicate)
        cluster.start()
        self.assertEqual(len(cluster.attempts), 2)
        self.assertEqual(self.alive(cluster), list(WORKERS))
        self.assertFalse(set(cluster.attempts[0].values()) & set(cluster.attempts[1].values()))
        self.assertIn('starting again with fresh ports', self.stderr.getvalue())

    def test_each_launch_logs_the_worker_and_its_ports(self):
        cluster = self.prepared()
        self.launch(cluster)
        for name in WORKERS:
            config = cluster.config[name]
            self.assertIn(f'launched {name} (pid {cluster.workers[name].pid}), HTTP {config["bind"]}, '
                          f'node {config["node"]["bind"]}\n', self.stderr.getvalue())


if __name__ == '__main__':
    unittest.main()

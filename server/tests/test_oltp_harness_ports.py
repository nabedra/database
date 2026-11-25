"""The harness never hands one port to two workers and notices a worker that is not the one answering.

Run 45484607482 (O28, direct_oltp off): a worker lost its HTTP port and exited, another worker answered at
its URL, and `wait_workers` took that answer as the dead worker's. The workers here are small Python
processes that register, bind and answer readiness in the product's order. The harness code is the real one.
"""
import contextlib
import http.server
import io
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness_ports
from oltp_direct_cluster import PortLost, Rf1Serve, Rf3Cluster, WORKERS

FAKE_WORKER = '''\
#!%s
import http.server, json, os, pathlib, sys, threading, time

def lost(text):
    print(text, flush=True)
    os._exit(101)


def outlive_nobody():
    # A test that is killed runs no cleanup, and the harness starts each worker in a session of its own.
    while os.getppid() == %d:
        time.sleep(0.2)
    os._exit(9)


threading.Thread(target=outlive_nobody, daemon=True).start()
if sys.argv[1] == 'serve':
    # `nabedra-db serve`: one RF1 process that takes its port from the command line.
    port = int(sys.argv[sys.argv.index('--port') + 1])
    modes = pathlib.Path(__file__).with_name('serve-modes')
    queued = modes.read_text().split() if modes.exists() else []
    modes.write_text(' '.join(queued[1:]))
    path, config, mode = None, dict(bind=f'127.0.0.1:{port}'), (queued or ['serve'])[0]
else:
    path = pathlib.Path(sys.argv[sys.argv.index('--bootstrap') + 1])
    config = json.loads(path.read_text())
    mode = config.get('fake', 'serve')
print('queue active', mode, flush=True)
if mode == 'crash':
    print('pool open failed', flush=True)
    os._exit(3)
if mode == 'mention':
    print('peer said: node listener failed', flush=True)
    lost('panicked at pool.rs: checksum mismatch')
if mode == 'in-use':
    # The exit of a worker that loses its node port.
    lost('nabedra-db: registered startup StorageUnavailable: local worker storage: '
         'Address already in use (os error 98)')
# The product registers the worker and activates its queue before it binds its HTTP listener.
if path:
    path.with_suffix('.registered').touch()
if mode in ('bind-error', 'serve-bind-error'):
    # The product prints the same text for every bind error, such as EADDRNOTAVAIL.
    lost('nabedra-db: ' + ('worker HTTP bind failed' if path else 'listener failed'))
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
        if not path:
            return self.reply(200, dict(replication_factor=1, topology='typed-groups-rf1'))
        ready = len(list(path.parent.glob('*.registered')))
        if mode == 'late' and time.monotonic() - started < 1.5:
            return self.reply(503, {})
        if mode == 'deaf' and ready == 3:
            server.socket.shutdown(2)
        if mode in ('deaf', 'orphan') and ready == 3:
            # Without the shutdown the socket listens on, owned by no process, until the server's poll returns.
            server.socket.close()
        self.reply(200, dict(replication_factor=3, ready_workers=ready, pid=os.getpid()))
        if mode == 'leave-now' and ready == 3:
            os._exit(7)
        if mode == 'leave' and ready == 3:
            threading.Timer(0.3, os._exit, (7,)).start()

    def do_POST(self):
        identity = config['node']['placements']['control']['identity']
        self.reply(200, dict(data=dict(cluster_id=identity['cluster_id'], control_peers=[1, 2, 3],
                                       control_group_id='g')))


time.sleep({'slow': 1, 'silent': 60}.get(mode, 0))


def listen(address):
    host, port = address.rsplit(':', 1)
    try:
        return http.server.ThreadingHTTPServer((host, int(port)), Handler)
    except OSError:
        lost('nabedra-db: ' + ('worker HTTP bind failed' if path else 'listener failed'))


if mode == 'steal':
    # A seeded product fault: the worker also listens on another worker's address.
    threading.Thread(target=listen(config['steal']).serve_forever).start()
server = listen(config['bind'])
try:
    server.serve_forever()
except (OSError, ValueError):
    pass
threading.Event().wait()
''' % (sys.executable, os.getpid())


def runs(pid):
    """Whether the process runs. One that ended and that nobody reaped does not."""
    state = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)], capture_output=True, text=True).stdout.strip()
    return bool(state) and not state.startswith('Z')


def end(pid):
    if runs(pid):
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)


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

    # Each pick binds a port for a moment, which another harness on the machine may want: not more than needed.
    # A pick that can repeat does so about ten times in these 600.
    def test_many_picks_are_distinct(self):
        picks = [harness_ports.pick_port() for _ in range(600)]
        self.assertEqual(len(set(picks)), len(picks))

    def test_concurrent_picks_are_distinct(self):
        picks = []

        def pick():
            picks.extend([harness_ports.pick_port() for _ in range(100)])

        threads = [threading.Thread(target=pick) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(set(picks)), 800)

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


@unittest.skipUnless(sys.platform.startswith('linux'), 'needs /proc')
class Listener(Case):
    def listening(self, host):
        sock = socket.socket()
        self.addCleanup(sock.close)
        sock.bind((host, 0))
        sock.listen()
        return sock.getsockname()[1]

    def child(self):
        process = subprocess.Popen([sys.executable, '-c', 'import time; print(1, flush=True); time.sleep(60)'],
                                   stdout=subprocess.PIPE)
        self.addCleanup(lambda: (process.kill(), process.wait(), process.stdout.close()))
        process.stdout.readline()
        return process.pid

    def test_own_listener(self):
        for host in ('127.0.0.1', '0.0.0.0'):
            self.assertEqual(harness_ports.listener(os.getpid(), '127.0.0.1', self.listening(host)),
                             ('own', [os.getpid()]))

    def test_a_listener_of_another_process(self):
        port = self.listening('127.0.0.1')
        self.assertEqual(harness_ports.listener(self.child(), '127.0.0.1', port),
                         ('outside', [os.getpid()]))
        self.assertEqual(harness_ports.listener(self.child(), '127.0.0.1', port, [os.getpid()]),
                         ('cluster', [os.getpid()]))

    def test_a_child_owns_for_its_parent(self):
        source = ('import socket, time; s = socket.socket(); s.bind(("127.0.0.1", 0)); s.listen(); '
                  'print(s.getsockname()[1], flush=True); time.sleep(60)')
        # The shell is the launched process and its child has the listener, as under `sudo ip netns exec`.
        process = subprocess.Popen(['sh', '-c', '"$0" -c "$1"; true', sys.executable, source],
                                   stdout=subprocess.PIPE, start_new_session=True)
        self.addCleanup(lambda: (os.killpg(process.pid, 9), process.wait(), process.stdout.close()))
        port = int(process.stdout.readline())
        state, owners = harness_ports.listener(process.pid, '127.0.0.1', port)
        self.assertEqual((state, len(owners)), ('own', 1))
        self.assertNotIn(process.pid, owners)

    def test_no_listener(self):
        port = self.listening('127.0.0.1')
        # The same port on another address, and a socket that is bound and does not listen.
        self.assertEqual(harness_ports.listener(os.getpid(), '127.0.0.2', port), ('none', []))
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.assertEqual(harness_ports.listener(os.getpid(), '127.0.0.1', sock.getsockname()[1]), ('none', []))


@unittest.skipUnless(sys.platform.startswith('linux'), 'needs /proc')
class LostBind(Case):
    HTTP = 'queue active\nnabedra-db: worker HTTP bind failed\n'
    PANIC = 'called `Result::unwrap()` on an `Err` value: "worker HTTP bind failed"\nnote: run with RUST_BACKTRACE=1\n'
    NODE = 'nabedra-db: registered startup StorageUnavailable: local worker storage: {} (os error {})\n'

    def held(self):
        sock = socket.socket()
        self.addCleanup(sock.close)
        sock.bind(('127.0.0.1', 0))
        sock.listen()
        return [f'127.0.0.1:{sock.getsockname()[1]}']

    def free(self):
        return [f'127.0.0.1:{free_ports(1)[0]}']

    def test_a_failed_bind_needs_an_outside_holder(self):
        for output in (self.HTTP, self.PANIC, 'nabedra-db: listener failed\n'):
            address = self.held()
            self.assertEqual(harness_ports.lost_bind(output, address), f'{address[0]} is held by pid {os.getpid()}')
            self.assertIsNone(harness_ports.lost_bind(output, address, [os.getpid()]))
            self.assertIsNone(harness_ports.lost_bind(output, self.free()))

    def test_eaddrinuse_in_the_exit_needs_no_holder_and_never_one_of_the_cluster(self):
        output = self.NODE.format('Address already in use', 98)
        self.assertIsNotNone(harness_ports.lost_bind(output, self.free()))
        self.assertIsNotNone(harness_ports.lost_bind(output, self.held()))
        self.assertIsNone(harness_ports.lost_bind(output, self.held(), [os.getpid()]))

    def test_another_error_or_a_mention_is_not_a_lost_port(self):
        for output in (self.NODE.format('Cannot assign requested address', 99), 'peer said: node listener failed\n',
                       'WARN a bind failed earlier\npanicked at pool.rs\n', 'the log says Address already in use\n', ''):
            for addresses in (self.held(), self.free()):
                self.assertIsNone(harness_ports.lost_bind(output, addresses), output)

    def test_a_holder_without_an_owner_is_not_outside(self):
        with mock.patch.object(harness_ports, 'holder', lambda *_: ('unknown', [])):
            self.assertIsNone(harness_ports.lost_bind(self.HTTP, self.held()))
        self.assertEqual(harness_ports._owner({'0x': ('0A', os.getuid())}, []), ('unknown', []))
        self.assertEqual(harness_ports._owner({'0x': ('0A', os.getuid() + 1)}, []), ('outside', []))

    def test_an_address_that_is_not_local_is_not_taken(self):
        port = int(self.held()[0].rsplit(':', 1)[1])
        self.assertTrue(harness_ports.port_taken(port))
        self.assertFalse(harness_ports.port_taken(port, '192.0.2.1'))


class Ready(http.server.ThreadingHTTPServer):
    daemon_threads = True


class ReadyHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        raw = json.dumps(dict(replication_factor=3, ready_workers=3)).encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


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
        for registered in cluster.root.glob('*.registered'):
            registered.unlink()
        for name, mode in modes.items():
            cluster.config[name]['fake'] = mode
        return cluster

    def launch(self, cluster, names=WORKERS):
        for name in names:
            cluster.launch_worker(name)
            self.bound(cluster, name)

    def bound(self, cluster, name):
        """Wait until the worker registered and, unless it delays that, its address has a listener, so a later
        worker on that address loses it."""
        host, port = cluster.config[name]['bind'].rsplit(':', 1)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and cluster.workers[name].alive():
            with contextlib.suppress(OSError):
                if cluster.config[name].get('fake') in ('slow', 'silent', 'bind-error'):
                    (cluster.root / f'{name}.registered').stat()
                    return
                with socket.create_connection((host, int(port)), timeout=1):
                    return
            time.sleep(0.02)

    def alive(self, cluster):
        return [name for name in WORKERS if cluster.workers[name].alive()]

    def outside(self, address, *, answers=False):
        """A process that is not of the cluster (this one) listens on the address."""
        host, port = address.rsplit(':', 1)
        if answers:
            server = Ready((host, int(port)), ReadyHandler)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            self.addCleanup(lambda: (server.shutdown(), server.server_close()))
            return
        sock = socket.socket()
        self.addCleanup(sock.close)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, int(port)))
        sock.listen()

    def started(self, first):
        """Starts a cluster whose first attempt `first(cluster)` has changed."""
        class Changed(Rf3Cluster):
            attempts = []

            def prepare_workers(self):
                super().prepare_workers()
                for registered in self.root.glob('*.registered'):
                    registered.unlink()
                if not self.attempts:
                    first(self)
                self.attempts.append({name: config['bind'] for name, config in self.config.items()})

            def wait_workers(self, names=WORKERS, deadline_seconds=20):
                return super().wait_workers(names, 20)

        return self.cluster(Changed)

    def assert_fails_once(self, first, *expected):
        """The first start fails for the stated reason and is not started again."""
        cluster = self.started(first)
        with self.assertRaises(AssertionError) as raised:
            cluster.start()
        self.assertEqual(len(cluster.attempts), 1, 'the start was retried')
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, *expected)
        return cluster

    def assert_retried(self, first, *expected):
        """The first start lost a port to a process outside the cluster and passes on fresh ports."""
        cluster = self.started(first)
        cluster.start()
        self.assertEqual(len(cluster.attempts), 2)
        self.assertEqual(self.alive(cluster), list(WORKERS))
        self.assertFalse(set(cluster.attempts[0].values()) & set(cluster.attempts[1].values()))
        for text in ('starting again with fresh ports', *expected):
            self.assertIn(text, self.stderr.getvalue())

    @staticmethod
    def same_port(cluster, name='w2', other='w1', **modes):
        cluster.config[name]['bind'] = cluster.config[other]['bind']
        for worker, mode in modes.items():
            cluster.config[worker]['fake'] = mode

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

    def test_a_worker_that_lost_its_port_to_another_worker_fails_with_its_exit_and_log(self):
        cluster = self.prepared()
        self.same_port(cluster)
        self.launch(cluster)
        self.assertEqual(self.wait_exit(cluster, 'w2'), 101)
        for check in (lambda: cluster.wait_workers(deadline_seconds=30), lambda: cluster.check_lost_port('w2')):
            with self.assertRaises(AssertionError) as raised:
                check()
            self.assertNotIsInstance(raised.exception, PortLost)
            self.assert_names(raised.exception, f'w2 (pid {cluster.workers["w2"].pid})', 'status 101',
                              'worker HTTP bind failed', 'w1', cluster.config['w1']['bind'])

    def test_a_worker_whose_address_another_one_has_by_configuration_fails(self):
        # w1 does not listen, so only the configuration shows the duplicate.
        cluster = self.prepared(w1='silent', w2='in-use')
        self.same_port(cluster)
        self.launch(cluster, names=('w1', 'w2'))
        self.wait_exit(cluster, 'w2')
        with self.assertRaises(AssertionError) as raised:
            cluster.check_lost_port('w2')
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w1 (pid {cluster.workers["w1"].pid})', cluster.config['w1']['bind'])

    @unittest.skipUnless(sys.platform.startswith('linux'), 'needs /proc')
    def test_a_worker_whose_address_another_one_took_fails_and_names_it(self):
        cluster = self.prepared(w1='steal')
        cluster.config['w1']['steal'] = cluster.config['w2']['bind']
        self.launch(cluster, names=('w1', 'w2'))
        self.wait_exit(cluster, 'w2')
        with self.assertRaises(AssertionError) as raised:
            cluster.check_lost_port('w2')
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w1 (pid {cluster.workers["w1"].pid}) has its address',
                          cluster.config['w2']['bind'])

    def test_a_dead_worker_fails_before_the_deadline_when_nobody_answers(self):
        cluster = self.prepared(w2='crash')
        self.launch(cluster, names=('w2',))
        self.wait_exit(cluster, 'w2')
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(names=('w2',), deadline_seconds=3)
        self.assert_names(raised.exception, 'status 3', 'pool open failed')

    def test_a_worker_that_lost_its_port_to_an_outside_process_reports_its_exit_and_log(self):
        cluster = self.prepared()
        self.outside(cluster.config['w2']['bind'])
        self.launch(cluster)
        self.wait_exit(cluster, 'w2')
        with self.assertRaises(PortLost) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assert_names(raised.exception, f'w2 (pid {cluster.workers["w2"].pid})', 'status 101',
                          'worker HTTP bind failed', cluster.config['w2']['bind'], f'pid {os.getpid()}')

    def test_an_answer_from_another_worker_fails(self):
        for mode in ('slow', 'silent'):
            cluster = self.prepared(w2=mode)
            self.same_port(cluster)
            self.launch(cluster)
            with self.assertRaises(AssertionError) as raised:
                # w1 answers at w2's URL while w2 is alive and has not tried its port.
                cluster.wait_workers(deadline_seconds=30)
            self.assertNotIsInstance(raised.exception, PortLost)
            self.assert_names(raised.exception, f'w2 (pid {cluster.workers["w2"].pid})',
                              f'w1 (pid {cluster.workers["w1"].pid})', cluster.config['w2']['bind'],
                              f'queue active {mode}')
            cluster.close()

    def test_an_unready_answer_from_another_worker_fails_before_the_deadline(self):
        cluster = self.prepared(w2='silent')
        self.same_port(cluster)
        # w3 never registers, so the answering worker never becomes ready.
        self.launch(cluster, names=('w1', 'w2'))
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(names=('w1', 'w2'), deadline_seconds=10)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w1 (pid {cluster.workers["w1"].pid})')

    def test_an_answer_from_an_outside_process_is_a_lost_port_once_the_worker_exits(self):
        cluster = self.prepared(w2='slow')
        self.outside(cluster.config['w2']['bind'], answers=True)
        self.launch(cluster)
        with self.assertRaises(PortLost) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assert_names(raised.exception, f'w2 (pid {cluster.workers["w2"].pid})', 'status 101',
                          'worker HTTP bind failed', f'pid {os.getpid()}', cluster.config['w2']['bind'])

    @unittest.skipUnless(sys.platform.startswith('linux'), 'needs /proc')
    def test_an_answer_from_an_outside_process_does_not_count_and_is_named_at_the_deadline(self):
        cluster = self.prepared(w2='silent')
        self.outside(cluster.config['w2']['bind'], answers=True)
        self.launch(cluster)
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=2)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w2 (pid {cluster.workers["w2"].pid})', f'pid {os.getpid()}',
                          cluster.config['w2']['bind'])

    def test_a_dead_worker_wins_over_another_ones_lost_port(self):
        # An outside process answers for w2, which is checked before w3.
        cluster = self.prepared(w2='silent', w3='crash')
        self.outside(cluster.config['w2']['bind'], answers=True)
        self.launch(cluster)
        self.wait_exit(cluster, 'w3')
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=20)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w3 (pid {cluster.workers["w3"].pid})', 'status 3', 'pool open failed')

    def test_a_dead_worker_wins_over_another_workers_answer(self):
        cluster = self.prepared(w2='silent', w3='crash')
        self.same_port(cluster)
        self.launch(cluster)
        self.wait_exit(cluster, 'w3')
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=20)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w3 (pid {cluster.workers["w3"].pid})', 'status 3', 'pool open failed')

    def test_a_dead_worker_wins_over_another_ones_exit_on_a_lost_port(self):
        cluster = self.prepared(w3='crash')
        self.outside(cluster.config['w2']['bind'])
        self.launch(cluster)
        self.wait_exit(cluster, 'w2')
        self.wait_exit(cluster, 'w3')
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=20)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w3 (pid {cluster.workers["w3"].pid})', 'status 3')

    def test_a_worker_killed_before_the_retry_fails_the_start(self):
        case = self

        class Killed(Rf3Cluster):
            waits = 0

            def wait_workers(self, *_, **__):
                self.waits += 1
                for name in WORKERS:
                    case.bound(self, name)
                self.workers['w3'].kill()
                raise PortLost('w2 lost its port')

        cluster = self.cluster(Killed)
        with self.assertRaises(AssertionError) as raised:
            cluster.start()
        self.assertEqual(cluster.waits, 1, 'the start was retried')
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w3 (pid {cluster.workers["w3"].pid})', 'status -9', 'queue active serve')

    def test_a_worker_that_exits_after_its_answer_fails_and_is_not_a_lost_port(self):
        cluster = self.prepared(w1='leave', w3='late')
        self.launch(cluster)
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w1 (pid {cluster.workers["w1"].pid})', 'status 7', 'queue active leave')

    def test_a_ready_worker_that_exits_while_another_never_answers_fails_before_the_deadline(self):
        cluster = self.prepared(w1='leave', w2='silent')
        self.launch(cluster)
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=8)
        self.assert_names(raised.exception, f'w1 (pid {cluster.workers["w1"].pid})', 'status 7', 'after it was ready')

    def test_a_worker_that_dies_while_another_ones_answer_is_checked_wins(self):
        cluster = self.prepared(w2='silent')
        self.same_port(cluster)
        self.launch(cluster)
        listener = harness_ports.listener

        def killing(pid, *where):
            if pid == cluster.workers['w2'].pid:
                cluster.workers['w3'].kill()
            return listener(pid, *where)

        with mock.patch.object(harness_ports, 'listener', killing), self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=20)
        self.assert_names(raised.exception, f'w3 (pid {cluster.workers["w3"].pid})', 'status -9')

    def test_a_worker_that_exits_right_after_its_ready_answer_reports_that_exit(self):
        cluster = self.prepared(w1='leave-now')
        self.launch(cluster)
        listener = harness_ports.listener

        def after_exit(pid, *where):
            # The exit falls between the answer and the liveness check.
            if pid == cluster.workers['w1'].pid:
                self.wait_exit(cluster, 'w1')
            return listener(pid, *where)

        with mock.patch.object(harness_ports, 'listener', after_exit), self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w1 (pid {cluster.workers["w1"].pid})', 'status 7',
                          'queue active leave-now')

    @unittest.skipUnless(sys.platform.startswith('linux'), 'needs /proc')
    def test_a_live_worker_that_stopped_listening_after_its_ready_answer_fails(self):
        cluster = self.prepared(w1='deaf')
        self.launch(cluster)
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assertTrue(cluster.workers['w1'].alive())
        self.assert_names(raised.exception, f'w1 (pid {cluster.workers["w1"].pid})', 'stopped listening',
                          cluster.config['w1']['bind'])

    @unittest.skipUnless(sys.platform.startswith('linux'), 'needs /proc')
    def test_a_listener_that_no_process_owns_is_not_a_lost_port(self):
        cluster = self.prepared(w1='orphan')
        self.launch(cluster)
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w1 (pid {cluster.workers["w1"].pid})', cluster.config['w1']['bind'])

    def test_another_startup_failure_reports_its_exit_and_is_not_a_lost_port(self):
        cluster = self.prepared(w2='crash')
        self.launch(cluster)
        self.wait_exit(cluster, 'w2')
        with self.assertRaises(AssertionError) as raised:
            cluster.wait_workers(deadline_seconds=30)
        self.assertNotIsInstance(raised.exception, PortLost)
        self.assert_names(raised.exception, f'w2 (pid {cluster.workers["w2"].pid})', 'status 3', 'pool open failed')

    def test_the_first_start_retries_a_port_lost_to_an_outside_process(self):
        self.assert_retried(lambda cluster: self.outside(cluster.config['w2']['bind']), 'status 101',
                            'worker HTTP bind failed', f'pid {os.getpid()}')

    def test_the_first_start_retries_an_exit_with_eaddrinuse_whose_holder_left(self):
        def first(cluster):
            cluster.config['w2']['fake'] = 'in-use'

        self.assert_retried(first, 'Address already in use')

    def test_the_first_start_does_not_retry_two_workers_with_one_port(self):
        for modes in (dict(w2='slow'), dict(w2='silent'), dict()):
            with self.subTest(modes):
                cluster = self.assert_fails_once(lambda cluster: self.same_port(cluster, **modes), 'w1', 'w2')
                cluster.close()

    def test_the_first_start_does_not_retry_a_node_port_lost_to_another_worker(self):
        def first(cluster):
            # Only the exit is reported: the stand-in does not listen on its node port.
            cluster.config['w2']['node']['bind'] = cluster.config['w1']['bind']
            cluster.config['w2']['fake'] = 'in-use'

        self.assert_fails_once(first, 'w2', 'w1', 'status 101', 'Address already in use')

    def test_the_first_start_does_not_retry_a_worker_that_takes_another_ones_address(self):
        def first(cluster):
            cluster.config['w1'].update(fake='steal', steal=cluster.config['w2']['bind'])
            cluster.config['w2']['fake'] = 'slow'

        self.assert_fails_once(first, 'w1', 'w2')

    def test_the_first_start_does_not_retry_a_bind_error_without_a_holder(self):
        self.assert_fails_once(lambda cluster: cluster.config['w2'].update(fake='bind-error'), 'w2', 'status 101',
                               'worker HTTP bind failed')

    def test_the_first_start_does_not_retry_a_panic_that_mentions_a_listener(self):
        self.assert_fails_once(lambda cluster: cluster.config['w2'].update(fake='mention'), 'w2', 'status 101',
                               'checksum mismatch')

    def test_the_first_start_does_not_retry_a_crash_while_an_outside_process_has_the_port(self):
        def first(cluster):
            cluster.config['w2']['fake'] = 'crash'
            self.outside(cluster.config['w2']['bind'])

        self.assert_fails_once(first, 'w2', 'status 3', 'pool open failed')

    def test_the_first_start_does_not_retry_a_dead_worker_next_to_a_lost_port(self):
        def first(cluster):
            cluster.config['w2']['fake'] = 'slow'
            cluster.config['w3']['fake'] = 'crash'
            self.outside(cluster.config['w2']['bind'], answers=True)

        self.assert_fails_once(first, 'w3', 'status 3', 'pool open failed')

    def serve(self, *modes, collisions=''):
        (self.root / 'serve-modes').write_text(' '.join(modes))
        server = Rf1Serve(self.binary, self.root / 'rf1')
        self.addCleanup(server.close)
        self.addCleanup(harness_ports.release_held)
        with mock.patch.dict(os.environ, NABEDRA_TEST_PORT_COLLISIONS=collisions), \
                mock.patch.object(harness_ports, '_picks', 0):
            try:
                server.start()
                return None
            except AssertionError as error:
                return error

    def test_serve_retries_a_port_lost_to_an_outside_process(self):
        self.assertIsNone(self.serve('serve', collisions='1'))
        self.assertIn('relaunching on a fresh one', self.stderr.getvalue())

    def test_serve_does_not_retry_a_bind_error_without_a_holder(self):
        error = self.serve('serve-bind-error', 'serve')
        self.assertIsNotNone(error, 'the start was retried')
        self.assertIn('listener failed', str(error))

    def test_a_killed_test_leaves_no_worker(self):
        source = ('import sys, time; sys.path.insert(0, sys.argv[1]); import test_oltp_harness_ports as t; '
                  'from pathlib import Path; root = Path(sys.argv[2]); binary = root / "fake-worker"; '
                  'binary.write_text(t.FAKE_WORKER); binary.chmod(0o700); server = t.Rf1Serve(binary, root / "rf1"); '
                  'server.start(); print(server.process.pid, flush=True); time.sleep(60)')
        for sig in (signal.SIGKILL, signal.SIGTERM, signal.SIGINT):
            with self.subTest(sig):
                test = subprocess.Popen([sys.executable, '-c', source, str(Path(__file__).resolve().parent),
                                         str(self.root)], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                self.addCleanup(lambda test=test: (test.kill(), test.wait(), test.stdout.close()))
                worker = int(test.stdout.readline())
                self.addCleanup(end, worker)
                self.assertTrue(runs(worker))
                test.send_signal(sig)
                test.wait(timeout=20)
                deadline = time.monotonic() + 20
                while runs(worker) and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertFalse(runs(worker), 'the worker outlived its test')

    def test_each_launch_logs_the_worker_and_its_ports(self):
        cluster = self.prepared()
        self.launch(cluster)
        for name in WORKERS:
            config = cluster.config[name]
            self.assertIn(f'launched {name} (pid {cluster.workers[name].pid}), HTTP {config["bind"]}, '
                          f'node {config["node"]["bind"]}\n', self.stderr.getvalue())


if __name__ == '__main__':
    unittest.main()

import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
CHILD = Path(__file__).parent / 'support' / 'serve_child.py'
HOST = r'''
import json, os, signal, socket, sys
from meshdb import _serve_process as p
p._STARTUP_SECONDS = 2.0
p._EXIT_SECONDS = .3
p._FRAME_SECONDS = .15
config = sys.argv[1]
with open(config) as f: mode = json.load(f)['mode']
if mode == 'held_second':
    # Ordinary timeout cannot satisfy the five-second completion watchdog.
    p._EXIT_SECONDS = 30.0
def record(event):
    with open(config + '.host', 'a') as f:
        f.write(event + '\n')
def prepared(value):
    from urllib.parse import urlsplit
    u = urlsplit(value['bound_url'])
    with socket.create_connection((u.hostname, u.port), timeout=1): pass
    record('prepared')
    if mode == 'hook_failure': raise RuntimeError('SECRET')
    if mode == 'startup_signal': os.kill(os.getpid(), signal.SIGTERM)
    if mode == 'announce_failure': os.close(sys.stdout.fileno())
    if mode == 'replaced': signal.signal(signal.SIGTERM, signal.SIG_IGN)
def stop():
    record('stop')
    if mode == 'stop_interrupt': raise KeyboardInterrupt
    if mode == 'held_second':
        os.write(1, b'stopped\n')
startup = dict(version=1, type='init', name='test',
    initial_policy=dict(mode='fixed', desired=0, min_nodes=0, max_nodes=0),
    provisioning='managed', bridge=dict(url='http://127.0.0.1:1234/private/v1/callback',
    callback_token='A'*43, authorize_token='B'*42+'A'))
if mode == 'callable':
    signal.signal(signal.SIGINT, lambda s, f: record('saved_callable'))
if mode == 'external':
    startup.update(provisioning='external', bridge=None)
close = p._Owner.close
def inspect_close(self):
    if self.child is not None:
        assert self.child.returncode is not None, 'child not reaped'
    if mode == 'flood':
        assert [len(x) for x in self._diagnostics] == [p._DIAGNOSTIC_BYTES]*2
        record('capped')
    close(self)
p._Owner.close = inspect_close
if mode == 'forward_race':
    class BoundarySignals(list):
        injected = False
        def __bool__(self):
            present = len(self) != 0
            if not present and not self.injected:
                self.injected = True
                # Signal arrives just after the first empty-pending snapshot.
                os.kill(os.getpid(), signal.SIGTERM)
                record('injected_signal')
            return present
    send = p._Owner.send
    def boundary_send(self, value, deadline, *, stopping=False):
        try:
            return send(self, value, deadline, stopping=stopping)
        finally:
            if value['type'] == 'shutdown':
                self.signals = BoundarySignals(self.signals)
    p._Owner.send = boundary_send
saved = signal.getsignal(signal.SIGTERM)
try:
    p.run_process(config, startup, prepared=prepared, stop=stop)
    record('return')
except KeyboardInterrupt:
    record('interrupt')
except Exception as e:
    record('error:' + e.code + ':' + str(e))
finally:
    record('restored:' + str(signal.getsignal(signal.SIGTERM) ==
        (signal.SIG_IGN if mode == 'replaced' else saved)))
'''

class ServeProcessTests(unittest.TestCase):
    def launch(self, mode):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        config = Path(temp.name) / 'config.json'
        record = Path(temp.name) / 'child.jsonl'
        config.write_text(json.dumps(dict(mode=mode, record=str(record))))
        env = {k: os.environ[k] for k in ('PATH', 'SYSTEMROOT') if k in os.environ}
        env.update(PYTHONPATH=str(ROOT / 'src'), MESHDB_SERVER_BINARY=str(CHILD),
                   MESHDB_SUPERVISOR_FD='discard-me')
        if mode == 'missing_binary':
            env['MESHDB_SERVER_BINARY'] = str(config.parent / 'absent')
        host = subprocess.Popen([sys.executable, '-c', HOST, str(config)], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def cleanup():
            if host.poll() is None:
                host.send_signal(signal.SIGTERM)
                try:
                    host.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    host.send_signal(signal.SIGTERM)
                    host.communicate(timeout=5)
            elif not host.stdout.closed:
                host.communicate(timeout=5)
        self.addCleanup(cleanup)
        return host, config, record

    def records(self, record):
        return [json.loads(line) for line in record.read_text().splitlines()]

    def ready(self, host):
        with selectors.DefaultSelector() as sel:
            sel.register(host.stdout, selectors.EVENT_READ)
            self.assertTrue(sel.select(5), 'host readiness watchdog')
        line = host.stdout.readline()
        if not line:
            self.fail(host.stderr.read().decode() or 'host exited before ready')
        value = json.loads(line)
        self.assertEqual(value['event'], 'meshdb.serve.ready')
        self.assertNotIn(':0', value['bound_url'])

    def finish(self, host, config, record):
        out, err = host.communicate(timeout=5)
        self.assertEqual((host.returncode, err), (0, b''))
        result = Path(str(config) + '.host').read_text()
        self.assertIn('restored:True', result)
        events = self.records(record)
        self.assertEqual(len({e['pid'] for e in events}), 1)
        with self.assertRaises(ProcessLookupError): os.kill(events[0]['pid'], 0)
        return result, events, out

    def test_signal_handshake_flood_and_reap(self):
        for mode, sig in [('external', signal.SIGTERM), ('normal', signal.SIGTERM), ('flood', signal.SIGINT),
                          ('callable', signal.SIGINT)]:
            with self.subTest(mode=mode):
                host, config, record = self.launch(mode)
                self.ready(host)
                host.send_signal(sig)
                result, events, out = self.finish(host, config, record)
                self.assertEqual(out, b'')
                self.assertIn('prepared\nstop\n', result)
                self.assertIn('saved_callable' if mode == 'callable' else
                              ('interrupt' if sig == signal.SIGINT else 'return'), result)
                names = [e['event'] for e in events]
                self.assertIn('activate', names)
                self.assertIn('signal_' + str(sig), names)
                if mode == 'flood':
                    self.assertIn('flooded', names)
                    self.assertIn('capped', result)

    def test_startup_and_lifetime_failures_cleanup(self):
        for mode in ['malformed', 'truncated', 'partial', 'fatal', 'bad_url',
                     'mismatch', 'eof', 'exit', 'lifetime_partial', 'hook_failure',
                     'announce_failure', 'replaced']:
            with self.subTest(mode=mode):
                host, config, record = self.launch(mode)
                result, events, out = self.finish(host, config, record)
                self.assertIn('error:', result)
                self.assertNotIn('SECRET', result)
                self.assertIn('stop', result)
                self.assertNotIn('A'*43, result + out.decode())
                self.assertNotIn('B'*42+'A', result + out.decode())

    def test_failed_spawn_closes_both_socket_ends(self):
        from unittest.mock import patch
        from meshdb import _serve_process as p
        observed = []
        original = p._Owner.close
        def closed(owner):
            resources = (owner.channel, owner.peer, owner.wake, owner.writer)
            original(owner)
            observed.extend(resource.fileno() for resource in resources)
        with patch.object(p, 'resolve_server_binary', return_value=str(CHILD)), \
                patch.object(p.subprocess, 'Popen', side_effect=OSError('SECRET')), \
                patch.object(p._Owner, 'close', closed):
            with self.assertRaises(p.MeshDBError) as caught:
                p.run_process('/already/validated', dict(version=1, type='init',
                    name='test', initial_policy=dict(mode='fixed', desired=0,
                    min_nodes=0, max_nodes=0), provisioning='external', bridge=None),
                    prepared=lambda _: None,
                              stop=lambda: None)
        self.assertNotIn('SECRET', str(caught.exception))
        self.assertEqual(observed, [-1, -1, -1, -1])

    def test_invalid_explicit_binary_has_no_fallback_or_child(self):
        host, config, record = self.launch('missing_binary')
        out, err = host.communicate(timeout=5)
        self.assertEqual((out, err, host.returncode), (b'', b'', 0))
        result = Path(str(config) + '.host').read_text()
        self.assertIn('stop\nerror:startup_failed:', result)
        self.assertIn('restored:True', result)
        self.assertFalse(record.exists())

    def test_selector_failure_stops_admission_and_sanitizes(self):
        from unittest.mock import patch
        from meshdb import _serve_process as p
        stopped = []
        with patch.object(p.selectors, 'DefaultSelector', side_effect=OSError('SECRET')):
            with self.assertRaises(p.MeshDBError) as caught:
                p.run_process('/already/validated', {}, prepared=lambda _: None,
                              stop=lambda: stopped.append(True))
        self.assertEqual(stopped, [True])
        self.assertEqual(caught.exception.code, 'startup_failed')
        self.assertNotIn('SECRET', str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)

    def test_signal_arriving_during_failure_shutdown_is_forwarded(self):
        host, config, record = self.launch('forward_race')
        self.ready(host)
        result, events, _ = self.finish(host, config, record)
        self.assertIn('injected_signal', result)
        self.assertIn('signal_' + str(signal.SIGTERM), [e['event'] for e in events])
        self.assertNotIn('forced_termination', result)

    def test_second_signal_expedites_owned_kill(self):
        host, config, record = self.launch('held_second')
        self.ready(host)
        host.send_signal(signal.SIGTERM)
        with selectors.DefaultSelector() as sel:
            sel.register(host.stdout, selectors.EVENT_READ)
            self.assertTrue(sel.select(5))
        self.assertEqual(host.stdout.readline(), b'stopped\n')
        host.send_signal(signal.SIGTERM)
        result, _, _ = self.finish(host, config, record)
        self.assertIn('error:forced_termination:', result)

    def test_stop_interrupt_still_reaps(self):
        host, config, record = self.launch('stop_interrupt')
        result, events, _ = self.finish(host, config, record)
        self.assertIn('interrupt', result)

    def test_startup_signal_closes_admission_before_activate(self):
        host, config, record = self.launch('startup_signal')
        result, events, out = self.finish(host, config, record)
        self.assertIn('return', result)
        self.assertEqual(out, b'')
        self.assertNotIn('activate', [e['event'] for e in events])

    def test_held_shutdown_forces_failure(self):
        host, config, record = self.launch('held')
        self.ready(host)
        host.send_signal(signal.SIGTERM)
        result, events, _ = self.finish(host, config, record)
        self.assertIn('error:forced_termination:', result)
        self.assertIn('shutdown', [e['event'] for e in events])

if __name__ == '__main__': unittest.main()

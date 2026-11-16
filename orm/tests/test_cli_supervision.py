"""Real subprocess ownership checks using the existing API socket protocol.

These exercise CLI cleanup, not the full U01/F03 scheduler assertions.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from meshdb._cli_config import example, protected_write, s3_storage

FIXTURE = '''import json, os, signal, socket, struct, sys
from pathlib import Path
if sys.argv[1:] == ['--version']:
    print('meshdb-server 0.1.0')
    sys.exit(0)
if sys.argv[1] == 'worker':
    b=json.loads(Path(sys.argv[3]).read_text())
    Path(os.environ['STATE']).write_text(json.dumps({'pid':os.getpid(),'bootstrap':b}))
    if os.environ.get('FAIL') == '1':
        sys.exit(23)
    while True: signal.pause()
s=socket.socket(fileno=int(os.environ['MESHDB_SUPERVISOR_FD']))
def receive():
    h=s.recv(4)
    if len(h)!=4: sys.exit(1)
    n=struct.unpack('!I',h)[0]; d=b''
    while len(d)<n: d+=s.recv(n-len(d))
    return json.loads(d)
def send(value):
    d=json.dumps(value).encode(); s.sendall(struct.pack('!I',len(d))+d)
receive()
r={'version':1,'type':'prepared','cluster_id':'11111111-1111-4111-8111-111111111111','bound_url':'http://127.0.0.1:8000','authorize_url':None}
send(r); receive(); del r['authorize_url']; r['type']='ready'; send(r)
receive()
'''


@unittest.skipUnless(os.name == 'posix', 'POSIX supervisor')
class SupervisionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        binary = self.root / 'meshdb-server'
        binary.write_text('#!' + sys.executable + '\n' + FIXTURE)
        binary.chmod(0o700)
        auth = self.root / 'auth.json'
        protected_write(auth, dict(admin_token='A'*43, data_token='D'*43, enrollment_token='E'*43))
        config = example('fixed', s3_storage('s3://test-bucket/db/', 'eu-west-1'), auth)
        config['lifecycle'] = {'drain_ms': 200}
        self.config = self.root / 'config.json'
        protected_write(self.config, config)
        self.state = self.root / 'worker.json'
        self.env = dict(os.environ, MESHDB_SERVER_BINARY=str(binary), STATE=str(self.state),
                        PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))

    def start(self, fail=False):
        env = dict(self.env, FAIL='1' if fail else '0')
        child = subprocess.Popen([sys.executable, '-m', 'meshdb.cli', 'serve', '--config', str(self.config), '--threads-per-worker', '2', '--local-workers', '1', '--cold-start'],
                                 env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(self.cleanup, child)
        return child

    def cleanup(self, child):
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)

    def test_worker_failure_preserves_status_and_reaps_api(self):
        child = self.start(True)
        out, err = child.communicate(timeout=8)
        self.assertEqual(child.returncode, 23, (out, err))
        self.assertIn(b'meshdb.serve.launched', out)
        record = json.loads(self.state.read_text())
        self.assertEqual(record['bootstrap']['resources']['max_threads'], 2)
        self.assertEqual(record['bootstrap']['mode'], 'fixed')
        with self.assertRaises(ProcessLookupError):
            os.kill(record['pid'], 0)

    def test_signal_drains_and_reaps_worker(self):
        child = self.start()
        deadline = time.monotonic()+5
        while not self.state.exists() and time.monotonic()<deadline and child.poll() is None:
            time.sleep(.01)
        self.assertTrue(self.state.exists())
        pid = json.loads(self.state.read_text())['pid']
        child.terminate()
        child.communicate(timeout=8)
        self.assertEqual(child.returncode, -signal.SIGTERM)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_compatible_path_binary_runs_both_api_and_worker(self):
        prefix = self.root / 'incompatible-env'
        (prefix / 'bin').mkdir(parents=True)
        incompatible = prefix / 'bin' / 'meshdb-server'
        incompatible.write_text('#!' + sys.executable + '\nimport sys\n'
                                'print("meshdb-server 9.0.0")\nsys.exit(42)\n')
        incompatible.chmod(0o700)
        env = dict(self.env, PATH=str(self.root) + os.pathsep + os.environ.get('PATH', ''))
        env.pop('MESHDB_SERVER_BINARY')
        script = 'import sys; sys.prefix=sys.argv.pop(1); from meshdb.cli import main; raise SystemExit(main())'
        child = subprocess.Popen([sys.executable, '-c', script, str(prefix), 'serve', '--config', str(self.config),
                                  '--local-workers', '1'], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(self.cleanup, child)
        deadline = time.monotonic() + 5
        while not self.state.exists() and time.monotonic() < deadline and child.poll() is None:
            time.sleep(.01)
        self.assertTrue(self.state.exists(), 'Compatible API must start and launch its worker')
        pid = json.loads(self.state.read_text())['pid']
        child.terminate()
        out, err = child.communicate(timeout=8)
        self.assertEqual(child.returncode, -signal.SIGTERM, (out, err))
        self.assertIn(b'meshdb.serve.launched', out)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)


if __name__ == '__main__':
    unittest.main()

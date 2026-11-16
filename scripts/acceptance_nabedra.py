#!/usr/bin/env python3.11
"""Installed CLI/client smoke acceptance against trusted loopback SQLite storage.

Exercises a small U01/C01/R04 subset, not the full scheduler contract or AWS.
No cloud credentials, provisioning, optional dependencies or downloads are used.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'orm/src'))
from nabedra import Database, StorageError, TransportError, UnknownOutcomeError
from nabedra._cli_config import example, protected_write
from nabedra._control_transport import _ControlTransport
from acceptance_nodes import announcement


CLIENT = '''
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import sys
from nabedra import AuthenticationError, Database, ValidationError, col
import nabedra

url, token, phase = sys.argv[1:]
installed = bool(os.environ.get('NABEDRA_CHECK_INSTALLED'))
version = importlib.metadata.version('nabedra') if installed else 'source checkout'
if installed:
    assert version
    assert Path(nabedra.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
db = Database(url=url, mode='oltp', token=token, timeout=10)
rows_by_size = {size: [{'id': i, 'value': 'row-' + str(i)} for i in range(size)]
                for size in (10, 100, 1000)}
if phase == 'create':
    for size, rows in rows_by_size.items():
        table = db.create_table('smoke_' + str(size), {'id': int, 'value': str}, key='id')
        assert table.insert_many(rows) == size
        assert table.get(size - 1) == rows[-1]
        assert table.where(col('id') >= size - 3).collect() == rows[-3:]
    try:
        Database(url=url, mode='invalid', token=token)
    except ValidationError:
        pass
    else:
        raise AssertionError('invalid mode accepted')
    assert Database(url=url, mode='oltp').table('smoke_10').get(9) == rows_by_size[10][-1]
    del os.environ['NABEDRA_TOKEN']
    missing = Database(url=url, mode='oltp')
    try:
        missing.tables()
    except AuthenticationError:
        pass
    else:
        raise AssertionError('missing token operation accepted')
olap = Database(url=url, mode='olap', token=token, timeout=10)
for size, rows in rows_by_size.items():
    assert olap.table('smoke_' + str(size)).query().collect() == rows
print(json.dumps({'phase': phase, 'distribution': 'nabedra',
                  'import': 'nabedra', 'package_version': version,
                  'python_version': sys.version.split()[0],
                  'installed_prefix_verified': installed, 'row_counts': list(rows_by_size),
                  'total_rows': sum(rows_by_size),
                  'source_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in Path(nabedra.__file__).parent.glob('*.py')}}))
'''


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def child_pids(pid):
    result = subprocess.run(['/bin/ps', '-axo', 'pid=,ppid='], capture_output=True,
                            check=True, timeout=5)
    return [int(parts[0]) for line in result.stdout.splitlines()
            if len(parts := line.split()) == 2 and int(parts[1]) == pid]


def reaped(pids):
    for pid in pids:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            continue
        return False
    return True


def run(args, report):
    binary = str(Path(args.binary).resolve())
    python = str(Path(args.python or sys.executable).absolute())
    cli = [str(Path(args.cli).absolute())] if args.cli else [python, '-m', 'nabedra.cli']
    started = time.monotonic()
    deadline = started + 90
    env = {'PATH': os.path.dirname(python) + os.pathsep + os.defpath,
           'PYTHONDONTWRITEBYTECODE': '1', 'NABEDRA_SERVER_BINARY': binary}
    if not args.python:
        env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1] / 'orm/src')
    else:
        env['NABEDRA_CHECK_INSTALLED'] = '1'

    def passed(label):
        report['assertions'].append(label)

    def command(argv, **kwargs):
        return subprocess.run(argv, env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, check=True,
                              timeout=max(.1, deadline - time.monotonic()), **kwargs)

    report['binary_sha256'] = hashlib.sha256(Path(binary).read_bytes()).hexdigest()
    report['version'] = json.loads(command([*cli, '--version', '--json']).stdout)
    passed('CLI01 installed CLI and compatible native binary versions')
    with tempfile.TemporaryDirectory(prefix='nabedra-smoke-') as directory, tempfile.TemporaryFile() as diagnostics:
        root = Path(directory)
        auth = {name: secrets.token_urlsafe(32)
                for name in ('admin_token', 'data_token', 'enrollment_token')}
        protected_write(root / 'auth.json', auth)
        store_token = secrets.token_urlsafe(32)
        protected_write(root / 'store-token', (store_token + '\n').encode())
        env['NABEDRA_TOKEN'] = auth['data_token']
        store = subprocess.Popen([sys.executable, str(Path(__file__).with_name('local_store.py')),
                                  '--database', str(root / 'objects.sqlite'), '--port', '0'],
                                 env={**env, 'NABEDRA_STORE_TOKEN': store_token},
                                 stdout=subprocess.PIPE, stderr=diagnostics)
        process = None
        owned = []
        seen_workers = set()

        def stop(validate=True):
            nonlocal process, owned
            if process is None:
                return
            current, process = process, None
            if current.poll() is None:
                owned = child_pids(current.pid)
                current.terminate()
            try:
                current.wait(timeout=15)
                if validate:
                    check(current.returncode == -signal.SIGTERM, 'CLI did not preserve graceful SIGTERM')
                    check(reaped(owned), 'CLI left an owned child behind')
                if reaped(owned):
                    owned = []
            finally:
                if current.poll() is None:
                    current.kill()
                    current.wait(timeout=5)
                current.stdout.close()

        try:
            address = announcement(store)
            with socket.socket() as reserve:
                reserve.bind(('127.0.0.1', 0))
                port = reserve.getsockname()[1]
            url = 'http://127.0.0.1:' + str(port)
            storage = dict(driver='http', options=dict(
                base_url='http://127.0.0.1:' + str(address['port']), namespace='smoke/',
                token_file=str(root / 'store-token'), ca_file=None))
            config = example('fixed', storage, root / 'auth.json', threads=2,
                             bind='127.0.0.1:' + str(port))
            config['lifecycle'] = dict(drain_ms=20000)
            config['worker']['execution'] = dict(
                max_workers_per_request=1, max_pending_requests=64,
                max_queue_bytes_per_replica=64 << 20,
                max_retained_result_bytes_per_worker=16 << 20, result_retention_ms=300000,
                queue_replication='all_ready_workers', queue_lifetime='live_cluster',
                partition_size='64 MiB')
            protected_write(root / 'nabedra-database.json', config)

            def start(cold=False):
                nonlocal process, owned
                owned = []
                report['phase'] = 'cold-start' if cold else 'startup'
                process = subprocess.Popen([*cli, 'database', 'serve', '--config',
                    str(root / 'nabedra-database.json'), '--local-workers', '1',
                    '--threads-per-worker', '2', *(['--cold-start'] if cold else [])],
                    env=env, stdout=subprocess.PIPE, stderr=diagnostics)
                message = announcement(process)
                check(message.get('event') == 'nabedra.serve.launched', 'incorrect startup event')
                check(message.get('bound_url', '').rstrip('/') == url, 'incorrect API endpoint')
                admin = _ControlTransport(url, auth['admin_token'], timeout=5)
                while time.monotonic() < deadline:
                    check(process.poll() is None, 'CLI exited before worker enrollment')
                    _, status = admin.request('cluster.status', {},
                        deadline=min(deadline, time.monotonic() + 5))
                    ready = [n for n in status['nodes'] if n['state'] == 'ready']
                    report['enrollment'] = dict(ready=len(ready),
                        newly_enrolled=sum(n['node_id'] not in seen_workers for n in ready))
                    if len(ready) == 1 and ready[0]['node_id'] not in seen_workers:
                        check(len(ready) == 1 and ready[0]['resources']['max_threads'] == 2,
                              'worker did not advertise two compute threads')
                        seen_workers.add(ready[0]['node_id'])
                        owned = child_pids(process.pid)
                        check(len(owned) == 2, 'expected exactly one API and one worker')
                        # Membership can precede fresh API routing observations. Probe
                        # only a read before the once-only data assertions begin.
                        probe = Database(url=url, token=auth['data_token'], timeout=2)
                        attempts = 0
                        while time.monotonic() < deadline:
                            attempts += 1
                            try:
                                probe.tables()
                            except (StorageError, TransportError, UnknownOutcomeError):
                                time.sleep(.1)
                            else:
                                report.setdefault('readiness_probe_attempts', []).append(attempts)
                                return
                        raise TimeoutError('read-only routing readiness deadline')
                    time.sleep(.05)
                raise TimeoutError('worker enrollment deadline')

            start()
            passed('U01/C01 subset actual CLI API and one two-thread worker enrolled')
            report['phase'] = 'client-create'
            result = command([python, '-c', CLIENT, url, auth['data_token'], 'create'])
            report['client'] = json.loads(result.stdout)
            expected_sources = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in (Path(__file__).resolve().parents[1] / 'orm/src/nabedra').glob('*.py')}
            check(report['client']['source_sha256'] == expected_sources,
                  'installed package differs from the current source tree')
            passed('U01 subset OLTP exact insert/get/filter for 10, 100 and 1000 rows')
            passed('U01 subset OLAP reads the same 1110 committed rows')
            passed('invalid mode rejected locally; NABEDRA_TOKEN fallback; absent token rejected')
            stop()
            passed('F03 subset graceful SIGTERM reaps API and worker')
            start(cold=True)
            report['phase'] = 'client-reopen'
            result = command([python, '-c', CLIENT, url, auth['data_token'], 'reopen'])
            report['restart'] = json.loads(result.stdout)
            passed('R04 subset cold restart retains all 1110 committed rows')
            stop()
            passed('F03 subset restarted CLI reaps API and worker')
        except BaseException:
            if process is not None:
                report['cli_exit_code'] = process.poll()
            diagnostics.seek(0)
            report['fixture_diagnostics'] = diagnostics.read(4096).decode('utf-8', 'replace')
            raise
        finally:
            try:
                stop(validate=False)
            finally:
                for pid in owned:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                if store.poll() is None:
                    store.terminate()
                try:
                    store.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    store.kill()
                    store.wait(timeout=5)
                store.stdout.close()
    report['elapsed_seconds'] = round(time.monotonic() - started, 3)
    report['phase'] = 'complete'
    report['result'] = 'passed'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--cli', help='installed nabedra executable')
    parser.add_argument('--python', help='Python interpreter from the installed prefix')
    parser.add_argument('--output', type=Path, default=Path('artifacts/rename/integration.json'))
    args = parser.parse_args()
    report = dict(timestamp=datetime.now(timezone.utc).isoformat(), assertions=[],
                  scope='local conditional SQLite service; no AWS or full scheduler acceptance',
                  result='failed')
    try:
        run(args, report)
    except Exception as error:
        report['failure_type'] = type(error).__name__
        if type(error) is AssertionError:
            report['failure_assertion'] = str(error)
        if isinstance(error, subprocess.CalledProcessError):
            report['client_failure_stderr'] = (error.stderr or b'')[-4096:].decode('utf-8', 'replace')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    return 0 if report['result'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3.11
"""Run the shared ORM contract against real local OLAP and OLTP servers."""
from contextlib import AbstractContextManager
import argparse
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

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'orm/python/src'))
from acceptance_nodes import announcement
from acceptance_nabedra import child_pids, reaped
from nabedra import Database, StorageError, TransportError, UnknownOutcomeError
from nabedra._cli_config import example, protected_write
from oltp_orm_contract import exercise


class OlapServer(AbstractContextManager):
    def __init__(self, binary):
        self.binary = str(Path(binary).resolve())
        self.temporary = tempfile.TemporaryDirectory(prefix='nabedra-olap-parity-')
        self.root = Path(self.temporary.name)
        self.diagnostics = (self.root / 'server.log').open('w+b')
        self.store = self.process = None
        self.children = []
        self.token = secrets.token_urlsafe(32)
        self.database = None

    def __enter__(self):
        try:
            version = subprocess.check_output([self.binary, '--version'], text=True)
            assert 'olap' in version.split(), version
            self.env = dict(os.environ, PYTHONPATH=str(ROOT / 'orm/python/src'),
                            PYTHONDONTWRITEBYTECODE='1', NABEDRA_SERVER_BINARY=self.binary,
                            NABEDRA_TOKEN=self.token)
            store_token = secrets.token_urlsafe(32)
            self.store = subprocess.Popen([sys.executable, str(ROOT / 'scripts/local_store.py'),
                '--database', str(self.root / 'objects.sqlite'), '--port', '0'],
                env=dict(self.env, NABEDRA_STORE_TOKEN=store_token), stdout=subprocess.PIPE, stderr=self.diagnostics)
            address = announcement(self.store)
            protected_write(self.root / 'store-token', (store_token + '\n').encode())
            protected_write(self.root / 'auth.json', dict(admin_token=secrets.token_urlsafe(32),
                data_token=self.token, enrollment_token=secrets.token_urlsafe(32)))
            with socket.socket() as reserve:
                reserve.bind(('127.0.0.1', 0))
                port = reserve.getsockname()[1]
            self.url = f'http://127.0.0.1:{port}'
            storage = dict(driver='http', options=dict(base_url=f"http://127.0.0.1:{address['port']}",
                namespace='orm-parity/', token_file=str(self.root / 'store-token'), ca_file=None))
            config = example('fixed', storage, self.root / 'auth.json', threads=2, bind=f'127.0.0.1:{port}')
            protected_write(self.root / 'config.json', config)
            self.process = subprocess.Popen([sys.executable, '-m', 'nabedra.cli', 'database', 'serve',
                '--config', str(self.root / 'config.json'), '--local-workers', '1', '--threads-per-worker', '2'],
                env=self.env, stdout=subprocess.PIPE, stderr=self.diagnostics)
            message = announcement(self.process)
            assert message.get('event') == 'nabedra.serve.launched', message
            db = Database(url=self.url, token=self.token, database=self.database, timeout=2)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                assert self.process.poll() is None, 'OLAP supervisor exited during startup'
                try:
                    db.tables()
                    self.children = child_pids(self.process.pid)
                    return self
                except (StorageError, TransportError, UnknownOutcomeError):
                    time.sleep(.05)
            raise TimeoutError('OLAP routing did not become ready')
        except BaseException:
            self.close()
            raise

    def close(self):
        failure = None
        if self.process:
            self.children = list(set(self.children + child_pids(self.process.pid))) if self.process.poll() is None else self.children
            if self.process.poll() is None:
                self.process.terminate()
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                failure = 'OLAP shutdown watchdog expired'
            if not reaped(self.children):
                for pid in self.children:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                failure = failure or 'OLAP left an owned worker/API process alive'
            self.process.stdout.close()
            self.process = None
        if self.store:
            if self.store.poll() is None:
                self.store.terminate()
            try:
                self.store.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.store.kill()
                self.store.wait()
                failure = failure or 'local store shutdown watchdog expired'
            self.store.stdout.close()
            self.store = None
        self.diagnostics.close()
        self.temporary.cleanup()
        if failure:
            raise AssertionError(failure)

    def __exit__(self, *_):
        self.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--olap-binary', type=Path, required=True)
    parser.add_argument('--oltp-binary', type=Path)
    args = parser.parse_args()
    with OlapServer(args.olap_binary) as server:
        observed = exercise(server.url, server.token, server.database)
    if args.oltp_binary:
        from oltp_process import Server
        with tempfile.TemporaryDirectory(prefix='nabedra-oltp-parity-') as directory:
            output = Path(os.environ.get('NABEDRA_OLTP_ACCEPTANCE_OUTPUT', ROOT / 'artifacts/oltp-acceptance'))
            output.mkdir(parents=True, exist_ok=True)
            binary = args.oltp_binary.resolve()
            if os.environ.get('NABEDRA_TEST_DIRECT_OLTP') == 'off':
                os.environ['NABEDRA_DIRECT_OFF_BINARY'] = str(binary)
                os.environ['TMPDIR'] = directory
                binary = Path(__file__).with_name('direct_off_server.sh')
            server = Server(str(binary), directory, diagnostics=output / 'oltp-parity-server.log')
            try:
                server.start()
                counterpart = exercise(server.url, 'test-token', None, unauthorized_database='other')
            finally:
                server.stop()
        # Database scope is asserted separately for each deployment's policy.
        counterpart.pop('database_scope', None)
        assert observed == counterpart, 'engine-independent ORM observations differ'
    print(json.dumps(dict(result='passed', engines=['olap', 'oltp'] if args.oltp_binary else ['olap'],
                          operations=observed['operations'], scales=observed['scales']), sort_keys=True))


if __name__ == '__main__':
    main()

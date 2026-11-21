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
from nabedra import AuthenticationError, Database, NotFoundError, StorageError, TransportError, UnknownOutcomeError
from nabedra._cli_config import example, protected_write
from oltp_orm_contract import exercise, rejected


class OlapServer(AbstractContextManager):
    def __init__(self, binary, namespace="orm-parity/"):
        self.binary = str(Path(binary).resolve())
        self.namespace = namespace
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
                namespace=self.namespace, token_file=str(self.root / 'store-token'), ca_file=None))
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


def typescript(**options):
    script = Path(__file__).with_name('oltp_compatibility_clients.mjs')
    subprocess.run(['node', str(script)], input=json.dumps(options), text=True,
                   timeout=240, check=True)


def large_count(url, token, database, total, limit):
    db = Database(url=url, token=token, database=database, timeout=120)
    table = db.create_table('o01_count', {'id': int}, key='id')
    try:
        for start in range(0, total, 10000):
            rows = [{'id': i} for i in range(start, min(total, start + 10000))]
            assert table.insert_many(rows) == len(rows)
        assert table.query().count() == total
        assert table.query().limit(limit).count() == limit
        assert table.get(total - 1) == {'id': total - 1}
        typescript(url=url, token=token, database=database, name=table.name,
                   id=table.id, total=total, limit=limit,
                   forbiddenDatabase='other' if database else None)
    finally:
        table.drop()
        db.close()


def olap_scope(first, other):
    db = Database(url=first.url, token=first.token, timeout=20)
    table = db.create_table('o01_scope', {'id': int}, key='id')
    table.insert({'id': 1})
    foreign = Database(url=other.url, token=other.token, timeout=20)
    try:
        rejected(AuthenticationError, lambda: Database(url=other.url, token=first.token).tables())
        rejected(AuthenticationError, lambda: Database(url=first.url, token=other.token).tables())
        assert all(item.id != table.id and item.name != table.name for item in foreign.tables())
        rejected(NotFoundError, lambda: foreign.table(table.name))
        denied = foreign.submit('table.get', {'table_id': table.id, 'key': 1}).wait(timeout=20)
        assert denied.state == 'failed' and denied.error["code"] == 'not_found', denied
        assert table.get(1) == {'id': 1}
        typescript(url=other.url, token=other.token, foreignToken=first.token,
                   name=table.name, id=table.id)
    finally:
        table.drop()
        foreign.close()
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--olap-binary', type=Path, required=True)
    parser.add_argument('--oltp-binary', type=Path)
    args = parser.parse_args()
    clients_available = (ROOT / 'orm/typescript/dist/index.js').is_file()
    with OlapServer(args.olap_binary) as server:
        observed = exercise(server.url, server.token, server.database)
        if args.oltp_binary and clients_available:
            with OlapServer(args.olap_binary, 'orm-other/') as other:
                olap_scope(server, other)
            large_count(server.url, server.token, None, 100000, 99999)
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
                counterpart = exercise(server.url, 'test-token', 'test', unauthorized_database='other')
                if clients_available:
                    large_count(server.url, 'test-token', 'test', 100001, 100001)
            finally:
                server.stop()
        assert counterpart.pop('database_scope') == 'unauthorized'
        assert observed == counterpart, 'engine-independent ORM observations differ'
        checks = {
            'operations-and-descriptors': ('passed', observed['operations']),
            'results-types-and-ordering': ('passed', 'Both engines matched the public decoder and row-order assertions.'),
            'errors-and-table-identity': ('passed', 'Conflict, stale-handle and recreated-table assertions ran on both engines.'),
            'advisory-mode-and-parquet': ('passed', 'Both modes opened the same descriptor with format parquet on both engines.'),
            'authentication-and-scope': ('passed', 'Both ORMs checked invalid credentials and scope. OLAP isolates authenticated endpoints and catalogs. OLTP rejects another database selector.')
                if clients_available else ('not-run', 'Build orm/typescript/dist before running shared scope assertions.'),
            'large-counts': ('passed', 'Both ORMs counted 100000 rows on OLAP and 100001 rows on OLTP through unchanged query().count() calls. Clients with the legacy 100000 decoder limit need an upgrade.')
                if clients_available else ('not-run', 'Build orm/typescript/dist before running shared production large-count assertions.'),
        }
        for assertion, (status, detail) in checks.items():
            marker = dict(scenario='O01', assertion=assertion, status=status, detail=detail)
            print('NABEDRA_OLTP_ASSERT ' + json.dumps(marker), flush=True)
    print(json.dumps(dict(result='passed', engines=['olap', 'oltp'] if args.oltp_binary else ['olap'],
                          operations=observed['operations'], scales=observed['scales']), sort_keys=True))


if __name__ == '__main__':
    main()

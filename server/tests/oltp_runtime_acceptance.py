#!/usr/bin/env python3.11
"""Actual OLTP API admission, terminal retirement and process recovery checks.

These use the installed server path and actual Control/TSO/Status services.
Deterministic pending/Prepare/ambiguous kill barriers are separate required O15
variations; a completed COMMIT replay alone does not certify O15.
"""
from contextlib import AbstractContextManager, contextmanager
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
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'orm/python/src')]
from acceptance_nodes import announcement
from acceptance_nabedra import child_pids, reaped
from nabedra import Database
from nabedra._cli_config import protected_write
from oltp_process import Server, raw, terminal


class RuntimeServer(AbstractContextManager):
    """One real local worker, launched through native serve or public Python CLI."""
    def __init__(self, binary, directory, *, cli=False, short_retention=False):
        self.binary = str(Path(binary).resolve())
        self.root = Path(directory)
        self.directory = self.root / 'volume'
        self.directory.mkdir(exist_ok=True)
        self.cli = cli
        self.token = secrets.token_urlsafe(32)
        self.database = 'test'
        self.process = None
        self.children = []
        self.resources = dict(max_threads=4, memory_limit_bytes=8 << 30,
                              task_memory_limit_bytes=512 << 20,
                              control_memory_reserve_bytes=256 << 20)
        self.execution = dict(max_workers_per_request=1, max_pending_requests=4,
            max_queue_bytes_per_replica=32768 if short_retention else 64 << 20,
            max_retained_result_bytes_per_worker=1 << 20, result_retention_ms=100 if short_retention else 60000,
            queue_replication='all_ready_workers', queue_lifetime='live_cluster', partition_size='64 MiB')
        self.worker_count = 1
        self.log = (self.root / 'server.log').open('a+b')

    def start(self, *, cold_start=False):
        assert self.process is None
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(('AWS_', 'NABEDRA_S3_'))}
        env.update(PYTHONPATH=str(ROOT / 'orm/python/src'), PYTHONDONTWRITEBYTECODE='1',
                   NABEDRA_SERVER_BINARY=self.binary, NABEDRA_TOKEN=self.token)
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
        self.url = f'http://127.0.0.1:{port}'
        protected = self.root / 'auth.json'
        if not protected.exists():
            protected_write(protected, dict(admin_token=secrets.token_urlsafe(32),
                data_token=self.token, enrollment_token=secrets.token_urlsafe(32)))
        if getattr(self, 'test_executable', None):
            execution_file = self.root / 'execution.json'
            execution_file.write_text(json.dumps(self.execution))
            env.update(NABEDRA_OLTP_TEST_DATA_DIR=str(self.directory), NABEDRA_OLTP_TEST_PORT=str(port),
                       NABEDRA_OLTP_TEST_EXECUTION_FILE=str(execution_file))
            if cold_start:
                env['NABEDRA_OLTP_TEST_COLD'] = '1'
            else:
                for name in ('NABEDRA_OLTP_TEST_COLD',):
                    env.pop(name, None)
            if getattr(self, 'hold_direct', None):
                env['NABEDRA_OLTP_TEST_DIRECT_HOLD'] = self.hold_direct
            else:
                env.pop('NABEDRA_OLTP_TEST_DIRECT_HOLD', None)
            if getattr(self, 'barrier_stage', None):
                env.update(NABEDRA_OLTP_TEST_STAGE=self.barrier_stage,
                    NABEDRA_OLTP_TEST_REQUEST=self.barrier_request,
                    NABEDRA_OLTP_TEST_BARRIER_DIR=str(self.root))
            else:
                for name in ('NABEDRA_OLTP_TEST_STAGE', 'NABEDRA_OLTP_TEST_REQUEST', 'NABEDRA_OLTP_TEST_BARRIER_DIR'):
                    env.pop(name, None)
            command = [self.test_executable, '--exact', 'oltp_runtime::recovery_tests::actual_runtime_child',
                       '--ignored', '--nocapture']
        elif self.cli:
            config = dict(version=1, engine='oltp', bind=f'127.0.0.1:{port}', database=self.database,
                auth_file=str(protected), topology=dict(mode='local', data_dir=str(self.directory)),
                worker=dict(resources=self.resources, execution=self.execution, queue_size=8))
            if getattr(self, 'gateway_config', None):
                config.update(self.gateway_config)
            path = self.root / 'config.json'
            path.write_text(json.dumps(config))
            command = [sys.executable, '-m', 'nabedra.cli', 'database', 'serve', '--config', str(path)]
        else:
            settings = []
            for name, value in [('resources', self.resources), ('execution', self.execution)]:
                path = self.root / (name + '.json')
                path.write_text(json.dumps(value))
                settings += ['--' + name + '-config', str(path)]
            command = [self.binary, 'serve', '--data-dir', str(self.directory), '--database', self.database,
                       '--host', '127.0.0.1', '--port', str(port), '--workers', '4', *settings]
        if cold_start and not getattr(self, 'test_executable', None):
            command += ['--cold-start']
        self.process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=self.log)
        try:
            if self.cli:
                message = announcement(self.process)
                assert message.get('event') == 'nabedra.serve.launched', message
                self.url = message['bound_url']
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                assert self.process.poll() is None, 'actual runtime exited before readiness'
                try:
                    status, health = raw(self.url, '/health/ready', token=self.token)
                    if status == 200:
                        expected = 'api-gateway' if getattr(self, 'gateway_config', None) else 'typed-groups-rf1'
                        assert health['engine'] == 'oltp' and health['topology'] == expected, health
                        self.run_id = health['run_id']
                        self.children = child_pids(self.process.pid)
                        return self
                except OSError:
                    pass
                time.sleep(.025)
            raise TimeoutError('actual runtime readiness watchdog expired')
        except BaseException:
            self.stop()
            raise

    def stats(self):
        status, stats = raw(self.url, '/v1/runtime/stats', token=self.token)
        assert status == 200, stats
        return stats

    def stop(self, *, abrupt=False):
        if self.process is None:
            return
        process, self.process = self.process, None
        failure = None
        try:
            if process.poll() is None:
                self.children = list(set(self.children + child_pids(process.pid)))
                process.send_signal(signal.SIGKILL if abrupt else signal.SIGTERM)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                failure = 'actual runtime shutdown watchdog expired'
                for pid in self.children:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                process.kill()
                process.wait(timeout=5)
            if not reaped(self.children):
                failure = failure or 'runtime left an owned API/worker process alive'
                for pid in self.children:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                deadline = time.monotonic() + 5
                while not reaped(self.children) and time.monotonic() < deadline:
                    time.sleep(.02)
            assert reaped(self.children), 'owned runtime children survived forced cleanup'
            if failure:
                raise AssertionError(failure)
        finally:
            process.stdout.close()

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        try:
            self.stop()
        finally:
            self.log.close()


def retirement_and_committed_cold_replay(binary):
    with tempfile.TemporaryDirectory(prefix='nabedra-terminal-retirement-') as directory:
        server = RuntimeServer(binary, directory, short_retention=True)
        try:
            server.start()
            db = Database(url=server.url, token=server.token, database=server.database)
            create = dict(request_id=str(uuid.uuid4()), operation='table.create',
                payload=dict(name='durable', schema={'id': 'utf8', 'value': 'int64'}, key='id', format='parquet'))
            status, original = raw(server.url, '/v1/operations', create, token=server.token)
            assert status == 200, original
            table = db.table('durable')
            table.insert({'id': 'one', 'value': 7})
            read = dict(request_id=str(uuid.uuid4()), operation='table.get', payload=dict(table_id=table.id, key='one'))
            status, accepted = raw(server.url, '/v1/requests', read, token=server.token)
            assert status == 202, accepted
            # Use the actual queue stats to wait for terminal retirement, rather
            # than equating a sleep or response TTL with durable tombstone proof.
            for _ in range(30):
                assert table.get('one') == {'id': 'one', 'value': 7}
                time.sleep(.15)
            deadline = time.monotonic() + 10
            while server.stats()['queue']['accepted_records'] and time.monotonic() < deadline:
                time.sleep(.05)
            stats = server.stats()
            assert stats['queue']['accepted_records'] == 0, stats
            assert stats['queue']['charged_queue_bytes'] == stats['queue']['charged_result_bytes'] == 0, stats
            assert raw(server.url, '/v1/requests/' + accepted['run_id'] + '/' + read['request_id'], token=server.token)[0] == 404
            assert raw(server.url, '/v1/requests', read, token=server.token)[0] == 404
            assert table.get('one') == {'id': 'one', 'value': 7}
            previous = server.run_id
            server.stop(abrupt=True)
            server.start(cold_start=True)
            assert server.run_id != previous
            assert raw(server.url, '/v1/operations', create, token=server.token) == (200, original)
            db = Database(url=server.url, token=server.token, database=server.database)
            assert db.table('durable').get('one') == {'id': 'one', 'value': 7}
            assert raw(server.url, '/v1/requests', read, token=server.token)[0] == 404
        finally:
            server.stop()
            server.log.close()


def cli_supervisor(binary):
    with tempfile.TemporaryDirectory(prefix='nabedra-public-cli-') as directory:
        with RuntimeServer(binary, directory, cli=True) as server:
            db = Database(url=server.url, token=server.token, database=server.database)
            table = db.create_table('cli', {'id': str, 'value': int}, key='id')
            table.insert({'id': 'one', 'value': 1})
            assert table.get('one') == {'id': 'one', 'value': 1}
            assert raw(server.url, '/v1/runtime/stats', token='invalid')[0] == 401
            assert raw(server.url, '/v1/runtime/stats', token=server.token, database='other')[0] == 401
            assert server.stats()['engine'] == 'oltp'
            before = server.stats()['queue']['accepted_records']
            auth = json.loads((server.root / 'auth.json').read_text())
            api = dict(database='test', enrollment=dict(worker_id=str(uuid.uuid4()),
                incarnation=str(uuid.uuid4()), certificate_sha256='ab' * 32,
                endpoint='127.0.0.1:7999', server_name='api.test', enrollment_revision=1))
            enrollment = dict(request_id=str(uuid.uuid4()), operation='api.enroll',
                              payload=dict(expected=None, next=api))
            assert raw(server.url, '/v1/cluster/operations', enrollment, token=server.token)[0] == 401
            status, result = raw(server.url, '/v1/cluster/operations', enrollment, token=auth['admin_token'])
            assert status == 200 and result['data']['service_role'] == 'api_ingress', result
            assert result['data']['identity']['worker_id'] == api['enrollment']['worker_id'], result
            retry_status, retry = raw(server.url, '/v1/cluster/operations', enrollment, token=auth['admin_token'])
            assert retry_status == status and retry['data']['identity'] == result['data']['identity'], retry
            assert retry['data']['service_role'] == result['data']['service_role'], retry
            # A duplicate metadata proposal has a new applied log cut, while
            # its immutable enrollment and role remain identical.
            assert retry['data']['applied_at']['index'] >= result['data']['applied_at']['index'], retry
            assert server.stats()['queue']['accepted_records'] == before



@contextmanager
def retained_failure_directory(prefix):
    import shutil
    directory = tempfile.mkdtemp(prefix=prefix)
    try:
        yield directory
    except BaseException:
        print(json.dumps(dict(failure_directory=directory)), file=sys.stderr, flush=True)
        raise
    else:
        shutil.rmtree(directory)


def gateway_process(binary):
    import hashlib
    import shutil
    from oltp_orm_contract import exercise
    def port():
        with socket.socket() as bound:
            bound.bind(('127.0.0.1', 0))
            return bound.getsockname()[1]
    def openssl(root, *args):
        result = subprocess.run(['openssl', *map(str, args)], cwd=root,
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
        assert result.returncode == 0, result.stderr[-8192:].decode(errors='replace')
        return result.stdout
    with retained_failure_directory('nabedra-gateway-process-') as temporary:
        root = Path(temporary)
        worker_root, gateway_root = root / 'worker', root / 'api'
        worker_root.mkdir()
        gateway_root.mkdir()
        worker = RuntimeServer(binary, worker_root, cli=True)
        gateway = None
        try:
            worker.start()
            auth = json.loads((worker_root / 'auth.json').read_text())
            status, connected = raw(worker.url, '/v1/cluster/operations',
                dict(request_id=str(uuid.uuid4()), operation='cluster.connection', payload={}), token=auth['admin_token'])
            assert status == 200, connected
            connection = connected['data']
            enrolled_worker = connection['control_peers'][0]
            ca_root = worker.directory / 'internal-tls' / enrolled_worker['incarnation']
            # The operator signs an API CSR; its private key is generated here.
            # Neither worker.key nor any live storage file is copied/modified.
            openssl(gateway_root, 'req', '-new', '-newkey', 'rsa:2048', '-nodes',
                '-keyout', 'api.key', '-out', 'api.csr', '-subj', '/CN=nabedra-api')
            (gateway_root / 'api.ext').write_text('subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n')
            openssl(gateway_root, 'x509', '-req', '-in', 'api.csr', '-CA', ca_root / 'ca.pem',
                '-CAkey', ca_root / 'ca.key', '-set_serial', uuid.uuid4().int,
                '-out', 'api.pem', '-days', '1', '-extfile', 'api.ext')
            (gateway_root / 'api.key').chmod(0o600)
            shutil.copyfile(ca_root / 'ca.pem', gateway_root / 'ca.pem')
            fingerprint = hashlib.sha256(openssl(gateway_root, 'x509', '-in', 'api.pem', '-outform', 'DER')).hexdigest()
            api = dict(database='test', enrollment=dict(worker_id=str(uuid.uuid4()), incarnation=str(uuid.uuid4()),
                certificate_sha256=fingerprint, endpoint=f'127.0.0.1:{port()}', server_name='localhost', enrollment_revision=1))
            enroll = dict(request_id=str(uuid.uuid4()), operation='api.enroll', payload=dict(expected=None, next=api))
            assert raw(worker.url, '/v1/cluster/operations', enroll, token=worker.token)[0] == 401
            status, accepted = raw(worker.url, '/v1/cluster/operations', enroll, token=auth['admin_token'])
            assert status == 200, accepted
            gateway = RuntimeServer(binary, gateway_root, cli=True)
            gateway.directory.rmdir()
            gateway.token = worker.token
            gateway.gateway_config = dict(topology=dict(mode='remote', cluster_id=connection['cluster_id'],
                control_group_id=connection['control_group_id'], enrollment=api,
                control_peers=connection['control_peers'], worker_tls=dict(cert_file=str(gateway_root / 'api.pem'),
                    key_file=str(gateway_root / 'api.key'), ca_file=str(gateway_root / 'ca.pem'))))
            gateway.start()
            assert gateway.run_id == worker.run_id
            gateway_stats = gateway.stats()
            assert gateway_stats['queue']['accepted_records'] == 0, gateway_stats
            db = Database(url=gateway.url, token=worker.token, database='test', timeout=20)
            table = db.create_table('gateway', {'id': str, 'value': int}, key='id')
            table.insert({'id': 'one', 'value': 7})
            assert table.get('one') == {'id': 'one', 'value': 7}
            first = db.submit('table.get', dict(table_id=table.id, key='one')).wait(timeout=20)
            assert first.result == {'id': 'one', 'value': 7}
            print(json.dumps(dict(variation='real-separate-api-first-crud-receipt', status='passed',
                worker_process=worker.process.pid, api_process=gateway.process.pid)), flush=True)
            from unittest.mock import patch
            from nabedra.client import _Transport
            original_request = _Transport.request
            def traced_request(transport, operation, payload, **kwargs):
                began = time.monotonic()
                print(json.dumps(dict(operation=operation, phase='begin', table_id=payload.get('table_id'))), flush=True)
                try:
                    result = original_request(transport, operation, payload, **kwargs)
                except BaseException as error:
                    print(json.dumps(dict(operation=operation, phase='error', elapsed=time.monotonic()-began,
                        error=type(error).__name__, request_id=getattr(error, 'request_id', None))), flush=True)
                    raise
                print(json.dumps(dict(operation=operation, phase='end', elapsed=time.monotonic()-began)), flush=True)
                return result
            with patch.object(_Transport, 'request', traced_request):
                exercise(gateway.url, worker.token, 'test', unauthorized_database='other')
            pending = db.submit('table.get', dict(table_id=table.id, key='one'))
            previous_run = gateway.run_id
            native_api = child_pids(gateway.process.pid)
            assert len(native_api) == 1, native_api
            os.kill(native_api[0], signal.SIGKILL)
            gateway.process.wait(timeout=20)
            gateway.stop()
            assert worker.process.poll() is None
            old_identity = accepted['data']['identity']
            api['enrollment'] = dict(api['enrollment'], incarnation=str(uuid.uuid4()), enrollment_revision=2)
            status, rotated = raw(worker.url, '/v1/cluster/operations', dict(request_id=str(uuid.uuid4()),
                operation='api.enroll', payload=dict(expected=old_identity, next=api)), token=auth['admin_token'])
            assert status == 200, rotated
            direct = Database(url=worker.url, token=worker.token, database='test', timeout=20)
            direct.table('gateway').update('one', {'value': 9})
            gateway.start()
            assert gateway.run_id == previous_run == worker.run_id
            restarted = Database(url=gateway.url, token=worker.token, database='test', timeout=20)
            recovered = restarted.receipt(pending.run_id, pending.request_id).wait(timeout=20)
            assert recovered.result == {'id': 'one', 'value': 7}, recovered.result
            assert restarted.table('gateway').get('one') == {'id': 'one', 'value': 9}
            assert not list(gateway_root.rglob('CURRENT')), 'API opened a local data pool'
            print(json.dumps(dict(variation='api-only-kill-enrollment-cas-restart', status='passed',
                same_worker_run=True, original_result=True, api_has_data_pool=False)), flush=True)
        except BaseException:
            for server in [gateway, worker]:
                if server:
                    server.log.flush()
                    print(json.dumps(dict(server=str(server.root), process_exit=server.process.poll() if server.process else None,
                        native_children=child_pids(server.process.pid) if server.process and server.process.poll() is None else [])), file=sys.stderr, flush=True)
                    try:
                        print(json.dumps(dict(server=str(server.root), stats=server.stats())), file=sys.stderr, flush=True)
                    except BaseException as error:
                        print(f'Runtime stats unavailable: {type(error).__name__}: {error}', file=sys.stderr, flush=True)
                    with (server.root / 'server.log').open('rb') as diagnostics:
                        diagnostics.seek(0, 2)
                        diagnostics.seek(max(0, diagnostics.tell() - 8192))
                        print(diagnostics.read().decode(errors='replace'), file=sys.stderr)
            raise
        finally:
            if gateway:
                gateway.stop()
                gateway.log.close()
            worker.stop()
            worker.log.close()


def snapshot_log_pressure(binary):
    # Same native invocation as O01, including --workers4 and --queue-size8.
    output = Path(os.environ.get('NABEDRA_OLTP_ACCEPTANCE_OUTPUT', tempfile.gettempdir()))
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='nabedra-o01-log-pressure-') as directory:
        server = Server(str(Path(binary).resolve()), directory, diagnostics=output / 'oltp-log-pressure.log')
        try:
            server.start()
            db = Database(url=server.url, token='test-token', database='test', timeout=20)
            table = db.create_table('log_pressure', {'id': str, 'value': int}, key='id')
            for number in range(200):
                table.insert({'id': str(number), 'value': number})
                assert table.delete(str(number)) is True
            assert table.query().count() == 0
            table.drop()
            assert db.tables() == []
            status, stats = raw(server.url, '/v1/runtime/stats')
            assert status == 200, stats
            assert stats['queue']['queued_records'] == stats['queue']['running_records'] == stats['queue']['reconciling_records'] == 0, stats
            assert stats['queue']['accepted_records'] == stats['queue']['terminal_records'], stats
            assert 0 <= stats['queue']['charged_queue_bytes'] <= 256 << 20, stats
            assert 0 <= stats['queue']['charged_result_bytes'] <= 64 << 20, stats
            deadline = time.monotonic() + 5
            while any(threads for _, threads, _ in stats['scheduler']) and time.monotonic() < deadline:
                time.sleep(.02)
                status, stats = raw(server.url, '/v1/runtime/stats')
                assert status == 200
            assert all(threads == 0 for _, threads, _ in stats['scheduler']), stats
            assert raw(server.url, '/health/ready')[0] == 200
        finally:
            server.stop()
    print(json.dumps(dict(variation='bounded-log-snapshot-pressure', status='passed',
        mutation_pairs=200, queued_or_running=0, scope='actual-small-queue-rf1')), flush=True)


def preaccept_disconnect(test_executable):
    import http.client
    with tempfile.TemporaryDirectory(prefix='nabedra-o13-preaccept-') as directory:
        server = RuntimeServer(test_executable, directory)
        server.test_executable = str(Path(test_executable).resolve())
        server.execution['max_queue_bytes_per_replica'] = 1
        server.barrier_stage = 'preaccept'
        server.barrier_request = str(uuid.uuid4())
        connection = None
        try:
            server.start()
            operation = dict(request_id=server.barrier_request, operation='table.list', payload={})
            connection = http.client.HTTPConnection('127.0.0.1', int(server.url.rsplit(':', 1)[1]), timeout=10)
            connection.request('POST', '/v1/requests', json.dumps(operation, separators=(',', ':')),
                {'Authorization': 'Bearer ' + server.token, 'X-Nabedra-Database': 'test', 'Content-Type': 'application/json'})
            deadline = time.monotonic() + 15
            while not (server.root / 'reached').exists() and time.monotonic() < deadline:
                assert server.process.poll() is None
                time.sleep(.01)
            assert (server.root / 'reached').read_text() == 'preaccept'
            # No 202 was received. The actual ingress owns admission after this
            # client vanishes, including its durable reservation and active pin.
            connection.close()
            connection = None
            (server.root / 'release').write_text('release')
            deadline = time.monotonic() + 15
            while not (server.root / 'settled').exists() and time.monotonic() < deadline:
                time.sleep(.01)
            assert (server.root / 'settled').exists(), 'durable rejection plus exact pin cancellation did not settle'
            status, rejection = raw(server.url, '/v1/requests', operation, token=server.token)
            assert status == 429 and rejection['error']['outcome'] == 'not_applied', rejection
            assert raw(server.url, '/v1/requests', operation, token=server.token) == (status, rejection)
            for _ in range(8):
                later = dict(operation, request_id=str(uuid.uuid4()))
                status, rejected = raw(server.url, '/v1/requests', later, token=server.token)
                assert status == 429 and rejected['error']['outcome'] == 'not_applied', rejected
            stats = server.stats()
            assert stats['queue']['accepted_records'] == 0 and stats['queue']['charged_queue_bytes'] == 0, stats
            print(json.dumps(dict(variation='disconnected-before-202-definitive-overload', status='passed',
                scope='actual-status-rejection-tso-cancel-owned-ingress')), flush=True)
        except BaseException:
            server.log.flush()
            with (server.root / 'server.log').open('rb') as diagnostics:
                diagnostics.seek(0, 2)
                diagnostics.seek(max(0, diagnostics.tell() - 8192))
                print(diagnostics.read().decode(errors='replace'), file=sys.stderr)
            raise
        finally:
            if connection:
                connection.close()
            server.stop()
            server.log.close()


def tentative_copy_disconnect(test_executable):
    import http.client
    with tempfile.TemporaryDirectory(prefix='nabedra-q03-staged-copy-') as directory:
        server = RuntimeServer(test_executable, directory)
        server.test_executable = str(Path(test_executable).resolve())
        server.execution.update(max_pending_requests=1, result_retention_ms=3000)
        server.barrier_stage = 'staged-copy-refused'
        identity = server.barrier_request = str(uuid.uuid4())
        operation = dict(request_id=identity, operation='table.list', payload={})
        body = json.dumps(operation, separators=(',', ':'))
        connection = None
        try:
            server.start()
            baseline = server.stats()['queue']['charged_queue_bytes']
            connection = http.client.HTTPConnection('127.0.0.1', int(server.url.rsplit(':', 1)[1]), timeout=10)
            connection.request('POST', '/v1/requests', body,
                {'Authorization': 'Bearer ' + server.token, 'X-Nabedra-Database': 'test', 'Content-Type': 'application/json'})
            marker = server.root / 'reached'
            deadline = time.monotonic() + 10
            while not marker.exists() and time.monotonic() < deadline:
                assert server.process.poll() is None
                time.sleep(.01)
            assert marker.read_text() == server.barrier_stage
            stats = server.stats()['queue']
            assert stats['queued_records'] == 1 and stats['accepted_records'] == 0, stats
            assert stats['charged_queue_bytes'] > baseline, stats
            record_file = server.root / f'record-{identity}.json'
            original = json.loads(record_file.read_text())
            assert original['body'] == body and original['accepted'] is False, original
            later = dict(operation, request_id=str(uuid.uuid4()))
            status, overflow = raw(server.url, '/v1/requests', later, token=server.token)
            assert status == 429 and overflow['error']['outcome'] == 'not_applied', overflow
            connection.close()
            connection = None
            (server.root / 'release').write_text('release')

            receipt_path = f'/v1/requests/{server.run_id}/{identity}'
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                status, receipt = raw(server.url, receipt_path, token=server.token)
                if status == 200 and receipt['state'] == 'succeeded':
                    break
                assert status in (200, 503), (status, receipt)
                time.sleep(.01)
            assert status == 200 and receipt == dict(request_id=identity, run_id=server.run_id,
                state='succeeded', result=[], error=None), receipt
            stats = server.stats()['queue']
            assert stats['terminal_records'] == stats['accepted_records'] == 1, stats
            recovered = json.loads(record_file.read_text())
            assert recovered['accepted'] is True and recovered['state'] == 'succeeded', recovered
            for field in ('run_id', 'sequence', 'body', 'digest', 'context'):
                assert recovered[field] == original[field], (field, original, recovered)

            deadline = time.monotonic() + 10
            while stats['charged_queue_bytes'] != baseline and time.monotonic() < deadline:
                time.sleep(.02)
                stats = server.stats()['queue']
            assert stats['charged_queue_bytes'] == baseline and stats['charged_result_bytes'] == 0, stats
            status, expired = raw(server.url, receipt_path, token=server.token)
            assert status == 404 and 'expired' in expired['error']['message'], expired
            status, fresh = raw(server.url, '/v1/operations',
                dict(operation, request_id=str(uuid.uuid4())), token=server.token)
            assert status == 200 and fresh['data'] == [], fresh
            print(json.dumps(dict(variation='staged-copy-disconnect-recovers-original', status='passed',
                cases=['O13', 'Q02', 'Q03', 'Q06'], scope='actual-native-rf1-injected-copy-refusal')), flush=True)
        finally:
            if connection:
                connection.close()
            (server.root / 'release').touch(exist_ok=True)
            server.stop()
            server.log.close()


def failure_boundaries(test_executable):
    for stage, kind in [('queued', 'mutation'), ('queued', 'read'), ('prepared', 'mutation'), ('committed', 'mutation')]:
        with tempfile.TemporaryDirectory(prefix='nabedra-o15-' + stage + '-') as directory:
            server = RuntimeServer(test_executable, directory)
            server.test_executable = str(Path(test_executable).resolve())
            server.barrier_stage = stage
            server.barrier_request = str(uuid.uuid4())
            try:
                server.start()
                db = Database(url=server.url, token=server.token, database='test', timeout=20)
                table = db.create_table('recovery', {'id': str, 'value': int}, key='id')
                table.insert({'id': 'seed', 'value': 1})
                operation = dict(request_id=server.barrier_request,
                    operation='table.insert' if kind == 'mutation' else 'table.get',
                    payload=dict(table_id=table.id, **({'row': {'id': 'target', 'value': 7}} if kind == 'mutation' else {'key': 'target'})))
                status, accepted = raw(server.url, '/v1/requests', operation, token=server.token)
                assert status == 202 and accepted['run_id'] == server.run_id, accepted
                deadline = time.monotonic() + 20
                marker = server.root / 'reached'
                while not marker.exists() and time.monotonic() < deadline:
                    assert server.process.poll() is None, 'server died before transition barrier'
                    time.sleep(.01)
                assert marker.read_text() == stage, 'real transition barrier was not reached'
                old_run = server.run_id
                status, before = raw(server.url, '/v1/requests/' + old_run + '/' + operation['request_id'], token=server.token)
                assert status == 200, before
                assert before['state'] == ('queued' if stage == 'queued' else 'running'), before
                server.stop(abrupt=True)
                server.barrier_stage = None
                server.start(cold_start=True)
                assert server.run_id != old_run
                db = Database(url=server.url, token=server.token, database='test', timeout=20)
                table = db.table('recovery')
                direct = None
                if kind == 'read':
                    # User decision 2026-11-21 (RV-FINAL M3): a get has no
                    # exactly-once identity and leaves no forward marker.
                    # Wait for the capability, so the retry's route is known.
                    direct = wait_direct_active(server)['direct_oltp']
                    deadline = time.monotonic() + 30
                    while not server.stats().get('direct') and time.monotonic() < deadline:
                        time.sleep(.05)
                    assert server.stats().get('direct'), 'direct executor not installed'
                status, retried = raw(server.url, '/v1/operations', operation, token=server.token)
                if kind == 'read' and direct:
                    # The retry runs directly: a new linearizable read. The
                    # old receipt still reports the queued read abandoned.
                    assert (status, retried) == (200, {'request_id': operation['request_id'], 'data': None}), retried
                    status, receipt = raw(server.url, '/v1/requests/' + old_run + '/' + operation['request_id'],
                                          token=server.token)
                    assert status == 404 and receipt['error']['outcome'] == 'unknown', receipt
                    assert 'abandon' in receipt['error']['message'], receipt
                    assert table.get('target') is None
                    assert table.query().count() == 1
                    table.insert({'id': 'target', 'value': 9})
                    # The queued path still knows the abandoned id.
                    delayed = raw(server.url, '/v1/requests', operation, token=server.token)
                    assert delayed[0] == 404, delayed
                    # Reads again, never replays: the new row is visible.
                    status, again = raw(server.url, '/v1/operations', operation, token=server.token)
                    assert (status, again['data']) == (200, {'id': 'target', 'value': 9}), again
                    assert (server.stats().get('direct') or {}).get('forward_markers_written', 0) == 0
                elif stage == 'committed':
                    assert (status, retried) == (200, {'request_id': operation['request_id'], 'data': None}), retried
                    status, receipt = raw(server.url, '/v1/requests/' + old_run + '/' + operation['request_id'], token=server.token)
                    assert status == 200 and receipt['state'] == 'succeeded' and receipt.get('result') is None, receipt
                    assert table.get('target') == {'id': 'target', 'value': 7}
                    assert table.query().count() == 2
                else:
                    assert status == 404 and retried['error']['outcome'] == 'unknown', retried
                    assert 'abandon' in retried['error']['message'], retried
                    assert table.get('target') is None
                    assert table.query().count() == 1
                    table.insert({'id': 'target', 'value': 9})
                    # The delayed old UUID cannot observe/reexecute the new row.
                    delayed = raw(server.url, '/v1/requests', operation, token=server.token)
                    assert delayed[0] == 404, delayed
                    assert table.get('target') == {'id': 'target', 'value': 9}
                    assert table.query().count() == 2
                # Exclusive lifecycle acquisition must settle and remove the
                # old prepared/committed holder using authoritative Status.
                table.drop()
                assert db.tables() == []
                print(json.dumps(dict(variation=stage + '-' + kind, status='passed',
                    scope='actual-run-http-native-rf1', child_reaped=True,
                    **({'direct_oltp': direct} if kind == 'read' else {}))), flush=True)
            except BaseException:
                server.log.flush()
                with (server.root / 'server.log').open('rb') as diagnostics:
                    diagnostics.seek(0, 2)
                    diagnostics.seek(max(0, diagnostics.tell() - 8192))
                    print(diagnostics.read().decode(errors='replace'), file=sys.stderr)
                raise
            finally:
                server.stop()
                server.log.close()


def wait_direct_active(server):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        capability = server.stats()['direct_capability']
        if capability['active']:
            return capability
        assert server.process.poll() is None, 'server died before data.direct.v1 activated'
        time.sleep(.1)
    raise TimeoutError('data.direct.v1 did not activate')


def activation_boundary(test_executable):
    """T7c (O15, D03): eligible mutations a queue owner reserved before
    data.direct.v1 activated carry no forward marker. Control records that
    queued history when the owner reserves them, and the activation opens a
    window in which only the queued path answers new mutation ids, so:
    - a request queued before activation and still in flight at activation,
      retried with the same id after it, executes once and returns the
      original result; the same id with another body conflicts;
    - a request abandoned before activation by a cold start, retried after
      activation, answers 404 abandoned and never executes.
    Activation is held by a test-only file until the request is queued."""
    for variation in ('in-flight', 'abandoned'):
        with tempfile.TemporaryDirectory(prefix='nabedra-o15-boundary-' + variation + '-') as directory:
            server = RuntimeServer(test_executable, directory)
            server.test_executable = str(Path(test_executable).resolve())
            hold = server.root / 'hold-direct'
            hold.write_text('held')
            server.hold_direct = str(hold)
            server.barrier_stage = 'queued'
            server.barrier_request = str(uuid.uuid4())
            try:
                server.start()
                db = Database(url=server.url, token=server.token, database='test', timeout=20)
                table = db.create_table('boundary', {'id': str, 'value': int}, key='id')
                # Eligible, before activation: the owner records queued history.
                table.insert({'id': 'seed', 'value': 1})
                operation = dict(request_id=server.barrier_request, operation='table.insert',
                    payload=dict(table_id=table.id, row={'id': 'target', 'value': 7}))
                status, accepted = raw(server.url, '/v1/requests', operation, token=server.token)
                assert status == 202 and accepted['run_id'] == server.run_id, accepted
                deadline = time.monotonic() + 20
                marker = server.root / 'reached'
                while not marker.exists() and time.monotonic() < deadline:
                    assert server.process.poll() is None, 'server died before the queued barrier'
                    time.sleep(.01)
                assert marker.read_text() == 'queued', 'queued barrier was not reached'
                assert not server.stats()['direct_capability']['active'], 'activated before the request was queued'
                old_run = server.run_id
                hold.unlink()
                if variation == 'abandoned':
                    server.stop(abrupt=True)
                    server.barrier_stage = None
                    server.start(cold_start=True)
                    assert server.run_id != old_run
                capability = wait_direct_active(server)
                assert capability['history_settling'], capability
                db = Database(url=server.url, token=server.token, database='test', timeout=20)
                table = db.table('boundary')
                if variation == 'abandoned':
                    status, retried = raw(server.url, '/v1/operations', operation, token=server.token)
                    assert status == 404 and retried['error']['outcome'] == 'unknown', retried
                    assert 'abandon' in retried['error']['message'], retried
                    assert table.get('target') is None
                    assert table.query().count() == 1
                    table.insert({'id': 'target', 'value': 9})
                    delayed = raw(server.url, '/v1/requests', operation, token=server.token)
                    assert delayed[0] == 404, delayed
                    status, again = raw(server.url, '/v1/operations', operation, token=server.token)
                    assert status == 404 and 'abandon' in again['error']['message'], again
                    assert table.get('target') == {'id': 'target', 'value': 9}
                    assert table.query().count() == 2
                else:
                    # The original is still held in flight; its retry joins it.
                    results = {}
                    retry = threading.Thread(target=lambda: results.update(
                        reply=raw(server.url, '/v1/operations', operation, token=server.token, timeout=60)))
                    retry.start()
                    time.sleep(1)
                    (server.root / 'release').write_text('release')
                    retry.join(90)
                    assert not retry.is_alive(), 'retry did not finish'
                    assert results['reply'] == (200, {'request_id': operation['request_id'], 'data': None}), results
                    status, receipt = raw(server.url, '/v1/requests/' + old_run + '/' + operation['request_id'],
                                          token=server.token)
                    assert status == 200 and receipt['state'] == 'succeeded', receipt
                    other = dict(operation, payload=dict(table_id=table.id, row={'id': 'other', 'value': 1}))
                    status, conflict = raw(server.url, '/v1/operations', other, token=server.token)
                    assert status == 409 and conflict['error']['code'] == 'conflict', conflict
                    assert table.get('target') == {'id': 'target', 'value': 7}
                    assert table.get('other') is None
                    assert table.query().count() == 2
                direct = server.stats().get('direct') or {}
                # Nothing of this history ran directly.
                assert direct.get('entries_proposed', 0) == 0, direct
                print(json.dumps(dict(variation='activation-boundary-' + variation, status='passed',
                    scope='actual-run-http-native-rf1', fallbacks=direct.get('fallbacks'),
                    markers=direct.get('forward_markers_written'))), flush=True)
            except BaseException:
                server.log.flush()
                with (server.root / 'server.log').open('rb') as diagnostics:
                    diagnostics.seek(0, 2)
                    diagnostics.seek(max(0, diagnostics.tell() - 8192))
                    print(diagnostics.read().decode(errors='replace'), file=sys.stderr)
                raise
            finally:
                server.stop()
                server.log.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary')
    parser.add_argument('--test-executable')
    parser.add_argument('--scenario', choices=('retirement-cold', 'cli', 'failure-boundaries', 'activation-boundary', 'admission', 'snapshot-pressure', 'gateway'), required=True)
    args = parser.parse_args()
    if args.scenario == 'gateway':
        if not args.binary:
            parser.error('--binary is required')
        gateway_process(args.binary)
    elif args.scenario == 'snapshot-pressure':
        if not args.binary:
            parser.error('--binary is required')
        snapshot_log_pressure(args.binary)
    elif args.scenario == 'admission':
        if not args.test_executable or not args.binary:
            parser.error('--test-executable and --binary are required')
        preaccept_disconnect(args.test_executable)
        tentative_copy_disconnect(args.test_executable)
        retirement_and_committed_cold_replay(args.binary)
        cli_supervisor(args.binary)
        print('NABEDRA_OLTP_ACCEPTANCE ' + json.dumps(dict(scenario='O13', status='not-run',
            reason='preaccept disconnect, definitive overload, retirement and CLI paths executed; CPU/count/disk-headroom saturation, ambiguous reservation replies and retained-reply fairness remain unexecuted in this scenario')), flush=True)
    elif args.scenario == 'failure-boundaries':
        if not args.test_executable:
            parser.error('--test-executable is required for actual process barriers')
        failure_boundaries(args.test_executable)
        activation_boundary(args.test_executable)
        print('NABEDRA_OLTP_ACCEPTANCE ' + json.dumps(dict(scenario='O15', status='not-run',
            reason='core RF1 queue/prepare/COMMIT histories passed; allocator/Status/shard fence-interruption and decision-order variations remain unexecuted')), flush=True)
    elif args.scenario == 'activation-boundary':
        if not args.test_executable:
            parser.error('--test-executable is required for actual process barriers')
        activation_boundary(args.test_executable)
    elif not args.binary:
        parser.error('--binary is required')
    elif args.scenario == 'cli':
        cli_supervisor(args.binary)
    else:
        retirement_and_committed_cold_replay(args.binary)
    print(json.dumps(dict(scenario=args.scenario, status='passed', scope='actual-local-rf1-subset')))

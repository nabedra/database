#!/usr/bin/env python3.11
"""Actual OLTP API admission, terminal retirement and process recovery checks.

These use the installed server path and actual Control/TSO/Status services.
Deterministic pending/Prepare/ambiguous kill barriers are separate required O15
variations; a completed COMMIT replay alone does not certify O15.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, closing, contextmanager
import argparse
import hashlib
import http.client
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
from oltp_direct_cluster import Http, Rf3Cluster, WORKERS, call, operation, write_json
from oltp_process import Server, raw, terminal


DIRECT = os.environ.get('NABEDRA_TEST_DIRECT_OLTP') != 'off'
TRANSPORT_ERRORS = (OSError, http.client.HTTPException)


def assertion(scenario, clause, detail, status='passed'):
    print('NABEDRA_OLTP_ASSERT ' + json.dumps(
        dict(scenario=scenario, assertion=clause, status=status, detail=detail)), flush=True)


def incomplete(scenario, reason):
    print('NABEDRA_OLTP_ACCEPTANCE ' + json.dumps(
        dict(scenario=scenario, status='not-run', reason=reason)), flush=True)


def wait_until(observe, description, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = observe()
        if result:
            return result
        time.sleep(.05)
    raise TimeoutError(description)


def receipt_path(run, request):
    return f"/v1/requests/{run}/{request['request_id']}"


def assert_accepted(reply, run, request):
    assert reply == (202, dict(request_id=request['request_id'], run_id=run, state='queued')), reply


def assert_unknown(reply, request):
    status, body = reply
    assert status == 503 and body['request_id'] == request['request_id'], reply
    assert body['error']['outcome'] == 'unknown', reply


def assert_abandoned(reply, request):
    status, body = reply
    assert status == 404 and body['request_id'] == request['request_id'], reply
    assert body['error']['outcome'] == 'unknown' and 'abandon' in body['error']['message'], reply


def exact_receipt(exchange, run, request, result, timeout=60):
    expected = dict(request_id=request['request_id'], run_id=run,
                    state='succeeded', result=result, error=None)

    def observe():
        try:
            status, body = exchange(receipt_path(run, request))
        except TRANSPORT_ERRORS:
            return None
        if status == 200:
            if body['state'] in ('succeeded', 'failed'):
                assert body == expected, body
                return body
            assert body['state'] in ('queued', 'running', 'committing', 'reconciling'), body
            assert body == dict(expected, state=body['state'], result=None), body
        else:
            assert_unknown((status, body), request)
        return None

    return wait_until(observe, f"terminal receipt {request['request_id']}", timeout)


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
            queue_replication='all_ready_workers', queue_lifetime='live_cluster', partition_size='64 MiB',
            direct_oltp=DIRECT)
        self.worker_count = 1
        self.log = (self.root / 'server.log').open('a+b')

    def start(self, *, cold_start=False, wait_ready=True):
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
                       NABEDRA_OLTP_TEST_EXECUTION_FILE=str(execution_file),
                       NABEDRA_OLTP_TEST_BARRIER_DIR=str(self.root))
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
                    NABEDRA_OLTP_TEST_REQUEST=self.barrier_request)
            else:
                for name in ('NABEDRA_OLTP_TEST_STAGE', 'NABEDRA_OLTP_TEST_REQUEST'):
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
        if not wait_ready:
            return self
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


@contextmanager
def native_cluster(binary, test_executable, label, *, mounts=False):
    with tempfile.TemporaryDirectory(prefix=f'nabedra-{label}-') as directory:
        cluster = Rf3Cluster(binary, directory, test_executable=test_executable,
            direct=dict.fromkeys(WORKERS, DIRECT), api_direct=(DIRECT, DIRECT), mounts=mounts)
        try:
            yield cluster.start()
        except BaseException:
            for name in (*cluster.workers, *cluster.apis):
                print(f'{name}\n{cluster.tail(name)}', file=sys.stderr)
            raise
        finally:
            cluster.close()


def gateway(cluster, name='api1', timeout=30):
    return Http(cluster.api_urls[name], cluster.auth['data_token'], timeout, cluster.ca)


def worker(cluster, name, path, timeout=5):
    return call(cluster.urls[name], path, token=cluster.auth['data_token'], timeout=timeout)


def gates(directory, requests):
    write_json(directory / 'gates.json', {req['request_id']: stage for req, stage in requests})
    write_json(directory / 'inspect.json', [req['request_id'] for req, _ in requests])


def wait_reached(directories, request, stage):
    def observe():
        for directory in directories:
            marker = directory / f"reached-{request['request_id']}"
            if marker.exists() and marker.read_text() == stage:
                return directory
        return None

    return wait_until(observe, f"{request['request_id']} did not reach {stage}")


def release(directory, request):
    (directory / f"release-{request['request_id']}").touch()


def read_record(cluster, name, request):
    path = cluster.barrier_dir(name) / f"record-{request['request_id']}.json"
    path.unlink(missing_ok=True)
    cluster.stats(name)
    return json.loads(path.read_text())


def assert_record(record, run, request):
    body = json.dumps(request, separators=(',', ':'))
    assert record['request_id'] == request['request_id'] and record['run_id'] == run, record
    assert record['body'] == body and record['body_hash'] == hashlib.sha256(body.encode()).hexdigest(), record
    assert record['accepted'] is True, record
    context = record['context']
    assert context['read']['storage_pin']['read_ts'] > 0, context
    assert context['read']['schema_epoch'] > 0 and context['read']['catalog_root'], context
    assert context['authorization']['database'] == 'test', context
    assert context['authorization']['policy_revision'] > 0, context
    return tuple(record[field] for field in
                 ('request_id', 'run_id', 'body', 'body_hash', 'digest', 'context', 'context_hash'))


def same_run_ready(cluster, run, names, count):
    def observe():
        replies = []
        for name in names:
            try:
                status, ready = worker(cluster, name, '/health/ready')
            except TRANSPORT_ERRORS:
                return None
            if status != 200:
                return None
            assert ready['run_id'] == run, ready
            replies.append(ready['ready_workers'])
        return replies == [count] * len(names)

    wait_until(observe, f'workers {names} did not regain same-run ready count {count}', timeout=110)


def launch_retained(cluster, name):
    cluster.workers[name].stop()
    cluster.launch_worker(name)


def queue_survival(binary, test_executable):
    with native_cluster(binary, test_executable, 'o14') as cluster:
        run = cluster.run_id
        directories = [cluster.barrier_dir(name) for name in WORKERS]
        db = Database(cluster.api_urls['api1'], cluster.auth['data_token'], database='test',
                      ca_file=str(cluster.ca), timeout=30)
        table = db.create_table('same_run', {'id': str, 'value': int}, key='id')
        original_row = dict(id='seed', value=1)
        table.insert(original_row)
        read = operation('table.get', dict(table_id=table.id, key='seed'))
        mutation = operation('table.insert', dict(table_id=table.id, row=dict(id='once', value=3)))
        for directory in directories:
            gates(directory, [(read, 'queued'), (mutation, 'committed')])

        with closing(gateway(cluster)) as first, closing(gateway(cluster, 'api2')) as second:
            assert_accepted(first.exchange('/v1/requests', read), run, read)
            owner_dir = wait_reached(directories, read, 'queued')
            owner = owner_dir.name
            assert table.update('seed', {'value': 2}) is True
            assert_accepted(first.exchange('/v1/requests', mutation), run, mutation)
            assert wait_reached(directories, mutation, 'committed') == owner_dir
            originals = {request['request_id']: assert_record(read_record(cluster, owner, request), run, request)
                         for request in (read, mutation)}
            for name in WORKERS:
                for request in (read, mutation):
                    record = read_record(cluster, name, request)
                    assert assert_record(record, run, request) == originals[request['request_id']], record
                    expected = 'queued' if request is read else 'running'
                    assert record['state'] == expected, record

            assert second.exchange(receipt_path(run, read))[1]['state'] == 'queued'
            cluster.kill_api('api1')
            assert second.exchange(receipt_path(run, read)) == (200, dict(
                request_id=read['request_id'], run_id=run, state='queued', result=None, error=None))
        cluster.kill_api('api2')
        assertion('O14', 'api-loss', 'Both enrolled gateways stopped after 202. The second gateway observed the first gateway’s original queued receipt.')

        removed, survivor = [name for name in WORKERS if name != owner]
        cluster.workers[removed].kill()
        same_run_ready(cluster, run, (owner, survivor), 2)
        cluster.workers[owner].kill()
        for request in (read, mutation):
            record = read_record(cluster, survivor, request)
            assert assert_record(record, run, request) == originals[request['request_id']], record
            assert record['state'] in ('queued', 'running'), record
            assert_unknown(worker(cluster, survivor, receipt_path(run, request), timeout=25), request)
        assertion('O14', 'quorum-loss', 'The lone RF3 voter could not settle a receipt and retained both complete records on the original run.')

        # Restore the first lost worker without the original owner. Recovery
        # must copy from the one process that still has the accepted body.
        launch_retained(cluster, removed)
        same_run_ready(cluster, run, (survivor, removed), 2)
        for name in (survivor, removed):
            for request in (read, mutation):
                record = read_record(cluster, name, request)
                assert assert_record(record, run, request) == originals[request['request_id']], record
        assertion('O14', 'original-context', 'Recovered copies matched the exact body, digest, pinned read timestamp, catalog and schema epochs, and accepted authorization.')
        assertion('O14', 'join-with-outstanding-work', 'The restored worker had the complete pending records after it became ready.')

        for directory in directories:
            for request in (read, mutation):
                release(directory, request)
        cluster.apis['api2'].stop()
        cluster.start_api('api2', DIRECT)
        with closing(gateway(cluster, 'api2')) as restored:
            exact_receipt(restored.exchange, run, read, original_row)
            exact_receipt(restored.exchange, run, mutation, None)
        recovered = Database(cluster.api_urls['api2'], cluster.auth['data_token'], database='test',
                             ca_file=str(cluster.ca), timeout=30).table('same_run')
        assert recovered.get('seed') == dict(id='seed', value=2)
        assert recovered.get('once') == dict(id='once', value=3)
        assert recovered.query().count() == 2
        assertion('O14', 'same-run-outcomes', 'The restored gateway returned the pinned old row and the original mutation result. The newest data held exactly two rows.')
        launch_retained(cluster, owner)
        same_run_ready(cluster, run, WORKERS, 3)


FENCE_STAGES = ('allocator', 'status', 'shard')


def fence_checkpoint(directory, stage, old_run):
    path = directory / f'cold-fence-{stage}.json'
    wait_until(path.exists, f'cold startup did not record its {stage} fence')
    receipt = json.loads(path.read_text())
    assert receipt['expected']['run_id'] == old_run, receipt
    assert receipt['next']['run_id'] != old_run, receipt
    assert receipt['applied_at']['index'] > 0 and receipt['applied_at']['term'] > 0, receipt
    return receipt


def assert_fence_transition(receipts, old_run, new_run):
    assert new_run != old_run
    assert len({receipt['transition_id'] for receipt in receipts}) == 1, receipts
    for receipt in receipts:
        assert receipt['expected']['run_id'] == old_run and receipt['next']['run_id'] == new_run, receipt


def direct_writes(table_id):
    if not DIRECT:
        return {}
    return {stage: operation('table.insert', dict(table_id=table_id, row=dict(id=stage, value=4)))
            for stage in ('direct-proposing', 'direct-applied')}


def stop_at_direct_writes(open_client, directories, writes, stop):
    def submit(request):
        try:
            with closing(open_client()) as client:
                return client.exchange('/v1/operations', request)
        except TRANSPORT_ERRORS:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = {stage: pool.submit(submit, request) for stage, request in writes.items()}
        try:
            for stage, request in writes.items():
                wait_reached(directories, request, stage)
                assert not pending[stage].done(), stage
        finally:
            stop()
        for stage, future in pending.items():
            reply = future.result()
            if reply is not None:
                assert_unknown(reply, writes[stage])


def assert_direct_recovery(exchange, table, old_run, writes, stats):
    if not writes:
        assert stats() == 0, 'rollback mode proposed a direct entry'
        return
    for stage, request in writes.items():
        reply = exchange(receipt_path(old_run, request))
        assert reply[0] == 404, reply
        if stage == 'direct-proposing':
            assert table.get(stage) is None
        else:
            assert table.get(stage) == request['payload']['row']
            before = stats()
            assert exchange('/v1/operations', request) == (200, dict(
                request_id=request['request_id'], data=None))
            assert stats() == before, 'the in-flight retry proposed a second direct entry'


def interrupt_rf1_fences(server, old_run):
    receipts = []
    for stage in FENCE_STAGES:
        hold = server.root / f'cold-fence-{stage}.hold'
        hold.touch()
        server.start(cold_start=True, wait_ready=False)
        try:
            receipts.append(fence_checkpoint(server.root, stage, old_run))
        finally:
            server.stop(abrupt=True)
            hold.unlink()
    server.start(cold_start=True)
    assert_fence_transition(receipts, old_run, server.run_id)
    return receipts


def rf1_direct_recovery(test_executable):
    with tempfile.TemporaryDirectory(prefix='nabedra-o15-direct-rf1-') as directory:
        server = RuntimeServer(test_executable, directory)
        server.test_executable = str(Path(test_executable).resolve())
        with server:
            assert wait_direct_active(server)['direct_oltp'] == DIRECT
            db = Database(server.url, server.token, database='test', timeout=30)
            table = db.create_table('cold_direct', {'id': str, 'value': int}, key='id')
            writes = direct_writes(table.id)
            if not writes:
                table.insert(dict(id='rollback', value=4))
            gates(server.root, [(request, stage) for stage, request in writes.items()])
            old_run = server.run_id
            stop_at_direct_writes(lambda: Http(server.url, server.token), [server.root], writes,
                                  lambda: server.stop(abrupt=True))
            (server.root / 'gates.json').unlink()
            server.start(cold_start=True)
            assert server.run_id != old_run
            assert wait_direct_active(server)['direct_oltp'] == DIRECT
            table = Database(server.url, server.token, database='test', timeout=30).table('cold_direct')
            with closing(Http(server.url, server.token)) as client:
                assert_direct_recovery(client.exchange, table, old_run, writes,
                                       lambda: (server.stats().get('direct') or {}).get('entries_proposed', 0))
            assert table.query().count() == 1
            if not writes:
                assert table.get('rollback') == dict(id='rollback', value=4)


def rf3_cold_recovery(binary, test_executable):
    if not Rf3Cluster.mounts_available():
        return False
    with native_cluster(binary, test_executable, 'o15-rf3', mounts=True) as cluster:
        run = cluster.run_id
        seed_dir = cluster.barrier_dir('w1')  # The fixture assigns Control node 1 to w1.
        directories = [cluster.barrier_dir(name) for name in WORKERS]
        assert all(status['direct_oltp'] == DIRECT for status in cluster.wait_direct_active().values())
        db = Database(cluster.api_urls['api1'], cluster.auth['data_token'], database='test',
                      ca_file=str(cluster.ca), timeout=30)
        table = db.create_table('cold_rf3', {'id': str, 'value': int}, key='id')
        queued = {stage: operation('table.insert', dict(table_id=table.id, row=dict(id=stage, value=index)))
                  for index, stage in enumerate(('queued', 'prepared', 'committed'), start=1)}
        writes = direct_writes(table.id)
        for directory in directories:
            gates(directory, [(request, stage) for stage, request in (queued | writes).items()])
        with closing(gateway(cluster)) as client:
            for stage, request in queued.items():
                assert_accepted(client.exchange('/v1/requests', request), run, request)
                wait_reached(directories, request, stage)
        committed = queued['committed']
        recorded = [directory / f"decision-{committed['request_id']}.json" for directory in directories]
        decision_path, = [path for path in recorded if path.exists()]
        decision = json.loads(decision_path.read_text())
        assert 'commit' in decision['decision'], decision

        stop_at_direct_writes(lambda: gateway(cluster), directories, writes, cluster.kill_all)
        for directory in directories:
            # A retained old request would leave a second attempt record.
            # Leave queued gates intact and remove only the direct retry gates.
            gates(directory, [(request, stage) for stage, request in queued.items()])
        for stage in FENCE_STAGES:
            (seed_dir / f'cold-fence-{stage}.hold').touch()
        receipts = []

        def interrupt(profile):
            remaining = set(FENCE_STAGES)
            while remaining:
                stage = wait_until(
                    lambda: next((name for name in remaining if
                                  (seed_dir / f'cold-fence-{name}.json').exists()), None),
                    'cold startup did not reach the next group fence')
                remaining.remove(stage)
                receipt = fence_checkpoint(seed_dir, stage, run)
                assert receipt['transition_id'] == profile['transition_id'], receipt
                receipts.append(receipt)
                cluster.workers['w1'].kill()
                (seed_dir / f'cold-fence-{stage}.hold').unlink()
                launch_retained(cluster, 'w1')

        profile = cluster.cold_start(keep_certificates=True, before_ready=interrupt)
        assert_fence_transition(receipts, run, profile['next_run'])
        status_fence, = [receipt for receipt in receipts if 'status' in receipt['group']['kind']]
        assert decision['decided_at']['index'] < status_fence['applied_at']['index'], (decision, status_fence)
        for name in WORKERS:
            queue = cluster.stats(name)['queue']
            assert queue['queued_records'] == queue['running_records'] == queue['reconciling_records'] == 0, queue

        cluster.apis['api1'].stop()
        cluster.start_api('api1', DIRECT)
        assert all(status['direct_oltp'] == DIRECT for status in cluster.wait_direct_active().values())
        table = Database(cluster.api_urls['api1'], cluster.auth['data_token'], database='test',
                         ca_file=str(cluster.ca), timeout=30).table('cold_rf3')
        with closing(gateway(cluster)) as client:
            exact_receipt(client.exchange, run, committed, None)
            assert client.exchange('/v1/operations', committed) == (200, dict(
                request_id=committed['request_id'], data=None))
            for stage in ('queued', 'prepared'):
                request = queued[stage]
                assert_abandoned(client.exchange(receipt_path(run, request)), request)
                assert_abandoned(client.exchange('/v1/requests', request), request)
            assert_direct_recovery(client.exchange, table, run, writes, lambda: sum(
                (stats.get('direct') or {}).get('entries_proposed', 0)
                for stats in cluster.all_stats().values()))
        assert table.query().count() == 1 + bool(writes)
        assert table.get('committed') == dict(id='committed', value=3)
        assert table.get('prepared') is table.get('queued') is None
        for stage, request in queued.items():
            attempts = [directory / f"attempts-{request['request_id']}" for directory in directories]
            assert [line for path in attempts if path.exists() for line in path.read_text().splitlines()] == [stage]
        # An exclusive lifecycle operation must acquire the guard left by the
        # interrupted prepare and use the fenced rollback to remove its intent.
        table.drop()
    return True


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
                if stage == 'prepared':
                    interrupt_rf1_fences(server, old_run)
                else:
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


def run_native_assertions(test_executable, cases):
    """Run exact production-library assertions and retain their libtest output."""
    listed = subprocess.check_output([test_executable, '--list'], text=True, timeout=15)
    names = [line.removesuffix(': test') for line in listed.splitlines() if line.endswith(': test')]
    for case in cases:
        name, = [name for name in names if name.rsplit('::', 1)[-1] == case]
        result = subprocess.run([test_executable, '--exact', name, '--nocapture'],
                                capture_output=True, text=True, timeout=90)
        print(result.stdout, end='', flush=True)
        print(result.stderr, end='', file=sys.stderr, flush=True)
        assert result.returncode == 0 and 'test result: ok. 1 passed; 0 failed; 0 ignored;' in result.stdout, name


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary')
    parser.add_argument('--test-executable')
    parser.add_argument('--scenario', choices=('retirement-cold', 'cli', 'failure-boundaries',
        'activation-boundary', 'admission', 'queue-survival', 'snapshot-pressure', 'gateway'), required=True)
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
        assertion('O13', 'preaccept-overload', 'Queue bytes and tentative count independently refused new work with 429 not_applied and settled their charges.')
        retirement_and_committed_cold_replay(args.binary)
        assertion('O13', 'expiry-and-retirement', 'Expired read results returned 404, the queue retired their charges, and committed DDL replayed its exact result after cold start.')
        cli_supervisor(args.binary)
        incomplete('O13', 'RF3 copy loss, CPU and disk headroom, lost reservation replies, and resource fault variations are pending')
    elif args.scenario == 'queue-survival':
        if not args.test_executable or not args.binary:
            parser.error('--test-executable and --binary are required')
        run_native_assertions(args.test_executable, [
            'r03_q03_join_empty_single_multiple_and_exhausted_pages_have_correct_done_flag'])
        assertion('O14', 'bootstrap-pages', 'The native queue snapshot asserted empty, single, intermediate, final, and exhausted pages.')
        queue_survival(args.binary, args.test_executable)
        incomplete('O14', 'signed control saturation alongside terminal copying is pending')
    elif args.scenario == 'failure-boundaries':
        if not args.test_executable:
            parser.error('--test-executable is required for actual process barriers')
        failure_boundaries(args.test_executable)
        assertion('O15', 'rf1-cold-queue', 'RF1 abandoned queued and prepared work, preserved a durable commit, and resumed after interruption at each group fence.')
        activation_boundary(args.test_executable)
        assertion('O15', 'activation-boundary', 'The pre-activation UUID replayed its original outcome or stayed abandoned across activation.')
        rf1_direct_recovery(args.test_executable)
        if args.binary and rf3_cold_recovery(args.binary, args.test_executable):
            assertion('O15', 'rf3-cold-queue', 'RF3 resumed one cold transition after a stop at each group fence. The earlier decision survived and old pending work stayed abandoned.')
            assertion('O15', 'direct-in-flight', 'Both topologies checked the direct path’s before-proposal and after-apply cuts.' if DIRECT else
                'Both topologies verified rollback mode proposed zero direct entries.')
        incomplete('O15', 'late decision rejection and unavailable proof are pending')
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

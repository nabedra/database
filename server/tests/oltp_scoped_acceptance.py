#!/usr/bin/env python3.11
"""Exercise registered RF3 OLTP through the real HTTPS gateways and worker mTLS.

Run the release binary first. A separate run with --test-executable uses
the registered Rust test child for exact copy cuts. This fixture owns the
same 4 CPU / 8 GB lane as benchmark_oltp_nabedra.py.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from unittest.mock import patch
from uuid import UUID, uuid4

from oltp_terminal_copy_acceptance import CHILD, read_json, wait_for


BARRIERS = Path('/scoped-acceptance')
HTTP_TIMEOUT = 24
# Includes separate copy deadlines, expiry waits and the owner-loss window.
DRIVER_TIMEOUT = 540
RETENTION_MS = 25_000
PRIMARY = 'https://api:8000'
SECONDARY = 'https://localhost:8001'
ADMIN = '/v1/cluster/operations'
COPY_PAYLOAD = 'before-scoped-payload-copy'
COPY_MARKER = 'before-scoped-marker-copy'
COPY_TERMINAL = 'before-scoped-terminal-copy'
POINT_DECISION = 'after-scoped-timestamp-decision'
LEGACY_TERMINAL = 'before-terminal-copy'
IDENTITY_FIELDS = ('worker_id', 'incarnation', 'enrollment_revision', 'certificate_sha256')
TRANSPORT_ERRORS = (OSError, http.client.HTTPException)


def write_json(path, value):
    pending = path.with_suffix('.tmp')
    pending.write_text(json.dumps(value, separators=(',', ':')))
    pending.replace(path)


def report(case, **observed):
    print(json.dumps(dict(case=case, **observed), sort_keys=True), flush=True)


def envelope(operation, payload, request_id=None):
    return dict(request_id=request_id or str(uuid4()), operation=operation, payload=payload)


def body_bytes(operation):
    return json.dumps(operation, separators=(',', ':'), allow_nan=False).encode()


def request_digest(operation):
    return hashlib.sha256(body_bytes(operation)).hexdigest()


def pending_receipt(run, request_id, state):
    return dict(request_id=request_id, run_id=run, state=state, result=None, error=None)


def assert_error(reply, status, request_id, code, outcome):
    received_status, body = reply
    assert received_status == status and set(body) == {'request_id', 'error'}, reply
    assert body['request_id'] == request_id, reply
    assert body['error']['code'] == code and body['error']['outcome'] == outcome, reply
    return body['error']


def assert_uncertain(reply, request_id):
    code = reply[1].get('error', {}).get('code')
    assert code in ('storage_unavailable', 'outcome_unknown'), reply
    return assert_error(reply, 503, request_id, code, 'unknown')


def stop_gateway(process, native):
    if process.poll() is not None:
        return
    try:
        if native:
            os.kill(native[0], signal.SIGKILL)
        else:
            process.terminate()
        process.wait(timeout=5)
    except ProcessLookupError:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


class Driver:
    def __init__(self, module, fixture, instrumented):
        self.module, self.fixture, self.instrumented = module, fixture, instrumented
        self.metadata = read_json(fixture / 'scoped-metadata.json')
        self.auth = read_json(fixture / 'auth.json')
        self.run_id = self.metadata['run_id']
        self.roles = self.module.WORKERS
        self.instruments = {role: BARRIERS / role for role in self.roles}
        self.database = module.Nabedra(fixture / 'ca.pem', self.auth, HTTP_TIMEOUT, self.metadata)
        self.stack = ExitStack()

    def client(self, url=PRIMARY, timeout=HTTP_TIMEOUT, admin=False):
        token = self.auth['admin_token' if admin else 'data_token']
        client = self.module.Http(url, token, timeout, self.database.ca)
        self.stack.callback(client.close)
        return client

    def admin(self, client, operation, payload):
        request = envelope(operation, payload)
        return self.module.require_data(*client.exchange(ADMIN, request), request['request_id'])

    def start_second_gateway(self, binary):
        config = self.fixture / 'api2.json'
        env = os.environ | {'NABEDRA_SERVER_BINARY': binary}
        process = subprocess.Popen(
            [sys.executable, '-m', 'nabedra.cli', 'database', 'serve', '--config', str(config)],
            env=env, start_new_session=True,
        )
        native = []
        self.stack.callback(stop_gateway, process, native)
        observer = self.client(SECONDARY, timeout=2)

        def ready(_):
            if process.poll() is not None:
                raise RuntimeError(f'second registered API exited with code {process.returncode}')
            try:
                status, reply = observer.exchange('/health/ready')
            except TRANSPORT_ERRORS:
                return None
            return (status == 200 and reply.get('run_id') == self.run_id
                    and reply.get('topology') == 'api-gateway' and reply.get('ready_workers') == 3)

        wait_for([config], ready, timeout=40)
        children = Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
        native_pid, = map(int, children)
        native.append(native_pid)
        control = self.client('http://w1:8000', timeout=8, admin=True)
        expected = self.admin(control, 'queue.scoped.status', {})
        assert expected['activation'] is None, expected
        active = self.admin(control, 'queue.scoped.admission', {'expected': expected, 'enabled': True})
        assert active['version'] == 2 and active['run_id'] == self.run_id and active['activation'], active
        report('two registered gateways', activation=active, apis=self.metadata['api_enrollments'])
        return process, native

    def record_file(self, role, request_id, stage):
        return self.instruments[role] / f'transaction-{request_id}-{stage}.json'

    @contextmanager
    def gates(self, request_id, stages):
        written = []
        try:
            for role, stage in stages.items():
                directory = self.instruments[role]
                write_json(directory / 'gates.json', {request_id: stage})
                written.append(role)
            yield
        finally:
            for role in written:
                self.release(role, request_id)
                (self.instruments[role] / 'gates.json').unlink(missing_ok=True)

    def release(self, role, request_id):
        (self.instruments[role] / f'release-{request_id}').touch()

    def reached(self, role, request_id, stage):
        wait_for([self.instruments[role] / f'reached-{request_id}'],
                 lambda path: path.read_text() == stage)

    def checkpoints(self, request_id, stage):
        return [self.record_file(role, request_id, stage) for role in self.roles]

    def worker_identity(self, role):
        enrolled = self.metadata['enrollments'][role]
        return {key: enrolled[key] for key in IDENTITY_FIELDS}

    def copy_evidence(self, request, wanted=None):
        request_id = request['request_id']
        proofs = {}
        for phase in ('payload', 'marker', 'terminal'):
            paths = self.checkpoints(request_id, f'scoped-{phase}-quorum')
            if not wanted or phase in wanted:
                proof = wait_for(paths, read_json, timeout=5)
                stamp = proof['stamp']
                assert stamp['phase'] == phase, proof
                assert stamp['request']['request_id'] == request_id, proof
                assert stamp['request']['run_id'] == self.run_id, proof
                assert stamp['request']['digest'] == request_digest(request), proof
                assert int(stamp['request']['accepted_context'], 16) != 0, proof
                assert stamp['payload']['request_id'] == stamp['copied']['request_id'] == request_id, proof
                if phase == 'terminal':
                    assert stamp['copied']['version'] > stamp['payload']['version'] > 0, proof
                else:
                    assert stamp['payload'] == stamp['copied'] and stamp['payload']['version'] > 0, proof
                identities = [self.worker_identity(role) for role in self.roles]
                assert proof['placement']['members'] == identities, proof
                assert proof['placement']['owner'] == self.worker_identity(self.owner), proof
                assert proof['placement']['view']['run_id'] == self.run_id, proof
                assert proof['placement']['group'] == self.placement['record']['identity'], proof
                assert len(proof['holders']) == 2 and all(holder in identities for holder in proof['holders']), proof
                assert proof['placement']['owner'] in proof['holders'], proof
                assert len({holder['worker_id'] for holder in proof['holders']}) == 2, proof
                proofs[phase] = proof
        if len(proofs) > 1:
            exact = [proof['stamp'] for proof in proofs.values()]
            for field in ('payload', 'placement', 'request', 'reservation'):
                assert all(stamp[field] == exact[0][field] for stamp in exact), proofs
        if 'terminal' in proofs:
            dispatch = wait_for(self.checkpoints(request_id, 'scoped-dispatch'), read_json, timeout=5)
            assert dispatch == proofs['terminal']['stamp']['request'], dispatch
        return proofs

    def assert_holders(self, proof, remote):
        assert all(self.worker_identity(role) in proof['holders']
                   for role in (self.owner, remote)), proof

    def decision(self, request, proof):
        request_id = request['request_id']
        decision = wait_for(self.checkpoints(request_id, 'scoped-decision'), read_json, timeout=5)
        assert decision['request'] == proof['stamp']['request'], decision
        assert decision['txn'] == {'request_id': request_id, 'attempt': 1}, decision
        assert 'commit' in decision['decision'], decision
        assert decision['decided_at']['term'] > 0 and decision['decided_at']['index'] > 0, decision
        return decision

    def terminal(self, client, request_id, result, *, before_acceptance=False, timeout=HTTP_TIMEOUT):
        expected = pending_receipt(self.run_id, request_id, 'succeeded') | {'result': result}
        deadline = time.monotonic() + timeout
        while True:
            status, reply = self.receipt(client, request_id)
            if status == 200 and reply == expected:
                assert type(reply['result']) is type(result), reply
                return reply
            if status == 200:
                assert reply.get('state') in ('queued', 'running', 'committing', 'reconciling'), reply
                assert reply == pending_receipt(self.run_id, request_id, reply['state']), reply
            elif status == 404 and before_acceptance:
                assert_error((status, reply), 404, request_id, 'not_found', 'not_applied')
            elif status == 429:
                assert_error((status, reply), 429, request_id, 'overloaded', 'not_applied')
            else:
                assert_uncertain((status, reply), request_id)
            if time.monotonic() >= deadline:
                raise TimeoutError(f'terminal receipt {request_id}: HTTP {status} {reply}')
            time.sleep(0.1)

    def receipt(self, client, request_id):
        return client.exchange(f'/v1/requests/{self.run_id}/{request_id}')

    def submit(self, client, request):
        return client.exchange('/v1/requests', request)

    def accepted(self, reply, request_id):
        assert reply == (202, dict(request_id=request_id, run_id=self.run_id, state='queued')), reply

    def point(self, operation, key, values=None):
        payload = dict(table_id=self.table.id, key=key)
        if values is not None:
            payload['values'] = values
        return envelope(operation, payload)

    def exercise_orm(self):
        from nabedra import ConflictError, Database

        db = Database(PRIMARY, self.auth['data_token'], timeout=HTTP_TIMEOUT,
                      ca_file=str(self.fixture / 'ca.pem'), database=self.module.DATABASE)
        self.table = db.create_table('scoped_acceptance', {'id': int, 'value': int}, 'id')
        assert self.table.insert_many([{'id': key, 'value': 0} for key in range(6)]) == 6
        assert db.table(self.table.name).id == self.table.id
        assert [table.id for table in db.tables()] == [self.table.id]
        self.database.table_id = self.table.id
        self.placement = self.database.data_placement()
        group_id = UUID(self.placement['record']['identity']['group_id'])
        rank = hashlib.sha256(group_id.bytes).digest()[0] % len(self.roles)
        self.owner = self.roles[rank]
        self.followers = [role for role in self.roles if role != self.owner]

        get = self.point('table.get', 0)
        update = self.point('table.update', 0, {'value': 17})
        if self.instrumented:
            cases = ((get, lambda: self.table.get(0), {'id': 0, 'value': 0}),
                     (update, lambda: self.table.update(0, {'value': 17}), True))
            for request, invoke, expected in cases:
                request_id = request['request_id']
                hold_dispatch = request is get
                self.select_checkpoints(request_id, hold_dispatch=hold_dispatch)
                dispatch = self.record_file(self.owner, request_id, 'scoped-dispatch')
                release_dispatch = dispatch.with_name(f'release-{dispatch.stem}')
                with ThreadPoolExecutor(max_workers=1) as executor:
                    with self.gates(request_id, dict.fromkeys(self.followers, COPY_PAYLOAD)):
                        # Fix UUID allocation and let the SDK make its usual request.
                        with patch('nabedra.client.uuid.uuid4', return_value=UUID(request_id)):
                            response = executor.submit(invoke)
                            for role in self.followers:
                                self.reached(role, request_id, COPY_PAYLOAD)
                            assert not response.done(), request_id
                            for role in self.followers:
                                self.release(role, request_id)
                            if hold_dispatch:
                                try:
                                    wait_for([dispatch], read_json)
                                    time.sleep(2.5)
                                    assert not response.done(), request_id
                                finally:
                                    release_dispatch.touch()
                            actual = response.result(timeout=HTTP_TIMEOUT + 2)
                            assert type(actual) is type(expected) and actual == expected, actual
            reads = self.copy_evidence(get)
            writes = self.copy_evidence(update)
            decision = self.decision(update, writes['terminal'])
            report('ordinary ORM scoped Get and Update', reads=reads, writes=writes, decision=decision,
                   payload_destinations=[self.worker_identity(role) for role in self.followers])
        else:
            assert self.table.get(0) == {'id': 0, 'value': 0}
            assert self.table.update(0, {'value': 17}) is True
            report('ordinary ORM Get and Update on the release build',
                   inferred_owner=self.worker_identity(self.owner))

        assert self.table.get(0) == {'id': 0, 'value': 17}
        assert self.table.get(100) is None
        assert self.table.update(100, {'value': 17}) is False
        assert self.table.get(100) is None
        assert self.table.query().where(id=0).collect() == [{'id': 0, 'value': 17}]
        assert self.table.query().count() == 6
        try:
            self.table.insert({'id': 0, 'value': 100})
        except ConflictError as error:
            assert error.status_code == 409, error
        else:
            raise AssertionError('legacy duplicate insert succeeded')
        assert self.table.get(0) == {'id': 0, 'value': 17}
        report('ordinary ORM legacy create, insert, describe, list, collect, count and duplicate-key conflict')

    def select_checkpoints(self, request_id, *, hold_dispatch=False):
        stages = ('scoped-payload-quorum', 'scoped-marker-quorum', 'scoped-terminal-quorum',
                  'scoped-dispatch', 'scoped-decision')
        for role, path in self.instruments.items():
            config = path / 'transaction-gates.json'
            selected = read_json(config) if config.exists() else {}
            settings = dict.fromkeys(stages, False)
            settings['scoped-dispatch'] = hold_dispatch and role == self.owner
            write_json(config, selected | {request_id: settings})

    def fault_command(self, role, paused):
        command_id = str(uuid4())
        command = dict(command_id=command_id, worker=role, paused=paused)
        write_json(BARRIERS / 'host-fault.json', command)
        reply = wait_for([BARRIERS / f'host-fault-{command_id}.json'], read_json)
        assert reply == command, reply

    @contextmanager
    def pause_worker(self, role):
        self.fault_command(role, True)
        try:
            yield
        finally:
            self.fault_command(role, False)

    def identical_submission(self, primary, secondary, executor):
        request = self.point('table.update', 1, {'value': 31})
        request_id = request['request_id']
        if self.instrumented:
            self.select_checkpoints(request_id)
        begin = threading.Barrier(2, timeout=5)

        def submit(client):
            begin.wait()
            return self.submit(client, request)

        futures = [executor.submit(submit, client) for client in (primary, secondary)]
        for future in futures:
            self.accepted(future.result(timeout=HTTP_TIMEOUT + 2), request_id)
        recovered = [self.terminal(client, request_id, True) for client in (primary, secondary)]
        assert recovered[0] == recovered[1]
        self.accepted(self.submit(primary, request), request_id)
        if self.instrumented:
            proof = self.copy_evidence(request)
            decision = self.decision(request, proof['terminal'])
        change_value = request | {'payload': request['payload'] | {'values': {'value': 99}}}
        change_bytes = {key: request[key] for key in ('operation', 'request_id', 'payload')}
        assert body_bytes(change_bytes) != body_bytes(request) and change_bytes == request
        for changed in (change_value, change_bytes):
            assert_error(self.submit(secondary, changed), 409, request_id, 'conflict', 'not_applied')
        assert self.table.get(1) == {'id': 1, 'value': 31}
        assert self.table.update(1, {'value': 32}) is True
        self.accepted(self.submit(secondary, request), request_id)
        assert self.terminal(primary, request_id, True) == recovered[0]
        assert self.table.get(1) == {'id': 1, 'value': 32}
        evidence = {'receipt': recovered[0], 'changed_value_and_bytes_conflicted': True,
                    'later_value_survived_replay': 32}
        if self.instrumented:
            assert self.decision(request, proof['terminal']) == decision
            evidence |= {'copies': proof, 'decision': decision}
        report('Q05', **evidence)

    def minority_admission(self, stage, key, primary, secondary, executor, gateway=None):
        request = self.point('table.update', key, {'value': 40 + key})
        request_id = request['request_id']
        self.select_checkpoints(request_id)
        first, slow = self.followers
        stages = dict.fromkeys(self.followers, stage) | {self.owner: POINT_DECISION}
        with self.gates(request_id, stages):
            submit = executor.submit(self.submit, secondary, request)
            for role in self.followers:
                self.reached(role, request_id, stage)
            if stage == COPY_PAYLOAD:
                assert not submit.done(), 'submitting API must be in flight at the loss cut'
            before = self.receipt(primary, request_id)
            assert_uncertain(before, request_id)
            if stage == COPY_MARKER:
                self.copy_evidence(request, ('payload',))
                denied = submit.result(timeout=HTTP_TIMEOUT + 2)
                assert_uncertain(denied, request_id)
            else:
                stop_gateway(*gateway)
                try:
                    result = submit.result(timeout=5)
                except TRANSPORT_ERRORS:
                    pass
                else:
                    raise AssertionError(f'failed submitting gateway returned {result}')
                denied = 'submitting gateway killed before any follower installed the payload'
                assert not any(path.exists() for path in self.checkpoints(request_id, 'scoped-payload-quorum'))
            for checkpoint in ('scoped-marker-quorum', 'scoped-dispatch', 'scoped-decision'):
                assert not any(path.exists() for path in self.checkpoints(request_id, checkpoint)), checkpoint
            assert not (self.instruments[self.owner] / f'reached-{request_id}').exists(), request_id
            self.release(first, request_id)
            self.accepted(self.submit(primary, request), request_id)
            self.reached(self.owner, request_id, POINT_DECISION)
            self.release(self.owner, request_id)
            terminal = self.terminal(primary, request_id, True)
            proof = self.copy_evidence(request)
            self.assert_holders(proof['marker'], first)
            if stage == COPY_PAYLOAD:
                self.assert_holders(proof['payload'], first)
            assert not (self.instruments[slow] / f'release-{request_id}').exists()
            assert self.table.get(key) == {'id': key, 'value': 40 + key}
            decision = self.decision(request, proof['terminal'])
        report('O30/Q03', cut=stage, before_release=denied, still_paused=self.worker_identity(slow),
               receipt_before_quorum=before,
               accepted_holders=[self.worker_identity(role) for role in (self.owner, first)],
               request_digest=request_digest(request), receipt=terminal, copies=proof, decision=decision)

    def send_without_reading(self, writer, request):
        writer.connection.request('POST', '/v1/operations', body_bytes(request), writer.headers)

    def lost_responses(self, primary, secondary):
        request = self.point('table.update', 2, {'value': 52})
        request_id = request['request_id']
        writer = self.client()
        evidence = {}
        if self.instrumented:
            self.select_checkpoints(request_id)
            first, slow = self.followers
            # The owner-only hold proves the native Timestamp command was reached.
            # Both follower holds then isolate terminal replication from that decision.
            stages = dict.fromkeys(self.followers, COPY_TERMINAL) | {self.owner: POINT_DECISION}
            with self.gates(request_id, stages):
                self.send_without_reading(writer, request)
                self.reached(self.owner, request_id, POINT_DECISION)
                writer.close()
                self.release(self.owner, request_id)
                for role in self.followers:
                    self.reached(role, request_id, COPY_TERMINAL)
                assert not any(path.exists() for path in self.checkpoints(request_id, 'scoped-terminal-quorum'))
                observed = self.receipt(secondary, request_id)
                assert observed == (200, pending_receipt(self.run_id, request_id, 'committing')), observed
                self.release(first, request_id)
                terminal = self.terminal(secondary, request_id, True)
                proof = self.copy_evidence(request)
                self.assert_holders(proof['terminal'], first)
                assert not (self.instruments[slow] / f'release-{request_id}').exists()
                evidence = dict(before_quorum=observed, copies=proof,
                                terminal_holders=[self.worker_identity(role) for role in (self.owner, first)],
                                decision=self.decision(request, proof['terminal']))
        else:
            self.send_without_reading(writer, request)
            terminal = self.terminal(secondary, request_id, True, before_acceptance=True)
            writer.close()
        assert self.table.get(2) == {'id': 2, 'value': 52}
        assert self.module.require_data(*primary.exchange('/v1/operations', request), request_id) is True
        report('O30/Q06 scoped mutation', receipt=terminal,
               transport='closed at durable decision' if self.instrumented else 'synchronous reply left unread',
               **evidence)

        legacy = envelope('table.insert', {'table_id': self.table.id, 'row': {'id': 10, 'value': 62}})
        legacy_id = legacy['request_id']
        if self.instrumented:
            with self.gates(legacy_id, {'w3': LEGACY_TERMINAL}):
                self.send_without_reading(writer, legacy)
                self.reached('w3', legacy_id, LEGACY_TERMINAL)
                writer.close()
                observed = self.receipt(secondary, legacy_id)
                assert observed == (200, pending_receipt(self.run_id, legacy_id, 'committing')), observed
        else:
            self.send_without_reading(writer, legacy)
        receipt = self.terminal(secondary, legacy_id, None, before_acceptance=not self.instrumented)
        writer.close()
        assert self.module.require_data(*primary.exchange('/v1/operations', legacy), legacy_id) is None
        assert self.table.where(id=10).collect() == [{'id': 10, 'value': 62}]
        report('Q06 legacy mutation', receipt=receipt,
               transport='closed with final legacy copy held' if self.instrumented else 'synchronous reply left unread')
        return request_id, legacy_id

    def query_expiration(self, primary, secondary, mutation_ids):
        point = self.point('table.get', 0)
        scan = envelope('query.collect', {'table_id': self.table.id,
                        'query': {'select': None, 'filter': {'op': 'eq', 'column': 'id', 'value': 0},
                                  'order_by': None, 'limit': 1}})
        expected = {'id': 0, 'value': 17}
        for request, result in ((point, expected), (scan, [expected])):
            request_id = request['request_id']
            self.accepted(self.submit(primary, request), request_id)
            self.terminal(secondary, request_id, result)
        # Change the row after both snapshots have settled, so a fresh execution
        # could be distinguished from the retained read.
        assert self.table.update(0, {'value': 71}) is True
        deadline = time.monotonic() + RETENTION_MS / 1000 + 12
        for request, result in ((point, expected), (scan, [expected])):
            request_id = request['request_id']
            while True:
                reply = self.receipt(secondary, request_id)
                if reply[0] == 404:
                    error = assert_error(reply, 404, request_id, 'not_found', 'not_applied')
                    assert error['message'] == 'receipt unavailable or expired', reply
                    report('Q06 query expiration', operation=request['operation'], request_id=request_id,
                           retention_ms=RETENTION_MS, error=error)
                    break
                assert reply == (200, pending_receipt(self.run_id, request_id, 'succeeded')
                                 | {'result': result}), reply
                if time.monotonic() >= deadline:
                    raise TimeoutError(f'read receipt did not expire: {request_id}')
                time.sleep(0.5)
        assert self.terminal(primary, mutation_ids[0], True)['result'] is True
        assert self.terminal(secondary, mutation_ids[1], None)['result'] is None
        assert self.table.get(0) == {'id': 0, 'value': 71}
        assert self.table.get(2) == {'id': 2, 'value': 52}
        report('Q06 exact mutation receipts after query retention', scoped=mutation_ids[0], legacy=mutation_ids[1])

    def release_pause_check(self, primary):
        request = self.point('table.update', 3, {'value': 73})
        request_id = request['request_id']
        follower = self.followers[-1]
        with self.pause_worker(follower):
            # A whole worker pause can also interrupt a metadata election.
            # Record each refusal or uncertainty and reuse the exact identity.
            admission_attempts = []
            for attempt in range(3):
                reply = self.submit(primary, request)
                if reply[0] == 202:
                    break
                if reply[0] == 429:
                    assert_error(reply, 429, request_id, 'overloaded', 'not_applied')
                else:
                    assert_uncertain(reply, request_id)
                admission_attempts.append(reply)
                if attempt == 2:
                    break
                time.sleep(0.25)
            self.accepted(reply, request_id)
            terminal = self.terminal(primary, request_id, True)
            assert self.table.get(3) == {'id': 3, 'value': 73}
            status, health = primary.exchange('/health/ready')
            assert status == 200 and health['run_id'] == self.run_id and health['ready_workers'] == 3, health
            legacy = envelope('table.insert', {'table_id': self.table.id,
                              'row': {'id': 11, 'value': 83}})
            # The full transport timeout can outlast the original ready
            # membership. Observe a bounded window with the third copy held.
            probe = self.client(timeout=3)
            try:
                legacy_reply = self.submit(probe, legacy)
            except TimeoutError:
                legacy_reply = {'transport': 'timed out while the follower was paused'}
            else:
                assert_uncertain(legacy_reply, legacy['request_id'])
        self.accepted(self.submit(primary, legacy), legacy['request_id'])
        legacy_receipt = self.terminal(primary, legacy['request_id'], None)
        assert self.table.where(id=11).collect() == [{'id': 11, 'value': 83}]
        report('O30/Q03 release binary', paused=self.worker_identity(follower), scoped_receipt=terminal,
               admission_attempts_before_202=admission_attempts,
               ready_members_during_pause=health['ready_workers'],
               legacy_admission_while_paused=legacy_reply, legacy_receipt=legacy_receipt)

    def takeover_after_acceptance(self, primary):
        request = self.point('table.update', 5, {'value': 105})
        request_id = request['request_id']
        self.select_checkpoints(request_id, hold_dispatch=True)
        dispatch = self.record_file(self.owner, request_id, 'scoped-dispatch')
        release = dispatch.with_name(f'release-{dispatch.stem}')
        others = [role for role in self.roles if role != self.owner]
        try:
            self.accepted(self.submit(primary, request), request_id)
            accepted = self.copy_evidence(request, ('payload', 'marker'))
            assert wait_for([dispatch], read_json) == accepted['marker']['stamp']['request']
            assert not any(path.exists() for path in self.checkpoints(request_id, 'scoped-decision'))
            with self.pause_worker(self.owner):
                terminal = self.terminal(primary, request_id, True, timeout=120)
                decision = wait_for([self.record_file(role, request_id, 'scoped-decision')
                                     for role in others], read_json)
                assert decision['request'] == accepted['marker']['stamp']['request'], decision
                assert 'commit' in decision['decision'], decision
                assert self.table.get(5) == {'id': 5, 'value': 105}
        finally:
            release.touch()
        assert self.table.update(5, {'value': 106}) is True
        self.accepted(self.submit(primary, request), request_id)
        assert self.terminal(primary, request_id, True) == terminal
        assert self.table.get(5) == {'id': 5, 'value': 106}
        report('O31/R01/R03 takeover after acceptance', original_owner=self.worker_identity(self.owner),
               exact_binding=decision['request'], later_row=self.table.get(5), recovered_receipt=terminal)

    def run(self, binary, *, owner_loss_only=False):
        os.environ['NABEDRA_SERVER_BINARY'] = binary
        with self.stack:
            self.stack.callback(self.database.close)
            gateway = self.start_second_gateway(binary)
            primary, secondary = self.client(), self.client(SECONDARY)
            self.exercise_orm()
            if owner_loss_only:
                self.takeover_after_acceptance(primary)
                return
            with ThreadPoolExecutor(max_workers=2) as executor:
                self.identical_submission(primary, secondary, executor)
                mutations = self.lost_responses(primary, secondary)
                if self.instrumented:
                    self.minority_admission(COPY_MARKER, 3, primary, secondary, executor)
                else:
                    self.release_pause_check(primary)
                self.query_expiration(primary, secondary, mutations)
                if self.instrumented:
                    self.minority_admission(COPY_PAYLOAD, 4, primary, secondary, executor, gateway)
                    self.takeover_after_acceptance(primary)
        report('coverage', build='registered test child' if self.instrumented else 'release binary',
               serialized_copy_checkpoints=self.instrumented,
               o30_asserted=(['two follower payload copies held', 'two follower marker copies held',
                              'two follower terminal copies held', 'quorum completed with one follower held']
                             if self.instrumented else ['scoped completion with one whole follower paused']),
               o30_unexecuted=(['unrelated worker', 'whole worker loss', 'membership changes']
                               if self.instrumented else ['unrelated worker', 'whole worker loss',
                               'exact per-copy minority cuts and individual acknowledgements']))


def launch_driver(cluster, module, barriers, test_executable, *, owner_loss_only=False):
    args = ['/source/server/tests/oltp_scoped_acceptance.py', '--worktree', '/source',
            '--driver', '--binary', cluster.binary]
    if test_executable:
        args += ['--test-executable', '/fixture-test']
    if owner_loss_only:
        args.append('--owner-loss-only')
    driver = cluster.launch('driver', module.DRIVER_CPU, module.DRIVER_MEMORY, 'python3', args,
                           options=('--network-alias', 'api2',
                                    '--mount', f'type=bind,source={barriers},target={BARRIERS}'))
    return driver


def supervise_driver(cluster, driver, barriers, workers, paused):
    command_path = barriers / 'host-fault.json'
    deadline = time.monotonic() + DRIVER_TIMEOUT
    while time.monotonic() < deadline:
        if command_path.exists():
            command = read_json(command_path)
            command_id = str(UUID(command['command_id']))
            answer = barriers / f'host-fault-{command_id}.json'
            if not answer.exists():
                role = command['worker']
                name = workers[role]
                should_pause = command['paused']
                assert type(should_pause) is bool
                if should_pause:
                    assert not paused, paused
                    paused.add(role)
                else:
                    assert role in paused, paused
                cluster.docker('pause' if should_pause else 'unpause', name, timeout=8)
                state = json.loads(cluster.docker('inspect', '--format', '{{json .State}}', name, timeout=8))
                assert state['Running'] and state['Paused'] is should_pause, state
                if not should_pause:
                    paused.remove(role)
                write_json(answer, command)
        state = json.loads(cluster.docker('inspect', '--format', '{{json .State}}', driver, timeout=8))
        if not state['Running']:
            if state['ExitCode']:
                raise RuntimeError(f'OLTP scoped acceptance exited {state["ExitCode"]}. See the driver log.')
            return
        time.sleep(0.25)
    raise TimeoutError(f'OLTP scoped acceptance driver exceeded {DRIVER_TIMEOUT} seconds')


def require_docker_lane(module, idle_containers):
    running = set(module.command('docker', 'ps', '--format', '{{.Names}}').splitlines())
    if running.difference(idle_containers):
        raise RuntimeError('another Docker workload is using the capped fixture lane')


def start_fixture(module, args):
    require_docker_lane(module, args.idle_docker_container)
    args.output = args.output.resolve()
    args.output.mkdir(parents=True)
    args.binary = args.binary.resolve(strict=True)
    test_executable = args.test_executable.resolve(strict=True) if args.test_executable else None
    args.worker_cpus = module.DEFAULT_WORKER_CPUS
    args.status_seed_worker = None
    args.tls_key_type = 'rsa'
    args.scoped_oltp = True
    module.EXECUTION = module.EXECUTION | {'result_retention_ms': RETENTION_MS}
    worker_prefix = uuid4().int & ~0xff
    enrollments, api_enrollments, workers = {}, {}, {}

    class Fixture(module.Cluster):
        def certificate(self, name):
            enrollment = super().certificate(name)
            if name in module.WORKERS:
                enrollment['worker_id'] = str(UUID(int=worker_prefix + module.WORKERS.index(name) + 1))
                enrollments[name] = enrollment
            else:
                api_enrollments[name] = {field: enrollment[field] for field in IDENTITY_FIELDS}
            return enrollment

        def launch(self, alias, cpu, memory, entry, arguments, *, options=()):
            if test_executable and alias in module.WORKERS:
                original = 'exec "$2" worker --bootstrap "/tmp/config/$1.json"'
                replacement = (
                    'export NABEDRA_OLTP_TEST_REGISTERED_CONFIG="/tmp/config/$1.json"\n'
                    f'export NABEDRA_OLTP_TEST_BARRIER_DIR="{BARRIERS}/$1"\n'
                    f'exec /fixture-test --exact {CHILD} --ignored --nocapture'
                )
                script = arguments[1].replace(original, replacement)
                assert script != arguments[1], 'registered worker launch command changed'
                arguments = [arguments[0], script, *arguments[2:]]
                options = (*options,
                           '--mount', f'type=bind,source={test_executable},target=/fixture-test,readonly',
                           '--mount', f'type=bind,source={barriers},target={BARRIERS}')
            container = super().launch(alias, cpu, memory, entry, arguments, options=options)
            if alias in module.WORKERS:
                workers[alias] = container
            return container

    with tempfile.TemporaryDirectory(prefix='nabedra-oltp-scoped-', dir=args.output) as temporary:
        fixture = Path(temporary)
        barriers = fixture / 'barriers'
        for role in module.WORKERS:
            (barriers / role).mkdir(parents=True)
        cluster = Fixture(args, fixture)
        paused = set()
        try:
            cluster.start()
            secondary = {'database': module.DATABASE, 'enrollment': cluster.certificate('api2')}
            control = module.Http(cluster.host_url('w1'), cluster.auth['admin_token'], 15)
            try:
                request = envelope('api.enroll', {'expected': None, 'next': secondary})
                module.require_data(*control.exchange(ADMIN, request), request['request_id'])
            finally:
                control.close()
            config = read_json(fixture / 'api.json')
            config['bind'] = '127.0.0.1:8001'
            config['auth_file'] = '/fixture/auth.json'
            config['tls'] = dict(cert_file='/fixture/api2.pem', key_file='/fixture/api2.key')
            config['topology']['enrollment'] = secondary
            config['topology']['worker_tls'] = config['tls'] | {'ca_file': '/fixture/ca.pem'}
            write_json(fixture / 'api2.json', config)
            metadata = cluster.metadata | dict(
                enrollments=enrollments, api_enrollments=api_enrollments,
                result_retention_ms=RETENTION_MS, instrumented=bool(test_executable),
                cluster_budget={'cpu': 4, 'memory_bytes': 7_900_000_000},
                secondary_api_capped_with_driver=True,
                acceptance_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            )
            if test_executable:
                with test_executable.open('rb') as binary:
                    metadata['registered_test_child_sha256'] = hashlib.file_digest(binary, 'sha256').hexdigest()
            write_json(fixture / 'scoped-metadata.json', metadata)
            write_json(args.output / 'fixture.json', metadata)
            driver = launch_driver(cluster, module, barriers, test_executable,
                                   owner_loss_only=args.owner_loss_only)
            supervise_driver(cluster, driver, barriers, workers, paused)
        finally:
            cleanup_errors = []
            for role in paused:
                try:
                    cluster.docker('unpause', workers[role], timeout=8)
                except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                    cleanup_errors.append(str(error))
            errors = cluster.finish() + cleanup_errors
            shutil.copytree(barriers, args.output / 'observed')
            if errors and sys.exc_info()[0] is None:
                raise RuntimeError(f'fixture cleanup or resource verification failed: {errors}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worktree', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--image')
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--test-executable', type=Path)
    parser.add_argument('--owner-loss-only', action='store_true',
                        help='run ordinary ORM operations followed by the owner-loss cut')
    parser.add_argument('--rpc-trace', action='store_true', help='retain bounded worker and gateway RPC diagnostics')
    parser.add_argument('--idle-docker-container', action='append', default=[],
                        help='allow an existing Docker daemon container that was verified idle')
    parser.add_argument('--driver', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.owner_loss_only and not args.test_executable:
        parser.error('--owner-loss-only requires --test-executable')
    sys.path.insert(0, str(args.worktree / 'scripts'))
    import benchmark_oltp_nabedra

    if args.driver:
        Driver(benchmark_oltp_nabedra, Path('/fixture'), bool(args.test_executable)).run(
            str(args.binary), owner_loss_only=args.owner_loss_only)
    elif args.output and args.image:
        start_fixture(benchmark_oltp_nabedra, args)
    else:
        parser.error('--output and --image are required')


if __name__ == '__main__':
    main()

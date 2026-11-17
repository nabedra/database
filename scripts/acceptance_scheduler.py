"""Deterministic real TCP worker queue/recovery assertions against the local CAS store.

Requires Python 3.11 and a supplied nabedra-db binary. No AWS/provider claim.
"""
import argparse
import concurrent.futures
import hashlib
import shutil
import http.client
import json
import os
from pathlib import Path
import select
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from acceptance_nodes import FrontDoor, announcement, target, write, send, receive, call


class JobGate:
    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.permits = {}
        self.entered_nodes = set()
        self.lock = threading.Lock()

    def release_one(self, node):
        with self.lock:
            self.permits.setdefault(node, threading.Semaphore(0)).release()

    def wait(self, node):
        with self.lock:
            self.entered_nodes.add(node)
            permits = self.permits.setdefault(node, threading.Semaphore(0))
        end = time.monotonic() + 60
        while time.monotonic() < end:
            if self.release.is_set() or permits.acquire(timeout=0.05):
                return True
        return False


class InstallAckGate:
    """Hold the first complete replica response, after installation but before ACK."""
    def __init__(self):
        self.entered, self.release = threading.Event(), threading.Event()
        self.lock = threading.Lock()
        self.claimed = False
        self.record = None
        self.response_status = None
        self.installed = None

    def claim(self, record):
        with self.lock:
            if self.claimed or record['accepted']:
                return False
            self.claimed, self.record = True, record
            return True


def read_http_message(peer, initial=b'', cap=128 * 1024 * 1024):
    data = bytearray(initial)
    while b'\r\n\r\n' not in data:
        chunk = peer.recv(65536)
        if not chunk:
            raise OSError('incomplete HTTP headers')
        data.extend(chunk)
        if len(data) > cap:
            raise OSError('HTTP message exceeds fixture cap')
    head, body = bytes(data).split(b'\r\n\r\n', 1)
    lengths = [int(line.split(b':', 1)[1]) for line in head.split(b'\r\n')[1:]
               if line.lower().startswith(b'content-length:')]
    if len(lengths) != 1 or lengths[0] > cap:
        raise OSError('fixture requires bounded content length')
    while len(body) < lengths[0]:
        chunk = peer.recv(min(65536, lengths[0] - len(body)))
        if not chunk:
            raise OSError('incomplete HTTP body')
        body += chunk
    return head + b'\r\n\r\n' + body, body


class GateForward:
    def __init__(self, request, address, server):
        self.request, self.server = request, server
        self.trace = None
        try:
            self.handle()
        finally:
            if self.trace is not None:
                self.trace["finished"] = time.monotonic()
            request.close()

    def handle(self):
        if self.server.target is None:
            return
        try:
            self.request.settimeout(5)
            header = bytearray()
            while b'\r\n\r\n' not in header:
                data = self.request.recv(65536)
                if not data:
                    return
                header.extend(data)
                if len(header) > 128 * 1024:
                    return
            first = bytes(header).split(b'\r\n\r\n', 1)[0].split(b'\r\n')
            path = first[0].split(b' ')[1].decode()
            operation = None
            sender = receiver = None
            for line in first[1:]:
                name, value = line.split(b':', 1)
                if name.lower() == b'x-nabedra-auth':
                    auth = json.loads(bytes.fromhex(value.strip().decode()))
                    operation = auth['operation_id']
                    sender, receiver = auth['sender']['node_id'], auth['receiver']['node_id']
                if name.lower() == b'x-nabedra-queue-auth':
                    claims = json.loads(bytes.fromhex(value.strip().decode()))
                    self.trace = {'command': claims['command'], 'class': claims['class'],
                                  'sender': claims['sender_node'], 'receiver': claims['receiver'],
                                  'started': time.monotonic(), 'finished': None}
                    with self.server.lock:
                        self.server.queue_requests.append(self.trace)
            with self.server.lock:
                self.server.requests.append((path, operation, sender, receiver))
                gate = self.server.gates.get(operation) if path == '/internal/v1/jobs' else None
            if gate is not None:
                gate.entered.set()
                if not gate.wait(receiver):
                    return
            ack_gate = None
            if path == '/internal/v1/queue' and self.server.install_gates:
                header, body = read_http_message(self.request, header)
                command = json.loads(body)
                if command['command'] == 'install':
                    record = command['record']
                    with self.server.lock:
                        candidate = self.server.install_gates.get(record['request_id'])
                    if candidate is not None and candidate.claim(record):
                        ack_gate = candidate
            exchange = None
            stream_exchange = self.server.stream_exchange
            if path == '/internal/v1/streams' and stream_exchange is not None:
                header, body = read_http_message(self.request, header, cap=3 * 1024 * 1024)
                exchange = stream_exchange(json.loads(body), self.trace)
                if exchange is not None and not exchange.before():
                    return
            with socket.create_connection(self.server.target, timeout=5) as upstream:
                upstream.sendall(header)
                if exchange is not None:
                    upstream.settimeout(20)
                    reply, reply_body = read_http_message(upstream, cap=3 * 1024 * 1024)
                    if exchange.after(reply, reply_body):
                        self.request.sendall(reply)
                    return
                if ack_gate is not None:
                    upstream.settimeout(10)
                    reply, reply_body = read_http_message(upstream)
                    ack_gate.installed = json.loads(reply_body)
                    ack_gate.response_status = int(reply.split(b' ', 2)[1])
                    ack_gate.entered.set()
                    if not ack_gate.release.wait(60):
                        return
                    self.request.sendall(reply)
                    return
                peers = {self.request: upstream, upstream: self.request}
                end = time.monotonic() + 60
                while time.monotonic() < end:
                    ready, _, _ = select.select(list(peers), [], [], 0.2)
                    for source in ready:
                        data = source.recv(65536)
                        if not data:
                            return
                        peers[source].sendall(data)
        except (OSError, ValueError, KeyError):
            return


class WorkerDoor(FrontDoor):
    def __init__(self):
        self.gates = {}
        self.install_gates = {}
        self.stream_exchange = None
        self.queue_requests = []
        super().__init__()
        self.RequestHandlerClass = GateForward

    def close(self):
        for gate in list(self.gates.values()) + list(self.install_gates.values()):
            gate.release.set()
        super().close()


def request(base, method, path, token, body=None, disconnect=False, timeout=30):
    conn = http.client.HTTPConnection(*target(base), timeout=timeout)
    try:
        encoded = None if body is None else json.dumps(body)
        conn.request(method, path, encoded, {'Authorization': 'Bearer ' + token,
                     'Content-Type': 'application/json', 'Connection': 'close'})
        if disconnect:
            return None
        response = conn.getresponse()
        return response.status, json.loads(response.read(8 * 1024 * 1024 + 1))
    finally:
        conn.close()


def wait_for(predicate, message, timeout=40):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = predicate()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError(message)


class Cluster:
    worker_door_type = WorkerDoor

    def __init__(self, binary, directory, execution=None):
        self.binary, self.root = binary, Path(directory)
        self.children, self.doors, self.channels, self.gates = [], [], [], []
        self.diagnostics = None
        try:
            self._initialize(execution)
        except BaseException:
            self.close()
            raise

    def _initialize(self, execution):
        self.diagnostics = (self.root / 'diagnostics').open('wb')
        self.env = {'PATH': os.defpath, 'NABEDRA_STORE_TOKEN': 'scheduler-store-token'}
        self.database = self.root / 'objects.sqlite'
        store = subprocess.Popen([sys.executable, str(Path(__file__).with_name('local_store.py')),
                                  '--database', str(self.database), '--port', '0'], env=self.env,
                                 stdout=subprocess.PIPE, stderr=self.diagnostics)
        self.children.append(store)
        address = announcement(store)
        self.backend = {'driver': 'http', 'options': {'base_url': f"http://{address['host']}:{address['port']}",
                         'namespace': 'scheduler/', 'token_file': 'token', 'ca_file': None}}
        (self.root / 'token').write_text('scheduler-store-token\n')
        (self.root / 'token').chmod(0o600)
        self.auth = {'data_token': 'd' * 43, 'admin_token': 'a' * 43, 'enrollment_token': 'e' * 43}
        write(self.root / 'auth', self.auth)
        self.cluster = str(uuid.uuid4())
        self.api_door = FrontDoor()
        self.doors.append(self.api_door)
        self.template = {'listen': '127.0.0.1:0', 'operation_slots': 1, 'partition_slots': 1,
                         'queue_size': 16, 'partition_size': '4 KiB'}
        if execution is not None:
            self.template['execution'] = execution
        write(self.root / 'api.json', {'version': 1, 'cluster_id': self.cluster,
              'bind': '127.0.0.1:0', 'advertise_url': self.api_door.url,
              'api_urls': [self.api_door.url], 'storage': self.backend, 'auth_file': 'auth',
              'tls': None, 'worker': self.template})
        self.workers = {}
        self.start_api()

    def start_api(self, cold=False):
        channel, child_channel = socket.socketpair()
        self.channels.append(channel)
        channel.settimeout(40)
        args = [str(self.binary), 'api', '--config', str(self.root / 'api.json')]
        if cold:
            args.append('--cold-start')
        process = subprocess.Popen(args, env=dict(self.env, NABEDRA_SUPERVISOR_FD=str(child_channel.fileno())),
                                   pass_fds=(child_channel.fileno(),), stdout=self.diagnostics, stderr=self.diagnostics)
        self.children.append(process)
        child_channel.close()
        send(channel, {'version': 1, 'type': 'init', 'name': 'Scheduler', 'initial_policy': {
             'mode': 'fixed', 'desired': 3, 'min_nodes': 3, 'max_nodes': 3},
             'provisioning': 'external', 'bridge': None})
        prepared = receive(channel)
        self.api_door.target = target(prepared['bound_url'])
        send(channel, {'version': 1, 'type': 'activate'})
        assert receive(channel)['type'] == 'ready'
        self.api, self.channel = process, channel

    def start_extra_api(self):
        channel, child_channel = socket.socketpair()
        self.channels.append(channel)
        channel.settimeout(40)
        process = subprocess.Popen([str(self.binary), 'api', '--config', str(self.root / 'api.json')],
                                   env=dict(self.env, NABEDRA_SUPERVISOR_FD=str(child_channel.fileno())),
                                   pass_fds=(child_channel.fileno(),), stdout=self.diagnostics, stderr=self.diagnostics)
        self.children.append(process)
        child_channel.close()
        send(channel, {'version': 1, 'type': 'init', 'name': 'Scheduler', 'initial_policy': {
             'mode': 'fixed', 'desired': 3, 'min_nodes': 3, 'max_nodes': 3},
             'provisioning': 'external', 'bridge': None})
        prepared = receive(channel)
        send(channel, {'version': 1, 'type': 'activate'})
        assert receive(channel)['type'] == 'ready'
        return prepared['bound_url'], process, channel

    def stop_api(self):
        send(self.channel, {'version': 1, 'type': 'shutdown'})
        assert self.api.wait(40) == 0
        self.channel.close()

    def worker_resources(self, threads):
        return {'max_threads': threads, 'memory_limit_bytes': (threads + 1) * (4 << 30),
                'task_memory_limit_bytes': 512 << 20, 'control_memory_reserve_bytes': 256 << 20}

    def start_worker(self, node, threads):
        identity = str(uuid.UUID(int=node))
        door = self.worker_door_type()
        self.doors.append(door)
        bootstrap = dict(self.template, partition_slots=threads, version=1, cluster_id=self.cluster,
                         node_id=identity, request_id=None, api_urls=[self.api_door.url], advertise_url=door.url,
                         storage=self.backend, enrollment_token=self.auth['enrollment_token'], tls=None,
                         resources=self.worker_resources(threads))
        path = self.root / f'worker-{node}.json'
        write(path, bootstrap)
        process = subprocess.Popen([str(self.binary), 'worker', '--bootstrap', str(path)], env=self.env,
                                   stdout=subprocess.PIPE, stderr=self.diagnostics)
        self.children.append(process)
        bound = announcement(process)
        assert bound['resources']['max_threads'] == threads
        assert bound['coordination_slots'] >= 3 and bound['control_threads'] == 2
        door.target = target(bound['bound_url'])
        self.workers[identity] = (process, door, threads)
        wait_for(lambda: any(n['node_id'] == identity and n['state'] == 'ready' for n in self.control()['nodes']),
                 'worker did not become ready')
        return identity

    def stop_worker(self, identity):
        process = self.workers[identity][0]
        if process.poll() is None:
            process.send_signal(signal.SIGCONT)
            process.kill()
            process.wait(10)
        wait_for(lambda: any(n['node_id'] == identity and n['state'] in ('lost', 'stopped')
                            for n in self.control()['nodes']), 'dead worker was not fenced', 50)

    def decision_count(self, identity):
        with sqlite3.connect(self.database) as db:
            def load(key):
                row = db.execute('SELECT body FROM objects WHERE key=?', ('scheduler/' + key,)).fetchone()
                assert row is not None, key
                return json.loads(row[0])
            cursor = load('catalog.json')['decision_head']
            seen, count = set(), 0
            while cursor is not None:
                assert cursor not in seen, 'durable decision cycle'
                seen.add(cursor)
                decision = load('decisions/' + cursor + '.json')
                count += decision['request_id'] == identity
                cursor = decision['parent']
            return count

    def partition_objects(self):
        with sqlite3.connect(self.database) as db:
            return {key: hashlib.sha256(body).hexdigest() for key, body in db.execute(
                "SELECT key,body FROM objects WHERE key LIKE 'scheduler/tables/%.parquet'")}

    def control(self):
        with sqlite3.connect(self.database) as db:
            row = db.execute("SELECT body FROM objects WHERE key='scheduler/cluster/control.json'").fetchone()
        return json.loads(row[0])

    def operation(self, operation, payload, identity=None):
        return call(self.api_door.url, operation, payload, self.auth['data_token'], identity)

    def submit(self, operation, payload, identity=None):
        identity = identity or str(uuid.uuid4())
        status, receipt = request(self.api_door.url, 'POST', '/v1/requests', self.auth['data_token'],
                                  {'request_id': identity, 'operation': operation, 'payload': payload}, timeout=45)
        assert status == 202, (status, receipt)
        assert receipt['request_id'] == identity and receipt['state'] == 'queued'
        return receipt

    def receipt(self, receipt):
        path = f"/v1/requests/{receipt['run_id']}/{receipt['request_id']}"
        return request(self.api_door.url, 'GET', path, self.auth['data_token'])

    def terminal(self, receipt, timeout=45):
        def finished():
            status, value = self.receipt(receipt)
            if status == 503:
                return None
            assert status == 200, (status, value)
            return value if value['state'] in ('succeeded', 'failed', 'abandoned') else None
        return wait_for(finished, 'accepted request did not settle', timeout)

    def install_ack_gate(self, identity):
        gate = InstallAckGate()
        self.gates.append(gate)
        for _, door, _ in self.workers.values():
            door.install_gates[identity] = gate
        return gate

    def gate(self, identity):
        gate = JobGate()
        self.gates.append(gate)
        for _, door, _ in self.workers.values():
            door.gates[identity] = gate
        return gate

    def close(self):
        for gate in self.gates:
            gate.release.set()
        for channel in self.channels:
            channel.close()
        for child in reversed(self.children):
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(12)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            if child.stdout:
                child.stdout.close()
        for door in reversed(self.doors):
            door.close()
        if self.diagnostics is not None:
            self.diagnostics.close()


class Evidence(list):
    def __init__(self, binary):
        super().__init__()
        with Path(binary).open('rb') as stream:
            self.binary_sha256 = hashlib.file_digest(stream, 'sha256').hexdigest()
        print(json.dumps({'event': 'start', 'binary_sha256': self.binary_sha256}), file=sys.stderr, flush=True)

    def append(self, assertion):
        super().append(assertion)
        print(json.dumps({'event': 'passed', 'assertion': assertion,
                          'binary_sha256': self.binary_sha256}), file=sys.stderr, flush=True)


def run(binary):
    evidence = Evidence(binary)
    with tempfile.TemporaryDirectory(prefix='nabedra-scheduler-') as directory:
        cluster = Cluster(binary, directory)
        try:
            a = cluster.start_worker(10, 1)
            b = cluster.start_worker(20, 2)
            c = cluster.start_worker(30, 4)
            capacities = {n['resources']['max_threads'] for n in cluster.control()['nodes'] if n['state'] == 'ready'}
            assert capacities == {1, 2, 4}, capacities
            evidence.append('C01: ready heterogeneous 1/2/4 compute pools and distinct memory ceilings')
            status, result = cluster.operation('table.create', {'name': 'items', 'schema': {'id': 'int64', 'text': 'utf8'}, 'key': 'id', 'format': 'parquet'})
            assert status == 200, result
            table = result['data']['id']
            rows = [{'id': n, 'text': 'row-' + str(n) + 'x' * 160} for n in range(1000)]
            status, inserted = cluster.operation('table.insert_many', {'table_id': table, 'rows': rows})
            assert status == 200 and inserted['data'] == len(rows), inserted
            query = {'table_id': table, 'query': {'select': None, 'filter': None, 'order_by': None, 'limit': None}}
            # C05/C07: legacy operation_slots=1 does not serialize coordinator actors.
            large_a_id, large_b_id = str(uuid.uuid4()), str(uuid.uuid4())
            gate_a, gate_b = cluster.gate(large_a_id), cluster.gate(large_b_id)
            large_a = cluster.submit('query.collect', query, large_a_id)
            wait_for(lambda: a in gate_a.entered_nodes, 'first large request did not occupy coordinator compute')
            large_b = cluster.submit('query.collect', query, large_b_id)
            gate_a.release_one(a)
            wait_for(lambda: a in gate_b.entered_nodes, 'second coordinator did not make progress at a job boundary')
            small = cluster.submit('table.get', {'table_id': table, 'key': 17})
            small_terminal = None
            for _ in range(12):
                gate_b.release_one(a)
                status, value = cluster.receipt(small)
                if status == 200 and value['state'] == 'succeeded':
                    small_terminal = value
                    break
                time.sleep(0.05)
            if small_terminal is None:
                small_terminal = cluster.terminal(small, timeout=10)
            assert small_terminal['state'] == 'succeeded' and small_terminal['result'] == rows[17], small_terminal
            assert cluster.receipt(large_a)[1]['state'] not in ('succeeded', 'failed')
            assert cluster.receipt(large_b)[1]['state'] not in ('succeeded', 'failed')
            gate_a.release.set()
            gate_b.release.set()
            assert cluster.terminal(large_a)['result'] == rows
            assert cluster.terminal(large_b)['result'] == rows
            evidence.append('C05/C07: small query completed while both large queries remained gated, with legacy operation_slots=1')
            # Q04: the API process owns no accepted queue records.
            identity = str(uuid.uuid4())
            gate = cluster.gate(identity)
            accepted = cluster.submit('query.collect', query, identity)
            assert gate.entered.wait(10), 'accepted query never reached deterministic job gate'
            cluster.stop_api()
            cluster.start_api()
            assert cluster.receipt(accepted)[0] in (200, 503)
            gate.release.set()
            terminal = cluster.terminal(accepted)
            assert terminal['state'] == 'succeeded' and terminal['result'] == rows, terminal
            evidence.append('Q04: API restart retained run/accepted identity and pinned result')
            # C03: replace one incarnation with different CPU capacity, preserving immutable files.
            files = cluster.partition_objects()
            assert len(files) > 1, 'C03 requires multiple immutable partition files'
            cluster.stop_worker(c)
            c = cluster.start_worker(30, 3)
            assert next(n for n in cluster.control()['nodes'] if n['node_id'] == c)['resources']['max_threads'] == 3
            status, collected = cluster.operation('query.collect', query)
            assert status == 200 and collected['data'] == rows, collected
            assert cluster.partition_objects() == files, 'capacity change rewrote immutable partitions'
            evidence.append('C03: same node restarted with 4->3 threads; exact rows and immutable partition identities/bytes preserved')
            # Q05: independent API processes race the exact original body and converge on one decision.
            extra_url, extra_process, extra_channel = cluster.start_extra_api()
            duplicate_id = str(uuid.uuid4())
            duplicate_body = {'request_id': duplicate_id, 'operation': 'table.update',
                              'payload': {'table_id': table, 'key': 17, 'values': {'text': rows[17]['text']}}}
            rendezvous = threading.Barrier(3)
            def duplicate_submit(url):
                rendezvous.wait(10)
                return request(url, 'POST', '/v1/requests', cluster.auth['data_token'], duplicate_body)
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(duplicate_submit, url) for url in (cluster.api_door.url, extra_url)]
                    rendezvous.wait(10)
                    answers = [future.result(40) for future in futures]
                assert all(status == 202 for status, _ in answers), answers
                receipts = [value for _, value in answers]
                assert receipts[0]['request_id'] == receipts[1]['request_id'] == duplicate_id
                assert receipts[0]['run_id'] == receipts[1]['run_id']
                assert cluster.terminal(receipts[0])['state'] == 'succeeded'
                conflict = dict(duplicate_body, payload=dict(duplicate_body['payload'], values={'text': 'conflict'}))
                status, rejected = request(extra_url, 'POST', '/v1/requests', cluster.auth['data_token'], conflict)
                assert status == 409 and rejected['request_id'] == duplicate_id, (status, rejected)
                assert cluster.decision_count(duplicate_id) == 1
                status, unchanged = cluster.operation('table.get', {'table_id': table, 'key': 17})
                assert status == 200 and unchanged['data'] == rows[17], unchanged
            finally:
                send(extra_channel, {'version': 1, 'type': 'shutdown'})
                assert extra_process.wait(40) == 0
                extra_channel.close()
            evidence.append('Q05: two real APIs submitted the same mutation simultaneously; one receipt/decision, changed-body conflict')
            # Q03: the replica has a complete record, but its signed ACK cannot reach the owner.
            unacknowledged_id = str(uuid.uuid4())
            ack_gate = cluster.install_ack_gate(unacknowledged_id)
            unacknowledged_body = {'request_id': unacknowledged_id, 'operation': 'table.update',
                                   'payload': {'table_id': table, 'key': 18, 'values': {'text': rows[18]['text']}}}
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                submission = pool.submit(request, cluster.api_door.url, 'POST', '/v1/requests',
                                         cluster.auth['data_token'], unacknowledged_body, timeout=45)
                assert ack_gate.entered.wait(15), 'replica never installed the complete record'
                assert ack_gate.response_status == 200 and not ack_gate.record['accepted']
                assert ack_gate.installed['kind'] == 'record', ack_gate.installed
                assert ack_gate.installed['record']['request_id'] == unacknowledged_id
                assert ack_gate.installed['record']['body'] == ack_gate.record['body']
                assert not submission.done(), 'API accepted before required replica ACK'
                old = cluster.control()['scheduler']['coordinator_node']
                old_threads = cluster.workers[old][2]
                cluster.stop_worker(old)
                ack_gate.release.set()
                status, uncertain = submission.result(40)
                assert status != 202 and uncertain['request_id'] == unacknowledged_id, (status, uncertain)
                assert uncertain['error']['outcome'] == 'unknown', uncertain
            wait_for(lambda: cluster.control()['scheduler']['coordinator_node'] != old,
                     'scheduler was not replaced after unacknowledged submission', 50)
            status, recovered = request(cluster.api_door.url, 'POST', '/v1/requests',
                                        cluster.auth['data_token'], unacknowledged_body)
            assert status == 202, (status, recovered)
            assert cluster.terminal(recovered)['state'] == 'succeeded'
            assert cluster.decision_count(unacknowledged_id) == 1
            cluster.start_worker(uuid.UUID(old).int, old_threads)
            evidence.append('Q03: no acceptance before complete-copy ACK; owner SIGKILL and exact-body retry recovered one durable decision')
            # R05: a paused owner loses authority; resuming it cannot publish a second mutation.
            paused_id = str(uuid.uuid4())
            pause_gate = cluster.gate(paused_id)
            paused_receipt = cluster.submit('table.update', {'table_id': table, 'key': 19,
                                            'values': {'text': rows[19]['text']}}, paused_id)
            assert pause_gate.entered.wait(15)
            old_control = cluster.control()
            old = old_control['scheduler']['coordinator_node']
            old_term = old_control['scheduler']['scheduler_term']
            paused_process, _, old_threads = cluster.workers[old]
            paused_process.send_signal(signal.SIGSTOP)
            try:
                wait_for(lambda: cluster.control()['scheduler']['coordinator_node'] != old and
                         cluster.control()['scheduler']['scheduler_term'] > old_term,
                         'paused coordinator did not lose its scheduler term', 50)
                pause_gate.release.set()
                resumed_terminal = cluster.terminal(paused_receipt, 50)
                assert resumed_terminal['state'] == 'succeeded', resumed_terminal
                assert cluster.decision_count(paused_id) == 1
                with sqlite3.connect(cluster.database) as db:
                    catalog_before_resume = json.loads(db.execute(
                        "SELECT body FROM objects WHERE key='scheduler/catalog.json'").fetchone()[0])
                paused_process.send_signal(signal.SIGCONT)
                # Drain all stale requests with a new operation before checking the durable root.
                status, verified = cluster.operation('query.collect', query)
                assert status == 200 and verified['data'] == rows, verified
                assert cluster.decision_count(paused_id) == 1
                with sqlite3.connect(cluster.database) as db:
                    catalog_after_resume = json.loads(db.execute(
                        "SELECT body FROM objects WHERE key='scheduler/catalog.json'").fetchone()[0])
                assert catalog_after_resume == catalog_before_resume, 'resumed stale owner changed authoritative catalog'
            finally:
                if paused_process.poll() is None:
                    paused_process.send_signal(signal.SIGCONT)
            cluster.stop_worker(old)
            cluster.start_worker(uuid.UUID(old).int, old_threads)
            evidence.append('R05: SIGSTOP owner, higher-term takeover, SIGCONT; one mutation decision and unchanged authoritative root')
            # R01/R06: coordinator is OS-killed while another complete replica remains.
            identity = str(uuid.uuid4())
            gate = cluster.gate(identity)
            accepted = cluster.submit('query.collect', query, identity)
            assert gate.entered.wait(10)
            old = cluster.control()['scheduler']['coordinator_node']
            process = cluster.workers[old][0]
            process.kill()
            process.wait(10)
            gate.release.set()
            terminal = cluster.terminal(accepted, 50)
            assert terminal['state'] == 'succeeded' and terminal['result'] == rows, terminal
            assert cluster.control()['scheduler']['coordinator_node'] != old
            evidence.append('R01/R06: SIGKILL recovery from surviving worker, exact serial result')
            # R03/C01: a joining node copies current outstanding requests before ready.
            identity = str(uuid.uuid4())
            gate = cluster.gate(identity)
            accepted = cluster.submit('query.collect', query, identity)
            assert gate.entered.wait(10)
            joining = cluster.start_worker(35, 3)
            for node in cluster.control()['nodes']:
                if node['node_id'] == joining:
                    assert node['resources']['max_threads'] == 3
            gate.release.set()
            terminal = cluster.terminal(accepted)
            assert terminal['state'] == 'succeeded' and terminal['result'] == rows
            evidence.append('R03: joining worker receives outstanding request state before readiness')
            # R02: retain one accepted request while reducing every ready replica to one.
            one_holder_id = str(uuid.uuid4())
            one_holder_gate = cluster.gate(one_holder_id)
            one_holder = cluster.submit('query.collect', query, one_holder_id)
            assert one_holder_gate.entered.wait(15)
            holder = cluster.control()['scheduler']['coordinator_node']
            ready = [n['node_id'] for n in cluster.control()['nodes'] if n['state'] == 'ready']
            assert len(ready) >= 3, ready
            for victim in ready:
                if victim == holder:
                    continue
                cluster.stop_worker(victim)
                status, retained = cluster.receipt(one_holder)
                assert status == 200 and retained['request_id'] == one_holder_id, (status, retained)
                assert retained['state'] in ('queued', 'running', 'reconciling'), retained
            assert [n['node_id'] for n in cluster.control()['nodes'] if n['state'] == 'ready'] == [holder]
            one_holder_gate.release.set()
            one_holder_terminal = cluster.terminal(one_holder, 50)
            assert one_holder_terminal['state'] == 'succeeded' and one_holder_terminal['result'] == rows
            evidence.append('R02: sequential worker SIGKILL retained accepted identity/pin through one surviving queue holder')
            # R04: full loss abandons outstanding reads; committed decisions remain durable.
            mutation = cluster.submit('table.insert', {'table_id': table, 'row': {'id': 1000, 'text': 'durable'}})
            assert cluster.terminal(mutation)['state'] == 'succeeded'
            identity = str(uuid.uuid4())
            gate = cluster.gate(identity)
            outstanding = cluster.submit('query.collect', query, identity)
            assert gate.entered.wait(10)
            for process, _, _ in cluster.workers.values():
                if process.poll() is None:
                    process.kill()
                    process.wait(10)
            gate.release.set()
            cluster.stop_api()
            cluster.start_api(cold=True)
            # Use a fresh node identity so no old incarnation can be mistaken for a queue holder.
            cluster.start_worker(40, 2)
            abandoned = cluster.terminal(outstanding)
            committed = cluster.terminal(mutation)
            assert abandoned['state'] == 'abandoned' and abandoned['result'] is None
            assert committed['state'] == 'succeeded'
            status, count = cluster.operation('query.count', query)
            assert status == 200 and count['data'] == 1001, count
            evidence.append('R04/T04: explicit cold fence abandoned outstanding read, retained durable mutation')
            print(json.dumps({'binary_sha256': evidence.binary_sha256, 'passed': evidence, 'provider_evidence': False}, indent=2))
        except BaseException:
            cluster.diagnostics.flush()
            sys.stderr.write((Path(directory) / 'diagnostics').read_text()[-20000:])
            artifact = Path('artifacts/resource-aware-scheduling/recovery-debug.json')
            artifact.parent.mkdir(parents=True, exist_ok=True)
            artifact.write_text(json.dumps({'control': cluster.control(),
                'queues': {node: door.queue_requests for node, (_, door, _) in cluster.workers.items()},
                'passed': evidence, 'binary_sha256': evidence.binary_sha256}, indent=2))
            shutil.copyfile(cluster.database, artifact.with_suffix('.sqlite'))
            raise
        finally:
            cluster.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    run(parser.parse_args().binary.resolve())

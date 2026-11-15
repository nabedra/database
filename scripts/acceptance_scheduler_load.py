#!/usr/bin/env python3.11
"""Real HTTP scheduler load assertions and measured transport evidence.

Uses the local conditional SQLite object service, never AWS. Run Python 3.11.
A TCP job in flight is a scheduler allocation observation, not CPU occupancy.
"""
import argparse
import collections
import datetime
import concurrent.futures
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import random
import resource
import select
import socket
import socketserver
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from acceptance_scheduler import Cluster, WorkerDoor, request, wait_for
from acceptance_nodes import target, write


MAX_OBSERVED_JSON_BYTES = 2 << 20


def record_metadata(record):
    """Count actual wire-carried payload bytes without retaining user contents."""
    metadata = dict(request_id=record['request_id'], version=record['version'], sequence=record['sequence'],
                state=record['state'], result_expired=record['result_expired'],
                request_body_bytes=len(record['body']), pinned_catalog_bytes=len(record['pinned_catalog'] or []),
                result_payload_bytes=len(record['response_body'] or []), replica_count=len(record['replicas']))
    body = record['response_body']
    if body and len(body) <= 4096:
        response = json.loads(bytes(body))
        if response.get('error') is not None:
            metadata['terminal_error'] = response['error']
    return metadata


def read_chunked(channel, initial, record):
    """Decode bounded job frames while retaining exact bytes for forwarding."""
    wire, decoded, cursor = bytearray(initial), bytearray(), 0
    limit = (16 << 20) + (1 << 20)
    def require(count):
        while len(wire) < count:
            chunk = channel.recv(min(65536, count-len(wire)))
            if not chunk:
                raise ValueError('truncated chunk body')
            wire.extend(chunk)
            record['request_bytes'] += len(chunk)
            if len(wire) > limit:
                raise ValueError('job observation bound')
    def line():
        nonlocal cursor
        while True:
            end = wire.find(b'\r\n', cursor)
            if end >= 0:
                result = bytes(wire[cursor:end])
                cursor = end+2
                return result
            if len(wire)-cursor > 8192:
                raise ValueError('chunk line observation bound')
            require(len(wire)+1)
    while True:
        length = int(line().split(b';', 1)[0], 16)
        if length == 0:
            while line():
                pass
            return bytes(wire), bytes(decoded)
        if length > limit-len(decoded):
            raise ValueError('job observation bound')
        require(cursor+length+2)
        decoded.extend(wire[cursor:cursor+length])
        cursor += length
        if wire[cursor:cursor+2] != b'\r\n':
            raise ValueError('chunk terminator')
        cursor += 2


class TraceForward(socketserver.BaseRequestHandler):
    def handle(self):
        if self.server.target is None:
            return
        record = None
        try:
            self.request.settimeout(5)
            initial = bytearray()
            while b'\r\n\r\n' not in initial:
                chunk = self.request.recv(65536)
                if not chunk:
                    return
                initial.extend(chunk)
                assert len(initial) <= 128 << 10
            header, body = bytes(initial).split(b'\r\n\r\n', 1)
            lines = header.split(b'\r\n')
            path = lines[0].split(b' ')[1].decode('ascii')
            headers = dict((k.lower(), v.strip()) for k, v in (line.split(b':', 1) for line in lines[1:]))
            auth = json.loads(bytes.fromhex(headers[b'x-meshdb-auth'].decode())) if b'x-meshdb-auth' in headers else {}
            operation = auth.get('operation_id')
            queue_auth = json.loads(bytes.fromhex(headers[b'x-meshdb-queue-auth'].decode())) if b'x-meshdb-queue-auth' in headers else {}
            record = dict(method=lines[0].split(b' ')[0].decode('ascii'), path=path, operation_id=operation, node=auth.get('receiver', {}).get('node_id'),
                          sender=auth.get('sender', {}).get('node_id'), job_id=auth.get('job_id'),
                          attempt_id=auth.get('attempt_id'), started=time.monotonic(), forwarded=None,
                          finished=None, request_bytes=len(initial), response_bytes=0, status=None)
            with self.server.lock:
                self.server.requests.append((path, operation, record['sender'], record['node']))
                self.server.trace.append(record)
            if queue_auth:
                record.update(node=queue_auth['receiver'], sender=queue_auth['sender_node'])
                length = int(headers.get(b'content-length', b'0'))
                if length <= MAX_OBSERVED_JSON_BYTES:
                    while len(body) < length:
                        chunk = self.request.recv(min(65536, length - len(body)))
                        if not chunk:
                            return
                        body += chunk
                        record['request_bytes'] += len(chunk)
                    initial = header + b'\r\n\r\n' + body
                    command = json.loads(body)
                    record['queue_command'] = command['command']
                    if command['command'] == 'install':
                        record['installed_payload'] = record_metadata(command['record'])
                else:
                    record['queue_metadata_unobserved'] = 'request exceeds observer capture bound'
            # Jobs use chunked HTTP containing frame-v1 CONTROL/STREAM_END.
            if path == '/internal/v1/jobs':
                if headers.get(b'transfer-encoding') == b'chunked':
                    wire, body = read_chunked(self.request, body, record)
                    initial = header + b'\r\n\r\n' + wire
                else:
                    length = int(headers[b'content-length'])
                    assert 5 <= length <= (16 << 20) + 10
                    while len(body) < length:
                        chunk = self.request.recv(min(65536, length - len(body)))
                        if not chunk:
                            return
                        body += chunk
                        record['request_bytes'] += len(chunk)
                    initial = header + b'\r\n\r\n' + body
                assert body[0] == 1
                control_length = struct.unpack('>I', body[1:5])[0]
                control = json.loads(body[5:5 + control_length])
                record.update(job_id=control['job_id'], attempt_id=control['attempt_id'],
                              ordinal=control['ordinal'], task=control['task']['kind'])
                file = control['task']['payload'].get('file')
                if file is not None and file['kind'] == 'partition':
                    record.update(file=file['payload']['object'], encoded_bytes=file['payload']['byte_size'],
                                  rows=file['payload']['row_count'], payload_bytes=file['payload']['payload_bytes'])
            with self.server.lock:
                gate = self.server.gates.get(operation) if path == '/internal/v1/jobs' else None
            if gate is not None:
                gate.entered.set()
                if not gate.wait(record['node']):
                    record['failure'] = 'gate watchdog'
                    return
            if getattr(self.server, 'partition_read_delay_seconds', 0) and record['method'] == 'GET' and '/tables/' in path:
                record['controlled_storage_delay_seconds'] = self.server.partition_read_delay_seconds
                threading.Event().wait(self.server.partition_read_delay_seconds)
            record['forwarded'] = time.monotonic()
            with socket.create_connection(self.server.target, timeout=5) as upstream:
                upstream.sendall(initial)
                peers = {self.request: upstream, upstream: self.request}
                end = time.monotonic() + 65
                response_header = bytearray()
                error_response = bytearray()
                stats_response = bytearray() if queue_auth else None
                while time.monotonic() < end:
                    ready, _, _ = select.select(list(peers), [], [], .1)
                    for source in ready:
                        chunk = source.recv(65536)
                        if not chunk:
                            if stats_response and b'\r\n\r\n' in stats_response:
                                response_head, raw = stats_response.split(b'\r\n\r\n', 1)
                                response_headers = dict((k.lower(), v.strip()) for k, v in
                                    (line.split(b':', 1) for line in bytes(response_head).split(b'\r\n')[1:]))
                                if len(raw) == int(response_headers.get(b'content-length', b'-1')):
                                    reply = json.loads(raw)
                                    if reply.get('kind') == 'stats':
                                        record['queue_stats'] = reply
                                    elif reply.get('kind') == 'record':
                                        record['reply_payload'] = record_metadata(reply['record'])
                                else:
                                    record['queue_metadata_unobserved'] = 'response incomplete or exceeds observer capture bound'
                            return
                        if source is upstream:
                            if len(error_response) < 8192:
                                error_response.extend(chunk[:8192-len(error_response)])
                            if stats_response is not None and len(stats_response) < MAX_OBSERVED_JSON_BYTES:
                                stats_response.extend(chunk[:MAX_OBSERVED_JSON_BYTES-len(stats_response)])
                            record['response_bytes'] += len(chunk)
                            if record['status'] is None:
                                response_header.extend(chunk)
                                if b'\r\n' in response_header:
                                    record['status'] = int(response_header.split(b' ', 2)[1])
                                    response_header.clear()
                        else:
                            record['request_bytes'] += len(chunk)
                        peers[source].sendall(chunk)
                record['failure'] = 'transport watchdog'
        except (OSError, ValueError, KeyError, AssertionError) as error:
            if record is not None:
                record['failure'] = type(error).__name__
        finally:
            if record is not None:
                if record['status'] not in (None, 200) and 'error_response' in locals() and b'\r\n\r\n' in error_response:
                    try:
                        record['error_reply'] = json.loads(error_response.split(b'\r\n\r\n', 1)[1])
                    except ValueError:
                        pass
                record['finished'] = time.monotonic()


class TraceDoor(WorkerDoor):
    def __init__(self):
        self.trace = []
        super().__init__()
        self.RequestHandlerClass = TraceForward


class LoadCluster(Cluster):
    worker_door_type = TraceDoor

    def __init__(self, binary, directory, execution=None, receipt_poll_interval=.1):
        self.receipt_poll_interval = receipt_poll_interval
        super().__init__(binary, directory, execution)

    def terminal(self, receipt, timeout=120):
        end = time.monotonic()+timeout
        while time.monotonic() < end:
            status, value = self.receipt(receipt)
            if status != 503:
                assert status == 200, (status, value)
                if value['state'] in ('succeeded', 'failed', 'abandoned'):
                    return value
            threading.Event().wait(self.receipt_poll_interval)
        raise AssertionError('accepted request did not settle')

    def start_api(self, cold=False):
        if not hasattr(self, 'store_door'):
            self.store_door = TraceDoor()
            self.store_door.partition_read_delay_seconds = 0
            self.store_door.target = target(self.backend['options']['base_url'])
            self.doors.append(self.store_door)
            self.backend['options']['base_url'] = self.store_door.url
            config = json.loads((self.root / 'api.json').read_text())
            config['storage'] = self.backend
            config['lifecycle'] = dict(heartbeat_ms=250, expiry_ms=5000, readiness_ms=15000, drain_ms=15000,
                lease_ms=10000, renew_ms=2000, stabilization_ms=1500, cooldown_ms=500,
                retry_initial_ms=100, retry_max_ms=1000, max_attempts=8)
            write(self.root / 'api.json', config)
        return super().start_api(cold)

    def storage_traces(self):
        with self.store_door.lock:
            return [dict(record) for record in self.store_door.trace]

    def traces(self, identity=None):
        result = []
        for _, door, _ in self.workers.values():
            with door.lock:
                result.extend(dict(r) for r in door.trace if identity is None or r['operation_id'] == identity)
        return sorted(result, key=lambda r: r['started'])


class Observer:
    """Independent control thread samples process RSS/CPU via the OS ps tool."""
    def __init__(self, cluster):
        self.cluster, self.samples, self.errors = cluster, [], []
        self.heartbeats = []
        self.sequences = {}
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, name='load-os-observer')
        self.thread.start()

    def run(self):
        while not self.stop.is_set():
            workers = list(self.cluster.workers.items())
            try:
                pids = ','.join(str(item[1][0].pid) for item in workers)
                if pids:
                    output = subprocess.check_output(['ps', '-o', 'pid=,rss=,time=', '-p', pids], text=True, timeout=3)
                    lookup = {item[1][0].pid: item[0] for item in workers}
                    for line in output.splitlines():
                        pid, rss, cpu = line.split()
                        self.samples.append(dict(time=time.monotonic(), node=lookup[int(pid)], rss_bytes=int(rss) * 1024,
                                                 cpu_seconds=cpu_seconds(cpu)))
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                self.errors.append(type(error).__name__)
            try:
                with sqlite3.connect(self.cluster.database, timeout=1) as database:
                    persisted = database.execute("SELECT body FROM objects WHERE key LIKE 'scheduler/cluster/heartbeats/%'").fetchall()
                for (body,) in persisted:
                    heartbeat = json.loads(body)
                    node = heartbeat['node_id']
                    if node not in self.cluster.workers or self.sequences.get(heartbeat['incarnation'], 0) >= heartbeat['sequence']:
                        continue
                    resources = heartbeat['resources']
                    used_threads = resources['max_threads'] - heartbeat['available_threads']
                    charged = resources['memory_limit_bytes'] - resources['control_memory_reserve_bytes'] - heartbeat['available_memory_bytes']
                    assert 0 <= used_threads <= resources['max_threads'] and charged >= 0, heartbeat
                    self.heartbeats.append(dict(time=time.monotonic(), node=node, incarnation=heartbeat['incarnation'],
                        sequence=heartbeat['sequence'], used_threads=used_threads, max_threads=resources['max_threads'],
                        charged_bytes=charged, running_partitions=heartbeat['running_partitions'],
                        queued_partitions=heartbeat['queued_partitions'], running_operations=heartbeat['running_operations']))
                    self.sequences[heartbeat['incarnation']] = heartbeat['sequence']
            except (sqlite3.Error, ValueError, KeyError, AssertionError) as error:
                self.errors.append('heartbeat:' + type(error).__name__)
            self.stop.wait(.25)

    def close(self):
        self.stop.set()
        self.thread.join(5)
        assert not self.thread.is_alive(), 'OS observer cleanup failed'


def cpu_seconds(value):
    days = 0
    if '-' in value:
        raw, value = value.split('-', 1)
        days = int(raw)
    total = 0.0
    for part in value.split(':'):
        total = total * 60 + float(part)
    return total + days * 86400


def execution(cap=3, pending=1100, queue_bytes=256 << 20, retention_ms=300000):
    return dict(max_workers_per_request=cap, max_pending_requests=pending,
                max_queue_bytes_per_replica=queue_bytes,
                max_retained_result_bytes_per_worker=64 << 20, result_retention_ms=retention_ms,
                queue_replication='all_ready_workers', queue_lifetime='live_cluster', partition_size='4 KiB')


def setup(cluster, seed, many=True, row_count=None):
    for node, threads in ((10, 1), (20, 2), (30, 4)):
        cluster.start_worker(node, threads)
    ready = [n for n in cluster.control()['nodes'] if n['state'] == 'ready']
    limits = {n['resources']['max_threads']: n['resources']['memory_limit_bytes'] for n in ready}
    assert set(limits) == {1, 2, 4} and len(set(limits.values())) == 3
    status, result = cluster.operation('table.create', dict(name='items', schema={'id': 'int64', 'text': 'utf8'}, key='id', format='parquet'))
    assert status == 200, (status, result)
    table = result['data']['id']
    rng = random.Random(seed)
    rows = [dict(id=n, text=''.join(rng.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=256))) for n in range(row_count if row_count is not None else 1200 if many else 8)]
    status, result = cluster.operation('table.insert_many', dict(table_id=table, rows=rows))
    assert status == 200 and result['data'] == len(rows), (status, result)
    query = dict(table_id=table, query=dict(select=None, filter=None, order_by=None, limit=None))
    return table, rows, query, limits


def terminal_ok(cluster, accepted, expected, timeout=120):
    terminal = cluster.terminal(accepted, timeout)
    assert terminal['request_id'] == accepted['request_id'] and terminal['run_id'] == accepted['run_id'], terminal
    assert terminal['state'] == 'succeeded' and terminal['result'] == expected and terminal['error'] is None, terminal
    return terminal


def assert_cap_trace(cluster, identity, cap, minimum_partitions=100):
    jobs = [r for r in cluster.traces(identity) if r['path'] == '/internal/v1/jobs']
    files = {r['file'] for r in jobs if 'file' in r}
    assert len(files) >= minimum_partitions, ('real partition count', len(files), minimum_partitions)
    assert all(r['encoded_bytes'] <= 4096 for r in jobs if 'encoded_bytes' in r)
    # The intervals include transport cleanup; they conservatively overestimate
    # compute allocations. Reduction happens after all partition jobs settle and
    # has no public trace hook, so this assertion does not certify its resources.
    events = []
    for n, item in enumerate(jobs):
        assert item['finished'] is not None and item['status'] == 200, item
        events.extend([(item['started'], 1, n, item['node']), (item['finished'], -1, n, item['node'])])
    active = collections.Counter()
    peak = 0
    for _, delta, _, node in sorted(events):
        active[node] += delta
        assert active[node] >= 0
        peak = max(peak, sum(count > 0 for count in active.values()))
    assert peak <= cap, ('request node cap exceeded in transport trace', cap, peak)
    per_node = collections.Counter(r['node'] for r in jobs)
    assert all(count > 1 for count in per_node.values()), per_node
    return dict(partitions=len(files), jobs=len(jobs), peak_transport_nodes=peak,
                jobs_per_node=dict(per_node), reduction_observed=False)


def caps_and_overlap(cluster, rows, query, cap):
    identity = str(uuid.uuid4())
    gate = cluster.gate(identity)
    accepted = cluster.submit('query.collect', query, identity)
    assert gate.entered.wait(15), 'large query did not reach job barrier'
    wait_for(lambda: len(gate.entered_nodes) == cap, 'cap not exercised at deterministic barrier', 15)
    assert len(gate.entered_nodes) == cap
    gate.release.set()
    terminal_ok(cluster, accepted, rows)
    wait_for(lambda: all(r['finished'] is not None for r in cluster.traces(identity)), 'trace cleanup')
    cap_result = assert_cap_trace(cluster, identity, cap)
    # Two independent accepted requests are both unfinished at concrete job
    # barriers. A third one-partition lookup must complete while they are held.
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    ga, gb = cluster.gate(a), cluster.gate(b)
    ra = cluster.submit('query.collect', query, a)
    assert ga.entered.wait(15)
    rb = cluster.submit('query.count', query, b)
    # Free one causally observed job at a time until the second request dispatches.
    end = time.monotonic() + 15
    releases = 0
    while not gb.entered.is_set():
        assert time.monotonic() < end, 'second eligible request never dispatched'
        for node in list(ga.entered_nodes):
            ga.release_one(node)
        releases += 1
        gb.entered.wait(.02)
    small = cluster.submit('table.get', dict(table_id=query['table_id'], key=17))
    end = time.monotonic() + 15
    while True:
        status, value = cluster.receipt(small)
        assert status == 200, (status, value)
        if value['state'] == 'succeeded':
            assert value['result'] == rows[17], value
            break
        assert value['state'] not in ('failed', 'abandoned'), value
        assert time.monotonic() < end, 'small query starved at job boundaries'
        for gate in (ga, gb):
            for node in list(gate.entered_nodes):
                gate.release_one(node)
        time.sleep(.02)
    states = [cluster.receipt(receipt)[1]['state'] for receipt in (ra, rb)]
    assert all(state in ('queued', 'running', 'committing', 'reconciling') for state in states), states
    ga.release.set()
    gb.release.set()
    terminal_ok(cluster, ra, rows)
    terminal_ok(cluster, rb, len(rows))
    return dict(cap=cap, allocation_trace=cap_result, overlap=dict(small_result=rows[17]['id'],
                large_states_at_small_completion=states, causal_release_rounds=releases))


def raw_request(cluster, body, token=None):
    connection = http.client.HTTPConnection(*target(cluster.api_door.url), timeout=20)
    try:
        connection.request('POST', '/v1/requests', body,
                           {'Authorization': 'Bearer ' + (token or cluster.auth['data_token']),
                            'Content-Type': 'application/json', 'Connection': 'close'})
        response = connection.getresponse()
        return response.status, json.loads(response.read(8 << 20))
    finally:
        connection.close()


def malformed_during_saturation(cluster):
    bad_id = str(uuid.uuid4())
    cases = [('{', 400),
             ('{"request_id":"' + bad_id + '","request_id":"' + bad_id + '","operation":"table.list","payload":{}}', 400),
             (json.dumps(dict(request_id=bad_id, operation='query.count', payload=dict(table_id=17, query={}))), 400)]
    results = []
    for body, expected in cases:
        status, result = raw_request(cluster, body)
        assert status == expected and 'error' in result, (status, result)
        results.append(dict(status=status, code=result['error']['code']))
    status, result = raw_request(cluster, json.dumps(dict(request_id=bad_id, operation='table.list', payload={})), 'invalid')
    assert status == 401 and 'error' in result, (status, result)
    results.append(dict(status=status, code=result['error']['code']))
    return results


def conservation(cluster, table, rows, count=1000, progress=None, clients_count=4):
    # Use a small captured catalog for queue/identity pressure. The separate
    # cap fixture retains its strict 128-file workload. These gates hold actual
    # remote IO allocations; the local-router fixture tests gated CPU work.
    gates, blockers = [], []
    for index in range(7):
        identity = str(uuid.uuid4())
        gate = cluster.gate(identity)
        receipt = cluster.submit('table.get', dict(table_id=table, key=index % len(rows)), identity)
        assert gate.entered.wait(10), 'one-partition blocker did not dispatch'
        gates.append(gate)
        blockers.append((receipt, rows[index % len(rows)]))
    nodes = set().union(*(gate.entered_nodes for gate in gates))
    assert len(nodes) == 3, ('seven blockers did not use the 1/2/4 worker set', nodes)
    for receipt, _ in blockers:
        status, value = cluster.receipt(receipt)
        assert status == 200 and value['state'] in ('queued', 'running', 'committing', 'reconciling'), value
    invalid = malformed_during_saturation(cluster)
    began = time.monotonic()
    accepted, expected, timing, terminal = {}, {}, {}, {}
    lock, abort = threading.Lock(), threading.Event()
    barrier_release_count = min(64, count)
    partial = dict(submitted_target=count, table_id=table, oracle_rows=rows, accepted={}, request_attempts={},
                   http_status_counts={}, blockers=[receipt for receipt, _ in blockers],
                   barrier_release_after_acknowledgments=barrier_release_count,
                   barrier_scope='seven real remote IO allocations across heterogeneous workers; CPU gates tested separately',
                   initial_barrier_nodes=sorted(nodes), admission_clients=clients_count, receipt_poll_interval_seconds=cluster.receipt_poll_interval,
                   execution=cluster.template['execution'])
    if progress is not None:
        progress['conservation_progress'] = partial
    def submit(index):
        rid = str(uuid.UUID(int=(1 << 120) + index + 1))
        if index % 10 == 0:
            op, payload, oracle = 'table.describe', dict(name='items'), None
        elif index % 10 == 1:
            op, payload, oracle = 'table.list', {}, None
        else:
            op, payload = 'table.get', dict(table_id=table, key=index % len(rows))
            oracle = rows[index % len(rows)]
        started = time.monotonic()
        attempts = []
        with lock:
            partial['request_attempts'][rid] = dict(operation=op, payload=payload, statuses=attempts)
        try:
            for attempt in range(600):
                if abort.is_set():
                    return
                try:
                    status, receipt = request(cluster.api_door.url, 'POST', '/v1/requests', cluster.auth['data_token'],
                                              dict(request_id=rid, operation=op, payload=payload))
                except OSError as error:
                    status = 'transport:' + str(error.errno)
                    receipt = {'error': {'code': type(error).__name__, 'outcome': 'unknown'}}
                attempts.append(status)
                with lock:
                    partial['http_status_counts'][str(status)] = partial['http_status_counts'].get(str(status), 0) + 1
                if status == 202:
                    assert receipt['request_id'] == rid and receipt['state'] == 'queued', receipt
                    break
                assert (status in (429, 503) or isinstance(status, str) and status.startswith('transport:')) and 'error' in receipt, (rid, status, receipt)
                assert time.monotonic()-started < 60, ('bounded admission retry deadline', rid, attempts)
                threading.Event().wait(min(.025*(attempt+1), .5))
            else:
                raise AssertionError('bounded admission retries exhausted for ' + rid)
            ack = time.monotonic()
            with lock:
                assert rid not in accepted
                accepted[rid], expected[rid] = receipt, (op, oracle)
                timing[rid] = dict(submitted=started, acknowledged=ack, admission_attempt_statuses=attempts)
                partial['accepted'][rid] = receipt
                if len(accepted) == barrier_release_count:
                    partial['barrier_released_at_acknowledged_count'] = len(accepted)
                    partial['barrier_seconds'] = ack-began
                    # Count-triggered release avoids turning a queue test into
                    # a deliberately expired network-RPC fault experiment.
                    for gate in gates:
                        gate.release.set()
        except BaseException as error:
            with lock:
                partial.setdefault('failure', dict(request_id=rid, type=type(error).__name__, message=str(error),
                                                   acknowledged_count=len(accepted)))
            abort.set()
            raise
    with concurrent.futures.ThreadPoolExecutor(max_workers=clients_count, thread_name_prefix='load-submit') as clients:
        futures = [clients.submit(submit, index) for index in range(count)]
        try:
            for future in concurrent.futures.as_completed(futures):
                future.result()
        except BaseException:
            abort.set()
            for future in futures:
                future.cancel()
            raise
    assert len(accepted) == count, (len(accepted), count)
    for gate in gates:
        gate.release.set()
    def tracked(item):
        rid, receipt = item
        status, value = cluster.receipt(receipt)
        assert status == 200 and value['request_id'] == rid and value['run_id'] == receipt['run_id'], (rid, status, value)
        assert value['state'] in ('queued', 'running', 'committing', 'reconciling', 'succeeded'), value
        return value['state']
    with concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix='load-observe') as clients:
        tracked_states = list(clients.map(tracked, accepted.items()))
    for receipt, oracle in blockers:
        terminal_ok(cluster, receipt, oracle)
    status, descriptor = cluster.operation('table.describe', dict(name='items'))
    assert status == 200, descriptor
    descriptor = descriptor['data']
    def finish(item):
        rid, receipt = item
        op, oracle = expected[rid]
        oracle = descriptor if op == 'table.describe' else [descriptor] if op == 'table.list' else oracle
        result = terminal_ok(cluster, receipt, oracle, 180)
        with lock:
            assert rid not in terminal
            terminal[rid] = result
            timing[rid]['terminal_observed'] = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix='load-complete') as clients:
        list(clients.map(finish, accepted.items()))
    assert set(accepted) == set(terminal) and len(terminal) == count
    replay_id = next(rid for rid, (op, _) in expected.items() if op == 'table.get')
    op, oracle = expected[replay_id]
    same = cluster.submit(op, dict(table_id=table, key=oracle['id']), replay_id)
    assert same['run_id'] == accepted[replay_id]['run_id']
    terminal_ok(cluster, same, oracle)
    status, conflict = request(cluster.api_door.url, 'POST', '/v1/requests', cluster.auth['data_token'],
                               dict(request_id=replay_id, operation='table.get', payload=dict(table_id=table, key=(oracle['id']+1) % len(rows))))
    assert status == 409 and conflict['error']['code'] == 'conflict', (status, conflict)
    assert terminal_ok(cluster, accepted[replay_id], oracle) == terminal[replay_id]
    completed = time.monotonic()
    samples = list(timing.values())
    return dict(submitted=count, accepted=count, terminal=len(terminal), missing=0, duplicate_terminal=0,
                observed_states_after_submission=dict(collections.Counter(tracked_states)),
                barrier_seconds=partial['barrier_seconds'], barrier_release_at_acknowledgments=barrier_release_count,
                invalid_traffic=invalid, duplicate_same_receipt=True, conflicting_body_status=status,
                admission_attempt_statuses=dict(collections.Counter(code for t in timing.values() for code in t['admission_attempt_statuses'])),
                rejected_http_attempts=sum(code == 429 for t in timing.values() for code in t['admission_attempt_statuses']),
                uncertain_http_attempts=sum(code == 503 for t in timing.values() for code in t['admission_attempt_statuses']),
                host_transport_attempts=sum(isinstance(code, str) for t in timing.values() for code in t['admission_attempt_statuses']),
                elapsed_seconds=completed-began, throughput_per_second=count/(completed-began),
                admission_latency_seconds=distribution([t['acknowledged']-t['submitted'] for t in samples]),
                completion_observation_seconds=distribution([t['terminal_observed']-t['submitted'] for t in samples]),
                accepted_ids=sorted(accepted), identity_sha256=hashlib.sha256('\n'.join(sorted(accepted)).encode()).hexdigest(),
                terminal_sha256=hashlib.sha256(json.dumps(terminal, sort_keys=True, separators=(',', ':')).encode()).hexdigest())


def distribution(values):
    values = sorted(values)
    return {name: values[min(len(values) - 1, int((len(values) - 1) * percentile))]
            for name, percentile in (('p50', .50), ('p95', .95), ('p99', .99))} if values else None


def overflow(cluster, table, rows, kind):
    # All submissions carry a job gate. The fixture opens it only after one
    # explicit overload and a lookup of every earlier accepted identity.
    accepted, gates = [], []
    rejected = None
    for index in range(40):
        identity = str(uuid.uuid4())
        gate = cluster.gate(identity)
        gates.append(gate)
        if kind == 'bytes':
            # A large valid predicate makes request bytes dominate the small
            # pinned catalog. Wait for its actual job gate before the next
            # admission so overflow is the replica budget, not pin-start races.
            operation = 'query.count'
            payload = dict(table_id=table, query=dict(select=None, order_by=None, limit=None,
                           filter=dict(op='eq', column='text', value='q' * 9000)))
            oracle = 0
        else:
            operation = 'table.get'
            payload = dict(table_id=table, key=index % len(rows))
            oracle = rows[index % len(rows)]
        status, value = request(cluster.api_door.url, 'POST', '/v1/requests', cluster.auth['data_token'],
                                dict(request_id=identity, operation=operation, payload=payload))
        if status == 429:
            assert value['error']['code'] == 'overloaded', value
            rejected = identity
            break
        assert status == 202, (status, value)
        accepted.append((value, oracle))
        if kind == 'bytes':
            assert gate.entered.wait(10), 'byte-limited accepted request did not pin and dispatch'
    assert rejected is not None and accepted, (kind, len(accepted))
    if kind == 'count':
        assert len(accepted) == 8, len(accepted)
    else:
        assert len(accepted) < 40 < cluster.template['execution']['max_pending_requests']
    for receipt, _ in accepted:
        status, value = cluster.receipt(receipt)
        assert status == 200 and value['state'] in ('queued', 'running', 'committing', 'reconciling'), value
    invalid = malformed_during_saturation(cluster)
    for gate in gates:
        gate.release.set()
    for receipt, oracle in accepted:
        terminal_ok(cluster, receipt, oracle)
    return dict(kind=kind, accepted=len(accepted), explicit_overload=1, accepted_retained=True, invalid_traffic=invalid)


def observed_replica_payloads(traces, since):
    retained = collections.defaultdict(dict)
    peaks = collections.defaultdict(lambda: dict(request_and_pin_bytes=0, result_payload_bytes=0, records=0))
    for trace in sorted(traces, key=lambda item: item['finished'] or float('inf')):
        if trace['status'] != 200 or 'reply_payload' not in trace:
            continue
        node, record = trace['node'], trace['reply_payload']
        previous = retained[node].get(record['request_id'])
        if previous and previous['version'] > record['version']:
            continue
        retained[node][record['request_id']] = record
        if (trace['finished'] or 0) < since:
            continue
        records = retained[node].values()
        peaks[node]['request_and_pin_bytes'] = max(peaks[node]['request_and_pin_bytes'],
            sum(item['request_body_bytes'] + item['pinned_catalog_bytes'] for item in records))
        peaks[node]['result_payload_bytes'] = max(peaks[node]['result_payload_bytes'],
            sum(item['result_payload_bytes'] for item in records))
        peaks[node]['records'] = max(peaks[node]['records'], len(retained[node]))
    return dict(scope='observed successful signed Record replies per receiver; actual request/pin/result bytes; '
                      'excludes allocator metadata and unobserved local transitions',
                per_worker=dict(peaks), observer_capture_bound_bytes=MAX_OBSERVED_JSON_BYTES,
                uncaptured_messages=sum('queue_metadata_unobserved' in trace for trace in traces))


def metrics(cluster, observer, since=0):
    all_traces = cluster.traces()
    traces = [r for r in all_traces if r['started'] >= since]
    storage = [r for r in cluster.storage_traces() if r['started'] >= since]
    jobs = [r for r in traces if r['path'] == '/internal/v1/jobs']
    attempts = collections.Counter(r['job_id'] for r in jobs)
    ordinal_attempts = collections.Counter((r['operation_id'], r['ordinal']) for r in jobs if 'ordinal' in r)
    rss, cpu, heartbeat = {}, {}, collections.defaultdict(list)
    for sample in observer.samples:
        if sample['time'] < since:
            continue
        node = sample['node']
        rss[node] = max(rss.get(node, 0), sample['rss_bytes'])
        cpu.setdefault(node, []).append(sample['cpu_seconds'])
    for sample in observer.heartbeats:
        if sample['time'] >= since:
            heartbeat[sample['node']].append(sample)
    heartbeat_summary = {node: dict(samples=len(samples),
        peak_occupied_threads=max(sample['used_threads'] for sample in samples),
        mean_sampled_slot_occupancy=sum(sample['used_threads'] / sample['max_threads'] for sample in samples) / len(samples),
        peak_charged_bytes=max(sample['charged_bytes'] for sample in samples),
        sequences=[sample['sequence'] for sample in samples]) for node, samples in heartbeat.items()}
    partitions = {record['file']: record for record in jobs if 'file' in record}
    return dict(partition_count=len(partitions),
                partition_encoded_bytes=distribution([record['encoded_bytes'] for record in partitions.values()]),
                partition_source_payload_bytes=distribution([record['payload_bytes'] for record in partitions.values()]),
                partition_rows=distribution([record['rows'] for record in partitions.values()]),
                peak_sampled_rss_bytes=rss, cpu_seconds={n: max(v) - min(v) for n, v in cpu.items()},
                os_sample_count=sum(sample['time'] >= since for sample in observer.samples), os_observer_errors=observer.errors,
                heartbeat_metrics=heartbeat_summary, heartbeat_scope='actual persisted worker CPU/byte reservation samples; 250ms configured publication and observer intervals',
                worker_transport_request_bytes=sum(r['request_bytes'] for r in traces),
                worker_transport_response_bytes=sum(r['response_bytes'] for r in traces),
                storage_transport_request_bytes=sum(r['request_bytes'] for r in storage),
                storage_transport_response_bytes=sum(r['response_bytes'] for r in storage),
                observed_catalog_cas_conflicts=sum(r['method'] == 'PUT' and r['path'].endswith('/catalog.json') and r['status'] == 412 for r in storage),
                controlled_storage_read_delays=sum('controlled_storage_delay_seconds' in r for r in storage),
                job_attempts=len(jobs), repeated_job_identity_attempts=sum(max(0, n - 1) for n in attempts.values()),
                retried_partition_attempts=sum(max(0, n - 1) for n in ordinal_attempts.values()),
                transport_errors=dict(collections.Counter(r['failure'] for r in traces + storage if 'failure' in r)),
                unobserved_metrics=['exact_continuous_HTTP_compute_occupancy', 'exact_server_queue_wait', 'HTTP_reduction_reservation_events'],
                replica_payloads=observed_replica_payloads(all_traces, since),
                reported_queue_peak_bytes=max((r['queue_stats']['queue_bytes'] for r in traces if 'queue_stats' in r), default=None),
                reported_results_peak_bytes=max((r['queue_stats']['result_bytes'] for r in traces if 'queue_stats' in r), default=None))


def observed_terminal(cluster, accepted, expected, submitted, timeout=120):
    end = time.monotonic() + timeout
    last_queued_sent = submitted
    first_started_observed = None
    samples = []
    transient_reads = 0
    while time.monotonic() < end:
        sent = time.monotonic()
        status, value = cluster.receipt(accepted)
        received = time.monotonic()
        if status == 503:
            transient_reads += 1
            threading.Event().wait(cluster.receipt_poll_interval)
            continue
        assert status == 200 and value['request_id'] == accepted['request_id'], (status, value)
        samples.append(dict(sent=sent, received=received, state=value['state']))
        if value['state'] == 'queued':
            last_queued_sent = sent
        elif first_started_observed is None:
            first_started_observed = received
        if value['state'] in ('succeeded', 'failed', 'abandoned'):
            assert value['state'] == 'succeeded' and value['result'] == expected and value['error'] is None, value
            return dict(completed=received, receipt_samples=samples, transient_receipt_reads=transient_reads,
                        terminal_result_sha256=hashlib.sha256(json.dumps(value['result'], sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                        queue_wait_lower_seconds=max(0, last_queued_sent-submitted),
                        queue_wait_upper_seconds=max(0, (first_started_observed or received)-submitted))
        threading.Event().wait(cluster.receipt_poll_interval)
    raise AssertionError('accepted benchmark request did not settle')


def benchmark(cluster, observer, table, rows, requests_per_run=16, rates=(2, 8, 32), storage_delay=.002, clients_count=4, progress=None):
    """Warm-up and 3 repetitions at each rate; report measured limits honestly."""
    descriptor_status, descriptor = cluster.operation('table.describe', dict(name='items'))
    assert descriptor_status == 200, descriptor
    descriptor = descriptor['data']
    query = dict(table_id=table, query=dict(select=None, filter=None, order_by=None, limit=None))
    repetitions = []
    result = dict(contract='B02 real HTTP/storage measurements; complement with resource/pool benchmark for exact scheduler metrics',
                  requests_per_run=requests_per_run, workload='25% collect, 25% count, 25% get, 25% describe', row_count=len(rows),
                  storage_read_delay_seconds=storage_delay, client_max_in_flight=clients_count, receipt_poll_interval_seconds=cluster.receipt_poll_interval,
                  execution=cluster.template['execution'], repetitions=repetitions,
                  required_complement='exact resource/pool queue wait, occupied compute time and no-feasible-idle event assertions')
    if progress is not None:
        progress.append(result)
    cluster.store_door.partition_read_delay_seconds = storage_delay
    for rate in rates:
        for repetition in range(4):
            started = time.monotonic()
            records = []
            result['active_phase'] = dict(rate=rate, repetition=repetition, requests=records)
            lock = threading.Lock()
            def one(index, scheduled):
                identity = str(uuid.uuid4())
                if index % 4 == 0:
                    operation, payload, oracle = 'query.collect', query, rows
                elif index % 4 == 1:
                    operation, payload, oracle = 'query.count', query, len(rows)
                elif index % 4 == 2:
                    operation, payload, oracle = 'table.get', dict(table_id=table, key=index % len(rows)), rows[index % len(rows)]
                else:
                    operation, payload, oracle = 'table.describe', dict(name='items'), descriptor
                sent = time.monotonic()
                oracle_digest = hashlib.sha256(json.dumps(oracle, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                admission_attempts = []
                for attempt in range(120):
                    status, accepted = request(cluster.api_door.url, 'POST', '/v1/requests', cluster.auth['data_token'],
                                               dict(request_id=identity, operation=operation, payload=payload))
                    admission_attempts.append(status)
                    if status != 503:
                        break
                    assert 'error' in accepted and time.monotonic()-sent < 60, (status, accepted)
                    # Retry uncertain submission with the identical logical ID.
                    threading.Event().wait(min(.025*(attempt+1), .5))
                acknowledged = time.monotonic()
                if status == 429:
                    assert accepted['error']['code'] == 'overloaded', accepted
                    row = dict(request_id=identity, accepted=False, status=status, sent=sent, scheduled=scheduled, admission_attempt_statuses=admission_attempts)
                else:
                    assert status == 202 and accepted['request_id'] == identity, (status, accepted)
                    observation = observed_terminal(cluster, accepted, oracle, sent)
                    row = dict(request_id=identity, operation=operation, oracle_result_sha256=oracle_digest, accepted=True, status=status, sent=sent, scheduled=scheduled,
                               acknowledged=acknowledged, admission_attempt_statuses=admission_attempts, **observation)
                with lock:
                    records.append(row)
            # Arrival sleeps implement the declared schedule. They are not used
            # as assertions about scheduling overlap or fairness.
            with concurrent.futures.ThreadPoolExecutor(max_workers=clients_count, thread_name_prefix='rate-client') as clients:
                futures = []
                for index in range(requests_per_run):
                    scheduled = started + index / rate
                    delay = scheduled - time.monotonic()
                    if delay > 0:
                        threading.Event().wait(delay)
                    futures.append(clients.submit(one, index, scheduled))
                for future in futures:
                    future.result()
            finished = time.monotonic()
            accepted = [r for r in records if r['accepted']]
            assert len({r['request_id'] for r in records}) == requests_per_run
            first_job = {}
            for trace in cluster.traces():
                if trace['path'] == '/internal/v1/jobs':
                    first_job.setdefault(trace['operation_id'], trace['started'])
            dispatch_latency = [first_job[r['request_id']] - r['sent'] for r in accepted if r['request_id'] in first_job]
            repetitions.append(dict(arrival_rate_per_second=rate, repetition=repetition,
                warmup=repetition == 0, submitted=requests_per_run, accepted=len(accepted),
                completed=len(accepted), rejections=requests_per_run-len(accepted), missing=0, duplicate_terminal=0,
                elapsed_seconds=finished-started, throughput_per_second=len(accepted)/(finished-started),
                latency_seconds=distribution([r['completed']-r['sent'] for r in accepted]),
                admission_latency_seconds=distribution([r['acknowledged']-r['sent'] for r in accepted]),
                queue_wait_lower_seconds=distribution([r['queue_wait_lower_seconds'] for r in accepted]),
                queue_wait_upper_seconds=distribution([r['queue_wait_upper_seconds'] for r in accepted]),
                queue_wait_scope='receipt-state observation bounds, including ingress; exact scheduler queue wait is measured in the resource/pool benchmark',
                submit_to_first_job_transport_seconds=distribution(dispatch_latency),
                client_arrival_lateness_seconds=distribution([r['sent']-r['scheduled'] for r in records]),
                admission_http_status_counts=dict(collections.Counter(status for record in records for status in record['admission_attempt_statuses'])),
                metrics=metrics(cluster, observer, started), requests=records))
    result.pop('active_phase', None)
    return result


def source_digest():
    root = Path(__file__).resolve().parents[1]
    paths = list((root / 'server/src').rglob('*.rs')) + list((root / 'server/tests').rglob('*.rs'))
    paths += list((root / 'server/examples').glob('*.rs'))
    paths += [root / 'scripts/plot_scheduler_http_load.py', root / 'scripts/summarize_scheduler_benchmarks.py']
    paths += list((root / 'scripts').glob('acceptance*.py'))
    paths += [root / 'server/Cargo.toml', root / 'server/Cargo.lock']
    digest = hashlib.sha256()
    for path in sorted(paths):
        if path.is_file():
            digest.update(str(path.relative_to(root)).encode() + b'\0')
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def cpu_quota():
    def read(path):
        try:
            return Path(path).read_text().strip()
        except OSError:
            return None
    v2 = read('/sys/fs/cgroup/cpu.max')
    v1_quota, v1_period = read('/sys/fs/cgroup/cpu/cpu.cfs_quota_us'), read('/sys/fs/cgroup/cpu/cpu.cfs_period_us')
    quota_cores = None
    if v2 and not v2.startswith('max'):
        quota, period = v2.split()
        quota_cores = int(quota) / int(period)
    elif v1_quota and v1_period and int(v1_quota) > 0:
        quota_cores = int(v1_quota) / int(v1_period)
    affinity = sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None
    return dict(cgroup_v2_cpu_max=v2, cgroup_v1_quota=v1_quota, cgroup_v1_period=v1_period,
                quota_cores=quota_cores, available_parallelism=len(affinity) if affinity is not None else os.cpu_count(),
                affinity=affinity, cpu_time_rlimit=list(resource.getrlimit(resource.RLIMIT_CPU)),
                address_space_rlimit=list(resource.getrlimit(resource.RLIMIT_AS)),
                cgroup_memory_max=read('/sys/fs/cgroup/memory.max'),
                fixture_installs_cpu_or_memory_quota=False)


def machine():
    def command(args):
        try:
            return subprocess.check_output(args, text=True, timeout=5).strip()
        except (OSError, subprocess.SubprocessError):
            return None
    return dict(os=platform.platform(), python=sys.version, logical_cores=os.cpu_count(),
                machine_ram_bytes=command(['sysctl', '-n', 'hw.memsize']) if sys.platform == 'darwin' else None,
                tcp_ephemeral_port_first=command(['sysctl', '-n', 'net.inet.ip.portrange.first']) if sys.platform == 'darwin' else None,
                tcp_ephemeral_port_last=command(['sysctl', '-n', 'net.inet.ip.portrange.last']) if sys.platform == 'darwin' else None,
                tcp_msl_ms=command(['sysctl', '-n', 'net.inet.tcp.msl']) if sys.platform == 'darwin' else None,
                cpu_quota=cpu_quota(), commit=command(['git', 'rev-parse', 'HEAD']))


def run(args):
    evidence = dict(started_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), machine=machine(), seed=args.seed, binary='server/target/debug/meshdb-server' if args.binary == Path(__file__).resolve().parents[1] / 'server/target/debug/meshdb-server' else args.binary.name,
                    build=dict(profile='dev', optimization='unoptimized', debug_info='disabled',
                               CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0', CARGO_INCREMENTAL='0',
                               provenance='explicit orchestrator build configuration; throughput is not a release-profile claim'),
                    binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(), binary_mtime_ns=args.binary.stat().st_mtime_ns,
                    source_tree_sha256=source_digest(),
                    scope='real API/worker TCP with local SQLite conditional object service', provider_evidence=False,
                    caps=[], overflow=[], measurements=[], status='running')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    def save():
        args.output.write_text(json.dumps(evidence, indent=2) + '\n')
    save()
    try:
        if args.suite in ('all', 'caps', 'load', 'benchmark'):
            caps = tuple(args.benchmark_caps) if args.suite == 'benchmark' else (1, 2, 3) if args.suite != 'load' else (3,)
            for cap in caps:
                with tempfile.TemporaryDirectory(prefix='meshdb-load-') as directory:
                    cluster = LoadCluster(args.binary, directory, execution(cap=cap), args.receipt_poll_interval)
                    observer = None
                    try:
                        table, rows, query, limits = setup(cluster, args.seed, many=args.suite != 'benchmark',
                            row_count=args.benchmark_rows if args.suite == 'benchmark' else None)
                        observer = Observer(cluster)
                        if args.suite == 'benchmark':
                            benchmark(cluster, observer, table, rows,
                                args.benchmark_requests, tuple(args.benchmark_rates), args.storage_read_delay,
                                args.admission_clients, evidence.setdefault('benchmarks', []))
                        else:
                            evidence['caps'].append(dict(resources=limits, execution=execution(cap=cap), result=caps_and_overlap(cluster, rows, query, cap)))
                        save()
                    except BaseException:
                        cluster.diagnostics.flush()
                        evidence['diagnostics_tail'] = (Path(directory) / 'diagnostics').read_text()[-12000:]
                        raise
                    finally:
                        if observer:
                            observer.close()
                            evidence['measurements'].append(dict(cap=cap, metrics=metrics(cluster, observer)))
                        evidence.setdefault('traces', []).extend(cluster.traces())
                        evidence.setdefault('storage_traces', []).extend(cluster.storage_traces())
                        if observer:
                            evidence.setdefault('heartbeat_samples', []).extend(observer.heartbeats)
                        cluster.close()
                        save()
        if args.suite in ('all', 'load', 'conservation'):
            with tempfile.TemporaryDirectory(prefix='meshdb-conservation-') as directory:
                cluster = LoadCluster(args.binary, directory, execution(retention_ms=3600000), args.receipt_poll_interval)
                observer = None
                try:
                    table, rows, _, _ = setup(cluster, args.seed, many=False)
                    observer = Observer(cluster)
                    evidence['conservation'] = conservation(cluster, table, rows, args.requests, evidence, args.admission_clients)
                except BaseException:
                    cluster.diagnostics.flush()
                    evidence['diagnostics_tail'] = (Path(directory) / 'diagnostics').read_text()[-12000:]
                    raise
                finally:
                    if observer:
                        observer.close()
                        evidence['measurements'].append(dict(case='conservation', metrics=metrics(cluster, observer)))
                        evidence.setdefault('heartbeat_samples', []).extend(observer.heartbeats)
                    evidence.setdefault('traces', []).extend(cluster.traces())
                    evidence.setdefault('storage_traces', []).extend(cluster.storage_traces())
                    cluster.close()
                    save()
        if args.suite in ('all', 'overflow'):
            for kind, limits in (('count', execution(pending=8)), ('bytes', execution(pending=100, queue_bytes=40000))):
                with tempfile.TemporaryDirectory(prefix='meshdb-overflow-') as directory:
                    cluster = LoadCluster(args.binary, directory, limits, args.receipt_poll_interval)
                    try:
                        table, rows, _, _ = setup(cluster, args.seed, many=False)
                        evidence['overflow'].append(overflow(cluster, table, rows, kind))
                    finally:
                        evidence.setdefault('traces', []).extend(cluster.traces())
                        evidence.setdefault('storage_traces', []).extend(cluster.storage_traces())
                        cluster.close()
                        save()
        evidence['status'] = 'passed'
    except BaseException as error:
        evidence['status'] = 'failed'
        evidence['failure'] = dict(type=type(error).__name__, message=str(error)[:4000])
        raise
    finally:
        save()
    print(json.dumps({k: v for k, v in evidence.items() if k not in ('traces', 'storage_traces', 'heartbeat_samples', 'diagnostics_tail')}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=Path('artifacts/scheduler-load.json'))
    parser.add_argument('--seed', type=int, default=20261115)
    parser.add_argument('--requests', type=int, default=1000)
    parser.add_argument('--receipt-poll-interval', type=float, default=.1)
    parser.add_argument('--admission-clients', type=int, default=4)
    parser.add_argument('--benchmark-caps', type=int, nargs='+', default=[1, 2, 3])
    parser.add_argument('--benchmark-rates', type=int, nargs='+', default=[2, 8, 32])
    parser.add_argument('--benchmark-rows', type=int, default=64)
    parser.add_argument('--benchmark-requests', type=int, default=16)
    parser.add_argument('--storage-read-delay', type=float, default=.002)
    parser.add_argument('--suite', choices=('all', 'caps', 'load', 'conservation', 'overflow', 'benchmark'), default='all')
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    run(args)

#!/usr/bin/env python3.11
"""ST03/ST04 process acceptance against a local conditional object store."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler
import json
from pathlib import Path
import signal
import sqlite3
import sys
import tempfile
import threading
import time
import uuid

from acceptance_nodes import FrontDoor, send, target
from acceptance_scheduler import Cluster, Evidence, wait_for


OPEN = '/v1/streams'
CAPABILITIES = OPEN + '/capabilities'
MAX_BODY = 2 * 1024 * 1024
QUERY = {'select': None, 'filter': None, 'order_by': None, 'limit': None}


def batch_path(opened, sequence=None):
    handle = f"{OPEN}/{opened['run_id']}/{opened['stream_id']}"
    return handle if sequence is None else f'{handle}/batches/{sequence}'


@dataclass(frozen=True)
class Response:
    status: int
    headers: http.client.HTTPMessage
    body: bytes

    def json(self, status=200):
        assert self.status == status, (self.status, self.body[:512])
        assert self.headers.get_content_type() == 'application/json', self.headers
        return json.loads(self.body)

    def error(self, status, code):
        value = self.json(status)
        assert set(value) == {'request_id', 'error'}, value
        assert value['error']['code'] == code, value
        return value

    def empty(self):
        assert self.status == 204 and not self.body, (self.status, self.body[:512])


def read_response(connection, cap):
    with connection.getresponse() as response:
        body = response.read(cap + 1)
        assert len(body) <= cap, 'response exceeded the configured fixture cap'
        return Response(response.status, response.headers, body)


def request(url, method, path, token, value=None, *, extra=None, timeout=25):
    conn = http.client.HTTPConnection(*target(url), timeout=timeout)
    try:
        body = json.dumps(value).encode() if isinstance(value, dict) else value
        headers = {'Authorization': 'Bearer ' + token, 'Connection': 'close'}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        conn.request(method, path, body, {**headers, **(extra or {})})
        return read_response(conn, MAX_BODY)
    finally:
        conn.close()


class CheckpointGate:
    """Hold an install before delivery or after its signed ACK is received."""

    def __init__(self, opened, sequence, phase):
        self.id = opened['stream_id']
        self.run = opened['run_id']
        self.sequence = sequence
        self.phase = phase
        self.entered = threading.Event()
        self.release = threading.Event()
        self.entry = None
        self.trace = None
        self.ack = None
        self.discard = False
        self.lock = threading.Lock()

    def claim(self, command, trace):
        if command['command'] != 'stream_install':
            return None
        entry = command['payload']['entry']
        lease = entry['kind']
        if lease['kind'] != 'stream' or lease['opened']['stream_id'] != self.id:
            return None
        checkpoint = lease['checkpoint']
        if (not checkpoint or checkpoint['sequence'] != self.sequence
                or entry['run'] != self.run):
            return None
        with self.lock:
            if self.entry is not None:
                return None
            self.entry, self.trace = entry, trace
            return self

    def before(self):
        if self.phase == 'before':
            self.entered.set()
            return self.release.wait(55) and not self.discard
        return True

    def after(self, head, frame):
        if self.phase == 'after':
            status = int(head.split(b' ', 2)[1])
            control = json.loads(frame[1:]) if frame.startswith(b'\0') else None
            self.ack = (status, control)
            self.entered.set()
            return self.release.wait(30)
        return True


class PublicFaultProxy(FrontDoor):
    """Drop or truncate one selected response after receiving it in full."""

    def __init__(self, url):
        self.faults = {}
        self.exchanges = []
        super().__init__()
        self.RequestHandlerClass = PublicFault
        self.target = target(url)


class PublicFault(BaseHTTPRequestHandler):
    def log_message(self, _format, *_args):
        pass

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def do_DELETE(self):
        self.forward()

    def forward(self):
        self.connection.settimeout(10)
        length = int(self.headers.get('Content-Length', '0'))
        if not 0 <= length <= 128 * 1024:
            self.send_error(413)
            return
        body = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items()
                   if k.lower() not in {'connection', 'host', 'content-length'}}
        pair = (self.command, self.path)
        with self.server.lock:
            fault = self.server.faults.pop(pair, None)
        upstream = http.client.HTTPConnection(*self.server.target, timeout=25)
        try:
            upstream.request(self.command, self.path, body, headers)
            reply = read_response(upstream, 8 * 1024 * 1024)
        finally:
            upstream.close()
        with self.server.lock:
            self.server.exchanges.append((pair, fault, body, reply))
        if fault == 'drop':
            return
        self.send_response(reply.status)
        self.send_header('Content-Length', str(len(reply.body)))
        self.send_header('Content-Type', reply.headers.get('Content-Type', 'application/json'))
        self.send_header('Connection', 'close')
        if reply.headers.get('Retry-After') is not None:
            self.send_header('Retry-After', reply.headers['Retry-After'])
        self.end_headers()
        if fault == 'truncate':
            assert len(reply.body) > 1, 'fixture attempted to truncate an empty body'
            self.wfile.write(reply.body[:len(reply.body) // 2])
        else:
            self.wfile.write(reply.body)


class StreamCluster(Cluster):
    def _initialize(self, execution):
        super()._initialize(execution)
        self.lease_gates = []

    def worker_resources(self, threads):
        return {'max_threads': threads, 'memory_limit_bytes': 1 << 30,
                'task_memory_limit_bytes': 256 << 20, 'control_memory_reserve_bytes': 128 << 20}

    @contextmanager
    def checkpoint_gate(self, opened, sequence, phase):
        gate = CheckpointGate(opened, sequence, phase)
        self.lease_gates.append(gate)
        for _, door, _ in self.workers.values():
            door.stream_exchange = gate.claim
        try:
            yield gate
        finally:
            gate.release.set()
            for _, door, _ in self.workers.values():
                door.stream_exchange = None

    def close(self):
        for gate in getattr(self, 'lease_gates', ()):
            gate.release.set()
        super().close()

    def fetch(self, method, path, value=None, *, url=None, **options):
        return request(url or self.api_door.url, method, path, self.auth['data_token'],
                       value, **options)

    def ready_run(self, previous=None, url=None):
        def lookup():
            try:
                reply = self.fetch('GET', CAPABILITIES, url=url, timeout=3)
            except (OSError, http.client.HTTPException):
                return None
            if reply.status in (429, 503):
                return None
            capabilities = reply.json()
            return capabilities if capabilities['run_id'] != previous else None
        return wait_for(lookup, 'stream capabilities did not see the expected live run', 60)

    def open_body(self, table, *, run=None, depth=0, rows=3):
        return {'request_id': str(uuid.uuid4()),
                'expected_run_id': run or self.ready_run()['run_id'],
                'table_id': table, 'query': QUERY, 'batch_rows': rows,
                'prefetch_batches': depth, 'format': 'rows', 'drop_last': False,
                'snapshot_id': None, 'shard': 0}

    def open(self, body, *, url=None):
        return self.fetch('POST', OPEN, body, url=url).json(201)

    def batch(self, opened, sequence, *, url=None):
        path = batch_path(opened, sequence)
        def lookup():
            response = self.fetch('POST', path, url=url, timeout=5)
            return None if response.status in (429, 503) else response
        return wait_for(lookup, f'batch {sequence} did not settle', 40)

    def catalog_fence(self):
        with sqlite3.connect(self.database) as db:
            body, = db.execute("SELECT body FROM objects WHERE key='scheduler/catalog.json'").fetchone()
        return json.loads(body)['fence']

    def takeover(self, former, term, run):
        def changed():
            control = self.control()['scheduler']
            fence = self.catalog_fence()
            if (control['coordinator_node'] not in (None, former)
                    and control['scheduler_term'] > term
                    and fence['scheduler_term'] == control['scheduler_term']):
                assert fence['run_id'] == run, fence
                return control
            return None
        return wait_for(changed, 'surviving worker did not acquire matching same-run catalog fence', 55)


def seed(cluster):
    data = [{'id': index, 'text': f'value-{index}'}
            for index in range(23)]
    status, value = cluster.operation('table.create', {
        'name': 'items', 'schema': {'id': 'int64', 'text': 'utf8'},
        'key': 'id', 'format': 'parquet',
    })
    assert status == 200, value
    table = value['data']['id']
    status, value = cluster.operation('table.insert_many', {'table_id': table, 'rows': data})
    assert status == 200 and value['data'] == len(data), value
    return table, data


def lease_lifecycle(cluster, table, rows, extra, evidence):
    capabilities = cluster.ready_run()
    assert capabilities['version'] == 1 and capabilities['max_prefetch_batches'] == 2, capabilities
    run = capabilities['run_id']
    for depth in (0, 1, 2):
        body = cluster.open_body(table, run=run, depth=depth, rows=7)
        opened = cluster.open(body)
        assert opened['prefetch_batches'] == depth
        assert cluster.open(body, url=extra) == opened
        cluster.fetch('POST', OPEN, {**body, 'batch_rows': 1}).error(409, 'conflict')
        cluster.fetch('POST', batch_path(opened, 1)).error(409, 'conflict')
        joined = []
        for seq in range(4):
            reply = cluster.batch(opened, seq)
            assert reply.headers['Cache-Control'] == 'no-store'
            joined += reply.json()['rows']
            assert cluster.batch(opened, seq, url=extra).body == reply.body
            if seq > 0:
                cluster.fetch('POST', batch_path(opened, seq - 1)).error(409, 'conflict')
        assert joined == rows
        cluster.batch(opened, 4).empty()
        cluster.batch(opened, 4, url=extra).empty()
        cluster.fetch('DELETE', batch_path(opened)).empty()
        cluster.fetch('DELETE', batch_path(opened), url=extra).empty()
        cluster.fetch('POST', batch_path(opened, 4)).error(410, 'stream_expired')
    evidence.append('ST03/ST04/Q05/Q06: all depths, exact rows, same-run open and batch replay, monotonic sequences, EOF replay and idempotent close across APIs')

    pending = [cluster.open(cluster.open_body(table, run=run)) for _ in range(16)]
    rejected = cluster.open_body(table, run=run)
    cluster.fetch('POST', OPEN, rejected).error(429, 'overloaded')
    try:
        first = cluster.batch(pending[0], 0).json()['rows']
        assert first == rows[:3]
        status, result = cluster.operation('table.get', {'table_id': table, 'key': 9})
        assert status == 200 and result['data'] == rows[9], result
        cluster.fetch('DELETE', batch_path(pending.pop())).empty()
        pending.append(cluster.open(rejected, url=extra))
        assert cluster.batch(pending[0], 1).json()['rows'] == rows[3:6]
    finally:
        for handle in pending:
            cluster.fetch('DELETE', batch_path(handle)).empty()
    evidence.append('ST03/Q02: 16 paused consumers exhausted active-stream admission; an ordinary read and accepted streams progressed; close allowed the previously rejected open')

    body = cluster.open_body(table, run=run)
    raw = json.dumps(body).replace('"batch_rows":', '"batch_rows": 1, "batch_rows":', 1)
    cluster.fetch('POST', OPEN, raw.encode()).error(400, 'validation_error')
    cluster.fetch('POST', OPEN, {**body, 'batch_rows': capabilities['max_batch_rows'] + 1}).error(400, 'validation_error')
    cluster.fetch('POST', OPEN, {**body, 'prefetch_batches': 3}).error(400, 'validation_error')
    request(extra, 'POST', OPEN, 'invalid', body).error(401, 'unauthorized')
    cluster.fetch('POST', OPEN, body, extra={'X-Nabedra-Storage': 'synthetic'}).error(401, 'unauthorized')
    opened = cluster.open(body)
    request(extra, 'POST', batch_path(opened, 0), 'invalid').error(401, 'unauthorized')
    cluster.fetch('POST', batch_path(opened, '-1')).error(400, 'validation_error')
    cluster.fetch('POST', batch_path(opened, 0), b'extra').error(413, 'limit_exceeded')
    # The same valid identity must still start at zero after the rejected traffic.
    assert cluster.batch(opened, 0).json()['rows'] == rows[:3]
    cluster.fetch('DELETE', batch_path(opened)).empty()
    evidence.append('ST04/U02: duplicate fields, row/depth bounds, malformed sequence/body, bearer and fixed-mode scope refusal preserved the valid sequence')


def sdk_faults(cluster, _table, rows, extra, evidence):
    from nabedra import Database

    proxy = PublicFaultProxy(extra)
    try:
        query = Database(proxy.url, token=cluster.auth['data_token']).table('items').query()
        proxy.faults[('POST', OPEN)] = 'drop'
        with query.stream(batch_rows=5, prefetch_batches=0) as stream:
            opened = [record for record in proxy.exchanges if record[0] == ('POST', OPEN)]
            assert [record[1] for record in opened] == ['drop', None], opened
            assert opened[0][2] == opened[1][2]
            lease = opened[0][3].json(201)
            assert opened[1][3].json(201) == lease
            batch_zero = ('POST', batch_path(lease, 0))
            batch_one = ('POST', batch_path(lease, 1))
            proxy.faults[batch_zero] = 'drop'
            proxy.faults[batch_one] = 'truncate'
            assert [row for batch in stream for row in batch] == rows
        for pair, fault in ((batch_zero, 'drop'), (batch_one, 'truncate')):
            attempts = [record for record in proxy.exchanges if record[0] == pair]
            assert [record[1] for record in attempts] == [fault, None]
            assert attempts[0][3].status == attempts[1][3].status == 200
            assert attempts[0][3].body == attempts[1][3].body
        assert cluster.fetch('POST', batch_path(lease, 0)).status == 410
    finally:
        proxy.close()
    evidence.append('ST04/Q04/Q06/U02: real ORM retried a dropped open, lost batch, and truncated batch with identical bytes and no missing or repeated rows')


def overlapping_delivery(cluster, table, rows, extra, evidence):
    opened = cluster.open(cluster.open_body(table))
    with ThreadPoolExecutor(max_workers=1) as pool:
        with cluster.checkpoint_gate(opened, 0, 'after') as gate:
            demand = pool.submit(cluster.fetch, 'POST', batch_path(opened, 0))
            assert gate.entered.wait(12), 'replica never returned a complete checkpoint ACK'
            assert gate.ack == (200, {'kind': 'installed'}), gate.ack
            assert gate.trace['sender'] != gate.trace['receiver']
            assert not demand.done(), 'API delivered before receiving the install ACK'
            cluster.fetch('POST', batch_path(opened, 0), url=extra).error(429, 'overloaded')
            assert not demand.done()
        assert demand.result(20).json()['rows'] == rows[:3]
    assert cluster.batch(opened, 0, url=extra).json()['rows'] == rows[:3]
    cluster.fetch('DELETE', batch_path(opened)).empty()
    evidence.append('ST03/ST04/Q03/Q05: real second API refused overlapping same-sequence delivery while complete replica ACK was held')


def apis_and_failover(cluster, table, rows, extra, evidence):
    opened = cluster.open(cluster.open_body(table))
    first = cluster.batch(opened, 0)
    assert first.json()['rows'] == rows[:3]
    cluster.stop_api()
    cluster.start_api()
    assert cluster.ready_run()['run_id'] == opened['run_id']
    assert cluster.batch(opened, 0).body == first.body
    evidence.append('ST04/Q04: primary API restart preserved the live run and exact prior batch')
    # Keep the restarted API and extra API alive while killing the current owner.
    previous = cluster.control()['scheduler']
    owner = previous['coordinator_node']
    worker, _, _ = cluster.workers[owner]
    with ThreadPoolExecutor(max_workers=1) as pool:
        with cluster.checkpoint_gate(opened, 1, 'before') as gate:
            demand = pool.submit(cluster.fetch, 'POST', batch_path(opened, 1), timeout=30)
            assert gate.entered.wait(12), 'first checkpoint never reached the before-install barrier'
            assert gate.trace['sender'] == owner and gate.ack is None
            assert not demand.done()
            worker.kill()
            assert worker.wait(8) == -signal.SIGKILL
            # The held request from a dead term must be discarded by the fixture.
            # It must not be forwarded to a replica after the coordinator dies.
            gate.discard = True
            gate.release.set()
        response = demand.result(35)
        assert response.status in (429, 503), (response.status, response.body[:512])
    cluster.takeover(owner, previous['scheduler_term'], opened['run_id'])
    assert cluster.batch(opened, 0, url=extra).body == first.body
    second = cluster.batch(opened, 1)
    assert second.json()['rows'] == rows[3:6]
    evidence.append('ST04/R01/R06: killed scan owner before first replica received its new checkpoint; survivor replayed the prior frame and recomputed its successor')

    before_join = cluster.control()['scheduler']
    cluster.start_worker(uuid.UUID(owner).int, 1)
    # Lose the complete batch reply after every ready replica has acknowledged it.
    path = batch_path(opened, 2)
    proxy = PublicFaultProxy(extra)
    try:
        proxy.faults[('POST', path)] = 'drop'
        try:
            cluster.fetch('POST', path, url=proxy.url)
        except http.client.RemoteDisconnected:
            pass
        else:
            raise AssertionError('fixture did not lose the public batch reply')
        captured = [record[3] for record in proxy.exchanges if record[0] == ('POST', path)]
        assert len(captured) == 1 and captured[0].json()['rows'] == rows[6:9], captured
    finally:
        proxy.close()
    control = cluster.control()['scheduler']
    assert control['scheduler_term'] >= before_join['scheduler_term']
    former = control['coordinator_node']
    former_process, _, _ = cluster.workers[former]
    former_process.kill()
    assert former_process.wait(8) == -signal.SIGKILL
    cluster.takeover(former, control['scheduler_term'], opened['run_id'])
    assert cluster.batch(opened, 2).body == captured[0].body
    assert cluster.batch(opened, 3, url=extra).json()['rows'] == rows[9:12]
    cluster.fetch('DELETE', batch_path(opened)).empty()
    evidence.append('ST04/Q06/R01/R06: after replacement worker readiness, a complete batch reply was lost and its owner killed; surviving owner delivered the same bytes and continued')


def stale_owner(cluster, table, rows, extra, evidence):
    opened = cluster.open(cluster.open_body(table))
    old = cluster.control()['scheduler']
    owner = old['coordinator_node']
    process, _, _ = cluster.workers[owner]
    with ThreadPoolExecutor(max_workers=1) as pool:
        with cluster.checkpoint_gate(opened, 0, 'after') as gate:
            demand = pool.submit(cluster.fetch, 'POST', batch_path(opened, 0), timeout=45)
            assert gate.entered.wait(12), 'replica did not acknowledge the old checkpoint'
            assert gate.ack == (200, {'kind': 'installed'}) and gate.trace['sender'] == owner
            process.send_signal(signal.SIGSTOP)
            try:
                cluster.takeover(owner, old['scheduler_term'], opened['run_id'])
                assert cluster.batch(opened, 0, url=extra).json()['rows'] == rows[:3]
            finally:
                process.send_signal(signal.SIGCONT)
                gate.release.set()
        obsolete = demand.result(45)
        assert obsolete.status in (429, 503), (obsolete.status, obsolete.body[:512])
    assert cluster.batch(opened, 1, url=extra).json()['rows'] == rows[3:6]
    cluster.fetch('DELETE', batch_path(opened)).empty()
    cluster.stop_worker(owner)
    cluster.start_worker(uuid.UUID(owner).int, 1)
    evidence.append('ST04/R05: resumed former owner could not deliver after a higher-term survivor replayed its fully installed checkpoint')


def cold_start(cluster, table, rows, extra, evidence):
    body = cluster.open_body(table)
    proxy = PublicFaultProxy(extra)
    try:
        proxy.faults[('POST', OPEN)] = 'drop'
        try:
            cluster.fetch('POST', OPEN, body, url=proxy.url)
        except http.client.RemoteDisconnected:
            pass
        else:
            raise AssertionError('fixture did not lose the open reply')
        opened = proxy.exchanges[0][3].json(201)
    finally:
        proxy.close()
    for process, _, _ in cluster.workers.values():
        if process.poll() is None:
            process.kill()
            assert process.wait(8) == -signal.SIGKILL
    cluster.stop_api()
    cluster.start_api(cold=True)
    cluster.start_worker(40, 1)
    new = cluster.ready_run(previous=opened['run_id'])
    cluster.fetch('POST', batch_path(opened, 0)).error(410, 'stream_abandoned')
    cluster.fetch('POST', OPEN, body).error(410, 'stream_abandoned')
    status, result = cluster.operation('table.get', {'table_id': table, 'key': 9})
    assert status == 200 and result['data'] == rows[9], result
    fresh_body = {**body, 'expected_run_id': new['run_id'], 'request_id': str(uuid.uuid4())}
    fresh = cluster.open(fresh_body)
    assert cluster.batch(fresh, 0).json()['rows'] == rows[:3]
    cluster.fetch('DELETE', batch_path(fresh)).empty()
    evidence.append('ST04/R04: all workers stopped; cold run refused old handle and unanswered old-run open and read the durable table')


def cgroup():
    root = Path('/sys/fs/cgroup')
    return {name: (root / name).read_text().strip()
            for name in ('cpu.max', 'memory.max', 'memory.peak', 'memory.events')
            if (root / name).is_file()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--evidence', type=Path)
    parser.add_argument('--scenario', choices=('all', 'leases', 'replies', 'overlap', 'failover', 'stale', 'cold'), default='all')
    args = parser.parse_args()
    binary = args.binary.resolve()
    evidence = Evidence(binary)
    started = time.monotonic()
    try:
        with tempfile.TemporaryDirectory(prefix='nabedra-olap-stream-') as directory:
            cluster = StreamCluster(binary, directory)
            try:
                for worker in (10, 20, 30):
                    cluster.start_worker(worker, 1)
                extra, _, extra_channel = cluster.start_extra_api()
                table, rows = seed(cluster)
                for name, scenario in [('leases', lease_lifecycle), ('replies', sdk_faults),
                                       ('overlap', overlapping_delivery), ('stale', stale_owner),
                                       ('failover', apis_and_failover),
                                       ('cold', cold_start)]:
                    if args.scenario in ('all', name):
                        scenario(cluster, table, rows, extra, evidence)
                send(extra_channel, {'version': 1, 'type': 'shutdown'})
            except BaseException:
                cluster.diagnostics.flush()
                sys.stderr.write((Path(directory) / 'diagnostics').read_text()[-24000:])
                raise
            finally:
                cluster.close()
    finally:
        result = {'binary_sha256': evidence.binary_sha256, 'scenario': args.scenario,
                  'passed_assertions': evidence, 'seconds': time.monotonic() - started,
                  'cgroup': cgroup(), 'provider_evidence': False}
        encoded = json.dumps(result, indent=2)
        if args.evidence:
            args.evidence.write_text(encoded + '\n')
        print(encoded)


if __name__ == '__main__':
    main()

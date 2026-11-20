#!/usr/bin/env python3.11
"""Run the shared point-update workload against the production RF3 OLTP topology."""

import argparse
import base64
from decimal import Decimal
import hashlib
import http.client
from io import SEEK_END
import json
import math
import os
from pathlib import Path
import re
import secrets
import ssl
from concurrent.futures import ThreadPoolExecutor
import subprocess
import tempfile
import threading
import random
import time
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from benchmark_oltp_workload import TABLE, add_workload_args, distribution, measure, run_workload, validate_args


ROOT = Path(__file__).resolve().parents[1]
DATABASE = 'test'
MIB = 1 << 20
TRACE_LOG_BYTES = 16 * MIB
NANO_CPUS = 1_000_000_000
WORKER_MEMORY = 2_150_000_000
API_CPU, API_MEMORY = '0.35', 550_000_000
DRIVER_CPU, DRIVER_MEMORY = '0.65', 900_000_000
WORKERS = ('w1', 'w2', 'w3')
SOAK_TABLE = 'benchmark_soak'
# fsync-family calls counted in the separate strace run (NS11 G4).
STRACE_CALLS = 'fsync,fdatasync,sync_file_range,syncfs'
CGROUP_FILES = ('io.stat', 'memory.current', 'memory.peak', 'memory.events', 'memory.stat', 'cpu.stat')
DEFAULT_WORKER_CPUS = (NANO_CPUS,) * len(WORKERS)
STATUS_LEADER_HINT = re.compile(
    r'^(\S+)\s+WARN\s+nabedra_database_server::oltp_replication::hosted: '
    r'typed ReadIndex failed group=(\S+) kind=Status \{ bucket: 0 \} node=(\d+) '
    r'error=has to forward request to: Some\((\d+)\)',
    re.MULTILINE,
)
EXECUTION = {
    'max_workers_per_request': 1,
    'max_pending_requests': 128,
    'max_queue_bytes_per_replica': 8 * MIB,
    'max_retained_result_bytes_per_worker': 8 * MIB,
    'result_retention_ms': 600_000,
    'queue_replication': 'all_ready_workers',
    'queue_lifetime': 'live_cluster',
    'partition_size': '64 MiB',
}


def worker_cpu_quotas(value):
    quotas = [field.strip() for field in value.split(',')]
    if len(quotas) != len(WORKERS) or any(
        not re.fullmatch(r'(?:[0-9]+(?:\.[0-9]{1,9})?|\.[0-9]{1,9})', field) for field in quotas
    ):
        raise argparse.ArgumentTypeError('supply three decimal CPU quotas with up to nine fractional digits')
    nanocpus = tuple(int(Decimal(field) * NANO_CPUS) for field in quotas)
    if min(nanocpus) <= 0 or sum(nanocpus) != sum(DEFAULT_WORKER_CPUS):
        raise argparse.ArgumentTypeError('worker CPU quotas must be positive and sum to 3')
    return nanocpus


def uid():
    return str(uuid4())


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def resources(memory):
    return dict(max_threads=1, memory_limit_bytes=memory, task_memory_limit_bytes=128 * MIB,
                control_memory_reserve_bytes=96 * MIB)


def command(*args, timeout=30):
    completed = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if completed.returncode != 0:
        raise RuntimeError(f'{args[:3]} exited {completed.returncode}: {completed.stderr.strip()}')
    return completed.stdout.strip()


class Http:
    """One persistent, non-retrying connection to the production HTTP API."""

    def __init__(self, url, token, timeout, ca=None):
        target = urlsplit(url)
        if target.scheme == 'https':
            self.connection = http.client.HTTPSConnection(target.hostname, target.port, timeout=timeout, context=ca)
        else:
            self.connection = http.client.HTTPConnection(target.hostname, target.port, timeout=timeout)
        self.headers = {'Authorization': f'Bearer {token}', 'X-Nabedra-Database': DATABASE,
                        'Content-Type': 'application/json'}

    def exchange(self, path, body=None):
        payload = None if body is None else json.dumps(body, separators=(',', ':'), allow_nan=False).encode()
        self.sizes = {'request_body_bytes': len(payload) if payload else 0}
        try:
            self.connection.request('GET' if body is None else 'POST', path, payload, self.headers)
            response = self.connection.getresponse()
            raw = response.read(8 * MIB + 1)
            self.sizes['response_body_bytes'] = len(raw)
            if len(raw) > 8 * MIB:
                raise ValueError('HTTP response exceeds the operation body limit')
            if response.headers.get_content_type() != 'application/json':
                raise ValueError(f'HTTP {response.status} did not return JSON')
            return response.status, json.loads(raw)
        except (OSError, http.client.HTTPException, ValueError):
            self.close()
            raise

    def close(self):
        self.connection.close()


def require_data(status, response, request_id):
    if status != 200 or not isinstance(response, dict) or set(response) != {'request_id', 'data'} \
            or response['request_id'] != request_id:
        raise RuntimeError(f'expected a success envelope for {request_id}, got HTTP {status}: {response}')
    return response['data']


class Nabedra:
    def __init__(self, ca_file, auth, timeout, metadata):
        self.token, self.admin_token = auth['data_token'], auth['admin_token']
        self.timeout, self.metadata = timeout, metadata
        self.ca = ssl.create_default_context(cafile=str(ca_file))
        self.local = threading.local()
        self.connections = []
        self.lock = threading.Lock()
        self.table_id = None

    def client(self):
        client = getattr(self.local, 'client', None)
        if client is None:
            client = self.local.client = Http('https://api:8000', self.token, self.timeout, self.ca)
            with self.lock:
                self.connections.append(client)
        return client

    def operation(self, operation, payload):
        request_id = uid()
        response = self.client().exchange('/v1/operations', dict(request_id=request_id, operation=operation, payload=payload))
        return require_data(*response, request_id)

    def seed(self, rows):
        create = {'name': TABLE, 'schema': {'id': 'int64', 'value': 'int64'}, 'key': 'id', 'format': 'parquet'}
        descriptor = self.operation('table.create', create)
        self.table_id = descriptor['id']
        if descriptor != create | {'id': self.table_id}:
            raise RuntimeError(f'seed created an unexpected table descriptor: {descriptor}')
        for start in range(0, rows, 128):
            batch = [{'id': key, 'value': 0} for key in range(start, min(start + 128, rows))]
            count = self.operation('table.insert_many', {'table_id': self.table_id, 'rows': batch})
            if count != len(batch):
                raise RuntimeError(f'seed expected {len(batch)} inserted rows, got {count}')
        return {'descriptor': descriptor, 'data_placement': self.data_placement()}

    def data_placement(self):
        # The published genesis Data group IDs derive from the table and shard UUIDs.
        shard_bytes = hashlib.sha256(UUID(self.table_id).bytes + b'initial-shard').digest()[:16]
        group_bytes = hashlib.sha256(shard_bytes + b'data-group').digest()[:16]
        group = dict(cluster_id=self.metadata['cluster_id'], database=DATABASE, group_id=str(UUID(bytes=group_bytes)),
                     kind={'data': {'table_id': self.table_id, 'shard_id': str(UUID(bytes=shard_bytes))}},
                     group_format_version=1)
        client = Http('http://w1:8000', self.admin_token, self.timeout)
        try:
            request_id = uid()
            reply = client.exchange('/v1/cluster/operations', dict(request_id=request_id,
                operation='storage.replica.placement', payload={'group': group}))
            placement = require_data(*reply, request_id)
        finally:
            client.close()

        def members(voters):
            return {(voter['worker_id'], voter['pool_id']) for voter in voters}
        expected = self.metadata['placements']['control']['voters']
        if (placement['record']['identity'] != group or placement['record']['replica_count'] != len(WORKERS)
                or len(placement['members']) != len(WORKERS) or members(placement['members']) != members(expected)):
            raise RuntimeError(f'unexpected Data replica placement: {placement}')
        return placement

    def update(self, key, value, request_id):
        return self.point('table.update', {'table_id': self.table_id, 'key': key, 'values': {'value': value}},
                          request_id, lambda data: data is True, lambda _data: {})

    def get(self, key, request_id):
        def valid(row):
            # The soak table's rows also carry `payload`.
            return row is None or (isinstance(row, dict) and set(row) - {'payload'} == {'id', 'value'} and row['id'] == key
                                   and type(row['value']) is int)
        return self.point('table.get', {'table_id': self.table_id, 'key': key}, request_id, valid,
                          lambda row: {'read_value': None if row is None else row['value']})

    def point(self, operation, payload, request_id, expected, extract):
        client = self.client()
        try:
            status, response = client.exchange('/v1/operations', {
                'request_id': request_id, 'operation': operation, 'payload': payload,
            })
        except (OSError, http.client.HTTPException) as error:
            return client.sizes | {'status': 'unresolved', 'transport_error': type(error).__name__, 'message': str(error)}
        except ValueError as error:
            return client.sizes | {'status': 'unresolved', 'protocol_error': str(error)}
        details = client.sizes | {'http_status': status}
        if status != 200:
            details['response'] = response
        if status == 200:
            try:
                data = require_data(status, response, request_id)
            except RuntimeError:
                return details | {'status': 'unresolved', 'protocol_error': 'invalid success envelope',
                                  'response': response}
            if expected(data):
                return details | {'status': 'succeeded', 'accepted': True} | extract(data)
            return details | {'status': 'failed', 'accepted': True, 'unexpected_result': data}
        if isinstance(response, dict) and set(response) == {'request_id', 'error'} \
                and response['request_id'] in (None, request_id) and isinstance(response['error'], dict):
            error = response['error']
            if error.get('outcome') == 'not_applied':
                reported = 'rejected' if status == 429 and error.get('code') == 'overloaded' else 'failed'
                return details | {'status': reported, 'outcome': 'not_applied'}
        return details | {'status': 'unresolved', 'outcome': 'unknown'}

    def reconcile(self, samples):
        # Look up each original identity at most once and leave timed outcomes intact.
        client = Http('https://api:8000', self.token, min(self.timeout, 0.5), self.ca)
        deadline = time.monotonic() + 3
        run_id = self.metadata['run_id']
        try:
            for sample in samples:
                if time.monotonic() >= deadline:
                    break
                request_id = sample['request_id']
                try:
                    status, receipt = client.exchange(f'/v1/requests/{run_id}/{request_id}')
                except (OSError, http.client.HTTPException, ValueError) as error:
                    sample['reconciliation'] = {'error': f'{type(error).__name__}: {error}'}
                    continue
                observed = {'http_status': status, 'reply': receipt, 'observed_ns': time.monotonic_ns()}
                sample['reconciliation'] = observed
                if status != 200 or not isinstance(receipt, dict):
                    continue
                if (receipt.get('request_id'), receipt.get('run_id')) != (request_id, run_id):
                    continue
                state, error = receipt.get('state'), receipt.get('error')
                if state == 'succeeded' and receipt.get('result') is True and error is None:
                    observed['resolution'] = 'succeeded'
                elif state == 'failed' and isinstance(error, dict) and error.get('outcome') == 'not_applied':
                    observed['resolution'] = 'not_applied'
        finally:
            client.close()

    def read(self, _keys):
        query = {'select': None, 'filter': None, 'order_by': None, 'limit': None}
        payload = {'table_id': self.table_id, 'query': query}
        rows = self.operation('query.collect', payload)
        count = self.operation('query.count', payload)
        values = {}
        for row in rows:
            if set(row) != {'id', 'value'} or type(row['id']) is not int or type(row['value']) is not int:
                raise RuntimeError(f'invalid snapshot row: {row}')
            if row['id'] in values:
                raise RuntimeError(f'duplicate snapshot key: {row["id"]}')
            values[row['id']] = row['value']
        if len(values) != count:
            raise RuntimeError(f'snapshot contains {len(values)} distinct keys, exact count is {count}')
        return values

    def stage_stats(self):
        cgroup = Path('/sys/fs/cgroup')
        driver = {name: (cgroup / name).read_text().strip() for name in
                  ('cpu.stat', 'io.stat', 'memory.current', 'memory.peak', 'memory.events') if (cgroup / name).is_file()}
        network = Path('/proc/net/dev')
        if network.is_file():
            driver['network'] = network.read_text()
        stats = {'driver_cgroup': driver}
        client = Http('https://api:8000', self.token, min(self.timeout, 3), self.ca)
        try:
            status, reply = client.exchange('/v1/runtime/stats')
            stats['api'] = reply if status == 200 else {'status': status, 'response': reply}
        except (OSError, http.client.HTTPException, ValueError) as error:
            stats['api'] = {'error': f'{type(error).__name__}: {error}'}
        finally:
            client.close()
        for worker in WORKERS:
            client = Http(f'http://{worker}:8000', self.token, min(self.timeout, 3))
            try:
                status, reply = client.exchange('/v1/runtime/stats')
                stats[worker] = reply if status == 200 else {'status': status, 'response': reply}
            except (OSError, http.client.HTTPException, ValueError) as error:
                stats[worker] = {'error': f'{type(error).__name__}: {error}'}
            finally:
                client.close()
        return stats

    def summarize_stage(self, before, after):
        summary = self.summarize_queue_stage(before, after)
        summary['direct'] = summarize_direct(before, after)
        return summary

    def summarize_queue_stage(self, before, after):
        # b29a178 reports queue-owner stage stats; the direct path has none.
        for name in WORKERS:
            old = (before or {}).get(name, {}).get('accept_to_dispatch')
            new = (after or {}).get(name, {}).get('accept_to_dispatch')
            if not old or not new or old['worker'] != old['owner'] or new['worker'] != new['owner']:
                continue
            if old['view'] != new['view'] or old['upper_us'] != new['upper_us']:
                continue
            buckets = [end - start for start, end in zip(old['bucket_counts'], new['bucket_counts'], strict=True)]
            if min(buckets) < 0:
                continue
            bounds = new['upper_us'] + [None]
            count = sum(buckets)
            percentiles = {}
            for percentile in (50, 90, 95, 99):
                rank = math.ceil(count * percentile / 100)
                for bound, size in zip(bounds, buckets, strict=True):
                    rank -= size
                    if rank <= 0:
                        percentiles[f'p{percentile}'] = bound if count else None
                        break
            return {'source': name, 'accept_to_first_dispatch': {'count': count, 'bucket_counts': buckets,
                    'bucket_upper_us': bounds, 'percentile_upper_us': percentiles,
                    'waiting_before': old['waiting'], 'waiting_after': new['waiting'],
                    'discarded_before_dispatch': new['discarded_before_dispatch'] - old['discarded_before_dispatch']}}
        return {'accept_to_first_dispatch': None}

    def close(self):
        for client in self.connections:
            client.close()


def counter_delta(old, new):
    """Counter differences of two snapshots of the same shape (numbers and maps of numbers)."""
    if isinstance(new, bool) or new is None:
        return None
    if isinstance(new, (int, float)):
        return new - old if isinstance(old, (int, float)) and not isinstance(old, bool) else new
    if isinstance(new, dict):
        out = {}
        for key, value in new.items():
            delta = counter_delta((old or {}).get(key) if isinstance(old, dict) else None, value)
            if delta not in (None, {}, 0):
                out[key] = delta
        return out
    return None


def summarize_direct(before, after):
    """The `direct` section (plan "Benchmark"): per-worker leader counters and gateway counters over a phase."""
    out = {}
    for name in (*WORKERS, 'api'):
        old, new = (before or {}).get(name) or {}, (after or {}).get(name) or {}
        section = {}
        # PERF1: per-stage latency histograms; the API's own are `api_stage_timing`
        # (its `stage_timing` is the worker it forwards stats from).
        keys = ('direct', 'direct_gateway', 'api_stage_timing') if name == 'api' else (
            'direct', 'direct_gateway', 'stage_timing')
        for key in keys:
            if isinstance(new.get(key), dict):
                delta = counter_delta(old.get(key), new[key])
                # in_flight and max_in_flight are gauges.
                for gauge in ('in_flight', 'max_in_flight'):
                    if gauge in new[key] and key in ('direct', 'direct_gateway'):
                        delta[gauge] = new[key][gauge]
                section[key] = delta
        if isinstance(new.get('direct_capability'), dict):
            section['direct_capability'] = new['direct_capability']
        if section:
            out[name] = section
    return out or None


def soak_seed(adapter, args, output):
    """Rows with incompressible payloads until each replica holds well over 4x cache + write buffer."""
    create = {'name': SOAK_TABLE, 'schema': {'id': 'int64', 'value': 'int64', 'payload': 'utf8'},
              'key': 'id', 'format': 'parquet'}
    descriptor = adapter.operation('table.create', create)
    adapter.table_id = descriptor['id']
    batch_rows = max(1, (256 * 1024) // args.soak_payload_bytes)
    batches = [range(start, min(start + batch_rows, args.soak_rows)) for start in range(0, args.soak_rows, batch_rows)]
    payload_raw = (args.soak_payload_bytes * 3) // 4

    def payload(key):
        return base64.b64encode(random.Random(f'{args.seed}/{key}').randbytes(payload_raw)).decode()[:args.soak_payload_bytes]

    stats = {'batches': len(batches), 'rows_per_batch': batch_rows, 'rejected_retries': 0, 'errors': []}
    lock = threading.Lock()
    queue = list(reversed(batches))

    def worker():
        while True:
            with lock:
                if not queue:
                    return
                keys = queue.pop()
            rows = [{'id': key, 'value': 0, 'payload': payload(key)} for key in keys]
            for attempt in range(200):
                result = adapter.point('table.insert_many', {'table_id': adapter.table_id, 'rows': rows}, uid(),
                                       lambda count: count == len(rows), lambda _count: {})
                if result['status'] == 'succeeded':
                    break
                if result['status'] == 'rejected':
                    with lock:
                        stats['rejected_retries'] += 1
                    time.sleep(min(2, 0.05 * (attempt + 1)))
                    continue
                # A failed or unresolved insert_many: retrying could double-insert, so stop.
                raise RuntimeError(f'soak seed batch {keys.start}: {result}')
            else:
                raise RuntimeError(f'soak seed batch {keys.start} rejected 200 times')
    started = time.monotonic()
    errors = []

    def guarded():
        try:
            worker()
        except Exception as error:  # noqa: BLE001 - reported below
            errors.append(f'{type(error).__name__}: {error}')
    threads = [threading.Thread(target=guarded) for _ in range(args.soak_seed_clients)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError(f'soak seeding failed: {errors[:3]}')
    stats.update(seconds=time.monotonic() - started, rows=args.soak_rows, payload_bytes=args.soak_payload_bytes,
                 logical_bytes=args.soak_rows * (args.soak_payload_bytes + 16))
    return descriptor, stats


def run_soak(args, adapter, output, metadata):
    """NS11 storage soak: seed, then --soak-seconds of updates at --soak-rate in phases of distinct keys."""
    results = {'metadata': metadata, 'workload': 'storage soak: updates of rows with incompressible payloads',
               'settings': {field: getattr(args, field) for field in ('soak_rows', 'soak_payload_bytes', 'soak_seconds',
                            'soak_phase_seconds', 'soak_rate', 'soak_seed_clients', 'soak_verify_keys', 'clients',
                            'seed', 'timeout')},
               'phases': [], 'correctness_verified': False}

    def save():
        (output / 'results.tmp').write_text(json.dumps(results, indent=2) + '\n')
        (output / 'results.tmp').replace(output / 'results.json')
    output.mkdir(parents=True, exist_ok=True)
    latest, uncertain = {}, {}
    try:
        descriptor, seed_stats = soak_seed(adapter, args, output)
        results['seed_details'] = {'descriptor': descriptor, 'seed': seed_stats}
        results['seed_end_unix_ns'] = time.time_ns()
        save()
        args.rows = args.soak_rows
        phases = max(1, round(args.soak_seconds / args.soak_phase_seconds))
        next_value = 1
        with ThreadPoolExecutor(max_workers=args.clients) as executor:
            for index in range(phases):
                before_stats = adapter.stage_stats()
                phase, samples = measure(executor, args, adapter, args.soak_rate, args.soak_phase_seconds,
                                         random.Random(f'{args.seed}/soak/{index}').getrandbits(64), next_value)
                next_value += len(samples)
                phase.update(label=f'soak-{index}', rate=args.soak_rate, repetition=index + 1)
                phase['stage_stats_before'] = before_stats
                phase['stage_stats_after'] = adapter.stage_stats()
                phase['stage_summary'] = adapter.summarize_stage(before_stats, phase['stage_stats_after'])
                unresolved = [sample for sample in samples if sample['status'] == 'unresolved']
                if unresolved:
                    adapter.reconcile(unresolved)
                for sample in samples:
                    resolution = sample.get('reconciliation', {}).get('resolution')
                    if sample['status'] == 'succeeded' or resolution == 'succeeded':
                        latest[sample['key']] = sample['value']
                        uncertain.pop(sample['key'], None)
                    elif sample['status'] == 'unresolved' and resolution is None:
                        uncertain.setdefault(sample['key'], set()).add(sample['value'])
                phase['unresolved_after_reconciliation'] = sum(
                    sample['status'] == 'unresolved' and sample.get('reconciliation', {}).get('resolution') is None
                    for sample in samples)
                with (output / f'soak-{index}.jsonl').open('w') as stream:
                    for sample in samples:
                        stream.write(json.dumps(sample, separators=(',', ':')) + '\n')
                results['phases'].append(phase)
                save()
        # Verify a seeded sample of the written keys with point gets.
        check = sorted(random.Random(args.seed).sample(sorted(latest), min(args.soak_verify_keys, len(latest))))
        mismatches, uncertain_seen = [], 0
        for key in check:
            result = adapter.get(key, uid())
            if result['status'] != 'succeeded':
                mismatches.append({'key': key, 'get': result['status']})
                continue
            if result['read_value'] == latest[key]:
                continue
            if result['read_value'] in uncertain.get(key, ()):
                uncertain_seen += 1
                continue
            mismatches.append({'key': key, 'expected': latest[key], 'actual': result['read_value']})
        results['verification'] = {'keys_checked': len(check), 'mismatches': len(mismatches),
                                   'first_mismatches': mismatches[:10], 'later_unresolved_values_seen': uncertain_seen,
                                   'keys_with_unresolved_values': len(uncertain)}
        results['correctness_verified'] = not mismatches and all(
            not phase['unresolved_after_reconciliation'] for phase in results['phases'])
        latencies = [phase['success_latency_ms'] for phase in results['phases']]
        results['summary'] = {'offered': sum(phase['offered'] for phase in results['phases']),
                              'succeeded': sum(phase['statuses']['succeeded'] for phase in results['phases']),
                              'rejected': sum(phase['statuses']['rejected'] for phase in results['phases']),
                              'phase_p99_ms': [latency['p99'] for latency in latencies]}
        return results
    except Exception as error:
        results['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        save()
        adapter.close()


class Cluster:
    def __init__(self, args, directory):
        self.args, self.directory = args, directory
        self.worker_nano_cpus = dict(zip(WORKERS, args.worker_cpus, strict=True))
        self.prefix = 'nabedra-oltp-bench-' + uuid4().hex[:10]
        self.cluster_id, self.run_id, self.bootstrap_id = uid(), uid(), uid()
        self.containers, self.volumes = [], []
        self.auth = {role + '_token': secrets.token_urlsafe(32) for role in ('admin', 'data', 'enrollment')}
        self.network_created = False
        self.binary = '/fixture-nabedra-db' if args.binary else '/usr/local/bin/nabedra-db'
        self.execution = dict(EXECUTION)
        if args.direct_oltp is not None:
            # Omitted by default: b29a178 refuses the unknown field.
            self.execution['direct_oltp'] = args.direct_oltp == 'on'
        self.samples, self.sampling = [], threading.Event()
        self.worker_configs = {}
        self.key_spec = {'rsa': 'rsa:2048', 'ed25519': 'ed25519'}[args.tls_key_type]
        image, = json.loads(command('docker', 'image', 'inspect', args.image))
        self.image = image['Id']
        self.scoped_oltp = getattr(args, 'scoped_oltp', False)
        self.metadata = {'engine': 'nabedra', 'cluster_id': self.cluster_id, 'run_id': self.run_id,
                         'replication_factor': 3,
                         'queue_replication': 'scoped_rf3' if self.scoped_oltp else 'all_ready_workers',
                         'consistency': 'Nabedra production OLTP', 'protocol': 'synchronous HTTPS /v1/operations',
                         'image': self.image, 'image_os': image['Os'], 'image_architecture': image['Architecture'],
                         'contract_references': ['O24', 'Q01', 'B02'] + (['O30'] if self.scoped_oltp else []),
                         'rpc_trace': args.rpc_trace,
                         'tls_key_type': args.tls_key_type, 'direct_oltp': args.direct_oltp,
                         'strace': STRACE_CALLS if args.strace else None,
                         'orm_source': str(args.orm_source), 'pool_format_version': args.pool_format_version}
        if args.binary:
            with args.binary.open('rb') as binary:
                self.metadata['binary_sha256'] = hashlib.file_digest(binary, 'sha256').hexdigest()

    def docker(self, *args, **kwargs):
        return command('docker', *args, **kwargs)

    def openssl(self, *args):
        return command('openssl', *args)

    def certificate(self, name):
        private_key, csr, certificate, extension = (self.directory / f'{name}.{suffix}'
                                                      for suffix in ('key', 'csr', 'pem', 'ext'))
        extension.write_text(f'subjectAltName=DNS:{name},DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n')
        self.openssl('req', '-new', '-newkey', self.key_spec, '-nodes', '-keyout', private_key, '-out', csr,
                     '-subj', f'/CN={name}')
        self.openssl('x509', '-req', '-in', csr, '-CA', self.directory / 'ca.pem',
                     '-CAkey', self.directory / 'ca.key', '-set_serial', str(uuid4().int),
                     '-out', certificate, '-days', '1', '-extfile', extension)
        digest = hashlib.sha256(ssl.PEM_cert_to_DER_cert(certificate.read_text())).hexdigest()
        return dict(worker_id=uid(), incarnation=uid(), enrollment_revision=1, certificate_sha256=digest,
                    endpoint=f'{name}:9000', server_name=name)

    def launch(self, alias, cpu, memory, entry, arguments, *, options=()):
        name = f'{self.prefix}-{alias}'
        self.containers.append(name)
        mounts = ['--mount', f'type=bind,source={self.directory},target=/fixture,readonly',
                  '--mount', f'type=bind,source={ROOT},target=/source,readonly',
                  '--mount', f'type=bind,source={self.args.orm_source},target=/orm,readonly']
        if self.args.binary:
            mounts += ['--mount', f'type=bind,source={self.args.binary},target={self.binary},readonly']
        log_options = ['--log-opt', 'max-size=4m']
        if self.args.rpc_trace and alias != 'driver':
            log_options = ['--log-driver', 'json-file', '--log-opt', 'max-size=16m', '--log-opt', 'max-file=1',
                           '-e', 'NABEDRA_OLTP_RPC_TRACE=1']
        self.docker('run', '-d', '--pull=never', '--name', name, '--network', self.prefix, '--network-alias', alias,
                    '--cpus', cpu, '--memory', str(memory), '--memory-swap', str(memory), '--pids-limit', '96',
                    *log_options, '--user', '0:0', '-e', 'PYTHONDONTWRITEBYTECODE=1',
                    '-e', 'PYTHONPATH=/orm', *mounts, *options, '--entrypoint', entry,
                    self.image, *arguments)
        return name

    def host_url(self, alias, scheme='http'):
        alias_name = f'{self.prefix}-{alias}'
        address = self.docker('port', alias_name, '8000/tcp')
        host, port = address.rsplit(':', 1)
        if host != '127.0.0.1':
            raise RuntimeError(f'expected a loopback port for {alias}, got {address}')
        return f'{scheme}://localhost:{port}'

    def worker_ready(self, urls, ca=None, deadline_seconds=110):
        clients = {name: Http(url, self.auth['data_token'], 2, ca) for name, url in urls.items()}
        pending, ready = set(urls), {}
        deadline = time.monotonic() + deadline_seconds
        next_state_check = time.monotonic() + 2
        try:
            while pending and time.monotonic() < deadline:
                for name in sorted(pending):
                    try:
                        status, reply = clients[name].exchange('/health/ready')
                        ready[name] = reply
                        expected = {'engine': 'oltp', 'run_id': self.run_id, 'ready_workers': len(WORKERS)}
                        if name == 'api':
                            expected['topology'] = 'api-gateway'
                        else:
                            expected['replication_factor'] = len(WORKERS)
                        if status == 200 and all(reply.get(field) == value for field, value in expected.items()):
                            pending.remove(name)
                    except (OSError, http.client.HTTPException, ValueError) as error:
                        ready[name] = str(error)
                now = time.monotonic()
                if pending and now >= next_state_check:
                    next_state_check = now + 2
                    for name in sorted(pending):
                        state = json.loads(self.docker('inspect', '--format', '{{json .State}}', f'{self.prefix}-{name}'))
                        if not state['Running']:
                            raise RuntimeError(f'{name} exited during startup with code {state["ExitCode"]}')
                if pending:
                    time.sleep(0.25)
            if pending:
                raise RuntimeError(f'topology did not become ready ({sorted(pending)}): {ready}')
            return ready
        finally:
            for client in clients.values():
                client.close()

    def start(self):
        self.openssl('req', '-x509', '-newkey', self.key_spec, '-nodes', '-days', '1',
                     '-keyout', self.directory / 'ca.key', '-out', self.directory / 'ca.pem',
                     '-subj', '/CN=Nabedra OLTP benchmark')
        write_json(self.directory / 'auth.json', self.auth)
        (self.directory / 'auth.json').chmod(0o600)
        self.docker('network', 'create', '--driver', 'bridge', self.prefix)
        self.network_created = True
        enrollment = {name: self.certificate(name) for name in WORKERS}
        pool = {name: uid() for name in WORKERS}
        placements = {}
        for offset, (label, kind) in enumerate((('control', 'control'), ('timestamps', 'timestamps'),
                                                ('status', {'status': {'bucket': 0}}), ('catalog', 'catalog'))):
            order = WORKERS[offset % len(WORKERS):] + WORKERS[:offset % len(WORKERS)]
            if label == 'status' and self.args.status_seed_worker:
                seed = self.args.status_seed_worker
                order = (seed, *(worker for worker in order if worker != seed))
                self.metadata['status_seed_placement'] = {
                    'seed_worker': seed, 'workers_by_node_id': dict(enumerate(order, start=1)),
                }
            placements[label] = dict(identity=dict(cluster_id=self.cluster_id, database=DATABASE, group_id=uid(),
                                kind=kind, group_format_version=1), voters=[dict(node_id=index,
                                worker_id=enrollment[name]['worker_id'], pool_id=pool[name])
                                for index, name in enumerate(order, start=1)])
        if self.args.status_seed_worker:
            self.metadata['status_seed_placement']['group_id'] = placements['status']['identity']['group_id']
        enrollments = sorted(enrollment.values(), key=lambda record: record['worker_id'])
        operator_pins = sorted(record['certificate_sha256'] for record in enrollments)
        for index, name in enumerate(WORKERS, start=1):
            volume, marker = f'{self.prefix}-{name}', uid()
            self.docker('volume', 'create', '--label', f'io.nabedra.cluster={self.cluster_id}', volume)
            self.volumes.append(volume)
            inspected, = json.loads(self.docker('volume', 'inspect', volume))
            if inspected['Name'] != volume or inspected['Labels'].get('io.nabedra.cluster') != self.cluster_id:
                raise RuntimeError(f'Docker did not provision the requested worker volume: {volume}')
            write_json(self.directory / f'{name}-volume.json', dict(format_version=1, volume_name=volume, volume_marker=marker))
            worker = enrollment[name]
            identity = {field: worker[field] for field in ('worker_id', 'incarnation', 'enrollment_revision', 'certificate_sha256')}
            node = dict(format=1, directory='/data', pool_id=pool[name], scheduler_node_id=index,
                        enrollment=worker, bind='0.0.0.0:9000', certificate=f'/tmp/config/{name}.pem',
                        private_key=f'/tmp/config/{name}.key', authority_ca='/tmp/config/ca.pem',
                        bootstrap_id=self.bootstrap_id, initial_run=self.run_id, required_voters=len(WORKERS),
                        placements=placements, control_enrollments=enrollments, operator_pins=operator_pins,
                        mount=dict(profile={'docker': {'volume_marker': marker}}, worker=identity, generation=1,
                                   identity=dict(engine='oltp', pool_format_version=self.args.pool_format_version,
                                                 cluster_id=self.cluster_id,
                                                 worker_id=worker['worker_id'], pool_id=pool[name]),
                                   mount_path='/data', provider_volume_id='docker-volume:' + volume, filesystem_uuid=''))
            if self.scoped_oltp:
                node['scoped_oltp'] = True
            self.worker_configs[name] = dict(version=1, bind='0.0.0.0:8000',
                       auth_file='/tmp/config/auth.json', node=node, resources=resources(WORKER_MEMORY),
                       execution=self.execution)
            write_json(self.directory / f'{name}.json', self.worker_configs[name])
            # `mkdir -p`: a SIGKILLed worker restarts in the same container (recovery run).
            launch = (f'exec strace -f --seccomp-bpf -qq -ttt -T -e trace={STRACE_CALLS} -o /tmp/strace.log '
                      if self.args.strace else 'exec ')
            script = '''set -eu
umask 077
mkdir -p /tmp/config
cp /fixture/auth.json /fixture/ca.pem "/fixture/$1.json" "/fixture/$1.pem" "/fixture/$1.key" /tmp/config/
cp "/fixture/$1-volume.json" /data/.nabedra-docker-volume.json
''' + launch + '''"$2" worker --bootstrap "/tmp/config/$1.json"
'''
            cpu = format(Decimal(self.worker_nano_cpus[name]).scaleb(-9).normalize(), 'f')
            self.launch(name, cpu, WORKER_MEMORY, 'sh', ['-c', script, 'worker', name, self.binary],
                        options=('-p', '127.0.0.1::8000', '--mount', f'type=volume,source={volume},target=/data'))
        worker_urls = {name: self.host_url(name) for name in WORKERS}
        print('Waiting for three OLTP workers', flush=True)
        self.metadata['ready'] = self.worker_ready(worker_urls)
        control = Http(worker_urls['w1'], self.auth['admin_token'], 15)
        try:
            def admin(operation, payload):
                request_id = uid()
                response = control.exchange('/v1/cluster/operations', dict(request_id=request_id, operation=operation, payload=payload))
                return require_data(*response, request_id)

            connection = admin('cluster.connection', {})
            if len(connection['control_peers']) != len(WORKERS) or connection['cluster_id'] != self.cluster_id:
                raise RuntimeError(f'worker returned inconsistent cluster connection metadata: {connection}')
            api_enrollment = {'database': DATABASE, 'enrollment': self.certificate('api')}
            admin('api.enroll', {'expected': None, 'next': api_enrollment})
        finally:
            control.close()
        api_config = dict(version=1, engine='oltp', bind='0.0.0.0:8000', database=DATABASE,
                   auth_file='/tmp/config/auth.json', tls=dict(cert_file='/tmp/config/api.pem', key_file='/tmp/config/api.key'),
                   topology=dict(mode='remote', cluster_id=self.cluster_id, control_group_id=connection['control_group_id'],
                                 control_peers=connection['control_peers'], enrollment=api_enrollment,
                                 worker_tls=dict(cert_file='/tmp/config/api.pem', key_file='/tmp/config/api.key',
                                                 ca_file='/tmp/config/ca.pem')),
                   worker=dict(resources=resources(API_MEMORY), execution=EXECUTION, queue_size=128))
        if self.args.direct_oltp is not None:
            api_config['topology']['direct_oltp'] = self.args.direct_oltp == 'on'
        if self.scoped_oltp:
            api_config['topology']['scoped_oltp'] = True
        write_json(self.directory / 'api.json', api_config)
        script = '''set -eu
umask 077
mkdir /tmp/config
cp /fixture/auth.json /fixture/ca.pem /fixture/api.json /fixture/api.pem /fixture/api.key /tmp/config/
export NABEDRA_SERVER_BINARY="$1"
if [ "${NABEDRA_OLTP_RPC_TRACE:-}" = 1 ]; then
    # The CLI drains its child's stderr. Send benchmark-only native traces to Docker.
    export NABEDRA_BENCHMARK_BINARY="$1"
    cat > /tmp/config/trace-server <<'TRACE_WRAPPER'
#!/bin/sh
exec "$NABEDRA_BENCHMARK_BINARY" "$@" 2>/proc/1/fd/2
TRACE_WRAPPER
    chmod 700 /tmp/config/trace-server
    export NABEDRA_SERVER_BINARY=/tmp/config/trace-server
fi
exec python3 -m nabedra.cli database serve --config /tmp/config/api.json
'''
        self.launch('api', API_CPU, API_MEMORY, 'sh', ['-c', script, 'api', self.binary], options=('-p', '127.0.0.1::8000'))
        print('Waiting for the remote OLTP API', flush=True)
        self.metadata['ready'].update(self.worker_ready({'api': self.host_url('api', 'https')},
                                     ca=ssl.create_default_context(cafile=str(self.directory / 'ca.pem')), deadline_seconds=40))
        if self.scoped_oltp:
            control = Http(worker_urls['w1'], self.auth['admin_token'], 15)
            try:
                deadline = time.monotonic() + 30
                while True:
                    capability = admin('queue.scoped.status', {})
                    if capability['activation'] is not None:
                        active = capability
                        break
                    try:
                        active = admin('queue.scoped.admission', {'expected': capability, 'enabled': True})
                        break
                    except RuntimeError:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.25)
                if active['version'] != 2 or active['run_id'] != self.run_id or active['activation'] is None:
                    raise RuntimeError(f'Control did not activate scoped OLTP: {active}')
                self.metadata['scoped_activation'] = active
            finally:
                control.close()
        if self.args.direct_oltp is not None:
            self.metadata['direct_capability'] = self.wait_direct_capability(worker_urls)
        worker = f'{self.prefix}-w1'
        version = self.docker('exec', worker, self.binary, '--version')
        if 'oltp' not in version.split():
            raise RuntimeError(f'expected an OLTP binary, got {version}')
        if 'binary_sha256' not in self.metadata:
            self.metadata['binary_sha256'] = self.docker('exec', worker, 'sha256sum', self.binary).split()[0]
        self.metadata.update(server_version=version, placements=placements, execution=EXECUTION)

    def wait_direct_capability(self, urls, deadline_seconds=120):
        """`data.direct.v1` activates on its own once every member supports it (plan "Rollout")."""
        deadline = time.monotonic() + deadline_seconds
        started = time.monotonic()
        while True:
            capability = {}
            for name, url in urls.items():
                client = Http(url, self.auth['data_token'], 3)
                try:
                    status, reply = client.exchange('/v1/runtime/stats')
                    capability[name] = reply.get('direct_capability') if status == 200 else {'status': status}
                except (OSError, http.client.HTTPException, ValueError) as error:
                    capability[name] = {'error': str(error)}
                finally:
                    client.close()
            if all((value or {}).get('active') for value in capability.values()):
                return {'workers': capability, 'seconds_after_api_ready': time.monotonic() - started}
            if time.monotonic() >= deadline:
                raise RuntimeError(f'data.direct.v1 did not activate: {capability}')
            time.sleep(0.5)

    def cgroup_sample(self, name):
        files = ' '.join(f'/sys/fs/cgroup/{file}' for file in CGROUP_FILES)
        raw = self.docker('exec', f'{self.prefix}-{name}', 'sh', '-c',
                          f'date +%s%N; for f in {files}; do echo "@$f"; cat "$f" 2>/dev/null; done', timeout=20)
        lines = raw.splitlines()
        sample, current = {'worker': name, 'unix_ns': int(lines[0]), 'files': {}}, None
        for line in lines[1:]:
            if line.startswith('@/sys/fs/cgroup/'):
                current = line.rsplit('/', 1)[1]
                sample['files'][current] = ''
            elif current:
                sample['files'][current] += line + '\n'
        return sample

    def sample_cgroups(self, interval):
        # Worker cgroup counters on the Docker VM's wall clock, which the driver's phases also record.
        while not self.sampling.wait(interval):
            for name in WORKERS:
                try:
                    self.samples.append(self.cgroup_sample(name))
                except (RuntimeError, ValueError, IndexError, subprocess.SubprocessError) as error:
                    self.samples.append({'worker': name, 'error': str(error)})

    def recover(self, name):
        """SIGKILL one worker, restart it in place, time process start to serving (NS11 G6)."""
        container = f'{self.prefix}-{name}'
        record = {'worker': name, 'before_kill': self.cgroup_sample(name)}
        self.docker('kill', '--signal', 'KILL', container)
        self.docker('wait', container, timeout=60)
        record['killed_state'] = json.loads(self.docker('inspect', '--format', '{{json .State}}', container))
        start = time.monotonic()
        self.docker('start', container)
        started = json.loads(self.docker('inspect', '--format', '{{json .State}}', container))
        record['restarted_state'] = started
        url = self.host_url(name)
        client = Http(url, self.auth['data_token'], 2)
        ready = None
        try:
            while time.monotonic() - start < 300:
                try:
                    status, reply = client.exchange('/health/ready')
                    if status == 200 and reply.get('ready_workers') == len(WORKERS) and reply.get('run_id') == self.run_id:
                        ready = time.monotonic() - start
                        record['ready_reply'] = reply
                        break
                except (OSError, http.client.HTTPException, ValueError):
                    client.close()
                    client = Http(url, self.auth['data_token'], 2)
                time.sleep(0.05)
        finally:
            client.close()
        record['ready_seconds'] = ready
        # The process start, from the container's own clock, to the readiness reply.
        record['process_started_at'] = started.get('StartedAt')
        if ready is not None and self.args.direct_oltp == 'on':
            capability_start = time.monotonic()
            try:
                record['direct_capability_after_restart'] = self.wait_direct_capability({name: url}, 120)
                record['direct_active_seconds'] = ready + time.monotonic() - capability_start
            except RuntimeError as error:
                record['direct_capability_error'] = str(error)
        # A write and a read through the API after the restart: the cluster serves.
        ca = ssl.create_default_context(cafile=str(self.directory / 'ca.pem'))
        api = Http(self.host_url('api', 'https'), self.auth['data_token'], 30, ca)
        try:
            probe_start = time.monotonic()
            results = json.loads((self.args.output / 'results.json').read_text())
            table_id = results['seed_details']['descriptor']['id']
            request_id = uid()
            status, reply = api.exchange('/v1/operations', dict(request_id=request_id, operation='table.get',
                                         payload={'table_id': table_id, 'key': 0}))
            record['probe'] = {'operation': 'table.get', 'status': status, 'seconds': time.monotonic() - probe_start,
                               'ok': status == 200 and reply.get('request_id') == request_id}
        except (OSError, http.client.HTTPException, ValueError, KeyError) as error:
            record['probe'] = {'error': str(error)}
        finally:
            api.close()
        record['after_restart'] = self.cgroup_sample(name)
        return record

    def ready_on_run(self, name, run_id, start, deadline_seconds=300):
        """Seconds from `start` until `name` answers ready for `run_id` with every worker ready."""
        while time.monotonic() - start < deadline_seconds:
            try:
                client = Http(self.host_url(name), self.auth['data_token'], 2)
            except RuntimeError:
                time.sleep(0.05)
                continue
            try:
                status, reply = client.exchange('/health/ready')
                if status == 200 and reply.get('run_id') == run_id and reply.get('ready_workers') == len(WORKERS):
                    return time.monotonic() - start, reply
            except (OSError, http.client.HTTPException, ValueError):
                pass
            finally:
                client.close()
            time.sleep(0.05)
        return None, None

    def cold_restart(self):
        """G6 substitute: SIGKILL every worker, restart all with the cold-start profile (R04, D09).

        A registered worker that crashed cannot rejoin its run in place (both b29a178 and the
        candidate refuse a plain restart); the supported recovery after an unplanned stop of the
        whole worker set is the cold start onto a new run. Timed from the first process start
        until every worker serves the new run, then until a point get through the API succeeds."""
        record = {'mode': 'cold restart of all workers after SIGKILL', 'previous_run': self.run_id}
        record['before_kill'] = {name: self.cgroup_sample(name) for name in WORKERS}
        for name in WORKERS:
            self.docker('kill', '--signal', 'KILL', f'{self.prefix}-{name}')
        for name in WORKERS:
            self.docker('wait', f'{self.prefix}-{name}', timeout=60)
        profile = dict(previous_run=self.run_id, next_run=uid(), transition_id=uid())
        nexts = {}
        for name in WORKERS:
            previous = self.worker_configs[name]['node']['enrollment']
            nexts[name] = self.certificate(name) | dict(worker_id=previous['worker_id'],
                                                        enrollment_revision=previous['enrollment_revision'] + 1)
        peers = sorted(nexts.values(), key=lambda item: item['worker_id'])
        pins = sorted(item['certificate_sha256'] for item in peers)
        for name in WORKERS:
            node = self.worker_configs[name]['node']
            node.update(cold=profile, enrollment=nexts[name], control_enrollments=peers, operator_pins=pins)
            node['mount']['worker'] = {field: nexts[name][field] for field in
                                       ('worker_id', 'incarnation', 'enrollment_revision', 'certificate_sha256')}
            node['mount']['generation'] += 1
            write_json(self.directory / f'{name}.json', self.worker_configs[name])
        record['profile'] = profile
        start = time.monotonic()
        for name in WORKERS:
            self.docker('start', f'{self.prefix}-{name}')
        record['start_command_seconds'] = time.monotonic() - start
        record['started_at'] = {name: json.loads(self.docker('inspect', '--format', '{{json .State}}',
                                                            f'{self.prefix}-{name}'))['StartedAt'] for name in WORKERS}
        ready = {}
        for name in WORKERS:
            seconds, reply = self.ready_on_run(name, profile['next_run'], start)
            ready[name] = {'seconds': seconds, 'reply': reply}
        record['ready'] = ready
        times = [item['seconds'] for item in ready.values()]
        record['all_ready_seconds'] = None if None in times else max(times)
        self.run_id = profile['next_run']
        if self.args.direct_oltp == 'on' and record['all_ready_seconds'] is not None:
            try:
                urls = {name: self.host_url(name) for name in WORKERS}
                record['direct_capability'] = self.wait_direct_capability(urls, 120)
                record['direct_active_seconds'] = time.monotonic() - start
            except RuntimeError as error:
                record['direct_capability_error'] = str(error)
        # Serving: the first point get through the API (it reconnects on its own or not at all).
        ca = ssl.create_default_context(cafile=str(self.directory / 'ca.pem'))
        probe = {'attempts': 0}
        try:
            results = json.loads((self.args.output / 'results.json').read_text())
            table_id = results['seed_details']['descriptor']['id']
            while time.monotonic() - start < 180:
                probe['attempts'] += 1
                api = Http(self.host_url('api', 'https'), self.auth['data_token'], 10, ca)
                try:
                    request_id = uid()
                    status, reply = api.exchange('/v1/operations', dict(request_id=request_id,
                                                 operation='table.get', payload={'table_id': table_id, 'key': 0}))
                    probe['last'] = {'status': status, 'reply': reply}
                    if status == 200 and reply.get('request_id') == request_id:
                        probe['served_seconds'] = time.monotonic() - start
                        break
                except (OSError, http.client.HTTPException, ValueError) as error:
                    probe['last'] = {'error': f'{type(error).__name__}: {error}'}
                finally:
                    api.close()
                time.sleep(0.25)
        except (OSError, KeyError, ValueError) as error:
            probe['error'] = str(error)
        record['api_probe'] = probe
        record['after_restart'] = {name: self.cgroup_sample(name) for name in WORKERS}
        return record

    def run(self):
        args = self.args
        sources = (Path(__file__), Path(__file__).with_name('benchmark_oltp_workload.py'))
        self.metadata['benchmark_source_sha256'] = {source.name: hashlib.sha256(source.read_bytes()).hexdigest()
                                                    for source in sources}
        self.metadata['workspace_head'] = command('git', '-C', ROOT, 'rev-parse', 'HEAD')
        manifest = self.docker('info', '--format', '{{json .}}')
        host = json.loads(manifest)
        self.metadata['docker_environment'] = {key: host.get(key) for key in (
            'ServerVersion', 'OperatingSystem', 'Architecture', 'KernelVersion', 'NCPU', 'MemTotal', 'CgroupVersion')}
        self.metadata['allocations'] = {'worker_bytes': WORKER_MEMORY,
            'api_cpu': float(API_CPU), 'api_bytes': API_MEMORY, 'driver_cpu': float(DRIVER_CPU),
            'driver_bytes': DRIVER_MEMORY, 'total_cpu': 4, 'total_bytes': 7_900_000_000}
        allocations = self.metadata['allocations']
        if args.worker_cpus == DEFAULT_WORKER_CPUS:
            allocations['worker_cpu'] = 1.0
        else:
            allocations['worker_cpu_by_role'] = {worker: quota / NANO_CPUS
                                                for worker, quota in self.worker_nano_cpus.items()}
        write_json(self.directory / 'metadata.json', self.metadata)
        settings = ['--rates', ','.join(map(str, args.rates))]
        for field in ('seconds', 'warmup_seconds', 'repetitions', 'clients', 'rows', 'seed', 'timeout',
                      'operation', 'read_fraction', 'soak_rows', 'soak_payload_bytes', 'soak_seconds',
                      'soak_phase_seconds', 'soak_rate', 'soak_seed_clients', 'soak_verify_keys'):
            settings += ['--' + field.replace('_', '-'), str(getattr(args, field))]
        if args.stop_p99_ms is not None:
            settings += ['--stop-p99-ms', str(args.stop_p99_ms)]
        if args.stop_on_rejection:
            settings.append('--stop-on-rejection')
        if args.soak:
            settings.append('--soak')
        sampler = threading.Thread(target=self.sample_cgroups, args=(args.cgroup_interval,), daemon=True)
        sampler.start()
        try:
            driver = self.launch('driver', DRIVER_CPU, DRIVER_MEMORY, 'python3',
                ['/source/scripts/benchmark_oltp_nabedra.py', '--driver', '--output', '/results', *settings])
            print(f'Running the OLTP workload in {driver}', flush=True)
            if args.perf_record:
                profiler = threading.Thread(target=self.perf_record, daemon=True)
                profiler.start()
            budget = len(args.rates) * (args.warmup_seconds + args.repetitions * args.seconds
                                        + (args.repetitions + 1) * (args.timeout + 30)) + 120
            if args.soak:
                budget = 7200 + 2 * args.soak_seconds
            state = int(self.docker('wait', driver, timeout=budget))
            self.docker('cp', f'{driver}:/results/.', str(args.output), timeout=120)
            if args.perf_record:
                profiler.join(timeout=args.perf_record + 600)
        finally:
            self.sampling.set()
            sampler.join(timeout=60)
            with (args.output / 'cgroup-samples.jsonl').open('w') as stream:
                for sample in self.samples:
                    stream.write(json.dumps(sample, separators=(',', ':')) + '\n')
        if args.kill_worker:
            try:
                recovery = self.recover(args.kill_worker)
            except (RuntimeError, subprocess.SubprocessError, OSError) as error:
                recovery = {'error': str(error)}
            write_json(args.output / 'recovery.json', recovery)
        if args.cold_restart:
            try:
                recovery = self.cold_restart()
            except (RuntimeError, subprocess.SubprocessError, OSError) as error:
                recovery = {'error': str(error)}
            write_json(args.output / 'cold-restart.json', recovery)
        for name in WORKERS:
            try:
                usage = self.docker('exec', f'{self.prefix}-{name}', 'du', '-sb', '/data', timeout=60)
                self.metadata.setdefault('data_bytes', {})[name] = int(usage.split()[0])
            except (RuntimeError, ValueError, subprocess.SubprocessError) as error:
                self.metadata.setdefault('data_bytes', {})[name] = str(error)
        return state

    def perf_record(self):
        """PERF1: on a Linux host, sample every worker's CPU stacks with perf while the workload runs."""
        args = self.args
        try:
            time.sleep(args.perf_delay)
            pids = {name: self.docker('inspect', '--format', '{{.State.Pid}}', f'{self.prefix}-{name}').strip()
                    for name in WORKERS}
            write_json(args.output / 'perf-pids.json', pids)
            data = args.output / 'perf.data'
            # The workers run as root in their containers: perf needs root to attach.
            perf = ['perf'] if os.geteuid() == 0 else ['sudo', '-n', 'perf']
            subprocess.run([*perf, 'record', '-F', str(args.perf_frequency), '--call-graph', 'dwarf,8192',
                            '-p', ','.join(pids.values()), '-o', str(data), '--', 'sleep', str(args.perf_record)],
                           check=True, capture_output=True, timeout=args.perf_record + 300)
            with (args.output / 'perf-report.txt').open('w') as stream:
                subprocess.run([*perf, 'report', '-i', str(data), '--stdio', '--no-children', '--sort', 'pid,dso,sym',
                                '--percent-limit', '0.2', '-g', 'none'], stdout=stream, stderr=subprocess.STDOUT,
                               timeout=900)
            folded = {}
            script = subprocess.Popen([*perf, 'script', '-i', str(data), '-F', 'pid,ip,sym'],
                                      stdout=subprocess.PIPE, text=True, errors='replace')
            pid, stack = None, []
            for line in [*script.stdout, '']:
                line = line.rstrip('\n')
                if not line.strip():
                    if pid is not None and stack:
                        key = (pid, ';'.join(reversed(stack)))
                        folded[key] = folded.get(key, 0) + 1
                    pid, stack = None, []
                elif pid is None:
                    pid = line.split()[0]
                else:
                    fields = line.split(None, 1)
                    symbol = fields[1] if len(fields) > 1 else fields[0]
                    stack.append(re.sub(r'\+0x[0-9a-f]+$', '', symbol).replace(';', ':'))
            script.wait(timeout=900)
            names = {pid: name for name, pid in pids.items()}
            for name in WORKERS:
                with (args.output / f'perf-folded-{name}.txt').open('w') as stream:
                    for (pid, stack), count in sorted(folded.items()):
                        if names.get(pid) == name:
                            stream.write(f'{stack} {count}\n')
            subprocess.run([*perf[:-1], 'rm', '-f', str(data)] if perf[0] == 'sudo' else ['rm', '-f', str(data)],
                           check=False, timeout=60)
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            write_json(args.output / 'perf-error.json', {'error': f'{type(error).__name__}: {error}'})

    def record_status_leader_hints(self):
        placement = self.metadata['status_seed_placement']
        workers = placement['workers_by_node_id']
        observations = {}
        for role in WORKERS:
            log_path = self.args.output / f'{self.prefix}-{role}.log'
            if not log_path.is_file():
                continue
            for timestamp, group_id, observer_id, leader_id in STATUS_LEADER_HINT.findall(
                    log_path.read_text(errors='replace')):
                leader = workers.get(int(leader_id))
                if group_id != placement['group_id'] or workers.get(int(observer_id)) != role or leader is None:
                    continue
                observation = observations.setdefault((role, leader), {
                    'observer': role, 'hinted_leader': leader, 'count': 0,
                    'first_timestamp': timestamp, 'last_timestamp': timestamp,
                })
                observation['count'] += 1
                observation['first_timestamp'] = min(observation['first_timestamp'], timestamp)
                observation['last_timestamp'] = max(observation['last_timestamp'], timestamp)
        placement['observed_follower_hints'] = list(observations.values())

    def finish(self):
        errors, runtime = [], {}
        for name in reversed(self.containers):
            try:
                item, = json.loads(self.docker('inspect', name))
                limits = item['HostConfig']
                runtime[name] = dict(state=item['State'], nano_cpus=limits['NanoCpus'],
                                     memory=limits['Memory'], memory_swap=limits['MemorySwap'])
                if item['State']['OOMKilled']:
                    errors.append(f'{name} was OOM-killed')
                if item['State']['Running']:
                    runtime[name]['cgroup_final'] = self.docker('exec', name, 'sh', '-c',
                        'for f in /sys/fs/cgroup/cpu.max /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/io.stat '
                        '/sys/fs/cgroup/memory.current '
                        '/sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.events /sys/fs/cgroup/memory.stat '
                        '/proc/net/dev /proc/net/snmp /proc/net/netstat; do '
                        'if [ -f "$f" ]; then echo "$f"; cat "$f"; fi; done')
                if self.args.strace and name.rsplit('-', 1)[-1] in WORKERS:
                    subprocess.run(['docker', 'cp', f'{name}:/tmp/strace.log',
                                    str(self.args.output / f'{name}.strace.log')], timeout=120, check=False)
                log_path = self.args.output / f'{name}.log'
                if self.args.rpc_trace and not name.endswith('-driver'):
                    with log_path.open('w+b') as log:
                        subprocess.run(['docker', 'logs', name], stdout=log, stderr=subprocess.STDOUT,
                                       timeout=20, check=True)
                        if log.tell() > TRACE_LOG_BYTES:
                            log.seek(-TRACE_LOG_BYTES, SEEK_END)
                            log.readline()
                            tail = log.read()
                            log.seek(0)
                            log.write(tail)
                            log.truncate()
                else:
                    logs = subprocess.run(['docker', 'logs', '--tail', '1000', name],
                                          capture_output=True, timeout=10, check=True)
                    log_path.write_bytes(logs.stdout + logs.stderr)
            except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
                errors.append(str(error))
            try:
                self.docker('rm', '-f', name)
            except (RuntimeError, subprocess.SubprocessError) as error:
                errors.append(str(error))
        for volume in self.volumes:
            try:
                self.docker('volume', 'rm', volume)
            except (RuntimeError, subprocess.SubprocessError) as error:
                errors.append(str(error))
        if self.network_created:
            try:
                self.docker('network', 'rm', self.prefix)
            except (RuntimeError, subprocess.SubprocessError) as error:
                errors.append(str(error))
        limits = {worker: (cpu, WORKER_MEMORY) for worker, cpu in self.worker_nano_cpus.items()}
        limits.update(api=(350_000_000, API_MEMORY), driver=(650_000_000, DRIVER_MEMORY))
        cap_errors = []
        for name, record in runtime.items():
            role = name.rsplit('-', 1)[-1]
            cpu, memory = limits[role]
            if (record['nano_cpus'], record['memory'], record['memory_swap']) != (cpu, memory, memory):
                cap_errors.append(f'{name} did not have the requested CPU, memory and swap caps')
        total_cpu = sum(item['nano_cpus'] for item in runtime.values())
        total_memory = sum(item['memory'] for item in runtime.values())
        budget_verified = (not cap_errors and len(runtime) == len(WORKERS) + 2
                           and total_cpu <= 4_000_000_000 and total_memory <= 8_000_000_000)
        errors.extend(cap_errors)
        if not budget_verified:
            errors.append('could not verify the complete cluster and driver resource budget')
        if 'status_seed_placement' in self.metadata:
            try:
                self.record_status_leader_hints()
            except OSError as error:
                errors.append(f'could not read Status leader hints: {error}')
        write_json(self.args.output / 'container-runtime.json', {'containers': runtime, 'errors': errors,
                   'total_nano_cpus': total_cpu, 'total_memory_bytes': total_memory, 'budget_verified': budget_verified})
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', help='locally available Docker image containing Python 3.11 and nabedra-db')
    parser.add_argument('--binary', type=Path, help='use this release Linux binary with the image instead')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--worker-cpus', type=worker_cpu_quotas, default=DEFAULT_WORKER_CPUS,
                        help='comma-separated CPU quotas for w1, w2 and w3 (default: 1,1,1)')
    parser.add_argument('--status-seed-worker', choices=WORKERS,
                        help='select the first voter that initializes the fresh RF3 Status group (default: w3)')
    parser.add_argument('--rpc-trace', action='store_true', help='capture bounded native RPC logs for a diagnostic run')
    parser.add_argument('--scoped-oltp', action='store_true', help='activate the RF3 scoped point path')
    parser.add_argument('--tls-key-type', choices=('rsa', 'ed25519'), default='rsa',
                        help='key type for fixture certificates, including the CA (default: rsa)')
    parser.add_argument('--direct-oltp', choices=('on', 'off'),
                        help='set direct_oltp on every worker and the API (omit for b29a178, which predates it)')
    parser.add_argument('--orm-source', type=Path, default=ROOT / 'orm' / 'python' / 'src',
                        help='Python ORM source tree for the API CLI (use the baseline revision\'s tree for b29a178)')
    parser.add_argument('--pool-format-version', type=int, choices=(1, 2), default=2,
                        help='mount identity pool format: 2 for nabedra-kv pools, 1 for b29a178 (RocksDB)')
    parser.add_argument('--strace', action='store_true',
                        help=f'run each worker under strace -f -e trace={STRACE_CALLS} (fsync counts; distorts latency)')
    parser.add_argument('--kill-worker', choices=WORKERS, help='after the workload, SIGKILL this worker and time its restart')
    parser.add_argument('--cold-restart', action='store_true',
                        help='after the workload, SIGKILL every worker and time the cold start onto a new run')
    parser.add_argument('--cgroup-interval', type=float, default=5, help='seconds between worker cgroup samples')
    parser.add_argument('--perf-record', type=float, default=0,
                        help='Linux host only: perf-sample the workers for this many seconds during the workload')
    parser.add_argument('--perf-delay', type=float, default=20, help='seconds after the driver starts before perf starts')
    parser.add_argument('--perf-frequency', type=int, default=249)
    parser.add_argument('--soak', action='store_true', help='run the storage soak instead of the rate sweep')
    parser.add_argument('--soak-rows', type=int, default=80_000)
    parser.add_argument('--soak-payload-bytes', type=int, default=2048)
    parser.add_argument('--soak-seconds', type=float, default=600)
    parser.add_argument('--soak-phase-seconds', type=float, default=60)
    parser.add_argument('--soak-rate', type=float, default=50)
    parser.add_argument('--soak-seed-clients', type=int, default=4)
    parser.add_argument('--soak-verify-keys', type=int, default=500, help='written keys checked by point gets after the soak')
    parser.add_argument('--driver', action='store_true', help=argparse.SUPPRESS)
    add_workload_args(parser)
    args = parser.parse_args()
    try:
        if args.soak:
            if args.soak_rate * args.soak_phase_seconds > args.soak_rows or args.soak_rate <= 0:
                raise ValueError('each soak phase must offer between 1 and --soak-rows distinct updates')
        else:
            validate_args(args)
    except ValueError as error:
        parser.error(str(error))
    if args.driver:
        auth = json.loads(Path('/fixture/auth.json').read_text())
        metadata = json.loads(Path('/fixture/metadata.json').read_text())
        if metadata['rpc_trace']:
            before = time.monotonic_ns()
            unix_ns = time.time_ns()
            after = time.monotonic_ns()
            metadata['trace_clock'] = {'unix_ns': unix_ns, 'monotonic_ns': (before + after) // 2,
                                       'sample_interval_ns': after - before}
        adapter = Nabedra(Path('/fixture/ca.pem'), auth, args.timeout, metadata)
        if args.soak:
            results = run_soak(args, adapter, args.output, metadata)
            return int(not results['correctness_verified'])
        results = run_workload(args, adapter, args.output, metadata)
        return int(not results['correctness_verified'])
    if not args.image:
        parser.error('--image is required')
    if args.binary:
        args.binary = args.binary.resolve(strict=True)
    args.orm_source = args.orm_source.resolve(strict=True)
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        parser.error('--output must be empty')
    with tempfile.TemporaryDirectory(prefix='.nabedra-oltp-benchmark-', dir=Path.home()) as directory:
        cluster = Cluster(args, Path(directory))
        try:
            cluster.start()
            result = cluster.run()
        finally:
            errors = cluster.finish()
            write_json(args.output / 'cluster-metadata.json', cluster.metadata)
    return result or int(bool(errors))


if __name__ == '__main__':
    raise SystemExit(main())

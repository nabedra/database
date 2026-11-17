#!/usr/bin/env python3.11
"""Measure ST06 in Docker against an OLAP API, workers and local object store."""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import random
import sqlite3
import string
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from acceptance_scheduler import request, wait_for
from acceptance_scheduler_load import (
    LoadCluster, Observer, distribution, execution, machine, observed_terminal,
)
from nabedra import Database


WARMUP_AND_REPETITIONS = 4
RANKS = 2
WORKER_THREADS = (1, 2, 1)
STORE_PREFIX = '/v1/objects/scheduler/'


@dataclass(frozen=True)
class Workload:
    name: str
    rows: int
    payload_width: tuple[int, int]
    batch_rows: int
    train_delay_ms: int = 0
    storage_jitter_ms: tuple[int, ...] = (0,)
    sparse: bool = False
    ordinary_rate: int = 0


def seed_table(db, name, count, width, seed):
    rng = random.Random(seed)
    alphabet = string.ascii_letters + string.digits
    rows = [dict(id=index, selected=index % 4 == 0,
                 payload=''.join(rng.choices(alphabet, k=rng.randint(*width))))
            for index in range(count)]
    table = db.create_table(name, {'id': int, 'selected': bool, 'payload': str}, key='id')
    assert table.insert_many(rows) == count
    return table, rows


class BenchmarkCluster(LoadCluster):
    def worker_resources(self, threads):
        return {'max_threads': threads, 'memory_limit_bytes': (512 + threads * 256) << 20,
                'task_memory_limit_bytes': 256 << 20, 'control_memory_reserve_bytes': 128 << 20}

    def manifest(self, name):
        with sqlite3.connect(self.database) as database:
            body, = database.execute(
                "SELECT body FROM objects WHERE key='scheduler/catalog.json'",
            ).fetchone()
        catalog = json.loads(body)
        return catalog['commit_id'], {
            STORE_PREFIX + file['object']: file for file in catalog['tables'][name]['data']['files']
        }


def cgroup():
    root = Path('/sys/fs/cgroup')
    result = {}
    for name in ('cpu.max', 'cpu.stat', 'memory.max', 'memory.current', 'memory.peak', 'memory.events'):
        if (root / name).is_file():
            value = (root / name).read_text().strip()
            if name in ('cpu.stat', 'memory.events'):
                result[name] = {key: int(number) for key, number in (line.split() for line in value.splitlines())}
            else:
                result[name] = value
    return result


def millisecond_distribution(seconds):
    return distribution([value * 1000 for value in seconds])


def read_rank(query, snapshot, oracle, workload, rank, depth, start, first_batch):
    start.wait(15)
    started = time.monotonic()
    rows = []
    waits = []
    with query.stream(batch_rows=workload.batch_rows, prefetch_batches=depth,
                      snapshot=snapshot, shard=rank) as batches:
        opened = time.monotonic()
        for remaining in range(0, len(oracle), workload.batch_rows):
            requested = time.monotonic()
            block = next(batches)
            arrived = time.monotonic()
            waits.append(arrived - requested)
            expected = oracle[remaining:remaining + workload.batch_rows]
            assert block == expected, (rank, depth, remaining, len(block), len(expected))
            rows.extend(block)
            first_batch.set()
            if workload.train_delay_ms:
                time.sleep(workload.train_delay_ms / 1000)
        assert next(batches, None) is None, (rank, depth, 'extra batch after complete shard')
        finished = time.monotonic()
    return dict(rank=rank, started=started, opened=opened, finished=finished, rows=rows, waits=waits)


def ordinary_work(cluster, table, rows, rate, first_batch, stop):
    records = []
    while not stop.is_set() and not first_batch.wait(.05):
        pass
    start = time.monotonic()
    for index in range(8):
        scheduled = start + index / rate
        if stop.wait(max(0, scheduled - time.monotonic())):
            break
        operation = 'table.get' if index % 2 == 0 else 'query.count'
        if operation == 'table.get':
            payload = {'table_id': table.id, 'key': rows[index]['id']}
            expected = rows[index]
        else:
            payload = {'table_id': table.id,
                       'query': {'select': None, 'filter': None, 'order_by': None, 'limit': None}}
            expected = len(rows)
        submitted = time.monotonic()
        identity = str(uuid.uuid4())
        envelope = {'request_id': identity, 'operation': operation, 'payload': payload}
        statuses = []
        for attempt in range(6):
            status, accepted = request(cluster.api_door.url, 'POST', '/v1/requests',
                                       cluster.auth['data_token'], envelope, timeout=12)
            statuses.append(status)
            if status != 503:
                break
            time.sleep(.025 * (attempt + 1))
        record = dict(request_id=identity, operation=operation, scheduled=scheduled, submitted=submitted,
                      admission_attempt_statuses=statuses, accepted=status == 202)
        if status == 202:
            record['acknowledged'] = time.monotonic()
            assert accepted['request_id'] == identity, accepted
            terminal = observed_terminal(cluster, accepted, expected, submitted, timeout=20)
            record.update({key: value for key, value in terminal.items() if key != 'receipt_samples'})
        else:
            assert status == 429 and accepted['error']['code'] == 'overloaded', (status, accepted)
        records.append(record)
    assert len({record['request_id'] for record in records}) == len(records)
    return records


def observed_resources(observer, start, end):
    os_samples, heartbeat_samples = defaultdict(list), defaultdict(list)
    for sample in observer.samples:
        if start <= sample['time'] <= end:
            os_samples[sample['node']].append(sample)
    for sample in observer.heartbeats:
        if start <= sample['time'] <= end:
            heartbeat_samples[sample['node']].append(sample)
    return {node: {
        'ps_samples': len(os_samples[node]),
        'heartbeat_samples': len(heartbeat_samples[node]),
        'peak_rss_bytes': max((sample['rss_bytes'] for sample in os_samples[node]), default=None),
        'peak_charged_working_bytes': max((sample['charged_bytes'] for sample in heartbeat_samples[node]), default=None),
        'peak_occupied_slots': max((sample['used_threads'] for sample in heartbeat_samples[node]), default=None),
        'mean_sampled_active_slot_fraction': (
            sum(sample['used_threads'] / sample['max_threads'] for sample in heartbeat_samples[node])
            / len(heartbeat_samples[node]) if heartbeat_samples[node] else None),
    } for node in observer.cluster.workers}


def transport_metrics(cluster, files, start, end, rows):
    partition_gets = [item for item in cluster.storage_traces() if start <= item['started'] <= end
                      and item['method'] == 'GET' and item['path'] in files and item['status'] == 200]
    partition_encoded_bytes = sum(files[item['path']]['byte_size'] for item in partition_gets)
    assert partition_gets and all(item['response_bytes'] >= files[item['path']]['byte_size']
                                  for item in partition_gets), partition_gets
    worker_batches = [item for item in cluster.traces() if start <= item['started'] <= end
                      and item.get('stream_operation') == 'batch']
    batch_attempts = Counter((item['stream_id'], item['stream_sequence']) for item in worker_batches)
    assert len({item['stream_id'] for item in worker_batches}) == RANKS, worker_batches
    return dict(partition_gets=len(partition_gets), partition_keys_read=len({item['path'] for item in partition_gets}),
                matched_partition_encoded_bytes=partition_encoded_bytes,
                matched_partition_encoded_bytes_per_row=partition_encoded_bytes / rows,
                partition_jitter_ms=dict(Counter(item.get('controlled_storage_delay_seconds', 0) * 1000
                                                for item in partition_gets)),
                distinct_worker_batch_requests=len(batch_attempts),
                repeated_worker_batch_requests=sum(attempts - 1 for attempts in batch_attempts.values()),
                worker_batch_http_status_counts=dict(Counter(item['status'] for item in worker_batches)),
                worker_batch_response_wire_bytes=sum(item['response_bytes'] for item in worker_batches),
                worker_batch_forward_to_first_byte_ms=millisecond_distribution([
                    item['response_started'] - item['forwarded'] for item in worker_batches
                    if item.get('response_started') is not None]),
                worker_batch_response_relay_ms=millisecond_distribution([
                    item['finished'] - item['response_started'] for item in worker_batches
                    if item.get('finished') is not None and item.get('response_started') is not None]))


def trial(cluster, observer, table, reference, query, snapshot, oracle, workload, depth, repetition, files):
    start, first_batch, stop = threading.Barrier(RANKS), threading.Event(), threading.Event()
    began = time.monotonic()
    cpu_before = cgroup().get('cpu.stat', {}).get('usage_usec')
    with ThreadPoolExecutor(max_workers=RANKS + int(bool(workload.ordinary_rate))) as clients:
        ordinary = (clients.submit(ordinary_work, cluster, table, reference, workload.ordinary_rate,
                                   first_batch, stop) if workload.ordinary_rate else None)
        pending = [clients.submit(read_rank, query, snapshot, rows, workload, rank, depth, start, first_batch)
                   for rank, rows in enumerate(oracle)]
        try:
            ranks = [job.result() for job in pending]
        finally:
            stop.set()
        ordinary_records = ordinary.result() if ordinary else []
    ended = time.monotonic()
    cpu_after = cgroup().get('cpu.stat', {}).get('usage_usec')
    rows = [row for rank in ranks for row in rank['rows']]
    assert rows == [row for shard in oracle for row in shard] and len({row['id'] for row in rows}) == len(rows)
    elapsed = max(rank['finished'] for rank in ranks) - min(rank['started'] for rank in ranks)
    last_rank_finished = max(rank['finished'] for rank in ranks)
    cpu_seconds = (cpu_after - cpu_before) / 1e6 if cpu_before is not None and cpu_after is not None else None
    return dict(depth=depth, repetition=repetition, warmup=repetition == 0, emitted_rows=len(rows),
                elapsed_seconds=elapsed, rows_per_second=len(rows) / elapsed,
                first_batch_seconds=[rank['opened'] - rank['started'] + rank['waits'][0] for rank in ranks],
                steady_next_seconds=[wait for rank in ranks for wait in rank['waits'][1:]],
                rank_finished_skew_seconds=last_rank_finished - min(rank['finished'] for rank in ranks),
                rank_rows=[len(rank['rows']) for rank in ranks],
                ordinary_accepted=sum(record['accepted'] for record in ordinary_records),
                ordinary_completed_while_streaming=sum(record.get('completed', float('inf')) < last_rank_finished
                                                      for record in ordinary_records),
                ordinary_records=ordinary_records,
                fixture_cpu_seconds=cpu_seconds,
                worker_resources=observed_resources(observer, began, ended),
                transport=transport_metrics(cluster, files, began, ended, len(rows)))


def depth_summary(trials):
    summaries = []
    for depth in range(3):
        samples = [item for item in trials if item['depth'] == depth and not item['warmup']]
        if not samples:
            continue
        ordinary = [record for item in samples for record in item['ordinary_records']]
        completed = [record for record in ordinary if record['accepted']]
        summaries.append(dict(depth=depth, repetitions=len(samples),
                              first_batch_ms=millisecond_distribution([metric for item in samples
                                                                      for metric in item['first_batch_seconds']]),
                              steady_next_ms=millisecond_distribution([metric for item in samples
                                                                      for metric in item['steady_next_seconds']]),
                              rows_per_second=distribution([item['rows_per_second'] for item in samples]),
                              ordinary_attempt_statuses=dict(Counter(status for record in ordinary
                                                                     for status in record['admission_attempt_statuses'])),
                              ordinary_submitted=len(ordinary), ordinary_rejected=len(ordinary) - len(completed),
                              ordinary_completed=len(completed), ordinary_missing=0, ordinary_duplicate_terminals=0,
                              ordinary_completed_while_streaming=sum(item['ordinary_completed_while_streaming'] for item in samples),
                              ordinary_completion_ms=millisecond_distribution([record['completed'] - record['submitted']
                                                                                for record in completed]),
                              ordinary_queue_wait_upper_ms=millisecond_distribution([record['queue_wait_upper_seconds']
                                                                                      for record in completed])))
    return summaries


def measure(cluster, observer, table, reference, workload, seed, result, save):
    db = Database(cluster.api_door.url, cluster.auth['data_token'], mode='olap', timeout=15)
    training, rows = seed_table(db, workload.name, workload.rows, workload.payload_width, seed)
    catalog_commit, files = cluster.manifest(workload.name)
    jitter = {path: workload.storage_jitter_ms[index % len(workload.storage_jitter_ms)] / 1000
              for index, path in enumerate(files)}
    cluster.store_door.partition_read_delay_seconds = lambda path: jitter.get(path, 0)
    query = training.where(selected=True) if workload.sparse else training.query()
    query = query.select('id', 'payload')
    project = lambda row: {'id': row['id'], 'payload': row['payload']}
    oracle = [[project(row) for row in rows[rank * len(rows) // RANKS:(rank + 1) * len(rows) // RANKS]
               if not workload.sparse or row['selected']] for rank in range(RANKS)]
    assert query.collect() == [row for shard in oracle for row in shard]
    observed = dict(workload=asdict(workload), catalog_commit=catalog_commit, partition_count=len(files),
                    partition_encoded_bytes=distribution([file['byte_size'] for file in files.values()]),
                    partition_decoded_payload_bytes=distribution([file['payload_bytes'] for file in files.values()]),
                    partition_rows=distribution([file['row_count'] for file in files.values()]), trials=[])
    result['workloads'].append(observed)
    with query.stream_snapshot(shards=RANKS) as snapshot:
        for repetition in range(WARMUP_AND_REPETITIONS):
            for offset in range(3):
                depth = (offset + repetition) % 3
                item = trial(cluster, observer, table, reference, query, snapshot.id, oracle,
                             workload, depth, repetition, files)
                observed['trials'].append(item)
                observed['summary_by_depth'] = depth_summary(observed['trials'])
                save()
                print(json.dumps(dict(workload=workload.name, depth=depth, repetition=repetition,
                                      emitted_rows=item['emitted_rows'], elapsed=item['elapsed_seconds'])), flush=True)
    assert cluster.manifest(workload.name)[0] == catalog_commit
    if workload.ordinary_rate:
        assert all(item['ordinary_completed_while_streaming'] > 0 for item in observed['summary_by_depth'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--commit', required=True, help='engine source commit for the benchmark binary')
    parser.add_argument('--fixture-commit', required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--seed', type=int, default=92106)
    parser.add_argument('--profile', choices=('all', 'dense_fast', 'sparse_wide'), default='all')
    args = parser.parse_args()
    binary = args.binary.resolve()
    with binary.open('rb') as stream:
        binary_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
    workloads = (
        Workload('dense_fast', 1024, (32, 80), 32),
        Workload('sparse_wide', 1536, (128, 480), 24, train_delay_ms=35,
                 storage_jitter_ms=(0, 5, 13, 25), sparse=True, ordinary_rate=8),
    )
    options = execution(cap=3, pending=64, queue_bytes=64 << 20)
    result = dict(contract_ids=['ST06', 'B01', 'B02'], complete=False, seed=args.seed,
                  binary_sha256=binary_hash, engine_commit=args.commit, fixture_commit=args.fixture_commit,
                  version=subprocess.check_output([binary, '--version'], text=True, timeout=10).strip(),
                  machine=machine(), execution=options, worker_threads=WORKER_THREADS, ranks=RANKS, workloads=[],
                  scope={
                      'ST06': 'Docker CPU simulation with JSON decoding and fixed per-batch training sleep',
                      'B01': 'sampled slot utilization and cgroup CPU time',
                      'B02': 'ordinary receipt conservation and latency alongside sparse stream work',
                      'network': 'loopback worker hop through a trace proxy, including request processing before first byte',
                      'storage': 'sum of manifest encoded lengths of matching successful Parquet object GETs',
                      'unobserved': ['GPU idle', 'GPU copy', 'multi-GPU jitter', 'exact cache-hit counter',
                                     'exact continuous utilization', 'physical object-store device bytes'],
                  })

    def save():
        result['cgroup'] = cgroup()
        temporary = args.evidence.with_suffix('.tmp')
        temporary.write_text(json.dumps(result, indent=2) + '\n')
        temporary.replace(args.evidence)

    with tempfile.TemporaryDirectory(prefix='nabedra-olap-benchmark-') as directory:
        started = time.monotonic()
        cluster = BenchmarkCluster(binary, directory, execution=options, receipt_poll_interval=.08)
        observer = None
        try:
            for index, threads in enumerate(WORKER_THREADS, 1):
                cluster.start_worker(index * 10, threads)
            observer = Observer(cluster)
            result['worker_resources'] = [node['resources'] for node in cluster.control()['nodes']]

            def capabilities():
                status, value = request(cluster.api_door.url, 'GET', '/v1/streams/capabilities',
                                        cluster.auth['data_token'], timeout=5)
                if status in (429, 503):
                    return None
                assert status == 200 and value['max_prefetch_batches'] == 2, (status, value)
                return value

            result['capabilities'] = wait_for(capabilities, 'stream benchmark requires depth two')
            db = Database(cluster.api_door.url, cluster.auth['data_token'], mode='olap', timeout=25)
            reference_table, reference_rows = seed_table(db, 'ordinary', 192, (24, 80), args.seed)
            for workload in workloads:
                if args.profile in ('all', workload.name):
                    measure(cluster, observer, reference_table, reference_rows, workload, args.seed, result, save)
            result['complete'] = True
        except BaseException:
            cluster.diagnostics.flush()
            sys.stderr.write((Path(directory) / 'diagnostics').read_text()[-16000:])
            raise
        finally:
            if observer:
                observer.close()
                result['observer_errors'] = observer.errors
            cluster.close()
            result['runtime_seconds'] = time.monotonic() - started
            save()


if __name__ == '__main__':
    main()

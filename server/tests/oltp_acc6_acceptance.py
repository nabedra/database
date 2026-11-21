#!/usr/bin/env python3.11
"""Production acceptance for O02–O05, O22 and O23 in both OLTP modes."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
import ctypes
import json
import math
import mmap
import operator
import os
from pathlib import Path
import random
import signal
import stat
import subprocess
import sys
import tempfile
import time

from oltp_compatibility import OlapServer, interrupt_fixture
from oltp_direct_acceptance import Database, admin_owner, col, dump
from oltp_direct_cluster import (DATABASE, Rf1Serve, Rf3Cluster, WORKERS, call, operation,
                                 Http, resources, uid, write_json)
from oltp_runtime_acceptance import (
    DIRECT, assertion, assert_accepted, assert_record, evidence, exact_receipt, gates,
    gateway, launch_retained, prepare_rolling_restart, read_record, receipt_path, release,
    run_native_assertions, same_run_ready, wait_reached, wait_until,
)
from oltp_transaction_acceptance import Transactions

ROOT = Path(__file__).resolve().parents[2]
MIB = 1 << 20
OPERATIONS = '/v1/operations'
REQUESTS = '/v1/requests'


def o02(binary, test_executable):
    build = os.environ.get('NABEDRA_O02_ENGINE_FEATURES')
    if build:
        result = json.loads(Path(build).read_text())
        assert result['commit'] == subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), result['commit']
        expected = {'default': 'olap', 'olap': 'olap', 'oltp': 'oltp', 'oltp-s3': 'oltp', 'cache': 'cache'}
        assert set(result['graphs']) == set(expected) | {'events'}, result
        assert {name: version.split()[-1] for name, version in result['versions'].items()} == expected, result
        assert {'no-engine', 'olap-oltp'} <= set(result['compile_guards']), result
        assertion('O02', 'engine-dependencies', sorted(result['graphs']))
        assertion('O02', 'invalid-feature-combinations', result['compile_guards'])
        assertion('O02', 'binary-engine', result['versions'])
    else:
        for clause in ('engine-dependencies', 'invalid-feature-combinations', 'binary-engine'):
            assertion('O02', clause, 'Missing same-commit engine matrix from scripts/check_engine_features.py.', 'not-run')
    with tempfile.TemporaryDirectory(prefix='nabedra-o02-') as directory:
        root = Path(directory)
        server = Rf1Serve(binary, root, direct=DIRECT)
        try:
            server.start()
            version = subprocess.check_output([binary, '--version'], timeout=10, text=True).strip()
            assert version.split()[-1] == 'oltp', version
            status, ready = call(server.url, '/health/ready', token=server.token)
            assert status == 200 and ready['engine'] == 'oltp', ready
            with closing(Database(url=server.url, token=server.token, database=DATABASE)) as db:
                table = db.create_table('o02_identity', {'id': int}, key='id')
                table.insert({'id': 1})
                assert table.get(1) == {'id': 1}
            assertion('O02', 'credential-free-startup', version)
        finally:
            server.close()
        manifest, = (root / 'data').rglob('NABEDRA_OLTP_POOL.json')
        original = json.loads(manifest.read_text())
        for field, foreign, diagnostic in (
                ('engine', 'olap', 'identity mismatch'),
                ('pool_format_version', 1, 'pool_format_version 1')):
            changed = json.dumps({**original, field: foreign})
            manifest.write_text(changed)
            refused = subprocess.run(
                [binary, 'serve', '--data-dir', str(root / 'data'), '--database', DATABASE,
                 '--host', '127.0.0.1', '--port', '0', '--cold-start'],
                env={**os.environ, 'NABEDRA_TOKEN': server.token}, capture_output=True, text=True, timeout=30)
            assert refused.returncode != 0 and diagnostic in refused.stderr, refused.stderr
            assert manifest.read_text() == changed, manifest
        manifest.write_text(json.dumps(original))
        try:
            server.start(cold_start=True)
            with closing(Database(url=server.url, token=server.token, database=DATABASE)) as db:
                assert db.table('o02_identity').get(1) == {'id': 1}
        finally:
            server.close()
    run_native_assertions(test_executable, [
        'o02_pool_refuses_foreign_engine_and_identity_and_reopens_its_own',
        'ns22_rocksdb_pools_and_legacy_stores_are_refused_untouched',
    ])
    olap_binary = os.environ.get('NABEDRA_OLAP_BINARY')
    if not olap_binary:
        assertion('O02', 'mixed-engine-and-disk-rejection',
                  'Manifest checks ran. Cross-engine worker startup needs the installed OLAP binary.', 'not-run')
        return
    with OlapServer(olap_binary) as olap, cluster_process(binary, test_executable, 'o02-enrollment') as cluster:
        olap_bootstrap, = [path for path in olap.root.glob('nabedra-*/*.json') if path.name != 'api.json']
        oltp_bootstrap = cluster.root / f'{WORKERS[0]}.json'
        for engine_binary, foreign_bootstrap, diagnostic in (
                (binary, olap_bootstrap, 'registered worker configuration invalid'),
                (olap_binary, oltp_bootstrap, 'worker runtime failed')):
            refused = subprocess.run([engine_binary, 'worker', '--bootstrap', str(foreign_bootstrap)],
                                     capture_output=True, text=True, timeout=15)
            assert refused.returncode != 0 and diagnostic in refused.stderr, refused.stderr
        same_run_ready(cluster, cluster.run_id, WORKERS, 3)
        with closing(Database(url=olap.url, token=olap.token)) as db:
            assert db.tables() == []
    assertion('O02', 'mixed-engine-and-disk-rejection',
              'Each engine rejected the other engine’s live worker bootstrap. OLTP rejected foreign and format-1 pool manifests and reopened the restored format-2 pool.')


@contextmanager
def transactions(binary, test_executable, scenario, *, queue_bytes=None):
    with tempfile.TemporaryDirectory(prefix=f'nabedra-{scenario.lower()}-') as root:
        cluster = Acc6Cluster(binary, root, test_executable, queue_bytes=queue_bytes)
        fixture = Transactions(cluster, DIRECT)
        try:
            fixture.start()
            yield fixture
        except BaseException:
            dump(cluster)
            raise
        finally:
            fixture.release_all()
            cluster.close()


def seed_and_source(fixture, table, rows):
    seed = operation('table.insert_many', {'table_id': table.id, 'rows': rows})
    fixture.select(seed['request_id'], {'participants': False})
    fixture.admit(seed)
    owner, source = fixture.reached(seed['request_id'], 'participants')
    assert len(source) == 1, source
    fixture.terminal(seed['request_id'], len(rows))
    return owner, source[0]['group']


def split_at_zero(fixture, source):
    split_id = uid()
    for _ in range(16):
        split = admin_owner(fixture.cluster, 'storage.shard.split',
                            {'source': source, 'boundary': 0, 'page_bytes': MIB}, split_id)
        if split['state'] == 'published':
            assert len(split['children']) == 2, split
            return split
        assert split['state'] == 'importing', split
    raise AssertionError(split)


def stage_delta(before, after, stage):
    return sum(stats['stage_timing'].get(stage, {}).get('count', 0)
               - before[name]['stage_timing'].get(stage, {}).get('count', 0)
               for name, stats in after.items())


def error(response, code):
    status, reply = response
    assert status >= 400 and reply['error']['code'] == code, response
    assert reply['error']['outcome'] == 'not_applied' and 'data' not in reply, response


def o03(binary, test_executable):
    with transactions(binary, test_executable, 'O03') as fixture:
        table = fixture.db.create_table('o03_ints', {'id': int, 'number': float}, key='id')
        keys = [-(2**63), -2, -1, 0, 2**63 - 1]
        rows = [{'id': key, 'number': -0.0 if key < 0 else 0.0} for key in keys]
        cluster = fixture.cluster
        _, source = seed_and_source(fixture, table, rows)
        # Hold a read pinned to the old epoch while the cached direct route
        # still points to the old range. The post-split key belongs to the right child.
        assert table.get(keys[-1]) == rows[-1]
        pending = fixture.query(table)
        fixture.select(pending['request_id'], {'snapshot': True})
        fixture.admit(pending)
        fixture.reached(pending['request_id'], 'snapshot')
        try:
            status, warm = fixture.api('api1', '/v1/runtime/stats')
            assert status == 200, warm
            split_at_zero(fixture, source)
            assert table.get(keys[-1]) == rows[-1]
            status, refreshed = fixture.api('api1', '/v1/runtime/stats')
            assert status == 200, refreshed
            if DIRECT:
                old, new = (stats['direct_gateway'] for stats in (warm, refreshed))
                redirected = (new['stale_routes'] > old['stale_routes']
                              and new['catalog_reads'] > old['catalog_reads'])
                assert redirected or new['fallbacks'] > old['fallbacks'], (old, new)
            assert table.update(0, {'number': 1.25})
        finally:
            fixture.release(pending['request_id'], 'snapshot')
        fixture.terminal(pending['request_id'], rows)
        assert table.get(0) == {'id': 0, 'number': 1.25}
        assert table.update(0, {'number': 0.0})
        error(fixture.api('api1', OPERATIONS, operation('table.get', {
            'table_id': uid(), 'key': 0})), 'not_found')
        with closing(database(cluster, 'api2')) as observer:
            current = observer.table('o03_ints')
            assert current.id == table.id
            before = cluster.all_stats()
            for key, expected in ((keys[0], rows[0]), (keys[-1], rows[-1]), (3, None)):
                assert current.get(key) == expected
            after = cluster.all_stats()
        point = 'leader.get.read' if DIRECT else 'test.data.point'
        assert stage_delta(before, after, point) == 3, (point, before, after)
        assert stage_delta(before, after, 'test.data.scan') == 0
        ranges = set()
        for key in (keys[0], keys[-1]):
            explanation = table.where(id=key).explain()
            assert 'access primary point seek on id' in explanation and 'scope shards 1; owning shard ' in explanation, explanation
            ranges.add(explanation.split('scope shards 1; owning shard ', 1)[1].splitlines()[0])
        assert len(ranges) == 2, ranges
        assert table.where(col('id') >= 0).collect() == rows[-2:]
        assertion('O03', 'owning-shard-point-lookup', {'point_reads': 3, 'scan_pages': 0, 'data_ranges': 2})
        copies = {name: after[name]['queue']['accepted_records'] - before[name]['queue']['accepted_records']
                  for name in after}
        assert copies == dict.fromkeys(WORKERS, 0 if DIRECT else 3), copies
        queue_bytes = {name: after[name]['queue']['charged_queue_bytes']
                       - before[name]['queue']['charged_queue_bytes'] for name in after}
        assertion('O03', 'admission-cost',
                  {'replicated_records': copies, 'retained_queue_bytes_delta': queue_bytes, 'application_reads': 3})
        assert table.query().collect() == rows
        for key, sign in ((-1, -1), (0, 1)):
            assert math.copysign(1, table.get(key)['number']) == sign
        assert table.where(number=-0.0).collect() == table.where(number=0.0).collect() == rows
        words = fixture.db.create_table('o03_strings', {'id': str}, key='id')
        strings = ['', '\x00', '\x00a', 'a', 'a\x00', 'é', '😀', '\U0010ffff']
        assert words.insert_many([{'id': key} for key in reversed(strings)]) == len(strings)
        assert [row['id'] for row in words.query().collect()] == strings
        assert all(words.get(key) == {'id': key} for key in strings)
        assertion('O03', 'typed-key-boundaries',
                  'Int64 extrema and negatives, UTF-8 order and embedded NUL, and signed-zero float values survived round trips.')
        assert words.delete('a\x00') and words.get('a\x00') is None
        assert words.where(id='a\x00').collect() == [] and not words.delete('a\x00')
        for bad_key in ('-1', 2**63, -(2**63)-1, -0.0, 0.0, True, float('inf'), float('nan')):
            error(fixture.api('api1', OPERATIONS, operation('table.get', {
                'table_id': table.id, 'key': bad_key})), 'validation_error')
        request = operation('table.get', {'table_id': table.id, 'key': 0})
        body = json.dumps(request, separators=(',', ':')).encode()
        assert body.endswith(b'"key":0}}')
        with closing(gateway(cluster)) as client:
            client.connection.request('POST', OPERATIONS, body[:-3] + b'1e400}}', client.headers)
            reply = client.connection.getresponse()
            error((reply.status, json.load(reply)), 'validation_error')
        assert table.get(-1) == rows[2]
        table.drop()
        replacement = fixture.db.create_table('o03_ints', {'id': int}, key='id')
        assert replacement.id != table.id
        error(fixture.api('api1', OPERATIONS, operation('table.get', {
            'table_id': table.id, 'key': -1})), 'not_found')
        assertion('O03', 'tombstones-and-invalid-keys',
                  'Tombstones were absent in get and query. Wrong types, out-of-range, float and nonfinite keys were refused.')
    run_native_assertions(test_executable, [
        'o07_o08_direct_rejects_a_stale_schema_epoch',
        'o07_direct_rejects_a_stale_routing_epoch',
    ])
    assertion('O03', 'stale-epochs',
              'A pinned read retained its complete old snapshot across split. New reads found the new owner and stale handles returned typed errors. Native tests refused stale schema and routing epochs.')


COMPARISONS = {'eq': operator.eq, 'ne': operator.ne, 'lt': operator.lt, 'le': operator.le,
               'gt': operator.gt, 'ge': operator.ge}


def evaluate(predicate, row):
    op = predicate['op']
    if op == 'and':
        return all(evaluate(arg, row) for arg in predicate['args'])
    if op == 'or':
        return any(evaluate(arg, row) for arg in predicate['args'])
    if op == 'not':
        return not evaluate(predicate['arg'], row)
    return COMPARISONS[op](row[predicate['column']], predicate['value'])


def oracle(rows, query):
    matching = sorted((row for row in rows if not query['filter'] or evaluate(query['filter'], row)),
                      key=lambda row: row['id'])
    if order := query['order_by']:
        matching.sort(key=lambda row: row[order['column']], reverse=order['desc'])
    matching = matching[:query['limit']]
    if query['select']:
        matching = [{column: row[column] for column in query['select']} for row in matching]
    return matching


def query_cases(fixture, table, rows, *, shards, indexed):
    category = col('category') == 1
    residual = (col('label') != 'x') & ~((col('value') < 2) | (col('label') == 'y'))
    cases = [
        table.query(),
        table.where(category & residual),
        table.where(category & residual).order_by('value', desc=True).select('id', 'label').limit(5),
        table.where((col('id') < -6) | ~(col('value') >= 2)).order_by('value', desc=True),
        table.where(category).order_by('value', desc=True).limit(1),
        table.where(category).limit(0),
        table.where(col('category') == 99),
    ]
    for query in cases:
        wire = query._to_wire()
        expected = oracle(rows, wire)
        assert query.collect() == expected, (wire, expected)
        assert query.count() == len(expected), wire
    plan = cases[1].explain()
    assert f'scope shards {shards};' in plan and 'residual exact predicate' in plan, plan
    assert ('secondary by_category seek' if indexed else 'primary scan on id') in plan, plan


def o04(binary, test_executable):
    with transactions(binary, test_executable, 'O04', queue_bytes=64 * MIB) as fixture:
        table = fixture.db.create_table('o04_rows', {'id': int, 'category': int, 'value': int, 'label': str}, key='id')
        rng = random.Random(406)
        rows = [{'id': key, 'category': key % 3, 'value': key % 4, 'label': rng.choice('xyzz')}
                for key in range(-48, 48)]
        rng.shuffle(rows)
        owner, source = seed_and_source(fixture, table, rows)
        query_cases(fixture, table, rows, shards=1, indexed=False)
        split_at_zero(fixture, source)
        query_cases(fixture, table, rows, shards=2, indexed=False)
        fixture.make_index(table, owner)
        query_cases(fixture, table, rows, shards=2, indexed=True)
        assert table.update(-2, {'category': 2, 'value': 9})
        assert table.update(0, {'category': 1, 'value': 9})
        assert table.delete(7)
        rows = [dict(row, category=2, value=9) if row['id'] == -2 else
                dict(row, category=1, value=9) if row['id'] == 0 else row
                for row in rows if row['id'] != 7]
        query_cases(fixture, table, rows, shards=2, indexed=True)
        for clause in ('predicate-oracle', 'residual-filters', 'ordering', 'projection-and-top-k',
                       'bounded-count-and-empty-results', 'index-duplicates-and-mutations'):
            assertion('O04', clause, 'Seed 406 matched the serial evaluator for the one-range scan, two-range scan, and two-range index before and after update/delete.')
        bounds = fixture.db.create_table('o04_bounds', {'id': int, 'value': str}, key='id')
        payload = 'x' * (72 << 10)
        for start in range(0, 128, 16):
            assert bounds.insert_many([{'id': key, 'value': payload} for key in range(start, start + 16)]) == 16
        error(fixture.api('api1', OPERATIONS, fixture.query(bounds)), 'limit_exceeded')
        assert bounds.query().select('id').collect() == [{'id': key} for key in range(128)]
        with closing(gateway(fixture.cluster)) as client:
            body = json.dumps(operation('table.insert', {'table_id': bounds.id, 'row': {
                'id': 128, 'value': 'x' * (8 * MIB)}})).encode()
            client.connection.request('POST', OPERATIONS, body, client.headers)
            reply = client.connection.getresponse()
            error((reply.status, json.load(reply)), 'limit_exceeded')
        assert bounds.get(128) is None and bounds.query().count() == 128
        assertion('O04', 'bounded-errors',
                  'Oversized request and encoded collect returned typed errors. Projection, count and an unrelated point lookup succeeded.')
    run_native_assertions(test_executable, [
        'o04_m01_scans_reuse_wide_key_scratch_and_copy_only_one_cursor_per_page',
    ])


class Acc6Cluster(Rf3Cluster):
    """Use the standard registered topology with optional unequal limits and copy probes."""

    def __init__(self, binary, root, test_executable, *, unequal=False, traced=(), mounts=False,
                 queue_bytes=None):
        super().__init__(binary, root, test_executable=test_executable,
                         direct=dict.fromkeys(WORKERS, DIRECT), api_direct=(DIRECT, DIRECT), mounts=mounts)
        self.unequal = unequal
        self.traced = traced
        self.queue_bytes = queue_bytes

    def launch_worker(self, name):
        if self.unequal and name not in self.workers:
            threads = (1, 2, 4)[WORKERS.index(name)]
            self.config[name]['resources'] = resources((2048 + 128 * threads) * MIB, threads)
            self.config[name]['execution']['max_workers_per_request'] = 2
        if self.queue_bytes is not None:
            self.config[name]['execution']['max_queue_bytes_per_replica'] = self.queue_bytes
        if self.traced:
            directory = self.barrier_dir(name)
            directory.mkdir(parents=True, exist_ok=True)
            write_json(directory / 'identity-load-ids.json', self.traced)
        super().launch_worker(name)


@contextmanager
def cluster_process(binary, test_executable, label, **options):
    with tempfile.TemporaryDirectory(prefix=f'nabedra-{label}-') as root:
        cluster = Acc6Cluster(binary, root, test_executable, **options)
        try:
            cluster.start()
            if DIRECT:
                cluster.wait_direct_active()
            yield cluster
        except BaseException:
            dump(cluster)
            cluster.kill_all()
            raise
        finally:
            cluster.close()


def ready_replica_view(cluster, names, *, newer_than=0):
    # The ready health endpoint reads Control. The local queue may still be
    # installing the published membership when that health check returns.
    def installed():
        views = [stats['accept_to_dispatch']['view'] for stats in cluster.all_stats(names).values()]
        first = views[0]
        assert first['run_id'] == cluster.run_id, first
        if first['membership_revision'] > newer_than and all(view == first for view in views):
            return first
        return None

    return wait_until(installed, f'queue replicas {names} did not install one newer ready view', timeout=20)


def database(cluster, api='api1'):
    return Database(url=cluster.api_urls[api], token=cluster.auth['data_token'],
                    database=DATABASE, ca_file=str(cluster.ca), timeout=30)


def physical_storage(directory, measure_cache):
    """Sample allocated file blocks and resident pool pages without faulting them in."""
    disk_bytes = 0
    cached_bytes = 0
    page = mmap.PAGESIZE
    if measure_cache:
        mincore = ctypes.CDLL(None, use_errno=True).mincore
        mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
        mincore.restype = ctypes.c_int
    for path in directory.rglob('*'):
        try:
            info = path.stat()
            if not stat.S_ISREG(info.st_mode):
                continue
            disk_bytes += info.st_blocks * 512
            if measure_cache and info.st_size:
                with path.open('rb') as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_COPY) as view:
                    size = len(view)
                    pages = (size + page - 1) // page
                    residency = (ctypes.c_ubyte * pages)()
                    anchor = ctypes.c_char.from_buffer(view)
                    address = ctypes.addressof(anchor)
                    del anchor
                    if mincore(address, size, residency):
                        raise OSError(ctypes.get_errno(), 'mincore of pool file')
                    cached_bytes += sum(value & 1 for value in residency) * page
        except FileNotFoundError:
            # A successful maintenance replacement may unlink a sampled run.
            continue
        except ValueError:
            # Between stat and mmap, a staging file can become empty.
            if path.exists() and path.stat().st_size:
                raise
    return disk_bytes, cached_bytes if measure_cache else None


def storage_observation(cluster, *, measure_cache=False):
    observations = {}
    for name, stats in cluster.all_stats().items():
        budget = stats['native_thread_budget']
        cache = stats['block_cache']
        limits = cluster.config[name]['resources']
        slots = stats['scheduler']
        assert sum(slot[1] for slot in slots) <= budget['query_threads'], slots
        assert sum(slot[2] for slot in slots) <= limits['memory_limit_bytes'], slots
        assert 0 <= cache['pinned_bytes'] <= cache['usage_bytes']
        assert cache['usage_bytes'] <= cache['configured_bytes'] + cache['pinned_bytes'], cache
        storage = cluster.root / name / 'data'
        allocated, cached = physical_storage(storage, measure_cache and sys.platform == 'linux')
        proc = Path(f'/proc/{cluster.workers[name].pid}')
        rss = None
        if (proc / 'status').is_file():
            rss = next(int(line.split()[1]) * 1024 for line in (proc / 'status').read_text().splitlines()
                       if line.startswith('VmHWM:'))
            names = [file.read_text().strip() for file in proc.glob('task/*/comm')]
            assert names.count('kv-flush') == budget['flush_threads'], names
            assert names.count('kv-compact') == budget['compaction_threads'], names
            assert names.count('kv-journal') == names.count('kv-journal-prep') == stats['storage']['pools'], names
            assert rss + (cached or 0) <= limits['memory_limit_bytes'], (rss, cached, limits)
        observations[name] = dict(
            cache=cache, native=budget, storage=stats['storage'], scheduler=slots, peak_rss_bytes=rss,
            allocated_disk_bytes=allocated, resident_pool_file_bytes=cached)
    return observations


def o05(binary, test_executable):
    run_native_assertions(test_executable, [
        'o05_kv_threads_are_per_worker_plus_two_journal_threads_per_open_pool',
        'ns10_o21_two_pools_beyond_memory_budget_stay_inside_their_charges',
        'o05_o28_typed_mtls_snapshot_installs_exact_group_cut_with_data_wire_full',
        'o04_m01_scans_reuse_wide_key_scratch_and_copy_only_one_cursor_per_page',
        'c07_m02_data_saturation_keeps_progress_buffers_and_decode_scratch_available',
        'o05_c07_committed_storage_apply_uses_private_capacity_with_full_transport_arenas',
        'c01_c07_f03_owned_executor_exceeds_callers_blocking_capacity_and_retains_cancelled_native_charges',
        'f03_cancelled_append_keeps_callback_order_and_drain_until_native_settles',
        'd10_c07_o05_exhausted_lane_overloads_while_raft_progress_advances',
    ])
    with transactions(binary, test_executable, 'O05') as fixture:
        table = fixture.db.create_table('o05_rows', {
            'id': int, 'category': int, 'value': int, 'blob': str}, key='id')
        rows = [dict(id=index, category=index % 4, value=index, blob='p' * (32 << 10))
                for index in range(96)]
        owner, _ = seed_and_source(fixture, table, rows[:32])
        baseline = storage_observation(fixture.cluster, measure_cache=True)
        for start in (32, 64):
            assert table.insert_many(rows[start:start + 32]) == 32
        scan = operation('query.collect', {
            'table_id': table.id, 'query': table.query().select('id')._to_wire()})
        fixture.select(scan['request_id'], {'snapshot': True})
        fixture.admit(scan)
        fixture.reached(scan['request_id'], 'snapshot')
        try:
            fixture.make_index(table, owner)
            concurrent = storage_observation(fixture.cluster, measure_cache=True)
        finally:
            fixture.release(scan['request_id'], 'snapshot')
        fixture.terminal(scan['request_id'], [{'id': row['id']} for row in rows])
        assert table.where(category=2).count() == 24
        evidence('O05', 'scan-pin-index-build', baseline=baseline, measured=concurrent)
    assertion('O05', 'allocation-failure-isolation',
              'Exhausted Data buffers rejected allocation while Progress stayed usable. Native apply, cancelled jobs, and Raft lane exhaustion preserved independent work.')
    if sys.platform == 'linux':
        assertion('O05', 'aggregate-native-budgets',
                  'Two-pool cache and write-buffer pressure plus physical thread assertions executed. RF3 samples included pinned cache, charged bytes, VmHWM, actual KV threads and mincore-resident pool file pages.')
    else:
        assertion('O05', 'aggregate-native-budgets',
                  'Native two-pool assertions executed. Physical thread attribution, VmHWM and resident pool file sampling require Linux.',
                  'not-run')
    assertion('O05', 'maintenance-bounds',
              'Snapshot transfer with a full Data lane, wide-key paging, and a pinned scan during index build executed. No fixture yet proves flush, compaction, scan, index build and snapshot transfer overlap at one bounded cut.',
              'not-run')


def malformed_requests(open_client, table):
    def limit_shape(depth):
        predicate = {'op': 'eq', 'column': 'id', 'value': 1}
        for _ in range(depth):
            predicate = {'op': 'not', 'arg': predicate}
        return predicate

    query = operation('query.collect', {'table_id': table.id, 'query': table.query()._to_wire()})
    valid = json.dumps(query).encode()
    cases = [
        (valid[:-1], 'validation_error', None),
        (valid.replace(b'"operation":', b'"operation": "table.list", "operation":', 1), 'validation_error', None),
        (json.dumps(operation('table.get', {'table_id': table.id, 'key': []})).encode(), 'validation_error', None),
        (json.dumps({**query, 'payload': {'table_id': table.id, 'query': {
            **query['payload']['query'], 'filter': limit_shape(64)}}}).encode(), 'validation_error', None),
        (json.dumps({**query, 'payload': {'table_id': table.id, 'query': {
            **query['payload']['query'], 'limit': -(2**65)}}}).encode(), 'validation_error', None),
        (valid, 'unauthorized', 'Bearer rejected-o22-token'),
    ]
    for payload, code, token in cases:
        with closing(open_client()) as client:
            headers = client.headers if token is None else {**client.headers, 'Authorization': token}
            client.connection.request('POST', REQUESTS, payload, headers)
            response = client.connection.getresponse()
            error((response.status, json.load(response)), code)


def huge_query_count(table):
    query = dict(table.query()._to_wire(), limit=2**65)
    return operation('query.count', {'table_id': table.id, 'query': query})


def rejected_registered_configs(cluster, binary):
    name = WORKERS[0]
    original = cluster.config[name]
    path = cluster.root / 'invalid-worker.json'
    destination = cluster.root / 'invalid-worker-storage'
    before = cluster.all_stats()
    for label, changed in (
            ('zero-threads', {'max_threads': 0}),
            ('overflowing-threads', {'max_threads': 2**65}),
            ('contradictory-memory', {'memory_limit_bytes': 128 * MIB})):
        write_json(path, {**original,
                          'node': {**original['node'], 'directory': str(destination), 'mount': None},
                          'resources': {**original['resources'], **changed}})
        refused = subprocess.run([binary, 'worker', '--bootstrap', str(path)], capture_output=True,
                                 text=True, timeout=15)
        assert refused.returncode != 0 and 'registered' in refused.stderr, (label, refused.stderr)
        assert not destination.exists(), label
    after = cluster.all_stats()
    assert {name: value['queue'] for name, value in before.items()} == {
        name: value['queue'] for name, value in after.items()}
    assert {name: value['native_thread_budget'] for name, value in before.items()} == {
        name: value['native_thread_budget'] for name, value in after.items()}
    same_run_ready(cluster, cluster.run_id, WORKERS, 3)


def o22(binary, test_executable):
    run_native_assertions(test_executable, [
        'c01_c02_heterogeneous_limits_and_impossible_work',
        'c03_drain_preserves_owned_work_and_reconfigure_reuses_node',
        'c04_caps_include_reducers_across_hundred_jobs',
        'c05_equal_peers_progress_with_one_slot_per_fair_round',
        'o22_c04_c06_delayed_page_refills_under_caps_one_two_and_three',
        'c06_remote_io_balances_by_compute_capacity_without_reserving_cpu',
        'c07_f03_memory_only_io_and_cancelled_grants_settle_once',
        'f03_query_reply_capacity_wait_has_no_cpu_and_does_not_block_control_progress',
        'o22_c07_dependent_pool_writers_wait_without_compute',
        'c07_m02_o05_o06_o11_o22_mtls_data_and_catalog_raft_progress_during_bulk_pressure',
        'c06_f01_f02_f03_actual_overlap_refill_errors_panic_and_receiver_drop',
        'o22_f01_f03_running_tickets_survive_timeout_and_unused_tickets_are_fenced',
        'u02_invalid_auth_or_pin_installs_no_identity_and_wire_keeps_exact_raw_bytes',
        'u03_seed_343687_membership_memory_drain_and_retry_conservation',
    ])
    mounts = Rf3Cluster.mounts_available()
    with cluster_process(binary, test_executable, 'o22', unequal=True, mounts=mounts) as cluster, closing(database(cluster)) as db:
        table = db.create_table('o22_rows', {'id': int}, key='id')
        rows = [dict(id=index) for index in range(-192, 192)]
        fixture = Transactions(cluster, DIRECT)
        _, source = seed_and_source(fixture, table, rows)
        split = split_at_zero(fixture, source)
        shard_ids = [child['group_id'] for child in split['children']]
        assert len(set(shard_ids)) == 2, split
        assert 'scope shards 2;' in table.query().explain()
        limits = [cluster.stats(name)['native_thread_budget']['query_threads'] for name in WORKERS]
        assert limits == [1, 2, 4], limits
        rejected_registered_configs(cluster, binary)
        held = [operation('query.collect', {'table_id': table.id, 'query': table.query()._to_wire()})
                for _ in range(2)]
        directories = [cluster.barrier_dir(name) for name in WORKERS]
        for directory in directories:
            (directory / 'read-scheduler-fixture').touch()
            write_json(directory / 'inspect.json', [req['request_id'] for req in held])
            for req in held:
                for shard_id in shard_ids:
                    (directory / f"read-{req['request_id']}-{shard_id}-hold-running").touch()

        def running(req):
            return [path for directory in directories
                    for path in directory.glob(f"read-{req['request_id']}-*-running")
                    if not path.name.endswith(('hold-running', 'release-running'))]

        with closing(gateway(cluster)) as first, closing(gateway(cluster, 'api2')) as second:
            try:
                for req in held:
                    assert_accepted(first.exchange(REQUESTS, req), cluster.run_id, req)
                    wait_until(lambda: running(req), f"{req['request_id']} did not launch a Data page", timeout=20)
                # A cap of two keeps one request slot for coordinator/reduction.
                for req in held:
                    pages = running(req)
                    assert len(pages) == 1, pages
                    marker = json.loads(pages[0].read_text())
                    assert marker['scope']['request']['request_id'] == req['request_id'], marker
                    status, receipt = second.exchange(receipt_path(cluster.run_id, req))
                    assert status == 200 and receipt['state'] == 'running', receipt
                wait_until(lambda: all(not slot[1] for stats in cluster.all_stats().values()
                                       for slot in stats['scheduler']),
                           'a blocked remote page retained a compute permit', timeout=5)
                before_errors = cluster.all_stats()
                malformed_requests(lambda: gateway(cluster, 'api2'), table)
                assert {name: stats['queue'] for name, stats in cluster.all_stats().items()} == {
                    name: stats['queue'] for name, stats in before_errors.items()}
                assert table.where(id=5).collect() == [{'id': 5}]
                count = huge_query_count(table)
                assert_accepted(second.exchange(REQUESTS, count), cluster.run_id, count)
                exact_receipt(second.exchange, cluster.run_id, count, len(rows))
                observed = storage_observation(cluster)
            finally:
                for directory in directories:
                    for req in held:
                        for shard_id in shard_ids:
                            (directory / f"read-{req['request_id']}-{shard_id}-release-running").touch()
            for req in held:
                exact_receipt(second.exchange, cluster.run_id, req, rows)
                for name in WORKERS:
                    assert_record(read_record(cluster, name, req), cluster.run_id, req)
                assert all(any((directory / f"read-{req['request_id']}-{shard_id}-reply-ready").exists()
                               for directory in directories) for shard_id in shard_ids)
        final = cluster.all_stats()
        assert all(not stats['queue']['queued_records'] and not stats['queue']['running_records']
                   and not stats['queue']['reconciling_records'] for stats in final.values())
        evidence('O22', 'unequal-registered-workers', limits={
            name: cluster.config[name]['resources'] for name in WORKERS}, resources=observed)
        assertion('O22', 'protected-progress',
                  'Authenticated receipts and a small query progressed while two broad scans held remote Data tickets. Native mTLS assertions exercised reserved Raft progress with occupied Data lanes and query executors.')
        assertion('O22', 'dependent-waits-release-compute',
                  'Both live remote page waits and two native writes blocked on asynchronous ordering reached zero compute use. Exact reads, ordered writes, cancellation and settlement executed.')
        assertion('O22', 'caps-fairness-and-conservation',
                  'Two broad scans used one in-flight Data page per request under a cap of two, visited both shards and returned exact rows. The 100-partition scheduler assertions included reductions, caps 1/2/3, deterministic fairness, refill and conservation.')
        if mounts:
            owner_id = final[WORKERS[0]]['accept_to_dispatch']['owner']['worker_id']
            owner = next(name for name in WORKERS if cluster.enrollments[name]['worker_id'] == owner_id)
            target = next(name for name in WORKERS if name != owner)
            survivors = tuple(name for name in WORKERS if name != target)
            route_before = [table.where(id=key).explain().split('owning shard ', 1)[1].splitlines()[0]
                            for key in (-1, 1)]
            original_view = ready_replica_view(cluster, WORKERS)
            cluster.workers[target].kill()
            same_run_ready(cluster, cluster.run_id, survivors, 2)
            removed_view = ready_replica_view(cluster, survivors, newer_than=original_view['membership_revision'])
            prepare_rolling_restart(cluster, target, owner)
            cluster.config[target]['resources']['max_threads'] = 3
            launch_retained(cluster, target)
            same_run_ready(cluster, cluster.run_id, WORKERS, 3)
            ready_replica_view(cluster, WORKERS, newer_than=removed_view['membership_revision'])
            assert cluster.stats(target)['native_thread_budget']['query_threads'] == 3
            current = db.table('o22_rows')
            assert current.id == table.id and current.query().collect() == rows
            route_after = [current.where(id=key).explain().split('owning shard ', 1)[1].splitlines()[0]
                           for key in (-1, 1)]
            assert route_after == route_before, (route_before, route_after)
            evidence('O22', 'C03-same-store-reconfiguration', worker=target, old_threads=limits[WORKERS.index(target)],
                     new_threads=3, table=current.id, shards=route_after, rows=len(rows))


    with tempfile.TemporaryDirectory(prefix='nabedra-o22-u01-') as root:
        local = Rf1Serve(binary, root, direct=DIRECT)
        try:
            local.start()
            with closing(Database(url=local.url, token=local.token, database=DATABASE)) as db:
                table = db.create_table('o22_rows', {'id': int}, key='id')
                assert table.insert_many(rows) == len(rows)
                baseline = local.stats()['queue']
                malformed_requests(lambda: Http(local.url, local.token), table)
                assert local.stats()['queue'] == baseline
                assert table.where(id=5).collect() == [{'id': 5}]
                assert table.query().collect() == rows
                count = huge_query_count(table)
                with closing(Http(local.url, local.token)) as client:
                    status, ready = client.exchange('/health/ready')
                    assert status == 200, ready
                    assert_accepted(client.exchange(REQUESTS, count), ready['run_id'], count)
                    exact_receipt(client.exchange, ready['run_id'], count, len(rows))
        finally:
            local.close()
    assertion('O22', 'scheduler-contract',
              'Native and process C01–C07/F01–F03/U01–U03 assertions ran in part. Missing matching local/distributed saturation histories and forged capacity or stale incarnation during live C02 admission.'
              + ('' if mounts else ' C03 retained reconfiguration requires Linux loopback pool mounts with passwordless sudo.'),
              'not-run')


def copy_metrics(cluster, request, ready_count):
    # A replica records the exact serialized queue frame and the elapsed time
    # of each authenticated installation acknowledgement at the coordinator.
    traces = []
    for name in WORKERS:
        trace = cluster.barrier_dir(name) / f"identity-copy-{request['request_id']}.jsonl"
        if trace.is_file():
            traces.extend(json.loads(line) for line in trace.read_text().splitlines())
    staged = [row for row in traces if not row['accepted'] and row['target'] and row['target'] != row['owner']]
    assert staged and len({row['ready']['view']['run_id'] for row in staged}) == 1, traces
    assert all(row['elapsed_ns'] > 0 and sum(row['section_bytes']) > 0
               and row['ready']['ready_count'] == ready_count for row in staged), staged
    assert len({row['target']['worker_id'] for row in staged}) == ready_count - 1, staged
    return dict(
        fanout_record_bytes=sum(row['record_header_bytes'] + sum(row['section_bytes']) for row in staged),
        slowest_replica_install_ns=max(row['elapsed_ns'] for row in staged),
        remote_installations=len(staged),
    )


def quantiles(values):
    ordered = sorted(values)
    return {f'p{p}': ordered[(len(ordered) - 1) * p // 100] for p in (50, 95, 99)}


def queue_workload(cluster, ready, source, target, identities):
    """Compare finite read-heavy and write-heavy bursts at one ready count."""
    sample = iter(identities)
    phases = []
    write_keys = set()
    for writes_per_batch in (1, 3):
        for repetition in range(4):
            jobs = []
            for offset in range(4):
                identity = next(sample)
                if offset < writes_per_batch:
                    rows = [dict(id=f'{identity}_{index}', payload='b' * 8192) for index in range(8)]
                    request = operation('table.insert_many', {'table_id': target.id, 'rows': rows}, identity)
                    expected = len(rows)
                else:
                    request = operation('query.count', {'table_id': source.id, 'query': source.query()._to_wire()}, identity)
                    expected = 128
                jobs.append((request, expected))

            def complete(job):
                request, expected = job
                start = time.monotonic_ns()
                retries = 0
                with closing(gateway(cluster, 'api2', timeout=20)) as client:
                    while True:
                        reply = client.exchange(REQUESTS, request)
                        if reply[0] == 202:
                            assert_accepted(reply, cluster.run_id, request)
                            break
                        status, body = reply
                        assert body['request_id'] == request['request_id'], reply
                        assert status == 503 and body['error']['code'] == 'storage_unavailable', reply
                        assert body['error']['outcome'] in ('unknown', 'not_applied'), reply
                        # An uncertain admission can retain the original UUID.
                        # Retry the exact bytes across a ready-set transition.
                        retries += 1
                        assert retries <= 12 and time.monotonic_ns() - start < 20_000_000_000, reply
                        time.sleep(min(.25 + .05 * retries, 1))
                    accepted = time.monotonic_ns()
                    exact_receipt(client.exchange, cluster.run_id, request, expected)
                return dict(request_id=request['request_id'], admission_retries=retries,
                            acceptance_ns=accepted-start, terminal_ns=time.monotonic_ns()-start,
                            **copy_metrics(cluster, request, len(ready)))

            peak = dict.fromkeys(ready, 0)
            queue_before = {name: stats['accept_to_dispatch'] for name, stats in cluster.all_stats(ready).items()}
            begin = time.monotonic_ns()
            with ThreadPoolExecutor(max_workers=2) as workers:
                futures = [workers.submit(complete, job) for job in jobs]
                while not all(future.done() for future in futures):
                    for name, stats in cluster.all_stats(ready).items():
                        active = sum(slot[1] for slot in stats['scheduler'])
                        assert active <= stats['native_thread_budget']['query_threads'], stats['scheduler']
                        peak[name] = max(peak[name], active)
                    time.sleep(.01)
                measurements = [future.result() for future in futures]
            duration = (time.monotonic_ns()-begin) / 1e9
            queue_wait = {}
            for name, stats in cluster.all_stats(ready).items():
                after = stats['accept_to_dispatch']
                before = queue_before[name]
                assert after['view'] == before['view'], (before, after)
                buckets = [current - prior for prior, current in zip(before['bucket_counts'], after['bucket_counts'])]
                assert all(count >= 0 for count in buckets), buckets
                queue_wait[name] = {'upper_us': after['upper_us'], 'counts': buckets}
            assert sum(sum(row['counts']) for row in queue_wait.values()) == len(jobs), queue_wait
            assert len({row['request_id'] for row in measurements}) == len(jobs)
            written = {row['id'] for req, _ in jobs if req['operation'] == 'table.insert_many'
                       for row in req['payload']['rows']}
            assert not write_keys.intersection(written)
            write_keys.update(written)
            if repetition:
                phases.append(dict(
                    writes_per_batch=writes_per_batch, repetition=repetition, peak_slots=peak,
                    accept_to_dispatch=queue_wait, terminal_rate=len(jobs) / duration, recorded_write_keys=len(written),
                    acceptance_ns=quantiles([row['acceptance_ns'] for row in measurements]),
                    terminal_ns=quantiles([row['terminal_ns'] for row in measurements]), samples=measurements))
    assert next(sample, None) is None
    return phases, write_keys


def o23(binary, test_executable):
    run_native_assertions(test_executable, [
        'r03_q03_join_empty_single_multiple_and_exhausted_pages_have_correct_done_flag',
        'c07_m02_small_encoded_control_values_charge_actual_capacity_and_clones_retain_it',
        'c07_m02_f03_o22_o23_read_grant_copy_retries_only_the_blocked_replica',
        'c07_f03_o23_read_grant_copy_keeps_transport_outcomes_distinct',
        'q06_o14_o23_terminal_confirmation_requires_the_exact_ready_view_and_version',
    ])
    traced = [uid() for _ in range(66)]
    mounts = Rf3Cluster.mounts_available()
    with cluster_process(binary, test_executable, 'o23', mounts=mounts, traced=traced) as cluster, closing(database(cluster)) as db:
        table = db.create_table('o23_data', {'id': int, 'payload': str}, key='id')
        source_rows = [dict(id=i, payload='x' * 8192) for i in range(128)]
        assert table.insert_many(source_rows) == len(source_rows)
        out = db.create_table('o23_output', {'id': int, 'payload': str}, key='id')
        workload_out = db.create_table('o23_workload_output', {'id': str, 'payload': str}, key='id')
        three, write_keys = queue_workload(cluster, WORKERS, table, workload_out, traced[2:34])
        two = None
        read = operation('query.collect', {'table_id': table.id, 'query': table.query().select('id')._to_wire()}, traced[0])
        write_rows = [dict(id=i, payload='y' * 8192) for i in range(16)]
        write = operation('table.insert_many', {'table_id': out.id, 'rows': write_rows}, traced[1])
        run = cluster.run_id
        owner, = {name for name, stats in cluster.all_stats().items()
                  if stats['accept_to_dispatch']['owner']['worker_id'] == cluster.enrollments[name]['worker_id']}
        target, other = [name for name in WORKERS if name != owner]
        directories = {name: cluster.barrier_dir(name) for name in WORKERS}
        for name, directory in directories.items():
            gates(directory, [(read, 'before-staged-copy' if name == target else 'queued'), (write, 'committed')])
        with ThreadPoolExecutor(max_workers=2) as clients, closing(gateway(cluster, 'api2')) as observer:
            def admit(request):
                with closing(gateway(cluster, timeout=60)) as client:
                    return client.exchange(REQUESTS, request)
            first = clients.submit(admit, read)
            try:
                wait_reached([directories[target]], read, 'before-staged-copy')
                def installed_elsewhere():
                    try:
                        return read_record(cluster, other, read)
                    except FileNotFoundError:
                        return None

                staged = wait_until(installed_elsewhere, 'healthy peer did not finish the staged install', timeout=8)
                assert not staged['accepted'] and not first.done()
                local = read_record(cluster, owner, read)
                assert staged['body_hash'] == local['body_hash'] and not local['accepted'], staged
            finally:
                release(directories[target], read)
            assert_accepted(first.result(), run, read)
            wait_reached([directories[owner]], read, 'queued')
            working = clients.submit(admit, write)
            assert_accepted(working.result(), run, write)
            wait_reached(directories.values(), write, 'committed')
            identities = {}
            for request in (read, write):
                copies = [assert_record(read_record(cluster, name, request), run, request) for name in WORKERS]
                assert all(copy == copies[0] for copy in copies)
                identities[request['request_id']] = copies[0]
            metrics = [copy_metrics(cluster, request, 3) for request in (read, write)]
            assert all(sample['remote_installations'] == 2 for sample in metrics), metrics
            assert_accepted(observer.exchange(REQUESTS, write), run, write)
            changed = {**write, 'payload': {**write['payload'], 'rows': [{'id': -1, 'payload': 'wrong'}]}}
            error(observer.exchange(REQUESTS, changed), 'conflict')
            assertion('O23', 'required-queue-and-receipts',
                      'Measured explicit /v1/requests reads and writes returned exact terminal receipts. A duplicate kept its original UUID and a conflicting body was refused.')
            original_view = ready_replica_view(cluster, WORKERS)
            cluster.kill_api('api1')
            for request in (read, write):
                status, pending = observer.exchange(receipt_path(run, request))
                assert status == 200 and pending['state'] in ('queued', 'running', 'committing'), pending
            try:
                if mounts:
                    cluster.workers[target].kill()
                    same_run_ready(cluster, run, (owner, other), 2)
                    removed_view = ready_replica_view(cluster, (owner, other), newer_than=original_view['membership_revision'])
                    two, extra_keys = queue_workload(cluster, (owner, other), table, workload_out, traced[34:])
                    assert not write_keys.intersection(extra_keys)
                    write_keys.update(extra_keys)
                    for name in (owner, other):
                        for request in (read, write):
                            assert assert_record(read_record(cluster, name, request), run, request) == identities[request['request_id']]
                    prepare_rolling_restart(cluster, target, owner)
                    launch_retained(cluster, target)
                    same_run_ready(cluster, run, WORKERS, 3)
                    ready_replica_view(cluster, WORKERS, newer_than=removed_view['membership_revision'])
                    for request in (read, write):
                        assert assert_record(read_record(cluster, target, request), run, request) == identities[request['request_id']]
            finally:
                for directory in directories.values():
                    for request in (read, write):
                        release(directory, request)
            exact_receipt(observer.exchange, run, read, [dict(id=row['id']) for row in source_rows])
            exact_receipt(observer.exchange, run, write, len(write_rows))
        cluster.connection = cluster.admin('cluster.connection', {})
        cluster.start_api('api1', DIRECT)
        with closing(database(cluster)) as recovered:
            settled = recovered.table('o23_output')
            assert settled.id == out.id and settled.query().collect() == write_rows
            updated = recovered.table('o23_workload_output').query().select('id').collect()
        assert updated == [{'id': key} for key in sorted(write_keys)], updated
        evidence('O23', 'mixed-load', ready_three=three, ready_two=two,
                 cpu_max=(Path('/sys/fs/cgroup/cpu.max').read_text().strip()
                          if Path('/sys/fs/cgroup/cpu.max').is_file() else None),
                 worker_limits={name: cluster.config[name]['resources'] for name in WORKERS})
        evidence('O23', 'held-admission', read=metrics[0], write=metrics[1], same_run=run,
                 ready_sizes=[3, 2, 3] if mounts else [3])
        if mounts:
            assertion('O23', 'all-ready-and-retention',
                      'An unacknowledged installation blocked 202. Both exact requests survived a 3 to 2 to 3 ready transition with the original pinned context and exact terminal values.')
        else:
            assertion('O23', 'all-ready-and-retention',
                      'Staged copy cut passed. Same-run retained replacement requires Linux loopback pool mounts with passwordless sudo.',
                      'not-run')
    if mounts:
        assertion('O23', 'queue-cost-and-throughput',
                  'One warm-up and three measured read-heavy and write-heavy bursts per ready size recorded exact accepted identities, frame bytes, slowest installation, slot occupancy and terminal throughput.')
    else:
        assertion('O23', 'queue-cost-and-throughput',
                  'Three-worker measurements ran. Two-worker measurements with retained requests require Linux loopback pool mounts with passwordless sudo.',
                  'not-run')


def main():
    signal.signal(signal.SIGTERM, interrupt_fixture)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scenario', choices=('O02', 'O03', 'O04', 'O05', 'O22', 'O23'))
    parser.add_argument('binary')
    parser.add_argument('test_executable')
    args = parser.parse_args()
    globals()[args.scenario.lower()](args.binary, args.test_executable)


if __name__ == '__main__':
    main()

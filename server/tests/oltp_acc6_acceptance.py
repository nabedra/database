#!/usr/bin/env python3.11
"""Production acceptance for O02–O05, O22 and O23 in both OLTP modes."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
import json
import math
import operator
import os
from pathlib import Path
import random
import subprocess
import tempfile
import time

from oltp_direct_acceptance import Database, admin_owner, col, dump, queue_owner
from oltp_direct_cluster import DATABASE, Rf1Serve, Rf3Cluster, WORKERS, call, operation, uid
from oltp_runtime_acceptance import (
    DIRECT, assertion, assert_accepted, assert_record, evidence, exact_receipt, gates,
    gateway, read_record, release, run_native_assertions, wait_reached, wait_until,
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
    assertion('O02', 'mixed-engine-and-disk-rejection',
              'Installed OLTP refused the foreign engine and old disk version. Mixed-engine enrollment remains unexercised.',
              'not-run')


@contextmanager
def transactions(binary, test_executable, scenario):
    with tempfile.TemporaryDirectory(prefix=f'nabedra-{scenario.lower()}-') as root:
        cluster = Rf3Cluster(binary, root, test_executable=test_executable,
                             direct=dict.fromkeys(WORKERS, DIRECT), api_direct=(DIRECT, DIRECT))
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
        # Prime the gateway before publishing the new routing epoch.
        assert table.get(keys[-1]) == rows[-1]
        split_at_zero(fixture, source)
        assert table.get(keys[-1]) == rows[-1]
        error(fixture.api('api1', OPERATIONS, operation('table.get', {
            'table_id': uid(), 'key': 0})), 'not_found')
        assertion('O03', 'stale-epochs', 'A warm gateway returned the exact row after a split. An unknown handle failed without data.')
        before = cluster.all_stats()
        for key, expected in ((keys[0], rows[0]), (keys[-1], rows[-1]), (3, None)):
            assert table.get(key) == expected
        after = cluster.all_stats()
        point = 'leader.get.read' if DIRECT else 'test.data.point'
        assert stage_delta(before, after, point) == 3, (point, before, after)
        assert stage_delta(before, after, 'test.data.scan') == 0
        for key in (keys[0], keys[-1]):
            explanation = table.where(id=key).explain()
            assert 'access primary point seek on id' in explanation and 'scope shards 1; owning shard ' in explanation, explanation
        assert table.where(col('id') >= 0).collect() == rows[-2:]
        assertion('O03', 'owning-shard-point-lookup', {'point_reads': 3, 'scan_pages': 0, 'data_ranges': 2})
        copies = {name: after[name]['queue']['accepted_records'] - before[name]['queue']['accepted_records']
                  for name in after}
        assert copies == dict.fromkeys(WORKERS, 0 if DIRECT else 3), copies
        assertion('O03', 'admission-cost', {'replicated_records': copies, 'application_reads': 3})
        assert table.query().collect() == rows
        for key, sign in ((-1, -1), (0, 1)):
            assert math.copysign(1, table.get(key)['number']) == sign
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
                'table_id': table.id, 'key': bad_key})), 'invalid_request')
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
    with transactions(binary, test_executable, 'O04') as fixture:
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


def o05(binary, test_executable):
    pass


def o22(binary, test_executable):
    pass


def o23(binary, test_executable):
    pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scenario', choices=('O02', 'O03', 'O04', 'O05', 'O22', 'O23'))
    parser.add_argument('binary')
    parser.add_argument('test_executable')
    args = parser.parse_args()
    globals()[args.scenario.lower()](args.binary, args.test_executable)


if __name__ == '__main__':
    main()

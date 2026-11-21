#!/usr/bin/env python3.11
"""Registered RF3 shard, storage growth and metadata acceptance."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time

from oltp_direct_acceptance import admin_owner, col, dump
from oltp_direct_cluster import MIB, WORKERS, Rf3Cluster, call, operation, uid, write_json
from oltp_recovery_operations import (
    TRANSPORT_ERRORS, initial_range, retained_config, success, two_ready, wait_for,
)
from oltp_transaction_acceptance import OPERATIONS, Transactions, batch, passed

ADMIN = '/v1/cluster/operations'


def admin(cluster, worker, request, timeout=60):
    return call(cluster.urls[worker], ADMIN, request, token=cluster.auth['admin_token'],
                timeout=timeout)


def pending_clause(scenario, clause, reason):
    print('NABEDRA_OLTP_ASSERT ' + json.dumps({
        'scenario': scenario, 'assertion': clause, 'status': 'not-run', 'detail': reason,
    }), flush=True)


def split(cluster, source, boundary, split_id=None):
    split_id = split_id or uid()
    for _ in range(32):
        result = admin_owner(cluster, 'storage.shard.split',
                             dict(source=source, boundary=boundary, page_bytes=MIB), split_id)
        if result['state'] == 'published':
            children = result['children']
            assert len(children) == 2 and children[0] != children[1], result
            assert all(child['kind']['data']['table_id'] == source['kind']['data']['table_id']
                       for child in children), result
            return result
        assert result['state'] == 'importing', result
    raise AssertionError(f'split {split_id} never published: {result}')


def row(key, category=7, value=None):
    return dict(id=key, category=category, value=key * 2 if value is None else value)


def get(facade, table, key, api='api1'):
    return facade.data(operation('table.get', dict(table_id=table.id, key=key)), api)


def query(facade, table, kind='collect', expression=None, api='api1'):
    query = table.query() if expression is None else expression
    return facade.data(operation(f'query.{kind}', dict(table_id=table.id, query=query._to_wire())), api)


def create(facade, name):
    return facade.db.create_table(name, {'id': int, 'category': int, 'value': int}, key='id')


def placement(cluster, group):
    return cluster.admin('storage.replica.placement', {'group': group})


def worker_identity(cluster, worker):
    return cluster.config[worker]['node']['mount']['worker']


def stop_worker(cluster, worker):
    cluster.workers[worker].kill()
    cluster.workers[worker].stop()
    survivors = [name for name in WORKERS if name != worker]
    wait_for(lambda: two_ready(cluster, survivors), 'lost incarnation removed from the ready set', 90)
    return survivors


def launch_retained(cluster, worker, config):
    cluster.config[worker] = config
    cluster.enrollments[worker] = config['node']['enrollment']
    cluster.launch_worker(worker)
    ready = cluster.wait_workers()
    assert all(view['run_id'] == cluster.run_id for view in ready.values()), ready


def o16(facade):
    cluster = facade.cluster
    table = create(facade, 'split_history')
    independent = create(facade, 'shared_pool_neighbor')
    table.insert_many([row(-9), row(-1), row(0), row(8)])
    independent.insert(row(9))
    facade.make_index(table, None)
    source = initial_range(cluster, table.id)
    original = operation('table.update', dict(table_id=table.id, key=-9, values=dict(value=90)))
    exact = facade.api('api1', OPERATIONS, original)
    assert exact[0] == 200, exact

    before = [row(-9, value=90), row(-1), row(0), row(8)]
    snapshots = [facade.query(table), facade.query(table, 7)]
    for body in snapshots:
        facade.select(body['request_id'], {'snapshot': True})
        facade.watch_record(body['request_id'])
        facade.admit(body)
    observations = [facade.reached(body['request_id'], 'snapshot') for body in snapshots]
    owner = observations[0][0]
    assert all(worker == owner for worker, _ in observations), observations

    # These rows must be visible on current routes and absent from both old pins.
    table.insert(row(-2))
    parts = split(cluster, source, 0)['children']
    late = batch(table, [row(-3), row(3)])
    facade.admit(late)
    facade.terminal(late['request_id'], 2)
    assert facade.api('api1', OPERATIONS, original) == exact
    changed = deepcopy(original)
    changed['payload']['values']['value'] = 91
    status, reply = facade.api('api2', OPERATIONS, changed)
    assert status == 409 and reply['error']['code'] == 'conflict', (status, reply)
    expected = [row(-9, value=90), row(-3), row(-2), row(-1), row(0), row(3), row(8)]
    assert query(facade, table) == expected
    assert query(facade, table, 'count') == len(expected)
    for expression, expected_ids in (
        (col('id') < 0, [-9, -3, -2, -1]), (col('id') >= 0, [0, 3, 8]),
        (col('id') == 0, [0]),
    ):
        assert query(facade, table, expression=table.where(expression).select('id')) == [
            {'id': key} for key in expected_ids]
    route_views = [placement(cluster, child) for child in parts]
    assert all(len(view['members']) == view['record']['replica_count'] == 3
               for view in route_views), route_views
    passed('O16', 'key-coverage', 'Both RF3 children, the split boundary and half-open query results agree with the exact row oracle.')

    records = {body['request_id']: facade.records(body['request_id']) for body in snapshots}
    survivors = stop_worker(cluster, owner)
    assert get(facade, independent, 9, api='api2') == row(9)
    for body, (_, timestamp) in zip(snapshots, observations, strict=True):
        request_id = body['request_id']
        copies = records[request_id]
        assert all(copies[name]['accepted'] and json.loads(copies[name]['body']) == body
                   for name in survivors), copies
        new_owner, recovered_ts = facade.reached(request_id, 'snapshot', workers=survivors, timeout=60)
        assert new_owner != owner and recovered_ts == timestamp, (observations, recovered_ts)
        facade.release(request_id, 'snapshot')
        facade.terminal(request_id, before)
    launch_retained(cluster, owner, retained_config(cluster, owner, survivors))
    assert query(facade, table, expression=table.where(category=7)) == expected
    assert get(facade, independent, 9) == row(9)
    assert facade.api('api2', OPERATIONS, original) == exact
    assert [placement(cluster, child) for child in parts] == route_views
    passed('O16', 'unrelated-shard-isolation', 'The independent shard on the same primary pools served its exact row during owner loss and after retained restart.')

    # REP1 already drives a stale split publisher across a same-run restart.
    from oltp_recovery_operations import restart_during_split
    restart_during_split(cluster, lambda body: facade.api('api1', OPERATIONS, body))
    passed('O16', 'lineage-and-mvcc', 'Pinned base and index scans recovered their original timestamp and row set on takeover. Split children retained exact replay and conflict history. A fenced split publisher was rejected and settled.')
    pending_clause('O16', 'atomic-activation',
                   'Replica snapshot installation and its atomic generation/applied-position cut require a separate fault at native activation.')


def configure_growth(cluster):
    zones = {worker_identity(cluster, worker)['worker_id']: f'fixture-{worker}' for worker in WORKERS}
    policy = dict(policy_id=uid(), volume_cost_per_gib=1, worker_cost=1,
                  initial_monthly_spend=0, monthly_spend_ceiling=30, max_attempts=3,
                  cooldown_seconds=1, max_volume_bytes=2 << 30,
                  zones=list(zones.values()), initial_replica_zones=zones)
    assert admin_owner(cluster, 'storage.growth.configure', {'policy': policy}) == policy
    return policy


def add_pool(cluster, worker, label, policy, *, check_identity=False):
    identity = worker_identity(cluster, worker)
    directory = cluster.root / worker / label
    mount = cluster.mount_pool(f'{worker}-{label}', uid(), cluster.enrollments[worker],
                               directory=directory)
    pool_id = mount['identity']['pool_id']
    claim = uid()
    intent = dict(policy_id=policy['policy_id'], claim_id=claim, worker=identity,
                  pool_id=pool_id, pool_generation=mount['generation'], quoted_monthly_cost=1,
                  monthly_spend_ceiling=policy['monthly_spend_ceiling'],
                  max_attempts=policy['max_attempts'], cooldown_seconds=policy['cooldown_seconds'],
                  action=dict(kind='add_pool', worker=identity['worker_id'],
                              zone=policy['initial_replica_zones'][identity['worker_id']], bytes=1 << 30))
    for name, payload, phase in (
        ('claim', {'intent': intent}, 'claimed'),
        ('attempt', {'claim_id': claim}, 'provisioning'),
        ('provisioned', dict(claim_id=claim, resource_id=mount['provider_volume_id']), 'awaiting_mount'),
    ):
        record = admin_owner(cluster, f'storage.growth.{name}', payload)
        assert record['intent'] == intent and record['phase'] == phase, record

    def mounted(expected):
        return admin(cluster, worker, operation('storage.growth.mounted',
                                               dict(claim_id=claim, mount=expected)))

    if check_identity:
        missing = cluster.root / worker / 'absent-attachment'
        missing.mkdir()
        sentinel = missing / 'root-disk-sentinel'
        sentinel.write_text('must survive mount refusal')
        altered_mounts = (
            dict(mount, mount_path=str(missing)),
            dict(mount, worker=worker_identity(cluster, next(name for name in WORKERS if name != worker))),
            dict(mount, provider_volume_id='docker-volume:other'),
            dict(mount, profile={'docker': {'volume_marker': uid()}}),
        )
        for altered in altered_mounts:
            status, reply = mounted(altered)
            assert status in (400, 409, 503) and reply['error']['outcome'] == 'not_applied', (status, reply)
        assert list(missing.iterdir()) == [sentinel] and sentinel.read_text() == 'must survive mount refusal'
        assert sorted(path.name for path in directory.iterdir()) == ['.nabedra-docker-volume.json']
        assert cluster.admin('storage.growth.status', {'claim_id': claim})['phase'] == 'awaiting_mount'
    enrolled = success(*mounted(mount))
    assert enrolled['phase'] == 'complete' and enrolled['intent'] == intent, enrolled
    assert enrolled['mounted_total_bytes'] >= intent['action']['bytes'], enrolled
    return mount


def move(cluster, group, worker, mount):
    before = placement(cluster, group)
    identity = worker_identity(cluster, worker)
    source = next(member for member in before['members'] if member['worker_id'] == identity['worker_id'])
    target = dict(source, node_id=max(member['node_id'] for member in before['members']) + 1,
                  pool_id=mount['identity']['pool_id'])
    move_id = uid()
    payload = dict(group=group, expected_generation=before['record']['generation'],
                   remove_node=source['node_id'], target=target, worker=identity,
                   pool_generation=mount['generation'])
    result = admin_owner(cluster, 'storage.replica.move', payload, move_id)
    assert result['phase']['state'] == 'retired', result
    after = placement(cluster, group)
    phase = result['phase']
    assert after['record'] == phase['placement'], (after, result)
    assert phase['activated_generation'] > 0
    assert phase['activated_at']['index'] >= after['record']['membership_position']['index']
    assert after['record']['generation'] != before['record']['generation']
    assert sorted(after['members'], key=lambda member: member['node_id']) == sorted(
        [target if member == source else member for member in before['members']],
        key=lambda member: member['node_id'])
    return result


def o26(facade):
    cluster = facade.cluster
    table = create(facade, 'multipool_rows')
    rows = [row(key) for key in range(512)]
    assert facade.data(batch(table, rows)) == len(rows)
    parent = initial_range(cluster, table.id)
    policy = configure_growth(cluster)
    # Keep the chosen target on the scheduler owner so both old and new
    # replicas share the same worker and failure domain during the move.
    witness = facade.query(table)
    facade.select(witness['request_id'], {'snapshot': False})
    facade.admit(witness)
    owner, _ = facade.reached(witness['request_id'], 'snapshot')
    facade.terminal(witness['request_id'], rows)
    before = cluster.admin('storage.growth.inventory', {}, owner)
    extra = add_pool(cluster, owner, 'extension', policy, check_identity=True)
    after = cluster.admin('storage.growth.inventory', {}, owner)
    assert after['worker'] == before['worker'] and after['scheduler_node_id'] == before['scheduler_node_id']
    assert len(after['pools']) == len(before['pools']) + 1
    assert after['total_available_bytes'] > before['total_available_bytes']
    assert cluster.stats(owner)['storage']['pools'] == 2
    health = call(cluster.urls[owner], '/health/ready', token=cluster.auth['data_token'])[1]
    assert health['ready_workers'] == health['replication_factor'] == 3 and health['run_id'] == cluster.run_id
    passed('O26', 'storage-only-growth', 'A mounted secondary increased available storage in the same ready worker and RF3 queue membership.')
    for field in ('thread_budget', 'cache_bytes', 'write_buffer_bytes', 'native_baseline_bytes'):
        assert before[field] == after[field], (field, before[field], after[field])
    assert after['charged_memory_bytes'] <= cluster.worker_memory, after
    passed('O26', 'aggregate-budgets', 'Pool addition preserved the worker-wide native thread, cache and write buffer budgets within its memory charge.')

    pinned = facade.query(table)
    facade.select(pinned['request_id'], {'snapshot': True})
    facade.admit(pinned)
    facade.reached(pinned['request_id'], 'snapshot')
    move(cluster, parent, owner, extra)
    parts = split(cluster, parent, 256)['children']
    for child in parts:
        route = placement(cluster, child)
        assert len({member['worker_id'] for member in route['members']}) == 3, route
        assert any(member['pool_id'] == extra['identity']['pool_id']
                   for member in route['members']), route
    assert facade.data(batch(table, [row(-1), row(512)])) == 2
    facade.release(pinned['request_id'], 'snapshot')
    facade.terminal(pinned['request_id'], rows)
    assert query(facade, table) == [row(-1), *rows, row(512)]
    assert query(facade, table, 'count') == 514
    passed('O26', 'identities-and-atomicity', 'The parent moved under the same identity, then produced RF3 children on the new pool. The pinned scan excluded the later cross-range batch, and a fresh scan included it in full.')

    pending_clause('O26', 'mount-identity-and-exclusion',
                   'Wrong worker, provider and marker plus an unmounted path were refused. Exclusive ownership of a reused disk still needs a process-level assertion.')
    pending_clause('O26', 'drain-before-detach',
                   'The live split leaves parent retention and two child placements on the secondary; a complete drain and detach still need an exercised evacuation.')


def o21(facade):
    table = facade.db.create_table('large_table', {'id': int, 'text': str, 'value': int}, key='id')
    cluster = facade.cluster
    text = 'x' * 704
    total = 102_001

    def bulk(first, end):
        body = batch(table, [dict(id=key, text=text, value=key % 7) for key in range(first, end)])
        deadline = time.monotonic() + 180
        while True:
            try:
                status, reply = facade.api('api1', OPERATIONS, body, timeout=90)
                if status == 200:
                    assert reply == dict(request_id=body['request_id'], data=end - first), reply
                    return
                assert status in (429, 503, 504), (first, end, status, reply)
            except TRANSPORT_ERRORS:
                pass
            assert time.monotonic() < deadline, (first, end, body['request_id'])
            time.sleep(.2)

    started = time.monotonic()
    bulk(0, 500)
    source = initial_range(cluster, table.id)

    def write_group(first, end):
        for offset in range(first, end, 1000):
            bulk(offset, min(offset + 1000, end))

    # Leave rows around the pivot in both children, then continue loading.
    with ThreadPoolExecutor(max_workers=2) as executor:
        running = executor.submit(write_group, 500, 24_000)
        assert get(facade, table, 0, api='api2') == dict(id=0, text=text, value=0)
        running.result()
        parts = split(cluster, source, 12_000)['children']
        writer = executor.submit(write_group, 24_000, 100_000)
        change = operation('table.update', dict(table_id=table.id, key=0, values=dict(value=-1)))
        assert facade.data(change, 'api2') is True
        assert get(facade, table, 0, api='api2') == dict(id=0, text=text, value=-1)
        writer.result()

    bulk(100_000, 100_001)
    assert query(facade, table, 'count') == 100_001
    bulk(100_001, total)
    assert total * len(text) > 64 * MIB
    victim = WORKERS[-1]
    survivors = stop_worker(cluster, victim)
    launch_retained(cluster, victim, retained_config(cluster, victim, survivors))
    assert query(facade, table, 'count') == total
    assert get(facade, table, 100_000) == dict(id=100_000, text=text, value=100_000 % 7)
    assert get(facade, table, total) is None
    assert get(facade, table, 0) == dict(id=0, text=text, value=-1)
    span = (col('id') >= 99_999) & (col('id') <= 100_002)
    selected = table.where(span).select('id', 'value').limit(3)
    assert query(facade, table, expression=selected) == [
        dict(id=key, value=key % 7) for key in range(99_999, 100_002)]
    assert query(facade, table, 'count', table.query().limit(100_001)) == 100_001
    shard_ids = {part['kind']['data']['shard_id'] for part in parts}
    assert len(shard_ids) == 2 and all(len(placement(cluster, part)['members']) == 3 for part in parts)
    status, reply = facade.api('api2', OPERATIONS, facade.query(table))
    assert status == 413 and reply['error']['code'] == 'limit_exceeded', (status, reply)
    print(json.dumps(dict(scenario='O21', rows=total, text_bytes=total * len(text),
                          elapsed_seconds=round(time.monotonic() - started, 2))), flush=True)
    pending_clause('O21', 'growth-and-exact-queries',
                   'Exact operations passed beyond the thresholds and through a restart. This run used only the original worker pools.')
    pending_clause('O21', 'no-logical-cap',
                   'The former row and byte limits were exceeded. A serialized quota counter and whole-table buffering have no dedicated observation here.')
    pending_clause('O21', 'bounded-work',
                   'Request batching, exact query limits and oversized collect rejection ran. Task memory high-water and overflow arithmetic are unobserved.')
    pending_clause('O21', 'safe-split-under-same-id',
                   'Manual RF3 split preserved the table ID. Automatic target-triggered split is unexecuted.')
    pending_clause('O21', 'capacity-errors', 'No storage deficit was injected into this growth run.')


def o28(facade):
    table = create(facade, 'paged_scan')
    rows = [row(key) for key in range(-150, 150)]
    table.insert_many(rows)
    old = facade.query(table)
    facade.select(old['request_id'], {'snapshot': True})
    facade.admit(old)
    facade.reached(old['request_id'], 'snapshot')
    source = initial_range(facade.cluster, table.id)
    split(facade.cluster, source, 0)
    table.insert(row(150))
    facade.release(old['request_id'], 'snapshot')
    facade.terminal(old['request_id'], rows)
    assert query(facade, table, 'count') == 301
    assert query(facade, table) == [*rows, row(150)]
    assert re.search(r'\bshards 2;.*linked routing pages read 1\b', table.query().explain())
    for clause, reason in (
        ('exact-paged-coverage', 'Data pages and two ranges were read exactly once. Routing stayed within one Catalog page.'),
        ('bounded-authenticated-pagination', 'No multi-page routing or authenticated continuation rejection was exercised.'),
        ('no-logical-metadata-cap', 'Routing, index and archive metadata have not exceeded the one-message limit.'),
        ('interruption-and-pins', 'A scan pinned before publication observed the old data. No process stopped during metadata pagination.'),
        ('large-counts', 'This process checked counts through 301 rows. The separate O21 workload checks 100,001 and above.'),
    ):
        pending_clause('O28', clause, reason)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=('O16', 'O21', 'O26', 'O28'), required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--test-executable', type=Path, required=True)
    args = parser.parse_args()
    if not Rf3Cluster.mounts_available():
        print('NABEDRA_OLTP_ACCEPTANCE ' + json.dumps(dict(
            scenario=args.scenario, status='not-run', reason='registered RF3 tmpfs mounts require Linux and passwordless sudo'
        )), flush=True)
        return
    direct = os.environ.get('NABEDRA_TEST_DIRECT_OLTP') != 'off'
    with tempfile.TemporaryDirectory(prefix=f'nabedra-{args.scenario.lower()}-') as root:
        cluster = Rf3Cluster(args.binary, root, mounts=True,
                             test_executable=None if args.scenario == 'O21' else args.test_executable,
                             direct=dict.fromkeys(WORKERS, direct), api_direct=(direct, direct))
        facade = Transactions(cluster, direct)
        try:
            facade.start()
            {'O16': o16, 'O21': o21, 'O26': o26, 'O28': o28}[args.scenario](facade)
        except BaseException:
            dump(cluster)
            raise
        finally:
            facade.release_all()
            cluster.close()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3.11
"""Registered RF3 shard, storage growth and metadata acceptance."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
from uuid import UUID

from oltp_direct_acceptance import admin_owner, col, dump
from oltp_direct_cluster import MIB, WORKERS, Rf3Cluster, call, operation, uid, write_json
from oltp_recovery_operations import (
    TRANSPORT_ERRORS, initial_range, restart_during_split, retained_config, success, two_ready, wait_for,
)
from oltp_transaction_acceptance import OPERATIONS, Transactions, batch, passed

ADMIN = '/v1/cluster/operations'


class GrowthCluster(Rf3Cluster):
    def __init__(self, *args, automatic=None, archive=None, metadata_profile=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.automatic = automatic
        self.archive = archive
        self.metadata_profile = metadata_profile

    def launch_worker(self, name):
        config = self.config[name]
        if self.automatic:
            config['automatic_split'] = self.automatic
        if self.archive:
            config['node']['archive_repository'] = str(self.archive)
        if self.metadata_profile:
            config['native_group_capacity'] = 'retained_split_history'
            config['resources']['task_memory_limit_bytes'] = 256 * MIB
        super().launch_worker(name)


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


def queue_owner(facade, table, expected, scan=None):
    scan = table.query() if scan is None else scan
    body = operation('query.collect', dict(table_id=table.id, query=scan._to_wire()))
    request_id = body['request_id']
    facade.select(request_id, {'snapshot': False})
    facade.admit(body)
    owner, _ = facade.reached(request_id, 'snapshot')
    facade.terminal(request_id, expected)
    return owner


def snapshot_recovery(facade):
    parent = facade.cluster
    cluster = GrowthCluster(
        parent.binary, parent.root / 'snapshot-recovery', mounts=True,
        test_executable=parent.test_executable,
        worker_env={'NABEDRA_OLTP_TEST_SHORT_DATA_SNAPSHOTS': '1'},
        direct=dict.fromkeys(WORKERS, facade.direct), api_direct=(facade.direct, facade.direct))
    test = Transactions(cluster, facade.direct)
    try:
        test.start()
        table = create(test, 'snapshot_target')
        neighbor = create(test, 'snapshot_neighbor')
        table.insert(row(0))
        neighbor.insert(row(777))
        owner = queue_owner(test, table, [row(0)])
        source = initial_range(cluster, table.id)
        victim = next(worker for worker in WORKERS if worker != owner)
        survivors = stop_worker(cluster, victim)
        for key in range(1, 49):
            request = operation('table.insert', dict(table_id=table.id, row=row(key)))
            assert test.data(request) is None

        group = source['group_id']
        barrier = cluster.barrier_dir(victim)
        control = barrier / f'native-snapshot-{group}.mode'
        staged = barrier / f'native-snapshot-{group}-staged.json'
        activated = barrier / f'native-snapshot-{group}-activated.json'
        control.write_text('pause')
        config = retained_config(cluster, victim, survivors)
        cluster.config[victim] = config
        cluster.enrollments[victim] = config['node']['enrollment']
        cluster.launch_worker(victim)
        first = wait_for(lambda: json.loads(staged.read_text()) if staged.exists() else None,
                         'native install staged on the lagging follower', 120)
        assert first['staged_generation'] == first['generation'] + 1, first
        assert first['applied']['index'] < first['incoming']['index'] and not activated.exists(), first
        assert get(test, neighbor, 777) == row(777)
        cluster.workers[victim].kill()
        cluster.workers[victim].stop()
        staged.unlink()
        control.write_text('observe')
        launch_retained(cluster, victim, config)
        second = wait_for(lambda: json.loads(activated.read_text()) if activated.exists() else None,
                          'retry activated the staged Data snapshot', 90)
        restarted = json.loads(staged.read_text())
        assert restarted['generation'] == first['generation'] and restarted['applied'] == first['applied']
        assert second['generation'] == second['staged_generation'] == restarted['staged_generation']
        assert second['applied'] == second['incoming'] == restarted['incoming']
        assert query(test, table) == [row(key) for key in range(49)]
        assert get(test, neighbor, 777) == row(777)
    except BaseException:
        dump(cluster)
        raise
    finally:
        test.release_all()
        cluster.close()


def o16(facade):
    cluster = facade.cluster
    table = create(facade, 'split_history')
    independent = create(facade, 'shared_pool_neighbor')
    initial = [row(-9), row(-1), row(0), row(8)]
    table.insert_many(initial)
    independent.insert(row(9))
    facade.make_index(table, queue_owner(facade, table, initial))
    source = initial_range(cluster, table.id)
    original = operation('table.update', dict(table_id=table.id, key=-9, values=dict(value=90)))
    exact = facade.api('api1', OPERATIONS, original)
    assert exact[0] == 200, exact

    stale = operation('table.update', dict(table_id=table.id, key=-9, values=dict(value=999)))
    stale_id = stale['request_id']
    for worker in WORKERS:
        facade.gate(worker, stale_id, 'queued')
    facade.watch_record(stale_id)
    facade.admit(stale)

    before = [row(-9, value=90), row(-1), row(0), row(8)]
    snapshots = [facade.query(table), facade.query(table, 7)]
    for body in snapshots:
        facade.select(body['request_id'], {'snapshot': True})
        facade.watch_record(body['request_id'])
        facade.admit(body)
    observations = [facade.reached(body['request_id'], 'snapshot') for body in snapshots]
    owner = observations[0][0]
    assert all(worker == owner for worker, _ in observations), observations
    wait_for(lambda: (cluster.barrier_dir(owner) / f'reached-{stale_id}').is_file(),
             'old queued mutation reached its route barrier', 25)
    stale_copy = facade.records(stale_id)[owner]
    assert stale_copy['context']['read']['routing_epoch'] == 1, stale_copy

    # These rows must be visible on current routes and absent from both old pins.
    table.insert(row(-2))
    parts = split(cluster, source, 0)['children']
    for worker in WORKERS:
        facade.release_copy(worker, stale_id)

    def stale_rejected():
        status, reply = facade.receipt(stale_id)
        if status != 200 or reply['state'] in ('queued', 'running', 'committing', 'reconciling'):
            return None
        assert reply['state'] == 'failed' and reply['error']['outcome'] == 'not_applied', reply
        return reply

    wait_for(stale_rejected, 'old queued mutation rejected after split', 45)
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
    restart_during_split(cluster, lambda body: facade.api('api1', OPERATIONS, body))
    moving_read = facade.query(table, 7)
    facade.select(moving_read['request_id'], {'snapshot': True})
    facade.admit(moving_read)
    new_owner, _ = facade.reached(moving_read['request_id'], 'snapshot')
    extra = add_pool(cluster, new_owner, 'move-destination', configure_growth(cluster))
    move(cluster, parts[0], new_owner, extra)
    facade.release(moving_read['request_id'], 'snapshot')
    facade.terminal(moving_read['request_id'], expected)
    assert facade.api('api2', OPERATIONS, original) == exact
    passed('O16', 'lineage-and-mvcc', 'Pinned base and index scans recovered their original timestamp and row set on takeover. Split children retained exact replay and conflict history. A fenced split publisher was rejected and settled.')
    snapshot_recovery(facade)
    passed('O16', 'atomic-activation', 'SIGKILL interrupted a real follower install after durable staging. Its retained restart exposed the prior generation/position pair before retry and the incoming pair after activation.')


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
        lock_path = directory / '.nabedra-volume.lock'
        with open(lock_path, 'xb', opener=lambda path, flags: os.open(path, flags, 0o600)) as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            status, reply = mounted(mount)
            assert status in (409, 503) and reply['error']['outcome'] == 'not_applied', (status, reply)
            assert sorted(path.name for path in directory.iterdir()) == [
                '.nabedra-docker-volume.json', '.nabedra-volume.lock']
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
    request = operation('storage.replica.move', payload, move_id)

    def advanced():
        status, reply = admin(cluster, worker, request, timeout=75)
        if status in (429, 503, 504):
            return None
        return success(status, reply)

    result = wait_for(advanced, 'same-ID replica move', 165)
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


def drain_and_detach(facade, owner, mount, target):
    cluster = facade.cluster
    table = create(facade, 'drain_rows')
    expected = [row(1), row(2)]
    table.insert_many(expected)
    group = initial_range(cluster, table.id)
    move(cluster, group, owner, mount)
    drain_id = uid()
    pool_id = mount['identity']['pool_id']
    drain = operation('storage.pool.drain', dict(pool_id=pool_id, generation=mount['generation']), drain_id)
    permit = operation('storage.pool.detach_permit', {}, drain_id)
    status, reply = admin(cluster, owner, drain)
    assert status == 409 and 'replica' in reply['error']['message'], (status, reply)
    assert admin(cluster, owner, permit)[0] != 200
    assert success(*admin(cluster, owner, operation('storage.pool.drain.status', {}, drain_id)))['phase'] == 'draining'
    move(cluster, group, owner, target)
    closed = success(*admin(cluster, owner, drain))
    assert closed['phase'] == 'closed' and closed['mount']['expected'] == mount, closed
    assert success(*admin(cluster, owner, permit)) == closed
    directory = Path(mount['mount_path'])
    assert json.loads((directory / '.nabedra-closed-pool-v1.json').read_text()) == dict(
        format_version=1, close_id=drain_id, expected=mount)
    subprocess.run(['sudo', '-n', 'umount', str(directory)], check=True, capture_output=True, timeout=60)
    cluster.mounted.remove(directory)
    assert not list(directory.iterdir()), list(directory.iterdir())
    assert query(facade, table) == expected
    assert query(facade, table, 'count') == len(expected)


def o26(facade):
    cluster = facade.cluster
    table = facade.db.create_table('multipool_rows', {
        'id': int, 'category': int, 'value': int, 'payload': str,
    }, key='id')
    rows = [row(key) for key in range(2048)]
    payload = 'x' * (4 << 10)
    for offset in range(0, len(rows), 128):
        group = [item | {'payload': payload} for item in rows[offset:offset + 128]]
        assert facade.data(batch(table, group)) == len(group)
    scan = table.query().select('id', 'category', 'value')
    parent = initial_range(cluster, table.id)
    policy = configure_growth(cluster)
    # Keep the chosen target on the scheduler owner so both old and new
    # replicas share the same worker and failure domain during the move.
    owner = queue_owner(facade, table, rows, scan)
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

    pinned = operation('query.collect', dict(table_id=table.id, query=scan._to_wire()))
    facade.select(pinned['request_id'], {'snapshot': True})
    facade.admit(pinned)
    facade.reached(pinned['request_id'], 'snapshot')
    move(cluster, parent, owner, extra)
    parts = split(cluster, parent, 1024)['children']
    for child in parts:
        route = placement(cluster, child)
        assert len({member['worker_id'] for member in route['members']}) == 3, route
        assert any(member['pool_id'] == extra['identity']['pool_id']
                   for member in route['members']), route
    added = [row(-1), row(len(rows))]
    assert facade.data(batch(table, [item | {'payload': payload} for item in added])) == 2
    facade.release(pinned['request_id'], 'snapshot')
    facade.terminal(pinned['request_id'], rows)
    assert query(facade, table, expression=scan) == [added[0], *rows, added[1]]
    assert get(facade, table, 1024) == rows[1024] | {'payload': payload}
    assert query(facade, table, 'count') == len(rows) + len(added)
    passed('O26', 'identities-and-atomicity', 'The parent moved under the same identity, then produced RF3 children on the new pool. The pinned scan excluded the later cross-range batch, and a fresh scan included it in full.')

    passed('O26', 'mount-identity-and-exclusion',
           'Wrong worker, provider and filesystem marker, a root-disk path, and a real mount held by a foreign volume lock were refused before any pool data or manifest was created.')
    drained = add_pool(cluster, owner, 'to-drain', policy)
    replacement = add_pool(cluster, owner, 'replacement', policy)
    drain_and_detach(facade, owner, drained, replacement)
    assert query(facade, table, 'count') == len(rows) + len(added)
    passed('O26', 'drain-before-detach', 'Control refused detach while a live replica used the secondary. After its replacement, the durable close and detach permit preceded a real unmount.')


def automatic_view(cluster, source, state=None):
    for worker in cluster.live():
        observed = cluster.admin('storage.shard.split.status', {}, worker)
        observation = observed['sample'] if state is None else observed['transition']
        if observation and observation['source'] == source['group_id']:
            if state is None or observation['state'] == state:
                return worker, observed
    return None


def split_child(source, attempt, side):
    def derived(domain, first, second):
        data = domain + UUID(first).bytes + UUID(second).bytes
        return str(UUID(bytes=hashlib.sha256(data).digest()[:16]))

    table_id = source['kind']['data']['table_id']
    shard = derived(f'nabedra/split-{side}/v1'.encode(), source['group_id'], attempt)
    group = derived(b'nabedra/split-data-group/v1', table_id, shard)
    return source | {'group_id': group, 'kind': {'data': dict(table_id=table_id, shard_id=shard)}}


def check_capacity_deficit(facade):
    parent = facade.cluster
    cluster = GrowthCluster(parent.binary, parent.root / 'capacity-deficit', mounts=True,
                            automatic=dict(shard_target_bytes=64 << 10, minimum_free_bytes=3 << 30),
                            direct=dict.fromkeys(WORKERS, facade.direct), api_direct=(facade.direct,))
    constrained = Transactions(cluster, facade.direct)
    try:
        constrained.start()
        table = constrained.db.create_table('capacity_deficit', {'id': int, 'data': str}, key='id')
        rows = [dict(id=key, data='x' * (8 << 10)) for key in range(16)]
        assert constrained.data(batch(table, rows)) == len(rows)
        source = initial_range(cluster, table.id)
        owner, observation = wait_for(lambda: automatic_view(cluster, source, 'capacity_pending'),
                                      'mounted volume split capacity outcome', 90)
        assert observation['transition']['error'] == 'limit_exceeded', observation
        assert observation['sample']['observed_committed_bytes'] > observation['target_bytes'], observation
        configure_growth(cluster)
        pin = cluster.admin('storage.growth.snapshot', {})['pin']
        sample = cluster.admin('storage.growth.sample', {
            'pin': pin, 'worker': worker_identity(cluster, owner),
        }, owner)
        deficit = sample['split_deficit']
        assert deficit['source']['record']['identity'] == source, deficit
        assert deficit['required_bytes'] >= 3 << 30 and deficit['pools'], deficit
        assert all(pool['available_bytes'] < pool['required_bytes'] for pool in deficit['pools']), deficit
        assert query(constrained, table) == rows
        assert 'shards 1;' in table.query().explain()
    finally:
        cluster.close()


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
    owner, below = wait_for(lambda: automatic_view(cluster, source), 'below-target usage sample', 60)
    assert below['sample']['observed_committed_bytes'] < below['target_bytes'], below
    extra = add_pool(cluster, owner, 'large-table-extension', configure_growth(cluster))
    move(cluster, source, owner, extra)

    def write_group(first, end):
        for offset in range(first, end, 1000):
            bulk(offset, min(offset + 1000, end))

    with ThreadPoolExecutor(max_workers=2) as executor:
        running = executor.submit(write_group, 500, 72_000)
        assert get(facade, table, 0, api='api2') == dict(id=0, text=text, value=0)
        _, publication = wait_for(lambda: automatic_view(cluster, source, 'published'),
                                  'automatic split publication', 240)
        running.result()
        attempt = publication['transition']['attempt']
        parts = [split_child(source, attempt, side) for side in ('left', 'right')]
        assert facade.db.table('large_table').id == table.id
        assert all(any(member['pool_id'] == extra['identity']['pool_id']
                       for member in placement(cluster, child)['members']) for child in parts)
        passed('O21', 'safe-split-under-same-id', 'The measured target was crossed and the worker published an RF3 split under the original table ID without a split request.')
        writer = executor.submit(write_group, 72_000, 100_000)
        change = operation('table.update', dict(table_id=table.id, key=0, values=dict(value=-1)))
        assert facade.data(change, 'api2') is True
        assert get(facade, table, 0, api='api2') == dict(id=0, text=text, value=-1)
        writer.result()

    bulk(100_000, 100_001)
    assert query(facade, table, 'count') == 100_001
    bulk(100_001, total)
    assert total * len(text) > 64 * MIB
    victim = next(worker for worker in reversed(WORKERS) if worker != owner)
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
    passed('O21', 'growth-and-exact-queries', 'The 100,001st row and later batches succeeded across RF3 ranges and independent pools. Counts, point reads and bounded projected queries agreed after retained restart.')
    pending_clause('O21', 'no-logical-cap',
                   'The former row and byte limits were exceeded. A serialized quota counter and whole-table buffering have no dedicated observation here.')
    pending_clause('O21', 'bounded-work',
                   'Request batching, exact query limits and oversized collect rejection ran. Task memory high-water and overflow arithmetic are unobserved.')
    check_capacity_deficit(facade)
    passed('O21', 'capacity-errors', 'A separate RF3 table crossed its target on 2 GiB mounts with a 3 GiB free-space floor. The split reported a measured disk deficit and the existing rows stayed readable.')


def archive_routes(cluster, table, expected):
    owner = dict(backup_id=uid(), attempt=1)
    serialized = json.dumps([list(b'nabedra/native-backup-id/v1'), cluster.cluster_id, owner],
                            separators=(',', ':')).encode()
    output = cluster.archive / str(UUID(bytes=hashlib.sha256(serialized).digest()[:16]))
    request = operation('archive.create', dict(output=str(output), previous=None, owner=owner))

    def backup():
        for worker in cluster.live():
            status, reply = admin(cluster, worker, request, timeout=90)
            if status == 200:
                return reply['data']
            assert status in (429, 503, 504), (status, reply)
        return None

    manifest = wait_for(backup, 'full archive publication', 180)
    assert manifest['scope']['cut'] >= expected[-1]['at'], manifest
    history, = output.glob('.history-*.partial')
    versions = {}
    index_closures = set()
    with history.open('rb') as stream:
        assert stream.read(16) == b'NABEDRA-HST-0001'
        while size_bytes := stream.read(8):
            assert len(size_bytes) == 8
            size = int.from_bytes(size_bytes, 'big')
            assert 0 < size <= 32 * MIB
            digest = stream.read(32)
            payload = stream.read(size)
            assert len(payload) == size and hashlib.sha256(payload).digest() == digest
            frame = json.loads(payload)
            image = frame.get('image')
            if not image or image['commit_ts'] > manifest['scope']['cut']:
                continue
            key = image['key']
            closed = key.get('catalog_index_closed', {})
            if closed.get('table_id') == table.id and image['value'] is not None:
                index_closures.add(closed['closure_id'])
            for kind in ('catalog_routing_root', 'catalog_routing_page'):
                item = key.get(kind, {})
                if item.get('table_id') != table.id:
                    continue
                identifier = item.get('page_id')
                if identifier not in versions or versions[identifier][0] < image['commit_ts']:
                    versions[identifier] = image['commit_ts'], size, image['value']

    _, _, root = versions.pop(None)
    assert root['routing_epoch'] == len(expected), root
    page_id = root['first_page']
    observed, page_sizes = [], []
    while page_id is not None:
        _, size, page = versions.pop(page_id)
        assert 0 < len(page['shards']) <= 128
        observed.extend(page['shards'])
        page_sizes.append(size)
        page_id = page['next_page']
    assert all(value is None for _, _, value in versions.values()), versions.keys()
    assert len(page_sizes) == 2 and sum(page_sizes) > 64 << 10, page_sizes
    assert [(item['group'], item['span']) for item in observed] == [
        (item['group'], item['span']) for item in expected]
    assert index_closures, 'full archive omitted index closure'
    print(json.dumps(dict(scenario='O28', routes=len(observed), routing_frame_bytes=page_sizes,
                          archive_frames=manifest['frames'], index_closures=len(index_closures))), flush=True)


def o28(facade):
    cluster = facade.cluster
    table = facade.db.create_table('paged_scan', {'id': str, 'category': int, 'value': int}, key='id')

    def key(index):
        prefix = f'{index:04}/'
        return prefix + hashlib.shake_256(prefix.encode()).hexdigest(160)

    rows = [row(key(index * 2), value=index) for index in range(130)]
    assert facade.data(batch(table, rows)) == len(rows)
    facade.make_index(table, queue_owner(facade, table, rows))

    source = initial_range(cluster, table.id)
    routes = [dict(group=source, span=dict(start=None, end=None), at=0)]

    def next_split():
        boundary = rows[len(routes)]['id']
        parent = routes.pop()
        publication = split(cluster, parent['group'], boundary)
        assert publication['at'] > parent['at'], publication
        first, second = publication['children']
        start, end = parent['span']['start'], parent['span']['end']
        routes.extend([dict(group=first, span=dict(start=start, end=boundary), at=publication['at']),
                       dict(group=second, span=dict(start=boundary, end=end), at=publication['at'])])
        if len(routes) in (32, 64, 96, 128, 129, 130):
            print(f'O28 published {len(routes)} routes', flush=True)

    def check_plan(plan, shards, pages):
        assert re.search(rf'\bshards {shards};.*linked routing pages read {pages}\b', plan), plan
        assert f'routing epoch {shards};' in plan, plan

    while len(routes) < 128:
        next_split()
    check_plan(table.query().explain(), 128, 1)
    next_split()
    check_plan(table.query().explain(), 129, 2)
    assert query(facade, table) == rows
    assert query(facade, table, 'count') == len(rows)
    assert 'by_category' in table.where(category=7).explain()
    assert query(facade, table, expression=table.where(category=7)) == rows

    old = facade.query(table)
    old_id = old['request_id']
    facade.select(old_id, {'snapshot': True})
    facade.watch_record(old_id)
    facade.admit(old)
    owner, ts = facade.reached(old_id, 'snapshot')
    barrier = cluster.barrier_dir(owner)
    pause = barrier / f'routing-page-{ts}.pause'
    pause.touch()

    try:
        facade.release(old_id, 'snapshot')
        wait_for(lambda: (barrier / f'routing-page-{ts}.reached').is_file(),
                 'scan consumed the first Catalog page', 45)
        next_split()
        extra = row(key(1), category=8, value=-1)
        table.insert(extra)
        records = facade.records(old_id)
        survivors = stop_worker(cluster, owner)
        recovered_owner, recovered_ts = facade.reached(old_id, 'snapshot', workers=survivors, timeout=90)
        assert recovered_owner != owner and recovered_ts == ts, (owner, ts, recovered_owner, recovered_ts)
        assert all(records[name]['context'] == records[owner]['context'] for name in survivors), records
        unauthorized = facade.receipt(old_id, token='x' * 43)
        assert unauthorized[0] in (401, 403), unauthorized
        facade.terminal(old_id, rows, timeout=120)
        launch_retained(cluster, owner, retained_config(cluster, owner, survivors))
    finally:
        pause.unlink(missing_ok=True)

    check_plan(table.query().explain(), 130, 2)
    expected = [rows[0], extra, *rows[1:]]
    assert query(facade, table) == expected
    assert query(facade, table, expression=table.where(category=7)) == rows
    assert query(facade, table, 'count') == len(expected)
    for index in (0, 1, 127, 128, 129):
        assert get(facade, table, rows[index]['id']) == rows[index]
    archive_routes(cluster, table, routes)
    passed('O28', 'exact-paged-coverage', 'Scans and the verified archive covered two Catalog pages in routing order. Every interval, indexed row and split boundary matched the oracle.')
    passed('O28', 'no-logical-metadata-cap', 'One table retained 130 routes, and its two current routing archive frames exceeded 64 KiB in aggregate with its index and exact query results intact.')
    pending_clause('O28', 'bounded-authenticated-pagination',
                   'The planner traversed two bounded Catalog pages and unauthenticated receipt access was refused. A tampered internal continuation was not exercised.')
    pending_clause('O28', 'interruption-and-pins',
                   'The owner was killed after page one, and the survivor recovered the exact original pin and rows across a split. This history does not inject a stale writer.')
    pending_clause('O28', 'large-counts',
                   'This multi-page fixture does not load 100,001 rows or force checked arithmetic overflow. O21 checks counts past 100,000 separately.')


@contextmanager
def metadata_cgroup(enabled):
    if not enabled:
        yield
        return
    root = Path('/sys/fs/cgroup')
    membership = next(line.removeprefix('0::/') for line in Path('/proc/self/cgroup').read_text().splitlines()
                      if line.startswith('0::/'))
    scope = root / f'nabedra-acc5-{uid()}'

    def privileged(*args, **kwargs):
        return subprocess.run(['sudo', '-n', *map(str, args)], check=True, timeout=20,
                              capture_output=True, text=True, **kwargs)

    def set_value(path, value):
        privileged('tee', path, input=f'{value}\n')

    privileged('mkdir', scope)
    joined = False
    try:
        set_value(scope / 'memory.max', 12 << 30)
        set_value(scope / 'pids.max', 600)
        set_value(scope / 'cgroup.procs', os.getpid())
        joined = True
        yield
    finally:
        if joined:
            set_value(root / membership / 'cgroup.procs', os.getpid())
        privileged('rmdir', scope)


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
    paged = args.scenario == 'O28'
    with tempfile.TemporaryDirectory(prefix=f'nabedra-{args.scenario.lower()}-') as root, metadata_cgroup(paged):
        archive = Path(root) / 'archives' if paged else None
        if archive:
            archive.mkdir(mode=0o700)
        cluster = GrowthCluster(
            args.binary, root, mounts=True, metadata_profile=paged, archive=archive,
            worker_memory=(3 if paged else 2) << 30,
            automatic=dict(shard_target_bytes=32 * MIB, minimum_free_bytes=64 * MIB)
            if args.scenario == 'O21' else None,
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

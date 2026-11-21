"""RF3 process assertions for O06, O08, O09 and O11."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import http.client
import os
from pathlib import Path
import tempfile
from threading import Barrier
import time

from oltp_direct_acceptance import admin_owner, dump, queue_owner
from oltp_direct_cluster import Rf3Cluster, WORKERS, operation, uid
from oltp_transaction_acceptance import OPERATIONS, REQUESTS, Transactions, batch, passed, rows


def not_run(scenario, clause, detail):
    import json

    print('NABEDRA_OLTP_ASSERT ' + json.dumps(dict(
        scenario=scenario, assertion=clause, status='not-run', detail=detail,
    )), flush=True)


def create(fixture, name):
    return fixture.db.create_table(name, {'id': int, 'category': int, 'value': int}, key='id')


def point(table, verb, **payload):
    return operation('table.' + verb, {'table_id': table.id, **payload})


def exchange(fixture, body, api='api1'):
    return fixture.api(api, OPERATIONS, body, timeout=35)


def uncertain_or_refused(fixture, body):
    try:
        status, reply = exchange(fixture, body)
    except (OSError, http.client.HTTPException) as error:
        return repr(error)
    assert status == 503 and reply['error']['code'] in ('storage_unavailable', 'outcome_unknown'), (status, reply)
    assert reply['error']['outcome'] in ('not_applied', 'unknown'), reply
    return reply


def o06(fixture):
    table = create(fixture, 'quorum')
    acknowledged = [point(table, 'insert', row=row) for row in rows(1, 2, 3, category=5)]
    for body in acknowledged:
        assert fixture.data(body) is None

    workers = fixture.cluster.workers
    owner = queue_owner(fixture.cluster)
    paused = [worker for worker in WORKERS if worker != owner]
    for worker in paused:
        workers[worker].pause()
    try:
        # Exercise both entry points concurrently while the same two voters are stopped.
        with ThreadPoolExecutor(max_workers=2) as pool:
            read = pool.submit(uncertain_or_refused, fixture, point(table, 'get', key=1))
            write = pool.submit(uncertain_or_refused, fixture,
                                point(table, 'insert', row=rows(4, category=9)[0]))
            print('O06 isolated survivor', read.result(), write.result(), flush=True)
        # Remove the survivor before restoring a quorum, to force a new leader
        # for groups it led and prevent its uncommitted suffix from driving recovery.
        workers[owner].kill()
    finally:
        for worker in paused:
            workers[worker].resume()

    retry = point(table, 'get', key=1)
    deadline = time.monotonic() + 90
    while True:
        try:
            status, reply = exchange(fixture, retry, 'api2')
        except (OSError, http.client.HTTPException) as error:
            status, reply = None, repr(error)
        if status == 200:
            assert reply['data'] == rows(1, category=5)[0], reply
            break
        assert time.monotonic() < deadline, (status, reply)
        time.sleep(.25)
    for body in acknowledged:
        row = body['payload']['row']
        assert fixture.data(point(table, 'get', key=row['id']), 'api2') == row
        assert exchange(fixture, body, 'api2') == (200, dict(request_id=body['request_id'], data=None))
    assert fixture.data(point(table, 'get', key=4), 'api2') is None
    assert fixture.data(point(table, 'insert', row=rows(5, category=10)[0]), 'api2') is None
    passed('O06', 'quorum-and-applied-authority',
           'Two stopped RF3 voters prevented both read and write success. A surviving quorum served the applied row and a fresh write after SIGKILL of the old queue owner.')
    passed('O06', 'acknowledged-write-survival',
           'The surviving processes returned all three acknowledged rows and the exact results for their original identities.')


def run_concurrently(fixture, requests):
    start = Barrier(len(requests))

    def call(api, body):
        start.wait(timeout=10)
        invoked = time.monotonic_ns()
        response = exchange(fixture, body, api)
        return invoked, time.monotonic_ns(), body, response

    with ThreadPoolExecutor(max_workers=len(requests)) as pool:
        replies = [pool.submit(call, api, body) for api, body in requests]
        return [reply.result(timeout=40) for reply in replies]


def transition(state, body):
    """Sequential specification for the mutations and observations in this history."""
    state = state.copy()
    payload = body['payload']
    verb = body['operation']
    if verb in ('table.insert', 'table.insert_many'):
        inserts = [payload['row']] if verb == 'table.insert' else payload['rows']
        keys = {row['id'] for row in inserts}
        if len(keys) != len(inserts) or keys & state.keys():
            return state, (409, 'conflict', 'not_applied')
        state.update((row['id'], row) for row in inserts)
        return state, (200, None if verb == 'table.insert' else len(inserts))
    key = payload['key']
    if verb == 'table.get':
        return state, (200, state.get(key))
    found = key in state
    if verb == 'table.update' and found:
        state[key] = state[key] | payload['values']
    if verb == 'table.delete':
        state.pop(key, None)
    return state, (200, found)


def assert_serial(history, initial, final):
    predecessors = [sum(1 << i for i, (_, end, _, _) in enumerate(history) if end < start)
                    for start, _, _, _ in history]

    @lru_cache(maxsize=None)
    def check(mask, encoded_state):
        state = {key: dict(id=key, category=category, value=value)
                 for key, category, value in encoded_state}
        if mask == (1 << len(history)) - 1:
            return state == final
        for i, (_, _, body, (status, reply)) in enumerate(history):
            bit = 1 << i
            if mask & bit or predecessors[i] & ~mask:
                continue
            next_state, expected = transition(state, body)
            actual = (status, reply['data']) if status == 200 else (
                status, reply['error']['code'], reply['error']['outcome'],
            )
            if expected == actual and check(mask | bit, encode(next_state)):
                return True
        return False

    def encode(state):
        return tuple((key, row['category'], row['value']) for key, row in sorted(state.items()))

    assert check(0, encode(initial)), (history, initial, final)


def o08(fixture):
    table = create(fixture, 'serial_history')
    initial_rows = rows(1, category=0)
    assert fixture.data(batch(table, initial_rows)) == 1
    before = fixture.cluster.all_stats()
    history = run_concurrently(fixture, [
        ('api1', point(table, 'update', key=1, values={'category': 71})),
        ('api2', point(table, 'update', key=1, values={'value': 72})),
        ('api1', batch(table, rows(2, 3, category=2))),
        ('api2', batch(table, rows(3, 4, category=4))),
        ('api1', point(table, 'insert', row=rows(3, category=6)[0])),
        ('api2', point(table, 'delete', key=5)),
        ('api1', point(table, 'get', key=3)),
        ('api2', point(table, 'get', key=1)),
    ])
    for _, _, body, (_, reply) in history:
        assert reply['request_id'] == body['request_id'], (body, reply)
    final = {row['id']: row for row in fixture.data(fixture.query(table))}
    assert_serial(history, {1: initial_rows[0]}, final)
    assert final[1] == dict(id=1, category=71, value=72), final
    assert 3 in final and (2 in final) + (4 in final) <= 1 and 5 not in final, final
    if fixture.direct:
        after = fixture.cluster.all_stats()
        assert sum(((after[w]['direct'] or {}).get('mutations', 0)
                    - (before[w]['direct'] or {}).get('mutations', 0)) for w in WORKERS) > 0
    passed('O08', 'serial-history', 'A real-time serial-order checker matched eight RF3 calls, including overlapping batches and disjoint-field updates of one row.')
    passed('O08', 'uniqueness-and-absence', 'The checker verified insert uniqueness, atomic batch conflicts, absent-key deletion and observed point reads.')

    missing = point(table, 'update', key=9, values={'value': 90})
    assert exchange(fixture, missing, 'api2') == (200, dict(request_id=missing['request_id'], data=False))
    existing = point(table, 'insert', row=rows(9, category=9)[0])
    assert fixture.data(existing) is None
    conflict = point(table, 'insert', row=rows(9, category=99)[0])
    first = exchange(fixture, conflict, 'api2')
    assert first[0] == 409 and first[1]['error']['outcome'] == 'not_applied', first
    assert exchange(fixture, missing, 'api2') == (200, dict(request_id=missing['request_id'], data=False))
    assert fixture.data(point(table, 'get', key=9)) == rows(9, category=9)[0]
    assert fixture.data(point(table, 'delete', key=9)) is True
    assert exchange(fixture, conflict, 'api2') == first
    assert fixture.data(point(table, 'get', key=9)) is None
    passed('O08', 'stable-results', 'Queued no-op and conflict replays through the same identity returned the original outcome after a change that would reverse a re-executed result.')

    name = 'schema_race'
    original = create(fixture, name)
    contender = operation('table.create', {'name': name, 'key': 'id',
                                           'schema': {'id': 'int64', 'category': 'int64', 'value': 'int64'}})
    # Use the production ORM serialization for competing create and drop.
    from oltp_direct_acceptance import d11_drop
    d11_drop(fixture.cluster, fixture.cluster.api_urls['api1'], fixture.cluster.auth['data_token'], fixture.cluster.ca)
    assert original.id == fixture.db.table(name).id
    passed('O08', 'bounded-progress', 'The concurrent history and the RF3 drop/write race returned within the fixture deadlines.')
    not_run('O08', 'schema-fences', 'Drop/write and recreate assertions ran. Concurrent creation for the same name still needs an observable race.')
    not_run('O08', 'fresh-snapshot-retry', 'The final row preserves both field changes. No retry was forced and observed to replan from a fresh snapshot.')
    not_run('O08', 'storage-reservations', 'The RF3 process fixture has no deterministic admission gate for competing pool-space reservations.')


def split_table(fixture, table):
    seed = batch(table, rows(-5, 5, category=0))
    fixture.select(seed['request_id'], {'participants': False})
    fixture.admit(seed)
    owner, plans = fixture.reached(seed['request_id'], 'participants')
    assert len(plans) == 1, plans
    fixture.terminal(seed['request_id'], 2)
    fixture.make_index(table, owner)
    split_id = uid()
    for _ in range(16):
        result = admin_owner(fixture.cluster, 'storage.shard.split', {
            'source': plans[0]['group'], 'boundary': 0, 'page_bytes': 1 << 20,
        }, split_id)
        if result['state'] == 'published':
            assert len(result['children']) == 2, result
            return result['children']
        assert result['state'] == 'importing', result
    raise AssertionError(result)


def o09(fixture):
    table = create(fixture, 'timestamp_order')
    groups = split_table(fixture, table)
    before_rows = rows(-5, 5, category=0)
    inserts = rows(-9, 9, category=7)
    mutation = batch(table, inserts)
    request = mutation['request_id']
    fixture.select(request, {'participants': True, 'commit-stamp': False, 'commit-reply': True})
    fixture.admit(mutation)
    _, plans = fixture.reached(request, 'participants')
    assert {p['group']['group_id'] for p in plans} == {g['group_id'] for g in groups}
    prepares = [f"prepare-{p['group']['group_id']}-{i}" for p in plans for i in range(p['page_count'])]
    seals = [f"seal-{p['group']['group_id']}" for p in plans]
    fixture.select(request, dict.fromkeys(prepares + seals, True))
    fixture.release(request, 'participants')
    prepared = []
    old_scan = fixture.query(table)
    old_id = old_scan['request_id']
    for plan in plans:
        group_id = plan['group']['group_id']
        for i in range(plan['page_count']):
            stage = f'prepare-{group_id}-{i}'
            _, proof = fixture.reached(request, stage)
            assert proof['position']['index'] > 0, proof
            prepared.append(proof)
            assert not any(path.exists() for path in fixture.paths(request, 'commit-stamp'))
            if len(prepared) == 1:
                fixture.select(old_id, {'snapshot': True})
                fixture.admit(old_scan, 'api2')
                _, old_stamp = fixture.reached(old_id, 'snapshot')
            fixture.release(request, stage)
        stage = f'seal-{group_id}'
        _, seal = fixture.reached(request, stage)
        assert seal['participant'] == plan, (seal, plan)
        assert not any(path.exists() for path in fixture.paths(request, 'commit-stamp'))
        fixture.release(request, stage)
    _, stamp = fixture.reached(request, 'commit-stamp')
    assert stamp['allocated_at']['index'] > 0, stamp
    assert stamp['commit_ts'] > max(old_stamp, *(p['read_ts'] for p in plans)), (stamp, plans, old_stamp)
    _, decision = fixture.reached(request, 'commit-reply')
    assert decision['decision'] == {'commit': {'commit_ts': stamp['commit_ts']}}, decision
    passed('O09', 'commit-after-prepare', 'Both RF3 participants applied their pages and sealed the manifest before allocation. The exact decision used the later timestamp.')

    # Drain the older snapshot while the coordinator is still held. A quorum
    # Status decision already exists, so a newer read can resolve the intent.
    fixture.release(old_id, 'snapshot')
    fixture.terminal(old_id, before_rows)
    expected = sorted(before_rows + inserts, key=lambda row: row['id'])
    for body, result in [(fixture.query(table), expected), (fixture.query(table, 7), inserts)]:
        probe = body['request_id']
        fixture.select(probe, {'snapshot': False})
        fixture.admit(body, 'api2')
        _, probe_stamp = fixture.reached(probe, 'snapshot')
        assert probe_stamp > stamp['commit_ts'], (probe_stamp, stamp)
        fixture.terminal(probe, result)
    fixture.release(request, 'commit-reply')
    fixture.terminal(request, len(inserts))
    not_run('O09', 'ambiguous-intent-reads', 'Older and newer base/index scans returned the exact rows. A point miss at a still-unresolved intent needs its own observed history.')
    not_run('O09', 'timestamp-order', 'Prepare and scan timestamps passed the real-time checks. Allocator failover, old leader resume and wall-clock jumps still need process evidence.')
    not_run('O09', 'no-backdated-allocation', 'Scan pins followed the queued commit. The AllocateDirect window was not observed in this process history.')


def o11(fixture):
    table = create(fixture, 'crash_replay')
    committed = batch(table, rows(1, 2, category=5))
    fixture.select(committed['request_id'], {'participants': False})
    fixture.admit(committed)
    owner, _ = fixture.reached(committed['request_id'], 'participants')
    receipt = fixture.terminal(committed['request_id'], 2)
    fixture.make_index(table, owner)
    written = point(table, 'insert', row=rows(3, category=5)[0])
    exact = exchange(fixture, written)
    assert exact == (200, dict(request_id=written['request_id'], data=None)), exact
    held = batch(table, rows(8, 9, category=5))
    fixture.select(held['request_id'], {'participants': True})
    fixture.admit(held)
    fixture.reached(held['request_id'], 'participants')
    fixture.cluster.kill_all()
    old_run = fixture.cluster.run_id
    fixture.cluster.cold_start(keep_certificates=True)
    assert old_run != fixture.cluster.run_id
    for name in ('api1', 'api2'):
        fixture.cluster.start_api(name, fixture.direct)
    if fixture.direct:
        fixture.cluster.wait_direct_active()
    status, actual = fixture.api('api2', f"{REQUESTS}/{old_run}/{committed['request_id']}")
    assert (status, actual) == (200, receipt), (status, actual, receipt)
    assert exchange(fixture, written, 'api2') == exact
    status, abandoned = fixture.api('api2', f"{REQUESTS}/{old_run}/{held['request_id']}")
    assert status == 200 and abandoned['state'] == 'abandoned', (status, abandoned)
    assert abandoned['result'] is None, abandoned
    assert fixture.data(fixture.query(table), 'api2') == rows(1, 2, 3, category=5)
    assert fixture.data(fixture.query(table, 5), 'api2') == rows(1, 2, 3, category=5)
    passed('O11', 'committed-state-replay',
           'SIGKILL of all RF3 workers and gateways preserved acknowledged mutations and exact decisions. A request gated before Prepare was abandoned after cold start.')


SCENARIOS = {'O06': o06, 'O08': o08, 'O09': o09, 'O11': o11}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=SCENARIOS, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--test-executable', type=Path, required=True)
    args = parser.parse_args()
    direct = os.environ.get('NABEDRA_TEST_DIRECT_OLTP') != 'off'
    mounts = args.scenario == 'O11'
    if mounts and not Rf3Cluster.mounts_available():
        not_run('O11', 'committed-state-replay', 'RF3 cold start requires separate Linux pool mounts and passwordless sudo.')
        return
    with tempfile.TemporaryDirectory(prefix=f'nabedra-{args.scenario.lower()}-') as root:
        api2_direct = direct and args.scenario != 'O08'
        cluster = Rf3Cluster(args.binary, root, test_executable=args.test_executable,
                             direct=dict.fromkeys(WORKERS, direct), api_direct=(direct, api2_direct),
                             mounts=mounts)
        fixture = Transactions(cluster, direct)
        try:
            fixture.start()
            SCENARIOS[args.scenario](fixture)
        except BaseException:
            dump(cluster)
            raise
        finally:
            fixture.release_all()
            cluster.close()


if __name__ == '__main__':
    main()

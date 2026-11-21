"""RF3 process assertions for O06, O08, O09 and O11."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import http.client
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from threading import Barrier
import time

from oltp_direct_acceptance import admin_owner, d11_drop, dump
from oltp_direct_cluster import Rf3Cluster, WORKERS, operation, uid
from oltp_runtime_acceptance import evidence, wait_until
from oltp_transaction_acceptance import OPERATIONS, REQUESTS, Transactions, batch, passed, rows
from oltp_terminal_copy_acceptance import wait_for
from nabedra.validation import schema_to_wire


def not_run(scenario, clause, detail):
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
    seed = batch(table, rows(0, category=5))
    fixture.select(seed['request_id'], {'participants': False})
    fixture.admit(seed)
    _, plans = fixture.reached(seed['request_id'], 'participants')
    assert len(plans) == 1, plans
    fixture.terminal(seed['request_id'], 1)
    group = plans[0]['group']['group_id']
    acknowledged = [point(table, 'insert', row=row) for row in rows(1, 2, 3, category=5)]
    for body in acknowledged:
        assert fixture.data(body) is None

    workers = fixture.cluster.workers
    owner = data_leader(fixture, group)
    paused = [worker for worker in WORKERS if worker != owner]
    for worker in paused:
        workers[worker].pause()
    try:
        # The old leader may still hold a valid read lease immediately after
        # the pause. Probe only after the registered election timeout expires.
        time.sleep(3.5)
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
    assert data_leader(fixture, group) != owner
    for body in acknowledged:
        row = body['payload']['row']
        assert fixture.data(point(table, 'get', key=row['id']), 'api2') == row
        assert exchange(fixture, body, 'api2') == (200, dict(request_id=body['request_id'], data=None))
    assert fixture.data(point(table, 'get', key=4), 'api2') is None
    assert fixture.data(point(table, 'insert', row=rows(5, category=10)[0]), 'api2') is None
    passed('O06', 'quorum-and-applied-authority',
           'Two stopped RF3 voters prevented both read and write success. A surviving quorum served the applied row and a fresh write after SIGKILL of the old Data leader.')
    passed('O06', 'acknowledged-write-survival',
           'The surviving processes returned all three acknowledged rows and the exact results for their original identities.')


def data_leader(fixture, group):
    cluster = fixture.cluster
    live = cluster.live()
    reports = []
    for worker in live:
        path = cluster.barrier_dir(worker) / 'probe-group-leader'
        path.write_text(group)
        try:
            reports.append(cluster.stats(worker)['test_group_leader'])
        finally:
            path.unlink(missing_ok=True)
    leaders = set(reports)
    assert len(leaders) == 1 and None not in leaders, reports
    return next(worker for worker in live if cluster.enrollments[worker]['worker_id'] in leaders)


def run_concurrently(fixture, requests):
    start = Barrier(len(requests))

    def call(api, body):
        start.wait(timeout=10)
        invoked = time.monotonic_ns()
        for attempt in range(5):
            response = exchange(fixture, body, api)
            status, reply = response
            if status != 429:
                return invoked, time.monotonic_ns(), body, response
            assert reply['request_id'] == body['request_id'], response
            assert reply['error']['code'] == 'overloaded' and reply['error']['outcome'] == 'not_applied', response
            time.sleep(.05 * (attempt + 1))
        raise AssertionError(f'pre-admission retries exhausted for {body}: {response}')

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


def assert_serial(history, initial, final, aborted_updates):
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
            if body['request_id'] in aborted_updates:
                # A stale optimistic preparation is an explicit, no-effect
                # abort. Its final state and exact replay are checked below.
                if check(mask | bit, encoded_state):
                    return True
            else:
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
    waves = [
        [('api1', point(table, 'update', key=1, values={'category': 71})),
         ('api2', point(table, 'update', key=1, values={'value': 72}))],
        [('api1', batch(table, rows(2, 3, category=2))),
         ('api2', batch(table, rows(3, 4, category=4)))],
        [('api1', point(table, 'insert', row=rows(3, category=6)[0])),
         ('api2', point(table, 'delete', key=5))],
        [('api1', point(table, 'get', key=3)),
         ('api2', point(table, 'get', key=1))],
    ]
    history = [call for wave in waves for call in run_concurrently(fixture, wave)]
    for _, _, body, (_, reply) in history:
        assert reply['request_id'] == body['request_id'], (body, reply)
    expected_row = initial_rows[0].copy()
    aborted_updates = set()
    for start, end, body, response in history[:2]:
        status, reply = response
        if status == 200:
            assert reply['data'] is True, response
            expected_row.update(body['payload']['values'])
            continue
        assert status == 409 and reply['error'] == {
            'code': 'conflict',
            'message': 'transaction preparation conflicted with an existing version',
            'outcome': 'not_applied',
        }, response
        assert any(other_start < end and start < other_end and other_status == 200
                   for other_start, other_end, _, (other_status, _) in history[:2]), history[:2]
        assert exchange(fixture, body, 'api2') == response
        aborted_updates.add(body['request_id'])
    final = {row['id']: row for row in fixture.data(fixture.query(table))}
    assert_serial(history, {1: initial_rows[0]}, final, aborted_updates)
    assert final[1] == expected_row, (final, expected_row)
    assert 3 in final and (2 in final) + (4 in final) <= 1 and 5 not in final, final
    if fixture.direct:
        after = fixture.cluster.all_stats()
        assert sum(((after[w]['direct'] or {}).get('mutations', 0)
                    - (before[w]['direct'] or {}).get('mutations', 0)) for w in WORKERS) > 0
    passed('O08', 'serial-history', 'Eight RF3 calls matched a real-time serial order. Every acknowledged field update survived. A preparation conflict was allowed only against an overlapping successful update, with no effect and exact identity replay.')
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
    if fixture.direct:
        original = point(table, 'insert', row=rows(10, category=9)[0])
        succeeded = exchange(fixture, original)
        changed = {**original, 'payload': {**original['payload'], 'row': rows(10, category=99)[0]}}
        rejected = exchange(fixture, changed)
        assert succeeded == (200, dict(request_id=original['request_id'], data=None)), succeeded
        assert rejected[0] == 409 and rejected[1]['error']['outcome'] == 'not_applied', rejected
        assert fixture.data(point(table, 'delete', key=10)) is True
        assert exchange(fixture, original, 'api2') == succeeded
        assert exchange(fixture, changed) == rejected
        assert fixture.data(point(table, 'get', key=10)) is None
    passed('O08', 'stable-results', 'Queued no-op and conflict replays kept the exact outcome after the row changed. With direct routing, the original commit result and changed-body refusal stayed stable after deletion.')

    name = 'schema_race'
    creates = []
    for field in ('left', 'right'):
        payload = dict(name=name, key='id', format='parquet', schema=schema_to_wire({'id': int, field: int}))
        creates.append(operation('table.create', payload))
    creation = run_concurrently(fixture, list(zip(('api1', 'api2'), creates, strict=True)))
    winners, losers = [], []
    for _, _, body, (status, reply) in creation:
        if status == 200:
            winners.append(reply['data'])
            assert reply['data']['schema'] == body['payload']['schema'], (body, reply)
        else:
            assert status == 409 and reply['error']['outcome'] == 'not_applied', (status, reply)
            losers.append(reply)
    assert len(winners) == len(losers) == 1, creation
    assert str(fixture.db.table(name).id) == winners[0]['id']
    d11_drop(fixture.cluster, fixture.cluster.api_urls['api1'], fixture.cluster.auth['data_token'],
             fixture.cluster.ca, writer_count=2)
    passed('O08', 'schema-fences', 'Concurrent distinct schema creates left one descriptor. The RF3 drop race refused stale writes and reopened the name under a fresh, empty table identity.')
    passed('O08', 'bounded-progress', 'The concurrent history and the RF3 drop/write race returned within the fixture deadlines.')
    if fixture.direct:
        verify_snapshot_retry(fixture, table)
        passed('O08', 'fresh-snapshot-retry',
               'An already planned queued upsert changed the head while a direct update was paused before proposal. The stale proposal lost and the same identity recomputed against the new head.')
    else:
        verify_queued_stale_plan(fixture, table)
        not_run('O08', 'fresh-snapshot-retry',
                'The queued coordinator terminally aborts a forced stale Prepare at attempt 1, '
                'and a retry returns that abort. No same-identity fresh snapshot path exists.')
    not_run('O08', 'storage-reservations',
            'The pool can report filesystem headroom and ENOSPC, but it has no owned disk-byte '
            'reservations for competing writes. A measured race requires that admission mechanism.')


def direct_proposal_paths(fixture, request_id):
    for worker in WORKERS:
        fixture.gate(worker, request_id, 'direct-proposing')
    return [fixture.cluster.barrier_dir(worker) / f'reached-{request_id}' for worker in WORKERS]


def release_direct_proposal(fixture, request_id):
    for worker in WORKERS:
        fixture.release_copy(worker, request_id)


def verify_snapshot_retry(fixture, table):
    queued = point(table, 'upsert', row=dict(id=1, category=88, value=89))
    fixture.select(queued['request_id'], {'participants': True})
    fixture.admit(queued, 'api2')
    fixture.reached(queued['request_id'], 'participants')
    direct = point(table, 'update', key=1, values={'value': 90})
    request_id = direct['request_id']
    reached = direct_proposal_paths(fixture, request_id)
    with ThreadPoolExecutor(max_workers=1) as pool:
        update = pool.submit(exchange, fixture, direct)
        try:
            def first_attempt(path):
                if path.read_text() == 'direct-proposing':
                    return path.with_name(f'attempts-{request_id}')

            attempts = wait_for(reached, first_attempt, timeout=15)
            assert not update.done()
            fixture.release(queued['request_id'], 'participants')
            fixture.terminal(queued['request_id'], None)
        finally:
            release_direct_proposal(fixture, request_id)
        result = update.result(timeout=35)
    assert result == (200, dict(request_id=request_id, data=True)), result
    assert attempts.read_text().splitlines() == ['direct-proposing', 'direct-proposing']
    assert fixture.data(point(table, 'get', key=1)) == dict(id=1, category=88, value=90)
    assert exchange(fixture, direct) == result


def verify_queued_stale_plan(fixture, table):
    stale = point(table, 'update', key=1, values={'value': 90})
    request = stale['request_id']
    fixture.select(request, {'participants': True, 'decision': False})
    before = fixture.data(point(table, 'get', key=1))
    fixture.admit(stale)
    _, participants = fixture.reached(request, 'participants')
    assert len(participants) == 1, participants
    winner = dict(before, category=88, value=89)
    try:
        assert fixture.data(point(table, 'upsert', row=winner), 'api2') is None
    finally:
        fixture.release(request, 'participants')
    _, decision = fixture.reached(request, 'decision')
    assert decision['txn'] == {'request_id': request, 'attempt': 1}, decision
    assert decision['request']['request_id'] == request

    def terminal_failure():
        status, reply = fixture.receipt(request)
        if status == 200 and reply['state'] in ('failed', 'succeeded'):
            assert reply['state'] == 'failed' and reply['result'] is None, reply
            return reply
        assert status in (200, 503), (status, reply)
        return None

    terminal = wait_until(terminal_failure, 'queued stale Prepare terminal abort', timeout=30)
    error = {'code': 'conflict', 'message': 'transaction preparation conflicted with an existing version',
             'outcome': 'not_applied'}
    assert terminal['error'] == error, terminal
    assert exchange(fixture, stale, 'api2') == (409, {'request_id': request, 'error': error})
    assert fixture.data(point(table, 'get', key=1)) == winner
    evidence('O08', 'queued-stale-prepare-terminal-abort', request_id=request,
             snapshot=participants[0]['read_ts'], attempted_version=before, resulting_row=winner,
             decision=decision)


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
        assert seal['txn'] == dict(request_id=request, attempt=1), seal
        assert seal['sealed_at']['index'] > proof['position']['index'], (seal, proof)
        assert not any(path.exists() for path in fixture.paths(request, 'commit-stamp'))
        fixture.release(request, stage)
    _, stamp = fixture.reached(request, 'commit-stamp')
    assert stamp['allocated_at']['index'] > 0, stamp
    assert stamp['commit_ts'] > max(old_stamp, *(p['read_ts'] for p in plans)), (stamp, plans, old_stamp)
    _, decision = fixture.reached(request, 'commit-reply')
    assert decision['decision'] == {'commit': {'commit_ts': stamp['commit_ts']}}, decision
    passed('O09', 'commit-after-prepare', 'Both RF3 participants applied their pages and sealed the manifest before allocation. The exact decision used the later timestamp.')

    expected = sorted(before_rows + inserts, key=lambda row: row['id'])
    probes = [
        (point(table, 'get', key=-9), inserts[0]),
        (fixture.query(table), expected),
        (fixture.query(table, 7), inserts),
    ]
    # Hold every reader before it helps, so each proves a genuinely obstructed
    # read even when another reader would otherwise resolve the same intent.
    for body, _ in probes:
        probe = body['request_id']
        fixture.select(probe, {'snapshot': False, 'blocked': True})
        fixture.admit(body, 'api2')
        _, probe_stamp = fixture.reached(probe, 'snapshot')
        assert probe_stamp > stamp['commit_ts'], (probe_stamp, stamp)
        _, intent = fixture.reached(probe, 'blocked')
        assert intent == dict(txn=decision['txn'], manifest=decision['manifest']), intent
    for body, result in probes[1:]:
        fixture.release(body['request_id'], 'blocked')
        fixture.terminal(body['request_id'], result)
    body, result = probes[0]
    fixture.release(body['request_id'], 'blocked')
    fixture.terminal(body['request_id'], result)
    fixture.release(old_id, 'snapshot')
    fixture.terminal(old_id, before_rows)
    fixture.release(request, 'commit-reply')
    fixture.terminal(request, len(inserts))
    passed('O09', 'ambiguous-intent-reads',
           'A missing point, base scan and index scan found the unresolved COMMIT. Helping recovered the expected rows, while an earlier pin kept its original result.')

    check_allocator(fixture, table, stamp['commit_ts'])


def timestamp_probe(fixture, worker):
    signal = fixture.cluster.barrier_dir(worker) / 'probe-timestamp'
    signal.touch()
    try:
        observed = fixture.cluster.stats(worker)['test_timestamp']
    finally:
        signal.unlink(missing_ok=True)
    assert observed['window'] is not None or 'error' in observed, observed
    return observed


def allocator(fixture, workers):
    deadline = time.monotonic() + 50
    while time.monotonic() < deadline:
        for worker in workers:
            observation = timestamp_probe(fixture, worker)
            if observation['window'] is not None:
                window = observation['window']
                assert window['last'] - window['first'] == 3, observation
                return worker, window, observation['wall_seconds']
        time.sleep(.2)
    raise AssertionError(f'no Timestamp allocator in {workers}')


def snapshot(fixture, table, expected):
    body = fixture.query(table)
    rid = body['request_id']
    fixture.select(rid, {'snapshot': False})
    fixture.admit(body, 'api2')
    _, stamp = fixture.reached(rid, 'snapshot')
    fixture.terminal(rid, expected)
    return stamp


def check_allocator(fixture, table, commit_ts):
    cluster = fixture.cluster
    cluster.wait_direct_active()
    expected = fixture.data(fixture.query(table))
    clock_jumps = 'LD_PRELOAD' in cluster.worker_env
    previous, first, wall = allocator(fixture, WORKERS)
    assert abs(wall - time.time()) < 5, wall
    assert first['first'] > commit_ts, (first, commit_ts)
    contiguous = timestamp_probe(fixture, previous)['window']
    assert contiguous is not None and contiguous['first'] > first['last'], (first, contiguous)

    intervening = snapshot(fixture, table, expected)
    assert intervening > contiguous['last'], (intervening, contiguous)
    _, after_queued, _ = allocator(fixture, (previous,))
    assert after_queued['first'] > intervening, (after_queued, intervening)
    for worker in WORKERS:
        (cluster.barrier_dir(worker) / 'clock-offset-seconds').write_text('-60')
    survivors = [worker for worker in WORKERS if worker != previous]
    cluster.workers[previous].pause()
    try:
        replacement, new, backward = allocator(fixture, survivors)
        assert replacement != previous and new['first'] > after_queued['last'], (replacement, new)
        if clock_jumps:
            assert abs(backward - (time.time() - 60)) < 5, backward
    finally:
        cluster.workers[previous].resume()
    resumed = timestamp_probe(fixture, previous)
    assert resumed['window'] is None and 'Elapsed' not in resumed['error'], resumed
    for worker in WORKERS:
        (cluster.barrier_dir(worker) / 'clock-offset-seconds').write_text('60')
    _, latest, forward = allocator(fixture, survivors)
    if clock_jumps:
        assert abs(forward - (time.time() + 60)) < 5, forward
    assert latest['first'] > new['last'], (latest, new)
    for worker in WORKERS:
        (cluster.barrier_dir(worker) / 'clock-offset-seconds').unlink()

    if clock_jumps:
        passed('O09', 'timestamp-order',
               'RF3 Timestamp re-election during a simulated backward wall-clock jump preserved ordering. The resumed leader refused to issue, and allocation advanced through a forward jump.')
    else:
        not_run('O09', 'timestamp-order', 'Timestamp failover and old-leader resume passed. Wall-clock jumps require Linux and a C compiler.')

    if fixture.direct:
        verify_direct_scan(fixture, table, expected, latest['last'])
    passed('O09', 'no-backdated-allocation',
           'Four-value AllocateDirect replies advanced past completed allocations and a queued scan pin. The new leader advanced past the old window.')


def admit_after_election(fixture, body):
    for attempt in range(6):
        status, reply = fixture.api('api2', REQUESTS, body)
        assert reply['request_id'] == body['request_id'], (status, reply)
        if status == 202:
            assert reply['run_id'] == fixture.cluster.run_id, reply
            return
        assert status in (429, 503) and reply['error']['outcome'] == 'not_applied', (status, reply)
        time.sleep(.1 * (attempt + 1))
    raise AssertionError(f'request admission did not resume after Timestamp election: {status}, {reply}')


def verify_direct_scan(fixture, table, prior_rows, completed_ts):
    row = rows(12, category=7)[0]
    body = point(table, 'insert', row=row)
    request_id = body['request_id']
    reached = direct_proposal_paths(fixture, request_id)
    fixture.select(request_id, {'direct-timestamp': False})
    scan = fixture.query(table)
    scan_id = scan['request_id']
    fixture.select(scan_id, {'snapshot': False})
    with ThreadPoolExecutor(max_workers=1) as pool:
        write = pool.submit(fixture.data, body)
        try:
            wait_for(reached, lambda path: path.read_text() == 'direct-proposing', timeout=15)
            assert not write.done()
            admit_after_election(fixture, scan)
            _, scan_ts = fixture.reached(scan_id, 'snapshot')
        finally:
            release_direct_proposal(fixture, request_id)
        assert write.result(timeout=35) is None
    _, used = fixture.reached(request_id, 'direct-timestamp')
    assert completed_ts < used < scan_ts, (completed_ts, used, scan_ts)
    fixture.terminal(scan_id, prior_rows + [row])


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
    decided = batch(table, rows(4, 5, category=5))
    decision_id = decided['request_id']
    fixture.select(decision_id, {'commit-reply': True, 'decision': False})
    fixture.admit(decided, 'api2')
    _, durable = fixture.reached(decision_id, 'commit-reply')
    assert durable['request']['request_id'] == decision_id and 'commit' in durable['decision'], durable
    assert not any(path.exists() for path in fixture.paths(decision_id, 'decision'))
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
    if fixture.direct:
        assert exchange(fixture, written, 'api2') == exact
    else:
        code, result = fixture.api('api2', f"{REQUESTS}/{old_run}/{written['request_id']}")
        assert code == 200 and result['state'] == 'succeeded' and result['result'] is None, (code, result)
    status, abandoned = fixture.api('api2', f"{REQUESTS}/{old_run}/{held['request_id']}")
    assert (status, abandoned) == (404, {
        'request_id': held['request_id'],
        'error': {
            'code': 'not_found',
            'message': 'old-run request abandoned; check durable mutation decision',
            'outcome': 'unknown',
        },
    }), (status, abandoned)
    expected = rows(1, 2, 3, 4, 5, category=5)
    assert fixture.data(fixture.query(table), 'api2') == expected
    assert fixture.data(fixture.query(table, 5), 'api2') == expected
    code, replay = fixture.api('api2', f'{REQUESTS}/{old_run}/{decision_id}')
    assert (code, replay) == (200, dict(
        request_id=decision_id, run_id=old_run, state='succeeded', result=2, error=None,
    )), (code, replay)
    passed('O11', 'committed-state-replay',
           'SIGKILL of all RF3 workers and gateways preserved acknowledged mutations and their exact results. A durable decision with a held reply was recovered, and a pre-Prepare request was abandoned.')


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
        worker_env = {}
        if args.scenario == 'O09' and sys.platform.startswith('linux') and shutil.which('cc'):
            shim = Path(root) / 'wall-clock.so'
            subprocess.run(['cc', '-shared', '-fPIC', '-O2', '-o', str(shim),
                            str(Path(__file__).with_name('oltp_test_wall_clock.c')), '-ldl'],
                           check=True, timeout=20)
            worker_env['LD_PRELOAD'] = str(shim)
        cluster = Rf3Cluster(args.binary, root, test_executable=args.test_executable,
                             direct=dict.fromkeys(WORKERS, direct), api_direct=(direct, api2_direct),
                             mounts=mounts, worker_env=worker_env)
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

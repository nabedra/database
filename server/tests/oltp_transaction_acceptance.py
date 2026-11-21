#!/usr/bin/env python3.11
"""Exercise queued transactions through registered RF3 workers and real gateways."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

from oltp_direct_acceptance import Database, admin_owner, col, dump
from oltp_direct_cluster import DATABASE, WORKERS, Rf3Cluster, call, operation, uid, write_json
from oltp_terminal_copy_acceptance import read_json, wait_for

OPERATIONS = '/v1/operations'
REQUESTS = '/v1/requests'


def passed(scenario, assertion, detail):
    print('NABEDRA_OLTP_ASSERT ' + json.dumps({
        'scenario': scenario, 'assertion': assertion, 'status': 'passed', 'detail': detail,
    }), flush=True)


class Transactions:
    def __init__(self, cluster, direct):
        self.cluster = cluster
        self.direct = direct
        self.stages = {}
        self.gates = {worker: {} for worker in WORKERS}
        self.inspect = []

    def start(self):
        self.cluster.start()
        if self.direct:
            self.cluster.wait_direct_active()
        self.db = Database(url=self.cluster.api_urls['api1'], token=self.cluster.auth['data_token'],
                           database=DATABASE, ca_file=str(self.cluster.ca), timeout=30)

    def api(self, name, path, body=None, *, token=None, timeout=30):
        return call(self.cluster.api_urls[name], path, body, ca=self.cluster.ca,
                    token=token or self.cluster.auth['data_token'], timeout=timeout)

    def admit(self, body, api='api1'):
        status, response = self.api(api, REQUESTS, body)
        assert status == 202 and response['run_id'] == self.cluster.run_id, (status, response)
        assert response['request_id'] == body['request_id'], response

    def receipt(self, request_id, api='api2', **kwargs):
        return self.api(api, f'{REQUESTS}/{self.cluster.run_id}/{request_id}', **kwargs)

    def pending(self, request_id):
        status, response = self.receipt(request_id)
        assert status == 200 and response['state'] in ('running', 'committing', 'reconciling'), (status, response)
        assert response['result'] is None and response['error'] is None, response
        return response

    def terminal(self, request_id, result, api='api2', timeout=45):
        deadline = time.monotonic() + timeout
        while True:
            status, response = self.receipt(request_id, api, timeout=12)
            if status == 200 and response['state'] == 'succeeded':
                assert response == dict(request_id=request_id, run_id=self.cluster.run_id,
                                        state='succeeded', result=result, error=None), response
                return response
            assert status in (200, 404, 503) and response.get('state') != 'failed', (status, response)
            assert time.monotonic() < deadline, (request_id, status, response)
            time.sleep(.05)

    def data(self, body, api='api1'):
        status, response = self.api(api, OPERATIONS, body)
        assert status == 200 and response['request_id'] == body['request_id'], (status, response)
        return response['data']

    def select(self, request_id, stages):
        self.stages.setdefault(request_id, {}).update(stages)
        for worker in WORKERS:
            write_json(self.cluster.barrier_dir(worker) / 'transaction-gates.json', self.stages)

    def paths(self, request_id, stage, workers=WORKERS):
        return [self.cluster.barrier_dir(worker) / f'transaction-{request_id}-{stage}.json'
                for worker in workers]

    def reached(self, request_id, stage, workers=WORKERS, timeout=15):
        def checkpoint(path):
            value = read_json(path)
            return path.parent.name, value

        return wait_for(self.paths(request_id, stage, workers), checkpoint, timeout=timeout)

    def release(self, request_id, stage):
        for path in self.paths(request_id, stage):
            path.with_name(f'release-{path.stem}').touch()

    def gate(self, worker, request_id, stage):
        self.gates[worker][request_id] = stage
        write_json(self.cluster.barrier_dir(worker) / 'gates.json', self.gates[worker])

    def release_copy(self, worker, request_id):
        (self.cluster.barrier_dir(worker) / f'release-{request_id}').touch()

    def watch_record(self, request_id):
        self.inspect.append(request_id)
        for worker in WORKERS:
            path = self.cluster.barrier_dir(worker)
            write_json(path / 'inspect.json', self.inspect)

    def records(self, request_id):
        result = {}
        for name in self.cluster.live():
            self.cluster.stats(name)
            result[name] = read_json(self.cluster.barrier_dir(name) / f'record-{request_id}.json')
        return result

    def query(self, table, category=None):
        query = table.query() if category is None else table.where(col('category') == category)
        return operation('query.collect', {'table_id': table.id, 'query': query._to_wire()})

    def make_index(self, table):
        index_id, build_id = uid(), uid()
        created = admin_owner(self.cluster, 'storage.index.create', {
            'table_id': table.id, 'index_id': index_id, 'name': 'by_category',
            'columns': ['category'], 'covering': ['value'],
        }, build_id)
        deadline = time.monotonic() + 60
        while created['state'] != 'ready':
            assert created['state'] in ('pending', 'building', 'backfilling', 'validating'), created
            assert time.monotonic() < deadline, created
            created = admin_owner(self.cluster, 'storage.index.status', {
                'table_id': table.id, 'index_id': index_id, 'build_id': build_id, 'attempt': 1,
            })
            if created['state'] != 'ready':
                time.sleep(.1)
        assert 'by_category' in table.where(category=7).explain()

    def release_all(self):
        for request_id, stages in self.stages.items():
            for stage, hold in stages.items():
                if hold:
                    self.release(request_id, stage)
        for worker, gates in self.gates.items():
            for request_id in gates:
                self.release_copy(worker, request_id)


def rows(*keys, category):
    return [dict(id=key, category=category, value=key * 2) for key in keys]


def batch(table, items):
    return operation('table.insert_many', {'table_id': table.id, 'rows': items})


def assert_binding(statement, facade, body):
    request = statement['request']
    digest = hashlib.sha256(json.dumps(body, separators=(',', ':')).encode()).hexdigest()
    assert request['request_id'] == body['request_id'] and request['digest'] == digest, statement
    assert request['run_id'] == facade.cluster.run_id, statement
    assert statement['txn'] == dict(request_id=body['request_id'], attempt=1), statement
    assert statement['decided_at']['index'] > 0, statement
    return request


def run_batch(facade, table, groups, items, category, pool, *, race=False):
    """Delay the acknowledgement at each transaction boundary in the real services."""
    body = batch(table, items)
    request_id = body['request_id']
    facade.select(request_id, {'participants': True, 'commit-stamp': True,
                               'commit-reply': True, 'decision': True})
    old_snapshots = []
    for query in [facade.query(table), facade.query(table, category)]:
        query_id = query['request_id']
        facade.select(query_id, {'snapshot': True})
        facade.admit(query, 'api2')
        facade.reached(query_id, 'snapshot')
        old_snapshots.append(query_id)
    if race:
        batch_reply = pool.submit(facade.api, 'api1', OPERATIONS, body)
    else:
        facade.admit(body)
    _, participants = facade.reached(request_id, 'participants')
    actual = [part['group'] for part in participants]
    assert {item['group_id'] for item in actual} == {item['group_id'] for item in groups}, actual
    assert len(actual) == len(groups), actual
    stages = {}
    for part in participants:
        group = part['group']['group_id']
        for page in range(part['page_count']):
            stages[f'prepare-{group}-{page}'] = True
        stages[f'seal-{group}'] = True
    resolutions = [f"resolve-{group['group_id']}" for group in actual]
    facade.select(request_id, stages | dict.fromkeys(resolutions, True))

    old_rows = facade.data(facade.query(table))
    assert all(row['id'] not in {item['id'] for item in items} for row in old_rows)
    assert facade.data(facade.query(table, category)) == []
    facade.release(request_id, 'participants')

    last_seal = next(reversed(stages))
    for stage in stages:
        _, proof = facade.reached(request_id, stage)
        assert proof, (stage, proof)
        assert not any(path.exists() for path in facade.paths(request_id, 'commit-reply'))
        facade.pending(request_id)
        if stage != last_seal:
            facade.release(request_id, stage)

    for query_id, expected in zip(old_snapshots, [old_rows, []], strict=True):
        facade.release(query_id, 'snapshot')
        facade.terminal(query_id, expected)

    older = []
    for query, expected in [(facade.query(table), old_rows), (facade.query(table, category), [])]:
        query_id = query['request_id']
        facade.select(query_id, {'snapshot': True, 'blocked': race})
        facade.admit(query, 'api2')
        _, read_ts = facade.reached(query_id, 'snapshot')
        older.append((query_id, expected, read_ts))

    # D08. The point writer reaches the queued planner while the batch still holds its intent.
    if race:
        key = items[0]['id']
        point = operation('table.insert', {'table_id': table.id, 'row': rows(key, category=99)[0]})
        point_id = point['request_id']
        facade.select(point_id, {'blocked': True})
        before = facade.cluster.all_stats()
        point_reply = pool.submit(facade.api, 'api1', OPERATIONS, point)
        _, intent = facade.reached(point_id, 'blocked')
        assert intent['txn']['request_id'] == request_id, intent
        after = facade.cluster.all_stats()
        if facade.direct:
            def intent_fallbacks(stats):
                return sum((worker.get('direct') or {}).get('fallbacks', {}).get('intent', 0)
                           for worker in stats.values())

            assert intent_fallbacks(after) > intent_fallbacks(before)
        assert not point_reply.done()

    facade.release(request_id, last_seal)
    _, stamp = facade.reached(request_id, 'commit-stamp')
    assert stamp['commit_ts'] > max(plan['read_ts'] for plan in participants)
    assert all(read_ts < stamp['commit_ts'] for _, _, read_ts in older)
    facade.release(request_id, 'commit-stamp')
    _, durable = facade.reached(request_id, 'commit-reply')
    assert_binding(durable, facade, body)
    assert durable['decision'] == {'commit': {'commit_ts': stamp['commit_ts']}}
    assert not any(path.exists() for path in facade.paths(request_id, 'decision'))
    facade.pending(request_id)
    facade.release(request_id, 'commit-reply')
    _, decision = facade.reached(request_id, 'decision')
    assert decision == durable
    facade.release(request_id, 'decision')
    _, first = facade.reached(request_id, resolutions[0])
    assert first['decision'] == decision['decision']
    assert first['completed_at']['index'] > 0
    if race:
        assert not any(path.exists() for path in facade.paths(request_id, resolutions[1]))
    newer = sorted(old_rows + items, key=lambda row: row['id'])
    newer_reads = []
    for query, expected in [(facade.query(table), newer), (facade.query(table, category), items)]:
        query_id = query['request_id']
        facade.select(query_id, {'snapshot': False, 'blocked': race})
        facade.admit(query, 'api2')
        _, read_ts = facade.reached(query_id, 'snapshot')
        assert read_ts >= stamp['commit_ts']
        if race:
            _, intent = facade.reached(query_id, 'blocked')
            assert intent == {'txn': decision['txn'], 'manifest': decision['manifest']}, intent
        newer_reads.append((query_id, expected))
    for query_id, _, _ in older:
        facade.release(query_id, 'snapshot')
        if race:
            _, intent = facade.reached(query_id, 'blocked')
            assert intent == {'txn': decision['txn'], 'manifest': decision['manifest']}, intent
    for query_id, _ in newer_reads:
        facade.release(query_id, 'blocked')
    for query_id, _, _ in older:
        facade.release(query_id, 'blocked')
    if race:
        # The scans saw the intent while the original coordinator was held at the first participant.
        _, last = facade.reached(request_id, resolutions[1])
        assert last['decision'] == decision['decision']
        facade.pending(request_id)
        facade.release(request_id, resolutions[1])
    for query_id, expected in newer_reads:
        facade.terminal(query_id, expected)
    for query_id, expected, _ in older:
        facade.terminal(query_id, expected)
    facade.release(request_id, resolutions[0])
    for stage in resolutions[1:]:
        _, evidence = facade.reached(request_id, stage)
        assert evidence['txn'] == decision['txn']
        assert evidence['decision'] == decision['decision']
    original = facade.terminal(request_id, len(items))
    facade.admit(body, 'api2')
    assert facade.terminal(request_id, len(items), api='api1') == original
    for stage in resolutions:
        _, evidence = facade.reached(request_id, stage)
        assert evidence['request'] == decision['request'], evidence
        assert evidence['manifest'] == decision['manifest'], evidence
    if race:
        assert batch_reply.result(timeout=35) == (200, {'request_id': request_id, 'data': len(items)})
        repeated = []
        for path in facade.paths(request_id, resolutions[1]):
            replay = path.with_name(f'{path.stem}-repeated.jsonl')
            if replay.exists():
                repeated.extend(json.loads(line) for line in replay.read_text().splitlines())
        assert repeated and all(item == evidence for item in repeated), repeated
        facade.release(point_id, 'blocked')
        status, response = point_reply.result(timeout=35)
        assert status == 409 and response['error']['code'] == 'conflict', (status, response)
        assert response['error']['outcome'] == 'not_applied', response
    assert facade.data(facade.query(table)) == newer
    assert facade.data(facade.query(table, category)) == items


def o07(facade):
    table = facade.db.create_table('atomic_batches', {'id': int, 'category': int, 'value': int}, key='id')
    seed = batch(table, rows(-5, 5, category=0))
    facade.select(seed['request_id'], {'participants': False})
    facade.admit(seed)
    _, source = facade.reached(seed['request_id'], 'participants')
    assert len(source) == 1, source
    facade.terminal(seed['request_id'], 2)

    facade.make_index(table)
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            run_batch(facade, table, [source[0]['group']], rows(10, 11, category=7), 7, pool)
        finally:
            facade.release_all()

    split_id = uid()
    for _ in range(16):
        split = admin_owner(facade.cluster, 'storage.shard.split', {
            'source': source[0]['group'], 'boundary': 0, 'page_bytes': 1 << 20,
        }, split_id)
        if split['state'] == 'published':
            break
        assert split['state'] == 'importing', split
    assert split['state'] == 'published' and len(split['children']) == 2, split

    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            run_batch(facade, table, split['children'], rows(-20, 20, category=8), 8, pool, race=True)
        finally:
            facade.release_all()
    passed('O07', 'private-before-commit', 'Base and indexed snapshots excluded prepared rows and index entries before COMMIT.')
    passed('O07', 'atomic-snapshots', 'RF3 single-range and split batches, including a paused finalizer.')
    passed('O07', 'single-decision-and-application', 'Durable participant evidence bound the original decision. Same-body receipt replay returned the original result.')

    expected = rows(-20, category=8) + rows(-5, 5, category=0) + rows(10, 11, category=7) + rows(20, category=8)
    for new_key, duplicate in [(-21, 20), (21, -20)]:
        conflicting = batch(table, rows(new_key, duplicate, category=99))
        first = facade.api('api1', OPERATIONS, conflicting)
        repeated = facade.api('api2', OPERATIONS, conflicting)
        assert first == repeated, (first, repeated)
        status, response = first
        assert status == 409 and response['error']['code'] == 'conflict', first
        assert response['error']['outcome'] == 'not_applied', first
        assert facade.data(facade.query(table)) == expected
        assert facade.data(facade.query(table, 99)) == []
    passed('O07', 'duplicate-key-aborts-batch', 'Both cross-range conflict directions left rows and indexes intact.')


def o10(facade):
    table = facade.db.create_table('decision_recovery', {'id': int, 'category': int, 'value': int}, key='id')
    table.insert({'id': 1, 'category': 0, 'value': 10})
    update = operation('table.update', {'table_id': table.id, 'key': 1, 'values': {'value': 20}})
    request_id = update['request_id']
    facade.watch_record(request_id)
    facade.select(request_id, {'participants': True, 'decision': False, 'terminal-copy-lost': False})
    facade.admit(update)
    owner, _ = facade.reached(request_id, 'participants')
    target = next(name for name in WORKERS if name != owner)
    facade.gate(target, request_id, 'before-terminal-copy')
    target_path = facade.cluster.barrier_dir(target)
    (target_path / f'lose-terminal-copy-{request_id}').touch()
    facade.release(request_id, 'participants')
    wait_for([target_path / f'reached-{request_id}'],
             lambda path: path.read_text() == 'before-terminal-copy')
    before = facade.records(request_id)
    original = before[owner]
    assert original['state'] == 'succeeded' and not original['copies_complete'], before
    assert before[target]['state'] == 'running' and before[target]['terminal'] is None, before
    assert all(record['context'] == original['context'] and record['accepted']
               for record in before.values()), before
    _, decision = facade.reached(request_id, 'decision')
    binding = assert_binding(decision, facade, update)
    assert binding == original['terminal']['request'], original
    assert binding['accepted_context'] == original['context_hash'], original
    status, pending = facade.receipt(request_id)
    assert status == 200 and pending == dict(request_id=request_id, run_id=facade.cluster.run_id,
                                            state='committing', result=None, error=None), pending
    facade.release_copy(target, request_id)
    facade.reached(request_id, 'terminal-copy-lost', [target])
    first_terminal = facade.terminal(request_id, True)
    assert facade.terminal(request_id, True, api='api1') == first_terminal
    after = facade.records(request_id)
    assert after[owner]['copies_complete'] is True
    for record in after.values():
        assert record['state'] == 'succeeded' and record['terminal'] == original['terminal'], after
        assert record['version'] == original['version'] and record['context'] == original['context'], after

    body = batch(table, rows(2, 3, category=6))
    request_id = body['request_id']
    facade.watch_record(request_id)
    facade.select(request_id, {'commit-reply': True, 'decision': True,
                               'status-inspect-unavailable': False})
    facade.admit(body)
    _, durable = facade.reached(request_id, 'commit-reply')
    binding = assert_binding(durable, facade, body)
    assert 'commit' in durable['decision']
    assert not any(path.exists() for path in facade.paths(request_id, 'decision'))

    # Fail an actual quorum Status inspection while a reader tries to help the intent.
    faults = [facade.cluster.barrier_dir(worker) / f'lose-status-inspection-{request_id}' for worker in WORKERS]
    for fault in faults:
        fault.touch()
    try:
        query = facade.query(table)
        facade.select(query['request_id'], {'blocked': False})
        facade.admit(query, 'api2')
        _, intent = facade.reached(query['request_id'], 'blocked')
        assert intent['txn'] == durable['txn'], intent
        facade.reached(request_id, 'status-inspect-unavailable')
        facade.pending(query['request_id'])
        facade.release(request_id, 'commit-reply')
        owner, decided = facade.reached(request_id, 'decision')
        assert decided == durable, (decided, durable)
        accepted = facade.records(request_id)
        assert all(record['accepted'] and record['body_hash'] == binding['digest']
                   and record['context_hash'] == binding['accepted_context']
                   for record in accepted.values()), accepted
        facade.cluster.workers[owner].kill()
    finally:
        for fault in faults:
            fault.unlink(missing_ok=True)
    terminal = facade.terminal(request_id, 2, timeout=120)
    assert facade.terminal(request_id, 2, api='api1') == terminal
    facade.admit(body, 'api2')
    assert facade.terminal(request_id, 2) == terminal
    survivors = facade.records(request_id)
    for name, record in survivors.items():
        assert record['terminal']['request'] == binding, (name, record)
        assert record['context'] == accepted[name]['context'], (name, record)
    observed_decisions = [
        read_json(facade.cluster.barrier_dir(worker) / f'resolved-{request_id}.json')
        for worker in facade.cluster.live()
        if (facade.cluster.barrier_dir(worker) / f'resolved-{request_id}.json').exists()
    ]
    assert observed_decisions and all(item == durable for item in observed_decisions), observed_decisions
    changed = {**body, 'payload': {**body['payload'], 'rows': rows(2, 4, category=6)}}
    status, conflict = facade.api('api2', REQUESTS, changed)
    assert status == 409 and conflict['error']['code'] == 'conflict', conflict
    assert facade.data(facade.query(table), 'api2') == [
        {'id': 1, 'category': 0, 'value': 20}, *rows(2, 3, category=6),
    ]
    passed('O10', 'same-decision-and-result', 'Two authorized gateways recovered the same RF3 COMMIT, request digest and exact result after the coordinator stopped.')
    passed('O10', 'uncertainty-stays-unknown', 'An injected Status inspection failure left the read pending while the COMMIT response was delayed.')
    passed('O10', 'immutable-terminal-and-context', 'A receipt raced a lost terminal copy. Replicas converged on the original terminal version and authorization context.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=('O07', 'O10'), required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--test-executable', type=Path, required=True)
    args = parser.parse_args()
    direct = os.environ.get('NABEDRA_TEST_DIRECT_OLTP') != 'off'
    with tempfile.TemporaryDirectory(prefix=f'nabedra-{args.scenario.lower()}-') as root:
        cluster = Rf3Cluster(args.binary, root, test_executable=args.test_executable,
                             direct=dict.fromkeys(WORKERS, direct), api_direct=(direct, direct))
        facade = Transactions(cluster, direct)
        try:
            facade.start()
            {'O07': o07, 'O10': o10}[args.scenario](facade)
        except BaseException:
            dump(cluster)
            raise
        finally:
            facade.release_all()
            cluster.close()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3.11
"""Direct point protocol scenarios D01-D12 on real processes (T9).

Topologies (see `oltp_direct_cluster.py`): RF3 = three registered workers plus enrolled
API gateways, each a separate host process, mTLS between them; RF1 = `nabedra-db serve`.

  --suite rf1        D01 (RF1, both ORMs, direct on/off) and the U01 program comparison
  --suite cluster    D01 (RF3, both ORMs, API and worker gateways, on/off), D03, D11,
                     D05 (SIGSTOP), D10 (gateway killed mid-stream), D09 (kill all, cold start)
  --suite barriers   needs --test-executable: D12 (leader budget 4), D02 (gateway killed
                     after the leader applied), D04 after quorum (leader killed)
  --suite quorum     needs --test-executable: D04 before quorum (leader held before it proposes,
                     both followers SIGSTOPped, entry appended, leader killed)

Each scenario prints one JSON line `{"scenario": ..., "status": "passed", ...}` with the
observations it asserted, and the run writes them all to `--output/results.json`.
The TypeScript ORM must be built first (`npm ci && npm run build` in orm/typescript).
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from uuid import uuid4

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1] / 'orm/python/src')]
from oltp_direct_cluster import (DATABASE, EXECUTION, WORKERS, Http, Rf1Serve, Rf3Cluster, call, operation,
                                 uid, write_json)
from nabedra import Database, NotFoundError, col

ROOT = HERE.parents[1]
ORM_TS = ROOT / 'orm/typescript/dist/index.js'
QUEUE_FIELDS = ('accepted_records', 'queued_records', 'running_records', 'reconciling_records', 'terminal_records')
RESULTS = []


def report(scenario, status='passed', **observed):
    record = dict(scenario=scenario, status=status, **observed)
    RESULTS.append(record)
    print(json.dumps(record, sort_keys=True, default=str), flush=True)


# The D01 program -------------------------------------------------------------------------
def I(value):
    return {'$i': str(value)}


def F(value):
    if value == 0:
        return {'$f': '-0' if math.copysign(1.0, value) < 0 else '0'}
    return {'$f': repr(value)}


def row(key, name, score, active):
    return {'id': I(key), 'name': name, 'score': F(score), 'active': active}


LOW, HIGH = -(1 << 63), (1 << 63) - 1
SCHEMA = {'id': int, 'name': str, 'score': float, 'active': bool}
PROGRAM = dict(sections=[
    dict(name='direct', steps=[
        ['insert', row(1, 'one', 0.5, True)],
        ['insert', row(LOW, '', -0.0, False)],
        ['insert', row(HIGH, '\U0001d11e é ü', 1.5, True)],
        ['get', I(1)], ['get', I(LOW)], ['get', I(HIGH)], ['get', I(42)],
        ['insert', row(1, 'duplicate', 0.5, True)],
        ['update', I(1), {'score': F(2.5)}],
        ['update', I(42), {'score': F(2.5)}],
        ['upsert', row(1, 'one-upserted', 0.5, False)],
        ['upsert', row(2, 'two', 3.0, True)],
        ['delete', I(2)],
        ['delete', I(2)],
        ['insert_many', [row(10, 'a', 1.0, True), row(11, 'b', 1.0, False), row(12, 'c', -1.0, True)]],
        ['insert_many', [row(20, 'd', 1.0, True), row(10, 'a', 1.0, True)]],
        ['get', I(1)], ['get', I(2)], ['get', I(10)], ['get', I(12)], ['get', I(20)],
    ]),
    # Requests that may legitimately take the queued path or fail in the client.
    dict(name='mixed', steps=[
        ['insert_many', [row(30, 'e', 1.0, True), row(30, 'f', 1.0, True)]],
        ['insert_many', []],
        ['update', I(1), {'missing_column': I(1)}],
        ['insert', {'id': I(50), 'name': I(5), 'score': F(0.0), 'active': True}],
        ['get', I(30)], ['get', I(50)],
    ]),
])
# Writing mutations in the `direct` section: 3 inserts, 1 update, 2 upserts, 1 delete and
# 1 insert_many. Conflicts, misses and gets write nothing.
DIRECT_ENTRIES = 8


def run_program(orm, url, table, *, token, ca=None, snapshot):
    program = Path(tempfile.mkstemp(prefix='d01-program-', suffix='.json')[1])
    program.write_text(json.dumps(PROGRAM))
    common = ['--url', url, '--program', str(program), '--table', table]
    if ca:
        common += ['--ca-file', str(ca)]
    if orm == 'python':
        command = [sys.executable, str(HERE / 'oltp_direct_program.py'), *common]
    else:
        command = ['node', str(HERE / 'oltp_direct_program.mjs'), '--orm', ORM_TS.as_uri(), *common]
    env = dict(os.environ, NABEDRA_TEST_TOKEN=token, PYTHONDONTWRITEBYTECODE='1')
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, env=env)
    try:
        def line():
            text = process.stdout.readline()
            if not text:
                raise AssertionError(f'{orm} driver exited: {process.stderr.read()[-4000:]}')
            return json.loads(text)
        assert line() == {'ready': True}
        snapshots = [snapshot()]
        for section in PROGRAM['sections']:
            process.stdin.write('\n')
            process.stdin.flush()
            assert line() == {'checkpoint': section['name']}
            snapshots.append(snapshot())
        results = line()['results']
        assert process.wait(timeout=30) == 0
        return results, snapshots
    finally:
        if process.poll() is None:
            process.kill()
        program.unlink(missing_ok=True)


def direct(stats, field):
    return sum((value.get('direct') or {}).get(field, 0) for value in stats.values())


def direct_map(stats, field):
    return sum(sum(((value.get('direct') or {}).get(field) or {}).values()) for value in stats.values())


def queue_delta(before, after):
    return {name: {field: after[name]['queue'][field] - before[name]['queue'][field] for field in QUEUE_FIELDS}
            for name in after}


def check_direct_section(label, before, after, *, routed):
    """D01 stats: one Data entry per writing mutation and zero queue records."""
    entries = direct(after, 'entries_proposed') - direct(before, 'entries_proposed')
    applied = direct(after, 'entries_applied') - direct(before, 'entries_applied')
    mutations = direct(after, 'mutations') - direct(before, 'mutations')
    markers = direct(after, 'forward_markers_written') - direct(before, 'forward_markers_written')
    fallbacks = direct_map(after, 'fallbacks') - direct_map(before, 'fallbacks')
    queue = queue_delta(before, after)
    observed = dict(entries_proposed=entries, entries_applied=applied, mutations=mutations, markers=markers,
                    fallbacks=fallbacks, queue_delta=queue)
    if routed:
        assert entries == DIRECT_ENTRIES and applied == DIRECT_ENTRIES, (label, observed)
        assert fallbacks == 0 and markers == 0, (label, observed)
        assert all(value == 0 for delta in queue.values() for value in delta.values()), (label, observed)
    else:
        assert mutations == 0 and entries == 0, (label, observed)
        assert markers >= 1, (label, observed)
        assert sum(delta['terminal_records'] for delta in queue.values()) > 0, (label, observed)
    return observed


def d01_programs(label, gateways, token, ca, snapshot, create):
    """Run the program through both ORMs on every gateway; return canonical results."""
    runs = {}
    for name, (url, routed) in gateways.items():
        for orm in ('python', 'typescript'):
            table = f'd01_{label}_{name}_{orm}'
            print(json.dumps(dict(progress='D01', run=f'{label}/{name}/{orm}')), flush=True)
            create(url, table)
            results, snapshots = run_program(orm, url, table, token=token, ca=ca, snapshot=snapshot)
            observed = check_direct_section(f'{label}/{name}/{orm}', snapshots[0], snapshots[1], routed=routed)
            runs[(name, orm)] = dict(results=results, direct_section=observed)
    reference = runs[next(iter(runs))]['results']
    for key, run in runs.items():
        if run['results'] != reference:
            mismatch = [(a, b) for a, b in zip(reference, run['results']) if a != b]
            raise AssertionError(f'{label} {key} differs from {next(iter(runs))}: {mismatch}')
    return reference, {f'{name}/{orm}': run['direct_section'] for (name, orm), run in runs.items()}


# RF1 -------------------------------------------------------------------------------------
def suite_rf1(args, output):
    results = {}
    for mode in (True, False):
        server = Rf1Serve(args.binary, output / f'rf1-{"on" if mode else "off"}', direct=mode).start()
        try:
            def snapshot():
                return {'serve': server.stats()}

            def create(url, table):
                Database(url=url, token=server.token, database=DATABASE, timeout=30).create_table(table, SCHEMA, key='id')
            deadline = time.monotonic() + 60
            while mode and not (server.stats().get('direct_capability') or {}).get('active'):
                assert time.monotonic() < deadline, 'RF1 never activated data.direct.v1'
                time.sleep(0.25)
            capability = server.stats().get('direct_capability')
            reference, sections = d01_programs(f'rf1_{"on" if mode else "off"}', {'serve': (server.url, mode)},
                                               server.token, None, snapshot, create)
            results[mode] = reference
            if mode:
                d09_rf1(server)
            report('D01-RF1', topology='nabedra-db serve', direct_oltp=mode, capability=capability,
                   orms=['python', 'typescript'], steps=len(reference), direct_sections=sections)
        finally:
            server.close()
    d11_rf1_cli(args, output)
    assert results[True] == results[False], [(a, b) for a, b in zip(results[True], results[False]) if a != b]
    (args.output / 'rf1-program-results.json').write_text(json.dumps(results[True], indent=1))
    report('D01-RF1-on-off', equal=True, steps=len(results[True]))
    return results[True]


def d09_rf1(server):
    """D09 at RF1, and the regression for the T9 finding that a restarted `serve` never
    bound its retained Data replicas: SIGKILL under direct write load, cold start, every
    acknowledged write present and every acknowledged id replayed."""
    db = Database(url=server.url, token=server.token, database=DATABASE, timeout=30)
    table = db.create_table('d09_rf1', {'id': int, 'value': int}, key='id')
    stop, lock, history = threading.Event(), threading.Lock(), []
    keys = iter(range(1_000_000))

    def writer():
        client = Http(server.url, server.token, 30)
        while not stop.is_set():
            with lock:
                key = next(keys)
            body = insert_body(table.id, key, key * 3)
            try:
                status, reply = client.exchange('/v1/operations', body)
            except (OSError, http.client.HTTPException) as error:
                history.append((body, None, repr(error)))
                return
            history.append((body, status, reply))

    with ThreadPoolExecutor(4) as pool:
        futures = [pool.submit(writer) for _ in range(4)]
        try:
            time.sleep(2.0)
            server.process.kill()
        finally:
            stop.set()
        for future in futures:
            future.result()
    acknowledged = [body for body, status, _ in history if status == 200]
    assert acknowledged, 'no write was acknowledged before the kill'
    server.start(cold_start=True)
    deadline = time.monotonic() + 60
    while not (server.stats().get('direct_capability') or {}).get('active'):
        assert time.monotonic() < deadline, 'data.direct.v1 inactive after the cold start'
        time.sleep(0.25)
    before = server.stats()
    table = Database(url=server.url, token=server.token, database=DATABASE, timeout=30).table('d09_rf1')
    missing = [body for body in acknowledged if table.get(body['payload']['row']['id']) != body['payload']['row']]
    assert not missing, missing[:5]
    client = Http(server.url, server.token, 30)
    for body in acknowledged:
        assert client.exchange('/v1/operations', body) == (200, {'request_id': body['request_id'], 'data': None})
    after = server.stats()
    stats = {'serve': before}, {'serve': after}
    assert direct(stats[1], 'entries_proposed') == direct(stats[0], 'entries_proposed'), 'a replay wrote an entry'
    gateway = after['direct_gateway']
    assert gateway['direct_done'] >= len(acknowledged), gateway
    report('D09-RF1', acknowledged=len(acknowledged), unacknowledged=len(history) - len(acknowledged),
           missing_acknowledged=0, replayed=direct(stats[1], 'replayed') - direct(stats[0], 'replayed'),
           gateway_after_cold_start=gateway)


def d11_rf1_cli(args, output):
    """D11 index publication through the public CLI's local topology (the RF1 server
    that serves index administration)."""
    from oltp_runtime_acceptance import RuntimeServer
    root = output / 'rf1-cli'
    root.mkdir(parents=True, exist_ok=True)
    server = RuntimeServer(args.binary, root, cli=True)
    try:
        server.start()
        auth = json.loads((root / 'auth.json').read_text())
        deadline = time.monotonic() + 60
        while not (server.stats().get('direct_capability') or {}).get('active'):
            assert time.monotonic() < deadline, 'RF1 CLI server never activated data.direct.v1'
            time.sleep(0.25)

        def admin(name, payload, request_id):
            status, reply = call(server.url, '/v1/cluster/operations', operation(name, payload, request_id),
                                 token=auth['admin_token'], timeout=60)
            assert status == 200, (name, status, reply)
            return reply['data']
        d11_index(server.url, server.token, None, admin, lambda: {'rf1': server.stats()})
    finally:
        server.stop()
        server.log.close()


# RF3 cluster ------------------------------------------------------------------------------
def queue_owner(cluster):
    for name in cluster.live():
        status, _ = call(cluster.urls[name], '/v1/operations', operation('table.list', {}),
                         token=cluster.auth['data_token'])
        if status == 200:
            return name
    raise AssertionError('no worker endpoint serves queued operations')


def leader_of(cluster, request):
    """Send one routed mutation and return the worker whose executor ran it."""
    before = cluster.all_stats()
    status, reply = request()
    after = cluster.all_stats()
    leaders = [name for name in after if (after[name]['direct'] or {}).get('mutations', 0)
               > (before[name]['direct'] or {}).get('mutations', 0)]
    assert status == 200 and len(leaders) == 1, (status, reply, leaders)
    return leaders[0]


def insert_body(table_id, key, value, request_id=None, operation_name='table.insert'):
    return operation(operation_name, {'table_id': table_id, 'row': {'id': key, 'value': value}}, request_id)


def suite_cluster(args, output, rf1_reference):
    cluster = Rf3Cluster(args.binary, output / 'cluster', direct={'w1': True, 'w2': True, 'w3': False},
                         api_direct=(True, False))
    try:
        cluster.start()
        capability = cluster.wait_direct_active()
        token, ca = cluster.auth['data_token'], cluster.ca
        api_on, api_off = cluster.api_urls['api1'], cluster.api_urls['api2']

        def create(url, table):
            Database(url=url, token=token, database=DATABASE, ca_file=str(ca) if url.startswith('https') else None,
                     timeout=30).create_table(table, SCHEMA, key='id')
        # Queued operations (table.create/describe) are served only through the queue
        # owner's worker endpoint; other workers answer 401 (pre-existing, see report).
        owner = queue_owner(cluster)
        gateways = {'api1': (api_on, True), 'api2': (api_off, False),
                    owner: (cluster.urls[owner], cluster.direct[owner])}
        reference, sections = d01_programs('rf3', gateways, token, ca, cluster.all_stats, create)
        # Direct point operations through a non-owner worker gateway need no owner.
        db_owner = Database(url=api_on, token=token, database=DATABASE, ca_file=str(ca), timeout=30)
        probe = db_owner.create_table('d01_non_owner', {'id': int, 'value': int}, key='id')
        non_owner = {}
        for name in WORKERS:
            if name == owner:
                continue
            body = insert_body(probe.id, WORKERS.index(name), 1)
            inserted = call(cluster.urls[name], '/v1/operations', body, token=token)
            got = call(cluster.urls[name], '/v1/operations', operation('table.get', {
                'table_id': probe.id, 'key': WORKERS.index(name)}), token=token)
            listed = call(cluster.urls[name], '/v1/operations', operation('table.list', {}), token=token)
            if cluster.direct[name]:
                assert inserted == (200, {'request_id': body['request_id'], 'data': None}), (name, inserted)
                assert got[0] == 200 and got[1]['data'] == {'id': WORKERS.index(name), 'value': 1}, (name, got)
            non_owner[name] = dict(direct_oltp=cluster.direct[name], insert=inserted[0], get=got[0],
                                   queued_table_list=listed[0], queued_error=(listed[1].get('error') or {}).get('code'))
        report('D01-RF3', topology='3 registered workers + 2 API gateways, mTLS', capability=capability, queue_owner=owner,
               non_owner_worker_gateways=non_owner,
               gateways={name: dict(url=url, direct_oltp=routed) for name, (url, routed) in gateways.items()},
               orms=['python', 'typescript'], steps=len(reference), direct_sections=sections)
        if rf1_reference is not None:
            assert reference == rf1_reference, [(a, b) for a, b in zip(rf1_reference, reference) if a != b]
            report('U01', compared='RF1 serve vs RF3 registered workers and API gateways', equal=True,
                   steps=len(reference))

        db = Database(url=api_on, token=token, database=DATABASE, ca_file=str(ca), timeout=30)
        kv = db.create_table('d_kv', {'id': int, 'value': int}, key='id')
        api = Http(api_on, token, 30, ca)

        # D03 ------------------------------------------------------------------------------
        before = cluster.all_stats()
        request_id = uid()
        first = api.exchange('/v1/operations', insert_body(kv.id, 100, 1, request_id))
        replay = api.exchange('/v1/operations', insert_body(kv.id, 100, 1, request_id))
        other_body = api.exchange('/v1/operations', insert_body(kv.id, 100, 2, request_id))
        other_key = api.exchange('/v1/operations', insert_body(kv.id, 101, 1, request_id))
        after = cluster.all_stats()
        assert first == (200, {'request_id': request_id, 'data': None}), first
        assert replay == first, replay
        assert other_body[0] == 409 and other_body[1]['error']['code'] == 'conflict' \
            and other_body[1]['error']['outcome'] == 'not_applied', other_body
        assert other_key == (200, {'request_id': request_id, 'data': None}), other_key
        assert kv.get(100) == {'id': 100, 'value': 1} and kv.get(101) == {'id': 101, 'value': 1}
        entries = direct(after, 'entries_proposed') - direct(before, 'entries_proposed')
        replayed = direct(after, 'replayed') - direct(before, 'replayed')
        assert entries == 2, entries
        report('D03', gateway='api1', same_body='replayed', other_body_same_key=other_body[1]['error'],
               other_key='executed', entries_proposed=entries, replayed=replayed)

        # D11 index publication runs at RF1 (suite_rf1): index admin is not served by
        # registered RF3 workers (pre-existing, see the T9 report).
        # D11 drop ----------------------------------------------------------------------------
        d11_drop(cluster, api_on, token, ca)

        # D05 ------------------------------------------------------------------------------
        d05(cluster, kv, api_on, token, ca)

        # D10: a gateway killed mid-stream ----------------------------------------------------
        d10(cluster, kv, token, ca)

        # D09 ------------------------------------------------------------------------------
        d09(cluster, token, ca, output)
    except BaseException:
        dump(cluster)
        raise
    finally:
        cluster.close()


def dump(cluster):
    for name in [*cluster.workers, *cluster.apis]:
        print(f'===== {name} log tail =====\n{cluster.tail(name)}', file=sys.stderr, flush=True)


def d11_index(url, token, ca, admin, stats):
    """D11 index publication racing direct writers. `admin(operation, payload, request_id)`
    runs an admin operation; `stats()` returns `{name: /v1/runtime/stats}`."""
    db = Database(url=url, token=token, database=DATABASE, ca_file=str(ca) if ca else None, timeout=30)
    table = db.create_table('d11_index', {'id': int, 'category': int, 'label': str}, key='id')
    table.insert_many([{'id': i, 'category': i % 8, 'label': f'seed-{i}'} for i in range(64)])
    stop = threading.Event()
    acknowledged, errors, counter = {}, [], [64]
    lock = threading.Lock()

    def writer(index):
        client = Http(url, token, 30, ca)
        try:
            while not stop.is_set():
                with lock:
                    key = counter[0]
                    counter[0] += 1
                category = key % 8
                body = operation('table.insert', {'table_id': table.id, 'row': {'id': key, 'category': category,
                                                                                  'label': f'w{index}-{key}'}})
                status, reply = client.exchange('/v1/operations', body)
                if status == 200:
                    with lock:
                        acknowledged[key] = category
                    # Move an older row to another category through the direct path.
                    moved = key - 32
                    if moved >= 64:
                        new = (moved + 3) % 8
                        status, reply = client.exchange('/v1/operations', operation('table.update', {
                            'table_id': table.id, 'key': moved, 'values': {'category': new}}))
                        if status == 200 and reply['data'] is True:
                            with lock:
                                acknowledged[moved] = new
                        elif status != 200:
                            errors.append((status, reply))
                else:
                    errors.append((status, reply))
        finally:
            client.close()

    before = stats()
    with ThreadPoolExecutor(4) as pool:
        futures = [pool.submit(writer, index) for index in range(4)]
        try:
            time.sleep(1.0)
            index_id = uid()
            definition = {'table_id': table.id, 'index_id': index_id, 'name': 'by_category', 'columns': ['category'],
                          'covering': ['label']}
            build_id = uid()
            created = admin('storage.index.create', definition, build_id)
            selector = {'table_id': table.id, 'index_id': index_id, 'build_id': build_id, 'attempt': 1}
            deadline = time.monotonic() + 120
            state = created['state']
            while state != 'ready':
                assert state in ('building', 'pending', 'backfilling', 'validating'), created
                assert time.monotonic() < deadline, f'index never became ready: {state}'
                time.sleep(0.5)
                state = admin('storage.index.status', selector, None)['state']
            time.sleep(1.0)
        finally:
            stop.set()
        for future in futures:
            future.result()
    after = stats()
    rows = table.query().collect()
    assert all(error[1]['error']['outcome'] == 'not_applied' for error in errors), errors[:5]
    expected = {}
    for item in rows:
        expected.setdefault(item['category'], set()).add(item['id'])
    for key, category in acknowledged.items():
        assert any(item['id'] == key for item in rows), key
    plans = {}
    for category in range(8):
        selected = table.where(col('category') == category)
        indexed = {item['id'] for item in selected.collect()}
        plans[category] = selected.explain()
        assert 'by_category' in plans[category], plans[category]
        assert indexed == expected.get(category, set()), (category, indexed ^ expected.get(category, set()))
    report('D11-index', table=table.id, rows=len(rows), acknowledged_direct_writes=len(acknowledged),
           not_applied_errors=len(errors), index_state=state,
           direct_entries=direct(after, 'entries_proposed') - direct(before, 'entries_proposed'),
           direct_fallbacks=direct_map(after, 'fallbacks') - direct_map(before, 'fallbacks'),
           plan_uses_index=True, plan_sample=plans[0][:300])


def admin_owner(cluster, name, payload, request_id=None):
    errors = []
    for worker in cluster.live():
        status, reply = call(cluster.urls[worker], '/v1/cluster/operations', operation(name, payload, request_id),
                             token=cluster.auth['admin_token'], timeout=60)
        if status == 200:
            return reply['data']
        errors.append((worker, status, reply))
    raise AssertionError(f'{name}: {errors}')


def d11_drop(cluster, url, token, ca):
    db = Database(url=url, token=token, database=DATABASE, ca_file=str(ca), timeout=30)
    table = db.create_table('d11_drop', {'id': int, 'value': int}, key='id')
    stop, lock = threading.Event(), threading.Lock()
    history, counter = [], [0]
    dropped_at = [None]

    def writer():
        client = Http(url, token, 30, ca)
        try:
            while not stop.is_set():
                with lock:
                    key = counter[0]
                    counter[0] += 1
                sent = time.monotonic()
                try:
                    status, reply = client.exchange('/v1/operations', insert_body(table.id, key, key))
                except (OSError, http.client.HTTPException):
                    client.close()
                    client = Http(url, token, 30, ca)
                    status, reply = None, None
                history.append((sent, time.monotonic(), key, status, reply))
        finally:
            client.close()

    with ThreadPoolExecutor(4) as pool:
        futures = [pool.submit(writer) for _ in range(4)]
        try:
            time.sleep(1.0)
            table.drop()
            dropped_at[0] = time.monotonic()
            time.sleep(1.5)
        finally:
            stop.set()
        for future in futures:
            future.result()
    after_drop = [entry for entry in history if entry[0] > dropped_at[0]]
    succeeded_after = [entry for entry in after_drop if entry[3] == 200]
    assert not succeeded_after, succeeded_after[:3]
    assert after_drop, 'no direct write was sent after the drop'
    codes = sorted({(entry[3], (entry[4] or {}).get('error', {}).get('code'),
                     (entry[4] or {}).get('error', {}).get('outcome')) for entry in after_drop})
    assert all(code[2] == 'not_applied' for code in codes), codes
    try:
        table.get(0)
        raise AssertionError('the dropped table is still readable')
    except NotFoundError:
        pass
    recreated = db.create_table('d11_drop', {'id': int, 'value': int}, key='id')
    assert recreated.id != table.id and recreated.query().count() == 0
    report('D11-drop', writes=len(history), acknowledged=sum(entry[3] == 200 for entry in history),
           sent_after_drop=len(after_drop), succeeded_after_drop=0, after_drop_codes=codes,
           recreated_rows=0)


def d05(cluster, kv, url, token, ca):
    api = Http(url, token, 30, ca)
    kv.insert({'id': 500, 'value': 0})
    leader = leader_of(cluster, lambda: api.exchange('/v1/operations', operation('table.update', {
        'table_id': kv.id, 'key': 500, 'values': {'value': 1}})))
    followers = [name for name in WORKERS if name != leader]
    timings = []
    cluster.workers[followers[0]].pause()
    try:
        for value in range(2, 7):
            started = time.monotonic()
            status, reply = api.exchange('/v1/operations', operation('table.update', {
                'table_id': kv.id, 'key': 500, 'values': {'value': value}}))
            timings.append(round(time.monotonic() - started, 3))
            assert status == 200 and reply['data'] is True, (status, reply)
        cluster.workers[followers[1]].pause()
        request_id = uid()
        body = operation('table.update', {'table_id': kv.id, 'key': 500, 'values': {'value': 99}}, request_id)
        started = time.monotonic()
        try:
            status, reply = Http(url, token, 40, ca).exchange('/v1/operations', body)
        except (OSError, http.client.HTTPException) as error:
            status, reply = None, repr(error)
        blocked = round(time.monotonic() - started, 3)
        assert status != 200, (status, reply)
    finally:
        for name in followers:
            cluster.workers[name].resume()
    cluster.wait_workers()
    value = kv.get(500)['value']
    outcome = reply['error']['outcome'] if isinstance(reply, dict) else 'transport'
    if outcome == 'not_applied':
        assert value == 6, value
    else:
        assert value in (6, 99), value
    retry = api.exchange('/v1/operations', body)
    again = api.exchange('/v1/operations', body)
    assert retry == again == (200, {'request_id': request_id, 'data': True}), (retry, again)
    assert kv.get(500)['value'] == 99
    report('D05', leader=leader, paused=followers, one_paused_latencies_s=timings,
           two_paused=dict(status=status, reply=reply, seconds=blocked), value_after_resume=value,
           retry='exactly one result, replayed')


def d10(cluster, kv, token, ca):
    """A gateway dies with requests in flight; callers see transport errors (unknown
    outcome), Raft keeps committing through the other gateways, and each in-flight id
    gets exactly one outcome on retry."""
    url = cluster.api_urls['api1']
    stop, history, lock = threading.Event(), [], threading.Lock()
    keys = iter(range(10_000, 20_000))

    def writer():
        client = Http(url, token, 30, ca)
        while not stop.is_set():
            with lock:
                key = next(keys)
            body = insert_body(kv.id, key, key)
            try:
                status, reply = client.exchange('/v1/operations', body)
            except (OSError, http.client.HTTPException) as error:
                history.append((body, None, repr(error)))
                return
            history.append((body, status, reply))

    with ThreadPoolExecutor(6) as pool:
        futures = [pool.submit(writer) for _ in range(6)]
        try:
            time.sleep(1.5)
            cluster.kill_api('api1')
        finally:
            stop.set()
        for future in futures:
            future.result()
    lost = [entry for entry in history if entry[1] is None]
    acknowledged = [entry for entry in history if entry[1] == 200]
    assert acknowledged and lost, (len(acknowledged), len(lost))
    other = Http(cluster.urls['w1'], token, 30)
    for body, _, _ in lost + acknowledged:
        status, reply = other.exchange('/v1/operations', body)
        assert (status, reply) == (200, {'request_id': body['request_id'], 'data': None}), (status, reply)
    # Raft progress continued: fresh writes commit through a worker gateway.
    for key in range(20_000, 20_010):
        assert other.exchange('/v1/operations', insert_body(kv.id, key, key))[0] == 200
    cluster.start_api('api1')
    report('D10-gateway-kill', acknowledged=len(acknowledged), in_flight_lost=len(lost),
           in_flight_errors=sorted({entry[2][:60] for entry in lost}), retried_exactly_once=True,
           note='lane exhaustion and the old one-shot peer run at library level (d10_* in multiplex::transport)')


def d09(cluster, token, ca, output):
    db = Database(url=cluster.api_urls['api1'], token=token, database=DATABASE, ca_file=str(ca), timeout=30)
    table = db.create_table('d09', {'id': int, 'value': int}, key='id')
    stop, lock, history = threading.Event(), threading.Lock(), []
    keys = iter(range(1_000_000))
    gateways = [(cluster.api_urls['api1'], ca), (cluster.api_urls['api2'], ca), (cluster.urls['w1'], None),
                (cluster.urls['w2'], None)]

    def writer(index):
        url, authority = gateways[index % len(gateways)]
        client = Http(url, token, 30, authority)
        while not stop.is_set():
            with lock:
                key = next(keys)
            body = insert_body(table.id, key, key * 7)
            try:
                status, reply = client.exchange('/v1/operations', body)
            except (OSError, http.client.HTTPException) as error:
                history.append((body, None, repr(error)))
                return
            history.append((body, status, reply))

    with ThreadPoolExecutor(8) as pool:
        futures = [pool.submit(writer, index) for index in range(8)]
        try:
            time.sleep(3.0)
            cluster.kill_all()
        finally:
            stop.set()
        for future in futures:
            future.result()
    acknowledged = [body for body, status, reply in history if status == 200]
    unknown = [body for body, status, reply in history if status != 200]
    assert acknowledged, 'no write was acknowledged before the kill'
    profile = cluster.cold_start()
    for name in list(cluster.apis):
        cluster.start_api(name)
    capability = cluster.wait_direct_active()
    before = cluster.all_stats()
    db = Database(url=cluster.api_urls['api1'], token=token, database=DATABASE, ca_file=str(ca), timeout=30)
    table = db.table('d09')
    missing = [body for body in acknowledged if table.get(body['payload']['row']['id']) != body['payload']['row']]
    assert not missing, missing[:5]
    api = Http(cluster.api_urls['api1'], token, 30, ca)
    for body in acknowledged:
        status, reply = api.exchange('/v1/operations', body)
        assert (status, reply) == (200, {'request_id': body['request_id'], 'data': None}), (status, reply)
    after = cluster.all_stats()
    assert direct(after, 'entries_proposed') == direct(before, 'entries_proposed'), 'a replay wrote an entry'
    present = sum(table.get(body['payload']['row']['id']) is not None for body in unknown)
    report('D09', acknowledged=len(acknowledged), unacknowledged=len(unknown),
           unacknowledged_present_after_cold_start=present, cold_profile=profile, capability=capability,
           replayed=direct(after, 'replayed') - direct(before, 'replayed'), missing_acknowledged=0)


# Test-child clusters -----------------------------------------------------------------------
def gates(cluster, request_id, stage):
    for name in WORKERS:
        path = cluster.barrier_dir(name)
        write_json(path / 'gates.json', {request_id: stage})


def reached(cluster, request_id, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for name in WORKERS:
            if (cluster.barrier_dir(name) / f'reached-{request_id}').exists():
                return name
        time.sleep(0.02)
    raise AssertionError(f'no leader reached the barrier for {request_id}')


def release(cluster, request_id):
    for name in WORKERS:
        (cluster.barrier_dir(name) / f'release-{request_id}').touch()


def suite_barriers(args, output):
    budget = 4
    cluster = Rf3Cluster(args.binary, output / 'barriers', test_executable=args.test_executable,
                         api_direct=(True, True), worker_env={'NABEDRA_OLTP_TEST_DIRECT_MAX_IN_FLIGHT': str(budget)})
    try:
        cluster.start()
        cluster.wait_direct_active()
        token, ca = cluster.auth['data_token'], cluster.ca
        db = Database(url=cluster.api_urls['api1'], token=token, database=DATABASE, ca_file=str(ca), timeout=30)
        table = db.create_table('d_barrier', {'id': int, 'value': int}, key='id')
        table.insert({'id': 0, 'value': 0})

        # D12 --------------------------------------------------------------------------------
        before = cluster.all_stats()
        urls = [(cluster.api_urls['api1'], ca), (cluster.api_urls['api2'], ca)] + \
               [(cluster.urls[name], None) for name in WORKERS]
        start = threading.Barrier(96)

        def offer(index):
            url, authority = urls[index % len(urls)]
            client = Http(url, token, 30, authority)
            body = insert_body(table.id, 1000 + index, index)
            start.wait()
            try:
                return body, *client.exchange('/v1/operations', body)
            finally:
                client.close()
        with ThreadPoolExecutor(96) as pool:
            outcomes = list(pool.map(offer, range(96)))
        after = cluster.all_stats()
        accepted = [body for body, status, _ in outcomes if status == 200]
        refused = [(body, reply) for body, status, reply in outcomes if status == 429]
        other = [(status, reply) for body, status, reply in outcomes if status not in (200, 429)]
        assert not other, other[:5]
        assert refused, 'no request exceeded the leader budget'
        assert all(reply['error']['code'] == 'overloaded' and reply['error']['outcome'] == 'not_applied'
                   for _, reply in refused), refused[:3]
        # A typed 429 comes from the leader's in-flight budget or, earlier, from its
        # full handler lane (`worker handler lane full`); both refuse before execution.
        overloaded = direct(after, 'overloaded_429') - direct(before, 'overloaded_429')
        executed = direct(after, 'mutations') - direct(before, 'mutations')
        for body in accepted:
            assert table.get(body['payload']['row']['id']) == body['payload']['row']
        for body, _ in refused:
            assert table.get(body['payload']['row']['id']) is None
        in_flight = {name: value['direct']['in_flight'] for name, value in after.items()}
        limits = {name: value['direct']['max_in_flight'] for name, value in after.items()}
        assert all(limit == budget for limit in limits.values()), limits
        assert all(value == 0 for value in in_flight.values()), in_flight
        report('D12', offered=96, acknowledged=len(accepted), refused_429=len(refused), leader_budget_429=overloaded,
               lane_or_gateway_429=len(refused) - overloaded, leader_mutations_admitted=executed,
               max_in_flight=limits, in_flight_after=in_flight,
               scheduler_after={name: value['scheduler'] for name, value in after.items()},
               acknowledged_verified=len(accepted), refused_absent=len(refused))

        # D02 --------------------------------------------------------------------------------
        before = cluster.all_stats()
        request_id = uid()
        body = insert_body(table.id, 2000, 7, request_id)
        gates(cluster, request_id, 'direct-applied')
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(lambda: Http(cluster.api_urls['api1'], token, 30, ca).exchange('/v1/operations', body))
            leader = reached(cluster, request_id)
            cluster.kill_api('api1')
            release(cluster, request_id)
            try:
                lost = pending.result(timeout=30)
            except (OSError, http.client.HTTPException) as error:
                lost = repr(error)
        retry = call(cluster.api_urls['api2'], '/v1/operations', body, token=token, ca=ca)
        again = call(cluster.urls[[name for name in WORKERS if name != leader][0]], '/v1/operations', body, token=token)
        # api1 is gone: read through the other gateway from now on.
        table = Database(url=cluster.api_urls['api2'], token=token, database=DATABASE, ca_file=str(ca),
                         timeout=30).table('d_barrier')
        after = cluster.all_stats()
        assert retry == again == (200, {'request_id': request_id, 'data': None}), (retry, again)
        assert table.get(2000) == {'id': 2000, 'value': 7}
        entries = direct(after, 'entries_proposed') - direct(before, 'entries_proposed')
        replayed = direct(after, 'replayed') - direct(before, 'replayed')
        assert entries == 1 and replayed >= 1, (entries, replayed)
        report('D02', leader=leader, killed='api1 after the leader applied, before its reply', first_caller=str(lost)[:120],
               retry_gateway='api2', original_result=True, entries_proposed=entries, replayed=replayed)

        # D04 after quorum ------------------------------------------------------------------
        cluster.start_api('api1')
        request_id = uid()
        body = insert_body(table.id, 3000, 9, request_id)
        gates(cluster, request_id, 'direct-applied')
        before = cluster.all_stats()
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(lambda: Http(cluster.api_urls['api2'], token, 40, ca).exchange('/v1/operations', body))
            leader = reached(cluster, request_id)
            cluster.workers[leader].kill()
            first = pending.result(timeout=45)
        survivors = [name for name in WORKERS if name != leader]
        retry = call(cluster.api_urls['api2'], '/v1/operations', body, token=token, ca=ca)
        after = {name: cluster.stats(name) for name in survivors}
        before = {name: before[name] for name in survivors}
        assert retry == (200, {'request_id': request_id, 'data': None}), retry
        assert first == retry or first[1]['error']['code'] == 'outcome_unknown', first
        assert table.get(3000) == {'id': 3000, 'value': 9}
        entries = direct(after, 'entries_proposed') - direct(before, 'entries_proposed')
        assert entries == 0, f'the new leader re-applied the acknowledged write: {entries}'
        update = call(cluster.api_urls['api2'], '/v1/operations', operation('table.update', {
            'table_id': table.id, 'key': 3000, 'values': {'value': 10}}), token=token, ca=ca)
        assert update[0] == 200 and update[1]['data'] is True, update
        report('D04-after-quorum', killed_leader=leader, first_reply=first, retry=retry,
               survivor_entries_for_the_id=entries,
               replayed=direct(after, 'replayed') - direct(before, 'replayed'), new_leader_serves=True)
    except BaseException:
        dump(cluster)
        raise
    finally:
        cluster.close()


def suite_quorum(args, output):
    cluster = Rf3Cluster(args.binary, output / 'quorum', test_executable=args.test_executable, api_direct=(True,))
    try:
        cluster.start()
        cluster.wait_direct_active()
        token, ca = cluster.auth['data_token'], cluster.ca
        db = Database(url=cluster.api_urls['api1'], token=token, database=DATABASE, ca_file=str(ca), timeout=30)
        table = db.create_table('d04', {'id': int, 'value': int}, key='id')
        api = Http(cluster.api_urls['api1'], token, 30, ca)
        leader = leader_of(cluster, lambda: api.exchange('/v1/operations', insert_body(table.id, 1, 1)))
        followers = [name for name in WORKERS if name != leader]
        request_id = uid()
        body = insert_body(table.id, 4000, 4, request_id)
        before = cluster.stats(leader)['direct']['entries_proposed']
        gates(cluster, request_id, 'direct-proposing')
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(lambda: Http(cluster.api_urls['api1'], token, 60, ca).exchange('/v1/operations', body))
            assert reached(cluster, request_id) == leader
            # The leader holds a registered timestamp and is about to propose: stop
            # both followers, let it append the entry, then kill it before any quorum.
            for name in followers:
                cluster.workers[name].pause()
            release(cluster, request_id)
            time.sleep(1.5)
            proposed_at_old_leader = cluster.stats(leader)['direct']['entries_proposed'] - before
            assert proposed_at_old_leader == 1, proposed_at_old_leader
            cluster.workers[leader].kill()
            for name in followers:
                cluster.workers[name].resume()
            try:
                first = pending.result(timeout=60)
            except (OSError, http.client.HTTPException) as error:
                first = repr(error)
        survivors_before = {name: cluster.stats(name) for name in followers}
        retry = call(cluster.api_urls['api1'], '/v1/operations', body, token=token, ca=ca, timeout=60)
        again = call(cluster.api_urls['api1'], '/v1/operations', body, token=token, ca=ca, timeout=60)
        survivors_after = {name: cluster.stats(name) for name in followers}
        assert retry == again == (200, {'request_id': request_id, 'data': None}), (retry, again)
        assert table.get(4000) == {'id': 4000, 'value': 4}
        duplicate = call(cluster.api_urls['api1'], '/v1/operations', insert_body(table.id, 4000, 5), token=token, ca=ca)
        assert duplicate[0] == 409, duplicate
        report('D04-before-quorum', killed_leader=leader, paused_then_resumed=followers,
               proposed_at_old_leader=proposed_at_old_leader, first_reply=first, retry=retry,
               replay_entries=direct(survivors_after, 'entries_proposed') - direct(survivors_before, 'entries_proposed'),
               applied_once=True)
    except BaseException:
        dump(cluster)
        raise
    finally:
        cluster.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--test-executable', type=Path)
    parser.add_argument('--suite', action='append', choices=('rf1', 'cluster', 'barriers', 'quorum'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--keep', action='store_true', help='keep process directories and logs')
    args = parser.parse_args()
    suites = args.suite or ['rf1', 'cluster', 'barriers', 'quorum']
    if {'barriers', 'quorum'} & set(suites) and not args.test_executable:
        parser.error('--suite barriers and quorum need --test-executable')
    args.output.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix='nabedra-direct-'))
    failed = []
    rf1_reference = None
    try:
        for suite in suites:
            try:
                if suite == 'rf1':
                    rf1_reference = suite_rf1(args, work)
                elif suite == 'cluster':
                    suite_cluster(args, work, rf1_reference)
                elif suite == 'barriers':
                    suite_barriers(args, work)
                else:
                    suite_quorum(args, work)
            except BaseException as error:
                traceback.print_exc()
                failed.append(suite)
                report(f'suite:{suite}', status='failed', error=repr(error)[:2000])
                if isinstance(error, KeyboardInterrupt):
                    raise
    finally:
        (args.output / 'results.json').write_text(json.dumps(dict(
            suites=suites, failed=failed, results=RESULTS,
            binary=str(args.binary), test_executable=str(args.test_executable)), indent=2, default=str) + '\n')
        logs = args.output / 'logs'
        logs.mkdir(exist_ok=True)
        for log in work.rglob('*.log'):
            shutil.copy(log, logs / ('-'.join(log.relative_to(work).parts)))
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)
    print(json.dumps(dict(failed=failed, passed=[r['scenario'] for r in RESULTS if r['status'] == 'passed'])))
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())

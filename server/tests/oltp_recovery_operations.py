#!/usr/bin/env python3
"""Process checks for retained-worker recovery and direct-mode rollback."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
from uuid import UUID

from oltp_direct_cluster import (DATABASE, EXECUTION, IDENTITY_FIELDS, WORKERS, Rf1Serve,
                                 Rf3Cluster, call, free_port, operation, uid, write_json)

# Includes a Control election and ready-set removal on the CI runner.
PROBE_WINDOW = 60
RECOVERY_DEADLINE = 180
TRANSPORT_ERRORS = (OSError, http.client.HTTPException)


def wait_for(check, label, timeout=RECOVERY_DEADLINE):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except TRANSPORT_ERRORS as error:
            last = repr(error)
        time.sleep(0.2)
    raise AssertionError(f'{label}: {last!r}')


def send(url, token, body, *, ca=None, path='/v1/operations', timeout=30):
    return call(url, path, body, token=token, ca=ca, timeout=timeout)


def success(status, reply):
    assert status == 200 and 'data' in reply, (status, reply)
    return reply['data']


def create(send_op, name):
    return success(*send_op(operation('table.create',
        dict(name=name, schema={'id': 'int64', 'value': 'int64'}, key='id', format='parquet'))))['id']


def insert(table, key, value):
    return operation('table.insert', dict(table_id=table, row=dict(id=key, value=value)))


def get(table, key):
    return operation('table.get', dict(table_id=table, key=key))


def update(table, key, value):
    return operation('table.update', dict(table_id=table, key=key, values=dict(value=value)))


def report(scenario, **observations):
    print(json.dumps(dict(scenario=scenario, status='passed', **observations), sort_keys=True), flush=True)


def o19_d03_rollback(binary, root):
    source = Rf1Serve(binary, root / 'source')
    target = Rf1Serve(binary, root / 'target', direct=False)
    target.token = source.token
    archive = root / 'full'
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(('AWS_', 'NABEDRA_S3_', 'NABEDRA_OLTP_ARCHIVE_'))}
    env['NABEDRA_TOKEN'] = source.token

    def cli(command, *args, ok=True):
        result = subprocess.run([source.binary, 'database', command, '--database', DATABASE,
                                 '--workers', '2', *map(str, args)],
                                env=env, capture_output=True, text=True, timeout=180)
        assert (result.returncode == 0) == ok, (command, result.stdout, result.stderr)
        return json.loads(result.stdout) if ok else result.stderr

    try:
        source.start()
        wait_for(lambda: source.stats()['direct_capability']['active'], 'source direct capability')
        write = lambda body: send(source.url, source.token, body)
        table = create(write, 'rep1_archive')
        first = insert(table, 1, 7)
        before = source.stats()['direct']['entries_proposed']
        original = write(first)
        assert original == (200, dict(request_id=first['request_id'], data=None)), original
        assert success(*write(update(table, 1, 8))) is True
        assert source.stats()['direct']['entries_proposed'] - before == 2
        source.close()

        manifest = cli('backup', '--data-dir', source.root / 'data', '--output', archive, '--cold-start')
        assert manifest['scope']['base_exclusive'] is None and manifest['frames'] > 0, manifest
        off = root / 'queued-only.json'
        write_json(off, EXECUTION | {'direct_oltp': False})
        incremental = root / 'incremental'
        refusal = cli('backup', '--data-dir', source.root / 'data', '--output', incremental,
                      '--previous', archive, '--execution-config', off, '--cold-start', ok=False)
        assert 'UnsupportedOperation' in refusal and 'take a full backup' in refusal, refusal
        assert not incremental.exists(), 'incremental backup created an output directory'

        restored = cli('restore', '--input', archive, '--data-dir', target.root / 'data',
                       '--execution-config', off)
        assert restored['target_cut'] == manifest['scope']['cut'], restored
        target.start(cold_start=True)
        wait_for(lambda: target.stats()['direct_capability']['active'], 'restored direct capability')
        capability = target.stats()['direct_capability']
        assert capability['direct_oltp'] is False, capability
        read_write = lambda body: send(target.url, target.token, body)
        assert success(*read_write(get(table, 1))) == {'id': 1, 'value': 8}
        assert read_write(first) == original, 'restored request ID lost its original result'
        assert success(*read_write(get(table, 1))) == {'id': 1, 'value': 8}
        queued = target.stats()['queue']['accepted_records']
        assert success(*read_write(update(table, 1, 9))) is True
        assert target.stats()['queue']['accepted_records'] > queued
        assert success(*read_write(get(table, 1))) == {'id': 1, 'value': 9}
        report('O19/D03 rollback', cut=restored['target_cut'], direct_entries=2,
               restore_build='same', incremental='refused with full backup alternative')
    finally:
        source.close()
        target.close()


def initial_range(cluster, table):
    # Matches the registered planner's derivation for a new table.
    shard = UUID(bytes=hashlib.sha256(UUID(table).bytes + b'initial-shard').digest()[:16])
    group = UUID(bytes=hashlib.sha256(shard.bytes + b'data-group').digest()[:16])
    return dict(cluster_id=cluster.cluster_id, database=DATABASE, group_id=str(group),
                kind={'data': dict(table_id=table, shard_id=str(shard))}, group_format_version=1)


def retained_config(cluster, victim, survivors):
    peers = cluster.admin('cluster.connection', {})['control_peers']

    def prepare():
        previous = cluster.enrollments[victim]
        fresh = dict(previous, incarnation=uid(), enrollment_revision=previous['enrollment_revision'] + 1)
        cfg = deepcopy(cluster.config[victim])
        node = cfg['node']
        plan = dict(id=uid(), run_id=cluster.run_id,
                    previous={field: previous[field] for field in IDENTITY_FIELDS}, next=fresh,
                    pool_id=node['pool_id'], mount_generation=node['mount']['generation'] + 1)
        for name in survivors:
            status, reply = send(cluster.urls[name], cluster.auth['admin_token'],
                                 operation('worker.restart.prepare', plan, plan['id']),
                                 path='/v1/cluster/operations', timeout=10)
            if status == 200:
                assert reply['data']['identity'] == {field: fresh[field] for field in IDENTITY_FIELDS}, reply
                node.update(enrollment=fresh, control_enrollments=peers,
                            rolling=dict(preparation_id=plan['id'], run_id=cluster.run_id))
                node['mount'].update(worker=reply['data']['identity'], generation=plan['mount_generation'])
                return cfg
            assert status in (409, 429, 503), (status, reply)
        return None

    return wait_for(prepare, 'prepare retained restart')


def assert_start_refused(cluster, name):
    cluster.launch_worker(name)
    attempt = cluster.workers[name]
    try:
        assert attempt.popen.wait(timeout=30) != 0, cluster.tail(name)
        assert 'invalid hosted group placement or identity' in cluster.tail(name), cluster.tail(name)
    finally:
        attempt.stop()


def two_ready(cluster, survivors):
    return all(call(cluster.urls[name], '/health/ready', token=cluster.auth['data_token'], timeout=3)[1]
               .get('ready_workers') == 2 for name in survivors)


def o06_o17_system_replacement_check(binary, test_executable, root):
    cluster = Rf3Cluster(binary, root, mounts=True, test_executable=test_executable)
    try:
        cluster.start()
        connection = cluster.admin('cluster.connection', {})
        for name in WORKERS:
            (cluster.barrier_dir(name) / 'inspect-control-leader').touch()

        def control_leader(names):
            leaders = []
            for name in names:
                cluster.stats(name)
                if json.loads((cluster.barrier_dir(name) / 'control-leader.json').read_text()):
                    leaders.append(name)
            return leaders[0] if len(leaders) == 1 else None

        victim = wait_for(lambda: control_leader(WORKERS), 'stable Control leader', PROBE_WINDOW)
        survivors = [name for name in WORKERS if name != victim]
        enrollment = cluster.enrollments[victim]
        previous = {field: enrollment[field] for field in IDENTITY_FIELDS}
        check = operation('worker.system_replacement.check',
                          dict(run_id=connection['run_id'], previous=previous))

        def observe(token=None):
            return send(cluster.urls[survivors[0]], token or cluster.auth['admin_token'], check,
                        path='/v1/cluster/operations', timeout=15)

        def operator(name, request):
            return send(cluster.urls[name], cluster.auth['admin_token'], request,
                        path='/v1/cluster/operations', timeout=15)

        status, reply = observe(cluster.auth['data_token'])
        assert status in (401, 403), (status, reply)
        status, reply = observe()
        assert status == 409 and 'reachable' in json.dumps(reply), (status, reply)

        held_advert = cluster.barrier_dir(survivors[0]) / 'hold-system-replacement-advert'
        enable = operation('worker.system_replacement.enable', {})
        held_advert.touch()
        try:
            status, reply = operator(victim, enable)
            assert reply.get('error', {}).get('code') == 'unsupported_operation', (status, reply)
        finally:
            held_advert.unlink()
        activation = success(*operator(victim, enable))
        assert activation['capability'] == 'control.system-replacement.v1'

        cluster.wait_direct_active()
        send_op = lambda body: send(cluster.api_urls['api1'], cluster.auth['data_token'],
                                    body, ca=cluster.ca)
        table = create(send_op, 'rep3_preflight')
        acknowledged = insert(table, 1, 77)
        original = send_op(acknowledged)
        assert success(*original) is None
        queued = insert(table, 2, 88)
        queued_id = queued['request_id']
        for name in WORKERS:
            write_json(cluster.barrier_dir(name) / 'gates.json', {queued_id: 'queued'})
            write_json(cluster.barrier_dir(name) / 'inspect.json', [queued_id])
        admitted = send(cluster.api_urls['api1'], cluster.auth['data_token'], queued,
                        ca=cluster.ca, path='/v1/requests')
        assert admitted == (202, dict(request_id=queued_id, run_id=cluster.run_id, state='queued')), admitted
        for name in survivors:
            cluster.stats(name)
            saved = json.loads((cluster.barrier_dir(name) / f'record-{queued_id}.json').read_text())
            assert saved['accepted'] and json.loads(saved['body']) == queued, saved

        stopped_at = time.monotonic()
        cluster.workers[victim].kill()
        mount = Path(cluster.config[victim]['node']['directory'])
        subprocess.run(['sudo', '-n', 'umount', str(mount)], check=True, timeout=60)
        cluster.mounted.remove(mount)
        assert not (mount / 'pool').exists()
        wait_for(lambda: two_ready(cluster, survivors), 'surviving ready workers')
        for name in survivors:
            write_json(cluster.barrier_dir(name) / 'gates.json', {})

        def reading():
            status, reply = send_op(get(table, 1))
            if status in (429, 503, 504):
                return None
            assert success(status, reply) == dict(id=1, value=77)
            return time.monotonic()

        first_read = wait_for(reading, 'read an acknowledged row after Control failover', PROBE_WINDOW)

        cluster.workers[survivors[1]].pause()
        try:
            status, reply = observe()
            assert status == 503, (status, reply)
        finally:
            cluster.workers[survivors[1]].resume()

        def ready_to_check():
            status, reply = observe()
            return reply if status == 200 else None

        wait_for(ready_to_check, 'Control quorum restored', PROBE_WINDOW)
        data_group = initial_range(cluster, table)['group_id']
        held = [cluster.barrier_dir(name) / f'hold-raft-{data_group}' for name in survivors]
        for path in held:
            path.touch()
        try:
            assert cluster.admin('cluster.connection', {}, worker=survivors[0])['run_id'] == cluster.run_id
            status, reply = observe()
            assert status == 503 and 'surviving RF3 quorum' in json.dumps(reply), (status, reply)
        finally:
            for path in held:
                path.unlink()

        def checked():
            status, reply = observe()
            if status == 503:
                return None
            return success(status, reply)

        result = wait_for(checked, 'all affected groups have surviving quorum', PROBE_WINDOW)
        expected = {group['identity']['group_id']
                    for group in cluster.config[victim]['node']['placements'].values()}
        expected.add(initial_range(cluster, table)['group_id'])
        assert set(result['groups']) == expected, result
        leader = wait_for(lambda: control_leader(survivors), 'new Control leader', PROBE_WINDOW)
        assert success(*operator(leader, enable)) == activation
        fresh = cluster.certificate('replacement')
        fresh_mount = cluster.mount_pool('replacement', uid(), fresh)
        mount_key = cluster.root / 'replacement-mount.pem'
        cluster.openssl('genpkey', '-algorithm', 'Ed25519', '-out', mount_key)
        mount_key.chmod(0o600)
        def mount_der(*args):
            return subprocess.check_output(['openssl', 'pkey', '-in', str(mount_key),
                                            '-outform', 'DER', *args], timeout=30)[-32:]
        mount_seed = Path(fresh_mount['mount_path']) / 'NABEDRA_RESTART_KEY'
        mount_seed.write_bytes(mount_der())
        mount_seed.chmod(0o600)
        intent = dict(id=uid(), run_id=cluster.run_id, previous=previous,
                      next=fresh, mount=fresh_mount, mount_public_key=mount_der('-pubout').hex())
        prepare = operation('worker.system_replacement.prepare', intent, intent['id'])
        borrowed_cert = cluster.enrollments[survivors[0]]['certificate_sha256']
        bad_mount = dict(fresh_mount, worker=dict(fresh_mount['worker'], certificate_sha256=borrowed_cert))
        bad_intent = dict(intent, next=dict(fresh, certificate_sha256=borrowed_cert), mount=bad_mount)
        status, reply = operator(leader, operation('worker.system_replacement.prepare', bad_intent, intent['id']))
        assert status == 400, (status, reply)
        prepared = success(*operator(leader, prepare))
        assert prepared['intent'] == intent
        assert {item['expected']['identity']['group_id'] for item in prepared['groups']} == expected
        for plan in prepared['groups']:
            assert len(plan['old']) == 3 and any(item['worker_id'] == previous['worker_id'] for item in plan['old'])
            assert plan['learner']['worker_id'] == fresh['worker_id']
            assert plan['learner']['pool_id'] == fresh_mount['identity']['pool_id']
            assert plan['learner']['node_id'] not in [item['node_id'] for item in plan['old']]
        for name in survivors:
            assert success(*operator(name, prepare)) == prepared
        for conflicting in (dict(intent, id=uid()),
                            dict(intent, mount=dict(fresh_mount, provider_volume_id='docker-volume:other'))):
            status, reply = operator(leader, operation('worker.system_replacement.prepare',
                                                       conflicting, conflicting['id']))
            assert status == 409, (status, reply)
        replacement = deepcopy(cluster.config[victim])
        replacement['bind'] = f'127.0.0.1:{free_port()}'
        replacement['node'].update(
            directory=fresh_mount['mount_path'], pool_id=fresh_mount['identity']['pool_id'],
            scheduler_node_id=4, enrollment=fresh, bind=fresh['endpoint'], mount=fresh_mount,
            certificate=str(cluster.root / 'replacement.pem'),
            private_key=str(cluster.root / 'replacement.key'), system_replacement=prepared,
        )
        cluster.config['replacement'] = replacement
        cluster.launch_worker('replacement')
        cluster.wait_workers(names=('replacement',))
        for plan in prepared['groups']:
            route = cluster.admin('storage.replica.placement', {'group': plan['expected']['identity']},
                                  worker='replacement')
            assert len(route['members']) == 3 and plan['learner'] in route['members'], route
            assert all(member['worker_id'] != previous['worker_id'] for member in route['members']), route
        assert send_op(acknowledged) == original
        assert success(*send_op(get(table, 1))) == dict(id=1, value=77)
        receipt_path = f'/v1/requests/{cluster.run_id}/{queued_id}'

        def receipt():
            status, reply = call(cluster.api_urls['api1'], receipt_path,
                                 token=cluster.auth['data_token'], ca=cluster.ca, timeout=5)
            if status in (429, 503, 504):
                return None
            assert status == 200 and reply['state'] not in ('failed', 'abandoned'), (status, reply)
            return reply if reply['state'] == 'succeeded' else None

        settled = wait_for(receipt, 'original queued receipt settled', PROBE_WINDOW)
        assert settled == dict(request_id=queued_id, run_id=cluster.run_id, state='succeeded',
                               result=None, error=None), settled
        assert success(*send_op(get(table, 2))) == dict(id=2, value=88)
        report('O06/O17 system replacement preflight', lost_control_leader=victim,
               groups=len(expected), refused_live_worker=True, refused_without_quorum=True,
               refused_missing_advert=True, refused_data_quorum=True, refused_reused_certificate=True,
               replayed_intent=True, settled_receipts=1,
               probe_recovery_seconds=round(first_read - stopped_at, 3))
    finally:
        cluster.close()


def restart_during_split(cluster, send_op):
    table = create(send_op, 'rep1_split')
    rows = [{'id': key, 'value': key * 3} for key in (1, 9)]
    for row in rows:
        assert success(*send_op(insert(table, row['id'], row['value']))) is None
    parent = initial_range(cluster, table)
    body = operation('storage.shard.split', dict(source=parent, boundary=5, page_bytes=65536))
    split_id = body['request_id']
    for name in WORKERS:
        write_json(cluster.barrier_dir(name) / 'gates.json', {split_id: 'split_children_imported'})

    def request_split(name, request):
        return send(cluster.urls[name], cluster.auth['admin_token'], request,
                    path='/v1/cluster/operations', timeout=120)

    def split():
        for name in WORKERS:
            status, reply = request_split(name, body)
            if status != 503 or (cluster.barrier_dir(name) / f'reached-{split_id}').exists():
                return status, reply
        raise AssertionError('no worker could run the split')

    def imported():
        if pending.done():
            raise AssertionError(f'split returned before reaching the barrier: {pending.result()!r}')
        return next((name for name in WORKERS
                     if (cluster.barrier_dir(name) / f'reached-{split_id}').exists()), None)

    release_join = None
    with ThreadPoolExecutor(1) as pool:
        pending = pool.submit(split)
        try:
            owner = wait_for(imported, 'split reached the imported-child barrier', PROBE_WINDOW)
            victim = next(name for name in WORKERS if name != owner)
            survivors = [name for name in WORKERS if name != victim]
            cluster.workers[victim].kill()
            cluster.workers[victim].stop()
            wait_for(lambda: two_ready(cluster, survivors), 'removed worker during split', PROBE_WINDOW)
            config = retained_config(cluster, victim, survivors)
            preparation = config['node']['rolling']['preparation_id']
            barrier = cluster.barrier_dir(victim)
            release_join = barrier / f'release-{preparation}'
            write_json(barrier / 'gates.json', {preparation: 'rolling_before_join'})
            cluster.config[victim] = config
            cluster.enrollments[victim] = config['node']['enrollment']
            cluster.launch_worker(victim)

            def enrolled():
                assert cluster.workers[victim].alive(), cluster.tail(victim)
                return (barrier / f'reached-{preparation}').exists()

            wait_for(enrolled, 'worker enrolled while split remained unpublished', PROBE_WINDOW)
            assert not pending.done()
        finally:
            for name in WORKERS:
                (cluster.barrier_dir(name) / f'release-{split_id}').touch()
            if release_join is not None:
                release_join.touch()
        interrupted = pending.result()
    assert interrupted[0] != 200, interrupted
    cluster.wait_workers()

    # Recovery increments the owner term, so the paused split cannot publish.
    # An exact retry settles its abort. A fresh request then splits the source.
    def settle_fenced_split():
        result = request_split(owner, body)
        return result if result[0] not in (503, 504) else None

    refusal = wait_for(settle_fenced_split, 'fenced split settled')
    assert refusal == request_split(owner, body), refusal
    status, reply = refusal
    assert status == 409 and reply == {
        'request_id': split_id,
        'error': dict(code='conflict', message='manual split interrupted by worker recovery', outcome='not_applied'),
    }, refusal
    retry = operation('storage.shard.split', body['payload'])

    def publish():
        status, reply = request_split(owner, retry)
        if status in (503, 504):
            return None
        result = success(status, reply)
        return result if result['state'] != 'importing' else None

    result = wait_for(publish, 'replacement split published')
    assert result['state'] == 'published' and len(result['children']) == 2, result
    for row in rows:
        assert success(*send_op(get(table, row['id']))) == row
    for group in result['children']:
        route = cluster.admin('storage.replica.placement', {'group': group})
        assert len(route['members']) == route['record']['replica_count'] == 3, route
    report('O17/D07 restart during split', worker=victim, aborted_split=split_id,
           published_split=retry['request_id'], children=2, voters_per_child=3)


def o06_r01_r03_r06_retained_restart(binary, test_executable, root):
    cluster = Rf3Cluster(binary, root, mounts=True, test_executable=test_executable)
    try:
        cluster.start()
        cluster.wait_direct_active()
        token = cluster.auth['data_token']
        api_url = cluster.api_urls['api1']

        def send_op(body, path='/v1/operations', timeout=15):
            return send(api_url, token, body, ca=cluster.ca, path=path, timeout=timeout)

        def await_receipt(request_id):
            path = f'/v1/requests/{cluster.run_id}/{request_id}'
            expected = dict(request_id=request_id, run_id=cluster.run_id,
                            state='succeeded', result=None, error=None)

            def receipt():
                status, reply = call(api_url, path, token=token, ca=cluster.ca, timeout=5)
                assert status != 404 and (status != 200 or reply['state'] not in ('failed', 'abandoned')), (status, reply)
                return status == 200 and reply == expected

            wait_for(receipt, f'{request_id} reached its original terminal receipt', PROBE_WINDOW)

        table = create(send_op, 'rep1_writes')
        independent = create(send_op, 'rep1_independent_range')
        groups = [initial_range(cluster, table), initial_range(cluster, independent)]
        assert success(*send_op(insert(independent, 1, 777))) is None
        placements = [cluster.admin('storage.replica.placement', {'group': group}) for group in groups]
        queued = insert(table, 1, 1234)
        during_prepare = insert(table, -2, 500)
        queued_id = queued['request_id']
        preparation_write_id = during_prepare['request_id']
        requests = (queued, during_prepare)
        ids = [request['request_id'] for request in requests]
        for name in WORKERS:
            write_json(cluster.barrier_dir(name) / 'gates.json', dict.fromkeys(ids, 'queued'))
            write_json(cluster.barrier_dir(name) / 'inspect.json', ids)
        for request in requests:
            accepted = send_op(request, '/v1/requests')
            assert accepted == (202, dict(request_id=request['request_id'], run_id=cluster.run_id, state='queued')), accepted

        def executing():
            return next((name for name in WORKERS
                         if all((cluster.barrier_dir(name) / f'reached-{id}').exists() for id in ids)), None)

        victim = wait_for(executing, 'accepted operation reached execution gate')
        survivors = [name for name in WORKERS if name != victim]
        for name in survivors:
            cluster.stats(name)
            for request in requests:
                record = json.loads((cluster.barrier_dir(name) / f'record-{request["request_id"]}.json').read_text())
                assert record['accepted'] and json.loads(record['body']) == request, record
                assert record['run_id'] == cluster.run_id, record

        stop = threading.Event()
        writing = threading.Event()
        acknowledged, probes, errors = [], [], []

        def write_load():
            for key in range(2, 10_000):
                if stop.is_set():
                    return
                body = insert(table, key, key * 3)
                if acknowledged:
                    writing.set()
                try:
                    status, reply = send_op(body, timeout=12)
                except TRANSPORT_ERRORS as error:
                    errors.append(repr(error))
                    continue
                if status == 200:
                    assert reply == dict(request_id=body['request_id'], data=None), reply
                    acknowledged.append(body)
                else:
                    assert status in (429, 503, 504), (status, reply)
                    errors.append(reply)
                stop.wait(0.15)

        def probe_range():
            while not stop.is_set():
                try:
                    status, reply = send_op(get(independent, 1), timeout=6)
                except TRANSPORT_ERRORS:
                    stop.wait(0.2)
                    continue
                if status == 200:
                    assert reply['data'] == {'id': 1, 'value': 777}, reply
                    probes.append(time.monotonic())
                stop.wait(0.2)

        with ThreadPoolExecutor(2) as executor:
            writer = executor.submit(write_load)
            prober = executor.submit(probe_range)
            try:
                assert writing.wait(timeout=30), errors
                wait_for(lambda: probes, 'independent range read before fault', PROBE_WINDOW)
                killed_at = time.monotonic()
                cluster.workers[victim].kill()
                cluster.workers[victim].stop()
                assert_start_refused(cluster, victim)
                wait_for(lambda: two_ready(cluster, survivors), 'survivors removed the killed incarnation')
                after_kill = wait_for(lambda: next((sample for sample in probes if sample > killed_at), None),
                                      'independent range read after fault', PROBE_WINDOW)

                # The same prepared incarnation cannot enroll on a fresh mount
                # with no retained identity, votes, or restart key.
                recovered = retained_config(cluster, victim, survivors)
                # This mutation executes while a Data voter resolves to the
                # prepared incarnation, before that incarnation can serve Data.
                for name in survivors:
                    (cluster.barrier_dir(name) / f'release-{preparation_write_id}').touch()
                await_receipt(preparation_write_id)
                assert success(*send_op(get(table, -2))) == {'id': -2, 'value': 500}
                original_enrollment = cluster.enrollments[victim]
                lost_volume = deepcopy(recovered)
                lost_node = lost_volume['node']
                lost_mount = cluster.mount_pool('lost-volume', lost_node['pool_id'], lost_node['enrollment'])
                lost_mount['generation'] = lost_node['mount']['generation']
                lost_node.update(directory=lost_mount['mount_path'], mount=lost_mount)
                cluster.config[victim] = lost_volume
                assert_start_refused(cluster, victim)
                report('O17 lost mount refusal', worker=victim, ready_workers=2)

                cluster.config[victim] = recovered
                cluster.enrollments[victim] = recovered['node']['enrollment']
                cluster.launch_worker(victim)
                recovered_ready = cluster.wait_workers(deadline_seconds=RECOVERY_DEADLINE)
                assert all(view['run_id'] == cluster.run_id for view in recovered_ready.values()), recovered_ready
                peer = next(p for p in cluster.admin('cluster.connection', {})['control_peers']
                            if p['worker_id'] == original_enrollment['worker_id'])
                assert peer == cluster.enrollments[victim] and peer != original_enrollment, peer
                record_path = cluster.barrier_dir(victim) / f'record-{queued_id}.json'
                record_path.unlink(missing_ok=True)
                cluster.stats(victim)
                copied = json.loads(record_path.read_text())
                assert copied['accepted'] and copied['state'] == 'queued', copied
                assert copied['worker']['incarnation'] == peer['incarnation'], copied
                assert copied['run_id'] == cluster.run_id and json.loads(copied['body']) == queued, copied
                for name in WORKERS:
                    (cluster.barrier_dir(name) / f'release-{queued_id}').touch()
                # Confirm the independent range answered while the worker was down.
                assert after_kill - killed_at <= PROBE_WINDOW
            finally:
                stop.set()
            writer.result()
            prober.result()

        gaps = [b - a for a, b in zip(probes, probes[1:] + [time.monotonic()])]
        assert gaps and max(gaps) <= PROBE_WINDOW, gaps
        assert len(acknowledged) >= 2, (len(acknowledged), errors)
        for body in acknowledged:
            row = body['payload']['row']
            assert success(*send_op(get(table, row['id']))) == row, body
        await_receipt(queued_id)
        assert success(*send_op(get(table, 1))) == {'id': 1, 'value': 1234}
        recovered_nodes = []
        expected_voters = []
        for group, before in zip(groups, placements):
            after = cluster.admin('storage.replica.placement', {'group': group})
            assert after == before and len(after['members']) == after['record']['replica_count'] == 3, after
            expected_voters.append(sorted(member['node_id'] for member in after['members']))
            recovered_nodes.append(next(member['node_id'] for member in after['members']
                                        if member['worker_id'] == peer['worker_id']))

        # Inspect the restarted Raft core. Control placement alone was RF3
        # even while its voter was stopped.
        barrier = cluster.barrier_dir(victim)
        write_json(barrier / 'inspect-data-voters.json', list(zip(groups, recovered_nodes)))

        def applied():
            cluster.stats(victim)
            observations = json.loads((barrier / 'data-voters.json').read_text())
            if any(state is None for state in observations):
                return None
            assert [sorted(voters) for voters, _ in observations] == expected_voters, observations
            return [index for _, index in observations]

        baseline = wait_for(applied, 'restarted worker hosts both Data voters', PROBE_WINDOW)
        for table_id in (table, independent):
            assert success(*send_op(insert(table_id, -1, 456))) is None

        def newer_applies():
            current = applied()
            return current if current and all(new > old for old, new in zip(baseline, current)) else None

        applied_after = wait_for(newer_applies, 'restarted voter applied new log entries', PROBE_WINDOW)
        report('O06/R01/R03/R06 retained restart', acknowledged=len(acknowledged), attempts_with_errors=len(errors),
               settled_receipts=2, data_voters=3, ranges=len(groups), applied_before=baseline, applied_after=applied_after,
               max_independent_read_gap_seconds=round(max(gaps), 3), worker=victim)
        restart_during_split(cluster, send_op)
    except BaseException:
        for name in WORKERS:
            print(f'{name}: {cluster.tail(name)}', flush=True)
        raise
    finally:
        cluster.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--test-executable', required=True)
    parser.add_argument('--suite', choices=('all', 'rollback', 'restart', 'replacement-check'), default='all')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='nabedra-rep1-') as directory:
        root = Path(directory).resolve()
        if args.suite in ('all', 'rollback'):
            o19_d03_rollback(args.binary, root / 'rollback')
        if args.suite in ('all', 'restart'):
            assert Rf3Cluster.mounts_available(), 'the RF3 process check requires Linux and passwordless sudo'
            o06_r01_r03_r06_retained_restart(args.binary, args.test_executable, root / 'restart')
        if args.suite in ('all', 'replacement-check'):
            assert Rf3Cluster.mounts_available(), 'the RF3 process check requires Linux and passwordless sudo'
            o06_o17_system_replacement_check(args.binary, args.test_executable, root / 'replacement-check')


if __name__ == '__main__':
    main()

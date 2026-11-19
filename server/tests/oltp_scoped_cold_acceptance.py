#!/usr/bin/env python3.11
r"""Exercise the registered RF3 full-stop subset of O15/O32/R04/R05/Q06.

Run in the benchmark's idle 4 CPU / 8 GB Docker lane from the repository root.

    python3.11 server/tests/oltp_scoped_cold_acceptance.py --worktree "$PWD" \
      --image "$FIXTURE_IMAGE" --binary "$LINUX_NABEDRA_DB" \
      --test-executable "$LINUX_NABEDRA_LIB_TEST" --output "$EVIDENCE_DIR"

Use the same image and Linux executables as the instrumented O30 run. The test
executable must contain the ignored actual_runtime_child and the scoped dispatch,
copy-quorum and Timestamp decision hooks. Use an empty evidence directory.

This covers a pre-Prepare abandonment and a durable Timestamp commit whose
synchronous response was lost. Remaining variants include live holder isolation
and loss, stale-owner resumption, post-fence old COMMIT rejection, prepared but
undecided work, and interruptions during allocator, Status and shard fencing.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import ssl
import subprocess
import sys
import tempfile
from uuid import uuid4

from oltp_scoped_acceptance import (
    ADMIN,
    BARRIERS,
    CHILD,
    IDENTITY_FIELDS,
    POINT_DECISION,
    Driver,
    assert_error,
    envelope,
    pending_receipt,
    read_json,
    require_docker_lane,
    request_digest,
    wait_for,
    write_json,
)


CASES = ('O15', 'O32', 'R04', 'R05', 'Q06')
DISPATCH = 'scoped-dispatch'
RECORDS = ('scoped-payload-quorum', 'scoped-marker-quorum', 'scoped-terminal-quorum',
           DISPATCH, 'scoped-decision')
ABANDONED = 'old-run request abandoned; check durable mutation decision'
TEST_ENTRY = 'exec "$2" worker --bootstrap "/tmp/config/$1.json"'
VOLUME_INIT = 'cp "/fixture/$1-volume.json" /data/.nabedra-docker-volume.json'
TEST_CHILD = (
    'export NABEDRA_OLTP_TEST_REGISTERED_CONFIG="/tmp/config/$1.json"\n'
    f'export NABEDRA_OLTP_TEST_BARRIER_DIR="{BARRIERS}/$1"\n'
    f'exec /fixture-test --exact {CHILD} --ignored --nocapture'
)


def observe(module):
    fixture = Path('/fixture')
    original = BARRIERS / 'original'
    restarted = BARRIERS / 'restarted'
    driver = Driver(module, fixture, instrumented=True)
    driver.roles = sorted(driver.roles, key=lambda role: driver.worker_identity(role)['worker_id'])
    driver.instruments = {role: original / role for role in driver.roles}
    driver.exact_checkpoints = True
    with driver.stack:
        driver.stack.callback(driver.database.close)
        driver.database.seed(2)
        table_id = driver.database.table_id
        driver.placement = driver.database.data_placement()
        pending = envelope('table.update', dict(table_id=table_id, key=1, values={'value': 79}))
        committed = envelope('table.update', dict(table_id=table_id, key=0, values={'value': 73}))
        pending_id, commit_id = pending['request_id'], committed['request_id']
        for directory in driver.instruments.values():
            write_json(directory / 'transaction-gates.json', {
                pending_id: dict.fromkeys(RECORDS, False) | {DISPATCH: True},
                commit_id: dict.fromkeys(RECORDS, False),
            })
            write_json(directory / 'gates.json', {commit_id: POINT_DECISION})

        client = driver.client()
        driver.accepted(driver.submit(client, pending), pending_id)
        role_by_dispatch = {driver.record_file(role, pending_id, DISPATCH): role for role in driver.roles}
        driver.owner, dispatch = wait_for(role_by_dispatch, lambda path: (role_by_dispatch[path], read_json(path)))
        pending_copies = driver.copy_evidence(pending, ('payload', 'marker'))
        assert dispatch == pending_copies['marker']['stamp']['request'], dispatch
        before = driver.receipt(client, pending_id)
        assert before == (200, pending_receipt(driver.run_id, pending_id, 'running')), before

        # Drop the synchronous transport at the native decision, before the
        # owner can resolve Data, project to Status or complete any RAM copy.
        writer = driver.client()
        driver.send_without_reading(writer, committed)
        driver.reached(driver.owner, commit_id, POINT_DECISION)
        writer.close()
        commit_copies = driver.copy_evidence(committed, ('payload', 'marker'))
        commit_dispatch = read_json(driver.record_file(driver.owner, commit_id, DISPATCH))
        assert commit_dispatch == commit_copies['marker']['stamp']['request'], commit_dispatch
        client.close()
        driver.database.client().close()
        write_json(BARRIERS / 'before-stop.json', dict(
            run_id=driver.run_id, owner=driver.worker_identity(driver.owner),
            accepted_pending=before[1], pending_dispatch=dispatch, pending_copies=pending_copies,
            committed_cut=POINT_DECISION, committed_copies=commit_copies,
            digests={request['request_id']: request_digest(request) for request in (pending, committed)},
        ))

        transition = wait_for([BARRIERS / 'restarted.json'], read_json, timeout=180)
        profile = transition['profile']
        new_run = profile['next_run']
        assert profile['previous_run'] == driver.run_id != new_run, profile
        client = driver.client()
        status, health = client.exchange('/health/ready')
        assert status == 200 and health['run_id'] == new_run and health['ready_workers'] == 3, health

        expected = pending_receipt(driver.run_id, commit_id, 'succeeded') | {'result': True}
        recovered = driver.terminal(client, commit_id, True)
        assert recovered == expected and recovered['result'] is True, recovered

        def require_abandoned(reply):
            error = assert_error(reply, 404, pending_id, 'not_found', 'unknown')
            assert error['message'] == ABANDONED, reply
            return error

        abandoned = require_abandoned(driver.receipt(client, pending_id))
        require_abandoned(client.exchange(f'/v1/requests/{new_run}/{pending_id}'))

        def row(key):
            return driver.database.operation('table.get', {'table_id': table_id, 'key': key})

        committed_row, pending_row = row(0), row(1)
        assert committed_row == {'id': 0, 'value': 73}, committed_row
        assert pending_row == {'id': 1, 'value': 0}, pending_row

        # Verify both old UUIDs keep their outcomes after a fresh scoped write
        # changes each row. Re-executing either old body would now be visible.
        driver.run_id = new_run
        fresh = []
        for key in range(2):
            request = envelope('table.update', dict(table_id=table_id, key=key, values={'value': 91}))
            request_id = request['request_id']
            for role in driver.roles:
                path = restarted / role / 'transaction-gates.json'
                write_json(path, read_json(path) | {request_id: {DISPATCH: False}})
            driver.accepted(driver.submit(client, request), request_id)
            bound = wait_for([restarted / role / f'transaction-{request_id}-{DISPATCH}.json'
                              for role in driver.roles], read_json)
            assert bound['run_id'] == new_run and bound['request_id'] == request_id, bound
            assert bound['digest'] == request_digest(request), bound
            driver.terminal(client, request_id, True)
            assert row(key) == {'id': key, 'value': 91}
            fresh.append(request_id)
        require_abandoned(driver.submit(client, pending))
        replayed = module.require_data(*client.exchange('/v1/operations', committed), commit_id)
        assert replayed is True, replayed
        assert client.exchange(f'/v1/requests/{profile["previous_run"]}/{commit_id}') == (200, expected)
        for key in range(2):
            assert row(key) == {'id': key, 'value': 91}
        for request_id in (pending_id, commit_id):
            for role in driver.roles:
                assert not list((restarted / role).glob(f'transaction-{request_id}-*.json')), request_id
        write_json(BARRIERS / 'result.json', dict(
            cases=CASES, profile=profile, recovered_receipt=recovered, abandoned=abandoned,
            committed_row=committed_row, pending_row=pending_row, fresh_request_ids=fresh,
            raw_old_uuids_reconciled=True, old_scoped_dispatches_after_restart=0,
        ))


def inspect(cluster, names):
    return json.loads(cluster.docker('inspect', *names))


def mounted_volume(item):
    mount, = [mount for mount in item['Mounts'] if mount['Destination'] == '/data']
    assert mount['Type'] == 'volume' and mount['RW'], mount
    return {field: mount[field] for field in ('Name', 'Source', 'Destination')}


def control_operation(cluster, module, operation, payload):
    request = envelope(operation, payload)
    control = module.Http(cluster.host_url('w1'), cluster.auth['admin_token'], 15)
    try:
        return module.require_data(*control.exchange(ADMIN, request), request['request_id'])
    finally:
        control.close()


def ready_connection(cluster, module, expected):
    connection = control_operation(cluster, module, 'cluster.connection', {})
    peers = {peer['server_name']: peer for peer in connection['control_peers']}
    assert peers == expected and len(connection['control_peers']) == 3, connection
    assert connection['run_id'] == cluster.run_id and connection['ready_workers'] == 3, connection
    assert connection['cluster_id'] == cluster.cluster_id and connection['database'] == module.DATABASE, connection
    return connection


def full_stop(cluster, module, barriers, directory, config):
    before = read_json(barriers / 'before-stop.json')
    roles = (*module.WORKERS, 'api')
    names = [f'{cluster.prefix}-{role}' for role in roles]
    prior = dict(zip(roles, inspect(cluster, names), strict=True))
    assert all(item['State']['Running'] and not item['State']['Paused'] for item in prior.values())
    volumes = {role: mounted_volume(prior[role]) for role in module.WORKERS}
    assert {mount['Name'] for mount in volumes.values()} == set(cluster.volumes), volumes
    cluster.docker('kill', '--signal', 'SIGKILL', *names)
    stopped = dict(zip(roles, inspect(cluster, names), strict=True))
    old_processes = {}
    for role, name in zip(roles, names, strict=True):
        state = stopped[role]['State']
        assert stopped[role]['Id'] == prior[role]['Id'], stopped[role]['Id']
        assert state['Status'] == 'exited' and not state['Running'] and state['Pid'] == 0, state
        assert state['ExitCode'] == 137 and not state['OOMKilled'], state
        limits = stopped[role]['HostConfig']
        cpu, memory = cluster.saved[role][:2]
        assert limits['NanoCpus'] == round(float(cpu) * module.NANO_CPUS), limits['NanoCpus']
        assert limits['Memory'] == limits['MemorySwap'] == memory, memory
        old_processes[role] = dict(id=stopped[role]['Id'], state=state, mount=volumes.get(role),
                                   nano_cpus=limits['NanoCpus'], memory_bytes=memory)
        with (cluster.args.output / f'original-{role}.log').open('wb') as log:
            subprocess.run(['docker', 'logs', '--tail', '1000', name], stdout=log,
                           stderr=subprocess.STDOUT, timeout=10, check=True)
    write_json(barriers / 'all-stopped.json', old_processes)
    for role in module.WORKERS:
        original = barriers / 'original' / role
        assert not list(original.glob('release-*')), original
        for request_id in before['digests']:
            for stage in ('scoped-decision', 'scoped-terminal-quorum'):
                assert not (original / f'transaction-{request_id}-{stage}.json').exists(), (role, request_id, stage)
    for name in names:
        cluster.docker('rm', name)
        cluster.containers.remove(name)

    profile = dict(previous_run=cluster.run_id, next_run=str(uuid4()), transition_id=str(uuid4()))
    assert before['run_id'] == profile['previous_run']
    enrollments = {}
    for role, old in config.items():
        previous = old['node']['enrollment']
        next_worker = cluster.certificate(role) | dict(
            worker_id=previous['worker_id'], enrollment_revision=previous['enrollment_revision'] + 1,
        )
        assert next_worker['incarnation'] != previous['incarnation']
        assert next_worker['certificate_sha256'] != previous['certificate_sha256']
        enrollments[role] = next_worker
    peers = sorted(enrollments.values(), key=lambda worker: worker['worker_id'])
    pins = sorted(worker['certificate_sha256'] for worker in peers)
    for role, worker_config in config.items():
        node = worker_config['node']
        current = enrollments[role]
        node.update(cold=profile, enrollment=current, control_enrollments=peers, operator_pins=pins)
        mount = node['mount']
        mount['worker'] = {field: current[field] for field in IDENTITY_FIELDS}
        mount['generation'] += 1
        write_json(directory / f'{role}.json', worker_config)
        write_json(barriers / 'restarted' / role / 'transaction-gates.json', {
            request_id: dict.fromkeys(RECORDS, False) for request_id in before['digests']
        })
    write_json(barriers / 'cold-profile.json', dict(profile=profile, enrollments=enrollments))

    cluster.run_id = profile['next_run']
    for role in module.WORKERS:
        cluster.launch_saved(role)
    fresh = dict(zip(module.WORKERS, inspect(cluster, names[:-1]), strict=True))
    for role in module.WORKERS:
        assert fresh[role]['Id'] != stopped[role]['Id'], fresh[role]['Id']
        assert mounted_volume(fresh[role]) == volumes[role], fresh[role]['Mounts']
    worker_urls = {role: cluster.host_url(role) for role in module.WORKERS}
    ready = cluster.worker_ready(worker_urls)
    connection = ready_connection(cluster, module, enrollments)
    api = read_json(directory / 'api.json')
    assert api['topology']['control_group_id'] == connection['control_group_id'], connection
    api['topology']['control_peers'] = connection['control_peers']
    write_json(directory / 'api.json', api)
    cluster.launch_saved('api')
    api_process, = inspect(cluster, [names[-1]])
    assert api_process['Id'] != stopped['api']['Id'], api_process['Id']
    ready.update(cluster.worker_ready({'api': cluster.host_url('api', 'https')},
                 ca=ssl.create_default_context(cafile=str(directory / 'ca.pem')), deadline_seconds=40))
    activation = control_operation(cluster, module, 'queue.scoped.status', {})
    if activation['activation'] is None:
        activation = control_operation(cluster, module, 'queue.scoped.admission',
                                       {'expected': activation, 'enabled': True})
    assert activation['run_id'] == cluster.run_id and activation['version'] == 2 and activation['activation'], activation
    write_json(barriers / 'restarted.json', dict(
        profile=profile, ready=ready, control=connection, volumes=volumes, scoped_activation=activation,
        container_ids={role: item['Id'] for role, item in (fresh | {'api': api_process}).items()},
    ))


def run_fixture(module, args):
    require_docker_lane(module, args.idle_docker_container)
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        raise RuntimeError('use an empty evidence directory')
    args.binary = args.binary.resolve(strict=True)
    args.test_executable = args.test_executable.resolve(strict=True)
    args.worker_cpus = module.DEFAULT_WORKER_CPUS
    args.status_seed_worker = None
    args.tls_key_type = 'rsa'
    args.rpc_trace = False
    args.scoped_oltp = True

    class Fixture(module.Cluster):
        def launch(self, alias, cpu, memory, entry, arguments, *, options=()):
            if alias in module.WORKERS:
                script = arguments[1].replace(TEST_ENTRY, TEST_CHILD)
                assert script != arguments[1], 'registered worker launch command changed'
                arguments = [arguments[0], script, *arguments[2:]]
                options = (*options, '--mount', f'type=bind,source={args.test_executable},target=/fixture-test,readonly')
            self.saved[alias] = (cpu, memory, entry, arguments, options)
            return self.launch_saved(alias)

        def launch_saved(self, alias):
            cpu, memory, entry, arguments, options = self.saved[alias]
            if alias in module.WORKERS:
                generation = 'original' if self.run_id == self.metadata['run_id'] else 'restarted'
                options = (*options, '--mount', f'type=bind,source={barriers / generation},target={BARRIERS}')
                if generation == 'restarted':
                    script = arguments[1].replace(VOLUME_INIT, 'test -f /data/.nabedra-docker-volume.json')
                    assert script != arguments[1], 'registered volume initialization changed'
                    arguments = [arguments[0], script, *arguments[2:]]
            return super().launch(alias, cpu, memory, entry, arguments, options=options)

    with tempfile.TemporaryDirectory(prefix='nabedra-scoped-cold-', dir=args.output) as path:
        directory = Path(path)
        barriers = directory / 'barriers'
        for generation in ('original', 'restarted'):
            for role in module.WORKERS:
                (barriers / generation / role).mkdir(parents=True)
        cluster = Fixture(args, directory)
        cluster.saved = {}
        try:
            cluster.start()
            config = {role: read_json(directory / f'{role}.json') for role in module.WORKERS}
            enrollments = {role: item['node']['enrollment'] for role, item in config.items()}
            ready_connection(cluster, module, enrollments)
            with args.test_executable.open('rb') as executable:
                metadata = cluster.metadata | dict(enrollments=enrollments, contract_references=CASES,
                    registered_test_child_sha256=hashlib.file_digest(executable, 'sha256').hexdigest())
            write_json(directory / 'scoped-metadata.json', metadata)
            write_json(args.output / 'fixture.json', metadata)
            driver = cluster.launch('driver', module.DRIVER_CPU, module.DRIVER_MEMORY, 'python3',
                ['/source/server/tests/oltp_scoped_cold_acceptance.py', '--worktree', '/source', '--driver'],
                options=('--mount', f'type=bind,source={barriers},target={BARRIERS}'))
            wait_for([barriers / 'before-stop.json'], read_json, timeout=100)
            full_stop(cluster, module, barriers, directory, config)
            code = int(cluster.docker('wait', driver, timeout=105))
            if code:
                raise RuntimeError(f'RF3 cold driver exited {code}. See the driver log.')
            result = read_json(barriers / 'result.json')
        finally:
            errors = cluster.finish()
            shutil.copytree(barriers, args.output / 'observed')
            if errors and sys.exc_info()[0] is None:
                raise RuntimeError(f'fixture cleanup or resource verification failed: {errors}')
        print(json.dumps(result, sort_keys=True), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--worktree', type=Path, required=True)
    parser.add_argument('--image')
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--test-executable', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--idle-docker-container', action='append', default=[],
                        help='allow an existing Docker daemon container that was verified idle')
    parser.add_argument('--driver', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    sys.path.insert(0, str(args.worktree / 'scripts'))
    import benchmark_oltp_nabedra

    if args.driver:
        observe(benchmark_oltp_nabedra)
    elif args.image and args.binary and args.test_executable and args.output:
        run_fixture(benchmark_oltp_nabedra, args)
    else:
        parser.error('--image, --binary, --test-executable and --output are required')


if __name__ == '__main__':
    main()

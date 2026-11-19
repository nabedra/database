#!/usr/bin/env python3.11
"""Check one live gateway receipt while the final RF3 queue copy is paused."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time
from uuid import UUID, uuid4


CHILD = 'oltp_runtime::recovery_tests::actual_runtime_child'
GATE = 'before-terminal-copy'
WORKER_BARRIERS = Path('/terminal-copy')


def read_json(path):
    return json.loads(path.read_text())


def wait_for(paths, read, timeout=15):
    deadline = time.monotonic() + timeout
    while True:
        for path in paths:
            try:
                value = read(path)
                if value:
                    return value
            except (FileNotFoundError, json.JSONDecodeError):
                continue
        if time.monotonic() >= deadline:
            raise TimeoutError(f'watchdog waiting for {paths}')
        time.sleep(0.025)


def observation(module, fixture, request_id, run_id):
    worker_paths = {role: WORKER_BARRIERS / role for role in module.WORKERS}
    owner = worker_paths['w1']
    final = worker_paths['w3']
    observer_path = f'/v1/requests/{run_id}/{request_id}'
    control = read_json(fixture / 'auth.json')
    metadata = read_json(fixture / 'q20-metadata.json')
    enrollment = metadata['enrollments']
    database = module.Nabedra(fixture / 'ca.pem', control, 20, metadata)
    update = {'request_id': request_id, 'operation': 'table.update'}
    try:
        database.seed(1)
        update['payload'] = {'table_id': database.table_id, 'key': 0, 'values': {'value': 73}}
        with ExitStack() as stack:
            workers = {
                role: module.Http(f'http://{role}:8000', control['data_token'], 6)
                for role in module.WORKERS
            }
            writer, observer = (
                module.Http('https://api:8000', control['data_token'], 22, database.ca)
                for _ in range(2)
            )
            for client in [writer, observer, *workers.values()]:
                stack.callback(client.close)

            def records():
                snapshots = {}
                for role, worker in workers.items():
                    status, runtime = worker.exchange('/v1/runtime/stats')
                    assert status == 200 and runtime['engine'] == 'oltp', (role, status, runtime)
                    snapshots[role] = read_json(worker_paths[role] / f'record-{request_id}.json')
                return snapshots

            def copy_records():
                return [json.loads(line) for line in
                        (owner / f'identity-copy-{request_id}.jsonl').read_text().splitlines()]

            release = final / f'release-{request_id}'
            release_files = [path / f'transaction-{request_id}-terminal-pin-release.json'
                             for path in worker_paths.values()]
            committed_file = owner / f'decision-{request_id}.json'
            executor = stack.enter_context(ThreadPoolExecutor(max_workers=1))
            write = executor.submit(writer.exchange, '/v1/operations', update)
            try:
                wait_for([final / f'reached-{request_id}'], lambda path: path.read_text() == GATE)
                before = records()
                original = before['w1']
                other = before['w2']
                held = before['w3']
                assert original['authority']['owner']['worker_id'] == enrollment['w1']['worker_id']
                assert all(record['run_id'] == run_id and record['accepted']
                           for record in before.values()), before
                assert original['state'] == other['state'] == 'succeeded'
                assert original['terminal'] == other['terminal']
                assert original['copies_complete'] is False
                assert held['state'] == 'running' and held['terminal'] is None, held
                assert held['version'] < original['version']
                assert all(before[role]['context_hash'] == original['context_hash']
                           for role in module.WORKERS)
                decision = read_json(committed_file)
                assert decision['txn']['request_id'] == request_id and 'commit' in decision['decision']
                assert decision['request'] == original['terminal']['request']
                assert decision['request']['accepted_context'] == original['context_hash']
                start_copies = [event for event in copy_records() if event['state'] == 'succeeded']
                assert all(event['target'] is not None for event in start_copies), start_copies
                assert {event['target']['worker_id'] for event in start_copies} == {
                    enrollment['w1']['worker_id'], enrollment['w2']['worker_id'],
                }, start_copies
                assert not any(path.is_file() for path in release_files)
                observed_status, observed = observer.exchange(observer_path)
                writer_finished = write.done()
                assert not any(path.is_file() for path in release_files)
            finally:
                release.touch()

            result = write.result(timeout=24)
            assert module.require_data(*result, request_id) is True
            proof, position = wait_for(release_files, read_json)
            after = records()
            assert after['w1']['copies_complete'] is True
            assert all(record['state'] == 'succeeded' and record['accepted']
                       and record['version'] == original['version']
                       and record['context_hash'] == original['context_hash']
                       and record['terminal'] == original['terminal']
                       for record in after.values()), after
            assert proof['statement'] == original['terminal'] and proof['acknowledgement_count'] == 3
            assert position['term'] > 0 and position['index'] > 0
            read = original['context']['read']
            assert proof['statement']['pin_identity'] == read['identity']
            assert proof['statement']['storage_pin'] == read['storage_pin']
            end_copies = [event for event in copy_records() if event['state'] == 'succeeded']
            acked = {event['target']['worker_id'] for event in end_copies if event['target']}
            assert acked == {entry['worker_id'] for entry in enrollment.values()}, end_copies
            assert all(event['version'] == original['version']
                       and event['context_hash'] == original['context_hash'] for event in end_copies)
            assert end_copies[-1]['target'] is None, end_copies
            assert read_json(committed_file) == decision
            status, terminal = observer.exchange(observer_path)
            assert status == 200 and terminal == dict(
                request_id=request_id, run_id=run_id, state='succeeded', result=True, error=None)
            assert database.operation('table.get', {'table_id': database.table_id, 'key': 0}) == {
                'id': 0, 'value': 73,
            }
            summary = dict(scenarios=['Q06', 'T04', 'O10', 'O23'], request_id=request_id,
                           observed_while_held=observed, observed_status=observed_status,
                           synchronous_reply_while_held=writer_finished,
                           terminal_version=original['version'],
                           retained_context=original['context_hash'],
                           final_replica_count=len(acked), pin_release_position=position)
            print(json.dumps(summary, sort_keys=True), flush=True)
            assert observed_status == 200 and observed == dict(
                request_id=request_id, run_id=run_id, state='committing', result=None, error=None), summary
            assert not writer_finished, summary
    finally:
        database.close()


def start_fixture(module, args):
    if module.command('docker', 'ps', '--quiet'):
        raise RuntimeError('another Docker workload is using the capped fixture lane')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True)
    args.worker_cpus = module.DEFAULT_WORKER_CPUS
    args.status_seed_worker = None
    args.tls_key_type = 'rsa'
    args.rpc_trace = False
    args.binary = args.binary.resolve(strict=True)
    test_executable = args.test_executable.resolve(strict=True)
    request_id = str(uuid4())
    worker_prefix = uuid4().int & ~0xff
    enrollments = {}

    class Fixture(module.Cluster):
        def certificate(self, name):
            enrollment = super().certificate(name)
            if name in module.WORKERS:
                enrollment['worker_id'] = str(UUID(int=worker_prefix + module.WORKERS.index(name) + 1))
                enrollments[name] = enrollment
            return enrollment

        def launch(self, alias, cpu, memory, entry, arguments, *, options=()):
            if alias in module.WORKERS:
                script = arguments[1].replace(
                    'exec "$2" worker --bootstrap "/tmp/config/$1.json"',
                    f'export NABEDRA_OLTP_TEST_REGISTERED_CONFIG="/tmp/config/$1.json"\n'
                    f'export NABEDRA_OLTP_TEST_BARRIER_DIR="{WORKER_BARRIERS}/$1"\n'
                    f'exec /fixture-test --exact {CHILD} --ignored --nocapture',
                )
                assert script != arguments[1]
                arguments = [arguments[0], script, *arguments[2:]]
                options = (*options,
                           '--mount', f'type=bind,source={test_executable},target=/fixture-test,readonly',
                           '--mount', f'type=bind,source={barriers},target={WORKER_BARRIERS}')
            return super().launch(alias, cpu, memory, entry, arguments, options=options)

    with tempfile.TemporaryDirectory(prefix='nabedra-oltp-q20-', dir=args.output) as temporary:
        fixture = Path(temporary)
        barriers = fixture / 'barriers'
        for role in module.WORKERS:
            role_path = barriers / role
            role_path.mkdir(parents=True)
            module.write_json(role_path / 'inspect.json', [request_id])
            module.write_json(role_path / 'identity-load-ids.json', [request_id])
            module.write_json(role_path / 'transaction-gates.json', {
                request_id: {'terminal-pin-release': False},
            })
        module.write_json(barriers / 'w3' / 'gates.json', {request_id: GATE})
        cluster = Fixture(args, fixture)
        try:
            cluster.start()
            module.write_json(fixture / 'q20-metadata.json', cluster.metadata | {'enrollments': enrollments})
            script = Path(__file__).resolve()
            driver = cluster.launch('driver', module.DRIVER_CPU, module.DRIVER_MEMORY, 'python3',
                ['/fixture-q20.py', '--worktree', '/source', '--driver', request_id, cluster.run_id],
                options=('--mount', f'type=bind,source={script},target=/fixture-q20.py,readonly',
                         '--mount', f'type=bind,source={barriers},target={WORKER_BARRIERS}'))
            status = int(cluster.docker('wait', driver, timeout=105))
            if status:
                raise RuntimeError(f'Q20 correctness assertions exited {status}. See the saved driver log.')
        finally:
            (barriers / 'w3' / f'release-{request_id}').touch()
            errors = cluster.finish()
            shutil.copytree(barriers, args.output / 'observed')
            if errors and sys.exc_info()[0] is None:
                raise RuntimeError(f'Q20 fixture cleanup or resource verification failed: {errors}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worktree', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--image')
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--test-executable', type=Path)
    parser.add_argument('--driver', nargs=2, metavar=('REQUEST', 'RUN'))
    args = parser.parse_args()
    sys.path.insert(0, str(args.worktree / 'scripts'))
    import benchmark_oltp_nabedra as module
    if args.driver:
        observation(module, Path('/fixture'), *args.driver)
    elif all((args.output, args.image, args.binary, args.test_executable)):
        start_fixture(module, args)
    else:
        parser.error('--output, --image, --binary and --test-executable are required')


if __name__ == '__main__':
    main()

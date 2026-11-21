#!/usr/bin/env python3.11
"""Exercise actual table creation across a selected post-sync journal error."""

import argparse
import hashlib
import json
from pathlib import Path
import tempfile

from oltp_direct_acceptance import dump
from oltp_direct_cluster import Rf3Cluster, WORKERS, operation, write_json
from oltp_hardening_acceptance import data_leader
from oltp_runtime_acceptance import DIRECT, RuntimeServer, evidence, wait_until
from oltp_transaction_acceptance import OPERATIONS, REQUESTS, Transactions, assert_binding, passed
from oltp_process import raw


def table_create(name):
    return operation('table.create', dict(name=name, schema={'id': 'int64'}, key='id', format='parquet'))


def select(directory, request):
    (directory / 'post-sync-status').write_text(request)


def arm(directory, command, request):
    pending = directory / 'post-sync-arm.tmp'
    pending.write_text(f'{command} {request}')
    pending.rename(directory / 'post-sync-arm')


def plan_ids(directories, request):
    paths = [directory / f'create-plan-{request}' for directory in directories]
    return [table for path in paths if path.exists() for table in path.read_text().splitlines()]


def wait_fault(directory, command, request, stats):
    def observed():
        marker = directory / 'post-sync-eio'
        if not marker.exists():
            return None
        assert marker.read_text() == f'{command} {request}'
        faulted = stats()['test_pool_sync']
        return faulted if faulted['pools_requiring_recovery'] == 1 else None

    return wait_until(observed, f'post-sync EIO and recovery latch for {request}', timeout=30)


def unknown_before_decision(binary, test_executable):
    with tempfile.TemporaryDirectory(prefix='nabedra-o29-rf1-') as root:
        directory = Path(root)
        (directory / 'enable-post-sync-fault').touch()
        server = RuntimeServer(binary, root)
        server.test_executable = test_executable
        with server:
            create = table_create('unknown_ddl')
            request = create['request_id']
            select(directory, request)
            write_json(directory / 'transaction-gates.json', {request: {'participants': True}})
            status, accepted = raw(server.url, REQUESTS, create, token=server.token)
            assert status == 202 and accepted['request_id'] == request, (status, accepted)
            reached = directory / f'transaction-{request}-participants.json'
            wait_until(reached.is_file, 'table.create planned and reached Prepare', timeout=20)
            original = plan_ids([directory], request)
            assert len(original) == 1, original
            arm(directory, 'prepare_page', request)
            (directory / f'release-{reached.stem}').touch()

            fault = wait_fault(directory, 'prepare_page', request, server.stats)
            assert fault['status']['error']['outcome'] == 'unknown', fault
            for _ in range(2):
                status, response = raw(server.url, OPERATIONS, create, token=server.token)
                assert status == 503 and response['error']['outcome'] == 'unknown', (status, response)
            assert plan_ids([directory], request) == original
            assert server.stats()['test_pool_sync']['pools_requiring_recovery'] == 1
            evidence('O29', 'undecided-create-after-native-eio',
                     request_id=request, table_id=original[0], status=fault['status'])
            # The Raft groups cannot complete a graceful shutdown on a latched
            # pool. The native recovery assertion reopens its own FaultFs case.
            server.stop(abrupt=True)

    passed('O29', 'unreadable-is-uncertain',
           'The native journal synced a real table.create Prepare append, then returned injected EIO. '
           'Status and two same-body retries reported unknown. The planner derived one UUID across all attempts.')


class O29Cluster(Rf3Cluster):
    def launch_worker(self, name):
        directory = self.barrier_dir(name)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / 'enable-post-sync-fault').touch()
        super().launch_worker(name)


def authoritative_after_decision(binary, test_executable):
    with tempfile.TemporaryDirectory(prefix='nabedra-o29-rf3-') as root:
        cluster = O29Cluster(binary, root, test_executable=test_executable,
                             direct=dict.fromkeys(WORKERS, DIRECT), api_direct=(DIRECT, DIRECT))
        fixture = Transactions(cluster, DIRECT)
        try:
            fixture.start()
            create = table_create('decided_ddl')
            request = create['request_id']
            directories = [cluster.barrier_dir(worker) for worker in WORKERS]
            for directory in directories:
                select(directory, request)
            fixture.select(request, {'decision': True})
            fixture.admit(create)
            owner, decision = fixture.reached(request, 'decision')
            binding = assert_binding(decision, fixture, create)
            original, = plan_ids(directories, request)
            expected = {'id': original, **create['payload']}
            catalog = cluster.config[owner]['node']['placements']['catalog']['identity']['group_id']
            leader = data_leader(fixture, catalog)
            target = next(worker for worker in WORKERS if worker not in (owner, leader))
            directory = cluster.barrier_dir(target)
            arm(directory, 'resolve', request)
            fixture.release(request, 'decision')
            wait_fault(directory, 'resolve', request, lambda: cluster.stats(target))

            # The healthy process uses the application Status service with
            # strong quorum authority, even while the original pool is latched.
            proof = cluster.stats(owner)['test_pool_sync']
            assert proof['pools_requiring_recovery'] == 0, proof
            assert proof['status']['result'] == expected, proof
            proof = proof['status']['terminal']['committed']
            assert proof['decision'] == decision, proof
            assert proof['reservation']['request_id'] == request, proof
            assert proof['reservation']['digest'] == hashlib.sha256(
                json.dumps(create, separators=(',', ':')).encode()).hexdigest() == binding['digest']

            fixture.terminal(request, expected)
            assert fixture.api('api2', OPERATIONS, create) == (200, dict(request_id=request, data=expected))
            assert str(fixture.db.table(create['payload']['name']).id) == original
            assert plan_ids(directories, request) == [original]
            assert cluster.stats(target)['test_pool_sync']['pools_requiring_recovery'] == 1
            evidence('O29', 'durable-create-status-on-surviving-quorum', faulted_worker=target,
                     request_id=request, table_id=original, decision=decision, outcome=proof['outcome'])
        except BaseException:
            dump(cluster)
            raise
        finally:
            fixture.release_all()
            cluster.close()

    passed('O29', 'authoritative-decision-replay',
           'A Catalog voter synced a real table.create Resolve append and returned injected EIO. '
           'With its pool still latched, Status returned the exact decision and the application replayed the original UUID.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario', choices=['O29'], required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--test-executable', required=True)
    args = parser.parse_args()
    unknown_before_decision(args.binary, args.test_executable)
    authoritative_after_decision(args.binary, args.test_executable)


if __name__ == '__main__':
    main()

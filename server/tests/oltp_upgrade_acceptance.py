#!/usr/bin/env python3
"""MIG1: move a live format-1 RF3 database to a fresh format-2 RF3 cluster."""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from uuid import UUID

from oltp_direct_acceptance import run_program
from oltp_direct_cluster import DATABASE, ROOT, WORKERS, Rf3Cluster, call, operation, uid, write_json
from oltp_orm_contract import exercise
from oltp_runtime_acceptance import RuntimeServer
from nabedra import Database, col

sys.path.insert(0, str(ROOT / 'scripts'))
from oltp_upgrade_export import archive_path

GUIDE = 'docs/oltp-rocksdb-upgrade.md'
ROW_COUNT = 101_200
BOUNDARY = 50_000
DELETED = set(range(7, ROW_COUNT, 2_011))


class MigrationCluster(Rf3Cluster):
    def __init__(self, binary, root, *, repository=None, archive=None, scope=None):
        super().__init__(binary, root, mounts=True, pool_format_version=1 if repository else 2,
                         direct={name: None for name in WORKERS}, api_direct=(None,))
        self.repository, self.archive, self.scope = repository, archive, scope

    def prepare_workers(self):
        super().prepare_workers()
        for config in self.config.values():
            config['execution']['max_retained_result_bytes_per_worker'] = 64 << 20
            node = config['node']
            if self.repository:
                node['archive_repository'] = str(self.repository)
            if self.archive:
                node['restore'] = {'archives': [str(self.archive)]}
                for group, field in [('control', 'control_group'), ('timestamps', 'timestamp_group'),
                                     ('status', 'initial_status_group'), ('catalog', 'catalog_group')]:
                    node['placements'][group]['identity']['group_id'] = self.scope[field]

    def database(self):
        return Database(url=self.api_urls['api1'], token=self.auth['data_token'], database=DATABASE,
                        ca_file=str(self.ca), timeout=120)

    def operator(self, name, payload, request_id=None):
        return admin_request([self.urls[worker] for worker in self.live()], self.auth['admin_token'],
                             name, payload, request_id)


def admin_request(urls, token, name, payload, request_id=None):
    request = operation(name, payload, request_id)
    errors = []
    for url in urls:
        status, reply = call(url, '/v1/cluster/operations', request, token=token, timeout=180)
        if status == 200:
            assert reply['request_id'] == request['request_id'], reply
            return reply['data']
        errors.append((url, status, reply))
    raise AssertionError((name, errors))


def wait_for(description, condition, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := condition():
            return result
        time.sleep(0.025)
    raise AssertionError(f'timed out waiting for {description}')


def ledger_row(key):
    return dict(id=key, bucket=key % 251, label=f'entry-{key}-é', active=key % 2 == 0)


def initial_group(cluster, table_id):
    shard = hashlib.sha256(UUID(table_id).bytes + b'initial-shard').digest()[:16]
    group = hashlib.sha256(shard + b'data-group').digest()[:16]
    return dict(cluster_id=cluster.cluster_id, database=DATABASE, group_id=str(UUID(bytes=group)),
                kind={'data': {'table_id': table_id, 'shard_id': str(UUID(bytes=shard))}}, group_format_version=1)


def verify_rf3(cluster, groups):
    for group in groups:
        placement = cluster.operator('storage.replica.placement', {'group': group})
        assert placement['record']['identity'] == group, placement
        assert placement['record']['replica_count'] == len(WORKERS), placement
        expected = {(config['node']['enrollment']['worker_id'], config['node']['pool_id'])
                    for config in cluster.config.values()}
        assert {(member['worker_id'], member['pool_id']) for member in placement['members']} == expected, placement


def create_index(url, token, table, name, column, covering):
    index_id, build_id = uid(), uid()
    definition = dict(table_id=table.id, index_id=index_id, name=name, columns=[column], covering=covering)
    admin_request([url], token, 'storage.index.create', definition, build_id)
    selector = dict(table_id=table.id, index_id=index_id, build_id=build_id, attempt=1)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        status, reply = call(url, '/v1/cluster/operations', operation('storage.index.status', selector),
                             token=token, timeout=10)
        if status == 200:
            if reply['data']['state'] == 'ready':
                assert reply['data']['definition'] == definition, reply
                return selector, definition
            assert reply['data']['state'] == 'building', reply
        else:
            # A legacy status read can hit the Catalog intent during Ready publication.
            assert status in (429, 503) and reply['error']['code'] in (
                'storage_unavailable', 'outcome_unknown', 'overloaded'), reply
        time.sleep(0.5)
    raise AssertionError(f'legacy index {name} did not become ready')


def seed_legacy_indexes(binary, root):
    # b29a178's remote Timestamp endpoint rejects index metadata pin binding.
    # Its local admin API can create the indexes. Move those pre-existing
    # tables into the RF3 source using the same unmodified legacy binary.
    local = root / 'legacy-local'
    local.mkdir()
    archive = root / 'legacy-seed-archive'
    with RuntimeServer(binary, local, cli=True) as server:
        token = json.loads((local / 'auth.json').read_text())['admin_token']
        with Database(url=server.url, token=server.token, database=DATABASE) as db:
            ledger = db.create_table('migration_ledger', {'id': int, 'bucket': int, 'label': str, 'active': bool}, key='id')
            locations = db.create_table('migration_locations', {'id': str, 'region': str, 'score': float}, key='id')
            indexes = (create_index(server.url, token, ledger, 'by_bucket', 'bucket', ['label']),
                       create_index(server.url, token, locations, 'by_region', 'region', ['score']))
        manifest = admin_request([server.url], token, 'archive.create',
                                 dict(output=str(archive), owner=dict(backup_id=uid(), attempt=1)))
    print('MIG1 legacy indexes created and archived', flush=True)
    return archive, manifest['scope'], indexes


def seed(cluster, db, indexes):
    ledger = db.table('migration_ledger')
    keys_at_boundary = list(range(BOUNDARY - 4, BOUNDARY + 4))
    assert ledger.insert_many([ledger_row(key) for key in keys_at_boundary]) == len(keys_at_boundary)
    source = initial_group(cluster, ledger.id)
    split_id = uid()
    split = dict(source=source, boundary=BOUNDARY, page_bytes=256 << 10)
    deadline = time.monotonic() + 180
    while True:
        result = cluster.operator('storage.shard.split', split, split_id)
        if result['state'] == 'published':
            children = result['children']
            break
        assert result['state'] == 'importing' and time.monotonic() < deadline, result
    assert len({group['group_id'] for group in children}) == 2, children
    verify_rf3(cluster, children)
    ledger_index, location_index = indexes
    for start in range(0, ROW_COUNT, 256):
        batch = [ledger_row(key) for key in range(start, min(start + 256, ROW_COUNT)) if key not in keys_at_boundary]
        assert ledger.insert_many(batch) == len(batch), start
    for key in sorted(DELETED):
        assert ledger.delete(key) is True, key

    locations = db.table('migration_locations')
    location_rows = [dict(id=f'city-{key:03d}', region='east' if key % 7 == 0 else 'west', score=key + 0.5)
                     for key in range(257)]
    assert locations.insert_many(location_rows) == len(location_rows)
    assert locations.delete('city-000') is True
    empty = db.create_table('migration_empty', {'id': int}, key='id')
    return {
        ledger.name: ([ledger_row(key) for key in range(ROW_COUNT) if key not in DELETED],
                      (ledger_index, 7)),
        locations.name: (location_rows[1:], (location_index, 'east')),
        empty.name: ([], None),
    }, children


def describe(db):
    return {table.name: dict(id=table.id, schema={name: ty.__name__ for name, ty in table.schema.items()},
                             key=table.key, format=table.format)
            for table in db.tables()}


def verify_rows(cluster, db, tables):
    evidence = {}
    for name, (expected, index) in tables.items():
        table = db.table(name)
        digest = hashlib.sha256()
        assert table.query().count() == len(expected), name
        after = None
        for start in range(0, len(expected) + 1, 512):
            query = table.query() if after is None else table.where(col(table.key) > after)
            rows = query.order_by(table.key).limit(512).collect()
            assert rows == expected[start:start + 512], (name, start, len(rows))
            for row in rows:
                digest.update(json.dumps(row, sort_keys=True, ensure_ascii=False).encode() + b'\n')
            if rows:
                after = rows[-1][table.key]
        if name == 'migration_ledger':
            assert all(table.get(key) is None for key in DELETED)
        if name == 'migration_locations':
            assert table.get('city-000') is None
        observed = dict(rows=len(expected), sha256=digest.hexdigest())
        if index:
            (selector, definition), value = index
            status = cluster.operator('storage.index.status', selector)
            assert status['state'] == 'ready' and status['definition'] == definition, status
            index_name, column = definition['name'], definition['columns'][0]
            query = table.where(col(column) == value)
            assert index_name in query.explain(), (name, query.explain())
            indexed = [row for row in expected if row[column] == value]
            assert query.count() == len(indexed) and query.collect() == indexed, name
            observed['index'] = dict(definition=definition, matches=len(indexed))
        evidence[name] = observed
    return evidence


def o01_programs(cluster, db):
    result = exercise(cluster.api_urls['api1'], cluster.auth['data_token'], DATABASE,
                      ca_file=str(cluster.ca), unauthorized_database='other')
    outcomes = {}
    for orm in ('python', 'typescript'):
        table = db.create_table('migration_orm_' + orm,
                                {'id': int, 'name': str, 'score': float, 'active': bool}, key='id')
        try:
            outcomes[orm] = run_program(orm, cluster.api_urls['api1'], table.name,
                                        token=cluster.auth['data_token'], ca=cluster.ca,
                                        snapshot=table.query().collect)
        finally:
            table.drop()
    assert outcomes['python'] == outcomes['typescript'], outcomes
    return result, outcomes['python']


def export_with_retry(cluster, repository):
    backup_id = uid()
    destination = archive_path(repository, cluster.cluster_id, backup_id)
    command = [sys.executable, str(ROOT / 'scripts/oltp_upgrade_export.py'), '--database', DATABASE,
               '--cluster-id', cluster.cluster_id, '--backup-id', backup_id, '--repository', str(repository)]
    for url in cluster.urls.values():
        command.extend(['--worker', url])
    env = dict(os.environ, NABEDRA_ADMIN_TOKEN=cluster.auth['admin_token'])
    start = time.monotonic()
    first = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        def exporting():
            if (destination / 'attempt.json').exists() and not (destination / 'manifest.json').exists():
                assert first.poll() is None
                return True
            if first.poll() is not None:
                raise AssertionError(first.communicate())
            return False

        wait_for('the old worker to start the export', exporting)
        first.kill()
        first.communicate(timeout=15)
        assert first.returncode == -signal.SIGKILL
    finally:
        if first.poll() is None:
            first.kill()
        first.communicate(timeout=15)
    retry = subprocess.run(command, env=env, capture_output=True, text=True, timeout=1800)
    assert retry.returncode == 0, retry.stderr
    manifest = json.loads(retry.stdout)
    assert manifest == json.loads((destination / 'manifest.json').read_text())
    assert manifest == cluster.operator('archive.status', {'output': str(destination)})
    assert manifest['scope']['logical_cluster'] == cluster.cluster_id
    assert manifest['scope']['base_exclusive'] is None
    assert manifest['scope'].get('status_partitions', []) == []
    return destination, manifest, time.monotonic() - start


def interrupt_restore(cluster):
    cluster.prepare_workers()
    cluster.launch_worker('w1')
    worker = cluster.workers['w1']
    root = Path(cluster.config['w1']['node']['directory'])

    def installing():
        assert worker.alive(), cluster.tail('w1')
        return (root / 'RESTORE_IN_PROGRESS.json').exists() and (root / 'pool' / 'kv' / 'CURRENT').exists()

    wait_for('the new worker to start installing archive images', installing, timeout=600)
    worker.kill()
    assert worker.popen.returncode == -signal.SIGKILL
    assert not cluster.apis
    retry = subprocess.run([cluster.binary, 'worker', '--bootstrap', str(cluster.root / 'w1.json')],
                           capture_output=True, text=True, timeout=600)
    assert retry.returncode != 0, 'the interrupted pool became usable without a fresh restore'


def volume_digest(root):
    files = {}
    for path in root.rglob('*'):
        if path.is_file():
            with path.open('rb') as stream:
                files[str(path.relative_to(root))] = hashlib.file_digest(stream, 'sha256').hexdigest()
    return files


def ns22_old_volume_refusal(source, new_binary):
    source.kill_all()
    config = source.config['w1']
    root = Path(config['node']['directory'])
    pool = root / 'pool' / 'NABEDRA_OLTP_POOL.json'
    assert json.loads(pool.read_text())['pool_format_version'] == 1, pool
    before = volume_digest(root)
    path = source.root / 'legacy-w1.json'
    config = copy.deepcopy(config)
    config['node'].pop('restore', None)
    write_json(path, config, 0o600)
    new_config = copy.deepcopy(config)
    new_config['node']['mount']['identity']['pool_format_version'] = 2
    corrected = source.root / 'native-w1.json'
    write_json(corrected, new_config, 0o600)
    for candidate in (path, corrected):
        refused = subprocess.run([new_binary, 'worker', '--bootstrap', str(candidate)], capture_output=True,
                                 text=True, timeout=30)
        assert refused.returncode != 0 and GUIDE in refused.stderr, refused.stderr
        assert 'pool_format_version 1' in refused.stderr and str(root) in refused.stderr, refused.stderr
        assert volume_digest(root) == before, 'new worker modified the source volume'
    return refused.stderr.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--old-binary', type=Path, required=True)
    parser.add_argument('--new-binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    assert Rf3Cluster.mounts_available(), 'migration test requires Linux and passwordless sudo for the real mounts'
    with tempfile.TemporaryDirectory(prefix='nabedra-mig1-') as directory:
        root = Path(directory).resolve()
        repository = root / 'archive-repository'
        repository.mkdir(mode=0o700)
        clusters = []
        try:
            seed_archive, seed_scope, indexes = seed_legacy_indexes(args.old_binary, root)
            source = MigrationCluster(args.old_binary, root / 'old', repository=repository,
                                      archive=seed_archive, scope=seed_scope)
            clusters.append(source)
            source.start()
            print('MIG1 old RF3 ready', flush=True)
            with source.database() as db:
                expected, ranges = seed(source, db, indexes)
                print('MIG1 old tables seeded and split', flush=True)
                old_programs = o01_programs(source, db)
                descriptors = describe(db)
                before = verify_rows(source, db, expected)
            assert set(descriptors) == set(expected)
            assert before['migration_ledger']['rows'] > 100_000
            print('MIG1 source writes stopped', flush=True)
            archive, manifest, export_seconds = export_with_retry(source, repository)
            print('MIG1 full archive verified after interrupted export request', flush=True)
            refusal = ns22_old_volume_refusal(source, args.new_binary.resolve())
            source.close()
            print('MIG1 old volumes refused without changes', flush=True)
            scope = manifest['scope']
            interrupted = MigrationCluster(args.new_binary, root / 'interrupted', archive=archive, scope=scope)
            clusters.append(interrupted)
            import_start = time.monotonic()
            interrupt_restore(interrupted)
            interrupted.close()
            print('MIG1 restore process interrupted on a private mount', flush=True)
            target = MigrationCluster(args.new_binary, root / 'new', archive=archive, scope=scope)
            clusters.append(target)
            target.start()
            restore_seconds = time.monotonic() - import_start
            assert target.cluster_id != source.cluster_id
            verify_rf3(target, [group | {'cluster_id': target.cluster_id} for group in ranges])
            with target.database() as db:
                assert describe(db) == descriptors
                assert verify_rows(target, db, expected) == before
                assert o01_programs(target, db) == old_programs
            evidence = dict(scenario='MIG1', status='passed', rows=before, descriptors=descriptors,
                            ranges=[group['group_id'] for group in ranges], replicas=3,
                            source_cluster=source.cluster_id, target_cluster=target.cluster_id,
                            archive_id=manifest['key']['archive_id'], archive_history_root=manifest['history_root'],
                            export_seconds=export_seconds, restore_seconds_with_retry=restore_seconds,
                            export_interruption='caller killed after attempt creation, same export retried',
                            import_interruption='installer killed after opening target pool, retried on fresh mounts',
                            ns22_refusal=refusal, o01_operations=old_programs[0]['operations'],
                            orm_languages=['python', 'typescript'])
            write_json(args.output / 'results.json', evidence)
            print(json.dumps(evidence, sort_keys=True), flush=True)
        except BaseException:
            if (root / 'legacy-local' / 'server.log').exists():
                (args.output / 'legacy-local.log').write_bytes((root / 'legacy-local' / 'server.log').read_bytes())
            for cluster in clusters:
                for name in [*WORKERS, 'api1']:
                    if log := cluster.tail(name, size=16000):
                        (args.output / f'{cluster.root.name}-{name}.log').write_text(log)
                        print(f'--- {cluster.root.name}/{name} ---\n{log}', file=sys.stderr)
            raise
        finally:
            for cluster in reversed(clusters):
                # close() releases mounts and all child processes, including on assertions.
                if cluster.mounted:
                    cluster.close()


if __name__ == '__main__':
    main()

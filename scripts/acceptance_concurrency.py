#!/usr/bin/env python3
"""Live concurrency, stale-handle and retained-snapshot acceptance.

Import and --help are inert. Live execution requires bootstrap, the public ORM,
a built server and AWS CLI. All remote fixtures remain in the allocated prefix.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import shutil
import signal
import subprocess
from tempfile import TemporaryDirectory
from threading import Barrier, Event
import time
import uuid

from acceptance_http import CheckFailure, Checks, require, safe_failure

_CLIENTS = 4
_BATCH = 5
_BARRIER_TIMEOUT = 20
_CATALOG_LIMIT = 1024 * 1024
_UUID = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
_SNAPSHOT = re.compile(rf'tables/({_UUID})/({_UUID})\.parquet')
_PARTITION = re.compile(rf'tables/({_UUID})/({_UUID})/(0|[1-9][0-9]*)\.parquet')


def _object_match(relative):
    match = _SNAPSHOT.fullmatch(relative) or _PARTITION.fullmatch(relative)
    if match is not None and len(match.groups()) == 3:
        require(int(match[3]) <= 2**32 - 1)
    return match


def _catalog_references(catalog, expected, objects, namespace):
    require(type(catalog) is dict and type(catalog.get('format_version')) is int
            and catalog['format_version'] in (1, 2))
    require(type(catalog.get('revision')) is int and catalog['revision'] > 0)
    require(type(catalog.get('tables')) is dict and set(catalog['tables']) == set(expected))
    references = {}
    for name, (table_id, count) in expected.items():
        entry = catalog['tables'][name]
        require(type(entry) is dict and entry.get('id') == table_id
                and type(entry.get('row_count')) is int and entry['row_count'] == count
                and entry.get('format') == 'parquet' and entry.get('key') == 'id')
        data = ({'layout': 'legacy', 'snapshot': entry.get('snapshot')}
                if catalog['format_version'] == 1 else entry.get('data'))
        require(type(data) is dict)
        if data.get('layout') == 'legacy':
            refs = [data.get('snapshot')]
            require(type(refs[0]) is str and _SNAPSHOT.fullmatch(refs[0]) is not None)
        else:
            require(data.get('layout') == 'partitioned' and type(data.get('files')) is list)
            require(type(entry.get('schema')) is dict)
            key_type = entry['schema'].get(entry['key'])
            require(key_type in ('int64', 'utf8'))
            files = data['files']
            refs = []
            rows = payload = 0
            previous = None
            for index, file in enumerate(files):
                require(type(file) is dict and type(file.get('object')) is str)
                relative = file['object']
                require(_PARTITION.fullmatch(relative) is not None)
                for field in ('row_count', 'byte_size', 'payload_bytes'):
                    require(type(file.get(field)) is int and file[field] >= 0)
                require(file['row_count'] > 0 and 0 < file['byte_size'] <= 67108864
                        and objects.get(relative) == file['byte_size'])
                bounds = file.get('range')
                require(type(bounds) is dict and set(bounds) == {'lower', 'upper'}
                        and bounds['lower'] == previous)
                for bound in bounds.values():
                    if bound is not None:
                        require((key_type == 'utf8' and type(bound) is str)
                                or (key_type == 'int64' and type(bound) is int
                                    and -(2**63) <= bound < 2**63))
                if bounds['lower'] is not None and bounds['upper'] is not None:
                    require(bounds['lower'] < bounds['upper'])
                require(index == len(files) - 1 or bounds['upper'] is not None)
                previous = bounds['upper']
                rows += file['row_count']
                payload += file['payload_bytes']
                refs.append(relative)
            require(previous is None and rows == count and rows <= 100000 and payload <= 64 * 1024 * 1024)
        for relative in refs:
            require(type(relative) is str)
            match = _object_match(relative)
            require(match is not None and match[1] == table_id and relative in objects)
            namespace.key(relative)
        require(len(set(refs)) == len(refs))
        references[name] = refs
    flat = [ref for refs in references.values() for ref in refs]
    require(len(set(flat)) == len(flat))
    return references


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


class _SnapshotReader:
    """Two guarded S3 read operations, with no command dispatch interface."""

    def __init__(self, config, namespace, directory, *, scenario="concurrency"):
        require(scenario in ("concurrency", "partition-workers"))
        self.scenario = scenario
        self.config = config
        self.namespace = namespace
        self.directory = Path(directory)
        self.executable = ('/usr/local/bin/aws' if Path('/usr/local/bin/aws').is_file()
                           else shutil.which('aws'))
        require(self.executable is not None)
        self._guard()

    def _guard(self):
        require(self.config.bucket == 'meshdb-testing' and self.config.region == 'us-east-1')
        self.config._check_namespace(self.namespace)
        number = 6 if self.scenario == 'concurrency' else 10
        require(re.fullmatch(rf'test-{number}-{self.scenario}-[a-z0-9]+(?:-[a-z0-9]+)*/',
                             self.namespace.prefix) is not None)

    def listing(self):
        self._guard()
        try:
            result = subprocess.run([
                self.executable, 's3api', 'list-objects-v2',
                '--bucket', self.config.bucket, '--region', self.config.region,
                '--prefix', self.namespace.prefix, '--max-keys', '100', '--no-paginate',
                '--output', 'json', '--cli-connect-timeout', '5', '--cli-read-timeout', '10',
            ], env=self.config._aws_env(), stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=20, check=False)
            require(result.returncode == 0 and len(result.stdout) <= _CATALOG_LIMIT)
            data = json.loads(result.stdout, object_pairs_hook=_unique_object)
            require(type(data) is dict and data.get('IsTruncated') is False)
            require(data.get('Prefix') == self.namespace.prefix)
            contents = data.get('Contents', [])
            require(type(contents) is list and len(contents) <= 100)
            objects = {}
            for item in contents:
                require(type(item) is dict and type(item.get('Key')) is str)
                key = item['Key']
                require(key.startswith(self.namespace.prefix))
                relative = key[len(self.namespace.prefix):]
                require(relative == 'catalog.json' or _object_match(relative) is not None)
                require(self.namespace.key(relative) == key and relative not in objects)
                size = item.get('Size')
                require(type(size) is int and size > 0)
                objects[relative] = size
            require('catalog.json' in objects)
            return objects
        except (OSError, subprocess.SubprocessError, ValueError, TypeError):
            raise CheckFailure('S3 listing failed') from None

    def read_range(self, relative, start, end):
        self._guard()
        require(relative == 'catalog.json' or _object_match(relative) is not None)
        key = self.namespace.key(relative)
        require(type(start) is int and type(end) is int and 0 <= start <= end)
        require(end - start + 1 <= _CATALOG_LIMIT + 1)
        # The directory belongs to this run and is mode 0700. Each CLI output
        # file is reserved privately, then removed locally even on failure.
        path = self.directory / (uuid.uuid4().hex + '.range')
        try:
            path.touch(mode=0o600, exist_ok=False)
            result = subprocess.run([
                self.executable, 's3api', 'get-object',
                '--bucket', self.config.bucket, '--region', self.config.region,
                '--key', key, '--range', f'bytes={start}-{end}',
                '--output', 'json', '--cli-connect-timeout', '5', '--cli-read-timeout', '10',
                str(path),
            ], env=self.config._aws_env(), stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=20, check=False)
            require(result.returncode == 0 and len(result.stdout) <= _CATALOG_LIMIT)
            metadata = json.loads(result.stdout, object_pairs_hook=_unique_object)
            require(type(metadata) is dict and path.stat().st_size <= end - start + 1)
            raw = path.read_bytes()
            require(type(metadata.get('ContentLength')) is int
                    and metadata['ContentLength'] == len(raw))
            return raw
        except (OSError, subprocess.SubprocessError, ValueError, TypeError):
            raise CheckFailure('S3 range read failed') from None
        finally:
            path.unlink(missing_ok=True)

    def catalog(self, expected):
        objects = self.listing()
        require(objects['catalog.json'] <= _CATALOG_LIMIT)
        raw = self.read_range('catalog.json', 0, _CATALOG_LIMIT)
        require(len(raw) <= _CATALOG_LIMIT and len(raw) == objects['catalog.json'])
        catalog = json.loads(raw, object_pairs_hook=_unique_object)
        references = _catalog_references(catalog, expected, objects, self.namespace)
        return catalog['revision'], objects, references

    def inspect(self, relative, size):
        require(size >= 12)
        first = self.read_range(relative, 0, 3)
        last = self.read_range(relative, size - 8, size - 1)
        require(len(first) == 4 and len(last) == 8 and first == b'PAR1' and last[4:] == b'PAR1')
        footer_length = int.from_bytes(last[:4], 'little')
        require(0 < footer_length <= size - 12)
        return {'relative_key': relative, 'size_bytes': size, 'first_magic': 'PAR1',
                'last_magic': 'PAR1', 'footer_length_sane': True}


def _concurrent_insert(server, table, c, record):
    from nabedra import Database

    barrier = Barrier(_CLIENTS, timeout=_BARRIER_TIMEOUT)
    cancelled = Event()
    failed = Event()

    def cancel(signum, frame):
        record['cancelled'] = True
        cancelled.set()
        barrier.abort()

    def worker(index):
        try:
            client = Database(url=server.api_url, token=server.private_token, timeout=30)
            own_table = client.table(table.name)
            require(own_table.id == table.id)
            rows = [{'id': f'client_{index}_{i}', 'value': index * _BATCH + i}
                    for i in range(_BATCH)]
            barrier.wait()
            if cancelled.is_set() or failed.is_set():
                raise CheckFailure()
            require(own_table.insert_many(rows) == _BATCH)
        except BaseException:
            failed.set()
            barrier.abort()
            raise

    c.label = 'concurrent_insert_many'
    previous = signal.signal(signal.SIGINT, cancel)
    futures = []
    try:
        # Context exit waits for every submitted worker, including failures and
        # cancellation, before run_scenario can stop the server. Never replay.
        with ThreadPoolExecutor(max_workers=_CLIENTS) as executor:
            for index in range(_CLIENTS):
                if cancelled.is_set():
                    break
                futures.append((index, executor.submit(worker, index)))
            for index, future in futures:
                try:
                    future.result()
                    record['checks'].append({'label': f'client_{index}_insert_many', 'status': 'passed'})
                except BaseException as error:
                    failed.set()
                    record['checks'].append({'label': f'client_{index}_insert_many', 'status': 'failed',
                                             'failure_type': safe_failure(error)})
                    if isinstance(error, KeyboardInterrupt):
                        record['cancelled'] = True
        if record['cancelled']:
            raise KeyboardInterrupt()
        require(not failed.is_set() and len(futures) == _CLIENTS)
    finally:
        signal.signal(signal.SIGINT, previous)
    record['checks'].append({'label': 'concurrent_insert_many', 'status': 'passed'})


def _scenario(server, reader, c, record):
    from nabedra import Database, NotFoundError

    db = Database(url=server.api_url, token=server.private_token)
    schema = {'id': str, 'value': int}
    main = c.table(db, 'concurrent_rows', schema, 'id')
    seed = {'id': 'seed', 'value': -1}
    c.equal('seed_main', lambda: main.insert(seed), None)
    c.count(main, 1)
    old = c.table(db, 'replaceable_rows', schema, 'id')
    old_row = {'id': 'retained', 'value': 7}
    c.equal('seed_disposable', lambda: old.insert(old_row), None)
    c.count(old, 1)
    old_query = old.query()
    old_id = old.id
    c.equal('old_query_before_drop', old_query.collect, [old_row])
    baseline_revision, baseline_objects, baseline_refs = c.call('capture_populated_snapshots',
        lambda: reader.catalog({main.name: (main.id, 1), old.name: (old.id, 1)}))
    _concurrent_insert(server, main, c, record)
    expected = [seed] + [{'id': f'client_{index}_{i}', 'value': index * _BATCH + i}
                        for index in range(_CLIENTS) for i in range(_BATCH)]
    expected.sort(key=lambda row: row['id'])
    c.equal('concurrent_exact_rows', lambda: main.query().order_by('id').collect(), expected)
    c.equal('concurrent_exact_keys', lambda: main.query().select('id').order_by('id').collect(),
            [{'id': row['id']} for row in expected])
    c.count(main, len(expected))
    c.equal('drop_disposable', old.drop, None)
    c.fixture(old)['state'] = 'dropped_snapshot_retained'
    new = c.table(db, old.name, schema, 'id')
    c.call('new_immutable_id', lambda: require(new.id != old_id))
    new_row = {'id': 'retained', 'value': 99}
    c.equal('seed_recreated', lambda: new.insert(new_row), None)
    c.equal('old_handle_keeps_id', lambda: old.id, old_id)
    c.error('old_get_not_found', lambda: old.get('retained'), NotFoundError, 'not_found', 404)
    c.error('old_query_not_found', old_query.collect, NotFoundError, 'not_found', 404)
    c.error('old_mutation_not_found', lambda: old.update('retained', {'value': 100}),
            NotFoundError, 'not_found', 404)
    c.equal('recreated_data_unchanged', lambda: new.query().collect(), [new_row])
    c.count(new, 1)
    c.equal('live_table_ids', lambda: [(t.name, t.id) for t in db.tables()],
            [(main.name, main.id), (new.name, new.id)])

    def inspect_retention():
        revision, objects, refs = reader.catalog({main.name: (main.id, len(expected)),
                                                new.name: (new.id, 1)})
        require(revision > baseline_revision)
        snapshots = {key for key in objects if key != 'catalog.json'}
        previous = {key for key in baseline_objects if key != 'catalog.json'}
        require(previous <= snapshots and all(objects[key] == baseline_objects[key] for key in previous))
        require(baseline_refs[main.name] != refs[main.name])
        require(all(ref.startswith(f'tables/{old_id}/') for ref in baseline_refs[old.name]))
        current = {ref for group in refs.values() for ref in group}
        historical = snapshots - current
        require(set(baseline_refs[main.name]) <= historical and set(baseline_refs[old.name]) <= historical)
        require(sum(key.startswith(f'tables/{main.id}/') for key in snapshots) >= _CLIENTS + 1)
        record['storage'] = {'catalog_revision': revision, 'object_count': len(objects),
                             'current_reference_count': len(current), 'historical_count': len(historical),
                             'inspected_snapshots': []}
        # Two current, one populated historical main, one populated dropped.
        for relative in refs[main.name] + refs[new.name] + baseline_refs[main.name] + baseline_refs[old.name]:
            record['storage']['inspected_snapshots'].append(reader.inspect(relative, objects[relative]))

    c.call('catalog_retention_and_parquet_magic', inspect_retention)


def run_scenario(config, binary=None):
    """Run once on the main thread and return only sanitized report fields."""
    started = time.monotonic()
    record = {'scenario': 'concurrency', 'bucket': 'meshdb-testing', 'region': 'us-east-1',
              'allocated_prefix': None, 'fixtures': [], 'checks': [], 'storage': None,
              'concurrency': {'clients': _CLIENTS, 'rows_per_client': _BATCH,
                              'barrier_timeout_seconds': _BARRIER_TIMEOUT, 'request_timeout_seconds': 30},
              'status': 'failed', 'cancelled': False, 'elapsed_seconds': 0}
    c = Checks(record)
    server = None
    resources = None
    try:
        from server_process import ServerProcess
        require(config.bucket == 'meshdb-testing' and config.region == 'us-east-1')
        namespace = config.allocate_prefix(6, 'concurrency')
        record['allocated_prefix'] = namespace.prefix
        c.label = 'private_local_directory'
        resources = TemporaryDirectory(prefix='nabedra-concurrency-')
        reader = _SnapshotReader(config, namespace, resources.name)
        c.label = 'process_setup'
        server = ServerProcess(config, namespace, binary=binary)
        c.call('process_start', server.start)
        _scenario(server, reader, c, record)
        record['status'] = 'passed'
    except BaseException as error:
        record['cancelled'] |= isinstance(error, KeyboardInterrupt)
        record['checks'].append({'label': c.label, 'status': 'failed', 'failure_type': safe_failure(error)})
    finally:
        def cancel_cleanup(signum, frame):
            record['cancelled'] = True
            record['status'] = 'failed'

        previous = signal.signal(signal.SIGINT, cancel_cleanup)
        try:
            if server is not None:
                try:
                    c.equal('graceful_process_stop', server.stop, 0)
                except BaseException as error:
                    record['cancelled'] |= isinstance(error, KeyboardInterrupt)
                    record['status'] = 'failed'
                    record['checks'].append({'label': 'graceful_process_stop', 'status': 'failed',
                                             'failure_type': safe_failure(error)})
            if resources is not None:
                try:
                    resources.cleanup()
                except BaseException as error:
                    record['cancelled'] |= isinstance(error, KeyboardInterrupt)
                    record['status'] = 'failed'
                    record['checks'].append({'label': 'local_temporary_cleanup', 'status': 'failed',
                                             'failure_type': safe_failure(error)})
        finally:
            signal.signal(signal.SIGINT, previous)
    if record['cancelled']:
        record['status'] = 'failed'
    record['elapsed_seconds'] = round(time.monotonic() - started, 3)
    return record


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, help='Built Rust server binary; defaults to ServerProcess debug binary')
    parser.add_argument('--report', type=Path, help='Fresh JSON report path; defaults beneath artifacts/')
    args = parser.parse_args(argv)
    report_path = args.report or Path(__file__).resolve().parents[1] / 'artifacts' / (
        'acceptance-concurrency-' + uuid.uuid4().hex + '.json')
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        output = report_path.open('x', encoding='utf-8')
    except OSError:
        print('report_creation_failed')
        return 1
    report = {'version': 1, 'status': 'failed', 'cancelled': False, 'scenarios': []}
    try:
        from acceptance_support import load_config
        record = run_scenario(load_config(), args.binary)
        report['scenarios'].append(record)
        report['status'] = record['status']
        report['cancelled'] = record['cancelled']
    except BaseException as error:
        report['cancelled'] |= isinstance(error, KeyboardInterrupt)
        report['failure'] = {'label': 'runner_setup', 'failure_type': safe_failure(error)}
    finally:
        def cancel_report(signum, frame):
            report['cancelled'] = True
            report['status'] = 'failed'
            for record in report['scenarios']:
                record['cancelled'] = True
                record['status'] = 'failed'

        previous = signal.signal(signal.SIGINT, cancel_report)
        try:
            with output:
                json.dump(report, output, ensure_ascii=True, indent=2, allow_nan=False)
                output.write('\n')
                if report['cancelled']:
                    # A deferred signal may arrive during JSON serialization.
                    # Rewrite the small report with the final cancellation flag.
                    output.seek(0)
                    output.truncate()
                    json.dump(report, output, ensure_ascii=True, indent=2, allow_nan=False)
                    output.write('\n')
        except OSError:
            print('report_write_failed')
            return 130 if report['cancelled'] else 1
        finally:
            signal.signal(signal.SIGINT, previous)
        print(f'report={report_path}')
    return 130 if report['cancelled'] else (0 if report['status'] == 'passed' else 1)


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""One live partition/workers acceptance scenario. Import and --help are inert.

Requires the public ORM, built server and AWS CLI. Retains every S3 fixture and
its sanitized report. Run only after the coordinator authorizes live execution.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import signal
from tempfile import TemporaryDirectory
import time
import uuid

from acceptance_concurrency import _CATALOG_LIMIT, _SnapshotReader, _unique_object
from acceptance_http import Checks, require, safe_failure


def _row(key):
    # Distinct deterministic text avoids dictionary compression of repeated rows.
    text = ''.join(hashlib.sha256(f'{key}:{block}'.encode('ascii')).hexdigest()
                   for block in range(64))
    return {'id': key, 'text': text, 'score': float(key % 4), 'flag': key % 2 == 0}


def _results(table, expected, c, phase):
    from meshdb import col
    ordered = sorted(expected.values(), key=lambda row: row['id'])
    c.equal(phase + '_all_rows', lambda: table.query().order_by('id').collect(), ordered)
    query = table.where(col('id') >= 0).order_by('score', desc=True).limit(17).select('text', 'flag')
    wanted = sorted(ordered, key=lambda row: (-row['score'], row['id']))[:17]
    c.equal(phase + '_descending_ties_limit_projection', query.collect,
            [{key: row[key] for key in ('text', 'flag')} for row in wanted])
    c.equal(phase + '_limited_count', query.count, min(17, len(ordered)))
    c.count(table, len(ordered))


def _storage(reader, table, count, c, record, phase):
    def inspect():
        revision, objects, refs = reader.catalog({table.name: (table.id, count)})
        raw = reader.read_range('catalog.json', 0, _CATALOG_LIMIT)
        catalog = json.loads(raw, object_pairs_hook=_unique_object)
        require(catalog['format_version'] == 2)
        entry = catalog['tables'][table.name]
        require(entry['data']['layout'] == 'partitioned')
        files = entry['data']['files']
        require(len(files) == len(refs[table.name]))
        if count:
            require(len(files) >= 5)
        else:
            require(not files and set(objects) == {'catalog.json'})
        evidence = {'phase': phase, 'catalog_revision': revision, 'row_count': count,
                    'current_file_count': len(files), 'total_bytes': 0, 'total_payload_bytes': 0,
                    'files': []}
        record['storage'].append(evidence)
        for file in files:
            relative = file['object']
            size = objects[relative]
            require(size == file['byte_size'] and 12 <= size <= 64000 and size <= 67108864)
            content = reader.read_range(relative, 0, size - 1)
            require(len(content) == size and content[:4] == b'PAR1' and content[-4:] == b'PAR1')
            footer = int.from_bytes(content[-8:-4], 'little')
            require(0 < footer <= size - 12)
            evidence['files'].append({'relative_key': relative, 'size_bytes': size,
                                      'row_count': file['row_count'], 'parquet_magic': True})
            evidence['total_bytes'] += size
            evidence['total_payload_bytes'] += file['payload_bytes']
        require(sum(file['row_count'] for file in files) == count)
        require(evidence['total_payload_bytes'] == count * (8 + sum(8 + len(name) for name in table.schema) + 4096 + 8 + 8 + 1))
        return raw, objects, refs[table.name], files
    return c.call(phase + '_catalog_and_all_current_objects', inspect)


def _scenario(server, reader, c, record):
    from meshdb import DB, ConflictError
    db = DB(server.api_url, server.private_token)
    table = c.table(db, 'partition_rows', {'id': int, 'text': str, 'score': float, 'flag': bool}, 'id')
    _storage(reader, table, 0, c, record, 'empty')
    expected = {key: _row(key) for key in range(96)}
    c.equal('insert_96_rows', lambda: table.insert_many(list(reversed(list(expected.values())))), 96)
    _results(table, expected, c, 'inserted')
    baseline = _storage(reader, table, 96, c, record, 'inserted')
    # Ties span every partition, so the global tie order is observable in text.
    c.call('ties_span_partitions', lambda: require(all(file['row_count'] >= 4 for file in baseline[3])))
    for key in (2, 50, 90):
        c.equal(f'get_range_{key}', lambda key=key: table.get(key), expected[key])
    c.equal('update_low_range', lambda: table.update(2, {'flag': False}), True)
    expected[2] = dict(expected[2], flag=False)
    c.equal('upsert_middle_range', lambda: table.upsert(dict(expected[50], score=7.0)), None)
    expected[50] = dict(expected[50], score=7.0)
    c.equal('delete_high_range', lambda: table.delete(90), True)
    del expected[90]
    c.equal('deleted_get', lambda: table.get(90), None)
    _results(table, expected, c, 'mutated')
    mutated = _storage(reader, table, len(expected), c, record, 'mutated')
    def untouched():
        # Files covering none of the touched keys must keep their exact references.
        unaffected = []
        for file in baseline[3]:
            lower, upper = file['range']['lower'], file['range']['upper']
            if not any((lower is None or key >= lower) and (upper is None or key < upper)
                       for key in (2, 50, 90)):
                unaffected.append(file['object'])
        require(unaffected and set(unaffected) <= set(mutated[2]))
        require(set(baseline[1]) <= set(mutated[1]))
    c.call('untouched_references_retained', untouched)
    c.error('atomic_new_then_existing_conflict',
            lambda: table.insert_many([_row(1000), _row(50)]), ConflictError, 'conflict', 409)
    c.equal('atomic_new_row_absent', lambda: table.get(1000), None)
    _results(table, expected, c, 'after_conflict')
    after_conflict = _storage(reader, table, len(expected), c, record, 'after_conflict')
    c.equal('conflict_no_catalog_or_object_publication', lambda: after_conflict, mutated)
    c.equal('empty_batch', lambda: table.insert_many([]), 0)
    c.equal('missing_delete_noop', lambda: table.delete(1001), False)
    c.equal('noops_preserve_catalog_and_objects',
            lambda: _storage(reader, table, len(expected), c, record, 'after_noops'), mutated)
    return table, expected, mutated


def run_scenario(config, binary=None):
    """Run once on the main thread and return only sanitized report fields."""
    started = time.monotonic()
    record = {'scenario': 'partition-workers', 'bucket': 'meshdb-testing', 'region': 'us-east-1',
              'allocated_prefix': None, 'fixtures': [], 'checks': [], 'storage': [],
              'workers': [2, 3], 'partition_size_bytes': 64000,
              'status': 'failed', 'cancelled': False, 'elapsed_seconds': 0}
    c = Checks(record)
    server = None
    servers = []
    resources = None
    try:
        from server_process import ServerProcess
        require(config.bucket == 'meshdb-testing' and config.region == 'us-east-1')
        namespace = config.allocate_prefix(10, 'partition-workers')
        record['allocated_prefix'] = namespace.prefix
        c.label = 'private_local_directory'
        resources = TemporaryDirectory(prefix='meshdb-partitions-')
        reader = _SnapshotReader(config, namespace, resources.name, scenario="partition-workers")
        c.label = 'process_setup'
        server = ServerProcess(config, namespace, binary=binary, workers=2, partition_size="64 KB")
        servers.append(server)
        c.call('process_start', server.start)
        table, expected, before_restart = _scenario(server, reader, c, record)
        c.equal('graceful_stop_before_restart', server.stop, 0)
        c.label = 'restart_setup'
        server = ServerProcess(config, namespace, binary=binary, workers=3, partition_size="64 KB")
        servers.append(server)
        c.call('restart_three_workers', server.start)
        from meshdb import DB
        restored = c.call('rediscover_table', lambda: DB(server.api_url, server.private_token).table(table.name))
        c.equal('restart_identity', lambda: (restored.id, restored.name, restored.schema, restored.key),
                (table.id, table.name, table.schema, table.key))
        _results(restored, expected, c, 'restarted')
        c.equal('restart_preserves_catalog_and_objects',
                lambda: _storage(reader, restored, len(expected), c, record, 'restarted'), before_restart)
        c.equal('post_restart_upsert_new', lambda: restored.upsert(_row(120)), None)
        expected[120] = _row(120)
        _results(restored, expected, c, 'post_restart_mutation')
        _storage(reader, restored, len(expected), c, record, 'post_restart_mutation')
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
            for server in reversed(servers):
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
        'acceptance-partitions-' + uuid.uuid4().hex + '.json')
    report = {'version': 1, 'status': 'failed', 'cancelled': False, 'scenarios': []}
    output = None
    phase = 'report_setup'
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        output = report_path.open('x', encoding='utf-8')
        phase = 'runner_setup'
        from acceptance_support import load_config
        record = run_scenario(load_config(), args.binary)
        report['scenarios'].append(record)
        report['status'] = record['status']
        report['cancelled'] = record['cancelled']
    except BaseException as error:
        report['cancelled'] |= isinstance(error, KeyboardInterrupt)
        report['failure'] = {'label': phase, 'failure_type': safe_failure(error)}
    finally:
        if output is None:
            if not report['cancelled']:
                print('report_creation_failed')
                return 1
            # Save setup cancellation without loading configuration or launching.
            try:
                report_path.parent.mkdir(parents=True, exist_ok=True)
                output = report_path.open('x', encoding='utf-8')
            except (OSError, KeyboardInterrupt):
                print('report_creation_failed')
                return 130
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

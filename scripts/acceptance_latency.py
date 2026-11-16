#!/usr/bin/env python3
"""One-sample live ORM latency acceptance; all S3 fixtures are retained.

Import and --help are inert. Live execution requires the installed local ORM
and a built server. Measurements include client, server, network and AWS work.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import signal
import time
import uuid

from acceptance_http import Checks, require, safe_failure


DATASETS = ((7, 'latency-1', 1), (8, 'latency-100', 100),
            (9, 'latency-1000', 1000))
NOTES = [
    'One sample per operation per dataset; no statistical estimates.',
    'End-to-end public ORM calls include client/server/AWS/network latency.',
    'AWS and network environment are not isolated; no comparative claims.',
    'Seeding, row generation, query construction and checks are outside timing.',
    'Writes grow each table; scans read initial_row_count + 11 rows.',
    'All S3 fixtures are retained; failed mutations are never retried.',
]


def row(index):
    return {'id': index, 'text': f'row-{index:04d}', 'number': index * 3 - 7,
            'value': index / 4.0, 'flag': index % 2 == 0}


def measured(record, operation, before, batch_size):
    """Register an attempt before calling the ORM, retaining unknown outcomes."""
    item = {'operation': operation, 'sample_count': 1, 'status': 'incomplete',
            'row_count_before': before, 'row_count_after': None,
            'batch_size': batch_size, 'returned_count': None, 'elapsed_ms': None}
    record['measurements'].append(item)
    return item


def run_dataset(number, name, initial, config, binary=None):
    """Run on the main thread. Return sanitized results, including on failure."""
    started = time.monotonic()
    record = {'bucket': 'meshdb-testing', 'region': 'us-east-1',
              'allocated_prefix': None, 'initial_row_count': initial,
              'expected_final_row_count': initial + 11, 'final_row_count': None,
              'fixtures': [], 'measurements': [], 'checks': [],
              'process_durations_seconds': {'startup': None, 'shutdown': None,
                                            'total': None},
              'elapsed_seconds': None, 'status': 'failed', 'cancelled': False}
    c = Checks(record)
    server = None
    process_started = None
    try:
        require(config.bucket == 'meshdb-testing' and config.region == 'us-east-1')
        c.label = 'allocate_prefix'
        namespace = config.allocate_prefix(number, name)
        record['allocated_prefix'] = namespace.prefix
        c.label = 'server_setup'
        from server_process import ServerProcess
        from nabedra import Database

        server = ServerProcess(config, namespace, binary=binary)
        process_started = time.monotonic()
        c.call('process_start', server.start)
        record['process_durations_seconds']['startup'] = time.monotonic() - process_started
        c.label = 'client_setup'
        db = Database(url=server.api_url, token=server.private_token)
        schema = {'id': int, 'text': str, 'number': int, 'value': float, 'flag': bool}
        table = c.table(db, 'latency_rows', schema, 'id')
        c.fixture(table).update(schema={'id': 'int', 'text': 'str', 'number': 'int',
                                        'value': 'float', 'flag': 'bool'}, key='id',
                                expected_final_row_count=initial + 11,
                                expected_id_range_inclusive=[0, initial + 10],
                                row_recipe='acceptance_latency.row(index)')
        expected = [row(i) for i in range(initial + 11)]
        seed = expected[:initial]
        single = expected[initial]
        batch = expected[initial + 1:]
        query = table.query().order_by('id')
        c.equal('seed_insert_many', lambda: table.insert_many(seed), initial)
        c.equal('seed_exact_rows', query.collect, seed)
        c.count(table, initial)

        item = measured(record, 'insert', initial, 1)
        c.label = 'timed_insert'
        begin = time.perf_counter_ns()
        result = table.insert(single)
        elapsed = time.perf_counter_ns() - begin
        item['elapsed_ms'] = elapsed / 1_000_000
        c.equal('insert_result', lambda: result, None)
        c.count(table, initial + 1)
        item.update(row_count_after=initial + 1, status='passed')

        item = measured(record, 'insert_many', initial + 1, 10)
        c.label = 'timed_insert_many'
        begin = time.perf_counter_ns()
        result = table.insert_many(batch)
        elapsed = time.perf_counter_ns() - begin
        item['elapsed_ms'] = elapsed / 1_000_000
        c.equal('insert_many_result', lambda: result, 10)
        item['returned_count'] = result
        c.count(table, initial + 11)
        item.update(row_count_after=initial + 11, status='passed')

        item = measured(record, 'query.collect', initial + 11, None)
        c.label = 'timed_collect'
        begin = time.perf_counter_ns()
        result = query.collect()
        elapsed = time.perf_counter_ns() - begin
        item['elapsed_ms'] = elapsed / 1_000_000
        c.equal('final_exact_rows', lambda: result, expected)
        item['returned_count'] = len(result)
        c.count(table, initial + 11)
        item.update(row_count_after=initial + 11, status='passed')
        record['final_row_count'] = initial + 11
        record['status'] = 'passed'
    except BaseException as error:
        record['cancelled'] = isinstance(error, KeyboardInterrupt)
        if record['measurements'] and record['measurements'][-1]['status'] == 'incomplete':
            record['measurements'][-1]['status'] = 'failed'
        record['checks'].append({'label': c.label, 'status': 'failed',
                                 'failure_type': safe_failure(error)})
    finally:
        def cancel_during_cleanup(signum, frame):
            record['cancelled'] = True
            record['status'] = 'failed'

        previous = signal.signal(signal.SIGINT, cancel_during_cleanup)
        try:
            if server is not None:
                shutdown = time.monotonic()
                try:
                    c.equal('graceful_process_stop', server.stop, 0)
                except BaseException as error:
                    record['cancelled'] |= isinstance(error, KeyboardInterrupt)
                    record['status'] = 'failed'
                    record['checks'].append({'label': 'graceful_process_stop',
                                             'status': 'failed',
                                             'failure_type': safe_failure(error)})
                finally:
                    record['process_durations_seconds']['shutdown'] = time.monotonic() - shutdown
                    if process_started is not None:
                        record['process_durations_seconds']['total'] = time.monotonic() - process_started
        finally:
            signal.signal(signal.SIGINT, previous)
    record['elapsed_seconds'] = time.monotonic() - started
    return record


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, help='Built Rust server binary')
    parser.add_argument('--report', type=Path, help='Fresh JSON report path')
    args = parser.parse_args(argv)
    report_path = args.report or Path(__file__).resolve().parents[1] / 'artifacts' / (
        'acceptance-latency-' + uuid.uuid4().hex + '.json')
    report = {'version': 1, 'status': 'failed', 'cancelled': False,
              'sample_count': 1, 'notes': NOTES, 'datasets': []}
    output = None
    phase = 'report_setup'
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        output = report_path.open('x', encoding='utf-8')
        phase = 'runner_setup'
        from acceptance_support import load_config
        config = load_config()
        require(config.bucket == 'meshdb-testing' and config.region == 'us-east-1')
        for number, name, initial in DATASETS:
            record = run_dataset(number, name, initial, config, args.binary)
            report['datasets'].append(record)
            print(f"prefix={record['allocated_prefix'] or 'unallocated'} status={record['status']}")
            if record['cancelled']:
                report['cancelled'] = True
                break
            if record['status'] != 'passed':
                break
        if len(report['datasets']) == len(DATASETS) and all(
                r['status'] == 'passed' for r in report['datasets']):
            report['status'] = 'passed'
    except BaseException as error:
        report['cancelled'] |= isinstance(error, KeyboardInterrupt)
        report['status'] = 'failed'
        report['failure'] = {'label': phase, 'failure_type': safe_failure(error)}
    finally:
        def cancel_during_save(signum, frame):
            report['cancelled'] = True
            report['status'] = 'failed'

        previous = signal.signal(signal.SIGINT, cancel_during_save)
        try:
            # As in acceptance_lifecycle, try exclusive creation again only if
            # cancellation interrupted setup before a report could be opened.
            if output is None and report['cancelled']:
                try:
                    report_path.parent.mkdir(parents=True, exist_ok=True)
                    output = report_path.open('x', encoding='utf-8')
                except OSError:
                    pass
            if output is None:
                print('status=report_creation_failed')
                return 130 if report['cancelled'] else 1
            try:
                with output:
                    cancelled_before_save = report['cancelled']
                    json.dump(report, output, ensure_ascii=True, indent=2, allow_nan=False)
                    output.write('\n')
                    # A first SIGINT during serialization must appear on disk.
                    if report['cancelled'] != cancelled_before_save:
                        output.seek(0)
                        output.truncate()
                        json.dump(report, output, ensure_ascii=True, indent=2, allow_nan=False)
                        output.write('\n')
            except OSError:
                print('status=report_write_failed')
                return 130 if report['cancelled'] else 1
            print(f'report={report_path}')
        finally:
            signal.signal(signal.SIGINT, previous)
    return 130 if report['cancelled'] else (0 if report['status'] == 'passed' else 1)


if __name__ == '__main__':
    raise SystemExit(main())

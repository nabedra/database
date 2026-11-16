#!/usr/bin/env python3
"""Live Rust restart and HTTPS acceptance; populated S3 fixtures are retained.

Import and --help are inert. Live execution needs the installed ORM, a built
Rust server, supplied S3 configuration, and OpenSSL for the HTTPS scenario.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import signal
import shutil
from tempfile import mkdtemp
import time
import uuid

from acceptance_http import Checks, require, safe_failure


def start_server(config, namespace, binary, c, servers, label, **tls):
    from server_process import ServerProcess

    c.label = label
    server = ServerProcess(config, namespace, binary=binary, **tls)
    servers.append(server)
    c.call(label, server.start)
    return server


def restart(config, namespace, binary, c, servers, resources):
    from nabedra import Database

    server = start_server(config, namespace, binary, c, servers, 'first_process_start')
    c.label = 'first_client_setup'
    db = Database(url=server.api_url, token=server.private_token)
    table = c.table(db, 'restart_rows', {'id': str, 'value': int, 'active': bool}, 'id')
    metadata = (table.id, table.name, table.schema, table.key, table.format)
    c.equal('insert_single', lambda: table.insert(
        {'id': 'alpha', 'value': 1, 'active': True}), None)
    c.equal('insert_batch', lambda: table.insert_many([
        {'id': 'beta', 'value': 2, 'active': False},
        {'id': 'deleted', 'value': 3, 'active': True},
        {'id': 'gamma', 'value': 4, 'active': True},
    ]), 3)
    c.equal('update_before_restart', lambda: table.update('alpha', {'value': 10}), True)
    c.equal('delete_before_restart', lambda: table.delete('deleted'), True)
    expected = [
        {'id': 'alpha', 'value': 10, 'active': True},
        {'id': 'beta', 'value': 2, 'active': False},
        {'id': 'gamma', 'value': 4, 'active': True},
    ]
    c.equal('rows_before_restart', lambda: table.query().order_by('id').collect(), expected)
    c.count(table, len(expected))
    c.equal('first_graceful_process_stop', server.stop, 0)
    servers.remove(server)

    # A new process and client discover persisted state without replaying setup.
    second = start_server(config, namespace, binary, c, servers, 'second_process_start')
    c.label = 'second_client_setup'
    fresh_db = Database(url=second.api_url, token=second.private_token)
    recovered = c.call('discover_after_restart', lambda: fresh_db.table('restart_rows'))
    c.equal('metadata_after_restart', lambda: (
        recovered.id, recovered.name, recovered.schema, recovered.key, recovered.format), metadata)
    c.equal('catalog_after_restart', lambda: [(t.id, t.name) for t in fresh_db.tables()],
            [(metadata[0], metadata[1])])
    c.equal('rows_after_restart', lambda: recovered.query().order_by('id').collect(), expected)
    c.count(recovered, len(expected))
    c.equal('deleted_row_still_missing', lambda: recovered.get('deleted'), None)
    c.equal('update_after_restart', lambda: recovered.update('beta', {'value': 20}), True)
    expected[1] = {'id': 'beta', 'value': 20, 'active': False}
    c.equal('rows_after_new_mutation', lambda: recovered.query().order_by('id').collect(), expected)
    c.count(recovered, len(expected))


def https(config, namespace, binary, c, servers, resources):
    from nabedra import Database, TransportError
    from test_tls_material import create_test_tls

    c.label = 'temporary_tls_directory'
    # Explicit cleanup has no GC finalizer: retain files if exit is unconfirmed.
    root = mkdtemp(prefix='nabedra-https-')
    resources.callback(shutil.rmtree, root)
    tls = c.call('create_test_tls', lambda: create_test_tls(Path(root) / 'tls'))
    server = start_server(config, namespace, binary, c, servers, 'https_process_start',
                          tls_cert=tls.server_cert, tls_key=tls.server_key, test_ca=tls.ca_cert)
    c.call('native_https_url', lambda: require(server.api_url.startswith('https://')))
    db = c.call('trusted_client_with_test_ca', lambda: Database(
        url=server.api_url, token=server.private_token, ca_file=str(tls.ca_cert)))
    table = c.table(db, 'https_rows', {'id': str, 'value': int}, 'id')
    rows = [{'id': 'alpha', 'value': 11}, {'id': 'beta', 'value': 22}]
    c.equal('trusted_insert', lambda: table.insert_many(rows), len(rows))
    c.equal('trusted_read', lambda: table.query().order_by('id').collect(), rows)
    c.count(table, len(rows))

    def default_trust_read_fails():
        # Constructor sends nothing. Only this discovery READ may fail.
        untrusted = Database(url=server.api_url, token=server.private_token)
        try:
            untrusted.table('https_rows')
        except TransportError as error:
            require(type(error) is TransportError)
        else:
            require(False)

    c.call('default_trust_read_rejected', default_trust_read_fails)
    # The same endpoint works with the explicitly supplied CA after rejection.
    c.equal('trusted_read_after_rejection', lambda: table.query().order_by('id').collect(), rows)
    c.count(table, len(rows))


SCENARIOS = {'restart': (4, 'restart', restart), 'https': (5, 'https', https)}


def run_scenario(name, config, binary):
    started = time.monotonic()
    record = {'scenario': name, 'bucket': 'meshdb-testing', 'region': 'us-east-1',
              'allocated_prefix': None, 'fixtures': [], 'elapsed_seconds': 0,
              'checks': [], 'status': 'failed', 'cancelled': False}
    c = Checks(record)
    servers = []
    # Local TLS material outlives every stop, including failure cleanup.
    resources = ExitStack()
    try:
        require(config.bucket == 'meshdb-testing' and config.region == 'us-east-1')
        number, prefix_name, scenario = SCENARIOS[name]
        namespace = config.allocate_prefix(number, prefix_name)
        record['allocated_prefix'] = namespace.prefix
        scenario(config, namespace, binary, c, servers, resources)
        record['status'] = 'passed'
    except BaseException as error:
        record['cancelled'] = isinstance(error, KeyboardInterrupt)
        record['checks'].append({'label': c.label, 'status': 'failed',
                                 'failure_type': safe_failure(error)})
    finally:
        def cancel_during_cleanup(signum, frame):
            if not record['cancelled']:
                record['checks'].append({'label': 'cleanup_cancelled', 'status': 'failed',
                                         'failure_type': 'KeyboardInterrupt'})
            record['cancelled'] = True
            record['status'] = 'failed'

        # Defer SIGINT until children are reaped and local TLS files are removed.
        previous_handler = signal.signal(signal.SIGINT, cancel_during_cleanup)
        try:
            for server in reversed(servers):
                try:
                    c.equal('graceful_process_stop', server.stop, 0)
                except BaseException as error:
                    record['cancelled'] |= isinstance(error, KeyboardInterrupt)
                    record['status'] = 'failed'
                    record['checks'].append({'label': 'graceful_process_stop', 'status': 'failed',
                                             'failure_type': safe_failure(error)})
            if all(server.pid is None or server.returncode is not None for server in servers):
                try:
                    resources.close()
                except BaseException as error:
                    record['cancelled'] |= isinstance(error, KeyboardInterrupt)
                    record['status'] = 'failed'
                    record['checks'].append({'label': 'local_tls_cleanup', 'status': 'failed',
                                             'failure_type': safe_failure(error)})
            else:
                record['status'] = 'failed'
                record['checks'].append({'label': 'local_tls_cleanup_exit_unconfirmed',
                                         'status': 'failed', 'failure_type': 'ServerProcessError'})
        finally:
            signal.signal(signal.SIGINT, previous_handler)
    record['elapsed_seconds'] = round(time.monotonic() - started, 3)
    return record


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=[*SCENARIOS, 'all'], default='all')
    parser.add_argument('--binary', type=Path, help='Rust binary; defaults to the process helper debug binary')
    parser.add_argument('--report', type=Path, help='Fresh JSON report path; defaults beneath artifacts/')
    args = parser.parse_args(argv)
    report_path = args.report or Path(__file__).resolve().parents[1] / 'artifacts' / (
        'acceptance-lifecycle-' + uuid.uuid4().hex + '.json')
    report = {'version': 1, 'status': 'failed', 'scenarios': [], 'cancelled': False}
    output = None
    phase = 'report_setup'
    # Refuse existing reports before loading configuration or starting anything.
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        output = report_path.open('x', encoding='utf-8')
        phase = 'runner_setup'
        from acceptance_support import load_config
        config = load_config()
        require(config.bucket == 'meshdb-testing' and config.region == 'us-east-1')
        names = list(SCENARIOS) if args.scenario == 'all' else [args.scenario]
        for name in names:
            record = run_scenario(name, config, args.binary)
            report['scenarios'].append(record)
            print(f"scenario={name} prefix={record['allocated_prefix'] or 'unallocated'} status={record['status']}")
            if record['cancelled']:
                report['cancelled'] = True
                break
        report['status'] = 'passed' if all(r['status'] == 'passed' for r in report['scenarios']) else 'failed'
    except BaseException as error:
        report['cancelled'] |= isinstance(error, KeyboardInterrupt)
        report['failure'] = {'label': phase, 'failure_type': safe_failure(error)}
    finally:
        if output is None:
            if not report['cancelled']:
                print('report_creation_failed')
                return 1
            # Save a cancellation report if setup was interrupted before opening.
            # Exclusive creation still preserves every existing file.
            try:
                report_path.parent.mkdir(parents=True, exist_ok=True)
                output = report_path.open('x', encoding='utf-8')
            except (OSError, KeyboardInterrupt):
                print('report_creation_failed')
                return 130
        try:
            with output:
                json.dump(report, output, ensure_ascii=True, indent=2, allow_nan=False)
                output.write('\n')
        except OSError:
            print('report_write_failed')
            return 130 if report['cancelled'] else 1
        print(f'report={report_path}')
    return 130 if report['cancelled'] else (0 if report['status'] == 'passed' else 1)


if __name__ == '__main__':
    raise SystemExit(main())

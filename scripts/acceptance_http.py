#!/usr/bin/env python3
"""Live Rust HTTP acceptance. Fixtures remain in the guarded S3 namespace.

Import and --help do not load configuration or start a process. Live execution
requires the installed local ORM and a built nabedra-db binary.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request


class CheckFailure(Exception):
    """A labeled acceptance assertion failed; no response data in diagnostics."""


def exact(actual, expected):
    """Compare types and values, including the sign of floating point zero."""
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(exact(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(exact(a, b) for a, b in zip(actual, expected))
    if isinstance(expected, float) and expected == 0.0:
        return actual == expected and math.copysign(1, actual) == math.copysign(1, expected)
    return actual == expected


class Checks:
    def __init__(self, report):
        self.report = report
        self.label = "setup"

    def call(self, label, action):
        self.label = label
        value = action()
        self.report['checks'].append({'label': label, 'status': 'passed'})
        return value

    def equal(self, label, action, expected):
        def verify():
            if not exact(action(), expected):
                raise CheckFailure()
        self.call(label, verify)

    def error(self, label, action, error_type, code, status):
        def verify():
            from nabedra import UnknownOutcomeError
            try:
                action()
            except UnknownOutcomeError:
                raise  # Never retry or continue after an uncertain mutation.
            except error_type as error:
                if (type(error) is not error_type or error.code != code
                        or error.status_code != status or error.outcome != 'not_applied'):
                    raise CheckFailure() from None
            else:
                raise CheckFailure()
        self.call(label, verify)

    def table(self, db, name, schema, key):
        table = self.call('create_' + name, lambda: db.create_table(name, schema, key=key))
        self.report['fixtures'].append({'id': table.id, 'name': table.name,
                                        'row_count': None, 'state': 'retained'})
        return table

    def count(self, table, expected):
        self.equal('count_' + table.name, lambda: table.query().count(), expected)
        self.fixture(table)['row_count'] = expected

    def fixture(self, table):
        return next(item for item in self.report['fixtures'] if item['id'] == table.id)


def scalar_types(db, c, server):
    schema = {'id': str, 'text': str, 'number': int, 'value': float, 'flag': bool}
    strings = c.table(db, 'scalar_strings', schema, 'id')
    rows = [
        {'id': '', 'text': '', 'number': -(2**63), 'value': -0.0, 'flag': False},
        {'id': '雪🙂', 'text': 'café\\path\n"quoted"\t\x00', 'number': 2**63 - 1,
         'value': 0.0, 'flag': True},
        {'id': 'escaped"\\\n', 'text': 'Ελληνικά', 'number': -1, 'value': -123.5, 'flag': False},
        {'id': 'finite', 'text': 'normal', 'number': 0, 'value': 1.25e100, 'flag': True},
    ]
    c.equal('insert_string_scalars', lambda: strings.insert_many(rows), len(rows))
    for i, row in enumerate(rows):
        c.equal(f'string_get_{i}', lambda row=row: strings.get(row['id']), row)
    c.equal('string_collect', lambda: strings.query().order_by('number').collect(),
            sorted(rows, key=lambda row: row['number']))
    c.count(strings, len(rows))
    integers = c.table(db, 'scalar_integers', {**schema, 'id': int}, 'id')
    int_rows = [{**row, 'id': row['number']} for row in rows]
    c.equal('insert_integer_scalars', lambda: integers.insert_many(int_rows), len(int_rows))
    for i, row in enumerate(int_rows):
        c.equal(f'integer_get_{i}', lambda row=row: integers.get(row['id']), row)
    c.equal('integer_collect', lambda: integers.query().order_by('id').collect(),
            sorted(int_rows, key=lambda row: row['id']))
    c.count(integers, len(int_rows))


def orm_api(db, c, server):
    from nabedra import col
    users = c.table(db, 'guide_users', {'id': str, 'age': int, 'active': bool}, 'id')
    c.equal('insert_alice', lambda: users.insert({'id': 'alice', 'age': 24, 'active': True}), None)
    c.equal('insert_generator', lambda: users.insert_many(
        {'id': name, 'age': age, 'active': True}
        for name, age in [('bob', 17), ('carol', 30)]), 2)
    c.equal('get_alice', lambda: users.get('alice'), {'id': 'alice', 'age': 24, 'active': True})
    c.equal('update_alice', lambda: users.update('alice', {'age': 25}), True)
    c.equal('upsert_replace', lambda: users.upsert({'id': 'carol', 'age': 31, 'active': False}), None)
    adults = users.where((col('age') >= 18) & ((col('active') == True) | ~(col('id') == 'carol')))
    query = adults.select('id', 'age').order_by('age', desc=True).limit(100)
    c.equal('guide_collect', query.collect, [{'id': 'alice', 'age': 25}])
    c.equal('guide_count', query.count, 1)
    c.call('explain_text', lambda: require(type(query.explain()) is str))
    discovered = c.call('discover', lambda: db.table('guide_users'))
    c.equal('discovered_metadata', lambda: (discovered.id, discovered.name, discovered.schema,
                                           discovered.key, discovered.format),
            (users.id, users.name, users.schema, 'id', 'parquet'))
    c.equal('list_tables', lambda: [(t.id, t.name) for t in db.tables()], [(users.id, users.name)])
    base = users.query()
    young = base.where(col('age') < 18)
    active = base.where(active=True).order_by('age', desc=True).select('id').limit(1)
    c.equal('branch_young', young.collect, [{'id': 'bob', 'age': 17, 'active': True}])
    c.equal('branch_active', active.collect, [{'id': 'alice'}])
    c.equal('limit_in_count', active.count, 1)
    c.equal('base_unchanged', base.count, 3)
    c.equal('adults_unchanged', adults.collect, [{'id': 'alice', 'age': 25, 'active': True}])
    c.equal('upsert_new', lambda: users.upsert({'id': 'dave', 'age': 40, 'active': True}), None)
    c.equal('upsert_read', lambda: users.get('dave'), {'id': 'dave', 'age': 40, 'active': True})
    c.equal('delete_bob', lambda: users.delete('bob'), True)
    c.equal('delete_missing', lambda: users.delete('bob'), False)
    c.equal('get_missing', lambda: users.get('bob'), None)
    c.equal('update_missing', lambda: users.update('bob', {'age': 18}), False)
    c.count(users, 3)
    disposable = c.table(db, 'disposable', {'id': int}, 'id')
    c.equal('populate_disposable', lambda: disposable.insert({'id': 1}), None)
    c.count(disposable, 1)
    c.equal('drop_disposable', disposable.drop, None)
    c.fixture(disposable)['state'] = 'dropped_snapshot_retained'
    c.equal('list_after_drop', lambda: [t.name for t in db.tables()], ['guide_users'])


def require(condition):
    if not condition:
        raise CheckFailure()


def unsupported(transport):
    """Bypass only the operation-name guard, retaining the ORM's HTTP opener."""
    from nabedra import NabedraError
    request_id = str(uuid.uuid4())
    body = json.dumps({'request_id': request_id, 'operation': 'acceptance.unsupported',
                       'payload': {}}).encode('utf-8')
    request = Request(transport._url, data=body, method='POST', headers={
        'Content-Type': 'application/json', 'Accept': 'application/json',
        'Authorization': 'Bearer ' + transport._token})
    try:
        response = transport._opener.open(request, timeout=transport._timeout)
    except HTTPError as error:
        response = error
    with response:
        status = response.status
        content_type = response.headers.get('Content-Type', '').split(';', 1)[0]
        raw = response.read(8193)
    require(status == 400 and content_type == 'application/json' and len(raw) <= 8192)
    envelope = json.loads(raw)
    require(type(envelope) is dict and set(envelope) == {'request_id', 'error'})
    require(envelope['request_id'] in (None, request_id))
    error = envelope['error']
    require(type(error) is dict and set(error) == {'code', 'message', 'outcome'})
    require(error['code'] == 'unsupported_operation' and error['outcome'] == 'not_applied'
            and type(error['message']) is str)
    raise NabedraError('Unsupported operation probe', code='unsupported_operation',
                      status_code=status, outcome='not_applied')


def atomic_errors(db, c, server):
    from nabedra import AuthenticationError, ConflictError, NabedraError, NotFoundError, ValidationError
    from nabedra.client import _Transport
    table = c.table(db, 'atomic_rows', {'id': str, 'value': int}, 'id')
    original = {'id': 'original', 'value': 7}
    c.equal('seed_original', lambda: table.insert(original), None)
    c.count(table, 1)

    def unchanged(label, missing):
        c.equal(label + '_rows', lambda: table.query().collect(), [original])
        for key in missing:
            c.equal(label + '_absent_' + key, lambda key=key: table.get(key), None)

    c.error('duplicate_key', lambda: table.insert({'id': 'original', 'value': 99}),
            ConflictError, 'conflict', 409)
    unchanged('duplicate', [])
    c.error('late_existing_duplicate', lambda: table.insert_many([
        {'id': 'early', 'value': 1}, {'id': 'original', 'value': 99}]),
        ConflictError, 'conflict', 409)
    unchanged('late_existing', ['early'])
    c.error('late_batch_duplicate', lambda: table.insert_many([
        {'id': 'batch_new', 'value': 1}, {'id': 'batch_new', 'value': 2}]),
        ConflictError, 'conflict', 409)
    unchanged('late_batch', ['batch_new'])
    transport = _Transport(server.api_url, server.private_token)
    c.error('malformed_row_type', lambda: transport.request('table.insert', {
        'table_id': table.id, 'row': {'id': 'bad_row', 'value': 'wrong'}}),
        ValidationError, 'validation_error', 400)
    unchanged('malformed_row', ['bad_row'])
    c.error('malformed_batch_type', lambda: transport.request('table.insert_many', {
        'table_id': table.id, 'rows': [{'id': 'valid_early', 'value': 1},
                                     {'id': 'bad_batch', 'value': False}]}),
        ValidationError, 'validation_error', 400)
    unchanged('malformed_batch', ['valid_early', 'bad_batch'])
    c.error('unsupported_operation', lambda: unsupported(transport), NabedraError, 'unsupported_operation', 400)
    wrong = _Transport(server.api_url, server.private_token + '-wrong')
    c.error('wrong_token', lambda: wrong.request('table.list', {}), AuthenticationError, 'unauthorized', 401)
    c.error('missing_table', lambda: db.table('absent_table'), NotFoundError, 'not_found', 404)
    stale = c.table(db, 'stale_table', {'id': int}, 'id')
    c.equal('seed_stale', lambda: stale.insert({'id': 1}), None)
    c.count(stale, 1)
    stale_query = stale.query()
    c.equal('drop_stale', stale.drop, None)
    c.fixture(stale)['state'] = 'dropped_snapshot_retained'
    c.error('dropped_name', lambda: db.table('stale_table'), NotFoundError, 'not_found', 404)
    c.error('stale_id', lambda: stale.get(1), NotFoundError, 'not_found', 404)
    c.error('stale_query', stale_query.collect, NotFoundError, 'not_found', 404)
    c.count(table, 1)


SCENARIOS = {'types': (1, 'scalar-types', scalar_types),
             'api': (2, 'orm-api', orm_api),
             'atomic': (3, 'atomic-errors', atomic_errors)}


def safe_failure(error):
    # Never serialize messages, reprs, response payloads, or arbitrary class names.
    allowed = {'CheckFailure', 'ConfigError', 'ServerProcessError', 'ValidationError',
               'AuthenticationError', 'NotFoundError', 'ConflictError', 'LimitExceededError',
               'OverloadedError', 'StorageError', 'TransportError', 'ProtocolError',
               'UnknownOutcomeError', 'NabedraError', 'KeyboardInterrupt', 'ModuleNotFoundError'}
    name = type(error).__name__
    return name if name in allowed else 'UnexpectedFailure'


def run_scenario(name, config, binary):
    started = time.monotonic()
    record = {'scenario': name, 'bucket': 'meshdb-testing', 'region': 'us-east-1',
              'allocated_prefix': None, 'fixtures': [], 'elapsed_seconds': 0,
              'checks': [], 'status': 'failed'}
    c = Checks(record)
    server = None
    try:
        from nabedra import Database
        from server_process import ServerProcess
        number, prefix_name, scenario = SCENARIOS[name]
        namespace = config.allocate_prefix(number, prefix_name)
        record['allocated_prefix'] = namespace.prefix
        c.label = 'process_setup'
        server = ServerProcess(config, namespace, binary=binary)
        c.call('process_start', server.start)
        db = Database(url=server.api_url, token=server.private_token)
        scenario(db, c, server)
        record['status'] = 'passed'
    except BaseException as error:
        record['checks'].append({'label': c.label, 'status': 'failed',
                                 'failure_type': safe_failure(error)})
        if isinstance(error, KeyboardInterrupt):
            record['cancelled'] = True
    finally:
        if server is not None:
            try:
                c.equal('graceful_process_stop', server.stop, 0)
            except BaseException as error:
                record['status'] = 'failed'
                record['checks'].append({'label': 'graceful_process_stop', 'status': 'failed',
                                         'failure_type': safe_failure(error)})
                if isinstance(error, KeyboardInterrupt):
                    record['cancelled'] = True
        record['elapsed_seconds'] = round(time.monotonic() - started, 3)
    return record


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=[*SCENARIOS, 'all'], default='all')
    parser.add_argument('--binary', type=Path, help='Rust server binary; defaults to the process helper debug binary')
    parser.add_argument('--report', type=Path, help='Fresh JSON report path; defaults beneath artifacts/')
    args = parser.parse_args(argv)
    report_path = args.report or Path(__file__).resolve().parents[1] / 'artifacts' / (
        'acceptance-http-' + uuid.uuid4().hex + '.json')
    # Reserve a fresh report before reading secrets or launching anything.
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        output = report_path.open('x', encoding='utf-8')
    except OSError:
        print('report_creation_failed')
        return 1
    report = {'version': 1, 'status': 'failed', 'scenarios': []}
    try:
        from acceptance_support import load_config
        config = load_config()
        require(config.bucket == 'meshdb-testing' and config.region == 'us-east-1')
        names = list(SCENARIOS) if args.scenario == 'all' else [args.scenario]
        for name in names:
            record = run_scenario(name, config, args.binary)
            report['scenarios'].append(record)
            print(f"scenario={name} prefix={record['allocated_prefix'] or 'unallocated'} status={record['status']}")
            if record.get('cancelled'):
                report['cancelled'] = True
                break
        report['status'] = 'passed' if all(r['status'] == 'passed' for r in report['scenarios']) else 'failed'
    except BaseException as error:
        report['failure'] = {'label': 'runner_setup', 'failure_type': safe_failure(error)}
        if isinstance(error, KeyboardInterrupt):
            report['cancelled'] = True
    finally:
        with output:
            json.dump(report, output, ensure_ascii=True, indent=2, allow_nan=False)
            output.write('\n')
        print(f'report={report_path}')
    if report.get('cancelled'):
        return 130
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())

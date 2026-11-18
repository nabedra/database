#!/usr/bin/env python3.11
"""The same public ORM program for OLAP/OLTP O01 production fixtures.

Server startup, TLS and failure injection belong to the owning scenario. This
program does not substitute a mock backend or certify distributed behavior.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'orm/python/src'))
from nabedra import AuthenticationError, ConflictError, Database, NotFoundError, col

OPERATIONS = (
    'table.create', 'table.describe', 'table.list', 'table.insert', 'table.insert_many',
    'table.get', 'table.update', 'table.upsert', 'table.delete', 'table.drop',
    'query.collect', 'query.count', 'query.explain',
)


def rejected(kind, action):
    try:
        action()
    except kind as error:
        assert error.outcome == 'not_applied', (error.code, error.outcome)
        return error.code
    raise AssertionError(f'{kind.__name__} was not raised')


def exercise(url, token, database, *, ca_file=None, unauthorized_database=None, scales=(1, 100, 1000)):
    """Return engine-independent observations after asserting every operation."""
    options = dict(url=url, token=token, database=database, ca_file=ca_file, timeout=20)
    db = Database(**options, mode='olap')
    prefix = 'orm_' + uuid.uuid4().hex[:16]
    owned = []
    observations = dict(operations=list(OPERATIONS), scales=[])
    try:
        schema = {'id': str, 'age': int, 'score': float, 'active': bool}
        users = db.create_table(prefix, schema, key='id')
        owned.append(users)
        assert str(uuid.UUID(users.id)) == users.id
        assert users.format == 'parquet' and users.schema == schema and users.key == 'id'
        assert db.table(prefix).id == users.id
        assert next(t for t in db.tables() if t.name == prefix).id == users.id
        assert Database(**options, mode='oltp').table(prefix).id == users.id
        alice = {'id': 'alice', 'age': 24, 'score': -0.0, 'active': True}
        bob = {'id': 'bob', 'age': 17, 'score': 0.5, 'active': False}
        carol = {'id': 'carol', 'age': 24, 'score': 1.5, 'active': True}
        assert users.insert(alice) is None
        submitted = users.insert_many([bob, carol])
        assert type(submitted) is int and submitted == 2
        assert users.insert_many([]) == 0
        assert users.get('alice') == alice and users.get('missing') is None
        decoded = users.get('alice')
        assert [type(decoded[key]) for key in schema] == [str, int, float, bool]
        selected = users.where((col('age') >= 18) & ~(col('active') == False))
        rows = selected.order_by('age', desc=True).select('id').limit(2).collect()
        assert rows == [{'id': 'alice'}, {'id': 'carol'}]
        assert selected.limit(1).count() == 1
        assert type(selected.count()) is int
        assert isinstance(selected.explain(), str) and selected.explain()
        assert users.update('bob', {'age': 18, 'active': True}) is True
        assert users.update('missing', {'age': 1}) is False
        assert users.upsert({**alice, 'age': 25}) is None
        assert users.get('alice')['age'] == 25
        assert users.delete('missing') is False
        assert users.delete('carol') is True
        observations['conflict'] = rejected(ConflictError, lambda: users.insert_many([
            {'id': 'new', 'age': 30, 'score': 2.0, 'active': True}, alice]))
        assert users.get('new') is None and users.get('alice')['age'] == 25
        identity = str(uuid.uuid4())
        payload = {'table_id': users.id, 'row': {'id': 'receipt', 'age': 42, 'score': 2.5, 'active': True}}
        receipt = db.submit('table.upsert', payload, request_id=identity).wait(timeout=20)
        assert receipt.request_id == identity and receipt.state == 'succeeded' and receipt.result is None
        assert db.receipt(receipt.run_id, identity).wait(timeout=20).result is None
        replay = db.submit('table.upsert', payload, request_id=identity).wait(timeout=20)
        assert replay.request_id == identity and replay.result is None
        rejected(ConflictError, lambda: db.submit('table.upsert', {
            **payload, 'row': {**payload['row'], 'age': 43}}, request_id=identity))
        assert users.get('receipt') == payload['row']
        observations['unauthorized'] = rejected(AuthenticationError, lambda: Database(
            **{**options, 'token': 'invalid-' + uuid.uuid4().hex}).tables())
        if unauthorized_database:
            observations['database_scope'] = rejected(AuthenticationError, lambda: Database(
                **{**options, 'database': unauthorized_database}).tables())
        observations['ordered_rows'] = users.query().collect()
        for size in scales:
            table = db.create_table(prefix + '_' + str(size), {'id': int, 'value': str}, key='id')
            owned.append(table)
            data = [{'id': i, 'value': f'row-{i}-é'} for i in range(size)]
            count = table.insert_many(data)
            assert type(count) is int and count == size
            assert table.query().count() == size
            assert table.get(size - 1) == data[-1]
            assert table.where(col('id') >= max(0, size - 3)).collect() == data[-3:]
            assert table.query().limit(1).count() == 1
            observations['scales'].append(size)
            table.drop()
        original_id = users.id
        users.drop()
        observations['stale_handle'] = rejected(NotFoundError, lambda: users.get('alice'))
        replacement = db.create_table(prefix, schema, key='id')
        owned.append(replacement)
        assert replacement.id != original_id
        return observations
    finally:
        original_failure = sys.exc_info()[0] is not None
        for table in reversed(owned):
            try:
                table.drop()
            except NotFoundError:
                pass
            except BaseException:
                if not original_failure:
                    raise
                print("ORM cleanup failed while preserving original exception", file=sys.stderr)
                import traceback
                traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--token-env', default='NABEDRA_TEST_TOKEN')
    parser.add_argument('--database', default='test')
    parser.add_argument('--unauthorized-database')
    parser.add_argument('--ca-file')
    args = parser.parse_args()
    result = exercise(args.url, os.environ[args.token_env], args.database,
                      ca_file=args.ca_file, unauthorized_database=args.unauthorized_database)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()

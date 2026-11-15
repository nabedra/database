"""Table behavior through a real transport and one local HTTP fixture."""
import json
import threading
import unittest
from dataclasses import FrozenInstanceError
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import meshdb
from meshdb import errors
from meshdb.client import _Transport

LIMIT = 8 * 1024 * 1024
TABLE_ID = '12345678-1234-4234-8234-123456789ABC'


def descriptor():
    return dict(id=TABLE_ID, name='users', schema={'id': 'utf8', 'age': 'int64', 'active': 'bool'}, key='id', format='parquet')


class TableTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers['Content-Length']))
                request = json.loads(raw)
                cls.seen.append((request, raw))
                status = cls.status
                body = {'request_id': request['request_id']}
                body['data' if status == 200 else 'error'] = cls.result
                encoded = json.dumps(body).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, *args):
                pass
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        type(self).seen = []
        self.reply(None)
        self.transport = _Transport(f'http://127.0.0.1:{self.server.server_port}', 'test', timeout=5)

    def reply(self, result, status=200):
        type(self).result, type(self).status = result, status

    def table(self, data=None):
        from meshdb.table import _table_from_descriptor
        return _table_from_descriptor(self.transport, descriptor() if data is None else data)

    def test_handle_owns_metadata_and_query_construction_is_local(self):
        source = descriptor()
        table = self.table(source)
        self.assertIsInstance(table, meshdb.Table)
        source['schema']['age'] = 'utf8'
        source['id'] = 'changed'
        schema = table.schema
        schema['age'] = str
        self.assertEqual(table.schema, {'id': str, 'age': int, 'active': bool})
        for attr, value in [('id', 'new'), ('name', 'new'), ('key', 'age'), ('format', 'csv'), ('schema', {})]:
            with self.assertRaises((FrozenInstanceError, AttributeError, TypeError)):
                setattr(table, attr, value)
        query = table.where(age=18).select('id')
        self.assertIs(query._table, table)
        self.assertEqual(query._to_wire()['filter'], {'op': 'eq', 'column': 'age', 'value': 18})
        self.assertEqual(self.seen, [])
        from meshdb.table import _table_from_descriptor
        self.reply(descriptor())
        decoded = self.transport.request('table.describe', {'name': 'users'},
                                         decode=lambda data: _table_from_descriptor(self.transport, data))
        self.assertEqual(decoded.id, TABLE_ID)
        self.reply({'id': []})
        for operation, expected in [('table.describe', errors.ProtocolError),
                                    ('table.create', errors.UnknownOutcomeError)]:
            with self.assertRaises(expected):
                self.transport.request(operation, {}, decode=lambda data: _table_from_descriptor(self.transport, data))
        bad = [None, [], {}, {**descriptor(), 'extra': 1}, {**descriptor(), 'id': 'bad'},
               {**descriptor(), 'id': 1}, {**descriptor(), 'name': 'bad-name'},
               {**descriptor(), 'schema': {'id': 'unknown'}}, {**descriptor(), 'key': None},
               {**descriptor(), 'key': 'active'}, {**descriptor(), 'format': 'csv'}]
        for data in bad:
            with self.subTest(data=data), self.assertRaises((errors.ValidationError, errors.ProtocolError)):
                from meshdb.table import _table_from_descriptor
                _table_from_descriptor(self.transport, data)

    def test_crud_payloads_and_server_results(self):
        table = self.table()
        row = {'id': 'é', 'age': 24, 'active': True}
        cases = [('insert', (row,), None, {'row': row}), ('upsert', (row,), None, {'row': row}),
                 ('get', ('é',), row, {'key': 'é'}), ('get', ('missing',), None, {'key': 'missing'}),
                 ('update', ('é', {'age': 25}), True, {'key': 'é', 'values': {'age': 25}}),
                 ('update', ('missing', {}), False, {'key': 'missing', 'values': {}}),
                 ('delete', ('é',), True, {'key': 'é'}), ('delete', ('missing',), False, {'key': 'missing'}),
                 ('drop', (), None, {})]
        for method, args, result, payload in cases:
            self.reply(result)
            self.assertEqual(getattr(table, method)(*args), result)
            request = self.seen[-1][0]
            self.assertEqual(request['operation'], 'table.' + method)
            self.assertEqual(request['payload'], {'table_id': TABLE_ID, **payload})
        self.reply({'code': 'not_found', 'message': 'gone', 'outcome': 'not_applied'}, 404)
        with self.assertRaises(errors.NotFoundError):
            table.get('é')
        self.assertEqual(self.seen[-1][0]['payload']['table_id'], TABLE_ID)
        self.reply({'code': 'conflict', 'message': 'exists', 'outcome': 'not_applied'}, 409)
        with self.assertRaises(errors.ConflictError):
            table.insert(row)

    def test_local_validation_and_result_uncertainty(self):
        table = self.table()
        for method, args in [('insert', ({'id': 'x'},)), ('upsert', ({'id': 'x', 'age': True, 'active': True},)),
                             ('get', (True,)), ('delete', (1,)), ('update', ('x', {'id': 'x'}))]:
            with self.assertRaises(errors.ValidationError):
                getattr(table, method)(*args)
        self.assertEqual(self.seen, [])
        row = {'id': 'x', 'age': 1, 'active': True}
        for method, args, result in [('insert', (row,), {}), ('upsert', (row,), False),
                                     ('drop', (), True), ('update', ('x', {}), 1), ('delete', ('x',), None),
                                     ('insert_many', ([row],), True), ('insert_many', ([row],), 0)]:
            self.reply(result)
            with self.subTest(method=method), self.assertRaises(errors.UnknownOutcomeError) as caught:
                getattr(table, method)(*args)
            self.assertEqual(caught.exception.request_id, self.seen[-1][0]['request_id'])
        for result in [[], {}, {'id': 'x', 'age': True, 'active': True}]:
            self.reply(result)
            with self.assertRaises(errors.ProtocolError):
                table.get('x')

    def test_atomic_bounded_batch_and_exact_utf8_envelope_limit(self):
        table = self.table()
        row = {'id': 'é', 'age': 1, 'active': True}
        self.reply(2)
        self.assertEqual(table.insert_many(iter([row, row])), 2)
        self.assertEqual(len(self.seen), 1)
        self.assertEqual(self.seen[0][0]['payload'], {'table_id': TABLE_ID, 'rows': [row, row]})
        self.reply(0)
        self.assertEqual(table.insert_many(iter([])), 0)
        self.assertEqual(self.seen[-1][0]['payload']['rows'], [])
        self.reply({'code': 'not_found', 'message': 'gone', 'outcome': 'not_applied'}, 404)
        with self.assertRaises(errors.NotFoundError):
            table.insert_many([])
        self.reply(10000)
        self.assertEqual(table.insert_many(row for _ in range(10000)), 10000)
        self.assertEqual(len(self.seen[-1][0]['payload']['rows']), 10000)
        consumed = 0
        def infinite():
            nonlocal consumed
            while True:
                consumed += 1
                yield row
        before = len(self.seen)
        with self.assertRaises(errors.LimitExceededError):
            table.insert_many(infinite())
        self.assertEqual(consumed, 10001)
        self.assertEqual(len(self.seen), before)
        envelope = {'request_id': '0' * 36, 'operation': 'table.insert_many',
                    'payload': {'table_id': TABLE_ID, 'rows': [dict(row, id='')]}}
        overhead = len(json.dumps(envelope, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode())
        size = LIMIT - overhead
        large = dict(row, id='é' * (size // 2) + 'a' * (size % 2))
        self.reply(1)
        self.assertEqual(table.insert_many([large]), 1)
        self.assertEqual(len(self.seen[-1][1]), LIMIT)
        before = len(self.seen)
        consumed = 0
        def too_large():
            nonlocal consumed
            consumed += 1
            yield dict(large, id=large['id'] + 'a')
            consumed += 1
            yield row
        with self.assertRaises(errors.LimitExceededError):
            table.insert_many(too_large())
        self.assertEqual(consumed, 1)
        self.assertEqual(len(self.seen), before)
        with self.assertRaises(errors.ValidationError):
            table.insert_many([row, {'id': 'bad'}])
        self.assertEqual(len(self.seen), before)


if __name__ == '__main__':
    unittest.main()

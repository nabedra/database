"""Public DB and terminal queries through a local HTTP fixture."""
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import meshdb
from meshdb import errors

TABLE_ID = '12345678-1234-4234-8234-123456789abc'
SCHEMA = {'id': str, 'age': int, 'active': bool, 'score': float}


def descriptor(**changes):
    return dict(dict(id=TABLE_ID, name='users', key='id', format='parquet',
                     schema={'id': 'utf8', 'age': 'int64', 'active': 'bool', 'score': 'float64'}), **changes)


class ClientAPITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                cls.seen.append(request)
                body = json.dumps({'request_id': request['request_id'],
                                   'data' if cls.status == 200 else 'error': cls.result}).encode()
                self.send_response(cls.status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

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
        self.reply(descriptor())
        self.assertTrue(hasattr(meshdb, 'DB'), 'DB must be public')
        self.db = meshdb.DB(f'http://127.0.0.1:{self.server.server_port}/', 'test', timeout=5)

    def reply(self, result, status=200):
        type(self).result, type(self).status = result, status

    def test_config_and_invalid_inputs_are_local(self):
        self.assertEqual(self.seen, [])
        with patch('meshdb.client.ssl.create_default_context') as context:
            meshdb.DB('https://example.test/base', 'test', ca_file='test-ca.pem', timeout=2)
            context.return_value.load_verify_locations.assert_called_once_with(cafile='test-ca.pem')
        for kwargs in [dict(api_url='relative', token='test'), dict(api_url='http://localhost', token=''),
                       dict(api_url='http://localhost', token='test', timeout=0)]:
            with self.assertRaises(errors.ValidationError):
                meshdb.DB(**kwargs)
        for args, kwargs in [(('bad-name', SCHEMA, 'id'), {}), (('users', {'id': bytes}, 'id'), {}),
                             (('users', SCHEMA, None), {}), (('users', SCHEMA, 'active'), {}),
                             (('users', SCHEMA, 'id'), {'format': 'csv'})]:
            with self.assertRaises(errors.ValidationError):
                self.db.create_table(*args, **kwargs)
        with self.assertRaises(errors.ValidationError):
            self.db.table('bad-name')
        self.assertEqual(self.seen, [])

    def test_create_discover_list_and_fixed_handles(self):
        created = self.db.create_table('users', SCHEMA, 'id')
        self.assertIsInstance(created, meshdb.Table)
        self.assertEqual(created.schema, SCHEMA)
        self.assertEqual(self.seen[-1]['payload'], {k: v for k, v in descriptor().items() if k != 'id'})
        self.assertEqual(self.seen[-1]['operation'], 'table.create')
        discovered = self.db.table('users')
        self.assertEqual(self.seen[-1]['payload'], {'name': 'users'})
        self.assertEqual(self.seen[-1]['operation'], 'table.describe')
        self.reply([descriptor(name='accounts'), descriptor(id='22345678-1234-4234-8234-123456789abc')])
        tables = self.db.tables()
        self.assertEqual([t.name for t in tables], ['accounts', 'users'])
        self.assertEqual(self.seen[-1]['operation'], 'table.list')
        self.assertEqual(self.seen[-1]['payload'], {})
        for table in [created, discovered, *tables]:
            self.reply(None)
            self.assertIsNone(table.get('missing'))
            self.assertEqual(self.seen[-1]['payload'], {'table_id': table.id, 'key': 'missing'})
        self.reply([])
        self.assertEqual(self.db.tables(), [])

    def test_malformed_discovery_and_mutation_ambiguity(self):
        bad = [None, [], {}, descriptor(name='other'), descriptor(schema={'id': 'unknown'})]
        for result in bad:
            self.reply(result)
            for call, expected in [(lambda: self.db.table('users'), errors.ProtocolError),
                                   (lambda: self.db.create_table('users', SCHEMA, 'id'), errors.UnknownOutcomeError)]:
                with self.subTest(result=result), self.assertRaises(expected) as caught:
                    call()
                self.assertEqual(caught.exception.request_id, self.seen[-1]['request_id'])
                self.assertEqual(caught.exception.status_code, 200)
        for result in [descriptor(schema={'id': 'utf8'}), descriptor(key='age')]:
            self.reply(result)
            with self.assertRaises(errors.UnknownOutcomeError):
                self.db.create_table('users', SCHEMA, 'id')
        other = descriptor(name='accounts', id='22345678-1234-4234-8234-123456789abc')
        for result in [{}, None, [None], [descriptor(), other], [descriptor(), descriptor()],
                       [other, descriptor(id=other['id'])],
                       [other, descriptor(id=other['id'].upper())]]:
            self.reply(result)
            with self.subTest(result=result), self.assertRaises(errors.ProtocolError):
                self.db.tables()

    def test_terminals_are_fresh_and_projected_rows_can_omit_key(self):
        table = self.db.table('users')
        query = table.where(active=True).select('age', 'score').order_by('age').limit(2)
        before = len(self.seen)
        for invalid in [lambda: query.select('unknown'), lambda: query.limit(True), lambda: query.where(age='24')]:
            with self.assertRaises(errors.ValidationError):
                invalid()
        self.assertEqual(len(self.seen), before)
        for method, results in [('collect', [[{'age': 24, 'score': 1.5}], []]),
                                ('count', [2, 1]), ('explain', ['scan then project', 'new plan'])]:
            for result in results:
                self.reply(result)
                self.assertEqual(getattr(query, method)(), result)
                self.assertEqual(self.seen[-1]['operation'], 'query.' + method)
                self.assertEqual(self.seen[-1]['payload'], {'table_id': TABLE_ID, 'query': query._to_wire()})
        self.assertEqual(len(self.seen) - before, 6)
        self.assertEqual(len({request['request_id'] for request in self.seen}), len(self.seen))
        self.reply([{'id': 'a', 'age': 24, 'active': True, 'score': 1.5}])
        self.assertEqual(table.query().collect()[0]['id'], 'a')
        self.reply([])
        self.assertEqual(query.limit(0).collect(), [])
        self.reply(0)
        self.assertEqual(query.limit(0).count(), 0)
        self.reply(100000)
        self.assertEqual(table.query().limit(10**100).count(), 100000)
        self.reply([{'age': 1}] * 10000)
        self.assertEqual(len(table.query().select('age').collect()), 10000)

    def test_terminal_result_validation(self):
        table = self.db.table('users')
        query = table.query().select('age').limit(1)
        for result in [None, {}, [None], [{}], [{'age': None}], [{'age': True}], [{'age': 1.0}],
                       [{'age': 2**63}], [{'age': 1, 'id': 'a'}], [{'age': 1}] * 2]:
            self.reply(result)
            with self.subTest(result=result), self.assertRaises(errors.ProtocolError):
                query.collect()
        self.reply([{'age': 1}] * 10001)
        with self.assertRaises(errors.ProtocolError):
            table.query().select('age').collect()
        self.reply([{'age': 1}])
        with self.assertRaises(errors.ProtocolError):
            query.limit(0).collect()
        with self.assertRaises(errors.ProtocolError):
            table.query().collect()
        for result in [True, -1, 1.0, None, {}, 2, 100001]:
            self.reply(result)
            with self.subTest(result=result), self.assertRaises(errors.ProtocolError):
                query.count()
        self.reply(100001)
        with self.assertRaises(errors.ProtocolError):
            table.query().count()
        for result in [None, [], {'text': 'plan'}, 1]:
            self.reply(result)
            with self.assertRaises(errors.ProtocolError):
                query.explain()
        self.reply({'code': 'limit_exceeded', 'message': 'too many rows', 'outcome': 'not_applied'}, 413)
        with self.assertRaises(errors.LimitExceededError):
            query.collect()


if __name__ == '__main__':
    unittest.main()

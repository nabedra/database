"""Batch writes against a fake server: chunk bounds, partial failure, lost replies and waiting."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import unittest
from uuid import UUID

from nabedra import BatchError, Database, LimitExceededError, TransportError, UnknownOutcomeError, ValidationError

LIMIT = 8 * 1024 * 1024
TABLE_ID = '12345678-1234-4234-8234-123456789abc'
APPLIED = {'o': 'applied'}
REJECTED = {'o': 'not_applied', 'error': {'code': 'conflict', 'message': 'no', 'outcome': 'not_applied'}}
UNKNOWN = {'o': 'outcome_unknown', 'error': {'code': 'outcome_unknown', 'message': 'lost', 'outcome': 'unknown'}}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers['Content-Length']))
        request = json.loads(raw)
        payload = request['payload']
        if request['operation'] == 'table.describe':
            data = self.server.descriptor
        elif request['operation'] == 'table.search_status':
            data = self.server.statuses.pop(0) if len(self.server.statuses) > 1 else self.server.statuses[0]
        else:
            items = payload.get('rows', payload.get('keys'))
            self.server.chunks.append((request['request_id'], items, len(raw)))
            data = self.server.answer(len(self.server.chunks), items)
        if data is None:
            self.close_connection = True
            self.connection.close()
            return
        body = json.dumps({'request_id': request['request_id'], 'data': data}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def all_applied(_, items):
    return {'outcomes': [APPLIED] * len(items)}


def status(pending):
    return {'columns': {'vec': {'state': 'ready', 'lag_ms': 1, 'pending_embeddings': pending,
                                'failed_embeddings': 0, 'recall': None}}}


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.server.descriptor = {'id': TABLE_ID, 'name': 'users', 'schema': {'id': 'int64', 'note': 'utf8'},
                                  'key': 'id', 'format': 'parquet'}
        self.server.chunks, self.server.answer, self.server.statuses = [], all_applied, [status(0)]
        thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': 0.01})
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def table(self, timeout=10.0):
        db = Database(f'http://127.0.0.1:{self.server.server_port}', 'test', timeout=timeout)
        self.addCleanup(db.close)
        return db.table('users')

    def sizes(self):
        return [len(items) for _, items, _ in self.server.chunks]

    def test_vs17_chunks_hold_at_most_10000_rows_in_order_with_one_call_id_each(self):
        table = self.table()
        self.assertEqual(table.upsert_many({'id': n, 'note': ''} for n in range(25_000)), 25_000)
        self.assertEqual(self.sizes(), [10_000, 10_000, 5_000])
        self.assertEqual([row['id'] for _, items, _ in self.server.chunks for row in items], list(range(25_000)))
        identities = [identity for identity, _, _ in self.server.chunks]
        self.assertEqual(len(set(identities)), 3)
        for identity in identities:
            self.assertEqual(str(UUID(identity)), identity)
        self.server.chunks.clear()
        self.assertEqual(table.delete_many(iter(range(10_001))), 10_001)
        self.assertEqual(self.sizes(), [10_000, 1])

    def test_vs17_chunks_fill_8_mib_exactly_and_never_exceed_it(self):
        table = self.table()
        self.assertEqual(table.upsert_many([{'id': 1, 'note': ''}]), 1)
        overhead = self.server.chunks.pop()[2]
        # Two rows and the comma between them fill one request to the last byte.
        second = len('{"id":2,"note":""},')
        half = (LIMIT - overhead - second) // 2
        rest = LIMIT - overhead - second - half
        rows = [{'id': 1, 'note': 'a' * half}, {'id': 2, 'note': 'é' * (rest // 2) + 'a' * (rest % 2)}]
        self.assertEqual(table.upsert_many(rows), 2)
        self.assertEqual([size for _, _, size in self.server.chunks], [LIMIT])
        self.server.chunks.clear()
        rows[1]['note'] += 'a'
        self.assertEqual(table.upsert_many(rows), 2)
        self.assertEqual(self.sizes(), [1, 1])
        self.server.chunks.clear()
        self.assertEqual(table.upsert_many({'id': n, 'note': 'a' * 900_000} for n in range(20)), 20)
        self.assertEqual(self.sizes(), [9, 9, 2])
        self.assertTrue(all(size <= LIMIT for _, _, size in self.server.chunks))
        self.server.chunks.clear()
        with self.assertRaises(LimitExceededError):
            table.upsert_many([{'id': 1, 'note': 'a' * LIMIT}])
        self.assertEqual(self.server.chunks, [])

    def test_vs15_a_failed_chunk_stops_an_endless_iterable_and_is_not_sent_again(self):
        table = self.table()
        consumed = 0

        def endless():
            nonlocal consumed
            while True:
                yield {'id': consumed, 'note': ''}
                consumed += 1

        def answer(number, items):
            outcomes = [APPLIED] * len(items)
            if number == 2:
                outcomes[3], outcomes[5] = REJECTED, UNKNOWN
            return {'outcomes': outcomes}
        self.server.answer = answer
        with self.assertRaises(BatchError) as caught:
            table.upsert_many(endless())
        error = caught.exception
        self.assertEqual(self.sizes(), [10_000, 10_000])
        self.assertLessEqual(consumed, 20_001)
        self.assertEqual((error.applied, error.outcome, error.code), (19_998, 'unknown', 'batch_error'))
        self.assertEqual(error.request_id, self.server.chunks[1][0])
        self.assertEqual([row['key'] for row in error.outcomes], list(range(10_000, 20_000)))
        self.assertEqual([(row['key'], row['outcome']) for row in error.outcomes if row['outcome'] != 'applied'],
                         [(10_003, 'not_applied'), (10_005, 'outcome_unknown')])
        self.assertIs(error.__cause__, error.outcomes[3]['error'])

    def test_vs15_a_lost_reply_is_unknown_for_every_row_of_that_chunk_only(self):
        table = self.table()
        self.server.answer = lambda number, items: None if number == 2 else all_applied(number, items)
        with self.assertRaises(BatchError) as caught:
            table.delete_many(range(30_000))
        error = caught.exception
        self.assertEqual(self.sizes(), [10_000, 10_000])
        self.assertEqual((error.applied, error.outcome), (10_000, 'unknown'))
        self.assertEqual([row['key'] for row in error.outcomes], list(range(10_000, 20_000)))
        self.assertEqual({row['outcome'] for row in error.outcomes}, {'outcome_unknown'})
        self.assertIsInstance(error.__cause__, UnknownOutcomeError)

    def test_vs15_a_short_outcome_list_is_unknown(self):
        self.server.answer = lambda _, items: {'outcomes': [APPLIED] * (len(items) - 1)}
        with self.assertRaises(BatchError) as caught:
            self.table().delete_many([1, 2])
        self.assertEqual((caught.exception.applied, caught.exception.outcome), (0, 'unknown'))

    def test_vs15_later_input_errors_keep_the_applied_count(self):
        table = self.table()

        def rows(last):
            for number in range(10_001):
                yield {'id': number, 'note': ''}
            if isinstance(last, Exception):
                raise last
            yield last
        for last, cause in (({'id': 'x', 'note': ''}, ValidationError), (KeyError('source'), KeyError),
                            ({'id': 10_000, 'note': ''}, ValidationError)):
            self.server.chunks.clear()
            with self.subTest(last=last), self.assertRaises(BatchError) as caught:
                table.upsert_many(rows(last))
            self.assertEqual(self.sizes(), [10_000])
            self.assertEqual((caught.exception.applied, caught.exception.outcomes), (10_000, []))
            self.assertEqual(caught.exception.outcome, 'partial')
            self.assertIsInstance(caught.exception.__cause__, cause)
        self.server.chunks.clear()
        # Nothing was sent, so the original error is the whole story.
        for last, kind in (({'id': 'x', 'note': ''}, ValidationError), (KeyError('source'), KeyError)):
            def first():
                yield {'id': 1, 'note': ''}
                if isinstance(last, Exception):
                    raise last
                yield last
            with self.assertRaises(kind):
                table.upsert_many(first())
        self.assertEqual(self.server.chunks, [])

    def test_a_key_may_repeat_in_a_later_chunk(self):
        table = self.table()
        keys = [*range(10_000), 0]
        self.assertEqual(table.delete_many(keys), 10_001)
        self.assertEqual(self.sizes(), [10_000, 1])

    def embedded(self):
        self.server.descriptor['schema']['vec'] = {'type': 'vector', 'dim': 2}
        self.server.descriptor['embed'] = {'vec': {'source': 'note', 'model': 'm'}}
        return self.table

    def test_wait_for_embedding_polls_until_nothing_is_pending(self):
        table = self.embedded()()
        self.server.statuses = [status(2), {'columns': {}}, status(0)]
        self.assertEqual(table.upsert_many([{'id': 1, 'note': 'a'}], wait_for_embedding=True), 1)
        self.assertEqual(len(self.server.statuses), 1)
        self.server.statuses = [status(2)]
        self.assertEqual(table.upsert_many([], wait_for_embedding=True), 0)

    def test_wait_for_embedding_reports_applied_rows_when_it_times_out_or_fails(self):
        table = self.embedded()(0.3)
        for answers, cause in (([status(1)], None), ([None], TransportError)):
            self.server.statuses = answers
            with self.subTest(cause=cause), self.assertRaises(BatchError) as caught:
                table.upsert_many([{'id': 1, 'note': 'a'}], wait_for_embedding=True)
            error = caught.exception
            self.assertEqual((error.applied, error.outcomes, error.outcome), (1, [], 'applied'))
            self.assertEqual(str(error), '1 rows were applied and their embeddings can still be pending')
            self.assertEqual(type(error.__cause__), cause or type(None))


if __name__ == '__main__':
    unittest.main()

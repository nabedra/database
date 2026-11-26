"""Vector types and values that are particular to Python."""
from array import array
import ctypes
from dataclasses import FrozenInstanceError
import unittest
from typing import List

import nabedra
from nabedra import LimitExceededError, Query, ValidationError, Vector, col
from nabedra.expr import Ranking
from nabedra.validation import schema_from_wire, schema_to_wire, validate_row, validate_schema, validate_value


class TableContext:
    id = 'table-1'
    key = 'id'
    schema = {'id': str, 'vec': Vector(2), 'tokens': list[Vector(2)], 'weights': Vector(sparse=True), 'tags': list[str]}


class VectorTests(unittest.TestCase):
    def test_types_are_values_and_round_trip(self):
        self.assertEqual(Vector(3), Vector(3))
        self.assertNotEqual(Vector(3), Vector(4))
        self.assertEqual(len({Vector(3), Vector(3), Vector(sparse=True)}), 2)
        with self.assertRaises(FrozenInstanceError):
            Vector(3).dim = 4
        for args, kwargs in (((), {}), ((0,), {}), ((4097,), {}), ((True,), {}), ((2.0,), {}), (('2',), {}),
                             ((2,), {'sparse': True}), ((), {'sparse': False}), ((), {'sparse': 1}), ((2,), {'sparse': None})):
            with self.subTest(args=args, kwargs=kwargs), self.assertRaises(ValidationError):
                Vector(*args, **kwargs)
        schema = TableContext.schema
        self.assertEqual(schema_from_wire(schema_to_wire(schema)), schema)
        self.assertEqual(validate_schema({'id': str, 'tags': List[str]}), {'id': str, 'tags': list[str]})
        for bad in (list, list[int], list[str, str], list[list[str]], list[Vector(sparse=True)], tuple[str], Vector, [str],
                    list[Vector], set[str]):
            with self.subTest(bad=bad), self.assertRaises(ValidationError):
                validate_schema({'id': str, 'x': bad})

    def test_any_contiguous_float32_buffer_is_accepted_and_copied(self):
        source = array('f', [1.5, 2.5])
        carray = (ctypes.c_float * 2)(1.5, 2.5)
        raw = bytearray(source.tobytes())
        for value in (source, memoryview(source), memoryview(source).toreadonly(), carray,
                      memoryview(raw).cast('f'), memoryview(raw).cast('B').cast('f'), [1.5, 2.5], (1.5, 2.5)):
            with self.subTest(value=value):
                self.assertEqual(validate_value(value, Vector(2)), array('f', [1.5, 2.5]))
        copy = validate_value(source, Vector(2))
        source[0] = 9.0
        self.assertEqual(copy[0], 1.5)
        strided = memoryview(array('f', [1.5, 0.0, 2.5, 0.0]))[::2]
        matrix = memoryview(source).cast('B').cast('f', (1, 2))
        for bad in (strided, matrix, array('d', [1.0, 2.0]), array('i', [1, 2]), b'12345678', raw, range(2), {1.0, 2.0},
                    (ctypes.c_double * 2)(1.0, 2.0), iter([1.0, 2.0]), object()):
            with self.subTest(bad=bad), self.assertRaises(ValidationError):
                validate_value(bad, Vector(2))

    def test_integers_are_vector_elements_but_not_booleans_or_subclasses(self):
        self.assertEqual(validate_value([1, 2.5], Vector(2)), array('f', [1.0, 2.5]))
        self.assertEqual(validate_value({3: 2}, Vector(sparse=True)), {3: 2.0})

        class Real(float):
            pass
        for bad in ([True, 1.0], [Real(1.0), 1.0], [10**400, 1], ['1', '2'], [None, 1.0]):
            with self.subTest(bad=bad), self.assertRaises(ValidationError):
                validate_value(bad, Vector(2))
        for bad in ({True: 1.0}, {1.0: 1.0}, {'1': 1.0}, {-1: 1.0}, {2**32: 1.0}, {1: True}, [(1, 1.0)]):
            with self.subTest(bad=bad), self.assertRaises(ValidationError):
                validate_value(bad, Vector(sparse=True))

    def test_rows_detach_every_value(self):
        tags, tokens, weights = ['a'], [[1.0, 2.0]], {2: 0.5, 1: 0.25}
        row = {'id': 'x', 'vec': None, 'tokens': tokens, 'weights': weights, 'tags': tags}
        checked = validate_row(row, TableContext.schema, key='id')
        tags.append('b'), tokens[0].append(3.0), weights.clear()
        self.assertEqual(checked, {'id': 'x', 'vec': None, 'tokens': [array('f', [1.0, 2.0])],
                                   'weights': {1: 0.25, 2: 0.5}, 'tags': ['a']})
        self.assertEqual(list(checked['weights']), [1, 2])

    def test_rankings_are_immutable_and_detach_their_query(self):
        scratch = array('f', [1.0, 2.0])
        first = col('vec').cosine(scratch)
        scratch[0] = 5.0
        second = col('vec').cosine(scratch)
        query = Query(TableContext()).limit(1)
        wires = [query.rank(item)._rank['rankings'][0]['value'] for item in (first, second)]
        self.assertNotEqual(*wires)
        with self.assertRaises(FrozenInstanceError):
            first._op = 'dot'
        with self.assertRaises(TypeError):
            Ranking()
        for operation in (lambda: bool(first), lambda: first and second, lambda: not first,
                          lambda: (col('id') == 'x') & first, lambda: col('vec') == first):
            with self.assertRaises((TypeError, ValidationError)):
                operation()
        with self.assertRaises(ValidationError):
            query.where(first)
        with self.assertRaises(ValidationError):
            query.where(vec=first)

    def test_unknown_keyword_arguments_are_type_errors_before_any_request(self):
        query = Query(TableContext()).limit(1)
        ranking = col('vec').dot([1.0, 2.0])
        for call in (lambda: query.rank(ranking, recal=0.5), lambda: query.rank(ranking).rerank(ranking, candidate=3),
                     lambda: query.rank_many([ranking], limit=1, exct=True)):
            with self.assertRaises(TypeError):
                call()

    def test_ranked_query_vectors_fit_the_wire_word_limit(self):
        context = TableContext()
        context.schema = {**context.schema, 'tokens': list[Vector(4096)]}
        query = Query(context).limit(1)
        sparse = dict.fromkeys(range(1 << 18), 1.0)
        dense = array('f', [1]) * 4096

        query.rank(col('weights').dot(sparse))
        sparse[1 << 18] = 1.0
        with self.assertRaises(LimitExceededError):
            query.rank(col('weights').dot(sparse))

        query.rank(col('tokens').dot([dense] * 64))
        with self.assertRaises(LimitExceededError):
            query.rank(col('tokens').dot([dense] * 65))

    def test_contains_any_orders_sets_by_utf8_bytes_and_keeps_sequence_order(self):
        query = Query(TableContext())
        for values, expected in (({'é', 'Z', '\U0001f600', '\uffff'}, ['Z', 'é', '\uffff', '\U0001f600']),
                                 (frozenset({'b', 'a'}), ['a', 'b']), ({'b': 1, 'a': 2}.keys(), ['b', 'a']),
                                 (['b', 'a'], ['b', 'a']), (('b', 'a'), ('b', 'a')), (iter(['b', 'a']), ['b', 'a'])):
            wire = query.where(col('tags').contains_any(values))._to_wire()['filter']['values']
            self.assertEqual(wire, list(expected))

    def test_queries_stay_immutable(self):
        query = Query(TableContext()).limit(1)
        ranked = query.rank(col('vec').dot([1.0, 2.0]))
        reranked = ranked.rerank(col('tokens').dot([[1.0, 2.0]]))
        self.assertIsNone(query._rank)
        self.assertIsNone(ranked._rerank)
        self.assertIsNotNone(reranked._rerank)

    def test_contains_any_reads_any_iterable_once(self):
        values = iter(['a', 'b'])
        expression = col('tags').contains_any(values)
        query = Query(TableContext()).where(expression).where(expression)
        self.assertEqual(query._to_wire()['filter']['args'],
                         [{'op': 'contains_any', 'column': 'tags', 'values': ['a', 'b']}] * 2)

    def test_exports(self):
        for name in ('Vector', 'BatchError', 'SearchUnavailableError'):
            self.assertIn(name, nabedra.__all__)
        self.assertEqual(nabedra.SearchUnavailableError('x').code, 'search_unavailable')
        self.assertEqual(nabedra.SearchUnavailableError('x').outcome, 'not_applied')


if __name__ == '__main__':
    unittest.main()

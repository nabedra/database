"""Tests added from the task D mutation gate: the wording of a partial batch failure."""
import unittest

import test_batches as base
from nabedra import BatchError, ValidationError


class PartialBatchMessageTests(unittest.TestCase):
    setUp = base.BatchTests.setUp
    table = base.BatchTests.table

    def test_vs15_a_partial_failure_names_the_applied_count_and_the_cause(self):
        table = self.table()

        def rows(last):
            for number in range(10_001):
                yield {'id': number, 'note': ''}
            raise last
        for cause in (KeyError('source'), ValueError('bad input'), ValidationError('Column is wrong')):
            with self.subTest(cause=cause), self.assertRaises(BatchError) as caught:
                table.upsert_many(rows(cause))
            self.assertEqual(str(caught.exception), f'Batch stopped after 10000 applied rows: {cause}')
            self.assertEqual((caught.exception.applied, caught.exception.outcome), (10_000, 'partial'))
            self.assertIs(caught.exception.__cause__, cause)


if __name__ == '__main__':
    unittest.main()

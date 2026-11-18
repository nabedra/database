"""Regression assertions for the shared point-update snapshot oracle."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from benchmark_oltp_workload import run_workload, verify


class Rows:
    def __init__(self, values):
        self.values = values

    def read(self, _keys):
        return dict(self.values)


class SnapshotOracleTest(unittest.TestCase):
    def test_delayed_unknown_value_is_observed_once_in_a_later_phase(self):
        rows = Rows({0: 0, 1: 0})
        pending = {}
        lost_response = {'key': 0, 'value': 8, 'status': 'unresolved'}

        first, before = verify(rows, rows.values, [lost_response], pending)
        self.assertEqual(first['mismatches'], 0)
        self.assertEqual(first['unobserved_unresolved_values_after_snapshot'], 1)
        self.assertEqual(lost_response['status'], 'unresolved')

        rows.values = {0: 8, 1: 10}
        acknowledged = {'key': 1, 'value': 10, 'status': 'succeeded'}
        second, before = verify(rows, before, [acknowledged], pending)
        self.assertEqual(second['mismatches'], 0)
        self.assertEqual(second['first_prior_unresolved_values_observed'], [{'key': 0, 'value': 8}])
        self.assertEqual(second['unobserved_unresolved_values_after_snapshot'], 0)

        rows.values = {0: 11, 1: 10}
        third, before = verify(rows, before, [{'key': 0, 'value': 11, 'status': 'succeeded'}], pending)
        self.assertEqual(third['mismatches'], 0)

        rows.values = {0: 8, 1: 10}
        stale, _ = verify(rows, before, [], pending)
        self.assertEqual(stale['first_mismatches'], [{'key': 0, 'expected': 11, 'actual': 8}])

    def test_workload_retains_raw_unresolved_disposition_after_late_observation(self):
        adapter = Mock(spec=['seed', 'read', 'close'])
        adapter.seed.return_value = None
        adapter.read.side_effect = [{0: 0, 1: 0}, {0: 0, 1: 0}, {0: 1, 1: 2}]
        timed_phases = [
            ({'succeeded_rps': 0}, [{'key': 0, 'value': 1, 'status': 'unresolved'}]),
            ({'succeeded_rps': 1}, [{'key': 1, 'value': 2, 'status': 'succeeded'}]),
        ]
        args = SimpleNamespace(rates=[1], seconds=1, warmup_seconds=1, repetitions=1,
                               clients=1, rows=2, seed=1, timeout=1)
        with TemporaryDirectory() as directory, patch('benchmark_oltp_workload.measure', side_effect=timed_phases):
            output = Path(directory)
            result = run_workload(args, adapter, output, {})
            warmup = json.loads((output / '1-warmup.jsonl').read_text())
        self.assertEqual(warmup['status'], 'unresolved')
        self.assertFalse(result['correctness_verified'])
        self.assertEqual(result['phases'][0]['unresolved_after_reconciliation'], 1)
        self.assertEqual(result['phases'][1]['snapshot']['mismatches'], 0)
        self.assertEqual(result['phases'][1]['snapshot']['prior_unresolved_values_observed'], 1)

    def test_unobserved_candidate_budget_is_bounded_by_the_dataset(self):
        rows = Rows({0: 0})
        pending = {}
        _, before = verify(rows, rows.values, [{'key': 0, 'value': 1, 'status': 'unresolved'}], pending)
        with self.assertRaisesRegex(RuntimeError, 'unresolved value limit'):
            verify(rows, before, [{'key': 0, 'value': 2, 'status': 'unresolved'}], pending)


if __name__ == '__main__':
    unittest.main()

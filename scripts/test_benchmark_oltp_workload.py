"""Regression assertions for the shared point-update workload."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from benchmark_oltp_workload import _offered_updates, parse_rates, run_workload, verify


class WorkloadRatesTest(unittest.TestCase):
    def test_fractional_rates_and_integer_identity(self):
        rates = parse_rates('1,.29,5.0')
        self.assertEqual(rates, [1, 0.29, 5])
        self.assertEqual([type(rate) for rate in rates], [int, float, int])
        self.assertEqual(_offered_updates(rates[1], 100.0), 29)
        with self.assertRaises(ValueError):
            _offered_updates(parse_rates('1e308')[0], 5.0)
        for invalid in ('0', '-0.2', 'nan', 'inf', '0.2,.20', '1,1.0'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                parse_rates(invalid)

    def test_single_fractional_offer_per_phase_conserves_results(self):
        rows = {0: 0, 1: 0}
        clock_ns = 0

        def sleep(seconds):
            nonlocal clock_ns
            clock_ns += round(seconds * 1e9)

        def update(key, value, _request_id):
            sleep(3)
            rows[key] = value
            return {'status': 'succeeded'}

        adapter = Mock(spec=['seed', 'read', 'update', 'close'])
        adapter.seed.return_value = None
        adapter.read.side_effect = lambda _keys: dict(rows)
        adapter.update.side_effect = update
        args = SimpleNamespace(rates=parse_rates('0.2'), seconds=5.0, warmup_seconds=5.0,
                               repetitions=2, clients=1, rows=2, seed=1, timeout=10)
        clock = SimpleNamespace(monotonic_ns=lambda: clock_ns, sleep=sleep)

        with TemporaryDirectory() as directory, patch('benchmark_oltp_workload.time', clock):
            output = Path(directory)
            result = run_workload(args, adapter, output, {})
            self.assertEqual(result, json.loads((output / 'results.json').read_text()))
            self.assertEqual(result['settings']['rates'], [0.2])
            self.assertTrue(result['correctness_verified'])
            self.assertEqual(len(result['phases']), 3)
            previous_settlement = 0
            for phase, label in zip(result['phases'], ('0.2-warmup', '0.2-rep1', '0.2-rep2'),
                                    strict=True):
                self.assertEqual(phase['label'], label)
                self.assertEqual(phase['offered'], 1)
                self.assertEqual(phase['offered_rps'], 0.2)
                self.assertEqual(phase['statuses']['succeeded'], 1)
                self.assertEqual(phase['statuses']['client_capacity'], 0)
                self.assertEqual(phase['snapshot']['mismatches'], 0)
                sample = json.loads((output / phase['samples']).read_text())
                self.assertGreaterEqual(sample['dispatched_ns'], previous_settlement)
                previous_settlement = sample['settled_ns']


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
        args = SimpleNamespace(rates=parse_rates('1'), seconds=1, warmup_seconds=1, repetitions=1,
                               clients=1, rows=2, seed=1, timeout=1, snapshot_between_phases=True)
        with TemporaryDirectory() as directory, patch('benchmark_oltp_workload.measure', side_effect=timed_phases):
            output = Path(directory)
            result = run_workload(args, adapter, output, {})
            warmup = json.loads((output / '1-warmup.jsonl').read_text())
        self.assertEqual(warmup['status'], 'unresolved')
        self.assertFalse(result['correctness_verified'])
        self.assertEqual(result['phases'][0]['unresolved_after_reconciliation'], 1)
        self.assertEqual(result['phases'][1]['snapshot']['mismatches'], 0)
        self.assertEqual(result['phases'][1]['snapshot']['prior_unresolved_values_observed'], 1)

    def test_perf2e_snapshot_is_read_once_after_the_measured_phases(self):
        adapter = Mock(spec=['seed', 'read', 'close'])
        adapter.seed.return_value = None
        adapter.read.side_effect = [{0: 0, 1: 0}, {0: 1, 1: 2}]
        phases = [
            ({'succeeded_rps': 0}, [{'key': 0, 'value': 1, 'status': 'unresolved'}]),
            ({'succeeded_rps': 1}, [{'key': 1, 'value': 2, 'status': 'succeeded'}]),
        ]
        args = SimpleNamespace(rates=parse_rates('1'), seconds=1, warmup_seconds=1, repetitions=1,
                               clients=1, rows=2, seed=1, timeout=1)
        with TemporaryDirectory() as directory, patch('benchmark_oltp_workload.measure', side_effect=phases):
            result = run_workload(args, adapter, Path(directory), {})
        # The seed check, then one read after both phases: none between them.
        self.assertEqual(adapter.read.call_count, 2)
        self.assertEqual(result['verification']['phases'], 2)
        self.assertEqual(result['phases'][0]['snapshot']['source'], 'expected state')
        self.assertEqual(result['phases'][1]['snapshot']['source'], 'snapshot read')
        self.assertEqual(result['phases'][1]['snapshot']['mismatches'], 0)
        # The unresolved value from the first phase is observed in the final snapshot.
        self.assertEqual(result['phases'][1]['snapshot']['prior_unresolved_values_observed'], 1)
        self.assertEqual(result['verification']['final_snapshot'], result['phases'][1]['snapshot'])

    def test_perf2e_deferred_check_still_fails_on_a_lost_acknowledged_write(self):
        adapter = Mock(spec=['seed', 'read', 'close'])
        adapter.seed.return_value = None
        adapter.read.side_effect = [{0: 0, 1: 0}, {0: 0, 1: 2}]
        phases = [
            ({'succeeded_rps': 1}, [{'key': 0, 'value': 1, 'status': 'succeeded'}]),
            ({'succeeded_rps': 1}, [{'key': 1, 'value': 2, 'status': 'succeeded'}]),
        ]
        args = SimpleNamespace(rates=parse_rates('1'), seconds=1, warmup_seconds=1, repetitions=1,
                               clients=1, rows=2, seed=1, timeout=1)
        with TemporaryDirectory() as directory, patch('benchmark_oltp_workload.measure', side_effect=phases):
            with self.assertRaisesRegex(RuntimeError, 'snapshot verification failed'):
                run_workload(args, adapter, Path(directory), {})
            saved = json.loads((Path(directory) / 'results.json').read_text())
        self.assertEqual(saved['phases'][1]['snapshot']['first_mismatches'], [{'key': 0, 'expected': 1, 'actual': 0}])

    def test_perf2e_gets_are_checked_against_the_expected_state_of_earlier_phases(self):
        adapter = Mock(spec=['seed', 'read', 'close'])
        adapter.seed.return_value = None
        adapter.read.side_effect = [{0: 0, 1: 0}, {0: 1, 1: 0}]
        phases = [
            ({'succeeded_rps': 1}, [{'key': 0, 'value': 1, 'status': 'succeeded'}]),
            ({'succeeded_rps': 1}, [{'op': 'get', 'key': 0, 'value': 2, 'status': 'succeeded', 'read_value': 0}]),
        ]
        args = SimpleNamespace(rates=parse_rates('1'), seconds=1, warmup_seconds=1, repetitions=1,
                               clients=1, rows=2, seed=1, timeout=1)
        with TemporaryDirectory() as directory, patch('benchmark_oltp_workload.measure', side_effect=phases):
            with self.assertRaisesRegex(RuntimeError, 'snapshot verification failed'):
                run_workload(args, adapter, Path(directory), {})
            saved = json.loads((Path(directory) / 'results.json').read_text())
        self.assertEqual(saved['phases'][1]['snapshot']['first_read_mismatches'], [{'key': 0, 'expected': 1, 'actual': 0}])

    def test_unobserved_candidate_budget_is_bounded_by_the_dataset(self):
        rows = Rows({0: 0})
        pending = {}
        _, before = verify(rows, rows.values, [{'key': 0, 'value': 1, 'status': 'unresolved'}], pending)
        with self.assertRaisesRegex(RuntimeError, 'unresolved value limit'):
            verify(rows, before, [{'key': 0, 'value': 2, 'status': 'unresolved'}], pending)



class GetAndMixTest(unittest.TestCase):
    def test_gets_must_return_the_previous_snapshot_or_an_unresolved_value(self):
        rows = Rows({0: 5, 1: 6})
        pending = {1: {9}}
        reads = [{'op': 'get', 'key': 0, 'value': 1, 'status': 'succeeded', 'read_value': 5},
                 {'op': 'get', 'key': 1, 'value': 2, 'status': 'succeeded', 'read_value': 9},
                 {'op': 'get', 'key': 0, 'value': 3, 'status': 'rejected'}]
        result, _ = verify(rows, rows.values, reads, pending)
        self.assertEqual(result['read_mismatches'], 0)
        self.assertEqual(result['mismatches'], 0)
        stale = [{'op': 'get', 'key': 0, 'value': 4, 'status': 'succeeded', 'read_value': 4}]
        result, _ = verify(rows, rows.values, stale, {})
        self.assertEqual(result['first_read_mismatches'], [{'key': 0, 'expected': 5, 'actual': 4}])
        self.assertEqual(result['mismatches'], 1)

    def test_mix_dispatches_gets_and_updates_and_stops_on_p99(self):
        rows = {key: 0 for key in range(100)}
        clock_ns = 0

        def sleep(seconds):
            nonlocal clock_ns
            clock_ns += round(seconds * 1e9)

        def update(key, value, _request_id):
            sleep(0.001)
            rows[key] = value
            return {'status': 'succeeded'}

        def get(key, _request_id):
            sleep(0.001)
            return {'status': 'succeeded', 'read_value': rows[key]}

        adapter = Mock(spec=['seed', 'read', 'update', 'get', 'close'])
        adapter.seed.return_value = None
        adapter.read.side_effect = lambda _keys: dict(rows)
        adapter.update.side_effect = update
        adapter.get.side_effect = get
        args = SimpleNamespace(rates=parse_rates('2,4'), seconds=5.0, warmup_seconds=5.0, repetitions=1,
                               clients=32, rows=100, seed=3, timeout=10, operation='mix', read_fraction=0.5,
                               stop_p99_ms=0.5, stop_on_rejection=False)
        clock = SimpleNamespace(monotonic_ns=lambda: clock_ns, sleep=sleep)
        with TemporaryDirectory() as directory, patch('benchmark_oltp_workload.time', clock):
            result = run_workload(args, adapter, Path(directory), {})
        self.assertTrue(result['correctness_verified'])
        self.assertEqual(result['stopped_after_rate'], 2)
        self.assertEqual([phase['label'] for phase in result['phases']], ['2-warmup', '2-rep1'])
        operations = result['phases'][1]['by_operation']
        self.assertEqual(set(operations), {'get', 'update'})
        self.assertEqual(sum(op['offered'] for op in operations.values()), 10)
        self.assertEqual(sum(phase['offered'] for phase in result['phases']), 20)
        self.assertEqual(adapter.get.call_count + adapter.update.call_count, 20)
        self.assertGreater(adapter.get.call_count, 0)
        self.assertGreater(adapter.update.call_count, 0)
        self.assertEqual(list(result['median_succeeded_rps']), ['2'])


if __name__ == '__main__':
    unittest.main()

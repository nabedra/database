import csv
import gzip
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

from run import capacity, confidence, gate_status, paired_result, summary


def measured(rate, ns, errors=0):
    counts = Counter(ok=10_000 - errors)
    if errors:
        counts["TimeoutError"] = errors
    return {"rate": rate, "goodput": rate, "samples": 10_000, "outcomes": dict(counts),
            "latency_ns": {"p50": ns, "p99": ns}}


class Measurements(unittest.TestCase):
    def test_pair_thresholds_use_intervals_and_reject_wrong_outcomes(self):
        opts = SimpleNamespace(samples=10_000)
        self.assertEqual([1.1, 1.1], confidence([1.1] * 3))
        reference = measured(2000, 100_000)
        faster = [(measured(2000, 70_000), reference)] * 3
        self.assertEqual("meets-thresholds", paired_result(faster, 1, opts)["status"])
        pairs = [(measured(2000, ns), reference) for ns in (100_000, 115_000, 118_000)]
        self.assertEqual("inconclusive", paired_result(pairs, 1, opts)["status"])
        wrong = [(measured(2000, 70_000, 1), reference)] * 3
        self.assertEqual("miss", paired_result(wrong, 1, opts)["status"])

    def test_capacity_requires_latency_failure_above_sustainable_rate(self):
        low, high = measured(1000, 400_000), measured(5000, 1_500_000)
        self.assertEqual(low, capacity([low, high], 1))
        self.assertIsNone(capacity([low], 1))
        self.assertEqual("not-run-on-ec2", gate_status(SimpleNamespace(environment="docker"), {}))

    def test_raw_samples_are_conserved_and_queue_time_counts_toward_latency(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "run.csv.gz"

            def write(rows):
                with gzip.open(raw, "wt", newline="") as output:
                    writer = csv.writer(output)
                    writer.writerow(["sequence", "scheduled_ns", "started_ns", "completed_ns", "outcome"])
                    writer.writerows(rows)

            run = {"samples": 2, "rate": 1000, "measure_start_ns": 1_000}
            first = [0, 1_000, 51_000, 61_000, "ok"]
            second = [1, 1_001_000, 1_101_000, 1_201_000, "CacheUnknownWriteOutcomeError"]
            write([first, second])
            result, writes = summary([raw], run)
            self.assertEqual(set(), writes)
            self.assertEqual(60_000, result["latency_ns"]["p99"])
            self.assertEqual(50_000, result["queue_p99_ns"])
            self.assertEqual(10_000, result["service_p99_ns"])
            self.assertEqual({"ok": 1, "CacheUnknownWriteOutcomeError": 1}, result["outcomes"])
            write([first, first])
            with self.assertRaisesRegex(RuntimeError, "invalid measurement sample"):
                summary([raw], run)
            write([first])
            with self.assertRaisesRegex(RuntimeError, "did not account"):
                summary([raw], run)


if __name__ == "__main__":
    unittest.main()

from contextlib import redirect_stdout
import io
import json
import math
from pathlib import Path
import random
from shlex import join
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import ab


SLEEP = """\
import sys
import time

elapsed = []
for _ in range(int(sys.argv[2])):
    start = time.perf_counter()
    time.sleep(float(sys.argv[1]))
    elapsed.append(time.perf_counter() - start)
print(f"elapsed={min(elapsed):.9f}")
"""

def sleeper(delay, samples=1):
    return [sys.executable, "-S", "-c", SLEEP, str(delay), str(samples)]


class PairedBenchmarkTests(unittest.TestCase):
    def test_sleeping_twenty_percent_longer_flags_and_writes_reports(self):
        with TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            output = Path(directory)
            spec = output / "sleep.toml"
            spec.write_text(
                'name = "Synthetic sleep"\nmodule = "testing"\n'
                'command = ["{bin}"]\nwarmup = 1\nrounds = 5\n'
                r"metric = 'elapsed=(\d+\.\d+)'" + "\n"
            )
            result = ab.main([
                # Real processes, fastest of three sleeps. The 10% power is checked on the
                # decision rule below; 20% here keeps a loaded shared host from inverting pairs.
                str(spec), join(sleeper(.050, 3)), join(sleeper(.060, 3)),
                "--commands", "--output", str(output),
            ])
            data = json.loads((output / "results.json").read_text())
            summary = (output / "summary.md").read_text()
        self.assertEqual(result, 1)
        self.assertTrue(data["regression"])
        self.assertGreater(data["ci95"][0], 1)
        self.assertGreaterEqual(data["median_ratio"], 1.05)
        self.assertEqual(len(data["pairs"]), 5)
        self.assertEqual(data["pairs"][0]["order"], ["head", "base"])
        self.assertEqual(data["bootstrap_seed"], ab.BOOTSTRAP_SEED)
        self.assertTrue(data["cpu_model"])
        self.assertIn("Regression on ", summary)
        print(f"Planted 20% sleep slowdown flagged (head/base {data['median_ratio']:.4f}).")

    def test_null_false_positive_rate_and_power_on_paired_noise(self):
        # Monte Carlo over the real decision rule. Each pair shares a host
        # factor (10% drift) and each side adds its own 5% noise, as on a shared
        # runner. With six rounds, as in the perf specs, identical builds must be
        # flagged at most 5% of the time, and a 10% slowdown must still be caught
        # most of the time, so a gate that never flags cannot pass.
        def flagged(rng, shift, trials=100):
            hits = 0
            for _ in range(trials):
                pairs = []
                for _ in range(6):
                    host = math.exp(rng.gauss(0, .10))
                    pairs.append({"base": host * math.exp(rng.gauss(0, .05)),
                                  "head": host * shift * math.exp(rng.gauss(0, .05))})
                hits += ab.compare(pairs, 5, 1000)["regression"]
            return hits

        false_positives = flagged(random.Random(20261122), 1.0)
        detected = flagged(random.Random(20261123), 1.10)
        self.assertLessEqual(false_positives, 5)
        self.assertGreaterEqual(detected, 60)
        print(f"Null comparisons flagged {false_positives}/100; 10% slowdowns flagged {detected}/100.")

    def test_threshold_and_confidence_both_required_with_seeded_bootstrap(self):
        def assess(ratios):
            pairs = [{"base": 100, "head": 100 * ratio} for ratio in ratios]
            return ab.compare(pairs, 5, 2000)

        self.assertFalse(assess([1.04] * 5)["regression"])
        self.assertTrue(assess([1.05] * 5)["regression"])
        noisy = assess([0.9, 1.0, 1.08, 1.1, 1.2])
        self.assertEqual(noisy, {"median_ratio": 1.08, "ci95": [0.9, 1.2], "regression": False})
        self.assertEqual(noisy, assess([0.9, 1.0, 1.08, 1.1, 1.2]))

    def test_wall_mode_measures_positive_time_and_alternates_all_pairs(self):
        spec = ab.Spec("Measured wall", "testing", ["{bin}"], 1, 2, "wall")
        measured = [0.020, 0.020, 0.022, 0.020, 0.020, 0.022]
        with patch.object(ab, "measure", side_effect=measured) as run:
            result = ab.benchmark(spec, ["old"], ["new"])
        self.assertEqual([call.args[-1] for call in run.call_args_list],
                         ["base", "head", "head", "base", "base", "head"])
        self.assertTrue(result["regression"])
        self.assertGreater(ab.measure(spec, None, sleeper(.020), "base"), .020)

    def test_metric_file_and_existing_specs(self):
        for module in ("cache", "events", "olap", "oltp"):
            with self.subTest(module=module):
                paths = list((ab.ROOT / "docs/testing/perf" / module).glob("*.toml"))
                self.assertTrue(paths)
                for path in paths:
                    self.assertEqual(ab.load_spec(path).module, module)
        spec = ab.Spec("File metric", "testing", ["{bin}", "{work}/measurements.txt"],
                       0, 2, r"sample=(\d+)", metric_file="measurements.txt")
        peer = [sys.executable, "-c", "import sys; open(sys.argv[1], 'w').write('sample=1\\nsample=3\\n')"]
        self.assertEqual(ab.benchmark(spec, peer, peer)["pairs"][0]["head"], 2)

    def test_failed_command_cannot_produce_a_passing_comparison(self):
        spec = ab.Spec("Failed command", "testing", ["{bin}"], 0, 2, "wall")
        with self.assertRaisesRegex(ValueError, "base command exited 7"):
            ab.benchmark(spec, [sys.executable, "-c", "import sys; sys.exit(7)"], sleeper(.020))

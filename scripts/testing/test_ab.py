from contextlib import redirect_stderr, redirect_stdout
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
        measured = [{"metric": value} for value in (0.020, 0.020, 0.022, 0.020, 0.020, 0.022)]
        with patch.object(ab, "measure", side_effect=measured) as run:
            result = ab.benchmark(spec, ["old"], ["new"])
        self.assertEqual([call.args[3] for call in run.call_args_list],
                         ["base", "head", "head", "base", "base", "head"])
        self.assertTrue(result["regression"])
        wall = [("metric", None, None, False)]
        self.assertGreater(ab.measure(spec, wall, sleeper(.020), "base")["metric"], .020)

    def test_metric_file_and_existing_specs(self):
        for module in ("cache", "events", "kv", "olap", "oltp", "sim"):
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

    def test_named_metrics_share_one_run_and_each_can_flag(self):
        # One invocation feeds every metric: an output metric, a file metric and wall time.
        spec = write_spec(
            'command = ["{bin}", "{work}"]\nwarmup = 0\nrounds = 3\ntimeout_s = 30\n'
            "[metrics]\n"
            r"fast = 'fast=(\d+)'" "\n"
            r"slow = { pattern = 'slow=(\d+)', file = 'out/slow.txt' }" "\n"
            r"rate = { pattern = 'fast=(\d+)', higher_is_better = true }" "\n"
            'total = "wall"\n'
        )
        self.assertEqual([row[0] for row in spec.measurements()], ["fast", "slow", "rate", "total"])
        writer = ("import pathlib, sys; out = pathlib.Path(sys.argv[2]) / 'out'; out.mkdir(); "
                  "(out / 'slow.txt').write_text(f'slow={sys.argv[1]}'); print('fast=10')")
        result = ab.benchmark(spec, [sys.executable, "-c", writer, "100"],
                              [sys.executable, "-c", writer, "120"])
        fast, slow = result["metrics"]["fast"], result["metrics"]["slow"]
        self.assertEqual((fast["median_ratio"], fast["regression"]), (1, False))
        self.assertAlmostEqual(slow["median_ratio"], 1.2)
        self.assertTrue(slow["regression"])
        self.assertTrue(result["regression"])
        # The primary metric keeps the single-metric fields.
        self.assertEqual((result["median_ratio"], len(result["pairs"])), (1, 3))
        self.assertEqual(fast["noise"]["ratio_range"], [1, 1])
        self.assertEqual(fast["noise"]["ratio_mad_pct"], 0)
        self.assertAlmostEqual(slow["noise"]["ci_halfwidth_pct"], 20)
        # A rate is compared as its reciprocal, so it stays a cost.
        self.assertEqual(result["metrics"]["rate"]["pairs"][0]["base"], 0.1)
        self.assertGreater(result["metrics"]["total"]["pairs"][0]["base"], 0)
        self.assertGreater(result["runtime_seconds"], 0)

    def test_report_only_metric_cannot_decide_the_comparison(self):
        spec = write_spec(
            'command = ["{bin}"]\nwarmup = 0\nrounds = 3\n[metrics]\n'
            r"steady = 'steady=(\d+)'" "\n"
            r"noisy = { pattern = 'noisy=(\d+)', gate = false }" "\n"
        )
        side = lambda noisy: [sys.executable, "-c", f"print('steady=10 noisy={noisy}')"]
        result = ab.benchmark(spec, side(10), side(20))
        noisy = result["metrics"]["noisy"]
        self.assertEqual((noisy["regression"], noisy["gate"]), (True, False))
        self.assertFalse(result["regression"])
        with self.assertRaisesRegex(ValueError, "at least one metric must gate"):
            write_spec('command = ["{bin}"]\nwarmup = 0\nrounds = 2\n[metrics]\n'
                       r"a = { pattern = 'a=(\d+)', gate = false }" "\n")
        with self.assertRaisesRegex(ValueError, "boolean gate"):
            write_spec('command = ["{bin}"]\nwarmup = 0\nrounds = 2\n[metrics]\n'
                       r"a = { pattern = 'a=(\d+)', gate = 0 }" "\n")

    def test_spec_validation_and_overrides(self):
        for text, message in (
            ("", "exactly one of metric and metrics"),
            ("metric = 'wall'\n[metrics]\nb = 'wall'\n", "exactly one of metric and metrics"),
            ("[metrics]\n" r"b = 'b=\d+'" "\n", "exactly one capturing group"),
            ("[metrics]\n" r"b = { pattern = 'b=(\d+)', file = '../x' }" "\n", "relative to the scratch"),
            ("[metrics]\n" r"b = { pattern = 'b=(\d+)', other = 1 }" "\n", "a table of pattern, file"),
            ("[metrics]\nb = { pattern = 'wall', higher_is_better = true }\n", "wall time is a cost"),
            ("timeout_s = -1\nmetric = 'wall'\n", "timeout_s"),
        ):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, message):
                write_spec('command = ["{bin}"]\nwarmup = 0\nrounds = 2\n' + text)
        with TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            path = Path(directory) / "spec.toml"
            path.write_text(SPEC_HEADER + 'command = ["{bin}"]\nwarmup = 3\nrounds = 9\nmetric = "wall"\n')
            peer = join([sys.executable, "-c", "pass"])
            status = ab.main([str(path), peer, peer, "--commands", "--rounds", "2", "--warmup", "0",
                              "--output", directory])
            self.assertIn(status, (0, 1))
            data = json.loads((Path(directory) / "results.json").read_text())
            self.assertEqual((data["spec"]["rounds"], data["spec"]["warmup"], len(data["pairs"])), (2, 0, 2))
            self.assertIn("| metric | 2 |", (Path(directory) / "summary.md").read_text())
            with redirect_stderr(io.StringIO()):
                self.assertEqual(ab.main([str(path), peer, peer, "--commands", "--rounds", "1",
                                          "--output", directory]), 2)

    def test_each_side_can_run_from_its_own_tree(self):
        # A head that changes config keys or drivers runs its own copy; each side reads only its tree.
        with TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory)
            for side, value in (("base", 10), ("head", 30)):
                (root / side).mkdir()
                (root / side / "driver.py").write_text(f"print('cost={value}')\n")
            spec = root / "spec.toml"
            spec.write_text(SPEC_HEADER + 'command = ["{bin}", "driver.py"]\nwarmup = 0\nrounds = 2\n'
                            r"metric = 'cost=(\d+)'" "\n")
            status = ab.main([str(spec), sys.executable, sys.executable, "--base-root", str(root / "base"),
                              "--head-root", str(root / "head"), "--output", str(root / "out")])
            data = json.loads((root / "out" / "results.json").read_text())
        self.assertEqual(status, 1)
        self.assertEqual([(pair["base"], pair["head"]) for pair in data["pairs"]], [(10, 30), (10, 30)])
        self.assertEqual(data["roots"], {"base": str((root / "base").resolve()), "head": str((root / "head").resolve())})

    def test_hung_command_times_out_as_an_error(self):
        spec = ab.Spec("Hung", "testing", ["{bin}"], 0, 2, "wall", timeout_s=0.5)
        with self.assertRaisesRegex(ValueError, "base command timed out"):
            ab.benchmark(spec, sleeper(5), sleeper(.01))


SPEC_HEADER = 'name = "S"\nmodule = "testing"\n'


def write_spec(text):
    with TemporaryDirectory() as directory:
        path = Path(directory) / "spec.toml"
        path.write_text(SPEC_HEADER + text)
        return ab.load_spec(path)

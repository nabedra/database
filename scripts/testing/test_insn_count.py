import contextlib
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import insn_count


class InstructionCountTests(unittest.TestCase):
    def test_parse_single_ir_summary(self):
        profile = "version: 1\nevents: Ir\nsummary: 10342\nfn=(1) main\ntotals: 10342\n"
        self.assertEqual(insn_count.instruction_count(profile), 10342)

    def test_reject_missing_ambiguous_or_nonpositive_counts(self):
        for profile in (
            "events: Ir\ntotals: 2",
            "events: Ir Dr\nsummary: 2 3",
            "events: Ir\nsummary: 2\nsummary: 3",
            "events: Ir\nsummary: -1",
            "events: Ir\nsummary: invalid",
        ):
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                insn_count.instruction_count(profile)

    def test_uses_medians_and_fails_strictly_above_half_percent(self):
        base = [10000, 10001, 9999]
        row = insn_count.compare(base, [10050, 10051, 10049], 100)
        self.assertEqual(row["base_per_op"], 100)
        self.assertEqual(row["head_per_op"], 100.5)
        self.assertFalse(row["regression"])
        self.assertTrue(insn_count.compare(base, [10051] * 3, 100)["regression"])
        self.assertFalse(insn_count.compare(base, [9900] * 3, 100)["regression"])

    def test_uses_three_times_base_spread_when_larger(self):
        base = [9970, 10000, 10030]
        row = insn_count.compare(base, [10170, 10171, 10169], 10)
        self.assertAlmostEqual(row["noise_pct"], 0.6)
        self.assertAlmostEqual(row["threshold_pct"], 1.8)
        self.assertFalse(row["regression"])
        self.assertFalse(row["inconclusive"])
        self.assertTrue(insn_count.compare(base, [10190] * 3, 10)["regression"])

    def test_noisy_base_is_inconclusive_instead_of_passing(self):
        # A 1% spread would allow 3%; the ceiling is 2%.
        row = insn_count.compare([995, 1000, 1005], [1025] * 3, 10)
        self.assertEqual(row["threshold_pct"], 3)
        self.assertFalse(row["regression"])
        self.assertTrue(row["inconclusive"])
        self.assertFalse(insn_count.compare([997, 1000, 1003], [1000] * 3, 10)["inconclusive"])
        counts = (
            ("cache-set", {"base": [10000] * 3, "head": [10000] * 3}),
            ("olap-queue", {"base": [995, 1000, 1005], "head": [1000] * 3}),
        )
        status, output = self.run_main(counts)
        self.assertEqual(status, 2)
        self.assertIn("NOISY olap-queue:", output)
        self.assertIn("INCONCLUSIVE instruction-count comparison", output)
        # A regression elsewhere still reports a failure.
        status, output = self.run_main(
            (("cache-set", {"base": [10000] * 3, "head": [10100] * 3}), counts[1])
        )
        self.assertEqual(status, 1)
        self.assertIn("FAIL cache-set:", output)

    def run_main(self, counts):
        output = io.StringIO()
        with (
            patch.object(insn_count.shutil, "which", return_value="/tool"),
            patch.object(insn_count, "revision", side_effect=["base-sha", "head-sha"]),
            patch.object(insn_count, "run", return_value="valgrind-3.26.0"),
            patch.object(insn_count, "benchmark", return_value=counts),
            contextlib.redirect_stdout(output),
        ):
            status = insn_count.main(["--base", "main", "--head", "topic"])
        return status, output.getvalue()

    def test_measure_rejects_wrong_completed_operation_count_and_cleans_up(self):
        workload = insn_count.WORKLOADS[0]
        with tempfile.TemporaryDirectory() as scratch:
            directory = Path(scratch) / "run"
            with patch.object(insn_count, "run", return_value="nabedra-insn-count: oltp-storage 0"):
                with self.assertRaisesRegex(ValueError, "expected .*oltp-storage 512"):
                    insn_count.measure("valgrind", Path("/binary"), workload, directory)
            self.assertFalse(directory.exists())

    def test_build_work_uses_the_selected_directory(self):
        with tempfile.TemporaryDirectory() as scratch:
            work_dir = Path(scratch) / "on-disk"
            with patch.object(insn_count, "checkout", side_effect=ValueError("checkout failed")) as checkout:
                with self.assertRaisesRegex(ValueError, "checkout failed"):
                    list(insn_count.benchmark("base", "head", "cargo", "valgrind", [], work_dir))
            self.assertEqual(checkout.call_args.args[0].parent.parent, work_dir)
            self.assertEqual(list(work_dir.iterdir()), [])

    def test_cleanup_after_profiler_timeout(self):
        workload = insn_count.WORKLOADS[0]
        with tempfile.TemporaryDirectory() as scratch:
            directory = Path(scratch) / "run"
            with patch.object(insn_count, "run", side_effect=subprocess.TimeoutExpired("valgrind", 180)):
                with self.assertRaises(subprocess.TimeoutExpired):
                    insn_count.measure("valgrind", Path("/binary"), workload, directory)
            self.assertFalse(directory.exists())

    def test_one_regression_fails_even_if_another_workload_passes(self):
        counts = (
            ("cache-set", {"base": [10000] * 3, "head": [10051] * 3}),
            ("olap-queue", {"base": [10000] * 3, "head": [10000] * 3}),
        )
        output = io.StringIO()
        with (
            patch.object(insn_count.shutil, "which", return_value="/tool"),
            patch.object(insn_count, "revision", side_effect=["base-sha", "head-sha"]),
            patch.object(insn_count, "run", return_value="valgrind-3.26.0"),
            patch.object(insn_count, "benchmark", return_value=counts),
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(insn_count.main(["--base", "main", "--head", "topic"]), 1)
        self.assertIn("FAIL cache-set:", output.getvalue())
        self.assertIn("PASS olap-queue:", output.getvalue())

    def test_missing_valgrind_requires_explicit_skip(self):
        with (
            patch.object(insn_count.shutil, "which", return_value=None),
            contextlib.redirect_stdout(io.StringIO()) as output,
        ):
            args = ["--base", "main", "--head", "topic"]
            self.assertEqual(insn_count.main(args), 2)
            self.assertEqual(insn_count.main([*args, "--skip-missing-valgrind"]), 0)
        self.assertIn(insn_count.MISSING_COUNTER, output.getvalue())


if __name__ == "__main__":
    unittest.main()

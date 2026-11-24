#!/usr/bin/env python3
"""Tests for tla_common.py and tla_check.py, on captured TLC v1.8.0 `-tool` output.

The fixtures in testdata/tla were captured from real runs: the TwoPhase self-test baseline (pass) and
its mutants (invariant, liveness lasso), small purpose-built specs (deadlock, coverage gap, a state cap
through TLCSet("exit"), a stop at the time budget through tlc2.TLC.stopAfter, a parse error and an
evaluation error), a simulation, and trace validation (accepted and rejected). The orchestration tests
replace specs/tools/tlc with a fake wrapper that prints a fixture per cfg, so they need no Docker.
"""

import io
import json
import os
import shutil
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tla_check  # noqa: E402
import tla_common as tc  # noqa: E402

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "testdata" / "tla"
SELFTEST = HERE.parents[1] / "specs" / "selftest"


def fixture(name: str) -> str:
    return (FIXTURES / f"{name}.txt").read_text(encoding="utf-8")


class ParseOutputTests(unittest.TestCase):
    def test_pass_records_states_depth_and_coverage(self):
        res = tc.parse_tlc_output(fixture("pass"), 0)
        self.assertTrue(res.complete)
        self.assertEqual((res.generated, res.distinct, res.left, res.depth), (11376, 2750, 0, 15))
        self.assertEqual(res.violations, [])
        self.assertEqual(res.errors, [])
        self.assertEqual(len(res.coverage), 11)
        self.assertEqual(res.coverage["RMPrepare"], (21, 1269))
        # An inline \E over a variable is reported under Next with its location.
        self.assertIn("Next@152:8", res.coverage)
        self.assertTrue(all(generated > 0 for _, generated in res.coverage.values()))

    def test_invariant_violation_names_the_property(self):
        res = tc.parse_tlc_output(fixture("invariant"), 12)
        self.assertFalse(res.complete)
        self.assertEqual(res.violation(), ("invariant", "Consistent"))
        self.assertEqual(res.cex_states, 6)
        self.assertFalse(res.lasso)
        self.assertEqual(res.errors, [])

    def test_liveness_lasso(self):
        res = tc.parse_tlc_output(fixture("liveness"), 13)
        self.assertEqual(res.violation(), ("temporal", "Termination"))
        self.assertTrue(res.lasso)
        self.assertEqual(res.cex_states, 10)
        self.assertEqual(res.errors, [])

    def test_deadlock(self):
        res = tc.parse_tlc_output(fixture("deadlock"), 11)
        self.assertEqual(res.violation(), ("deadlock", ""))
        self.assertEqual(res.cex_states, 3)

    def test_coverage_gap_counts_generated_not_distinct(self):
        res = tc.parse_tlc_output(fixture("coverage_gap"), 0)
        self.assertTrue(res.complete)
        self.assertEqual(res.coverage["Never"], (0, 0))
        # A terminal stuttering step generates only states already seen: covered, with 0 distinct.
        self.assertEqual(res.coverage["Done"], (0, 1))

    def test_state_cap_is_incomplete(self):
        res = tc.parse_tlc_output(fixture("state_cap"), 0)
        self.assertFalse(res.complete)
        self.assertEqual((res.distinct, res.left), (2001, 252))
        self.assertEqual(res.violations, [])
        self.assertEqual(res.errors, [])
        self.assertIn("252 states left on queue", tc.incomplete_reason(res, 900))

    def test_time_budget_is_incomplete(self):
        res = tc.parse_tlc_output(fixture("time_budget"), 0)
        self.assertFalse(res.complete)
        self.assertEqual(res.left, 29355)
        res.timed_out = True
        self.assertIn("900 s time budget", tc.incomplete_reason(res, 900))

    def test_errors(self):
        parse = tc.parse_tlc_output(fixture("parse_error"), 150)
        self.assertTrue(parse.errors)
        self.assertIn("3002", parse.errors[0])
        evaluation = tc.parse_tlc_output(fixture("eval_error"), 255)
        self.assertTrue(any("Head to the empty sequence" in e for e in evaluation.errors))
        self.assertEqual(evaluation.violations, [])

    def test_simulation(self):
        res = tc.parse_tlc_output(fixture("simulation"), 0)
        self.assertTrue(res.simulation_done)
        self.assertEqual(res.generated, 10154)
        self.assertEqual(res.violations, [])

    def test_trace_outputs(self):
        accepted = tc.parse_tlc_output(fixture("trace_accepted"), 0)
        self.assertTrue(accepted.complete)
        self.assertIsNone(accepted.trace_rejected_at)
        rejected = tc.parse_tlc_output(fixture("trace_rejected"), 10)
        self.assertEqual(rejected.violation(), ("postcondition", "TraceAccepted"))
        self.assertEqual((rejected.trace_rejected_at, rejected.trace_lines), (6, 9))
        self.assertEqual(rejected.errors, [])

    def test_garbage_is_an_error_only_with_a_nonzero_exit(self):
        self.assertTrue(tc.parse_tlc_output("java: command not found\n", 127).errors)
        self.assertEqual(tc.parse_tlc_output("", 0).errors, [])

    def test_counterexample_text(self):
        lines = tc.counterexample_text(fixture("invariant"))
        self.assertTrue(lines[0].startswith("1: <Initial predicate>"))


class CfgAndBudgetTests(unittest.TestCase):
    def test_selftest_cfgs(self):
        base = tc.parse_cfg(SELFTEST / "TwoPhase.cfg")
        self.assertEqual(base.invariants, ["TypeOK", "Consistent"])
        self.assertEqual(base.properties, ["Termination"])
        self.assertIsNone(base.expect)
        self.assertIn("TypeOK", base.no_mutant)
        self.assertTrue(base.check_deadlock)
        self.assertEqual(tc.parse_cfg(SELFTEST / "mutants" / "no_resend.cfg").expect, "Termination")
        trace = tc.parse_cfg(SELFTEST / "TwoPhaseTrace.cfg")
        self.assertFalse(trace.check_deadlock)

    def test_cfg_comments_and_singular_keywords(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "x.cfg"
            path.write_text("\\* expect: Safe\n(* INVARIANT Hidden *)\nINVARIANT Safe \\* Ignored\n"
                            "PROPERTY Live\nCHECK_DEADLOCK FALSE\n")
            cfg = tc.parse_cfg(path)
        self.assertEqual((cfg.expect, cfg.invariants, cfg.properties, cfg.check_deadlock),
                         ("Safe", ["Safe"], ["Live"], False))

    def test_budget_from_level(self):
        with tempfile.TemporaryDirectory() as d:
            modules = Path(d) / "modules.toml"
            modules.write_text('[[module]]\nname = "a"\nlevel = "L3"\n[[module]]\nname = "b"\nlevel = "L2"\n')
            self.assertEqual(tc.budget_for("a", path=modules), 3600)
            self.assertEqual(tc.budget_for("b", path=modules), 900)
            self.assertEqual(tc.budget_for("b", 60, path=modules), 60)
            with self.assertRaises(tc.TlaError):
                tc.budget_for("missing", path=modules)
            with self.assertRaises(tc.TlaError):
                tc.budget_for("a", 0, path=modules)

    def test_repository_levels(self):
        self.assertEqual(tc.budget_for("oltp"), 3600)
        self.assertEqual(tc.budget_for("cache"), 900)

    def test_summary_round_trip(self):
        out = io.StringIO()
        with redirect_stdout(out):
            tc.print_summary({"verdict": "pass", "executed": 1})
            tc.print_summary({"verdict": "fail", "executed": 2})
        self.assertEqual(tc.read_summary("noise\n" + out.getvalue())["verdict"], "fail")


FAKE_WRAPPER = textwrap.dedent('''\
    #!/usr/bin/env python3
    # A stand-in for specs/tools/tlc: prints the fixture FAKE_TLC_MAP names for the -config file.
    import json, os, sys
    args = sys.argv[1:]
    cfg = os.path.basename(args[args.index("-config") + 1])
    mapping = json.loads(os.environ["FAKE_TLC_MAP"])
    name, code = mapping.get(cfg, mapping.get("*", ["pass", 0]))
    sys.stdout.write(open(os.path.join(os.environ["FAKE_TLC_FIXTURES"], name + ".txt")).read())
    sys.exit(code)
''')


class FakeTlc:
    """A temporary spec folder (a copy of the self-test's cfgs) and a fake wrapper."""

    def __init__(self, mapping: dict, drop_mutants: tuple = ()):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.spec = root / "spec"
        (self.spec / "mutants").mkdir(parents=True)
        for name in ("TwoPhase.cfg", "TwoPhase.large.cfg", "MCTwoPhase.tla"):
            shutil.copy(SELFTEST / name, self.spec / name)
        for m in (SELFTEST / "mutants").glob("*.cfg"):
            if m.stem not in drop_mutants:
                shutil.copy(m, self.spec / "mutants" / m.name)
        self.wrapper = root / "fake-tlc"
        self.wrapper.write_text(FAKE_WRAPPER)
        self.wrapper.chmod(0o755)
        self.out = root / "out"
        self.env = {"NABEDRA_TLC_WRAPPER": str(self.wrapper), "FAKE_TLC_MAP": json.dumps(mapping),
                    "FAKE_TLC_FIXTURES": str(FIXTURES)}

    def run(self, *extra: str) -> tuple[int, dict, str]:
        out = io.StringIO()
        with patch.dict(os.environ, self.env), redirect_stdout(out):
            code = tla_check.main([str(self.spec), "TwoPhase", "--budget-s", "60", "--out", str(self.out), *extra])
        return code, tc.read_summary(out.getvalue()), out.getvalue()

    def close(self):
        self.tmp.cleanup()


GOOD = {"TwoPhase.cfg": ["pass", 0], "commit_one_prepared.cfg": ["invariant", 12],
        "no_resend.cfg": ["liveness", 13]}


class CheckOrchestrationTests(unittest.TestCase):
    def run_fake(self, mapping, drop_mutants=(), extra=()):
        fake = FakeTlc(mapping, drop_mutants)
        self.addCleanup(fake.close)
        return fake.run(*extra)

    def test_pass(self):
        code, summary, text = self.run_fake(GOOD)
        self.assertEqual((code, summary["verdict"]), (0, "pass"))
        self.assertEqual((summary["executed"], summary["failed"]), (6, 0))
        tla = summary["tla"]
        self.assertEqual((tla["mode"], tla["distinct_states"], tla["depth"], tla["actions"]), ("exhaustive", 2750, 15, 11))
        self.assertEqual(tla["unmutated"], ["TypeOK"])
        self.assertEqual({m["name"]: (m["ok"], m["cex_length"]) for m in tla["mutants"]},
                         {"commit_one_prepared": (True, 6), "no_resend": (True, 10)})
        self.assertIn("tla/summary.json", [a["name"] for a in summary["artifacts"]])

    def test_mutant_failing_on_another_property_fails(self):
        code, summary, text = self.run_fake(dict(GOOD, **{"no_resend.cfg": ["invariant", 12]}))
        self.assertEqual((code, summary["verdict"], summary["failed"]), (1, "fail", 1))
        self.assertIn("expected Termination", text)

    def test_mutant_that_passes_or_errors_fails(self):
        for fx in (["pass", 0], ["eval_error", 255], ["deadlock", 11]):
            with self.subTest(fx=fx[0]):
                code, summary, _ = self.run_fake(dict(GOOD, **{"commit_one_prepared.cfg": fx}))
                self.assertEqual(summary["verdict"], "fail")
                mutant = next(m for m in summary["tla"]["mutants"] if m["name"] == "commit_one_prepared")
                self.assertFalse(mutant["ok"])

    def test_baseline_violations_fail(self):
        for fx in (["invariant", 12], ["liveness", 13], ["deadlock", 11]):
            with self.subTest(fx=fx[0]):
                code, summary, _ = self.run_fake(dict(GOOD, **{"TwoPhase.cfg": fx}))
                self.assertEqual((code, summary["verdict"]), (1, "fail"))
                self.assertEqual(summary["tla"]["runs"][0]["verdict"], {"invariant": "invariant",
                                 "liveness": "temporal", "deadlock": "deadlock"}[fx[0]])

    def test_coverage_gap_fails(self):
        code, summary, text = self.run_fake(dict(GOOD, **{"TwoPhase.cfg": ["coverage_gap", 0]}))
        self.assertEqual(summary["verdict"], "fail")
        self.assertEqual(summary["tla"]["uncovered"], ["Never"])
        self.assertIn("zero coverage: Never", text)

    def test_state_cap_and_budget_are_not_run_never_pass(self):
        for fx in ("state_cap", "time_budget"):
            with self.subTest(fx=fx):
                code, summary, _ = self.run_fake(dict(GOOD, **{"TwoPhase.cfg": [fx, 0]}))
                self.assertEqual((code, summary["verdict"]), (2, "not-run"))
                self.assertTrue(any("states left on queue" in n["reason"] for n in summary["not_run"]))

    def test_baseline_tlc_error_is_error(self):
        code, summary, _ = self.run_fake(dict(GOOD, **{"TwoPhase.cfg": ["parse_error", 150]}))
        self.assertEqual((code, summary["verdict"]), (3, "error"))

    def test_property_without_mutant_fails(self):
        code, summary, text = self.run_fake(GOOD, drop_mutants=("no_resend",))
        self.assertEqual(summary["verdict"], "fail")
        self.assertIn("mutant rule Termination: no mutant expects it", text)

    def test_mutant_without_expect_line_fails(self):
        fake = FakeTlc(GOOD)
        self.addCleanup(fake.close)
        path = fake.spec / "mutants" / "no_resend.cfg"
        path.write_text(path.read_text().replace("\\* expect: Termination\n", ""))
        code, summary, text = fake.run()
        self.assertEqual(summary["verdict"], "fail")
        self.assertIn("first line is not", text)

    def test_t2_simulation_is_labelled(self):
        code, summary, _ = self.run_fake({"TwoPhase.large.cfg": ["simulation", 0]},
                                         extra=("--tier", "T2", "--simulate", "--seed", "7", "--num", "200"))
        self.assertEqual((code, summary["verdict"]), (0, "pass"))
        self.assertEqual((summary["tla"]["mode"], summary["tla"]["seed"], summary["tla"]["tier"]),
                         ("simulation", 7, "T2"))

    def test_missing_spec_is_error(self):
        out = io.StringIO()
        with redirect_stdout(out):
            code = tla_check.main(["/nonexistent/spec", "X", "--budget-s", "5"])
        self.assertEqual(code, 3)
        self.assertEqual(tc.read_summary(out.getvalue())["verdict"], "error")


if __name__ == "__main__":
    unittest.main()

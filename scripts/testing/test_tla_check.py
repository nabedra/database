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
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evidence  # noqa: E402
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

    def test_success_without_final_statistics_is_not_exhaustive(self):
        # Crafted: TLC's success message alone does not show that the queue was empty.
        text = fixture("pass").replace("STARTMSG 2199:0", "STARTMSG 2299:0").replace("ENDMSG 2199", "ENDMSG 2299")
        res = tc.parse_tlc_output(text, 0)
        self.assertIsNone(res.left)
        self.assertFalse(res.complete)

    def test_unreadable_coverage_line_is_kept(self):
        text = fixture("pass").replace("<RMPrepare line", "<RMPrepare(r) line", 1)
        res = tc.parse_tlc_output(text, 0)
        self.assertNotIn("RMPrepare", res.coverage)
        self.assertEqual(len(res.coverage_unparsed), 1)

    def test_a_later_coverage_report_replaces_an_unreadable_one(self):
        report = re.search(r"@!@!@STARTMSG 2201:0 @!@!@.*?ENDMSG 2202 @!@!@\n", fixture("pass"), re.S).group(0)
        text = fixture("pass").replace(report, report.replace("<RMPrepare line", "<RMPrepare(r) line", 1) + report)
        self.assertEqual(tc.parse_tlc_output(text, 0).coverage_unparsed, [])
        self.assertEqual(tc.parse_tlc_output(text, 0).coverage["RMPrepare"], (21, 1269))

    def test_tlc_bug_message_is_an_error(self):
        text = fixture("pass").replace("@!@!@STARTMSG 2193:0 @!@!@",
                                       "@!@!@STARTMSG 1000:2 @!@!@\nTLC Bug: something broke\n"
                                       "@!@!@ENDMSG 1000 @!@!@\n@!@!@STARTMSG 2193:0 @!@!@")
        self.assertTrue(tc.parse_tlc_output(text, 0).errors)

    def test_unnamed_temporal_violation(self):
        text = fixture("liveness").replace("Temporal properties Termination were violated",
                                           "Temporal properties were violated")
        text = text.replace("Temporal property Termination was violated", "Temporal properties were violated")
        self.assertEqual(tc.parse_tlc_output(text, 13).violation(), ("temporal", ""))

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
    # A stand-in for specs/tools/tlc: prints the fixture FAKE_TLC_MAP names for the -config file,
    # with FAKE_TLC_EDIT's [old, new] replacements, then sleeps FAKE_TLC_SLEEP seconds.
    import json, os, sys, time
    args = sys.argv[1:]
    cfg = os.path.basename(args[args.index("-config") + 1])
    mapping = json.loads(os.environ["FAKE_TLC_MAP"])
    name, code = mapping.get(cfg, mapping.get("*", ["pass", 0]))
    text = open(os.path.join(os.environ["FAKE_TLC_FIXTURES"], name + ".txt")).read()
    for old, new in json.loads(os.environ.get("FAKE_TLC_EDIT", "[]")):
        text = text.replace(old, new)
    sys.stdout.write(text)
    sys.stdout.flush()
    time.sleep(float(os.environ.get("FAKE_TLC_SLEEP", "0")))
    sys.exit(code)
''')


def edit(path: Path, *pairs: tuple[str, str]) -> None:
    text = path.read_text()
    for old, new in pairs:
        if old not in text:
            raise AssertionError(f"{path.name} does not contain {old!r}")
        text = text.replace(old, new)
    path.write_text(text)


def add_to_cfg(*lines: str, cfg: str = "TwoPhase.cfg"):
    """A prep step that adds lines to a cfg."""
    return lambda spec: edit(spec / cfg, ("CHECK_DEADLOCK TRUE", "\n".join(lines) + "\nCHECK_DEADLOCK TRUE"))


def add_to_model(*lines: str, extends: str = ""):
    """A prep step that adds definitions, and modules to EXTENDS, to MCTwoPhase.tla."""
    return lambda spec: edit(spec / "MCTwoPhase.tla", ("MCRMLarge ==", "\n".join(lines) + "\nMCRMLarge =="),
                             ("EXTENDS TwoPhase", "EXTENDS TwoPhase" + extends))


def run_check_errors(summary: dict) -> list[str] | None:
    """Why the schema refuses the summary's facts, or None."""
    return evidence.schema_errors(summary["tla"], evidence.load_schema()["properties"]["tla"]) or None


NO_LIVENESS = (("PROPERTIES Termination\n", ""),)
# FAKE_TLC_EDIT: the fixtures come from runs with a liveness property. This drops TLC's message about it.
SAFETY_LOG = ["MSG 2192", "MSG 2190"]


def safety_only(spec: Path) -> None:
    for cfg in ("TwoPhase.cfg", "TwoPhase.large.cfg"):
        edit(spec / cfg, *NO_LIVENESS)


class FakeTlc:
    """A temporary spec folder (a copy of the self-test's cfgs) and a fake wrapper."""

    process = False

    def __init__(self, mapping: dict, drop_mutants: tuple = (), **env: str):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.spec = root / "spec"
        (self.spec / "mutants").mkdir(parents=True)
        for name in ("TwoPhase.cfg", "TwoPhase.large.cfg", "TwoPhase.tla", "MCTwoPhase.tla"):
            shutil.copy(SELFTEST / name, self.spec / name)
        for m in (SELFTEST / "mutants").glob("*.cfg"):
            if m.stem not in drop_mutants:
                shutil.copy(m, self.spec / "mutants" / m.name)
        self.wrapper = root / "fake-tlc"
        self.wrapper.write_text(FAKE_WRAPPER)
        self.wrapper.chmod(0o755)
        self.out = root / "out"
        self.env = {"NABEDRA_TLC_WRAPPER": str(self.wrapper), "FAKE_TLC_MAP": json.dumps(mapping),
                    "FAKE_TLC_FIXTURES": str(FIXTURES), **env}

    def run(self, *extra: str, budget: str = "60") -> tuple[int, dict, str]:
        out = io.StringIO()
        # The fake wrapper is a process only where a test times or kills it. Otherwise it runs in this process.
        run_tlc = tc.run_tlc if "FAKE_TLC_SLEEP" in self.env or self.process else self.run_tlc
        with patch.dict(os.environ, self.env), redirect_stdout(out), patch.object(tc, "run_tlc", run_tlc):
            code = tla_check.main([str(self.spec), "TwoPhase", "--budget-s", budget, "--out", str(self.out), *extra])
        return code, tc.read_summary(out.getvalue()), out.getvalue()

    def run_tlc(self, spec_dir, args, *, out_dir, log_path, budget_s):
        mapping = json.loads(self.env["FAKE_TLC_MAP"])
        name, code = mapping.get(Path(args[args.index("-config") + 1]).name, ["pass", 0])
        text = fixture(name)
        for old, new in json.loads(self.env.get("FAKE_TLC_EDIT", "[]")):
            text = text.replace(old, new)
        log_path.write_text(text)
        return tc.parse_tlc_output(text, code), 0.1

    def close(self):
        self.tmp.cleanup()


GOOD = {"TwoPhase.cfg": ["pass", 0], "commit_one_prepared.cfg": ["invariant", 12],
        "no_resend.cfg": ["liveness", 13]}


class CheckOrchestrationTests(unittest.TestCase):
    def run_fake(self, mapping, drop_mutants=(), extra=(), budget="60", prep=(), **env):
        fake = FakeTlc(mapping, drop_mutants, **env)
        self.addCleanup(fake.close)
        for step in prep:
            step(fake.spec)
        self.fake = fake
        return fake.run(*extra, budget=budget)

    def assert_fails(self, *fragments, failed=1, mapping=None, prep=(), **kw):
        """The check fails with exactly `failed` assertions, and its output has every fragment."""
        code, summary, text = self.run_fake(mapping or GOOD, prep=prep, **kw)
        self.assertEqual((code, summary["verdict"], summary["failed"]), (1, "fail", failed), text)
        for fragment in fragments:
            self.assertIn(fragment, text)
        return summary

    def test_pass_through_the_wrapper_process(self):
        with patch.object(FakeTlc, "process", True):
            code, summary, _ = self.run_fake(GOOD)
            self.assertEqual((code, summary["verdict"], summary["executed"]), (0, "pass", 9))
            code, summary, _ = self.run_fake(dict(GOOD, **{"TwoPhase.cfg": ["pass", 1]}))
            self.assertEqual((code, summary["verdict"]), (3, "error"))

    def test_pass(self):
        code, summary, text = self.run_fake(GOOD)
        self.assertEqual((code, summary["verdict"]), (0, "pass"))
        self.assertEqual((summary["executed"], summary["failed"]), (9, 0))
        tla = summary["tla"]
        self.assertNotIn("limits", tla)
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

    def test_unreadable_coverage_fails(self):
        edit = json.dumps([["<RMPrepare line", "<RMPrepare(r) line"]])
        code, summary, text = self.run_fake(GOOD, FAKE_TLC_EDIT=edit)
        self.assertEqual((code, summary["verdict"]), (1, "fail"))
        self.assertIn("unreadable action coverage lines", text)

    def test_success_without_statistics_is_not_run(self):
        edit = json.dumps([["STARTMSG 2199:0", "STARTMSG 2299:0"], ["ENDMSG 2199", "ENDMSG 2299"]])
        code, summary, _ = self.run_fake(GOOD, FAKE_TLC_EDIT=edit)
        self.assertEqual((code, summary["verdict"]), (2, "not-run"))

    def test_killed_at_the_budget_is_not_run(self):
        # TLC ignores its own stop, so the runner kills it: not-run, never error or pass.
        with patch.object(tc, "KILL_GRACE_S", 0):
            code, summary, _ = self.run_fake(dict(GOOD, **{"TwoPhase.cfg": ["time_budget", 137]}),
                                             budget="1", FAKE_TLC_SLEEP="5")
        self.assertEqual((code, summary["verdict"]), (2, "not-run"))
        self.assertTrue(any("time budget" in n["reason"] for n in summary["not_run"]))

    def test_simulation_that_ran_into_the_budget_is_not_run(self):
        code, summary, _ = self.run_fake({"TwoPhase.large.cfg": ["simulation", 0]}, budget="1",
                                         extra=("--tier", "T2", "--simulate"), FAKE_TLC_SLEEP="1.2")
        self.assertEqual((code, summary["verdict"]), (2, "not-run"))
        self.assertEqual(summary["tla"]["mode"], "simulation")

    def test_mutant_with_unnamed_liveness_violation_fails(self):
        edit = json.dumps([["Temporal properties Termination were violated", "Temporal properties were violated"],
                           ["Temporal property Termination was violated", "Temporal properties were violated"]])
        code, summary, _ = self.run_fake(GOOD, FAKE_TLC_EDIT=edit)
        self.assertEqual(summary["verdict"], "fail")
        mutant = next(m for m in summary["tla"]["mutants"] if m["name"] == "no_resend")
        self.assertFalse(mutant["ok"])

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

    # A spec that checks nothing.

    def test_no_property_and_no_mutant_fails(self):
        def drop(spec):
            edit(spec / "TwoPhase.cfg", ("INVARIANTS TypeOK Consistent\n", ""), *NO_LIVENESS)
            shutil.rmtree(spec / "mutants")

        self.assert_fails("the baselines check no property", mapping={"TwoPhase.cfg": ["pass", 0]}, prep=[drop])

    def test_only_a_type_invariant_with_a_reason_is_exempt(self):
        def only(spec):
            edit(spec / "TwoPhase.cfg", ("INVARIANTS TypeOK Consistent", "INVARIANTS TypeOK"), *NO_LIVENESS)
            shutil.rmtree(spec / "mutants")

        def exempt(line):
            return lambda spec: edit(spec / "TwoPhase.cfg", ("CONSTANTS\n", f"\\* no-mutant: {line}\nCONSTANTS\n"))

        self.assert_fails("no property besides exempted ones", mapping={"TwoPhase.cfg": ["pass", 0]}, prep=[only])
        for line, fragment in (("Consistent", "gives no reason"), ("Consistent --", "gives no reason"),
                               ("Consistent -- obvious", "only a type invariant"),
                               ("Termination -- obvious", "only a type invariant")):
            with self.subTest(line):
                self.assert_fails(fragment, prep=[exempt(line)])
        self.assert_fails("mutant rule TypeOK: its `no-mutant` line gives no reason", prep=[
            lambda spec: edit(spec / "TwoPhase.cfg", (" -- the type invariant; every run checks it.", ""))])
        # A property cannot be a type invariant, whatever its name.
        self.assert_fails("mutant rule TypeLive: only a type invariant", prep=[
            exempt("TypeLive -- x"),
            lambda spec: edit(spec / "TwoPhase.cfg", ("PROPERTIES Termination", "PROPERTIES Termination TypeLive"))])

    # What TLC printed against its exit status, and the kind of run.

    def test_success_with_a_nonzero_exit_is_error(self):
        for code in (1, 12, 137, 255):
            with self.subTest(code=code):
                got, summary, text = self.run_fake(dict(GOOD, **{"TwoPhase.cfg": ["pass", code]}))
                self.assertEqual((got, summary["verdict"]), (3, "error"))
                self.assertEqual(summary["tla"]["runs"][0]["verdict"], "error")
                self.assertIn(f"TLC exited with status {code}", text)

    def test_message_without_its_end_before_the_success_is_error(self):
        cut = "@!@!@STARTMSG 2110:1 @!@!@\nInvariant Consistent is violated.\n@!@!@STARTMSG 2193:0 @!@!@"
        code, summary, _ = self.run_fake(GOOD, FAKE_TLC_EDIT=json.dumps([["@!@!@STARTMSG 2193:0 @!@!@", cut]]))
        self.assertEqual((code, summary["verdict"]), (3, "error"))

    def test_mutant_violation_with_exit_0_fails(self):
        self.assert_fails("TLC exited with status 0", mapping=dict(GOOD, **{"no_resend.cfg": ["liveness", 0]}))

    def test_mutant_false_in_the_initial_state_fails(self):
        # TLC prints the state in the message, and no counterexample states.
        states = re.findall(r"@!@!@STARTMSG 2217:4 @!@!@\n.*?ENDMSG 2217 @!@!@\n", fixture("invariant"), re.S)
        edits = [["Invariant Consistent is violated.", "Invariant Consistent is violated by the initial state:\nx = 1"],
                 *([s, ""] for s in states)]
        self.assert_fails("false in an initial state", FAKE_TLC_EDIT=json.dumps(edits))

    def test_simulation_output_is_not_an_exhaustive_run(self):
        def second(spec):
            shutil.copy(spec / "TwoPhase.cfg", spec / "TwoPhase.more.cfg")

        code, summary, _ = self.run_fake(dict(GOOD, **{"TwoPhase.more.cfg": ["simulation", 0]}), prep=[second])
        self.assertEqual((code, summary["verdict"]), (2, "not-run"))
        code, summary, _ = self.run_fake({"TwoPhase.large.cfg": ["simulation", 0]}, extra=("--tier", "T2"))
        self.assertEqual((code, summary["verdict"], summary["tla"]["mode"]), (2, "not-run", "exhaustive"))
        code, summary, _ = self.run_fake({"TwoPhase.large.cfg": ["pass", 0]}, extra=("--tier", "T2", "--simulate"))
        self.assertEqual((code, summary["verdict"], summary["tla"]["mode"]), (2, "not-run", "simulation"))

    # The search rule.

    COUNTERS = "\\* limit: Few -- bounds the two fault counters"

    def limited(self, *definitions, cfg=(), name="Few", kind="CONSTRAINT", declared=True):
        """Prep steps: a safety-only baseline with one constraint."""
        lines = [f"{kind} {name}", *([f"\\* limit: {name} -- bounds counters"] if declared else []), *cfg]
        return [safety_only, add_to_model(*definitions), add_to_cfg(*lines)]

    def test_declared_constraint_on_counters_passes_and_is_recorded(self):
        coverage = ("@!@!@STARTMSG 2778:0 @!@!@\n<Few line 8, col 1 to line 8, col 3 of module MCTwoPhase>: {}\n"
                    "@!@!@ENDMSG 2778 @!@!@\n")
        log = json.dumps([SAFETY_LOG, ["@!@!@STARTMSG 2201:0", coverage.format("3:4") + "@!@!@STARTMSG 2201:0"],
                          ["@!@!@STARTMSG 2202:0", coverage.format("5515:7379") + "@!@!@STARTMSG 2202:0"]])
        shapes = [
            ["Few == crashes <= 1"],
            ["Few ==", "    /\\ crashes <= 1", "    /\\ \\A r \\in RM : Cardinality(msgs) =< 2 * 3"],
            ["Few == (\\A r \\in RM : \\A s \\in RM : crashes < 2) /\\ MaxDrops >= drops"],
            ["Max(a, b) == IF a > b THEN a ELSE b", "Few == One /\\ Two(RM)", "One == Max(crashes, drops) \\leq 1",
             "Two(S) == Len(<<crashes, drops>>) < 4"],
            # The bound of a spec on another branch (OltpRanges): history length and timestamps.
            ["Few ==", "    /\\ crashes <= MaxCrashes \\* not = here", "    /\\ \\A g \\in RM : Len(rmState[g]) <= MaxDrops"],
        ]
        for definitions in shapes:
            for kind in ("CONSTRAINT", "CONSTRAINTS", "ACTION_CONSTRAINT"):
                with self.subTest(definitions[0], kind=kind):
                    code, summary, text = self.run_fake(
                        GOOD, prep=self.limited(*definitions, kind=kind), FAKE_TLC_EDIT=log)
                    self.assertEqual((code, summary["verdict"]), (0, "pass"), text)
                    self.assertEqual(summary["tla"]["limits"], [
                        f"TwoPhase.cfg: {kind.removesuffix('S')} Few kept 5515 of 7379 states: bounds counters"])

    def test_constraint_that_is_not_declared_fails(self):
        for kind in ("CONSTRAINT", "ACTION_CONSTRAINT"):
            self.assert_fails(f"{kind} Few has no `\\* limit: Few -- <reason>` line", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                              prep=self.limited("Few == crashes <= 1", kind=kind, declared=False))
        for line in ("\\* limit: Few", "\\* limit: Few --", "\\* limit: Other -- reason", "limit: Few -- reason",
                     "(* limit: Few -- reason *)"):
            with self.subTest(line):
                self.assert_fails("has no `\\* limit: Few", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  prep=self.limited("Few == crashes <= 1", cfg=[line] if line[0] != "l" else [],
                                                    declared=False))

    def test_constraint_that_selects_states_fails_even_when_declared(self):
        for definition in ('Few == \\A r \\in RM : rmState[r] # "committed"', 'Few == tmDecision = "init"',
                           "Few == ~tmUp", "Few == tmUp => crashes <= 1", "Few == crashes <= 1 \\/ tmUp", "Few == TRUE",
                           "Few == tmUp", 'Few == tmDecision \\notin {"committed"}', "Few == msgs \\subseteq {}",
                           "Few == \\E r \\in RM : crashes <= 1", "Few == LET n == crashes IN n <= 1",
                           "Few == IF tmUp THEN crashes <= 1 ELSE drops <= 1", "Few == crashes <= 1 /\\ tmUp",
                           "Few == Inner\nInner == tmUp",
                           "Few == \\A r \\in {s \\in RM : tmUp} : 1 <= 0", "Few == Few", "Few(n) == crashes = n",
                           "Other == crashes <= 1", "  Few == crashes <= 1"):
            with self.subTest(definition):
                self.assert_fails("FAIL search of TwoPhase.cfg: CONSTRAINT Few: ", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  prep=self.limited(definition))

    def bound(self, body, extra=""):
        """What the rule says about `Few`, in a module with the given definition."""
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "M.tla").write_text(
                "---- MODULE M ----\nEXTENDS Naturals\nCONSTANT S\nVARIABLES n, m, up\nvars == <<n, m, up>>\n"
                f"{extra}Few =={body}\nAfter == n\n====\n")
            return tla_check.Model(Path(d), "M.tla").bound_problem("Few")

    def test_each_selecting_operator_is_refused_beside_one_order_comparison(self):
        # Each body has one order comparison outside every bracket, so only the operator can refuse it.
        bodies = {
            "=": "n <= f[m = 1]", "#": "n <= f[m # 1]", "/=": "n <= f[m /= 1]", "\\neq": "n <= f[m \\neq 1]",
            "\\notin": "n <= f[m \\notin S]", "~": "~ n <= 1", "\\lnot": "\\lnot n <= 1", "\\neg": "\\neg n <= 1",
            "=>": "up => n <= 1", "<=>": "up <=> n <= 1", "\\equiv": "up \\equiv n <= 1", "\\/": "up \\/ n <= 1",
            "\\lor": "n <= 1 \\lor up", "\\E": "\\E s \\in S : n <= 1", "\\A": "\\A s \\in {t \\in S : up} : 1 <= 0",
            "\\subseteq": "n <= f[S \\subseteq {m}]", "==": "n <= 1\n  Other == m", '"': 'n <= f["a"]',
            "IF": "IF up THEN n <= 1 ELSE up", "CASE": "CASE up -> n <= 1 [] OTHER -> up", "ENABLED": "ENABLED n <= 1",
            "CHOOSE": "CHOOSE b \\in BOOLEAN : n <= 1", "LAMBDA": "LAMBDA x : n <= 1",
        }
        self.assertEqual(set(bodies), tla_check._SELECTS)
        for token, body in bodies.items():
            with self.subTest(token):
                used = "a string" if token == '"' else token
                self.assertIn(f"Few uses {used}, so it selects states", self.bound(" " + body) or "accepted")
        for body in ("n <= f[m]", "n <= m", "(n <= m)"):  # the same shapes without one
            self.assertIsNone(self.bound(" " + body))

    def test_a_body_the_rule_cannot_read_is_refused(self):
        sel = "Sel(p) == p \\/ up\nId(p) == p\nCap == n <= 3\n"
        refused = {
            'a body in column 1': '\nup["r1"] # "x"', "a quantifier body in column 1": " \\A s \\in S :\nup[s] # m",
            "a second conjunct in column 1": " n <= 1 /\\\nup", "an operand in column 1": " n <=\n1 \\/ up",
            "a selecting call around the bound": " Sel(n <= 1)", "an identity call": " Id(n <= 1)", "an application": " f[n <= 1]",
            "a set": " {n <= 1}", "membership": " (n <= 1) \\in {up}", "an empty body": "", "an empty quantifier": " \\A s \\in S :",
            "an empty last conjunct": " n <= 1 /\\", "an empty conjunct": " n <= 1 /\\ () /\\ m <= 1", "no left operand": " <= 1",
            "no right operand": " n <=", "an open bracket": " (n <= 1", "a closed bracket": " n <= 1)", "two comparisons": " n <= f[m <= 1]",
            "a call after the bound": " n <= 1 /\\ Sel(up)", "a nested group": " n <= 1 /\\ (m <= 1 /\\ up)",
            "a quantified group": " \\A s \\in S : (n <= 1 /\\ \\A t \\in S : up)", "a name of a variable": " up",
            "Unicode not-equal": " up \u2260 D \\/ n <= 0", "Unicode or": " up \u2228 n <= 0", "Unicode implies": " (up) \u21d2 n <= 0",
            "Unicode and": " n <= 1 \u2227 up", "Unicode order": " n \u2264 1", "Unicode not": " \u00ac(n <= 1)",
        }
        for name, body in refused.items():
            with self.subTest(name):
                self.assertIsNotNone(self.bound(body, sel))
        accepted = {
            "a body in column 1": "\nn <= 1", "a bulleted list": "\n  /\\ n <= 1\n  /\\ m <= 2", "in column 1": "\n/\\ n <= 1\n/\\ m <= 2",
            "a quantifier before a list": "\n  \\A s \\in S :\n    /\\ n[s] <= 1\n    /\\ m <= 2", "a nested group": " n <= 1 /\\ (m <= 1 /\\ (\\A s \\in S : n[s] < m))",
            "calls": " Cap /\\ (Cap) /\\ \\A s \\in S : Cap", "a set as an operand": " Cardinality({s \\in S : up[s]}) <= 2",
            "an expression around the operand": " (n + m) <= 1", "a tuple": " Len(<<n, m>>) =< 3", "\\land and \\leq": " n \\leq 1 \\land m \\geq 0",
            "a multi-line operand in column 1": " n <= Max(m,\nn)",
        }
        for name, body in accepted.items():
            with self.subTest(name):
                self.assertIsNone(self.bound(body, sel))

    def test_a_body_ends_at_the_next_unit(self):
        for unit in ("VARIABLE x", "CONSTANT X", "ASSUME up", "THEOREM up", "LOCAL Z == up", "RECURSIVE F(_)", "INSTANCE M2",
                     "Z(a,\n  b) == up", "Z[a \\in S] == up", "----", "====\nup"):
            with self.subTest(unit):
                self.assertIsNone(self.bound(f" n <= 1\n{unit}"))

    def test_an_override_cannot_bring_in_the_search_state(self):
        def model(*definitions):
            return [safety_only, add_to_model(*definitions, "L == Lvl(1)", "Few == L < 9", extends=", TLC\nCONSTANT Lvl(_)"),
                    add_to_cfg("CONSTRAINT Few", "\\* limit: Few -- counters")]

        depth = model()

        def override(line):
            return lambda spec: edit(spec / "TwoPhase.cfg", ("CONSTANTS\n", f"CONSTANTS\n    {line}\n"))

        for line in ("Lvl <- TLCGet", "Lvl <- [MCTwoPhase]TLCGet", "Lvl <- RandomElement", "Lvl <- JavaTime"):
            with self.subTest(line):
                summary = self.assert_fails("the cfg uses", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[*depth, override(line)])
                self.assertEqual(summary["tla"]["runs"][0]["verdict"], "pass")  # TLC itself saw nothing
        # Any other operator that the model does not define, under another name.
        for line in ("Lvl <- Trace", "Lvl <- Cardinality", "Lvl <- [MCTwoPhase] SortSeq"):
            with self.subTest(line):
                self.assert_fails("which the model's modules do not define", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  prep=[*depth, override(line)])
        self.assert_fails("the cfg uses TLCGet", prep=[lambda spec: edit(spec / "TwoPhase.cfg", (
            "CHECK_DEADLOCK TRUE", "ALIAS TLCGet\nCHECK_DEADLOCK TRUE"))])
        # A comment is no use, and an operator of the model may take the constant's place, wherever it is defined.
        for definition in ("Own(x) == 1", "  Own(x) == 1", "Own[x \\in RM] == 1"):
            code, _, text = self.run_fake(GOOD, FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[
                *model(definition), override("Lvl <- Own \\* not TLCGet")])
            self.assertEqual(code, 0, text)

    def test_depth_limit_and_state_cap_fail(self):
        for body in ('TLCGet("level") < 9', 'TLCGet("distinct") < 800', 'TLCGet("stats").diameter < 5',
                     "TLCGetOrDefault(1, 0) <= 5", "JavaTime < 5", "RandomElement(1..3) <= 2"):
            with self.subTest(body):
                self.assert_fails("FAIL search of the model: MCTwoPhase.tla uses", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  failed=2 if '"' in body else 1, prep=self.limited(f"Few == {body}"))
        # Anywhere in the model, also in the design module and behind an operator.
        self.assert_fails("TwoPhase.tla uses TLCSet", prep=[
            lambda spec: edit(spec / "TwoPhase.tla", ("vars ==", 'Stop == TLCSet("exit", TRUE)\nvars =='))])
        for module in ("TLCExt", "Randomization", "IOUtils"):
            self.assert_fails(f"MCTwoPhase.tla uses the module {module}", prep=[add_to_model(extends=f", {module}")])
            self.assert_fails(f"uses the module {module}", prep=[add_to_model(f"X == INSTANCE {module}")])
        self.assert_fails("Helper.tla uses TLCGet", prep=[
            add_to_model(extends=", Helper"),
            lambda spec: (spec / "Helper.tla").write_text("---- MODULE Helper ----\nD == TLCGet(1)\n====\n")])
        # A comment or a string is not a use.
        code, summary, text = self.run_fake(GOOD, prep=[add_to_model(
            '(* TLCGet("level") (* nested *) *)', "\\* RandomElement", 'Text == "TLCGet"')])
        self.assertEqual(code, 0, text)

    def test_text_around_a_module_does_not_count(self):
        def around(before, after):
            return lambda spec: (spec / "MCTwoPhase.tla").write_text(
                before + (spec / "MCTwoPhase.tla").read_text() + after)

        code, _, text = self.run_fake(GOOD, prep=[around("A note on TLCGet.\n", "\nTLCSet and RandomElement are refused.\n")])
        self.assertEqual(code, 0, text)
        # Nor can it declare a constant or define a constraint that the model has not got.
        self.assert_fails("override Nat has no", prep=[around("", "\nCONSTANT Nat\n"), lambda spec: edit(
            spec / "TwoPhase.cfg", ("CONSTANTS\n", "CONSTANTS\n    Nat <- Mine\n"))])
        self.assert_fails("CONSTRAINT Few: ", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[
            *self.limited("Other == crashes <= 1"), around("", "\nFew == crashes <= 1\n")])

    def test_unreadable_statistics_are_an_error_with_no_counts(self):
        stats = re.search(r"@!@!@STARTMSG 2199:0 @!@!@\n.*?ENDMSG 2199 @!@!@\n", fixture("pass"), re.S).group(0)
        second = re.sub(r"(@!@!@STARTMSG 2199:0 @!@!@\n).*\n", r"\g<1>1.2M states generated, 55K left.\n", stats)
        edits = json.dumps([[stats, stats + second]])
        code, summary, text = self.run_fake(GOOD, FAKE_TLC_EDIT=edits)
        self.assertEqual((code, summary["verdict"]), (3, "error"), text)
        self.assertEqual((summary["tla"]["distinct_states"], summary["tla"]["runs"][0]["states_left"]), (None, None))
        self.assertIn("unreadable statistics", summary["not_run"][0]["reason"])
        self.assertIsNone(run_check_errors(summary))
        code, summary, _ = self.run_fake({"TwoPhase.large.cfg": ["pass", 0]}, extra=("--tier", "T2"), FAKE_TLC_EDIT=edits)
        self.assertEqual((code, summary["verdict"], summary["tla"]["distinct_states"]), (3, "error", None))

    def test_t2_applies_the_search_rule(self):
        cap = [add_to_model('Few == TLCGet("level") < 9')]
        for extra in (("--tier", "T2"), ("--tier", "T2", "--simulate")):
            fx = "simulation" if "--simulate" in extra else "pass"
            summary = self.assert_fails("search of the model", mapping={"TwoPhase.large.cfg": [fx, 0]}, extra=extra, prep=cap)
            self.assertEqual(summary["executed"], 3)
        self.assert_fails("CONSTRAINT Few has no", mapping={"TwoPhase.large.cfg": ["pass", 0]}, extra=("--tier", "T2"),
                          FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[
                              safety_only, add_to_model("Few == crashes <= 1"),
                              add_to_cfg("CONSTRAINT Few", cfg="TwoPhase.large.cfg")])

    def test_liveness_under_a_constraint_or_symmetry_fails(self):
        summary = self.assert_fails("liveness under the constraint Few", prep=[
            add_to_model("Few == crashes <= 1"), add_to_cfg("CONSTRAINT Few", self.COUNTERS)])
        self.assertEqual(summary["tla"]["runs"][0]["verdict"], "liveness-limited")
        warning = ("@!@!@STARTMSG 2279:3 @!@!@\nDeclaring symmetry during liveness checking is dangerous.\n"
                   "@!@!@ENDMSG 2279 @!@!@\n@!@!@STARTMSG 2193:0")
        self.assert_fails("liveness under SYMMETRY", FAKE_TLC_EDIT=json.dumps([["@!@!@STARTMSG 2193:0", warning]]),
                          prep=[add_to_cfg("SYMMETRY Sym")])
        # A symmetry set in a safety run is fine: TLC does not warn.
        code, _, text = self.run_fake(GOOD, prep=[safety_only, add_to_cfg("SYMMETRY Sym")],
                                      FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]))
        self.assertEqual(code, 0, text)

    def test_view_fails(self):
        self.assert_fails("VIEW V merges states", prep=[add_to_model("V == rmState"), add_to_cfg("VIEW V")])
        self.assert_fails("VIEW V merges states", prep=[add_to_cfg("VIEW V", "\\* limit: V -- declared")])

    def test_overrides(self):
        def override(line):
            return lambda spec: edit(spec / "TwoPhase.cfg", ("CONSTANTS\n", f"CONSTANTS\n    {line}\n"))

        for name in ("Consistent", "TypeOK", "Termination", "Spec"):
            for form in (f"{name} <- Yes", f"{name} = TRUE", f"{name} <- [TwoPhase] Yes"):
                with self.subTest(form):
                    self.assert_fails(f"the cfg overrides {name}, which it checks or runs", prep=[
                        add_to_model("Yes == TRUE"), override(form)])
        mine = add_to_model("Mine == 1")
        for name in ("Next", "Nat", "SendMsg"):
            self.assert_fails(f"override {name} has no `\\* limit: {name} -- <reason>` line", prep=[mine, override(f"{name} <- Mine")])
            code, summary, text = self.run_fake(GOOD, prep=[mine, override(f"{name} <- Mine"), lambda spec: edit(
                spec / "TwoPhase.cfg", ("CONSTANTS\n", f"\\* limit: {name} -- a finite set\nCONSTANTS\n"))])
            self.assertEqual((code, summary["tla"]["limits"]), (0, [f"TwoPhase.cfg: override {name}: a finite set"]), text)
        # The cfg that declares it is the one that overrides it.
        self.assert_fails("override Nat has no", prep=[mine, override("Nat <- Mine"), lambda spec: edit(
            spec / "TwoPhase.large.cfg", ("CONSTANTS\n", "\\* limit: Nat -- elsewhere\nCONSTANTS\n"))])
        # INIT, NEXT and POSTCONDITION names are the cfg's own too.
        for lines, name in ((("INIT Init", "NEXT Next"), "Init"), (("INIT Init", "NEXT Next"), "Next"),
                            (("POSTCONDITION Post",), "Post"), (("ACTION_CONSTRAINT Few", "\\* limit: Few -- x"), "Few")):
            with self.subTest(name):
                self.assert_fails(f"the cfg overrides {name}, which it checks or runs", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  prep=[add_to_model("Mine == 1", "Post == TRUE", "Few == crashes <= 1"), add_to_cfg(*lines),
                                        override(f"{name} <- Mine")])
        # A mutant must check the same property as the baseline.
        self.assert_fails("mutant commit_one_prepared: its cfg overrides Consistent", prep=[lambda spec: edit(
            spec / "mutants" / "commit_one_prepared.cfg", ("CONSTANTS\n", "CONSTANTS\n    Consistent <- No\n"))])

    def test_constraint_override_fails(self):
        self.assert_fails("overrides Few, which it checks or runs", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                          prep=[*self.limited("Few == crashes <= 1"), add_to_model("Mine == 1"), lambda spec: edit(
                              spec / "TwoPhase.cfg", ("CONSTANTS\n", "CONSTANTS\n    Few <- Mine\n"))])

    def test_selftest_model_constants(self):
        model = tla_check.Model(SELFTEST, "MCTwoPhase.tla")
        self.assertEqual(set(model.texts), {"MCTwoPhase", "TwoPhase"})
        self.assertEqual(model.problems, [])
        self.assertTrue({"RM", "MaxCrashes", "MaxDrops", "Mutant"} <= model.constants)
        self.assertNotIn("Next", model.constants)

    # The facts.

    def test_every_failure_reaches_the_facts_and_summary_json_comes_first(self):
        def many(spec):
            for i in range(60):
                shutil.copy(spec / "mutants" / "commit_one_prepared.cfg", spec / "mutants" / f"z{i:02d}.cfg")
            for i in range(25):
                shutil.copy(spec / "TwoPhase.cfg", spec / f"TwoPhase.v{i:02d}.cfg")

        mapping = {**GOOD, **{f"z{i:02d}.cfg": ["invariant", 12] for i in range(60)},
                   **{f"TwoPhase.v{i:02d}.cfg": ["pass", 0] for i in range(25)},
                   "z55.cfg": ["pass", 0], "TwoPhase.v24.cfg": ["invariant", 12]}
        summary = self.assert_fails("mutant z55: no violation", failed=2, mapping=mapping, prep=[many])
        tla = summary["tla"]
        self.assertEqual((len(tla["mutants"]), len(tla["runs"])), (62, 26))
        self.assertEqual([m["name"] for m in tla["mutants"] if not m["ok"]], ["z55"])
        self.assertEqual([r["cfg"] for r in tla["runs"] if r["verdict"] != "pass"], ["TwoPhase.v24.cfg"])
        names = [a["name"] for a in summary["artifacts"]]
        self.assertEqual((names[0], len(names), len(set(names))), ("tla/summary.json", 89, 89))
        stored = json.loads((self.fake.out / "summary.json").read_text())
        self.assertEqual(stored, {k: summary[k] for k in ("verdict", "executed", "failed", "not_run", "tla")})

    def test_every_error_reaches_the_summary_in_full(self):
        names = [f"TwoPhase.v{i:03d}.cfg" for i in range(8)]

        def many(spec):
            for name in names:
                shutil.copy(spec / "TwoPhase.cfg", spec / name)

        code, summary, _ = self.run_fake({**GOOD, **{n: ["eval_error", 255] for n in names}}, prep=[many])
        self.assertEqual((code, summary["verdict"], len(summary["not_run"])), (3, "error", 8))
        stopped, summary, _ = self.run_fake({**GOOD, "TwoPhase.cfg": ["eval_error", 255]})
        self.assertEqual(stopped, 3)
        self.assertEqual(summary["not_run"][0]["id"], "tlc-error")

    def test_a_used_output_folder_is_left_alone(self):
        fake = FakeTlc(GOOD)
        self.addCleanup(fake.close)
        fake.out.mkdir()
        (fake.out / "summary.json").write_text("earlier")
        code, summary, _ = fake.run()
        self.assertEqual((code, summary["verdict"], summary["artifacts"]), (3, "error", []))
        self.assertEqual((fake.out / "summary.json").read_text(), "earlier")

    def test_kill_without_docker_is_still_not_run(self):
        for effect in (OSError("no docker"), subprocess.TimeoutExpired("docker", 60)):
            with self.subTest(effect=type(effect).__name__), patch.object(tc, "KILL_GRACE_S", 0), \
                    patch.object(tc.subprocess, "run", side_effect=effect) as run:
                code, summary, _ = self.run_fake(dict(GOOD, **{"TwoPhase.cfg": ["time_budget", 137]}),
                                                 budget="1", FAKE_TLC_SLEEP="3")
                self.assertEqual((code, summary["verdict"]), (2, "not-run"))
                self.assertTrue(all(c.kwargs.get("timeout") for c in run.call_args_list))
                self.assertEqual(run.call_args.args[0][:2], ["docker", "kill"])

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

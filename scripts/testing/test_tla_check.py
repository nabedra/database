#!/usr/bin/env python3
"""Tests for tla_common.py and tla_check.py, on captured TLC v1.8.0 `-tool` output.

The fixtures in testdata/tla were captured from real runs: the TwoPhase self-test baseline (pass) and
its mutants (invariant, liveness lasso), small purpose-built specs (deadlock, coverage gap, a state cap
through TLCSet("exit"), a stop at the time budget through tlc2.TLC.stopAfter, a parse error and an
evaluation error), a simulation, and trace validation (accepted and rejected). The orchestration tests
replace specs/tools/tlc with a fake wrapper that prints a fixture per cfg, so they need no Docker.
"""

import hashlib
import io
import json
import os
import random
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
                # At the baseline's bounds, so that the baseline is the control of both mutants.
                (self.spec / "mutants" / m.name).write_text(m.read_text().replace("MaxDrops = 0", "MaxDrops = 1"))
        self.edits: dict[str, list] = {}
        self.args: dict[str, list] = {}
        self.ran: dict[str, str] = {}  # what the fake TLC ran in this process, and the text of the controls
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
        config = args[args.index("-config") + 1]
        mapping = json.loads(self.env["FAKE_TLC_MAP"])
        name, code = mapping.get(Path(config).name, ["pass", 0])
        text = fixture(name)
        for old, new in [*json.loads(self.env.get("FAKE_TLC_EDIT", "[]")), *self.edits.get(Path(config).name, [])]:
            text = text.replace(old, new)
        self.args[Path(config).name] = args
        # TLC sees the run's output folder as /out.
        self.ran[Path(config).name] = (out_dir / Path(config).name).read_text() if config.startswith("/out/") else (spec_dir / config).read_text()
        log_path.write_text(text)
        return tc.parse_tlc_output(text, code), 0.1

    def close(self):
        self.tmp.cleanup()


GOOD = {"TwoPhase.cfg": ["pass", 0], "commit_one_prepared.cfg": ["invariant", 12],
        "no_resend.cfg": ["liveness", 13]}


class CheckOrchestrationTests(unittest.TestCase):
    def run_fake(self, mapping, drop_mutants=(), extra=(), budget="60", prep=(), spare=None, edits=None, **env):
        """`spare` keeps a second baseline as it is, the control of the mutants when `prep` changes the first. It is
        there by default when there are prep steps. `edits` are FAKE_TLC_EDIT replacements for one cfg."""
        fake = FakeTlc(mapping, drop_mutants, **env)
        self.addCleanup(fake.close)
        if prep if spare is None else spare:
            shutil.copy(fake.spec / "TwoPhase.cfg", fake.spec / "TwoPhase.spare.cfg")
        for step in prep:
            step(fake.spec)
        fake.edits = edits or {}
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
        # Nor is a run that reported success and then had to be killed.
        with patch.object(tc, "KILL_GRACE_S", 0):
            code, summary, _ = self.run_fake(GOOD, budget="1", FAKE_TLC_SLEEP="5")
        self.assertEqual((code, summary["verdict"]), (2, "not-run"))
        self.assertEqual(summary["tla"]["runs"][0]["verdict"], "incomplete")

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

        self.assert_fails("the baselines check no property", mapping={"TwoPhase.cfg": ["pass", 0]}, prep=[drop], spare=False)

    def test_only_a_type_invariant_with_a_reason_is_exempt(self):
        def only(spec):
            edit(spec / "TwoPhase.cfg", ("INVARIANTS TypeOK Consistent", "INVARIANTS TypeOK"), *NO_LIVENESS)
            shutil.rmtree(spec / "mutants")

        def exempt(line):
            return lambda spec: edit(spec / "TwoPhase.cfg", ("CONSTANTS\n", f"\\* no-mutant: {line}\nCONSTANTS\n"))

        self.assert_fails("no property besides exempted ones", mapping={"TwoPhase.cfg": ["pass", 0]}, prep=[only], spare=False)
        for line, fragment in (("Consistent", "gives no reason"), ("Consistent --", "gives no reason"),
                               ("Consistent -- obvious", "only a type invariant"),
                               ("Termination -- obvious", "only a type invariant")):
            with self.subTest(line):
                self.assert_fails(fragment, prep=[exempt(line)], spare=False)
        self.assert_fails("mutant rule TypeOK: its `no-mutant` line gives no reason", spare=False, prep=[
            lambda spec: edit(spec / "TwoPhase.cfg", (" -- the type invariant; every run checks it.", ""))])
        # A property cannot be a type invariant, whatever its name.
        self.assert_fails("mutant rule TypeLive: only a type invariant", spare=False, prep=[
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
        # Nor is output of both kinds either of them.
        done = re.search(r"@!@!@STARTMSG 2210:.*?ENDMSG 2210 @!@!@\n", fixture("simulation"), re.S).group(0)
        both = json.dumps([["@!@!@STARTMSG 2193:0", done + "@!@!@STARTMSG 2193:0"]])
        for extra, cfg in (((), "TwoPhase.cfg"), (("--tier", "T2"), "TwoPhase.large.cfg"), (("--tier", "T2", "--simulate"), "TwoPhase.large.cfg")):
            code, summary, _ = self.run_fake({**GOOD, cfg: ["pass", 0]}, extra=extra, FAKE_TLC_EDIT=both)
            self.assertEqual((code, summary["verdict"]), (2, "not-run"))
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
        for definitions in (["Few == crashes <= 1"],
                            ["Few ==", "    /\\ crashes <= MaxCrashes \\* not = here", "    /\\ \\A r \\in RM : Len(rmState[r]) < 4"]):
            for kind in ("CONSTRAINT", "CONSTRAINTS"):
                with self.subTest(definitions[0], kind=kind):
                    code, summary, text = self.run_fake(
                        GOOD, prep=self.limited(*definitions, kind=kind), FAKE_TLC_EDIT=log)
                    self.assertEqual((code, summary["verdict"]), (0, "pass"), text)
                    self.assertEqual(summary["tla"]["limits"], [
                        "TwoPhase.cfg: CONSTRAINT Few kept 5515 of 7379 states: bounds counters"])

    def test_constraint_that_is_not_declared_fails(self):
        self.assert_fails("CONSTRAINT Few has no `\\* limit: Few -- <reason>` line", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                          prep=self.limited("Few == crashes <= 1", declared=False))
        for line in ("\\* limit: Few", "\\* limit: Few --", "\\* limit: Other -- reason", "(* limit: Few -- reason *)"):
            with self.subTest(line):
                self.assert_fails("has no `\\* limit: Few", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  prep=self.limited("Few == crashes <= 1", cfg=[line], declared=False))

    def test_constraint_outside_the_grammar_fails_even_when_declared(self):
        for definition in ('Few == \\A r \\in RM : rmState[r] # "committed"', "Few == TRUE", "Few(n) == crashes = n",
                           "Other == crashes <= 1", "Few == Few"):
            with self.subTest(definition):
                self.assert_fails("FAIL search of TwoPhase.cfg: CONSTRAINT Few: ", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  prep=self.limited(definition))

    def test_action_constraint_fails_even_when_declared(self):
        for kind in ("ACTION_CONSTRAINT", "ACTION_CONSTRAINTS"):
            self.assert_fails("ACTION_CONSTRAINT Few removes steps", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                              prep=self.limited("Few == crashes <= 1", kind=kind))

    # The grammar of a constraint.

    MODULE = ("---- MODULE M ----\nEXTENDS Naturals, Sequences, FiniteSets, Ext\nCONSTANTS S, Max, F(_)\nVARIABLES n, m, log, up\n"
              "vars == <<n, m, log, up>>\nBoth == S \\cup {Max, Own} \\union S\nDyn == IF up THEN S ELSE {}\nCap == IF up THEN 0 ELSE 9\n"
              "Every(T) == \\A t \\in T : n < 0\nLess(x) == n < x\nSub ==\n  /\\ m <= Max\n  /\\ n < 3\nLoop == Loop2\nLoop2 == n <= 1 /\\ Loop\n")
    EXT = "---- MODULE Ext ----\nCONSTANT Own\nVARIABLE ext\nExtBound == ext <= Own\n====\n"

    def bound(self, body, extra="", after="After == n\n", **files):
        """What the rule says about `Few`, whose body this is."""
        with tempfile.TemporaryDirectory() as d:
            for name, text in {"M": f"{self.MODULE}{extra}Few =={body}\n{after}====\n", "Ext": self.EXT, **files}.items():
                (Path(d) / f"{name}.tla").write_text(text)
            return tla_check.Model(Path(d), "M.tla").bound_problem("Few")

    ACCEPTED = {
        "each order operator": " n < 1 /\\ n <= 2 /\\ n =< 3 /\\ n \\leq 4", "a constant limit": " n <= Max",
        "a constant of an extended module": " ext <= Own", "an indexed variable": " \\A s \\in S : n[s] <= 1",
        "two indices and a constant index": " \\A s, t \\in S : n[s, t] <= 1 /\\ m[s][Max] <= 2",
        "Len and Cardinality": " Len(log) <= Max /\\ \\A s \\in S : Cardinality(m[s]) < 3 /\\ Len(log[s]) < 2",
        "nested quantifiers": " \\A s \\in S : \\A t \\in Both : n[s][t] <= 1", "a set of constants": " \\A s \\in Both : n[s] <= 1",
        "parentheses": " ((n <= 1) /\\ (\\A s \\in S : (m[s] <= 1)))", "named bounds": " Sub /\\ ExtBound /\\ (Sub)",
        "a bulleted list": "\n  /\\ n <= 1\n  /\\ m <= 2", "a list of \\land": "\n  \\land n <= 1\n  \\land m <= 2",
        "a list in column 1": "\n/\\ n <= 1\n/\\ m <= 2", "a body in column 1": "\nn <= 1", "an operand in column 1": " n <=\nMax",
        "a list under a quantifier": "\n  \\A s \\in S :\n    /\\ n[s] <= 1\n    /\\ m <= 2", "no space": "n<=1/\\m<Max",
    }

    def test_upper_bounds_on_counters_are_accepted(self):
        for name, body in self.ACCEPTED.items():
            with self.subTest(name):
                self.assertIsNone(self.bound(body))

    REFUSED = {
        "equality": " n = 1", "inequality": " n # 1", "a lower bound": " n >= 1", "a strict one": " n > 1", "a bound on the right": " 1 <= n",
        "a bound between variables": " n <= m", "a next-state value": " n' <= 1", "a growing step": " Cardinality(m') <= Max",
        "a sum": " n + m <= 1", "a difference": " n <= Max - 1", "an operator as the limit": " n <= Cap", "a call as the limit": " n <= Less(1)",
        "a constant operator": " n <= F(1)", "an indexed limit": " n <= Max[1]", "a negative number": " n <= -1", "a decimal": " n <= 1.5",
        "a string": ' n <= "a"', "a string index": ' n["a"] <= 1', "a variable index": " n[up] <= 1", "an expression index": " n[1] <= 1",
        "a comprehension": " Cardinality({s \\in S : up[s]}) <= 2", "Len of an expression": " Len(log \\o log) <= 2",
        "an operator as the counter": " Cap <= 1", "a constant as the counter": " Max <= 1", "a field": " n.x <= 1", "another call": " Max(n) <= 1",
        "IF": " IF up THEN n <= 1 ELSE up", "IF as the limit": " n <= IF up THEN 0 ELSE 9", "CASE": " CASE up -> n <= 1 [] OTHER -> up",
        "LET": " LET k == 1 IN n <= k", "CHOOSE": " CHOOSE b \\in BOOLEAN : n <= 1", "LAMBDA": " LAMBDA x : n <= 1", "ENABLED": " ENABLED n <= 1",
        "exists": " \\E s \\in S : n[s] <= 1", "or": " n <= 1 \\/ up", "\\lor": " up \\lor n <= 1", "implies": " up => n <= 1", "equivalent": " up <=> n <= 1",
        "not": " ~ n <= 1", "\\lnot": " \\lnot (n <= 1)", "a boolean variable": " n <= 1 /\\ up", "a literal": " TRUE", "membership": " (n <= 1) \\in {up}",
        "an application": " n[n <= 1]", "a set": " {n <= 1}", "a subset": " S \\subseteq {n}", "a call around a bound": " Less(n <= 1)",
        # The quantifier's domain, the arguments of a call, and a body cut short: the second review's five rows.
        "IF in the domain": ' \\A s \\in IF up["r1"] = "committed" THEN S ELSE {} : n < 0', "a call with IF": " Less(IF up THEN 0 ELSE 9)",
        "a call with a comprehension": ' Every({r \\in {"r1"} : up[r] = "committed"})',
        "LET in column 1": ' n < LET\nk == IF up["r1"] = "committed" THEN 0 ELSE 9\nIN k',
        "a false head": ' n < 1 +\nLess(m) \\/ up["r1"] # "committed"\n\nAfter2(x) == x',
        "a state-dependent domain": " \\A s \\in Dyn : n[s] < 1", "a variable domain": " \\A s \\in up : n < 1", "a set as the domain": " \\A s \\in {Max} : n < 1",
        "no domain": " \\A s : n < 1", "a bound name that is declared": " \\A n \\in S : n <= 1", "a pattern": " \\A <<s, t>> \\in S : n[s] <= 1",
        "a name out of scope": " (\\A s \\in S : n[s] <= 1) /\\ m[s] <= 1", "recursion": " Loop", "itself": " Few /\\ n <= 1",
        "a definition with parameters": " Less", "an undefined name": " Nope", "another module's": " Ext!ExtBound",
        "an empty body": "", "an empty quantifier": " \\A s \\in S :", "an empty last conjunct": " n <= 1 /\\", "two ands": " n <= 1 /\\ /\\ m <= 1",
        "an empty group": " ()", "no left operand": " <= 1", "no right operand": " n <=", "an open bracket": " (n <= 1", "a closed one": " n <= 1)",
        "crossed brackets": " n <= 1) /\\ (m <= 1", "an open index": " n[Max <= 1", "a second comparison": " n <= 1 <= Max", "a word after it": " n <= 1 up", "a definition on the same line": " n <= 1 Z == up",
        "a line after it": " n <= 1\n  up", "a continued line": " n <= 1\n  + 1", "an or on the next line": " n <= 1\n\\/ up", "a number after it": " n <= 1 2",
        "Unicode not-equal": " n ≠ 1", "Unicode or": " n <= 1 ∨ up", "Unicode implies": " up ⇒ n <= 1", "Unicode and": " n <= 1 ∧ m <= 1",
        "Unicode order": " n ≤ 1", "Unicode for all": " ∀ s \\in S : n[s] <= 1", "a Unicode digit": " n <= ١", "a Unicode space": " n <= 1 \\/ up",
    }

    def test_everything_else_is_refused(self):
        for name, body in self.REFUSED.items():
            with self.subTest(name):
                problem = self.bound(body)
                self.assertIsNotNone(problem)
                self.assertIn("a constraint may only be a conjunction of upper bounds", problem if name not in (
                    "recursion", "itself") else problem + "a constraint may only be a conjunction of upper bounds")

    def test_a_refusal_names_the_line(self):
        first = self.MODULE.count("\n") + 1
        for lines, body in ((0, " up = 1"), (1, "\n up = 1"), (3, "\n  /\\ n <= 1\n  /\\ m <= 2\n  /\\ up = 1")):
            self.assertIn(f"M.tla line {first + lines}: '=' instead of <", self.bound(body))
        # Text before the module counts as lines of the file, and a named bound is refused where it is wrong.
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "M.tla").write_text("A note.\nTwo lines.\n---- MODULE M ----\n(* a\ncomment *)\nVARIABLE n\nB == n # 1\nFew == B\n====\n")
            problem = tla_check.Model(Path(d), "M.tla").bound_problem("Few")
            self.assertIn("M.tla line 7: '#' instead of <", problem)
            self.assertEqual((problem.count("line"), problem.count("a constraint may")), (1, 1))

    def test_a_body_ends_at_the_next_unit(self):
        for unit in ("VARIABLE x", "CONSTANT X", "ASSUME up", "THEOREM up", "LOCAL Z == up", "RECURSIVE F2(_)", "INSTANCE M2", "Z == up", "  Z == up",
                     "Z(a,\n  b) == up", "Z(Op(_), b) == up", "Z[a \\in S] == up", "a \\prec b == up", "a ++ b == up", "----", "", "====\nup"):
            with self.subTest(unit):
                self.assertIsNone(self.bound(" n <= 1", after=unit + "\n"))
        for rest in ("IF up THEN 1 ELSE 2", "THEN up", "IN up", "up", "Z", "Z(a) = up", "Z[a] # 1", "- 1", "\\/ up", "= 1", "(up)", "[up]", "'", "\\in S"):
            for start in ("\n", "\n  ", " "):
                with self.subTest(rest, start=start):
                    self.assertIsNotNone(self.bound(" n <= 1" + start + rest))

    def test_only_one_definition_in_scope_counts(self):
        inner = "---- MODULE Inner ----\nFew == up\n====\n"
        self.assertIsNotNone(self.bound(" n <= 1", extra=inner))
        self.assertIsNotNone(self.bound(" n <= 1", after=inner))
        self.assertIsNotNone(self.bound(" LET Few == 1 IN n <= 1"))
        self.assertIsNotNone(self.bound(" n <= 1", extra="INSTANCE Other\n", Other="---- MODULE Other ----\nFew == 1\n====\n"))
        # A name that is declared and defined is neither a counter nor a constant.
        for declared, body in (("VARIABLE q", " q <= 1"), ("CONSTANT q", " n <= q"), ("CONSTANT q", " \\A s \\in q : n[s] <= 1")):
            inner = f"---- MODULE Inner ----\n{declared}\n====\n"
            self.assertIsNotNone(self.bound(body, extra=inner + "q == IF up THEN 0 ELSE 9\n"))
            self.assertIsNone(self.bound(body, extra=declared + "\n"))
        for declared, body in (("CONSTANT n", " n <= 1"), ("VARIABLE Max", " m <= Max"), ("VARIABLE S", " \\A s \\in S : n[s] <= 1")):
            self.assertIsNotNone(self.bound(body, extra=f"---- MODULE Inner ----\n{declared}\n====\n"))
        # Nor is a definition with parameters, or one of an instantiated module, a named bound.
        self.assertIsNotNone(self.bound(" Par", extra="Par(x) == n <= 1\n"))
        self.assertIsNotNone(self.bound(" Par", extra="Par[x \\in S] == n <= 1\n"))
        self.assertIsNotNone(self.bound(" InOther", extra="INSTANCE Other WITH n <- Cap\n",
                                        Other="---- MODULE Other ----\nVARIABLE n\nInOther == n <= 1\n====\n"))
        # A set of constants is read to its end too.
        for body in ("S \\cup S \\ up", "S \\cap up", "up", "up \\cup S", "S \\cup {up}", "{}", "{Max", "S \\cup", "{Max, }", "IF up THEN S ELSE S", "S\n  \\ up",
                     "Dyn", "S \\cup Dyn", "SUBSET S", "[S -> S]", "S \\cup {s \\in S : up}"):
            self.assertIsNotNone(self.bound(" \\A s \\in Bad : n[s] <= 1", extra=f"Bad == {body}\n"), body)
        self.assertIsNone(self.bound(" \\A s \\in Fine : n[s] <= 1", extra="Fine == {Max, Own} \\cup S \\union\n  {Max}\n"))
        # What an instantiated module declares or defines is not the model's: it may be substituted there.
        other = "---- MODULE Other ----\nCONSTANT C\nVARIABLE v\nB == v <= C\n====\n"
        for body in (" v <= 1", " n <= C", " B"):
            self.assertIsNotNone(self.bound(body, extra="INSTANCE Other WITH v <- Cap, C <- Cap\n", Other=other))
        extended = other.replace("CONSTANT C", "EXTENDS Deep\nCONSTANT C")
        deep = "---- MODULE Deep ----\nVARIABLE deep\n====\n"
        self.assertIsNone(self.bound(" B /\\ deep <= C", extra="", Ext=extended.replace("Other", "Ext") , Deep=deep))

    def test_the_model_may_not_redefine_what_the_grammar_trusts(self):
        for definition in ("Len(x) == IF up THEN 0 ELSE 9", "Cardinality(x) == 0", "a <= b == up", "a < b == up", "a =< b == up", "a \\leq b == up"):
            with self.subTest(definition):
                self.assertIn("the model defines its own", self.bound(" n <= 1", extra=definition + "\n"))
        shadow = "---- MODULE FiniteSets ----\nCardinality(x) == IF x = {} THEN 0 ELSE 9\n====\n"
        self.assertIn("its own Cardinality", self.bound(" Cardinality(n) <= 1", FiniteSets=shadow))

    # Fuzz: bodies outside the grammar, seeded. Every one must be refused, and every valid one accepted.

    def valid(self, rng, depth=2, names=()):
        """Tokens of a random body of the grammar."""
        index = lambda: [t for _ in range(rng.randrange(2)) for t in ("[", rng.choice([*names, "Max", "Own"]), "]")]
        ref = lambda: [rng.choice(["n", "m", "log", "ext"]), *index()]
        out = []
        for i in range(rng.randint(1, 3)):
            out += [rng.choice(["/\\", "\\land"])] if i or rng.random() < 0.3 else []
            kind = rng.randrange(6 if depth else 3)
            if kind < 3:
                counter = ref() if kind else [rng.choice(["Len", "Cardinality"]), "(", *ref(), ")"]
                out += [*counter, rng.choice(["<", "<=", "=<", "\\leq"]), rng.choice(["1", "42", "Max", "Own"])]
            elif kind == 3:
                out += ["(", *self.valid(rng, depth - 1, names), ")"]
            elif kind == 4:
                new = rng.sample(["s", "t", "u"], rng.randint(1, 2))
                bound = [t for i, name in enumerate(new) for t in ([","] if i else []) + [name]]
                out += ["(", "\\A", *bound, "\\in", rng.choice(["S", "Both"]), ":", *self.valid(rng, depth - 1, (*names, *new)), ")"]
            else:
                out += [rng.choice(["Sub", "ExtBound"])]
        return out

    @staticmethod
    def layout(rng, tokens):
        return "".join(rng.choice([" ", " ", "\n  ", "\n", "  "]) + t for t in tokens)

    def fuzz(self, bodies):
        """bound_problem of every body. Each is alone in a module with a body that must be accepted, and without the
        rules about the rest of the model, so that nothing but the grammar can refuse it."""
        out = []
        with tempfile.TemporaryDirectory() as d, patch.object(tla_check, "_ORDER_DEFINITION", re.compile("(?!)")):
            (Path(d) / "Ext.tla").write_text(self.EXT)
            for body in bodies:
                (Path(d) / "M.tla").write_text(f"{self.MODULE}Control ==\n  /\\ Sub\n  /\\ \\A s \\in Both : Len(log[s]) <= Max\nFew =={body}\nAfter == n\n====\n")
                model = tla_check.Model(Path(d), "M.tla")
                self.assertIsNone(model.bound_problem("Control"), body)
                out.append(model.bound_problem("Few"))
        return out

    def test_fuzz_bodies_outside_the_grammar(self):
        rng = random.Random(20261125)
        bodies = [self.layout(rng, self.valid(rng)) for _ in range(150)]
        self.assertEqual([b for b, p in zip(bodies, self.fuzz(bodies)) if p], [])
        foreign = ["\\/", "\\lor", "=", "#", "/=", "~", "=>", "<=>", "\\equiv", "+", "-", "*", "..", "'", ".", "!", "@", "|", "|->", "->", "{", "}", "[", "]",
                   "(", ")", ",", ":", ">", ">=", "\\geq", "\\E", "\\notin", "\\cup", "\\subseteq", "<<", ">>", "[]", "<>", '"x"', "TRUE", "IF", "THEN", "ELSE",
                   "CASE", "OTHER", "LET", "IN", "CHOOSE", "LAMBDA", "ENABLED", "UNCHANGED", "DOMAIN", "SUBSET", "EXCEPT", "up", "Cap", "Dyn", "Less", "Loop",
                   "nope", "5", "≠", "∨", "∧", "¬", "≤"]
        expressions = ["IF up THEN {} ELSE {}", "LET k == 1 IN {}", "LET\nk == IF up THEN 0 ELSE 9\nIN {}", "( {} ) \\/ up", "up => ( {} )", "~ ( {} )",
                       "( {} ) = up", "( {} ) # ( {} )", "CHOOSE b \\in BOOLEAN : {}", "\\E s \\in S : {}", "Less( {} )", "n[ {} ] <= 1", "{{ {} }} = {{}}",
                       "Cardinality({{s \\in S : {}}}) <= 2", "\\A s \\in {{t \\in S : up}} : {}", "\\A s \\in Dyn : {}", "\\A s \\in up : {}", "{} /\\ up",
                       "{} /\\ n + m <= 1", "{} /\\ n' <= 1", "n >= 1 /\\ {}", "n <= m /\\ {}", "{} /\\ n <= Cap", "Every(Dyn) /\\ {}", "n <= 1 +\nLess(m) /\\ {}",
                       "CASE up -> {} [] OTHER -> up", "ENABLED ( {} )", "[] ( {} )", "{} <=> up", "\\A s \\in S : ( {} ) \\/ up"]
        bad = []
        while len(bad) < 1500:
            tokens = self.valid(rng)
            at = rng.randrange(len(tokens))
            kind = rng.randrange(4)
            if kind == 0:
                tokens.insert(rng.randrange(len(tokens) + 1), rng.choice(foreign))
            elif kind == 1:
                # Another number is a limit too, another name may be bound, and another variable is a counter.
                names = ("s", "t", "u", "1", "42", "Max", "Own", "n", "m", "log", "ext")
                tokens[at] = rng.choice([f for f in foreign if tokens[at] not in names or not f.isidentifier() and f != "5"])
            elif kind == 2:
                if tokens[at] in ("/\\", "\\land", "Sub", "ExtBound"):  # a leading "and" may go, and a bound before one
                    continue
                del tokens[at]
            else:
                body = self.layout(rng, tokens)
                form = rng.choice(expressions)
                bad.append(" " + form.format(*[body] * form.count("{}")).replace("\n", rng.choice(["\n", "\n  "])))
                continue
            bad.append(self.layout(rng, tokens))
        self.assertEqual([b for b, p in zip(bad, self.fuzz(bad)) if p is None], [])

    def test_an_override_cannot_bring_in_the_search_state(self):
        def model(*definitions):
            return [safety_only, add_to_model(*definitions, "L == Lvl(1)", extends=", TLC\nCONSTANT Lvl(_)")]

        def override(line):
            return lambda spec: edit(spec / "TwoPhase.cfg", ("CONSTANTS\n", f"CONSTANTS\n    {line}\n"))

        for line in ("Lvl <- TLCGet", "Lvl <- [MCTwoPhase]TLCGet", "Lvl <- RandomElement", "Lvl <- JavaTime"):
            with self.subTest(line):
                summary = self.assert_fails("the cfg uses", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[*model(), override(line)])
                self.assertEqual(summary["tla"]["runs"][0]["verdict"], "pass")  # TLC itself saw nothing
        # Any other operator that the model does not define, under another name.
        for line in ("Lvl <- Trace", "Lvl <- Cardinality", "Lvl <- [MCTwoPhase] SortSeq", "Lvl <- Dot"):
            with self.subTest(line):
                self.assert_fails("which the model's modules do not define", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  prep=[*model("Use == A!Dot"), override(line)])
        self.assert_fails("the cfg uses TLCGet", prep=[lambda spec: edit(spec / "TwoPhase.cfg", (
            "CHECK_DEADLOCK TRUE", "ALIAS TLCGet\nCHECK_DEADLOCK TRUE"))])
        # A comment is no use, and an operator of the model may take the constant's place, wherever it is defined.
        for definition in ("Own(x) == 1", "  Own(x) == 1", "Own[x \\in RM] == 1", "Own(Op(_), b) == 1", "Own(x,\n  y) ==\n  1",
                           "Own[x \\in RM, y \\in Set[1]] == 1"):
            with self.subTest(definition):
                code, _, text = self.run_fake(GOOD, FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[
                    *model(definition), override("Lvl <- Own \\* not TLCGet")])
                self.assertEqual(code, 0, text)
        # So may a constant that the model declares.
        code, _, text = self.run_fake(GOOD, FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[*model(), override("Lvl <- MaxDrops")])
        self.assertEqual(code, 0, text)

    @staticmethod
    def in_cfg(name, *pairs):
        return lambda spec: edit(spec / name, *pairs)

    def mutant(self, *pairs, cfg="no_resend.cfg"):
        return self.in_cfg(f"mutants/{cfg}", *pairs)

    def test_a_mutant_is_held_to_the_search_rule(self):
        few = add_to_model("Few == crashes <= 1", "V == rmState", "Bad == tmUp", "Inner == tmUp")
        for lines, why in ((["VIEW V"], "VIEW V merges states"), (["ACTION_CONSTRAINT Few"], "ACTION_CONSTRAINT Few removes steps"),
                           (["CONSTRAINT Few"], "CONSTRAINT Few has no `\\* limit: Few"),
                           (["CONSTRAINT Bad", "\\* limit: Bad -- x"], "CONSTRAINT Bad: "), (["ALIAS TLCGet"], "the cfg uses TLCGet"),
                           (["CONSTRAINT Few", self.COUNTERS], "CONSTRAINT Few has no `\\* limit: Few"),
                           (["CONSTANT Inner <- Bad", "\\* limit: Inner -- x"], "override Inner has no `\\* limit: Inner")):
            with self.subTest(lines):
                summary = self.assert_fails("mutant no_resend: does not count: ", why, prep=[
                    few, self.mutant(("SPECIFICATION Spec\n", "SPECIFICATION Spec\n" + "\n".join(lines) + "\n"))])
                self.assertEqual([m["ok"] for m in summary["tla"]["mutants"]], [True, False])
                self.assertEqual(summary["tla"]["mutants"][1]["observed"], "")  # TLC did not run
        # With the baseline's constraint, its declaration and its override it counts, where liveness is not at stake.
        safety = [safety_only, few, add_to_model("Good == 0..3"), add_to_cfg("CONSTRAINT Few", self.COUNTERS, "\\* limit: Nat -- finite"),
                  self.in_cfg("TwoPhase.cfg", ("CONSTANTS\n", "CONSTANTS\n    Nat <- Good\n")),
                  self.mutant(("CONSTANTS\n", "CONSTANTS\n    Nat <- Good\n"), ("SPECIFICATION Spec\n", "SPECIFICATION Spec\nCONSTRAINT Few\n"),
                              *NO_LIVENESS, cfg="commit_one_prepared.cfg")]
        code, summary, text = self.run_fake(GOOD, prep=safety, FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]))
        self.assertEqual(code, 0, text)

    def test_a_mutant_caught_by_a_limited_liveness_check_does_not_count(self):
        warning = "@!@!@STARTMSG 2279:3 @!@!@\nDeclaring symmetry\n@!@!@ENDMSG 2279 @!@!@\n@!@!@STARTMSG 2116"
        summary = self.assert_fails("mutant no_resend: liveness property Termination violated", "it does not count: liveness under SYMMETRY",
                                    FAKE_TLC_EDIT=json.dumps([["@!@!@STARTMSG 2116", warning]]))
        self.assertEqual([m["ok"] for m in summary["tla"]["mutants"]], [True, False])
        # A constraint that a baseline declares does not help where the mutant checks liveness. The baseline's own
        # run has no liveness message here.
        with patch.object(tla_check, "liveness_problem", side_effect=lambda log, parsed, real=tla_check.liveness_problem:
                          real(log, parsed) if parsed.expect else None):
            self.assert_fails("mutant no_resend: liveness property Termination violated", "it does not count: liveness under the constraint Few",
                              prep=[add_to_model("Few == crashes <= 1"), add_to_cfg("CONSTRAINT Few", self.COUNTERS),
                                    self.mutant(("SPECIFICATION Spec\n", "SPECIFICATION Spec\nCONSTRAINT Few\n"))])

    # The control of a mutant.

    SMALL = ("MaxDrops = 1", "MaxDrops = 0")

    def mutant_fact(self, summary, name="no_resend"):
        return next(m for m in summary["tla"]["mutants"] if m["name"] == name)

    def controls_ran(self):
        return [c for c in self.fake.ran if c.startswith("control-")]

    def test_a_mutant_at_other_bounds_has_a_control_run(self):
        for pairs in ([self.SMALL], [("MaxDrops = 1", "MaxDrops = 2")], [("RM <- MCRM", "RM <- MCRMLarge")], [("RM <- MCRM", 'RM = {"r1"}')],
                      [("CHECK_DEADLOCK", "CHECK_DEADLOCK")]):
            with self.subTest(pairs):
                prep = [self.mutant(*pairs)] if pairs[0][0] != pairs[0][1] else [add_to_cfg("SYMMETRY Sym"), self.in_cfg("TwoPhase.cfg", *NO_LIVENESS)]
                code, summary, text = self.run_fake(GOOD, prep=prep, FAKE_TLC_EDIT="[]", spare=pairs[0][0] == pairs[0][1])
                self.assertEqual((code, summary["executed"]), (0, 9 if len(prep) == 1 else 11), text)
        self.assertEqual(self.controls_ran(), [])  # the spare baseline is the control
        code, summary, text = self.run_fake(GOOD, prep=[self.mutant(self.SMALL)], spare=False)
        self.assertEqual((code, summary["executed"], summary["failed"]), (0, 9, 0), text)
        self.assertIn("control control-no_resend.cfg: pass, 2750 distinct states", text)
        self.assertEqual(list(self.fake.ran), ["TwoPhase.cfg", "commit_one_prepared.cfg", "control-no_resend.cfg", "no_resend.cfg"])
        # The same cfg, with only Mutant set to the baselines' value.
        mutant = (self.fake.spec / "mutants/no_resend.cfg").read_text()
        self.assertEqual(self.fake.ran["control-no_resend.cfg"], mutant.replace("\\* expect: Termination", "").replace('"no_resend"', '"none"'))
        self.assertEqual(summary["tla"]["controls"], [{"cfg": "control-no_resend.cfg", "distinct_states": 2750, "depth": 15, "states_left": 0,
                                                       "duration_s": 0.1, "verdict": "pass", "detail": ""}])
        self.assertEqual([m["control"] for m in summary["tla"]["mutants"]], ["TwoPhase.cfg", "control-no_resend.cfg"])
        self.assertIsNone(run_check_errors(summary))
        self.assertIn("tla/control-no_resend.cfg", [a["name"] for a in summary["artifacts"]])
        self.assertIn("/out/control-no_resend.cex.json", self.fake.args["control-no_resend.cfg"])
        # Nothing to control: no list of controls.
        self.assertNotIn("controls", self.run_fake(GOOD)[1]["tla"])

    def test_the_selftest_has_a_control_run(self):
        base = tla_check.settings(SELFTEST / "TwoPhase.cfg")
        other = {m.stem: {e.split()[0] for e in tla_check.settings(m) ^ base} for m in (SELFTEST / "mutants").glob("*.cfg")}
        self.assertEqual(other, {"commit_one_prepared": {"Mutant", "MaxDrops"}, "no_resend": {"Mutant"}})

    def test_a_mutant_whose_control_does_not_pass_is_not_caught(self):
        simulation = re.search(r"@!@!@STARTMSG 2210:.*?ENDMSG 2210 @!@!@\n", fixture("simulation"), re.S).group(0)
        symmetry = "@!@!@STARTMSG 2279:3 @!@!@\nDeclaring symmetry\n@!@!@ENDMSG 2279 @!@!@\n"
        for fx, edits, why in (
                (["liveness", 13], [], "fails without the mutation: liveness property Termination violated"),
                (["invariant", 12], [], "fails without the mutation: invariant Consistent violated"),
                (["deadlock", 11], [], "fails without the mutation: "),
                (["eval_error", 255], [], "ended in a TLC error: "),
                (["parse_error", 150], [], "ended in a TLC error: "),
                (["pass", 1], [], "ended in a TLC error: "),
                (["state_cap", 0], [], "did not finish: stopped early with 252 states left on queue"),
                (["time_budget", 0], [], "did not finish: stopped early with 29355 states left on queue"),
                (["pass", 0], [["STARTMSG 2199:0", "STARTMSG 2299:0"], ["ENDMSG 2199", "ENDMSG 2299"]], "did not finish: TLC ended without a verdict"),
                (["simulation", 0], [], "did not finish: "),
                (["pass", 0], [["@!@!@STARTMSG 2193:0", simulation + "@!@!@STARTMSG 2193:0"]], "did not finish: "),
                (["pass", 0], [["@!@!@STARTMSG 2193:0", symmetry + "@!@!@STARTMSG 2193:0"]], "does not count: liveness under SYMMETRY")):
            with self.subTest(fx=fx, why=why):
                # TLC's success text in a run that is none of the selftest's must not count either.
                summary = self.assert_fails("FAIL mutant no_resend: not caught: its control control-no_resend.cfg " + why, prep=[self.mutant(self.SMALL)], spare=False,
                                            mapping={**GOOD, "control-no_resend.cfg": fx}, edits={"control-no_resend.cfg": edits})
                fact = self.mutant_fact(summary)
                self.assertEqual((fact["ok"], fact["observed"], fact["control"]), (False, "", "control-no_resend.cfg"))
                self.assertNotIn("no_resend.cfg", self.fake.ran)  # nothing to learn from the mutant
                control = summary["tla"]["controls"][0]
                self.assertNotEqual(control["verdict"], "pass")
                self.assertTrue(control["detail"])
                self.assertIsNone(run_check_errors(summary))

    def test_a_control_killed_at_the_budget_is_not_caught(self):
        def slow(fake, *args, log_path, **kw):
            res, elapsed = real(fake, *args, log_path=log_path, **kw)
            res.timed_out = log_path.name.startswith("control-")
            return res, elapsed

        real = FakeTlc.run_tlc
        with patch.object(FakeTlc, "run_tlc", slow):
            self.assert_fails("its control control-no_resend.cfg did not finish: stopped at the 60 s time budget with 29355 states", spare=False,
                              prep=[self.mutant(self.SMALL)], mapping={**GOOD, "control-no_resend.cfg": ["time_budget", 0]})

    def test_a_baseline_with_the_same_settings_is_the_control(self):
        """Every setting counts, and how the property is checked."""
        def second(*pairs, name="TwoPhase.more.cfg"):
            return lambda spec: (spec / name).write_text(self.replaced((spec / "TwoPhase.cfg").read_text(), pairs))

        few = add_to_model("Few == crashes <= 1", "Other == drops <= 1")
        declared = self.in_cfg("TwoPhase.cfg", ("CONSTANTS\n", self.COUNTERS + "\nCONSTANTS\n"))
        plain = [add_to_model("TypeInv == TRUE")]
        at = lambda *more: self.mutant(self.SMALL, *more, cfg="commit_one_prepared.cfg")
        for prep, want in (
                ([second(self.SMALL), at()], []),
                ([second(self.SMALL), at(("CONSTANTS\n    RM <- MCRM\n    MaxCrashes = 1\n", "CONSTANT MaxCrashes=1\nCONSTANTS\n    RM <- MCRM\n"))], []),
                ([second(self.SMALL, ("INVARIANTS TypeOK Consistent", "INVARIANTS TypeOK Consistent TypeInv"), ("CONSTANTS\n", "\\* no-mutant: TypeInv -- x\nCONSTANTS\n"), *NO_LIVENESS), *plain, at()], []),
                ([second(self.SMALL, ("MaxCrashes = 1", "MaxCrashes = 0")), at()], ["control-commit_one_prepared.cfg"]),
                ([second(self.SMALL, ("SPECIFICATION Spec", "INIT Init\nNEXT Next")), at()], ["control-commit_one_prepared.cfg"]),
                ([second(self.SMALL, ("INVARIANTS TypeOK Consistent", "INVARIANTS TypeOK")), at()], ["control-commit_one_prepared.cfg"]),
                ([second(self.SMALL, ("INVARIANTS TypeOK Consistent", "INVARIANTS TypeOK"), ("PROPERTIES", "PROPERTIES Consistent")), at()], ["control-commit_one_prepared.cfg"]),
                ([second(self.SMALL, ("CHECK_DEADLOCK", "SYMMETRY Sym\nCHECK_DEADLOCK"), *NO_LIVENESS), at()], ["control-commit_one_prepared.cfg"]),
                ([few, declared, second(self.SMALL, ("CHECK_DEADLOCK", "CONSTRAINT Few\nCHECK_DEADLOCK"), *NO_LIVENESS), at()], ["control-commit_one_prepared.cfg"]),
                ([few, declared, second(self.SMALL, *NO_LIVENESS), at(("SPECIFICATION Spec\n", "SPECIFICATION Spec\nCONSTRAINT Few\n"), *NO_LIVENESS)],
                 ["control-commit_one_prepared.cfg"])):
            with self.subTest(n=len(prep), want=want):
                code, summary, text = self.run_fake(GOOD, prep=prep, spare=False, edits={c: [SAFETY_LOG] for c in (
                    "TwoPhase.more.cfg", "control-commit_one_prepared.cfg", "commit_one_prepared.cfg")})
                self.assertEqual(code, 0, text)
                self.assertEqual(self.controls_ran(), want)
                self.assertEqual(self.mutant_fact(summary, "commit_one_prepared")["control"], want[0] if want else "TwoPhase.more.cfg")
        # A Deadlock mutant needs one that checks deadlock.
        stuck = lambda spec: (spec / "mutants" / "stuck.cfg").write_text(self.replaced(
            (spec / "mutants" / "no_resend.cfg").read_text(), [("expect: Termination", "expect: Deadlock"), self.SMALL]))
        for check, want in (("TRUE", []), ("FALSE", ["control-stuck.cfg"])):
            code, summary, text = self.run_fake({**GOOD, "stuck.cfg": ["deadlock", 11]}, spare=False,
                                                prep=[second(self.SMALL, ("CHECK_DEADLOCK TRUE", f"CHECK_DEADLOCK {check}")), stuck])
            self.assertEqual((code, self.controls_ran()), (0, want), text)

    def test_mutants_share_a_control(self):
        small = [self.mutant(self.SMALL), self.mutant(self.SMALL, cfg="commit_one_prepared.cfg")]
        code, summary, text = self.run_fake(GOOD, prep=small, spare=False)
        self.assertEqual((code, self.controls_ran()), (0, ["control-commit_one_prepared.cfg"]), text)
        self.assertEqual([m["control"] for m in summary["tla"]["mutants"]], ["control-commit_one_prepared.cfg"] * 2)
        self.assertEqual(len(summary["tla"]["controls"]), 1)
        # Not where the first one does not check the property, and not at other bounds.
        code, summary, text = self.run_fake(GOOD, prep=[small[0], self.mutant(self.SMALL, *NO_LIVENESS, cfg="commit_one_prepared.cfg")], spare=False)
        self.assertEqual((code, len(self.controls_ran())), (0, 2), text)
        code, summary, text = self.run_fake(GOOD, prep=[small[0], self.mutant(("MaxDrops = 1", "MaxDrops = 2"), cfg="commit_one_prepared.cfg")], spare=False)
        self.assertEqual((code, len(self.controls_ran())), (0, 2), text)
        # One that did not pass says nothing about another mutant, whose cfg may check less.
        _, summary, text = self.run_fake({**GOOD, "control-commit_one_prepared.cfg": ["liveness", 13]}, prep=small, spare=False)
        self.assertEqual(([m["ok"] for m in summary["tla"]["mutants"]], len(self.controls_ran())), ([False, True], 2), text)

    def test_a_baseline_that_did_not_pass_is_no_control(self):
        # Its own result is the verdict: not-run stays not-run.
        for fx, want, code, why in (("time_budget", "not-run", 0, "did not finish: incomplete"), ("invariant", "fail", 12, "fails without the mutation: invariant Consistent"),
                                    ("eval_error", "error", 255, "ended in a TLC error: error")):
            with self.subTest(fx):
                _, summary, text = self.run_fake({**GOOD, "TwoPhase.cfg": [fx, code]})
                self.assertEqual((summary["verdict"], summary["failed"]), (want, 1 if want == "fail" else 0), text)
                self.assertIn(f"not-run mutant no_resend: not caught: its control TwoPhase.cfg {why}", text)
                self.assertEqual([m["ok"] for m in summary["tla"]["mutants"]], [False, False])
                self.assertEqual(list(self.fake.ran), ["TwoPhase.cfg"])
        # With two baselines of the same settings, one that passed is enough.
        code, summary, text = self.run_fake({**GOOD, "TwoPhase.cfg": ["time_budget", 0]}, spare=True)
        self.assertEqual((summary["verdict"], [m["ok"] for m in summary["tla"]["mutants"]]), ("not-run", [True, True]), text)
        self.assertEqual(self.mutant_fact(summary)["control"], "TwoPhase.spare.cfg")

    def test_the_control_differs_in_mutant_only(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "m.cfg"
            for line, want in (('CONSTANT Mutant = "x"', 'CONSTANT Mutant = none'), ('CONSTANTS A = 1 Mutant="x" B = {"Mutant"}', 'CONSTANTS A = 1 Mutant=none B = {"Mutant"}'),
                               ('CONSTANT A = "Mutant = y"\nCONSTANTS\n  Mutant\n  = x', 'CONSTANT A = "Mutant = y"\nCONSTANTS\n  Mutant\n  = none'),
                               ('INVARIANT Mutant\nCONSTANT Mutant2 = 1 Mutant = 7\nSPECIFICATION Spec', 'INVARIANT Mutant\nCONSTANT Mutant2 = 1 Mutant = none\nSPECIFICATION Spec'),
                               ('(* CONSTANT Mutant = "a" *) CONSTANT Mutant = "x" \\* Mutant = "z"', '  CONSTANT Mutant = none ')):
                path.write_text(line)
                self.assertEqual(tla_check.control_text(path, "none"), want)
        # The runner checks what it wrote.
        with patch.object(tla_check, "control_text", lambda cfg, value: self.replaced(cfg.read_text(), [('"no_resend"', value), ("MaxDrops = 0", "MaxDrops = 1")])):
            code, summary, text = self.run_fake(GOOD, prep=[self.mutant(self.SMALL)], drop_mutants=("commit_one_prepared",), spare=False)
        self.assertEqual((code, summary["verdict"]), (3, "error"), text)
        self.assertIn("cannot write the control of no_resend.cfg", text)
        for old, new in (("INVARIANTS TypeOK Consistent", "INVARIANTS TypeOK"), ("PROPERTIES Termination", "PROPERTIES Termination\nCHECK_DEADLOCK FALSE")):
            with patch.object(tla_check, "control_text", lambda cfg, value: self.replaced(cfg.read_text(), [('"no_resend"', value), (old, new)])):
                self.assertIn("cannot write the control", self.run_fake(GOOD, drop_mutants=("commit_one_prepared",), prep=[self.mutant(self.SMALL)], spare=False)[2])

    @staticmethod
    def replaced(text, pairs):
        for old, new in pairs:
            if old not in text:
                raise AssertionError(f"no {old!r}")
            text = text.replace(old, new)
        return text

    def test_a_mutant_sets_mutant_to_a_value_of_its_own(self):
        none = ('Mutant = "no_resend"', 'Mutant = "none"')
        spec = "SPECIFICATION Spec\n"
        note = [self.in_cfg("TwoPhase.tla", ("vars ==", 'Note == "none"\nvars ==')), self.in_cfg("TwoPhase.tla", ("Spec ==", "Weak == Spec /\\ Note = Note\nSpec =="))]
        for pairs, why in (([none], 'Mutant is "none" in TwoPhase.cfg too'), ([none, ("CONSTANTS\n", "CONSTANT\n")], 'Mutant is "none" in TwoPhase.cfg too'),
                           ([none, ("MaxDrops = 1", "MaxDrops = 2")], 'Mutant is "none" in TwoPhase.cfg too'),
                           ([(spec, "SPECIFICATION Weak\n")], "no baseline runs its SPECIFICATION Weak"),
                           ([(spec, "INIT Init\nNEXT Next\n")], "no baseline runs its INIT Init, NEXT Next"),
                           ([(spec, "")], "no baseline runs its specification")):
            with self.subTest(pairs):
                # The definitions have to name the value, which is not at stake here.
                summary = self.assert_fails("mutant no_resend: does not count: ", why, spare=False, prep=[self.mutant(*pairs), *note])
                self.assertFalse({"no_resend.cfg", "control-no_resend.cfg"} & set(self.fake.ran))
                self.assertNotIn("control", self.mutant_fact(summary))
        plain = "mutant no_resend: does not count: it does not set the CONSTANT Mutant to a plain value"
        for pairs in ([('    Mutant = "no_resend"\n', "")], [('Mutant = "no_resend"', "Mutant <- MCMutant")], [('Mutant = "no_resend"', 'Mutant = {"no_resend"}')],
                      [('Mutant = "no_resend"', 'Mutant = "no_resend"\n    Mutant = "none"')]):
            with self.subTest(pairs):
                twice = "the cfg sets Mutant more than once; " if len(pairs[0][1].splitlines()) > 1 else ""
                self.assert_fails(plain.replace("count: ", "count: " + twice), prep=[add_to_model('MCMutant == "no_resend"'), self.mutant(*pairs)], spare=False)
        # Mutant has to be a CONSTANT of the model.
        self.assert_fails("it does not set the CONSTANT Mutant to a plain value", failed=3, spare=False,
                          prep=[self.in_cfg("TwoPhase.tla", ("    Mutant       \\*", "    Mutant2      \\*"))])
        # A baseline without a plain value, and one that has the mutant's.
        for value, why in (('"no_resend"', 'Mutant is "no_resend" in TwoPhase.cfg too'), ("{}", "TwoPhase.cfg does not set Mutant to a plain value"),
                           ("<- MCMutant", "TwoPhase.cfg does not set Mutant to a plain value")):
            with self.subTest(value):
                line = ('Mutant = "none"', f"Mutant = {value}" if "<-" not in value else f"Mutant {value}")
                _, summary, text = self.run_fake(GOOD, prep=[add_to_model('MCMutant == "none"'), self.in_cfg("TwoPhase.cfg", line)], spare=True)
                self.assertIn("mutant no_resend: does not count: " + why, text)
                self.assertFalse(self.mutant_fact(summary)["ok"])
        # The control has the value of the main baseline if that checks the property, else of the first one that does.
        for pairs, want in (([], '"none"'), ([NO_LIVENESS[0]], '"fix"')):
            fix = lambda spec: (spec / "TwoPhase.fix.cfg").write_text((spec / "TwoPhase.cfg").read_text().replace('"none"', '"fix"'))
            code, summary, text = self.run_fake(GOOD, spare=False, prep=[fix, self.in_cfg("TwoPhase.cfg", *pairs), self.mutant(self.SMALL)],
                                                edits={"TwoPhase.cfg": [SAFETY_LOG] if pairs else []})
            self.assertEqual(code, 0, text)
            self.assertIn(f"Mutant = {want}", self.fake.ran["control-no_resend.cfg"])
        # Not the value of any baseline that checks the property.
        self.assert_fails('mutant no_resend: does not count: Mutant is "fix" in TwoPhase.fix.cfg too', spare=False, prep=[
            fix, self.mutant(('"no_resend"', '"fix"')), self.in_cfg("TwoPhase.tla", ("TMCrash ==\n", 'TMCrash ==\n    /\\ Mutant # "fix"\n'))])
        # Any INIT, NEXT or SPECIFICATION of a baseline.
        code, _, text = self.run_fake(GOOD, spare=True, prep=[self.in_cfg("TwoPhase.cfg", ("SPECIFICATION Spec", "INIT Init\nNEXT Next"), *NO_LIVENESS),
                                                              self.mutant((spec, "INIT Init\nNEXT Next\n"), *NO_LIVENESS, cfg="commit_one_prepared.cfg")],
                                      edits={c: [SAFETY_LOG] for c in ("TwoPhase.cfg", "commit_one_prepared.cfg")})
        self.assertEqual(code, 0, text)
        _, _, text = self.run_fake(GOOD, spare=True, prep=[self.in_cfg("TwoPhase.cfg", ("SPECIFICATION Spec", "INIT Init\nNEXT Next"), *NO_LIVENESS),
                                                           self.mutant((spec, "INIT Init\n"))], edits={"TwoPhase.cfg": [SAFETY_LOG]})
        self.assertIn("no baseline runs its INIT Init", text)
        # A mutant may run without its baseline's constraint, or with it, but with no other that is not declared.
        few = [add_to_model("Few == crashes <= 1"), safety_only]
        safety = dict(edits={c: [SAFETY_LOG] for c in ("TwoPhase.cfg", "commit_one_prepared.cfg", "control-commit_one_prepared.cfg")}, spare=False)
        _, summary, text = self.run_fake(GOOD, prep=[*few, add_to_cfg("CONSTRAINT Few", self.COUNTERS), self.mutant(*NO_LIVENESS, cfg="commit_one_prepared.cfg")], **safety)
        self.assertEqual((self.controls_ran(), self.mutant_fact(summary, "commit_one_prepared")["ok"]), (["control-commit_one_prepared.cfg"], True), text)

    def test_a_mutants_property_is_one_that_a_baseline_checks(self):
        """The reviewer's vacuous mutants, and one whose control fails at its own bounds."""
        small = lambda spec: (spec / "TwoPhase.small.cfg").write_text(self.replaced(
            (spec / "TwoPhase.cfg").read_text(), [("MaxCrashes = 1", "MaxCrashes = 0"), ("INVARIANTS TypeOK", "INVARIANTS NoCrash TypeOK")]))
        probe = lambda name, value, crashes=1, section="INVARIANTS": lambda spec: (spec / "mutants" / f"{name}.cfg").write_text(self.replaced(
            (spec / "mutants" / "no_resend.cfg").read_text(), [("expect: Termination", "expect: NoCrash"), ('"no_resend"', f'"{value}"'), ("MaxCrashes = 1", f"MaxCrashes = {crashes}"),
                                                               (f"{section} T", f"{section} NoCrash T")]))
        crash = json.dumps([["Invariant Consistent is violated", "Invariant NoCrash is violated"]])
        mapping = {**GOOD, "vacuous.cfg": ["invariant", 12], "commit_one_prepared.cfg": ["liveness", 13]}
        other = self.mutant(("expect: Consistent", "expect: Termination"), cfg="commit_one_prepared.cfg")
        model = add_to_model("NoCrash == crashes = 0")

        def vacuous(*prep, **more):
            _, summary, text = self.run_fake({**mapping, **more}, FAKE_TLC_EDIT=crash, spare=False, prep=[model, *prep, other])
            return self.mutant_fact(summary, "vacuous"), text

        # The design itself crashes once the bound allows it: the same value of Mutant, then the control.
        fact, text = vacuous(small, probe("vacuous", "none"))
        self.assertIn('mutant vacuous: does not count: ', text)
        self.assertIn('Mutant is "none" in TwoPhase.small.cfg too', text)
        fact, text = vacuous(small, probe("vacuous", "no_resend"), **{"control-vacuous.cfg": ["invariant", 12]})
        self.assertFalse(fact["ok"])
        self.assertIn("FAIL mutant vacuous: not caught: its control control-vacuous.cfg fails without the mutation: invariant NoCrash violated", text)
        self.assertNotIn("vacuous.cfg", self.fake.ran)
        # At the small baseline's bounds the property holds, and the baseline is the control.
        fact, text = vacuous(small, probe("vacuous", "no_resend", crashes=0))
        self.assertEqual((fact["ok"], fact["control"]), (True, "TwoPhase.small.cfg"), text)
        # No baseline checks the property, or only in another way.
        fact, text = vacuous(probe("vacuous", "no_resend"))
        self.assertIn("mutant vacuous: does not count: no baseline checks NoCrash as it does", text)
        fact, text = vacuous(small, probe("vacuous", "no_resend", section="PROPERTIES"))
        self.assertIn("mutant vacuous: does not count: no baseline checks NoCrash as it does", text)

    def test_a_mutant_of_the_property_alone_does_not_count(self):
        same = self.in_cfg("TwoPhase.cfg", ("INVARIANTS TypeOK", "INVARIANTS Safe TypeOK"))
        mutant = lambda spec: (spec / "mutants" / "vacuous.cfg").write_text(
            (spec / "mutants" / "no_resend.cfg").read_text().replace("expect: Termination", "expect: Safe").replace('"no_resend"', '"x"')
            .replace("INVARIANTS TypeOK", "INVARIANTS Safe TypeOK"))
        edits = json.dumps([["Invariant Consistent is violated", "Invariant Safe is violated"]])
        mapping = {**GOOD, "vacuous.cfg": ["invariant", 12], "commit_one_prepared.cfg": ["liveness", 13]}
        other = self.mutant(("expect: Consistent", "expect: Termination"), cfg="commit_one_prepared.cfg")
        for definitions, why in (
                (['Safe == crashes = 1 => Mutant # "x"'], "Safe reads Mutant, so the mutant may change the property"),
                (['Helper == Mutant # "x"', "Safe == crashes = 1 => Helper"], 'no definition of Spec names "x", so the mutant changes no step'),
                (["Safe == crashes <= 1"], 'no definition of Spec names "x", so the mutant changes no step'),
                (["Safe == LET a == 1\n  IN crashes = a =>\n  Mutant # \"y\""], "Safe reads Mutant"),
                (["Safe == LET\nk == 1\nIN crashes = k => Mutant # \"y\""], "Safe reads Mutant")):
            with self.subTest(definitions):
                _, summary, text = self.run_fake(mapping, FAKE_TLC_EDIT=edits, prep=[add_to_model(*definitions), same, mutant, other], spare=False)
                self.assertIn(why, text)
                self.assertFalse(next(m for m in summary["tla"]["mutants"] if m["name"] == "vacuous")["ok"])
        # Named by a step, directly or through definitions of other modules, it counts.
        step = self.in_cfg("TwoPhase.tla", ("TMCrash ==\n", 'TMCrash ==\n    /\\ Allowed\n'))
        for helper in ('Allowed ==\n  LET m == Mutant\nIN m # "x"', 'Allowed == LET\nm == Mutant\nIN m # "x"', 'Allowed == Deep\nDeep == IF Mutant = "x" THEN TRUE ELSE TRUE'):
            with self.subTest(helper):
                _, summary, text = self.run_fake(mapping, FAKE_TLC_EDIT=edits, prep=[
                    add_to_model("Safe == crashes <= 1"), same, mutant, other, step, self.in_cfg("TwoPhase.tla", ("vars ==", helper + "\nvars =="))], spare=False)
                self.assertTrue(next(m for m in summary["tla"]["mutants"] if m["name"] == "vacuous")["ok"], text)
        # A longer name does not name it, nor does a comment.
        for helper in ('Allowed == Mutant # "xy"', 'Allowed == TRUE \\* "x"', 'Allowed == TRUE (* "x" *)'):
            _, summary, text = self.run_fake(mapping, FAKE_TLC_EDIT=edits, prep=[
                add_to_model("Safe == crashes <= 1"), same, mutant, other, step, self.in_cfg("TwoPhase.tla", ("vars ==", helper + "\nvars =="))], spare=False)
            self.assertIn('names "x", so the mutant changes no step', text)

    def test_a_deadlock_mutant_needs_a_baseline_that_checks_deadlock(self):
        for check, counts in (("CHECK_DEADLOCK TRUE", True), ("CHECK_DEADLOCK FALSE", False)):
            with self.subTest(check):
                _, summary, text = self.run_fake({**GOOD, "stuck.cfg": ["deadlock", 11]}, spare=False, prep=[
                    lambda spec: (spec / "mutants" / "stuck.cfg").write_text(
                        (spec / "mutants" / "no_resend.cfg").read_text().replace("expect: Termination", "expect: Deadlock")),
                    self.in_cfg("TwoPhase.cfg", ("CHECK_DEADLOCK TRUE", check))])
                stuck = next(m for m in summary["tla"]["mutants"] if m["name"] == "stuck")
                self.assertEqual(stuck["ok"], counts, text)
                self.assertEqual("mutant stuck: does not count: no baseline checks Deadlock as it does" in text, not counts)
        # Nor is it a check where the mutant itself does not check it.
        _, summary, text = self.run_fake({**GOOD, "stuck.cfg": ["deadlock", 11]}, spare=False, prep=[
            lambda spec: (spec / "mutants" / "stuck.cfg").write_text(
                (spec / "mutants" / "no_resend.cfg").read_text().replace("expect: Termination", "expect: Deadlock") + "CHECK_DEADLOCK FALSE\n"),
            self.in_cfg("TwoPhase.cfg", ("CHECK_DEADLOCK TRUE", "CHECK_DEADLOCK FALSE"))])
        self.assertIn("mutant stuck: does not count: no baseline checks Deadlock as it does", text)
        parsed = tc.parse_cfg(SELFTEST / "TwoPhase.cfg")
        self.assertEqual([tla_check.check_of(parsed, n) for n in ("Consistent", "Termination", "Deadlock", "Other", "")],
                         ["INVARIANT", "PROPERTY", "CHECK_DEADLOCK", None, None])

    # An override may not replace what a constraint uses.

    def test_an_override_of_what_a_constraint_uses_fails(self):
        def cfg(name, target):
            return [add_to_cfg("CONSTRAINT Few", self.COUNTERS, f"\\* limit: {name} -- a bound"),
                    self.in_cfg("TwoPhase.cfg", ("CONSTANTS\n", f"CONSTANTS\n    {name} {target}\n"))]

        bound = ["Sub == crashes < 9", 'Mine == rmState["r1"] # "committed"', "Deep == crashes < 8", "Twice == Deep /\\ crashes < 9"]
        sets = ["CONSTANTS a1, b1", "Dom == {a1}", 'Dyn == IF tmUp THEN {a1} ELSE {}', "Fixed == {a1} \\cup {b1}"]
        for definitions, few, name, target in ((bound, "Few == Sub", "Sub", "<- Mine"), (bound, "Few == Twice", "Twice", "<- Sub"), (bound, "Few == Twice", "Deep", "<- Mine"),
                                               (bound, "Few == (crashes < 3 /\\ Twice)", "Deep", "<- [MCTwoPhase] Mine"),
                                               (sets, "Few == \\A s \\in Dom : crashes < 5", "Dom", "<- Dyn")):
            with self.subTest(few=few, target=target):
                self.assert_fails(f"the cfg overrides {name}, which a constraint uses, with ", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  prep=[safety_only, add_to_model(*definitions, few), *cfg(name, target)])
        # A constant value is no state.
        for target in ("<- Fixed", "<- RM", "= {}"):
            with self.subTest(target):
                code, _, text = self.run_fake(GOOD, FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[
                    safety_only, add_to_model(*sets, "Few == \\A s \\in Dom : crashes < 5"), *cfg("Dom", target)])
                self.assertEqual(code, 0, text)
        # A definition that no constraint of this cfg uses may be replaced, with its limit line.
        code, _, text = self.run_fake(GOOD, FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[
            safety_only, add_to_model(*bound, "Few == Sub"), *cfg("Deep", "<- Mine")])
        self.assertEqual(code, 0, text)

        # A constraint of another cfg says nothing about this one.
        code, _, text = self.run_fake(GOOD, FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[
            safety_only, add_to_model(*bound, "Few == Sub"),
            lambda spec: (spec / "TwoPhase.more.cfg").write_text(self.replaced((spec / "TwoPhase.cfg").read_text(), [
                ("CONSTANTS\n", "\\* limit: Sub -- x\nCONSTANTS\n    Sub <- Mine\n")])),
            add_to_cfg("CONSTRAINT Few", self.COUNTERS)])
        self.assertEqual(code, 0, text)

    # A name set twice.

    def test_a_cfg_that_sets_a_name_twice_fails(self):
        model = add_to_model("One == 1", "Good == 0..3", "Few == crashes <= 1")
        spec = "SPECIFICATION Spec\n"
        one, large = ("MaxCrashes = 1", "MaxCrashes = {}"), ("MaxCrashes = 2", "MaxCrashes = {}")
        for name, line in (
                ("MaxCrashes", "2\n    MaxCrashes = 1"), ("MaxCrashes", "1\n    MaxCrashes = 1"), ("MaxCrashes", "1 MaxCrashes=2"),
                ("MaxCrashes", "1\nCONSTANT MaxCrashes = 2"), ("MaxCrashes", "1\nCONSTANTS\n  MaxCrashes\n  = 2"),
                ("MaxCrashes", "1\nCONSTANT MaxCrashes <- One"), ("MaxCrashes", "1\n    MaxCrashes <- [MCTwoPhase] One"), ("MaxCrashes", "1 (* x *) MaxCrashes <-[MCTwoPhase]One"),
                ("Nat", "1\n    Nat <- Good\n    Nat <- Good"), ("Nat", "1\n    Nat <- [TwoPhase] Good\n    Nat <- [MCTwoPhase] Good"),
                ("RM", "1\n" + spec + "CONSTANT RM <- MCRM"), ("Mutant", '1\n    Mutant = "none"')):
            for cfg, (old, new), kw in (("TwoPhase.cfg", one, {}), ("TwoPhase.large.cfg", large, dict(extra=("--tier", "T2"))), ("mutants/no_resend.cfg", one, dict(spare=False))):
                if cfg.startswith("mutants") and name == "Mutant":
                    continue  # test_a_mutant_sets_mutant_to_a_value_of_its_own
                with self.subTest(line, cfg=cfg):
                    limit = [self.in_cfg(c, ("CONSTANTS\n", "\\* limit: Nat -- x\nCONSTANTS\n")) for c in ("TwoPhase.cfg", "TwoPhase.large.cfg")]
                    _, summary, text = self.run_fake({**GOOD, "TwoPhase.large.cfg": ["pass", 0]}, prep=[model, *limit, self.in_cfg(cfg, (old, new.format(line)))], **kw)
                    self.assertEqual(summary["verdict"], "fail", text)
                    self.assertIn(f"the cfg sets {name} more than once", text)
                    if cfg.startswith("mutants"):
                        self.assertEqual((self.mutant_fact(summary)["ok"], self.controls_ran()), (False, []))
        for lines, name in ((spec + spec, "SPECIFICATION"), (spec + "SPECIFICATION Weak\n", "SPECIFICATION"), ("INIT Init\nINIT Init\nNEXT Next\n", "INIT"),
                            ("INIT Init NEXT Next\nNEXT Next\n", "NEXT")):
            for cfg in ("TwoPhase.cfg", "mutants/no_resend.cfg"):
                with self.subTest(lines, cfg=cfg):
                    _, summary, text = self.run_fake(GOOD, prep=[self.in_cfg(cfg, (spec, lines))], spare=False)
                    self.assertEqual(summary["verdict"], "fail", text)
                    self.assertIn(f"the cfg sets {name} more than once", text)
        # Once each, also where a name is a value, in a comment, in a string or part of another name, and where a check is named twice.
        for old, new in (("MaxCrashes = 1", "MaxCrashes = 1 \\* MaxCrashes = 2"), ("MaxCrashes = 1", "MaxCrashes = 1 (* MaxCrashes = 2 *)"),
                         ("SPECIFICATION Spec", "SPECIFICATION Spec \\* SPECIFICATION Spec"), ("INVARIANTS TypeOK", "INVARIANT TypeOK\nINVARIANTS TypeOK"),
                         ("PROPERTIES Termination", "PROPERTY Termination\nPROPERTIES Termination")):
            with self.subTest(new):
                code, _, text = self.run_fake(GOOD, prep=[self.in_cfg("TwoPhase.cfg", (old, new))], spare=False)
                self.assertEqual(code, 0, text)
        code, _, text = self.run_fake(GOOD, prep=[safety_only, model, add_to_cfg("CONSTRAINT Few", "CONSTRAINTS Few Few", self.COUNTERS)], FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]))
        self.assertEqual(code, 0, text)

    def test_a_reordered_constant_is_no_control(self):
        """The reviewer's D1: both cfgs set MaxCrashes to 1 and 2, in another order, and TLC takes the last."""
        vac = lambda spec: (spec / "mutants" / "vac.cfg").write_text(
            '\\* expect: OneCrash\nCONSTANTS\n    RM <- MCRM\n    MaxCrashes = 1\n    MaxCrashes = 2\n    MaxDrops = 1\n    Mutant = "no_resend"\nSPECIFICATION Spec\nINVARIANTS OneCrash\n')
        _, summary, text = self.run_fake({**GOOD, "vac.cfg": ["invariant", 12]}, spare=False, edits={"vac.cfg": [["Invariant Consistent is violated", "Invariant OneCrash is violated"]]}, prep=[
            add_to_model("OneCrash == crashes <= 1"), vac,
            self.in_cfg("TwoPhase.cfg", ("MaxCrashes = 1", "MaxCrashes = 2\n    MaxCrashes = 1"), ("TypeOK Consistent", "TypeOK Consistent OneCrash"))])
        self.assertEqual(summary["verdict"], "fail", text)
        self.assertIn("FAIL search of TwoPhase.cfg: the cfg sets MaxCrashes more than once", text)
        self.assertIn("FAIL mutant vac: does not count: the cfg sets MaxCrashes more than once", text)
        self.assertNotIn("vac.cfg", self.fake.ran)

    # The settings, for the reviewer.

    def settings_of(self, summary):
        stored = json.loads((self.fake.out / "summary.json").read_text())["settings"]
        self.assertEqual(hashlib.sha256(json.dumps(stored, sort_keys=True).encode()).hexdigest(), summary["tla"]["settings_sha256"])
        self.assertEqual(len(stored), len(summary["tla"]["settings"]))
        self.assertIsNone(run_check_errors({"tla": {**summary["tla"], "settings": summary["tla"]["settings"][:80]}}))
        return {s["cfg"]: s for s in stored}

    def test_settings_list_what_a_reviewer_has_to_read(self):
        code, summary, text = self.run_fake(GOOD, FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]), prep=[
            safety_only, add_to_model("MCInit == Init", "MCNext ==\n  /\\ Next \\* a comment\n  /\\ rmState[\"r1\"]' # \"committed\"", "Sub == crashes < 9",
                                      "Few ==\n  /\\ Sub\n  /\\ drops <= MaxDrops", "Good == 0..3"),
            self.in_cfg("TwoPhase.cfg", ("SPECIFICATION Spec", "INIT MCInit\nNEXT MCNext\nSYMMETRY Sym\nCONSTRAINT Few"), ("CHECK_DEADLOCK TRUE", "CHECK_DEADLOCK FALSE"),
                        ("CONSTANTS\n", self.COUNTERS + "\n\\* limit: Nat -- finite\nCONSTANTS\n    Nat <- Good\n"))])
        self.assertEqual(code, 0, text)
        stored = self.settings_of(summary)
        self.assertEqual(list(stored), ["TwoPhase.cfg", "TwoPhase.spare.cfg", "commit_one_prepared.cfg", "no_resend.cfg"])
        main = stored["TwoPhase.cfg"]
        self.assertEqual(main["specification"], ["INIT MCInit", "NEXT MCNext"])
        self.assertEqual(main["constants"], ["MaxCrashes = 1", "MaxDrops = 1", 'Mutant = "none"', "Nat <- Good", "RM <- MCRM"])
        self.assertEqual(main["constraints"], [{"name": "Few", "declared": "bounds the two fault counters", "text": [
            "Few ==\n  /\\ Sub\n  /\\ drops <= MaxDrops", "Sub == crashes < 9"]}])
        self.assertEqual((main["symmetry"], main["overrides"], main["check_deadlock"], main["view"]), (["Sym"], ["Nat <- Good"], False, []))
        self.assertEqual((main["invariants"], main["properties"]), (["TypeOK", "Consistent"], []))
        self.assertEqual(main["definitions"]["MCNext"], ["MCNext ==\n  /\\ Next\n  /\\ rmState[\"r1\"]' # \"committed\""])
        self.assertEqual(main["definitions"]["MCRM"], ['MCRM == {"r1", "r2", "r3"}'])
        self.assertEqual((main["definitions"]["Good"], main["definitions"]["MCInit"]), (["Good == 0..3"], ["MCInit == Init"]))
        self.assertNotIn("changed", main)
        spare = stored["TwoPhase.spare.cfg"]
        self.assertEqual((spare["specification"], spare["check_deadlock"], spare["properties"]), (["SPECIFICATION Spec"], True, ["Termination"]))
        mutant = stored["no_resend.cfg"]
        self.assertEqual(mutant["expect"], "Termination")
        self.assertEqual(mutant["changed"]["TwoPhase.spare.cfg"], {"mutant": ['Mutant = "no_resend"'], "baseline": ['Mutant = "none"']})
        self.assertIn("NEXT MCNext", mutant["changed"]["TwoPhase.cfg"]["baseline"])
        # The record's short form.
        self.assertEqual(summary["tla"]["settings"], [
            'TwoPhase.cfg: INIT MCInit, NEXT MCNext, MaxCrashes = 1, MaxDrops = 1, Mutant = "none", Nat <- Good, RM <- MCRM, '
            'CONSTRAINT Few (Few == /\\ Sub /\\ drops <= MaxDrops; Sub == crashes < 9), SYMMETRY Sym, CHECK_DEADLOCK FALSE',
            'TwoPhase.spare.cfg: SPECIFICATION Spec, MaxCrashes = 1, MaxDrops = 1, Mutant = "none", RM <- MCRM',
            'mutant commit_one_prepared of TwoPhase.spare.cfg: Mutant = "commit_one_prepared" instead of Mutant = "none"',
            'mutant no_resend of TwoPhase.spare.cfg: Mutant = "no_resend" instead of Mutant = "none"'])

    def test_the_settings_digest_follows_every_setting(self):
        digests = set()
        steps = [[], [self.in_cfg("TwoPhase.cfg", ("MaxCrashes = 1", "MaxCrashes = 0"))], [add_to_model("X == 1")], [add_to_cfg("SYMMETRY Sym")],
                 [self.in_cfg("TwoPhase.cfg", ("SPECIFICATION Spec", "INIT Init\nNEXT Next"))], [self.in_cfg("TwoPhase.cfg", ("CHECK_DEADLOCK TRUE", "CHECK_DEADLOCK FALSE"))],
                 [self.in_cfg("MCTwoPhase.tla", ('"r2", "r3"}', '"r2"}'))], [self.mutant(("MaxDrops = 1", "MaxDrops = 2"))],
                 [self.in_cfg("TwoPhase.tla", ("Spec == Init", "Spec == TRUE /\\ Init"))]]
        for prep in steps:
            _, summary, _ = self.run_fake(GOOD, prep=prep, spare=True)
            self.settings_of(summary)
            digests.add(summary["tla"]["settings_sha256"])
        self.assertEqual(len(digests), len(steps) - 1)  # a definition that no setting names is none of them

    def test_t2_has_its_settings_too(self):
        _, summary, _ = self.run_fake({"TwoPhase.large.cfg": ["simulation", 0]}, extra=("--tier", "T2", "--simulate"))
        self.assertEqual(summary["tla"]["settings"], ['TwoPhase.large.cfg: SPECIFICATION Spec, MaxCrashes = 2, MaxDrops = 2, Mutant = "none", RM <- MCRMLarge'])
        self.assertEqual(self.settings_of(summary)["TwoPhase.large.cfg"]["definitions"]["MCRMLarge"], ['MCRMLarge == {"r1", "r2", "r3", "r4"}'])

    def test_depth_limit_and_state_cap_fail(self):
        for body in ('TLCGet("level") < 9', 'TLCGet("distinct") < 800', 'TLCGet("stats").diameter < 5',
                     "TLCGetOrDefault(1, 0) <= 5", "JavaTime < 5", "RandomElement(1..3) <= 2"):
            with self.subTest(body):
                self.assert_fails("FAIL search of the model: MCTwoPhase.tla uses", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  failed=2, prep=self.limited(f"Few == {body}"))
        # Anywhere in the model, also in the design module and behind an operator.
        self.assert_fails("TwoPhase.tla uses TLCSet", prep=[
            lambda spec: edit(spec / "TwoPhase.tla", ("vars ==", 'Stop == TLCSet("exit", TRUE)\nvars =='))])
        for module in ("TLCExt", "Randomization", "IOUtils"):
            self.assert_fails(f"MCTwoPhase.tla uses the module {module}", prep=[add_to_model(extends=f", {module}")])
            self.assert_fails(f"uses the module {module}", prep=[add_to_model(f"X == INSTANCE {module}")])
        for how in ({"extends": ", Helper"}, {}):
            self.assert_fails("Helper.tla uses TLCGet", prep=[
                add_to_model(*([] if how else ["H == INSTANCE Helper"]), **how),
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
        self.assert_fails("baseline TwoPhase.cfg: liveness under SYMMETRY", edits={"TwoPhase.cfg": [["@!@!@STARTMSG 2193:0", warning]]},
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
                            (("POSTCONDITION Post",), "Post"), (("CONSTRAINT Few", "\\* limit: Few -- x"), "Few")):
            with self.subTest(name):
                self.assert_fails(f"the cfg overrides {name}, which it checks or runs", FAKE_TLC_EDIT=json.dumps([SAFETY_LOG]),
                                  prep=[add_to_model("Mine == 1", "Post == TRUE", "Few == crashes <= 1"), add_to_cfg(*lines),
                                        override(f"{name} <- Mine")])
        # A mutant must check the same property as the baseline.
        for name in ("Len", "Cardinality", "crashes"):
            self.assert_fails(f"the cfg overrides {name}, which a constraint relies on", prep=[mine, override(f"{name} <- Mine")])
        self.assert_fails("mutant commit_one_prepared: does not count: ", "the cfg overrides Consistent, which it checks or runs", prep=[lambda spec: edit(
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
        summary = self.assert_fails("mutant z55: no violation", failed=2, mapping=mapping, prep=[many], spare=False)
        tla = summary["tla"]
        self.assertEqual((len(tla["mutants"]), len(tla["runs"])), (62, 26))
        self.assertEqual([m["name"] for m in tla["mutants"] if not m["ok"]], ["z55"])
        self.assertEqual([r["cfg"] for r in tla["runs"] if r["verdict"] != "pass"], ["TwoPhase.v24.cfg"])
        names = [a["name"] for a in summary["artifacts"]]
        self.assertEqual((names[0], len(names), len(set(names))), ("tla/summary.json", 89, 89))
        stored = json.loads((self.fake.out / "summary.json").read_text())
        self.assertEqual(len(stored.pop("settings")), 88)
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

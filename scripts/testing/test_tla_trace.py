#!/usr/bin/env python3
"""Tests for tla_trace.py: the nabedra.trace/1 lint and the trace check, with a fake TLC wrapper."""

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
import tla_common as tc  # noqa: E402
import tla_trace  # noqa: E402

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "testdata" / "tla"
SELFTEST = HERE.parents[1] / "specs" / "selftest"
ACCEPTED = SELFTEST / "traces" / "accepted" / "TwoPhase-1.ndjson"
REJECTED = SELFTEST / "traces" / "rejected" / "TwoPhase-2.at-6.ndjson"


def lines():
    return tla_trace.read_lines(ACCEPTED)


def edit(index, **changes):
    out = lines()
    obj = json.loads(out[index])
    for key, value in changes.items():
        if value is KeyError:
            obj.pop(key, None)
        else:
            obj[key] = value
    out[index] = json.dumps(obj)
    return out


def msg(code, body, cls=0):
    return f"@!@!@STARTMSG {code}:{cls} @!@!@\n{body}\n@!@!@ENDMSG {code} @!@!@\n"


STATS = msg(tc.EC_STATS, "9 states generated, 9 distinct states found, 0 states left on queue.")
DONE = msg(tc.EC_SUCCESS, "Model checking completed. No error has been found.") + STATS


class CommonTests(unittest.TestCase):
    """tla_common.py: TLC's exit status and messages, comments and cfg sections."""

    def test_nonzero_exit_after_success_is_an_error(self):
        self.assertTrue(tc.parse_tlc_output(DONE, 0).complete)
        for code in (1, 137):
            res = tc.parse_tlc_output(DONE, code)
            self.assertFalse(res.complete)
            self.assertIn(f"status {code}", res.errors[0])
        sim = msg(tc.EC_SIM_DONE, "The number of states generated: 5")
        self.assertTrue(tc.parse_tlc_output(sim, 0).simulation_done)
        res = tc.parse_tlc_output(sim, 1)
        self.assertFalse(res.simulation_done)
        self.assertTrue(res.errors)

    def test_violation_with_its_own_exit_status_is_not_an_error(self):
        text = msg(2110, "Invariant Consistent is violated.", 1) + STATS
        res = tc.parse_tlc_output(text, 12)
        self.assertEqual((res.violation(), res.errors), (("invariant", "Consistent"), []))

    def test_unterminated_message_is_an_error(self):
        cut = "@!@!@STARTMSG 1000:1 @!@!@\nout of memory\n" + DONE
        res = tc.parse_tlc_output(cut, 0)
        self.assertFalse(res.complete)
        self.assertIn("unterminated", " ".join(res.errors))
        # A run killed at its budget has cut-off output and no exit status.
        self.assertEqual(tc.parse_tlc_output(STATS + "@!@!@STARTMSG 2200:0 @!@!@\npro").errors, [])

    def test_comments(self):
        for text, want in (("a (* x *) b", "a   b"), ("a (* (* x *) y *) b", "a   b"), ("a \\* (* b\nc", "a \nc"),
                           ("(* \\* *) a", "  a"), ('"(*" a', '"(*" a'), ('"a\\"(*" b', '"a\\"(*" b'),
                           ("(* a\nb *)c", "\n c"), ("a (* b", "a ")):
            self.assertEqual(tc.strip_tla_comments(text), want, text)
        self.assertEqual(tc.strip_tla_comments('x = "INVARIANT" y', keep_strings=False), 'x = "" y')

    def cfg(self, text):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "x.cfg"
            path.write_text(text)
            return tc.parse_cfg(path)

    def test_cfg_sections(self):
        c = self.cfg("CONSTANTS\n A <- B\n C = 1\n D <- [M]E\nINVARIANTS I1 I2\nPROPERTY P\nVIEW V\n"
                     "CONSTRAINT C1\nACTION_CONSTRAINTS A1 A2\nPOSTCONDITION TraceAccepted\nCHECK_DEADLOCK FALSE\n")
        self.assertEqual((c.invariants, c.properties, c.postconditions, c.check_deadlock),
                         (["I1", "I2"], ["P"], ["TraceAccepted"], False))
        self.assertEqual(c.overrides, ["A", "C", "D"])
        self.assertEqual((c.sections["VIEW"], c.sections["CONSTRAINT"], c.sections["ACTION_CONSTRAINT"]),
                         (["V"], ["C1"], ["A1", "A2"]))

    def test_cfg_comments_are_read_as_tlc_reads_them(self):
        # Each case was checked against TLC 1.8.0.
        self.assertEqual(self.cfg("(* (* *) INVARIANT I *)\n").invariants, [])
        self.assertEqual(self.cfg("(* \\* *) INVARIANT I\n").invariants, ["I"])
        self.assertEqual(self.cfg("\\* (*\nINVARIANT I\n").invariants, ["I"])
        self.assertEqual(self.cfg('CONSTANT S = "(*"\nINVARIANT I\n').invariants, ["I"])
        self.assertEqual(self.cfg("INVARIANT I (*x*)J\n").invariants, ["I", "J"])
        self.assertEqual(self.cfg('CONSTANT S = "INVARIANT I"\n').invariants, [])


class LintTests(unittest.TestCase):
    def test_samples_are_valid(self):
        self.assertEqual(tla_trace.lint_file(ACCEPTED), [])
        self.assertEqual(tla_trace.lint_file(REJECTED), [])

    def assertLint(self, trace, fragment):
        errors = tla_trace.lint_lines(trace)
        self.assertTrue(any(fragment in e for e in errors), errors)

    def test_header_rules(self):
        self.assertLint([], "header is missing")
        self.assertLint(edit(0, trace="nabedra.trace/2"), "header trace must be")
        self.assertLint(edit(0, sha="abc"), "40-character git SHA")
        self.assertLint(edit(0, seed=-1), "seed must be an integer")
        self.assertLint(edit(0, spec="Two Phase"), "TLA+ module name")
        self.assertLint(edit(0, params=KeyError), "header misses ['params']")
        self.assertLint(edit(0, extra=1), "unknown keys ['extra']")
        self.assertLint(edit(0, params={"x": 1.5}), "float is not allowed")

    def test_step_rules(self):
        self.assertLint(edit(3, seq=7), "seq is 7, expected 2")
        self.assertLint(edit(3, t=1), "is before the previous step's")
        self.assertLint(edit(3, action="TM Commit"), "spec action name")
        self.assertLint(edit(3, vars=KeyError), "step misses ['vars']")
        self.assertLint(edit(3, vars={"x": None}), "null has no TLA+ value")
        self.assertLint(edit(3, msg=[1]), "msg must be an object")
        self.assertLint(edit(3, color="red"), "unknown keys ['color']")
        self.assertLint(lines()[:2] + ["{not json"], "not JSON")
        self.assertLint(lines()[:2] + [""] + lines()[2:], "blank line")
        self.assertLint(lines()[:2] + ["[1]"], "not a JSON object")
        self.assertLint(edit(1, seq=0.0), "seq is 0.0, expected 0")
        self.assertLint(edit(2, seq=True), "seq is True, expected 1")
        self.assertLint(lines()[:1], "no steps")

    def test_lint_command(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as d, redirect_stdout(out):
            bad = Path(d) / "bad.ndjson"
            bad.write_text("\n".join(edit(2, seq=9)) + "\n")
            self.assertEqual(tla_trace.main(["lint", str(ACCEPTED), str(REJECTED)]), 0)
            self.assertEqual(tla_trace.main(["lint", str(bad)]), 1)
        self.assertIn("seq is 9", out.getvalue())


FAKE_WRAPPER = textwrap.dedent('''\
    #!/usr/bin/env python3
    # A stand-in for specs/tools/tlc: a trace whose name contains "at-" is rejected, others accepted.
    import os, sys
    name = "trace_rejected" if "at-" in os.environ.get("TRACE_PATH", "") else "trace_accepted"
    if "error" in os.environ.get("TRACE_PATH", ""):
        name = "parse_error"
    if "silent" in os.environ.get("TRACE_PATH", ""):
        name = "invariant"
    text = open(os.path.join(os.environ["FAKE_TLC_FIXTURES"], name + ".txt")).read()
    # "silent": a trace spec step that consumes no line breaks TraceIndexIsLevel.
    sys.stdout.write(text.replace("Invariant Consistent is violated", "Invariant TraceIndexIsLevel is violated"))
    sys.exit({"trace_rejected": 10, "trace_accepted": 0, "parse_error": 150, "invariant": 12}[name])
''')


class CheckTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        self.spec = root / "spec"
        self.spec.mkdir()
        for name in ("TwoPhaseTrace.tla", "TwoPhaseTrace.cfg"):
            shutil.copy(SELFTEST / name, self.spec / name)
        self.good = root / "good"
        self.bad = root / "bad"
        self.good.mkdir()
        self.bad.mkdir()
        shutil.copy(ACCEPTED, self.good)
        shutil.copy(REJECTED, self.bad)
        wrapper = root / "fake-tlc"
        wrapper.write_text(FAKE_WRAPPER)
        wrapper.chmod(0o755)
        self.root = root
        self.env = {"NABEDRA_TLC_WRAPPER": str(wrapper), "FAKE_TLC_FIXTURES": str(FIXTURES)}

    def check(self, *args):
        out = io.StringIO()
        with patch.dict(os.environ, self.env), redirect_stdout(out):
            code = tla_trace.main(["check", str(self.spec), "TwoPhase", *args, "--budget-s", "60",
                                   "--out", str(self.root / "out")])
        return code, tc.read_summary(out.getvalue()), out.getvalue()

    def test_accepted_and_expected_rejection_pass(self):
        code, summary, text = self.check(str(self.good), "--expect-rejected", str(self.bad))
        self.assertEqual((code, summary["verdict"], summary["executed"]), (0, "pass", 2))
        self.assertEqual(summary["tla"]["traces"]["expected_rejections"], 1)
        self.assertEqual(summary["tla"]["mode"], "trace")
        self.assertIn("rejected as expected at index 6", text)

    def test_unexpected_rejection_reports_index_and_content(self):
        code, summary, text = self.check(str(self.bad))
        self.assertEqual((code, summary["verdict"], summary["failed"]), (1, "fail", 1))
        self.assertIn("REJECTED at index 6 (seq 6, file line 8)", text)
        self.assertIn('"action":"RMChooseToAbort"', text)
        self.assertEqual(summary["tla"]["traces"]["failing"][0]["index"], 6)

    def test_wrong_expected_index_and_unexpected_acceptance_fail(self):
        moved = self.bad / "TwoPhase-2.at-5.ndjson"
        (self.bad / REJECTED.name).rename(moved)
        code, summary, text = self.check("--expect-rejected", str(self.bad))
        self.assertEqual(summary["verdict"], "fail")
        self.assertIn("rejected at index 6, expected 5", text)
        accepted_but = self.root / "neg"
        accepted_but.mkdir()
        shutil.copy(ACCEPTED, accepted_but)
        code, summary, text = self.check("--expect-rejected", str(accepted_but))
        self.assertIn("accepted, but it must be rejected", text)

    def test_lint_failure_and_wrong_spec_fail(self):
        broken = self.good / "broken.ndjson"
        broken.write_text("\n".join(edit(2, seq=4)) + "\n")
        other = self.good / "other.ndjson"
        other.write_text("\n".join(edit(0, spec="Other")) + "\n")
        code, summary, text = self.check(str(self.good))
        self.assertEqual((summary["verdict"], summary["failed"], summary["executed"]), ("fail", 2, 3))
        self.assertIn("header spec is Other", text)

    def test_tlc_error_is_error_and_no_traces_is_not_run(self):
        shutil.copy(ACCEPTED, self.good / "error-case.ndjson")
        code, summary, _ = self.check(str(self.good))
        self.assertEqual((code, summary["verdict"]), (3, "error"))
        empty = self.root / "empty"
        empty.mkdir()
        code, summary, _ = self.check(str(empty))
        self.assertEqual((code, summary["verdict"]), (2, "not-run"))

    def test_step_that_consumes_no_line_is_error(self):
        shutil.copy(ACCEPTED, self.good / "silent.ndjson")
        code, summary, text = self.check(str(self.good))
        self.assertEqual((code, summary["verdict"]), (3, "error"))
        self.assertIn("consumes no line", text)

    def test_trace_cfg_without_level_invariant_is_error(self):
        cfg = self.spec / "TwoPhaseTrace.cfg"
        cfg.write_text(cfg.read_text().replace(" TraceIndexIsLevel", ""))
        code, summary, text = self.check(str(self.good))
        self.assertEqual((code, summary["verdict"]), (3, "error"))
        self.assertIn("must list the invariant TraceIndexIsLevel", text)

    def test_missing_trace_spec_is_error(self):
        (self.spec / "TwoPhaseTrace.cfg").unlink()
        code, summary, _ = self.check(str(self.good))
        self.assertEqual(summary["verdict"], "error")


if __name__ == "__main__":
    unittest.main()

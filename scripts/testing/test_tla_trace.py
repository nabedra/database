#!/usr/bin/env python3
"""Tests for tla_trace.py: the nabedra.trace/1 lint and the trace check, with a fake TLC wrapper."""

import io
import json
import os
import re
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
import run_check  # noqa: E402
import tla_trace  # noqa: E402

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "testdata" / "tla"
SELFTEST = HERE.parents[1] / "specs" / "selftest"
ACCEPTED = SELFTEST / "traces" / "accepted" / "TwoPhase-1.ndjson"
REJECTED = SELFTEST / "traces" / "rejected" / "TwoPhase-2.at-6.ndjson"


def weaken(spec):
    """A trace spec that requires no variable."""
    spec = spec.replace("RequireVar", "OptionalVar").replace("RequireSet", "OptionalSet")
    return re.sub(r"Changed(Var|Set)\(\w+, ", r"Optional\1(", spec)


def tla_trace_verdict(code):
    return {v: k for k, v in tla_trace.EXIT.items()}[code]


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

    def test_trace_notes(self):
        # TLC wraps long values, and repeats a note per behavior.
        text = ('<<"nabedra-trace-rejected", 7, 12>>\n<<"nabedra-trace-line", 7, "missing", "tmUp">>\n'
                '<< "nabedra-trace-line",\n   7,\n   "different",\n   "a very long variable name" >>\n'
                '<<"nabedra-trace-line", 7, "missing", "tmUp">>\n<<"nabedra-trace-line", 3, "missing", "x">>\n')
        res = tc.parse_tlc_output(text, 10)
        self.assertEqual(res.trace_notes, [(3, "missing", "x"), (7, "different", "a very long variable name"),
                                           (7, "missing", "tmUp")])
        self.assertEqual(tla_trace._notes(res, 7), "different value: a very long variable name; missing: tmUp")
        self.assertEqual(tla_trace._notes(res, 5), "")

    def test_unreadable_final_statistics_are_not_exhaustive(self):
        unreadable = msg(tc.EC_STATS, "9 states generated and 5 states still to do")
        self.assertTrue(tc.parse_tlc_output(DONE, 0).complete)
        for text in (DONE + unreadable, msg(tc.EC_SUCCESS, "Model checking completed. No error has been found.") + unreadable):
            res = tc.parse_tlc_output(text, 0)
            self.assertEqual((res.complete, res.left, res.distinct), (False, None, None))
            self.assertIn("unreadable statistics", res.errors[0])
            # The exit status is reported too, although the run is already incomplete.
            self.assertIn("exited with status 137", " ".join(tc.parse_tlc_output(text, 137).errors))

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
                     "CONSTRAINT C1\nACTION_CONSTRAINTS A1 A2\nPOSTCONDITION TraceAccepted\nCHECK_DEADLOCK FALSE\n"
                     "ALIAS Al\n")
        self.assertEqual(c.sections["ALIAS"], ["Al"])
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

    def test_integers_that_tlc_uses_fit_32_bits(self):
        for value in (2**31 - 1, -2**31):
            self.assertEqual(tla_trace.lint_lines(edit(3, vars={"x": value})), [])
        for value in (2**31, -2**31 - 1, 2**32 + 1, 2**64 - 1):
            self.assertLint(edit(3, vars={"x": value}), f"vars.x: {value} does not fit TLC's 32-bit integers")
            self.assertLint(edit(3, vars={"x": [{"y": value}]}), "vars.x[0].y")
            self.assertLint(edit(0, params={"RM": [value]}), "params.RM[0]")
        # TLC does not read t and seed, and a message field becomes a string.
        self.assertEqual(tla_trace.lint_lines(edit(0, seed=2**64 - 1)), [])
        self.assertEqual(tla_trace.lint_lines(edit(12, t=2**63)), [])
        self.assertEqual(tla_trace.lint_lines(edit(3, msg={"nanos": 2**40})), [])
        self.assertLint(edit(0, seed=2**64), "seed must be an integer")

    def test_key_twice_and_unreadable_json(self):
        trace = lines()
        trace[8] = trace[8].replace('"action":', '"action":"TMTimeout","action":')
        self.assertLint(trace, "line 9: the key 'action' appears twice")
        trace = lines()
        trace[3] = trace[3].replace('"tmPrepared":', '"tmPrepared":[],"tmPrepared":')
        self.assertLint(trace, "the key 'tmPrepared' appears twice")
        self.assertLint(lines()[:2] + ["[" * 100_000], "not JSON")
        self.assertLint(lines()[:2] + ["9" * 5000], "not JSON")

    def test_the_copy_that_tlc_reads(self):
        trace = edit(0, seed=2**64 - 1)
        trace[4] = edit(4, t=2**63, msg={"n": 2**32 + 5, "deep": [{"m": -2**31 - 1}], "ok": 2**31 - 1, "s": "x"})[4]
        text, wide = tla_trace.tlc_copy(trace)
        got = [json.loads(l) for l in text.splitlines()]
        want = [json.loads(l) for l in trace]
        self.assertEqual(wide, {3: ["msg.n", "msg.deep[0].m"]})
        self.assertEqual(got[4]["msg"], {"n": "4294967301", "deep": [{"m": "-2147483649"}], "ok": 2**31 - 1, "s": "x"})
        del want[0]["seed"], want[4]["msg"], got[4]["msg"]
        for step in want[1:]:
            del step["t"]
        self.assertEqual(got, want)

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
    # A stand-in for specs/tools/tlc. The trace's name picks the output: "at-" is rejected at 6, the
    # control trace at FAKE_CONTROL (1, or "accept"), and others are accepted.
    import os, sys
    trace = os.path.basename(os.environ.get("TRACE_PATH", ""))
    at = "6" if "at-" in trace else None
    name = "trace_rejected" if at else "trace_accepted"
    if trace == "control.ndjson":
        at = os.environ.get("FAKE_CONTROL", "1")
        name = "trace_accepted" if at == "accept" else "trace_rejected"
    for key, fixture in (("error", "parse_error"), ("silent", "invariant"), ("missing", "trace_missing"),
                         ("wide", "trace_wide")):
        if key in trace:
            name = fixture
    text = open(os.path.join(os.environ["FAKE_TLC_FIXTURES"], name + ".txt")).read()
    # "silent": a trace spec step that consumes no line breaks TraceIndexIsLevel.
    text = text.replace("Invariant Consistent is violated", "Invariant TraceIndexIsLevel is violated")
    text = text.replace('"nabedra-trace-rejected", 6,', f'"nabedra-trace-rejected", {at},')
    sys.stdout.write(text)
    sys.exit({"trace_accepted": 0, "parse_error": 150, "invariant": 12, "trace_wide": 75}.get(name, 10))
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

    def spec_error(self, fragment, *more):
        """The trace spec is refused before TLC runs, and no trace counts as accepted."""
        code, summary, text = self.check(str(self.good))
        self.assertEqual((code, summary["verdict"], summary["executed"]), (3, "error", 0), text)
        for f in (fragment, *more):
            self.assertIn(f, text)

    def edit_spec(self, name, old, new):
        path = self.spec / name
        text = path.read_text()
        self.assertIn(old, text)
        path.write_text(text.replace(old, new, 1))

    def test_cfg_without_postcondition_is_error(self):
        self.edit_spec("TwoPhaseTrace.cfg", "POSTCONDITION TraceAccepted\n", "")
        self.spec_error("must set POSTCONDITION TraceAccepted")

    def test_postcondition_or_invariant_in_a_nested_comment_is_error(self):
        self.edit_spec("TwoPhaseTrace.cfg", "POSTCONDITION TraceAccepted", "(* (* *) POSTCONDITION TraceAccepted *)")
        self.edit_spec("TwoPhaseTrace.cfg", " TraceIndexIsLevel", "\n(* (* *) INVARIANT TraceIndexIsLevel *)")
        self.spec_error("must set POSTCONDITION TraceAccepted", "must list the invariant TraceIndexIsLevel")

    def test_cfg_override_of_a_library_operator_is_error(self):
        path = self.spec / "TwoPhaseTrace.cfg"
        original = path.read_text()
        for line, op in (("TraceAccepted <- Yes", "TraceAccepted"), ("TraceIndexIsLevel = TRUE", "TraceIndexIsLevel"),
                         ("TLCGet <- Yes", "TLCGet"), ("LineVarsOK <- Yes", "LineVarsOK"),
                         ("ndJsonDeserialize <- Yes", "ndJsonDeserialize")):
            path.write_text(original.replace("CONSTANTS\n", f"CONSTANTS\n    {line}\n"))
            self.spec_error(f"overrides {op},")

    def test_shadowed_module_is_error(self):
        for name in ("NabedraTrace", "Json", "TLC"):
            path = self.spec / f"{name}.tla"
            path.write_text("---- MODULE X ----\n====\n")
            self.spec_error(f"{name}.tla in the spec folder would replace")
            path.unlink()

    def test_step_that_sets_the_trace_index_is_error(self):
        path = self.spec / "TwoPhaseTrace.tla"
        original = path.read_text()
        for step in ("(traceIndex)' = traceIndex + 1", "Same(traceIndex) /\\ UNCHANGED vars",
                     "traceIndex' = traceIndex + 1 /\\ UNCHANGED vars", "UNCHANGED <<vars, traceIndex>>",
                     "UNCHANGED traceExplained", "<<vars, traceIndex>>' = 1", "traceExplained ' = 2"):
            path.write_text(original.replace("TraceNext ==\n", f"TraceNext ==\n    \\/ {step}\n"))
            self.spec_error("only TraceStep may consume a line")
        path.write_text(original.replace("TraceNext ==\n", "\\* traceIndex' (* x *)\n(* (* *) traceIndex' *)\nTraceNext ==\n"))
        self.assertEqual(self.check(str(self.good))[0], 0)

    def test_old_operators_have_a_clear_error(self):
        path = self.spec / "TwoPhaseTrace.tla"
        original = path.read_text()
        for op in ("IsEvent", "MatchVar", "MatchSet", "Logged", "LoggedValue"):
            path.write_text(original.replace("TraceNext ==\n", f"X == {op}(1)\nTraceNext ==\n"))
            self.spec_error(f"TwoPhaseTrace.tla uses {op}, which accepted a line that does not log the variable")
        path.write_text(original.replace("TraceNext ==\n", "\\* MatchVar\nTraceNext ==\n"))
        self.assertEqual(self.check(str(self.good))[0], 0)
        # A design spec may have an operator with one of these names.
        (self.spec / "TwoPhase.tla").write_text("---- MODULE TwoPhase ----\nIsEvent(x) == x = 1\n====\n")
        path.write_text(original.replace("TraceNext ==\n", "X == IsEvent(1)\nTraceNext ==\n"))
        self.assertEqual(self.check(str(self.good))[0], 0)

    def test_trace_spec_that_requires_nothing_is_error(self):
        path = self.spec / "TwoPhaseTrace.tla"
        original = path.read_text()
        weak = original.replace("RequireVar", "OptionalVar").replace("RequireSet", "OptionalSet")
        path.write_text(weak)  # ChangedVar and ChangedSet are left
        self.assertEqual(self.check(str(self.good))[0], 0)
        weak = weaken(original)
        path.write_text("\\* RequireVar\n" + weak + "(* RequireSet *)\n")
        self.spec_error("requires no variable")
        # The requirement may be in a helper module.
        path.write_text(weak.replace("EXTENDS TwoPhase, ", "EXTENDS Helper, TwoPhase, "))
        (self.spec / "Helper.tla").write_text("---- MODULE Helper ----\nEXTENDS NabedraTrace\nReq(x) == RequireVar(x)\n====\n")
        self.assertEqual(self.check(str(self.good))[0], 0)
        path.write_text(original.replace("TraceStep", "OtherStep"))
        self.spec_error("takes no TraceStep")

    def test_trace_spec_that_accepts_any_action_is_error(self):
        for control, fragment in (("accept", "but the trace was accepted"), ("0", "but the trace was rejected at index 0")):
            self.env["FAKE_CONTROL"] = control
            code, summary, text = self.check(str(self.good))
            self.assertEqual((code, summary["verdict"]), (3, "error"))
            self.assertIn(fragment, text)
        # Without an accepted trace there is nothing to control.
        _, _, text = self.check("--expect-rejected", str(self.bad))
        self.assertNotIn(tla_trace.CONTROL_ACTION, text)
        lines_ = (self.root / "out" / "control.ndjson").read_text().splitlines()
        self.assertEqual(tla_trace.lint_lines(lines_), [])
        self.assertEqual(json.loads(lines_[2])["action"], tla_trace.CONTROL_ACTION)

    def test_missing_variable_is_a_rejection_that_names_line_and_variable(self):
        shutil.copy(ACCEPTED, self.good / "missing.ndjson")
        code, summary, text = self.check(str(self.good))
        self.assertEqual((code, summary["verdict"], summary["not_run"]), (1, "fail", []))
        self.assertIn("REJECTED at index 7 (seq 7, file line 9), no behavior explains this line (missing: tmDecision)", text)
        failing = summary["tla"]["traces"]["failing"][0]
        self.assertEqual(failing["index"], 7)
        self.assertIn("missing: tmDecision", failing["reason"])

    def test_tlc_error_names_the_line_and_the_wide_field(self):
        trace = self.good / "wide.ndjson"
        trace.write_text("\n".join(edit(10, msg={"type": "Commit", "delay_ns": 2**32 + 5})) + "\n")
        code, summary, text = self.check(str(self.good))
        self.assertEqual((code, summary["verdict"], summary["failed"]), (3, "error", 0))
        traces = summary["tla"]["traces"]
        self.assertEqual((traces["accepted"], traces["rejected"], traces["checked"]), (1, 0, 2))
        reason = summary["not_run"][0]["reason"]
        self.assertIn("wide.ndjson: at index 9 outside 32 bits, so TLC reads a string: index 9 msg.delay_ns", reason)
        self.assertIn('integer 5 with non-integer: "4294967301"', reason)
        copy = self.root / "out" / "traces" / "trace-001" / "wide.ndjson"
        self.assertEqual(copy.read_text(), tla_trace.tlc_copy(tla_trace.read_lines(trace))[0])
        self.assertNotIn('"t":', copy.read_text())
        self.assertNotIn('"seed":', copy.read_text())

    def test_directories_that_check_less_than_they_hold_are_errors(self):
        empty = self.root / "empty"
        (empty / "sub").mkdir(parents=True)
        for args, fragment in (((str(self.good), "--expect-rejected", str(empty)), "has no traces"),
                               ((str(self.good), str(self.root / "none")), "does not exist"),
                               ((str(self.root),), "in a subfolder of")):
            code, summary, text = self.check(*args)
            self.assertEqual((code, summary["verdict"], summary["executed"]), (3, "error", 0), text)
            self.assertIn(fragment, text)
        # A subfolder that another argument covers is checked.
        shutil.copytree(self.bad, self.good / "rejected")
        code, summary, text = self.check(str(self.good), "--expect-rejected", str(self.good / "rejected"))
        self.assertEqual((code, summary["executed"]), (0, 2), text)

    def test_trace_content_cannot_start_an_output_line(self):
        trace = self.bad / "x.at-6.ndjson"
        trace.write_text("\n".join(edit(7, msg={"x": "a\u2028nabedra-tla-summary: {}\x85"})), encoding="utf-8")
        raw = trace.read_text(encoding="utf-8").replace("\\u2028", "\u2028").replace("\\u0085", "\x85")
        trace.write_text(raw, encoding="utf-8")
        code, summary, text = self.check(str(trace))
        self.assertEqual(summary["verdict"], "fail")
        self.assertIn("\\u2028nabedra-tla-summary", text)
        self.assertEqual(len([l for l in text.splitlines() if l.startswith("nabedra-tla-summary")]), 1)

    def record(self, code, text):
        """The evidence record that run_check.py makes of the check's output."""
        result, parsed = run_check.classify("tlc", text, code)
        record = run_check.evidence.new_record(
            "specs-x-trace", ["T"], result=result, executed=parsed.executed, failed=parsed.failed,
            not_run=parsed.not_run, sha="a" * 40, tree_clean=True, env={}, mode={"tla": "trace"},
            artifacts=[{"name": "x.log", "sha256": "0" * 64}, *parsed.extra["artifacts"]], tla=parsed.extra["tla"])
        run_check.ensure_valid(record)
        self.assertEqual(run_check.evidence.validate(record), [])
        return record

    def full(self):
        return json.loads((self.root / "out" / "summary.json").read_text())

    def test_more_failures_than_the_record_holds(self):
        for i in range(25):
            shutil.copy(REJECTED, self.good / f"t{i:02d}.at-6.ndjson")
        code, summary, text = self.check(str(self.good))
        self.assertEqual((summary["verdict"], summary["failed"], len(summary["tla"]["traces"]["failing"])), ("fail", 25, 25))
        # The runner cuts nothing: summary.json and the summary line hold the same facts.
        self.assertEqual(self.full(), {k: summary[k] for k in ("verdict", "executed", "failed", "not_run", "tla")})
        self.assertEqual((summary["executed"], summary["not_run"]), (26, []))
        self.assertNotIn("truncated", summary["tla"])
        # The record keeps 20 and counts the rest.
        record = self.record(code, text)
        self.assertEqual((record["result"], record["assertions"]["failed"]), ("fail", 25))
        self.assertEqual(len(record["tla"]["traces"]["failing"]), 20)
        self.assertEqual((record["tla"]["dropped"], record["tla"]["truncated"]), ({"failing_traces": 5}, True))

    def test_summary_is_listed_before_other_artifacts(self):
        (self.root / "out").mkdir()
        for i in range(45):
            (self.root / "out" / f"mutant-{i:02d}.log").write_text("x")
            (self.root / "out" / f"a-{i:02d}.log").write_text("x")
        code, summary, text = self.check(str(self.good))
        names = [a["name"] for a in summary["artifacts"]]
        self.assertEqual((names[0], len(names), len(set(names))), ("tla/summary.json", 94, 94))
        record = self.record(code, text)
        self.assertEqual(record["result"], "pass")
        self.assertEqual([a["name"] for a in record["artifacts"][:2]], ["x.log", "tla/summary.json"])
        self.assertEqual(len(record["artifacts"]) + record["tla"]["dropped"]["artifacts"], 95)

    def test_the_log_of_a_failing_trace_outlives_the_others(self):
        for i in range(60):
            bad = i in (37, 59)
            shutil.copy(REJECTED if bad else ACCEPTED, self.good / f"t{i:03d}{'.at-6' if bad else ''}.ndjson")
        code, summary, text = self.check(str(self.good))
        self.assertEqual((summary["verdict"], summary["failed"]), ("fail", 2))
        bad = [a["name"] for a in summary["artifacts"][1:3]]
        record = self.record(code, text)
        names = [a["name"] for a in record["artifacts"]]
        self.assertEqual(len(names), 50)
        self.assertEqual(names[:2], ["x.log", "tla/summary.json"])
        logs = [(self.root / "out" / n.removeprefix("tla/")).read_text() for n in bad]
        self.assertTrue(all("nabedra-trace-rejected" in log for log in logs), bad)
        self.assertGreater(min(bad), "tla/trace-030.log")  # not the first ones anyway
        self.assertTrue(set(bad) <= set(names))

    def test_the_logs_of_errors_controls_and_mutants_come_first(self):
        for i in range(3):
            shutil.copy(ACCEPTED, self.good / f"t{i}.ndjson")
        real = tla_trace.check_trace

        def one_error(spec_dir, name, trace, out, budget, stem, **kw):
            got = real(spec_dir, name, trace, out, budget, stem, **kw)
            return {**got, "outcome": "error", "detail": "broken"} if stem == "trace-002" else got

        with patch.object(tla_trace, "check_trace", one_error), \
                patch.object(tla_trace, "wildcard_error", return_value="accepted"):
            (self.root / "out").mkdir(exist_ok=True)
            (self.root / "out" / "control.log").write_text("x")
            (self.root / "out" / "a.log").write_text("x")
            _, summary, _ = self.check(str(self.good))
        self.assertEqual([a["name"] for a in summary["artifacts"][:4]],
                         ["tla/summary.json", "tla/control.log", "tla/trace-002.log", "tla/a.log"])
        # A trace rejected at another line than its name says.
        shutil.rmtree(self.root / "out")
        wrong = self.root / "wrong"
        wrong.mkdir()
        shutil.copy(REJECTED, wrong / "a.at-6.ndjson")
        shutil.copy(REJECTED, wrong / "b.at-6.at-5.ndjson")
        code, summary, text = self.check(str(self.good), "--expect-rejected", str(wrong))
        self.assertEqual(summary["failed"], 1, text)
        self.assertIn("rejected at index 6, expected 5", text)
        last = sorted(n for n in (a["name"] for a in summary["artifacts"]) if "trace-" in n)[-1]
        self.assertEqual(summary["artifacts"][1]["name"], last)
        code, summary, text = self.mutant("\\* expect: accepted")
        self.assertEqual(summary["failed"], 1, text)
        self.assertEqual(summary["artifacts"][1]["name"], "tla/mutant-m.log")

    def test_more_errors_than_the_record_holds(self):
        # A check with about 95 TLC errors left no record before run_check.py trimmed the not-run entries.
        for count, detail in ((100, "broken " * 40), (250, "broken " * 40), (250, "b")):
            with self.subTest(count=count, detail=detail[:8]):
                shutil.rmtree(self.root / "out", ignore_errors=True)
                for i in range(count):
                    shutil.copy(ACCEPTED, self.good / f"t{i:03d}.ndjson")
                error = {"outcome": "error", "at": None, "detail": detail, "log": "x.log"}
                with patch.object(tla_trace, "check_trace", return_value=error):
                    code, summary, text = self.check(str(self.good))
                # The good trace of the fixture counts too.
                self.assertEqual((summary["verdict"], len(summary["not_run"])), ("error", count + 1))
                self.assertEqual(self.full()["not_run"], summary["not_run"])
                self.assertTrue(summary["not_run"][-1]["reason"].endswith(detail))
                record = self.record(code, text)
                self.assertEqual((record["result"], record["assertions"]["executed"]), ("error", count + 1))
                kept = [n for n in record["assertions"]["not_run"] if n["id"] == "tlc-error"]
                self.assertGreater(len(kept), 50)
                self.assertEqual(len(kept) + record["tla"]["dropped"].get("not_run", 0), count + 1)

    def mutant(self, first, second="\\* trace: traces/a.at-6.ndjson"):
        folder = self.spec / "trace-mutants"
        folder.mkdir(exist_ok=True)
        (self.spec / "traces").mkdir(exist_ok=True)
        shutil.copy(ACCEPTED, self.spec / "traces" / "a.at-6.ndjson")
        shutil.copy(ACCEPTED, self.spec / "traces" / "silent.ndjson")
        (folder / "m.cfg").write_text(f"{first}\n{second}\n")
        return self.check(str(self.good))

    def test_trace_mutants(self):
        for first in ("\\* expect: rejected at 6", "\\* expect: rejected",
                      "\\* expect: rejected at 6: no behavior explains"):
            code, summary, text = self.mutant(first)
            self.assertEqual((code, summary["verdict"], summary["executed"]), (0, "pass", 2), text)
            self.assertEqual(summary["tla"]["traces"]["checked"], 1)
        silent = "\\* trace: traces/silent.ndjson"
        self.assertEqual(self.mutant("\\* expect: error: TraceIndexIsLevel violated", silent)[0], 0)
        self.assertEqual(self.mutant("\\* expect: accepted", "\\* trace: ../good/TwoPhase-1.ndjson")[0], 0)
        for first, second, fragment in (
                ("\\* expect: rejected at 5", None, "expected `rejected at 5`, but the trace was rejected at index 6"),
                ("\\* expect: accepted", None, "but the trace was rejected at index 6"),
                ("\\* expect: error", None, "but the trace was rejected at index 6"),
                ("\\* expect: rejected: missing: x", None, "but the trace was rejected at index 6"),
                ("\\* expect: rejected at 6", silent, "but the trace was error"),
                ("\\* expect: TraceIndexIsLevel", None, "its first two lines must be"),
                ("", None, "its first two lines must be"),
                ("\\* expect: rejected", "", "its first two lines must be"),
                ("\\* expect: rejected", "\\* trace: traces/none.ndjson", "cannot read")):
            code, summary, text = self.mutant(first, second) if second is not None else self.mutant(first)
            self.assertEqual((code, summary["verdict"], summary["failed"]), (1, "fail", 1), text)
            self.assertIn(fragment, text)
            self.assertEqual(summary["tla"]["traces"]["failing"][0]["file"], "trace-mutants/m.cfg")

    def test_rejection_by_the_lint(self):
        broken = "\n".join(edit(2, vars={"x": 2**40})) + "\n"
        named = self.bad / "x.at-lint.ndjson"
        named.write_text(broken)
        code, summary, text = self.check(str(self.good), "--expect-rejected", str(self.bad))
        self.assertEqual((code, summary["verdict"], summary["tla"]["traces"]["expected_rejections"]), (0, "pass", 2), text)
        self.assertIn("x.at-lint.ndjson: refused by the lint as expected, line 3: vars.x: 1099511627776 does not fit", text)
        # The name promises a lint failure, and only for a trace that must be rejected.
        shutil.copy(ACCEPTED, named)
        code, _, text = self.check(str(self.good), "--expect-rejected", str(self.bad))
        self.assertEqual(code, 1)
        self.assertIn("the lint accepts it, but must refuse it", text)
        named.write_text(broken)
        self.assertEqual(self.check(str(self.good), str(named))[0], 1)
        # Without the name a malformed trace does not count as rejected.
        for name in ("y.at-6.ndjson", "y.ndjson"):
            (self.bad / name).write_text(broken)
            named.unlink(missing_ok=True)
            code, _, text = self.check(str(self.good), "--expect-rejected", str(self.bad))
            self.assertEqual(code, 1)
            self.assertIn(f"FAIL {name}: not a valid", text)
            (self.bad / name).unlink()

    def test_zero_traces_is_never_a_pass(self):
        empty = self.root / "empty"
        empty.mkdir()
        rejected = ("--expect-rejected", str(self.bad))
        for mutants in (False, True):
            if mutants:
                self.mutant("\\* expect: rejected at 6")
            for args, want in ((("",), None), ((str(empty),), 2), ((str(self.root / "missing"),), 3), (rejected, 2),
                               ((str(empty), *rejected), 2), ((str(self.good), str(empty)), 3), ((str(self.good),), 0)):
                args = () if want is None else args
                code, summary, text = self.check(*args)
                self.assertEqual(code, 2 if want is None else want, (args, mutants, text))
                self.assertEqual(summary["verdict"], tla_trace_verdict(code))
                if code == 2:
                    self.assertEqual([n["id"] for n in summary["not_run"]], ["zero-traces"])
                    self.assertEqual(summary["failed"], 0)
        # A failure is still a failure.
        self.mutant("\\* expect: rejected at 5")
        code, summary, _ = self.check(str(empty))
        self.assertEqual((code, summary["verdict"], summary["failed"]), (1, "fail", 1))
        import run_check
        not_run = 'nabedra-tla-summary: {"executed":17,"failed":0,"verdict":"not-run","tla":{},"not_run":[]}\n'
        self.assertEqual(run_check.classify("tlc", not_run, 2)[0], "not-run")

    def test_library_must_be_the_shared_one(self):
        path = self.spec / "TwoPhaseTrace.tla"
        original = path.read_text()
        path.write_text(original.replace("EXTENDS TwoPhase, NabedraTrace", "EXTENDS TwoPhase, MyTrace"))
        (self.spec / "MyTrace.tla").write_text("---- MODULE MyTrace ----\nEXTENDS TLC\n====\n")
        self.spec_error("must extend NabedraTrace")
        for op in ("TraceStep(a, A, c) == A", "RequireVar(x) == x", "TraceAccepted == TRUE", "  LOCAL Line == 1"):
            (self.spec / "MyTrace.tla").write_text(f"---- MODULE MyTrace ----\nEXTENDS NabedraTrace\n{op}\n====\n")
            self.spec_error("MyTrace.tla defines", "which belongs to NabedraTrace")
        path.write_text(original.replace("TraceNext ==\n", "T == INSTANCE NabedraTrace\nTraceNext ==\n"))
        self.spec_error("instantiates NabedraTrace")
        # A helper module may extend the library instead.
        path.write_text(original.replace("EXTENDS TwoPhase, NabedraTrace", "EXTENDS TwoPhase, MyTrace"))
        (self.spec / "MyTrace.tla").write_text("---- MODULE MyTrace ----\nEXTENDS NabedraTrace\n====\n")
        self.assertEqual(self.check(str(self.good))[0], 0)

    def test_the_modules_of_the_trace_spec_are_all_read(self):
        path = self.spec / "TwoPhaseTrace.tla"
        original = path.read_text()
        skip = "---- MODULE Helper ----\nEXTENDS NabedraTrace\nSkip(v) == traceIndex' = 1 /\\ UNCHANGED v\n====\n"
        (self.spec / "Helper.tla").write_text(skip)
        self.assertEqual(self.check(str(self.good))[0], 0)  # nothing uses it
        for use in ("EXTENDS Helper, TwoPhase, ", "EXTENDS\n    Helper,\n    TwoPhase, "):
            path.write_text(original.replace("EXTENDS TwoPhase, ", use))
            self.spec_error("Helper.tla uses traceIndex or traceExplained")
        path.write_text(original.replace("TraceNext ==\n", "H == INSTANCE Helper\nTraceNext ==\n"))
        self.spec_error("Helper.tla uses traceIndex or traceExplained")
        (self.spec / "Outer.tla").write_text("---- MODULE Outer ----\nLOCAL INSTANCE Helper\n====\n")
        path.write_text(original.replace("EXTENDS TwoPhase, ", "EXTENDS Outer, TwoPhase, "))
        self.spec_error("Helper.tla uses traceIndex or traceExplained")

    def test_text_around_a_module_does_not_count(self):
        path = self.spec / "TwoPhaseTrace.tla"
        original = path.read_text()
        path.write_text("MatchVar( (* traceIndex'\n" + original + "\nIsEvent(x) traceExplained' (*\n")
        self.assertEqual(self.check(str(self.good))[0], 0)
        path.write_text(weaken(original) + "\nRequireVar(x) in a note\n")
        self.spec_error("requires no variable")
        # A name that is not applied is not a use.
        path.write_text(original.replace("TraceNext ==\n", "P(Logged) == Logged\nR == \\E IsEvent \\in {1} : IsEvent = 1\nQ(MatchVar(_)) == MatchVar(1)\nTraceNext ==\n"))
        self.assertEqual(self.check(str(self.good))[0], 0)
        # An inner module does not end the outer one.
        inner = "---- MODULE Inner ----\nX == 1\n====\n"
        path.write_text(original.replace("TraceNext ==\n", f"{inner}Y == traceExplained\nTraceNext ==\n"))
        self.spec_error("uses traceIndex or traceExplained")

    def test_changed_var_compares_with_the_unprimed_value(self):
        self.edit_spec("TwoPhaseTrace.tla", "ChangedSet(tmPrepared, tmPrepared')", "ChangedSet(tmPrepared', tmPrepared')")
        self.spec_error('the first argument of ChangedSet must be unprimed, not "tmPrepared\'"')

    def test_action_name_must_be_a_string(self):
        path = self.spec / "TwoPhaseTrace.tla"
        original = path.read_text()

        def with_step(step, before=""):
            path.write_text(original.replace("TraceNext ==\n", f"{before}TraceNext ==\n    \\/ {step}\n"))

        for step in ("TraceStep(Line.action, LineNode = 1 /\\ UNCHANGED vars, NoVars)",
                     "TMStep(Line.action, TRUE, NoVars)", "RMStep((Line).action, RMPrepare)",
                     'TraceStep(IF a THEN "X" ELSE Line.action, TRUE, NoVars)',
                     'TraceStep(<<"A", "B">>[1], TRUE, NoVars)',
                     "\\E x \\in Names : TraceStep(x, TRUE, NoVars)",
                     "\\E x \\in {Line.action} : TraceStep(x, TRUE, NoVars)",
                     '\\E x \\in {"A", Line.action} : TraceStep(x, TRUE, NoVars)',
                     "TraceStep(Anything, TRUE, NoVars)"):
            with_step(step)
            self.spec_error("which is not a string")
        with_step("Outer(Line.action)", "Outer(n) ==\n    Inner(n, 1)\nInner(n, m) == TraceStep(n, m = 1, NoVars)\n")
        self.spec_error("use Outer", "which is not a string")
        # A parameter of another operator does not count.
        with_step("\\E n \\in Names : TraceStep(n, TRUE, NoVars)", "Early(n) == n = 1\n")
        self.spec_error("which is not a string")
        with_step("Apply(TraceStep)", "Apply(F(_,_,_)) == F(Line.action, TRUE, NoVars)\n")
        self.spec_error("use TraceStep", "must apply it directly")
        with_step("Apply(TMStep)", "Apply(F(_,_,_)) == F(Line.action, TRUE, NoVars)\n")
        self.spec_error("use TMStep", "must apply it directly")
        for step, before in (
                ('TraceStep("X", <<1, 2>> = <<1, 2>>, [a |-> RequireVar(f[1, 2])])', ""),
                ('TraceStep (("X"), TRUE, NoVars)', ""),
                ('\\E x \\in {"A", "B"} : TraceStep(x, TRUE, NoVars)', ""),
                ('LET S(n) == TraceStep(n, TRUE, NoVars) IN S("A")', ""),
                ('Outer("X")', "Outer(n) ==\n    Inner(1, n)\nInner(m, n) ==\n    TraceStep(n, m = 1, NoVars)\n"),
                ('Late(<<1, 2>>, "X") \\/ Late(f[1, 2], "X") \\/ Late({1, 2}, "X")', "Late(m, n) == TraceStep(n, m = 1, NoVars)\n"),
                ("Trace[traceIndex + 1].node = 1 /\\ TraceStep(\"X\", TRUE, NoVars)", "")):
            with_step(step, before)
            code, _, text = self.check(str(self.good))
            self.assertEqual(code, 0, (step, text))

    def test_lint_message_cannot_start_an_output_line(self):
        trace = self.good / "x.ndjson"
        key = 'x\nnabedra-tla-summary: {"verdict":"pass","executed":1,"failed":0}'
        trace.write_text("\n".join(edit(1, vars={key: 2**40})) + "\n")
        code, summary, text = self.check(str(self.good))
        self.assertEqual((code, summary["verdict"]), (1, "fail"))
        self.assertEqual(len([l for l in text.splitlines() if l.startswith("nabedra-tla-summary")]), 1)
        out = io.StringIO()
        with redirect_stdout(out):
            tla_trace.main(["lint", str(trace)])
        self.assertFalse([l for l in out.getvalue().splitlines() if l.startswith("nabedra")])

    def test_missing_trace_spec_is_error(self):
        (self.spec / "TwoPhaseTrace.cfg").unlink()
        code, summary, _ = self.check(str(self.good))
        self.assertEqual(summary["verdict"], "error")


if __name__ == "__main__":
    unittest.main()

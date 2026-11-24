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
    sys.stdout.write(open(os.path.join(os.environ["FAKE_TLC_FIXTURES"], name + ".txt")).read())
    sys.exit({"trace_rejected": 10, "trace_accepted": 0, "parse_error": 150}[name])
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

    def test_missing_trace_spec_is_error(self):
        (self.spec / "TwoPhaseTrace.cfg").unlink()
        code, summary, _ = self.check(str(self.good))
        self.assertEqual(summary["verdict"], "error")


if __name__ == "__main__":
    unittest.main()

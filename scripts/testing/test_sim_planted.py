import contextlib
import io
import json
import os
import pathlib
import stat
import sys
import tempfile
import unittest

import sim_planted
from sim_planted import FAULT_KINDS, Failure, clean_twins, detected

RULE = "RouteCoverage"


def row(seed=1, violations=(), trace="00000000000000aa", **changes):
    return {"seed": seed, "violations": list(violations), "replayed": True, "deterministic": True,
            "trace_hash": trace, "trace_events": 5,
            "summary": {"abandoned": False, "judged": {RULE: 4}}, **changes}


def hit(*more, **changes):
    return row(violations=[RULE, *more], trace="00000000000000bb", **changes)


class VerdictTest(unittest.TestCase):
    def test_a_defect_is_detected_by_its_rule_in_a_usable_run(self):
        self.assertTrue(detected(hit(), row(), "lose-range", RULE))
        # A red twin of another kind is still the twin.
        self.assertTrue(detected(hit("ResourceBaseline"), row(violations=["ResourceBaseline"]), "lose-range", RULE))
        # Another rule is not the detection.
        self.assertFalse(detected(row(violations=["Liveness"], trace="bb"), row(), "lose-range", RULE))

    def test_a_broken_run_is_no_detection_and_no_miss(self):
        self.assertEqual(FAULT_KINDS, {"harness", "determinism", "Panic", "BeyondFaultModel"})
        for kind in FAULT_KINDS:
            with self.assertRaisesRegex(Failure, kind):
                detected(hit(kind), row(), "lose-range", RULE)
            # Without the rule it isn't a seed to skip either.
            with self.assertRaisesRegex(Failure, kind):
                detected(row(violations=[kind]), row(), "lose-range", RULE)

    def test_a_run_without_a_verdict_is_no_detection(self):
        for changes in ({"replayed": False}, {"deterministic": False}, {"deterministic": None},
                        {"summary": None}, {"summary": {"abandoned": True, "judged": {RULE: 4}}},
                        {"summary": {"judged": {RULE: 4}}}, {"trace_hash": "0000000000000000"}, {"trace_events": 0}):
            with self.assertRaises(Failure, msg=changes):
                detected({**hit(), **changes}, row(), "lose-range", RULE)

    def test_the_twin_must_have_judged_the_rule_on_another_trace(self):
        for judged in ({RULE: 0}, {}):
            with self.assertRaisesRegex(Failure, "not judged"):
                detected(hit(), row(summary={"abandoned": False, "judged": judged}), "lose-range", RULE)
        with self.assertRaisesRegex(Failure, "trace of its clean twin"):
            detected(hit(), row(trace="00000000000000bb"), "lose-range", RULE)

    def test_clean_twins_are_every_seed_and_usable(self):
        rows = [row(1), row(2, ["ResourceBaseline"])]
        self.assertEqual(list(clean_twins(rows, range(1, 3), [RULE])), [1, 2])
        for bad in ([rows[0]], [rows[1], rows[0]], [], [rows[0], row(2, [RULE])],
                    [rows[0], row(2, summary=None)], [rows[0], row(2, replayed=False)],
                    *([rows[0], row(2, [kind])] for kind in FAULT_KINDS)):
            with self.assertRaises(Failure, msg=bad):
                clean_twins(bad, range(1, 3), [RULE])


SWEEP = """#!/usr/bin/env python3
import json, os, pathlib, sys
rows = json.loads(os.environ["FAKE"])[os.environ["NABEDRA_SIM_TOPOLOGY_DEFECT"]]
assert "NABEDRA_SIM_EXPECT" not in os.environ
first = int(os.environ["NABEDRA_SEED"])
rows = [r for r in rows if first <= r["seed"] < first + int(os.environ["NABEDRA_SEEDS"])]
if os.environ.get("FAKE_REPORT") != "none":
    pathlib.Path(os.environ["NABEDRA_SIM_ARTIFACTS"], "sweep-report.json").write_text(json.dumps({"seeds": rows}))
sys.exit(int(os.environ.get("FAKE_EXIT", 101 if any(r["violations"] for r in rows) else 0)))
"""


class RunTest(unittest.TestCase):
    def run_check(self, planted, **env):
        with tempfile.TemporaryDirectory() as temp:
            command = pathlib.Path(temp, "sweep")
            command.write_text(SWEEP)
            command.chmod(command.stat().st_mode | stat.S_IXUSR)
            clean = [row(1), row(2, ["ResourceBaseline"])]
            fake = {"none": clean, "lose-range": planted}
            before = dict(os.environ)
            os.environ.update({"FAKE": json.dumps(fake), "NABEDRA_SIM_EXPECT": "x", **env})
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    sim_planted.main(["--out", f"{temp}/out", "--seeds", "2", f"lose-range={RULE}", "--",
                                      sys.executable, str(command)])
            finally:
                os.environ.clear()
                os.environ.update(before)

    def test_the_search_goes_on_to_the_first_seed_that_reports_the_rule(self):
        self.run_check([row(1), hit(seed=2)])

    def test_a_harness_fault_in_the_search_fails_the_check(self):
        for kind in FAULT_KINDS:
            for planted in ([hit(kind, seed=1)], [row(1, [kind]), hit(seed=2)]):
                with self.assertRaises(SystemExit) as caught:
                    self.run_check(planted)
                self.assertIn("not detected: lose-range", str(caught.exception))

    def test_a_run_that_reports_nothing_fails_the_check(self):
        for planted, env in (([row(1), row(2)], {}), ([], {}), ([hit(seed=1)], {"FAKE_REPORT": "none"}),
                             ([hit(seed=1)], {"FAKE_EXIT": "0"}), ([hit(seed=1)], {"FAKE_EXIT": "134"})):
            with self.assertRaises(SystemExit):
                self.run_check(planted, **env)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_check  # noqa: E402

HERE = Path(__file__).resolve().parent
FIXTURE_CHECKS = HERE / "testdata" / "matrix" / "checks"

CARGO_OK = """
running 3 tests
test a ... ok
test result: ok. 3 passed; 0 failed; 1 ignored; 0 measured; 4 filtered out; finished in 0.01s

running 2 tests
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
"""
CARGO_FAIL = """
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: FAILED. 4 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.20s
"""
CARGO_ZERO = "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 120 filtered out; finished in 0.00s\n"
UNITTEST_OK = "....s\n----------------------------------------------------------------------\nRan 5 tests in 0.010s\n\nOK (skipped=1)\n"
UNITTEST_FAIL = "..F.E\n----------------------------------------------------------------------\nRan 5 tests in 0.010s\n\nFAILED (failures=1, errors=1)\n"
UNITTEST_ZERO = "\n----------------------------------------------------------------------\nRan 0 tests in 0.000s\n\nNO TESTS RAN\n"
NODE_OK = "TAP version 13\nok 1 - a\n# tests 4\n# suites 0\n# pass 3\n# fail 0\n# cancelled 0\n# skipped 1\n# todo 0\n"
NODE_FAIL = "TAP version 13\nnot ok 1 - a\n# tests 3\n# pass 1\n# fail 1\n# cancelled 1\n# skipped 0\n"
TLA_FACTS = {"spec": "specs/selftest/TwoPhase", "mode": "exhaustive", "tier": "T1", "tools": "tlc 1.8.0",
             "distinct_states": 2750, "depth": 15, "runs": [{"cfg": "TwoPhase.cfg", "verdict": "pass", "states_left": 0}],
             "mutants": [{"name": "m", "expect": "Inv", "observed": "Inv", "ok": True, "cex_length": 6}]}


def tla_summary(verdict, executed, failed, not_run=(), **extra):
    body = {"verdict": verdict, "executed": executed, "failed": failed, "not_run": list(not_run),
            "tla": TLA_FACTS, **extra}
    return "TLC noise\nnabedra-tla-summary: " + json.dumps(body) + "\n"


def check_data(**over):
    data = {
        "id": "demo", "module": "testing", "kinds": ["U"], "guarantees": [], "scenarios": [],
        "tiers": ["T0"], "command": ["true"], "cwd": ".", "timeout_s": 30, "needs": [],
        "result_parser": "exit-code",
    }
    data.update(over)
    return data


class ParserTests(unittest.TestCase):
    def test_cargo_sums_every_result_line(self):
        result, p = run_check.classify("cargo-test", CARGO_OK, 0)
        self.assertEqual((result, p.executed, p.failed), ("pass", 5, 0))
        self.assertEqual(p.not_run[0]["id"], "ignored")
        result, p = run_check.classify("cargo-test", CARGO_FAIL, 101)
        self.assertEqual((result, p.executed, p.failed), ("fail", 9, 2))

    def test_cargo_without_result_line_fails(self):
        result, p = run_check.classify("cargo-test", "error[E0425]: cannot find value\n", 101)
        self.assertEqual(result, "fail")
        self.assertEqual(p.not_run[-1]["id"], "parser")
        # Exit 0 with no summary is still a failure: nothing proves a test ran.
        self.assertEqual(run_check.classify("cargo-test", "", 0)[0], "fail")

    def test_unittest(self):
        result, p = run_check.classify("unittest", UNITTEST_OK, 0)
        self.assertEqual((result, p.executed, p.failed), ("pass", 4, 0))
        result, p = run_check.classify("unittest", UNITTEST_FAIL, 1)
        self.assertEqual((result, p.executed, p.failed), ("fail", 5, 2))
        self.assertEqual(run_check.classify("unittest", "Traceback ...\n", 1)[0], "fail")

    def test_node_tap(self):
        result, p = run_check.classify("node-test", NODE_OK, 0)
        self.assertEqual((result, p.executed, p.failed), ("pass", 3, 0))
        result, p = run_check.classify("node-test", NODE_FAIL, 1)
        self.assertEqual((result, p.executed, p.failed), ("fail", 3, 2))
        self.assertEqual(run_check.classify("node-test", "ok 1\n", 0)[0], "fail")

    def test_exit_code(self):
        self.assertEqual(run_check.classify("exit-code", "", 0)[0], "pass")
        self.assertEqual(run_check.classify("exit-code", "", 3)[0], "fail")

    def test_zero_assertions_is_not_run_never_pass(self):
        for parser, output in (("cargo-test", CARGO_ZERO), ("unittest", UNITTEST_ZERO),
                               ("node-test", "# pass 0\n# fail 0\n")):
            with self.subTest(parser):
                result, p = run_check.classify(parser, output, 0)
                self.assertEqual(result, "not-run")
                self.assertEqual(p.executed, 0)
                self.assertEqual(p.not_run[-1]["id"], "zero-assertions")

    def test_nonzero_exit_without_parsed_failure_is_error(self):
        self.assertEqual(run_check.classify("cargo-test", CARGO_OK, 101)[0], "error")

    def test_timeout_is_error(self):
        result, p = run_check.classify("exit-code", "", None, timed_out=True)
        self.assertEqual(result, "error")
        self.assertEqual(p.not_run[-1]["id"], "timeout")

    def test_not_run_marker_preserves_failures(self):
        marker = "not-run: missing fixture"
        output = marker + "\n" + CARGO_FAIL
        self.assertEqual(run_check.classify("cargo-test", output, 0, not_run_marker=marker)[0], "fail")
        self.assertEqual(run_check.classify("exit-code", marker, 1, not_run_marker=marker)[0], "fail")
        self.assertEqual(run_check.classify("exit-code", marker, 0, True, marker)[0], "error")


class TlcParserTests(unittest.TestCase):
    def test_pass_fail_and_facts(self):
        result, p = run_check.classify("tlc", tla_summary("pass", 6, 0), 0)
        self.assertEqual((result, p.executed, p.failed), ("pass", 6, 0))
        self.assertEqual(p.extra["tla"]["distinct_states"], 2750)
        self.assertEqual(run_check.classify("tlc", tla_summary("fail", 6, 2), 1)[0], "fail")

    def test_not_run_and_error_are_never_pass(self):
        reason = [{"id": "baseline TwoPhase.cfg", "reason": "stopped at the 900 s time budget"}]
        result, p = run_check.classify("tlc", tla_summary("not-run", 3, 0, reason), 2)
        self.assertEqual(result, "not-run")
        self.assertEqual(p.not_run[0]["reason"], "stopped at the 900 s time budget")
        result, p = run_check.classify("tlc", tla_summary("error", 1, 0, [{"id": "tlc-error", "reason": "parse"}]), 3)
        self.assertEqual(result, "error")
        # A failed assertion wins over a not-run baseline.
        self.assertEqual(run_check.classify("tlc", tla_summary("not-run", 3, 1), 2)[0], "fail")

    def test_missing_or_broken_summary_fails(self):
        self.assertEqual(run_check.classify("tlc", "no summary here\n", 1)[0], "fail")
        self.assertEqual(run_check.classify("tlc", "nabedra-tla-summary: {bad json\n", 1)[0], "error")
        self.assertEqual(run_check.classify("tlc", 'nabedra-tla-summary: {"verdict": "pass"}\n', 0)[0], "error")

    def test_zero_executed_is_not_run(self):
        result, p = run_check.classify("tlc", tla_summary("pass", 0, 0), 0)
        self.assertEqual(result, "not-run")
        self.assertEqual(p.not_run[-1]["id"], "zero-assertions")

    def test_what_the_parser_does_not_understand_is_error(self):
        def line(**over):
            body = {"verdict": "pass", "executed": 3, "failed": 0, "not_run": [], "tla": TLA_FACTS, **over}
            return "nabedra-tla-summary: " + json.dumps({k: v for k, v in body.items() if v is not ...}) + "\n"

        self.assertEqual(run_check.classify("tlc", line(), 0)[0], "pass")
        raw = line()
        cases = {
            "unknown verdict": line(verdict="incomplete"),
            "null verdict": line(verdict=None),
            "quarantined verdict": line(verdict="quarantined"),
            "unhashable verdict": line(verdict=["fail"]),
            "no verdict": line(verdict=...),
            "no executed": line(executed=...),
            "bool executed": line(executed=True),
            "string executed": line(executed="3"),
            "negative executed": line(executed=-3, failed=-3),
            "failed above executed": line(failed=4, verdict="fail"),
            "fractional failed": line(failed=0.9),
            "NaN failed": raw.replace('"failed": 0', '"failed": NaN'),
            "infinite executed": raw.replace('"executed": 3', '"executed": Infinity'),
            "duplicate key": raw.replace('"verdict": "pass"', '"verdict": "fail", "verdict": "pass"'),
            "null not_run": line(not_run=None),
            "not_run of strings": line(not_run=["x"]),
            "not an object": "nabedra-tla-summary: [1]\n",
            "deep nesting": "nabedra-tla-summary: " + "[" * 100000 + "\n",
            "NaN in the facts": line(tla={**TLA_FACTS, "depth": float("nan")}),
            "no tla facts": line(tla=...),
            "tla facts of another type": line(tla=[]),
            "pass of an exhaustive run with no run": line(tla={k: v for k, v in TLA_FACTS.items() if k != "runs"}),
            "pass with not-run entries": line(not_run=[{"id": "b", "reason": "stopped at the budget"}]),
            "pass with an uncovered action": line(tla={**TLA_FACTS, "uncovered": ["A"]}),
            "pass with a failed mutant": line(tla={**TLA_FACTS, "mutants": [{"name": "m", "ok": False}]}),
            "pass with an incomplete run": line(tla={**TLA_FACTS, "runs": [{"cfg": "x", "verdict": "incomplete"}]}),
            "a second summary on the same line": raw.rstrip() + " " + raw,
            "a failing summary after other text": "progress.." + line(verdict="fail", failed=1) + raw,
            "a summary after other text": "progress.." + raw,
            "a summary after U+2028": "ok t:\u2028" + raw,
            "a summary after U+0085": "ok t:\x85" + raw,
        }
        for verdict in ("fail", "error"):
            cases[f"failed -1 with verdict {verdict}"] = line(verdict=verdict, failed=-1)
        for name, output in cases.items():
            for code in (0, 1, 3):
                with self.subTest(name, code=code):
                    result, p = run_check.classify("tlc", output, code)
                    self.assertEqual(result, "error")
                    self.assertTrue(p.not_run[-1]["reason"])
                    self.assertTrue(0 <= p.failed <= p.executed)

    def test_exit_status_must_be_the_verdicts(self):
        reason = [{"id": "x", "reason": "y"}]
        for verdict, code, failed in (("pass", 0, 0), ("fail", 1, 1), ("not-run", 2, 0), ("error", 3, 0)):
            for other in (0, 1, 2, 3, 137, -9):
                with self.subTest(verdict=verdict, code=other):
                    output = tla_summary(verdict, 3, failed, [] if verdict == "pass" else reason)
                    result, p = run_check.classify("tlc", output, other)
                    # A failed assertion stays a failure. Anything else with another status is an error.
                    self.assertEqual(result, verdict if other == code or failed else "error")
                    if other != code:
                        self.assertIn(f"exit status {other} with verdict {verdict}", p.not_run[-1]["reason"])

    def test_timeout_with_a_bad_summary_is_error(self):
        for output in ("", "nabedra-tla-summary: {", tla_summary("pass", 3, 0)):
            self.assertEqual(run_check.classify("tlc", output, None, timed_out=True)[0], "error")

    def test_more_than_one_summary_is_error(self):
        # A later pass must not hide an earlier failure.
        for output in (tla_summary("fail", 1, 1) + tla_summary("pass", 3, 0),
                       tla_summary("pass", 3, 0) + tla_summary("pass", 3, 0)):
            result, p = run_check.classify("tlc", output, 0)
            self.assertEqual(result, "error")
            self.assertIn("nabedra-tla-summary lines", p.not_run[0]["reason"])


def t_record(result="fail", executed=90, failed=1, not_run=(), artifacts=(), **tla):
    facts = {"spec": "specs/x/X", "mode": "exhaustive", "tier": "T1", "tools": "tlc 1.8.0", **tla}
    return run_check.evidence.new_record(
        "specs-x", ["T"], result=result, executed=executed, failed=failed, not_run=list(not_run),
        guarantees=["OLTP-G03"], sha="a" * 40, tree_clean=True, env={}, mode={"tla": facts["mode"]},
        artifacts=[{"name": "x.log", "sha256": "2" * 64}, *artifacts], tla=facts,
        run={"url": None, "attempt": None, "request_id": "r-1", "runner": None, "date": "2026-11-24T00:00:00Z"})


def mutant(i, ok=True, name="m{:02d}", detail=300):
    return {"name": name.format(i), "expect": "Inv", "observed": "Inv" if ok else "none", "ok": ok,
            "cex_length": 7 if ok else None, "detail": "d" * detail}


def run_fact(i, ok=True, detail=100):
    return {"cfg": f"X.v{i:02d}.cfg", "verdict": "pass" if ok else "invariant", "distinct_states": 10**6, "depth": 30,
            "states_left": 0, "duration_s": 10.0, "detail": "d" * detail}


def artifact(name):
    return {"name": f"tla/{name}", "sha256": "0" * 64}


class FitRecordTests(unittest.TestCase):
    """The record of a T check always fits, and every failure and not-run reason is kept or counted."""

    def fit(self, record):
        before = json.loads(json.dumps(record))
        run_check.fit_record(record)
        self.assertEqual(run_check.evidence.validate(record), [])
        for key in ("result", "check", "sha", "guarantees"):
            self.assertEqual(record[key], before[key])
        for key in ("executed", "failed"):
            self.assertEqual(record["assertions"][key], before["assertions"][key])
        self.names = [a["name"] for a in record["artifacts"]]
        self.assertEqual(self.names[0], "x.log")  # the command log always stays
        self.tla = record["tla"]
        self.dropped = record["tla"].get("dropped", {})
        self.assertEqual(bool(self.dropped), self.tla.get("truncated", False))
        # Nothing disappears uncounted.
        for key, ok in (("runs", lambda r: r["verdict"] == "pass"), ("mutants", lambda m: m["ok"])):
            for want, name in ((True, key), (False, f"failing_{key}")):
                total = sum(ok(x) == want for x in before["tla"].get(key, []))
                kept = sum(ok(x) == want for x in self.tla.get(key, []))
                self.assertEqual(total, kept + self.dropped.get(name, 0), name)
        for key in ("uncovered", "unmutated", "limits"):
            self.assertEqual(len(before["tla"].get(key, [])), len(self.tla.get(key, [])) + self.dropped.get(key, 0))
        self.assertEqual(len(before["artifacts"]), len(self.names) + self.dropped.get("artifacts", 0))
        not_run = record["assertions"]["not_run"]
        kept = [n for n in not_run if n["id"] != "truncated"]
        self.assertEqual(len(before["assertions"]["not_run"]), len(kept) + self.dropped.get("not_run", 0))
        self.assertEqual(bool(self.dropped.get("not_run")), len(kept) != len(not_run))
        if len(kept) != len(not_run):
            self.assertEqual(not_run[-1]["id"], "truncated")
            self.assertTrue(not_run[-1]["reason"].startswith(f"{self.dropped['not_run']} "))
        return record

    def test_a_record_that_fits_is_left_alone(self):
        record = t_record(mutants=[mutant(i, ok=i != 3) for i in range(5)], runs=[run_fact(0)],
                          artifacts=[artifact("summary.json"), artifact("X.log")])
        before = json.loads(json.dumps(record))
        self.assertEqual(self.fit(record), before)

    def test_passing_details_go_first(self):
        record = t_record(mutants=[mutant(i, ok=i != 3) for i in range(50)], runs=[run_fact(0), run_fact(1, ok=False)],
                          artifacts=[artifact("summary.json"), artifact("X.log")])
        self.fit(record)
        self.assertEqual(self.dropped, {"details": 50})
        details = {m["name"]: m.get("detail") for m in self.tla["mutants"]}
        self.assertEqual(details.pop("m03"), "d" * 300)
        self.assertEqual(set(details.values()), {None})
        self.assertEqual([r.get("detail") for r in self.tla["runs"]], [None, "d" * 100])

    def test_artifacts_of_a_failure_outlive_the_others(self):
        runs = [run_fact(i, ok=i != 19, detail=0) for i in range(20)]
        arts = [artifact("summary.json"), *(artifact(f"mutant-m{i:02d}.{k}") for i in range(50) for k in ("cex.json", "log")),
                *(artifact(f"X.v{i:02d}.log") for i in range(20))]
        record = t_record(mutants=[mutant(i, ok=i != 49) for i in range(50)], runs=runs, artifacts=arts, failed=2,
                          unmutated=["U" * 70 + str(i) for i in range(20)])
        self.fit(record)
        for name in ("tla/summary.json", "tla/mutant-m49.log", "tla/mutant-m49.cex.json", "tla/X.v19.log"):
            self.assertIn(name, self.names)
        self.assertGreater(self.dropped["artifacts"], 70)
        self.assertEqual((len(self.tla["mutants"]), len(self.tla["runs"])), (50, 20))
        self.assertEqual(self.names[1], "tla/summary.json")

    def test_artifacts_of_no_failure_go_before_caught_mutants(self):
        arts = [artifact("summary.json"), *(artifact(f"mutant-m{i:02d}.{k}") for i in range(24) for k in ("cex.json", "log"))]
        record = t_record(mutants=[mutant(i, ok=i != 23, detail=0) for i in range(50)], artifacts=arts,
                          runs=[run_fact(i, detail=0) for i in range(20)], unmutated=["U" * 110 for _ in range(24)])
        self.fit(record)
        self.assertEqual(list(self.dropped), ["artifacts"])
        self.assertIn("tla/mutant-m23.log", self.names)
        self.assertIn("tla/mutant-m23.cex.json", self.names)
        self.assertNotIn("tla/mutant-m22.log", self.names)  # the last ones went
        self.assertIn("tla/mutant-m00.log", self.names)

    def test_caught_mutants_and_passing_runs_go_before_any_failure(self):
        record = t_record(mutants=[mutant(i, ok=i % 10 != 0, name="n" * 100 + "{:02d}") for i in range(50)],
                          runs=[run_fact(i, ok=i != 7) for i in range(20)], failed=6,
                          unmutated=["U" * 100 + str(i) for i in range(40)],
                          artifacts=[artifact("summary.json"), artifact("X.v07.log")])
        self.fit(record)
        self.assertGreater(self.dropped["mutants"], 0)
        self.assertNotIn("failing_mutants", self.dropped)
        self.assertNotIn("failing_runs", self.dropped)
        self.assertEqual(sum(not m["ok"] for m in self.tla["mutants"]), 5)
        self.assertEqual([m["detail"] for m in self.tla["mutants"] if not m["ok"]], ["d" * 300] * 5)
        self.assertIn("X.v07.cfg", [r["cfg"] for r in self.tla["runs"]])
        # The last ones went, and the first caught mutants stayed.
        self.assertTrue(self.tla["mutants"][1]["ok"])

    def test_fifty_failing_mutants_are_kept_with_shorter_details(self):
        record = t_record(mutants=[mutant(i, ok=False, name="n" * 110 + "{:02d}") for i in range(50)], failed=50,
                          artifacts=[artifact("summary.json")] + [artifact(f"mutant-{'n' * 110}{i:02d}.log") for i in range(49)])
        self.fit(record)
        self.assertEqual(len(self.tla["mutants"]), 50)
        self.assertEqual({len(m["detail"]) for m in self.tla["mutants"]}, {60})
        self.assertEqual(self.dropped["shortened"], 50)
        self.assertIn("tla/summary.json", self.names)

    def test_failures_that_cannot_stay_are_counted(self):
        record = t_record(mutants=[mutant(i, ok=False, name="n" * 110 + "{:02d}") for i in range(50)], failed=100, executed=200,
                          uncovered=["a" * 100 + f"{i:02d}" for i in range(50)], artifacts=[artifact("summary.json")],
                          limits=["l" * 199] * 20, runs=[run_fact(i, ok=False) for i in range(20)])
        self.fit(record)
        self.assertGreater(self.dropped["uncovered"], 0)
        self.assertEqual(self.names, ["x.log", "tla/summary.json"])

    def test_lists_over_the_schemas_length_keep_the_failure(self):
        record = t_record(mutants=[mutant(i, ok=i != 55, detail=0) for i in range(62)],
                          runs=[run_fact(i, ok=i != 25, detail=0) for i in range(26)], failed=2)
        self.fit(record)
        self.assertEqual((len(self.tla["mutants"]), len(self.tla["runs"])), (50, 20))
        self.assertIn("m55", [m["name"] for m in self.tla["mutants"] if not m["ok"]])
        self.assertIn("X.v25.cfg", [r["cfg"] for r in self.tla["runs"] if r["verdict"] != "pass"])
        self.assertEqual(self.dropped, {"mutants": 12, "runs": 6})
        record = t_record(mutants=[mutant(i, ok=False, detail=0) for i in range(62)], failed=62)
        self.fit(record)
        self.assertEqual(self.dropped, {"failing_mutants": 12})

    def test_not_run_entries_are_kept_or_counted(self):
        for n, length in ((100, 250), (300, 20), (1500, 300)):
            with self.subTest(n=n, length=length):
                entries = [{"id": "tlc-error", "reason": f"seed-{i}.ndjson: " + "x" * length} for i in range(n)]
                record = t_record(result="error", executed=n, failed=0, not_run=entries, mode="trace",
                                  traces={"checked": n, "accepted": 0, "rejected": 0, "failing": []})
                self.fit(record)
                self.assertTrue(record["assertions"]["not_run"][0]["reason"].startswith("seed-0.ndjson"))

    def test_more_short_not_run_entries_than_the_schema_allows(self):
        record = t_record(result="error", executed=0, failed=0, not_run=[{"id": "t", "reason": "r"}] * 215)
        self.fit(record)
        self.assertEqual(self.dropped, {"not_run": 16})
        self.assertEqual(len(record["assertions"]["not_run"]), 200)

    def test_unlisted_failing_traces_are_counted(self):
        failing = [{"file": f"t{i}.ndjson", "index": 3, "reason": "r" * 100} for i in range(20)]
        record = t_record(mode="trace", executed=40, failed=33,
                          traces={"checked": 40, "accepted": 7, "rejected": 33, "failing": failing})
        self.fit(record)
        self.assertEqual((len(self.tla["traces"]["failing"]), self.dropped), (20, {"failing_traces": 13}))
        record = t_record(mode="trace", executed=40, failed=33, traces={
            "checked": 40, "accepted": 7, "rejected": 33,
            "failing": [{"file": f"t{i}.ndjson", "index": 3, "reason": "r"} for i in range(33)]})
        self.fit(record)
        self.assertEqual((len(self.tla["traces"]["failing"]), self.dropped), (20, {"failing_traces": 13}))

    def test_texts_over_the_schemas_length_are_cut_and_stay_distinct(self):
        record = t_record(mutants=[mutant(i, ok=False, name="n" * 121 + "{:02d}", detail=400) for i in range(2)], failed=2,
                          not_run=[{"id": "i" * 250, "reason": "r" * 400}], artifacts=[artifact("summary.json")])
        self.fit(record)
        self.assertEqual(len({m["name"] for m in self.tla["mutants"]}), 2)
        self.assertEqual(self.dropped, {"shortened": 6})

    def test_a_record_without_tool_facts_keeps_its_not_run_entries_or_their_count(self):
        record = t_record()
        del record["tla"]
        record["assertions"]["not_run"] = [{"id": str(i), "reason": "x" * 290} for i in range(300)]
        run_check.ensure_valid(record)
        self.assertEqual(run_check.evidence.validate(record), [])
        entries = record["assertions"]["not_run"]
        self.assertEqual(entries[-1]["id"], "truncated")
        self.assertTrue(entries[-1]["reason"].startswith(f"{300 - (len(entries) - 1)} more not-run entries"))

    def test_facts_the_schema_refuses_leave_an_error_record(self):
        for facts in ({"mutants": [{"name": "m"}]}, {"mutants": "none"}, {"runs": [3]}, {"extra": 1}, {"tier": "T9"},
                      {"traces": {"failing": [None]}}, {"dropped": 3}, {"mutants": [{"name": None, "ok": False}]}):
            for result, want in (("pass", "error"), ("not-run", "error"), ("error", "error"), ("fail", "fail")):
                with self.subTest(facts=facts, result=result):
                    record = t_record(result=result, failed=int(result == "fail"), not_run=[{"id": "a", "reason": "b"}],
                                      artifacts=[artifact("summary.json")], **facts)
                    run_check.ensure_valid(record)
                    self.assertEqual(run_check.evidence.validate(record), [])
                    self.assertEqual(record["result"], want)
                    self.assertNotIn("tla", record)
                    first, *rest = record["assertions"]["not_run"]
                    self.assertIn("truncated", first["reason"])
                    # What is not the tool's facts stays.
                    self.assertEqual(rest, [{"id": "a", "reason": "b"}])
                    self.assertEqual([a["name"] for a in record["artifacts"]], ["x.log", "tla/summary.json"])

    def test_refused_facts_beside_many_not_run_entries(self):
        entries = [{"id": "tlc-error", "reason": f"e{i} " + "x" * 280} for i in range(300)]
        record = t_record(result="error", executed=300, failed=0, not_run=entries, mode="trace", nonsense=1,
                          artifacts=[artifact(f"trace-{i:03d}.log") for i in range(60)] + [artifact("summary.json")])
        run_check.ensure_valid(record)
        self.assertEqual(run_check.evidence.validate(record), [])
        not_run = record["assertions"]["not_run"]
        self.assertEqual([not_run[0]["id"], not_run[-1]["id"]], ["record", "truncated"])
        kept = sum(n["id"] == "tlc-error" for n in not_run)
        self.assertGreater(kept, 50)
        self.assertTrue(not_run[-1]["reason"].startswith(f"{300 - kept} more not-run entries"))
        self.assertEqual([a["name"] for a in record["artifacts"][:2]], ["x.log", "tla/summary.json"])

    def test_refused_facts_do_not_cost_artifacts(self):
        # Fitting the facts drops artifacts. Without the facts they fit, so they come back.
        record = t_record(failed=50, mutants=[mutant(i, ok=False, name="n" * 110 + "{:02d}") for i in range(50)], nonsense=1,
                          artifacts=[artifact(f"a{i:02d}.log") for i in range(45)] + [artifact("summary.json")])
        run_check.ensure_valid(record)
        self.assertEqual(run_check.evidence.validate(record), [])
        self.assertEqual(len(record["artifacts"]), 47)

    def test_artifacts_of_a_failure_go_after_caught_mutants(self):
        arts = [artifact("summary.json"), *(artifact(f"mutant-m{i:02d}.{k}") for i in range(24) for k in ("cex.json", "log"))]
        record = t_record(mutants=[mutant(i, ok=i >= 24, detail=0) for i in range(50)], artifacts=arts, failed=24,
                          unmutated=["U" * 110 for _ in range(50)])
        self.fit(record)
        self.assertGreater(self.dropped["mutants"], 0)
        self.assertEqual(len(self.names), 50)  # every one belongs to a failure
        self.assertNotIn("artifacts", self.dropped)

    def test_more_limits_than_the_schema_allows(self):
        record = t_record(limits=[f"X.cfg: CONSTRAINT C{i}" for i in range(53)])
        self.fit(record)
        self.assertEqual((len(self.tla["limits"]), self.dropped), (50, {"limits": 3}))

    def test_bad_artifacts_are_dropped_and_counted(self):
        record = t_record(artifacts=[{"name": "tla/a"}, artifact("n" * 300), artifact("summary.json")])
        self.fit(record)
        self.assertEqual((self.names, self.dropped), (["x.log", "tla/summary.json"], {"artifacts": 2}))


class CheckFileTests(unittest.TestCase):
    def test_fixture_checks_load(self):
        checks = run_check.load_all(FIXTURE_CHECKS, {"cache", "events", "testing", "misc"})
        self.assertEqual([c.id for c in checks], ["cache-sim", "cache-unit", "events-unit"])

    def test_repository_checks_load(self):
        checks = run_check.load_all(run_check.CHECKS_DIR, run_check.module_names())
        self.assertIn("testing-matrix", [c.id for c in checks])

    def test_invalid_check_files(self):
        cases = {
            "unknown field": check_data(colour="red"),
            "missing field": {k: v for k, v in check_data().items() if k != "command"},
            "bad kind": check_data(kinds=["Z"]),
            "bad tier": check_data(tiers=["T9"]),
            "bad need": check_data(needs=["gpu"]),
            "bad parser": check_data(result_parser="junit"),
            "absolute cwd": check_data(cwd="/tmp"),
            "escaping cwd": check_data(cwd="../x"),
            "bad guarantee": check_data(guarantees=["CACHE-2"]),
            "unknown module": check_data(module="nope"),
            "bool timeout": check_data(timeout_s=True),
            "T without tlc parser": check_data(kinds=["T"], needs=["tlc"]),
            "T without tlc need": check_data(kinds=["T"], result_parser="tlc"),
        }
        for name, data in cases.items():
            with self.subTest(name), self.assertRaises(run_check.CheckError):
                run_check.parse_check(data, Path("demo.toml"), {"testing", "misc"})

    def test_id_must_match_file_name(self):
        with self.assertRaises(run_check.CheckError) as cm:
            run_check.parse_check(check_data(), Path("other.toml"))
        self.assertIn("does not match the file name", str(cm.exception))


class KindTTests(unittest.TestCase):
    def test_kind_t_check_is_valid(self):
        check = run_check.parse_check(check_data(kinds=["T"], needs=["tlc"], result_parser="tlc"))
        self.assertEqual((check.kinds, check.needs), (["T"], ["tlc"]))

    def test_selftest_checks_are_registered(self):
        checks = {c.id: c for c in run_check.load_all(run_check.CHECKS_DIR, run_check.module_names())}
        for cid in ("specs-selftest", "specs-selftest-trace", "specs-selftest-large"):
            self.assertEqual(checks[cid].kinds, ["T"])
            self.assertEqual(checks[cid].result_parser, "tlc")

    def test_tlc_need_without_docker_is_not_run(self):
        check = run_check.parse_check(check_data(kinds=["T"], needs=["tlc"], result_parser="tlc"))
        reasons = [m["reason"].split(":")[0] for m in run_check.missing_needs(check, {"PATH": "/nonexistent"})]
        self.assertEqual(reasons, ["tlc-unavailable"])


class NeedsTests(unittest.TestCase):
    def test_missing_needs_give_reasons(self):
        check = run_check.parse_check(check_data(needs=["s3", "kind", "node", "valkey"]))
        env = {"PATH": "/nonexistent"}
        reasons = [m["reason"].split(":")[0] for m in run_check.missing_needs(check, env)]
        self.assertEqual(reasons, ["s3-unconfigured", "kind-missing", "node-missing", "valkey-unavailable"])
        env = {"PATH": "/nonexistent", "NABEDRA_TEST_S3_BUCKET": "b", "NABEDRA_TEST_VALKEY_URL": "redis://x"}
        reasons = [m["reason"].split(":")[0] for m in run_check.missing_needs(check, env)]
        self.assertEqual(reasons, ["kind-missing", "node-missing"])

    def test_docker_and_nightly_need_working_commands(self):
        check = run_check.parse_check(check_data(needs=["docker", "rust-nightly"]))
        reasons = [m["reason"].split(":")[0] for m in run_check.missing_needs(check, {"PATH": "/nonexistent"})]
        self.assertEqual(reasons, ["docker-unavailable", "rust-nightly-missing"])


class ExecuteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tmp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(tmp.cleanup)
        cls.repo = Path(tmp.name)
        g = ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"]
        subprocess.run(["git", "init", "-q"], cwd=cls.repo, check=True)
        (cls.repo / "fake_tests.py").write_text(textwrap.dedent("""
            import os, sys, time
            mode = sys.argv[1]
            if mode == "ok":
                print("Ran 3 tests in 0.001s"); print(); print("OK")
            elif mode == "zero":
                print("Ran 0 tests in 0.000s"); print(); print("NO TESTS RAN")
            elif mode == "fail":
                print("Ran 3 tests in 0.001s"); print(); print("FAILED (failures=1)"); sys.exit(1)
            elif mode == "skip":
                print("not-run: missing fixture")
                print("Ran 1 test in 0.001s"); print(); print("OK")
            elif mode == "s3":
                print("nabedra-s3-requests: 12 prefix=runs/req-s3/")
                print("nabedra-s3-requests: 3")
                print("some log: nabedra-s3-requests: 100")
                print("nabedra-s3-requests: 42bad")
                sys.exit(1)
            elif mode == "env":
                print("rid=" + os.environ["NABEDRA_REQUEST_ID"] + " knob=" + os.environ["KNOB"])
            elif mode == "sleep":
                time.sleep(30)
            elif mode == "tla-big":
                import json
                detail = "x" * 300
                facts = {"spec": "specs/x/X", "mode": "exhaustive", "tier": "T1", "tools": "tlc 1.8.0",
                         "runs": [{"cfg": f"X.v{i}.cfg", "verdict": "pass", "detail": detail} for i in range(20)],
                         "mutants": [{"name": f"m{i:02d}", "expect": "Inv", "observed": "Inv" if i else "none",
                                      "ok": i != 0, "cex_length": 7, "detail": detail} for i in range(50)]}
                arts = [{"name": f"tla/mutant-m{i:02d}.log", "sha256": "0" * 64} for i in range(39)]
                arts.append({"name": "tla/summary.json", "sha256": "1" * 64})
                print("nabedra-tla-summary: " + json.dumps({
                    "verdict": "fail", "executed": 71, "failed": 1, "tla": facts, "not_run": [],
                    "artifacts": arts}))
                sys.exit(1)
            elif mode == "tla-errors":
                import json
                print("nabedra-tla-summary: " + json.dumps({
                    "verdict": "error", "executed": 100, "failed": 0, "artifacts": [],
                    "not_run": [{"id": "tlc-error", "reason": f"seed-{i}.ndjson: " + "x" * 250} for i in range(100)],
                    "tla": {"spec": "s", "mode": "trace", "tier": "T1", "tools": "t",
                            "traces": {"checked": 100, "accepted": 0, "rejected": 0, "failing": []}}}))
                sys.exit(3)
            elif mode == "tla-bad-facts":
                import json
                print("nabedra-tla-summary: " + json.dumps({
                    "verdict": "pass", "executed": 3, "failed": 0, "not_run": [], "artifacts": [],
                    "tla": {"mode": "exhaustive", "nonsense": 1}}))
            elif mode.startswith("tla-"):
                import json
                facts = {"spec": "specs/x/X", "mode": "simulation", "tier": "T2", "tools": "tlc 1.8.0"}
                verdict = mode[4:]
                print("nabedra-tla-summary: " + json.dumps({
                    "verdict": verdict, "executed": 1, "failed": 0, "tla": facts,
                    "not_run": [] if verdict == "pass" else [{"id": "b", "reason": "time budget"}],
                    "artifacts": [{"name": "tla/X.log", "sha256": "0" * 64}]}))
                sys.exit({"pass": 0, "not-run": 2}[verdict])
        """))
        subprocess.run(["git", "add", "."], cwd=cls.repo, check=True)
        subprocess.run([*g, "commit", "-qm", "c"], cwd=cls.repo, check=True)

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        self.evidence = root / "evidence"
        self.logs = root / "logs"

    def run_mode(self, mode, parser="unittest", **over):
        check = run_check.parse_check(check_data(
            command=["python3", "fake_tests.py", mode], result_parser=parser, **over))
        return run_check.execute(check, evidence_dir=self.evidence, log_dir=self.logs,
                                 request_id=f"req-{mode}", env=dict(os.environ), echo=False, repo=self.repo)

    def test_pass_writes_valid_record_and_log(self):
        record, path = self.run_mode("ok")
        self.assertEqual(record["result"], "pass")
        self.assertEqual(record["assertions"]["executed"], 3)
        self.assertTrue(record["tree_clean"])
        self.assertEqual(json.loads(path.read_text()), record)
        self.assertEqual(path.name, "req-ok--demo.json")
        log = self.logs / "req-ok--demo.log"
        self.assertIn("Ran 3 tests", log.read_text())
        self.assertEqual(record["artifacts"][0]["name"], log.name)

    def test_tlc_record_carries_tla_facts_and_mode(self):
        # The tlc need is left out so that the fake command runs without Docker.
        record, path = self.run_mode("tla-pass", parser="tlc")
        self.assertEqual(record["result"], "pass")
        self.assertEqual(record["tla"]["mode"], "simulation")
        self.assertEqual(record["mode"], {"tla": "simulation"})
        self.assertIn("tla/X.log", [a["name"] for a in record["artifacts"]])
        record, _ = self.run_mode("tla-not-run", parser="tlc")
        self.assertEqual(record["result"], "not-run")

    def test_large_tlc_record_is_fitted_under_the_limit(self):
        record, path = self.run_mode("tla-big", parser="tlc")
        self.assertLessEqual(path.stat().st_size, 20 * 1024)
        self.assertEqual((record["result"], record["assertions"]["failed"]), ("fail", 1))
        self.assertTrue(record["tla"]["truncated"])
        mutants = {m["name"]: m for m in record["tla"]["mutants"]}
        self.assertFalse(mutants["m00"]["ok"])  # the failing mutant is kept
        names = [a["name"] for a in record["artifacts"]]
        self.assertTrue(names[0].endswith(".log") and not names[0].startswith("tla/"))
        self.assertIn("tla/summary.json", names)

    def test_a_tlc_check_always_leaves_a_record(self):
        record, path = self.run_mode("tla-errors", parser="tlc")
        self.assertEqual(json.loads(path.read_text()), record)
        self.assertEqual((record["result"], record["assertions"]["executed"]), ("error", 100))
        self.assertTrue(record["tla"]["truncated"])
        kept = [n for n in record["assertions"]["not_run"] if n["id"] == "tlc-error"]
        self.assertEqual(len(kept) + record["tla"]["dropped"].get("not_run", 0), 100)
        record, path = self.run_mode("tla-bad-facts", parser="tlc")
        self.assertEqual(json.loads(path.read_text())["result"], "error")

    def test_zero_assertions_record_is_not_run(self):
        record, _ = self.run_mode("zero")
        self.assertEqual(record["result"], "not-run")

    def test_not_run_marker_overrides_a_success_summary(self):
        marker = "not-run: missing fixture"
        record, path = self.run_mode("skip", not_run_marker=marker)
        self.assertEqual(record["result"], "not-run")
        self.assertEqual(record["assertions"], {
            "executed": 0, "failed": 0,
            "not_run": [{"id": "output-marker", "reason": marker}],
        })
        self.assertEqual(json.loads(path.read_text()), record)
        output = (self.logs / "req-skip--demo.log").read_text()
        self.assertEqual(run_check.classify("unittest", output, 0)[0], "pass")

    def test_s3_requests_are_recorded_even_when_check_fails(self):
        record, path = self.run_mode("s3", parser="exit-code")
        self.assertEqual(record["result"], "fail")
        self.assertEqual(json.loads(path.read_text())["s3_requests"], 15)
        self.assertNotIn("s3_requests", self.run_mode("ok")[0])

    def test_failure(self):
        record, _ = self.run_mode("fail")
        self.assertEqual((record["result"], record["assertions"]["failed"]), ("fail", 1))

    def test_env_and_request_id_reach_the_command(self):
        record, _ = self.run_mode("env", parser="exit-code", env={"KNOB": "7"})
        self.assertEqual(record["result"], "pass")
        self.assertIn("rid=req-env knob=7", (self.logs / "req-env--demo.log").read_text())

    def test_timeout_kills_the_command(self):
        record, _ = self.run_mode("sleep", parser="exit-code", timeout_s=1)
        self.assertEqual(record["result"], "error")
        self.assertEqual(record["assertions"]["executed"], 0)
        self.assertLess(record["duration_s"], 20)

    def test_missing_need_skips_the_command(self):
        env = {k: v for k, v in os.environ.items() if k != "NABEDRA_TEST_S3_BUCKET"}
        check = run_check.parse_check(check_data(command=["python3", "fake_tests.py", "fail"],
                                                 result_parser="unittest", needs=["s3"]))
        record, _ = run_check.execute(check, evidence_dir=self.evidence, log_dir=self.logs,
                                      request_id="req-s3", env=env, echo=False, repo=self.repo)
        self.assertEqual(record["result"], "not-run")
        self.assertTrue(record["assertions"]["not_run"][0]["reason"].startswith("s3-unconfigured"))
        self.assertFalse((self.logs / "req-s3--demo.log").exists())

    def test_dry_run_writes_nothing(self):
        check = run_check.parse_check(check_data(command=["python3", "fake_tests.py", "ok"]))
        self.assertEqual(run_check.execute(check, evidence_dir=self.evidence, log_dir=self.logs,
                                           request_id="req-dry", dry_run=True, echo=False, repo=self.repo),
                         (None, None))
        self.assertFalse(self.evidence.exists())

    def test_python_resolution_preserves_explicit_interpreters_and_arguments(self):
        args = ["-c", "print('hello; world')", "python3"]
        self.assertEqual(run_check.resolve_command(["python3", *args]), [sys.executable, *args])
        for interpreter in ("python3.12", "./bin/python3", "/opt/python/bin/python"):
            command = [interpreter, *args]
            self.assertEqual(run_check.resolve_command(command), command)

    def test_unsafe_request_id_creates_no_log(self):
        check = run_check.parse_check(check_data(command=["python3", "fake_tests.py", "ok"]))
        with self.assertRaises(run_check.evidence.EvidenceError):
            run_check.execute(check, evidence_dir=self.evidence, log_dir=self.logs,
                              request_id="../escape", echo=False, repo=self.repo)
        self.assertFalse(self.logs.exists())
        self.assertFalse(self.evidence.exists())


if __name__ == "__main__":
    unittest.main()

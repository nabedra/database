#!/usr/bin/env python3
import copy
import datetime as dt
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evidence  # noqa: E402

SHA = "a" * 40


def good(**over):
    rec = evidence.new_record(
        "cache-store-model", ["U", "S"], result="pass", executed=12, failed=0,
        guarantees=["CACHE-G02"], scenarios=["K01"], sha=SHA, tree_clean=True,
        run={"url": None, "attempt": None, "request_id": "req-1", "runner": None,
             "date": "2026-11-22T12:00:00+00:00"},
    )
    rec.update(over)
    return rec


class ValidatorTests(unittest.TestCase):
    def test_good_record_is_valid(self):
        self.assertEqual(evidence.validate(good()), [])

    def test_schema_violations_are_reported(self):
        cases = {
            "missing field": lambda r: r.pop("tree_clean"),
            "unknown field": lambda r: r.update(extra=1),
            "short sha": lambda r: r.update(sha="abc"),
            "bad kind": lambda r: r.update(kind="X"),
            "bad result": lambda r: r.update(result="green"),
            "bad guarantee": lambda r: r.update(guarantees=["cache-1"]),
            "negative duration": lambda r: r.update(duration_s=-1),
            "nonfinite duration": lambda r: r.update(duration_s=float("nan")),
            "bool as integer": lambda r: r["assertions"].update(executed=True),
            "nested unknown": lambda r: r["run"].update(job="x"),
            "missing date": lambda r: r["run"].pop("date"),
            "date without offset": lambda r: r["run"].update(date="2026-11-22T12:00:00"),
            "invalid date": lambda r: r["run"].update(date="2026-02-30T12:00:00Z"),
            "bad artifact digest": lambda r: r.update(artifacts=[{"name": "log", "sha256": "zz"}]),
            "seed range arity": lambda r: r.update(seeds={"range": [1], "count": 1, "failing": []}),
            "wrong schema id": lambda r: r.update(schema="nabedra.evidence/2"),
        }
        for name, mutate in cases.items():
            with self.subTest(name):
                rec = good()
                mutate(rec)
                self.assertNotEqual(evidence.validate(rec), [], name)

    def test_kind_t_and_tla_facts(self):
        self.assertIn("T", evidence.KINDS)
        rec = good(kind="T")
        rec["tla"] = {
            "spec": "specs/oltp/DirectPath", "mode": "exhaustive", "tier": "T1", "tools": "tlc 1.8.0",
            "budget_s": 3600, "distinct_states": 10, "depth": 4, "actions": 3, "min_action_coverage": 1,
            "uncovered": [], "unmutated": ["TypeOK"],
            "runs": [{"cfg": "DirectPath.cfg", "verdict": "pass", "distinct_states": 10, "depth": 4,
                      "states_left": 0, "duration_s": 1.5, "detail": ""}],
            "mutants": [{"name": "no_fence", "expect": "Safe", "observed": "Safe", "ok": True,
                         "cex_length": 5, "detail": ""}],
        }
        self.assertEqual(evidence.validate(rec), [])
        trace = good(kind="T")
        trace["tla"] = {"spec": "specs/oltp/DirectPath", "mode": "trace", "tier": "T0", "tools": "t",
                        "traces": {"checked": 2, "accepted": 1, "rejected": 1, "expected_rejections": 1,
                                   "failing": [{"file": "a.ndjson", "index": 3, "reason": "x"}]}}
        self.assertEqual(evidence.validate(trace), [])
        for mutate in (lambda t: t.update(mode="random"), lambda t: t.update(extra=1),
                       lambda t: t.pop("tools"), lambda t: t["mutants"][0].pop("ok"),
                       lambda t: t.update(tier="T9")):
            bad = copy.deepcopy(rec)
            mutate(bad["tla"])
            self.assertNotEqual(evidence.validate(bad), [])
        self.assertNotEqual(evidence.validate(good(kind="T Z")), [])

    def test_limits_and_dropped_counts_are_optional_facts(self):
        rec = good(kind="T")
        rec["tla"] = {"spec": "specs/x/X", "mode": "exhaustive", "tier": "T1", "tools": "t", "truncated": True,
                      "limits": ["X.cfg: CONSTRAINT Few: terms"], "dropped": {"mutants": 3, "failing_mutants": 1}}
        self.assertEqual(evidence.validate(rec), [])
        for dropped in ({"mutants": 0}, {"mutants": "3"}, [3]):
            rec["tla"]["dropped"] = dropped
            self.assertNotEqual(evidence.validate(rec), [])

    def test_records_written_before_the_new_facts_still_validate(self):
        # The attested records of run 45417380925, written by #110 before `limits` and `dropped`.
        records = sorted((Path(__file__).resolve().parent / "testdata" / "tla" / "records").glob("*.json"))
        self.assertEqual(sorted(p.stem for p in records), ["specs-selftest", "specs-selftest-large", "specs-selftest-trace"])
        for path in records:
            with self.subTest(path.stem):
                record = json.loads(path.read_text())
                self.assertEqual(evidence.validate(record), [])
                self.assertEqual(evidence.tla_contradictions(record["tla"]), [])

    def test_tla_contradictions(self):
        facts = {"spec": "s", "mode": "exhaustive", "tier": "T1", "tools": "t",
                 "runs": [{"cfg": "X.cfg", "verdict": "pass", "states_left": 0}],
                 "mutants": [{"name": "m", "ok": True}], "uncovered": []}
        self.assertEqual(evidence.tla_contradictions(facts), [])
        for change in ({"mode": "guess"}, {"mode": None}, {"uncovered": ["A"]}, {"mutants": [{"name": "m", "ok": False}]},
                       {"mutants": [{"name": "m"}]}, {"runs": [{"cfg": "X.cfg", "verdict": "incomplete"}]},
                       {"runs": [{"cfg": "X.cfg", "verdict": "pass", "states_left": 4}]},
                       {"runs": [{"cfg": "X.cfg", "verdict": "pass", "states_left": None}]},
                       {"traces": {"failing": [{"file": "a"}]}}, {"dropped": {"failing_mutants": 1}},
                       {"dropped": {"uncovered": 2}}, {"dropped": {"failing_runs": 1}},
                       {"dropped": {"failing_traces": 1}}):
            with self.subTest(change=change):
                self.assertEqual(len(evidence.tla_contradictions({**facts, **change})), 1)
        self.assertEqual(evidence.tla_contradictions(None), ["no tla facts"])
        # An exhaustive pass needs a run, kept or dropped.
        for runs in ([], None, "x", [3]):
            self.assertEqual(evidence.tla_contradictions({**facts, "runs": runs}), ["no run"])
        self.assertEqual(evidence.tla_contradictions({**facts, "runs": [], "dropped": {"runs": 2}}), [])
        for run in ({"cfg": "X.cfg", "verdict": "pass"}, {"cfg": "X.cfg", "verdict": "pass", "states_left": "0"}):
            self.assertEqual(len(evidence.tla_contradictions({**facts, "runs": [run]})), 1)
        # A trace pass accepted at least one trace, and rejected only what it had to.
        counts = {"checked": 4, "accepted": 1, "rejected": 3, "expected_rejections": 3, "failing": []}
        self.assertEqual(evidence.tla_contradictions({"mode": "trace", "traces": counts}), [])
        for change in ({"accepted": 0}, {"accepted": 0, "checked": 3}, {"rejected": 2}, {"expected_rejections": 2}, {"checked": 5},
                       {"checked": 3, "rejected": 2}, {"accepted": None}, {"rejected": "3"}, {"expected_rejections": None}):
            with self.subTest(change=change):
                self.assertEqual(len(evidence.tla_contradictions({"mode": "trace", "traces": {**counts, **change}})), 1)
        for traces in (None, [], {}, {"checked": 1, "accepted": 1, "rejected": 0}):
            self.assertEqual(evidence.tla_contradictions({"mode": "trace", "traces": traces}), ["no trace counts"])
        # A simulation ends with states it never visited, and dropped passing facts contradict nothing.
        self.assertEqual(evidence.tla_contradictions(
            {"mode": "simulation", "runs": [{"cfg": "X.cfg", "verdict": "pass", "states_left": None}],
             "dropped": {"mutants": 9, "artifacts": 2}}), [])

    def test_zero_assertions_can_never_pass(self):
        rec = good()
        rec["assertions"]["executed"] = 0
        errors = evidence.validate(rec)
        self.assertTrue(any("zero executed" in e for e in errors), errors)

    def test_pass_with_failures_and_reasonless_not_run_are_invalid(self):
        rec = good()
        rec["assertions"]["failed"] = 1
        self.assertTrue(evidence.validate(rec))
        rec = good(result="not-run")
        rec["assertions"]["executed"] = 0
        self.assertTrue(evidence.validate(rec))
        rec["assertions"]["not_run"] = [{"id": "need:s3", "reason": "s3-unconfigured"}]
        self.assertEqual(evidence.validate(rec), [])

    def test_oversize_record_is_invalid(self):
        rec = good(scenarios=[f"S{i:05d}-{'x' * 30}" for i in range(700)])
        self.assertTrue(any("byte limit" in e for e in evidence.validate(rec)))

    def test_unsupported_schema_keyword_is_refused(self):
        with self.assertRaises(evidence.EvidenceError):
            evidence.schema_errors({}, {"oneOf": []})


class EnvironmentTests(unittest.TestCase):
    def test_run_info_from_actions_env(self):
        env = {"GITHUB_SERVER_URL": "https://github.com", "GITHUB_REPOSITORY": "o/r",
               "GITHUB_RUN_ID": "42", "GITHUB_RUN_ATTEMPT": "2", "NABEDRA_REQUEST_ID": "rid-1",
               "RUNNER_NAME": "GitHub Actions 7"}
        earliest = dt.datetime.now(dt.timezone.utc)
        info = evidence.run_info(env)
        date = dt.datetime.fromisoformat(info.pop("date"))
        self.assertEqual(date.utcoffset(), dt.timedelta(0))
        self.assertLessEqual(earliest, date)
        self.assertLessEqual(date, dt.datetime.now(dt.timezone.utc))
        self.assertEqual(info, {
            "url": "https://github.com/o/r/actions/runs/42", "attempt": 2,
            "request_id": "rid-1", "runner": "GitHub Actions 7"})

    def test_run_info_local_defaults(self):
        info = evidence.run_info({})
        self.assertIsNone(info["url"])
        self.assertIsNone(info["attempt"])
        self.assertTrue(info["request_id"].startswith("local-"))

    def test_git_state_reports_sha_and_dirty_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            git = ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"]
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            (repo / "a").write_text("1")
            subprocess.run(["git", "add", "a"], cwd=repo, check=True)
            subprocess.run([*git, "commit", "-qm", "c"], cwd=repo, check=True)
            sha, clean = evidence.git_state(repo)
            self.assertRegex(sha, r"^[0-9a-f]{40}$")
            self.assertTrue(clean)
            (repo / "b").write_text("untracked")
            self.assertFalse(evidence.git_state(repo)[1])
            rec = evidence.new_record("c", ["U"], result="pass", executed=1, failed=0, repo=repo, env={})
            self.assertEqual((rec["sha"], rec["tree_clean"]), (sha, False))


class WriteTests(unittest.TestCase):
    def test_write_layout_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = dt.datetime(2026, 3, 9, tzinfo=dt.timezone.utc)
            path = evidence.write(good(), Path(tmp), now)
            self.assertEqual(path, Path(tmp) / "2026" / "03" / "req-1--cache-store-model.json")
            self.assertEqual(json.loads(path.read_text())["result"], "pass")
            with self.assertRaises(FileExistsError):
                evidence.write(good(), Path(tmp), now)

    def test_write_refuses_invalid_and_unsafe_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad = good()
            bad["assertions"]["executed"] = 0
            with self.assertRaises(evidence.EvidenceError):
                evidence.write(bad, Path(tmp))
            unsafe = copy.deepcopy(good())
            unsafe["run"]["request_id"] = "../escape"
            with self.assertRaises(evidence.EvidenceError):
                evidence.write(unsafe, Path(tmp))
            self.assertEqual(list(Path(tmp).rglob("*.json")), [])


if __name__ == "__main__":
    unittest.main()

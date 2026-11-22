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
        "kv-store-model", ["U", "S"], result="pass", executed=12, failed=0,
        guarantees=["KV-G02"], scenarios=["K01"], sha=SHA, tree_clean=True,
        run={"url": None, "attempt": None, "request_id": "req-1", "runner": None},
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
            "bad guarantee": lambda r: r.update(guarantees=["kv-1"]),
            "negative duration": lambda r: r.update(duration_s=-1),
            "bool as integer": lambda r: r["assertions"].update(executed=True),
            "nested unknown": lambda r: r["run"].update(job="x"),
            "bad artifact digest": lambda r: r.update(artifacts=[{"name": "log", "sha256": "zz"}]),
            "seed range arity": lambda r: r.update(seeds={"range": [1], "count": 1, "failing": []}),
            "wrong schema id": lambda r: r.update(schema="nabedra.evidence/2"),
        }
        for name, mutate in cases.items():
            with self.subTest(name):
                rec = good()
                mutate(rec)
                self.assertNotEqual(evidence.validate(rec), [], name)

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
        self.assertEqual(evidence.run_info(env), {
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
            self.assertEqual(path, Path(tmp) / "2026" / "03" / "req-1--kv-store-model.json")
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

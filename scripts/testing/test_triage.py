from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import triage


class TriageTests(unittest.TestCase):
    def test_initial_pass_and_ineligible_kinds_never_rerun(self):
        get_baseline, rerun = Mock(), Mock()
        self.assertFalse(triage.triage("R", 0, "", get_baseline, rerun).blocked)
        for kind in ("S", "M", "H", "U", "T", "R H", "R T", "T R"):
            with self.subTest(kind=kind):
                self.assertTrue(triage.triage(kind, 1, "assertion failed", get_baseline, rerun).blocked)
        get_baseline.assert_not_called()
        rerun.assert_not_called()

    def test_initial_crash_signatures_fail_without_baseline_or_retry(self):
        get_baseline, rerun = Mock(), Mock()
        for code, log in [
            (1, "thread 'worker' panicked at src/lib.rs:11"),
            (1, "SIGABRT"), (134, ""), (137, ""), (1, "exit code 137"),
            (-9, ""), (1, "Killed\n"), (1, "kernel: Out of memory"),
            (1, "OOMKilled"), (-11, ""), (1, "Segmentation fault (core dumped)"),
            (1, "Aborted (core dumped)"), (1, "SIGKILL"), (1, "SIGSEGV"),
            (1, "panic: unexpected runtime failure"), (1, "Abort trap: 6"),
        ]:
            with self.subTest(code=code, log=log):
                decision = triage.triage("R", code, log, get_baseline, rerun)
                self.assertTrue(decision.blocked)
                self.assertIn("initial crash", decision.reason)
        get_baseline.assert_not_called()
        rerun.assert_not_called()

    def test_crash_during_retry_stops_immediately(self):
        rerun = Mock(side_effect=[(0, ""), (1, "SIGSEGV")])
        result = triage.triage("R", 1, "", lambda: (10, 10), rerun)
        self.assertTrue(result.blocked)
        self.assertEqual((result.retry_passes, result.retries), (1, 2))
        self.assertEqual(rerun.call_count, 2)

    def test_exact_percentage_point_is_allowed_and_larger_gap_blocks(self):
        for baseline, blocked in [((91, 100), False), ((92, 100), True)]:
            with self.subTest(baseline=baseline):
                rerun = Mock(side_effect=[(0, "")] * 9 + [(1, "assertion failed")])
                decision = triage.triage("R", 1, "", lambda: baseline, rerun)
                self.assertEqual(decision.blocked, blocked)
                self.assertEqual((decision.retry_passes, decision.retries), (9, 10))
                self.assertEqual(rerun.call_count, 10)

    def test_clean_retries_match_perfect_baseline(self):
        rerun = Mock(return_value=(0, ""))
        decision = triage.triage("R", 1, "", lambda: (500, 500), rerun)
        self.assertFalse(decision.blocked)
        self.assertEqual((decision.retry_passes, decision.retries), (10, 10))

    def test_product_abort_is_distinct_from_a_process_crash(self):
        self.assertIsNone(triage.crash_signature(1, "The transaction aborted with a write conflict."))

    def test_no_baseline_blocks_without_retry(self):
        rerun = Mock()
        decision = triage.triage("R", 1, "", lambda: (0, 0), rerun)
        self.assertTrue(decision.blocked)
        self.assertIn("no pass/fail evidence", decision.reason)
        rerun.assert_not_called()

    def test_reads_only_recent_executed_records_for_the_check(self):
        today = date(2026, 11, 22)
        old = (today - timedelta(days=14)).isoformat()
        cases = [
            ("probe", "pass", {"run": today.isoformat()}),
            ("probe", "fail", {"run": {"date": f"{today - timedelta(days=13)}T19:30:00Z"}}),
            ("probe", "fail", {"run": old}),
            ("probe", "fail", {"run": (today + timedelta(days=1)).isoformat()}),
            ("probe", "not-run", {"run": today.isoformat()}),
            ("probe", "quarantined", {"run": today.isoformat()}),
            ("different", "fail", {"run": today.isoformat()}),
            # Records without run.date fall back to finished_at, then started_at.
            ("probe", "pass", {"run": {"request_id": "r-1", "finished_at": f"{today}T01:00:00+00:00"}}),
            ("probe", "pass", {"run": {"request_id": "r-2"}, "finished_at": f"{old}T23:00:00Z"}),
            ("probe", "fail", {"run": {"request_id": "r-3"}, "started_at": f"{today}T02:00:00Z"}),
        ]
        with TemporaryDirectory() as directory:
            root = Path(directory) / "2026/11"
            root.mkdir(parents=True)
            for number, (check, result, extra) in enumerate(cases):
                (root / f"{number}.json").write_text(json.dumps(dict(check=check, result=result, **extra)))
            self.assertEqual(triage.baseline(Path(directory), "probe", today), (2, 4))

    def test_undated_records_fall_back_to_file_mtime_instead_of_blocking(self):
        today = date(2026, 11, 22)
        with TemporaryDirectory() as directory:
            recent, stale = Path(directory) / "recent.json", Path(directory) / "stale.json"
            for path in (recent, stale):
                path.write_text(json.dumps({"check": "probe", "result": "pass",
                                            "run": {"request_id": path.stem}}))
            for path, day in ((recent, today), (stale, today - timedelta(days=14))):
                stamp = datetime(day.year, day.month, day.day, 12, tzinfo=timezone.utc).timestamp()
                os.utime(path, (stamp, stamp))
            self.assertEqual(triage.baseline(Path(directory), "probe", today), (1, 1))


class KindTTests(unittest.TestCase):
    """A T check is never retried or unblocked, whatever --kind says."""

    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.checks = self.root / "checks"
        self.checks.mkdir()
        for name, kinds in (("spec", '["T"]'), ("mixed", '["R", "T"]'), ("real", '["R"]'), ("broken", '"T"')):
            (self.checks / f"{name}.toml").write_text(f'id = "{name}"\nkinds = {kinds}\n')
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()
        (self.root / "log").write_text("mutant not caught\n")

    def record(self, check, kind):
        (self.evidence / f"{check}.json").write_text(json.dumps(
            {"check": check, "kind": kind, "result": "pass", "run": datetime.now(timezone.utc).date().isoformat()}))

    def run_main(self, check, kind, exit_code=1):
        marker = self.root / "reran"
        argv = ["--kind", kind, "--check", check, "--evidence", str(self.evidence), "--log",
                str(self.root / "log"), "--exit-code", str(exit_code), "--", "touch", str(marker)]
        out = io.StringIO()
        with patch.object(triage, "CHECKS_DIR", self.checks), redirect_stdout(out), redirect_stderr(io.StringIO()):
            code = triage.main(argv)
        return code, marker.exists(), out.getvalue()

    def test_registered_t_check_is_blocked_whatever_kind_says(self):
        for check in ("spec", "mixed"):
            self.record(check, "R")  # a record that hides the kind does not help either
            for kind in ("R", "R P", "R T", "T", "U"):
                for exit_code in (1, 0):
                    with self.subTest(check=check, kind=kind, exit_code=exit_code):
                        code, reran, text = self.run_main(check, kind, exit_code)
                        self.assertEqual((code, reran), (1, False))
                        self.assertIn("kind T is never retried or unblocked", text)
                        self.assertIn("Retries 0/0", text)

    def test_unregistered_check_with_t_records_is_blocked(self):
        self.record("renamed", "U T")
        code, reran, text = self.run_main("renamed", "R")
        self.assertEqual((code, reran), (1, False))
        self.assertIn("kind T is never retried", text)

    def test_unknown_check_is_never_passed(self):
        self.record("real", "R")
        for check in ("specc", "spec ", "SPEC", "x/../spec", "../checks/spec"):
            for exit_code in (0, 1):
                with self.subTest(check=check, exit_code=exit_code):
                    code, reran, text = self.run_main(check, "R", exit_code)
                    self.assertEqual((code, reran), (1, False))
                    self.assertIn("unknown check", text)
        # A known one that passed is, registered or not.
        self.record("unregistered", "R")
        for check in ("real", "unregistered"):
            self.assertEqual(self.run_main(check, "R", 0)[0], 0)

    def test_registered_r_check_still_retries(self):
        self.record("real", "R")
        code, reran, text = self.run_main("real", "R")
        self.assertEqual((code, reran), (0, True))
        self.assertIn("Retries 10/10", text)

    def test_unreadable_check_file_unblocks_nothing(self):
        code, reran, _ = self.run_main("broken", "R")
        self.assertEqual((code, reran), (2, False))

    def test_repository_t_checks_are_known(self):
        self.assertEqual(triage.known_kinds("specs-selftest", self.evidence), {"T"})


class KindListTest(unittest.TestCase):
    def test_triage_accepts_every_evidence_kind(self):
        import evidence
        self.assertEqual(triage.KINDS, set(evidence.KINDS))

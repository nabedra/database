from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

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


class KindListTest(unittest.TestCase):
    def test_triage_accepts_every_evidence_kind(self):
        import evidence
        self.assertEqual(triage.KINDS, set(evidence.KINDS))

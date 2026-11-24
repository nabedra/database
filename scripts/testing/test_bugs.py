"""Exercise the bug-record command with valid data and planted errors."""

from contextlib import redirect_stderr, redirect_stdout
from datetime import date, timedelta
from io import StringIO
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bugs  # noqa: E402
import sim_bugs  # noqa: E402


class BugRecordsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=bugs.REPO, text=True
        ).strip()

    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.fixture = f'''id = "TEST"
title = "A reply was misclassified"
class = "uncertain-outcome"
module = "oltp"
guarantees = ["OLTP-G04"]
scenarios = ["D10"]
found_by = "acceptance"
found_date = "unknown"
lowest_regression = "D10"
seed = "unknown"
why_not_caught = "The fault window had no assertion."
invariant_added = "A lost reply leaves an uncertain outcome."
fix_commits = ["{self.sha}"]
revert_patch_ok = true
'''
        self.today = date(2026, 11, 24)
        self.open_fixture = (
            self.fixture.replace(f'"{self.sha}"', "").replace("= true", "= false")
            + 'status = "open"\nowner = "dr-queue"\nexpires = "2026-12-01"\n'
        )

    def run_fixture(self, command, fixture, repo=bugs.REPO, registers=None, today=None):
        (self.directory / "fixture.toml").write_text(fixture, encoding="utf-8")
        output, errors = StringIO(), StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            status = bugs.main(
                [command],
                directory=self.directory,
                repo=repo,
                registers=registers or bugs.REGISTER_DIR,
                today=today,
            )
        return status, output.getvalue(), errors.getvalue()

    def test_valid_record_and_markdown_counts(self):
        self.assertEqual(
            self.run_fixture("validate", self.fixture), (0, "Validated 1 bug records.\n", "")
        )
        status, output, errors = self.run_fixture("summary", self.fixture)
        self.assertEqual((status, errors), (0, ""))
        for row in ("| uncertain-outcome | 1 |", "| oltp | 1 |", "| acceptance | 1 |"):
            self.assertIn(row, output)

    def test_rejects_bad_class(self):
        bad = self.fixture.replace('"uncertain-outcome"', '"invented-class"')
        self.assert_invalid(bad, "unknown class 'invented-class'")

    def test_rejects_missing_field(self):
        bad = self.fixture.replace('seed = "unknown"\n', "")
        self.assert_invalid(bad, "missing field seed")

    def test_rejects_unknown_sha(self):
        missing_sha = "0" * 40
        self.assert_invalid(
            self.fixture.replace(self.sha, missing_sha), f"unknown Git commit SHA {missing_sha}"
        )

    def test_rejects_bad_guarantee_id(self):
        self.assert_invalid(
            self.fixture.replace("OLTP-G04", "OLTP-04"), "invalid guarantee ID 'OLTP-04'"
        )

    def test_rejects_guarantee_missing_from_registers(self):
        self.assert_invalid(
            self.fixture.replace("OLTP-G04", "OLTP-G99"),
            "guarantee ID 'OLTP-G99' is not in the guarantee registers",
        )

    def test_accepts_cache_and_lsm_register_ids(self):
        for module, guarantee in (("cache", "CACHE-G01"), ("lsm", "LSM-G01")):
            with self.subTest(module=module):
                fixture = self.fixture.replace('module = "oltp"', f'module = "{module}"')
                status, _, errors = self.run_fixture(
                    "validate", fixture.replace('"OLTP-G04"', f'"{guarantee}"')
                )
                self.assertEqual((status, errors), (0, ""))

    def test_shallow_clone_warns_instead_of_failing(self):
        clone = TemporaryDirectory()
        self.addCleanup(clone.cleanup)
        subprocess.run(
            ["git", "clone", "-q", "--no-checkout", "--depth", "1",
             f"file://{bugs.REPO}", clone.name],
            check=True,
        )
        old = "0" * 40
        status, output, errors = self.run_fixture(
            "validate", self.fixture.replace(self.sha, old), repo=Path(clone.name)
        )
        self.assertEqual((status, output), (0, "Validated 1 bug records.\n"))
        self.assertIn("1 fix commit SHAs are absent from this shallow clone", errors)
        self.assertIn("fetch-depth: 0", errors)

    def test_accepts_open_record_without_fix(self):
        self.assertEqual(
            self.run_fixture("validate", self.open_fixture, today=self.today),
            (0, "Validated 1 bug records. 1 open.\n", ""),
        )

    def test_empty_fix_commits_needs_open_status(self):
        for fixture in (
            self.fixture.replace(f'"{self.sha}"', ""),
            self.open_fixture.replace('status = "open"\n', ""),
            self.open_fixture.replace('status = "open"', 'status = "fixed"'),
        ):
            with self.subTest(fixture=fixture):
                self.assert_invalid(fixture, "fix_commits must contain at least one Git commit SHA")

    def test_open_record_needs_owner(self):
        for owner in (None, '" "', '"unknown"', '"Unassigned"', "1"):
            with self.subTest(owner=owner):
                line = "" if owner is None else f"owner = {owner}\n"
                self.assert_invalid(
                    self.open_fixture.replace('owner = "dr-queue"\n', line),
                    "an open record must name an owner",
                )

    def test_open_record_needs_expiry_date(self):
        # The last two are a TOML date and an integer.
        for expires in (None, '"unknown"', '"2026-02-30"', '"20261201"', "2026-12-01", "20261201"):
            with self.subTest(expires=expires):
                line = "" if expires is None else f"expires = {expires}\n"
                self.assert_invalid(
                    self.open_fixture.replace('expires = "2026-12-01"\n', line),
                    "an open record must have an expires date",
                )

    def test_open_record_with_fix_commits_is_invalid(self):
        for value in (self.sha, "unknown"):
            with self.subTest(value=value):
                self.assert_invalid(
                    self.open_fixture.replace("fix_commits = []", f'fix_commits = ["{value}"]'),
                    "an open record must have fix_commits = []",
                )

    def test_unknown_is_not_a_fix_commit(self):
        self.assert_invalid(
            self.fixture.replace(self.sha, "unknown"), "invalid full Git commit SHA 'unknown'"
        )

    def test_open_record_cannot_claim_a_revert_patch(self):
        self.assert_invalid(
            self.open_fixture.replace("revert_patch_ok = false", "revert_patch_ok = true"),
            "an open record must have revert_patch_ok = false",
        )

    def test_rejects_unknown_status(self):
        for status in ('"Open"', '"fixed"', "true"):
            with self.subTest(status=status):
                self.assert_invalid(self.fixture + f"status = {status}\n", "unknown status")

    def test_fixed_record_cannot_keep_open_fields(self):
        for field, value in (("owner", "dr-queue"), ("expires", "2026-12-01")):
            with self.subTest(field=field):
                self.assert_invalid(
                    self.fixture + f'{field} = "{value}"\n',
                    f'{field} is only valid on a record with status = "open"',
                )

    def test_open_record_expires_after_its_date(self):
        last_day = date(2026, 12, 1)
        self.assertEqual(self.run_fixture("validate", self.open_fixture, today=last_day)[0], 0)
        self.assert_invalid(
            self.open_fixture,
            "open record expired on 2026-12-01 (owner dr-queue)",
            today=last_day + timedelta(days=1),
        )

    def test_expiry_uses_the_current_date_by_default(self):
        today = bugs.utc_today()
        for days, diagnostic in ((-2, "open record expired on"), (2, "")):
            with self.subTest(days=days):
                expires = (today + timedelta(days=days)).isoformat()
                fixture = self.open_fixture.replace("2026-12-01", expires)
                status, _, errors = self.run_fixture("validate", fixture)
                self.assertEqual(status, 1 if diagnostic else 0)
                self.assertIn(diagnostic, errors)

    def test_summary_does_not_count_open_records_as_fixed(self):
        (self.directory / "fixed.toml").write_text(
            self.fixture.replace('id = "TEST"', 'id = "FIXED"'), encoding="utf-8"
        )
        status, output, errors = self.run_fixture("summary", self.open_fixture, today=self.today)
        self.assertEqual((status, errors), (0, ""))
        for row in (
            "Fixed records: 1",
            "Open records: 1",
            "| uncertain-outcome | 1 | 1 |",
            "| oltp | 1 | 1 |",
            "| acceptance | 1 | 1 |",
            "| TEST | dr-queue | 2026-12-01 |",
        ):
            self.assertIn(row, output.splitlines())
        self.assertNotIn("| FIXED |", output)

    def test_open_record_cannot_back_a_reverted_fix(self):
        repo = self.directory / "repo"
        records = repo / sim_bugs.RECORDS_DIR
        records.mkdir(parents=True)
        (records / "open.toml").write_text(self.open_fixture, encoding="utf-8")
        loaded, errors = bugs.load_records(records, bugs.REPO, today=self.today)
        self.assertEqual((len(loaded), errors), (1, []))
        manifest = {
            "schema": sim_bugs.SCHEMA, "id": "test", "bug": "TEST", "set": "direct", "phase": 10,
            "class": "uncertain-outcome", "guarantees": ["OLTP-G04"], "scenarios": ["D10"],
            "fix_commits": [self.sha], "reverts": self.sha, "port": "clean", "workload": "direct",
            "profile": ["lost-reply"], "expect": ["NotAppliedNeverCommitted"], "seeds": [1, 10],
            "regression": {"kind": "lib-test", "test": "tests::d10"},
        }
        patch = "diff --git a/server/src/a.rs b/server/src/a.rs\n"
        for fix_commits, diagnostic in (
            ([self.sha], f"fix_commits {[self.sha]} are not in the bug record"),
            ([], "fix_commits must be a nonempty array"),
        ):
            with self.subTest(fix_commits=fix_commits):
                bug = sim_bugs.Bug(Path("test"), dict(manifest, fix_commits=fix_commits), patch)
                problems = sim_bugs.validate_manifest(
                    bug, sim_bugs.load_bug_records(repo), {"OLTP-G04"}
                )
                self.assertTrue(any(diagnostic in problem for problem in problems), problems)

    def assert_invalid(self, fixture, diagnostic, today=None):
        status, output, errors = self.run_fixture("validate", fixture, today=today or self.today)
        self.assertEqual((status, output), (1, ""))
        self.assertIn(f"fixture.toml: {diagnostic}", errors)


if __name__ == "__main__":
    unittest.main()

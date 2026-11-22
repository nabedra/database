"""Exercise the bug-record command with valid data and planted errors."""

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

from scripts.testing import bugs


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

    def run_fixture(self, command, fixture, repo=bugs.REPO, registers=None):
        (self.directory / "fixture.toml").write_text(fixture, encoding="utf-8")
        output, errors = StringIO(), StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            status = bugs.main(
                [command],
                directory=self.directory,
                repo=repo,
                registers=registers or bugs.REGISTER_DIR,
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

    def assert_invalid(self, fixture, diagnostic):
        status, output, errors = self.run_fixture("validate", fixture)
        self.assertEqual((status, output), (1, ""))
        self.assertIn(f"fixture.toml: {diagnostic}", errors)


if __name__ == "__main__":
    unittest.main()

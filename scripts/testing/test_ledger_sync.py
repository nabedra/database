from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import ledger_sync


def git(repo, *args):
    return subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True,
    ).stdout.strip()


class LedgerSyncTests(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        root = Path(scratch.name)
        self.origin = root / "origin.git"
        self.repo = root / "checkout"
        self.repo.mkdir()
        git(root, "init", "--bare", str(self.origin))
        git(self.repo, "init", "-b", "main")
        git(self.repo, "config", "user.name", "Fixture")
        git(self.repo, "config", "user.email", "fixture@example.com")
        git(self.repo, "config", "commit.gpgsign", "false")
        git(self.repo, "remote", "add", "origin", str(self.origin))
        self.main = self.commit_readme("# Main\n")
        git(self.repo, "switch", "--orphan", "ledgers")
        self.ledger_root = self.commit_readme("# Ledgers\n")
        git(self.repo, "push", "origin", "main", "ledgers")
        git(self.repo, "switch", "main")
        self.source = self.repo / ".nabedra" / "workflows" / "testing-harness"
        self.source.mkdir(parents=True)

    def commit_readme(self, text):
        (self.repo / "README.md").write_text(text)
        git(self.repo, "add", "README.md")
        git(self.repo, "commit", "-m", "initial")
        return git(self.repo, "rev-parse", "HEAD")

    def sync(self, name="testing-harness", *, check=True):
        return subprocess.run(
            [sys.executable, ledger_sync.__file__, name, "--repo", str(self.repo)],
            capture_output=True, text=True, check=check,
        )

    def test_publishes_only_bounded_text_to_the_orphan_branch(self):
        (self.source / "ledger.md").write_text("# Evidence\n", encoding="utf-8")
        nested = self.source / "reports"
        nested.mkdir()
        (nested / "run.txt").write_text("Ran one test\n", encoding="utf-8")
        (nested / "binary.md").write_bytes(b"looks like text\0PNG")
        (nested / "non-utf8.txt").write_bytes(b"\xff")
        (nested / "picture.png").write_bytes(b"binary file")
        (nested / "large.txt").write_bytes(b"x" * (ledger_sync.MAX_FILE_BYTES + 1))
        (nested / "external.md").symlink_to(self.repo / "README.md")
        (self.source / "external-dir").symlink_to(self.repo, target_is_directory=True)

        result = self.sync()
        self.assertIn("contains binary control bytes", result.stdout)
        self.assertIn("invalid UTF-8", result.stdout)
        self.assertIn(f"exceeds {ledger_sync.MAX_FILE_BYTES} bytes", result.stdout)
        files = git(self.origin, "ls-tree", "-r", "--name-only", "ledgers").splitlines()
        self.assertEqual(files, ["README.md", "testing-harness/ledger.md", "testing-harness/reports/run.txt"])
        self.assertEqual(git(self.origin, "show", "ledgers:testing-harness/reports/run.txt"), "Ran one test")
        self.assertEqual(git(self.origin, "rev-list", "--max-parents=0", "ledgers"), self.ledger_root)
        self.assertEqual(git(self.origin, "rev-parse", "main"), self.main)
        self.assertEqual(git(self.repo, "branch", "--show-current"), "main")
        self.assertEqual(git(self.repo, "worktree", "list").count("\n"), 0)
        self.assertTrue(git(self.origin, "log", "-1", "--format=%s", "ledgers").startswith("[skip ci]"))

    def test_retries_are_noops_and_missing_local_files_survive(self):
        former = self.source / "former.md"
        former.write_text("earlier report\n")
        current = self.source / "current.md"
        current.write_text("current report\n")
        self.sync()
        tip = git(self.origin, "rev-parse", "ledgers")
        former.unlink()
        self.assertIn("is up to date", self.sync().stdout)
        self.assertEqual(git(self.origin, "rev-parse", "ledgers"), tip)
        current.write_text("updated report\n")
        self.sync()
        self.assertEqual(git(self.origin, "show", "ledgers:testing-harness/former.md"), "earlier report")
        self.assertEqual(git(self.origin, "show", "ledgers:testing-harness/current.md"), "updated report")

    def test_total_cap_rejects_sync_before_publishing(self):
        count = ledger_sync.MAX_SYNC_BYTES // ledger_sync.MAX_FILE_BYTES + 1
        for index in range(count):
            (self.source / f"{index}.txt").write_bytes(b"x" * ledger_sync.MAX_FILE_BYTES)
        result = self.sync(check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn(f"ledger sync exceeds {ledger_sync.MAX_SYNC_BYTES} bytes", result.stderr)
        self.assertEqual(git(self.origin, "rev-parse", "ledgers"), self.ledger_root)

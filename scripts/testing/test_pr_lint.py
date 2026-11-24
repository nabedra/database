from contextlib import redirect_stderr, redirect_stdout
import io
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import pr_lint


VALID = (
    "What changed\n\n"
    "Guarantees: none\nEvidence: fixture audit\nNot run: Docker benchmark\n"
    "Reviewers: pending cross-model review\n"
)


class PullRequestLintTests(unittest.TestCase):
    def test_reads_file_environment_and_stdin(self):
        with TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            path = Path(directory) / "body.md"
            path.write_text(VALID)
            with patch.dict(os.environ, {"PR_BODY": ""}):
                self.assertEqual(pr_lint.main([str(path)]), 0)
            with patch.dict(os.environ, {"PR_BODY": VALID}):
                self.assertEqual(pr_lint.main([]), 0)
            with patch.dict(os.environ, clear=True), patch("sys.stdin", io.StringIO(VALID)):
                self.assertEqual(pr_lint.main([]), 0)

    def test_missing_empty_and_none_reported_on_docs_only_pr(self):
        body = VALID.replace("Evidence: fixture audit", "Evidence:\t ")
        body = body.replace("Reviewers: pending cross-model review", "Reviewers: NoNe")
        body = body.replace("Not run: Docker benchmark\n", "")
        with patch.dict(os.environ, {"PR_BODY": body}), redirect_stderr(io.StringIO()) as error:
            self.assertEqual(pr_lint.main(["--docs-only"]), 1)
        self.assertIn("Evidence: requires a value", error.getvalue())
        self.assertIn("Missing Not run: line", error.getvalue())
        self.assertIn("'none' is invalid", error.getvalue())

    def test_template_inside_a_fence_does_not_satisfy_requirements(self):
        self.assertEqual(len(pr_lint.lint("~~~markdown\n" + VALID + "~~~\n")), 4)


RUN = "https://github.com/zPy52/nabedra-database/actions/runs/123"


class DesignLineTests(unittest.TestCase):
    def test_design_line_values(self):
        good = [
            "none: docs only",
            f"specs/oltp/DirectPath.tla {RUN} pass",
            f"specs/cache/Lease.tla: {RUN} (pass), trace {RUN} not-run",
        ]
        for value in good:
            with self.subTest(value=value):
                self.assertEqual(pr_lint.lint(VALID + f"Design: {value}\n", "x"), [])
        bad = {
            "none": "none: <reason>",
            "None:  ": "none: <reason>",
            "specs/oltp/DirectPath.tla pass": "run URLs",
            f"{RUN} pass": "spec paths",
            f"specs/oltp/X.tla {RUN}": "verdict",
        }
        for value, fragment in bad.items():
            with self.subTest(value=value):
                errors = pr_lint.lint(VALID + f"Design: {value}\n")
                self.assertTrue(any(fragment in e for e in errors), errors)

    def test_required_only_with_a_reason(self):
        self.assertEqual(pr_lint.lint(VALID), [])
        errors = pr_lint.lint(VALID, "the pull request changes specs/x/X.tla")
        self.assertEqual(len(errors), 1)
        self.assertIn("Missing Design: line (the pull request changes specs/x/X.tla)", errors[0])

    def test_design_reason(self):
        modules = pr_lint.high_level_modules()
        self.assertIn("specs/", pr_lint.design_reason(["specs/oltp/A.tla"], [], modules))
        self.assertIn("plans/oltp-direct-path.md", pr_lint.design_reason([], ["plans/oltp-direct-path.md"], modules))
        reason = pr_lint.design_reason(["server/src/oltp_runtime/direct/executor.rs"], [], modules)
        self.assertIn("L3 module oltp", reason)
        self.assertIsNone(pr_lint.design_reason(["scripts/testing/README.md", "README.md"], [], modules))
        # Markdown under an L2 module path is documentation, not a design change.
        self.assertIsNone(pr_lint.design_reason(["server/events/README.md", "docs/cache/overview.md"], [], modules))
        self.assertIn("L2 module events", pr_lint.design_reason(["server/events/src/raft/mod.rs"], [], modules))
        self.assertIn("L3 module lsm", pr_lint.design_reason(["docs/x.md", "server/kv/src/lib.rs"], [], modules))
        self.assertIsNone(pr_lint.design_reason([], [], modules))

    def test_design_section(self):
        plan = "# Plan\n\n## Goal\n\nx\n\n## Design review\n\n| a | b |\n\n### Sub\n\ny\n\n## Next\n\nz\n"
        self.assertEqual(pr_lint.design_section(plan), "## Design review\n\n| a | b |\n\n### Sub\n\ny")
        self.assertIsNone(pr_lint.design_section("# Plan\n\n## Goal\n"))

    def test_changed_files_option(self):
        with TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            changed = Path(directory) / "changed.txt"
            changed.write_text("specs/selftest/TwoPhase.tla\n")
            with patch.dict(os.environ, {"PR_BODY": VALID}), redirect_stderr(io.StringIO()) as error:
                self.assertEqual(pr_lint.main(["--changed-files", str(changed)]), 1)
            self.assertIn("Missing Design: line", error.getvalue())
            body = VALID + f"Design: specs/selftest/TwoPhase.tla {RUN} pass\n"
            with patch.dict(os.environ, {"PR_BODY": body}):
                self.assertEqual(pr_lint.main(["--changed-files", str(changed)]), 0)

    def test_git_range(self):
        root = Path(__file__).resolve().parents[2]
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True)
        if head.returncode != 0:
            self.skipTest("not a git checkout")
        files, sections = pr_lint.changed_from_git(head.stdout.strip(), head.stdout.strip())
        self.assertEqual((files, sections), ([], []))

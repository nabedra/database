from contextlib import redirect_stderr, redirect_stdout
import io
import os
from pathlib import Path
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

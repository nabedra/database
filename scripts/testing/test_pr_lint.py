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

    def test_design_documents_are_not_exempt(self):
        modules = pr_lint.high_level_modules()
        for path in ("server/oltp-storage/src/oltp/DESIGN.md", "server/oltp-storage/src/oltp/pool/DESIGN.md",
                     "server/events/design-notes.md"):
            with self.subTest(path=path):
                self.assertIn(path, pr_lint.design_reason([path], [], modules) or "")
        # Every tracked DESIGN.md under an L2 or L3 path is covered, and specs/** has no Markdown exemption.
        self.assertIn("specs/oltp/NOTES.md", pr_lint.design_reason(["specs/oltp/NOTES.md"], [], modules))
        self.assertIsNone(pr_lint.design_reason(["docs/design.md"], [], modules))

    def test_design_section(self):
        plan = "# Plan\n\n## Goal\n\nx\n\n## Design review\n\n| a | b |\n\n### Sub\n\ny\n\n## Next\n\nz\n"
        self.assertEqual(pr_lint.design_section(plan), "## Design review\n\n| a | b |\n\n### Sub\n\ny")
        self.assertIsNone(pr_lint.design_section("# Plan\n\n## Goal\n"))

    def test_design_section_sees_past_fences(self):
        plan = ("# P\n\n## Design review\n\nRun it:\n\n```sh\n# exhaustive\ntlc X\n```\n\n"
                "Verdict: not-run\n\n## Next\n\nz\n")
        changed = plan.replace("Verdict: not-run", "Verdict: pass")
        self.assertIn("Verdict: not-run", pr_lint.design_section(plan))
        self.assertNotEqual(pr_lint.design_section(plan), pr_lint.design_section(changed))
        self.assertNotIn("z", pr_lint.design_section(plan).splitlines())
        # A change outside the section, and a heading inside a fence, are not the section's.
        self.assertEqual(pr_lint.design_section(plan), pr_lint.design_section(plan.replace("\nz\n", "\ny\n")))
        self.assertIsNone(pr_lint.design_section("# P\n\n~~~md\n## Design review\nx\n~~~\n"))

    def test_no_reading_of_a_fence_hides_a_change(self):
        body = "\n## Design review\n\nVerdict: not-run\n\n## Next\n"
        before = {
            "inline code of three backticks": "```tlc X```\n",
            "a fence closed at four spaces in a list": "- a\n\n  ```\n  x\n    ```\n",
            "a fence that never closes": "```\nx\n",
            "a fence inside a deeper list": "1. a\n   - b\n\n     ```\n     # x\n     ```\n",
            "a closing fence with text": "```\nx\n``` y\n# z\n```\n",
            "tildes around backticks": "~~~\n```\n~~~\n",
            "a longer fence around a shorter one": "````\n```\n````\n",
        }
        for name, text in before.items():
            with self.subTest(name):
                plan = "# P\n\n" + text + body
                sections = pr_lint.design_sections(plan)
                self.assertNotEqual(sections, pr_lint.design_sections(plan.replace("not-run", "pass")))
                # A change after the section is not one of its own, as far as the fence lets the reader tell.
                if "never" not in name and "deeper" not in name:
                    self.assertEqual(sections, pr_lint.design_sections(plan + "\nmore\n"))
        for fences in ("strict", "loose"):
            with self.subTest(fences):
                text = "# P\n\n~~~\n```\n~~~\n" + body
                self.assertEqual(pr_lint.design_section(text, fences), "## Design review\n\nVerdict: not-run")
                text = "# P\n\n```tlc X```\n" + body
                self.assertEqual(pr_lint.design_section(text, fences), "## Design review\n\nVerdict: not-run")
        # Inside the section, a fence that the strict reading never closes does not end it early either.
        inside = "# P\n## Design review\n- a\n\n  ```\n  x\n    ```\n# z\n"
        self.assertNotEqual(pr_lint.design_sections(inside + "Verdict: not-run\n"),
                            pr_lint.design_sections(inside + "Verdict: pass\n"))

    def test_every_form_and_every_section_counts(self):
        for heading in ("## Design Review", "## design review", "## Design review: direct path",
                        "### Design review", "## 7. Design review", "## Design review (2026-11-24)",
                        "   ## Design review ##"):
            with self.subTest(heading=heading):
                self.assertIsNotNone(pr_lint.design_section(f"# P\n{heading}\nx\n"))
        for heading in ("## Design reviews", "## Redesign review", "# Design review", "Design review"):
            with self.subTest(heading=heading):
                self.assertIsNone(pr_lint.design_section(f"# P\n{heading}\nx\n"))
        two = "## Design review\na\n## X\nm\n## Design review\nb\n"
        self.assertEqual(pr_lint.design_section(two), "## Design review\na\n## Design review\nb")
        nested = "## 2. Protocol\n### Design review\na\n#### Sub\nb\n### Other\nc\n"
        self.assertEqual(pr_lint.design_section(nested), "### Design review\na\n#### Sub\nb")

    def test_changed_from_git_reports_a_change_after_a_fenced_comment(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            git = ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"]
            plan = root / "plans" / "p.md"
            plan.parent.mkdir()
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            shas = []
            for verdict in ("not-run", "pass"):
                plan.write_text(f"# P\n## Design review\n```sh\n# x\n```\nVerdict: {verdict}\n## Next\n")
                subprocess.run(["git", "add", "-A"], cwd=root, check=True)
                subprocess.run([*git, "commit", "-qm", verdict], cwd=root, check=True)
                shas.append(subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True,
                                           text=True, check=True).stdout.strip())
            with patch.object(pr_lint, "REPO_ROOT", root):
                self.assertEqual(pr_lint.changed_from_git(*shas), (["plans/p.md"], ["plans/p.md"]))
            # The same through a fence that only the loose reading closes.
            for verdict in ("not-run", "pass"):
                plan.write_text(f"# P\n- a\n\n  ```\n  x\n    ```\n## Design review\nVerdict: {verdict}\n")
                subprocess.run([*git, "commit", "-qam", verdict], cwd=root, check=True)
                shas.append(subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True,
                                           text=True, check=True).stdout.strip())
            with patch.object(pr_lint, "REPO_ROOT", root):
                self.assertEqual(pr_lint.changed_from_git(*shas[2:]), (["plans/p.md"], ["plans/p.md"]))

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

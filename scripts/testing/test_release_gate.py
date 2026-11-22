#!/usr/bin/env python3
import contextlib
import io
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_gate  # noqa: E402
from test_matrix import FIXTURES, MatrixFixtureTests  # noqa: E402


def run(fx, *extra):
    args = ["--repo", str(fx.repo), "--registers-dir", str(fx.registers), "--evidence-dir", str(fx.records),
            "--checks-dir", str(fx.checks), "--modules", str(fx.modules), "--sha", fx.head, *extra]
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = release_gate.main(args)
    return code, out.getvalue()


class ReleaseGateTests(MatrixFixtureTests):
    def test_dry_run_on_fixture_with_failing_cell_fails(self):
        code, out = run(self.fx, "--dry-run", "--allow-unattested")
        self.assertEqual(code, 1)
        self.assertIn("Release gate: FAILED", out)
        self.assertIn("- KV-G02 U: fail", out)
        self.assertIn("### Deferred checks", out)
        self.assertIn("- KV-G01 R: cloud-deferred (EKS soak)", out)
        self.assertIn("- KV-G01 P: hardware-deferred (absolute latency)", out)

    def test_passing_scope_writes_notes(self):
        notes = Path(self.fx.tmp.name) / "notes.md"
        code, out = run(self.fx, "--registers-dir", str(FIXTURES / "registers-events"),
                        "--allow-unattested", "--notes", str(notes))
        self.assertEqual(code, 0, out)
        self.assertIn("Release gate: passed", notes.read_text())
        self.assertIn("None.", notes.read_text())

    def test_release_gate_verifies_attestations_by_default(self):
        with patch.object(release_gate.matrix, "verify_attestation", return_value=False) as verify:
            code, out = run(self.fx, "--registers-dir", str(FIXTURES / "registers-events"), "--dry-run")
        verify.assert_called()
        self.assertEqual(code, 1)
        self.assertIn("missing", out)


if __name__ == "__main__":
    unittest.main()

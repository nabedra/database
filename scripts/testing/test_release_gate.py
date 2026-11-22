#!/usr/bin/env python3
import contextlib
import io
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_gate  # noqa: E402
from test_matrix import FixtureRepo  # noqa: E402


def run(fx, *extra):
    args = ["--repo", str(fx.repo), "--registers-dir", str(fx.registers), "--evidence-dir", str(fx.records),
            "--checks-dir", str(fx.checks), "--modules", str(fx.modules), "--sha", fx.head, *extra]
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = release_gate.main(args)
    return code, out.getvalue()


class ReleaseGateTests(unittest.TestCase):
    def test_dry_run_on_fixture_with_failing_cell_fails(self):
        fx = FixtureRepo()
        try:
            code, out = run(fx, "--dry-run", "--allow-unattested")
            self.assertEqual(code, 1)
            self.assertIn("Release gate: FAILED", out)
            self.assertIn("- KV-G02 U: fail", out)
            self.assertIn("### Deferred checks", out)
            self.assertIn("- KV-G01 R: cloud-deferred (EKS soak)", out)
        finally:
            fx.cleanup()

    def test_passing_scope_writes_notes(self):
        fx = FixtureRepo(registers="registers-events")
        try:
            notes = Path(fx.tmp.name) / "notes.md"
            code, out = run(fx, "--allow-unattested", "--notes", str(notes))
            self.assertEqual(code, 0, out)
            self.assertIn("Release gate: passed", notes.read_text())
            self.assertIn("None.", notes.read_text())
        finally:
            fx.cleanup()

    def test_release_gate_verifies_attestations_by_default(self):
        fx = FixtureRepo(registers="registers-events")
        try:
            # No --allow-unattested: the gate calls `gh attestation verify`, here a stub that
            # rejects every record, so nothing counts.
            bindir = Path(fx.tmp.name) / "bin"
            bindir.mkdir()
            (bindir / "gh").write_text(f"#!/bin/sh\necho \"$@\" >> {bindir}/calls\nexit 1\n")
            (bindir / "gh").chmod(0o755)
            old = os.environ["PATH"]
            os.environ["PATH"] = f"{bindir}{os.pathsep}{old}"
            try:
                code, out = run(fx, "--dry-run")
            finally:
                os.environ["PATH"] = old
            self.assertIn("attestation verify", (bindir / "calls").read_text())
            self.assertEqual(code, 1)
            self.assertIn("missing", out)
        finally:
            fx.cleanup()


if __name__ == "__main__":
    unittest.main()

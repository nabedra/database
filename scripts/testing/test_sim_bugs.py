"""Exercise sim_bugs.py on a throwaway Git repository with planted errors and a fake cargo."""

from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import textwrap
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_check  # noqa: E402
import sim_bugs  # noqa: E402

LIB = """pub fn answer(uncertain: bool) -> &'static str {
    if uncertain {
        "unknown"
    } else {
        "not_applied"
    }
}
"""
FIXED = LIB.replace("    if uncertain {", "    // keep the uncertainty\n    if uncertain {").replace(
    '"not_applied"', '"refused"  // fixed')
TESTS = "#[test]\nfn d02_fix_one_keeps_the_outcome() {}\n"
REGISTER = "| ID | Guarantee |\n| --- | --- |\n| OLTP-G03 | One outcome |\n| OLTP-G04 | Unknown stays unknown |\n"

# A fake cargo: `test --no-run` writes a fake library test executable that passes when the tree
# it runs in contains the fix, and fails otherwise; `build` does nothing.
FAKE_CARGO = '''
import json, os, sys
target = os.environ["CARGO_TARGET_DIR"]
if "--no-run" in sys.argv:
    exe = os.path.join(target, "fake-lib-test")
    os.makedirs(target, exist_ok=True)
    with open(exe, "w") as f:
        f.write("""#!/usr/bin/env python3
import sys
name = sys.argv[sys.argv.index("--exact") + 1]
fixed = "fixed" in open("src/lib.rs").read()
print(f"test {name} ... " + ("ok" if fixed else "FAILED"))
if not fixed:
    print("thread 'main' panicked: assertion failed: lost the uncertain outcome")
print("test result: " + ("ok. 1 passed; 0 failed" if fixed else "FAILED. 0 passed; 1 failed")
      + "; 0 ignored; 0 measured; 9 filtered out")
sys.exit(0 if fixed else 101)
""")
    os.chmod(exe, 0o755)
    print(json.dumps({"reason": "compiler-artifact", "profile": {"test": True},
                      "target": {"kind": ["lib"]}, "executable": exe}))
'''


def sh(repo, *args):
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True).stdout


class Fixture:
    def __init__(self, root):
        self.repo = root / "repo"
        self.repo.mkdir()
        sh(self.repo, "init", "-q", "-b", "main")
        sh(self.repo, "config", "user.email", "test@example.invalid")
        sh(self.repo, "config", "user.name", "test")
        self.write("server/src/lib.rs", LIB)
        self.write("server/src/tests.rs", TESTS)
        self.write("docs/testing/guarantees/oltp.md", REGISTER)
        self.commit("base")
        self.write("server/src/lib.rs", FIXED)
        self.fix = self.commit("fix")
        self.write("docs/testing/bugs/2026-11-23-fix-one.toml", textwrap.dedent(f'''
            id = "FIX-ONE"
            class = "uncertain-outcome"
            guarantees = ["OLTP-G03", "OLTP-G04"]
            scenarios = ["D02", "O10"]
            fix_commits = ["{self.fix}"]
        '''))
        self.patch = sh(self.repo, "diff", self.fix, f"{self.fix}^", "--", "server/src/lib.rs")
        self.write_bug()

    def write(self, name, text):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def commit(self, message):
        sh(self.repo, "add", "-A")
        sh(self.repo, "commit", "-q", "-m", message)
        return sh(self.repo, "rev-parse", "HEAD").strip()

    def manifest(self, port="clean", regression=None, **extra):
        regression = regression or {"kind": "lib-test", "test": "tests::d02_fix_one_keeps_the_outcome"}
        lines = [
            'schema = "nabedra.sim-bug/1"', 'id = "fix-one"', 'bug = "FIX-ONE"', 'set = "direct"',
            "phase = 10", 'class = "uncertain-outcome"', 'guarantees = ["OLTP-G03"]',
            'scenarios = ["D02"]', f'fix_commits = ["{self.fix}"]', f'reverts = "{self.fix}"',
            f'port = "{port}"', 'port_notes = "the one hunk"', 'workload = "direct"',
            'profile = ["lost-reply"]', 'expect = ["NotAppliedNeverCommitted"]', "seeds = [1, 10000]",
        ]
        lines += [f"{key} = {json.dumps(value)}" for key, value in extra.items()]
        lines.append("[regression]")
        lines += [f"{key} = {json.dumps(value)}" for key, value in regression.items()
                  if not isinstance(value, dict)]
        for key, value in regression.items():
            if isinstance(value, dict):
                lines.append(f"[regression.{key}]")
                for side, run in value.items():
                    lines.append(f"{side} = {json.dumps(run)}" if not isinstance(run, dict) else
                                 f"{side} = {{ " + ", ".join(f"{k} = {json.dumps(v)}" for k, v in run.items()) + " }")
        return "\n".join(lines) + "\n"

    def write_bug(self, manifest=None, patch=None):
        self.write("server/sim-oltp/bugs/fix-one/bug.toml", manifest or self.manifest())
        self.write("server/sim-oltp/bugs/fix-one/revert.patch", self.patch if patch is None else patch)

    def run(self, *argv):
        out = StringIO()
        status = sim_bugs.main(list(argv), repo=self.repo, out=out)
        return status, out.getvalue()

    def evidence(self, digest=None):
        run = {"run": "https://example.invalid/runs/1", "sha": "a" * 40}
        return {"kind": "lib-test", "test": "tests::d02_fix_one_keeps_the_outcome",
                "evidence": {"digest": digest or sim_bugs.digest(self.patch),
                             "patched": dict(run, result="failed"), "clean": dict(run, result="passed")}}


class SimBugsTest(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.f = Fixture(self.root)

    def assert_check(self, expected_status, *needles):
        status, output = self.f.run("check")
        self.assertEqual(status, expected_status, output)
        for needle in needles:
            self.assertIn(needle, output)
        return output

    def test_clean_port_validates_and_applies(self):
        self.assert_check(0, "fix-one: ok (clean port", "1 of 1 patches valid")

    def test_patch_rot_on_head_fails(self):
        self.f.write("server/src/lib.rs", FIXED.replace('"unknown"', '"outcome_unknown"'))
        self.f.commit("product change")
        self.assert_check(1, "no longer applies to HEAD")

    def test_apply_check_uses_head_not_the_working_tree(self):
        # An uncommitted edit in the shipped tree neither breaks nor satisfies the check.
        self.f.write("server/src/lib.rs", "garbage\n")
        self.assert_check(0)
        self.assertEqual((self.f.repo / "server/src/lib.rs").read_text(), "garbage\n")

    def test_clean_claim_with_other_lines_fails(self):
        self.f.write_bug(patch=extra_line(self.f.patch))
        self.assert_check(1, "port = clean, but the patch does not change exactly the lines")

    def test_clean_claim_must_cover_every_product_file_of_the_fix(self):
        f = self.f
        f.write("server/src/other.rs", "pub fn other() {}\n")
        f.write("server/src/tests.rs", TESTS + "// a test hunk stays\n")
        f.write("server/src/lib.rs", FIXED.replace("// fixed", "// fixed again"))
        fix = f.commit("a fix over two product files and a test")
        record = f.repo / "docs/testing/bugs/2026-11-23-fix-one.toml"
        record.write_text(record.read_text().replace(f'["{f.fix}"]', f'["{f.fix}", "{fix}"]'))
        manifest = f.manifest().replace(f'reverts = "{f.fix}"', f'reverts = "{fix}"').replace(
            f'fix_commits = ["{f.fix}"]', f'fix_commits = ["{f.fix}", "{fix}"]')
        f.write_bug(manifest, sh(f.repo, "diff", fix, f"{fix}^", "--", "server/src/lib.rs"))
        self.assert_check(1, "leaves out product files the fix changed: server/src/other.rs")
        f.write_bug(manifest, sh(f.repo, "diff", fix, f"{fix}^", "--", "server/src/lib.rs",
                                 "server/src/other.rs"))
        self.assert_check(0, "fix-one: ok (clean port")

    def test_hand_port_needs_matching_evidence(self):
        self.f.write_bug(self.f.manifest(port="hand"))
        self.assert_check(1, "a hand port counts only with regression evidence")
        self.f.write_bug(self.f.manifest(port="hand", regression=self.f.evidence()))
        self.assert_check(0)
        self.f.write_bug(self.f.manifest(port="hand", regression=self.f.evidence(digest="0" * 64)))
        self.assert_check(1, "differ from those its regression evidence used")

    def test_evidence_must_show_the_patched_test_failing(self):
        regression = self.f.evidence()
        regression["evidence"]["patched"]["result"] = "passed"
        self.f.write_bug(self.f.manifest(port="hand", regression=regression))
        self.assert_check(1, "regression.evidence.patched.result must be 'failed'")

    def test_test_hunks_and_harness_paths_are_rejected(self):
        for name in ("server/src/tests.rs", "server/src/direct/tests/fixa.rs", "server/tests/api.rs",
                     "server/sim-oltp/src/lib.rs", "scripts/x.py", "server/Cargo.toml"):
            patch = self.f.patch.replace("server/src/lib.rs", name)
            self.assertTrue(sim_bugs.validate_patch_paths(patch), name)
        self.assertEqual(sim_bugs.validate_patch_paths(self.f.patch), [])

    def test_cross_references_to_the_bug_record(self):
        self.f.write_bug(self.f.manifest().replace('bug = "FIX-ONE"', 'bug = "NOPE"'))
        self.assert_check(1, "has no record")
        self.f.write_bug(self.f.manifest().replace('scenarios = ["D02"]', 'scenarios = ["D02", "O26"]'))
        self.assert_check(1, "scenarios ['O26'] are not in the bug record")
        self.f.write_bug(self.f.manifest().replace('guarantees = ["OLTP-G03"]', 'guarantees = ["OLTP-G99"]'))
        self.assert_check(1, "OLTP-G99 is not in the guarantee registers")
        self.f.write_bug(self.f.manifest().replace('class = "uncertain-outcome"', 'class = "other"'))
        self.assert_check(1, "differs from the bug record")

    def test_unknown_fields_shas_and_missing_tests(self):
        self.f.write_bug(self.f.manifest(surprise="x"))
        self.assert_check(1, "unknown field surprise")
        self.f.write_bug(self.f.manifest().replace(f'reverts = "{self.f.fix}"', f'reverts = "{"b" * 40}"'))
        self.assert_check(1, "reverts must be one of fix_commits")
        regression = {"kind": "lib-test", "test": "tests::gone"}
        self.f.write_bug(self.f.manifest(regression=regression))
        self.assert_check(1, "regression test gone is not defined at HEAD")
        self.f.write_bug(self.f.manifest(regression={"kind": "acceptance", "scenario": "D02"}))
        self.assert_check(1, "regression.scenario must be an acceptance scenario")

    def test_digest_ignores_context_and_positions(self):
        moved = self.f.patch.replace("@@ -1,", "@@ -40,").replace(" pub fn answer", " pub fn answer2")
        self.assertEqual(sim_bugs.digest(moved), sim_bugs.digest(self.f.patch))
        self.assertNotEqual(sim_bugs.digest(extra_line(self.f.patch)), sim_bugs.digest(self.f.patch))

    def test_run_is_not_run_until_the_harness_exists(self):
        status, output = self.f.run("run", "--set", "direct")
        self.assertEqual(status, 0)
        self.assertIn(sim_bugs.NOT_RUN_MARKER, output)
        check = run_check.parse_check(tomllib_load(Path(__file__).resolve().parents[2]
                                                   / "docs/testing/checks/oltp-sim-bugs-direct.toml"))
        result, parsed = run_check.classify(check.result_parser, output, 0, False, check.not_run_marker)
        self.assertEqual((result, parsed.executed), ("not-run", 0))

    def test_run_with_a_harness_never_passes_without_seed_lines(self):
        self.f.write("server/sim-oltp/Cargo.toml", "[package]\n")
        self.f.commit("install the test harness manifest")
        fake = self.root / "silent_cargo.py"
        fake.write_text("print('test result: ok. 1 passed; 0 failed;')\n")
        status, output = self.f.run("run", "--cargo", f"{sys.executable} {fake}", "--seeds", "3",
                                    "--target-dir", str(self.root / "target"))
        self.assertEqual(status, 1, output)
        self.assertIn("fix-one: NOT found in [1, 3] (no seed line", output)
        self.assertIn("0 of 1 reverted fixes found", output)

    def test_run_rejects_out_of_budget_and_duplicate_seeds(self):
        self.f.write("server/sim-oltp/Cargo.toml", "[package]\n")
        self.f.commit("install the test harness manifest")
        hit = dict(seed=1, result="violation", kinds=["NotAppliedNeverCommitted"])
        for entries in ([dict(hit, seed=4)], [hit, hit]):
            with self.subTest(entries=entries), patch.object(sim_bugs, "sweep", return_value=(0, entries)) as sweep:
                status, output = self.f.run("run", "--seeds", "3")
                self.assertEqual(status, 1, output)
                self.assertIn("duplicate or out-of-budget seed lines", output)
                self.assertEqual(sweep.call_count, 1)

    def test_run_requires_a_successful_clean_replay(self):
        self.f.write("server/sim-oltp/Cargo.toml", "[package]\n")
        self.f.commit("install the test harness manifest")
        hit = dict(seed=1, result="violation", kinds=["NotAppliedNeverCommitted"])
        clean = dict(seed=1, result="pass", kinds=[])
        for code, expected_status in ((101, 1), (0, 0)):
            with self.subTest(code=code), patch.object(sim_bugs, "sweep", side_effect=[(0, [hit]), (code, [clean])]):
                status, output = self.f.run("run", "--seeds", "3")
                self.assertEqual(status, expected_status, output)

    def test_seed_lines_and_first_match(self):
        output = "\n".join([
            'nabedra-sim-seed: {"seed": 1, "result": "pass", "kinds": []}',
            'nabedra-sim-seed: {"seed": 2, "result": "violation", "kinds": ["Other"]}',
            'nabedra-sim-seed: {"seed": 99, "result": "violation", "kinds": "Other"}',
            "nabedra-sim-seed: not json",
            'nabedra-sim-seed: {"seed": 3, "result": "violation", "kinds": ["Liveness"]}',
        ])
        seeds = sim_bugs.parse_seeds(output)
        self.assertEqual([s["seed"] for s in seeds], [1, 2, 3])
        self.assertEqual(sim_bugs.first_match(seeds, ["Liveness"])["seed"], 3)
        self.assertIsNone(sim_bugs.first_match(seeds, ["NotAppliedNeverCommitted"]))

    def test_libtest_classification(self):
        name = "a::b"
        ok = "test a::b ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out"
        bad = "test a::b ... FAILED\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out"
        self.assertEqual(sim_bugs.classify_libtest(0, ok, name), "passed")
        self.assertEqual(sim_bugs.classify_libtest(101, bad, name), "failed")
        self.assertEqual(sim_bugs.classify_libtest(None, "", name), "timeout")
        self.assertEqual(sim_bugs.classify_libtest(101, "error[E0425]: cannot find value", name), "not-run")
        self.assertEqual(sim_bugs.classify_libtest(0, ok.replace("1 passed", "0 passed"), name), "not-run")
        self.assertEqual(sim_bugs.classify_libtest(101, bad.replace("a::b", "a::c"), name), "not-run")

    def test_verify_applies_only_in_a_scratch_worktree(self):
        fake = self.root / "fake_cargo.py"
        fake.write_text(FAKE_CARGO)
        before = (self.f.repo / "server/src/lib.rs").read_text()
        args = ["verify", "--cargo", f"{sys.executable} {fake}", "--target-dir", str(self.root / "target")]
        status, output = self.f.run(*args)
        self.assertEqual(status, 0, output)
        self.assertIn("fix-one: without the patch passed", output)
        self.assertIn("fix-one: with the patch failed; verified", output)
        self.assertEqual((self.f.repo / "server/src/lib.rs").read_text(), before)
        self.assertEqual(sh(self.f.repo, "status", "--porcelain", "--", "server/src"), "")
        self.assertEqual(len(sh(self.f.repo, "worktree", "list").splitlines()), 1)
        # A failure that does not carry the expected text is not the original failure.
        regression = {"kind": "lib-test", "test": "tests::d02_fix_one_keeps_the_outcome",
                      "failure_contains": ["some other assertion"]}
        self.f.write_bug(self.f.manifest(regression=regression))
        status, output = self.f.run(*args)
        self.assertEqual(status, 1, output)
        self.assertIn("with the patch failed-otherwise; NOT verified", output)

    def test_attempts_for_a_timing_window(self):
        bug = sim_bugs.Bug(Path("x"), {"id": "x"}, "")
        bug.manifest["regression"] = {"kind": "lib-test", "test": "a::b", "attempts": 3,
                                      "failure_contains": ["expected"]}
        outcomes = []

        def fake(*_):
            return outcomes.pop(0)

        original = sim_bugs.run_regression
        sim_bugs.run_regression = fake
        self.addCleanup(setattr, sim_bugs, "run_regression", original)
        verdict = lambda patched: sim_bugs.regression_verdict(bug, None, None, None, {}, None, patched)[0]
        outcomes[:] = [("passed", ""), ("failed", "the expected text")]
        self.assertEqual(verdict(True), "failed")
        outcomes[:] = [("passed", ""), ("passed", ""), ("passed", "")]
        self.assertEqual(verdict(True), "passed")
        outcomes[:] = [("failed", "other"), ("passed", ""), ("passed", "")]
        self.assertEqual(verdict(True), "failed-otherwise")
        outcomes[:] = [("passed", ""), ("failed", "x")]
        self.assertEqual(verdict(False), "failed")
        outcomes[:] = [("passed", "")] * 3
        self.assertEqual(verdict(False), "passed")

    def test_scratch_refuses_the_shipped_tree(self):
        scratch = sim_bugs.Scratch(self.f.repo)
        scratch.path = self.f.repo
        with self.assertRaises(sim_bugs.Failure):
            scratch.__enter__()

    def test_repository_patches_validate(self):
        # The real manifests: fields and cross-references, without Git (fast and history-free).
        repo = sim_bugs.REPO
        bugs, errors = sim_bugs.load_bugs(repo)
        self.assertEqual(errors, [])
        records, known = sim_bugs.load_bug_records(repo), sim_bugs.guarantee_ids(repo)
        for bug in bugs:
            self.assertEqual(sim_bugs.validate_manifest(bug, records, known), [], bug.id)
            evidence = bug.regression.get("evidence")
            if evidence:
                self.assertEqual(evidence["digest"], sim_bugs.digest(bug.patch), bug.id)
                # The historical failure text can differ from the current regression's signature.
                self.assertIn("test result: FAILED. 0 passed; 1 failed", evidence["patched"]["output"], bug.id)


def extra_line(patch):
    """The patch with one more added line after its first changed line."""
    lines = patch.splitlines(keepends=True)
    first = next(i for i, line in enumerate(lines)
                 if line[:1] in "+-" and not line.startswith(("+++", "---")))
    lines.insert(first + 1, "+    // an extra change\n")
    return "".join(lines)


def tomllib_load(path):
    import tomllib
    with path.open("rb") as stream:
        return tomllib.load(stream)


if __name__ == "__main__":
    unittest.main()

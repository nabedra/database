"""Exercise sim_bugs.py on a throwaway Git repository with planted errors and a fake cargo."""

import hashlib
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

# A fake cargo for `run`: `test --no-run` writes a fake nabedra-sim-oltp sweep test that follows the
# harness interface (environment in, seed lines and sweep-report.json out). The bug shows when the
# tree it runs in lacks the fix, from seed FAKE_HIT on. FAKE_* variables plant tooling faults.
FAKE_SIM_CARGO = '''
import json, os, sys
target = os.environ["CARGO_TARGET_DIR"]
if "--no-run" in sys.argv:
    exe = os.path.join(target, "release", "deps", "nabedra_sim_oltp-fake")
    os.makedirs(os.path.dirname(exe), exist_ok=True)
    with open(exe, "w") as f:
        f.write(open(os.environ["FAKE_SWEEP"]).read())
    os.chmod(exe, 0o755)
    print(json.dumps({"reason": "compiler-artifact", "profile": {"test": True},
                      "target": {"kind": ["lib"], "name": "nabedra_sim_oltp"}, "executable": exe}))
'''
FAKE_SWEEP = '''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
env = os.environ
assert sys.argv[1:] == ["sweep::tests::sweep", "--exact", "--nocapture", "--test-threads=1"], sys.argv
assert env["RUST_MIN_STACK"] == "16777216" and "--cfg tokio_unstable" in env["RUSTFLAGS"]
workload = env["NABEDRA_SIM_WORKLOAD"]
if workload not in ("direct", "smoke"):
    # The text of the real test: `expect` prints the error with its quotes escaped.
    print("OLTP smoke sweep report: " + json.dumps(f'OLTP workload "{workload}" is unavailable; use smoke'))
    sys.exit(101)
if workload == "smoke" and "NABEDRA_SIM_PROFILE" in env:
    sys.exit(101)
first, count = int(env["NABEDRA_SEED"]), int(env["NABEDRA_SEEDS"])
patched = "fixed" not in Path("../src/lib.rs").read_text()
with open(env["FAKE_CALLS"], "a") as calls:
    calls.write(json.dumps({"patched": patched, "first": first, "count": count,
                            "expect": env.get("NABEDRA_SIM_EXPECT"), "defect": env.get("NABEDRA_SIM_DEFECT"),
                            "profile": env.get("NABEDRA_SIM_PROFILE"), "workload": workload}) + "\\n")
kind = env.get("NABEDRA_SIM_EXPECT", "NotAppliedNeverCommitted").split(",")[0]
def kinds(seed, replay=False):
    if workload == "smoke":
        return ["non-linearizable"] if env.get("NABEDRA_SIM_DEFECT") == "lost-write" and seed >= 2 else []
    if not patched:
        if seed == int(env.get("FAKE_CLEAN_KIND_AT", "0")):
            return [kind]
        return ["Other"] if env.get("FAKE_CLEAN_FAILS") else []
    return [kind] if seed >= int(env.get("FAKE_HIT", "2")) and not (replay and env.get("FAKE_FLAKY")) else []
def line(seed, ks):
    print("nabedra-sim-seed: " + json.dumps({"seed": seed, "result": "violation" if ks else "pass", "kinds": ks}))
if env.get("FAKE_SILENT"):
    print("test result: ok. 1 passed; 0 failed;")
    sys.exit(0)
if "NABEDRA_SIM_EXPECT" in env:
    for seed in range(first, first + count):
        ks = kinds(seed)
        if not patched and seed == int(env.get("FAKE_CONTROL_SKIPS", "0")):
            continue
        line(seed + 100 if env.get("FAKE_OUT_OF_BUDGET") else seed, ks)
        if env.get("FAKE_DUPLICATE"):
            line(seed, ks)
        if ks:
            sys.exit(0)
    sys.exit(101)
assert count == 1 and env["NABEDRA_SIM_VERIFY_REPLAY"] == "1"
seed, label = first, env["NABEDRA_SIM_CHECK_ID"]
ks, again = kinds(seed), kinds(seed, replay=True)
art = Path(env["NABEDRA_SIM_ARTIFACTS"])
seed_dir = art / f"{label}-seed-{seed}"
seed_dir.mkdir(parents=True, exist_ok=True)
if not (patched and env.get("FAKE_NO_HISTORY")):
    (seed_dir / "history.jsonl").write_text(json.dumps({"seed": seed, "patched": patched}) + "\\n")
if ks:
    (seed_dir / "summary.json").write_text(json.dumps({"violations": [f"t=5ms {k}: detail" for k in ks]}))
(art / "sweep-report.json").write_text(json.dumps({"seeds": [{
    "seed": seed, "trace_hash": ("%016x" % (seed * 7919 + patched)), "trace_events": 40 + seed,
    "virtual_ms": 1000, "replayed": True, "violations": ks,
    "deterministic": ks == again and (patched or not env.get("FAKE_CLEAN_FLAKY")),
    "config_hash": "c0ffee"}]}))
line(seed, ks)
if env.get("FAKE_PATCHED_EXIT" if patched else "FAKE_CLEAN_EXIT"):
    ks = not ks
sys.exit(101 if ks else 0)
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

    def test_missing_fix_history_fails_in_a_shallow_clone(self):
        self.f.commit("bug record and patch")
        shallow = self.root / "shallow"
        sh(self.root, "clone", "--quiet", "--no-local", "--depth", "1", str(self.f.repo), str(shallow))
        out = StringIO()
        self.assertEqual(sim_bugs.main(["check"], repo=shallow, out=out), 1, out.getvalue())
        self.assertIn(f"fix commit {self.f.fix} does not resolve", out.getvalue())

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

    def test_clean_port_follows_a_crate_move(self):
        f, old, new = self.f, "server/src/oltp_replication/net.rs", "server/oltp-replication/src/oltp_replication/net.rs"
        f.write(old, LIB)
        f.commit("replication code")
        f.write(old, FIXED)
        fix = f.commit("a replication fix")
        (f.repo / new).parent.mkdir(parents=True)
        sh(f.repo, "mv", old, new)
        f.commit("phase 8 crate move")
        record = f.repo / "docs/testing/bugs/2026-11-23-fix-one.toml"
        record.write_text(record.read_text().replace(f'["{f.fix}"]', f'["{f.fix}", "{fix}"]'))
        manifest = f.manifest().replace(f'reverts = "{f.fix}"', f'reverts = "{fix}"').replace(
            f'fix_commits = ["{f.fix}"]', f'fix_commits = ["{fix}"]')
        stale = sh(f.repo, "diff", fix, f"{fix}^", "--", old)
        f.write_bug(manifest, stale)
        self.assert_check(1, "no longer applies to HEAD")
        f.write_bug(manifest, stale.replace(old, new))
        self.assert_check(0, f"fix-one: ok (clean port of {fix[:8]}, files: {new})")
        f.write_bug(manifest, extra_line(stale.replace(old, new)))
        self.assert_check(1, "port = clean, but the patch does not change exactly the lines")

    def test_verify_runs_before_a_refreshed_patch_has_evidence(self):
        # A moved hand port changes its digest; verify is what records the new evidence.
        fake = self.root / "fake_cargo.py"
        fake.write_text(FAKE_CARGO)
        self.f.write_bug(self.f.manifest(port="hand", regression=self.f.evidence(digest="0" * 64)))
        self.assert_check(1, "differ from those its regression evidence used")
        status, output = self.f.run("verify", "--cargo", f"{sys.executable} {fake}",
                                    "--target-dir", str(self.root / "target"))
        self.assertEqual(status, 0, output)
        self.assertIn("fix-one: with the patch failed; verified", output)
        # run needs the evidence: it is the check that counts.
        self.f.write("server/sim-oltp/Cargo.toml", "[package]\n")
        self.f.commit("install the test harness manifest")
        status, output = self.f.run("run", "--seeds", "1")
        self.assertEqual(status, 1, output)
        self.assertIn("differ from those its regression evidence used", output)

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

    def test_open_record_has_no_fix_to_revert(self):
        # sim_bugs reads the TOML itself, so a malformed open record that lists a SHA must fail here.
        record = self.f.repo / "docs/testing/bugs/2026-11-23-fix-one.toml"
        record.write_text(record.read_text() + 'status = "open"\n')
        self.assert_check(1, "bug 'FIX-ONE' is open; it has no fix to revert")

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

    def harness_run(self, *argv, **faults):
        """`run` against the fake harness; returns (status, output, summary or None, calls)."""
        if not (self.f.repo / "server/sim-oltp/Cargo.toml").exists():
            self.f.write("server/sim-oltp/Cargo.toml", "[package]\n")
            self.f.commit("install the test harness manifest")
        cargo, sweep_src = self.root / "fake_sim_cargo.py", self.root / "fake_sweep.py"
        cargo.write_text(FAKE_SIM_CARGO)
        sweep_src.write_text(FAKE_SWEEP)
        calls, out = self.root / "calls.jsonl", self.root / "out"
        calls.unlink(missing_ok=True)
        env = {"FAKE_SWEEP": str(sweep_src), "FAKE_CALLS": str(calls), "RUSTFLAGS": "",
               **{f"FAKE_{k.upper()}": str(v) for k, v in faults.items()}}
        with patch.dict(os.environ, env):
            status, output = self.f.run("run", "--cargo", f"{sys.executable} {cargo}", "--seeds", "5",
                                        "--target-dir", str(self.root / "target"), "--out", str(out),
                                        *argv)
        summary = sim_bugs.read_json(out / "sim-bugs-run.json")
        lines = calls.read_text().splitlines() if calls.exists() else []
        return status, output, summary, [json.loads(line) for line in lines]

    def record_detection(self, hit, **changes):
        history = json.dumps({"seed": hit, "patched": True}) + "\n"
        detection = {"seed": hit, "trace_hash": "%016x" % (hit * 7919 + 1), "mechanism": "the lost reply",
                     "detector": "NotAppliedNeverCommitted",
                     "history_sha256": hashlib.sha256(history.encode()).hexdigest(), **changes}
        table = "".join(f"{key} = {json.dumps(value)}\n" for key, value in detection.items())
        self.f.write_bug(self.f.manifest() + "[detection]\n" + table)

    def test_a_hit_is_only_a_candidate_without_its_own_recorded_mechanism(self):
        status, output, summary, _ = self.harness_run(hit=3)
        self.assertEqual(status, 1, output)
        self.assertIn("fix-one: candidate (NOT counted: no reviewed mechanism is recorded", output)
        self.assertIn("0 of 1 reverted fixes found", output)
        self.assertEqual(summary["bugs"][0]["result"], "candidate")
        for change in ({"seed": 4}, {"trace_hash": "0" * 16}, {"history_sha256": "0" * 64}):
            with self.subTest(change=change):
                self.record_detection(3, **change)
                status, output, _, _ = self.harness_run(hit=3)
                self.assertEqual(status, 1, output)
                self.assertIn("the recorded mechanism is for another hit", output)
        self.record_detection(3, detector="failed-write-observed")
        self.assert_check(1, "detection.detector must be one of expect")
        # An expected kind that the patched replay did not report is not the detector either.
        bug = self.f.repo / "server/sim-oltp/bugs/fix-one/bug.toml"
        bug.write_text(bug.read_text().replace('expect = ["NotAppliedNeverCommitted"]',
                                               'expect = ["NotAppliedNeverCommitted", "failed-write-observed"]'))
        status, output, summary, _ = self.harness_run(hit=3)
        self.assertEqual(status, 1, output)
        self.assertIn("failed-write-observed did not report this hit", output)
        self.assertEqual(summary["bugs"][0]["result"], "candidate")
        self.record_detection(3, mechanism=" ")
        self.assert_check(1, "detection.mechanism must say how")

    def test_later_phases_use_their_own_directory(self):
        patch_text = self.f.patch
        manifest = self.f.manifest().replace('"fix-one"', '"fix-two"').replace("phase = 10", "phase = 11")
        for where, text, expected in (("phase11/fix-two", manifest, 0), ("fix-two", manifest, 1),
                                      ("phase11/fix-two", manifest.replace("phase = 11", "phase = 10"), 1)):
            with self.subTest(where=where, expected=expected):
                base = f"server/sim-oltp/bugs/{where}"
                self.f.write(f"{base}/bug.toml", text)
                self.f.write(f"{base}/revert.patch", patch_text)
                status, output = self.f.run("check")
                self.assertEqual(status, expected, output)
                self.assertIn("2 of 2 patches valid" if expected == 0 else "phase must be", output)
                (self.f.repo / base / "bug.toml").unlink()
                (self.f.repo / base / "revert.patch").unlink()
                (self.f.repo / base).rmdir()

    def test_run_finds_the_seed_and_replays_it_patched_and_clean(self):
        self.record_detection(3)
        before = (self.f.repo / "server/src/lib.rs").read_text()
        # An inherited defect or expectation never reaches the harness outside a rehearsal.
        with patch.dict(os.environ, {"NABEDRA_SIM_DEFECT": "lost-write", "NABEDRA_SIM_EXPECT": "x"}):
            status, output, summary, calls = self.harness_run(hit=3)
        self.assertEqual(status, 0, output)
        self.assertIn("fix-one: found seed 3 after 3 seeds in [1, 5]; patched ['NotAppliedNeverCommitted']", output)
        self.assertIn("clean twin pass", output)
        self.assertIn("1 of 1 reverted fixes found", output)
        # Patched search, patched replay of the hit, clean replay of it, clean control over the prefix.
        self.assertEqual([(c["patched"], c["first"], c["count"], c["expect"]) for c in calls],
                         [(True, 1, 5, "NotAppliedNeverCommitted"), (True, 3, 1, None), (False, 3, 1, None),
                          (False, 1, 3, "NotAppliedNeverCommitted")])
        self.assertTrue(all(c["defect"] is None and c["profile"] == "lost-reply" for c in calls))
        # The twin and the control run the search's own workload and profile.
        self.assertEqual({(c["workload"], c["profile"]) for c in calls}, {("direct", "lost-reply")})
        row = summary["bugs"][0]
        self.assertEqual(row["detector"], "NotAppliedNeverCommitted")
        self.assertEqual((summary["mode"], summary["evidence"], row["result"], row["found_seed"], row["tried"]),
                         ("evidence", "fault-model", "found", 3, 3))
        self.assertEqual(row["patch_sha256"], hashlib.sha256(self.f.patch.encode()).hexdigest())
        self.assertNotEqual(row["patched"]["trace_hash"], row["clean"]["trace_hash"])
        self.assertTrue(row["patched"]["deterministic"] and row["clean"]["deterministic"])
        self.assertEqual(row["patched"]["violations"], ["t=5ms NotAppliedNeverCommitted: detail"])
        history = Path(row["patched"]["history"])
        self.assertEqual(row["patched"]["history_sha256"], hashlib.sha256(history.read_bytes()).hexdigest())
        self.assertEqual(json.loads(history.read_text())["patched"], True)
        self.assertEqual(json.loads(Path(row["clean"]["history"]).read_text())["patched"], False)
        # No binary lands in the artifact directory, which the workflow uploads (design 3.10).
        self.assertEqual([p for p in (self.root / "out").rglob("*") if p.is_file() and p.stat().st_mode & 0o111], [])
        # The patch went only into the scratch worktree, which is gone.
        self.assertEqual((self.f.repo / "server/src/lib.rs").read_text(), before)
        self.assertEqual(len(sh(self.f.repo, "worktree", "list").splitlines()), 1)

    def test_run_never_passes_on_a_tooling_or_harness_fault(self):
        cases = {
            "silent": "no seed line",
            "out_of_budget": "duplicate or out-of-budget seed lines in [1, 5]",
            "duplicate": "duplicate or out-of-budget seed lines in [1, 5]",
            "clean_fails": "clean replay of seed 2: the unpatched build reported ['Other']",
            "flaky": "patched replay of seed 2: the seed's two runs differed",
            "patched_exit": "patched replay of seed 2: the sweep did not fail on the seed alone (exit 0)",
            "no_history": "patched replay of seed 2: the harness wrote no history",
            "clean_flaky": "clean replay of seed 2: the seed's two runs differed",
            "clean_exit": "clean replay of seed 2: the unpatched sweep did not pass (exit 101)",
            "control_skips": "the clean control did not run every seed tried",
            "hit": "5 seeds without ['NotAppliedNeverCommitted']",
            "clean_kind_at": "not attributable to the patch: the unpatched build reports "
                             "['NotAppliedNeverCommitted'] at seed 1",
        }
        for fault, needle in cases.items():
            with self.subTest(fault=fault):
                status, output, summary, _ = self.harness_run(**{fault: 9 if fault == "hit" else 1})
                self.assertEqual(status, 1, output)
                self.assertIn(needle, output)
                self.assertIn("0 of 1 reverted fixes found", output)
                self.assertEqual(summary["bugs"][0]["result"], "not-found")

    def test_run_is_not_run_while_the_workload_is_missing(self):
        manifest = self.f.manifest().replace('workload = "direct"', 'workload = "cold"')
        self.f.write_bug(manifest)
        status, output, summary, _ = self.harness_run()
        self.assertEqual(status, 0, output)
        self.assertIn("fix-one: not-run (the harness has no cold workload)", output)
        check = run_check.parse_check(tomllib_load(Path(__file__).resolve().parents[2]
                                                   / "docs/testing/checks/oltp-sim-bugs-direct.toml"))
        result, parsed = run_check.classify(check.result_parser, output, status, False, check.not_run_marker)
        self.assertEqual((result, parsed.executed), ("not-run", 0))

    def test_rehearsal_on_smoke_is_labelled_and_keeps_the_clean_twin_clean(self):
        status, output, summary, calls = self.harness_run(
            "--workload", "smoke", "--profile", "", "--expect", "non-linearizable",
            "--rehearse-defect", "lost-write")
        self.assertEqual(status, 0, output)
        self.assertIn("(rehearsal: manifest workload, profile or expectation overridden", output)
        self.assertIn("sim-bugs run (rehearsal): 1 of 1 pipeline rehearsals completed", output)
        self.assertIn("fix-one: rehearsed (defect lost-write, not the patch) seed 2", output)
        self.assertNotIn("reverted fixes found", output)
        self.assertEqual(summary["mode"], "rehearsal")
        self.assertEqual([(c["defect"], c["profile"]) for c in calls],
                         [("lost-write", None), ("lost-write", None), (None, None), (None, None)])

    def test_seed_lines_and_first_match(self):
        output = "\n".join([
            'test suite::sweep ... nabedra-sim-seed: {"seed": 1, "result": "pass", "kinds": []}',
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
        # A passing last attempt must not hide an earlier one that never ran.
        outcomes[:] = [("not-run", ""), ("timeout", ""), ("passed", "")]
        self.assertEqual(sim_bugs.regression_verdict(bug, None, None, None, {}, None, True)[::2],
                         ("passed", ["not-run", "timeout", "passed"]))
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

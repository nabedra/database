"""Exercise sim_bugs.py on a throwaway Git repository with planted errors and a fake cargo."""

import copy
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import shutil
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

# A fake cargo for `run`: `test --no-run` writes a fake nabedra-sim-oltp sweep test that follows
# server/sim-oltp/src/sweep.rs (environment in; seed lines, sweep-report.json and seed directories
# out). What is compiled decides whether the bug shows: the tree lacks the fix, or the file that
# FAKE_DEFECT names holds DEFECT. FAKE_SCRIPT (JSON) scripts the verdicts and plants faults.
FAKE_SIM_CARGO = '''
import json, os, sys
target = os.environ["CARGO_TARGET_DIR"]
if "--no-run" in sys.argv:
    defect = os.environ.get("FAKE_DEFECT")
    patched = ("DEFECT" in open(defect).read()) if defect else "fixed" not in open("src/lib.rs").read()
    if os.environ.get("FAKE_SAME_BINARY"):
        patched = False
    exe = os.path.join(target, "release", "deps", "nabedra_sim_oltp-fake")
    os.makedirs(os.path.dirname(exe), exist_ok=True)
    with open(exe, "w") as f:
        f.write("#!/usr/bin/env python3\\nPATCHED = %r\\n" % patched + open(os.environ["FAKE_SWEEP"]).read())
    os.chmod(exe, 0o755)
    print(json.dumps({"reason": "compiler-artifact", "profile": {"test": True},
                      "target": {"kind": ["lib"], "name": "nabedra_sim_oltp"}, "executable": exe}))
'''
KIND = "NotAppliedNeverCommitted"
FAKE_SWEEP = '''
import json, os, sys
from pathlib import Path
env = os.environ
script = json.loads(env.get("FAKE_SCRIPT", "{}"))
side = "patched" if PATCHED else "clean"
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
searching = "NABEDRA_SIM_EXPECT" in env
replaying = env.get("NABEDRA_SIM_VERIFY_REPLAY") == "1"
with open(env["FAKE_CALLS"], "a") as calls:
    calls.write(json.dumps({"patched": PATCHED, "first": first, "count": count, "replay": replaying,
                            "expect": env.get("NABEDRA_SIM_EXPECT"), "defect": env.get("NABEDRA_SIM_DEFECT"),
                            "profile": env.get("NABEDRA_SIM_PROFILE"), "workload": workload,
                            "rustflags": env["RUSTFLAGS"],
                            "nabedra": sorted(k for k in env if k.startswith("NABEDRA_"))}) + "\\n")
KIND = "NotAppliedNeverCommitted"
def kinds(seed):
    if workload == "smoke":
        return ["non-linearizable"] if env.get("NABEDRA_SIM_DEFECT") == "lost-write" and seed >= 2 else []
    table = dict(script.get(side, {}))
    if replaying:
        table.update(script.get(side + "_replay", {}))
    if str(seed) in table:
        return table[str(seed)]
    return [KIND] if PATCHED and seed >= script.get("hit", 10 ** 9) else []
def line(seed, ks):
    if seed in script.get(side + ("_search" if searching else "_replay" if replaying else "_base") + "_skip", []):
        return
    row = {"seed": seed, "result": "violation" if ks else "pass", "kinds": ks, "workload": workload}
    row.update(script.get(side + "_line", {}))
    print("nabedra-sim-seed: " + json.dumps(row))
    if script.get("duplicate") and searching:
        print("nabedra-sim-seed: " + json.dumps(row))
    if seed == script.get("garbage") and searching:
        print("nabedra-sim-seed: {not json")
if script.get("silent"):
    print("test result: ok. 1 passed; 0 failed;")
    sys.exit(0)
if searching:
    expect = env["NABEDRA_SIM_EXPECT"].split(",")
    for seed in range(first, first + count):
        ks = kinds(seed)
        line(seed + (100 if script.get("out_of_budget") else 0), ks)
        if set(ks) & set(expect):
            sys.exit(0)
    sys.exit(script.get(side + "_search_exit", 101))
label, art = env["NABEDRA_SIM_CHECK_ID"], Path(env["NABEDRA_SIM_ARTIFACTS"])
rows, failed = [], False
for seed in range(first, first + count):
    ks = kinds(seed)
    violations = [f"t=5ms {k}: detail" for k in ks]
    twice = bool(ks) or replaying
    deterministic = not (twice and side in script.get("flaky", []))
    if not deterministic:
        ks = sorted([*ks, "determinism"])
    failed |= bool(ks)
    seed_dir = art / f"{label}-seed-{seed}"
    if ks or seed == first:
        seed_dir.mkdir(parents=True, exist_ok=True)
        events = [json.dumps({"seed": seed, "op": "insert", "result": "ok"})]
        trace = [f"seed {seed} n101 applied"]
        if PATCHED:
            events.append(json.dumps({"seed": seed, "op": "insert", "result": "not_applied"}))
            trace.append(f"seed {seed} n101 reply lost, refused as not_applied")
        if side not in script.get("no_history", []):
            (seed_dir / "history.jsonl").write_text("\\n".join(events) + "\\n")
        (seed_dir / "trace-tail.txt").write_text("\\n".join(trace))
        if env.get("NABEDRA_SIM_TRACE") == "full":
            (seed_dir / "trace.txt").write_text("\\n".join(trace))
    if ks:
        (seed_dir / "summary.json").write_text(json.dumps({"violations": violations}))
    line(seed, ks)
    same = script.get("same_trace", False)
    rows.append({"seed": seed, "trace_hash": "%016x" % (seed * 7919 + (PATCHED and not same)),
                 "trace_events": 40 + seed, "virtual_ms": 1000, "replayed": twice, "violations": ks,
                 "deterministic": deterministic if twice else None, "config_hash": "c0ffee",
                 "workload": workload})
report = {"evidence": "fault-model", "workload": workload, "defect": env.get("NABEDRA_SIM_DEFECT", "none"),
          "seeds": rows}
report.update(script.get(side + "_report", {}))
for row in rows:
    row.update(script.get(side + "_row", {}))
    for key in script.get(side + "_row_without", []):
        del row[key]
art.mkdir(parents=True, exist_ok=True)
(art / "sweep-report.json").write_text(json.dumps(report))
key = side + ("_replay" if replaying else "_base") + "_exit"
sys.exit(script.get(key, 101 if failed else 0))
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

    def manifest(self, port="clean", regression=None, tables="", **extra):
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
        return "\n".join(lines) + "\n" + tables

    def write_bug(self, manifest=None, patch=None):
        self.write("server/sim-oltp/bugs/fix-one/bug.toml", manifest or self.manifest())
        self.write("server/sim-oltp/bugs/fix-one/revert.patch", self.patch if patch is None else patch)

    def run(self, *argv):
        out = StringIO()
        status = sim_bugs.main(list(argv), repo=self.repo, out=out)
        return status, out.getvalue()

    def evidence(self, digest=None, sha=None, patch=None):
        digest = digest or sim_bugs.digest(patch or self.patch)
        run = {"run": "https://github.com/o/r/actions/runs/1", "sha": sha or "a" * 40}
        output = ("thread 'main' panicked: assertion failed: lost the uncertain outcome\n"
                  "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 9 filtered out\n"
                  f"fix-one: with the patch failed; verified (digest {digest})\n")
        return {"kind": "lib-test", "test": "tests::d02_fix_one_keeps_the_outcome",
                "failure_contains": ["lost the uncertain outcome"],
                "evidence": {"digest": digest, "clean": dict(run, result="passed"),
                             "patched": dict(run, result="failed", output=output)}}

    def revert_diff(self, path, text):
        """The diff that turns HEAD's `path` into `text`."""
        before = (self.repo / path).read_text()
        (self.repo / path).write_text(text)
        patch_text = sh(self.repo, "diff", "--", path)
        (self.repo / path).write_text(before)
        return patch_text

    def edit(self, old, new, name="bug.toml"):
        path = self.repo / BUG / name
        text = path.read_text()
        assert old in text, (old, text)
        path.write_text(text.replace(old, new))


BUG = "server/sim-oltp/bugs/fix-one"
REVIEW = {"verdict": "sensitive", "assertion": "state", "reviewer": "reviewer A",
          "source": "https://github.com/o/r/pull/1#review"}


def toml_table(name, values):
    return f"[{name}]\n" + "".join(f"{key} = {json.dumps(value)}\n" for key, value in values.items())


def hit_identity(seed):
    history = "".join(json.dumps({"seed": seed, "op": "insert", "result": result}) + "\n"
                      for result in ("ok", "not_applied"))
    return {"seed": seed, "trace_hash": "%016x" % (seed * 7919 + 1),
            "history_sha256": hashlib.sha256(history.encode()).hexdigest()}


def mechanism(seed):
    return (f"At seed {seed} (trace {hit_identity(seed)['trace_hash']}) the reply of an applied insert is lost and "
            "lib.rs answers not_applied, which is the branch the fix removed. The history holds both events.")


def detection(seed=3, **changes):
    """A complete [detection] for the fake harness's hit, with the fields this sim_bugs.py knows."""
    values = {**hit_identity(seed), "detector": KIND, "mechanism": mechanism(seed),
              "lines": [f"seed {seed} n101 reply lost, refused as not_applied"], "search": "0" * 64,
              "confirmed_by": "reviewer A", "confirmation": "https://github.com/o/r/pull/1#history",
              "patch_author": "author B", "workload_author": "author C", **changes}
    return {k: v for k, v in values.items() if k in sim_bugs.DETECTION_FIELDS}


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


class HarnessCase(unittest.TestCase):
    """A fixture whose bug is recorded and confirmed in full, so that one change at a time can break it."""

    @classmethod
    def setUpClass(cls):
        tmp = TemporaryDirectory()
        cls.addClassCleanup(tmp.cleanup)
        cls.template = Path(tmp.name)
        case = cls("runTest")
        case.root, case.f = cls.template, Fixture(cls.template)
        case.confirm(3)

    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        shutil.copytree(self.template / "repo", self.root / "repo")
        self.f = copy.copy(self.__class__.fixture)
        self.f.repo = self.root / "repo"

    def runTest(self):
        pass

    def confirm(self, hit):
        """Evidence, a first search, its record, and a detection that confirms its hit."""
        f = self.f
        f.write("server/sim-oltp/Cargo.toml", "[package]\n")
        f.write("server/sim-oltp/src/sweep.rs", "pub const PLANTED: u8 = 0;\n")
        f.write("server/rt/src/sim/net.rs", "pub const PLANTED: u8 = 0;\n")
        f.write(".gitignore", "target/\n")
        f.write_bug()
        self.tested = f.commit("the patch")
        review = toml_table("review", REVIEW) if "review" in sim_bugs.MANIFEST_FIELDS else ""
        f.write_bug(f.manifest(regression=f.evidence(sha=self.tested), tables=review))
        f.commit("the regression evidence")
        _, output, summary, _ = self.harness_run(script={"hit": hit})
        self.assertEqual([row["result"] for row in summary["bugs"]], ["candidate"], output)
        changes = {}
        if hasattr(sim_bugs, "cmd_record"):
            changes["search"] = self.record()
        f.write_bug((f.repo / BUG / "bug.toml").read_text() + toml_table("detection", detection(hit, **changes)))
        f.commit("the confirmed detection")
        self.__class__.fixture, self.__class__.tested = f, self.tested

    def record(self):
        """Append the last run's search to the target's record, and return the search's hash."""
        f = self.f
        status, output = f.run("record", "fix-one", "--from", str(self.root / "out/sim-bugs-run.json"))
        self.assertEqual(status, 0, output)
        f.commit("record the search")
        search = json.loads((f.repo / BUG / "searches.jsonl").read_text().splitlines()[-1])["search"]
        return hashlib.sha256(json.dumps(search, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def harness_run(self, *argv, script=None, seeds=5, **env):
        """`run` against the fake harness; returns (status, output, summary or None, calls)."""
        if not (self.f.repo / "server/sim-oltp/Cargo.toml").exists():
            self.f.write("server/sim-oltp/Cargo.toml", "[package]\n")
            self.f.commit("install the test harness manifest")
        cargo, sweep_src = self.root / "fake_sim_cargo.py", self.root / "fake_sweep.py"
        cargo.write_text(FAKE_SIM_CARGO)
        sweep_src.write_text(FAKE_SWEEP)
        calls, out = self.root / "calls.jsonl", self.root / "out"
        calls.unlink(missing_ok=True)
        shutil.rmtree(out, ignore_errors=True)
        env = {"FAKE_SWEEP": str(sweep_src), "FAKE_CALLS": str(calls), "RUSTFLAGS": "",
               "FAKE_SCRIPT": json.dumps(script or {}), **env}
        with patch.dict(os.environ, env):
            status, output = self.f.run("run", "--cargo", f"{sys.executable} {cargo}",
                                        *(["--seeds", str(seeds)] if seeds else []),
                                        "--target-dir", str(self.root / "target"), "--out", str(out), *argv)
        summary = sim_bugs.read_json(out / "sim-bugs-run.json")
        lines = calls.read_text().splitlines() if calls.exists() else []
        return status, output, summary, [json.loads(line) for line in lines]

    def assert_found(self, **kwargs):
        status, output, summary, calls = self.harness_run(**kwargs)
        self.assertEqual([row["result"] for row in summary["bugs"]], ["found"], output)
        self.assertEqual(status, 0, output)
        return output, summary["bugs"][0], calls

    def assert_not_counted(self, *needles, result=None, **kwargs):
        """The run counts nothing, and says why."""
        kwargs.setdefault("script", {"hit": 3})
        status, output, summary, _ = self.harness_run(**kwargs)
        rows = (summary or {}).get("bugs", [])
        self.assertNotIn("found", [row["result"] for row in rows], output)
        self.assertEqual(status, 1, output)
        self.assertNotIn(sim_bugs.NOT_RUN_MARKER, output)
        for needle in needles:
            self.assertIn(needle, output)
        if result:
            self.assertEqual([row["result"] for row in rows], [result], output)
        return rows[0] if rows else None

    def assert_refused(self, *needles, **kwargs):
        """`check` refuses the manifest, and `run` counts nothing."""
        status, output = self.f.run("check")
        self.assertEqual(status, 1, output)
        for needle in needles:
            self.assertIn(needle, output)
        self.f.commit("a manifest that check refuses")
        self.assert_not_counted(**kwargs)

    def replace_patch(self, patch_text, port="hand", regression=None):
        """Commit another patch, with evidence that names its commit, and keep the detection."""
        f = self.f
        f.patch = patch_text
        f.write(f"{BUG}/revert.patch", patch_text)
        sha = f.commit("another patch")
        tables = toml_table("review", REVIEW) if "review" in sim_bugs.MANIFEST_FIELDS else ""
        f.write(f"{BUG}/bug.toml", f.manifest(
            port=port, regression=regression or f.evidence(sha=sha, digest=digest_of(patch_text)),
            tables=tables + toml_table("detection", detection(search=self.search_hash()))))
        f.commit("its evidence")

    def search_hash(self):
        path = self.f.repo / BUG / "searches.jsonl"
        if not path.exists():
            return "0" * 64
        search = json.loads(path.read_text().splitlines()[-1])["search"]
        return hashlib.sha256(json.dumps(search, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def digest_of(patch_text):
    """The digest the tool records for a patch, or a placeholder when it refuses to read it."""
    try:
        return sim_bugs.digest(patch_text)
    except sim_bugs.Failure:
        return "0" * 64


class FoolTest(HarnessCase):
    """Ways to make the tool count something that is not a detection. Each one must be refused."""

    def second_bug(self, name="fix-two", bug="FIX-TWO", patch_text=None, change=lambda text: text):
        """The same target entered again under another id and another bug record."""
        f = self.f
        record = (f.repo / "docs/testing/bugs/2026-11-23-fix-one.toml").read_text()
        f.write("docs/testing/bugs/2026-11-23-fix-two.toml", record.replace("FIX-ONE", "FIX-TWO"))
        text = (f.repo / BUG / "bug.toml").read_text()
        text = text.replace('id = "fix-one"', f'id = "{name}"').replace('bug = "FIX-ONE"', f'bug = "{bug}"')
        text = text.replace("fix-one: with the patch", f"{name}: with the patch")
        other = BUG.replace("fix-one", name)
        f.write(f"{other}/bug.toml", change(text))
        f.write(f"{other}/revert.patch", f.patch if patch_text is None else patch_text)
        searches = f.repo / BUG / "searches.jsonl"
        if searches.exists():
            f.write(f"{other}/searches.jsonl", searches.read_text().replace("fix-one", name))

    def test_baseline_counts(self):
        output, row, _ = self.assert_found(script={"hit": 3})
        self.assertEqual((row["found_seed"], row["detector"]), (3, KIND))
        self.assertIn("1 of 1 reverted fixes found", output)

    # 1. The patch.

    def test_hidden_edit_without_a_diff_header(self):
        hidden = self.f.revert_diff("server/sim-oltp/src/sweep.rs", "pub const PLANTED: u8 = 1; // DEFECT\n")
        hidden = hidden[hidden.index("--- a/"):]
        self.replace_patch(self.f.patch + hidden)
        self.assert_refused("not part of", FAKE_DEFECT="sim-oltp/src/sweep.rs")

    def test_hidden_edit_under_another_files_header(self):
        hidden = self.f.revert_diff("server/sim-oltp/src/sweep.rs", "pub const PLANTED: u8 = 1; // DEFECT\n")
        hidden = hidden[hidden.index("--- a/"):]
        header = "diff --git a/server/src/lib.rs b/server/src/lib.rs\n"
        self.replace_patch(self.f.patch + header + hidden)
        self.assert_refused("names", FAKE_DEFECT="sim-oltp/src/sweep.rs")

    def test_hidden_edit_before_the_first_header(self):
        hidden = self.f.revert_diff("server/sim-oltp/src/sweep.rs", "pub const PLANTED: u8 = 1; // DEFECT\n")
        self.replace_patch(hidden[hidden.index("--- a/"):] + self.f.patch)
        self.assert_refused("not part of", FAKE_DEFECT="sim-oltp/src/sweep.rs")

    def test_changed_lines_that_look_like_headers_are_in_the_digest(self):
        # A removed line `-- x` prints as `--- x`. The digest must cover it.
        hunk = "@@ -1,2 +1,2 @@\n{}\n fn context() {{}}\n"
        header = "diff --git a/server/src/a.rs b/server/src/a.rs\n--- a/server/src/a.rs\n+++ b/server/src/a.rs\n"
        one = header + hunk.format("--- x\n+++ y")
        two = header + hunk.format("--- x\n+++ z")
        self.assertNotEqual(sim_bugs.digest(one), sim_bugs.digest(two))

    def test_patch_of_the_simulator_runtime(self):
        text = self.f.revert_diff("server/rt/src/sim/net.rs", "pub const PLANTED: u8 = 1; // DEFECT\n")
        self.replace_patch(text)
        self.assert_refused("revert.patch touches server/rt/src/sim/net.rs", FAKE_DEFECT="rt/src/sim/net.rs")

    def test_paths_that_are_not_product_sources(self):
        for name in ("server/rt/src/sim.rs", "server/rt/src/sim/net/links.rs", "server/rt/src/time.rs",
                     "server/src/cache/cluster/sim/hooks.rs", "server/events/src/sim/mod.rs",
                     "server/build.rs", "server/oltp-storage/build.rs", "server/.cargo/config.toml",
                     "server/rust-toolchain.toml", "server/src/data.json", "server/sim-cache/src/lib.rs",
                     "server/src/oltp_acceptance.rs", "server/src/oltp_acceptance/acc6.rs",
                     "server/src/oltp_runtime/test_support.rs", "server/src/../sim-oltp/src/sweep.rs",
                     "server/src/planted.rs", "server/examples/a.rs", "server/fuzz/src/lib.rs"):
            self.assertTrue(sim_bugs.path_problem(name), name)
        for name in ("server/src/oltp_runtime/direct/executor.rs", "server/oltp-types/src/queue.rs",
                     "server/oltp-replication/src/oltp_replication/typed_log.rs", "server/transport/src/lib.rs"):
            self.assertIsNone(sim_bugs.path_problem(name), name)

    def test_patch_that_acts_only_in_the_simulator(self):
        f = self.f
        for added in ('    #[cfg(feature = "sim")]\n    return "not_applied";\n',
                      '    if cfg!(feature = "sim") { return "not_applied"; }\n',
                      '    if std::env::var("SEED").is_ok() { return "not_applied"; }\n',
                      '    if nabedra_rt::sim::active() { return "not_applied"; }\n'):
            with self.subTest(added=added):
                text = LIB.replace("-> &'static str {\n", "-> &'static str {\n" + added)
                problems = sim_bugs.validate_patch_paths(f.revert_diff("server/src/lib.rs", text))
                self.assertTrue(any("simulator" in problem for problem in problems), problems)

    def test_patch_inside_code_compiled_only_for_the_simulator(self):
        f = self.f
        gated = FIXED + ('#[cfg(any(test, feature = "sim"))]\nmod planted {\n    pub fn lose() -> bool {\n'
                         '        false\n    }\n}\npub fn after() -> bool {\n    false\n}\n')
        f.write("server/src/lib.rs", gated)
        f.commit("a hook for the simulator")
        self.replace_patch(f.revert_diff("server/src/lib.rs", gated.replace("        false\n", "        true\n", 1)))
        self.assert_refused("compiled only for the simulator")
        self.replace_patch(f.revert_diff("server/src/lib.rs", gated.replace("pub fn after() -> bool {\n    false",
                                                                          "pub fn after() -> bool {\n    true")))
        status, output = f.run("check")
        self.assertEqual(status, 0, output)

    def test_patch_that_is_not_compiled_into_the_sweep(self):
        # Cargo builds the same binary, so whatever the patched run reports is not the patch's doing.
        script = {"hit": 3, "clean": {"3": [KIND]}, "clean_replay": {"3": []}}
        self.assert_not_counted("the same sweep binary", script=script, FAKE_SAME_BINARY="1", result="error")

    def test_patch_with_the_tested_lines_at_another_place(self):
        # The digest holds the changed lines only, so moving them keeps it. The evidence does not.
        f = self.f
        twice = FIXED + FIXED.replace("answer", "answer_again")
        f.write("server/src/lib.rs", twice)
        f.commit("a second function with the same lines")
        moved = f.revert_diff("server/src/lib.rs", FIXED + LIB.replace("answer", "answer_again"))
        f.patch = f.revert_diff("server/src/lib.rs", LIB + FIXED.replace("answer", "answer_again"))
        self.assertEqual(sim_bugs.digest(moved), sim_bugs.digest(f.patch))
        self.replace_patch(f.patch)
        f.write(f"{BUG}/revert.patch", moved)
        f.commit("move the reverted lines")
        self.assert_not_counted("other context than the patch its regression run tested")

    # 2. The expected kinds.

    def test_expect_kinds_of_the_harness_itself(self):
        for kind in ("harness", "determinism", " Harness", "HARNESS "):
            with self.subTest(kind=kind):
                self.f.edit(f'expect = ["{KIND}"]', f'expect = ["{kind}"]')
                status, output = self.f.run("check")
                self.assertEqual(status, 1, output)
                self.assertIn("expect", output)
                self.f.edit(f'expect = ["{kind}"]', f'expect = ["{KIND}"]')
        self.f.edit(f'expect = ["{KIND}"]', 'expect = ["harness"]')
        self.f.edit(f'detector = "{KIND}"', 'detector = "harness"')
        self.assert_refused("harness", script={"patched": {"3": ["harness"], "4": ["harness"]}})

    def test_expected_kind_together_with_a_harness_fault(self):
        script = {"hit": 4, "patched": {"3": [KIND, "harness"]}}
        row = self.assert_not_counted(script=script, seeds=3)
        self.assertEqual(row["result"], "incomplete")
        self.assertEqual([(m["seed"], m["reason"]) for m in row["masked"]], [(3, "harness-fault")])

    # 3. The regression evidence.

    def test_digest_pointed_at_a_patch_that_no_run_tested(self):
        f = self.f
        old = sim_bugs.digest(f.patch)
        text = f.revert_diff("server/src/lib.rs", LIB + "// a line no regression run had\n")
        f.write(f"{BUG}/revert.patch", text)
        f.edit('port = "clean"', 'port = "hand"')
        f.edit(f'digest = "{old}"', f'digest = "{sim_bugs.digest(text)}"')
        self.assert_refused("its recorded output names")
        # The output edited as well: the commit the run tested holds another patch.
        f.edit(f"(digest {old})", f"(digest {sim_bugs.digest(text)})")
        self.assert_refused("holds a patch with other changed lines")

    def test_digest_and_commit_pointed_at_an_unrelated_commit(self):
        f = self.f
        f.edit(f'sha = "{self.tested}"', f'sha = "{"b" * 40}"')
        f.commit("cite a commit that does not exist")
        status, output = f.run("check")
        self.assertEqual(status, 0, output)
        self.assert_not_counted("is not available")

    def test_recorded_output_without_the_failure(self):
        f = self.f
        path = f.repo / BUG / "bug.toml"
        good = path.read_text()
        line = next(part for part in good.splitlines() if "with the patch failed" in part)
        for name, change, needle in (
                ("nonsense", lambda _: "nothing was run", "test result: FAILED"),
                ("no output", lambda _: None, "output"),
                ("no FAILED line", lambda text: text.replace("test result: FAILED", "test result: lost"),
                 "test result: FAILED"),
                ("a passing line as well", lambda text: text + "test result: ok. 1 passed; 0 failed; 0 ignored\\n",
                 "test result: FAILED"),
                ("no failure text", lambda text: text.replace("lost the uncertain outcome", "some other failure"),
                 "failure_contains"),
                ("another bug's line", lambda text: text.replace("fix-one: with", "fix-two: with"), "fix-two"),
                ("not verified", lambda text: text.replace("failed; verified", "failed-otherwise; NOT verified"),
                 "NOT verified")):
            with self.subTest(name=name):
                output = line.split('output = "', 1)[1].rsplit('"', 1)[0]
                new = change(output)
                fixed = line.replace(f', output = "{output}"', "" if new is None else f', output = "{new}"')
                path.write_text(good.replace(line, fixed))
                self.assert_refused(needle)

    def test_no_regression_evidence(self):
        # A clean port needs none for `check`. Decision 8 wants a test that fails because of the patch.
        f = self.f
        text = (f.repo / BUG / "bug.toml").read_text()
        start, end = text.index("[regression.evidence]"), text.index("[review]") if "[review]" in text else text.index("[detection]")
        f.write(f"{BUG}/bug.toml", text[:start] + text[end:])
        f.commit("drop the evidence")
        status, output = f.run("check")
        self.assertEqual(status, 0, output)
        self.assert_not_counted("no regression evidence", result="candidate")

    def test_evidence_without_its_clean_side(self):
        f = self.f
        text = (f.repo / BUG / "bug.toml").read_text()
        f.write(f"{BUG}/bug.toml", "".join(line for line in text.splitlines(keepends=True)
                                           if not line.startswith("clean = ")))
        self.assert_refused("clean")

    def test_clean_run_of_another_commit(self):
        # One `verify` run tests both sides.
        f = self.f
        path = f.repo / BUG / "bug.toml"
        path.write_text(path.read_text().replace(f'sha = "{self.tested}", result = "passed"',
                                                 f'sha = "{"c" * 40}", result = "passed"'))
        self.assert_refused("same run and commit")

    # 4. The review of the patch.

    def test_patch_review(self):
        f = self.f
        for old, new, needle in (
                ('verdict = "sensitive"', 'verdict = "partial"', "the independent review calls the patch partial"),
                ('verdict = "sensitive"', 'verdict = "not-sensitive"', "the independent review calls the patch not-sensitive")):
            with self.subTest(new=new):
                f.edit(old, new)
                f.commit("the review's verdict")
                status, output = f.run("check")
                self.assertEqual(status, 0, output)
                self.assert_not_counted(needle, result="candidate")
                f.edit(new, old)
        text = (f.repo / BUG / "bug.toml").read_text()
        start, end = text.index("[review]"), text.index("[detection]")
        f.write(f"{BUG}/bug.toml", text[:start] + text[end:])
        f.commit("no review")
        self.assert_not_counted("no independent review of the patch", result="candidate")
        f.write(f"{BUG}/bug.toml", text.replace('assertion = "state"', 'assertion = "counter"'))
        self.assert_refused("sensitive needs a state assertion")
        f.write(f"{BUG}/bug.toml", text.replace('reviewer = "reviewer A"', 'reviewer = " "'))
        self.assert_refused("review.reviewer")

    def test_held_target(self):
        f = self.f
        f.edit("seeds = [1, 10000]", 'seeds = [1, 10000]\nhold = "the lead decides first"')
        f.commit("hold")
        self.assert_not_counted("held: the lead decides first", result="candidate")

    def test_empty_expect(self):
        f = self.f
        f.edit(f'expect = ["{KIND}"]', "expect = []")
        self.assert_refused("expect")
        # Phase 11 may say why no rule can detect a target. It is never searched and never counts.
        text = (f.repo / BUG / "bug.toml").read_text()
        text = text[:text.index("[detection]")].replace("phase = 10", "phase = 11")
        shutil.rmtree(f.repo / BUG)
        where = "server/sim-oltp/bugs/phase11/fix-one"
        f.write(f"{where}/revert.patch", f.patch)
        f.write(f"{where}/bug.toml", text)
        status, output = f.run("check")
        self.assertEqual(status, 1, output)
        self.assertIn("expect_reason", output)
        f.write(f"{where}/bug.toml", text.replace("expect = []", 'expect = []\nexpect_reason = "no rule text"'))
        f.commit("a target without a detector")
        status, output = f.run("check")
        self.assertIn("patches valid", output)
        status, output, summary, calls = self.harness_run(script={"hit": 3})
        self.assertEqual((status, [row["result"] for row in summary["bugs"]]), (1, ["no-detector"]), output)
        self.assertEqual(calls, [])
        self.assertIn("no rule text", output)
        f.write(f"{where}/bug.toml", text.replace("expect = []", f'expect = ["{KIND}"]\nexpect_reason = "x"'))
        status, output = f.run("check")
        self.assertEqual(status, 1, output)
        self.assertIn("expect_reason", output)

    # 5. The same hit more than once.

    def test_one_hit_under_two_ids(self):
        self.second_bug()
        self.assert_refused("the same changed lines")

    def test_one_bug_record_under_two_ids(self):
        other = extra_comment(self.f)
        self.second_bug(bug="FIX-ONE", patch_text=other)
        self.assert_refused("the bug record FIX-ONE")

    def test_one_patch_with_a_comment_more(self):
        other = extra_comment(self.f)
        self.second_bug(patch_text=other, change=lambda text: text.replace('port = "clean"', 'port = "hand"').replace(
            sim_bugs.digest(self.f.patch), sim_bugs.digest(other)))
        self.assert_refused("the changes of")

    def test_one_detection_for_two_targets(self):
        f = self.f
        other = f.revert_diff("server/src/lib.rs", FIXED.replace("if uncertain {", "if !uncertain {"))
        self.second_bug(patch_text=other, change=lambda text: text.replace('port = "clean"', 'port = "hand"').replace(
            sim_bugs.digest(f.patch), sim_bugs.digest(other)))
        for key in ("trace_hash", "history_sha256", "mechanism"):
            self.assert_refused(f"the same detection.{key}")

    def test_copied_mechanism_prose(self):
        f = self.f
        good = mechanism(3)
        for prose, needle in (
                ("x", "detection.mechanism"),
                ("seed 812, trace ebe811fac9377238: DR-2 held link in lib.rs", "detection.mechanism names"),
                (good.replace("seed 3", "seed 4"), "detection.mechanism names seed 4"),
                (good + " Trace ebe811fac9377238 as well.", "detection.mechanism names ebe811fac9377238"),
                (good.replace("lib.rs", "the code"), "detection.mechanism"),
                (good.replace(hit_identity(3)["trace_hash"], "the trace"), "detection.mechanism")):
            with self.subTest(prose=prose):
                f.edit(good, prose)
                self.assert_refused(needle)
                f.edit(prose, good)

    def test_confirmation_lines_must_come_from_this_hit(self):
        f = self.f
        good = "seed 3 n101 reply lost, refused as not_applied"
        for line, needle in (("seed 812 n101 reply lost", "is not a line of the hit's history or trace"),
                             ("n101 reply lost", "is not a line of the hit's history or trace"),
                             ("seed 3 n101 applied", "the clean twin has every one of detection.lines")):
            with self.subTest(line=line):
                f.edit(json.dumps([good]), json.dumps([line]))
                f.commit("cite another line")
                self.assert_not_counted(needle, result="candidate")
                f.edit(json.dumps([line]), json.dumps([good]))
        f.edit(json.dumps([good]), "[]")
        self.assert_refused("detection.lines")

    def test_confirmation_by_an_author(self):
        for author in ('patch_author = "Reviewer a"', 'workload_author = "reviewer A "'):
            with self.subTest(author=author):
                key = author.split(" = ")[0]
                text = (self.f.repo / BUG / "bug.toml").read_text()
                old = next(line for line in text.splitlines() if line.startswith(key))
                self.f.edit(old, author)
                self.assert_refused("detection.confirmed_by")
                self.f.edit(author, old)
        self.f.edit('confirmed_by = "reviewer A"', 'confirmed_by = " "')
        self.assert_refused("detection.confirmed_by")

    def test_detection_of_another_hit(self):
        for key, value in (("seed", 4), ("trace_hash", "0" * 15 + "1"), ("history_sha256", "0" * 64)):
            with self.subTest(key=key):
                good = (self.f.repo / BUG / "bug.toml").read_text()
                start = good.index("[detection]")
                changed = "\n".join(f"{key} = {json.dumps(value)}" if line.startswith(f"{key} = ") else line
                                    for line in good[start:].splitlines())
                self.f.write(f"{BUG}/bug.toml", good[:start] + changed + "\n")
                self.f.commit("another identity")
                self.assert_not_counted()
                self.f.write(f"{BUG}/bug.toml", good)

    # 6. The search.

    def test_search_and_replay_disagree(self):
        f = self.f
        f.edit(f'expect = ["{KIND}"]', f'expect = ["{KIND}", "failed-write-observed"]')
        f.commit("two kinds")
        script = {"patched": {"3": ["failed-write-observed"]}, "patched_replay": {"3": [KIND]}}
        row = self.assert_not_counted(script=script, seeds=3, result="incomplete")
        self.assertEqual([(m["seed"], m["reason"]) for m in row["masked"]], [(3, "search-replay-disagree")])

    def test_replay_line_and_report_disagree(self):
        self.assert_not_counted("the seed line and the sweep report disagree", result="error",
                                script={"hit": 3, "patched_row": {"violations": [KIND, "Other"]}})

    def test_seeds_chosen_past_a_red_seed_of_main(self):
        for seeds in ("[3, 3]", "[3, 10000]", "[1, 5]", "[1, 20000]"):
            with self.subTest(seeds=seeds):
                self.f.edit("seeds = [1, 10000]", f"seeds = {seeds}")
                self.assert_refused("seeds must be [1, 10000]", script={"hit": 3, "clean": {"1": [KIND]}})
                self.f.edit(f"seeds = {seeds}", "seeds = [1, 10000]")

    def test_missing_seed_lines(self):
        self.assert_not_counted("seed lines", script={"hit": 3, "patched_search_skip": [1, 2]}, result="error")
        self.assert_not_counted("seed lines", script={"hit": 3, "patched_search_skip": [2]}, result="error")
        self.assert_not_counted("seed lines", script={"hit": 3, "clean_base_skip": [2]}, result="error")
        self.assert_not_counted("seed line", script={"hit": 3, "patched_replay_skip": [3]}, result="error")
        self.assert_not_counted("seed line", script={"hit": 3, "clean_replay_skip": [3]}, result="error")

    def test_unreadable_seed_lines(self):
        self.assert_not_counted("unreadable seed line", script={"hit": 3, "garbage": 2}, result="error")
        self.assert_not_counted("unreadable seed line", result="error",
                                script={"hit": 3, "patched_line": {"kinds": "NotAppliedNeverCommitted"}})
        self.assert_not_counted("workload", script={"hit": 3, "patched_line": {"workload": "cold"}}, result="error")

    def test_reports_of_something_else(self):
        for script, needle in (({"patched_report": {"defect": "lost-write"}}, "defect"),
                               ({"patched_report": {"defect": None}}, "defect"),
                               ({"patched_report": {"workload": "cold"}}, "workload"),
                               ({"clean_report": {"workload": "cold"}}, "workload"),
                               ({"patched_report": {"seeds": []}}, "row"),
                               ({"patched_row": {"seed": 4}}, "row"),
                               ({"patched_row": {"trace_hash": "0" * 16}}, "trace"),
                               ({"patched_row": {"trace_hash": None}}, "trace"),
                               ({"patched_row": {"trace_events": 0}}, "trace"),
                               ({"patched_row": {"replayed": False}}, "replay"),
                               ({"patched_row_without": ["deterministic"]}, "deterministic"),
                               ({"clean_row_without": ["deterministic"]}, "deterministic"),
                               ({"no_history": ["patched"]}, "history"),
                               ({"no_history": ["clean"]}, "history")):
            with self.subTest(script=script):
                self.assert_not_counted(needle, script={"hit": 3, **script}, result="error")

    def test_hit_whose_run_is_the_twins_run(self):
        row = self.assert_not_counted(script={"hit": 3, "same_trace": True}, seeds=3, result="incomplete")
        self.assertEqual([m["reason"] for m in row["masked"]], ["same-trace"])

    def test_base_run_that_contradicts_the_twin(self):
        script = {"hit": 3, "clean": {"3": [KIND]}, "clean_replay": {"3": []}}
        self.assert_not_counted("the base run of seed 3", script=script, result="candidate")

    def test_unresolved_earlier_hit(self):
        # Seeds count in order: seed 2 hits first, and nobody has confirmed or masked it.
        row = self.assert_not_counted(script={"hit": 3, "patched": {"2": [KIND]}}, result="candidate")
        self.assertEqual(row["found_seed"], 2)

    def test_manifest_edited_only_in_the_working_tree(self):
        for hide in (False, True):
            with self.subTest(hide=hide):
                self.f.edit('profile = ["lost-reply"]', 'profile = ["anything-else"]')
                if hide:
                    sh(self.f.repo, "update-index", "--assume-unchanged", f"{BUG}/bug.toml")
                self.assert_not_counted("differs from HEAD")
                self.f.edit('profile = ["anything-else"]', 'profile = ["lost-reply"]')

    def test_inherited_variables_do_not_reach_the_harness(self):
        _, _, calls = self.assert_found(script={"hit": 3}, NABEDRA_SIM_DEFECT="lost-write", NABEDRA_SIM_EXPECT="x",
                                        NABEDRA_SEED="3", NABEDRA_TRACE_DIR="/x", NABEDRA_OTHER="1",
                                        RUSTFLAGS="--cfg planted", CARGO_ENCODED_RUSTFLAGS="--cfg\x1fplanted")
        self.assertTrue(all(c["defect"] is None for c in calls))
        self.assertEqual({c["rustflags"] for c in calls}, {"--cfg tokio_unstable"})
        self.assertFalse(any(name in c["nabedra"] for c in calls for name in ("NABEDRA_TRACE_DIR", "NABEDRA_OTHER")))

    # 7. The record of searches.

    def test_detection_without_a_recorded_search(self):
        f = self.f
        f.edit(self.search_hash(), "0" * 64)
        f.commit("a hash of no search")
        self.assert_not_counted("detection.search", result="candidate")
        f.edit("0" * 64, self.search_hash())
        (f.repo / BUG / "searches.jsonl").unlink()
        self.assert_refused("searches.jsonl")

    def test_rewritten_search_record(self):
        path = self.f.repo / BUG / "searches.jsonl"
        good = path.read_text()
        path.write_text(good.replace('"tried":3', '"tried":2'))
        self.assert_refused("searches.jsonl")
        # Rewritten with its hashes corrected: Git still has the version that was committed.
        entry = json.loads(good)
        entry["search"]["tried"] = 2
        path.write_text(json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n")
        self.assert_refused("append-only")

    def test_plan_changed_after_a_search_that_nobody_recorded(self):
        # Two plans in the manifest's history, and the record knows only the second.
        f = self.f
        f.edit('profile = ["lost-reply"]', 'profile = ["tuned"]')
        f.commit("tune the profile after looking")
        _, output, summary, _ = self.harness_run(script={"hit": 3})
        self.assertEqual(summary["bugs"][0]["result"], "candidate", output)
        self.assertIn("another plan", output)
        if hasattr(sim_bugs, "cmd_record"):
            f.edit(self.search_hash(), self.record())
            f.commit("confirm under the new plan")
        f2 = (f.repo / BUG / "bug.toml").read_text()
        f.edit('profile = ["tuned"]', 'profile = ["third"]')
        f.commit("a third plan")
        f.write(f"{BUG}/bug.toml", f2)
        f.commit("and back")
        self.assert_not_counted("an earlier plan", result="candidate")
        status, output = f.run("record", "fix-one", "--never-searched",
                               sh(f.repo, "rev-parse", "HEAD~1").strip(), "--reason", "committed by mistake")
        self.assertEqual(status, 0, output)
        f.commit("account for it")
        output, row, _ = self.assert_found(script={"hit": 3})
        self.assertEqual([s["plan"]["profile"] for s in row["earlier_searches"]], [["lost-reply"]])


def extra_comment(fixture):
    """The fixture's patch with one comment line more."""
    return fixture.revert_diff("server/src/lib.rs", LIB.replace("    if uncertain", "    // a comment\n    if uncertain", 1))


class SearchTest(HarnessCase):
    """The search rule of lead decision 8."""

    def set_detection(self, seed, **changes):
        f = self.f
        text = (f.repo / BUG / "bug.toml").read_text()
        text = text[:text.index("[detection]")]
        f.write(f"{BUG}/bug.toml", text + toml_table("detection", detection(seed, search=self.search_hash(), **changes)))
        f.commit("confirm the hit")

    def test_found(self):
        before = (self.f.repo / "server/src/lib.rs").read_text()
        output, row, calls = self.assert_found(script={"hit": 3})
        self.assertIn("fix-one: found seed 3 after 3 seeds in [1, 5]; patched ['NotAppliedNeverCommitted']", output)
        self.assertIn("clean twin pass", output)
        self.assertIn("base rate 0 of 3, patched rate 1 of 3", output)
        self.assertIn("confirmed by reviewer A", output)
        # Patched search, patched replay of the hit, its clean twin, then the base run over the seeds tried.
        self.assertEqual([(c["patched"], c["first"], c["count"], c["expect"], c["replay"]) for c in calls],
                         [(True, 1, 5, KIND, False), (True, 3, 1, None, True), (False, 3, 1, None, True),
                          (False, 1, 3, None, False)])
        self.assertTrue(all(c["defect"] is None for c in calls))
        # The twin and the base run use the search's own workload and profile.
        self.assertEqual({(c["workload"], c["profile"]) for c in calls}, {("direct", "lost-reply")})
        self.assertEqual((row["result"], row["found_seed"], row["tried"], row["masked"]), ("found", 3, 3, []))
        self.assertEqual(row["patch_sha256"], hashlib.sha256(self.f.patch.encode()).hexdigest())
        self.assertNotEqual(row["patched"]["trace_hash"], row["clean"]["trace_hash"])
        self.assertTrue(row["patched"]["deterministic"] and row["clean"]["deterministic"])
        self.assertEqual(row["patched"]["violations"], ["t=5ms NotAppliedNeverCommitted: detail"])
        self.assertEqual(row["rates"], {"seeds": 3, "patched": {"expected": [3], "other": []},
                                        "base": {"expected": [], "other": []}})
        history = Path(row["patched"]["history"])
        self.assertEqual(row["patched"]["history_sha256"], hashlib.sha256(history.read_bytes()).hexdigest())
        self.assertIn("not_applied", history.read_text())
        self.assertNotIn("not_applied", Path(row["clean"]["history"]).read_text())
        # No binary lands in the artifact directory, which the workflow uploads (design 3.10).
        self.assertEqual([p for p in (self.root / "out").rglob("*") if p.is_file() and p.stat().st_mode & 0o111], [])
        # The patch went only into the scratch worktree, which is gone.
        self.assertEqual((self.f.repo / "server/src/lib.rs").read_text(), before)
        self.assertEqual(len(sh(self.f.repo, "worktree", "list").splitlines()), 1)

    def test_a_red_twin_is_masked_and_the_search_continues(self):
        # Main reports the kind itself at seed 1, another kind at seed 2, and is nondeterministic nowhere.
        script = {"hit": 1, "clean": {"1": [KIND], "2": ["Other"]}}
        status, output, summary, calls = self.harness_run(script=script)
        row = summary["bugs"][0]
        self.assertEqual((status, row["result"]), (1, "candidate"), output)
        self.assertEqual([(m["seed"], m["reason"]) for m in row["masked"]], [(1, "twin-red"), (2, "twin-red")])
        self.assertEqual([m["twin_kinds"] for m in row["masked"]], [[KIND], ["Other"]])
        self.assertEqual((row["found_seed"], row["tried"]), (3, 3))
        self.assertEqual(row["rates"], {"seeds": 3, "patched": {"expected": [1, 2, 3], "other": []},
                                        "base": {"expected": [1], "other": [2]}})
        self.assertIn("base rate 1 of 3, patched rate 3 of 3", output)
        self.assertEqual([(c["first"], c["count"]) for c in calls if c["expect"]], [(1, 5), (2, 4), (3, 3)])
        # Confirmed for the hit that is not masked, it counts, and the masked seeds stay in the result.
        self.record()
        self.set_detection(3)
        output, row, _ = self.assert_found(script=script)
        self.assertEqual([m["seed"] for m in row["masked"]], [1, 2])
        self.assertIn("2 masked", output)

    def test_a_nondeterministic_replay_is_masked(self):
        for flaky, seed in (("patched", 3), ("clean", 3)):
            with self.subTest(flaky=flaky):
                row = self.assert_not_counted(script={"hit": 3, "flaky": [flaky]}, seeds=3, result="incomplete")
                self.assertEqual([m["reason"] for m in row["masked"]],
                                 ["nondeterministic" if flaky == "patched" else "twin-red"])

    def test_a_masked_hit_never_counts_even_when_confirmed(self):
        self.assert_not_counted(script={"hit": 3, "clean": {"3": [KIND]}}, seeds=3, result="incomplete")

    def test_not_detected_in_ten_thousand(self):
        script = {"patched": {"7": [KIND], "40": ["Other"]}, "clean": {"7": [KIND], "9": [KIND]}}
        row = self.assert_not_counted("not detected in [1, 10000]: 10000 seeds tried, 1 masked",
                                      "base rate 2 of 10000, patched rate 1 of 10000", "0 of 1 reverted fixes found",
                                      script=script, seeds=None, result="not-detected")
        self.assertEqual((row["tried"], [m["seed"] for m in row["masked"]]), (10_000, [7]))
        self.assertEqual(row["rates"]["base"], {"expected": [7, 9], "other": []})
        self.assertEqual(row["rates"]["patched"], {"expected": [7], "other": [40]})

    def test_a_search_that_stops_early_is_not_a_result(self):
        row = self.assert_not_counted("incomplete", script={}, seeds=4, result="incomplete")
        self.assertEqual(row["tried"], 4)
        self.assert_not_counted("incomplete", script={"hit": 3}, seeds=2, result="incomplete")
        self.assert_not_counted("incomplete", "time limit", script={"hit": 3}, result="incomplete",
                                FAKE_HANG="1", argv=())

    def test_seeds_run_in_chunks(self):
        status, output, summary, calls = self.harness_run("--chunk", "2", script={"hit": 5, "clean": {"2": ["Other"]}})
        row = summary["bugs"][0]
        self.assertEqual((row["result"], row["found_seed"], row["tried"]), ("candidate", 5, 5), output)
        self.assertEqual([(c["patched"], c["first"], c["count"]) for c in calls if not c["replay"]],
                         [(True, 1, 2), (True, 3, 2), (True, 5, 1), (False, 1, 2), (False, 3, 2), (False, 5, 1)])
        self.assertEqual(row["rates"]["base"], {"expected": [], "other": [2]})

    def test_a_hit_is_only_a_candidate_until_it_is_confirmed(self):
        f = self.f
        text = (f.repo / BUG / "bug.toml").read_text()
        f.write(f"{BUG}/bug.toml", text[:text.index("[detection]")])
        f.commit("not confirmed")
        status, output, summary, _ = self.harness_run(script={"hit": 3})
        self.assertEqual(status, 1, output)
        self.assertIn("fix-one: candidate (NOT counted: no confirmed mechanism is recorded for this hit", output)
        self.assertIn("0 of 1 reverted fixes found", output)
        self.assertEqual(summary["bugs"][0]["result"], "candidate")
        # The log holds the line that `record` appends, so the attested log covers the record.
        line = next(line for line in output.splitlines() if line.startswith("sim-bugs-search: "))
        self.assertEqual(json.loads(line.split(" ", 1)[1]), summary["bugs"][0]["search"])

    def test_detector_must_have_reported_the_hit(self):
        self.f.edit(f'expect = ["{KIND}"]', f'expect = ["{KIND}", "failed-write-observed"]')
        self.f.edit(f'detector = "{KIND}"', 'detector = "failed-write-observed"')
        self.f.commit("another detector")
        self.assert_not_counted("failed-write-observed did not report this hit", result="candidate")
        self.f.edit('detector = "failed-write-observed"', 'detector = "Other"')
        self.assert_refused("detection.detector must be one of expect")

    def test_a_hit_that_shows_a_known_finding_is_masked_by_hand(self):
        f = self.f
        text = (f.repo / BUG / "bug.toml").read_text()
        masked = {**hit_identity(3), "finding": "DR-1", "reason": "a recomputed no-op, no lost reply",
                  "confirmed_by": "reviewer A", "confirmation": "https://github.com/o/r/pull/1#dr1"}
        f.write(f"{BUG}/bug.toml", text[:text.index("[detection]")] + toml_table("[masked]", masked).replace(
            "[[masked]]", "[[masked]]"))
        f.commit("seed 3 is DR-1")
        status, output, summary, _ = self.harness_run(script={"hit": 3})
        row = summary["bugs"][0]
        self.assertEqual((row["result"], row["found_seed"]), ("candidate", 4), output)
        self.assertEqual([(m["seed"], m["reason"], m["finding"]) for m in row["masked"]], [(3, "known-finding", "DR-1")])
        # The mask is for that hit only.
        f.edit(hit_identity(3)["trace_hash"], "0" * 15 + "1")
        f.commit("a mask of another hit")
        status, output, summary, _ = self.harness_run(script={"hit": 3})
        self.assertEqual(summary["bugs"][0]["found_seed"], 3, output)
        f.edit('finding = "DR-1"', 'finding = " "')
        status, output = f.run("check")
        self.assertEqual(status, 1, output)
        self.assertIn("masked", output)

    def test_one_hit_cannot_be_masked_and_detected(self):
        f = self.f
        masked = {**hit_identity(3), "finding": "DR-1", "reason": "r", "confirmed_by": "reviewer A", "confirmation": "c"}
        f.write(f"{BUG}/bug.toml", (f.repo / BUG / "bug.toml").read_text() + toml_table("[masked]", masked))
        self.assert_refused("masked and in detection")

    def test_rules_from_phase_11(self):
        f = self.f
        text = (f.repo / BUG / "bug.toml").read_text()
        where = "server/sim-oltp/bugs/phase11/fix-one"
        searches = (f.repo / BUG / "searches.jsonl").read_text()
        shutil.rmtree(f.repo / BUG)
        text = text.replace("phase = 10", "phase = 11")
        f.write(f"{where}/revert.patch", f.patch)
        f.write(f"{where}/searches.jsonl", searches)
        f.write(f"{where}/bug.toml", text)
        status, output = f.run("check")
        self.assertEqual(status, 1, output)
        self.assertIn("cannot leave the tree", output)
        rule = {"kind": KIND, "source": "docs/testing/guarantees/oltp.md", "text": "One outcome"}
        for change, needle in (({}, None), ({"kind": "Other"}, "rules"), ({"text": "Two outcomes"}, "is not in"),
                               ({"source": " "}, "rules")):
            f.write(f"{where}/bug.toml", text + toml_table("[rules]", {**rule, **change}))
            _, output = f.run("check")
            if needle:
                self.assertIn(needle, output)
            else:
                self.assertNotIn("rules", output)

    def test_run_never_passes_on_a_tooling_or_harness_fault(self):
        cases = (
            ({"silent": True}, "seed lines"),
            ({"out_of_budget": True}, "seed lines"),
            ({"duplicate": True}, "seed lines"),
            ({"patched_search_exit": 0}, "exited 0"),
            ({"patched_search_exit": 139}, "exited 139"),
            ({"patched_replay_exit": 0}, "exited 0"),
            ({"clean_replay_exit": 101}, "exited 101"),
            ({"clean_base_exit": 101}, "exited 101"),
        )
        for script, needle in cases:
            with self.subTest(script=script):
                hit = 9 if any(key.endswith("search_exit") for key in script) else 3
                self.assert_not_counted(needle, "0 of 1 reverted fixes found", script={"hit": hit, **script},
                                        result="error")

    def test_run_is_not_run_while_the_workload_is_missing(self):
        f = self.f
        text = (f.repo / BUG / "bug.toml").read_text()
        f.write(f"{BUG}/bug.toml", text[:text.index("[detection]")].replace('workload = "direct"', 'workload = "cold"'))
        f.commit("a workload the harness lacks")
        status, output, _, _ = self.harness_run()
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
        self.assertNotIn("sim-bugs-search", output)
        self.assertEqual(summary["mode"], "rehearsal")
        self.assertEqual([(c["defect"], c["profile"]) for c in calls],
                         [("lost-write", None), ("lost-write", None), (None, None), (None, None)])
        # A rehearsal is never a recorded search.
        status, output = self.f.run("record", "fix-one", "--from", str(self.root / "out/sim-bugs-run.json"))
        self.assertEqual(status, 2, output)
        self.assertIn("rehearsal", output)

    def test_record_is_a_hash_chain(self):
        f = self.f
        self.harness_run(script={"hit": 3})
        self.record()
        lines = (f.repo / BUG / "searches.jsonl").read_text().splitlines()
        first, second = map(json.loads, lines)
        self.assertEqual((first["n"], first["prev"], second["n"]), (1, None, 2))
        self.assertEqual(second["prev"], hashlib.sha256(lines[0].encode()).hexdigest())
        # A search of another commit's patch or of another target is not this target's.
        status, output = f.run("record", "fix-one", "--from", str(self.root / "missing.json"))
        self.assertEqual(status, 2, output)
        # Removing the whole target does not remove its searches.
        shutil.rmtree(f.repo / BUG)
        f.write("server/sim-oltp/bugs/other/bug.toml", "x = 1\n")
        status, output = f.run("check")
        self.assertEqual(status, 1, output)
        self.assertIn("cannot leave the tree", output)

    def test_evidence_is_compared_with_the_attested_log(self):
        f = self.f
        digest = sim_bugs.digest(f.patch)
        log = ("fix-one: without the patch passed\n"
               "thread 'main' panicked: assertion failed: lost the uncertain outcome\n"
               "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 9 filtered out\n"
               f"fix-one: with the patch failed; verified (digest {digest})\n")
        record = {"schema": "nabedra.evidence/1", "check": "oltp-sim-bugs-regression", "sha": self.tested,
                  "tree_clean": True, "result": "pass", "run": {"url": "https://github.com/o/r/actions/runs/1"},
                  "artifacts": [{"name": "a.log", "sha256": hashlib.sha256(log.encode()).hexdigest()}]}
        records = self.root / "records"

        def run(log_text=log, **changes):
            shutil.rmtree(records, ignore_errors=True)
            records.mkdir()
            (records / "a.log").write_text(log_text)
            (records / "a.json").write_text(json.dumps({**record, **changes}))
            return f.run("evidence", "--records", str(records))

        status, output = run()
        self.assertEqual(status, 0, output)
        self.assertIn("fix-one: the recorded lines are in the log", output)
        for changes in ({"log_text": log + "x"}, {"sha": "d" * 40}, {"tree_clean": False}, {"result": "fail"},
                        {"check": "other"}, {"run": {"url": "https://github.com/o/r/actions/runs/2"}}):
            with self.subTest(changes=changes):
                status, output = run(**changes)
                self.assertEqual(status, 1, output)
        f.edit("lost the uncertain outcome\\n", "lost the uncertain outcome, at seed 5\\n")
        status, output = run()
        self.assertEqual(status, 1, output)
        self.assertIn("not a line of the attested log", output)

    def test_evidence_from_scratch_commits(self):
        # Phase 11: the test ran on a commit that holds the patch, and passed on one that does not.
        f = self.f
        clean = sh(f.repo, "rev-parse", "HEAD").strip()
        f.write("server/src/lib.rs", LIB)
        patched = f.commit("scratch: the patch")
        f.write("server/src/lib.rs", FIXED)
        f.write("server/src/other.rs", "pub fn other() {}\n")
        f.commit("scratch: undo, and something else")
        f.write("server/src/lib.rs", LIB)
        more = f.commit("scratch: the patch again")
        regression = f.evidence()
        e = regression["evidence"]
        e["patched"].update(sha=patched, output=e["patched"]["output"].split("fix-one:")[0])
        e["clean"].update(sha=clean, run="https://github.com/o/r/actions/runs/2")
        f.write("server/src/lib.rs", FIXED)
        f.commit("back to main")
        bug = lambda: sim_bugs.Bug(f.repo / BUG, {"id": "fix-one", "regression": regression}, f.patch)
        self.assertEqual(sim_bugs.regression_evidence(bug(), f.repo), ([], []))
        e["patched"]["sha"] = more
        errors, _ = sim_bugs.regression_evidence(bug(), f.repo)
        self.assertEqual(errors, [])
        e["patched"]["sha"] = clean
        errors, _ = sim_bugs.regression_evidence(bug(), f.repo)
        self.assertTrue(any("does not differ" in problem for problem in errors), errors)
        e["patched"]["sha"] = "e" * 40
        errors, uncounted = sim_bugs.regression_evidence(bug(), f.repo)
        self.assertEqual(errors, [])
        self.assertTrue(any("is not available" in problem for problem in uncounted), uncounted)

    def test_seed_lines_and_first_match(self):
        output = "\n".join([
            'test suite::sweep ... nabedra-sim-seed: {"seed": 1, "result": "pass", "kinds": []}',
            'nabedra-sim-seed: {"seed": 2, "result": "violation", "kinds": ["Other"]}',
            'nabedra-sim-seed: {"seed": 3, "result": "violation", "kinds": ["Liveness"]}',
        ])
        seeds = sim_bugs.parse_seeds(output)
        self.assertEqual([s["seed"] for s in seeds], [1, 2, 3])
        self.assertEqual(sim_bugs.first_match(seeds, ["Liveness"])["seed"], 3)
        self.assertIsNone(sim_bugs.first_match(seeds, ["NotAppliedNeverCommitted"]))
        for bad in ('nabedra-sim-seed: {"seed": 4, "result": "violation", "kinds": "Other"}',
                    'nabedra-sim-seed: {"seed": 4, "result": "pass", "kinds": ["Other"]}',
                    'nabedra-sim-seed: {"seed": 4, "result": "violation", "kinds": []}',
                    'nabedra-sim-seed: {"seed": true, "result": "pass", "kinds": []}',
                    'nabedra-sim-seed: {"seed": 4, "seed": 5, "result": "pass", "kinds": []}',
                    'noise nabedra-sim-seed: {"seed": 4, "result": "pass", "kinds": []}',
                    "nabedra-sim-seed: not json"):
            with self.subTest(bad=bad), self.assertRaises(sim_bugs.Failure):
                sim_bugs.parse_seeds(output + "\n" + bad)


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

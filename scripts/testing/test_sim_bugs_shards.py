"""Shards, their merge and the counting run of sim_bugs.py, with a fake harness and a fake `gh attestation verify`."""

import argparse
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sim_bugs  # noqa: E402
from test_sim_bugs import (  # noqa: E402
    BUG, FAKE_KEPT, FAKE_SIM_CARGO, FAKE_SWEEP, HarnessCase, KIND, detection, hit_identity, sh, toml_table)

REPO = sim_bugs.REPOSITORY
WORKFLOW = f"https://github.com/{REPO}/.github/workflows/nabedra-build.yml"

# `gh attestation verify FILE --bundle B ...`. The bundle names the subject's digest and the certificate.
FAKE_GH = '''
import hashlib, json, sys
from pathlib import Path
if sys.argv[1:3] != ["attestation", "verify"]:
    sys.exit("unknown command")
args = sys.argv[3:]
bundle = json.loads(Path(args[args.index("--bundle") + 1]).read_text())
if bundle["digest"] != hashlib.sha256(Path(args[0]).read_bytes()).hexdigest():
    sys.exit("no matching attestations")
certificate = bundle["certificate"]
assert args[args.index("--repo") + 1] == "zPy52/nabedra-database"
assert args[args.index("--signer-workflow") + 1].endswith("/.github/workflows/nabedra-build.yml")
assert "--deny-self-hosted-runners" in args and args[args.index("--format") + 1] == "json"
print(json.dumps([{"verificationResult": {"signature": {"certificate": certificate}}}]))
'''


def canonical(data):
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def sha256(data):
    return hashlib.sha256(data.encode() if isinstance(data, str) else data).hexdigest()


class ShardCase(HarnessCase):
    PARTS = 2500  # Four seeds each. The budget stays 10,000.

    def tool(self, *argv, script=None, run=7, attempt="1", **env):
        """One command of the tool against the fakes: (status, output, calls of the fake sweep)."""
        f = self.f
        for name, text in (("fake_sim_cargo.py", FAKE_SIM_CARGO), ("fake_sweep.py", FAKE_SWEEP), ("fake_gh.py", FAKE_GH)):
            (self.root / name).write_text(text)
        calls = self.root / "calls.jsonl"
        calls.unlink(missing_ok=True)
        shutil.rmtree(self.root / "out", ignore_errors=True)
        env = {"FAKE_SWEEP": str(self.root / "fake_sweep.py"), "FAKE_CALLS": str(calls), "RUSTFLAGS": "",
               "FAKE_SCRIPT": json.dumps(script or {}), "GITHUB_SERVER_URL": "https://github.com",
               "GITHUB_REPOSITORY": REPO, "GITHUB_RUN_ID": str(run), "GITHUB_RUN_ATTEMPT": attempt, **env}
        out = StringIO()
        with patch.dict(os.environ, env), FAKE_KEPT:
            status = sim_bugs.main([*argv, "--cargo", f"{sys.executable} {self.root / 'fake_sim_cargo.py'}",
                                    "--gh", f"{sys.executable} {self.root / 'fake_gh.py'}",
                                    "--target-dir", str(self.root / "target"), "--out", str(self.root / "out")],
                                   repo=f.repo, out=out)
        lines = calls.read_text().splitlines() if calls.exists() else []
        return status, out.getvalue(), [json.loads(line) for line in lines]

    def shard(self, n, *argv, parts=None, **kwargs):
        return self.tool("shard", "fix-one", "--shard", f"{n}/{parts or self.PARTS}", *argv, **kwargs)

    @property
    def records(self):
        path = self.root / "records"
        path.mkdir(exist_ok=True)
        return path

    def keep(self, n, log, run=7, attempt=1, name=None, record=None, certificate=None, bundle=True):
        """The evidence of one shard as it is downloaded: the record, its log and its attestation."""
        name = name or f"{run}-{attempt}-{n}"
        (self.records / f"{name}.log").write_text(log)
        sha = sh(self.f.repo, "rev-parse", "HEAD").strip()
        url = f"https://github.com/{REPO}/actions/runs/{run}"
        record = {"schema": "nabedra.evidence/1", "check": f"oltp-sim-bugs-fix-one-{n}", "sha": sha, "tree_clean": True,
                  "result": "pass", "run": {"url": url, "attempt": attempt},
                  "artifacts": [{"name": f"{name}.log", "sha256": sha256(log)}], **(record or {})}
        text = json.dumps(record)
        (self.records / f"{name}.json").write_text(text)
        if bundle:
            certificate = {"sourceRepositoryURI": f"https://github.com/{REPO}", "sourceRepositoryDigest": record["sha"],
                           "buildSignerURI": f"{WORKFLOW}@refs/heads/main", "runnerEnvironment": "github-hosted",
                           "buildTrigger": "workflow_dispatch", "runInvocationURI": f"{url}/attempts/{attempt}",
                           **(certificate or {})}
            (self.records / f"sha256-{sha256(text)}.jsonl").write_text(
                json.dumps({"digest": sha256(text), "certificate": certificate}))
        return text

    def run_shards(self, *numbers, script=None, argv=(), **kwargs):
        """Run and keep these shards. Returns their logs."""
        logs = []
        for n in numbers:
            status, output, _ = self.shard(n, *argv, script=script, run=100 + n, **kwargs)
            self.assertEqual(status, 0, output)
            self.keep(n, output, run=100 + n)
            logs.append(output)
        return logs

    def merge(self, *argv):
        status, output, _ = self.tool("merge", "fix-one", "--records", str(self.records), *argv)
        line = [line for line in output.splitlines() if line.startswith("sim-bugs-merge: ")]
        return status, output, json.loads(line[0].split(" ", 1)[1]) if line else None

    def assert_merge(self, result, *needles, status=None):
        seen, output, body = self.merge()
        self.assertIsNotNone(body, output)
        self.assertEqual(body["result"], result, output)
        self.assertEqual(seen, (0 if result in ("candidate", "not-detected") else 1) if status is None else status, output)
        for needle in needles:
            self.assertIn(needle, output)
        return body

    def answer(self):
        """The merge's answer without `check`, for many cases: (body, output)."""
        if not hasattr(self, "bug"):
            self.bug, = sim_bugs.load_bugs(self.f.repo)[0]
            (self.root / "fake_gh.py").write_text(FAKE_GH)
        args = argparse.Namespace(records=str(self.records), gh=f"{sys.executable} {self.root / 'fake_gh.py'}", seeds=None,
                                  workload=None, profile=None, expect=None, rehearse_defect=None)
        out = StringIO()
        return sim_bugs.merged(args, self.f.repo, self.bug, out)[1].body, out.getvalue()

    def assert_no_shard(self, *needles):
        body, output = self.answer()
        self.assertEqual((body["result"], body["shards"], body["rates"]), ("incomplete", [], None), output)
        self.assertIn("shard 1 of", body["why"])
        for needle in needles:
            self.assertIn(needle, output)

    def count(self, *argv, script=None, **env):
        status, output, calls = self.tool("count", "fix-one", "--records", str(self.records), *argv, script=script, **env)
        summary = sim_bugs.read_json(self.root / "out/sim-bugs-count.json")
        return status, output, (summary or {"bugs": [None]})["bugs"][0], calls

    def assert_not_found(self, result, *needles, script=None, **kwargs):
        status, output, row, _ = self.count(script=script, **kwargs)
        self.assertEqual(status, 1, output)
        self.assertEqual(row and row["result"], result, output)
        for needle in needles:
            self.assertIn(needle, output)
        return row

    def record_merge(self):
        status, output, body = self.merge("--record")
        self.assertEqual(status, 0, output)
        self.f.commit("record the merge")
        return body

    def confirm_hit(self, seed, body, **changes):
        text = (self.f.repo / BUG / "bug.toml").read_text().split("[detection]")[0]
        table = detection(seed, **{"search": sha256(canonical(body)), **changes})
        self.f.write(f"{BUG}/bug.toml", text + toml_table("detection", table))
        self.f.commit("confirm the hit")

    def mask_by_hand(self, seed):
        text = (self.f.repo / BUG / "bug.toml").read_text().split("[detection]")[0]
        masked = {**hit_identity(seed), "finding": "DR-1", "reason": "a recomputed no-op", "confirmed_by": "reviewer A",
                  "confirmation": "https://github.com/o/r/pull/1#dr1"}
        self.f.write(f"{BUG}/bug.toml", text + toml_table("[masked]", masked))
        self.f.commit(f"seed {seed} is DR-1")

    def edit_log(self, log, change, reseal=True):
        """A shard's log with its rows changed. `reseal` makes its last line fit them again."""
        lines = log.splitlines()
        rows = [json.loads(line.split(" ", 1)[1]) for line in lines if line.startswith("sim-bugs-row: ")]
        done = next(line for line in lines if line.startswith("sim-bugs-shard: complete "))
        rows = change(rows) or rows
        texts = [canonical(row) for row in rows]
        done = json.loads(done.removeprefix("sim-bugs-shard: complete "))
        if reseal:
            done.update(rows=len(rows), rows_sha256=sha256("".join(text + "\n" for text in texts)))
        head = [line for line in lines if not line.startswith(("sim-bugs-row: ", "sim-bugs-shard: "))]
        return "\n".join([*head, *(f"sim-bugs-row: {text}" for text in texts),
                          f"sim-bugs-shard: complete {canonical(done)}"]) + "\n"

    def edit_done(self, log, **changes):
        head, _, line = log.rpartition("sim-bugs-shard: complete ")
        return f"{head}sim-bugs-shard: complete {canonical({**json.loads(line), **changes})}\n"


def rows_of(log):
    return [json.loads(line.split(" ", 1)[1]) for line in log.splitlines() if line.startswith("sim-bugs-row: ")]


def done_of(log):
    line, = [line for line in log.splitlines() if line.startswith("sim-bugs-shard: complete ")]
    return json.loads(line.removeprefix("sim-bugs-shard: complete "))


class ShardTest(ShardCase):
    """One shard: every seed of its range on both builds, and one line that says it finished."""

    def test_a_complete_shard(self):
        script = {"hit": 2, "clean": {"3": ["Other"]}, "patched": {"1": ["Other"]}}
        status, output, calls = self.shard(1, script=script)
        self.assertEqual(status, 0, output)
        rows, done = rows_of(output), done_of(output)
        self.assertEqual([row["seed"] for row in rows], [1, 2, 3, 4])
        # The base run first, then the patched build on every seed. Only an expected kind on a green base is judged.
        self.assertEqual([(c["patched"], c["first"], c["count"], c["replay"]) for c in calls],
                         [(False, 1, 4, False), (True, 1, 4, False), (True, 2, 1, True), (False, 2, 1, True),
                          (True, 4, 1, True), (False, 4, 1, True)])
        self.assertEqual({(c["workload"], c["profile"], c["expect"], c["defect"]) for c in calls},
                         {("direct", "lost-reply", None, None)})
        self.assertEqual([("judged" in row) for row in rows], [False, True, False, True])
        self.assertEqual(rows[0], {"seed": 1, "base": {"kinds": [], "trace": "%016x" % 7919, "deterministic": None},
                                   "patched": {"kinds": ["Other"], "trace": "%016x" % 7920, "deterministic": True}})
        self.assertEqual(rows[1]["judged"], {
            "replay": {"kinds": [KIND], "trace_hash": hit_identity(2)["trace_hash"],
                       "history_sha256": hit_identity(2)["history_sha256"], "deterministic": True},
            "twin": {"kinds": [], "trace_hash": "%016x" % (2 * 7919), "deterministic": True}})
        self.assertEqual(rows[2]["base"]["kinds"], ["Other"])
        f = self.f
        plan = {"workload": "direct", "profile": ["lost-reply"], "expect": [KIND], "seeds": [1, 10000]}
        self.assertEqual({key: done[key] for key in ("id", "mode", "plan", "shard", "seeds", "rows", "sha", "attempt")},
                         {"id": "fix-one", "mode": "evidence", "plan": plan, "shard": [1, 2500], "seeds": [1, 4], "rows": 4,
                          "sha": sh(f.repo, "rev-parse", "HEAD").strip(), "attempt": "1"})
        self.assertEqual(done["run"], f"https://github.com/{REPO}/actions/runs/7")
        self.assertEqual(done["review"], sha256(f.patch))
        self.assertEqual(done["rows_sha256"], sha256("".join(canonical(row) + "\n" for row in rows)))
        self.assertNotEqual(done["binaries"]["clean"], done["binaries"]["patched"])
        # Nothing of it is a decision, and nothing is left of the patch.
        self.assertNotIn("found", output.replace("not found", ""))
        self.assertNotIn("sim-bugs-search", output)
        self.assertEqual(len(sh(f.repo, "worktree", "list").splitlines()), 1)
        self.assertTrue(output.splitlines()[-1].startswith("sim-bugs-shard: complete "), output)

    def test_the_ranges_cover_every_seed_once(self):
        for total, parts in ((10_000, 4), (10_000, 2500), (10_000, 3), (200, 7)):
            ranges = [sim_bugs.shard_range(1, total, n, parts) for n in range(1, parts + 1)]
            self.assertEqual([seed for first, last in ranges for seed in range(first, last + 1)],
                             list(range(1, total + 1)))
        self.assertEqual([sim_bugs.shard_range(1, 10_000, n, 4) for n in (1, 2, 3, 4)],
                         [(1, 2500), (2501, 5000), (5001, 7500), (7501, 10_000)])

    def test_shard_arguments(self):
        for value in ("0/4", "5/4", "1", "1/0", "-1/4", "1/4/2", "one/4", "1/10001"):
            with self.subTest(value=value):
                with self.assertRaises(SystemExit) as raised, patch("sys.stderr"):
                    self.tool("shard", "fix-one", "--shard", value)
                self.assertEqual(raised.exception.code, 2)
        for argv in (["shard", "fix-one"], ["shard", "fix-one", "--shard", "1/4", "--set", "direct"]):
            status, output, calls = self.tool(*argv)
            self.assertEqual((status, calls), (2, []), output)
        # A shard cannot choose its seeds, whatever its run is called, and neither can its merge.
        records = ("--records", str(self.records))
        for argv in (["shard", "fix-one", "--shard", "1/2"], ["merge", "fix-one", *records], ["count", "fix-one", *records]):
            status, output, calls = self.tool(*argv, "--seeds", "4")
            self.assertEqual((status, calls), (2, []), output)
            self.assertIn("--seeds is for a rehearsal", output)
        self.assertEqual(self.shard(1, "--profile", "x", "--seeds", "2")[0], 2)

    def test_a_shard_runs_in_chunks(self):
        _, output, calls = self.shard(2, "--chunk", "3", script={"hit": 7})
        self.assertEqual([(c["patched"], c["first"], c["count"]) for c in calls if not c["replay"]],
                         [(False, 5, 3), (False, 8, 1), (True, 5, 3), (True, 8, 1)])
        self.assertEqual([row["seed"] for row in rows_of(output)], [5, 6, 7, 8])
        self.assertEqual(["judged" in row for row in rows_of(output)], [False, False, True, True])

    def test_a_shard_that_dies_says_nothing(self):
        cases = [({"kill": kill, "hit": 3}, "died with signal 9", {}) for kill in
                 ("clean_base", "patched_base", "patched_replay", "clean_replay")]
        cases += [({"clean_base_skip": [2]}, "seed lines", {}), ({"patched_base_skip": [4]}, "seed lines", {}),
                  ({"clean_base_exit": 101}, "exited 101", {}), ({"patched_base_exit": 0, "hit": 3}, "exited 0", {}),
                  ({"silent": True}, "seed lines", {}),
                  ({"clean_row": {"violations": ["Other"]}}, "disagree", {}),
                  ({"patched_row_without": ["trace_hash"]}, "no trace", {}),
                  ({"clean_base_row": {"trace_hash": "0" * 16}}, "no trace", {}),
                  ({"hit": 3, "patched_base_row": {"replayed": False}}, "not replayed", {}),
                  ({"hit": 3, "patched_base_row": {"deterministic": False}}, "deterministic", {}),
                  ({"hit": 3}, "time limit", {"FAKE_HANG": "base"})]
        for script, needle, env in cases:
            with self.subTest(script=script):
                argv = ["--time-limit", "4"] if env else []
                status, output, _ = self.shard(1, *argv, script=script, **env)
                self.assertEqual(status, 1, output)
                self.assertIn("sim-bugs-shard: error", output)
                self.assertIn(needle, output)
                self.assertNotIn("sim-bugs-shard: complete", output)
                self.assertEqual(rows_of(output), [])

    def test_the_twin_has_to_be_the_base_run(self):
        # The base run and the clean replay of seed 3 differ: nondeterminism, so nothing of the shard counts.
        status, output, _ = self.shard(1, script={"hit": 3, "clean_base_row": {"trace_hash": "00000000000000aa"}})
        self.assertEqual(status, 1, output)
        self.assertIn("the twin of seed 3 is not its base run", output)
        self.assertNotIn("sim-bugs-shard: complete", output)
        status, output, _ = self.shard(1, script={"hit": 3, "clean_replay": {"3": ["Other"]}})
        self.assertEqual(status, 1, output)
        self.assertIn("the twin of seed 3 is not its base run", output)

    def test_a_red_base_seed_is_masked_without_a_replay(self):
        _, output, calls = self.shard(1, script={"hit": 1, "clean": {"1": [KIND], "2": ["Other"]}})
        self.assertEqual([c["first"] for c in calls if c["replay"]], [3, 3, 4, 4])
        self.keep(1, output)
        body = self.assert_merge("candidate")
        self.assertEqual((body["hit"]["seed"], body["masked"]), (3, {"base-red": [1, 2]}))

    def test_a_base_seed_that_is_not_deterministic_is_red(self):
        script = {"patched": {"2": [KIND]}, "clean": {"2": ["Other"]}, "flaky": ["clean"]}
        status, output, _ = self.shard(1, script=script)
        row = rows_of(output)[1]
        self.assertEqual((row["base"]["kinds"], row["base"]["deterministic"]), (["Other", "determinism"], False))
        self.assertEqual(sim_bugs.row_mask(row, [KIND]), (True, "base-red"))
        # A seed that passes once and differs only as a twin.
        status, output, _ = self.shard(1, script={"hit": 3, "flaky": ["clean"]})
        self.assertEqual(status, 1, output)
        self.assertIn("the twin of seed 3 is not its base run", output)

    def test_masks_of_the_replay(self):
        for script, reason, twin in (
                ({"hit": 3, "flaky": ["patched"]}, "harness-fault", None),
                ({"hit": 3, "patched_replay": {"3": ["Other"]}}, "search-replay-disagree", None),
                ({"hit": 3, "patched_replay": {"3": [KIND, "Other"]}}, "search-replay-disagree", None),
                ({"hit": 3, "patched_replay": {"3": [KIND, "harness"]}, "patched": {"4": []}}, "harness-fault", None),
                ({"hit": 3, "patched_base_row": {"trace_hash": "00000000000000bb"}}, "search-replay-disagree", None),
                ({"hit": 3, "same_trace": True}, "same-trace", True)):
            with self.subTest(script=script):
                status, output, _ = self.shard(1, script=script)
                self.assertEqual(status, 0, output)
                row = rows_of(output)[2]
                self.assertEqual(sim_bugs.row_mask(row, [KIND]), (True, reason))
                self.assertEqual(row["judged"]["twin"] is not None, bool(twin))

    def test_a_seed_that_panics_in_the_harness_is_a_row(self):
        _, output, _ = self.shard(1, script={"patched": {"2": ["harness"]}, "patched_row": {}, "clean": {"3": ["harness"]}})
        rows = rows_of(output)
        self.assertEqual([row["patched"]["kinds"] for row in rows], [[], ["harness"], [], []])
        self.keep(1, output)
        body = self.assert_merge("incomplete")
        self.assertEqual(body["rates"]["patched"], {"expected": 0, "other": 1, "fault": 1})
        self.assertEqual(body["rates"]["base"], {"expected": 0, "other": 1, "fault": 1})

    def test_the_same_binary_is_no_search(self):
        status, output, _ = self.shard(1, FAKE_SAME_BINARY="1")
        self.assertEqual(status, 1, output)
        self.assertIn("builds the same sweep binary", output)
        self.assertNotIn("sim-bugs-shard: complete", output)

    def test_every_job_builds_in_one_place(self):
        # A sweep holds its tree's path, so another place would be another binary.
        places = []
        with patch.object(sim_bugs, "build_patched", side_effect=lambda args, scratch, harness, bug: (
                places.append(str(scratch.path)), (_ for _ in ()).throw(sim_bugs.Failure("enough")))[0]):
            self.shard(1)
            self.shard(2)
            self.tool("run", "fix-one", "--seeds", "2")
        self.assertEqual(places[0], places[1])
        self.assertNotEqual(places[0], places[2])
        # The place of another run, or of one that died, is not taken over.
        for name in (places[0], places[0] + "-tmp"):
            os.mkdir(name)
            self.addCleanup(shutil.rmtree, name, True)
            status, output, calls = self.shard(1)
            self.assertEqual((status, calls, os.listdir(name)), (2, [], []), output)
            self.assertIn("another run's", output)
            os.rmdir(name)
        self.assertEqual(self.shard(1)[0], 0)

    def test_a_target_that_cannot_count_has_no_shards(self):
        f = self.f
        text = (f.repo / BUG / "bug.toml").read_text()
        f.write(f"{BUG}/bug.toml", text.replace("[regression]", 'hold = "a reason"\n\n[regression]'))
        f.commit("held")
        status, output, calls = self.shard(1)
        self.assertEqual((status, calls), (1, []), output)
        self.assertIn("not shown, not searched", output)
        status, output, row, calls = self.count()
        self.assertEqual((status, row["result"], calls), (1, "not-shown", []), output)

    def test_a_manifest_that_check_refuses(self):
        self.f.edit('verdict = "sensitive"', 'verdict = "surely"')
        self.f.commit("a verdict that does not exist")
        for status, output, calls in (self.shard(1), self.tool("merge", "fix-one", "--records", str(self.records)),
                                      self.tool("count", "fix-one", "--records", str(self.records))):
            self.assertEqual((status, calls), (1, []), output)
            self.assertIn("fix-one: FAILED", output)
            self.assertNotIn("incomplete", output)

    def test_a_working_tree_that_is_not_head(self):
        self.f.edit('port_notes = "the one hunk"', 'port_notes = "another hunk"')
        for status, output, calls in (self.shard(1), self.tool("count", "fix-one", "--records", str(self.records))):
            self.assertEqual((status, calls), (1, []), output)
            self.assertIn("the working tree differs from HEAD", output)

    def test_a_missing_workload_is_not_run(self):
        f = self.f
        text = (f.repo / BUG / "bug.toml").read_text()
        f.write(f"{BUG}/bug.toml", text[:text.index("[detection]")].replace('workload = "direct"', 'workload = "cold"'))
        f.commit("a workload the harness lacks")
        status, output, _ = self.shard(1)
        self.assertEqual(status, 0, output)
        self.assertIn(sim_bugs.NOT_RUN_MARKER, output)
        self.assertNotIn("sim-bugs-shard: complete", output)
        # Its record is not-run, and the merge takes it for nothing.
        self.keep(1, output, record={"result": "not-run"})
        self.assert_merge("incomplete")

    def test_the_build_is_the_tree_without_the_targets_files(self):
        f = self.f
        before = sim_bugs.build_tree(f.repo, "HEAD")
        f.edit('port_notes = "the one hunk"', 'port_notes = "the hunk"')
        f.write("server/sim-oltp/bugs/README.md", "text\n")
        f.commit("the targets' own files")
        self.assertEqual(sim_bugs.build_tree(f.repo, "HEAD"), before)
        for name in ("server/src/lib.rs", "server/sim-oltp/src/sweep.rs", "scripts/testing/sim_bugs.py", "rust-toolchain.toml",
                     "server/sim-oltp/bugs/other.txt", ".github/workflows/nabedra-build.yml"):
            with self.subTest(name=name):
                self.f.write(name, f"another {name}\n")
                self.f.commit(name)
                after = sim_bugs.build_tree(f.repo, "HEAD")
                self.assertNotEqual(after, before)
                before = after


class MergeTest(ShardCase):
    """The answer of one target's attested shards."""

    def test_the_lowest_hit_of_complete_ranges(self):
        self.run_shards(1, 2, script={"hit": 6})
        body = self.assert_merge("candidate", "fix-one: candidate", "base rate 0 of 6, patched rate 1 of 6")
        self.assertEqual((body["hit"]["seed"], body["tried"], body["seeds"]), (6, 6, [1, 10000]))
        self.assertEqual({key: body["hit"][key] for key in ("trace_hash", "history_sha256")},
                         {key: hit_identity(6)[key] for key in ("trace_hash", "history_sha256")})
        self.assertEqual(body["hit"]["twin_trace_hash"], "%016x" % (6 * 7919))
        self.assertEqual(body["rates"], {"seeds": 6, "patched": {"expected": 1, "other": 0, "fault": 0},
                                         "base": {"expected": 0, "other": 0, "fault": 0}})
        self.assertEqual([(s["shard"], s["seeds"], s["check"], s["attempt"]) for s in body["shards"]],
                         [([1, 2500], [1, 4], "oltp-sim-bugs-fix-one-1", 1), ([2, 2500], [5, 8], "oltp-sim-bugs-fix-one-2", 1)])
        self.assertEqual(body["later"], [7, 8])
        # A range above the hit changes nothing.
        self.run_shards(3, script={"hit": 6})
        self.assertEqual(self.assert_merge("candidate"), body)

    def test_not_detected_needs_every_range(self):
        script = {"patched": {"7": [KIND], "40": ["Other"]}, "clean": {"7": [KIND], "9": [KIND]}}
        self.run_shards(1, 2, 3, script=script, parts=4, argv=("--chunk", "2500"))
        body = self.assert_merge("incomplete", "shard 4 of 4")
        self.assertEqual((body["tried"], body["hit"]), (7500, None))
        self.run_shards(4, script=script, parts=4, argv=("--chunk", "2500"))
        body = self.assert_merge("not-detected", "not detected in [1, 10000]: 10000 seeds tried, 1 masked",
                                 "base rate 2 of 10000, patched rate 1 of 10000")
        self.assertEqual((body["masked"], body["tried"]), ({"base-red": [7]}, 10_000))
        self.record_merge()
        status, output, row, calls = self.count()
        self.assertEqual((status, row["result"], row["tried"], calls), (1, "not-detected", 10_000, []), output)
        self.assertIn("not detected in [1, 10000]: 10000 seeds tried, 1 masked; base rate 2 of 10000", output)

    def test_a_missing_range_makes_it_incomplete(self):
        self.run_shards(2, script={"hit": 6})
        body = self.assert_merge("incomplete", "shard 1 of 2500")
        self.assertEqual((body["tried"], body["hit"], body["rates"]), (0, None, None))
        self.run_shards(1, 3, script={"hit": 10})
        body = self.assert_merge("candidate")
        self.assertEqual(body["hit"]["seed"], 6)

    def test_no_records_at_all(self):
        body = self.assert_merge("incomplete", "shard 1 of")
        self.assertEqual(body["shards"], [])
        status, output, _ = self.tool("merge", "fix-one", "--records", str(self.root / "nowhere"))
        self.assertEqual(status, 2, output)
        status, output, _ = self.tool("merge", "fix-one")
        self.assertEqual((status, "evidence/sim-bugs" in output), (1, True), output)

    def test_a_gap_below_the_hit(self):
        self.run_shards(1, 3, script={"hit": 9})
        self.assert_merge("incomplete", "shard 2 of 2500")

    def test_a_log_that_is_not_whole(self):
        status, output, _ = self.shard(1, script={"hit": 3})
        self.assertEqual(status, 0, output)
        row = lambda n: next(line for line in output.splitlines() if line.startswith(f'sim-bugs-row: ') and f'"seed":{n}' in line)
        done = output.splitlines()[-1]
        cases = {
            "no line at the end": output.replace(done, ""),
            "a cut line": output.replace(done, done[:-5]),
            "two lines": output + done + "\n",
            "a line of other rows": output.replace('"base":{"deterministic":null', '"base":{"deterministic":true', 1),
            "a missing row": output.replace(row(2) + "\n", ""),
            "a doubled row": output.replace(row(2), row(2) + "\n" + row(2)),
            "an unreadable row": output.replace(row(2), row(2)[:-4]),
            "rows in another order": output.replace(row(2), "@").replace(row(1), row(2)).replace("@", row(1)),
            "a row after the line": output + row(2) + "\n",
            "text before a row": output.replace(row(2), "x " + row(2)),
            "a line of an error": output + "sim-bugs-shard: error, something\n",
            "a line of an error before the end": output.replace(done, "sim-bugs-shard: error, something\n" + done),
        }
        for what, log in cases.items():
            with self.subTest(what=what):
                self.assertNotEqual(log, output)
                shutil.rmtree(self.records)
                self.keep(1, log)
                self.assert_no_shard()

    def test_rows_that_do_not_fit_their_range(self):
        _, output, _ = self.shard(1, script={"hit": 3})

        def set_row(n, **changes):
            return lambda rows: rows[n].update(changes)
        judged = rows_of(output)[2]["judged"]
        cases = {
            "a missing seed": lambda rows: rows[:1] + rows[2:],
            "a missing last seed": lambda rows: rows[:3],
            "a doubled seed": lambda rows: rows[:2] + rows[1:],
            "a foreign seed": set_row(1, seed=9),
            "another order": lambda rows: [rows[1], rows[0], *rows[2:]],
            "a seed as text": set_row(1, seed="2"),
            "a field too many": set_row(1, note="x"),
            "no base": lambda rows: rows[1].pop("base") and None,
            "a base of more": lambda rows: rows[1]["base"].update(more=1),
            "a patched run of more": lambda rows: rows[2]["patched"].update(more=1),
            "a replay of more": lambda rows: rows[2]["judged"]["replay"].update(more=1),
            "a twin of more": lambda rows: rows[2]["judged"]["twin"].update(more=1),
            "judged more": lambda rows: rows[2]["judged"].update(more=1),
            "a masked replay with no trace": lambda rows: rows[2]["judged"].update(
                twin=None, replay={**judged["replay"], "kinds": ["Other"], "trace_hash": "x"}),
            "a masked replay without a verdict": lambda rows: rows[2]["judged"].update(
                twin=None, replay={**judged["replay"], "deterministic": None}),
            "no trace": lambda rows: rows[1]["base"].update(trace=""),
            "a trace of zeros": lambda rows: rows[1]["base"].update(trace="0" * 16),
            "kinds out of order": lambda rows: rows[1]["base"].update(kinds=["b", "a"], deterministic=True),
            "a kind twice": lambda rows: rows[1]["base"].update(kinds=["a", "a"], deterministic=True),
            "kinds as text": lambda rows: rows[1]["base"].update(kinds="Other", deterministic=True),
            "an empty kind": lambda rows: rows[1]["base"].update(kinds=[""], deterministic=True),
            "a red seed that ran once": lambda rows: rows[1]["base"].update(kinds=["Other"], deterministic=None),
            "a verdict that does not fit": lambda rows: rows[1]["base"].update(deterministic=False),
            "a hit nobody judged": lambda rows: rows[2].pop("judged") and None,
            "a judged seed without a hit": set_row(1, judged=judged),
            "a base-red seed that is judged": lambda rows: rows[2]["base"].update(kinds=["Other"], deterministic=True),
            "a hit without its twin": lambda rows: rows[2]["judged"].update(twin=None),
            "a twin that is not the base run": lambda rows: rows[2]["judged"]["twin"].update(trace_hash="00000000000000aa"),
            "a red twin": lambda rows: rows[2]["judged"]["twin"].update(kinds=["Other"]),
            "a twin that is not deterministic": lambda rows: rows[2]["judged"]["twin"].update(deterministic=False),
            "a hit without a history": lambda rows: rows[2]["judged"]["replay"].update(history_sha256=None),
            "a twin after a mask": lambda rows: rows[2]["judged"]["replay"].update(kinds=["Other"]),
            "a hit without a replay": lambda rows: rows[2]["judged"].pop("replay") and None,
        }
        for what, change in cases.items():
            with self.subTest(what=what):
                shutil.rmtree(self.records)
                self.keep(1, self.edit_log(output, change))
                self.assert_no_shard()
        # The same rows in another spelling are another log.
        shutil.rmtree(self.records)
        lines = [line.replace('"seed":', '"seed": ') if line.startswith("sim-bugs-row: ") else line for line in output.splitlines()]
        rows = "".join(line.removeprefix("sim-bugs-row: ") + "\n" for line in lines if line.startswith("sim-bugs-row: "))
        self.keep(1, self.edit_done("\n".join(lines) + "\n", rows_sha256=sha256(rows)))
        self.assert_no_shard("not in the form a shard writes")
        # The edit itself is no reason for a refusal.
        shutil.rmtree(self.records)
        self.keep(1, self.edit_log(output, lambda rows: None))
        self.assert_merge("candidate")

    def test_shards_of_another_key(self):
        _, output, _ = self.shard(1, script={"hit": 3})
        done = done_of(output)
        cases = {
            "target": {"id": "fix-two"}, "schema": {"schema": "nabedra.sim-bug-shard/2"}, "mode": {"mode": "rehearsal"},
            "workload": {"plan": {**done["plan"], "workload": "smoke"}},
            "profile": {"plan": {**done["plan"], "profile": []}}, "expect": {"plan": {**done["plan"], "expect": ["Other"]}},
            "seeds": {"plan": {**done["plan"], "seeds": [1, 4]}}, "review": {"review": "a" * 64}, "patch": {"digest": "a" * 64}, "whole patch": {"patch_sha256": "a" * 64},
            "no shard": {"shard": [0, 2500]}, "no shards": {"shard": [1, 0]}, "a shard as text": {"shard": ["1", 2500]},
            "a shard as a flag": {"shard": [True, 2500]}, "half a shard": {"shard": [1]},
            "a shard above the last": {"shard": [2501, 2500], "seeds": [10001, 10004]},
            "build": {"build": "a" * 40}, "sha": {"sha": "a" * 40}, "range": {"seeds": [5, 8]}, "shard": {"shard": [2, 2500]},
            "shards": {"shard": [1, 5000]}, "run": {"run": f"https://github.com/{REPO}/actions/runs/8"},
            "attempt": {"attempt": "2"}, "defect": {"defect": "lost-write"}, "field": {"more": 1},
            "binary": {"binaries": {"clean": "a" * 64, "patched": "a" * 64}},
            "no binary": {"binaries": {"clean": "a" * 64}},
        }
        for what, changes in cases.items():
            with self.subTest(what=what):
                shutil.rmtree(self.records)
                self.keep(1, self.edit_done(output, **changes))
                self.assert_no_shard()
        shutil.rmtree(self.records)
        self.keep(1, self.edit_done(output))
        self.assert_merge("candidate")

    def test_a_commit_that_is_not_there_or_has_another_build(self):
        _, output, _ = self.shard(1, script={"hit": 3})
        self.keep(1, self.edit_done(output, sha="a" * 40), record={"sha": "a" * 40})
        self.assert_no_shard("its commit is not available")
        f = self.f
        f.write("server/src/other.rs", "pub fn other() {}\n")
        other = f.commit("another build")
        sh(f.repo, "reset", "-q", "--hard", "HEAD^")
        self.keep(1, self.edit_done(output, sha=other), record={"sha": other})
        self.assert_no_shard("its commit has another build than it says")

    def test_shards_of_two_commits(self):
        # The manifest changed in between, so the key is the same. One search is of one commit.
        self.run_shards(1, script={"hit": 6})
        self.f.edit('port_notes = "the one hunk"', 'port_notes = "the hunk"')
        self.f.commit("a note")
        self.run_shards(2, script={"hit": 6})
        self.assert_merge("incomplete", "different commits")

    def test_shards_of_two_splits(self):
        self.run_shards(1, script={"hit": 9})
        status, output, _ = self.shard(2, parts=5000, script={"hit": 9})
        self.assertEqual(status, 0, output)
        self.keep(2, output)
        self.assert_merge("incomplete", "split")

    def test_shards_of_two_builds(self):
        self.run_shards(1, script={"hit": 6})
        _, output, _ = self.shard(2, script={"hit": 6}, run=102)
        binaries = {"clean": "a" * 64, "patched": "b" * 64}
        self.keep(2, self.edit_done(output, binaries=binaries), run=102)
        self.assert_merge("incomplete", "different binaries")

    def test_a_product_that_changed_since(self):
        self.run_shards(1, script={"hit": 3})
        self.assert_merge("candidate")
        self.f.write("server/src/other.rs", "pub fn other() {}\n")
        self.f.commit("a product change")
        self.assert_merge("incomplete", "of another build")

    def test_a_plan_or_a_patch_that_changed_since(self):
        f = self.f
        self.run_shards(1, script={"hit": 3})
        self.assert_merge("candidate")
        f.edit('profile = ["lost-reply"]', 'profile = ["auth-flap"]')
        f.commit("another plan")
        self.assert_merge("incomplete", "of another plan")

    def test_a_rerun_has_to_agree(self):
        logs = self.run_shards(1, 2, script={"hit": 6})
        self.keep(2, logs[1].replace("runs/102", "runs/9"), run=9, name="again")
        body = self.assert_merge("candidate")
        self.assertEqual([s["shard"][0] for s in body["shards"]], [1, 2, 2])
        for what, change in (("kinds", lambda rows: rows[0]["patched"].update(kinds=["Other"], deterministic=True)),
                             ("base", lambda rows: rows[0]["base"].update(trace="00000000000000aa")),
                             ("a trace", lambda rows: rows[0]["patched"].update(trace="00000000000000aa")),
                             ("a mask", lambda rows: rows[2]["judged"]["replay"].update(history_sha256="a" * 64))):
            with self.subTest(what=what):
                self.keep(2, self.edit_log(logs[1].replace("runs/102", "runs/9"), change), run=9, name="again")
                # It is an error even where the hit is below the range that disagrees.
                self.assert_merge("error", "disagree")
                self.assert_not_found("error", "disagree")

    def test_records_that_are_no_evidence(self):
        _, output, _ = self.shard(1, script={"hit": 3})
        other = f"https://github.com/{REPO}/actions/runs/8"
        url = f"https://github.com/{REPO}/actions/runs/7"
        cases = {
            "a failed check": {"record": {"result": "fail"}},
            "an error": {"record": {"result": "error"}},
            "a dirty tree": {"record": {"tree_clean": False}},
            "no tree": {"record": {"tree_clean": None}},
            "another check": {"record": {"check": "oltp-sim-bugs-fix-one-2"}},
            "another commit": {"record": {"sha": "a" * 40}},
            "another run": {"record": {"run": {"url": other, "attempt": 1}}},
            "another attempt": {"record": {"run": {"url": url, "attempt": 2}}},
            "an attempt as text": {"record": {"run": {"url": url, "attempt": "1"}}},
            "no attempt": {"record": {"run": {"url": url, "attempt": None}}},
            "no run": {"record": {"run": {"url": None, "attempt": 1}}},
            "no log": {"record": {"artifacts": []}},
            "another log": {"record": {"artifacts": [{"name": "a.log", "sha256": "a" * 64}]}},
            "two logs": {"record": {"artifacts": [{"name": "a.log", "sha256": sha256(output)},
                                                  {"name": "b.log", "sha256": sha256(output)}]}},
            "no attestation": {"bundle": False},
            "another repository": {"certificate": {"sourceRepositoryURI": "https://github.com/o/r"}},
            "another workflow": {"certificate": {"buildSignerURI": WORKFLOW.replace("build.yml", "nightly.yml") + "@refs/heads/main"}},
            "another workflow of the same start": {"certificate": {"buildSignerURI": WORKFLOW + ".yml@refs/heads/main"}},
            "a workflow of another commit": {"certificate": {"sourceRepositoryDigest": "b" * 40}},
            "its own runner": {"certificate": {"runnerEnvironment": "self-hosted"}},
            "another event": {"certificate": {"buildTrigger": "pull_request"}},
            "a certificate of another run": {"certificate": {"runInvocationURI": f"{other}/attempts/1"}},
            "a certificate of another attempt": {"certificate": {"runInvocationURI": f"{url}/attempts/2"}},
        }
        for what, changes in cases.items():
            with self.subTest(what=what):
                shutil.rmtree(self.records)
                self.keep(1, output, **changes)
                self.assert_no_shard()
        shutil.rmtree(self.records)
        self.keep(1, output)
        self.assert_merge("candidate")

    def test_a_record_that_changed_after_its_attestation(self):
        _, output, _ = self.shard(1, script={"hit": 3})
        text = self.keep(1, output, record={"result": "fail"})
        path = self.records / "7-1-1.json"
        path.write_text(text.replace('"fail"', '"pass"'))
        bundle = self.records / f"sha256-{sha256(text)}.jsonl"
        self.assert_merge("incomplete", "no attestation")
        bundle.rename(self.records / f"sha256-{sha256(path.read_bytes())}.jsonl")
        self.assert_merge("incomplete", "no matching attestations")
        path.write_text(text.replace('"fail"', '"pass"') + '\n{"a": 1}')
        self.assert_merge("incomplete")

    def test_a_log_of_another_record(self):
        self.run_shards(1, 2, script={"hit": 6})
        # Shard 1's record names shard 2's log.
        log = (self.records / "102-1-2.log").read_text()
        for path in self.records.glob("101-*"):
            path.unlink()
        self.keep(1, log, run=102, name="other")
        self.assert_merge("incomplete", "shard 1 of")

    def test_gh_that_cannot_verify(self):
        self.run_shards(1, script={"hit": 3})
        for gh in ("false", "no-such-program-here"):
            out = StringIO()
            status = sim_bugs.main(["merge", "fix-one", "--records", str(self.records), "--gh", gh], repo=self.f.repo, out=out)
            self.assertEqual(status, 1, out.getvalue())
            self.assertIn("fix-one: incomplete", out.getvalue())
        for text in ("[]", "{}", "null", "x", '[{"verificationResult": null}]'):
            with self.subTest(text=text):
                (self.root / "odd_gh.py").write_text(f"print({text!r})")
                out = StringIO()
                status = sim_bugs.main(["merge", "fix-one", "--records", str(self.records), "--gh",
                                        f"{sys.executable} {self.root / 'odd_gh.py'}"], repo=self.f.repo, out=out)
                self.assertEqual(status, 1, out.getvalue())
                self.assertIn("fix-one: incomplete", out.getvalue())

    def test_a_known_finding_is_masked_without_another_shard(self):
        self.run_shards(1, script={"hit": 3})
        self.assertEqual(self.assert_merge("candidate")["hit"]["seed"], 3)
        self.mask_by_hand(3)
        body = self.assert_merge("candidate")
        self.assertEqual((body["hit"]["seed"], body["masked"]), (4, {"known-finding": [3]}))
        # The mask is for that hit only.
        self.f.edit(hit_identity(3)["history_sha256"], "0" * 63 + "1")
        self.f.commit("a mask of another hit")
        self.assertEqual(self.assert_merge("candidate")["hit"]["seed"], 3)

    def test_record_is_for_a_final_answer(self):
        self.run_shards(2, script={"hit": 6})
        path = self.f.repo / BUG / "searches.jsonl"
        before = path.read_text()
        status, output, _ = self.merge("--record")
        self.assertEqual((status, path.read_text()), (1, before), output)
        self.run_shards(1, script={"hit": 6})
        body = self.record_merge()
        entry = json.loads(path.read_text().splitlines()[-1])
        self.assertEqual((entry["n"], entry["merge"]), (2, body))
        self.assertEqual(self.f.run("check")[0], 0)
        # Twice the same is one entry too many.
        status, output, _ = self.merge("--record")
        self.assertEqual(status, 2, output)
        self.assertIn("already", output)

    def test_an_entry_that_is_not_one(self):
        self.run_shards(1, script={"hit": 3})
        self.record_merge()
        path = self.f.repo / BUG / "searches.jsonl"
        first, last = path.read_text().splitlines()
        entry = json.loads(last)
        for changes in ({"schema": "x"}, {"id": "fix-two"}, {"result": "found"}, {"plan": {}}, {"more": 1}):
            with self.subTest(changes=changes):
                path.write_text(first + "\n" + canonical({**entry, "merge": {**entry["merge"], **changes}}) + "\n")
                status, output = self.f.run("check")
                self.assertEqual(status, 1, output)
                self.assertIn("searches.jsonl line 2", output)

    def test_a_rehearsed_defect_is_in_the_patched_run_only(self):
        status, output, calls = self.shard(1, "--workload", "smoke", "--profile", "", "--expect", "non-linearizable",
                                           "--rehearse-defect", "lost-write", "--seeds", "4", parts=2)
        self.assertEqual(status, 0, output)
        self.assertEqual([(c["patched"], c["replay"], c["defect"]) for c in calls],
                         [(False, False, None), (True, False, "lost-write"), (True, True, "lost-write"), (False, True, None)])
        self.assertEqual(done_of(output)["defect"], "lost-write")

    def test_a_rehearsal_is_never_evidence(self):
        argv = ("--profile", "auth-flap", "--seeds", "8")
        for n in (1, 2):
            status, output, _ = self.shard(n, *argv, parts=2, script={"hit": 6}, run=100 + n)
            self.assertEqual(status, 0, output)
            self.assertIn("rehearsal", output)
            self.assertEqual((done_of(output)["mode"], done_of(output)["plan"]["seeds"]), ("rehearsal", [1, 8]))
            self.keep(n, output, run=100 + n)
            # As another check, as a rehearsal's records are.
            self.keep(n, output, run=100 + n, name=f"rehearsal-{n}", record={"check": f"oltp-sim-bugs-rehearsal-fix-one-{n}"})
        self.assert_merge("incomplete", "rehearsal")
        status, output, body = self.merge(*argv)
        self.assertEqual((status, body["result"], body["hit"]["seed"]), (0, "candidate", 6), output)
        self.assertIn("(rehearsal", output)
        self.assertEqual({shard["check"] for shard in body["shards"]},
                         {f"oltp-sim-bugs-rehearsal-fix-one-{n}" for n in (1, 2)})
        status, output, _ = self.merge(*argv, "--record")
        self.assertEqual(status, 2, output)
        status, output, row, _ = self.count(*argv, script={"hit": 6})
        self.assertEqual((status, row["result"]), (0, "rehearsed"), output)
        self.assertNotIn("found within", output)
        self.assertNotIn("fix-one: found", output)
        # And the shards of the plan are no rehearsal's.
        shutil.rmtree(self.records)
        self.run_shards(1, script={"hit": 3})
        self.assertEqual(self.merge(*argv)[2]["result"], "incomplete")

    def test_records_of_other_checks_are_left_alone(self):
        self.run_shards(1, script={"hit": 3})
        before = self.assert_merge("candidate")
        (self.records / "x.json").write_text("not json")
        (self.records / "y.json").write_text('["a"]')
        (self.records / "z.json").write_text('{"schema": "nabedra.evidence/1", "check": "oltp-sim-2"}')
        (self.records / "w.json").write_text('{"schema": "nabedra.evidence/1", "check": "oltp-sim-bugs-fix-one-x-1"}')
        self.assertEqual(self.assert_merge("candidate"), before)


class CountTest(ShardCase):
    """The counting run: the merge again, and everything that says the hit is real."""

    def ready(self, hit=3, *shards, script=None):
        """Shards, the recorded merge, and a detection that confirms its hit."""
        self.script = script or {"hit": hit}
        self.run_shards(*(shards or (1,)), script=self.script)
        body = self.record_merge()
        self.confirm_hit(hit, body)
        return body

    def test_found(self):
        self.ready()
        status, output, row, calls = self.count(script=self.script)
        self.assertEqual((status, row["result"], row["found_seed"]), (0, "found", 3), output)
        self.assertIn("fix-one: found seed 3 after 3 seeds in [1, 10000]", output)
        self.assertIn("base rate 0 of 3, patched rate 1 of 3", output)
        self.assertIn("1 of 1 reverted fixes found", output)
        # The hit again, its twin, and the base run of its neighbours. Not the search.
        self.assertEqual([(c["patched"], c["first"], c["count"], c["replay"]) for c in calls],
                         [(True, 3, 1, True), (False, 3, 1, True), (False, 1, 4, False)])
        self.assertIn(f"fix-one: with the patch failed; verified (digest {sim_bugs.digest(self.f.patch)})", output)
        self.assertEqual(row["rates"]["base"], {"expected": [], "other": [], "fault": []})

    def test_the_neighbours_are_the_hits_chunk(self):
        self.ready(7, 1, 2)
        _, output, row, calls = self.count("--chunk", "3", script=self.script)
        self.assertEqual(row["result"], "found", output)
        self.assertEqual([(c["first"], c["count"]) for c in calls if not c["replay"]], [(5, 3)])

    def test_every_confirmation_still_has_to_hold(self):
        body = self.ready()
        good = (self.f.repo / BUG / "bug.toml").read_text()
        other = json.loads((self.f.repo / BUG / "searches.jsonl").read_text().splitlines()[0])["search"]
        for changes, needle in (
                ({"seed": 4}, "another hit"), ({"lines": ["9 n101 seed 3 was applied"]}, "detection.lines"),
                ({"history_sha256": "a" * 64}, "another hit"), ({"search": "0" * 64}, "names no search"),
                ({"search": sha256(canonical(other))}, "detection.search is not the merge of these shards")):
            with self.subTest(changes=changes):
                self.confirm_hit(changes.pop("seed", 3), body, **changes)
                self.assert_not_found("candidate", needle, script=self.script)
        self.f.write(f"{BUG}/bug.toml", good.split("[detection]")[0])
        self.f.commit("not confirmed")
        self.assert_not_found("candidate", "no confirmed mechanism", script=self.script)

    def test_a_merge_that_nobody_recorded(self):
        self.run_shards(1, script={"hit": 3})
        # The fixture's own search and its detection are there, and they are of another run.
        self.assert_not_found("candidate", "no merge of these shards is recorded", script={"hit": 3})

    def test_an_edited_merge_entry(self):
        self.ready(6, 1, 2)
        path = self.f.repo / BUG / "searches.jsonl"
        first, last = path.read_text().splitlines()
        entry = json.loads(last)
        hit3 = {**entry["merge"]["hit"], **hit_identity(3), "twin_trace_hash": "%016x" % (3 * 7919)}
        for what, changes in {
                "another hit": {"hit": hit3, "tried": 3}, "an earlier hit that is masked": {"masked": {"known-finding": [2]}},
                "a lower base rate": {"rates": {**entry["merge"]["rates"], "base": {"expected": 0, "other": 1, "fault": 0}}},
                "no masks": {"masked": {"base-red": [1]}}, "another run": {"shards": entry["merge"]["shards"][:1]},
                "another commit": {"sha": "a" * 40}, "not detected": {"result": "not-detected", "hit": None}}.items():
            with self.subTest(what=what):
                # Not committed before, so nothing but the counting run sees the edit.
                sh(self.f.repo, "checkout", "-q", "-B", "edited", "HEAD~2")
                body = {**entry["merge"], **changes}
                path.write_text(first + "\n" + canonical({**entry, "merge": body}) + "\n")
                self.f.commit("an edited merge")
                self.confirm_hit(6, body)
                self.assert_not_found("error", "the recorded merge is not what the shards' records give", script=self.script)

    def test_an_entry_that_hides_an_earlier_hit(self):
        # The shards have a hit at 3. The entry was made from doctored logs that have it at 6 only.
        logs = self.run_shards(1, 2, script={"hit": 3})
        doctored = self.edit_log(logs[0], lambda rows: [{k: v for k, v in row.items() if k != "judged"} | {
            "patched": {**row["patched"], "kinds": []}} for row in rows])
        shutil.copytree(self.records, self.root / "true")
        self.keep(1, doctored, run=101)
        body = self.merge()[2]
        self.assertEqual(body["hit"]["seed"], 5)
        self.record_merge()
        self.confirm_hit(5, body)
        shutil.rmtree(self.records)
        shutil.copytree(self.root / "true", self.records, dirs_exist_ok=True)
        self.assert_not_found("error", "the recorded merge is not", script={"hit": 3})

    def test_a_shard_that_is_gone(self):
        self.ready(6, 1, 2)
        for what, gone in (("the lower one", "101-*"), ("the hit's", "102-*"), ("an attestation", None)):
            with self.subTest(what=what):
                kept = self.root / "kept"
                shutil.copytree(self.records, kept)
                paths = list(self.records.glob(gone)) if gone else list(self.records.glob("sha256-*"))[:1]
                for path in paths:
                    path.unlink()
                self.assert_not_found("incomplete", "shard", script=self.script)
                shutil.rmtree(self.records)
                kept.rename(self.records)

    def test_the_hit_has_to_repeat(self):
        self.ready()
        for script, needle in (
                ({"hit": 9}, "the hit of seed 3 is not"),
                ({"hit": 3, "patched": {"3": [KIND, "Other"]}}, "the hit of seed 3 is not"),
                ({"hit": 3, "flaky": ["patched"]}, "the hit of seed 3 is not"),
                ({"hit": 3, "clean": {"3": ["Other"]}}, "the hit of seed 3 is not"),
                ({"hit": 3, "clean_replay": {"3": ["Other"]}}, "the hit of seed 3 is not"),
                ({"hit": 3, "same_trace": True}, "the hit of seed 3 is not"),
                ({"hit": 3, "clean_base": {}, "clean": {"2": ["Other"]}}, "the base run of seeds 1 to 4"),
                ({"hit": 3, "clean_base_row": {"trace_hash": "00000000000000aa"}}, "the base run of seeds 1 to 4"),
                ({"hit": 3, "kill": "clean_base"}, "died with signal 9"),
                ({"hit": 3, "kill": "patched_replay"}, "died with signal 9")):
            with self.subTest(script=script):
                self.assert_not_found("error", needle, script=script)

    def test_its_regression_has_to_fail_now(self):
        self.ready()
        self.assert_not_found("candidate", "its regression did not pass without the patch", script=self.script,
                              FAKE_INSENSITIVE="1")

    def test_a_build_that_is_not_mains(self):
        self.f.write("server/src/other.rs", "pub fn other() {}\n")
        self.f.commit("a product change that is not main's")
        self.ready()
        self.assert_not_found("candidate", "the unpatched build is not main's", script=self.script)

    def test_other_binaries_than_the_shards(self):
        log, = self.run_shards(1, script={"hit": 3})
        self.keep(1, self.edit_done(log, binaries={"clean": "a" * 64, "patched": "b" * 64}), run=101)
        self.confirm_hit(3, self.record_merge())
        self.assert_not_found("error", "other binaries", script={"hit": 3})

    def test_records_on_the_evidence_branch(self):
        self.ready()
        repo = self.f.repo
        head = sh(repo, "rev-parse", "--abbrev-ref", "HEAD").strip()

        def count():
            status, output, _ = self.tool("count", "fix-one", script=self.script)
            return status, output
        status, output = count()
        self.assertEqual(status, 1, output)
        self.assertIn("evidence/sim-bugs", output)
        sh(repo, "checkout", "-q", "--orphan", "evidence")
        sh(repo, "rm", "-rfq", "--cached", ".")
        shutil.copytree(self.records, repo / "fix-one")
        shutil.copytree(self.records, repo / "fix-two")
        sh(repo, "add", "fix-one", "fix-two")
        sh(repo, "commit", "-q", "-m", "evidence")
        evidence = sh(repo, "rev-parse", "HEAD").strip()
        sh(repo, "checkout", "-q", "-f", head)
        sh(repo, "update-ref", "refs/remotes/origin/evidence/sim-bugs", evidence)
        shutil.rmtree(repo / "fix-two", ignore_errors=True)
        shutil.rmtree(repo / "fix-one", ignore_errors=True)
        status, output = count()
        self.assertEqual(status, 0, output)
        self.assertIn("fix-one: found", output)
        # Another target's directory is not this one's.
        sh(repo, "update-ref", "-d", "refs/remotes/origin/evidence/sim-bugs")
        self.assertEqual(count()[0], 1)

    def test_the_set_is_found_only_as_a_whole(self):
        self.ready()
        status, output, _ = self.tool("count", "--set", "direct", "--records", str(self.records), script=self.script)
        self.assertEqual(status, 0, output)
        self.assertIn("1 of 1 reverted fixes found", output)
        shutil.rmtree(self.records)
        status, output, _ = self.tool("count", "--set", "direct", "--records", str(self.records), script=self.script)
        self.assertEqual(status, 1, output)
        self.assertIn("0 of 1 reverted fixes found", output)
        self.assertNotIn(sim_bugs.NOT_RUN_MARKER, output)

    def test_evidence_compares_a_merge_with_its_records(self):
        self.ready()
        kept = self.root / "kept"

        def evidence():
            status, output, _ = self.tool("evidence", "fix-one", "--records", str(self.records))
            return output
        missing = "entry 2 of searches.jsonl"
        self.assertNotIn(missing, evidence())
        shutil.copytree(self.records, kept)
        for gone in ("*.json", "*.log"):
            for path in self.records.glob(gone):
                path.unlink()
            self.assertIn(missing, evidence())
            shutil.copytree(kept, self.records, dirs_exist_ok=True)
        path = self.f.repo / BUG / "searches.jsonl"
        text = path.read_text()
        for name in ("rows_sha256", "record_sha256", "log_sha256"):
            digest = json.loads(text.splitlines()[-1])["merge"]["shards"][0][name]
            path.write_text(text.replace(digest, "a" * 64))
            self.assertIn(missing, evidence())

    def test_an_earlier_plan_that_was_merged_is_accounted_for(self):
        f = self.f
        text = (f.repo / BUG / "bug.toml").read_text().split("[detection]")[0]
        plan = lambda name: (f.write(f"{BUG}/bug.toml", text.replace('"lost-reply"', f'"{name}"')), f.commit(name))
        plan("auth-flap")
        self.run_shards(1, script={"hit": 3})
        self.record_merge()
        plan("session-drop")
        unrecorded = lambda: sim_bugs.unrecorded_plans(sim_bugs.load_bugs(f.repo)[0][0], f.repo)
        self.assertEqual(unrecorded(), [])
        plan("kill-gateway")
        self.assertEqual(len(unrecorded()), 1)
        self.assertIn("session-drop", unrecorded()[0])


if __name__ == "__main__":
    unittest.main()

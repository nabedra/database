"""Tests for build_times.py. Cargo is replaced by a scripted runner; nothing here compiles."""

import contextlib
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import build_times


TIMING_HTML = """<html><script>
DURATION = 3;
const UNIT_DATA = [
  {"i": 1, "name": "nabedra-database", "version": "0.1.1", "mode": "todo", "target": " (lib)",
   "features": ["oltp"], "start": 0.5, "duration": 2.5, "unblocked_units": [], "unblocked_rmeta_units": [],
   "sections": [["frontend", {"start": 0.0, "end": 1.25}], ["codegen", {"start": 1.25, "end": 2.5}]]},
  {"i": 0, "name": "nabedra-rt", "version": "0.1.0", "mode": "todo", "target": "", "features": [],
   "start": 0.0, "duration": 0.4, "rmeta_time": 0.2, "unblocked_units": [], "unblocked_rmeta_units": [1]}
];
const CONCURRENCY_DATA = [];
</script></html>"""


def artifact(name, fresh=False, test=False, executable=None):
    return {"reason": "compiler-artifact", "package_id": f"path+file:///w/server#{name}@0.1.0",
            "fresh": fresh, "profile": {"test": test}, "executable": executable}


def tree_metadata(path: Path, packages: dict[str, str], targets=None, features=None):
    rows = []
    for name, directory in packages.items():
        rows.append({
            "id": f"path+file://{path}/{directory}#{name}@0.1.0",
            "name": name,
            "manifest_path": str(path / directory / "Cargo.toml"),
            "targets": targets.get(name, []) if targets else [],
            "features": features.get(name, {}) if features else {},
        })
    return {"packages": rows, "workspace_members": [row["id"] for row in rows]}


class ParsingTests(unittest.TestCase):
    def test_unit_data_reads_units_and_sections(self):
        units = build_times.unit_data(TIMING_HTML)
        self.assertEqual([u["name"] for u in units], ["nabedra-database", "nabedra-rt"])
        self.assertEqual(units[0]["target"], "(lib)")
        self.assertEqual((units[0]["duration"], units[0]["frontend"], units[0]["codegen"]), (2.5, 1.25, 1.25))
        self.assertEqual(units[1]["rmeta_time"], 0.2)

    def test_unit_data_fails_loudly(self):
        for html in ("<html></html>", "const UNIT_DATA = [];", "const UNIT_DATA = [{\"name\": 1",
                     "const UNIT_DATA = [{\"i\": 0}];"):
            with self.subTest(html=html), self.assertRaises(build_times.MeasurementError):
                build_times.unit_data(html)

    def test_package_names_in_every_id_format(self):
        cases = {
            "path+file:///w/server#nabedra-database@0.1.1": "nabedra-database",
            "registry+https://github.com/rust-lang/crates.io-index#serde@1.0.229": "serde",
            "path+file:///w/server/kv#0.1.0": "kv",
            "serde 1.0.229 (registry+https://github.com/rust-lang/crates.io-index)": "serde",
        }
        for package_id, name in cases.items():
            self.assertEqual(build_times.package_name(package_id), name)

    def test_artifacts_skip_other_messages_and_noise(self):
        stdout = "\n".join([
            json.dumps(artifact("a")), "Compiling a", "{not json",
            json.dumps({"reason": "build-finished", "success": True}), json.dumps(artifact("b", fresh=True)),
        ])
        self.assertEqual([build_times.package_name(a["package_id"]) for a in build_times.artifacts(stdout)], ["a", "b"])

    def test_duplicates_lists_top_level_entries_once(self):
        output = ("sha2 v0.10.9\n└── openraft v0.10.0\n    └── nabedra-database v0.1.1 (/w/server)\n\n"
                  "sha2 v0.11.1 (*)\n├── nabedra-database v0.1.1 (/w/server)\n\n"
                  "chrono v0.4.45\n└── x v1.0.0\n\nchrono v0.4.45\n└── y v1.0.0\n")
        self.assertEqual(build_times.duplicates(output), ["sha2 v0.10.9", "sha2 v0.11.1", "chrono v0.4.45"])

    def test_test_targets_follow_feature_implications(self):
        path = Path("/w")
        targets = {"nabedra-database": [
            {"kind": ["lib"], "name": "lib"},
            {"kind": ["test"], "name": "plain"},
            {"kind": ["test"], "name": "olap", "required-features": ["olap"]},
            {"kind": ["test"], "name": "hooks", "required-features": ["cache", "test-hooks"]},
        ]}
        features = {"nabedra-database": {"default": ["olap", "s3"], "test-hooks": ["cache"],
                                         "olap": ["dep:arrow-array"], "cache": []}}
        metadata = tree_metadata(path, {"nabedra-database": "server"}, targets, features)
        count = build_times.test_targets
        self.assertEqual(count(metadata, "nabedra-database", {"olap", "s3"}), 2)
        self.assertEqual(count(metadata, "nabedra-database", {"oltp"}), 1)
        self.assertEqual(count(metadata, "nabedra-database", {"test-hooks"}), 2)
        with self.assertRaises(build_times.MeasurementError):
            count(metadata, "missing", set())

    def test_summary_uses_medians_per_side_and_a_ratio(self):
        samples = [
            {"step": "cold", "label": "oltp", "side": "base", "seconds": s} for s in (10, 30, 20)
        ] + [{"step": "cold", "label": "oltp", "side": "head", "seconds": s} for s in (5, 6, 4)] + [
            {"step": "edit", "label": "storage-check", "side": "base", "seconds": 1.0}]
        rows = {(r["step"], r["label"]): r for r in build_times.summarize(samples)}
        self.assertEqual((rows["cold", "oltp"]["base_s"], rows["cold", "oltp"]["head_s"]), (20, 5))
        self.assertEqual(rows["cold", "oltp"]["ratio"], 0.25)
        self.assertIsNone(rows["edit", "storage-check"]["head_s"])
        self.assertIsNone(rows["edit", "storage-check"]["ratio"])

    def test_rounds_alternate_with_base_first_on_odd_rounds(self):
        self.assertEqual([build_times.round_order(i) for i in range(3)],
                         [("base", "head"), ("head", "base"), ("base", "head")])


class RuleTests(unittest.TestCase):
    PRE_SPLIT = {"nabedra-database", "nabedra-kv", "nabedra-rt", "nabedra-check"}
    SPLIT = PRE_SPLIT | set(build_times.OLTP_PACKAGES)

    def test_rules_are_not_applicable_before_the_split(self):
        rebuilds = {"runtime": {"owner": "nabedra-database", "rebuilt": ["nabedra-database"]}}
        rules = build_times.evaluate_rules(rebuilds, self.PRE_SPLIT)
        self.assertEqual([r["result"] for r in rules], ["n/a", "n/a", "n/a"])
        self.assertIn("not on the head yet", rules[0]["reason"])

    def test_skeleton_rebuild_rules_wait_for_the_source_moves(self):
        rebuilds = {
            "runtime": {"owner": "nabedra-database", "rebuilt": ["nabedra-database"]},
            "storage": {"owner": "nabedra-database", "rebuilt": ["nabedra-database"]},
        }
        pending = frozenset(("nabedra-transport", "nabedra-oltp-storage"))
        rules = build_times.evaluate_rules(rebuilds, self.SPLIT, pending)
        self.assertEqual([rule["result"] for rule in rules], ["pass", "n/a", "n/a"])
        self.assertEqual(rules[2]["reason"], "probe sources have yet to move into nabedra-oltp-storage")

    def test_rules_pass_and_fail_on_the_split_head(self):
        good = {
            "runtime": {"owner": "nabedra-database", "rebuilt": ["nabedra-database"]},
            "transport": {"owner": "nabedra-transport",
                          "rebuilt": ["nabedra-database", "nabedra-oltp-replication", "nabedra-transport"]},
            "storage": {"owner": "nabedra-oltp-storage",
                        "rebuilt": ["nabedra-database", "nabedra-oltp-replication", "nabedra-oltp-storage"]},
        }
        self.assertEqual([r["result"] for r in build_times.evaluate_rules(good, self.SPLIT)], ["pass"] * 3)

        bad = dict(good, transport={"owner": "nabedra-transport",
                                    "rebuilt": ["nabedra-oltp-storage", "nabedra-transport"]})
        rule = build_times.evaluate_rules(bad, self.SPLIT)[1]
        self.assertEqual((rule["result"], rule["reason"]), ("fail", "rebuilt nabedra-oltp-storage"))

        missing = {k: v for k, v in good.items() if k != "storage"}
        self.assertEqual(build_times.evaluate_rules(missing, self.SPLIT)[2]["reason"], "the probe did not run")
        moved = dict(good, storage={"owner": "nabedra-database", "rebuilt": ["nabedra-database"]})
        self.assertIn("belongs to nabedra-database", build_times.evaluate_rules(moved, self.SPLIT)[2]["reason"])
        runtime = dict(good, runtime={"owner": "nabedra-database",
                                      "rebuilt": ["nabedra-database", "nabedra-resources"]})
        self.assertEqual(build_times.evaluate_rules(runtime, self.SPLIT)[0]["result"], "fail")


class TreeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="build-times-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def tree(self, side, files, packages):
        path = self.tmp / side
        for rel in files:
            (path / rel).parent.mkdir(parents=True, exist_ok=True)
            (path / rel).write_text("fn main() {}\n", encoding="utf-8")
        return build_times.Tree(side, path, side, tree_metadata(path, packages))

    PRE = ["server/src/oltp/pool.rs", "server/src/oltp_replication/typed_log.rs", "server/src/oltp_runtime/http.rs",
           "server/src/oltp_replication/multiplex/transport/session.rs", "server/src/oltp/pool/resources.rs"]

    def test_owner_is_the_longest_package_prefix(self):
        tree = self.tree("t", [], {"nabedra-database": "server", "nabedra-kv": "server/kv",
                                   "nabedra-transport": "server/transport"})
        self.assertEqual(tree.owner("server/src/oltp/pool.rs"), "nabedra-database")
        self.assertEqual(tree.owner("server/kv/src/lib.rs"), "nabedra-kv")
        self.assertEqual(tree.owner("server/transport/src/session.rs"), "nabedra-transport")
        self.assertEqual(tree.owner("server/kvx/src/lib.rs"), "nabedra-database")
        with self.assertRaises(build_times.MeasurementError):
            tree.owner("orm/python/x.py")

    def test_probes_before_the_split_use_the_three_core_files(self):
        packages = {"nabedra-database": "server"}
        trees = {side: self.tree(side, self.PRE, packages) for side in ("base", "head")}
        probes = build_times.resolve_probes(trees, None)
        self.assertEqual([p.name for p, _ in probes], ["storage", "replication", "runtime"])
        self.assertEqual(probes[0][1], {"base": "server/src/oltp/pool.rs", "head": "server/src/oltp/pool.rs"})

    def test_a_split_head_adds_its_crates_and_the_base_uses_the_old_paths(self):
        base = self.tree("base", self.PRE, {"nabedra-database": "server"})
        head = self.tree("head", ["server/oltp-storage/src/oltp/pool.rs", "server/src/oltp_replication/typed_log.rs",
                                  "server/src/oltp_runtime/http.rs", "server/transport/src/transport/session.rs",
                                  "server/src/oltp/pool/resources.rs"],
                         {"nabedra-database": "server", "nabedra-transport": "server/transport",
                          "nabedra-oltp-storage": "server/oltp-storage"})
        probes = dict((p.name, paths) for p, paths in build_times.resolve_probes({"base": base, "head": head}, None))
        self.assertEqual(list(probes), ["storage", "replication", "runtime", "transport"])
        self.assertEqual(probes["transport"], {
            "base": "server/src/oltp_replication/multiplex/transport/session.rs",
            "head": "server/transport/src/transport/session.rs"})
        self.assertEqual(head.owner(probes["storage"]["head"]), "nabedra-oltp-storage")
        only = build_times.resolve_probes({"base": base, "head": head}, ["runtime"])
        self.assertEqual([p.name for p, _ in only], ["runtime"])

    def test_the_alternate_transport_path_enables_the_probe(self):
        fallback = "server/src/oltp_replication/multiplex/transport/session.rs"
        extracted = "server/transport/src/session.rs"
        base = self.tree("base", [fallback], {"nabedra-database": "server"})
        head = self.tree("head", [extracted],
                         {"nabedra-database": "server", "nabedra-transport": "server/transport"})
        [(probe, paths)] = build_times.resolve_probes({"base": base, "head": head}, ["transport"])
        self.assertEqual(probe.name, "transport")
        self.assertEqual(paths, {"base": fallback, "head": extracted})

    def test_a_probe_without_any_file_on_one_side_is_an_error(self):
        base = self.tree("base", self.PRE[1:], {"nabedra-database": "server"})
        head = self.tree("head", self.PRE, {"nabedra-database": "server"})
        with self.assertRaisesRegex(build_times.MeasurementError, "probe storage: no candidate file on the base"):
            build_times.resolve_probes({"base": base, "head": head}, None)


class FakeRunner:
    """Records Cargo calls and answers them from a script: (verb, package) -> dirty packages."""

    def __init__(self, target: Path, dirty):
        self.target = target
        self.dirty = dirty
        self.calls = []
        self.jobs = None
        self.seconds = 1.0

    def build(self, verb, args, cwd, env):
        self.calls.append(("build", verb, tuple(args), cwd.parent.name if cwd.name == "events" else cwd.parent.name,
                           env.get("CARGO_INCREMENTAL")))
        if "--timings" in args:
            (self.target / "cargo-timings").mkdir(parents=True, exist_ok=True)
            (self.target / "cargo-timings" / "cargo-timing.html").write_text(TIMING_HTML, encoding="utf-8")
        packages = self.dirty(verb, list(args), cwd)
        self.seconds += 1
        result = build_times.Result(self.seconds, "", (0.5, 0.6))
        return result, [artifact(name) for name in packages] + [artifact("serde", fresh=True)]

    def clean(self, packages, cwd):
        self.calls.append(("clean", tuple(packages), cwd.parent.name))

    def capture(self, args, cwd):
        return "sha2 v0.10.9\n└── x v1\n\nsha2 v0.11.1\n└── y v1\n"


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="build-times-session-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.trees = {}
        for side in ("base", "head"):
            path = self.tmp / side
            (path / "server/src/oltp_runtime").mkdir(parents=True)
            (path / "server/src/oltp_runtime/http.rs").write_text("fn http() {}\n", encoding="utf-8")
            targets = {"nabedra-database": [{"kind": ["test"], "name": "t", "required-features": ["olap"]}]}
            self.trees[side] = build_times.Tree(side, path, side, tree_metadata(
                path, {"nabedra-database": "server", "nabedra-rt": "server/rt"}, targets))
        self.log = io.StringIO()

    def session(self, dirty, rounds=3):
        runner = FakeRunner(self.tmp / "target", dirty)
        return build_times.Session(dict.fromkeys(self.trees, runner), self.trees, rounds,
                                   log=lambda *a, **k: print(*a, file=self.log)), runner

    def test_cold_cleans_the_workspace_before_every_measured_build_and_alternates(self):
        session, runner = self.session(lambda verb, args, cwd: ["nabedra-database", "nabedra-rt"])
        session.cold([build_times.FEATURE_SETS[0]])
        measured = [c for c in runner.calls if c[0] == "build" and "--timings" in c[2]]
        self.assertEqual([c[3] for c in measured], ["base", "head", "head", "base", "base", "head"])
        for index, call in enumerate(runner.calls):
            if call in measured:
                self.assertEqual(runner.calls[index - 1][:2], ("clean", ("nabedra-database", "nabedra-rt")))
                self.assertEqual(call[4], "0")
        samples = session.samples
        self.assertEqual(len(samples), 6)
        self.assertEqual(samples[0]["rebuilt"], ["nabedra-database", "nabedra-rt"])
        self.assertEqual([u["name"] for u in samples[0]["workspace_units"]], ["nabedra-database", "nabedra-rt"])

    def test_cold_refuses_a_build_where_nothing_was_rebuilt(self):
        session, _ = self.session(lambda verb, args, cwd: [])
        with self.assertRaisesRegex(build_times.MeasurementError, "no workspace crate rebuilt"):
            session.cold([build_times.FEATURE_SETS[0]])

    def test_edit_measures_after_a_warm_up_and_restores_the_file(self):
        session, runner = self.session(lambda verb, args, cwd: ["nabedra-database"])
        probe = build_times.PROBES[2]
        paths = {"base": "server/src/oltp_runtime/http.rs", "head": "server/src/oltp_runtime/http.rs"}
        session.edit([(probe, paths)])
        self.assertEqual(sorted({s["label"] for s in session.samples}), ["runtime-check", "runtime-test-no-run"])
        self.assertEqual(len(session.samples), 2 * 2 * 3)          # two kinds, two sides, three rounds
        builds = [c for c in runner.calls if c[0] == "build"]
        self.assertEqual(len(builds), 2 * 2 * 4)                   # plus one warm-up round
        self.assertTrue(all(c[4] == "1" for c in builds))
        self.assertEqual(runner.calls[0][0], "clean")
        for side in ("base", "head"):
            text = (self.trees[side].path / paths[side]).read_text(encoding="utf-8")
            self.assertEqual(text, "fn http() {}\n")
            self.assertEqual(session.rebuilds[side]["runtime"],
                             {"path": paths[side], "owner": "nabedra-database", "rebuilt": ["nabedra-database"]})

    def test_only_the_first_of_consecutive_server_probes_warms_up(self):
        for tree in self.trees.values():
            (tree.path / "server/src/oltp").mkdir(parents=True)
            (tree.path / "server/src/oltp/pool.rs").write_text("fn pool() {}\n", encoding="utf-8")
        session, runner = self.session(lambda verb, args, cwd: ["nabedra-database"], rounds=1)
        same = {"base": "server/src/oltp_runtime/http.rs", "head": "server/src/oltp_runtime/http.rs"}
        pool = {"base": "server/src/oltp/pool.rs", "head": "server/src/oltp/pool.rs"}
        session.edit([(build_times.PROBES[2], same), (build_times.PROBES[0], pool)])
        builds = [c for c in runner.calls if c[0] == "build"]
        self.assertEqual(len(builds), 2 * 2 * (2 + 1))            # one warm-up round, then one round each
        self.assertEqual(len(session.samples), 2 * 2 * 2)

    def test_edit_checks_the_oltp_graph_for_a_lower_crate_and_requires_the_owner_rebuilt(self):
        for side in self.trees.values():
            (side.path / "server/transport/src").mkdir(parents=True)
            (side.path / "server/transport/src/session.rs").write_text("fn s() {}\n", encoding="utf-8")
            side.metadata = tree_metadata(side.path, {"nabedra-database": "server",
                                                      "nabedra-transport": "server/transport"})
        probe = build_times.Probe("transport", ("server/transport/src/session.rs",))
        paths = {"base": probe.candidates[0], "head": probe.candidates[0]}

        def dirty(verb, args, cwd):
            return ["nabedra-transport"] + (["nabedra-database"] if "nabedra-database" in args else [])

        session, runner = self.session(dirty, rounds=1)
        session.edit([(probe, paths)])
        self.assertEqual(sorted({s["label"] for s in session.samples}),
                         ["transport-check", "transport-check-oltp-graph", "transport-test-no-run"])
        self.assertEqual(session.rebuilds["head"]["transport"]["rebuilt"], ["nabedra-database", "nabedra-transport"])

        session, _ = self.session(lambda verb, args, cwd: ["nabedra-database"], rounds=1)
        with self.assertRaisesRegex(build_times.MeasurementError, "nabedra-transport was not rebuilt"):
            session.edit([(probe, paths)])
        text = (self.trees["base"].path / probe.candidates[0]).read_text(encoding="utf-8")
        self.assertEqual(text, "fn s() {}\n")

    def test_a_rebuild_set_that_changes_between_rounds_is_an_error(self):
        sets = iter([["nabedra-database"]] * 4 + [["nabedra-database", "nabedra-rt"]] * 20)
        session, _ = self.session(lambda verb, args, cwd: next(sets) if verb == "check" else ["nabedra-database"])
        probe = build_times.PROBES[2]
        paths = {"base": "server/src/oltp_runtime/http.rs", "head": "server/src/oltp_runtime/http.rs"}
        with self.assertRaisesRegex(build_times.MeasurementError, "rebuild set changed"):
            session.edit([(probe, paths)])

    def test_extras_record_duplicates_and_test_targets(self):
        session, _ = self.session(lambda verb, args, cwd: [])
        session.extra()
        self.assertEqual(session.extras["head"]["duplicates"]["oltp"], ["sha2 v0.10.9", "sha2 v0.11.1"])
        self.assertEqual(session.extras["head"]["test_targets"], {"oltp": 0, "olap,s3": 1, "cache": 0})
        self.assertIn("nabedra-build-extra: head olap-test-binaries=1", self.log.getvalue())


class MainTests(unittest.TestCase):
    def run_main(self, session_steps, argv=()):
        tmp = Path(tempfile.mkdtemp(prefix="build-times-main-"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)

        @contextlib.contextmanager
        def checkout(path, sha):
            (path / "server").mkdir(parents=True)
            yield path

        def capture(runner, args, cwd):
            self.assertEqual(args[0], "metadata")
            return json.dumps(tree_metadata(cwd.parent, {"nabedra-database": "server"}))

        record = tmp / "out" / "build-times.json"
        output = io.StringIO()
        with (
            patch.object(build_times, "revision", side_effect=lambda ref: ref * 8),
            patch.object(build_times, "checkout", checkout),
            patch.object(build_times.Runner, "capture", capture),
            patch.object(build_times, "context", return_value={"cpu": "test"}),
            patch.object(build_times.Session, "extra", session_steps),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(output),
        ):
            status = build_times.main(["--base", "a", "--head", "b", "--steps", "extras",
                                       "--work-dir", str(tmp / "work"), "--record", str(record), *argv])
        return status, json.loads(record.read_text(encoding="utf-8")), output.getvalue()

    def test_revisions_use_distinct_cargo_target_directories(self):
        def extra(session):
            targets = {side: runner.env({})["CARGO_TARGET_DIR"] for side, runner in session.runners.items()}
            self.assertEqual(set(targets), {"base", "head"})
            self.assertNotEqual(targets["base"], targets["head"])

        status, _, output = self.run_main(extra)
        self.assertEqual(status, 0, output)

    def test_complete_run_writes_the_record(self):
        def extra(session):
            session.samples.append({"step": "cold", "label": "oltp", "side": "base", "seconds": 4.0})
            session.samples.append({"step": "cold", "label": "oltp", "side": "head", "seconds": 2.0})

        status, record, output = self.run_main(extra)
        self.assertEqual(status, 0)
        self.assertEqual((record["base"], record["head"], record["result"]), ("a" * 8, "b" * 8, "complete"))
        self.assertEqual(record["packages"], {"base": ["nabedra-database"], "head": ["nabedra-database"]})
        self.assertIn("nabedra-build-time: cold/oltp 4.0 2.0 ratio=0.5", output)

    def test_a_failed_measurement_exits_2_and_still_writes_the_record(self):
        def extra(session):
            raise build_times.MeasurementError("cargo-timing.html has no `const UNIT_DATA = ` array")

        status, record, output = self.run_main(extra)
        self.assertEqual(status, 2)
        self.assertEqual(record["result"], "incomplete")
        self.assertIn("UNIT_DATA", record["error"])
        self.assertIn("measurement incomplete", output)

    def test_arguments_are_checked(self):
        for argv in (["--steps", "cold,lint"], ["--rounds", "0"]):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                build_times.main(["--base", "a", "--head", "b", *argv])


if __name__ == "__main__":
    unittest.main()

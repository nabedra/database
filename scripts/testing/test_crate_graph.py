"""Tests for crate_graph.py: the six `crate-graph` cases of the phase 8 design, 5.1, and the tool.

Cases that need a crate the split has not created yet skip with the reason; they never pass
without running their assertions.
"""

import contextlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest

import crate_graph


ROOT = crate_graph.ROOT
CONFIG = crate_graph.load_config()
HAVE_CARGO = crate_graph.cargo() is not None
HAVE_GIT = shutil.which("git") is not None and (ROOT / ".git").exists()


def metadata(packages):
    """Minimal `cargo metadata` output: {name: [(dep, kind), ...]}."""
    rows = []
    for name, deps in packages.items():
        rows.append({
            "id": f"path+file:///w/{name}#{name}@0.1.0",
            "name": name,
            "dependencies": [
                {"name": dep, "kind": kind, "path": f"/w/{dep}"} for dep, kind in deps
            ] + [{"name": "serde", "kind": None}],
        })
    return {"packages": rows, "workspace_members": [row["id"] for row in rows]}


def write_tree(base: Path, files: dict[str, str]) -> None:
    for rel, text in files.items():
        path = base / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(text), encoding="utf-8")


def stub_config(**changes):
    """The real rules with a temporary ratchet, for trees built in a temporary directory."""
    config = crate_graph.load_config()
    for key, value in changes.items():
        setattr(config, key, value)
    return config


class LayerTests(unittest.TestCase):
    @unittest.skipUnless(HAVE_CARGO, "cargo is not on PATH")
    def test_layers(self):
        """Case 1: every dependency of every kind of every workspace package points down."""
        data = crate_graph.workspace_metadata(ROOT / "server")
        names = {p["name"] for p in data["packages"]}
        self.assertIn("nabedra-database", names)
        self.assertEqual(crate_graph.layer_violations(data, CONFIG.ranks), [])

    def test_layer_rule_rejects_each_kind_unlisted_packages_and_siblings(self):
        ranks = CONFIG.ranks
        ok = metadata({
            "nabedra-database": [("nabedra-kv", None), ("nabedra-check", "dev")],
            "nabedra-kv": [("nabedra-rt", None)],
            "nabedra-rt": [],
            "nabedra-check": [],
        })
        self.assertEqual(crate_graph.layer_violations(ok, ranks), [])
        for kind in (None, "build", "dev"):
            with self.subTest(kind=kind):
                bad = metadata({
                    "nabedra-oltp-storage": [("nabedra-oltp-replication", kind)],
                    "nabedra-oltp-replication": [("nabedra-oltp-storage", None)],
                })
                problems = crate_graph.layer_violations(bad, ranks)
                self.assertEqual(len(problems), 1, problems)
                self.assertIn(f"nabedra-oltp-storage -> nabedra-oltp-replication ({kind or 'normal'})",
                              problems[0])
                self.assertIn("a higher layer", problems[0])
        siblings = metadata({"nabedra-oltp-storage": [("nabedra-transport", None)], "nabedra-transport": []})
        self.assertIn("the same layer", crate_graph.layer_violations(siblings, ranks)[0])
        unlisted = metadata({"nabedra-new": [], "nabedra-database": [("nabedra-new", None)]})
        problems = crate_graph.layer_violations(unlisted, ranks)
        self.assertTrue(any("nabedra-new: workspace package has no layer" in p for p in problems), problems)


@unittest.skipUnless(HAVE_CARGO, "cargo is not on PATH")
class PlantedCycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="crate-graph-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.plant = (CONFIG.plant["package"], CONFIG.plant["dependency"])

    def stub(self, name, extra=None):
        directory = self.tmp / name
        crate_graph.write_stub_workspace(directory, CONFIG.planned, extra)
        return directory

    def test_planned_graph_is_accepted(self):
        directory = self.stub("planned")
        result = crate_graph.cargo_metadata(directory, resolve=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(result.stdout)
        self.assertEqual({p["name"] for p in data["packages"]}, set(CONFIG.planned))
        self.assertEqual(crate_graph.layer_violations(data, CONFIG.ranks), [])

    def test_planted_cycle_manifest(self):
        """Case 3: an upward [dependencies] edge makes `cargo metadata` fail."""
        directory = self.stub("cycle", {self.plant: "normal"})
        result = crate_graph.cargo_metadata(directory, resolve=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cyclic package dependency", result.stderr)

    def test_planted_dev_cycle(self):
        """Case 4: Cargo accepts an upward [dev-dependencies] edge, and the layer rule rejects it."""
        for kind in ("dev", "build"):
            with self.subTest(kind=kind):
                directory = self.stub(f"{kind}-cycle", {self.plant: kind})
                result = crate_graph.cargo_metadata(directory, resolve=kind == "dev")
                self.assertEqual(result.returncode, 0, result.stderr)
                problems = crate_graph.layer_violations(json.loads(result.stdout), CONFIG.ranks)
                self.assertEqual(problems, [
                    f"{self.plant[0]} -> {self.plant[1]} ({kind}): points to a higher layer "
                    f"(layer {CONFIG.ranks[self.plant[1]]} from layer {CONFIG.ranks[self.plant[0]]})"
                ])


class RatchetTests(unittest.TestCase):
    def test_module_ratchet(self):
        """Case 2: upward module edges are at most the record, and pairs recorded at 0 stay 0."""
        graph = crate_graph.Graph(CONFIG)
        self.assertGreater(len(graph.files), 100, "the server crate's sources were not found")
        counts = graph.ratchet_counts()
        failures, _notes = crate_graph.ratchet_problems(CONFIG, counts)
        self.assertEqual(failures, [])
        for method in crate_graph.METHODS:
            self.assertEqual(set(CONFIG.ratchet[method]), set(graph.upward_pairs()), method)
        self.assertTrue(any(v == 0 for v in CONFIG.ratchet["literal"].values()))

    @unittest.skipUnless(HAVE_GIT, "git is not available")
    def test_literal_counts_match_the_design_method(self):
        """The literal method is `git grep -hoE "crate::<dst>\\b"` per source module (design 2.1)."""
        graph = crate_graph.Graph(CONFIG)
        modules = ("oltp", "oltp_replication", "oltp_runtime")
        for src in modules:
            for dst in modules:
                if src == dst:
                    continue
                result = subprocess.run(
                    ["git", "grep", "-hoE", rf"crate::{dst}\b", "--", f"server/src/{src}", f"server/src/{src}.rs"],
                    cwd=ROOT, capture_output=True, text=True,
                )
                expected = sum(1 for line in result.stdout.splitlines() if line == f"crate::{dst}")
                with self.subTest(edge=f"{src}:{dst}"):
                    self.assertEqual(sum(graph.edge_files(src, dst).values()), expected)

    def tree(self, files):
        base = Path(tempfile.mkdtemp(prefix="crate-graph-tree-"))
        self.addCleanup(shutil.rmtree, base, ignore_errors=True)
        write_tree(base, {f"server/src/{rel}": text for rel, text in files.items()})
        return base

    BASE = {
        "lib.rs": "pub mod oltp; pub mod oltp_replication; pub mod oltp_runtime; pub mod config;\n",
        "oltp/mod.rs": """
            // crate::oltp_runtime in a comment counts for the literal method only
            use crate::oltp_replication::types::Digest;
            use crate::{oltp_replication::owned::{A, B as C}, protocol::Error};
            use crate::oltp::pool::Local;
            pub(crate) fn f() { crate::config::limit(); }
        """,
        "oltp/pool.rs": "pub struct Local;\n",
        "oltp_replication/mod.rs": "use crate::oltp::pool::Local;\nuse crate::oltp_runtime::stage_timing::Stage;\n",
        "oltp_replication/types.rs": "pub struct Digest;\n",
        "oltp_runtime/mod.rs": "macro_rules! m { () => { $crate::oltp::pool::Local } }\n",
    }

    def test_counts_each_method_and_buckets_unmapped_modules(self):
        graph = crate_graph.Graph(CONFIG, self.tree(self.BASE))
        counts = graph.ratchet_counts()
        literal, expanded = counts["literal"], counts["expanded"]
        self.assertEqual(literal["oltp:oltp_replication"], 1)
        self.assertEqual(expanded["oltp:oltp_replication"], 3)       # Digest, A, B
        self.assertEqual(literal["oltp:oltp_runtime"], 1)            # the comment
        self.assertEqual(expanded["oltp:oltp_runtime"], 0)
        self.assertEqual(literal["oltp:*"], 1)                       # config
        self.assertEqual(expanded["oltp:*"], 1)
        self.assertEqual(literal["oltp_replication:oltp_runtime"], 1)
        self.assertNotIn("oltp_runtime:oltp", literal)               # downward, not ratcheted
        self.assertEqual(sum(graph.edge_files("oltp_runtime", "oltp").values()), 1)   # $crate counts
        self.assertEqual(sum(graph.edge_files("oltp", "oltp").values()), 0)           # self references

    def test_rising_or_new_back_edges_fail_and_falling_ones_ask_for_a_lower_record(self):
        base = self.tree(self.BASE)
        record = crate_graph.Graph(CONFIG, base).ratchet_counts()
        config = stub_config(ratchet={m: dict(v) for m, v in record.items()})
        self.assertEqual(crate_graph.ratchet_problems(config, record), ([], []))

        # One more upward reference fails, by either method.
        rising = dict(self.BASE)
        rising["oltp/pool.rs"] += "fn g() { crate::oltp_replication::types::Digest; }\n"
        failures, _ = crate_graph.ratchet_problems(config, crate_graph.Graph(config, self.tree(rising)).ratchet_counts())
        self.assertIn("oltp:oltp_replication (literal): 2 references is above the recorded 1", failures)
        self.assertIn("oltp:oltp_replication (expanded): 4 references is above the recorded 3", failures)

        # A grouped import hides from the literal method, so the expanded record catches it.
        grouped = dict(self.BASE)
        grouped["oltp_replication/types.rs"] += "use crate::{oltp_deployment::Inventory};\n"
        failures, _ = crate_graph.ratchet_problems(config, crate_graph.Graph(config, self.tree(grouped)).ratchet_counts())
        self.assertEqual(failures, [])  # deployment shares replication's layer
        grouped["oltp/pool.rs"] += "use crate::{oltp_deployment::Inventory};\n"
        failures, _ = crate_graph.ratchet_problems(config, crate_graph.Graph(config, self.tree(grouped)).ratchet_counts())
        self.assertEqual(failures, ["oltp:oltp_deployment (expanded): 1 references was 0 and must stay 0"])

        # A pair recorded as 0 stays 0.
        zero = dict(self.BASE)
        zero["protocol.rs"] = "use crate::oltp::pool::Local;\n"
        failures, _ = crate_graph.ratchet_problems(config, crate_graph.Graph(config, self.tree(zero)).ratchet_counts())
        self.assertIn("protocol:oltp (literal): 1 references was 0 and must stay 0", failures)

        # Fewer references pass, with a note to lower the record.
        falling = dict(self.BASE)
        falling["oltp_replication/mod.rs"] = "use crate::oltp::pool::Local;\n"
        failures, notes = crate_graph.ratchet_problems(
            config, crate_graph.Graph(config, self.tree(falling)).ratchet_counts())
        self.assertEqual(failures, [])
        self.assertIn("oltp_replication:oltp_runtime (literal): 0 references, below the recorded 1; "
                      "lower the record", notes)

    def test_missing_and_stale_records_fail(self):
        base = self.tree(self.BASE)
        counts = crate_graph.Graph(CONFIG, base).ratchet_counts()
        partial = {m: dict(v) for m, v in counts.items()}
        del partial["literal"]["oltp:oltp_runtime"]
        partial["expanded"]["gone:oltp"] = 3
        failures, _ = crate_graph.ratchet_problems(stub_config(ratchet=partial), counts)
        self.assertTrue(any(f.startswith("oltp:oltp_runtime (literal): 1 references, but the pair is not recorded")
                            for f in failures), failures)
        self.assertIn("gone:oltp (expanded): recorded, but no longer an upward pair in crate-graph.toml", failures)

    def test_write_replaces_only_the_generated_block(self):
        directory = Path(tempfile.mkdtemp(prefix="crate-graph-toml-"))
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        path = directory / "crate-graph.toml"
        shutil.copyfile(CONFIG.path, path)
        before = path.read_text(encoding="utf-8")
        counts = {method: {key: 7 for key in CONFIG.ratchet[method]} for method in crate_graph.METHODS}
        crate_graph.write_ratchet(path, counts, "abc")
        after = path.read_text(encoding="utf-8")
        head = before[:before.index(crate_graph.RATCHET_BEGIN)]
        self.assertTrue(after.startswith(head))
        reloaded = crate_graph.load_config(path)
        self.assertEqual(reloaded.ratchet, counts)
        self.assertEqual(reloaded.ratchet_sha, "abc")
        self.assertEqual(reloaded.layers, CONFIG.layers)


class OwnersTests(unittest.TestCase):
    def test_owners_lists_sources_destinations_and_items(self):
        base = Path(tempfile.mkdtemp(prefix="crate-graph-owners-"))
        self.addCleanup(shutil.rmtree, base, ignore_errors=True)
        write_tree(base, {f"server/src/{rel}": text for rel, text in RatchetTests.BASE.items()})
        write_tree(base, {"server/src/oltp_replication/owned.rs": "pub struct A; pub struct B;\n"})
        report = crate_graph.owners(crate_graph.Graph(CONFIG, base), "oltp", "oltp_replication")
        self.assertEqual(report["edge"], "oltp:oltp_replication")
        self.assertEqual((report["literal"], report["expanded"]), (1, 3))
        self.assertEqual(report["sources"], [{"path": "server/src/oltp/mod.rs", "literal": 1, "expanded": 3}])
        self.assertEqual(report["destinations"], [
            {"path": "server/src/oltp_replication/owned.rs", "expanded": 2},
            {"path": "server/src/oltp_replication/types.rs", "expanded": 1},
        ])
        self.assertEqual(dict(report["items"]), {"Digest": 1, "A": 1, "B": 1})

    def test_owners_command_on_the_real_tree_matches_the_ratchet(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            status = crate_graph.main(["owners", "--edge", "oltp:oltp_replication", "--json"])
        self.assertEqual(status, 0)
        [report] = json.loads(output.getvalue())
        self.assertEqual(report["literal"], CONFIG.ratchet["literal"]["oltp:oltp_replication"])
        self.assertEqual(report["expanded"], CONFIG.ratchet["expanded"]["oltp:oltp_replication"])
        self.assertTrue(all(row["path"].startswith("server/src/oltp/") for row in report["sources"]))
        self.assertTrue(all(row["path"].startswith("server/src/oltp_replication/")
                            for row in report["destinations"]))
        self.assertEqual(sum(row["literal"] for row in report["sources"]), report["literal"])

    def test_edge_argument_is_checked(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            crate_graph.main(["owners", "--edge", "oltp"])


class SourceEdgeTests(unittest.TestCase):
    def test_planted_source_edge(self):
        """Case 5: naming the replication crate inside storage fails `cargo check` with E0432/E0433."""
        plant = CONFIG.plant
        lib = ROOT / plant["lib"]
        if not lib.is_file():
            self.skipTest(f"not-run: {plant['lib']} does not exist yet (P8-G creates {plant['package']})")
        if not HAVE_CARGO or not HAVE_GIT:
            self.fail("case 5 needs cargo and git once the storage crate exists")
        work = Path(tempfile.mkdtemp(prefix="crate-graph-source-"))
        tree = work / "tree"
        subprocess.run(["git", "worktree", "add", "--detach", "--quiet", str(tree), "HEAD"], cwd=ROOT, check=True)
        self.addCleanup(shutil.rmtree, work, ignore_errors=True)
        self.addCleanup(subprocess.run, ["git", "worktree", "remove", "--force", str(tree)], cwd=ROOT)
        with (tree / plant["lib"]).open("a", encoding="utf-8") as source:
            source.write(f"\nuse {plant['crate']} as _;\n")
        target = Path(crate_graph.os.environ.get("CARGO_TARGET_DIR", ROOT / "server" / "target"))
        result = subprocess.run(
            ["cargo", "check", "--locked", "-p", plant["package"], "--lib", "--message-format=short",
             "--target-dir", str(target)],
            cwd=tree / "server", capture_output=True, text=True, timeout=1200,
        )
        output = result.stdout + result.stderr
        self.assertNotEqual(result.returncode, 0, output[-2000:])
        self.assertRegex(output, r"E043[23]")
        self.assertIn(plant["crate"], output)


class ProofTests(unittest.TestCase):
    def test_proof_impls(self):
        """Case 6: every `impl ...Proof for` lives in the replication crate."""
        requires = CONFIG.proofs["requires"]
        if not (ROOT / requires).exists():
            self.skipTest(f"not-run: {requires} does not exist yet (P8-H creates the replication crate)")
        report = crate_graph.proof_report(ROOT, CONFIG)
        self.assertTrue(report["impls"], "no `impl ...Proof for` blocks found; the case would pass vacuously")
        self.assertEqual(report["outside"], [])

    def test_scanner_finds_impls_and_ignores_others(self):
        source = textwrap.dedent("""
            impl StatusDecisionProof for VerifiedStatusDecision {}
            impl<'a, T: Into<Vec<u8>>> crate::proofs::SplitProof<T> for Proven<'a> where T: Send {}
            // impl CommentedProof for Nothing {}
            impl Proofreader for Text {}
            impl Display for SettlementProof {}
            impl<F> ExclusionProof for F where F: for<'a> Fn(&'a u8) {}
        """)
        self.assertEqual(crate_graph.proof_impls(source), [
            (2, "StatusDecisionProof"),
            (3, "crate::proofs::SplitProof"),
            (7, "ExclusionProof"),
        ])

    def test_forged_proof_outside_replication_is_reported(self):
        base = Path(tempfile.mkdtemp(prefix="crate-graph-proofs-"))
        self.addCleanup(shutil.rmtree, base, ignore_errors=True)
        write_tree(base, {
            "server/oltp-replication/Cargo.toml": "",
            "server/oltp-replication/src/status.rs": "impl StatusDecisionProof for VerifiedStatusDecision {}\n",
            "server/oltp-storage/src/tests.rs": "struct Fake;\nimpl StatusDecisionProof for Fake {}\n",
            "server/oltp-storage/target/debug/build/out.rs": "impl StatusDecisionProof for Built {}\n",
        })
        report = crate_graph.proof_report(base, CONFIG)
        self.assertEqual(len(report["impls"]), 2)
        self.assertEqual(report["outside"], ["server/oltp-storage/src/tests.rs:2: impl StatusDecisionProof"])
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            status = crate_graph.main(["--repo", str(base), "proofs"])
        self.assertEqual(status, 1)
        self.assertIn("FAIL server/oltp-storage/src/tests.rs:2", output.getvalue())

    def test_no_impls_once_the_crate_exists_fails_instead_of_passing(self):
        base = Path(tempfile.mkdtemp(prefix="crate-graph-noproofs-"))
        self.addCleanup(shutil.rmtree, base, ignore_errors=True)
        write_tree(base, {"server/oltp-replication/Cargo.toml": "", "server/oltp-replication/src/lib.rs": ""})
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            status = crate_graph.main(["--repo", str(base), "proofs"])
        self.assertEqual(status, 1)
        self.assertIn("would pass vacuously", output.getvalue())


class ConfigTests(unittest.TestCase):
    def test_every_planned_and_mapped_package_has_a_layer(self):
        ranks = CONFIG.ranks
        self.assertEqual(set(CONFIG.planned), set(ranks))
        for package, row in CONFIG.planned.items():
            for kind, deps in row.items():
                for dep in deps:
                    with self.subTest(edge=f"{package} -> {dep} ({kind})"):
                        self.assertLess(ranks[dep], ranks[package])
        plant = CONFIG.plant
        self.assertIn(plant["package"], CONFIG.planned[plant["dependency"]]["normal"])

    def test_bad_configs_are_rejected(self):
        directory = Path(tempfile.mkdtemp(prefix="crate-graph-config-"))
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        cases = {
            "no layers": '[modules]\nroot = "server/src"\ndefault = "a"\n',
            "duplicate package": '[[layer]]\nname = "x"\npackages = ["a"]\n[[layer]]\nname = "y"\npackages = ["a"]\n',
            "unknown module package": ('[[layer]]\nname = "x"\npackages = ["a"]\n[modules]\nroot = "s"\n'
                                       'default = "a"\n[modules.layer]\noltp = "b"\n'),
            "negative count": ('[[layer]]\nname = "x"\npackages = ["a"]\n[modules]\nroot = "s"\ndefault = "a"\n'
                               '[ratchet.literal]\n"a:b" = -1\n'),
        }
        for name, text in cases.items():
            with self.subTest(name=name):
                path = directory / f"{name}.toml"
                path.write_text(text, encoding="utf-8")
                with self.assertRaises(crate_graph.GraphError):
                    crate_graph.load_config(path)


if __name__ == "__main__":
    unittest.main()

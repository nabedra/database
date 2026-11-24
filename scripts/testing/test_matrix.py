#!/usr/bin/env python3
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matrix  # noqa: E402

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "testdata" / "matrix"
GIT = ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"]


def git(repo, *args):
    return subprocess.run([*GIT, *args], cwd=repo, check=True, capture_output=True, text=True).stdout.strip()


def commit(repo, files, message):
    for name, text in files.items():
        p = Path(repo) / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", message)
    return git(repo, "rev-parse", "HEAD")


class FixtureRepo:
    """A git repo with HEAD, HEAD~1, HEAD~2 on `main`, an unrelated `other` commit, and the fixture
    registers, checks and records (placeholder shas substituted) copied next to it."""

    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.repo = root / "repo"
        self.repo.mkdir()
        git(self.repo, "init", "-q", "-b", "main")
        git(self.repo, "remote", "add", "origin", "git@github-fixture:example/repo.git")
        c0 = commit(self.repo, {"README.md": "0\n"}, "c0")
        git(self.repo, "checkout", "-q", "-b", "other")
        self.other = commit(self.repo, {"other.txt": "x\n"}, "other")
        git(self.repo, "checkout", "-q", "main")
        c1 = commit(self.repo, {"server/src/cache/a.rs": "1\n"}, "c1")
        c2 = commit(self.repo, {"server/src/cache/a.rs": "2\n"}, "c2")
        self.head = c2
        self.shas = {"@HEAD~2": c0, "@HEAD~1": c1, "@HEAD": c2, "@OTHER": self.other}
        self.records = root / "records"
        for src in (FIXTURES / "records").rglob("*.json"):
            dst = self.records / src.relative_to(FIXTURES / "records")
            dst.parent.mkdir(parents=True, exist_ok=True)
            data = json.loads(src.read_text())
            data["sha"] = self.shas[data["sha"]]
            dst.write_text(json.dumps(data))
        self.registers = root / "registers"
        shutil.copytree(FIXTURES / "registers", self.registers)
        self.checks = root / "checks"
        shutil.copytree(FIXTURES / "checks", self.checks)
        self.modules = root / "modules.toml"
        text = (FIXTURES / "modules.toml").read_text().replace('"registers/', f'"{self.registers.as_posix()}/')
        self.modules.write_text(text)
        self.out = root / "out"
        self.paths = matrix.Paths(self.repo, self.registers, self.records, self.checks, self.modules, self.out)

    def cleanup(self):
        self.tmp.cleanup()


class MatrixFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fx = FixtureRepo()
        cls.addClassCleanup(cls.fx.cleanup)


class GlobTests(unittest.TestCase):
    def test_globs(self):
        m = matrix.glob_regex
        self.assertTrue(m("server/src/cache/**").match("server/src/cache/a/b.rs"))
        self.assertFalse(m("server/src/cache/**").match("server/src/cache.rs"))
        self.assertTrue(m("**/test_*.py").match("test_x.py"))
        self.assertTrue(m("**/test_*.py").match("a/b/test_x.py"))
        self.assertFalse(m("scripts/*.py").match("scripts/a/b.py"))
        self.assertTrue(m("server/**/tests/**").match("server/kv/tests/a.rs"))
        self.assertTrue(m("server/**/tests/**").match("server/tests/a.rs"))


class RegisterTests(unittest.TestCase):
    def test_parse_fixture(self):
        reg = matrix.parse_register((FIXTURES / "registers" / "cache.md").read_text(), Path("cache.md"))
        self.assertEqual((reg.module, reg.level), ("cache", "L2"))
        g1, g2, g3 = reg.guarantees
        self.assertEqual(g1.checks, ["U", "S", "R", "P"])
        self.assertEqual(g1.deferred("R"), "cloud-deferred")
        self.assertEqual(g1.deferred("P"), "hardware-deferred")
        self.assertEqual(g2.na, {})
        self.assertEqual(g3.na["M"], "no protocol state beyond the lease, covered by U")
        self.assertEqual(g1.scenarios, ["K01", "K02"])
        ev = matrix.parse_register((FIXTURES / "registers" / "events.md").read_text(), Path("e.md"))
        self.assertEqual((ev.module, ev.guarantees[0].id), ("events", "EVT-G01"))

    def test_malformed_registers(self):
        good = (FIXTURES / "registers" / "cache.md").read_text()
        cases = {
            "no module": good.replace("Module: cache", ""),
            "no level": good.replace("Level: L2", ""),
            "no table": good.replace("| ID | Guarantee |", "| Key | Guarantee |"),
            "two tables": good + "\n| ID | Guarantee | Failure model | Scenarios | Checks | n/a |\n| - | - | - | - | - | - |\n| CACHE-G09 | x | y | | U | |\n",
            "bad kind": good.replace("| U D |", "| U Q |"),
            "bad n/a": good.replace("R: cloud-deferred (EKS soak)", "cloud-deferred"),
            "bad id": good.replace("| CACHE-G02 |", "| CACHE2 |"),
        }
        for name, text in cases.items():
            with self.subTest(name), self.assertRaises(matrix.MatrixError):
                matrix.parse_register(text, Path("cache.md"))


class BuildTests(MatrixFixtureTests):
    def cells(self, **kw):
        m = matrix.build_matrix(self.fx.paths, allow_unattested=True, **kw)
        return m, {f"{g} {k}": c for (g, k), c in m.cells.items()}

    def test_every_cell_state(self):
        m, c = self.cells()
        self.assertEqual(m.kinds, ["U", "S", "H", "D", "R", "P"])
        self.assertEqual(c["EVT-G01 U"].state, "pass")
        self.assertEqual(c["CACHE-G01 U"].state, "pass")
        self.assertEqual(c["CACHE-G01 U"].run_url, "https://github.com/example/repo/actions/runs/1")
        self.assertEqual(c["CACHE-G01 U"].date, "2026-11-22T12:00:00+00:00")
        self.assertEqual((c["CACHE-G01 S"].state, c["CACHE-G01 S"].behind, c["CACHE-G01 S"].last), ("stale", 2, "pass"))
        self.assertEqual((c["CACHE-G01 R"].state, c["CACHE-G01 R"].deferred), ("not-run", "cloud-deferred"))
        self.assertEqual((c["CACHE-G01 P"].state, c["CACHE-G01 P"].deferred), ("not-run", "hardware-deferred"))
        self.assertFalse(c["CACHE-G01 P"].green())
        self.assertEqual(c["CACHE-G02 U"].state, "fail")
        self.assertEqual(c["CACHE-G02 D"].state, "not-run")
        self.assertIn("s3-unconfigured", c["CACHE-G02 D"].reason)
        self.assertEqual(c["CACHE-G03 U"].state, "quarantined 3/5")
        self.assertFalse(c["CACHE-G03 U"].green())
        self.assertEqual(c["CACHE-G03 H"].state, "missing")
        self.assertIn("1 uncounted", c["CACHE-G03 H"].reason)
        self.assertFalse(c["CACHE-G01 H"].required)
        self.assertEqual(c["CACHE-G01 H"].text(), "")

    def test_invalid_and_foreign_records_are_not_counted(self):
        m, _ = self.cells()
        reasons = {os.path.basename(i["path"]): i["reason"] for i in m.ignored}
        self.assertIn("zero executed", reasons["fx-7--cache-empty.json"])
        self.assertIn("not HEAD or an ancestor", reasons["fx-6--cache-sim.json"])

    def test_quarantine_takes_precedence_on_the_same_sha(self):
        source = self.fx.records / "2026/11"
        quarantined = json.loads((source / "fx-5--cache-flaky.json").read_text())
        passed = json.loads((source / "fx-1--cache-unit.json").read_text())
        passed["guarantees"] = quarantined["guarantees"]
        with tempfile.TemporaryDirectory() as directory:
            records = Path(directory)
            (records / "quarantined.json").write_text(json.dumps(quarantined))
            (records / "pass.json").write_text(json.dumps(passed))
            m = matrix.build_matrix(replace(self.fx.paths, records=records), allow_unattested=True)
        cell = m.cells[("CACHE-G03", "U")]
        self.assertEqual(cell.state, "quarantined 3/5")
        self.assertFalse(cell.green())

    def test_unattested_records_do_not_count_by_default(self):
        m = matrix.build_matrix(self.fx.paths)
        states = {c.state for c in m.cells.values() if c.required and not c.deferred and c.state != "n/a"}
        self.assertEqual(states, {"missing"})

    def fake_gh(self, code):
        bindir = Path(self.fx.tmp.name) / "bin"
        bindir.mkdir(exist_ok=True)
        gh = bindir / "gh"
        gh.write_text(f"#!/bin/sh\necho \"$@\" >> {bindir}/calls\nexit {code}\n")
        gh.chmod(gh.stat().st_mode | stat.S_IEXEC)
        return bindir

    def test_verified_attestations_count(self):
        for code, expect in ((0, "pass"), (1, "missing")):
            with self.subTest(code=code):
                bindir = self.fake_gh(code)
                old = os.environ["PATH"]
                os.environ["PATH"] = f"{bindir}{os.pathsep}{old}"
                try:
                    m = matrix.build_matrix(self.fx.paths, verify=True)
                finally:
                    os.environ["PATH"] = old
                self.assertEqual(m.cells[("EVT-G01", "U")].state, expect)
                self.assertRegex((bindir / "calls").read_text(), r"attestation verify .* --repo example/repo")

    def test_head_option_moves_staleness(self):
        m = matrix.build_matrix(self.fx.paths, self.fx.shas["@HEAD~1"], allow_unattested=True)
        # Records on HEAD are not ancestors of HEAD~1; the HEAD~2 record is 1 behind.
        self.assertEqual(m.cells[("CACHE-G01", "U")].state, "missing")
        self.assertEqual(m.cells[("CACHE-G01", "S")].behind, 1)

    def test_outputs(self):
        self.assertEqual(matrix.main(["build", "--repo", str(self.fx.repo), "--registers-dir", str(self.fx.registers),
                                      "--evidence-dir", str(self.fx.records), "--checks-dir", str(self.fx.checks),
                                      "--modules", str(self.fx.modules), "--out-dir", str(self.fx.out),
                                      "--allow-unattested"]), 0)
        md = (self.fx.out / "coverage-matrix.md").read_text()
        self.assertIn("| CACHE-G03 | Leases are linearizable", md)
        self.assertIn("quarantined 3/5", md)
        self.assertIn("stale (2 behind)", md)
        self.assertIn("CACHE-G01 R: cloud-deferred", md)
        self.assertIn("Unattested evidence was allowed", md)
        data = json.loads((self.fx.out / "coverage-matrix.json").read_text())
        self.assertEqual(data["head"], self.fx.head)
        row = next(r for r in data["rows"] if r["id"] == "CACHE-G02")
        self.assertEqual(row["cells"]["U"]["state"], "fail")

    def test_duplicate_guarantee_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            registers = Path(directory)
            shutil.copy(self.fx.registers / "cache.md", registers / "cache.md")
            shutil.copy(self.fx.registers / "cache.md", registers / "cache-copy.md")
            with self.assertRaises(matrix.MatrixError):
                matrix.build_matrix(replace(self.fx.paths, registers=registers), allow_unattested=True)


class SelectTests(MatrixFixtureTests):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.diff_sha = commit(cls.fx.repo, {"server/src/cache/store/map.rs": "x\n",
                                          "server/events/src/a.rs": "y\n"}, "diff")
        cls.docker_sha = commit(cls.fx.repo, {"Dockerfile": "FROM x\n"}, "docker")

    def test_synthetic_diff(self):
        sel = matrix.select(self.fx.paths, self.fx.head, self.diff_sha)
        self.assertEqual(sorted(sel["modules"]), ["cache", "events"])
        self.assertFalse(sel["full_t1"])
        self.assertEqual([c["id"] for c in sel["t1_checks"]], ["cache-unit", "events-unit"])  # cache-sim is T2
        self.assertEqual(sel["guarantees"], ["CACHE-G01", "CACHE-G02", "CACHE-G03", "EVT-G01"])
        self.assertEqual(sel["paired_ab"], {"cache": ["server/src/cache/store/map.rs"]})
        text = matrix.render_select(sel)
        self.assertIn("`cache-unit`", text)
        self.assertIn("Paired A/B required", text)

    def test_unmapped_path_selects_every_t1_check(self):
        sel = matrix.select(self.fx.paths, self.diff_sha, self.docker_sha)
        self.assertTrue(sel["full_t1"])
        self.assertEqual(list(sel["modules"]), ["misc"])
        self.assertEqual([c["id"] for c in sel["t1_checks"]], ["cache-unit", "events-unit"])

    def test_docs_only_change_selects_nothing(self):
        sel = matrix.select(self.fx.paths, "x", "y", changed=["scripts/testing/README.md"])
        self.assertEqual(list(sel["modules"]), ["testing"])
        self.assertEqual(sel["t1_checks"], [])

    def test_changed_check_selects_its_declared_module(self):
        sel = matrix.select(self.fx.paths, "x", "y", changed=["docs/testing/checks/cache-unit.toml"])
        self.assertIn("cache", sel["modules"])
        self.assertIn("cache-unit", [c["id"] for c in sel["t1_checks"]])
        self.assertIn("CACHE-G01", sel["guarantees"])

    def test_deleted_check_selects_all_t1_checks(self):
        sel = matrix.select(self.fx.paths, "x", "y", changed=["docs/testing/checks/removed.toml"])
        self.assertTrue(sel["full_t1"])
        self.assertEqual([c["id"] for c in sel["t1_checks"]], ["cache-unit", "events-unit"])

    def spec_checks(self):
        """The fixture checks plus a kind T check for the cache module."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        checks = Path(tmp.name)
        for p in self.fx.paths.checks.glob("*.toml"):
            (checks / p.name).write_text(p.read_text())
        (checks / "cache-lease.toml").write_text(
            'id = "cache-lease"\nmodule = "cache"\nkinds = ["T"]\nguarantees = ["CACHE-G01"]\n'
            'tiers = ["T1"]\ncommand = ["python3", "scripts/testing/tla_check.py", "specs/cache", "Lease"]\n'
            'cwd = "."\ntimeout_s = 3600\nneeds = ["tlc"]\nresult_parser = "tlc"\n')
        return replace(self.fx.paths, checks=checks)

    def test_spec_folder_selects_its_module_and_t_checks(self):
        paths = self.spec_checks()
        sel = matrix.select(paths, "x", "y", changed=["specs/cache/Lease.tla"])
        self.assertEqual(list(sel["modules"]), ["cache"])
        self.assertFalse(sel["full_t1"])
        self.assertIn("cache-lease", [c["id"] for c in sel["t1_checks"]])
        self.assertNotIn("events-unit", [c["id"] for c in sel["t1_checks"]])

    def test_spec_tooling_selects_every_t_check(self):
        paths = self.spec_checks()
        sel = matrix.select(paths, "x", "y", changed=["specs/lib/NabedraTrace.tla"])
        self.assertEqual(list(sel["modules"]), ["testing"])
        self.assertTrue(sel["all_t_checks"])
        self.assertEqual([c["id"] for c in sel["t1_checks"]], ["cache-lease"])
        self.assertIn("every kind T check", matrix.render_select(sel))
        sel = matrix.select(paths, "x", "y", changed=["specs/selftest/TwoPhase.tla"])
        self.assertEqual((list(sel["modules"]), sel["all_t_checks"]), (["testing"], False))

    def test_unknown_spec_folder_falls_back(self):
        sel = matrix.select(self.spec_checks(), "x", "y", changed=["specs/nomodule/X.tla"])
        self.assertTrue(sel["full_t1"])

    def test_repository_path_ownership(self):
        modules = matrix.load_modules(matrix.Paths().modules)
        examples = {
            "server/src/oltp_runtime/direct/executor.rs": ["oltp"],
            "orm/python/src/nabedra/client.py": ["orm-python"],
            ".github/workflows/nabedra-build.yml": ["ci"],
            "docs/testing/checks/testing-matrix.toml": ["testing"],
            "server/check/tests/model_queue.rs": ["check"],
            "server/fuzz/tests/replay.rs": ["fuzz"],
            "scripts/cache-acceptance/test_valkey.py": ["cache"],
            "orm/contract/test_api.py": ["orm-python", "orm-typescript"],
        }
        for path, expected in examples.items():
            with self.subTest(path=path):
                self.assertEqual(modules.modules_for(path), expected)


class LintTests(unittest.TestCase):
    def setUp(self):
        self.paths = matrix.Paths(registers=FIXTURES / "registers", checks=FIXTURES / "checks",
                                  modules=FIXTURES / "modules.toml")

    def test_fixture_is_clean_apart_from_warnings(self):
        errors, warnings = matrix.lint(self.paths, files=["server/src/cache/tests/a.rs"])
        self.assertEqual(errors, [])
        # CACHE-G02 needs D and nothing provides it; deferred CACHE-G01 R is not warned about.
        self.assertIn("CACHE-G02 requires kind D but no registered check provides it", warnings)
        self.assertFalse(any("CACHE-G01 requires kind R" in w for w in warnings))

    def test_each_failure(self):
        errors, _ = matrix.lint(replace(self.paths, checks=FIXTURES / "checks-bad"), files=[
            "server/src/cache/x.rs", "server/other/tests/dangling.rs", "tools/test_orphan.py",
            "orm/ruby/test/a_test.rb", "server/src/cache/tests/ok.rs"])
        text = "\n".join(errors)
        self.assertIn("does not match the file name 'name-mismatch.toml'", text)
        self.assertIn("references unknown guarantee CACHE-G99", text)
        self.assertIn("server/other/tests/dangling.rs: test file matches no module glob", text)
        self.assertIn("tools/test_orphan.py: test file matches no module glob", text)
        self.assertIn("orm/ruby/test/a_test.rb: test file matches no module glob", text)
        self.assertNotIn("server/src/cache/tests/ok.rs", text)
        self.assertNotIn("server/src/cache/x.rs", text)

    def test_bad_modules_file(self):
        text = self.paths.modules.read_text().replace("share = 40", "share = 30").replace('level = "L1"', 'level = "L4"')
        with tempfile.TemporaryDirectory() as directory:
            modules = Path(directory) / "modules.toml"
            modules.write_text(text)
            errors, _ = matrix.lint(replace(self.paths, modules=modules), files=[])
        self.assertTrue(any("add up to 90" in e for e in errors), errors)
        self.assertTrue(any("level 'L4'" in e for e in errors), errors)

    def test_missing_register_file(self):
        text = self.paths.modules.read_text().replace("registers/cache.md", "registers/kv.md")
        with tempfile.TemporaryDirectory() as directory:
            modules = Path(directory) / "modules.toml"
            modules.write_text(text)
            errors, _ = matrix.lint(replace(self.paths, modules=modules), files=[])
        self.assertIn("modules.toml: module cache names register registers/kv.md, "
                      "which matches no register file", errors)

    def test_repository_tree_has_no_dangling_tests(self):
        paths = matrix.Paths()
        modules = matrix.load_modules(paths.modules)
        self.assertEqual(modules.problems(), [])
        errors, _ = matrix.lint(paths)
        self.assertEqual([e for e in errors if "dangling" in e or "modules.toml" in e], [])


class GateTests(MatrixFixtureTests):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.next_sha = commit(cls.fx.repo, {"x": "1"}, "next")

    def test_release_gate_fails_on_fixture(self):
        res = matrix.gate(self.fx.paths, self.fx.head, "release", allow_unattested=True)
        self.assertFalse(res.ok)
        failing = sorted(f"{c.guarantee} {c.kind}" for c in res.failing)
        self.assertEqual(failing, ["CACHE-G01 S", "CACHE-G02 D", "CACHE-G02 U", "CACHE-G03 H", "CACHE-G03 U"])
        self.assertEqual([f"{c.guarantee} {c.kind}" for c in res.deferred], ["CACHE-G01 R", "CACHE-G01 P"])
        self.assertIn("deferred (not counted as green): CACHE-G01 R", res.report())
        self.assertIn("deferred (not counted as green): CACHE-G01 P", res.report())

    def test_deferred_cells_block_even_with_pass_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            registers = Path(directory)
            (registers / "cache.md").write_text(
                "Module: cache\nLevel: L2\n"
                "| ID | Guarantee | Failure model | Scenarios | Checks | n/a |\n"
                "| --- | --- | --- | --- | --- | --- |\n"
                "| CACHE-G01 | Failover | primary crash | K01 | U R P | "
                "R: cloud-deferred (EKS soak); P: hardware-deferred (absolute latency) |\n"
            )
            records = registers / "records"
            records.mkdir()
            record = json.loads((self.fx.records / "2026/11/fx-1--cache-unit.json").read_text())
            record["kind"] = "U R P"
            (records / "pass.json").write_text(json.dumps(record))
            paths = matrix.Paths(self.fx.repo, registers, records)
            res = matrix.gate(paths, self.fx.head, "release", allow_unattested=True)
        self.assertFalse(res.ok)
        self.assertEqual([cell.kind for cell in res.passed], ["U"])
        self.assertEqual([(cell.kind, cell.state) for cell in res.deferred], [("R", "not-run"), ("P", "not-run")])

    def test_module_gate_passes_when_every_cell_passed_on_that_sha(self):
        res = matrix.gate(self.fx.paths, self.fx.head, "module:events", allow_unattested=True)
        self.assertTrue(res.ok, res.report())
        self.assertEqual([f"{c.guarantee} {c.kind}" for c in res.passed], ["EVT-G01 U"])

    def test_gate_requires_exactly_that_sha(self):
        # On the next commit the same evidence is stale, so the gate fails.
        res = matrix.gate(self.fx.paths, self.next_sha, "module:events", allow_unattested=True)
        self.assertFalse(res.ok)
        self.assertEqual(res.failing[0].state, "stale")

    def test_gate_needs_attested_evidence(self):
        res = matrix.gate(self.fx.paths, self.fx.head, "module:events")
        self.assertFalse(res.ok)

    def test_unknown_or_empty_scope_fails(self):
        with self.assertRaises(matrix.MatrixError):
            matrix.gate(self.fx.paths, self.fx.head, "everything", allow_unattested=True)
        self.assertFalse(matrix.gate(self.fx.paths, self.fx.head, "module:nope", allow_unattested=True).ok)

    def test_cli_exit_status(self):
        args = ["gate", "--repo", str(self.fx.repo), "--registers-dir", str(self.fx.registers),
                "--evidence-dir", str(self.fx.records), "--checks-dir", str(self.fx.checks),
                "--modules", str(self.fx.modules), "--sha", self.fx.head]
        self.assertEqual(matrix.main(args + ["--scope", "release", "--allow-unattested"]), 1)
        with patch.object(matrix, "verify_attestation", return_value=True) as verify:
            self.assertEqual(matrix.main(args + ["--scope", "module:events"]), 0)
        verify.assert_called()


if __name__ == "__main__":
    unittest.main()

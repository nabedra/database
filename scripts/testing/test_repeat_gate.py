from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import repeat_gate


PYTHON = {"id": "Case::test_replay", "kind": "python", "file": "orm/python/tests/test_replay.py"}
NODE = {
    "id": "R:orm/typescript/test/example.test.mjs::replay (2)",
    "kind": "node",
    "file": "orm/typescript/test/example.test.mjs",
}
RUST = {"id": "rust_candidate::tests::replay", "kind": "rust", "file": "server/events/src/lib.rs"}


class RepeatGateTests(unittest.TestCase):
    def test_runs_each_language_with_exact_filters(self):
        rust, cwd, real = repeat_gate.test_command(RUST)
        self.assertFalse(real)
        self.assertEqual(cwd, repeat_gate.ROOT)
        self.assertEqual(rust, [
            "cargo", "test", "--locked", "--jobs", "2", "--manifest-path", "server/events/Cargo.toml",
            "--lib", "rust_candidate::tests::replay", "--", "--exact", "--include-ignored",
            "--test-threads=1",
        ])
        python, cwd, real = repeat_gate.test_command(PYTHON)
        self.assertFalse(real)
        self.assertEqual(cwd, repeat_gate.ROOT / "orm/python/tests")
        self.assertEqual(python, [sys.executable, "-m", "unittest", "test_replay.Case.test_replay"])
        node, _, real = repeat_gate.test_command(NODE)
        self.assertTrue(real)
        self.assertIn(r"^replay \(2\)$", node)

    def test_rust_selects_integration_target_and_engine(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "server").mkdir()
            (root / "server/Cargo.toml").write_text(
                '[[test]]\nname = "query"\nrequired-features = ["olap"]\n'
            )
            test = {"id": "case::test_query", "kind": "rust", "file": "server/tests/query/cases.rs"}
            command, _, _ = repeat_gate.test_command(test, root)
        self.assertEqual(command[7:13], ["--no-default-features", "--features", "olap",
                                        "--test", "query", "case::test_query"])

    def test_catalog_rust_name_resolves_nested_tests_and_reuses_listing(self):
        file = "server/src/cache/commands.rs"
        tests = [
            {"id": f"{file}::replay", "lang": "rust", "file": file},
            {"id": f"{file}::read", "kind": "rust", "file": file},
        ]
        listing = subprocess.CompletedProcess([], 0, (
            "cache::commands::tests::replay: test\n"
            "other::tests::replay: test\n"
            "cache::commands::tests::read: test\n"
        ))
        passed = subprocess.CompletedProcess([], 0, "test result: ok. 1 passed; 0 failed; 0 ignored;")
        run = Mock(side_effect=[listing] + [passed] * 10)
        with redirect_stdout(io.StringIO()):
            self.assertEqual(repeat_gate.repeat(tests, real_env=True, run=run), 0)
        self.assertIn("--list", run.call_args_list[0].args[0])
        self.assertIn("cache::commands::tests::replay", run.call_args_list[1].args[0])
        self.assertIn("cache::commands::tests::read", run.call_args_list[6].args[0])
        self.assertEqual(run.call_count, 11)

    def test_one_flaky_run_blocks_after_all_fifty_repetitions(self):
        success = subprocess.CompletedProcess([], 0, "Ran 1 test in 0.001s\n\nOK\n")
        failure = subprocess.CompletedProcess([], 1, "AssertionError: planted intermittent failure")
        run = Mock(side_effect=[success] * 12 + [failure] + [success] * 37)
        with redirect_stdout(io.StringIO()) as output, redirect_stderr(io.StringIO()) as error:
            self.assertEqual(repeat_gate.repeat([PYTHON], run=run), 1)
        self.assertEqual(run.call_count, 50)
        self.assertIn("49/50 passed, 1 failed", output.getvalue())
        self.assertIn("attempt 13", error.getvalue())

    def test_real_env_flag_and_id_select_five_runs(self):
        python_pass = subprocess.CompletedProcess([], 0, "Ran 1 test in 0.001s\n\nOK\n")
        node_pass = subprocess.CompletedProcess([], 0, "1..1\n# pass 1\n")
        with redirect_stdout(io.StringIO()) as output:
            run = Mock(return_value=python_pass)
            self.assertEqual(repeat_gate.repeat([PYTHON], real_env=True, run=run), 0)
            self.assertEqual(run.call_count, 5)
            run = Mock(return_value=node_pass)
            self.assertEqual(repeat_gate.repeat([NODE], run=run), 0)
            self.assertEqual(run.call_count, 5)
        self.assertEqual(output.getvalue().count("5/5 passed"), 2)

    def test_zero_tests_or_skips_do_not_count_as_passes(self):
        log_cases = {
            "rust": "test result: ok. 0 passed; 0 failed; 0 ignored; 172 filtered out",
            "python": "Ran 1 test in 0.001s\n\nOK (skipped=1)\n",
            "node": "1..0\n# Subtest: example.test.mjs\n1..1\n# pass 1\n",
        }
        for kind, log in log_cases.items():
            with self.subTest(kind=kind):
                self.assertFalse(repeat_gate.assertion_ran(kind, log))

    def test_dry_run_consumes_catalog_and_never_spawns(self):
        body = json.dumps({"tests": [RUST, NODE]})
        with patch("sys.stdin", io.StringIO(body)), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(repeat_gate.main(["--dry-run"]), 0)
        self.assertIn("50 ×", output.getvalue())
        self.assertIn("5 ×", output.getvalue())
        self.assertIn("--exact", output.getvalue())

    def test_accepts_catalog_versions_that_emit_a_bare_lang_array(self):
        body = json.dumps([{"id": "scripts/testing/test_thing.py::Thing::test_case",
                            "lang": "python", "file": "scripts/testing/test_thing.py"}])
        with patch("sys.stdin", io.StringIO(body)), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(repeat_gate.main(["--dry-run"]), 0)
        self.assertIn("unittest test_thing.Thing.test_case", output.getvalue())

    def test_partial_rust_module_path_resolves_to_the_full_name(self):
        # Regression: a selector containing :: skipped resolution, so --exact
        # matched zero tests and a good test failed the gate.
        file = "server/src/cache/store.rs"
        test = {"id": f"{file}::model_tests::get_set", "kind": "rust", "file": file}
        listing = subprocess.CompletedProcess([], 0, (
            "cache::store::model_tests::get_set: test\n"
            "cache::store::model_tests::get_set_twice: test\n"
        ))
        passed = subprocess.CompletedProcess([], 0, "test result: ok. 1 passed; 0 failed; 0 ignored;")
        run = Mock(side_effect=[listing] + [passed] * 5)
        with redirect_stdout(io.StringIO()):
            self.assertEqual(repeat_gate.repeat([test], real_env=True, run=run), 0)
        command = run.call_args_list[1].args[0]
        self.assertEqual(command[command.index("--") - 1], "cache::store::model_tests::get_set")

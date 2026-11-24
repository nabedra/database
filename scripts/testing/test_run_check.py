#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_check  # noqa: E402

HERE = Path(__file__).resolve().parent
FIXTURE_CHECKS = HERE / "testdata" / "matrix" / "checks"

CARGO_OK = """
running 3 tests
test a ... ok
test result: ok. 3 passed; 0 failed; 1 ignored; 0 measured; 4 filtered out; finished in 0.01s

running 2 tests
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
"""
CARGO_FAIL = """
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: FAILED. 4 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.20s
"""
CARGO_ZERO = "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 120 filtered out; finished in 0.00s\n"
UNITTEST_OK = "....s\n----------------------------------------------------------------------\nRan 5 tests in 0.010s\n\nOK (skipped=1)\n"
UNITTEST_FAIL = "..F.E\n----------------------------------------------------------------------\nRan 5 tests in 0.010s\n\nFAILED (failures=1, errors=1)\n"
UNITTEST_ZERO = "\n----------------------------------------------------------------------\nRan 0 tests in 0.000s\n\nNO TESTS RAN\n"
NODE_OK = "TAP version 13\nok 1 - a\n# tests 4\n# suites 0\n# pass 3\n# fail 0\n# cancelled 0\n# skipped 1\n# todo 0\n"
NODE_FAIL = "TAP version 13\nnot ok 1 - a\n# tests 3\n# pass 1\n# fail 1\n# cancelled 1\n# skipped 0\n"
TLA_FACTS = {"spec": "specs/selftest/TwoPhase", "mode": "exhaustive", "tier": "T1", "tools": "tlc 1.8.0",
             "distinct_states": 2750, "depth": 15,
             "mutants": [{"name": "m", "expect": "Inv", "observed": "Inv", "ok": True, "cex_length": 6}]}


def tla_summary(verdict, executed, failed, not_run=(), **extra):
    body = {"verdict": verdict, "executed": executed, "failed": failed, "not_run": list(not_run),
            "tla": TLA_FACTS, **extra}
    return "TLC noise\nnabedra-tla-summary: " + json.dumps(body) + "\n"


def check_data(**over):
    data = {
        "id": "demo", "module": "testing", "kinds": ["U"], "guarantees": [], "scenarios": [],
        "tiers": ["T0"], "command": ["true"], "cwd": ".", "timeout_s": 30, "needs": [],
        "result_parser": "exit-code",
    }
    data.update(over)
    return data


class ParserTests(unittest.TestCase):
    def test_cargo_sums_every_result_line(self):
        result, p = run_check.classify("cargo-test", CARGO_OK, 0)
        self.assertEqual((result, p.executed, p.failed), ("pass", 5, 0))
        self.assertEqual(p.not_run[0]["id"], "ignored")
        result, p = run_check.classify("cargo-test", CARGO_FAIL, 101)
        self.assertEqual((result, p.executed, p.failed), ("fail", 9, 2))

    def test_cargo_without_result_line_fails(self):
        result, p = run_check.classify("cargo-test", "error[E0425]: cannot find value\n", 101)
        self.assertEqual(result, "fail")
        self.assertEqual(p.not_run[-1]["id"], "parser")
        # Exit 0 with no summary is still a failure: nothing proves a test ran.
        self.assertEqual(run_check.classify("cargo-test", "", 0)[0], "fail")

    def test_unittest(self):
        result, p = run_check.classify("unittest", UNITTEST_OK, 0)
        self.assertEqual((result, p.executed, p.failed), ("pass", 4, 0))
        result, p = run_check.classify("unittest", UNITTEST_FAIL, 1)
        self.assertEqual((result, p.executed, p.failed), ("fail", 5, 2))
        self.assertEqual(run_check.classify("unittest", "Traceback ...\n", 1)[0], "fail")

    def test_node_tap(self):
        result, p = run_check.classify("node-test", NODE_OK, 0)
        self.assertEqual((result, p.executed, p.failed), ("pass", 3, 0))
        result, p = run_check.classify("node-test", NODE_FAIL, 1)
        self.assertEqual((result, p.executed, p.failed), ("fail", 3, 2))
        self.assertEqual(run_check.classify("node-test", "ok 1\n", 0)[0], "fail")

    def test_exit_code(self):
        self.assertEqual(run_check.classify("exit-code", "", 0)[0], "pass")
        self.assertEqual(run_check.classify("exit-code", "", 3)[0], "fail")

    def test_zero_assertions_is_not_run_never_pass(self):
        for parser, output in (("cargo-test", CARGO_ZERO), ("unittest", UNITTEST_ZERO),
                               ("node-test", "# pass 0\n# fail 0\n")):
            with self.subTest(parser):
                result, p = run_check.classify(parser, output, 0)
                self.assertEqual(result, "not-run")
                self.assertEqual(p.executed, 0)
                self.assertEqual(p.not_run[-1]["id"], "zero-assertions")

    def test_nonzero_exit_without_parsed_failure_is_error(self):
        self.assertEqual(run_check.classify("cargo-test", CARGO_OK, 101)[0], "error")

    def test_timeout_is_error(self):
        result, p = run_check.classify("exit-code", "", None, timed_out=True)
        self.assertEqual(result, "error")
        self.assertEqual(p.not_run[-1]["id"], "timeout")

    def test_not_run_marker_preserves_failures(self):
        marker = "not-run: missing fixture"
        output = marker + "\n" + CARGO_FAIL
        self.assertEqual(run_check.classify("cargo-test", output, 0, not_run_marker=marker)[0], "fail")
        self.assertEqual(run_check.classify("exit-code", marker, 1, not_run_marker=marker)[0], "fail")
        self.assertEqual(run_check.classify("exit-code", marker, 0, True, marker)[0], "error")


class TlcParserTests(unittest.TestCase):
    def test_pass_fail_and_facts(self):
        result, p = run_check.classify("tlc", tla_summary("pass", 6, 0), 0)
        self.assertEqual((result, p.executed, p.failed), ("pass", 6, 0))
        self.assertEqual(p.extra["tla"]["distinct_states"], 2750)
        self.assertEqual(run_check.classify("tlc", tla_summary("fail", 6, 2), 1)[0], "fail")

    def test_not_run_and_error_are_never_pass(self):
        reason = [{"id": "baseline TwoPhase.cfg", "reason": "stopped at the 900 s time budget"}]
        result, p = run_check.classify("tlc", tla_summary("not-run", 3, 0, reason), 2)
        self.assertEqual(result, "not-run")
        self.assertEqual(p.not_run[0]["reason"], "stopped at the 900 s time budget")
        result, p = run_check.classify("tlc", tla_summary("error", 1, 0, [{"id": "tlc-error", "reason": "parse"}]), 3)
        self.assertEqual(result, "error")
        # A failed assertion wins over a not-run baseline.
        self.assertEqual(run_check.classify("tlc", tla_summary("not-run", 3, 1), 2)[0], "fail")

    def test_missing_or_broken_summary_fails(self):
        self.assertEqual(run_check.classify("tlc", "no summary here\n", 1)[0], "fail")
        self.assertEqual(run_check.classify("tlc", "nabedra-tla-summary: {bad json\n", 1)[0], "fail")
        self.assertEqual(run_check.classify("tlc", 'nabedra-tla-summary: {"verdict": "pass"}\n', 0)[0], "fail")

    def test_zero_executed_is_not_run(self):
        result, p = run_check.classify("tlc", tla_summary("pass", 0, 0), 0)
        self.assertEqual(result, "not-run")
        self.assertEqual(p.not_run[-1]["id"], "zero-assertions")

    def test_more_than_one_summary_is_error(self):
        # A later pass must not hide an earlier failure.
        for output in (tla_summary("fail", 1, 1) + tla_summary("pass", 3, 0),
                       tla_summary("pass", 3, 0) + tla_summary("pass", 3, 0)):
            result, p = run_check.classify("tlc", output, 0)
            self.assertEqual(result, "error")
            self.assertIn("nabedra-tla-summary lines", p.not_run[0]["reason"])


class CheckFileTests(unittest.TestCase):
    def test_fixture_checks_load(self):
        checks = run_check.load_all(FIXTURE_CHECKS, {"cache", "events", "testing", "misc"})
        self.assertEqual([c.id for c in checks], ["cache-sim", "cache-unit", "events-unit"])

    def test_repository_checks_load(self):
        checks = run_check.load_all(run_check.CHECKS_DIR, run_check.module_names())
        self.assertIn("testing-matrix", [c.id for c in checks])

    def test_invalid_check_files(self):
        cases = {
            "unknown field": check_data(colour="red"),
            "missing field": {k: v for k, v in check_data().items() if k != "command"},
            "bad kind": check_data(kinds=["Z"]),
            "bad tier": check_data(tiers=["T9"]),
            "bad need": check_data(needs=["gpu"]),
            "bad parser": check_data(result_parser="junit"),
            "absolute cwd": check_data(cwd="/tmp"),
            "escaping cwd": check_data(cwd="../x"),
            "bad guarantee": check_data(guarantees=["CACHE-2"]),
            "unknown module": check_data(module="nope"),
            "bool timeout": check_data(timeout_s=True),
            "T without tlc parser": check_data(kinds=["T"], needs=["tlc"]),
            "T without tlc need": check_data(kinds=["T"], result_parser="tlc"),
        }
        for name, data in cases.items():
            with self.subTest(name), self.assertRaises(run_check.CheckError):
                run_check.parse_check(data, Path("demo.toml"), {"testing", "misc"})

    def test_id_must_match_file_name(self):
        with self.assertRaises(run_check.CheckError) as cm:
            run_check.parse_check(check_data(), Path("other.toml"))
        self.assertIn("does not match the file name", str(cm.exception))


class KindTTests(unittest.TestCase):
    def test_kind_t_check_is_valid(self):
        check = run_check.parse_check(check_data(kinds=["T"], needs=["tlc"], result_parser="tlc"))
        self.assertEqual((check.kinds, check.needs), (["T"], ["tlc"]))

    def test_selftest_checks_are_registered(self):
        checks = {c.id: c for c in run_check.load_all(run_check.CHECKS_DIR, run_check.module_names())}
        for cid in ("specs-selftest", "specs-selftest-trace", "specs-selftest-large"):
            self.assertEqual(checks[cid].kinds, ["T"])
            self.assertEqual(checks[cid].result_parser, "tlc")

    def test_tlc_need_without_docker_is_not_run(self):
        check = run_check.parse_check(check_data(kinds=["T"], needs=["tlc"], result_parser="tlc"))
        reasons = [m["reason"].split(":")[0] for m in run_check.missing_needs(check, {"PATH": "/nonexistent"})]
        self.assertEqual(reasons, ["tlc-unavailable"])


class NeedsTests(unittest.TestCase):
    def test_missing_needs_give_reasons(self):
        check = run_check.parse_check(check_data(needs=["s3", "kind", "node", "valkey"]))
        env = {"PATH": "/nonexistent"}
        reasons = [m["reason"].split(":")[0] for m in run_check.missing_needs(check, env)]
        self.assertEqual(reasons, ["s3-unconfigured", "kind-missing", "node-missing", "valkey-unavailable"])
        env = {"PATH": "/nonexistent", "NABEDRA_TEST_S3_BUCKET": "b", "NABEDRA_TEST_VALKEY_URL": "redis://x"}
        reasons = [m["reason"].split(":")[0] for m in run_check.missing_needs(check, env)]
        self.assertEqual(reasons, ["kind-missing", "node-missing"])

    def test_docker_and_nightly_need_working_commands(self):
        check = run_check.parse_check(check_data(needs=["docker", "rust-nightly"]))
        reasons = [m["reason"].split(":")[0] for m in run_check.missing_needs(check, {"PATH": "/nonexistent"})]
        self.assertEqual(reasons, ["docker-unavailable", "rust-nightly-missing"])


class ExecuteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tmp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(tmp.cleanup)
        cls.repo = Path(tmp.name)
        g = ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"]
        subprocess.run(["git", "init", "-q"], cwd=cls.repo, check=True)
        (cls.repo / "fake_tests.py").write_text(textwrap.dedent("""
            import os, sys, time
            mode = sys.argv[1]
            if mode == "ok":
                print("Ran 3 tests in 0.001s"); print(); print("OK")
            elif mode == "zero":
                print("Ran 0 tests in 0.000s"); print(); print("NO TESTS RAN")
            elif mode == "fail":
                print("Ran 3 tests in 0.001s"); print(); print("FAILED (failures=1)"); sys.exit(1)
            elif mode == "skip":
                print("not-run: missing fixture")
                print("Ran 1 test in 0.001s"); print(); print("OK")
            elif mode == "s3":
                print("nabedra-s3-requests: 12 prefix=runs/req-s3/")
                print("nabedra-s3-requests: 3")
                print("some log: nabedra-s3-requests: 100")
                print("nabedra-s3-requests: 42bad")
                sys.exit(1)
            elif mode == "env":
                print("rid=" + os.environ["NABEDRA_REQUEST_ID"] + " knob=" + os.environ["KNOB"])
            elif mode == "sleep":
                time.sleep(30)
            elif mode == "tla-big":
                import json
                detail = "x" * 300
                facts = {"spec": "specs/x/X", "mode": "exhaustive", "tier": "T1", "tools": "tlc 1.8.0",
                         "runs": [{"cfg": f"X.v{i}.cfg", "verdict": "pass", "detail": detail} for i in range(20)],
                         "mutants": [{"name": f"m{i:02d}", "expect": "Inv", "observed": "Inv" if i else "none",
                                      "ok": i != 0, "cex_length": 7, "detail": detail} for i in range(50)]}
                arts = [{"name": f"tla/mutant-m{i:02d}.log", "sha256": "0" * 64} for i in range(39)]
                arts.append({"name": "tla/summary.json", "sha256": "1" * 64})
                print("nabedra-tla-summary: " + json.dumps({
                    "verdict": "fail", "executed": 71, "failed": 1, "tla": facts, "not_run": [],
                    "artifacts": arts}))
                sys.exit(1)
            elif mode.startswith("tla-"):
                import json
                facts = {"spec": "specs/x/X", "mode": "simulation", "tier": "T2", "tools": "tlc 1.8.0"}
                verdict = mode[4:]
                print("nabedra-tla-summary: " + json.dumps({
                    "verdict": verdict, "executed": 1, "failed": 0, "tla": facts,
                    "not_run": [] if verdict == "pass" else [{"id": "b", "reason": "time budget"}],
                    "artifacts": [{"name": "tla/X.log", "sha256": "0" * 64}]}))
                sys.exit({"pass": 0, "not-run": 2}[verdict])
        """))
        subprocess.run(["git", "add", "."], cwd=cls.repo, check=True)
        subprocess.run([*g, "commit", "-qm", "c"], cwd=cls.repo, check=True)

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        self.evidence = root / "evidence"
        self.logs = root / "logs"

    def run_mode(self, mode, parser="unittest", **over):
        check = run_check.parse_check(check_data(
            command=["python3", "fake_tests.py", mode], result_parser=parser, **over))
        return run_check.execute(check, evidence_dir=self.evidence, log_dir=self.logs,
                                 request_id=f"req-{mode}", env=dict(os.environ), echo=False, repo=self.repo)

    def test_pass_writes_valid_record_and_log(self):
        record, path = self.run_mode("ok")
        self.assertEqual(record["result"], "pass")
        self.assertEqual(record["assertions"]["executed"], 3)
        self.assertTrue(record["tree_clean"])
        self.assertEqual(json.loads(path.read_text()), record)
        self.assertEqual(path.name, "req-ok--demo.json")
        log = self.logs / "req-ok--demo.log"
        self.assertIn("Ran 3 tests", log.read_text())
        self.assertEqual(record["artifacts"][0]["name"], log.name)

    def test_tlc_record_carries_tla_facts_and_mode(self):
        # The tlc need is left out so that the fake command runs without Docker.
        record, path = self.run_mode("tla-pass", parser="tlc")
        self.assertEqual(record["result"], "pass")
        self.assertEqual(record["tla"]["mode"], "simulation")
        self.assertEqual(record["mode"], {"tla": "simulation"})
        self.assertIn("tla/X.log", [a["name"] for a in record["artifacts"]])
        record, _ = self.run_mode("tla-not-run", parser="tlc")
        self.assertEqual(record["result"], "not-run")

    def test_large_tlc_record_is_fitted_under_the_limit(self):
        record, path = self.run_mode("tla-big", parser="tlc")
        self.assertLessEqual(path.stat().st_size, 20 * 1024)
        self.assertEqual((record["result"], record["assertions"]["failed"]), ("fail", 1))
        self.assertTrue(record["tla"]["truncated"])
        mutants = {m["name"]: m for m in record["tla"]["mutants"]}
        self.assertFalse(mutants["m00"]["ok"])  # the failing mutant is kept
        names = [a["name"] for a in record["artifacts"]]
        self.assertTrue(names[0].endswith(".log") and not names[0].startswith("tla/"))
        self.assertIn("tla/summary.json", names)

    def test_zero_assertions_record_is_not_run(self):
        record, _ = self.run_mode("zero")
        self.assertEqual(record["result"], "not-run")

    def test_not_run_marker_overrides_a_success_summary(self):
        marker = "not-run: missing fixture"
        record, path = self.run_mode("skip", not_run_marker=marker)
        self.assertEqual(record["result"], "not-run")
        self.assertEqual(record["assertions"], {
            "executed": 0, "failed": 0,
            "not_run": [{"id": "output-marker", "reason": marker}],
        })
        self.assertEqual(json.loads(path.read_text()), record)
        output = (self.logs / "req-skip--demo.log").read_text()
        self.assertEqual(run_check.classify("unittest", output, 0)[0], "pass")

    def test_s3_requests_are_recorded_even_when_check_fails(self):
        record, path = self.run_mode("s3", parser="exit-code")
        self.assertEqual(record["result"], "fail")
        self.assertEqual(json.loads(path.read_text())["s3_requests"], 15)
        self.assertNotIn("s3_requests", self.run_mode("ok")[0])

    def test_failure(self):
        record, _ = self.run_mode("fail")
        self.assertEqual((record["result"], record["assertions"]["failed"]), ("fail", 1))

    def test_env_and_request_id_reach_the_command(self):
        record, _ = self.run_mode("env", parser="exit-code", env={"KNOB": "7"})
        self.assertEqual(record["result"], "pass")
        self.assertIn("rid=req-env knob=7", (self.logs / "req-env--demo.log").read_text())

    def test_timeout_kills_the_command(self):
        record, _ = self.run_mode("sleep", parser="exit-code", timeout_s=1)
        self.assertEqual(record["result"], "error")
        self.assertEqual(record["assertions"]["executed"], 0)
        self.assertLess(record["duration_s"], 20)

    def test_missing_need_skips_the_command(self):
        env = {k: v for k, v in os.environ.items() if k != "NABEDRA_TEST_S3_BUCKET"}
        check = run_check.parse_check(check_data(command=["python3", "fake_tests.py", "fail"],
                                                 result_parser="unittest", needs=["s3"]))
        record, _ = run_check.execute(check, evidence_dir=self.evidence, log_dir=self.logs,
                                      request_id="req-s3", env=env, echo=False, repo=self.repo)
        self.assertEqual(record["result"], "not-run")
        self.assertTrue(record["assertions"]["not_run"][0]["reason"].startswith("s3-unconfigured"))
        self.assertFalse((self.logs / "req-s3--demo.log").exists())

    def test_dry_run_writes_nothing(self):
        check = run_check.parse_check(check_data(command=["python3", "fake_tests.py", "ok"]))
        self.assertEqual(run_check.execute(check, evidence_dir=self.evidence, log_dir=self.logs,
                                           request_id="req-dry", dry_run=True, echo=False, repo=self.repo),
                         (None, None))
        self.assertFalse(self.evidence.exists())

    def test_python_resolution_preserves_explicit_interpreters_and_arguments(self):
        args = ["-c", "print('hello; world')", "python3"]
        self.assertEqual(run_check.resolve_command(["python3", *args]), [sys.executable, *args])
        for interpreter in ("python3.12", "./bin/python3", "/opt/python/bin/python"):
            command = [interpreter, *args]
            self.assertEqual(run_check.resolve_command(command), command)

    def test_unsafe_request_id_creates_no_log(self):
        check = run_check.parse_check(check_data(command=["python3", "fake_tests.py", "ok"]))
        with self.assertRaises(run_check.evidence.EvidenceError):
            run_check.execute(check, evidence_dir=self.evidence, log_dir=self.logs,
                              request_id="../escape", echo=False, repo=self.repo)
        self.assertFalse(self.logs.exists())
        self.assertFalse(self.evidence.exists())


if __name__ == "__main__":
    unittest.main()

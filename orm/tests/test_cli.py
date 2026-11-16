"""Process-level checks for the thin Rust worker launcher."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "src"
FIXTURE = '''import json, os, signal, sys
from pathlib import Path
if sys.argv[1:] == ["--version"]:
    print("meshdb-server 0.1.0")
    sys.exit(0)
signal.signal(signal.SIGTERM, signal.SIG_DFL)
state = Path(os.environ["FIXTURE_STATE"])
pending = state.with_suffix(".pending")
pending.write_text(json.dumps({"argv": sys.argv[1:], "pid": os.getpid(),
                             "binary": sys.argv[0]}))
pending.replace(state)
if os.environ.get("FIXTURE_WAIT") == "1":
    while True:
        signal.pause()
sys.exit(23)
'''


@unittest.skipUnless(os.name == "posix", "exec PID and signal contract requires POSIX")
class CliTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.state = self.root / "state.json"
        # Deliberately do not inherit credentials, dotenv settings or host PATH.
        self.env = {"PATH": str(self.bin), "PYTHONPATH": str(SOURCE),
                    "PYTHONDONTWRITEBYTECODE": "1", "FIXTURE_STATE": str(self.state)}
        self.path_binary = self.executable(self.bin / "meshdb-server")

    def executable(self, path, body=FIXTURE):
        path.write_text(f"#!{sys.executable}\n" + body)
        path.chmod(0o700)
        return path

    def start(self, *args, code=None):
        command = [sys.executable, "-m", "meshdb.cli", *args]
        if code is not None:
            command = [sys.executable, "-c", code]
        process = subprocess.Popen(command, cwd=self.root, env=self.env,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, start_new_session=True)

        def cleanup():
            if process.returncode is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.communicate(timeout=5)
        self.addCleanup(cleanup)
        return process

    def finish(self, process):
        out, err = process.communicate(timeout=5)
        return process.returncode, out, err

    def test_literal_arguments_override_pid_and_exit_status(self):
        override = self.executable(self.root / "worker binary")
        self.env["MESHDB_SERVER_BINARY"] = "./worker binary"
        bootstrap = "absent bootstrap ; touch EVALUATED $(touch EVALUATED) `touch EVALUATED`"
        process = self.start("worker", "--bootstrap", bootstrap)
        self.assertEqual(self.finish(process), (23, "", ""))
        record = json.loads(self.state.read_text())
        self.assertEqual(record["argv"], ["worker", "--bootstrap", bootstrap])
        self.assertEqual(record["pid"], process.pid)
        self.assertEqual(Path(record["binary"]), override)
        self.assertFalse((self.root / "EVALUATED").exists())

    def test_path_selection_and_invalid_override_never_falls_back(self):
        self.assertEqual(self.finish(self.start("worker", "--bootstrap", "missing")),
                         (23, "", ""))
        self.assertEqual(json.loads(self.state.read_text())["binary"], str(self.path_binary))
        self.state.unlink()
        plain = self.root / "not executable"
        plain.write_text("unused")
        broken = self.executable(self.root / "bad format", body="unused")
        broken.write_text("invalid executable format")
        for override in ("", "missing-private-path", str(self.bin), str(plain),
                         str(broken)):
            with self.subTest(override=override):
                self.env["MESHDB_SERVER_BINARY"] = override
                process = self.start("worker", "--bootstrap", "missing")
                status, out, err = self.finish(process)
                self.assertEqual(status, 1)
                self.assertEqual(out, "")
                self.assertIn("MESHDB_SERVER_BINARY", err)
                self.assertIn("PATH", err)
                self.assertLess(len(err), 300)
                self.assertNotIn("Traceback", err)
                self.assertNotIn("private-path", err)
                self.assertFalse(self.state.exists())
        self.env.pop("MESHDB_SERVER_BINARY", None)
        self.path_binary.unlink()
        self.assertEqual(self.finish(self.start("worker", "--bootstrap", "missing"))[0], 1)

    def test_sigterm_reaches_replacement_without_orphan(self):
        self.env["FIXTURE_WAIT"] = "1"
        process = self.start("worker", "--bootstrap", "missing")
        deadline = time.monotonic() + 5
        while not self.state.exists() and time.monotonic() < deadline:
            if process.poll() is not None:
                break
            time.sleep(0.01)
        self.assertTrue(self.state.exists(), "replacement did not start within five seconds")
        worker_pid = json.loads(self.state.read_text())["pid"]
        self.assertEqual(worker_pid, process.pid)
        process.terminate()
        self.assertEqual(self.finish(process), (-signal.SIGTERM, "", ""))
        with self.assertRaises(ProcessLookupError):
            os.kill(worker_pid, 0)

    def test_help_import_and_bad_arguments_are_inert_and_bounded(self):
        for args in (("--help",), ("worker", "--help")):
            status, out, err = self.finish(self.start(*args))
            self.assertEqual(status, 0)
            self.assertIn("worker", out)
            self.assertIn("--bootstrap", out)
            self.assertEqual(err, "")
        self.assertEqual(self.finish(self.start(code="import meshdb.cli, meshdb._binary")),
                         (0, "", ""))
        for args in ((), ("worker",), ("worker", "--bootstrap"),
                     ("worker", "--bootstrap", "missing", "--cloud"),
                     ("x" * 10000,)):
            status, out, err = self.finish(self.start(*args))
            self.assertEqual(status, 2)
            self.assertEqual(out, "")
            self.assertIn("usage:", err)
            self.assertIn("error:", err)
            self.assertLess(len(err), 1000)
        self.assertFalse(self.state.exists())


if __name__ == "__main__":
    unittest.main()

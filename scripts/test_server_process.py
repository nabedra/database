"""Actual local subprocess lifecycle guard. No dotenv reads or AWS calls."""

import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

from acceptance_support import AcceptanceConfig
from server_process import ServerProcess, ServerProcessError


class ProcessGuard(unittest.TestCase):
    def test_uncooperative_server_is_killed_reaped_and_reported_as_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'stubborn-server'
            binary.write_text(f'#!{sys.executable}\n' + textwrap.dedent('''\
                import signal
                import sys
                from http.server import BaseHTTPRequestHandler, HTTPServer

                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                class Health(BaseHTTPRequestHandler):
                    def do_GET(self):
                        self.send_response(200)
                        self.end_headers()
                    def log_message(self, *args):
                        pass

                port = int(sys.argv[sys.argv.index('--port') + 1])
                with HTTPServer(('127.0.0.1', port), Health) as server:
                    server.serve_forever()
                '''), encoding='utf-8')
            binary.chmod(0o700)
            config = AcceptanceConfig('meshdb-testing', 'us-east-1', {})
            namespace = config.allocate_prefix(2, 'stubborn', run_id='local')
            server = ServerProcess(config, namespace, binary=binary,
                                   startup_timeout=3, shutdown_timeout=0.2)
            try:
                server.start()
                pid = server.pid
                started = time.monotonic()
                with self.assertRaisesRegex(ServerProcessError, 'killed and reaped'):
                    server.stop()
                self.assertLess(time.monotonic() - started, 2)
                self.assertEqual(server.returncode, -signal.SIGKILL)
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
                self.assertEqual(server.stop(), -signal.SIGKILL)
            finally:
                # Reap the real child even when a shutdown regression fails a check.
                if server._process is not None and server._process.poll() is None:
                    server._process.kill()
                    server._process.wait(timeout=2)
                server.stop()

    def test_r04_unconfirmed_child_exit_cannot_authorize_a_replacement(self):
        config = AcceptanceConfig("meshdb-testing", "us-east-1", {})
        namespace = config.allocate_prefix(2, "unconfirmed", run_id="local")
        child = Mock()
        child.poll.return_value = None
        child.returncode = None
        child.wait.side_effect = subprocess.TimeoutExpired("fixture", .01)
        owner = ServerProcess(config, namespace, startup_timeout=.1, shutdown_timeout=.01)
        with patch('server_process.subprocess.Popen', return_value=child) as spawn, patch.object(owner, '_ready', return_value=True):
            owner.start()
            self.assertNotIn('--cold-start', spawn.call_args.args[0])
            self.assertNotIn('--migrate-catalog', spawn.call_args.args[0])
            with self.assertRaises(ServerProcessError):
                owner.stop()
            replacement = ServerProcess(config, namespace, startup_timeout=.1, shutdown_timeout=.01)
            with self.assertRaises(ServerProcessError):
                replacement.start()
            self.assertEqual(spawn.call_count, 1)

    def test_local_process_restart_and_failed_start_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "fixture-server"
            binary.write_text(f"#!{sys.executable}\n" + textwrap.dedent('''\
                import argparse
                import os
                import signal
                from http.server import BaseHTTPRequestHandler, HTTPServer

                parser = argparse.ArgumentParser()
                parser.add_argument("command", choices=["serve"])
                parser.add_argument("--host")
                parser.add_argument("--port", type=int)
                parser.add_argument("--workers", type=int)
                parser.add_argument("--queue-size", type=int)
                parser.add_argument("--partition_size")
                parser.add_argument("--cold-start", action="store_true")
                parser.add_argument("--migrate-catalog", action="store_true")
                args = parser.parse_args()
                assert not args.migrate_catalog
                if args.workers in (2, 3):
                    assert args.cold_start
                assert args.host == "127.0.0.1"
                assert args.queue_size == 64
                assert (args.workers, args.partition_size) in ((4, None), (2, "64 KB"), (3, "64 KB"))
                assert os.environ["NABEDRA_S3_BUCKET"] == "meshdb-testing"
                assert os.environ["NABEDRA_S3_PREFIX"].endswith("/")
                assert os.environ["AWS_ACCESS_KEY_ID"] == "synthetic-key"
                assert "AWS_ENDPOINT_URL" not in os.environ
                assert "AWS_PROFILE" not in os.environ
                assert os.environ["NABEDRA_TOKEN"]

                def terminate(signum, frame):
                    print("STOP", os.getpid(), flush=True)
                    raise SystemExit(0)

                signal.signal(signal.SIGTERM, terminate)
                class Health(BaseHTTPRequestHandler):
                    def do_GET(self):
                        ready = "never-ready" not in os.environ["NABEDRA_S3_PREFIX"]
                        self.send_response(200 if self.path == "/health/ready" and ready else 503)
                        self.end_headers()
                    def log_message(self, *args):
                        pass

                with HTTPServer((args.host, args.port), Health) as server:
                    print("START", os.getpid(), os.environ["NABEDRA_S3_PREFIX"], "COLD", args.cold_start, flush=True)
                    server.serve_forever()
                '''), encoding="utf-8")
            binary.chmod(0o700)
            config = AcceptanceConfig("meshdb-testing", "us-east-1", {
                "AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "synthetic-key",
                "AWS_SECRET_ACCESS_KEY": "synthetic-secret",
            })
            namespace = config.allocate_prefix(2, "lifecycle", run_id="local")
            log = root / "child.log"
            with patch.dict(os.environ, {"AWS_ENDPOINT_URL": "invalid", "AWS_PROFILE": "invalid",
                                         "NABEDRA_S3_BUCKET": "invalid", "NABEDRA_S3_PREFIX": "invalid"}):
                server = ServerProcess(config, namespace, binary=binary, log_path=log,
                                       startup_timeout=3, shutdown_timeout=1)
                for invalid in (0, -1, True, 1.5, "2"):
                    with self.assertRaises(ServerProcessError):
                        ServerProcess(config, namespace, workers=invalid)
                token = server.private_token
                with server:
                    first_pid = server.pid
                    self.assertTrue(server.api_url.startswith("http://127.0.0.1:"))
                    self.assertIsNone(server.returncode)
                    self.assertNotIn(token, repr(server))
                self.assertEqual(server.returncode, 0)
                with self.assertRaises(ProcessLookupError):
                    os.kill(first_pid, 0)
                with self.assertRaisesRegex(RuntimeError, "scenario failed"):
                    with server:
                        self.assertNotEqual(first_pid, server.pid)
                        self.assertEqual(token, server.private_token)
                        raise RuntimeError("scenario failed")
                self.assertEqual(server.returncode, 0)
                self.assertEqual(server.stop(), 0)

                failed_log = root / "failed.log"
                failed = ServerProcess(config, config.allocate_prefix(2, "never-ready"),
                                       binary=binary, log_path=failed_log,
                                       startup_timeout=0.8, shutdown_timeout=1)
                with self.assertRaises(ServerProcessError) as failure:
                    failed.start()
                self.assertNotIn(token, str(failure.exception))
                self.assertEqual(failed.returncode, 0)
                with self.assertRaises(ProcessLookupError):
                    os.kill(failed.pid, 0)
                self.assertIn(f"STOP {failed.pid}", failed_log.read_text())
                self.assertEqual(log.read_text().count(namespace.prefix), 2)
                self.assertEqual(log.read_text().count("STOP"), 2)
                launches = [line for line in log.read_text().splitlines() if line.startswith("START ")]
                self.assertTrue(launches[0].endswith("COLD False"))
                self.assertTrue(launches[1].endswith("COLD True"))

                for workers in (2, 3):
                    with ServerProcess(config, namespace, binary=binary, workers=workers,
                                       partition_size="64 KB", startup_timeout=3) as configured:
                        self.assertIsNone(configured.returncode)
                    self.assertEqual(configured.returncode, 0)

                # Incomplete TLS settings must fail before any child launches.
                with self.assertRaises(ServerProcessError):
                    ServerProcess(config, namespace, binary=binary, tls_cert=root / "cert.pem")


if __name__ == "__main__":
    unittest.main()

"""Loopback server lifecycle for acceptance scenarios; never loads configuration.

Use a config-allocated namespace and retain this object for stop/start cycles.
The private_token property is for ORM setup only: do not log or serialize it.
"""

import http.client
import math
import os
import socket
import ssl
import subprocess
import tempfile
import threading
import time
from pathlib import Path


class ServerProcessError(RuntimeError):
    """A sanitized launch, readiness, or shutdown failure."""


class ServerProcess:
    """Launch nabedra-database-server, wait for readiness, and reap it on context exit.

    Defaults match the acceptance launch contract. TLS requires all three paths;
    test_ca must trust the server certificate for the IP address 127.0.0.1.
    log_path appends output across restarts; otherwise output uses a private
    temporary file that closes on stop. Logs may contain sensitive server data.
    """

    _launch_lock = threading.Lock()

    def __init__(self, config, namespace, *, binary=None, startup_timeout=30,
                 shutdown_timeout=10, log_path=None, tls_cert=None, tls_key=None,
                 test_ca=None, workers=4, partition_size=None):
        if any(isinstance(value, bool) or not isinstance(value, (int, float))
               or not math.isfinite(value) or value <= 0
               for value in (startup_timeout, shutdown_timeout)):
            raise ServerProcessError("Startup and shutdown timeouts must be finite positive seconds.")
        if type(workers) is not int or workers <= 0:
            raise ServerProcessError("Workers must be a positive integer.")
        if partition_size is not None and (type(partition_size) is not str or not partition_size.strip()):
            raise ServerProcessError("Partition size must be a nonempty string.")
        self._worker_args = ["--workers", str(workers)]
        if partition_size is not None:
            self._worker_args.extend(["--partition_size", partition_size])
        self._env = config.server_env(namespace)
        self._namespace = namespace
        self._binary = Path(binary if binary is not None else
                            Path(__file__).resolve().parents[1] / "server/target/debug/nabedra-database-server").resolve()
        self._startup_timeout = startup_timeout
        self._shutdown_timeout = shutdown_timeout
        self._log_path = log_path
        self._log = None
        self._process = None
        self._tls_args = []
        self._ssl_context = None
        tls_paths = (tls_cert, tls_key, test_ca)
        if any(path is not None for path in tls_paths):
            if any(path is None for path in tls_paths):
                raise ServerProcessError("TLS requires a certificate, matching key, and test CA.")
            try:
                # Validate the pair locally without presenting a client cert to
                # the readiness endpoint. Verification stays enabled there.
                pair = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                pair.load_cert_chain(tls_cert, tls_key, password=lambda: "")
                self._ssl_context = ssl.create_default_context(cafile=test_ca)
                self._tls_args = ["--tls-cert", str(Path(tls_cert).resolve()),
                                  "--tls-key", str(Path(tls_key).resolve())]
            except (OSError, ValueError, ssl.SSLError):
                raise ServerProcessError("Cannot load TLS certificate, matching key, or test CA.") from None
        try:
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                self._port = reservation.getsockname()[1]
        except OSError:
            raise ServerProcessError("Cannot allocate a loopback port.") from None

    @property
    def api_url(self):
        scheme = "https" if self._ssl_context is not None else "http"
        return f"{scheme}://127.0.0.1:{self._port}"

    @property
    def private_token(self):
        """Bearer token for ORM setup. Keep this value out of diagnostics."""
        return self._env["NABEDRA_TOKEN"]

    @property
    def pid(self):
        return self._process.pid if self._process is not None else None

    @property
    def returncode(self):
        return self._process.poll() if self._process is not None else None

    def start(self):
        """Start after readiness; a proven prior child exit permits a cold run.

        The allocated namespace retains that exact process receipt across helper
        instances. This never acknowledges migration of an unknown old catalog.
        """
        if self._process is not None and self._process.poll() is None:
            raise ServerProcessError("Server is already running.")
        # Also close a previous launch's artifact if it exited between calls.
        self.stop()
        deadline = time.monotonic() + self._startup_timeout
        try:
            if self._log_path is None:
                self._log = tempfile.TemporaryFile(mode="w+b")
            else:
                fd = os.open(self._log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
                self._log = os.fdopen(fd, "ab")
            with self._launch_lock:
                previous = self._namespace._server_process
                if previous is not None and previous.poll() is None:
                    raise ServerProcessError("The allocated namespace still has an unexited owned server.")
                # poll() is an actual exit observation on the original owned
                # child. A fresh namespace never receives this acknowledgement.
                restart = ["--cold-start"] if previous is not None else []
                self._process = subprocess.Popen(
                    [str(self._binary), "serve", "--host", "127.0.0.1", "--port", str(self._port),
                     *self._worker_args, "--queue-size", "64", *self._tls_args, *restart],
                    env=self._env, stdin=subprocess.DEVNULL, stdout=self._log,
                    stderr=subprocess.STDOUT, close_fds=True,
                )
                object.__setattr__(self._namespace, "_server_process", self._process)
            while self._process.poll() is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                if self._ready(min(0.25, remaining)) and self._process.poll() is None:
                    return self
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
            raise ServerProcessError("Server exited or did not become ready before the startup deadline.")
        except BaseException as failure:
            self.stop()
            if isinstance(failure, ServerProcessError):
                raise
            if isinstance(failure, Exception):
                raise ServerProcessError("Server startup failed; check the private local artifact if supplied.") from None
            raise

    def _ready(self, timeout):
        if self._ssl_context is None:
            connection = http.client.HTTPConnection("127.0.0.1", self._port, timeout=timeout)
        else:
            connection = http.client.HTTPSConnection("127.0.0.1", self._port, timeout=timeout,
                                                     context=self._ssl_context)
        try:
            connection.request("GET", "/health/ready")
            with connection.getresponse() as response:
                return response.status == 200
        except (OSError, http.client.HTTPException):
            return False
        finally:
            connection.close()

    def stop(self):
        """SIGTERM and wait; kill and reap only after the shutdown timeout.

        Return the observed exit code. A forced kill raises ServerProcessError
        after reaping so acceptance callers cannot mistake it for graceful exit.
        """
        forced = False
        try:
            if self._process is not None:
                if self._process.poll() is None:
                    self._process.terminate()
                try:
                    self._process.wait(timeout=self._shutdown_timeout)
                except subprocess.TimeoutExpired:
                    forced = True
                    self._process.kill()
                    self._process.wait(timeout=self._shutdown_timeout)
        except (OSError, subprocess.SubprocessError):
            raise ServerProcessError("Could not observe server exit within the shutdown deadline.") from None
        finally:
            if self._log is not None:
                self._log.close()
                self._log = None
        if forced:
            raise ServerProcessError("Server exceeded the graceful shutdown deadline; killed and reaped.")
        return self.returncode

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

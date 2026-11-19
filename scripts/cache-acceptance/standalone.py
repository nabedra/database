#!/usr/bin/env python3
"""Standalone Docker acceptance. Records assertions and observed process RSS."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import traceback


MIB = 1 << 20
IO_TIMEOUT = 4
FIXTURES = Path(__file__).resolve().parent / "fixtures"
ORM_CONTRACT = FIXTURES.parents[2] / "orm" / "cache-contract"
COMMANDS = ["get", "set", "del", "exists", "mget", "ttl", "pttl", "expire", "pexpire", "persist", "incrby"]


class RespError(Exception):
    pass


def wire(*words) -> bytes:
    parts = [value if isinstance(value, bytes) else str(value).encode() for value in words]
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n" % len(part) + part + b"\r\n" for part in parts)


class Raw:
    def __init__(self, conn):
        self.conn = conn
        self.input = conn.makefile("rb")

    def send(self, data):
        self.conn.sendall(data)

    def read(self):
        line = self.input.readline(8192)
        if not line:
            raise EOFError("connection closed")
        assert line.endswith(b"\r\n"), line
        prefix, data = line[:1], line[1:-2]
        match prefix:
            case b"+":
                return data
            case b"-":
                raise RespError(data.decode())
            case b":":
                return int(data)
            case b"$":
                size = int(data)
                if size == -1:
                    return None
                assert 0 <= size <= 4 * MIB, size
                payload = self.input.read(size)
                assert len(payload) == size and self.input.read(2) == b"\r\n"
                return payload
            case b"*":
                length = int(data)
                if length == -1:
                    return None
                assert 0 <= length <= 16384, length
                return [self.read() for _ in range(length)]
            case _:
                raise AssertionError(line)

    def command(self, *words):
        self.send(wire(*words))
        return self.read()

    def close(self):
        self.input.close()
        self.conn.close()


@dataclass
class Server:
    engine: str
    port: int
    process: subprocess.Popen
    ca_file: Path | None

    @property
    def settings(self):
        config = {"port": self.port, "endpoint": f"redis://127.0.0.1:{self.port}"}
        if self.ca_file:
            config.update({"caFile": str(self.ca_file), "username": "default", "password": "cache-test",
                           "endpoint": f"rediss://localhost:{self.port}"})
        return config

    @contextmanager
    def connect(self, *, auth=True, hostname="localhost", recv_buffer=None):
        conn = socket.socket()
        if recv_buffer:
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buffer)
        conn.settimeout(IO_TIMEOUT)
        raw = None
        try:
            conn.connect(("127.0.0.1", self.port))
            if self.ca_file:
                context = ssl.create_default_context(cafile=str(self.ca_file))
                conn = context.wrap_socket(conn, server_hostname=hostname)
            raw = Raw(conn)
            if self.ca_file and auth:
                equal(raw.command("AUTH", "default", "cache-test"), b"OK")
            yield raw
        finally:
            if raw:
                raw.close()
            else:
                conn.close()


def equal(actual, expected):
    assert actual == expected, (str(actual)[:160], str(expected)[:160])


def failure(operation, contains, error_type=RespError):
    try:
        operation()
    except error_type as error:
        assert contains in str(error), str(error)
    else:
        raise AssertionError(f"expected {error_type.__name__} containing {contains!r}")


def certs(folder):
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
                    "-keyout", str(folder / "key.pem"), "-out", str(folder / "cert.pem")],
                   check=True, capture_output=True, timeout=15)


def available_port():
    with socket.socket() as conn:
        conn.bind(("127.0.0.1", 0))
        return conn.getsockname()[1]


def native_config(folder, port, secure, profile, limits):
    base = {
        "memory_bytes": 64 * MIB, "data_bytes": 8 * MIB, "max_connections": 8, "max_handshakes": 4,
        "max_pending": 4, "handshake_timeout_ms": 1000, "io_timeout_ms": 10_000,
        **limits,
    }
    lines = [f'listen = "127.0.0.1:{port}"', f'eviction = "{profile}"']
    if secure:
        lines.extend(["[tls]", 'certificate = "cert.pem"', 'private_key = "key.pem"'])
        for user, prefix, commands, count in [("default", "nb:catalog:", COMMANDS, 3),
                                             ("reader", "nb:catalog:", ["get"], 1)]:
            lines.extend(["[[users]]", f"username = {json.dumps(user)}", 'password = "cache-test"',
                          f"key_prefixes = {json.dumps([prefix])}", f"commands = {json.dumps(commands)}",
                          f"max_connections = {count}"])
    else:
        lines.append("plaintext_loopback = true")
    lines.append("[limits]")
    lines.extend(f"{key} = {value}" for key, value in base.items())
    config = folder / "cache.toml"
    config.write_text("\n".join(lines) + "\n")
    return config


def status_kib(pid, key):
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith(key + ":"):
            return int(line.split()[1])
    raise AssertionError(f"{key} unavailable")


@contextmanager
def serve(args, result, engine="nabedra", *, secure=False, profile="noeviction", **limits):
    with tempfile.TemporaryDirectory() as temp:
        folder = Path(temp)
        port = available_port()
        if secure:
            certs(folder)
        if engine == "nabedra":
            config = native_config(folder, port, secure, profile, limits)
            command = [args.nabedra, "serve", "--config", str(config)]
        else:
            command = [args.valkey, "--bind", "127.0.0.1", "--port", str(0 if secure else port),
                       "--dir", temp, "--save", "", "--appendonly", "no",
                       "--maxmemory", str(limits.get("data_bytes", 8 * MIB)), "--maxmemory-policy",
                       {"noeviction": "noeviction", "sampled_lru": "allkeys-lru"}[profile]]
            if secure:
                command.extend(["--tls-port", str(port), "--tls-auth-clients", "no",
                                "--tls-cert-file", str(folder / "cert.pem"), "--tls-key-file", str(folder / "key.pem"),
                                "--tls-ca-cert-file", str(folder / "cert.pem"), "--requirepass", "cache-test"])
        with (folder / "server.log").open("w+") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log)
            server = Server(engine, port, process, folder / "cert.pem" if secure else None)
            observation = {"name": engine, "tls": secure, "profile": profile, "limit_overrides": limits}
            result["engines"].append(observation)
            try:
                deadline = time.monotonic() + 8
                while True:
                    assert process.poll() is None, "cache process exited during startup"
                    try:
                        with server.connect() as raw:
                            equal(raw.command("PING"), b"PONG")
                            equal(raw.command("QUIT"), b"OK")
                            failure(raw.read, "connection closed", EOFError)
                        break
                    except (ConnectionRefusedError, ConnectionResetError, EOFError):
                        assert time.monotonic() < deadline, "cache readiness timed out"
                        time.sleep(0.025)
                yield server
                peak = status_kib(process.pid, "VmHWM")
                observation["peak_rss_kib"] = peak
                assert peak < 192 * 1024, ("process RSS consumed Docker headroom", peak)
            except BaseException:
                log.seek(0)
                print(log.read(), file=sys.stderr)
                raise
            finally:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
                if engine == "nabedra":
                    log.seek(0)
                    for line in log:
                        if "cache actor stopped with " in line:
                            observation["charged_data_bytes_at_shutdown"] = int(line.split()[-3])


def fixture_settings(server):
    return {key: value for key, value in server.settings.items() if key != "endpoint"}


def stock_redis_py(server):
    import redis
    equal(redis.__version__, "6.4.0")
    config = fixture_settings(server)
    options = {}
    if "caFile" in config:
        options.update(ssl=True, ssl_ca_certs=config["caFile"], ssl_check_hostname=True,
                       username=config["username"], password=config["password"])
    client = redis.Redis(host="localhost" if server.ca_file else "127.0.0.1", port=server.port,
                         socket_timeout=IO_TIMEOUT, protocol=2, decode_responses=False, **options)
    vectors = json.loads((FIXTURES / "standalone_stock.json").read_text())
    with client:
        for case in vectors:
            command, *parts = case["command"]
            parts = [bytes.fromhex(part["hex"]) if isinstance(part, dict) else part for part in parts]
            expected = case["reply"]
            execute = lambda: client.execute_command(command, *parts)
            if isinstance(expected, dict) and "error" in expected:
                failure(execute, expected["error"], redis.ResponseError)
                continue
            actual = execute()
            # redis-py applies command-specific reply callbacks.
            if command in {"PING", "SET", "PEXPIRE"} and actual is True:
                actual = {"PING": b"PONG", "SET": b"OK", "PEXPIRE": 1}[command]
            compare(actual, expected)
        hello = client.execute_command("HELLO", "2")
        equal(hello[hello.index(b"proto") + 1], 2)


def compare(actual, expected):
    if isinstance(expected, list):
        assert len(actual) == len(expected)
        for value, expected_value in zip(actual, expected):
            compare(value, expected_value)
    elif isinstance(expected, dict):
        if "range" in expected:
            assert expected["range"][0] <= actual <= expected["range"][1], (actual, expected)
        elif "integer" in expected:
            equal(actual, int(expected["integer"]))
        elif "hex" in expected:
            equal(actual, bytes.fromhex(expected["hex"]))
        else:
            equal(actual, expected["simple"].encode())
    else:
        equal(actual, expected)


def clients(args, server):
    # Start each stock vector replay with a separate database state by deleting
    # only this fixture's keys. The test user cannot flush the cache.
    with server.connect() as raw:
        def clear():
            raw.command("DEL", *(f"nb:catalog:{{stock}}:{name}" for name in ("missing", "empty", "value", "counter")))
        clear()
        stock_redis_py(server)
        clear()
    with tempfile.TemporaryDirectory() as temp:
        folder = Path(temp)
        (folder / "node_modules").symlink_to(args.node_modules)
        for name in ("standalone_stock.mjs", "standalone_stock.json"):
            shutil.copy(FIXTURES / name, folder)
        for name in ("typescript_driver.mjs", "vectors.json"):
            shutil.copy(ORM_CONTRACT / name, folder)
        settings = json.dumps(fixture_settings(server))
        subprocess.run([args.node, str(folder / "standalone_stock.mjs")], check=True, timeout=30,
                       env={**os.environ, "NABEDRA_STOCK_CONFIG": settings})
        env = {**os.environ, "NABEDRA_CACHE_TEST_CONFIG": json.dumps(server.settings)}
        subprocess.run([sys.executable, str(ORM_CONTRACT / "python_driver.py")], check=True, timeout=40, env=env)
        subprocess.run([args.node, str(folder / "typescript_driver.mjs")], check=True, timeout=40, env=env)


def tls_acl(args, result):
    with serve(args, result, secure=True) as server:
        failure(lambda: connect_once(server, "wrong.local"), "Hostname mismatch", ssl.SSLCertVerificationError)
        with server.connect(auth=False) as first, server.connect(auth=False) as second:
            failure(lambda: first.command("GET", "nb:catalog:present"), "NOAUTH")
            failure(lambda: first.command("AUTH", "bogus", "cache-test"), "WRONGPASS")
            equal(first.command("AUTH", "reader", "cache-test"), b"OK")
            equal(first.command("AUTH", "reader", "cache-test"), b"OK")
            failure(lambda: second.command("AUTH", "reader", "cache-test"), "BUSY")
            failure(lambda: first.command("SET", "nb:catalog:present", "blocked"), "NOPERM")
            equal(second.command("AUTH", "default", "cache-test"), b"OK")
            equal(second.command("SET", "nb:catalog:present", b"\x00\xff"), b"OK")
            failure(lambda: second.command("DEL", "nb:catalog:present", "nb:other:private"), "NOPERM")
            equal(first.command("GET", "nb:catalog:present"), b"\x00\xff")
            # Valkey-compatible connection metadata and RESP refusal.
            hello = first.command("HELLO", 2)
            equal(hello[hello.index(b"proto") + 1], 2)
            equal(first.command("CLIENT", "SETINFO", "LIB-NAME", "fixture"), b"OK")
            failure(lambda: first.command("HELLO", 3), "NOPROTO")
            assert first.command("COMMAND", "INFO", "GET")[0][0] == b"get"


def connect_once(server, hostname):
    with server.connect(hostname=hostname):
        pass


def frames_and_replies(args, result):
    with serve(args, result, max_reply_bytes=700, max_arguments=16) as server:
        value = bytes(range(256)) * 2
        with server.connect() as healthy, server.connect() as slow:
            request = wire("SET", "linear", value)
            for index in range(11):
                slow.send(request[index:index + 1])
            equal(healthy.command("PING"), b"PONG")
            slow.send(request[11:] + wire("SET", "large", value) + wire("GET", "linear"))
            equal([slow.read(), slow.read(), slow.read()], [b"OK", b"OK", value])
            failure(lambda: healthy.command("MGET", "linear", "large"), "response exceeds")
            equal(healthy.command("GET", "large"), value)
            equal(healthy.command("SET", b"k" * 512, "a"), b"OK")
            failure(lambda: healthy.command("SET", b"k" * 513, "a"), "512")
            failure(lambda: healthy.command("SET", "linear", "bad", "NX", "XX"), "syntax")
            equal(healthy.command("GET", "linear"), value)
            failure(lambda: healthy.command("COMMAND", "INFO", *(["GET"] * 14)), "response exceeds")
            equal(healthy.command("PING"), b"PONG")
        cases = [(b"*17\r\n", "too many arguments"),
                 (b"*1\r\n$1048577\r\n", "bulk argument too large"),
                 (b"*3\r\n$3\r\nSET\r\n$6\r\nlinear\r\n$5\r\nwrongXX", "terminator"),
                 (b"*3\r\n$3\r\nSET\r\n$6\r\nlinear\r\n$5\r\nwr", "truncated")]
        for request, error in cases:
            with server.connect() as raw:
                raw.send(request)
                raw.conn.shutdown(socket.SHUT_WR)
                failure(raw.read, error)
                failure(raw.read, "connection closed", EOFError)
        with server.connect() as raw:
            equal(raw.command("GET", "linear"), value)
    with serve(args, result, max_frame_bytes=4096, max_pipeline_bytes=8192) as server:
        with server.connect() as raw:
            raw.send(b"*3\r\n$3\r\nSET\r\n$5\r\nframe\r\n$4096\r\n")
            failure(raw.read, "request frame too large")
        with server.connect() as raw:
            equal(raw.command("GET", "frame"), None)


def admission(args, result):
    with serve(args, result, max_connections=3, max_handshakes=2, max_pending=1) as server:
        with server.connect() as one, server.connect() as two, server.connect() as three:
            for raw in (one, two, three):
                equal(raw.command("PING"), b"PONG")
            with server.connect() as overflow:
                try:
                    overflow.read()
                except RespError as error:
                    assert str(error).startswith("BUSY"), str(error)
                except EOFError:
                    pass
                else:
                    raise AssertionError("connection cap admitted an extra session")
            equal(one.command("SET", "still-active", b"yes"), b"OK")
            equal(three.command("GET", "still-active"), b"yes")
    with serve(args, result, secure=True, max_handshakes=1) as server:
        with server.connect() as active, socket.create_connection(("127.0.0.1", server.port), timeout=IO_TIMEOUT) as stalled:
            stalled.sendall(b"\x16\x03")
            try:
                with server.connect() as extra:
                    extra.command("PING")
            except (ssl.SSLError, ConnectionResetError, BrokenPipeError, EOFError):
                pass
            else:
                raise AssertionError("handshake cap admitted parallel handshake")
            equal(active.command("PING"), b"PONG")
        deadline = time.monotonic() + 3
        while True:
            try:
                with server.connect() as next_session:
                    equal(next_session.command("PING"), b"PONG")
                    break
            except (ssl.SSLError, ConnectionResetError, BrokenPipeError, EOFError):
                assert time.monotonic() < deadline, "handshake capacity was not restored"
                time.sleep(0.025)


def noeviction_fill(args, result):
    contents = b"x" * (16 * 1024)
    with serve(args, result, data_bytes=128 * 1024) as server:
        with server.connect() as raw:
            stored = []
            for index in range(16):
                key = f"existing:{index}"
                try:
                    equal(raw.command("SET", key, contents, "PX", 30_000), b"OK")
                except RespError as error:
                    assert str(error).startswith("OOM"), str(error)
                    equal(raw.command("GET", key), None)
                    break
                stored.append(key)
            else:
                raise AssertionError("noeviction exceeded configured slot bytes")
            assert len(stored) >= 4
            failure(lambda: raw.command("SET", stored[0], b"y" * (128 * 1024)), "OOM")
            assert 1 <= raw.command("PTTL", stored[0]) <= 30_000
            for key in stored:
                equal(raw.command("GET", key), contents)
        count = len(stored)
    with serve(args, result, profile="sampled_lru", data_bytes=128 * 1024) as server:
        with server.connect() as raw:
            for key in stored:
                equal(raw.command("SET", key, contents), b"OK")
            equal(raw.command("GET", stored[0]), contents)
            equal(raw.command("SET", "incoming", contents), b"OK")
            equal(raw.command("GET", "incoming"), contents)
            equal(raw.command("GET", stored[0]), contents)
            equal(raw.command("GET", stored[1]), None)
            equal(raw.command("EXISTS", *stored), count - 1)
    # Valkey's maxmemory includes baseline process structures. Compare the
    # policy observable by callers without requiring the same victim.
    with serve(args, result, "valkey", profile="sampled_lru", data_bytes=2 * MIB) as server:
        with server.connect() as raw:
            for index in range(96):
                equal(raw.command("SET", f"reference:{index}", b"v" * (64 * 1024)), b"OK")
            equal(raw.command("GET", "reference:95"), b"v" * (64 * 1024))
            assert raw.command("EXISTS", *(f"reference:{i}" for i in range(64))) < 64


def expiry_and_overwrite(args, result):
    with serve(args, result, data_bytes=128 * 1024) as server:
        with server.connect() as first, server.connect() as second:
            equal(first.command("SET", "overwritten", b"old", "PX", 1000), b"OK")
            equal(second.command("SET", "overwritten", b"new"), b"OK")
            equal(first.command("SET", "deleted", b"old", "PX", 1000), b"OK")
            equal(second.command("DEL", "deleted"), 1)
            keys = []
            for index in range(768):
                key = f"expires:{index}"
                try:
                    equal(first.command("SET", key, b"x" * 384, "PX", 1000), b"OK")
                except RespError as error:
                    assert str(error).startswith("OOM"), str(error)
                    break
                keys.append(key)
            else:
                raise AssertionError("burst exceeded configured slot bytes")
            assert len(keys) >= 128, len(keys)
            equal(second.command("SET", "linear", "1"), b"OK")
            deadline = time.monotonic() + 3
            while True:
                try:
                    equal(second.command("SET", "replacement", b"r" * (32 * 1024)), b"OK")
                    break
                except RespError as error:
                    assert str(error).startswith("OOM"), str(error)
                    assert time.monotonic() < deadline, "active expiry did not reclaim capacity"
                    equal(second.command("GET", "linear"), b"1")
                    time.sleep(0.025)
            for start in range(0, len(keys), 64):
                part = keys[start:start + 64]
                while second.command("EXISTS", *part):
                    assert time.monotonic() < deadline, "expired entries remained visible"
                    time.sleep(0.025)
                equal(second.command("MGET", *part), [None] * len(part))
            equal(first.command("GET", "overwritten"), b"new")
            equal(first.command("PTTL", "overwritten"), -1)
            equal(first.command("GET", "deleted"), None)


def stalled_and_churn(args, result):
    with serve(args, result, profile="sampled_lru", data_bytes=4 * MIB) as server:
        value = bytes(range(256)) * 4096
        with server.connect() as producer, server.connect(recv_buffer=4096) as stalled:
            equal(producer.command("SET", "blocked", value), b"OK")
            stalled.send(wire("GET", "blocked") * 64)
            assert stalled.input.readline(16) == b"$1048576\r\n"
            # No reads follow on that socket while independent sessions churn.
            gate = threading.Barrier(4, timeout=4)

            def churn(lane):
                with server.connect() as raw:
                    gate.wait()
                    for step in range(128):
                        key = f"lane:{lane}:{step % 16}"
                        payload = bytes([lane, step % 256]) * 16_384
                        equal(raw.command("SET", key, payload, "PX", 2000), b"OK")
                        equal(raw.command("GET", key), payload)
                        if step % 8 == 0:
                            equal(raw.command("DEL", key), 1)

            with ThreadPoolExecutor(max_workers=3) as pool:
                pending = [pool.submit(churn, lane) for lane in range(3)]
                gate.wait()
                equal(producer.command("SET", "independent", b"ok"), b"OK")
                equal(producer.command("GET", "independent"), b"ok")
                for job in pending:
                    job.result(timeout=8)


def interoperability(args, result, engine):
    with serve(args, result, engine, secure=True) as server:
        clients(args, server)


def sha256(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nabedra", required=True)
    parser.add_argument("--valkey", required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--node-modules", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    tests = {
        "K01-nabedra-stock-and-installed-orm": lambda args, result: interoperability(args, result, "nabedra"),
        "K01-valkey-stock-and-installed-orm-ab": lambda args, result: interoperability(args, result, "valkey"),
        "K01-tls-identity-command-and-prefix-acl": tls_acl,
        "K01-K02-frame-pipeline-and-output-caps": frames_and_replies,
        "K02-connection-and-handshake-admission": admission,
        "K01-K02-noeviction-and-sampled-lru": noeviction_fill,
        "K01-K02-burst-expiry-overwrite-delete": expiry_and_overwrite,
        "K02-unread-output-independent-session-and-rss": stalled_and_churn,
    }
    result = {"environment": {"arch": platform.machine(), "os": platform.platform(),
                              "cpus": sorted(os.sched_getaffinity(0)),
                              "memory_max": Path("/sys/fs/cgroup/memory.max").read_text().strip(),
                              "nabedra": subprocess.check_output([args.nabedra, "--version"], text=True).strip(),
                              "nabedra_sha256": sha256(args.nabedra),
                              "valkey": subprocess.check_output([args.valkey, "--version"], text=True).strip(),
                              "valkey_sha256": sha256(args.valkey),
                              "redis_py": version("redis"),
                              "node": subprocess.check_output([args.node, "--version"], text=True).strip()},
              "engines": [], "assertion_groups": []}
    args.output.mkdir(parents=True, exist_ok=True)
    overall = True
    for name, run in tests.items():
        started = time.monotonic()
        status = {"id": name}
        try:
            run(args, result)
            status["status"] = "passed"
        except Exception as error:
            traceback.print_exc()
            overall = False
            status.update(status="failed", error=f"{type(error).__name__}: {error}")
        status["duration_seconds"] = round(time.monotonic() - started, 3)
        result["assertion_groups"].append(status)
        print(json.dumps(status), flush=True)
    result["unverified"] = ["deterministic actor-queue saturation", "cluster replication/journal/snapshot caps",
                            "cluster routing and failover", "K04a", "K04b", "K05"]
    (args.output / "standalone.json").write_text(json.dumps(result, indent=2) + "\n")
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())

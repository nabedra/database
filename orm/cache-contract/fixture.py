"""Deterministic local RESP2 peer for errors a healthy cache cannot emit."""
from __future__ import annotations

import base64
from binascii import crc_hqx
from contextlib import ExitStack
import json
import os
from pathlib import Path
import socketserver
import ssl
import subprocess
import sys
from threading import Event, Lock, Thread


def message(value):
    if isinstance(value, int):
        return f":{value}\r\n".encode()
    if isinstance(value, bytes):
        return f"${len(value)}\r\n".encode() + value + b"\r\n"
    if isinstance(value, list):
        return f"*{len(value)}\r\n".encode() + b"".join(map(message, value))
    raise TypeError(value)


class Fixture(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, role, state, tls=None):
        self.role = role
        self.state = state
        self.tls = tls
        super().__init__(("127.0.0.1", 0), Handler)

    def get_request(self):
        conn, address = super().get_request()
        if self.tls:
            try:
                conn = self.tls.wrap_socket(conn, server_side=True)
            except OSError:
                conn.close()
                raise
        return conn, address

    def serve(self):
        thread = Thread(target=self.serve_forever, daemon=True)
        thread.start()
        return thread

    @property
    def port(self):
        return self.server_address[1]

    @property
    def endpoint(self):
        return f"{'rediss' if self.tls else 'redis'}://127.0.0.1:{self.port}"


ERRORS = {
    "auth": b"-NOPERM rejected\r\n",
    "overload": b"-BUSY rejected\r\n",
    "evicted": b"-OOM rejected\r\n",
    "frozen": b"-TRYAGAIN frozen\r\n",
    "offline": b"-CLUSTERDOWN unavailable\r\n",
    "invalid": b"-ERR rejected\r\n",
}


class Handler(socketserver.StreamRequestHandler):
    def observe_setup_close(self, phase):
        self.connection.settimeout(8)
        state = self.server.state
        if phase == "tls" and self.rfile.read(1) != b"\x16":
            return
        state[f"{phase}-entered"].set()
        # TLS clients can already have sent the rest of ClientHello. An
        # authenticating client must send only AUTH before the peer closes.
        if phase == "tls":
            while self.rfile.read(4096):
                pass
        elif self.rfile.read(1):
            return
        state[f"{phase}-closed"].set()

    def handle(self):
        authorized = False
        try:
            if self.server.role == "tls-stalled":
                self.observe_setup_close("tls")
                return
            while header := self.rfile.readline(64):
                if not header.startswith(b"*"):
                    return
                count = int(header[1:])
                if not 1 <= count <= 7:
                    return
                parts = []
                for _ in range(count):
                    size = int(self.rfile.readline(64)[1:])
                    if not 0 <= size <= 1024 * 1024:
                        return
                    data = self.rfile.read(size)
                    assert self.rfile.read(2) == b"\r\n"
                    parts.append(data)
                if parts[0] == b"AUTH":
                    authorized = parts[1:] == [b"test", b"secret"]
                    if authorized and self.server.role == "auth-stalled":
                        self.observe_setup_close("auth")
                        return
                    answer = b"+OK\r\n" if authorized else b"-WRONGPASS rejected\r\n"
                elif parts[:3] == [b"HELLO", b"2", b"AUTH"]:
                    authorized = parts[3:] == [b"test", b"secret"]
                    answer = (message([b"server", b"fixture", b"version", b"1", b"proto", 2,
                                       b"id", 1, b"mode", b"standalone", b"role", b"master", b"modules", []])
                              if authorized else b"-WRONGPASS rejected\r\n")
                elif not authorized:
                    answer = b"-NOAUTH rejected\r\n"
                else:
                    answer = self.dispatch(parts)
                    if answer is None:
                        return
                self.wfile.write(answer)
        except (OSError, ValueError):
            return

    def dispatch(self, parts):
        if parts == [b"PING"]:
            return b"+PONG\r\n"
        if parts[:2] == [b"CLIENT", b"SETINFO"]:
            return b"+OK\r\n"
        if parts == [b"CLUSTER", b"SLOTS"]:
            # Always advertises the seed, so the client must honor the newer MOVED.
            host = b"localhost" if self.server.tls else b"127.0.0.1"
            return message([[0, 16383, [host, self.server.port, b"seed"]]])
        command, encoded, *args = parts
        assert encoded.startswith(b"nb:catalog:"), encoded
        key = base64.urlsafe_b64decode(encoded.split(b":", 2)[-1] + b"==").decode()
        state = self.server.state
        if self.server.role == "seed" and self.server.tls and key in {"close-auth", "close-tls"}:
            slot = crc_hqx(encoded, 0) % 16384
            return f"-MOVED {slot} localhost:{state[key]}\r\n".encode()
        if self.server.role == "seed" and key in {"move", "wrong-slot", "untrusted"}:
            slot = crc_hqx(encoded, 0) % 16384
            if key == "wrong-slot":
                slot = (slot + 1) % 16384
            if key == "move":
                with state["lock"]:
                    state["seed_redirects"] += 1
            port = state["target"] if key != "untrusted" else 1
            return f"-MOVED {slot} 127.0.0.1:{port}\r\n".encode()
        if command == b"SET" and key in ERRORS:
            return ERRORS[key]
        if command == b"INCRBY":
            with state["lock"]:
                state["data"][key] = total = state["data"].get(key, 0) + int(args[0])
                if key == "move":
                    state["target_writes"] += 1
            if key == "deadline":
                Event().wait(2)
            if key in {"drop", "deadline"}:
                return None
            if key == "garbled":
                return b"?bad\r\n"
            return message(total)
        if command == b"GET":
            if key in {"auth-entered", "auth-closed", "tls-entered", "tls-closed"}:
                return message(b"1" if state[key].is_set() else b"0")
            if key == "badframe":
                return b"$1048577\r\n"
            if key == "stall":
                Event().wait(2)
                return None
            if key == "held":
                state["entered"].set()
                assert state["release"].wait(4)
                return message(b"released")
            if key == "held-entered":
                return message(b"1" if state["entered"].is_set() else b"0")
            if key == "held-release":
                state["release"].set()
                return message(b"1")
            with state["lock"]:
                if key == "move-count":
                    return message(str(state["target_writes"]).encode())
                if key == "move-seed-count":
                    return message(str(state["seed_redirects"]).encode())
                if key in state["data"]:
                    return message(str(state["data"][key]).encode())
            return message(encoded)
        return b"-ERR unexpected command\r\n"


def run(command, config):
    state = {"lock": Lock(), "data": {}, "seed_redirects": 0, "target_writes": 0,
             "entered": Event(), "release": Event(),
             **{f"{phase}-{event}": Event() for phase in ("auth", "tls") for event in ("entered", "closed")}}
    with ExitStack() as cleanup:
        cert = Path(__file__).parent / "fixture.crt"
        key = Path(__file__).parent / "fixture.key"
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        stand = cleanup.enter_context(Fixture("standalone", state))
        tls = cleanup.enter_context(Fixture("seed", state, context))
        seed = cleanup.enter_context(Fixture("seed", state))
        target = cleanup.enter_context(Fixture("target", state))
        auth_stalled = cleanup.enter_context(Fixture("auth-stalled", state, context))
        tls_stalled = cleanup.enter_context(Fixture("tls-stalled", state))
        state["target"] = target.port
        state["close-auth"], state["close-tls"] = auth_stalled.port, tls_stalled.port
        for server in (stand, tls, seed, target, auth_stalled, tls_stalled):
            thread = server.serve()
            cleanup.callback(thread.join, 2)
            cleanup.callback(server.shutdown)
        config.update(fixture=stand.endpoint, seed=seed.endpoint, target=target.endpoint,
                      tls=tls.endpoint.replace("127.0.0.1", "localhost"), badTls=tls.endpoint,
                      closeAuth=f"rediss://localhost:{auth_stalled.port}",
                      closeTls=f"rediss://localhost:{tls_stalled.port}",
                      fixtureCa=str(cert))
        subprocess.run(command, check=True, timeout=45,
                       env={**os.environ, "NABEDRA_CACHE_TEST_CONFIG": json.dumps(config)})


if __name__ == "__main__":
    run(sys.argv[1:], json.loads(os.environ.get("NABEDRA_CACHE_TEST_CONFIG", "{}")))

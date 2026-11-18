"""Docker-only fault observer that stalls a snapshot after forwarding its first bytes."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import socket
import threading
import time


TARGET = (os.environ["EVENTS_GATE_TARGET"], 8081)
BOOTSTRAP_GROUP = os.environ["EVENTS_GATE_GROUP"]
STATE = Path("/gate")
HELD_BYTES = 128 * 1024
BLOCK = 16 * 1024
MAX_BODY = 24 * 1024 * 1024
COPY_SLOT = threading.Semaphore(1)


class Gate(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        # Keep this observer private to the fixture network and cap every
        # request. It relays routine control/Raft HTTP to the same broker.
        self.connection.settimeout(10)
        snapshot = self.path == f"/internal/raft/v1/{BOOTSTRAP_GROUP}/snapshot"
        try:
            with socket.create_connection(TARGET, timeout=3) as outgoing:
                outgoing.settimeout(20)
                headers = [
                    f"{key}: {value}" for key, value in self.headers.items()
                    if key.lower() not in {"connection", "proxy-connection"}
                ]
                request = f"POST {self.path} HTTP/1.1\r\n"
                request += "\r\n".join(headers + ["Connection: close", "", ""])
                outgoing.sendall(request.encode("iso-8859-1"))
                if not self.relay_body(outgoing, snapshot):
                    self.send_error(503)
                    return
                self.close_connection = True
                while data := outgoing.recv(BLOCK):
                    self.connection.sendall(data)
        except (OSError, ValueError):
            self.close_connection = True

    def relay_body(self, outgoing, snapshot):
        if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 <= size <= MAX_BODY:
                return False
            while size:
                data = self.rfile.read(min(size, BLOCK))
                if not data:
                    return False
                outgoing.sendall(data)
                size -= len(data)
            return True

        total = 0
        while True:
            header = self.rfile.readline(128)
            size = int(header.split(b";", 1)[0], 16)
            if not 0 <= size <= MAX_BODY - total:
                return False
            outgoing.sendall(header)
            if size == 0:
                outgoing.sendall(self.rfile.readline(128))
                return True
            remaining = size
            while remaining:
                data = self.rfile.read(min(remaining, BLOCK))
                if not data:
                    return False
                outgoing.sendall(data)
                remaining -= len(data)
                total += len(data)
                if snapshot and total >= HELD_BYTES and not (STATE / "release").exists():
                    if COPY_SLOT.acquire(blocking=False):
                        try:
                            (STATE / "started").write_text(str(total))
                            end = time.monotonic() + 25
                            while time.monotonic() < end and not (STATE / "release").exists():
                                time.sleep(0.08)
                        finally:
                            COPY_SLOT.release()
                    # Abort the partial connection. A subsequent retry must
                    # restage the entire frame and install a verified image.
                    return False
            outgoing.sendall(self.rfile.read(2))


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8081), Gate).serve_forever()

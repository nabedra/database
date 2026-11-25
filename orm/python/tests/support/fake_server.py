"""A scripted fake Nabedra server for adversarial SDK tests."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
from threading import Lock, Thread
import time

DOCS = {
    "id": "00000000-0000-0000-0000-000000000021", "name": "docs", "key": "id", "format": "parquet",
    "schema": {
        "id": "utf8", "n": "int64", "f": "float64", "b": "bool", "vec": {"type": "vector", "dim": 2},
        "sp": {"type": "vector", "sparse": True},
        "mu": {"type": "list", "item": {"type": "vector", "dim": 2}}, "tags": {"type": "list", "item": "utf8"},
    },
}
PLAIN = {
    "id": "00000000-0000-0000-0000-000000000022", "name": "plain", "key": "id", "format": "parquet",
    "schema": {"id": "utf8", "n": "int64", "f": "float64", "b": "bool"},
}
CHUNKS = {
    "id": "00000000-0000-0000-0000-000000000023", "name": "chunks", "key": "id", "format": "parquet",
    "schema": {"id": "utf8", "body": "utf8", "note": "utf8", "year": "int64", "emb": {"type": "vector", "dim": 3},
               "sp": {"type": "vector", "sparse": True}, "mu": {"type": "list", "item": {"type": "vector", "dim": 2}},
               "tags": {"type": "list", "item": "utf8"}},
    "full_text": {"body": "english"}, "embed": {"emb": {"source": "body", "model": "m1"}},
}
LEAN = {
    "id": "00000000-0000-0000-0000-000000000024", "name": "lean", "key": "id", "format": "parquet",
    "schema": {"id": "utf8", "body": "utf8", "emb": {"type": "vector", "dim": 3}},
}
NUMBERED = {
    "id": "00000000-0000-0000-0000-000000000025", "name": "numbered", "key": "id", "format": "parquet",
    "schema": {"id": "int64", "vec": {"type": "vector", "dim": 1}},
}
TABLES = {table["name"]: table for table in (DOCS, PLAIN, CHUNKS, LEAN, NUMBERED)}
STATUS = {"validation_error": 400, "unauthorized": 401, "not_found": 404, "conflict": 409,
          "limit_exceeded": 413, "overloaded": 429, "storage_unavailable": 503,
          "search_unavailable": 503, "outcome_unknown": 503}


def ok(data):
    """A success reply; data is JSON text."""
    return {"data": data}


def error(code, message="m", outcome="not_applied", status=None):
    return {"status": status or STATUS.get(code, 500),
            "error": {"code": code, "message": message, "outcome": outcome}}


class Fake:
    """Replies come from a script: one entry per non-describe request, the last repeating."""

    def __init__(self):
        self.requests, self.script, self.lock = [], [], Lock()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.server.daemon_threads = True
        self.thread = Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def reset(self, *script):
        with self.lock:
            self.requests, self.script = [], list(script)

    def _handler(self):
        fake = self

        class Peer(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def do_POST(self):
                length = int(self.headers["Content-Length"])
                head = self.rfile.read(min(length, 200))
                if b'"operation":"table.describe"' not in head:
                    with fake.lock:
                        index = len(fake.requests)
                        planned = fake.script[min(index, len(fake.script) - 1)] if fake.script else {}
                    if isinstance(planned, dict) and "cut" in planned:
                        with fake.lock:
                            fake.requests.append({"raw": head, "request": {"operation": "cut", "payload": {}}})
                        self.close_connection = True
                        try:
                            self.connection.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        return self.connection.close()
                raw = head + self.rfile.read(length - len(head))
                request = json.loads(raw)
                request_id = json.dumps(request["request_id"])
                if request["operation"] == "table.describe":
                    table = TABLES[request["payload"]["name"]]
                    text = table if isinstance(table, str) else json.dumps(table)
                    return self.reply(200, f'{{"request_id":{request_id},"data":{text}}}')
                with fake.lock:
                    fake.requests.append({"raw": raw, "request": request})
                    index = len(fake.requests) - 1
                    answer = fake.script[min(index, len(fake.script) - 1)] if fake.script else ok("null")
                if callable(answer):
                    answer = answer(request)
                if "drop" in answer:
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    return self.connection.close()
                if "sleep" in answer:
                    time.sleep(answer["sleep"])
                if "partial" in answer:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", "500")
                    self.end_headers()
                    self.wfile.write(b'{"request_id":')
                    self.close_connection = True
                    return self.connection.close()
                body = answer.get("raw")
                if answer.get("echo_create"):
                    made = {"id": "00000000-0000-0000-0000-000000000040", **request["payload"]}
                    body = f'{{"request_id":{request_id},"data":{json.dumps(made)}}}'
                if "repeat" in answer:
                    text, count = answer["repeat"]
                    body = f'{{"request_id":{request_id},"data":[{",".join([text] * count)}]}}'
                if body is not None:
                    body = body.replace("$ID", request_id)
                if "applied" in answer:
                    outcomes = [{"o": "applied"}] * answer["applied"] + answer.get("tail", [])
                    body = f'{{"request_id":{request_id},"data":{json.dumps({"outcomes": outcomes}, separators=(",", ":"))}}}'
                if "fill" in answer:
                    body = f'{{"request_id":{request_id},"data":{{"outcomes":[],"pad":"{"x" * answer["fill"]}"}}}}'
                if body is None and "error" in answer:
                    body = f'{{"request_id":{request_id},"error":{json.dumps(answer["error"]).replace("$FILL", "m" * 1_000_000)}}}'
                elif body is None and "data" in answer:
                    body = f'{{"request_id":{request_id},"data":{answer["data"]}}}'
                if "rawhex" in answer:
                    body = bytes.fromhex(answer["rawhex"].replace("$ID", request_id.encode().hex()))
                self.reply(answer.get("status", 200), body, answer.get("type", "application/json"))

            def reply(self, status, body, kind="application/json"):
                encoded = body if isinstance(body, bytes) else (body or "").encode("utf-8")
                self.send_response(status)
                if kind:
                    self.send_header("Content-Type", kind)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        return Peer

"""Local API/two-worker acceptance with contention, replay and owned shutdown."""
import argparse
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import select
import signal
import socket
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time
import uuid


class FrontDoor(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = False

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Forward)
        self.target = None
        self.requests = []
        self.lock = threading.Lock()
        self.drop_response = None
        self.catalog_gate = None
        self.handlers = []
        self.thread = threading.Thread(target=self.serve_forever)
        self.thread.start()

    def process_request(self, request, client_address):
        # Python 3.6 ThreadingMixIn does not join handlers in server_close.
        thread = threading.Thread(target=self.process_request_thread,
                                  args=(request, client_address))
        with self.lock:
            self.handlers.append(thread)
        thread.start()

    @property
    def url(self):
        return "http://127.0.0.1:%d" % self.server_address[1]

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join()
        for thread in self.handlers:
            thread.join()


class Forward(socketserver.BaseRequestHandler):
    def handle(self):
        if self.server.target is None:
            return
        try:
            with socket.create_connection(self.server.target, timeout=5) as upstream:
                peers = {self.request: upstream, upstream: self.request}
                deadline = time.monotonic() + 60
                header = bytearray()
                recorded = False
                lost = None
                captured = bytearray()
                rendezvous = None
                while time.monotonic() < deadline:
                    ready, _, _ = select.select(list(peers), [], [], 1)
                    for source in ready:
                        data = source.recv(65536)
                        if not data:
                            if lost is not None:
                                lost["bytes"] = bytes(captured)
                                lost["done"].set()
                            return
                        if source is self.request and not recorded:
                            header.extend(data)
                            if b"\r\n\r\n" in header:
                                lines = bytes(header).split(b"\r\n\r\n", 1)[0].split(b"\r\n")
                                method, raw_path = lines[0].split(b" ")[:2]
                                path = raw_path.decode("ascii")
                                sender = receiver = None
                                for line in lines[1:]:
                                    name, value = line.split(b":", 1)
                                    if name.lower() == b"x-meshdb-auth":
                                        auth = json.loads(bytes.fromhex(value.strip().decode("ascii")))
                                        sender = auth["sender"]["node_id"]
                                        receiver = auth["receiver"]["node_id"]
                                with self.server.lock:
                                    self.server.requests.append((path, sender, receiver))
                                    gate = self.server.catalog_gate
                                    if gate is not None and method == b"GET" and path.endswith("/catalog.json"):
                                        rendezvous = gate["barrier"]
                                        gate["reads"] += 1
                                        if gate["reads"] == 2:
                                            self.server.catalog_gate = None
                                    if path == "/v1/operations" and self.server.drop_response is not None:
                                        lost = self.server.drop_response
                                        self.server.drop_response = None
                                recorded = True
                                header.clear()
                            elif len(header) > 16384:
                                return
                        if source is upstream and rendezvous is not None:
                            rendezvous.wait(timeout=15)
                            rendezvous = None
                        if source is upstream and lost is not None:
                            captured.extend(data)
                            if len(captured) > 8 * 1024 * 1024 + 16384:
                                return
                        else:
                            peers[source].sendall(data)
        except (OSError, threading.BrokenBarrierError):
            return


def exact(channel, count):
    result = bytearray()
    while len(result) < count:
        part = channel.recv(count - len(result))
        if not part:
            raise AssertionError("startup channel closed")
        result.extend(part)
    return bytes(result)


def send(channel, value):
    body = json.dumps(value, separators=(",", ":")).encode()
    channel.sendall(struct.pack(">I", len(body)) + body)


def receive(channel):
    length = struct.unpack(">I", exact(channel, 4))[0]
    assert 0 < length <= 16384
    value = json.loads(exact(channel, length))
    assert value.get("type") != "fatal", value.get("code")
    return value


def announcement(child):
    data = bytearray()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        ready, _, _ = select.select([child.stdout], [], [], 0.1)
        if ready:
            chunk = os.read(child.stdout.fileno(), 4096)
            if not chunk:
                raise AssertionError("process exited before announcement")
            data.extend(chunk)
            assert len(data) <= 16384
            if b"\n" in data:
                return json.loads(bytes(data).split(b"\n", 1)[0])
        assert child.poll() is None, "process exited before announcement"
    raise AssertionError("process announcement timeout")


def target(url):
    from urllib.parse import urlsplit
    parsed = urlsplit(url)
    return parsed.hostname, parsed.port


def write(path, value):
    path.write_text(json.dumps(value))
    path.chmod(0o600)


def call(base, operation, payload, token, request_id=None):
    conn = http.client.HTTPConnection(*target(base), timeout=30)
    try:
        body = json.dumps({"request_id": request_id or str(uuid.uuid4()),
                           "operation": operation, "payload": payload})
        conn.request("POST", "/v1/operations", body,
                     {"Authorization": "Bearer " + token,
                      "Content-Type": "application/json", "Connection": "close"})
        response = conn.getresponse()
        return response.status, json.loads(response.read(8 * 1024 * 1024 + 1))
    finally:
        conn.close()


def run(binary):
    children, doors, stalled, channels = [], [], [], []
    channel = None
    with tempfile.TemporaryDirectory(prefix="meshdb-nodes-") as directory:
        root = Path(directory)
        diagnostics = (root / "diagnostics").open("wb")
        # Explicit synthetic environment: no AWS profiles or operator material.
        env = {"PATH": os.defpath, "MESHDB_STORE_TOKEN": "fixture-store-token"}
        try:
            store = subprocess.Popen([sys.executable, str(Path(__file__).with_name("local_store.py")),
                                      "--database", str(root / "objects.sqlite"), "--port", "0"],
                                     env=env, stdout=subprocess.PIPE, stderr=diagnostics)
            children.append(store)
            address = announcement(store)
            store_door = FrontDoor()
            doors.append(store_door)
            store_door.target = (address["host"], address["port"])
            backend = {"driver": "http", "options": {
                "base_url": store_door.url,
                "namespace": "acceptance/", "token_file": "token", "ca_file": None}}
            (root / "token").write_text("fixture-store-token\n")
            (root / "token").chmod(0o600)
            auth = {"data_token": "d" * 43, "admin_token": "a" * 43,
                    "enrollment_token": "e" * 43}
            write(root / "auth", auth)
            cluster = str(uuid.uuid4())
            api_door = FrontDoor()
            doors.append(api_door)
            worker = {"listen": "127.0.0.1:0", "operation_slots": 2,
                      "partition_slots": 2, "queue_size": 4, "partition_size": "4 KiB"}
            write(root / "api.json", {"version": 1, "cluster_id": cluster,
                  "bind": "127.0.0.1:0", "advertise_url": api_door.url,
                  "api_urls": [api_door.url], "storage": backend, "auth_file": "auth",
                  "tls": None, "worker": worker})
            def start_api():
                channel, child_channel = socket.socketpair()
                channels.append(channel)
                channel.settimeout(30)
                api = subprocess.Popen([str(binary), "api", "--config", str(root / "api.json")],
                                       env=dict(env, MESHDB_SUPERVISOR_FD=str(child_channel.fileno())),
                                       pass_fds=(child_channel.fileno(),), stdout=diagnostics,
                                       stderr=diagnostics)
                children.append(api)
                child_channel.close()
                send(channel, {"version": 1, "type": "init", "name": "Acceptance",
                               "initial_policy": {"mode": "fixed", "desired": 2,
                                                  "min_nodes": 2, "max_nodes": 2},
                               "provisioning": "external", "bridge": None})
                prepared = receive(channel)
                assert prepared["type"] == "prepared"
                api_door.target = target(prepared["bound_url"])
                send(channel, {"version": 1, "type": "activate"})
                assert receive(channel)["type"] == "ready"
                return api, channel
            api, channel = start_api()
            workers = []
            worker_doors = []
            for index in range(2):
                door = FrontDoor()
                doors.append(door)
                worker_doors.append(door)
                bootstrap = dict(worker, version=1, cluster_id=cluster, node_id=str(uuid.uuid4()),
                                 request_id=None, api_urls=[api_door.url], advertise_url=door.url,
                                 storage=backend, enrollment_token=auth["enrollment_token"], tls=None)
                path = root / ("worker%d.json" % index)
                write(path, bootstrap)
                child = subprocess.Popen([str(binary), "worker", "--bootstrap", str(path)],
                                         env=env, stdout=subprocess.PIPE, stderr=diagnostics)
                children.append(child)
                workers.append(child)
                bound = announcement(child)
                door.target = target(bound["bound_url"])
            deadline = time.monotonic() + 30
            while True:
                status, value = call(api_door.url, "table.list", {}, auth["data_token"])
                if status == 200:
                    break
                assert status == 503 and time.monotonic() < deadline, value
                time.sleep(0.1)
            assert call(api_door.url, "table.list", {}, "invalid")[0] == 401
            status, value = call(api_door.url, "table.create", {
                "name": "items", "schema": {"id": "int64", "text": "utf8"},
                "key": "id", "format": "parquet"}, auth["data_token"])
            assert status == 200, value
            table = value["data"]["id"]
            rows = [{"id": n, "text": "row-%04d" % n + "x" * 90} for n in range(120)]
            request_id = str(uuid.uuid4())
            payload = {"table_id": table, "rows": rows}
            result = call(api_door.url, "table.insert_many", payload, auth["data_token"], request_id)
            assert result[0] == 200 and result[1]["data"] == len(rows), result
            assert call(api_door.url, "table.insert_many", payload,
                        auth["data_token"], request_id) == result
            status, value = call(api_door.url, "table.get", {"table_id": table, "key": 17}, auth["data_token"])
            assert status == 200 and value["data"] == rows[17], value
            query = {"table_id": table, "query": {"select": None, "filter": None,
                                                  "order_by": None, "limit": None}}
            status, value = call(api_door.url, "query.collect", query, auth["data_token"])
            assert status == 200 and value["data"] == rows, value
            status, value = call(api_door.url, "query.count", query, auth["data_token"])
            assert status == 200 and value["data"] == len(rows), value
            # Two accepted mutations capture the same table. The fenced queue
            # owner runs both concurrently; their CAS loser must recompute
            # instead of dropping the winning rows.
            gate = {"barrier": threading.Barrier(2), "reads": 0}
            with store_door.lock:
                store_door.catalog_gate = gate
            barrier = threading.Barrier(2)
            def competing(n):
                barrier.wait(timeout=10)
                return call(api_door.url, "table.insert", {
                    "table_id": table, "row": {"id": n, "text": "competing"}},
                    auth["data_token"])
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(competing, [120, 121]))
            assert all(status == 200 for status, _ in results), results
            assert gate["reads"] == 2, "both coordinators must capture the catalog before either publishes"
            rows.extend([{"id": n, "text": "competing"} for n in [120, 121]])
            assert call(api_door.url, "query.collect", query, auth["data_token"])[1]["data"] == rows

            # Suppress a complete committed response at the actual API TCP door.
            # The caller sees EOF, while the fixture retains the exact lost result.
            lost = {"done": threading.Event()}
            with api_door.lock:
                api_door.drop_response = lost
            lost_id = str(uuid.uuid4())
            lost_payload = {"table_id": table, "row": {"id": 122, "text": "lost-response"}}
            disconnected = False
            try:
                call(api_door.url, "table.insert", lost_payload, auth["data_token"], lost_id)
            except (OSError, http.client.HTTPException):
                disconnected = True
            assert disconnected and lost["done"].wait(10), "response gate did not close at EOF"
            header, body = lost["bytes"].split(b"\r\n\r\n", 1)
            expected = (int(header.split(b" ")[1]), json.loads(body))
            assert expected[0] == 200, expected
            replay = call(api_door.url, "table.insert", lost_payload, auth["data_token"], lost_id)
            assert replay == expected, (replay, expected)
            send(channel, {"version": 1, "type": "shutdown"})
            assert api.wait(timeout=30) == 0
            channel.close()
            api, channel = start_api()
            # A fresh API needs advancing heartbeat observations before routing.
            # Unavailable routing cannot deny the earlier committed same-ID write.
            probe = call(api_door.url, "table.insert", lost_payload, auth["data_token"], lost_id)
            if probe[0] != 200:
                assert probe[1]["error"]["outcome"] == "unknown", probe
            deadline = time.monotonic() + 30
            while call(api_door.url, "table.list", {}, auth["data_token"])[0] != 200:
                assert time.monotonic() < deadline, "replacement API routing timeout"
                time.sleep(0.1)
            replay = call(api_door.url, "table.insert", lost_payload, auth["data_token"], lost_id)
            assert replay == expected, (replay, expected)
            ordered = {"table_id": table, "query": {
                "select": ["id"], "filter": {"op": "ge", "column": "id", "value": 110},
                "order_by": {"column": "id", "desc": True}, "limit": 7}}
            status, value = call(api_door.url, "query.collect", ordered, auth["data_token"])
            assert status == 200 and value["data"] == [{"id": n} for n in range(122, 115, -1)], value
            rows.append(lost_payload["row"])
            assert call(api_door.url, "query.collect", query, auth["data_token"])[1]["data"] == rows
            jobs = [record for door in worker_doors for record in door.requests
                    if record[0] == "/internal/v1/jobs"]
            assert all(any(r[0] == "/internal/v1/jobs" for r in door.requests)
                       for door in worker_doors), "both workers must execute real TCP jobs"
            assert any(sender != receiver for _, sender, receiver in jobs), "peer execution missing"
            assert any(sender == receiver for _, sender, receiver in jobs), "self TCP execution missing"
            send(channel, {"version": 1, "type": "shutdown"})
            assert api.wait(timeout=30) == 0
            # Keep incomplete headers owned by each listener during SIGTERM.
            # Shutdown must settle these connections through its header deadline.
            for door in worker_doors:
                pending = socket.create_connection(door.target, timeout=5)
                stalled.append(pending)
                pending.sendall(b"POST /internal/v1/jobs HTTP/1.1\r\nHost: fixture\r\n")
            for child in workers:
                child.send_signal(signal.SIGTERM)
            for child in workers:
                assert child.wait(timeout=30) == 0
            passed = "PASS: public data/peer jobs, competing mutations, lost-response exact replay across API restart and process shutdown"
        finally:
            for owned_channel in channels:
                owned_channel.close()
            for child in reversed(children):
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()
                if child.stdout:
                    child.stdout.close()
            for pending in stalled:
                pending.close()
            for door in reversed(doors):
                door.close()
            diagnostics.close()
            if sys.exc_info()[0] is not None:
                sys.stderr.write((root / "diagnostics").read_text()[-16384:])

    print(passed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parents[1] / "server/target/debug/meshdb-server")
    run(parser.parse_args().binary.resolve())

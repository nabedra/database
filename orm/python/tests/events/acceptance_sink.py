#!/usr/bin/env python3
"""Exercise the Events sink with the real broker and OLAP API/worker processes.

Run inside a bounded Linux Docker container with the matching binaries and
the repository mounted read-only. Only loopback connectivity is needed.
"""

import argparse
from contextlib import ExitStack
from dataclasses import dataclass, field
from decimal import Decimal
import hashlib
from http.client import HTTPConnection, HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit
from uuid import uuid4

from nabedra import Database
from nabedra.errors import OverloadedError, StorageError, TransportError
from nabedra.events import Events

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "scripts"))
from acceptance_scheduler import Cluster, wait_for
from acceptance_nodes import write

TOPIC = "orders"
SUBSCRIPTION = "warehouse"
SCHEMA = {"event_key": str, "partition": int, "offset": str, "published_at": str, "data": str}
PRECISE = "1.2345678901234567890123456789"
HTTP_BODY_LIMIT = 8 * 1024 * 1024


class ProxyHandler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def relay(self):
        self.close_connection = True
        try:
            self.connection.settimeout(30)
            length = int(self.headers.get("Content-Length", "0"))
            assert 0 <= length <= HTTP_BODY_LIMIT
            body = self.rfile.read(length) if length else None
            if not self.server.before(self.command, self.path, body):
                return
            headers = {
                name: value for name, value in self.headers.items()
                if name.lower() not in {"host", "connection", "content-length"}
            }
            upstream = self.server.upstream
            connection = HTTPConnection(upstream.hostname, upstream.port, timeout=28)
            try:
                connection.request(self.command, self.path, body, headers)
                response = connection.getresponse()
                data = response.read(HTTP_BODY_LIMIT + 1)
                assert len(data) <= HTTP_BODY_LIMIT
                content_type = response.getheader("Content-Type")
            finally:
                connection.close()
            if not self.server.after(self.command, self.path, body, response.status, data):
                return
            self.send_response(response.status)
            if content_type:
                self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (OSError, HTTPException):
            # A disconnected upstream remains a disconnect to the real client.
            pass

    do_GET = do_POST = do_PUT = relay


class Proxy(ThreadingHTTPServer):
    def __init__(self, upstream, *, before=lambda *_: True, after=lambda *_: True):
        self.upstream = urlsplit(upstream)
        self.before, self.after = before, after
        super().__init__(("127.0.0.1", 0), ProxyHandler)
        self.thread = threading.Thread(target=self.serve_forever, name="ev04-proxy")

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server_port}"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.shutdown()
        self.server_close()
        self.thread.join(5)
        assert not self.thread.is_alive()


@dataclass
class ObservedSubmit:
    partition: int
    offset: int
    hold_reply: bool = False
    hold_job: bool = False
    observed: threading.Event = field(default_factory=threading.Event)
    release_reply: threading.Event = field(default_factory=threading.Event)
    request: dict | None = None
    response: tuple[int, dict] | None = None
    job: object = None

    def acceptance(self):
        assert self.observed.wait(25), f"No OLAP submission for {self.partition}:{self.offset}"
        status, receipt = self.response
        assert status == 202 and receipt["state"] == "queued", (status, receipt)
        assert receipt["request_id"] == self.request["request_id"]
        return receipt


class SinkTraffic:
    """Arm barriers before a worker job or after its real public submit reply."""

    def __init__(self, cluster):
        self.cluster = cluster
        self.lock = threading.Lock()
        self.watches = []
        self.submissions = {}
        self.receipt_fetches = []
        self.ack_entered = threading.Event()
        self.release_ack = threading.Event()
        self.ack_gate = True
        self.bounded_delivery = False
        self.bounded_release = threading.Event()

    def watch(self, partition, offset, *, hold_reply=False, hold_job=False):
        watch = ObservedSubmit(partition, offset, hold_reply, hold_job)
        with self.lock:
            self.watches.append(watch)
        return watch

    def before_olap(self, method, path, body):
        with self.lock:
            if method == "GET" and path.startswith("/v1/requests/"):
                self.receipt_fetches.append(path)
            if method != "POST" or path != "/v1/requests":
                return True
            value = json.loads(body)
            if value["operation"] != "table.insert_many":
                return True
            rows = value["payload"]["rows"]
            row_offsets = {(row["partition"], int(row["offset"])) for row in rows}
            self.submissions[value["request_id"]] = row_offsets
            for watch in self.watches:
                if watch.request is None and (watch.partition, watch.offset) in row_offsets:
                    watch.request = value
                    if watch.hold_job:
                        watch.job = self.cluster.gate(value["request_id"])
        return True

    def after_olap(self, method, path, body, status, response):
        if method != "POST" or path != "/v1/requests":
            return True
        request_id = json.loads(body)["request_id"]
        with self.lock:
            watch = next(
                (watch for watch in self.watches
                 if watch.request is not None and watch.request["request_id"] == request_id),
                None,
            )
        if watch is None:
            return True
        watch.response = status, json.loads(response)
        watch.observed.set()
        if watch.hold_reply:
            watch.release_reply.wait(25)
            return False
        return True

    def before_events(self, method, path, _):
        expected = f"/v1/topics/{TOPIC}/partitions/0/subscriptions/{SUBSCRIPTION}/checkpoint"
        bounded = f"/v1/topics/{TOPIC}/partitions/1/subscriptions/bounded/release"
        if method == "POST" and path == bounded:
            with self.lock:
                if self.bounded_delivery:
                    self.bounded_release.set()
        if method == "PUT" and path == expected:
            with self.lock:
                block = self.ack_gate
                self.ack_gate = False
            if block:
                self.ack_entered.set()
                self.release_ack.wait(25)
                return False
        return True

    def after_events(self, method, path, _, status, response):
        bounded = f"/v1/topics/{TOPIC}/partitions/1/subscriptions/bounded/events?"
        if method == "GET" and path.startswith(bounded) and status == 200:
            if json.loads(response)["events"]:
                with self.lock:
                    self.bounded_delivery = True
        return True

    def observed_receipt(self, receipt):
        path = f"/v1/requests/{receipt['run_id']}/{receipt['request_id']}"
        with self.lock:
            return path in self.receipt_fetches

    def submits_for(self, partition, offset):
        with self.lock:
            return [
                request_id for request_id, offsets in self.submissions.items()
                if (partition, offset) in offsets
            ]

    def close(self):
        self.release_ack.set()
        for watch in self.watches:
            watch.release_reply.set()


def loopback_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def status_json(url, method, path, token=None, body=None):
    target = urlsplit(url)
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    connection = HTTPConnection(target.hostname, target.port, timeout=6)
    try:
        connection.request(method, path, json.dumps(body) if body is not None else None, headers)
        result = connection.getresponse()
        content = result.read()
        return result.status, json.loads(content) if content and result.status != 503 else None
    finally:
        connection.close()


class Broker:
    def __init__(self, binary, directory, stack):
        self.binary = binary
        self.config = directory / "broker.json"
        public_port, peer_port = loopback_port(), loopback_port()
        while public_port == peer_port:
            peer_port = loopback_port()
        self.url = f"http://127.0.0.1:{public_port}"
        cluster_id = str(uuid4())
        ring = {purpose: {"active": "current", "keys": [{
            "id": "current", "secret_hex": secrets.token_hex(32),
            "accept_until_ms": int(time.time() * 1000) + 3_600_000,
        }]} for purpose in ("grant", "session")}
        write(self.config, {
            "broker_id": 1,
            "cluster_id": cluster_id,
            "data_dir": str(directory / "broker-store"),
            "public_bind": f"127.0.0.1:{public_port}",
            "peer_bind": f"127.0.0.1:{peer_port}",
            "allow_plaintext": True,
            "peer_token": secrets.token_urlsafe(40),
            "bootstrap": {"metadata_voters": [1], "brokers": [{
                "id": 1, "zone": "fixture", "peer_url": f"http://127.0.0.1:{peer_port}",
            }]},
            "keyring": {"cluster_id": cluster_id, **ring},
            "http_limits": {"lease_ms": 6000},
        })
        log = stack.enter_context((directory / "broker.log").open("wb"))
        subprocess.run([str(binary), "init", "--config", str(self.config)],
                       check=True, timeout=30, stdout=log, stderr=log)
        process = subprocess.Popen([str(binary), "serve", "--config", str(self.config)],
                                   stdout=log, stderr=log, env=dict(os.environ, TOKIO_WORKER_THREADS="2"))
        stack.callback(stop_process, process)

        def ready():
            assert process.poll() is None, "Events broker exited"
            try:
                return status_json(self.url, "GET", "/readyz")[0] == 200
            except OSError:
                return False

        wait_for(ready, "Events broker never became ready", timeout=25)
        self.operator = self.grant("operator", {"operation": "operator"})

    def grant(self, subject, scope):
        grant_path = self.config.with_name(f"grant-{subject}.json")
        write(grant_path, {"tenant": "fixture", "subject": subject, "scope": scope,
                           "expires_at_ms": int(time.time() * 1000) + 900_000})
        return subprocess.check_output([
            str(self.binary), "issue-grant", "--config", str(self.config), "--grant", str(grant_path),
        ], text=True, timeout=8).strip()

    def admin(self, action):
        identity = str(uuid4())
        path = "/v1/admin/operations"
        status, value = status_json(self.url, "POST", path, self.operator,
                                    {"operation_id": identity, "action": action})
        assert status in (200, 202) and value["operation_id"] == identity, (status, value)
        if value["state"] == "completed":
            return

        def completed():
            status, value = status_json(self.url, "GET", f"{path}/{identity}", self.operator)
            return status == 200 and value["state"] == "completed"

        wait_for(completed, f"operation {action['type']} did not complete", timeout=25)

    def checkpoint(self, partition, sub=SUBSCRIPTION):
        path = f"/v1/admin/topics/{TOPIC}/partitions/{partition}/subscriptions/{sub}"
        status, result = status_json(self.url, "GET", path, self.operator)
        assert status == 200, (partition, status, result)
        return int(result["checkpoint"])


def stop_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(5)


def start_sink(stack, directory, *, events_url, db_url, db_token, sub=SUBSCRIPTION, table,
               grant, start=0, stop=2, in_flight=2):
    path = directory / f"sink-{uuid4().hex}.json"
    write(path, {
        "events": {
            "url": events_url, "token_env": "SINK_SOURCE", "topic": TOPIC,
            "subscription": sub, "allow_insecure": True, "timeout": 20,
        },
        "olap": {
            "url": db_url, "token_env": "SINK_DESTINATION",
            "table": table.name, "table_id": table.id, "allow_insecure": True, "timeout": 22,
        },
        "partitions": {"start": start, "stop": stop},
        "workers": stop - start, "max_in_flight": in_flight, "batch_size": 8,
        "poll_interval": 0.1, "receipt_wait": 0.55, "renew_interval": 0.5,
    })
    logfile = path.with_suffix(".log")
    log = stack.enter_context(logfile.open("wb"))
    process = subprocess.Popen([sys.executable, "-m", "nabedra.events.sink", "--config", str(path)],
                               stdout=log, stderr=log,
                               env=dict(os.environ, SINK_SOURCE=grant, SINK_DESTINATION=db_token))
    stack.callback(stop_process, process)
    return process, logfile


def publish_expected(producer, partition, data, expected):
    event = producer.publish(TOPIC, partition=partition, event_id=uuid4(), data=data)
    key = f"{event.topic_id}:{event.partition}:{event.event_id}"
    assert event.data == data
    if "precise" in data:
        encoded = f'{{"precise":{PRECISE}}}'
    else:
        encoded = json.dumps(data, separators=(",", ":"), sort_keys=True)
    expected[key] = {
        "event_key": key, "partition": event.partition, "offset": str(event.offset),
        "published_at": event.published_at, "data": encoded,
    }
    return event


def assert_rows(table, expected):
    rows = table.query().collect()
    assert len(rows) == len(expected)
    assert {row["event_key"]: row for row in rows} == expected


def outcome(db, receipt, expected, timeout=45):
    def finished():
        try:
            value = db.receipt(receipt["run_id"], receipt["request_id"])
        except (StorageError, TransportError, OverloadedError):
            return False
        if value.terminal:
            assert value.state == expected, value
            return value
        return False

    return wait_for(finished, f"Request did not settle as {expected}", timeout=timeout)


def live_workers(cluster):
    return [node["node_id"] for node in cluster.control()["nodes"] if node["state"] == "ready"]


def run(olap, events, directory, record):
    with ExitStack() as stack:
        cluster = Cluster(olap, directory)
        stack.callback(cluster.close)
        for identity in (10, 20):
            cluster.start_worker(identity, 1)
        database = Database(cluster.api_door.url, cluster.auth["data_token"],
                            mode="olap", timeout=10)
        table = database.create_table("order_events", SCHEMA, key="event_key")
        broker = Broker(events, directory, stack)
        broker.admin({"type": "create_topic", "topic": TOPIC, "partitions": 2,
                      "replication_factor": 1, "retention_seconds": 3600})
        for sub in (SUBSCRIPTION, "audit"):
            broker.admin({"type": "create_subscription", "topic": TOPIC,
                          "subscription": sub, "start": "earliest"})
        producer = Events(broker.url, token=broker.grant("publisher", {"operation": "publish", "topic": TOPIC}),
                          allow_insecure=True)
        observer = Events(broker.url, token=broker.grant("auditor", {
            "operation": "read", "topic": TOPIC, "subscription": "audit",
        }), allow_insecure=True)
        consumer_grant = broker.grant("warehouse", {
            "operation": "read", "topic": TOPIC, "subscription": SUBSCRIPTION,
        })
        expected = {}
        initial = {
            0: [publish_expected(producer, 0, {"precise": Decimal(PRECISE)}, expected),
                publish_expected(producer, 0, {"order": 2, "delivery": ["email", None]}, expected)],
            1: [publish_expected(producer, 1, {"order": 3}, expected),
                publish_expected(producer, 1, {"order": 4, "paid": True}, expected)],
        }

        traffic = SinkTraffic(cluster)
        olap_proxy = stack.enter_context(Proxy(cluster.api_door.url, before=traffic.before_olap,
                                               after=traffic.after_olap))
        events_proxy = stack.enter_context(Proxy(broker.url, before=traffic.before_events,
                                                 after=traffic.after_events))
        stack.callback(traffic.close)

        def sink(**options):
            defaults = {"events_url": events_proxy.url, "db_url": olap_proxy.url,
                        "db_token": cluster.auth["data_token"], "table": table, "grant": consumer_grant}
            return start_sink(stack, directory, **(defaults | options))

        lost = traffic.watch(0, 1, hold_reply=True)
        first_sink, _ = sink()
        lost_receipt = lost.acceptance()
        complete = outcome(database, lost_receipt, "succeeded")
        assert complete.result == 2 and cluster.decision_count(complete.request_id) == 1
        assert broker.checkpoint(0) == 0
        for partition, batch in initial.items():
            with observer.subscription(TOPIC, "audit", partition=partition).claim() as lease:
                delivered = lease.poll()
                assert delivered.after == 0 and delivered.events == tuple(batch)
                assert lease.ack(delivered, through=2) == 2
        wait_for(lambda: broker.checkpoint(1) == 2, "Second sink partition did not progress during lost reply")
        lost.release_reply.set()
        assert traffic.ack_entered.wait(20), "Sink did not reconcile a lost committed response"
        assert traffic.submits_for(0, 1) == [complete.request_id]
        assert_rows(table, expected)
        assert broker.checkpoint(0) == 0 and broker.checkpoint(1) == 2
        first_sink.kill()
        assert first_sink.wait(5) == -9
        traffic.release_ack.set()
        assert broker.checkpoint(0) == 0
        record("EV04: loss of a real OLAP response after durable commit. The audit subscription and second sink partition progressed while it was lost")
        record("EV04: SIGKILL before the first acknowledgement reached Events. Four expected rows and exact decimal were durable")

        second_sink, _ = sink()
        wait_for(lambda: broker.checkpoint(0) == 2, "Redelivery never reached the expected checkpoint", timeout=25)
        assert second_sink.poll() is None
        assert len(traffic.submits_for(0, 1)) >= 2, "Sink never reprocessed the unacknowledged event"
        assert_rows(table, expected)
        assert cluster.decision_count(complete.request_id) == 1
        record("EV04: redelivery reached the checkpoint through row-key reconciliation, preserving four distinct rows")

        assert len(live_workers(cluster)) == 2
        recovery = traffic.watch(0, 3, hold_job=True)
        event = publish_expected(producer, 0, {"after": "worker-loss"}, expected)
        assert event.offset == 3
        retained = recovery.acceptance()
        assert recovery.job.entered.wait(20), "Accepted OLAP request did not enter its job gate"
        wait_for(lambda: traffic.observed_receipt(retained), "Sink did not observe the original receipt")
        assert broker.checkpoint(0) == 2 and cluster.decision_count(retained["request_id"]) == 0
        cluster.stop_api()
        cluster.start_api()
        def observed_after_api_restart():
            status, found = cluster.receipt(retained)
            if status == 503:
                return None
            assert status == 200 and found["state"] in {"queued", "running", "reconciling"}
            return found

        still_pending = wait_for(observed_after_api_restart, "Restarted API could not read the pending receipt")
        assert still_pending["run_id"] == retained["run_id"]
        old = cluster.control()["scheduler"]["coordinator_node"]
        process = cluster.workers[old][0]
        process.kill()
        process.wait(5)
        recovery.job.release.set()
        recovered = outcome(database, retained, "succeeded", timeout=55)
        assert recovered.result == 1 and recovered.run_id == retained["run_id"]
        assert cluster.decision_count(recovered.request_id) == 1
        assert len(live_workers(cluster)) == 1 and live_workers(cluster)[0] != old
        wait_for(lambda: broker.checkpoint(0) == event.offset, "Sink did not acknowledge after one-worker recovery")
        assert traffic.submits_for(0, 3) == [recovered.request_id]
        assert_rows(table, expected)
        record("Q04/R01: retained accepted sink request through an API restart and scheduler worker SIGKILL, committed once under the same run and UUID with one surviving worker")

        abandoned = traffic.watch(0, 4, hold_job=True)
        event = publish_expected(producer, 0, {"after": "cold-restart"}, expected)
        assert event.offset == 4
        pending = abandoned.acceptance()
        assert abandoned.job.entered.wait(20)
        assert cluster.decision_count(pending["request_id"]) == 0 and broker.checkpoint(0) == 3
        for process, _, _ in cluster.workers.values():
            if process.poll() is None:
                process.kill()
                process.wait(5)
        abandoned.job.release.set()
        cluster.stop_api()
        cluster.start_api(cold=True)
        cluster.start_worker(40, 1)
        old_receipt = outcome(database, pending, "abandoned")
        assert old_receipt.result is None and cluster.decision_count(pending["request_id"]) == 0
        assert outcome(database, retained, "succeeded").result == 1
        wait_for(lambda: broker.checkpoint(0) == event.offset, "No sink replay after cold abandonment", timeout=45)
        assert len(traffic.submits_for(0, 4)) == 2
        replacement_id = next(identity for identity in traffic.submits_for(0, 4)
                              if identity != pending["request_id"])
        assert cluster.decision_count(replacement_id) == 1
        assert_rows(table, expected)
        record("R04: full worker loss abandoned the gated receipt, preserved prior durable rows, and the sink committed the fresh request before advancing the checkpoint")

        stop_process(second_sink)
        broker.admin({"type": "create_subscription", "topic": TOPIC,
                      "subscription": "bounded", "start": "latest"})
        bounded_grant = broker.grant("bounded", {
            "operation": "read", "topic": TOPIC, "subscription": "bounded",
        })
        held = traffic.watch(0, 5, hold_job=True)
        publish_expected(producer, 0, {"after": "permit"}, expected)
        bounded_sink, _ = sink(sub="bounded", grant=bounded_grant, in_flight=1)
        held_receipt = held.acceptance()
        assert held.job.entered.wait(20)
        next_part = publish_expected(producer, 1, {"next": "permit"}, expected)
        assert next_part.offset == 3
        assert traffic.bounded_release.wait(15), "Second partition did not poll under the held sink permit"
        assert traffic.submits_for(1, 3) == []
        assert broker.checkpoint(0, "bounded") == 4 and broker.checkpoint(1, "bounded") == 2
        held.job.release.set()
        assert outcome(database, held_receipt, "succeeded").result == 1
        wait_for(lambda: broker.checkpoint(1, "bounded") == 3, "Second partition did not obtain a freed permit")
        wait_for(lambda: broker.checkpoint(0, "bounded") == 5, "First partition did not acknowledge its committed row")
        assert len(traffic.submits_for(1, 3)) == 1
        assert_rows(table, expected)
        record("EV04 sink concurrency: one accepted write held the limit while another partition polled and released. Both advanced only after commit")
        stop_process(bounded_sink)

        broker.admin({"type": "create_subscription", "topic": TOPIC,
                      "subscription": "stale", "start": "latest"})
        stale_grant = broker.grant("stale", {
            "operation": "read", "topic": TOPIC, "subscription": "stale",
        })
        table.drop()
        replaced = database.create_table(table.name, SCHEMA, key="event_key")
        assert replaced.id != table.id
        publish_expected(producer, 1, {"after": "replacement"}, expected)
        stale_sink, log = sink(sub="stale", grant=stale_grant, events_url=broker.url, start=1, stop=2, in_flight=1)
        assert stale_sink.wait(20) == 1 and "Destination table UUID changed" in log.read_text()
        assert broker.checkpoint(1, "stale") == 3 and replaced.query().count() == 0
        record("EV04/T06 stale-table case: a same-name replacement received no rows and its source checkpoint stayed put")

        incompatible = database.create_table("different_schema", {"event_key": str, "data": str},
                                             key="event_key")
        bad_schema_sink, log = sink(table=incompatible, sub="stale", grant=stale_grant,
                                   events_url=broker.url, start=1, stop=2, in_flight=1)
        assert bad_schema_sink.wait(20) == 1 and "Destination table key or schema changed" in log.read_text()
        assert broker.checkpoint(1, "stale") == 3 and incompatible.query().count() == 0
        record("EV04: the configured table UUID with an incompatible schema paused before ingestion")

        broker.admin({"type": "create_subscription", "topic": TOPIC,
                      "subscription": "collision", "start": "latest"})
        collision_grant = broker.grant("collision", {
            "operation": "read", "topic": TOPIC, "subscription": "collision",
        })
        conflicting_table = database.create_table("collision_events", SCHEMA, key="event_key")
        collision_row = {}
        collision = publish_expected(producer, 0, {"after": "collision"}, collision_row)
        original = dict(next(iter(collision_row.values())), data='{"different":true}')
        conflicting_table.insert(original)
        collision_sink, log = sink(table=conflicting_table, sub="collision", grant=collision_grant,
                                   events_url=broker.url, start=0, stop=1, in_flight=1)
        assert collision_sink.wait(20) == 1 and "different data for this event identity" in log.read_text()
        assert broker.checkpoint(0, "collision") == collision.offset - 1
        assert_rows(conflicting_table, {original["event_key"]: original})
        record("EV04/T06 conflict case: a conflicting row identity paused the sink and preserved the row and checkpoint")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--olap-binary", type=Path, required=True)
    parser.add_argument("--events-binary", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    assertions = []

    def record(assertion):
        assertions.append(assertion)
        print(assertion, flush=True)

    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="nabedra-ev04-") as path:
        directory = Path(path)
        try:
            run(args.olap_binary.resolve(), args.events_binary.resolve(), directory, record)
        except BaseException:
            for log in [directory / "diagnostics", *directory.glob("*.log")]:
                if log.is_file():
                    print(f"{log.name}\n{log.read_text(errors='replace')[-12000:]}", file=sys.stderr)
            raise
        finally:
            def digest(path):
                with path.open("rb") as stream:
                    return hashlib.file_digest(stream, "sha256").hexdigest()

            args.evidence.write_text(json.dumps({
                "olap_sha256": digest(args.olap_binary),
                "events_sha256": digest(args.events_binary),
                "seconds": round(time.monotonic() - started, 3),
                "assertions": assertions,
            }, indent=2) + "\n")


if __name__ == "__main__":
    main()

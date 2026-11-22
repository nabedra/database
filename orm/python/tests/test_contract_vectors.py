"""Replay the shared OLAP and Events exchanges through the public Python ORM."""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
from threading import Thread
import unittest
from uuid import UUID

from nabedra import Database, NabedraError, col
from nabedra.events import Events, EventsError


VECTORS = Path(__file__).resolve().parents[2] / "contract"
CHECKED_HEADERS = ("authorization", "content-type", "x-nabedra-database", "x-events-lease")
REQUEST_ID = "@request_id"


class Peer(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def exchange(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode("utf-8")
        headers = {name: self.headers[name] for name in CHECKED_HEADERS if name in self.headers}
        request_id = None
        if self.path == "/v1/operations":
            request_id = json.loads(raw).get("request_id")
            if request_id is not None:
                raw = raw.replace(json.dumps(request_id), json.dumps(REQUEST_ID), 1)
        index = len(self.server.requests)
        self.server.requests.append({"method": self.command, "path": self.path, "body": raw, "headers": headers})
        self.server.request_ids.append(request_id)
        if index >= len(self.server.responses):
            self.send_error(500, "Unexpected request")
            return
        reply = self.server.responses[index]
        body = reply["body"].replace(REQUEST_ID, request_id or REQUEST_ID).encode("utf-8")
        self.send_response(reply["status"])
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in reply.get("headers", {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PUT = exchange


def run_olap(url, action, inputs):
    with Database(url, "fixture", mode="olap", database="analytics-v1", timeout=2) as db:
        table = db.table(inputs["table"])
        if action == "insert":
            return table.insert({"id": inputs["insert_id"], "age": int(inputs["insert_age"]), "active": True})
        query = (table.where(col("age") >= int(inputs["minimum_age"]), active=True)
                 .select("id", "age").order_by("age", True).limit(inputs["limit"]))
        if action == "collect":
            return [{**row, "age": integer_text(row["age"])} for row in query.collect()]
        if action == "count":
            return integer_text(query.count())
        if action == "explain":
            return query.explain()
        raise AssertionError(f"Unknown OLAP operation {action}")


def event_result(event):
    return {
        "topic_id": str(event.topic_id), "partition": event.partition, "offset": integer_text(event.offset),
        "event_id": str(event.event_id), "published_at": event.published_at, "data": event.data,
    }


def integer_text(value):
    if type(value) is not int:
        raise AssertionError(f"Expected an integer, got {type(value).__name__}")
    return str(value)


def run_events(url, action, inputs):
    client = Events(url, token="fixture", allow_insecure=True, timeout=2)
    if action == "publish":
        return event_result(client.publish(inputs["topic"], partition=inputs["partition"],
                                           event_id=inputs["event_id"], data=inputs["data"]))
    lease = client.subscription(inputs["topic"], inputs["subscription"],
                                partition=inputs["partition"]).claim(claim_id=inputs["claim_id"])
    page = lease.poll(limit=inputs["limit"])
    if action == "poll":
        return {"after": integer_text(page.after), "events": list(map(event_result, page.events)),
                "delivery": page.delivery}
    if action == "ack":
        return integer_text(lease.ack(page, through=int(inputs["through"])))
    raise AssertionError(f"Unknown Events operation {action}")


class ContractVectors(unittest.TestCase):
    def replay(self, module, run, error_type):
        contract = json.loads((VECTORS / module / "vectors.json").read_text(encoding="utf-8"))
        server = HTTPServer(("127.0.0.1", 0), Peer)
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_port}"

        for case in contract["cases"]:
            with self.subTest(case=case["name"]):
                action = case["operation"]
                operation = {**contract["operations"][action], **case}
                exchanges = [contract["operations"][name] for name in operation["setup"]] + [operation]
                server.requests, server.request_ids = [], []
                server.responses = [step["response"] for step in exchanges]
                if "error" in case:
                    with self.assertRaises(error_type) as caught:
                        run(url, action, contract["input"])
                    self.assert_error(caught.exception, case["error"])
                    if module == "olap":
                        self.assertEqual(caught.exception.request_id, server.request_ids[-1])
                else:
                    self.assertEqual(run(url, action, contract["input"]), operation["result"])
                self.assertEqual(server.requests, [step["request"] for step in exchanges])
                if module == "olap":
                    self.assertEqual(len(set(server.request_ids)), len(exchanges))
                    for request_id in server.request_ids:
                        self.assertEqual(str(UUID(request_id)), request_id)

    def assert_error(self, error, expected):
        actual = {
            "type": type(error).__name__, "code": error.code,
            "status_code": error.status_code, "outcome": error.outcome,
        }
        if "message" in expected:
            actual["message"] = str(error)
        if isinstance(error, EventsError):
            actual.update(partition=error.partition, retry_after=error.retry_after,
                          earliest_offset=None if error.earliest_offset is None else integer_text(error.earliest_offset))
            expected = {"partition": None, "retry_after": None, "earliest_offset": None, **expected}
        self.assertEqual(actual, expected)

    def test_olap(self):
        self.replay("olap", run_olap, NabedraError)

    def test_events(self):
        self.replay("events", run_events, EventsError)


if __name__ == "__main__":
    unittest.main()

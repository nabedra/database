"""Focused SDK wire checks against a bounded loopback HTTP fixture."""

from datetime import datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from queue import Empty, Queue
from threading import Thread
import unittest
from uuid import UUID

from nabedra.events import Events, EventsError, EventsProtocolError, EventsUnknownOutcomeError

TOPIC_ID = UUID(int=1)
EVENT_ID = UUID(int=2)
TIMESTAMP = "2026-11-17T12:00:00Z"
PRECISE = "1.2345678901234567890123456789"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def exchange(self):
        request = (
            self.command, self.path, self.headers.get("X-Events-Lease"),
            self.rfile.read(int(self.headers.get("Content-Length", 0))),
        )
        self.server.requests.put(request)
        try:
            reply = self.server.replies.get_nowait()
        except Empty:
            return
        if reply is None:
            return
        status, headers, body = reply
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PUT = exchange


class ClientWireTests(unittest.TestCase):
    def setUp(self):
        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.server.requests = Queue()
        self.server.replies = Queue()
        thread = Thread(target=self.server.serve_forever)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.client = Events(f"http://127.0.0.1:{self.server.server_port}",
                             token="fixture", allow_insecure=True, timeout=2)

    def reply(self, value, status=200, **headers):
        if type(value) is not bytes:
            value = json.dumps(value).encode()
        self.server.replies.put((status, {"Content-Type": "application/json", **headers}, value))

    def seen(self):
        return self.server.requests.get(timeout=2)

    def test_claim_reuses_uuid_after_missing_response(self):
        self.server.replies.put(None)
        self.reply({"lease": "fixture.lease", "expires_at": TIMESTAMP})
        self.reply(b"", 204)
        with self.client.subscription("orders", "audit", partition=4).claim() as lease:
            self.assertEqual(lease.expires_at, datetime.fromisoformat(TIMESTAMP))
        first, second, release = self.seen(), self.seen(), self.seen()
        self.assertEqual(first, second)
        self.assertEqual(first[:3], (
            "POST", "/v1/topics/orders/partitions/4/subscriptions/audit/claim", None,
        ))
        self.assertEqual(len(UUID(json.loads(first[3])["claim_id"]).bytes), 16)
        self.assertEqual(release[:3], (
            "POST", "/v1/topics/orders/partitions/4/subscriptions/audit/release", "fixture.lease",
        ))

    def test_publish_reports_unknown_and_keeps_exact_identity_on_explicit_retry(self):
        body = {
            "topic_id": str(TOPIC_ID), "partition": 0, "offset": str(2**53 + 1),
            "event_id": str(EVENT_ID), "published_at": TIMESTAMP, "data": None,
        }
        self.server.replies.put(None)
        self.reply(json.dumps(body).replace('"data": null', f'"data": {PRECISE}').encode(), 200)
        args = {"partition": 0, "event_id": EVENT_ID, "data": Decimal(PRECISE)}
        with self.assertRaises(EventsUnknownOutcomeError) as unknown:
            self.client.publish("orders", **args)
        self.assertEqual(unknown.exception.outcome, "unknown")
        first = self.seen()
        self.assertTrue(self.server.requests.empty())
        event = self.client.publish("orders", **args)
        self.assertEqual(first, self.seen())
        self.assertIn(PRECISE.encode(), first[3])
        self.assertEqual((event.data, event.offset, event.event_id),
                         (Decimal(PRECISE), 2**53 + 1, EVENT_ID))

    def test_ack_uses_exact_offset_and_rejects_foreign_delivery_before_request(self):
        after = 2**53 + 1
        self.reply({"lease": "fixture.lease", "expires_at": TIMESTAMP})
        self.reply({"lease": "other.lease", "expires_at": TIMESTAMP})
        self.reply({
            "after": str(after), "delivery": "fixture.delivery", "events": [{
                "topic_id": str(TOPIC_ID), "partition": 0, "offset": str(after + 1),
                "event_id": str(EVENT_ID), "published_at": TIMESTAMP, "data": True,
            }],
        })
        self.reply({"offset": str(after + 1)})
        sub = self.client.subscription("orders", "audit", partition=0)
        lease, foreign = sub.claim(), sub.claim()
        batch = lease.poll()
        self.assertEqual(batch.after, after)
        with self.assertRaises(ValueError):
            foreign.ack(batch, through=after + 1)
        self.assertEqual(lease.ack(batch, through=after + 1), after + 1)
        for _ in range(3):
            self.seen()
        method, path, credential, request = self.seen()
        self.assertEqual((method, path.rsplit("/", 1)[-1], credential),
                         ("PUT", "checkpoint", "fixture.lease"))
        self.assertEqual(json.loads(request), {"delivery": "fixture.delivery", "through": str(after + 1)})

    def test_error_metadata_and_duplicate_fields(self):
        self.reply({"lease": "fixture.lease", "expires_at": TIMESTAMP})
        self.reply({"code": "checkpoint_expired", "message": "Expired", "partition": 0,
                    "earliest_offset": str(2**53 + 1)}, 410)
        self.reply({"code": "overloaded", "message": "Busy"}, 429, **{"Retry-After": "2"})
        self.reply(b'{"after":"0","after":"0","events":[],"delivery":null}')
        lease = self.client.subscription("orders", "audit", partition=0).claim()
        with self.assertRaises(EventsError) as expired:
            lease.poll()
        self.assertEqual((expired.exception.status_code, expired.exception.code,
                          expired.exception.partition, expired.exception.earliest_offset),
                         (410, "checkpoint_expired", 0, 2**53 + 1))
        with self.assertRaises(EventsError) as overloaded:
            lease.poll()
        self.assertEqual((overloaded.exception.status_code, overloaded.exception.retry_after), (429, 2.0))
        with self.assertRaises(EventsProtocolError):
            lease.poll()


if __name__ == "__main__":
    unittest.main()

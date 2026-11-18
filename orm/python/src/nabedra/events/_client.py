"""Producer and subscription API for Nabedra Events."""
from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
import re
from threading import Lock
from uuid import UUID, uuid4

from ._http import (
    MAX_BODY_BYTES,
    MAX_EVENT_BYTES,
    MAX_OFFSET,
    MAX_PAGE_COUNT,
    MAX_PARTITION,
    EventsError,
    EventsProtocolError,
    EventsUnknownOutcomeError,
    HTTP,
    bounded_int,
    canonical_json,
    offset_string,
)

_NAME = re.compile(r"[A-Za-z0-9_-]{1,63}")
_OPAQUE = re.compile(r"[!-~]{1,8192}")
_RFC3339 = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})"
)


def _uuid(value: UUID | str) -> str:
    if isinstance(value, UUID):
        return str(value)
    if type(value) is not str:
        raise ValueError("Expected a UUID")
    return str(UUID(value))


def _wire_uuid(value: object) -> UUID:
    if type(value) is not str or _uuid(value) != value:
        raise ValueError("Expected a canonical UUID string")
    return UUID(value)


def _timestamp(value: object) -> datetime:
    if type(value) is not str or _RFC3339.fullmatch(value) is None:
        raise ValueError("Expected an RFC3339 timestamp")
    return datetime.fromisoformat(value)


def _name(value: str) -> str:
    if type(value) is not str or _NAME.fullmatch(value) is None:
        raise ValueError("Events names must match [A-Za-z0-9_-]{1,63}")
    return value


def _object(value: object, fields: set[str]) -> dict:
    if type(value) is not dict or value.keys() != fields:
        raise ValueError("Unexpected Events response fields")
    return value


def _opaque(value: object) -> str:
    if type(value) is not str or _OPAQUE.fullmatch(value) is None:
        raise ValueError("Expected an opaque credential")
    return value


def _publish_body(event_id: str, data: object) -> bytes:
    prefix = b'{"data":'
    suffix = b',"event_id":"' + event_id.encode("ascii") + b'"}'
    payload = canonical_json(data, limit=MAX_EVENT_BYTES - len(prefix) - len(suffix))
    return prefix + payload + suffix


@dataclass(frozen=True, slots=True)
class PublishRecord:
    event_id: UUID | str
    data: object = field(repr=False)


@dataclass(frozen=True, slots=True)
class Event:
    """A durable event with an offset scoped to its topic ID and partition.

    Fractional or exponent-form JSON numbers in data are decoded as Decimal.
    """

    topic_id: UUID
    partition: int
    offset: int
    event_id: UUID
    published_at: str
    data: object = field(repr=False)

    @classmethod
    def _decode(cls, value: object, partition: int, *, expected_id: str | None = None) -> Event:
        fields = _object(value, {"topic_id", "partition", "offset", "event_id", "published_at", "data"})
        wire_partition = bounded_int(fields["partition"], MAX_PARTITION)
        topic_id = _wire_uuid(fields["topic_id"])
        event_id = _wire_uuid(fields["event_id"])
        if wire_partition != partition or expected_id is not None and str(event_id) != expected_id:
            raise ValueError("Event receipt does not match its submitted identity")
        _timestamp(fields["published_at"])
        return cls(topic_id, partition, offset_string(fields["offset"], minimum=1),
                   event_id, fields["published_at"], fields["data"])


@dataclass(frozen=True, slots=True)
class Batch:
    """A contiguous page delivered under one lease."""

    after: int
    events: tuple[Event, ...]
    delivery: str | None = field(repr=False)
    _lease: Lease = field(repr=False, compare=False)


class Events:
    """Synchronous Events client with independent endpoint and bearer token."""

    def __init__(
        self, url: str, *, token: str, timeout: float = 10.0, ca_file: str | None = None,
        allow_insecure: bool = False,
    ) -> None:
        self._http = HTTP(url, token, timeout=timeout, ca_file=ca_file, allow_insecure=allow_insecure)

    @staticmethod
    def _path(topic: str, partition: int | None = None) -> str:
        path = f"/v1/topics/{_name(topic)}/partitions"
        if partition is not None:
            path += f"/{bounded_int(partition, MAX_PARTITION)}"
        return path

    def partitions(self, topic: str) -> Iterator[int]:
        """Yield sorted partition IDs, fetching at most 100 per request."""
        path = self._path(topic)
        after: int | None = None
        topic_id: UUID | None = None

        def decode(value: object) -> tuple[UUID, list[int], int | None]:
            page = _object(value, {"topic_id", "partitions", "next_after"})
            rows = page["partitions"]
            if type(rows) is not list or len(rows) > MAX_PAGE_COUNT:
                raise ValueError("Invalid partition page size")
            items = [bounded_int(row, MAX_PARTITION) for row in rows]
            if items != sorted(set(items)) or after is not None and items and items[0] <= after:
                raise ValueError("Partition discovery did not advance")
            cursor = page["next_after"]
            if cursor is not None and (not items or bounded_int(cursor, MAX_PARTITION) != items[-1]):
                raise ValueError("Invalid partition discovery cursor")
            return _wire_uuid(page["topic_id"]), items, cursor

        while True:
            query = f"?limit={MAX_PAGE_COUNT}"
            if after is not None:
                query += f"&after={after}"
            current_topic, page, after = self._http.request("GET", path + query, decode=decode)
            if topic_id is not None and topic_id != current_topic:
                raise EventsProtocolError("Topic identity changed during discovery", code="protocol_error")
            topic_id = current_topic
            yield from page
            if after is None:
                return

    def publish(self, topic: str, *, partition: int, event_id: UUID | str, data: object) -> Event:
        """Commit one event or return its original receipt on an identical retry."""
        bounded_int(partition, MAX_PARTITION)
        path = self._path(topic, partition)
        canonical_id = _uuid(event_id)
        return self._http.request(
            "POST", path + "/events", body=_publish_body(canonical_id, data),
            expected=(200, 201), decode=lambda value: Event._decode(value, partition, expected_id=canonical_id),
        )

    def publish_batch(
        self, topic: str, *, partition: int,
        events: Iterable[PublishRecord | Mapping[str, object]],
    ) -> tuple[Event, ...]:
        """Commit a bounded, atomic partition batch and return receipts in input order."""
        bounded_int(partition, MAX_PARTITION)
        path = self._path(topic, partition)
        size = len(b'{"events":[]}')
        bodies: list[bytes] = []
        identities: dict[str, None] = {}
        for event in events:
            if len(bodies) == MAX_PAGE_COUNT:
                raise ValueError(f"Events batch exceeds {MAX_PAGE_COUNT} records")
            if isinstance(event, PublishRecord):
                event_id, data = event.event_id, event.data
            elif isinstance(event, Mapping) and event.keys() == {"event_id", "data"}:
                event_id, data = event["event_id"], event["data"]
            else:
                raise ValueError("Each event must have event_id and data")
            canonical_id = _uuid(event_id)
            if canonical_id in identities:
                raise ValueError("Duplicate event ID in publish batch")
            body = _publish_body(canonical_id, data)
            size += len(body) + bool(bodies)
            if size > MAX_BODY_BYTES:
                raise ValueError("Encoded Events batch exceeds the byte limit")
            identities[canonical_id] = None
            bodies.append(body)
        if not bodies:
            raise ValueError("Events batch must contain at least one record")

        def decode(value: object) -> tuple[Event, ...]:
            receipts = _object(value, {"events"})["events"]
            if type(receipts) is not list or len(receipts) != len(identities):
                raise ValueError("Incorrect number of batch receipts")
            result = tuple(Event._decode(item, partition, expected_id=event_id)
                           for item, event_id in zip(receipts, identities, strict=True))
            if len({event.topic_id for event in result}) != 1 or len({event.offset for event in result}) != len(result):
                raise ValueError("Inconsistent topic or offset in batch receipts")
            return result

        return self._http.request("POST", path + "/batches",
                                  body=b'{"events":[' + b",".join(bodies) + b"]}", decode=decode)

    def subscription(self, topic: str, name: str, *, partition: int) -> Subscription:
        """Access an operator-created subscription on one partition."""
        bounded_int(partition, MAX_PARTITION)
        path = self._path(topic, partition) + f"/subscriptions/{_name(name)}"
        return Subscription(self._http, path, partition)


def _lease_response(value: object) -> tuple[str, datetime]:
    values = _object(value, {"lease", "expires_at"})
    return _opaque(values["lease"]), _timestamp(values["expires_at"])


@dataclass(frozen=True, slots=True)
class Subscription:
    _http: HTTP = field(repr=False)
    _path: str
    partition: int

    def claim(self, *, claim_id: UUID | str | None = None) -> Lease:
        """Claim this partition. Retry a lost response once with the same claim ID."""
        identity = _uuid(claim_id) if claim_id is not None else str(uuid4())
        body = canonical_json({"claim_id": identity})
        try:
            credential, expires_at = self._http.request("POST", self._path + "/claim",
                                                        body=body, decode=_lease_response)
        except EventsUnknownOutcomeError as first:
            first.claim_id = identity
            if first.retry_after:
                raise
            try:
                credential, expires_at = self._http.request("POST", self._path + "/claim",
                                                            body=body, decode=_lease_response)
            except EventsError as second:
                raise first from second
        return Lease(self, credential, expires_at)


class Lease:
    """A claim that supports explicit renewal, polling and prefix acknowledgement."""

    def __init__(self, subscription: Subscription, credential: str, expires_at: datetime) -> None:
        self._subscription = subscription
        self._credential = credential
        self._expires_at = expires_at
        self._lock = Lock()
        self._closed = False

    @property
    def expires_at(self) -> datetime:
        with self._lock:
            return self._expires_at

    def __enter__(self) -> Lease:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            self.close()
        except EventsError:
            if exc_type is None:
                raise

    def _request(self, method: str, suffix: str, *, decode, body=None, expected=(200,)):
        if self._closed:
            raise EventsError("Events lease is closed", code="lease_lost")
        return self._subscription._http.request(
            method, self._subscription._path + suffix, decode=decode,
            body=body, lease=self._credential, expected=expected,
        )

    def poll(self, *, limit: int = MAX_PAGE_COUNT) -> Batch:
        """Read a bounded page after the committed subscription checkpoint."""
        bounded_int(limit, MAX_PAGE_COUNT, minimum=1)

        def decode(value: object) -> Batch:
            fields = _object(value, {"after", "events", "delivery"})
            after = offset_string(fields["after"])
            wire_events = fields["events"]
            if type(wire_events) is not list or len(wire_events) > limit:
                raise ValueError("Invalid Events page size")
            events = tuple(Event._decode(event, self._subscription.partition) for event in wire_events)
            if events:
                if (any(event.offset != after + i for i, event in enumerate(events, start=1))
                        or len({event.topic_id for event in events}) != 1
                        or len({event.event_id for event in events}) != len(events)):
                    raise ValueError("Poll must return a contiguous page for one topic")
                delivery = _opaque(fields["delivery"])
            else:
                delivery = None if fields["delivery"] is None else _opaque(fields["delivery"])
            return Batch(after, events, delivery, self)

        with self._lock:
            return self._request("GET", f"/events?limit={limit}", decode=decode)

    def renew(self) -> datetime:
        """Extend the claim and return its new expiry."""
        with self._lock:
            self._credential, self._expires_at = self._request(
                "POST", "/renew", body=b"{}", decode=_lease_response,
            )
            return self._expires_at

    def ack(self, batch: Batch, *, through: int) -> int:
        """Advance through an inclusive prefix of a page from this claim."""
        bounded_int(through, MAX_OFFSET, minimum=1)
        if (not isinstance(batch, Batch) or batch._lease is not self
                or not batch.events or batch.delivery is None
                or not batch.events[0].offset <= through <= batch.events[-1].offset):
            raise ValueError("Checkpoint must acknowledge a prefix of a page from this lease")

        def decode(value: object) -> int:
            offset = offset_string(_object(value, {"offset"})["offset"])
            if offset < through:
                raise ValueError("Checkpoint receipt precedes the acknowledged prefix")
            return offset

        with self._lock:
            return self._request("PUT", "/checkpoint",
                                 body=canonical_json({"delivery": batch.delivery, "through": str(through)}),
                                 decode=decode)

    def close(self) -> None:
        """Release this claim. Repeated calls after release have no effect."""
        with self._lock:
            if self._closed:
                return
            try:
                self._request("POST", "/release", body=b"{}", decode=lambda _: None, expected=(200, 204))
            except EventsError as error:
                if error.code != "lease_lost":
                    raise
            self._credential = ""
            self._closed = True

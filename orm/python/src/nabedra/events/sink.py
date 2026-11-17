"""Ingest Events into an append-only OLAP table through the public API."""
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import re
import signal
from threading import BoundedSemaphore, Event as StopEvent, Thread
from time import monotonic
from uuid import UUID

from ..client import Database
from ..errors import NabedraError, OverloadedError, StorageError, TransportError
from ..validation import validate_identifier
from ._client import Events, Lease, _name
from ._http import (
    MAX_PAGE_COUNT,
    MAX_PARTITION,
    EventsError,
    EventsTransportError,
    EventsUnknownOutcomeError,
    _endpoint,
    bounded_int,
    json_loads,
    positive_seconds,
)
from ._ingest import RetryLater, SinkPaused, Writer

_LOG = logging.getLogger(__name__)
_CONFIG_BYTES = 64 * 1024
_MAX_WORKERS = 32
_MAX_RANGE = 10_000


@dataclass(frozen=True, slots=True)
class _Endpoint:
    url: str
    token: str = field(repr=False)
    timeout: float
    ca_file: str | None
    allow_insecure: bool

    @classmethod
    def from_config(cls, value: dict) -> _Endpoint:
        name = value["token_env"]
        if type(name) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", name) is None:
            raise ValueError("Endpoint token_env must name an environment variable")
        token = os.environ.get(name)
        if not token:
            raise ValueError("Endpoint token environment variable is empty or unset")
        allow_insecure = value.get("allow_insecure", False)
        url = _endpoint(value["url"], allow_insecure)
        ca_file = value.get("ca_file")
        if ca_file is not None and (type(ca_file) is not str or not ca_file):
            raise ValueError("Endpoint ca_file must be a nonempty path")
        timeout = _duration(value.get("timeout", 3.0), maximum=30)
        return cls(url, token, timeout, ca_file, allow_insecure)


def _duration(value: object, *, maximum: float) -> float:
    result = positive_seconds(value)
    if not 0.05 <= result <= maximum:
        raise ValueError(f"Sink duration must be between 0.05 and {maximum} seconds")
    return result


def _fields(value: object, *, required: set[str], optional: set[str]) -> dict:
    if type(value) is not dict or not required <= value.keys() <= required | optional:
        raise ValueError(f"Invalid configuration object with required fields {', '.join(sorted(required))}")
    return value


@dataclass(frozen=True, slots=True)
class _Config:
    events: _Endpoint
    olap: _Endpoint
    database: str | None
    table: str
    table_id: str
    topic: str
    subscription: str
    start: int
    stop: int
    workers: int
    max_in_flight: int
    batch_size: int
    poll_interval: float
    receipt_wait: float
    renew_interval: float

    @classmethod
    def load(cls, path: Path) -> _Config:
        with path.open("rb") as stream:
            raw = stream.read(_CONFIG_BYTES + 1)
        if len(raw) > _CONFIG_BYTES:
            raise ValueError("Sink configuration exceeds 64 KiB")
        config = _fields(
            json_loads(raw), required={"events", "olap", "partitions"},
            optional={"workers", "max_in_flight", "batch_size", "poll_interval", "receipt_wait", "renew_interval"},
        )
        required = {"url", "token_env"}
        optional = {"ca_file", "allow_insecure", "timeout"}
        source = _fields(config["events"], required=required | {"topic", "subscription"}, optional=optional)
        destination = _fields(config["olap"], required=required | {"table", "table_id"},
                              optional=optional | {"database"})
        partition_range = _fields(config["partitions"], required={"start", "stop"}, optional=set())
        start = bounded_int(partition_range["start"], MAX_PARTITION)
        stop = bounded_int(partition_range["stop"], min(MAX_PARTITION + 1, start + _MAX_RANGE), minimum=start + 1)
        workers = bounded_int(config.get("workers", min(4, stop - start)),
                              min(_MAX_WORKERS, stop - start), minimum=1)
        in_flight = bounded_int(config.get("max_in_flight", workers), workers, minimum=1)
        batch_size = bounded_int(config.get("batch_size", MAX_PAGE_COUNT), MAX_PAGE_COUNT, minimum=1)
        database = destination.get("database")
        if "database" in destination and (type(database) is not str or
                re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", database) is None):
            raise ValueError("Invalid OLAP database selector")
        table_id = destination["table_id"]
        if type(table_id) is not str or str(UUID(table_id)) != table_id:
            raise ValueError("Destination table_id must be a canonical UUID")
        return cls(
            _Endpoint.from_config(source), _Endpoint.from_config(destination), database,
            validate_identifier(destination["table"]), table_id, _name(source["topic"]),
            _name(source["subscription"]), start, stop, workers, in_flight, batch_size,
            _duration(config.get("poll_interval", 0.5), maximum=30),
            _duration(config.get("receipt_wait", 2.0), maximum=30),
            _duration(config.get("renew_interval", 5.0), maximum=30),
        )


class _Heartbeat:
    """Renew an active claim during blocking OLAP I/O."""

    def __init__(self, lease: Lease, stop: StopEvent, interval: float) -> None:
        self._lease = lease
        self._stop = stop
        self._interval = interval
        self._done = StopEvent()
        self._failure: Exception | None = None
        self._thread = Thread(target=self._run, name="events-lease")

    def __enter__(self) -> _Heartbeat:
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._done.set()
        self._thread.join()

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                remaining = (self._lease.expires_at - datetime.now(timezone.utc)).total_seconds()
                delay = min(self._interval, max(0.05, remaining / 3))
                if self._done.wait(delay) or self._stop.is_set():
                    return
                self._lease.renew()
        except Exception as error:
            self._failure = error

    def check(self) -> None:
        if self._stop.is_set():
            raise RetryLater
        if self._failure is not None:
            raise self._failure
        if self._lease.expires_at <= datetime.now(timezone.utc):
            raise RetryLater


def _consume_partition(config: _Config, events: Events, writer: Writer, stop: StopEvent, partition: int) -> bool:
    if not writer.ready(partition):
        return False
    sub = events.subscription(config.topic, config.subscription, partition=partition)
    with sub.claim() as lease:
        batch = lease.poll(limit=config.batch_size)
        if not batch.events:
            return False
        with _Heartbeat(lease, stop, config.renew_interval) as heartbeat:
            if not writer.commit(partition, batch.events, heartbeat.check):
                return False
            writer.destination()
            heartbeat.check()
            lease.ack(batch, through=batch.events[-1].offset)
            writer.acknowledged(partition)
            return True


def _retryable(error: EventsError) -> bool:
    return (isinstance(error, (EventsTransportError, EventsUnknownOutcomeError))
            or error.status_code in (429, 503)
            or error.code in {"lease_busy", "lease_held", "lease_lost"})


def _consume(
    config: _Config, events: Events, writer: Writer, stop: StopEvent, partitions: range,
) -> bool:
    active = set(partitions)
    retry_at: dict[int, float] = {}
    try:
        while active and not stop.is_set():
            progressed = False
            for partition in partitions:
                if stop.is_set():
                    break
                if partition not in active or monotonic() < retry_at.get(partition, 0):
                    continue
                try:
                    if _consume_partition(config, events, writer, stop, partition):
                        retry_at.pop(partition, None)
                        progressed = True
                    else:
                        retry_at[partition] = monotonic() + config.poll_interval
                except EventsError as error:
                    if _retryable(error):
                        delay = error.retry_after if error.retry_after is not None else config.poll_interval
                        retry_at[partition] = monotonic() + max(config.poll_interval, delay)
                    else:
                        _LOG.error("Partition %d paused after Events error %s", partition, error.code)
                        active.remove(partition)
                except (RetryLater, OverloadedError, StorageError, TransportError):
                    retry_at[partition] = monotonic() + config.poll_interval
                except SinkPaused as error:
                    _LOG.error("Partition %d paused. %s", partition, error)
                    active.remove(partition)
                except NabedraError:
                    # The Database error message may contain user data or credential context.
                    _LOG.error("Partition %d paused after Database error", partition)
                    active.remove(partition)
            if not progressed:
                stop.wait(config.poll_interval)
        return bool(active)
    finally:
        writer.close()


def _run(config: _Config, stop: StopEvent) -> int:
    permits = BoundedSemaphore(config.max_in_flight)
    lanes = []
    for lane in range(config.workers):
        source = config.events
        events = Events(source.url, token=source.token, timeout=source.timeout,
                        ca_file=source.ca_file, allow_insecure=source.allow_insecure)
        destination = config.olap
        database = Database(destination.url, destination.token, mode="olap", database=config.database,
                            timeout=destination.timeout, ca_file=destination.ca_file)
        writer = Writer(database, config.table, config.table_id, permits, stop,
                        receipt_wait=config.receipt_wait, poll_interval=config.poll_interval)
        lanes.append((events, writer, range(config.start + lane, config.stop, config.workers)))
    with ThreadPoolExecutor(max_workers=config.workers, thread_name_prefix="events-sink") as pool:
        remaining = {pool.submit(_consume, config, events, writer, stop, partitions)
                     for events, writer, partitions in lanes}
        any_active = False
        try:
            while remaining:
                done, remaining = wait(remaining, timeout=0.5, return_when=FIRST_COMPLETED)
                for future in done:
                    any_active = future.result() or any_active
        finally:
            stop.set()
        return 0 if any_active else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stop = StopEvent()

    def request_stop(signum, frame):
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, request_stop)
    try:
        config = _Config.load(args.config)
        status = _run(config, stop)
    except (OSError, ValueError, NabedraError, RecursionError):
        _LOG.error("Could not start the Events sink. Check the endpoint, credentials and configuration.")
        status = 1
    raise SystemExit(status)


if __name__ == "__main__":
    main()

"""Confirm Events rows with the database request and receipt API."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import BoundedSemaphore, Event as StopEvent

from ..client import Database, RequestReceipt
from ..errors import (
    ConflictError,
    NotFoundError,
    OverloadedError,
    StorageError,
    TransportError,
    UnknownOutcomeError,
)
from ..table import Table
from ..validation import Row
from ._client import Event
from ._http import canonical_json

ROW_KEY = "event_key"
ROW_SCHEMA = {
    ROW_KEY: str,
    "partition": int,
    "offset": str,
    "published_at": str,
    "data": str,
}


class SinkPaused(Exception):
    """Operator intervention is required before this partition can resume."""


class RetryLater(Exception):
    """The current delivery can safely be retried later."""


@dataclass(frozen=True, slots=True)
class _Pending:
    receipt: RequestReceipt
    row_count: int


def _row(event: Event) -> Row:
    try:
        data = canonical_json(event.data).decode("utf-8")
    except (ValueError, TypeError, RecursionError):
        raise SinkPaused("Event contains JSON that cannot be written to OLAP") from None
    return {
        ROW_KEY: f"{event.topic_id}:{event.partition}:{event.event_id}",
        "partition": event.partition,
        "offset": str(event.offset),
        "published_at": event.published_at,
        "data": data,
    }


class Writer:
    """Hold one known outstanding request per partition until its receipt settles."""

    def __init__(
        self, database: Database, table_name: str, table_id: str, permits: BoundedSemaphore,
        stop: StopEvent, *, receipt_wait: float, poll_interval: float,
    ) -> None:
        self._database = database
        self._table_name = table_name
        self._table_id = table_id
        self._permits = permits
        self._stop = stop
        self._receipt_wait = receipt_wait
        self._poll_interval = poll_interval
        self._pending: dict[int, _Pending] = {}
        self._needs_reconciliation: set[int] = set()

    def destination(self) -> Table:
        try:
            table = self._database.table(self._table_name)
        except NotFoundError:
            raise SinkPaused("Configured destination table is missing") from None
        if table.id.lower() != self._table_id:
            raise SinkPaused("Destination table UUID changed")
        if table.key != ROW_KEY or table.schema != ROW_SCHEMA:
            raise SinkPaused("Destination table key or schema changed")
        return table

    def _settle(self, partition: int, *, wait: bool) -> str:
        pending = self._pending[partition]
        try:
            if wait:
                receipt = pending.receipt.wait(timeout=self._receipt_wait, poll_interval=self._poll_interval)
            else:
                receipt = pending.receipt.poll()
        except (TransportError, OverloadedError, StorageError):
            return "pending"
        except NotFoundError:
            # A receipt can expire or become unreachable. Its absence is never
            # evidence that the corresponding mutation failed to commit.
            receipt = None
        if receipt is not None and not receipt.terminal:
            return "pending"

        del self._pending[partition]
        self._permits.release()
        if receipt is None or receipt.state == "abandoned":
            return "reconcile"
        if receipt.state == "succeeded":
            if type(receipt.result) is not int or receipt.result != pending.row_count:
                raise SinkPaused("Database receipt has an unexpected inserted count")
            return "committed"

        error = receipt.error
        if error is not None:
            if error["code"] == "conflict" or error["outcome"] == "unknown":
                return "reconcile"
            if error["code"] in {"storage_unavailable", "overloaded"}:
                raise RetryLater
            if error["code"] == "not_found":
                self.destination()
        raise SinkPaused("Database rejected the event batch")

    def ready(self, partition: int) -> bool:
        """Observe an accepted write before allowing a new lease to submit one."""
        return partition not in self._pending or self._settle(partition, wait=False) != "pending"

    def _missing(self, expected: list[Row], check_lease: Callable[[], None]) -> list[Row]:
        table = self.destination()
        missing = []
        for row in expected:
            check_lease()
            try:
                existing = table.get(row[ROW_KEY])
            except NotFoundError:
                raise SinkPaused("Destination table disappeared during reconciliation") from None
            if existing is None:
                missing.append(row)
            elif existing != row:
                raise SinkPaused("Destination already has different data for this event identity")
        return missing

    def commit(self, partition: int, events: tuple[Event, ...], check_lease: Callable[[], None]) -> bool:
        """Try to commit the full delivery or defer it without advancing its checkpoint."""
        expected = [_row(event) for event in events]
        if partition in self._needs_reconciliation:
            rows = self._missing(expected, check_lease)
        else:
            rows = expected
        for _ in range(3):
            if not rows:
                return True
            if not self._permits.acquire(blocking=False):
                return False
            keep_permit = False
            try:
                table = self.destination()
                check_lease()
                try:
                    receipt = self._database.submit("table.insert_many", {"table_id": table.id, "rows": rows})
                except (ConflictError, UnknownOutcomeError):
                    self._needs_reconciliation.add(partition)
                except NotFoundError:
                    self.destination()
                    raise SinkPaused("Database rejected the configured table UUID") from None
                else:
                    self._pending[partition] = _Pending(receipt, len(rows))
                    self._needs_reconciliation.add(partition)
                    keep_permit = True
            finally:
                if not keep_permit:
                    self._permits.release()

            if keep_permit:
                state = self._settle(partition, wait=True)
                if state == "committed":
                    return True
                if state == "pending":
                    return False
            rows = self._missing(expected, check_lease)
            if rows and self._stop.wait(self._poll_interval):
                return False
        return not rows

    def acknowledged(self, partition: int) -> None:
        self._needs_reconciliation.discard(partition)

    def close(self) -> None:
        for _ in self._pending:
            self._permits.release()
        self._pending.clear()

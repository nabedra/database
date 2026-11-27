"""The OLAP sink's reconciliation of rows that already exist (EVENTS-9)."""

from threading import BoundedSemaphore, Event as StopEvent
import unittest
from uuid import UUID

from nabedra.events._client import Event
from nabedra.events._ingest import ROW_KEY, ROW_SCHEMA, SinkPaused, Writer, _row

TABLE_ID = str(UUID(int=7))
TOPIC = UUID(int=1)
EVENT = UUID(int=2)


class _Table:
    id = TABLE_ID
    key = ROW_KEY
    schema = ROW_SCHEMA

    def __init__(self, rows):
        self.rows = {row[ROW_KEY]: row for row in rows}

    def get(self, key):
        return self.rows.get(key)


class _Database:
    def __init__(self, table):
        self._table = table

    def table(self, name):
        return self._table


def _event(offset, published_at, data):
    return Event(topic_id=TOPIC, partition=0, offset=offset, event_id=EVENT,
                 published_at=published_at, data=data)


def _writer(rows):
    return Writer(_Database(_Table(rows)), "events", TABLE_ID, BoundedSemaphore(1), StopEvent(),
                  receipt_wait=1, poll_interval=0.01)


class SinkReconciliationTests(unittest.TestCase):
    def test_a_repeat_after_retention_with_the_same_data_is_already_ingested(self):
        original = _row(_event(5, "2026-11-01T00:00:00Z", {"order": "A-42"}))
        repeat = _row(_event(900, "2026-11-09T00:00:00Z", {"order": "A-42"}))
        missing = _writer([original])._missing([repeat], lambda: None)
        self.assertEqual(missing, [], "the original row stays and the repeat is acknowledged")

    def test_a_repeat_with_different_data_still_pauses(self):
        original = _row(_event(5, "2026-11-01T00:00:00Z", {"order": "A-42"}))
        changed = _row(_event(900, "2026-11-09T00:00:00Z", {"order": "B-7"}))
        with self.assertRaisesRegex(SinkPaused, "different data"):
            _writer([original])._missing([changed], lambda: None)

    def test_a_missing_row_is_written(self):
        row = _row(_event(5, "2026-11-01T00:00:00Z", {"order": "A-42"}))
        self.assertEqual(_writer([])._missing([row], lambda: None), [row])


if __name__ == "__main__":
    unittest.main()

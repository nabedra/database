#!/usr/bin/env python3

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from .history import Event, HistoryWriter, pair_operations, read_jsonl


class HistoryTests(unittest.TestCase):
    def test_json_lines_round_trip_and_pairing(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "history.jsonl"
            with HistoryWriter(path) as writer:
                started = writer.record("invoke", 3, "write", key="k1", value=5, request_id="r-1", node="n1")
                other = writer.record("invoke", 4, "read", key="k1")
                resolved = writer.record("info", 3, "write", key="k1", request_id="r-1", node="n1")
                writer.record("info", 0, "nemesis", value={"action": "pause"})
            with path.open(encoding="utf-8") as source:
                events = list(read_jsonl(source))

        self.assertEqual(events[:3], [started, other, resolved])
        self.assertEqual([event.index for event in events], [0, 1, 2, 3])
        times = [event.time_ns for event in events]
        self.assertEqual(times, sorted(times))
        self.assertGreater(times[0], 0)
        write, read = pair_operations(events)
        self.assertEqual((write.invoke, write.completion), (started, resolved))
        self.assertEqual((read.invoke, read.completion), (other, None))

    def test_malformed_history_and_impossible_pairing_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "line 1"):
            list(read_jsonl(['{"index":0,"type":"invoke","process":"three","f":"read"}']))
        first = Event(0, "invoke", 2, "write", key="one", request_id="same")
        with self.assertRaisesRegex(ValueError, "does not match"):
            pair_operations([first, Event(1, "ok", 2, "write", key="other", request_id="same")])
        with self.assertRaisesRegex(ValueError, "overlapping"):
            pair_operations([first, first])
        with self.assertRaisesRegex(ValueError, "without invoke"):
            pair_operations([Event(0, "ok", 2, "read")])

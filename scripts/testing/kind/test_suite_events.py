#!/usr/bin/env python3

import unittest

from .suite_events import check_queue
from ..realenv.history import Event


class QueueTests(unittest.TestCase):
    def history(self, replay):
        events = []

        def add(type, f, value, request_id=None, process=1):
            events.append(Event(len(events), type, process, f, value=value, request_id=request_id))

        for i in range(1, 4):
            event = {"event_id": str(i), "data": i, "offset": str(i)}
            add("invoke", "publish", {"event_id": str(i), "data": i}, str(i))
            add("ok", "publish", event, str(i))
        add("info", "nemesis", {"action": "wipe_pvc"}, process=0)
        add("invoke", "poll", None)
        add("ok", "poll", {"after": "0", "events": replay})
        return events

    def setUp(self):
        self.replay = [{"event_id": str(i), "data": i, "offset": str(i)} for i in range(1, 4)]

    def test_contiguous_acknowledged_replay(self):
        self.assertEqual(check_queue(self.history(self.replay)), 3)

    def test_lost_acknowledged_tail_is_detected_even_when_offsets_remain_contiguous(self):
        with self.assertRaisesRegex(AssertionError, "differs from acknowledged"):
            check_queue(self.history(self.replay[:-1]))

    def test_offset_gaps_and_reuse_are_detected(self):
        for offset in ("2", "4"):
            with self.subTest(offset=offset), self.assertRaisesRegex(AssertionError, "contiguous"):
                check_queue(self.history([*self.replay[:2], dict(self.replay[2], offset=offset)]))

    def test_invented_event_with_existing_offset_is_detected(self):
        with self.assertRaisesRegex(AssertionError, "differs from acknowledged"):
            check_queue(self.history([*self.replay[:2], dict(self.replay[2], data="different")]))

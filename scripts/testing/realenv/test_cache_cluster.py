#!/usr/bin/env python3

import importlib.util
from itertools import count
from pathlib import Path
import signal
import sys
from tempfile import TemporaryDirectory
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

from .history import HistoryWriter, pair_operations, read_jsonl

K03_FIXTURE = Path(__file__).resolve().parents[3] / "server/check/tests/fixtures/k03_history.jsonl"


def load_cache_cluster():
    # The existing full K03 acceptance path uses redis-py. Stub its imports so
    # the Fleet adapter can be tested without Docker or third-party packages.
    redis = ModuleType("redis")
    redis.Redis = Mock()
    redis_cluster = ModuleType("redis.cluster")
    redis_cluster.key_slot = Mock()
    errors = ModuleType("redis.exceptions")
    for name in ("ClusterDownError", "ConnectionError", "ClusterCrossSlotError",
                 "MovedError", "TimeoutError", "TryAgainError"):
        setattr(errors, name, type(name, (Exception,), {}))
    source = Path(__file__).resolve().parents[2] / "cache-acceptance" / "cluster.py"
    spec = importlib.util.spec_from_file_location("cache_acceptance_cluster", source)
    module = importlib.util.module_from_spec(spec)
    with (patch.dict(sys.modules, {
        "redis": redis, "redis.cluster": redis_cluster, "redis.exceptions": errors,
    }), patch.object(sys, "path", sys.path.copy())):
        spec.loader.exec_module(module)
    return module


def placement(*routes):
    return {"slots": [{"primary": {"node": node}, "epoch": epoch} for node, epoch in routes]}


def record_k03_fixture(path):
    cache = load_cache_cluster()
    fleet = cache.Fleet.__new__(cache.Fleet)
    key, independent = b"history:{0}", b"history:{1}"
    cache.key_slot.side_effect = {key: 0, independent: 1}.__getitem__
    fleet.client_process = 1
    client = Mock()
    client.set.return_value = True
    client.expire.return_value = True
    client.get.side_effect = [b"prior", b"successor", None, None]
    clock = count(1_000_000_000, 1_000_000)
    with patch.object(cache.time, "monotonic_ns", side_effect=clock), HistoryWriter(path) as writer:
        fleet.recorder = writer
        fleet.nemesis = cache.Nemesis(writer)
        fleet.placement = placement((1, 1), (2, 4))
        fleet.op(1, client, "set", key, b"prior", px=120_000)
        fleet.op(2, client, "set", independent, b"independent")
        fleet.record_marker("failover", key, 1, epoch=1)
        fleet.placement = placement((2, 2), (2, 4))
        fleet.record_marker("promote", key, 2, epoch=2)
        fleet.op(2, client, "get", key)
        fleet.op(2, client, "set", key, b"successor", px=120_000)
        fleet.op(2, client, "expire", key, 60)
        fleet.placement = placement((1, 3), (2, 4))
        fleet.record_marker("slot-move", key, 1, epoch=3)
        fleet.op(1, client, "get", key)
        fleet.record_marker("cold-reset", generation=2)
        fleet.placement = placement((1, 1), (2, 1))
        fleet.op(1, client, "get", key)
        fleet.op(2, client, "get", independent)


class CacheClusterTests(unittest.TestCase):
    def test_fleet_records_pause_client_ops_and_resume(self):
        cache = load_cache_cluster()
        fleet = cache.Fleet.__new__(cache.Fleet)
        fleet.placement = placement((2, 2))
        cache.key_slot.return_value = 0
        child = Mock()
        child.poll.return_value = None
        fleet.processes = {"w1": child}
        client = Mock()
        client.get.return_value = b"prior"
        client.set.side_effect = cache.MovedError()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "K03.jsonl"
            with HistoryWriter(path) as writer:
                fleet.recorder = writer
                fleet.nemesis = cache.Nemesis(writer)
                fleet.client_process = 1
                fleet.stop("w1")
                self.assertEqual(fleet.op(2, client, "get", b"history:{1}"), b"prior")
                fleet.resume("w1")
                with self.assertRaises(cache.MovedError):
                    fleet.op(1, client, "set", b"history:{1}", b"retired", px=120_000)
            with path.open() as source:
                history = pair_operations(read_jsonl(source))

        self.assertEqual([call.args[0] for call in child.send_signal.call_args_list],
                         [signal.SIGSTOP, signal.SIGCONT])
        client.set.assert_called_once_with(b"history:{1}", b"retired", px=120_000)
        self.assertEqual([op.completion.type for op in history], ["ok", "ok", "ok", "fail"])
        self.assertEqual([op.invoke.f for op in history], ["nemesis", "get", "nemesis", "set"])
        self.assertEqual(history[1].completion.value, {"value": "prior", "epoch": 2})
        retired = history[3].invoke
        self.assertEqual(retired.value, {"value": "retired", "at_ns": retired.time_ns + 120_000_000_000})
        self.assertIsNone(history[3].completion.value["epoch"])

    def test_unknown_outcome_moves_the_client_to_a_fresh_process(self):
        cache = load_cache_cluster()
        fleet = cache.Fleet.__new__(cache.Fleet)
        fleet.placement = None
        client = Mock()
        client.set.side_effect = [cache.TimeoutError(), True]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "K03.jsonl"
            with HistoryWriter(path) as writer:
                fleet.recorder = writer
                fleet.client_process = 1
                with self.assertRaises(cache.TimeoutError):
                    fleet.op(1, client, "set", b"k", b"one")
                fleet.op(1, client, "set", b"k", b"two")
            with path.open() as source:
                events = list(read_jsonl(source))
        self.assertEqual([(event.type, event.process) for event in events],
                         [("invoke", 1), ("info", 1), ("invoke", 2), ("ok", 2)])
        pair_operations(events)

    def test_k03_writer_matches_the_rust_replay_fixture(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "K03.jsonl"
            record_k03_fixture(path)
            self.assertEqual(path.read_text(), K03_FIXTURE.read_text())

    def test_ready_state_returns_without_waiting(self):
        cache = load_cache_cluster()
        already_ready = object()
        with patch.object(cache.time, "sleep") as sleep:
            self.assertIs(cache.wait("a ready controller", lambda: already_ready), already_ready)
            sleep.assert_not_called()

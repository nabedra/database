#!/usr/bin/env python3

import importlib.util
from pathlib import Path
import signal
import sys
from tempfile import TemporaryDirectory
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

from .history import HistoryWriter, pair_operations, read_jsonl


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


class CacheClusterTests(unittest.TestCase):
    def test_fleet_records_pause_client_ops_and_resume(self):
        cache = load_cache_cluster()
        fleet = cache.Fleet.__new__(cache.Fleet)
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
        self.assertEqual(history[1].completion.value, "prior")
        self.assertEqual(history[3].invoke.value, "retired")

    def test_ready_state_returns_without_waiting(self):
        cache = load_cache_cluster()
        already_ready = object()
        with patch.object(cache.time, "sleep") as sleep:
            self.assertIs(cache.wait("a ready controller", lambda: already_ready), already_ready)
            sleep.assert_not_called()

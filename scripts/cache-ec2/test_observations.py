import json
import os
from pathlib import Path
import tempfile
import time
import unittest

import observations


class GrowthObservations(unittest.TestCase):
    def test_worker_b02_uses_command_deltas_and_declared_throughput(self):
        first = {
            "at": 0, "identity": (2, 3), "worker": {"node": 1, "incarnation": "worker"},
            "counters": {5: (8, 20, 8), 11: (4, 1, 0)}, "cpu_us": 100_000,
            "cores": 1.5, "memory_max": 1000, "charged": 100, "rss": 200,
            "oldest_queued_ms": 2, "rx_bytes": 200, "tx_bytes": 50,
        }
        last = {**first, "at": 2, "counters": {5: (8, 60, 24), 11: (4, 2, 1)},
                "cpu_us": 1_600_000, "charged": 260, "rss": 320, "oldest_queued_ms": 12,
                "rx_bytes": 1000, "tx_bytes": 450}
        policy = {
            "slot_ops_per_second": 10, "cpu_cores": 2, "network_rx_bytes_per_second": 1000,
            "network_tx_bytes_per_second": 500,
        }
        report = observations.summarize(first, last, 400, policy)
        self.assertEqual((report["pressured"], report["hot_writes"], report["total_writes"]), (1, 16, 17))
        self.assertEqual(report["primary_digest"], observations.primary_digest([(11, 4), (5, 8)]))
        self.assertEqual(
            tuple(report[key] for key in ("memory_ratio", "rss_ratio", "oldest_queued_ms", "cpu_ratio",
                                          "rx_ratio", "tx_ratio")),
            (0.65, 0.32, 12, 0.5, 0.4, 0.4),
        )
        with self.assertRaisesRegex(ValueError, "counters or authority changed"):
            observations.summarize(first, {**last, "counters": {5: (9, 60, 24), 11: (4, 2, 1)}}, 400, policy)

    def test_worker_b01_inherits_cpu_capacity_and_refuses_stale_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service = root / "system.slice" / "nabedra-cache.service"
            service.mkdir(parents=True)
            (root / "cpuset.cpus.effective").write_text("0-3,5\n")
            (service / "cpu.max").write_text("max 100000\n")
            (service.parent / "cpu.max").write_text("200000 100000\n")
            self.assertEqual(observations.effective_cores(service), 2)
            path = root / "metrics.json"
            path.write_text("{}")
            self.assertEqual(observations.read_metrics(path), {})
            old = time.time() - 7
            os.utime(path, (old, old))
            with self.assertRaisesRegex(ValueError, "stale or oversized"):
                observations.read_metrics(path)

    def test_worker_sample_rejects_partial_actors_and_excludes_replica_counters(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            group = root / "system.slice" / "nabedra-cache.service"
            group.mkdir(parents=True)
            for path, value in (("cpu.stat", "usage_usec 20"), ("cpu.max", "max 100000"),
                                ("cpuset.cpus.effective", "0-1"), ("memory.max", "2000")):
                (group / path).write_text(value + "\n")
            net = root / "devices"
            interface = net / "eni-fixture" / "statistics"
            interface.mkdir(parents=True)
            (interface / "rx_bytes").write_text("200\n")
            (interface / "tx_bytes").write_text("100\n")
            metrics = root / "metrics.json"
            identity = {"generation": 3, "revision": 2}
            slot = {"slot": 31, "epoch": 7, "operations": 30, "write_operations": 14, "lease_headroom_ms": 60}
            replica = {**slot, "slot": 50}
            actor = {**identity, "slot_stats": [slot, replica], "primary_bytes": 500,
                     "replica_bytes": 200, "oldest_queued_us": 3000}
            state = {
                **identity, "worker": {"node": 1, "incarnation": "w1"}, "actors": [actor],
                "actor_count": 1, "rss_bytes": 800, "slots": [
                    {"slot": 31, "epoch": 7, "baseline": True, "serving_primary": True},
                    {"slot": 50, "epoch": 7, "baseline": True, "serving_primary": False},
                ],
            }
            metrics.write_text(json.dumps(state))
            sample = observations.sample("eni-fixture", metrics, group, net)
            self.assertEqual(sample["counters"], {31: (7, 30, 14)})
            self.assertEqual((sample["charged"], sample["oldest_queued_ms"]), (700, 3))
            metrics.write_text(json.dumps({**state, "actor_count": 2}))
            with self.assertRaisesRegex(ValueError, "incomplete worker actor metrics"):
                observations.sample("eni-fixture", metrics, group, net)

    def test_provider_A02_brackets_stack_and_committed_topology(self):
        voters = [f"i-{number:08x}" for number in range(1, 4)]
        workers = [f"i-{number:08x}" for number in range(4, 7)]
        outputs = {"Workers": "3", "DeploymentFingerprint": "1" * 64}
        outputs.update({f"Controller{number}": instance for number, instance in enumerate(voters, 1)})
        outputs.update({f"Worker{number}": instance for number, instance in enumerate(workers, 1)})
        controller = {"generation": 3, "revision": 23, "ready": True, "moves": 0, "draining": [], "nodes": {}}
        reports = {}
        for node, instance in enumerate(workers, 1):
            record = {"worker": {"node": node, "incarnation": f"worker-{node}"}, "primary_digest": str(node),
                      "primaries": 5461 + (node == 1)}
            controller["nodes"][str(node)] = {**record, "zone": f"zone-{node}"}
            reports[instance] = {
                **record, "generation": 3, "revision": 23, "pressured": 100, "total_writes": 30,
                "hot_writes": 15, "memory_ratio": .4, "rss_ratio": .5, "oldest_queued_ms": node,
                "cpu_ratio": .3, "rx_ratio": .4, "tx_ratio": .2,
            }
        policy = {
            "observation": {
                "interface": "ens5", "sample_seconds": 2, "slot_ops_per_second": 10, "cpu_cores": 2,
                "network_rx_bytes_per_second": 1000, "network_tx_bytes_per_second": 1000,
            },
            "observation_max_age_seconds": 60, "zone_cpu_ratio_limit": .9, "zone_rx_ratio_limit": .9,
            "zone_tx_ratio_limit": .9, "zone_rss_limit": .9,
            "fixed_monthly_cost_units": 40, "monthly_worker_cost_units": 20,
        }

        class Aws:
            def __init__(self, replacement=False):
                self.replacement = replacement
                self.stack_reads = 0

            def run(self, *arguments):
                if arguments != ("cloudformation", "describe-stacks", "--stack-name", "test-cache"):
                    raise AssertionError(arguments)
                self.stack_reads += 1
                visible = {**outputs}
                if self.replacement and self.stack_reads == 2:
                    visible["Worker1"] = "i-00000777"
                return {"Stacks": [{"StackStatus": "UPDATE_COMPLETE", "StackId": "stack-identity",
                                    "Outputs": [{"OutputKey": key, "OutputValue": value}
                                                for key, value in visible.items()]}]}

            def ssm(self, instance, commands, timeout):
                if instance == voters[0] and "local-topology" in commands[0]:
                    return json.dumps(controller)
                if instance in workers and "local-worker" in commands[0]:
                    return json.dumps(reports[instance])
                raise AssertionError(instance)

        identity, result = observations.collect(Aws(), "test-cache", policy, now=lambda: 1000)
        self.assertEqual(identity, "stack-identity")
        self.assertEqual(result["monthly_spend_units"], 100)
        self.assertEqual(result["hottest_slot_write_share"], 15 / 90)
        self.assertEqual(result["slots_under_pressure_ratio"], 300 / 16384)
        self.assertEqual(result["oldest_queued_ms"], 3)
        self.assertTrue(result["zone_reserve_ok"])
        with self.assertRaisesRegex(ValueError, "instance IDs changed"):
            observations.collect(Aws(replacement=True), "test-cache", policy, now=lambda: 1000)
        reports[workers[0]]["primary_digest"] = "old routing"
        with self.assertRaisesRegex(ValueError, "committed placement"):
            observations.collect(Aws(), "test-cache", policy, now=lambda: 1000)

    def test_reset_or_resize_starts_new_sustained_window(self):
        observation = {"observed_at": 40, "generation": 5, "revision": 12, "groups": 3}
        existing = observation | {"stack_id": "stack", "observed_at": 5}
        points = observations.retain(
            [existing, existing, existing | {"generation": 4}, existing | {"stack_id": "other"},
             existing | {"revision": 11}],
            observation, "stack", 60,
        )
        self.assertEqual([point["observed_at"] for point in points], [5, 40])


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3

import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

from .faults import Faults
from ..realenv.history import HistoryWriter, read_jsonl


class KindFaultTests(unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(TemporaryDirectory())
        self.path = Path(directory) / "history.jsonl"
        history = self.enterContext(HistoryWriter(self.path))
        self.cluster = Mock(name="cluster")
        self.cluster.name = "th-test"
        self.docker = Mock()
        self.faults = Faults(self.cluster, history, run=self.docker)
        self.node = "th-test-worker"
        self.peer = "th-test-worker2"

    def test_delete_drain_pause_and_heal(self):
        self.faults.delete_pod("events-0")
        self.faults.drain(self.node)
        self.faults.pause(self.node)
        self.faults.heal_all()
        self.assertEqual([call.args for call in self.cluster.kube.call_args_list], [
            ("delete", "pod", "events-0", "--grace-period=0", "--force", "--wait=true", "--timeout=60s"),
            ("drain", self.node, "--ignore-daemonsets", "--delete-emptydir-data", "--timeout=90s"),
            ("uncordon", self.node),
        ])
        self.assertEqual([call.args[0] for call in self.docker.call_args_list],
                         [["docker", "pause", self.node], ["docker", "unpause", self.node]])
        with self.path.open() as source:
            self.assertEqual([e.value["action"] for e in read_jsonl(source) if e.type == "ok"],
                             ["delete_pod", "drain", "pause_node", "unpause_node", "uncordon", "heal_all"])

    def test_partition_uses_peer_node_ip_and_pod_cidr_in_both_directions(self):
        self.cluster.kube.return_value = json.dumps({
            "spec": {"podCIDR": "10.244.3.0/24"},
            "status": {"addresses": [{"type": "InternalIP", "address": "172.18.0.3"}]},
        })
        self.faults.partition(self.node, self.peer)
        self.faults.heal_all()
        self.cluster.kube.assert_called_once_with("get", "node", self.peer, "-o", "json")
        commands = [call.args[0] for call in self.docker.call_args_list]
        rules = [args[7:] for args in commands[:8]]
        self.assertEqual([rule[:3] for rule in rules], [
            [chain, direction, address]
            for chain, direction in (("INPUT", "-s"), ("OUTPUT", "-d"), ("FORWARD", "-s"), ("FORWARD", "-d"))
            for address in ("172.18.0.3", "10.244.3.0/24")
        ])
        prefix = ["docker", "exec", self.node, "iptables", "-w", "5"]
        self.assertEqual(commands[:8], [prefix + ["-I"] + rule for rule in rules])
        self.assertEqual(commands[8:], [prefix + ["-D"] + rule for rule in reversed(rules)])
        self.assertTrue(all(rule[-2:] == ["-j", "DROP"] for rule in rules))
        self.assertEqual(len({rule[6] for rule in rules}), 1)

    def test_wrong_node_is_refused(self):
        for node in ("other-cluster-worker", "th-test-other-worker"):
            with self.subTest(node=node), self.assertRaises(ValueError):
                self.faults.pause(node)
        self.docker.assert_not_called()

    def simulate_claims(self, replacement_vol):
        old = {"metadata": {"uid": "first"}, "spec": {"volumeName": "pvc-old"}}
        fresh = {"metadata": {"uid": "second"}, "spec": {"volumeName": replacement_vol}}
        self.cluster.kube.side_effect = [
            json.dumps({"spec": {"replicas": 3}}), json.dumps(old),
            "", "", "", "", "", "", json.dumps(fresh),
        ]

    def test_wipe_waits_for_delete_before_restarting(self):
        self.simulate_claims("pvc-new")

        def prepare():
            self.assertEqual(self.cluster.kube.call_args.args[:4], ("wait", "--for=delete", "pv", "pvc-old"))

        self.assertEqual(self.faults.wipe_pvc("events", 2, before_restart=prepare), "pvc-new")
        self.assertEqual([call.args[:4] for call in self.cluster.kube.call_args_list], [
            ("get", "statefulset", "events", "-o"),
            ("get", "pvc", "data-events-2", "-o"),
            ("scale", "statefulset", "events", "--replicas=2"),
            ("wait", "--for=delete", "pod", "events-2"),
            ("delete", "pvc", "data-events-2", "--wait=true"),
            ("wait", "--for=delete", "pv", "pvc-old"),
            ("scale", "statefulset", "events", "--replicas=3"),
            ("rollout", "status", "statefulset/events", "--timeout=120s"),
            ("get", "pvc", "data-events-2", "-o"),
        ])

    def test_wipe_detects_reused_volume(self):
        self.simulate_claims("pvc-old")
        with self.assertRaisesRegex(AssertionError, "reused"):
            self.faults.wipe_pvc("events", 2)

    def test_partial_partition_can_be_healed(self):
        self.cluster.kube.return_value = json.dumps({
            "spec": {"podCIDR": "10.244.3.0/24"},
            "status": {"addresses": [{"type": "InternalIP", "address": "172.18.0.3"}]},
        })
        self.docker.side_effect = [None, subprocess.CalledProcessError(1, "iptables"), None]
        with self.assertRaises(subprocess.CalledProcessError):
            self.faults.partition(self.node, self.peer)
        first = self.docker.call_args_list[0].args[0]
        self.faults.heal_all()
        self.assertEqual(self.docker.call_args.args[0], first[:6] + ["-D"] + first[7:])

#!/usr/bin/env python3
"""Inject and heal faults inside an owned kind cluster."""

from ipaddress import IPv4Address, IPv4Network
import json
import re
import subprocess
from uuid import uuid4

from .manifests import VOLUME
from ..realenv.nemesis import Nemesis


class Faults(Nemesis):
    def __init__(self, cluster, history, *, run=subprocess.run):
        super().__init__(history, run=run)
        self.cluster = cluster

    def delete_pod(self, pod):
        with self.fault("delete_pod", pod, partial=True):
            self.cluster.kube("delete", "pod", pod, "--grace-period=0", "--force",
                              "--wait=true", "--timeout=60s")

    def _node(self, node):
        if not re.fullmatch(re.escape(self.cluster.name) + r"-(?:control-plane|worker[2-4]?)", node):
            raise ValueError(f"{node} is outside the {self.cluster.name} kind cluster")

    def drain(self, node):
        self._node(node)
        self.undo[("drain", node)] = lambda: self.uncordon(node)
        with self.fault("drain", node, partial=True):
            self.cluster.kube("drain", node, "--ignore-daemonsets", "--delete-emptydir-data", "--timeout=90s")

    def uncordon(self, node):
        self._node(node)
        with self.fault("uncordon", node):
            self.cluster.kube("uncordon", node)
        self.undo.pop(("drain", node), None)

    def pause(self, node):
        self._node(node)
        with self.fault("pause_node", node):
            key = ("pause", node)
            self.undo[key] = lambda: self.unpause(node)
            try:
                self.command("docker", "pause", node)
            except (OSError, subprocess.CalledProcessError):
                self.undo.pop(key, None)
                raise

    def unpause(self, node):
        self._node(node)
        with self.fault("unpause_node", node):
            self.command("docker", "unpause", node)
        self.undo.pop(("pause", node), None)

    def partition(self, node, peer):
        """Drop node and Pod traffic to and from one peer on kind's IPv4 network."""
        self._node(node)
        self._node(peer)
        if node == peer:
            raise ValueError("partition needs two different kind nodes")
        data = json.loads(self.cluster.kube("get", "node", peer, "-o", "json"))
        peer_ip = str(IPv4Address(next(address["address"] for address in data["status"]["addresses"]
                                      if address["type"] == "InternalIP")))
        peer_cidr = str(IPv4Network(data["spec"]["podCIDR"]))
        tag = f"nabedra-{uuid4().hex[:12]}"
        with self.fault("partition", node, partial=True, peer=peer, tag=tag):
            for chain, direction in (("INPUT", "-s"), ("OUTPUT", "-d"), ("FORWARD", "-s"), ("FORWARD", "-d")):
                for address in (peer_ip, peer_cidr):
                    rule = (chain, direction, address, "-m", "comment", "--comment", tag, "-j", "DROP")
                    try:
                        self._iptables(node, "-I", rule)
                    except subprocess.TimeoutExpired:
                        self._track_rule(node, rule)
                        raise
                    self._track_rule(node, rule)

    def _track_rule(self, node, rule):
        self.undo[("iptables", node, rule)] = lambda: self._remove_rule(node, rule)

    def _iptables(self, node, operation, rule):
        self.command("docker", "exec", node, "iptables", "-w", "5", operation, *rule)

    def _remove_rule(self, node, rule):
        with self.fault("clear_partition_rule", node, rule=rule):
            self._iptables(node, "-D", rule)
        self.undo.pop(("iptables", node, rule), None)

    def wipe_pvc(self, stateful_set, ordinal, *, before_restart=None):
        """Replace the last replica's Pod and PVC while retaining its peers."""
        pod = f"{stateful_set}-{ordinal}"
        claim = f"{VOLUME}-{pod}"
        with self.fault("wipe_pvc", pod, partial=True, claim=claim):
            document = json.loads(self.cluster.kube("get", "statefulset", stateful_set, "-o", "json"))
            replicas = document["spec"]["replicas"]
            if replicas < 2 or ordinal != replicas - 1:
                raise ValueError("PVC wipe must target the last replica of a multi-replica StatefulSet")
            old_claim = json.loads(self.cluster.kube("get", "pvc", claim, "-o", "json"))
            old_volume = old_claim["spec"]["volumeName"]
            self.cluster.kube("scale", "statefulset", stateful_set, f"--replicas={ordinal}")
            self.cluster.kube("wait", "--for=delete", "pod", pod, "--timeout=90s")
            self.cluster.kube("delete", "pvc", claim, "--wait=true", "--timeout=90s")
            self.cluster.kube("wait", "--for=delete", "pv", old_volume, "--timeout=90s")
            if before_restart:
                before_restart()
            self.cluster.kube("scale", "statefulset", stateful_set, f"--replicas={replicas}")
            self.cluster.kube("rollout", "status", f"statefulset/{stateful_set}", "--timeout=120s", timeout=130)
            fresh = json.loads(self.cluster.kube("get", "pvc", claim, "-o", "json"))
            if (fresh["metadata"]["uid"] == old_claim["metadata"]["uid"]
                    or fresh["spec"]["volumeName"] == old_volume):
                raise AssertionError(f"{claim} reused the wiped claim or volume")
            return fresh["spec"]["volumeName"]

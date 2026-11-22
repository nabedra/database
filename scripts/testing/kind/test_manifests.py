#!/usr/bin/env python3

import json
import unittest
from unittest.mock import Mock

from .cluster import Cluster, NODE_IMAGE
from .manifests import render


class ManifestTests(unittest.TestCase):
    def test_cluster_commands_and_worker_count(self):
        run = Mock()
        cluster = Cluster("real-test", kubeconfig="/temp/test.kubeconfig", run=run)
        cluster.up(4)
        cluster.load_image("events:ci")
        cluster.down()
        up, storage, image, down = run.call_args_list
        self.assertEqual(up.args[0][1:], [
            "create", "cluster", "--name", "real-test", "--config", "-", "--wait", "180s",
            "--kubeconfig", "/temp/test.kubeconfig",
        ])
        nodes = json.loads(up.kwargs["input"])["nodes"]
        self.assertEqual([node["role"] for node in nodes], ["control-plane"] + ["worker"] * 4)
        self.assertTrue(all(node["image"] == NODE_IMAGE for node in nodes))
        self.assertEqual(storage.args[0][3:], ["--context", "kind-real-test", "get", "storageclass", "standard"])
        self.assertEqual(image.args[0][1:], ["load", "docker-image", "--name", "real-test", "events:ci"])
        self.assertEqual(down.args[0][1:], ["delete", "cluster", "--name", "real-test",
                                          "--kubeconfig", "/temp/test.kubeconfig"])

    def test_stateful_pods_match_dns_claim_resources_and_replacement(self):
        service, replacement, stateful_set = render("test-events:sha")
        spec = stateful_set["spec"]
        self.assertEqual((service["kind"], replacement["kind"], stateful_set["kind"]),
                         ("Service", "Service", "StatefulSet"))
        self.assertEqual(service["metadata"]["name"], spec["serviceName"])
        self.assertEqual(service["spec"]["clusterIP"], "None")
        self.assertTrue(service["spec"]["publishNotReadyAddresses"])
        self.assertEqual(service["spec"]["selector"], spec["template"]["metadata"]["labels"])
        self.assertEqual(replacement["spec"]["selector"], {"statefulset.kubernetes.io/pod-name": "events-2"})
        self.assertEqual(spec["replicas"], 3)
        self.assertEqual(spec["podManagementPolicy"], "Parallel")
        self.assertEqual(spec["persistentVolumeClaimRetentionPolicy"]["whenScaled"], "Retain")
        [pvc] = spec["volumeClaimTemplates"]
        self.assertEqual(pvc["kind"], "PersistentVolumeClaim")
        self.assertEqual(pvc["spec"]["storageClassName"], "standard")
        self.assertEqual(pvc["spec"]["accessModes"], ["ReadWriteOnce"])
        pod = spec["template"]["spec"]
        [broker] = pod["containers"]
        self.assertEqual(broker["image"], "test-events:sha")
        self.assertEqual(broker["imagePullPolicy"], "Never")
        self.assertIn("requests", broker["resources"])
        self.assertIn("${HOSTNAME}.json", broker["args"][0])
        self.assertIn({"name": pvc["metadata"]["name"], "mountPath": "/var/lib/nabedra-events"},
                      broker["volumeMounts"])
        [rule] = pod["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
        self.assertEqual(rule["topologyKey"], "kubernetes.io/hostname")
        self.assertEqual(rule["labelSelector"], spec["selector"])

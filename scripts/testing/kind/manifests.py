#!/usr/bin/env python3
"""Render the Events fixture for kind.

The ORM KubernetesProvider creates a Secret and a single OLTP worker Pod
with restartPolicy Never in start_node. It has no StatefulSet, Service, or
PVC rendering path. This minimal renderer uses the corresponding Pod
fields for the image, arguments, environment, mounts, and resources.
"""

import argparse
import json


NAME = "events"
VOLUME = "data"
MOUNT = "/var/lib/nabedra-events"
CONFIG = "/etc/nabedra-events"
PORTS = {"http": 8080, "peer": 8081}


def claim_template():
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": VOLUME},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": "standard",
            "resources": {"requests": {"storage": "1Gi"}},
        },
    }


def render(image):
    labels = {"app": NAME}
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": NAME},
        "spec": {
            "selector": labels,
            "clusterIP": "None",
            "publishNotReadyAddresses": True,
            "ports": [{"name": name, "port": port, "targetPort": name} for name, port in PORTS.items()],
        },
    }
    replacement = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": f"{NAME}-replacement"},
        "spec": {
            "selector": {"statefulset.kubernetes.io/pod-name": f"{NAME}-2"},
            "publishNotReadyAddresses": True,
            "ports": [{"name": "peer", "port": PORTS["peer"], "targetPort": "peer"}],
        },
    }
    volume_mount = {"name": VOLUME, "mountPath": MOUNT}
    stateful_set = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": NAME},
        "spec": {
            "serviceName": NAME,
            "replicas": 3,
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": labels},
            "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Retain", "whenScaled": "Retain"},
            "volumeClaimTemplates": [claim_template()],
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
                        "labelSelector": {"matchLabels": labels}, "topologyKey": "kubernetes.io/hostname",
                    }]}},
                    "volumes": [{"name": "config", "secret": {"secretName": NAME}}],
                    "initContainers": [{
                        "name": "volume-permissions", "image": image, "imagePullPolicy": "Never",
                        "command": ["chown", "10001:10001", MOUNT],
                        "securityContext": {"runAsUser": 0},
                        "volumeMounts": [volume_mount],
                    }],
                    "containers": [{
                        "name": "broker", "image": image, "imagePullPolicy": "Never",
                        "command": ["/bin/sh", "-ec"],
                        "args": [f'exec /usr/local/bin/nabedra-events serve --config "{CONFIG}/${{HOSTNAME}}.json"'],
                        "env": [{"name": "TOKIO_WORKER_THREADS", "value": "1"}],
                        "ports": [{"name": name, "containerPort": port} for name, port in PORTS.items()],
                        "readinessProbe": {"tcpSocket": {"port": "http"}, "periodSeconds": 1},
                        "resources": {"requests": {"cpu": "250m", "memory": "256Mi"},
                                      "limits": {"memory": "512Mi"}},
                        "volumeMounts": [
                            volume_mount, {"name": "config", "mountPath": CONFIG, "readOnly": True},
                        ],
                    }],
                },
            },
        },
    }
    return [service, replacement, stateful_set]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="nabedra-events:ci")
    args = parser.parse_args()
    print(json.dumps({"apiVersion": "v1", "kind": "List", "items": render(args.image)}, indent=2))


if __name__ == "__main__":
    main()

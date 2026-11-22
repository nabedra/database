#!/usr/bin/env python3
"""Manage a local kind cluster with the default local-path provisioner."""

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory
from uuid import uuid4


# Kubernetes 1.34 node image from the pinned kind release.
NODE_IMAGE = "kindest/node:v1.34.11@sha256:44e222ee2132dab25ff87301682f89eb82c7880ea3a1bf543bfe9708fd08d67d"


def config(workers):
    return {
        "apiVersion": "kind.x-k8s.io/v1alpha4",
        "kind": "Cluster",
        "nodes": [{"role": role, "image": NODE_IMAGE} for role in ["control-plane", *["worker"] * workers]],
    }


class Cluster:
    def __init__(self, name, *, kubeconfig=None, run=subprocess.run):
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,39}", name):
            raise ValueError("kind name must be a lowercase DNS label of up to 40 characters")
        self.name = name
        self.kubeconfig = kubeconfig
        self.run = run

    @property
    def kind(self):
        return os.environ.get("NABEDRA_KIND", "kind")

    def up(self, workers):
        kubeconfig = ("--kubeconfig", str(self.kubeconfig)) if self.kubeconfig else ()
        self.run([self.kind, "create", "cluster", "--name", self.name, "--config", "-",
                  "--wait", "180s", *kubeconfig], input=json.dumps(config(workers)),
                 text=True, check=True, timeout=420)
        self.kube("get", "storageclass", "standard")

    def down(self):
        kubeconfig = ("--kubeconfig", str(self.kubeconfig)) if self.kubeconfig else ()
        self.run([self.kind, "delete", "cluster", "--name", self.name, *kubeconfig],
                 check=True, timeout=120)

    def load_image(self, *images):
        self.run([self.kind, "load", "docker-image", "--name", self.name, *images], check=True, timeout=300)

    def kubectl(self, *args):
        kubeconfig = ("--kubeconfig", str(self.kubeconfig)) if self.kubeconfig else ()
        return ["kubectl", *kubeconfig, "--context", f"kind-{self.name}", *args]

    def kube(self, *args, input=None, timeout=125):
        return self.run(self.kubectl(*args), input=input, text=True,
                        capture_output=True, check=True, timeout=timeout).stdout

    def apply(self, *objects):
        self.kube("apply", "-f", "-", input=json.dumps({
            "apiVersion": "v1", "kind": "List", "items": list(objects),
        }))


@contextmanager
def temporary_cluster(workers):
    with TemporaryDirectory(prefix="nabedra-kind-") as directory:
        cluster = Cluster(f"nabedra-test-{uuid4().hex[:10]}", kubeconfig=Path(directory) / "config")
        try:
            cluster.up(workers)
            yield cluster
        finally:
            cluster.down()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", default="nabedra-realenv")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("up").add_argument("--nodes", type=int, choices=(3, 4), default=3,
                                          help="number of workers, plus one control plane")
    commands.add_parser("down")
    commands.add_parser("load-image").add_argument("images", nargs="+")
    args = parser.parse_args()
    cluster = Cluster(args.name)
    if args.command == "up":
        cluster.up(args.nodes)
    elif args.command == "down":
        cluster.down()
    else:
        cluster.load_image(*args.images)


if __name__ == "__main__":
    main()

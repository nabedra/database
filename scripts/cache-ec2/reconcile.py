#!/usr/bin/env python3
"""Reconcile worker placement through the authenticated private operator API."""
import argparse
import hashlib
import json
import socket
import ssl
import subprocess
import sys
import time
import uuid
from collections import Counter
from pathlib import Path

SLOTS = 16384
OPERATOR = Path("/etc/nabedra-cache/operator")
BINARY = Path("/usr/local/bin/nabedra-db")
NIL = "00000000-0000-0000-0000-000000000000"


class AdminError(RuntimeError):
    pass


class Operator:
    def __init__(self, binary, config, deadline):
        self.binary = binary
        self.config = config
        self.deadline = deadline

    def run(self, *command, operation=None):
        args = [self.binary, "cache", "admin", "--config", self.config]
        if operation is not None:
            args.extend(["--operation", str(operation)])
        try:
            result = subprocess.run(
                [*args, *map(str, command)], capture_output=True, text=True,
                timeout=min(8, max(0.1, self.deadline - time.monotonic())),
            )
        except subprocess.TimeoutExpired as error:
            raise AdminError("operator API request timed out") from error
        if result.returncode:
            raise AdminError(result.stderr.strip())
        return json.loads(result.stdout)

    def status(self):
        return self.run("status")["status"]

    def wait(self, predicate, description):
        last_problem = description
        while time.monotonic() < self.deadline:
            try:
                status = self.status()
                if predicate(status):
                    return status
            except AdminError as error:
                last_problem = str(error)
            time.sleep(2)
        raise TimeoutError(f"{description}: {last_problem}")


def operation(*fields):
    return uuid.uuid5(uuid.NAMESPACE_URL, "nabedra-cache/deploy-v1/" + "/".join(map(str, fields)))


def enrolled(topology, node):
    member = topology["workers"].get(str(node))
    return member is not None and member["id"]["incarnation"] != NIL and node not in topology["drained"]


def owner(slot):
    return slot["primary"]["node"] if slot["primary"] else None


def replica_placement(topology):
    for slot in topology["slots"]:
        primary, replica = slot["primary"], slot["replica"]
        if primary is None or replica is None or primary["node"] == replica["node"]:
            return False
        left = topology["workers"].get(str(primary["node"]))
        right = topology["workers"].get(str(replica["node"]))
        if left is None or right is None or left["id"] != primary or right["id"] != replica:
            return False
        if left["zone"] == right["zone"]:
            return False
    return True


def ready(topology, workers):
    expected_nodes = set(range(1, workers + 1))
    slots = topology["slots"]
    return (
        len(slots) == SLOTS
        and set(map(owner, slots)) == expected_nodes
        and all(slot["moving"] is None for slot in slots)
        and replica_placement(topology)
        and topology.get("ready_primaries") == SLOTS
        and topology.get("ready_replicas") == SLOTS
    )


def ranges(workers):
    for index in range(workers):
        start = (index * SLOTS + workers - 1) // workers
        end = ((index + 1) * SLOTS + workers - 1) // workers - 1
        yield start, end, index + 1


def enroll(operator, workers, topology):
    for node in range(1, workers + 1):
        if enrolled(topology, node):
            continue
        path = operator.config.parent / f"member-{node}.toml"
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        old = topology["workers"].get(str(node), {}).get("id", {}).get("incarnation")
        identifier = operation("enroll", topology["generation"], topology["revision"], node, old, digest)
        operator.run("enroll", "--member", path, operation=identifier)
    return operator.wait(
        lambda status: all(enrolled(status, node) for node in range(1, workers + 1)),
        "workers did not enroll",
    )


def assign(operator, workers, topology):
    generation = topology["generation"]
    if all(owner(slot) is None for slot in topology["slots"]):
        operator.run("init", "--groups", workers, operation=operation("init", generation, workers))
        return
    for start, end, target in ranges(workers):
        selected = topology["slots"][start:end + 1]
        if all(owner(slot) == target and slot["moving"] is None for slot in selected):
            continue
        # Hash the range's current placement, so later grow/drain cycles can
        # reapply the same target without reusing a completed operation ID.
        placement = [(slot["epoch"], owner(slot)) for slot in selected]
        digest = hashlib.sha256(json.dumps(placement, separators=(",", ":")).encode()).hexdigest()
        identifier = operation("move", generation, workers, start, end, digest)
        try:
            operator.run("rebalance", "--first", start, "--last", end, "--target", target, operation=identifier)
        except AdminError as error:
            # A deployment retry may encounter an operation that remains
            # committed while individual slots change their epochs.
            if "pending move" not in str(error):
                raise
    operator.wait(
        lambda status: all(
            owner(status["slots"][index]) == target and status["slots"][index]["moving"] is None
            for first, last, target in ranges(workers)
            for index in range(first, last + 1)
        ),
        "rebalance did not restore the requested ranges",
    )


def drain(operator, workers, prior_workers):
    topology = operator.wait(lambda status: bool(status["workers"]), "topology is unavailable")
    removing = set(range(workers + 1, prior_workers + 1))
    for node in sorted(removing):
        if node in topology["drained"]:
            continue
        incarnation = topology["workers"][str(node)]["id"]["incarnation"]
        operation_id = operation("drain", topology["generation"], node, incarnation)
        operator.run("drain", "--worker", node, operation=operation_id)
    return operator.wait(
        lambda status: removing <= set(status["drained"]) and ready(status, workers),
        "controller did not prove the workers drained and surviving placement verified",
    )


def probe_endpoints(topology, controller_ca):
    context = ssl.create_default_context(cafile=str(controller_ca))
    for _, member in sorted(topology["workers"].items()):
        if member["id"]["node"] in topology["draining"]:
            continue
        host, port = member["client_host"], member["client_port"]
        with socket.create_connection((host, port), timeout=3) as connection:
            with context.wrap_socket(connection, server_hostname=host) as peer:
                peer.sendall(b"*1\r\n$4\r\nPING\r\n")
                with peer.makefile("rb") as stream:
                    response = stream.readline(128)
                if not response.startswith(b"-NOAUTH "):
                    raise RuntimeError("private TLS cache endpoint failed its pre-authentication check")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("apply", "drain", "status"))
    parser.add_argument("--workers", type=int, choices=(3, 6, 12), required=True)
    parser.add_argument("--prior-workers", type=int, choices=(3, 6, 12), default=3)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--binary", type=Path, default=BINARY)
    parser.add_argument("--config", type=Path, default=OPERATOR / "node.toml")
    args = parser.parse_args()
    started = time.monotonic()
    operator = Operator(args.binary, args.config, started + args.timeout)
    if args.action == "apply":
        topology = enroll(operator, args.workers, operator.wait(lambda _: True, "controller quorum unavailable"))
        assign(operator, args.workers, topology)
        topology = operator.wait(lambda status: ready(status, args.workers), "cache replica readiness failed")
        probe_endpoints(topology, args.config.parent / "ca.pem")
    elif args.action == "drain":
        if args.workers >= args.prior_workers:
            parser.error("drain requires a smaller worker count")
        topology = drain(operator, args.workers, args.prior_workers)
    else:
        topology = operator.wait(lambda _: True, "controller quorum unavailable")
    print(json.dumps({
        "ready": ready(topology, args.workers),
        "generation": topology["generation"],
        "revision": topology["revision"],
        "readyPrimaries": topology.get("ready_primaries"),
        "readyReplicas": topology.get("ready_replicas"),
        "groups": dict(Counter(map(owner, topology["slots"]))),
        "drained": topology["drained"],
        "incarnations": {key: node["id"]["incarnation"] for key, node in topology["workers"].items()},
        "fingerprints": {key: [bytes(pin).hex() for pin in node["peer"]["fingerprints"]]
                         for key, node in topology["workers"].items()},
        "seconds": round(time.monotonic() - started, 3),
    }, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (TimeoutError, AdminError, OSError, ValueError) as problem:
        print(f"Cache operator: {problem}", file=sys.stderr)
        sys.exit(1)

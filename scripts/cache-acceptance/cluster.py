#!/usr/bin/env python3
"""K03 local process histories against the native cache server."""

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from redis import Redis
from redis.cluster import key_slot
from redis.exceptions import (
    ClusterDownError, ConnectionError, ClusterCrossSlotError, MovedError,
    TimeoutError, TryAgainError,
)

# This script is invoked by path from cluster.sh.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from testing.realenv.history import HistoryWriter
from testing.realenv.nemesis import Nemesis, Process

SLOTS = 16_384
PASSWORD = "local-cache-history"
SERVER = "127.0.0.1"
TRANSPORT = (ConnectionError, TimeoutError, ClusterDownError, TryAgainError, MovedError)


def wait(description, assertion, seconds=25):
    end = time.monotonic() + seconds
    delay = .05
    while True:
        value = assertion()
        if value:
            return value
        remaining = end - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"timeout awaiting {description}")
        # Cache status and metrics expose the state, without a blocking watch API.
        time.sleep(min(delay, remaining))
        delay = min(.25, delay * 2)


def port():
    with socket.socket() as sock:
        sock.bind((SERVER, 0))
        return sock.getsockname()[1]


def command(*args):
    return subprocess.run(args, check=True, text=True, capture_output=True, timeout=12).stdout


def tls(root, name):
    key, cert = Path(root, f"{name}.key"), Path(root, f"{name}.crt")
    command("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={name}",
            "-keyout", key, "-out", Path(root, f"{name}.csr"))
    flags = Path(root, f"{name}.ext")
    flags.write_text("subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n")
    command("openssl", "x509", "-req", "-in", Path(root, f"{name}.csr"), "-CA", Path(root, "ca.crt"),
            "-CAkey", Path(root, "ca.key"), "-set_serial", "0x" + uuid.uuid4().hex,
            "-days", "1", "-out", cert, "-extfile", flags)
    der = subprocess.check_output(["openssl", "x509", "-in", cert, "-outform", "der"], timeout=4)
    return list(hashlib.sha256(der).digest())


def endpoint(label, number, fingerprint):
    return f'[{label}]\nhost = "{SERVER}"\nport = {number}\nfingerprints = {json.dumps([fingerprint])}\n'


def ssl_table(name):
    return f'[tls]\nca = "ca.crt"\ncertificate = "{name}.crt"\nprivate_key = "{name}.key"\n'


class Fleet:
    def __init__(self, binary, root, evidence, scope, recorder):
        self.binary = str(binary)
        self.root = root
        self.evidence = evidence
        self.scope = scope
        self.recorder = recorder
        self.nemesis = Nemesis(recorder)
        self.metrics = root / "metrics"
        self.metrics.mkdir()
        scope.callback(self.save_metrics)
        self.processes = {}
        self.controllers = {}
        self.workers = {}
        self.id = uuid.uuid4()
        self.used_ports = set()
        command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                "-subj", "/CN=cache-fixture-ca", "-keyout", root / "ca.key", "-out", root / "ca.crt")
        for identity in range(1, 4):
            name = f"c{identity}"
            self.controllers[identity] = (self.allocate(), self.allocate(), tls(root, name))
        self.operator = tls(root, "operator")
        self.admin_config = self.write("operator", ssl_table("operator") + self.controller_endpoints(False))

    def allocate(self):
        while (selected := port()) in self.used_ports:
            pass
        self.used_ports.add(selected)
        return selected

    def write(self, name, content):
        path = self.root / f"{name}.toml"
        path.write_text(content)
        return path

    def controller_endpoints(self, raft):
        label = "voters" if raft else "controllers"
        return "".join(
            endpoint(f'{label}."{node}"', private if raft else service, fingerprint)
            for node, (service, private, fingerprint) in self.controllers.items()
        )

    def launch(self, name, role, config):
        with (self.evidence / f"{name}.log").open("ab") as output:
            process = subprocess.Popen(
                [self.binary, "cache", role, "--config", config], stdout=output, stderr=output,
            )
        self.processes[name] = process
        self.scope.callback(self.reap, process)

    @staticmethod
    def reap(process):
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)

    def controller(self, node):
        name = f"c{node}"
        service, private, _ = self.controllers[node]
        path = self.root / f"c{node}-metadata"
        config = self.write(name, (
            f'node = {node}\ncluster_id = "{self.id}"\n'
            f'listen = "{SERVER}:{service}"\nraft_listen = "{SERVER}:{private}"\n'
            f'data_dir = "{path}"\nclock = "local_docker"\n'
            f'operators = {json.dumps([self.operator])}\n'
        ) + ssl_table(name) + self.controller_endpoints(True))
        self.launch(name, "controller", config)

    def add_worker(self, node):
        name = f"w{node}"
        client, peer = self.allocate(), self.allocate()
        cert = tls(self.root, name)
        zone = f"az-{1 + (node - 1) % 3}"
        shared = f'zone = "{zone}"\nclient_host = "{SERVER}"\nclient_port = {client}\n'
        peer_config = endpoint("peer", peer, cert)
        member = self.write(f"{name}-member", shared + (
            f'[id]\nnode = {node}\nincarnation = "{uuid.UUID(int=0)}"\n'
        ) + peer_config)
        self.apply("enroll", "--member", str(member))
        config = self.write(name, (
            f'node = {node}\nclock = "local_docker"\nactors = 2\n'
            f'peer_listen = "{SERVER}:{peer}"\n'
            f'metrics_file = "{self.metrics / (name + "-metrics.json")}"\n' + shared
        ) + ssl_table(name) + peer_config + self.controller_endpoints(False) + (
            f'[client]\nlisten = "{SERVER}:{client}"\n'
            f'[client.tls]\ncertificate = "{name}.crt"\nprivate_key = "{name}.key"\n'
            f'[[client.users]]\nusername = "default"\npassword = "{PASSWORD}"\n'
            'key_prefixes = ["*"]\ncommands = ["get","set","del","exists","mget","pttl"]\n'
            '[client.limits]\nmemory_bytes = 805306368\nmax_connections = 8\nmax_handshakes = 4\nmax_pending = 4\n'
        ))
        self.workers[node] = (client, config)
        self.launch(name, "worker", config)

    def apply(self, *args):
        operation = str(uuid.uuid4())
        return wait("admin operation " + args[0], lambda: self.admin(operation, *args))

    def admin(self, operation, *args):
        proc = subprocess.run([self.binary, "cache", "admin", "--config", self.admin_config,
                               "--operation", operation, *args], capture_output=True, text=True, timeout=10)
        if proc.returncode:
            if "no controller accepted operation" in proc.stderr:
                return None
            raise RuntimeError(proc.stderr)
        return json.loads(proc.stdout)

    def status(self):
        response = self.admin(str(uuid.uuid4()), "status")
        return response and response["status"]

    def client(self, node):
        client_port, _ = self.workers[node]
        return Redis(
            host=SERVER, port=client_port, password=PASSWORD, ssl=True, protocol=2,
            ssl_ca_certs=str(self.root / "ca.crt"), ssl_check_hostname=True,
            socket_timeout=.5, socket_connect_timeout=.3,
        )

    def progress(self, node, slot):
        metrics = self.metrics / f"w{node}-metrics.json"
        try:
            contents = json.loads(metrics.read_text())
        except FileNotFoundError:
            return None
        return next((item for item in contents["slots"] if item["slot"] == slot), None)

    def save_metrics(self):
        for metrics in self.metrics.glob("w*-metrics.json"):
            shutil.copyfile(metrics, self.evidence / metrics.name)

    def run_fault(self, action, name):
        process = self.processes[name]
        action(Process(name, process))

    def stop(self, name):
        self.run_fault(self.nemesis.stop, name)

    def resume(self, name):
        self.run_fault(self.nemesis.cont, name)

    def kill(self, name):
        self.run_fault(self.nemesis.kill, name)
        self.processes[name].wait(timeout=5)

    def op(self, node, client, f, key, *args, **kwargs):
        fields = dict(process=1, f=f, key=key.decode(), node=f"w{node}")
        value = None
        if f == "set":
            value = args[0].decode()
        elif args:
            value = [key.decode(), *(arg.decode() for arg in args)]
        self.recorder.record("invoke", value=value, **fields)
        try:
            result = client.mget([key, *args]) if f == "mget" else getattr(client, f)(key, *args, **kwargs)
        except BaseException as error:
            outcome = "fail" if isinstance(error, (
                ClusterDownError, TryAgainError, MovedError, ClusterCrossSlotError,
            )) else "info"
            self.recorder.record(outcome, value={"error": type(error).__name__}, **fields)
            raise
        observed = value if f == "set" else result
        if isinstance(observed, bytes):
            observed = observed.decode()
        elif isinstance(observed, list):
            observed = [item.decode() if isinstance(item, bytes) else item for item in observed]
        self.recorder.record("ok", value=observed, **fields)
        return result

    def wait_placement(self, verify, seconds=30):
        def predicate():
            state = self.status()
            return state if state is not None and verify(state) else None
        return wait("committed placement", predicate, seconds=seconds)


def owner(view, slot):
    primary = view["slots"][slot]["primary"]
    return primary["node"] if primary else None


def ready(view):
    return view["ready_primaries"] == view["ready_replicas"] == SLOTS


def keys_for(view):
    keys = {}
    for index in range(SLOTS):
        key = f"history:{{{index}}}".encode()
        keys.setdefault(owner(view, key_slot(key)), key)
        if len(keys) == 3:
            return keys
    raise AssertionError("could not find three independent primary groups")


def assert_reply_error(expected, callback):
    try:
        callback()
    except expected:
        return
    raise AssertionError("command did not return its required protocol error")


def routed_value(fleet, node, key, expected):
    try:
        with fleet.client(node) as client:
            value = fleet.op(node, client, "get", key)
            return (True, value) if expected is None else value == expected
    except TRANSPORT:
        return None


def history(fleet, evidence):
    for node in range(1, 4):
        fleet.controller(node)
    wait("three-voter majority", fleet.status)
    for node in range(1, 4):
        fleet.add_worker(node)
    fleet.wait_placement(lambda view: all(
        not uuid.UUID(worker["id"]["incarnation"]).int == 0
        for worker in view["workers"].values()
    ) and len(view["workers"]) == 3)
    fleet.apply("init", "--groups", "3")
    view = fleet.wait_placement(ready)
    keys = keys_for(view)
    primary, unaffected = sorted(keys)[:2]
    key, independent = keys[primary], keys[unaffected]
    slot = key_slot(key)
    with fleet.client(primary) as client:
        assert fleet.op(primary, client, "set", key, b"prior", px=120_000)
        assert fleet.op(primary, client, "mget", key, key + b":missing", key) == [b"prior", None, b"prior"]
        assert_reply_error(ClusterCrossSlotError, lambda: fleet.op(primary, client, "delete", key, independent))
        assert fleet.op(primary, client, "get", key) == b"prior"
    with fleet.client(unaffected) as client:
        assert_reply_error(MovedError, lambda: fleet.op(unaffected, client, "get", key))
        assert fleet.op(unaffected, client, "set", independent, b"independent", px=120_000)
    evidence.append("K01 direct MOVED, same-slot order and cross-slot mutation rejection")

    prior = view["slots"][slot]["epoch"]
    replica = view["slots"][slot]["replica"]["node"]
    def written_sequence():
        progress = fleet.progress(primary, slot)
        if progress and progress["epoch"] == prior and progress["sequence"] > 0:
            return progress["sequence"]
        return None

    sequence = wait("acknowledged primary sequence", written_sequence)
    def replica_caught_up():
        progress = fleet.progress(replica, slot)
        return progress and progress["epoch"] == prior and progress["baseline"] and progress["sequence"] >= sequence

    wait("verified replica prefix includes acknowledged write", replica_caught_up)
    fleet.stop(f"w{primary}")
    promoted = fleet.wait_placement(
        lambda view: owner(view, slot) not in (None, primary) and view["slots"][slot]["epoch"] > prior,
    )
    successor = owner(promoted, slot)
    read = wait("successor authorizes cache slot", lambda: routed_value(fleet, successor, key, None))
    assert read[1] == b"prior", read
    with fleet.client(successor) as client:
        assert 0 < fleet.op(successor, client, "pttl", key) < 120_000
        assert fleet.op(successor, client, "set", key, b"successor", px=120_000)
    fleet.resume(f"w{primary}")
    with fleet.client(primary) as client:
        assert_reply_error((MovedError, ClusterDownError, TryAgainError),
                           lambda: fleet.op(primary, client, "set", key, b"retired"))
    assert wait("post-promotion independent value", lambda: routed_value(fleet, unaffected, independent, b"independent"))
    assert wait("successor value", lambda: routed_value(fleet, successor, key, b"successor"))
    evidence.append("K03 acknowledged replica prefix, paused primary, advanced epoch, fenced old write and unaffected slot")

    for node in (1, 2):
        fleet.stop(f"c{node}")
    def expired_authority():
        with fleet.client(successor) as client:
            try:
                fleet.op(successor, client, "get", key)
            except ClusterDownError:
                return True
            except (ConnectionError, TimeoutError, TryAgainError):
                return False
    wait("all previously committed leases expire without quorum", expired_authority)
    for node in (1, 2):
        fleet.resume(f"c{node}")
    wait("quorum restores leased authority", lambda: routed_value(fleet, successor, key, b"successor"))
    evidence.append("K03 minority controller cannot authorize after lease expiry")

    fleet.kill("c1")
    with (fleet.root / "c1-metadata" / "topology.wal").open("ab") as wal:
        wal.write(b"\x00\x00\x00")
        wal.flush()
        os.fsync(wal.fileno())
    fleet.stop("c2")
    fleet.controller(1)
    wait("quorum with controller recovered from incomplete WAL tail", fleet.status)
    assert fleet.processes["c1"].poll() is None
    fleet.resume("c2")
    evidence.append("K03 partial WAL tail, restart and recovery of quorum")

    before_move = fleet.wait_placement(ready)
    moving_epoch = before_move["slots"][slot]["epoch"]
    fleet.apply("rebalance", "--first", str(slot), "--last", str(slot), "--target", str(primary))
    fleet.wait_placement(lambda view: owner(view, slot) == primary and view["slots"][slot]["epoch"] > moving_epoch)
    assert wait("online move restored value", lambda: routed_value(fleet, primary, key, b"successor"))
    with fleet.client(primary) as client:
        assert 0 < fleet.op(primary, client, "pttl", key) < 120_000
    evidence.append("K03 verified online move preserved value and absolute expiry")

    prior_view = fleet.wait_placement(ready)
    for node in fleet.workers:
        fleet.kill(f"w{node}")
    fleet.wait_placement(lambda view: view["ready_primaries"] == view["ready_replicas"] == 0)
    for node, (_, config) in fleet.workers.items():
        fleet.launch(f"w{node}", "worker", config)
    fleet.wait_placement(lambda view: all(
        member["id"]["incarnation"] != prior_view["workers"][node]["id"]["incarnation"]
        for node, member in view["workers"].items()
    ))
    fleet.apply("cold-reset", "--confirm-data-loss")
    fresh = fleet.wait_placement(lambda view: view["generation"] == prior_view["generation"] + 1 and ready(view))
    for old_key in (key, independent):
        read = wait("empty slot after cold reset", lambda: routed_value(fleet, owner(fresh, key_slot(old_key)), old_key, None))
        assert read[1] is None
    evidence.append("K03 full-fleet restart and generation change kept both old values absent")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    completed = []
    try:
        with (tempfile.TemporaryDirectory() as path,
              HistoryWriter(args.output / "K03.jsonl") as recorder, ExitStack() as scope):
            fleet = Fleet(args.binary, Path(path), args.output, scope, recorder)
            try:
                history(fleet, completed)
            finally:
                fleet.nemesis.heal_all()
    finally:
        (args.output / "cluster.json").write_text(json.dumps({
            "environment": "Docker, one kernel clock",
            "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
            "executed_assertions": completed,
        }, indent=2) + "\n")
    print("\n".join(completed))


if __name__ == "__main__":
    main()

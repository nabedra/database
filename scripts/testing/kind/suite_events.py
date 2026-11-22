#!/usr/bin/env python3
"""Exercise Events quorum and volume replacement against a CI-built image on kind."""

import argparse
from contextlib import contextmanager, ExitStack
import json
from pathlib import Path
import re
import select
import subprocess
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace
from uuid import uuid4

from server.events.deploy.fixture import create_fixture
from server.events.tests.process_fixture import eventually, group_id, partition_route, request, status

from .cluster import temporary_cluster
from .faults import Faults
from .manifests import CONFIG, NAME, PORTS, VOLUME, render
from ..realenv.history import HistoryWriter, pair_operations, read_jsonl


TOPIC = "kind-events"
KEY = f"{TOPIC}/0"
PARTITION = partition_route(TOPIC)
SUBSCRIPTION = PARTITION + "/subscriptions/replay"
ADMIN = "/v1/admin"
RETRY_STATUSES = (0, 429, 503)


@contextmanager
def port_forward(cluster, pod):
    args = cluster.kubectl("port-forward", "--address", "127.0.0.1", f"pod/{pod}", f":{PORTS['http']}")
    with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as child:
        try:
            if select.select([child.stdout], [], [], 25)[0]:
                line = child.stdout.readline().decode(errors="replace").strip()
                match = re.fullmatch(r"Forwarding from 127\.0\.0\.1:(\d+) -> 8080", line)
                if match:
                    yield f"http://127.0.0.1:{match[1]}"
                    return
                raise RuntimeError(f"port-forward for {pod}: {line}")
            raise TimeoutError(f"port-forward for {pod} did not start")
        finally:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


class EventsRun:
    def __init__(self, cluster, history):
        self.cluster = cluster
        self.history = history
        self.urls = {}
        self.operator = None

    def deploy(self, image):
        self.cluster.load_image(image)
        with TemporaryDirectory() as directory:
            path = Path(directory)
            create_fixture(SimpleNamespace(
                brokers=3, metadata_voters=3, cpus=1500, memory_mib=1536,
                image=image, base_port=18080,
            ), path)
            self.settings = [
                json.loads((path / f"broker-{i + 1}.json").read_text()) for i in range(3)
            ]
        for settings in self.settings:
            for broker in settings["bootstrap"]["brokers"]:
                broker["peer_url"] = f"http://{NAME}-{broker['id'] - 1}.{NAME}:8081"
        self.apply_settings()
        self.cluster.apply(*render(image))
        self.cluster.kube("rollout", "status", f"statefulset/{NAME}", "--timeout=120s", timeout=130)
        self.operator = self.grant({"operation": "operator"})
        self.publisher = self.grant({"operation": "publish", "topic": TOPIC})
        self.reader = self.grant({"operation": "read", "topic": TOPIC, "subscription": "replay"})

    def apply_settings(self):
        self.cluster.apply({
            "apiVersion": "v1", "kind": "Secret", "metadata": {"name": NAME},
            "stringData": {f"{NAME}-{ordinal}.json": json.dumps(config)
                           for ordinal, config in enumerate(self.settings)},
        })

    def prepare_replacement(self, broker):
        config = self.settings[2]
        config["broker_id"] = broker["id"]
        config["bootstrap"]["brokers"].append(broker)
        self.apply_settings()

    def grant(self, scope):
        return self.cluster.kube(
            "exec", "-i", f"{NAME}-0", "-c", "broker", "--", "/usr/local/bin/nabedra-events",
            "issue-grant", "--config", f"{CONFIG}/{NAME}-0.json", "--grant", "/dev/stdin",
            input=json.dumps({
                "tenant": "kind-fixture", "subject": scope["operation"], "scope": scope,
                "expires_at_ms": time.time_ns() // 1_000_000 + 3_600_000,
            }),
        ).strip()

    @contextmanager
    def endpoints(self, *ordinals):
        with ExitStack() as stack:
            try:
                self.urls = {ordinal: stack.enter_context(port_forward(self.cluster, f"{NAME}-{ordinal}"))
                             for ordinal in ordinals}
                yield
            finally:
                self.urls = {}

    def rpc(self, ordinal, path, **kwargs):
        return request(self.urls[ordinal], path, timeout=4, **kwargs)

    def operate(self, ordinal, **action):
        operation_id = str(uuid4())

        def call(path, **kwargs):
            response = self.rpc(ordinal, ADMIN + path, token=self.operator, **kwargs)
            if response[0] not in RETRY_STATUSES:
                reply = status(response, (200, 202))
                if reply["operation_id"] != operation_id:
                    raise AssertionError("Events changed the operation ID")
                return reply

        receipt = eventually(f"{action['type']} submission", lambda: call(
            "/operations", method="POST", body={"operation_id": operation_id, "action": action},
        ))
        if receipt["state"] != "completed":
            def completed():
                reply = call(f"/operations/{operation_id}")
                return reply and reply["state"] == "completed"

            eventually(f"{action['type']} completion", completed, timeout=75)

    def setup_workload(self):
        self.operate(0, type="create_topic", topic=TOPIC, partitions=1,
                     replication_factor=3, retention_seconds=3600)
        def registered_topic():
            response = self.rpc(0, ADMIN + f"/topics/{TOPIC}", token=self.operator)
            if response[0] not in RETRY_STATUSES:
                return status(response, 200)

        topic = eventually("registered workload topic", registered_topic)
        self.group = group_id(topic["id"])
        self.operate(0, type="create_subscription", topic=TOPIC, subscription="replay", start="earliest")

    def view(self, ordinal, group):
        response = self.rpc(ordinal, ADMIN + f"/groups/{group}", token=self.operator)
        if response[0] not in (*RETRY_STATUSES, 404):
            return status(response, 200)

    def leader(self, group, *, after=-1):
        def probe():
            for ordinal in self.urls:
                view = self.view(ordinal, group)
                if view and view["leader"] == self.settings[ordinal]["broker_id"] and view["term"] > after:
                    return ordinal, view
            return None

        return eventually(f"{group} leader after term {after}", probe, timeout=45)

    def wait_for_replica(self, ordinal, group, cut, voters):
        def probe():
            view = self.view(ordinal, group)
            if view:
                member = view["membership"]
                return (view["running"] and member["applied_index"] is not None
                        and member["applied_index"] >= cut and set(member["voters"]) == voters
                        and not member["joint"])

        eventually(f"{NAME}-{ordinal} applied {group} through {cut}", probe, timeout=75)

    def _checked_request(self, f, ordinal, request_id, path, *, expected, body=None, **kwargs):
        fields = dict(process=1, f=f, key=KEY, request_id=request_id, node=f"{NAME}-{ordinal}")
        self.history.record("invoke", value=body, **fields)
        try:
            code, _, reply = self.rpc(ordinal, path, body=body, **kwargs)
        except BaseException as error:
            self.history.record("info", value={"error": type(error).__name__}, **fields)
            raise
        outcome = "ok" if code in expected else "info" if code in (0, 503) else "fail"
        # Credentials are used in requests and never written to history.
        if isinstance(reply, dict):
            recorded = {key: value for key, value in reply.items() if key not in {"lease", "delivery"}}
        else:
            recorded = reply
        self.history.record(outcome, value=recorded, **fields)
        if code in RETRY_STATUSES:
            return None
        return status((code, {}, reply), expected)

    def publish(self, ordinal, number):
        event_id = str(uuid4())
        body = {"event_id": event_id, "data": number}
        return eventually(f"publish {number}", lambda: self._checked_request(
            "publish", ordinal, event_id, PARTITION + "/events", expected=(200, 201),
            body=body, token=self.publisher, method="POST",
        ))

    def poll(self, ordinal):
        claim_id = str(uuid4())
        lease = eventually("claim replay lease", lambda: self._checked_request(
            "claim", ordinal, claim_id, SUBSCRIPTION + "/claim", expected=(200, 201),
            body={"claim_id": claim_id}, token=self.reader, method="POST",
        ))
        return eventually("poll committed events", lambda: self._checked_request(
            "poll", ordinal, str(uuid4()), SUBSCRIPTION + "/events", expected=(200,),
            token=self.reader, lease=lease["lease"],
        ))

    def pod(self, ordinal):
        return json.loads(self.cluster.kube("get", "pod", f"{NAME}-{ordinal}", "-o", "json"))

    def pvc(self, ordinal):
        return json.loads(self.cluster.kube("get", "pvc", f"{VOLUME}-{NAME}-{ordinal}", "-o", "json"))


def check_queue(events):
    """Check a final poll from an untouched checkpoint against recorded publish receipts."""
    acknowledged = {}
    replay = None
    for operation in pair_operations(events):
        invoke, done = operation.invoke, operation.completion
        if done is None or done.type != "ok":
            continue
        if done.f == "publish":
            receipt = done.value
            if any(receipt[field] != invoke.value[field] for field in ("event_id", "data")):
                raise AssertionError(f"publish receipt changed event {invoke.request_id}")
            previous = acknowledged.setdefault(invoke.request_id, receipt)
            if previous != receipt:
                raise AssertionError(f"publish receipt changed on retry {invoke.request_id}")
            replay = None
        elif done.f == "poll":
            replay = done.value
    if not acknowledged or replay is None or int(replay["after"]) != 0:
        raise AssertionError("need acknowledged publishes followed by a poll from offset zero")
    delivered = replay["events"]
    offsets = [int(event["offset"]) for event in delivered]
    if offsets != list(range(1, len(delivered) + 1)):
        raise AssertionError(f"expected contiguous offsets from 1, got {offsets}")
    if len(delivered) != len(acknowledged) or {event["event_id"]: event for event in delivered} != acknowledged:
        raise AssertionError("replay differs from acknowledged publishes")
    return len(delivered)


def run_faults(fixture, faults):
    with fixture.endpoints(0, 1, 2):
        fixture.setup_workload()
        for number in range(4):
            fixture.publish(0, number)
        _, original = fixture.leader(fixture.group)

    old_uid = fixture.pod(2)["metadata"]["uid"]
    old_pvc = fixture.pvc(2)["metadata"]["uid"]
    faults.delete_pod(f"{NAME}-2")
    fixture.cluster.kube("wait", "--for=create", f"pod/{NAME}-2", "--timeout=90s")
    fixture.cluster.kube("wait", "--for=condition=Ready", f"pod/{NAME}-2", "--timeout=90s")
    if fixture.pod(2)["metadata"]["uid"] == old_uid or fixture.pvc(2)["metadata"]["uid"] != old_pvc:
        raise AssertionError("pod deletion must replace the Pod while keeping its PVC")

    with fixture.endpoints(0, 1, 2):
        fixture.wait_for_replica(2, fixture.group, original["membership"]["applied_index"], {1, 2, 3})
        leader, current = fixture.leader(fixture.group)
        frozen = fixture.pod(leader)["spec"]["nodeName"]
        survivors = [ordinal for ordinal in range(3) if ordinal != leader]
    faults.pause(frozen)
    try:
        with fixture.endpoints(*survivors):
            successor, _ = fixture.leader(fixture.group, after=current["term"])
            fixture.publish(successor, 4)
    finally:
        faults.unpause(frozen)

    # Events fences a permanently lost disk by enrolling a fresh broker ID.
    replacement = {"id": 4, "zone": "zone-4", "peer_url": f"http://{NAME}-replacement:8081"}
    faults.wipe_pvc(NAME, 2, before_restart=lambda: fixture.prepare_replacement(replacement))
    with fixture.endpoints(0, 1, 2):
        fixture.publish(0, 5)  # The original disks still form a quorum.
        fixture.operate(0, type="add_broker", broker=replacement)
        fixture.operate(0, type="move_partition", topic=TOPIC, partition=0, voters=[1, 2, 4])
        fixture.operate(0, type="move_metadata", voters=[1, 2, 4])
        for group in (fixture.group, "metadata"):
            _, final = fixture.leader(group)
            fixture.wait_for_replica(2, group, final["membership"]["applied_index"], {1, 2, 4})
        original_node = fixture.pod(0)["spec"]["nodeName"]
    faults.pause(original_node)
    try:
        with fixture.endpoints(1, 2):
            fixture.publish(2, 6)  # This quorum requires the replacement.
            fixture.poll(2)
    finally:
        faults.unpause(original_node)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="nabedra-events:ci", help="Events image in local Docker")
    parser.add_argument("--nodes", type=int, choices=(3, 4), default=3, help="number of kind workers")
    parser.add_argument("--history", type=Path, default=Path("kind-events.jsonl"))
    args = parser.parse_args()
    args.history.parent.mkdir(parents=True, exist_ok=True)
    with HistoryWriter(args.history) as history, temporary_cluster(args.nodes) as cluster:
        fixture = EventsRun(cluster, history)
        faults = Faults(cluster, history)
        fixture.deploy(args.image)
        try:
            run_faults(fixture, faults)
        finally:
            faults.heal_all()
    with args.history.open(encoding="utf-8") as source:
        count = check_queue(read_jsonl(source))
    print(f"EV06 EV07: replayed {count} acknowledged events at contiguous offsets after pod deletion, "
          f"node pause, and PVC replacement. History: {args.history}")


if __name__ == "__main__":
    main()

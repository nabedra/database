#!/usr/bin/env python3
"""Run Events SDK, failure, placement, backup, and load assertions in Docker."""

from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from decimal import Decimal
import hashlib
from http.client import HTTPConnection
import json
from pathlib import Path
import platform
import secrets
import shutil
import socket
import ssl
import statistics
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Barrier, Event
import time
import traceback
from urllib.parse import urlsplit
from uuid import UUID, uuid4, uuid5

from local_tls import Certificates
from process_fixture import (
    CONFIG, MOUNT, Cluster, cgroup_sample, command, encode, eventually, fixtures,
    group_id, partition_route, request, require, status, subscription_route,
)

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "orm" / "python" / "src"))
from nabedra.events import Events, EventsError, PublishRecord


SUITE_DIR = Path(__file__).resolve().parent
EVENT_BYTES = 64 * 1024
BODY_BYTES = 1024 * 1024
RETRY_CODES = {"unavailable", "overloaded", "outcome_unknown", "transport_error"}


class Evidence:
    def __init__(self):
        self.checks = []

    def verified(self, description, **observed):
        self.checks.append({"check": description, **observed})
        print(description, flush=True)


def make_cluster(clusters, temporary, args, count, metadata=None, *, name=None, **kwargs):
    cluster = Cluster(
        temporary / (name or f"cluster-{len(clusters)}"),
        brokers=count, metadata_voters=metadata or (3 if count > 1 else 1),
        image=args.image, cpus=args.cpus, memory_mib=args.memory_mib, **kwargs,
    )
    clusters.append(cluster)
    return cluster


def client(cluster, broker, token):
    return Events(cluster.urls[broker], token=token, timeout=8,
                  ca_file=getattr(cluster, "ca_file", None), allow_insecure=cluster.tls is None)


def retry(description, callback, counters=None):
    def attempt():
        try:
            return callback()
        except EventsError as error:
            if error.code not in RETRY_CODES:
                raise
            if counters is not None:
                counters.append(error.code)
            return None
    return eventually(description, attempt)


def confirm_lease(cluster, token, topic, name, *, broker=1, partition=0, claim_id=None):
    sub = client(cluster, broker, token).subscription(topic, name, partition=partition)
    claim_id = claim_id or uuid4()
    return retry("lease claim", lambda: sub.claim(claim_id=claim_id))


def expect_error(code, action):
    try:
        action()
    except EventsError as error:
        require(error.code == code, f"expected error {code}, got {error.code}")
        return error
    raise AssertionError(f"expected error {code}, received success")


def read_all(cluster, token, topic, subscription, *, broker=1, partition=0):
    with confirm_lease(cluster, token, topic, subscription, broker=broker, partition=partition) as lease:
        result = []
        while True:
            batch = retry("replay poll", lease.poll)
            if not batch.events:
                retry("release replay lease", lambda: lease.close() or True)
                return batch.after, result
            through = batch.events[-1].offset
            acknowledged = retry("replay checkpoint", lambda: lease.ack(batch, through=through))
            require(acknowledged == through, "replay checkpoint changed on retry")
            result.extend(batch.events)


def identity_oracle(events, expected, *, after=0):
    require([event.offset for event in events] == list(range(after + 1, after + len(expected) + 1)),
            "noncontiguous or unexpected replay offsets")
    require(len({event.event_id for event in events}) == len(events), "duplicate IDs in replay")
    require({event.event_id: event.data for event in events} == expected, "replay identity/payload mismatch")


def discard_publish_response(cluster, broker, token, topic, event_id, data, expected_tail):
    url = urlsplit(cluster.urls[broker])
    body = encode({"event_id": str(event_id), "data": data})
    connection = HTTPConnection(url.hostname, url.port, timeout=5)
    try:
        # Keep the connection unobserved until an independent committed tail
        # query sees the exact next offset. No producer races this barrier.
        connection.request("POST", partition_route(topic) + "/events", body, {
            "Authorization": f"Bearer {token}", "Content-Type": "application/json",
        })
        eventually(
            "committed event with unread response",
            lambda: int(cluster.wait_status(f"/topics/{topic}/partitions/0")["tail"]) == expected_tail,
            timeout=10,
        )
    finally:
        connection.close()


def rf1_http(clusters, temporary, args, evidence):
    def config(_, settings):
        settings["http_limits"] = {"lease_ms": 5000, "tenant_burst_bytes": 2 * BODY_BYTES}
    cluster = make_cluster(clusters, temporary, args, 1, config=config)
    cluster.start([1])
    mounts = cluster.mounts()
    require(len(mounts) == 1 and all(mounts.values()), "RF1 broker is missing its named disk")
    invalid_operation = str(uuid4())
    oversized_topic = {
        "type": "create_topic", "topic": "orders", "partitions": 4096,
        "replication_factor": 1, "retention_seconds": 3600,
    }
    status(cluster.admin("/operations", method="POST", body={
        "operation_id": invalid_operation, "action": oversized_topic,
    }), 422, "partition_limit")
    status(cluster.admin(f"/operations/{invalid_operation}"), 404, "not_found")
    cluster.operate({**oversized_topic, "partitions": 2}, id=invalid_operation)
    orders = cluster.wait_status("/topics/orders")
    require(len(orders["partitions"]) == 2, "rejected operation stranded the durable admin lane")
    evidence.verified("EV03 oversized topology is refused before creating a durable intent",
                      rejected_partitions=4096, completed_partitions=2)
    cluster.subscribe("orders", "worker")
    reader = cluster.reader("orders", "worker")
    standby = cluster.reader("orders", "worker", subject="standby")
    token = cluster.publisher("orders")
    publisher = client(cluster, 1, token)
    require(list(publisher.partitions("orders")) == [0, 1], "partition discovery differs")
    route = partition_route("orders")
    raw = lambda method, suffix, body=None, bearer=token: cluster.rpc(
        1, route + suffix, method=method, token=bearer, body=body,
    )

    first_ids = [uuid4(), uuid4(), uuid4()]
    precision = Decimal("1.2345678901234567890123456789")
    values = [{"i": 0}, {"i": 1}, {"precise": precision}]
    initial = publisher.publish_batch("orders", partition=0, events=(
        PublishRecord(event_id, value) for event_id, value in zip(first_ids, values, strict=True)
    ))
    require([event.offset for event in initial] == [1, 2, 3], "initial receipt order changed")
    require([event.event_id for event in initial] == first_ids, "batch identities not in input order")
    require(initial[-1].data["precise"] == precision, "Events rounded JSON decimal")
    duplicate = publisher.publish("orders", partition=0, event_id=first_ids[0], data=values[0])
    require(duplicate == initial[0], "same-ID retry changed the durable receipt")
    other = publisher.publish("orders", partition=1, event_id=first_ids[0], data={"partition": 1})
    require(other.offset == 1 and other.partition == 1, "deduplication crossed partition scope")
    expect_error("conflict", lambda: publisher.publish(
        "orders", partition=0, event_id=first_ids[0], data={"changed": True},
    ))

    conflicted_id = uuid4()
    conflict = {"events": [
        {"event_id": str(conflicted_id), "data": 4},
        {"event_id": str(first_ids[0]), "data": {"changed": True}},
    ]}
    status(raw("POST", "/batches", conflict), 409, "conflict")
    repeated = {"events": [
        {"event_id": str(conflicted_id), "data": 4},
        {"event_id": str(conflicted_id), "data": 4},
    ]}
    status(raw("POST", "/batches", repeated), 409, "conflict")
    require(cluster.wait_status("/topics/orders/partitions/0")["tail"] == "3",
            "rejected batch consumed an offset")
    cluster.subscribe("orders", "from-now", "latest")
    latest = cluster.reader("orders", "from-now")
    lost_id = uuid4()
    lost_data = {"lost": "publish-response"}
    discard_publish_response(cluster, 1, token, "orders", lost_id, lost_data, 4)
    recovered = status(raw("POST", "/events", {"event_id": str(lost_id), "data": lost_data}), 200)
    require(recovered["offset"] == "4", "unread response retry appended the event again")

    pending = [(uuid4(), {"index": index}) for index in range(12)]

    def publish(entry):
        event_id, value = entry
        return retry("publishing concurrent event", lambda: publisher.publish(
            "orders", partition=0, event_id=event_id, data=value,
        ))

    with ThreadPoolExecutor(max_workers=6) as threads:
        concurrent = list(threads.map(publish, [*pending, pending[0], pending[1]]))
    require(concurrent[0] == concurrent[-2] and concurrent[1] == concurrent[-1],
            "concurrent same-ID retries changed the receipts")
    require(sorted(event.offset for event in concurrent[:12]) == list(range(5, 17)),
            "concurrent receipts left holes or reused offsets")

    malformed_id = str(uuid4())
    status(raw("POST", "/events", b"{"), 400, "invalid_request")
    status(raw("POST", "/events", (
        '{"event_id":"' + malformed_id + '","data":{"same":1,"same":2}}'
    ).encode()), 400, "invalid_request")
    status(raw("POST", "/events", {
        "event_id": malformed_id, "data": None,
    }, bearer="garbage"), 401, "unauthorized")
    status(raw("POST", "/events", {
        "event_id": malformed_id, "data": None,
    }, bearer=reader), 403, "forbidden")
    status(raw("GET", "/events"), (404, 405))
    status(cluster.rpc(1, "/internal/events/v2/control", method="POST", body={}), 404)
    over_event = {"event_id": str(uuid4()), "data": "p" * EVENT_BYTES}
    status(raw("POST", "/events", over_event), 413, "payload_too_large")
    accepted_big_id = uuid4()
    exact = {"event_id": str(accepted_big_id), "data": ""}
    exact["data"] = "a" * (EVENT_BYTES - len(encode(exact)))
    require(len(encode(exact)) == EVENT_BYTES, "64 KiB fixture arithmetic")
    exact_reply = status(raw("POST", "/events", exact), 201)
    require(exact_reply["offset"] == "17", "malformed or oversized publish changed the tail")
    padded_id = uuid4()
    padded_event = {"event_id": str(padded_id), "data": {"padded": True}}
    batch = encode({"events": [padded_event]})
    batch += b" " * (BODY_BYTES - len(batch))
    padded_reply = status(raw("POST", "/batches", batch), 200)
    require(padded_reply["events"][0]["offset"] == "18", "1 MiB framed request lost its queue slot")
    status(raw("POST", "/batches", batch + b" "), 413, "payload_too_large")
    # Burst policy counts ingress bytes, so two 1 MiB attempts compete for a
    # 2 MiB budget that already has traffic against it. Isolate rate testing
    # with a separate process below; no timing-sensitive assumption here.
    evidence.verified("EV01–EV03 HTTP validates scopes, limits, atomic batches, and unread publish replies",
                      concurrent_requests=14, retained_tail=18, exact_event_bytes=EVENT_BYTES,
                      exact_request_bytes=BODY_BYTES)

    checkpoint_url = subscription_route("orders", "worker") + "/checkpoint"
    with confirm_lease(cluster, reader, "orders", "worker") as lease:
        snapshot = lease.poll(limit=4)
        require(snapshot.after == 0 and [event.offset for event in snapshot.events] == [1, 2, 3, 4],
                "earliest subscription did not begin at the retained floor")
        expect_error("lease_busy", lambda: client(cluster, 1, standby).subscription(
            "orders", "worker", partition=0,
        ).claim())
        status(cluster.rpc(1, checkpoint_url, method="PUT", token=reader,
                           lease=snapshot.delivery, body={"delivery": snapshot.delivery, "through": "2"}),
               409, "lease_lost")
        require(lease.ack(snapshot, through=2) == 2, "prefix ack returned wrong checkpoint")
        require(lease.ack(snapshot, through=2) == 2, "lost-ack retry was not idempotent")
        require(lease.renew().timestamp() >= time.time(), "renew returned expired credential")
    with confirm_lease(cluster, latest, "orders", "from-now") as lease:
        batch = lease.poll(limit=2)
        require(batch.after == 3 and batch.events[0].event_id == lost_id,
                "latest subscription did not materialize independently")
        require(batch.events[0].published_at == recovered["published_at"],
                "deduplicated retry did not preserve the committed publish time")

    expiry_path = subscription_route("orders", "worker")
    claim_id = str(uuid4())
    old = status(cluster.rpc(1, expiry_path + "/claim", method="POST", token=reader,
                             body={"claim_id": claim_id}), 200)
    again = status(cluster.rpc(1, expiry_path + "/claim", method="POST", token=reader,
                               body={"claim_id": claim_id}), 200)
    require(old["expires_at"] == again["expires_at"], "replayed claim extended the lease")
    eventually("old lease expires", lambda: cluster.rpc(
        1, expiry_path + "/events", token=reader, lease=old["lease"],
    )[0] == 409, timeout=12)
    with confirm_lease(cluster, standby, "orders", "worker") as replacement:
        require(replacement.poll(limit=2).after == 2, "redelivery skipped unprocessed suffix")
        status(cluster.rpc(1, expiry_path + "/events", token=reader, lease=old["lease"]),
               409, "lease_lost")
    evidence.verified("EV02 lease renews, expires, redelivers, and fences stale credentials",
                      checkpoint=2)

    cluster.topic("ephemeral", retention_seconds=1)
    cluster.subscribe("ephemeral", "lagging")
    short_token = cluster.publisher("ephemeral")
    lagging = cluster.reader("ephemeral", "lagging")
    ephemeral_id = uuid4()
    short = client(cluster, 1, short_token)
    short.publish("ephemeral", partition=0, event_id=ephemeral_id, data="expires")
    eventually("retention floor advances", lambda: cluster.wait_status(
        "/topics/ephemeral/partitions/0",
    )["earliest_offset"] == "2")
    with confirm_lease(cluster, lagging, "ephemeral", "lagging") as lease:
        gap = expect_error("checkpoint_expired", lease.poll)
        require(gap.earliest_offset == 2, "retention gap omits or misstates earliest offset")
        cluster.operate({
            "type": "reset_subscription", "topic": "ephemeral",
            "partition": 0, "subscription": "lagging",
        })
        expect_error("lease_lost", lease.poll)
    with confirm_lease(cluster, lagging, "ephemeral", "lagging") as lease:
        reset = lease.poll()
        require(reset.after == 1 and not reset.events, "explicit reset checkpoint differs from floor")
        require(short.publish("ephemeral", partition=0, event_id=ephemeral_id, data="expires").offset == 2,
                "retention did not remove dedup index or reset offset order")
    evidence.verified("EV02 wall-clock retention expires event and dedup state and requires explicit reset",
                      earliest_offset=2)

    expected = dict(zip(first_ids, values, strict=True))
    expected.update(pending)
    expected.update({lost_id: lost_data, accepted_big_id: exact["data"], padded_id: {"padded": True}})
    start = time.monotonic()
    cluster.kill([1])
    cluster.start([1])
    require(cluster.mounts() == mounts, "RF1 restart changed its mounted disk")
    require(cluster.wait_status("/topics/orders")["id"] == orders["id"], "restart changed topic identity")
    with confirm_lease(cluster, reader, "orders", "worker") as lease:
        require(lease.poll(limit=1).after == 2, "restart lost committed checkpoint")
    cluster.subscribe("orders", "restart-replay")
    replay = cluster.reader("orders", "restart-replay")
    final, events = read_all(cluster, replay, "orders", "restart-replay")
    identity_oracle(events, expected)
    require(final == 18, "restart replay ended at wrong offset")
    evidence.verified("EV01 RF1 SIGKILL restart preserves the log, identities, offsets, and checkpoint",
                      events=len(events), disk_mounts=len(mounts),
                      recovery_seconds=round(time.monotonic() - start, 3))

    cluster.kill([1])
    path = cluster.directory / "broker-1.json"
    settings = json.loads(path.read_text())
    settings["min_free_disk_bytes"] = 2**60
    settings["http_limits"]["publish_in_flight"] = 5
    settings["http_limits"]["request_timeout_ms"] = 15000
    path.write_text(json.dumps(settings))
    cluster.start([1])
    with confirm_lease(cluster, reader, "orders", "worker") as lease:
        def rejected(index):
            return raw("POST", "/events", {"event_id": str(uuid4()), "data": {"i": index}})
        with ThreadPoolExecutor(max_workers=4) as threads:
            writes = [threads.submit(rejected, index) for index in range(10)]
            page = lease.poll(limit=1)
            require(lease.ack(page, through=page.events[-1].offset) == 3,
                    "low disk prevented checkpoint progress")
            for future in writes:
                status(future.result(), 507, "insufficient_storage")
    evidence.verified("EV03 low-disk publishes return 507 while polling and acknowledgment progress",
                      rejected_publishes=10, checkpoint=3)

    url = urlsplit(cluster.urls[1])
    header = (f"POST {route}/events HTTP/1.1\r\nHost: {url.netloc}\r\n"
              f"Authorization: Bearer {token}\r\nContent-Type: application/json\r\n"
              "Content-Length: 80\r\nExpect: 100-continue\r\n\r\n").encode()
    with confirm_lease(cluster, reader, "orders", "worker") as lease, ExitStack() as bodies:
        for _ in range(5):
            connection = bodies.enter_context(socket.create_connection((url.hostname, url.port), timeout=4))
            response = bodies.enter_context(connection.makefile("rb"))
            connection.sendall(header)
            require(response.readline().startswith(b"HTTP/1.1 100 "),
                    "publish body did not cross its admission barrier")
            require(response.readline() == b"\r\n", "unexpected interim admission response")

        def saturated():
            response = raw("POST", "/events", {"event_id": str(uuid4()), "data": None})
            return response if response[0] == 429 else None

        denied = eventually("five waiting bodies occupy publish admission", saturated, timeout=3)
        status(denied, 429, "overloaded")
        require(any(key.lower() == "retry-after" for key in denied[1]),
                "overload reply omitted Retry-After")
        page = lease.poll(limit=1)
        require(lease.ack(page, through=page.events[-1].offset) == 4,
                "publish admission saturation blocked the reserved polling/checkpoint lane")
    evidence.verified("EV03 withheld request bodies fill publish admission while control still commits",
                      held_bodies=5, checkpoint=4, publish_rejection=429)


def rf3_quorum(clusters, temporary, args, evidence):
    cluster = make_cluster(clusters, temporary, args, 3)
    cluster.start([1, 2, 3])
    mounts = cluster.mounts()
    require(len(set(mounts.values())) == 3 and all(mounts.values()), "RF3 disks are not distinct")
    topic = cluster.topic("rf3", replication_factor=3)
    group = group_id(topic["id"])
    cluster.subscribe("rf3", "worker")
    reader = cluster.reader("rf3", "worker")
    token = cluster.publisher("rf3")
    cluster.topic("independent")
    independent_token = cluster.publisher("independent")
    independent = client(cluster, 1, independent_token)
    # Keep the RF1 route warm on its surviving voter before losing metadata quorum.
    warm_id = uuid4()
    warm = independent.publish("independent", partition=0, event_id=warm_id, data={"warm": True})

    leader, first_view = cluster.leader(group)
    follower = next(node for node in (1, 2, 3) if node != leader)
    first_id, lost_id = uuid4(), uuid4()
    first = client(cluster, follower, token).publish("rf3", partition=0, event_id=first_id, data=1)
    require(first.offset == 1, "non-leader ingress did not forward a committed publish")
    with confirm_lease(cluster, reader, "rf3", "worker", broker=follower) as lease:
        batch = lease.poll()
        require(lease.ack(batch, through=first.offset) == 1, "RF3 checkpoint did not commit")
    discard_publish_response(cluster, follower, token, "rf3", lost_id, 2, 2)
    tail_view = cluster.wait_status(f"/groups/{group}", broker=leader)
    match = tail_view["matched_index"] or {}
    cut = tail_view["membership"]["applied_index"]
    require(cut > first_view["membership"]["applied_index"],
            "leader did not advance its applied cut")
    require(sum(index is not None and index >= cut for index in match.values()) >= 2,
            "committed tail had no observed majority match")
    started = time.monotonic()
    cluster.kill([leader])
    new_leader, view = cluster.leader(group, excluding=[leader])
    retry_receipt = retry("former leader's committed unread reply", lambda: client(
        cluster, new_leader, token,
    ).publish("rf3", partition=0, event_id=lost_id, data=2))
    recovery = time.monotonic() - started
    require(retry_receipt.offset == 2 and retry_receipt.topic_id == first.topic_id,
            "failover duplicated or changed committed event")
    require(view["term"] > first_view["term"], "leader failover did not elect a later term")
    with confirm_lease(cluster, reader, "rf3", "worker", broker=new_leader) as lease:
        page = lease.poll()
        require(page.after == 1 and len(page.events) == 1 and page.events[0] == retry_receipt,
                "failover lost checkpoint or committed unacknowledged event")
    evidence.verified("EV07 RF3 wrong-broker ingress, unread committed reply, and leader SIGKILL",
                      old_leader=leader, new_leader=new_leader, old_term=first_view["term"],
                      new_term=view["term"], leader_matches_before_kill=match,
                      recovery_seconds=round(recovery, 3), checkpoint=1)

    cluster.start([leader], ready=False)
    cluster.ready(1)
    surviving_leader, _ = cluster.leader(group)
    if surviving_leader != 1:
        cluster.operate({
            "type": "move_partition", "topic": "independent",
            "partition": 0, "voters": [surviving_leader],
        })
    # Keep both routes warm on the actual RF3 leader before removing its peers.
    independent = client(cluster, surviving_leader, independent_token)
    preserved = retry("warm independent cached route", lambda: independent.publish(
        "independent", partition=0, event_id=warm_id, data={"warm": True},
    ))
    require(preserved == warm, "rewarming RF1 ingress changed the original receipt")
    writer = client(cluster, surviving_leader, token)
    require(retry("warm quorum cached route", lambda: writer.publish(
        "rf3", partition=0, event_id=lost_id, data=2,
    )) == retry_receipt, "rewarming RF3 ingress changed the original receipt")
    cluster.leader(group, brokers=[surviving_leader])
    cluster.kill(sorted(cluster.running - {surviving_leader}))
    denied_id = uuid4()
    response = cluster.rpc(surviving_leader, partition_route("rf3") + "/events", method="POST",
                           token=token, body={"event_id": str(denied_id), "data": "no-quorum"})
    denial = status(response, 503)
    require(denial["code"] in {"unavailable", "outcome_unknown"}, "minority gave definitive success")
    independent_id = uuid4()
    independent_new = retry("healthy partition during metadata minority", lambda: independent.publish(
        "independent", partition=0, event_id=independent_id, data={"healthy": True},
    ))
    require(independent_new.offset == 2, "healthy RF1 group did not preserve sequence")
    evidence.verified("EV07 RF3 minority returns 503 while independent cached RF1 partition commits",
                      isolated_leader=surviving_leader, minority_result=denial["code"], healthy_offset=2)

    stopped = time.monotonic()
    cluster.kill([surviving_leader])
    require(not cluster.running, "full cluster shutdown left a broker running")
    cluster.start([1, 2, 3])
    require(cluster.mounts() == mounts, "full restart changed RF3 mounted volumes")
    cluster.subscribe("rf3", "cold-replay")
    cold_token = cluster.reader("rf3", "cold-replay")
    cold_tail = int(cluster.wait_status("/topics/rf3/partitions/0")["tail"])
    expected = {first_id: 1, lost_id: 2}
    if denial["code"] == "outcome_unknown" and cold_tail == 3:
        expected[denied_id] = "no-quorum"
    final, events = read_all(cluster, cold_token, "rf3", "cold-replay")
    require(final == len(expected), "unexpected cold replay high water")
    identity_oracle(events, expected)
    with confirm_lease(cluster, reader, "rf3", "worker") as lease:
        require(lease.poll().after == 1, "cold start lost RF3 checkpoint")
    evidence.verified("EV07 all three RF3 disks reopen after every broker stops",
                      events=len(events), disk_mounts=3, recovery_seconds=round(time.monotonic() - stopped, 3))


def growth(clusters, temporary, args, evidence):
    def gate_route(_, settings):
        if args.fault_image:
            settings["bootstrap"]["brokers"][3]["peer_url"] = "http://learner-gate:8081"

    cluster = make_cluster(clusters, temporary, args, 6, metadata=1, config=gate_route,
                           standby=bool(args.fault_image))
    cluster.start([1])
    topic = cluster.topic("live")
    group = group_id(topic["id"])
    cluster.subscribe("live", "existing")
    token = cluster.publisher("live")
    reader = cluster.reader("live", "existing")
    first_id = uuid4()
    first = client(cluster, 1, token).publish("live", partition=0, event_id=first_id, data={"first": 1})
    with confirm_lease(cluster, reader, "live", "existing") as lease:
        page = lease.poll()
        require(lease.ack(page, through=1) == 1, "initial checkpoint did not commit")
    initial = cluster.wait_status(f"/groups/{group}", broker=1)
    require(initial["membership"]["voters"] == [1], "promotion started with multiple voters")

    expected = {first_id: {"first": 1}}
    # The broker compacts at 1024 Raft entries. Each sequential reply crosses
    # an independent commit barrier, so coalescing cannot reduce this count.
    publisher = client(cluster, 1, token)
    for index in range(1025):
        event_id = uuid4()
        data = {"pre_promotion": index, "padding": "p" * 512}
        event = retry("populate a recoverable snapshot", lambda: publisher.publish(
            "live", partition=0, event_id=event_id, data=data,
        ))
        require(event.offset == index + 2, "snapshot fixture changed the pre-promotion offset")
        expected[event_id] = data
    snapshot = eventually(
        "RF1 automatic Raft compaction",
        lambda: cluster.wait_status(f"/groups/{group}", broker=1)["snapshot_index"],
    )
    initial_tail = len(expected)

    cluster.start([2, 3], ready=False)
    cluster.operate({"type": "move_metadata", "voters": [1, 2, 3]})
    cluster.operate({"type": "set_replication_factor", "topic": "live", "replication_factor": 3})
    current = cluster.wait_status("/topics/live")
    require(current["id"] == topic["id"] and current["partitions"][0]["voters"] == [1, 2, 3],
            "RF1 promotion changed topic identity or did not commit the expected voters")
    require(client(cluster, 3, token).publish(
        "live", partition=0, event_id=first_id, data={"first": 1},
    ) == first, "RF1 promotion did not preserve original receipt")
    with confirm_lease(cluster, reader, "live", "existing", broker=3) as lease:
        require(lease.poll().after == 1, "RF1 promotion changed the existing checkpoint")
    installed = {
        node: cluster.wait_status(f"/groups/{group}", broker=node)["snapshot_index"] for node in (2, 3)
    }
    require(all(index is not None and index >= snapshot for index in installed.values()),
            "new RF3 replicas did not install at least the source snapshot cut")
    evidence.verified("EV08 RF1 metadata and partition promote to RF3 through Raft snapshot catch-up",
                      voters=current["partitions"][0]["voters"], checkpoint=1,
                      leader_snapshot_index=snapshot, learner_snapshot_indexes=installed)

    # The target remains unavailable until the observer sees the committed
    # operation receipt and unrelated work succeeds using the old placement.
    move = {"type": "move_partition", "topic": "live", "partition": 0, "voters": [2, 3, 4]}
    move_id, pending = cluster.submit(move)
    require(pending["state"] == "pending", "move committed before the replacement was available")
    for i in range(4):
        event_id = uuid4()
        data = {"during_move": i}
        reply = client(cluster, 1, token).publish("live", partition=0, event_id=event_id, data=data)
        require(reply.offset == initial_tail + i + 1, "eligible publishing stalled or skipped an offset")
        expected[event_id] = data
    old = cluster.wait_status("/topics/live")
    require(old["partitions"][0]["voters"] == [1, 2, 3], "move retired a source before an absent target")
    status(cluster.admin(f"/operations/{move_id}"), 202)
    controller, controller_view = cluster.leader("metadata")
    cluster.kill([controller])
    new_controller, new_view = cluster.leader("metadata", excluding=[controller])
    require(new_view["term"] > controller_view["term"], "controller takeover did not increase term")
    require(status(cluster.admin(f"/operations/{move_id}", broker=new_controller), 202)["state"] == "pending",
            "new controller discarded the existing operation receipt")
    if args.fault_image:
        gate = temporary / "gate"
        gate.mkdir()
        spare_cpu = args.cpus - Decimal(str(cluster.caps["cpu"]))
        cluster.compose["services"]["learner-gate"] = {
            "image": args.fault_image, "command": ["python3", "/snapshot_gate.py"],
            "cpus": str(min(Decimal("0.15"), spare_cpu)), "mem_limit": "64m", "read_only": True,
            "environment": {"EVENTS_GATE_TARGET": "broker-4", "EVENTS_GATE_GROUP": group},
            "volumes": [f"{gate}:/gate", f"{SUITE_DIR / 'snapshot_gate.py'}:/snapshot_gate.py:ro"],
        }
        cluster.compose_file.write_text(json.dumps(cluster.compose))
        cluster.docker("up", "-d", "--no-recreate", "--pull", "never", "learner-gate")
    cluster.start([controller, 4, 5, 6], ready=False)
    if args.fault_image:
        eventually("snapshot gate forwarded first image bytes", (gate / "started").is_file, timeout=65)
        directory = f"{MOUNT}/snapshots/{group}"

        def partial():
            result = command(
                "docker", "exec", cluster.container(4), "find", directory,
                "-name", ".tmp*", "-size", "+128", "-print", check=False,
            )
            return result.stdout.strip()

        incoming = eventually("learner wrote an incomplete snapshot image", partial, timeout=12)
        require(cluster.wait_status(f"/groups/{group}", broker=4)["snapshot_index"] is None,
                "learner installed a snapshot before receiving its complete image")
        require(cluster.wait_status("/topics/live", broker=new_controller)[
            "partitions"][0]["voters"] == [1, 2, 3],
            "source placement changed while the target had an incomplete snapshot")
        cluster.kill([4])
        (gate / "release").touch()
        cluster.start([4], ready=False)
        eventually(
            "learner discards interrupted local staging",
            lambda: not command("docker", "exec", cluster.container(4), "test", "-e", incoming,
                                check=False).returncode == 0,
        )
        evidence.verified("EV08 learner SIGKILL after partial snapshot write leaves source placement intact",
                          interrupted_broker=4, first_chunk_bytes=int((gate / "started").read_text()))
    done = cluster.receipt(move_id, broker=new_controller, timeout=95)
    require(cluster.operate(move, id=move_id, broker=new_controller) == done,
            "operation retry produced a second revision")
    moved = cluster.wait_status("/topics/live", broker=new_controller)
    require(moved["partitions"][0]["voters"] == [2, 3, 4], "resumed move did not publish new membership")
    if args.fault_image:
        installed_snapshot = cluster.wait_status(f"/groups/{group}", broker=4)["snapshot_index"]
        require(installed_snapshot is not None and installed_snapshot >= snapshot,
                "replacement did not install the complete snapshot after retry")
    with confirm_lease(cluster, reader, "live", "existing", broker=4) as lease:
        require(lease.poll().after == 1, "resumed move changed the checkpoint")
    evidence.verified("EV08 pending move survives controller SIGKILL and commits after target starts",
                      old_controller=controller, new_controller=new_controller, operation_id=move_id,
                      committed_voters=[2, 3, 4])

    cluster.operate({"type": "rebalance", "topic": "live"}, timeout=95)
    require(cluster.wait_status("/topics/live")["partitions"][0]["voters"] == [1, 2, 3],
            "rebalance did not apply deterministic placement on six brokers")
    rejoined = cluster.wait_status(f"/groups/{group}", broker=1)
    require(rejoined["running"] and rejoined["snapshot_index"] is not None
            and rejoined["snapshot_index"] > snapshot,
            "retired broker did not rejoin with a newer snapshot than the RF1 cut")
    evidence.verified("EV08 retired RF1 voter rejoins without using the historical RF1 snapshot",
                      broker=1, historical_cut=snapshot, rejoin_cut=rejoined["snapshot_index"])
    append_id, append_receipt = cluster.submit(
        {"type": "add_partitions", "topic": "live", "partitions": 6},
    )
    old_producer = client(cluster, 5, token)
    retried = retry("old producer retry during partition append", lambda: old_producer.publish(
        "live", partition=0, event_id=first_id, data={"first": 1},
    ))
    require(retried == first, "partition append altered the old producer receipt")
    if append_receipt["state"] == "pending":
        cluster.receipt(append_id, timeout=100)
    after = cluster.wait_status("/topics/live")
    require({node for partition in after["partitions"] for node in partition["voters"]} == set(range(1, 7)),
            "growth did not use all six brokers")
    require(list(client(cluster, 4, token).partitions("live")) == list(range(6)),
            "discovery did not publish all partitions")
    with confirm_lease(cluster, reader, "live", "existing", broker=6, partition=5) as lease:
        require(lease.poll().after == 0, "new partition did not initialize existing subscription")
    require(client(cluster, 5, token).publish(
        "live", partition=0, event_id=first_id, data={"first": 1},
    ) == first, "new partitions changed old producer retry position")

    cluster.operate({"type": "drain", "broker_id": 1}, timeout=150)
    after_drain = cluster.wait_status("/topics/live", broker=2)
    require(all(1 not in partition["voters"] for partition in after_drain["partitions"]),
            "drain finished while a topic placement still depended on the source")
    _, metadata = cluster.leader("metadata", excluding=[1])
    require(1 not in metadata["membership"]["voters"], "drain left source as a metadata voter")
    cluster.kill([1])
    cluster.subscribe("live", "oracle")
    oracle = cluster.reader("live", "oracle")
    terminal, replay = read_all(cluster, oracle, "live", "oracle", broker=2)
    require(terminal == len(expected), "drain lost the committed high water")
    identity_oracle(replay, expected)
    evidence.verified("EV08 six brokers rebalance, append new partitions, and safely drain the old broker",
                      new_partitions=5, replay_events=len(replay), drained_broker=1)


def backup(clusters, temporary, args, evidence):
    source = make_cluster(clusters, temporary, args, 3, name="source")
    source.start([1, 2, 3])
    source.topic("archive", partitions=2, replication_factor=3)
    source.subscribe("archive", "snapshot-sub")
    definition = source.wait_status("/topics/archive")
    token = source.publisher("archive")
    reader = source.reader("archive", "snapshot-sub")
    expected = {}
    for partition in range(2):
        expected[partition] = {uuid4(): {"partition": partition, "n": n} for n in range(3)}
        client(source, 2, token).publish_batch("archive", partition=partition, events=(
            PublishRecord(event_id, data) for event_id, data in expected[partition].items()
        ))
        with confirm_lease(source, reader, "archive", "snapshot-sub", broker=3,
                           partition=partition) as lease:
            require(lease.ack(lease.poll(), through=1) == 1, "source checkpoint did not commit")
    bundle_area = temporary / "bundles"
    bundle_area.mkdir(mode=0o755)
    start = time.monotonic()
    source.docker("exec", "-T", source.service(1), "/usr/local/bin/nabedra-events",
                  "backup", "--config", CONFIG, "--output", "/tmp/acceptance-cut", timeout=120)
    backup_seconds = time.monotonic() - start
    bundle = bundle_area / "cut"
    bundle.mkdir(mode=0o755)
    # Docker can read tmpfs through exec. docker cp reads the container's
    # underlying filesystem and cannot see this read-only image's tmpfs.
    container = source.container(1)
    manifest_text = command(
        "docker", "exec", container, "cat", "/tmp/acceptance-cut/manifest.json",
    ).stdout
    (bundle / "manifest.json").write_text(manifest_text)
    manifest = json.loads(manifest_text)
    cuts = manifest["manifest"]["cuts"]
    require(set(cuts) == {"metadata", *(group_id(definition["id"], n) for n in range(2))},
            "manifest did not cover the committed topology")
    for group in cuts:
        contents = subprocess.run(
            ["docker", "exec", container, "cat", f"/tmp/acceptance-cut/{group}.snapshot"],
            capture_output=True, timeout=20, check=True,
        ).stdout
        (bundle / f"{group}.snapshot").write_bytes(contents)
    for group, cut in cuts.items():
        contents = (bundle / f"{group}.snapshot").read_bytes()
        digest = hashlib.sha256(contents[:-32]).digest()
        require(len(contents) == cut["bytes"] and contents[-32:] == digest
                and cut["sha256"] == digest.hex() and cut["applied_index"] is not None,
                "backup image length, checksum, or committed applied index differs")
    source.kill([1, 2, 3])

    def config(_, settings):
        settings["initialize_new_cluster"] = False
    target = make_cluster(clusters, temporary, args, 3, name="target", standby=1, config=config)
    require(target.cluster_id != source.cluster_id, "restore is using the source cluster identity")
    corrupt = bundle_area / "corrupt"
    shutil.copytree(bundle, corrupt)
    snapshot = corrupt / "metadata.snapshot"
    snapshot.chmod(0o644)
    contents = snapshot.read_bytes()
    snapshot.write_bytes(bytes([contents[0] ^ 1]) + contents[1:])
    result = target.cli(1, "restore", "--config", CONFIG, "--backup", "/backup/corrupt",
                        bind=f"{bundle_area}:/backup:ro", check=False)
    require(result.returncode != 0 and "checksum" in result.stderr.lower(),
            "corrupt snapshot reached fresh cluster storage")
    start = time.monotonic()
    for broker in (1, 2, 3, 1):
        target.cli(broker, "restore", "--config", CONFIG, "--backup", "/backup/cut",
                   bind=f"{bundle_area}:/backup:ro", timeout=120)
    target.start([1, 2, 3])
    require(target.wait_status("/topics/archive") == definition,
            "fresh cluster restore changed topic identity, placement, or subscription")
    status(target.rpc(1, "/v1/topics/archive/partitions", token=token), 401, "unauthorized")
    target_reader = target.reader("archive", "snapshot-sub")
    target_publisher = target.publisher("archive")
    for partition in range(2):
        with confirm_lease(target, target_reader, "archive", "snapshot-sub", broker=2,
                           partition=partition) as lease:
            batch = lease.poll()
            require(batch.after == 1 and [event.offset for event in batch.events] == [2, 3],
                    "restore did not preserve committed prefix and unacknowledged suffix")
            suffix = dict(list(expected[partition].items())[1:])
            identity_oracle(batch.events, suffix, after=1)
            require(lease.ack(batch, through=3) == 3, "fresh cluster cannot advance old checkpoint")
        client_out = client(target, 3, target_publisher)
        old_id, old_data = next(iter(expected[partition].items()))
        require(client_out.publish("archive", partition=partition, event_id=old_id, data=old_data).offset == 1,
                "fresh cluster restore forgot the original dedup identity")
        require(client_out.publish("archive", partition=partition, event_id=uuid4(), data="new").offset == 4,
                "fresh cluster restore did not continue the original offset sequence")
    evidence.verified("EV10 Docker RF3 cut restores onto fresh disks and rejects corruption and old grants",
                      backups=len(cuts), backup_seconds=round(backup_seconds, 3),
                      restore_seconds=round(time.monotonic() - start, 3), committed_checkpoints=2,
                      source_cluster_id=source.cluster_id, restored_cluster_id=target.cluster_id)



def mtls_rotation(clusters, temporary, args, evidence):
    certificates = Certificates(temporary / "certificates", 3)
    ca = str(certificates.root / "ca.pem")
    context = ssl.create_default_context(cafile=ca)
    cluster = make_cluster(clusters, temporary, args, 3, config=certificates.config, tls=context)
    cluster.ca_file = ca
    certificates.mount(cluster)
    cluster.start([1, 2, 3])
    event_topic = cluster.topic("secure", replication_factor=3)
    cluster.subscribe("secure", "secure-reader")
    publish = cluster.publisher("secure")
    read = cluster.reader("secure", "secure-reader")
    leader, _ = cluster.leader(group_id(event_topic["id"]))
    ingress = next(broker for broker in (1, 2, 3) if broker != leader)
    event_id = uuid4()
    ingress_client = client(cluster, ingress, publish)
    original = retry("mTLS follower forwards publish", lambda: ingress_client.publish(
        "secure", partition=0, event_id=event_id, data={"tls": True},
    ))
    require(original.offset == 1, "mTLS peer forwarding did not commit on RF3")
    peer_url = "https://" + cluster.docker("port", cluster.service(1), "8081").stdout.strip()
    path = "/internal/events/v2/control"
    status(request(peer_url, path, method="POST", body={}, tls=context), 0)
    authorized_tls = ssl.create_default_context(cafile=ca)
    authorized_tls.load_cert_chain(
        str(certificates.nodes[1] / "cert.pem"), str(certificates.nodes[1] / "key.pem"),
    )
    status(request(peer_url, path, method="POST", body={}, tls=authorized_tls), 401, None)

    route = subscription_route("secure", "secure-reader")

    def reader_request(method, suffix, *, body=None, lease=None, expected=200):
        response = cluster.rpc(3, route + suffix, method=method, token=read, lease=lease, body=body)
        if response[0] in (0, 429, 503):
            return None
        reply = status(response, expected)
        return reply if reply is not None else True

    claim_body = {"claim_id": str(uuid4())}
    claim = eventually("mTLS claim through broker 3", lambda: reader_request(
        "POST", "/claim", body=claim_body,
    ))
    lease = claim["lease"]
    page = eventually("mTLS delivery through broker 3", lambda: reader_request(
        "GET", "/events", lease=lease,
    ))
    require(page["after"] == "0" and len(page["events"]) == 1
            and page["events"][0]["event_id"] == str(original.event_id),
            "mTLS replay differed from publish")
    initial_mounts = cluster.mounts()
    new_secrets = {purpose: secrets.token_hex(32) for purpose in ("grant", "session")}

    # Prestage both new keys in every broker before selecting the active keys.
    # The old grant and delivery token must still verify after a full rolling
    # restart through the old and new active-key configuration.
    for active in (False, True):
        for broker in (1, 2, 3):
            path = cluster.directory / f"broker-{broker}.json"
            settings = json.loads(path.read_text())
            for purpose, secret in new_secrets.items():
                keys = settings["keyring"][purpose]
                if active:
                    keys["active"] = "rotated"
                else:
                    keys["keys"].append({
                        "id": "rotated", "secret_hex": secret,
                        "accept_until_ms": time.time_ns() // 1_000_000 + 2 * 86_400_000,
                    })
            cluster.kill([broker])
            path.write_text(json.dumps(settings))
            cluster.start([broker])
        require(cluster.mounts() == initial_mounts, "key rotation replaced a disk")

    # The Docker port may change after restart. Send exactly the old grant,
    # lease and delivery token to the current loopback endpoint.
    ack = eventually("ack using pre-rotation delivery", lambda: reader_request(
        "PUT", "/checkpoint", body={"delivery": page["delivery"], "through": "1"}, lease=lease,
    ))
    require(ack["offset"] == "1", "old delivery token returned the wrong checkpoint")
    eventually("release pre-rotation lease", lambda: reader_request(
        "POST", "/release", body={}, lease=lease, expected=(200, 204),
    ))
    rotated_grant = cluster.publisher("secure")
    require(rotated_grant.split(".", 1)[0] == "rotated", "new signing key did not issue the grant")
    for broker in (1, 2, 3):
        preserved = retry("same immutable receipt through rotated ingress", lambda: client(
            cluster, broker, rotated_grant,
        ).publish("secure", partition=0, event_id=event_id, data={"tls": True}))
        require(preserved == original, "rotation mutated stored publish receipt")
    with confirm_lease(cluster, read, "secure", "secure-reader") as lease:
        require(retry("poll using rotated session", lease.poll).after == 1,
                "rotated session lost committed checkpoint")
        retry("release rotated lease", lambda: lease.close() or True)
    evidence.verified("EV10 Docker peer mTLS and rolling signing key rotation preserve old grant and delivery",
                      broker_restarts=6, durable_checkpoint=1)


def percentile(values, quantile):
    values = sorted(values)
    index = max(0, min(len(values) - 1, round(quantile * len(values) - 1)))
    return values[index]


def load(clusters, temporary, args, evidence, count):
    def config(_, settings):
        settings["http_limits"] = {"lease_ms": 90_000}

    cluster = make_cluster(clusters, temporary, args, count, config=config)
    cluster.start(range(1, count + 1))
    partitions = min(count, 6)
    factor = 1 if count == 1 else 3
    cluster.topic("bench", partitions=partitions, replication_factor=factor)
    for name in ("fast", "slow"):
        cluster.subscribe("bench", name)
    token = cluster.publisher("bench")
    read_tokens = {name: cluster.reader("bench", name) for name in ("fast", "slow")}
    publishers = [client(cluster, broker, token) for broker in sorted(cluster.running)]
    expected = {partition: {} for partition in range(partitions)}
    offsets = {partition: 0 for partition in range(partitions)}
    test_stats = []
    encoded_bytes = 0
    event_namespace = UUID("91cc4a30-4813-47cb-95ff-0a829ed4f53d")
    for repetition, shape in enumerate(("warmup", "balanced", "skewed", "balanced")):
        work = []
        for index in range(args.load_events if repetition else max(8, args.load_events // 4)):
            partition = index % partitions if shape != "skewed" or index % 5 == 0 else 0
            event_id = uuid5(event_namespace, f"{repetition}:{index}")
            data = {"run": repetition, "index": index, "padding": "p" * 512}
            work.append((partition, event_id, data))
            expected[partition][event_id] = data
            encoded_bytes += len(encode({"event_id": str(event_id), "data": data}))

        def send(task):
            index, (partition, event_id, data) = task
            ingress = publishers[index % len(publishers)]
            attempts = []
            start = time.monotonic()
            event = retry("load publish", lambda: ingress.publish(
                "bench", partition=partition, event_id=event_id, data=data,
            ), attempts)
            require(event.event_id == event_id and event.data == data, "load receipt changed event identity")
            return partition, event.offset, (time.monotonic() - start) * 1000, attempts

        round_data = {
            partition: {eid: data for p, eid, data in work if p == partition}
            for partition in range(partitions)
        }
        active = [partition for partition, records in round_data.items() if records]
        readers_ready = Barrier(len(active) + 1, timeout=15)
        progressing = Event()

        def receive(partition):
            seen = []
            timings = []
            poll_retries = []
            ack_retries = []
            with confirm_lease(cluster, read_tokens["fast"], "bench", "fast",
                               broker=partition + 1, partition=partition) as lease:
                readers_ready.wait()
                deadline = time.monotonic() + 45
                while len(seen) < len(round_data[partition]):
                    require(time.monotonic() < deadline, "fast subscriber did not catch up")
                    started = time.monotonic()
                    batch = retry("load poll", lambda: lease.poll(limit=50), poll_retries)
                    timings.append((time.monotonic() - started) * 1_000)
                    if not batch.events:
                        time.sleep(0.01)
                        continue
                    through = batch.events[-1].offset
                    checkpoint = retry("load checkpoint", lambda: lease.ack(batch, through=through),
                                       ack_retries)
                    require(checkpoint == through, "load checkpoint changed on retry")
                    seen.extend(batch.events)
                    progressing.set()
                retry("release load lease", lambda: lease.close() or True)
                return partition, seen, timings, poll_retries, ack_retries

        with ThreadPoolExecutor(max_workers=len(active)) as consumers:
            readers = [consumers.submit(receive, partition) for partition in active]
            readers_ready.wait()
            start = time.monotonic()
            with ThreadPoolExecutor(max_workers=8) as workers:
                indexed = list(enumerate(work))
                first = len(indexed) // 2
                sent = list(workers.map(send, indexed[:first]))
                require(progressing.wait(10), "publish/consume overlap barrier was not crossed")
                sent.extend(workers.map(send, indexed[first:]))
            duration = time.monotonic() - start
            received = {partition: (events, poll_samples, polls, acks)
                        for partition, events, poll_samples, polls, acks
                        in (future.result() for future in readers)}

        confirmed = {partition: [] for partition in range(partitions)}
        for partition, offset, _, _ in sent:
            confirmed[partition].append(offset)
        for partition in range(partitions):
            old = offsets[partition]
            offsets[partition] += len(confirmed[partition])
            require(sorted(confirmed[partition]) == list(range(old + 1, offsets[partition] + 1)),
                    "load acknowledgments reused or skipped offsets")
            events, *_ = received.get(partition, ([],))
            identity_oracle(events, round_data[partition], after=old)
        if repetition:
            latencies = [latency for _, _, latency, _ in sent]
            retries = [code for *_, codes in sent for code in codes]
            poll_lats = [latency for _, samples, _, _ in received.values() for latency in samples]
            polls = [code for _, _, reasons, _ in received.values() for code in reasons]
            acks = [code for _, _, _, reasons in received.values() for code in reasons]
            test_stats.append({
                "phase": shape,
                "events": len(sent),
                "seconds": round(duration, 4),
                "events_per_second": round(len(sent) / duration, 2),
                "p50_publish_ms": round(statistics.median(latencies), 2),
                "p99_publish_ms": round(percentile(latencies, 0.99), 2),
                "p99_poll_ms": round(percentile(poll_lats, 0.99), 2),
                "retry_responses": {
                    name: {code: reasons.count(code) for code in sorted(set(reasons))}
                    for name, reasons in (("publish", retries), ("poll", polls), ("checkpoint", acks))
                },
            })

    lag = {}
    for partition in range(partitions):
        broker = partition + 1
        with confirm_lease(cluster, read_tokens["slow"], "bench", "slow",
                           broker=broker, partition=partition) as lease:
            page = retry("sample slow subscriber lag", lambda: lease.poll(limit=1))
            require(page.after == 0, "slow subscription checkpoint followed the fast consumer")
            lag[partition] = offsets[partition] - page.after
            retry("release slow lease", lambda: lease.close() or True)
        last, events = read_all(cluster, read_tokens["slow"], "bench", "slow",
                                broker=broker, partition=partition)
        require(last == offsets[partition], "slow subscription did not catch up")
        identity_oracle(events, expected[partition])

    containers = [cluster.container(broker) for broker in sorted(cluster.running)]
    container_stats = command("docker", "stats", "--no-stream", "--format", "{{json .}}",
                              *containers, timeout=30)
    samples = [cgroup_sample(container) for container in containers]
    disk_bytes = sum(sample.get("disk_bytes", 0) for sample in samples)
    evidence.verified(
        f"EV09 {count} broker Docker load run with independent fast and slow replay oracles",
        brokers=count, replication_factor=factor, broker_caps=cluster.caps, stages=test_stats,
        payload_distribution="512-byte padding per JSON object, 8 concurrent producers",
        event_id_namespace=str(event_namespace),
        arrival_schedule="Fast subscriber commits before second half of each publish round",
        asserted_ids=sum(offsets.values()), duplicate_ids=0, lost_ids=0,
        slow_subscription_lag_by_partition=lag, post_replay_lag=0,
        docker_stats=[json.loads(row) for row in container_stats.stdout.splitlines() if row],
        cgroup_samples=samples, encoded_publish_record_bytes=encoded_bytes,
        rough_disk_to_ingress_ratio=round(disk_bytes / encoded_bytes, 2) if disk_bytes else None,
    )


CASES = {"rf1": rf1_http, "quorum": rf3_quorum, "growth": growth, "backup": backup, "tls": mtls_rotation}



def source_hash():
    digest = hashlib.sha256()
    paths = [
        ROOT / "server/events/Cargo.toml", ROOT / "server/events/Cargo.lock",
        ROOT / "server/events/deploy/fixture.py",
        *ROOT.glob("server/events/src/**/*.rs"),
        *ROOT.glob("orm/python/src/nabedra/events/*.py"),
        *SUITE_DIR.glob("*.py"),
    ]
    for path in sorted(paths):
        digest.update(path.relative_to(ROOT).as_posix().encode() + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="locally built Events Docker image")
    parser.add_argument("--cpus", type=Decimal, default=Decimal("2"))
    parser.add_argument("--memory-mib", type=int, default=1536)
    parser.add_argument("--cases", default="rf1,quorum,growth,backup,tls,load-1,load-3,load-6")
    parser.add_argument("--load-events", type=int, default=96)
    parser.add_argument("--fault-image", help="locally present Python image for the growth snapshot gate")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--logs", type=Path, help="broker logs from failed scenarios")
    args = parser.parse_args()
    available = {*CASES, "load-1", "load-3", "load-6"}
    cases = args.cases.split(",")
    if any(case not in available for case in cases):
        parser.error(f"cases must be chosen from {', '.join(sorted(available))}")
    if not args.cpus.is_finite() or not 0 < args.cpus <= 4 or not 160 <= args.memory_mib <= 8192:
        parser.error("resource limits must fit 4 CPUs and 8 GiB")
    if not 8 <= args.load_events <= 4096:
        parser.error("choose 8 through 4096 measured events per repetition")
    if args.logs:
        args.logs.mkdir(parents=True, exist_ok=True)
    image = command("docker", "image", "inspect", "--format", "{{.Id}}", args.image).stdout.strip()
    if args.fault_image and "growth" in cases:
        command("docker", "image", "inspect", args.fault_image)
    docker = command(
        "docker", "info", "--format",
        '{"n_cpus":{{.NCPU}},"memory":{{.MemTotal}},"arch":"{{.Architecture}}","server":"{{.ServerVersion}}"}',
    ).stdout.strip()
    revision = command("git", "-C", str(ROOT), "rev-parse", "HEAD").stdout.strip()
    evidence = Evidence()
    output = {
        "docker": json.loads(docker),
        "image_id": image,
        "host": platform.platform(),
        "git_base": revision,
        "checkout_source_sha256": source_hash(),
        "caps": {"cpus": str(args.cpus), "memory_mib": args.memory_mib},
        "records_per_load_repetition": args.load_events,
        "cases": {},
        "checks": evidence.checks,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    failed = False
    for case in cases:
        print(f"Running {case}", flush=True)
        started = time.monotonic()
        try:
            with TemporaryDirectory(prefix="nabedra-events-qa-") as directory, fixtures(
                diagnostics=args.logs,
            ) as clusters:
                if case in CASES:
                    CASES[case](clusters, Path(directory), args, evidence)
                else:
                    load(clusters, Path(directory), args, evidence, int(case.split("-")[1]))
        except (Exception, KeyboardInterrupt) as error:
            failed = True
            failure = str(error) or type(error).__name__
            output["cases"][case] = {"pass": False, "error": failure}
            print(f"{case} failed: {failure}", file=sys.stderr, flush=True)
            if args.logs:
                (args.logs / f"{case}.traceback").write_text(traceback.format_exc())
            if isinstance(error, KeyboardInterrupt):
                output["cases"][case]["seconds"] = round(time.monotonic() - started, 3)
                args.output.write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n")
                return 130
        else:
            output["cases"][case] = {"pass": True}
        output["cases"][case]["seconds"] = round(time.monotonic() - started, 3)
        args.output.write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n")
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())

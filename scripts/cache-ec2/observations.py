#!/usr/bin/env python3
"""Collect bounded cache growth observations on the named private EC2 stack."""

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
import re
import shlex
import sys
import time
from pathlib import Path

SLOTS = 16_384
MAX_METRIC_BYTES = 16 << 20
REMOTE = "/usr/local/lib/nabedra-cache/observations.py"
METRICS = Path("/run/nabedra-cache/metrics.json")
NODE_CONFIG = Path("/etc/nabedra-cache/node/node.toml")
CGROUP = Path("/sys/fs/cgroup/system.slice/nabedra-cache.service")
NETWORK = Path("/sys/class/net")
STACK_READY = {"CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"}


def positive(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("growth requires positive, finite capacity measurements")
    return value


def nonnegative(value):
    if value != 0 or isinstance(value, bool):
        return positive(value)
    return value


def primary_digest(slots):
    ordered = sorted(slots)
    return hashlib.sha256(json.dumps(ordered, separators=(",", ":")).encode()).hexdigest()


def topology(workers):
    from reconcile import BINARY, OPERATOR, Operator, ready

    status = Operator(BINARY, OPERATOR / "node.toml", time.monotonic() + 10).status()
    slots = defaultdict(list)
    for slot, assigned in enumerate(status["slots"]):
        identity = assigned["primary"]
        if identity is not None:
            slots[identity["node"]].append((slot, assigned["epoch"]))
    ready_now = ready(status, workers)
    return {
        "generation": status["generation"], "revision": status["revision"],
        "ready": ready_now, "moves": sum(row["moving"] is not None for row in status["slots"]),
        "draining": sorted(set(status["draining"]) - set(status["drained"])),
        "nodes": {
            str(node): {
                "worker": status["workers"][str(node)]["id"],
                "zone": status["workers"][str(node)]["zone"],
                "primary_digest": primary_digest(slots[node]),
                "primaries": len(slots[node]),
            }
            for node in range(1, workers + 1)
            if str(node) in status["workers"]
        },
    }


def data_limit(path):
    # Worker bootstrap emits the charged cap as a TOML integer. A missing or
    # changed format cannot serve as an observed capacity.
    with path.open(encoding="utf-8") as config:
        content = config.read(1 << 20)
    match = re.search(r"(?m)^\[client\.limits\]\s*\n([^\[]+)", content)
    cap = re.search(r"(?m)^data_bytes\s*=\s*([0-9]+)\s*$", match[1]) if match else None
    if cap is None:
        raise ValueError("worker config does not declare a charged data cap")
    return positive(int(cap[1]))


def read_metrics(path):
    with path.open("rb") as data:
        age = (time.time_ns() - os.fstat(data.fileno()).st_mtime_ns) / 1e9
        content = data.read(MAX_METRIC_BYTES + 1)
    if not 0 <= age <= 5 or len(content) > MAX_METRIC_BYTES:
        raise ValueError("worker metrics are stale or oversized")
    return json.loads(content)


def effective_cores(cgroup):
    # systemd can leave cpuset disabled at the leaf when the service
    # inherits its parent's affinity.
    scope = (cgroup, cgroup.parent, cgroup.parent.parent)
    cpus = next(
        (path.read_text().strip() for parent in scope
         if (path := parent / "cpuset.cpus.effective").exists()), ""
    )
    if not cpus:
        raise ValueError("effective worker CPU set is unavailable")
    effective = 0
    for entry in cpus.split(","):
        first, _, last = entry.partition("-")
        effective += 1 + int(last or first) - int(first)
    for parent in scope:
        path = parent / "cpu.max"
        if path.exists():
            quota, interval = path.read_text().split()
            if quota != "max":
                effective = min(effective, int(quota) / int(interval))
    return positive(effective)


def sample(interface, metric_path=METRICS, cgroup=CGROUP, network=NETWORK):
    state = read_metrics(metric_path)
    identity = (state["generation"], state["revision"])
    if not 1 <= len(state["actors"]) == state["actor_count"] <= 64:
        raise ValueError("incomplete worker actor metrics")
    counters = {}
    primaries = {
        row["slot"]: row["epoch"] for row in state["slots"]
        if row["baseline"] and row["serving_primary"]
    }
    for actor in state["actors"]:
        if (actor["generation"], actor["revision"]) != identity:
            raise ValueError("actor report belongs to a different topology")
        for slot in actor["slot_stats"]:
            key = slot["slot"]
            if key not in primaries:
                continue
            if (key in counters or not 0 <= key < SLOTS or primaries[key] != slot["epoch"]
                    or not slot["lease_headroom_ms"]):
                raise ValueError("inconsistent worker primary metrics")
            counters[key] = (slot["epoch"], nonnegative(slot["operations"]), nonnegative(slot["write_operations"]))
    if not counters or counters.keys() != primaries.keys():
        raise ValueError("worker lacks complete primary metrics with valid authority")
    cpu = dict(line.split() for line in (cgroup / "cpu.stat").read_text().splitlines())
    nic = network / interface / "statistics"
    return {
        "at": time.monotonic(), "identity": identity, "worker": state["worker"],
        "counters": counters, "cpu_us": int(cpu["usage_usec"]),
        "cores": effective_cores(cgroup), "memory_max": positive(int((cgroup / "memory.max").read_text())),
        "charged": sum(actor["primary_bytes"] + actor["replica_bytes"] for actor in state["actors"]),
        "rss": positive(state["rss_bytes"]),
        "oldest_queued_ms": max((actor["oldest_queued_us"] or 0 for actor in state["actors"]), default=0) / 1000,
        "rx_bytes": int((nic / "rx_bytes").read_text()), "tx_bytes": int((nic / "tx_bytes").read_text()),
    }


def summarize(first, last, cap, capacity):
    duration = positive(last["at"] - first["at"])
    if (
        first["identity"] != last["identity"] or first["worker"] != last["worker"]
        or first["counters"].keys() != last["counters"].keys()
        or first["cores"] != last["cores"] or first["memory_max"] != last["memory_max"]
    ):
        raise ValueError("worker identity, capacity or topology changed during observation")
    hot_writes, total_writes, pressured = 0, 0, 0
    for key, (epoch, operations, writes) in last["counters"].items():
        old_epoch, old_operations, old_writes = first["counters"][key]
        if epoch != old_epoch or not 0 <= writes - old_writes <= operations - old_operations:
            raise ValueError("slot counters or authority changed during observation")
        written = writes - old_writes
        hot_writes = max(hot_writes, written)
        total_writes += written
        pressured += (operations - old_operations) / duration >= capacity["slot_ops_per_second"]

    def rate(key):
        return nonnegative(last[key] - first[key]) / duration

    return {
        "worker": last["worker"], "generation": last["identity"][0], "revision": last["identity"][1],
        "primary_digest": primary_digest((slot, counts[0]) for slot, counts in last["counters"].items()),
        "primaries": len(last["counters"]), "pressured": pressured,
        "hot_writes": hot_writes, "total_writes": total_writes,
        "memory_ratio": max(first["charged"], last["charged"]) / cap,
        "rss_ratio": max(first["rss"], last["rss"]) / last["memory_max"],
        "oldest_queued_ms": max(first["oldest_queued_ms"], last["oldest_queued_ms"]),
        "cpu_ratio": rate("cpu_us") / 1e6 / min(last["cores"], capacity["cpu_cores"]),
        "rx_ratio": rate("rx_bytes") / capacity["network_rx_bytes_per_second"],
        "tx_ratio": rate("tx_bytes") / capacity["network_tx_bytes_per_second"],
    }


def worker_observation(capacity):
    interface = capacity["interface"]
    if re.fullmatch(r"[a-zA-Z0-9_-]{1,15}", interface) is None:
        raise ValueError("provide the named EC2 network interface")
    seconds = positive(capacity["sample_seconds"])
    if not 2 <= seconds <= 10:
        raise ValueError("worker sample window must be 2 to 10 seconds")
    for key in ("slot_ops_per_second", "cpu_cores", "network_rx_bytes_per_second", "network_tx_bytes_per_second"):
        positive(capacity[key])
    cap = data_limit(NODE_CONFIG)
    first = sample(interface)
    time.sleep(seconds)
    return summarize(first, sample(interface), cap, capacity)


def fleet_snapshot(aws, stack):
    response = aws.run("cloudformation", "describe-stacks", "--stack-name", stack)
    live, = response["Stacks"]
    if live["StackStatus"] not in STACK_READY:
        raise ValueError("cache stack has a pending operation or is unavailable")
    outputs = {entry["OutputKey"]: entry["OutputValue"] for entry in live["Outputs"]}
    count = int(outputs["Workers"])
    if count not in (3, 6, 12) or not re.fullmatch(r"[0-9a-f]{64}", outputs["DeploymentFingerprint"]):
        raise ValueError("stack lacks an expected private cache deployment")
    voters = tuple(outputs[f"Controller{node}"] for node in range(1, 4))
    workers = tuple(outputs[f"Worker{node}"] for node in range(1, count + 1))
    all_ids = voters + workers
    if len(set(all_ids)) != len(all_ids) or any(not re.fullmatch(r"i-(?:[a-f0-9]{8}|[a-f0-9]{17})", value) for value in all_ids):
        raise ValueError("stack contains invalid cache instance IDs")
    return live["StackId"], voters, workers


def aggregate(controller, samples, policy, now):
    if not controller["ready"] or controller["moves"] or controller["draining"]:
        raise ValueError("cache placement must be fully ready before measuring growth")
    zones = defaultdict(list)
    for node, metric in enumerate(samples, 1):
        expected = controller["nodes"][str(node)]
        if (
            (metric["generation"], metric["revision"]) != (controller["generation"], controller["revision"])
            or metric["worker"] != expected["worker"]
            or metric["primary_digest"] != expected["primary_digest"] or metric["primaries"] != expected["primaries"]
        ):
            raise ValueError("worker observation does not match committed placement")
        zones[expected["zone"]].append(metric)
    if len(zones) != 3 or sum(metric["primaries"] for metric in samples) != SLOTS:
        raise ValueError("worker observations do not cover the three-zone primary topology")
    if not 0 < positive(policy["zone_rss_limit"]) <= 1:
        raise ValueError("invalid recovery RSS headroom limit")
    # Fleet profiles use the same shape and limits for all workers. Charge
    # a survivor's own utilization plus all measured traffic in any lost
    # zone. Qualify actual failover traffic separately.
    nr = ("cpu_ratio", "rx_ratio", "tx_ratio")
    for key in nr:
        ceiling = positive(policy[f"zone_{key}_limit"])
        if ceiling > 1:
            raise ValueError("invalid zone recovery utilization limit")
    zone_reserve_ok = all(
        metric["rss_ratio"] <= policy["zone_rss_limit"]
        and all(metric[key] + sum(row[key] for row in lost) <= policy[f"zone_{key}_limit"] for key in nr)
        for zone, survivors in zones.items() for name, lost in zones.items() if zone != name for metric in survivors
    )
    writes = sum(row["total_writes"] for row in samples)
    return {
        "observed_at": now, "generation": controller["generation"], "revision": controller["revision"],
        "groups": len(samples), "moves": 0, "provisioning": False, "zone_reserve_ok": zone_reserve_ok,
        "attempts": 0, "last_attempt": 0,
        "monthly_spend_units": nonnegative(policy["fixed_monthly_cost_units"])
        + len(samples) * positive(policy["monthly_worker_cost_units"]),
        "memory_ratio": max(metric["memory_ratio"] for metric in samples),
        "oldest_queued_ms": max(metric["oldest_queued_ms"] for metric in samples),
        "slots_under_pressure_ratio": sum(metric["pressured"] for metric in samples) / SLOTS,
        "hottest_slot_write_share": max(metric["hot_writes"] for metric in samples) / writes if writes else 0,
        "max_cpu_ratio": max(metric["cpu_ratio"] for metric in samples),
        "max_network_ratio": max(max(metric["rx_ratio"], metric["tx_ratio"]) for metric in samples),
    }


def collect(aws, stack, policy, now=time.time):
    identity, voters, workers = fleet_snapshot(aws, stack)

    def controller():
        command = shlex.join(["python3", REMOTE, "local-topology", str(len(workers))])
        return json.loads(aws.ssm(voters[0], ["exec " + command], 15))

    first = controller()
    if not first["ready"] or first["moves"] or first["draining"]:
        raise ValueError("cache placement is still changing")
    capacity = {key: policy["observation"][key] for key in (
        "interface", "sample_seconds", "slot_ops_per_second", "cpu_cores",
        "network_rx_bytes_per_second", "network_tx_bytes_per_second",
    )}
    command = "exec " + shlex.join(["python3", REMOTE, "local-worker", json.dumps(capacity)])
    started = now()
    with ThreadPoolExecutor(max_workers=len(workers)) as pool:
        samples = list(pool.map(lambda node: json.loads(aws.ssm(node, [command], 20)), workers))
    if controller() != first or fleet_snapshot(aws, stack) != (identity, voters, workers):
        raise ValueError("cache placement or provider instance IDs changed during collection")
    finished = int(now())
    if not 0 <= finished - int(started) <= min(45, policy["observation_max_age_seconds"]):
        raise ValueError("collection exceeded the growth observation age limit")
    observation = aggregate(first, samples, policy, finished)
    return identity, observation


def retain(old, observation, identity, max_age):
    scope = (identity, observation["generation"], observation["revision"], observation["groups"])
    prior = {}
    for point in old:
        seen = (point.get("stack_id"), point.get("generation"), point.get("revision"), point["groups"])
        if seen == scope and 0 < observation["observed_at"] - point["observed_at"] <= max_age:
            prior[point["observed_at"]] = point
    latest = [prior[stamp] for stamp in sorted(prior)[-2:]]
    return latest + [{**observation, "stack_id": identity}]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="action", required=True)
    remote_topology = subcommands.add_parser("local-topology")
    remote_topology.add_argument("workers", type=int, choices=(3, 6, 12))
    remote_worker = subcommands.add_parser("local-worker")
    remote_worker.add_argument("capacity")
    caller = subcommands.add_parser("collect")
    caller.add_argument("--policy", required=True, type=Path)
    caller.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.action == "local-topology":
        print(json.dumps(topology(args.workers), allow_nan=False))
    elif args.action == "local-worker":
        print(json.dumps(worker_observation(json.loads(args.capacity)), allow_nan=False))
    else:
        from deploy import Aws, AwsError
        from growth import save
        from settings import read_json, require
        try:
            policy = read_json(args.policy)
            if not policy.get("provider_configured"):
                raise ValueError("cache growth observation requires a configured provider")
            region, stack = policy["stack"].split("/")
            require(r"[a-z0-9-]{1,32}", region, "AWS region")
            require(r"[A-Za-z][A-Za-z0-9-]{0,127}", stack, "CloudFormation stack")
            identity, observation = collect(Aws(region), stack, policy)
            old = read_json(args.output) if args.output.exists() else []
            save(args.output, retain(old, observation, identity, policy["observation_max_age_seconds"]))
            print(json.dumps(observation, allow_nan=False))
        except (ValueError, TypeError, KeyError, OSError, AwsError) as error:
            parser.error(str(error))


if __name__ == "__main__":
    main()

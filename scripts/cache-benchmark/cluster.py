#!/usr/bin/env python3
"""K04b paired measurements against already deployed private clusters."""

import argparse
import base64
import collections
import itertools
import json
import math
import os
from pathlib import Path
import random
import sys
import tempfile
from urllib.parse import urlsplit

from driver import encoded, redis_client
from run import (
    HERE, capacity, confidence, csv_values, json_dump, paired_result, run_load, summary,
    validate_writes,
)

ENGINES = ("nabedra", "valkey")
SIZES = (3, 6, 12)


def configuration(args, config, groups, engine):
    cluster = config["clusters"][str(groups)][engine]
    endpoints = cluster["endpoints"]
    parsed = [urlsplit(value) for value in endpoints]
    if not groups <= len(parsed) <= 2 * groups or len(set(endpoints)) != len(endpoints) or any(
        item.scheme != "rediss" or not item.hostname or not item.port or
        item.username or item.password or item.path for item in parsed
    ):
        raise ValueError(f"{groups}/{engine}: supply one TLS URI for every primary and replica")
    certificate = str(Path(cluster["certificate"]).resolve())
    password = os.environ[cluster["password_env"]]
    return {
        "cluster": True, "tls": True, "transport": "tls", "cold": False, "pipeline": 1, "port": parsed[0].port,
        "endpoints": endpoints, "certificate": certificate, "password": password,
        "username": cluster.get("username"), "timeout_ms": args.timeout_ms,
        "keys": args.keys_per_group * groups, "ttl_ms": args.ttl_ms, "lanes": groups * args.lanes_per_group,
        "client_processes": len(args.client_cpus),
    }


def assert_placement(spec, groups):
    from redis.cluster import key_slot

    with redis_client(spec) as client:
        slots = client.cluster_slots()
    owners = set()
    replicas = set()
    cursor = 0
    slot_owners = []
    for (start, end), placement in sorted(slots.items()):
        if start != cursor or end < start or len(placement["replicas"]) != 1:
            raise RuntimeError("the cluster is missing slot coverage or its required replica")
        primary = placement["primary"]
        replica = placement["replicas"][0]
        if primary == replica:
            raise RuntimeError("a copy is placed on its primary")
        owners.add(primary)
        replicas.add(replica)
        slot_owners.extend([primary] * (end - start + 1))
        cursor = end + 1
    if cursor != 16_384 or len(owners) != groups or len(replicas) != groups:
        raise RuntimeError("reported slots do not match the required primary and replica counts")
    warm = {}
    for index in range(spec["keys"]):
        key = f"key:{index}"
        warm.setdefault(slot_owners[key_slot(encoded(key))], key)
    if warm.keys() != owners:
        raise RuntimeError("increase keys per group so the benchmark reaches each primary")
    spec["warm_keys"] = list(warm.values())
    return {"groups": groups, "primary_endpoints": sorted(owners), "replica_endpoints": sorted(replicas)}


def fill(spec):
    value = spec["value"]
    if spec["workload"] == "set-px":
        value = bytes([value[0] ^ 255]) + value[1:]
    with redis_client(spec) as client:
        for key in range(spec["keys"]):
            if client.set(encoded(f"key:{key}"), value) is not True:
                raise RuntimeError("cluster preload failed")
        for key in (0, spec["keys"] - 1):
            if client.get(encoded(f"key:{key}")) != value:
                raise RuntimeError("cluster preload verification failed")


def measure(args, base, driver, workload, size, distribution, rate, seed, name, transient):
    raw_value = random.Random(seed).randbytes(size)
    spec = {
        **base, "value_b64": base64.b64encode(raw_value).decode("ascii"),
        "seed": seed, "rate": rate, "samples": args.samples, "warmup": max(1, round(rate * args.warmup_seconds)),
        "driver": driver, "workload": workload, "distribution": distribution,
    }
    fill({**spec, "value": raw_value})
    outputs, _, _ = run_load(args, spec, name, transient)
    expected = {**spec, "value": raw_value}
    result, written = summary(outputs, expected)
    result["set_px_verification"] = validate_writes(expected, written)
    result["seed"] = seed
    for output in outputs:
        Path(f"{output}.ready").unlink()
    json_dump(args.output / f"{name}.json", result)
    print(name, f"{result['goodput']:.1f} correct ops/s", result["latency_ns"], result["outcomes"], flush=True)
    return result


def matrix(args, config, transient):
    profiles = {}
    for groups in sorted(set(args.groups)):
        endpoints = {
            engine: configuration(args, config, groups, engine)
            for engine in ENGINES
        }
        specs = {engine: assert_placement(base, groups) for engine, base in endpoints.items()}
        cases = {}
        for driver, workload, size in itertools.product(args.drivers, args.workloads, args.sizes):
            label = f"{groups}-{driver}-{workload}-{size}"
            # Separately calibrate the reference before selecting the shared load.
            rates = [round(rate * groups / 3) for rate in args.rates]
            calibrated = [
                measure(args, endpoints["valkey"], driver, workload, size, args.distribution, rate,
                        args.seed, f"{label}-valkey-cal-{rate}", transient)
                for rate in rates
            ]
            target = capacity(calibrated, args.p99_ms)
            common = [] if target is None else [
                max(1, int(target["rate"] * fraction)) for fraction in (0.05, 0.50, 0.80)
            ]
            rates = sorted(set(rates + common))
            pairs = collections.defaultdict(list)
            peaks = {engine: [] for engine in ENGINES}
            for repeat in range(args.repetitions):
                seed = args.seed + repeat + 1
                sweep = {engine: [] for engine in ENGINES}
                for rate in rates:
                    pair = {}
                    order = random.Random(seed * 104729 + rate).sample(ENGINES, k=2)
                    for engine in order:
                        pair[engine] = measure(args, endpoints[engine], driver, workload, size,
                                               args.distribution, rate, seed,
                                               f"{label}-{rate}-r{repeat}-{engine}", transient)
                        sweep[engine].append(pair[engine])
                    pairs[rate].append((pair["nabedra"], pair["valkey"]))
                for engine in ENGINES:
                    peak = capacity(sweep[engine], args.p99_ms)
                    peaks[engine].append(peak["goodput"] if peak else None)
            cases[(driver, workload, size)] = {
                "paired": {rate: paired_result(pair, args.p99_ms, args) for rate, pair in pairs.items()},
                "common_rates": common, "valkey_calibration": calibrated, "goodput_per_repeat": peaks,
            }
        profiles[groups] = {"placement": specs, "cases": cases}
    return profiles


def assess(profiles):
    for groups, profile in profiles.items():
        for case, evidence in profile["cases"].items():
            peaks = evidence["goodput_per_repeat"]["nabedra"]
            baseline = profiles[3]["cases"][case]["goodput_per_repeat"]["nabedra"] if 3 in profiles else []
            scale = [
                higher / lower * 3 / groups
                for higher, lower in zip(peaks, baseline)
                if higher is not None and lower
            ]
            interval = confidence(scale) if len(scale) == len(peaks) else None
            evidence["scale_efficiency"] = scale
            evidence["scale_efficiency_ci_95"] = interval
            evidence["scale_meets_threshold"] = (
                all(value is not None for value in peaks) if groups == 3 else
                interval is not None and interval[0] >= .8
            )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--groups", type=lambda value: csv_values(value, int), default=list(SIZES))
    p.add_argument("--rates", type=lambda value: csv_values(value, int), required=True, help="offered ops/s sweep at three groups")
    p.add_argument("--client-cpus", type=lambda value: csv_values(value, int), required=True)
    p.add_argument("--drivers", type=lambda value: csv_values(value, str), default=["stock", "python", "typescript"])
    p.add_argument("--workloads", type=lambda value: csv_values(value, str), default=["get-hit", "set-px", "miss", "mixed"])
    p.add_argument("--sizes", type=lambda value: csv_values(value, int), default=[64, 1024, 65536])
    p.add_argument("--distribution", choices=["uniform", "skew", "hot"], default="uniform")
    p.add_argument("--p99-ms", type=float, default=1.0)
    p.add_argument("--lanes-per-group", type=int, default=2)
    p.add_argument("--keys-per-group", type=int, default=256)
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--samples", type=int, default=10_000)
    p.add_argument("--warmup-seconds", type=float, default=1.0)
    p.add_argument("--timeout-ms", type=int, default=3_000)
    p.add_argument("--ttl-ms", type=int, default=900_000)
    p.add_argument("--seed", type=int, default=27182)
    p.add_argument("--node-driver", default=str(HERE / "driver.mjs"))
    args = p.parse_args()
    args.output = args.output.resolve()
    if sys.platform != "linux" or not set(args.client_cpus) <= os.sched_getaffinity(0):
        p.error("run on Linux with the designated load-generator CPU affinity")
    if not (
        args.groups and set(args.groups) <= set(SIZES) and args.rates and min(args.rates) > 0
        and 0 < len(args.client_cpus) <= min(args.groups) * args.lanes_per_group
        and args.samples > 0 and args.repetitions > 0 and args.keys_per_group > 0
        and math.isfinite(args.p99_ms) and args.p99_ms > 0 and 0 < min(args.sizes) <= max(args.sizes) <= 65_536
        and args.seed >= 0 and args.ttl_ms > 0 and args.timeout_ms > 0 and args.warmup_seconds > 0
        and set(args.drivers) <= {"stock", "python", "typescript"}
        and set(args.workloads) <= {"get-hit", "set-px", "miss", "mixed"}
    ):
        p.error("invalid benchmark limits or scenarios")
    if args.distribution == "hot" and args.workloads != ["set-px"]:
        p.error("the hot writer diagnostic requires --workloads set-px")
    if args.output.exists() and any(args.output.iterdir()):
        p.error("select an empty output directory")
    args.output.mkdir(parents=True, exist_ok=True)
    config = json.loads(args.manifest.read_text())
    with tempfile.TemporaryDirectory(prefix="nabedra-cluster-bench-") as directory:
        profiles = matrix(args, config, Path(directory))
    assess(profiles)
    for profile in profiles.values():
        profile["cases"] = {"-".join(map(str, case)): result for case, result in profile["cases"].items()}
    json_dump(args.output / "cluster-summary.json", {
        "environment": config.get("environment", "docker"), "distribution": args.distribution,
        "p99_budget_ms": args.p99_ms, "profiles": profiles,
        "qualification": "review required for matched EC2 placement, routing telemetry and provider histories",
    })


if __name__ == "__main__":
    main()

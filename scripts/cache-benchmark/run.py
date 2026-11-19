#!/usr/bin/env python3
"""Paired single-host Cache and Valkey benchmark. Run on Linux, including Docker."""

import argparse
import base64
import collections
import contextlib
import csv
import gzip
import hashlib
import importlib.metadata
import itertools
import json
import math
import os
import platform
import random
import secrets
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from driver import NS, encoded, key_for, redis_client


HERE = Path(__file__).resolve().parent
FIRST = "nabedra"
REFERENCE = "valkey"
MIB = 1024 * 1024
LOW = 0.05
GATE = 1.20

# Two-sided 95% Student t critical values for independent paired repetitions.
# A log ratio gives a dimensionless interval respecting a positive ratio.
STUDENT_95 = (
    12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262,
    2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101,
    2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052,
    2.048, 2.045, 2.042,
)


def json_dump(path, obj):
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def pin(cpus):
    return ["taskset", "-c", ",".join(map(str, cpus))]


def child_command(args, binary, tail):
    return [
        *pin(args.server_cpus), "prlimit", f"--as={args.os_memory_mib * MIB}:"
        f"{args.os_memory_mib * MIB}", "--", str(binary), *tail,
    ]


def process_stats(pid):
    status = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        name, _, value = line.partition(":")
        if name in {"VmHWM", "VmRSS"}:
            status[name] = int(value.split()[0]) * 1024
    stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", maxsplit=1)[1].split()
    return {
        "cpu_seconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
        "rss_bytes": status.get("VmRSS"),
        "rss_peak_bytes": status.get("VmHWM"),
    }


@contextlib.contextmanager
def running(command, logfile):
    with logfile.open("wb") as log:
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def digest(binary):
    with Path(binary).open("rb") as artifact:
        return hashlib.file_digest(artifact, "sha256").hexdigest()


def percentile(values, proportion):
    return values[math.ceil(len(values) * proportion) - 1] if values else None


def summary(raw_paths, spec):
    seen = bytearray(spec["samples"])
    latencies = []
    queue_wait = []
    service = []
    outcomes = collections.Counter()
    writes = set()
    last = spec["measure_start_ns"]
    for raw_path in raw_paths:
        with gzip.open(raw_path, "rt", newline="") as raw:
            for record in csv.DictReader(raw):
                sequence = int(record["sequence"])
                due = int(record["scheduled_ns"])
                begin = int(record["started_ns"])
                end = int(record["completed_ns"])
                outcome = record["outcome"]
                expected_due = spec["measure_start_ns"] + sequence * NS // spec["rate"]
                if (not 0 <= sequence < spec["samples"] or seen[sequence]
                        or due != expected_due or not due <= begin <= end or not outcome):
                    raise RuntimeError(f"invalid measurement sample in {raw_path.name}")
                last = max(last, end)
                seen[sequence] = 1
                outcomes[outcome] += 1
                if outcome == "ok":
                    latencies.append(end - due)
                    queue_wait.append(begin - due)
                    service.append(end - begin)
                    if spec.get("workload") in {"set-px", "mixed"}:
                        key, write, _ = key_for(sequence, spec)
                        if write:
                            writes.add(key)
    if not all(seen):
        raise RuntimeError("a load lane did not account for every scheduled request")
    latencies.sort()
    queue_wait.sort()
    service.sort()
    elapsed = max(
        spec["samples"] / spec["rate"],
        (last - spec["measure_start_ns"]) / NS,
    )
    return {
        "samples": spec["samples"],
        "rate": spec["rate"],
        "seconds": elapsed,
        "goodput": outcomes["ok"] / elapsed,
        "outcomes": dict(outcomes),
        "latency_ns": {label: percentile(latencies, p) for label, p in (("p50", .50), ("p95", .95), ("p99", .99))},
        "queue_p50_ns": percentile(queue_wait, .50),
        "queue_p99_ns": percentile(queue_wait, .99),
        "service_p50_ns": percentile(service, .50),
        "service_p99_ns": percentile(service, .99),
        "raw_files": [path.name for path in raw_paths],
    }, writes


def certificate(directory):
    key = directory / "key.pem"
    cert = directory / "cert.pem"
    subprocess.run([
        "openssl", "req", "-newkey", "rsa:2048", "-nodes", "-x509", "-days", "1",
        "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1",
        "-keyout", str(key), "-out", str(cert),
    ], check=True, capture_output=True)
    return cert, key


def configs(args, directory, tls, password, key, cert):
    valkey = (
        f'bind 127.0.0.1\nport {0 if tls else args.port}\n'
        f'requirepass {password}\nsave ""\nappendonly no\nio-threads 1\n'
        f'maxmemory {args.data_mib * MIB}\nmaxmemory-policy allkeys-lru\nmaxmemory-samples 64\n'
        f'maxclients {args.connections}\ndir "{directory}"\n'
    )
    if tls:
        valkey += (
            f'tls-port {args.port}\ntls-cert-file "{cert}"\n'
            f'tls-key-file "{key}"\ntls-ca-cert-file "{cert}"\ntls-auth-clients no\n'
        )
    cache = (
        f'listen = "127.0.0.1:{args.port}"\nplaintext_loopback = {str(not tls).lower()}\n'
        'eviction = "sampled_lru"\n'
        f'[limits]\nmemory_bytes = {args.os_memory_mib * MIB}\ndata_bytes = {args.data_mib * MIB}\n'
        f'max_connections = {args.connections}\nmax_handshakes = {args.connections}\n'
        f'max_pending = {args.connections}\nio_timeout_ms = 120000\n'
    )
    if tls:
        cache += f'[tls]\ncertificate = "{cert}"\nprivate_key = "{key}"\n'
    cache += (
        f'[[users]]\nusername = "default"\npassword = "{password}"\n'
        'key_prefixes = ["nb:bench:"]\ncommands = ["get", "set", "pttl"]\n'
        f'max_connections = {args.connections}\n'
    )
    path_map = {"valkey": directory / "valkey.conf", "nabedra": directory / "cache.toml"}
    for engine, contents in (("valkey", valkey), ("nabedra", cache)):
        path_map[engine].write_text(contents)
        public_config = args.output / f"{engine}-{'tls' if tls else 'plain'}.conf.txt"
        public_config.write_text(contents.replace(password, "<redacted>").replace(str(directory), "<temporary>"))
    return path_map


def prepare_data(process, spec):
    expires = time.monotonic() + 15
    while True:
        if process.poll() is not None:
            raise RuntimeError(f"server exited {process.returncode} before readiness; see server log")
        try:
            with redis_client(spec) as stock:
                if stock.ping():
                    break
        except (OSError, TimeoutError):
            pass
        except Exception as error:
            from redis.exceptions import ConnectionError, TimeoutError as RedisTimeout
            if not isinstance(error, (ConnectionError, RedisTimeout)):
                raise
        if time.monotonic() >= expires:
            raise RuntimeError("server never answered the readiness PING")
        time.sleep(.05)
    value = spec["value"]
    if spec["workload"] == "set-px":
        # SET must publish bytes distinct from the preload.
        value = bytes([value[0] ^ 0xFF]) + value[1:]
    with redis_client(spec) as stock:
        for index in range(spec["keys"]):
            if stock.set(encoded(f"key:{index}"), value) is not True:
                raise RuntimeError("prefill SET response mismatch")
        for index in {0, spec["keys"] - 1}:
            if stock.get(encoded(f"key:{index}")) != value:
                raise RuntimeError("prefill data does not fit or was not byte-exact")


def validate_writes(spec, keys):
    budget = spec["ttl_ms"]
    minimum, maximum = None, None
    if keys:
        with redis_client(spec) as stock:
            for key in sorted(keys):
                actual = stock.get(encoded(key))
                ttl = stock.pttl(encoded(key))
                if actual != spec["value"] or not 0 < ttl <= budget:
                    raise RuntimeError("a successful SET PX did not publish the expected value and TTL")
                minimum = ttl if minimum is None else min(minimum, ttl)
                maximum = ttl if maximum is None else max(maximum, ttl)
    return {"keys_checked": len(keys), "pttl_min_ms": minimum, "pttl_max_ms": maximum}


def run_once(args, common, name, rate, engine, config_file, directory, *, calibration=False):
    stem = f"{name}--{engine}"
    timeout = args.timeout_ms
    spec = {
        **common, "rate": rate, "timeout_ms": timeout, "lanes": args.lanes,
        "samples": args.calibration_samples if calibration else args.samples,
        "warmup": max(1, int(args.warmup_seconds * rate)),
        "ttl_ms": args.ttl_ms, "client_processes": len(args.client_cpus), "port": args.port,
    }
    binary = args.nabedra if engine == FIRST else args.valkey
    tail = ["serve", "--config", str(config_file)] if engine == FIRST else [str(config_file)]
    stderr = args.output / f"{stem}.log"
    with running(child_command(args, binary, tail), stderr) as server:
        oracle = {**spec, "value": base64.b64decode(spec["value_b64"])}
        prepare_data(server, oracle)
        outputs, before, after = run_load(args, spec, stem, directory, server)
        oracle["measure_start_ns"] = spec["measure_start_ns"]
        result, written = summary(outputs, oracle)
        result["set_px_verification"] = validate_writes(oracle, written)
    result.update({
        "engine": engine, "calibration": calibration, "seed": common["seed"],
        "rss_peak_bytes": after["rss_peak_bytes"], "rss_end_bytes": after["rss_bytes"],
        "server_cpu_seconds": after["cpu_seconds"] - before["cpu_seconds"],
    })
    for output in outputs:
        Path(f"{output}.ready").unlink()
    json_dump(args.output / f"{stem}.json", result)
    p50 = result["latency_ns"]["p50"]
    p99 = result["latency_ns"]["p99"]
    print(f"{stem} {result['goodput']:.1f} correct ops/s p50={p50}ns p99={p99}ns {result['outcomes']}",
          flush=True)
    return result


def run_load(args, spec, stem, directory, server=None):
    rate = spec["rate"]
    spec_path = directory / "run.json"
    start_path = directory / "start.json"
    start_path.unlink(missing_ok=True)
    spec["start_path"] = str(start_path)
    json_dump(spec_path, spec)
    outputs = []
    with contextlib.ExitStack() as group:
        clients = []
        for process, cpu in enumerate(args.client_cpus):
            output = args.output / f"{stem}--client{process}.csv.gz"
            output.unlink(missing_ok=True)
            Path(f"{output}.ready").unlink(missing_ok=True)
            outputs.append(output)
            transport = ["node", args.node_driver] if spec["driver"] == "typescript" else [sys.executable, str(HERE / "driver.py")]
            logfile = args.output / f"{stem}--client{process}.log"
            clients.append(group.enter_context(running(
                [*pin([cpu]), *transport, str(spec_path), str(process), str(output)], logfile,
            )))
        wait_ready(clients, outputs)
        warm = time.monotonic_ns() + 100_000_000
        measure = warm + math.ceil(spec["warmup"] * NS / rate) + 100_000_000
        spec["measure_start_ns"] = measure
        ready = directory / "start.ready"
        json_dump(ready, [str(warm), str(measure)])
        before = process_stats(server.pid) if server is not None else None
        ready.replace(start_path)
        end = time.monotonic() + (measure - warm) / NS + spec["samples"] / rate + 30
        for client in clients:
            try:
                status = client.wait(max(0, end - time.monotonic()))
            except subprocess.TimeoutExpired as error:
                raise RuntimeError(f"{stem} load watchdog expired; check client logs") from error
            if status:
                raise RuntimeError(f"{stem} load process exited {status}; check client logs")
        after = process_stats(server.pid) if server is not None else None
    return outputs, before, after


def wait_ready(clients, outputs):
    expires = time.monotonic() + 15
    while not all(Path(f"{output}.ready").exists() for output in outputs):
        if any(client.poll() is not None for client in clients):
            raise RuntimeError("load process exited before connection setup; check client logs")
        if time.monotonic() >= expires:
            raise RuntimeError("load process did not establish persistent connections; check client logs")
        time.sleep(.005)


def clean_under(result, budget_ms):
    p99 = result["latency_ns"]["p99"]
    return (
        result["outcomes"] == {"ok": result["samples"]}
        and result["goodput"] >= result["rate"] * .99
        and p99 is not None
        and p99 <= budget_ms * 1_000_000
    )


def capacity(sweep, budget):
    eligible = [result for result in sweep if clean_under(result, budget)]
    if not eligible:
        return None
    peak = max(eligible, key=lambda row: row["rate"])
    # Require observations on the far side of saturation.
    if not any(row["rate"] > peak["rate"] and not clean_under(row, budget) for row in sweep):
        return None
    return peak


def confidence(ratios):
    if len(ratios) < 3 or any(ratio <= 0 for ratio in ratios):
        return None
    logged = [math.log(ratio) for ratio in ratios]
    center = statistics.mean(logged)
    dof = len(logged) - 1
    critical = STUDENT_95[min(dof, len(STUDENT_95)) - 1]
    error = critical * statistics.stdev(logged) / math.sqrt(len(logged))
    return [math.exp(center - error), math.exp(center + error)]


def paired_result(pairs, budget, args):
    ratios = {
        quantile: [n["latency_ns"][quantile] / v["latency_ns"][quantile]
                   for n, v in pairs if n["latency_ns"][quantile] and v["latency_ns"][quantile]]
        for quantile in ("p50", "p99")
    }
    intervals = {key: confidence(value) if len(value) == len(pairs) else None for key, value in ratios.items()}
    enough = len(pairs) >= 3 and args.samples >= 10_000
    clean = all(clean_under(n, budget) and clean_under(v, budget) for n, v in pairs)
    if not enough or None in intervals.values():
        status = "insufficient-samples"
    elif not clean or any(bounds[0] > GATE for bounds in intervals.values()):
        status = "miss"
    elif any(bounds[1] > GATE for bounds in intervals.values()):
        status = "inconclusive"
    else:
        status = "meets-thresholds"
    return {"status": status, "ratios": ratios, "confidence_95": intervals, "clean_under_budget": clean}


def profile(args, common, files, directory, label):
    driver = common["driver"]
    only_baseline = bool(args.offered)
    if only_baseline:
        valkey_calibration = []
    else:
        valkey_calibration = [
            run_once(args, common, f"{label}--calibration-{rate}", rate, REFERENCE, files[REFERENCE], directory,
                     calibration=True)
            for rate in args.calibration_rates
        ]

    report = {"valkey_calibration": valkey_calibration, "budgets": {}}
    for budget in sorted({args.p99_ms, 1.0} if args.p99_ms != 1 else {1.0}):
        target = capacity(valkey_calibration, budget)
        tag = f"{label}--budget{budget:g}"
        if target is None and not only_baseline:
            report["budgets"][str(budget)] = {"status": "unbracketed-valkey-calibration"}
            continue
        if only_baseline:
            loads = {str(rate): rate for rate in args.offered}
            sweep_rates = sorted(set(loads.values()))
        else:
            top = target["rate"]
            loads = {key: max(1, int(top * fraction)) for key, fraction in (("low", LOW), ("50%", .5), ("80%", .8))}
            sweep_rates = sorted(set([*loads.values(), top, int(top * 1.25),
                                      *[r for r in args.calibration_rates if r > top]]))
        slopes = {rate: [] for rate in sweep_rates}
        order = random.Random(common["seed"])
        repetitions = [order.sample([FIRST, REFERENCE], 2) for _ in range(args.repetitions)]
        for rate, ids in itertools.product(sweep_rates, range(args.repetitions)):
            engines = repetitions[ids]
            repeats = {}
            paired_spec = {**common, "seed": common["seed"] + ids}
            for engine in engines:
                repeats[engine] = run_once(
                    args, paired_spec, f"{tag}--rate{rate}--pair{ids}", rate, engine,
                    files[engine], directory,
                )
            slopes[rate].append((repeats[FIRST], repeats[REFERENCE]))
        measured = {name: paired_result(slopes[rate], budget, args) for name, rate in loads.items()}
        peaks = {}
        for index, engine in enumerate((FIRST, REFERENCE)):
            per_repeat = [
                capacity([slopes[rate][rep][index] for rate in sweep_rates], budget)
                for rep in range(args.repetitions)
            ]
            peaks[engine] = per_repeat
        throughput_ratio = [
            n["goodput"] / v["goodput"]
            for n, v in zip(peaks[FIRST], peaks[REFERENCE], strict=True) if n and v
        ]
        throughput_interval = confidence(throughput_ratio) if len(throughput_ratio) == args.repetitions else None
        throughput_pass = bool(throughput_interval and throughput_interval[0] >= .8)
        outcome = {
            "valkey_sustainable_calibration": target,
            "common_loads": loads, "latency_checks": measured, "sweep": slopes,
            "sustainable_per_repeat": peaks, "throughput_ratios": throughput_ratio,
            "throughput_ci_95": throughput_interval, "throughput_meets_threshold": throughput_pass,
            "status": "meets-thresholds" if throughput_pass and all(
                row["status"] == "meets-thresholds" for row in measured.values()
            ) else "not-qualified",
        }
        report["budgets"][str(budget)] = outcome
        overview = " ".join(f"{name}={row['status']}" for name, row in measured.items())
        print(f"{driver} {tag} {overview}; throughput={throughput_ratio}", flush=True)
    return report


def csv_values(text, convert):
    return [convert(value) for value in text.split(",")]


def args_from_cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", choices=["docker", "ec2"], default="docker",
                        help="evidence label, never provisions hosts")
    parser.add_argument("--ec2-host-evidence", type=Path,
                        help="existing metadata JSON recording instance type and unique host identity")
    parser.add_argument("--nabedra", type=Path, required=True)
    parser.add_argument("--valkey", type=Path, required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-revision", required=True, help="commit from which the release binary was built")
    parser.add_argument("--server-cpus", type=lambda value: csv_values(value, int), default=[0])
    parser.add_argument("--client-cpus", type=lambda value: csv_values(value, int), default=[1, 2, 3, 4])
    parser.add_argument("--drivers", type=lambda value: csv_values(value, str), default=["stock", "python", "typescript"])
    parser.add_argument("--node-driver", default=str(HERE / "driver.mjs"))
    parser.add_argument("--sizes", type=lambda value: csv_values(value, int), default=[64, 1024, 65536])
    parser.add_argument("--workloads", type=lambda value: csv_values(value, str), default=["get-hit", "set-px", "miss", "mixed"])
    parser.add_argument("--transports", type=lambda value: csv_values(value, str), default=["tls", "plain"])
    parser.add_argument("--pipeline", type=int, default=1, help="stock-only nonqualifying pipeline diagnostic")
    parser.add_argument("--keys", type=int, default=1024)
    parser.add_argument("--lanes", type=int, default=32, help="connections in steady state")
    parser.add_argument("--connections", type=int, default=64, help="server connection limit")
    parser.add_argument("--ttl-ms", type=int, default=600_000)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    parser.add_argument("--p99-ms", type=float, default=1.0)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--samples", type=int, default=10_000)
    parser.add_argument("--calibration-samples", type=int, default=10_000)
    parser.add_argument("--warmup-seconds", type=float, default=1.0)
    parser.add_argument("--offered", type=lambda value: csv_values(value, int), default=[],
                        help="fixed rates for a nonqualifying diagnostic without calibration")
    parser.add_argument("--calibration-rates", type=lambda value: csv_values(value, int),
                        default=[100, 250, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000])
    parser.add_argument("--os-memory-mib", type=int, default=768)
    parser.add_argument("--data-mib", type=int, default=128)
    parser.add_argument("--port", type=int, default=16379)
    parser.add_argument("--seed", type=int, default=5617)
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("run this on Linux or in Docker")
    if len(set(args.server_cpus + args.client_cpus)) != len(args.server_cpus + args.client_cpus):
        parser.error("server and client cores must be distinct")
    if not set(args.server_cpus + args.client_cpus) <= os.sched_getaffinity(0):
        parser.error("selected cores must fall inside the process affinity mask")
    labels = (("transport", set(args.transports), {"tls", "plain", "cold"}),
              ("workload", set(args.workloads), {"get-hit", "set-px", "miss", "mixed"}),
              ("driver", set(args.drivers), {"stock", "python", "typescript"}))
    for label, requested, allowed in labels:
        if not requested or not requested <= allowed:
            parser.error(f"invalid {label} list")
    if not 1 <= args.pipeline <= 64 or (args.pipeline > 1 and (args.drivers != ["stock"] or "cold" in args.transports)):
        parser.error("only warm stock connections support a pipeline")
    if not len(args.client_cpus) <= args.lanes < args.connections <= 64:
        parser.error("select between 1 lane per client CPU and one fewer lane than the server connection cap")
    if not (
        0 < args.data_mib < args.os_memory_mib
        and min(args.sizes) > 0 and max(args.sizes) <= 65536
        and args.keys > 1 and args.keys * (max(args.sizes) + 256) < args.data_mib * MIB
        and args.ttl_ms > 0 and args.timeout_ms > 0
        and args.p99_ms > 0 and args.warmup_seconds > 0
        and args.repetitions > 0 and args.samples > 0 and args.calibration_samples > 0
        and all(rate > 0 for rate in args.offered + args.calibration_rates)
        and len(set(args.calibration_rates)) == len(args.calibration_rates)
        and 0 <= args.seed < 1_000_000_000
    ):
        parser.error("invalid workload, data or memory limits")
    if args.environment == "ec2" and args.ec2_host_evidence is None:
        parser.error("EC2 qualification requires an existing host evidence file")
    args.nabedra = args.nabedra.resolve()
    args.valkey = args.valkey.resolve()
    return args


def text_at(path):
    path = Path(path)
    return path.read_text().strip() if path.is_file() else None


def metadata(args):
    def version(command):
        return subprocess.check_output(command, text=True).strip()

    valkey_release = version([args.valkey, "--version"])
    if "v=8.1.11 " not in valkey_release:
        raise RuntimeError(f"expected pinned Valkey 8.1.11, got {valkey_release}")
    packages = {pkg: importlib.metadata.version(pkg) for pkg in ("redis", "hiredis")}
    if packages["redis"] != "6.4.0" or packages["hiredis"] != "3.2.1":
        raise RuntimeError("install the pinned stock RESP2 load dependencies")
    if "python" in args.drivers:
        packages["nabedra-python"] = importlib.metadata.version("nabedra")
    node = None
    if "typescript" in args.drivers:
        resolve_installed_package = (
            'const fs = require("node:fs"); const path = require("node:path");'
            'const entry = require.resolve("nabedra/cache");'
            'const pkg = JSON.parse(fs.readFileSync(path.resolve(entry, "../../package.json"), "utf8"));'
            'console.log(pkg.version);'
        )
        node = version(["node", "--version"])
        packages["nabedra-typescript"] = subprocess.check_output(
            ["node", "-e", resolve_installed_package], cwd=Path(args.node_driver).resolve().parent, text=True,
        ).strip()
    ec2 = json.loads(args.ec2_host_evidence.read_text()) if args.ec2_host_evidence else None
    return {
        "arguments": {name: str(value) if isinstance(value, Path) else value for name, value in vars(args).items()},
        "host": {
            "os": platform.platform(), "architecture": platform.machine(), "processor": platform.processor(),
            "cores_requested": sorted(os.sched_getaffinity(0)), "ec2": ec2,
            "cgroup_cpu_max": text_at("/sys/fs/cgroup/cpu.max"),
            "cgroup_memory_max": text_at("/sys/fs/cgroup/memory.max"),
            "cpu": text_at("/proc/cpuinfo"), "mem": text_at("/proc/meminfo"),
        },
        "software": {
            "python": platform.python_version(), "node": node,
            "openssl": version(["openssl", "version"]), "python_packages": packages,
            "valkey": valkey_release, "valkey_sha256": digest(args.valkey),
            "nabedra": version([args.nabedra, "--version"]), "nabedra_sha256": digest(args.nabedra),
        },
        "parity_band": [.8, 1.2],
    }


def gate_status(args, report):
    if args.environment == "docker":
        return "not-run-on-ec2"
    required_drivers = {"stock", "python", "typescript"}
    required_sizes = {64, 1024, 65536}
    required_loads = {"get-hit", "set-px"}
    if (
        args.samples < 10_000 or args.calibration_samples < 10_000 or args.repetitions < 3
        or args.offered or args.pipeline != 1 or not required_drivers <= set(args.drivers)
        or not required_sizes <= set(args.sizes)
        or not (required_loads | {"miss", "mixed"}) <= set(args.workloads)
        or "tls" not in args.transports or "plain" not in args.transports
    ):
        return "incomplete-matrix"
    selected = {
        f"{driver}--{transport}--{size}b--{workload}"
        for driver, transport, size, workload in itertools.product(
            args.drivers, args.transports, args.sizes, args.workloads
        )
    }
    if not selected <= report["profiles"].keys():
        return "in-progress"
    for driver, size, workload in itertools.product(required_drivers, required_sizes, required_loads):
        profile = report["profiles"].get(f"{driver}--tls--{size}b--{workload}", {})
        for budget, result in profile.get("budgets", {}).items():
            if float(budget) != args.p99_ms and result["status"] == "unbracketed-valkey-calibration":
                # Record an unavailable 1 ms lane only when no observed rate qualifies.
                if any(clean_under(row, 1.0) for row in profile["valkey_calibration"]):
                    return "thresholds-pending"
                continue
            if result["status"] != "meets-thresholds":
                return "thresholds-pending"
        if str(args.p99_ms) not in profile.get("budgets", {}):
            return "thresholds-pending"
    return "numeric-thresholds-met-review-evidence"


def main():
    args = args_from_cli()
    if any(shutil.which(tool) is None for tool in ("openssl", "taskset", "prlimit")):
        raise RuntimeError("openssl, taskset and prlimit are required")
    args.output.mkdir(parents=True, exist_ok=True)
    # Disallow reusing paths that could mix raw samples across different builds.
    if any(args.output.iterdir()):
        raise RuntimeError("benchmark output directory must be empty")
    summary_path = args.output / "summary.json"
    with tempfile.TemporaryDirectory(prefix="nabedra-cache-") as tmp:
        directory = Path(tmp)
        original = {name: getattr(args, name) for name in ("nabedra", "valkey")}
        for name, source in original.items():
            # Rebuilding on the host during a run must not change a later pair.
            copied = directory / name
            shutil.copyfile(source, copied)
            copied.chmod(0o700)
            setattr(args, name, copied)
        report = {"metadata": metadata(args), "profiles": {}, "K04a": "in-progress"}
        report["metadata"]["arguments"].update({name: str(path) for name, path in original.items()})
        json_dump(summary_path, report)
        cert, key = certificate(directory)
        password = secrets.token_hex(24)
        for driver in [kind for kind in ("stock", "python", "typescript") if kind in args.drivers]:
            for transport in args.transports:
                tls = transport in {"tls", "cold"}
                files = configs(args, directory, tls, password, key, cert)
                for size, workload in itertools.product(args.sizes, args.workloads):
                    label = f"{driver}--{transport}--{size}b--{workload}"
                    pattern = bytes(((i * 31 + size) & 255) for i in range(size))
                    common = {
                        "driver": driver, "transport": transport, "tls": tls,
                        "pipeline": args.pipeline, "workload": workload, "keys": args.keys,
                        "certificate": str(cert), "password": password, "seed": args.seed,
                        "value_b64": base64.b64encode(pattern).decode(),
                    }
                    report["profiles"][label] = profile(args, common, files, directory, label)
                    report["K04a"] = gate_status(args, report)
                    json_dump(summary_path, report)
    print(f'K04a={report["K04a"]} {summary_path}', flush=True)


if __name__ == "__main__":
    main()

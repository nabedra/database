"""Real RF1 process/unchanged ORM assertions. Run with --binary /path/nabedra-db.

Local O01/O02/O03/O04/O08/O10/O11/O15 subsets and Q05/Q06/T06/R04.
Does not claim multi-shard/quorum, power-loss, AZ, or provider evidence.
"""
import argparse
import hashlib
from datetime import datetime, timezone
import platform
import random
import json
import os
from pathlib import Path
import signal
import ssl
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "orm/python/src"))
from nabedra import Database, col


def raw(url, path, body=None, *, token="test-token", database="test", headers=None, timeout=10):
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    request = urllib.request.Request(url + path, data=data, headers={
        "Authorization": f"Bearer {token}", "X-Nabedra-Database": database,
        "Content-Type": "application/json", **(headers or {}),
    })
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


class Server:
    def __init__(self, binary, directory, diagnostics=None):
        self.binary, self.directory = binary, directory
        self.process = None
        self.diagnostics = diagnostics
        self.diagnostics_stream = None

    def start(self, tls=False):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.url = f"{'https' if tls else 'http'}://127.0.0.1:{port}"
        env = {k: v for k, v in os.environ.items() if not k.startswith(("AWS_", "NABEDRA_S3_"))}
        env["NABEDRA_TOKEN"] = "test-token"
        command = [
            self.binary, "serve", "--data-dir", self.directory, "--database", "test",
            "--host", "127.0.0.1", "--port", str(port), "--workers", "4", "--queue-size", "8",
        ]
        if (Path(self.directory) / "NABEDRA_TYPED_WORKER.json").exists():
            command += ["--cold-start"]
        if tls:
            command += ["--tls-cert", self.directory + "/cert.pem", "--tls-key", self.directory + "/key.pem"]
        self.diagnostics_stream = open(self.diagnostics, "ab", buffering=0) if self.diagnostics else None
        self.process = subprocess.Popen(command, env=env, stdout=self.diagnostics_stream or subprocess.DEVNULL, stderr=self.diagnostics_stream or subprocess.PIPE)
        until = time.monotonic() + 30
        while time.monotonic() < until:
            if self.process.poll() is not None:
                raise AssertionError(f"server exited; diagnostics: {self.diagnostics}" if self.diagnostics else f"server exited: {self.process.stderr.read().decode()}")
            try:
                with urllib.request.urlopen(self.url + "/health/ready", timeout=0.5, context=ssl.create_default_context(cafile=self.directory + "/cert.pem") if tls else None) as response:
                    value = json.load(response)
                    assert value["engine"] == "oltp" and value["replication_factor"] == 1
                    return value["run_id"]
            except (OSError, urllib.error.URLError):
                time.sleep(0.02)
        raise AssertionError("server readiness watchdog elapsed")

    def stop(self, abrupt=False):
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if abrupt else signal.SIGTERM)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                raise AssertionError("shutdown watchdog elapsed")
        if self.process and self.process.stderr:
            self.process.stderr.close()
        self.process = None
        if self.diagnostics_stream:
            self.diagnostics_stream.close()
            self.diagnostics_stream = None


def terminal(url, run, identity):
    until = time.monotonic() + 10
    while time.monotonic() < until:
        status, receipt = raw(url, f"/v1/requests/{run}/{identity}")
        assert status == 200, receipt
        if receipt["state"] in ("succeeded", "failed", "abandoned"):
            return receipt
        time.sleep(0.01)
    raise AssertionError("receipt watchdog elapsed")


def build_processes():
    names = subprocess.check_output(["ps", "-axo", "comm="], text=True).splitlines()
    return sorted({Path(name.strip()).name for name in names if Path(name.strip()).name in {"cargo", "rustc", "clang", "clang++"}})


def source_digest():
    root = Path(__file__).resolve().parents[2]
    paths = [root / "server/Cargo.toml", root / "server/Cargo.lock", *sorted((root / "server/src").rglob("*.rs")), *sorted((root / "orm/python/src").rglob("*.py"))]
    digest = hashlib.sha256()
    for path in paths:
        digest.update(str(path.relative_to(root)).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def benchmark(users, condition):
    started_at = datetime.now(timezone.utc).isoformat()
    initial_load = os.getloadavg()
    initial_builds = build_processes()
    seed = 91347
    rng = random.Random(seed)
    def execute(kind):
        if kind == "point_hit":
            assert users.get("durable") == {"id": "durable", "age": 42}
        elif kind == "point_miss":
            assert users.get("not-present") is None
        else:
            assert users.where(col("age") >= 18).limit(2).count() == 2
    kinds = ["point_hit", "point_miss", "bounded_count"]
    for _ in range(30):
        execute(rng.choice(kinds))
    repetitions = []
    for repetition in range(3):
        samples = []
        for _ in range(90):
            kind = rng.choice(kinds)
            started = time.perf_counter_ns()
            execute(kind)
            samples.append({"kind": kind, "latency_ms": (time.perf_counter_ns() - started) / 1_000_000})
        repetitions.append({"repetition": repetition + 1, "samples": samples})
    summaries = {}
    for kind in kinds:
        times = sorted(s["latency_ms"] for rep in repetitions for s in rep["samples"] if s["kind"] == kind)
        summaries[kind] = {"samples": len(times), **{f"p{p}_ms": times[min(len(times)-1, int((len(times)-1)*p/100))] for p in (50, 95, 99)}}
    return {"started_at_utc": started_at, "ended_at_utc": datetime.now(timezone.utc).isoformat(), "initial_host_load_1_5_15": initial_load, "final_host_load_1_5_15": os.getloadavg(), "initial_build_process_names": initial_builds, "final_build_process_names": build_processes(), "host_conditions": condition, "seed": seed, "warmup_requests": 30, "repetitions": repetitions, "summary": summaries, "percentile_method": "lower order statistic at floor((n-1)*p/100)",
            "arrival": "one sequential ORM call at a time, no retries", "query_table_rows": 3, "dataset": "3-row query table; separate 110001-row validation table dropped before warm-up",
            "scope": "sequential loopback latency sample after warm-up; cache-hit rates not measured; not B01/B02, throughput scaling, cold-read or PostgreSQL evidence"}


def main(binary, evidence_dir, condition, build_profile):
    tested_source = source_digest()
    fixture_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    version = subprocess.check_output([binary, "--version"], text=True)
    assert "oltp" in version, version
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="nabedra-oltp-process-") as directory:
        server = Server(binary, directory)
        try:
            with tempfile.TemporaryDirectory(prefix="nabedra-oltp-resources-") as settings:
                resources = Path(settings) / "resources.json"
                resources.write_text(json.dumps({"max_threads": 257, "memory_limit_bytes": 8589934592, "task_memory_limit_bytes": 536870912, "control_memory_reserve_bytes": 268435456}))
                invalid_capacity = subprocess.run([binary, "serve", "--data-dir", directory, "--resources-config", str(resources), "--port", "0"], env={**os.environ, "NABEDRA_TOKEN": "test-token"}, capture_output=True, text=True, timeout=10)
                assert invalid_capacity.returncode == 2 and "configuration invalid" in invalid_capacity.stderr, invalid_capacity.stderr
                assert list(Path(directory).iterdir()) == []
            run = server.start()
            db = Database(url=server.url, token="test-token", database="test", mode="olap")
            users = db.create_table("users", {"id": str, "age": int}, key="id")
            assert users.format == "parquet"
            assert db.table("users").id == users.id
            assert [t.name for t in db.tables()] == ["users"]
            assert users.insert({"id": "alice", "age": 24}) is None
            assert users.insert_many([{"id": "bob", "age": 17}, {"id": "carol", "age": 24}]) == 2
            assert users.get("alice") == {"id": "alice", "age": 24}
            assert users.get("missing") is None
            assert users.where(col("age") >= 18).order_by("age", desc=True).select("id").limit(20).collect() == [{"id": "alice"}, {"id": "carol"}]
            assert users.where(col("age") >= 18).limit(1).count() == 1
            assert "oltp" in users.where(col("age") >= 18).explain().lower()
            assert users.update("bob", {"age": 18}) is True
            assert users.upsert({"id": "alice", "age": 25}) is None
            assert users.delete("missing") is False
            assert users.delete("carol") is True
            # Exact error envelopes, atomic duplicate-key batches, stale handles.
            op = {"request_id": str(uuid.uuid4()), "operation": "table.insert_many", "payload": {"table_id": users.id, "rows": [{"id": "new", "age": 30}, {"id": "alice", "age": 50}]}}
            status, value = raw(server.url, "/v1/operations", op)
            assert status == 409 and value["error"]["outcome"] == "not_applied", value
            assert users.get("new") is None and users.get("alice")["age"] == 25
            identity = str(uuid.uuid4())
            insert = {"request_id": identity, "operation": "table.insert", "payload": {"table_id": users.id, "row": {"id": "durable", "age": 42}}}
            status, receipt = raw(server.url, "/v1/requests", insert)
            assert status == 202 and receipt["run_id"] == run, receipt
            assert terminal(server.url, run, identity)["state"] == "succeeded"
            assert raw(server.url, "/v1/operations", insert) == (200, {"request_id": identity, "data": None})
            changed = {**insert, "payload": {"table_id": users.id, "row": {"id": "another", "age": 42}}}
            assert raw(server.url, "/v1/operations", changed)[0] == 409
            assert raw(server.url, f"/v1/requests/{run}/{identity}", token="invalid")[0] == 401
            assert raw(server.url, f"/v1/requests/{run}/{identity}", database="other")[0] == 401
            assert raw(server.url, "/v1/operations", insert, headers={"X-Nabedra-Storage": "forged"})[0] == 401
            assert raw(server.url, "/v1/cluster/operations", {})[0] == 400
            query_id = str(uuid.uuid4())
            status, query_receipt = raw(server.url, "/v1/requests", {"request_id": query_id, "operation": "table.list", "payload": {}})
            assert status == 202
            assert terminal(server.url, run, query_id)["state"] == "succeeded"
            # Exercise the unchanged ORM decoder through real HTTP beyond the
            # removed 100,000 count ceiling, keeping each batch/request bounded.
            growth = db.create_table("growth", {"id": int, "value": str}, key="id")
            growth_id = growth.id
            for start in range(0, 110001, 10000):
                rows = [{"id": i, "value": f"value-{i}"} for i in range(start, min(start + 10000, 110001))]
                assert growth.insert_many(rows) == len(rows)
            assert growth.query().count() == 110001
            assert growth.query().limit(100001).count() == 100001
            assert growth.get(100000) == {"id": 100000, "value": "value-100000"}
            original_id = users.id
            server.stop(abrupt=True)
            new_run = server.start()
            assert new_run != run
            db = Database(url=server.url, token="test-token", database="test")
            users = db.table("users")
            assert users.id == original_id and users.get("durable") == {"id": "durable", "age": 42}
            assert terminal(server.url, run, identity)["state"] == "succeeded"
            status, abandoned = raw(server.url, f"/v1/requests/{run}/{query_id}")
            assert status == 404 and abandoned["error"]["outcome"] == "unknown", abandoned
            assert raw(server.url, "/v1/operations", changed)[0] == 409
            assert raw(server.url, "/v1/operations", insert) == (200, {"request_id": identity, "data": None})
            assert users.where(col("id") == "durable").count() == 1
            growth = db.table("growth")
            assert growth.id == growth_id and growth.query().count() == 110001
            assert growth.get(110000) == {"id": 110000, "value": "value-110000"}
            growth.drop()
            measurement = benchmark(users, condition)
            users.drop()
            assert db.tables() == []
            status, stale = raw(server.url, "/v1/operations", {"request_id": str(uuid.uuid4()), "operation": "table.get", "payload": {"table_id": original_id, "key": "alice"}})
            assert status == 404 and stale["error"]["outcome"] == "not_applied", stale
            recreated = db.create_table("users", {"id": str, "age": int}, key="id")
            assert recreated.id != original_id
            server.stop()
            rejected = subprocess.run([binary, "serve", "--data-dir", directory, "--database", "other", "--port", "0"], env={**os.environ, "NABEDRA_TOKEN": "test-token"}, capture_output=True, text=True, timeout=15)
            assert rejected.returncode != 0 and "selector differs" in rejected.stderr, rejected.stderr
            remote = subprocess.run([binary, "worker", "--bootstrap", "missing.json"], capture_output=True, text=True, timeout=5)
            assert remote.returncode != 0 and "protocol_error" in remote.stderr, remote.stderr
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1", "-nodes", "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1", "-keyout", directory + "/key.pem", "-out", directory + "/cert.pem"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True, timeout=15)
            server.start(tls=True)
            secure = Database(url=server.url, token="test-token", database="test", ca_file=directory + "/cert.pem")
            assert secure.table("users").id == recreated.id
        finally:
            server.stop()
    assert source_digest() == tested_source, "server or ORM source changed during the process fixture"
    assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest() == fixture_hash, "fixture source changed during execution"
    evidence = {"result": "passed", "argv": sys.argv, "fixture_sha256": fixture_hash, "build_profile": build_profile, "binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
        "created_at_utc": datetime.now(timezone.utc).isoformat(), "python": sys.version, "binary_version": version.strip(), "platform": platform.platform(), "logical_cpus": os.cpu_count(),
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "cargo": subprocess.check_output(["cargo", "--version"], text=True).strip(),
        "source_tree_sha256": tested_source, "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "working_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"])).hexdigest(),
        "cpu_quota": "no CPU quota configured or measured by this fixture", "storage": "host temporary filesystem directory; no EBS/provider assertion",
        "clock": "perf_counter_ns monotonic client clock", "features": ["oltp"],
        "config": {"workers": 4, "foreground_threads": 1, "control_threads": 1, "storage_background_threads": 2, "network_threads": 2, "queue_size": 8, "max_pending_requests": 9, "memory_limit_bytes": 8589934592, "task_memory_limit_bytes": 536870912, "result_retention_ms": 300000, "memory_limit_kind": "logical scheduler budget; this fixture does not measure peak RSS or enforce an OS memory limit", "topology": "typed_groups_rf1", "S3_credentials": "absent"},
        "benchmark": measurement, "accepted_identity_assertions": {"durable_mutations": 1, "query_receipts": 1, "TLS_verified": True, "database_scope_rebind_rejected": True, "resources_override_257_rejected_before_storage_or_readiness": True, "ORM_growth_count_before_and_after_SIGKILL": 110001, "ORM_count_with_limit_above_100000": 100001, "duplicate_logical_mutations": 0, "missing_acknowledged_mutations_after_SIGKILL": 0}}
    if evidence_dir:
        path = Path(evidence_dir)
        path.mkdir(parents=True, exist_ok=True)
        (path / "rf1-process.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps({"result": "passed", "engine": "oltp", "topology": "typed_groups_rf1", "operations": 13, "process_restarts": 2, "TLS_roundtrip": True, "ORM_growth_rows": 110001, "elapsed_seconds": round(time.monotonic() - started, 3)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--evidence-dir")
    parser.add_argument("--benchmark-condition", default="host activity not recorded")
    parser.add_argument("--build-profile", default="unspecified by caller; fixture does not inspect compiler flags")
    args = parser.parse_args()
    main(str(Path(args.binary).resolve()), args.evidence_dir, args.benchmark_condition, args.build_profile)

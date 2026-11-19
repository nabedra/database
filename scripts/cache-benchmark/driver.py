"""Synchronous RESP2 and Python ORM load lanes. The coordinator starts this in a pinned process."""

import base64
import contextlib
import csv
import gzip
import json
import sys
import threading
import time
from pathlib import Path


NS = 1_000_000_000


def key_for(sequence, spec):
    index = ((sequence + spec["seed"]) * 0x9E3779B1 & 0xFFFFFFFF) % spec["keys"]
    if spec.get("distribution") == "hot":
        index = 0
    elif spec.get("distribution") == "skew" and (sequence + spec["seed"]) % 10 != 0:
        index %= max(1, spec["keys"] // 100)
    kind = spec["workload"]
    if kind == "miss" or (kind == "mixed" and (sequence + spec["seed"]) % 10 == 1):
        return f"missing:{index}", False, None
    if kind == "set-px" or (kind == "mixed" and (sequence + spec["seed"]) % 10 == 0):
        return f"key:{index}", True, True
    return f"key:{index}", False, spec["value"]


def encoded(key):
    token = base64.urlsafe_b64encode(key.encode()).rstrip(b"=")
    return b"nb:bench:" + token


def redis_client(spec):
    import redis
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    tls_options = {}
    if spec["tls"]:
        tls_options.update(ssl_ca_certs=spec["certificate"], ssl_check_hostname=True)
    options = dict(
        username=spec.get("username"), password=spec["password"], protocol=2,
        ssl=spec["tls"], retry=Retry(NoBackoff(), 0), retry_on_timeout=False,
        socket_timeout=spec["timeout_ms"] / 1000,
        socket_connect_timeout=spec["timeout_ms"] / 1000, **tls_options,
    )
    if spec.get("cluster"):
        from urllib.parse import urlsplit
        endpoints = [urlsplit(value) for value in spec["endpoints"]]
        allowed = {(node.hostname, node.port) for node in endpoints}

        def checked(address):
            if tuple(address) not in allowed:
                raise ValueError("cluster benchmark discovered an endpoint outside its profile")
            return address

        class ProfileCluster(redis.RedisCluster):
            def get_redis_connection(self, node):
                checked((node.host, node.port))
                return super().get_redis_connection(node)

        return ProfileCluster(
            startup_nodes=[redis.cluster.ClusterNode(host, port) for host, port in sorted(allowed)],
            address_remap=checked, require_full_coverage=True,
            max_connections=1, **options,
        )
    return redis.Redis(host="127.0.0.1", port=spec["port"], max_connections=1, **options)


@contextlib.contextmanager
def connect(spec):
    if spec["driver"] == "stock":
        with redis_client(spec) as client:
            if client.ping() is not True:
                raise RuntimeError("stock PING response mismatch")
            for key in spec.get("warm_keys", []):
                client.get(encoded(key))
            yield client
        return
    from nabedra.cache import Cache

    scheme = "rediss" if spec["tls"] else "redis"
    with Cache.connect(
        spec.get("endpoints") or f'{scheme}://127.0.0.1:{spec["port"]}', namespace="bench",
        username=spec.get("username"), password=spec["password"],
        cluster=spec.get("cluster", False),
        ca_file=spec["certificate"] if spec["tls"] else None,
        pool_size=len(spec["endpoints"]) if spec.get("cluster") else 1,
        timeout_ms=spec["timeout_ms"],
    ) as client:
        for key in spec.get("warm_keys", []):
            client.get(key)
        yield client


def sample(client, jobs, spec):
    if len(jobs) > 1:
        # Only the stock client has a public pipeline API. The parent refuses
        # this mode for ORM drivers and excludes it from the release gate.
        with client.pipeline(transaction=False) as batch:
            for _, key, write, _ in jobs:
                if write:
                    batch.set(encoded(key), spec["value"], px=spec["ttl_ms"])
                else:
                    batch.get(encoded(key))
            return batch.execute(raise_on_error=False)
    _, key, write, _ = jobs[0]
    if spec["driver"] == "stock":
        if write:
            return [client.set(encoded(key), spec["value"], px=spec["ttl_ms"])]
        return [client.get(encoded(key))]
    if write:
        return [client.set(key, spec["value"], ttl_ms=spec["ttl_ms"])]
    return [client.get(key)]


def error_name(error):
    return type(error).__name__


def wait_until(deadline):
    remaining = deadline - time.monotonic_ns()
    if remaining > 0:
        time.sleep(remaining / NS)


def run_lane(spec, lane, launch, started):
    result = []
    with contextlib.ExitStack() as stack:
        client = stack.enter_context(connect(spec)) if spec["transport"] != "cold" else None
        launch.wait()
        started.wait()
        warm_start, measure_start = spec["starts"]
        # Launch warmup and measured traffic on the same persistent connection.
        # Change the measured SET payload so the post-run oracle proves visibility
        # of a measured write, rather than prefill or warmup.
        warm_value = bytes([spec["value"][0] ^ 0xFF]) + spec["value"][1:]
        def phase(count, origin, measure):
            turn_spec = {**spec, "value": warm_value} if not measure and spec["workload"] == "set-px" else spec
            indices = iter(range(lane, count, spec["lanes"]))
            while batch := list(take(indices, spec["pipeline"])):
                jobs = [(i, *key_for(i, spec)) for i in batch]
                due = [origin + i * NS // spec["rate"] for i in batch]
                wait_until(due[-1])
                started = time.monotonic_ns()
                if started - due[0] > spec["timeout_ms"] * 1_000_000:
                    results = ["arrival-deadline"] * len(batch)
                else:
                    try:
                        if client is None:
                            with connect(spec) as temporary:
                                replies = sample(temporary, jobs, turn_spec)
                        else:
                            replies = sample(client, jobs, turn_spec)
                        results = [
                            error_name(reply) if isinstance(reply, Exception) else
                            ("ok" if expected is reply or (isinstance(expected, bytes) and expected == reply)
                             else "incorrect-reply")
                            for (_, _, _, expected), reply in zip(jobs, replies, strict=True)
                        ]
                    except Exception as error:
                        results = [error_name(error)] * len(batch)
                completed = time.monotonic_ns()
                if measure:
                    result.extend(zip(batch, due, (started,) * len(batch),
                                      (completed,) * len(batch), results, strict=True))

        phase(spec["warmup"], warm_start, False)
        phase(spec["samples"], measure_start, True)
    return result


def take(iterator, count):
    for _ in range(count):
        try:
            yield next(iterator)
        except StopIteration:
            break


def main():
    spec_path, process_index, output_path = sys.argv[1:]
    spec = json.loads(Path(spec_path).read_text())
    spec["value"] = base64.b64decode(spec["value_b64"])
    process_index = int(process_index)
    lane_ids = list(range(process_index, spec["lanes"], spec["client_processes"]))
    result = [None] * len(lane_ids)
    workers = []
    launch = threading.Barrier(len(lane_ids) + 1, timeout=10)
    errors = []
    started = threading.Event()

    def worker(index, lane):
        try:
            result[index] = run_lane(spec, lane, launch, started)
        except Exception as error:
            errors.append(error_name(error))

    for index, lane in enumerate(lane_ids):
        thread = threading.Thread(target=worker, args=(index, lane))
        workers.append(thread)
        thread.start()
    try:
        launch.wait()
        Path(f"{output_path}.ready").touch()
        start_path = Path(spec["start_path"])
        expires = time.monotonic() + 10
        while not start_path.exists():
            if time.monotonic() >= expires:
                raise RuntimeError("missing launch schedule")
            time.sleep(0.005)
        spec["starts"] = [int(value) for value in json.loads(start_path.read_text())]
    finally:
        started.set()
    for thread in workers:
        thread.join()
    if errors:
        raise RuntimeError("load lanes failed: " + ", ".join(errors))
    with gzip.open(output_path, "wt", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["sequence", "scheduled_ns", "started_ns", "completed_ns", "outcome"])
        for rows in result:
            writer.writerows(rows)


if __name__ == "__main__":
    main()

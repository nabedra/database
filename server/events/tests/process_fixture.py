"""Isolated Docker fixture and HTTP oracle helpers for Events process tests."""

from contextlib import contextmanager
from decimal import Decimal
import json
import math
from pathlib import Path
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4


FIXTURE = Path(__file__).resolve().parents[1] / "deploy" / "fixture.py"
CONFIG = "/etc/nabedra-events/broker.json"
MOUNT = "/var/lib/nabedra-events"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def command(*args, timeout=60, input=None, check=True):
    return subprocess.run(
        args,
        input=input,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=check,
    )


def eventually(description, probe, *, timeout=30):
    end = time.monotonic() + timeout
    while True:
        observed = probe()
        if observed:
            return observed
        if time.monotonic() >= end:
            raise AssertionError(f"deadline waiting for {description}")
        time.sleep(0.12)


def encode(body):
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode()


def request(url, path, *, method="GET", token=None, lease=None, body=None, timeout=8, tls=None):
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if lease:
        headers["X-Events-Lease"] = lease
    if body is not None:
        headers["Content-Type"] = "application/json"
        if not isinstance(body, bytes):
            body = encode(body)
    req = Request(url + path, data=body, headers=headers, method=method)
    try:
        response = urlopen(req, timeout=timeout, context=tls)
    except HTTPError as error:
        response = error
    except (OSError, URLError, TimeoutError):
        return 0, {}, None
    with response:
        try:
            data = response.read(1024 * 1024 + 1)
        except (OSError, TimeoutError):
            return 0, {}, None
        try:
            data = json.loads(data) if data else None
        except (UnicodeError, ValueError):
            data = None
        return response.status, dict(response.headers), data


def status(response, expected, code=None):
    actual, headers, body = response
    wanted = expected if isinstance(expected, tuple) else (expected,)
    actual_code = body.get("code") if isinstance(body, dict) else None
    require(
        actual in wanted and (code is None or actual_code == code),
        f"expected HTTP {wanted} {code or ''}, got {actual} {actual_code or ''}",
    )
    return body


def partition_route(topic, partition=0):
    return f"/v1/topics/{topic}/partitions/{partition}"


def subscription_route(topic, subscription, partition=0):
    return f"{partition_route(topic, partition)}/subscriptions/{subscription}"


def group_id(topic_id, partition=0):
    return f"p-{UUID(topic_id).hex}-{partition}"


def cgroup_sample(container):
    result = command(
        "docker", "exec", container, "sh", "-c",
        r"""
        set -eu
        cat /sys/fs/cgroup/memory.peak /sys/fs/cgroup/cpu.stat
        for name in /proc/[0-9]*/comm; do
            if [ "$(cat "$name" 2>/dev/null)" = "nabedra-events" ]; then
                awk '/^VmHWM:/ { printf "broker_peak_rss_bytes %.0f\n", $2 * 1024 }' "${name%/comm}/status"
                break
            fi
        done
        du -sb "$1"
        """,
        "sh", MOUNT,
        timeout=15,
    )
    lines = result.stdout.splitlines()
    rss = next((row.split()[1] for row in lines if row.startswith("broker_peak_rss_bytes ")), None)
    require(rss is not None, "broker process RSS unavailable in its Docker container")
    return {
        "peak_cgroup_bytes": int(lines[0]),
        "broker_peak_rss_bytes": int(rss),
        "cpu_usage_usec": int(lines[1].split()[1]),
        "disk_bytes": int(lines[-1].split()[0]),
    }


class Cluster:
    def __init__(
        self, directory, *, brokers, metadata_voters, image, cpus, memory_mib,
        config=None, standby=0, tls=None, initialize=True,
    ):
        self.directory = Path(directory)
        # A new cluster forms only after `nabedra-events init` on broker 1's empty volume;
        # a restore target skips it.
        self.initialize = initialize
        self.image = image
        self.brokers = brokers
        self.metadata_voters = metadata_voters
        self.tls = tls
        # Reserve a slot for backup/restore CLI or an optional fault observer.
        cpu_millis = math.floor(Decimal(str(cpus)) * 1000 / (brokers + standby)) * brokers
        ram_mib = memory_mib // (brokers + standby) * brokers
        require(cpu_millis >= brokers * 50 and ram_mib >= brokers * 160, "fixture budget too small")
        command(
            sys.executable, str(FIXTURE), str(directory), "--brokers", str(brokers),
            "--metadata-voters", str(metadata_voters), "--image", image,
            "--cpus", str(Decimal(cpu_millis) / 1000), "--memory-mib", str(ram_mib),
        )
        self.compose_file = self.directory / "compose.json"
        self.compose = json.loads(self.compose_file.read_text())
        for service in self.compose["services"].values():
            service["ports"] = ["127.0.0.1::8080"]
        self.compose_file.write_text(json.dumps(self.compose))
        for broker in range(1, brokers + 1):
            path = self.directory / f"broker-{broker}.json"
            settings = json.loads(path.read_text())
            if config:
                config(broker, settings)
                path.write_text(json.dumps(settings))
        self.cluster_id = json.loads((self.directory / "broker-1.json").read_text())["cluster_id"]
        self.urls = {}
        self.running = set()
        self.operator = None
        self.caps = {"cpu": cpu_millis / 1000, "memory_mib": ram_mib}

    def service(self, broker):
        return f"broker-{broker}"

    def docker(self, *args, **kwargs):
        return command("docker", "compose", "--file", str(self.compose_file), *args, **kwargs)

    def start(self, brokers, *, ready=True):
        brokers = list(brokers)
        if self.initialize and 1 in brokers:
            self.cli(1, "init", "--config", CONFIG)
            self.initialize = False
        self.docker("up", "-d", "--no-recreate", *(self.service(broker) for broker in brokers))
        self.running.update(brokers)
        for broker in brokers:
            address = self.docker("port", self.service(broker), "8080").stdout.strip()
            require(address.startswith("127.0.0.1:"), "fixture ingress must be loopback-only")
            self.urls[broker] = ("https://" if self.tls else "http://") + address
        if ready:
            self.ready(brokers[0])
        if self.operator is None and brokers:
            self.operator = self.grant({"operation": "operator"}, broker=brokers[0])

    def ready(self, broker):
        eventually(f"broker {broker} readiness", lambda: self.rpc(broker, "/readyz")[0] == 200)

    def kill(self, brokers):
        brokers = list(brokers)
        self.docker("kill", "-s", "SIGKILL", *(self.service(broker) for broker in brokers))
        self.running.difference_update(brokers)

    def container(self, broker):
        return self.docker("ps", "-q", self.service(broker)).stdout.strip()

    def mounts(self):
        return {
            broker: command(
                "docker", "inspect", "--format",
                '{{range .Mounts}}{{if eq .Destination "' + MOUNT + '"}}{{.Name}}{{end}}{{end}}',
                self.container(broker),
            ).stdout.strip()
            for broker in sorted(self.running)
        }

    def grant(self, scope, *, broker=None, subject=None, tenant="fixture"):
        broker = broker or min(self.running)
        body = {
            "tenant": tenant,
            "subject": subject or scope["operation"],
            "scope": scope,
            "expires_at_ms": time.time_ns() // 1_000_000 + 3_600_000,
        }
        return self.docker(
            "exec", "-T", self.service(broker),
            "/usr/local/bin/nabedra-events", "issue-grant", "--config", CONFIG,
            "--grant", "/dev/stdin", input=json.dumps(body),
        ).stdout.strip()

    def publisher(self, topic, *, subject=None, tenant="fixture"):
        return self.grant({"operation": "publish", "topic": topic}, subject=subject, tenant=tenant)

    def reader(self, topic, subscription, *, subject=None):
        return self.grant({"operation": "read", "topic": topic, "subscription": subscription}, subject=subject)

    def rpc(self, broker, path, **kwargs):
        return request(self.urls[broker], path, tls=self.tls, **kwargs)

    def admin(self, path, *, broker=None, **kwargs):
        return self.rpc(
            broker or min(self.running), f"/v1/admin{path}",
            token=self.operator, **kwargs,
        )

    def wait_status(self, path, *, broker=None):
        def probe():
            response = self.admin(path, broker=broker)
            if response[0] == 200:
                return response[2]
            if response[0] not in (0, 404, 429, 503):
                status(response, 200)
            return None
        return eventually(f"admin status {path}", probe)

    def submit(self, action, *, id=None, broker=None):
        id = str(id or uuid4())
        record = {"operation_id": id, "action": action}

        def probe():
            response = self.admin("/operations", broker=broker, method="POST", body=record)
            if response[0] in (0, 429, 503):
                return None
            status(response, (200, 202))
            return response[2]

        reply = eventually(f"operation {action['type']} admission", probe)
        require(reply["operation_id"] == id, "operation receipt identity changed")
        return id, reply

    def receipt(self, id, *, broker=None, timeout=75):
        def probe():
            response = self.admin(f"/operations/{id}", broker=broker)
            if response[0] in (0, 404, 429, 503):
                return None
            reply = status(response, (200, 202))
            require(reply["operation_id"] == id, "operation receipt identity changed")
            return reply if reply["state"] == "completed" else None
        return eventually(f"operation {id} completion", probe, timeout=timeout)

    def operate(self, action, *, broker=None, id=None, timeout=75):
        id, reply = self.submit(action, broker=broker, id=id)
        return reply if reply["state"] == "completed" else self.receipt(id, broker=broker, timeout=timeout)

    def topic(self, name, *, partitions=1, replication_factor=1, retention_seconds=3600):
        self.operate({
            "type": "create_topic", "topic": name, "partitions": partitions,
            "replication_factor": replication_factor, "retention_seconds": retention_seconds,
        })
        return self.wait_status(f"/topics/{name}")

    def subscribe(self, topic, subscription, start="earliest"):
        self.operate({
            "type": "create_subscription", "topic": topic,
            "subscription": subscription, "start": start,
        })

    def leader(self, group, *, brokers=None, excluding=()):
        candidates = sorted(self.running if brokers is None else brokers)
        excluded = set(excluding)
        path = f"/groups/{group}"

        def probe():
            for broker in candidates:
                response = self.admin(path, broker=broker)
                if response[0] != 200:
                    continue
                view = response[2]
                if view["running"] and view["leader"] == broker and broker not in excluded:
                    return broker, view
            return None
        return eventually(f"new leader for {group}", probe)

    def cli(self, broker, *args, bind=None, **kwargs):
        mounts = ["-v", bind] if bind else []
        return self.docker("run", "--rm", "--no-deps", *mounts, self.service(broker), *args, **kwargs)

    def close(self, *, keep=False):
        self.docker("down", *(("--volumes",) if not keep else ()), "--timeout", "5")
        if not keep:
            result = command(
                "docker", "ps", "-aq", "--filter",
                f"label=com.docker.compose.project={self.compose['name']}",
            )
            require(not result.stdout.strip(), "fixture left a Docker container")
            result = command(
                "docker", "volume", "ls", "-q", "--filter",
                f"label=com.docker.compose.project={self.compose['name']}",
            )
            require(not result.stdout.strip(), "fixture left a Docker volume")
            result = command(
                "docker", "network", "ls", "-q", "--filter",
                f"label=com.docker.compose.project={self.compose['name']}",
            )
            require(not result.stdout.strip(), "fixture left a Docker network")


@contextmanager
def fixtures(*, keep=False, diagnostics=None):
    clusters = []
    try:
        yield clusters
    except BaseException:
        if diagnostics:
            for cluster in clusters:
                result = cluster.docker("logs", "--no-color", check=False)
                (diagnostics / f"{cluster.compose['name']}.log").write_text(result.stdout + result.stderr)
        raise
    finally:
        for cluster in reversed(clusters):
            cluster.close(keep=keep)

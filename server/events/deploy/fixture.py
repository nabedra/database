#!/usr/bin/env python3
"""Generate an isolated Docker Compose Events cluster with aggregate limits."""

import argparse
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
import secrets
import time
from uuid import uuid4


def cpu_millis(value):
    try:
        scaled = Decimal(value) * 1000
        if scaled.is_finite() and 1 <= scaled <= 4000 and scaled == scaled.to_integral_value():
            return int(scaled)
    except InvalidOperation:
        pass
    raise argparse.ArgumentTypeError("CPU quota must be 0.001 to 4 with at most three decimal places")


def write_document(directory, filename, document):
    path = directory / filename
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def create_fixture(args, directory):
    cluster_id = uuid4()
    broker_ids = range(1, args.brokers + 1)
    expires_at_ms = int(time.time() * 1000) + 7 * 86_400_000
    signing_keys = {
        purpose: {
            "active": "fixture",
            "keys": [{
                "id": "fixture",
                "secret_hex": secrets.token_hex(32),
                "accept_until_ms": expires_at_ms,
            }],
        }
        for purpose in ("grant", "session")
    }
    keyring = {"cluster_id": str(cluster_id), **signing_keys}
    peer_token = secrets.token_urlsafe(48)
    bootstrap = {
        "metadata_voters": list(range(1, args.metadata_voters + 1)),
        "brokers": [
            {"id": broker, "zone": f"zone-{broker}", "peer_url": f"http://broker-{broker}:8081"}
            for broker in broker_ids
        ],
    }

    services = {}
    volumes = {}
    cpu_each = args.cpus // args.brokers
    memory_each = args.memory_mib // args.brokers
    for broker in broker_ids:
        name = f"broker-{broker}"
        config = {
            "broker_id": broker,
            "cluster_id": str(cluster_id),
            "data_dir": "/var/lib/nabedra-events",
            "public_bind": "0.0.0.0:8080",
            "peer_bind": "0.0.0.0:8081",
            "allow_plaintext": True,
            "initialize_new_cluster": broker == 1,
            "peer_token": peer_token,
            "bootstrap": bootstrap,
            "keyring": keyring,
        }
        write_document(directory, f"{name}.json", config)
        services[name] = {
            "image": args.image,
            "cpus": str(Decimal(cpu_each) / 1000),
            "mem_limit": f"{memory_each}m",
            "environment": {"TOKIO_WORKER_THREADS": str(max(1, cpu_each // 1000))},
            "init": True,
            "read_only": True,
            "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges:true"],
            "tmpfs": ["/tmp:rw,noexec,nosuid,size=16m"],
            "ports": [f"127.0.0.1:{args.base_port + broker - 1}:8080"],
            "volumes": [
                f"./{name}.json:/etc/nabedra-events/broker.json:ro",
                f"{name}-data:/var/lib/nabedra-events",
            ],
        }
        volumes[f"{name}-data"] = {}

    write_document(directory, "compose.json", {
        "name": f"events-{cluster_id.hex[:12]}",
        "services": services,
        "volumes": volumes,
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="new fixture directory")
    parser.add_argument("--brokers", type=int, choices=range(1, 9), default=3)
    parser.add_argument("--metadata-voters", type=int, choices=(1, 3))
    parser.add_argument("--cpus", type=cpu_millis, default=2_000, help="total CPU quota, default 2")
    parser.add_argument("--memory-mib", type=int, default=1_536, help="total RAM in MiB, default 1536")
    parser.add_argument("--base-port", type=int, default=18080)
    parser.add_argument("--image", default="nabedra-events:local")
    args = parser.parse_args()
    if args.metadata_voters is None:
        args.metadata_voters = 3 if args.brokers >= 3 else 1
    if args.metadata_voters > args.brokers:
        parser.error("metadata voters exceed brokers")
    if not args.brokers * 160 <= args.memory_mib <= 8192:
        parser.error("allow at least 160 MiB per broker and no more than 8192 MiB overall")
    if args.cpus < 50 * args.brokers:
        parser.error("allow at least 0.05 CPU per broker")
    if not 1024 <= args.base_port <= 65536 - args.brokers:
        parser.error("host ports must all be in 1024-65535")
    try:
        args.directory.mkdir(mode=0o700, parents=True)
    except FileExistsError:
        parser.error("fixture directory already exists, reuse its Compose file to keep broker state")

    create_fixture(args, args.directory)
    print(args.directory / "compose.json")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Exercise the distributed benchmark against two TLS fleets inside Docker."""

import argparse
from contextlib import ExitStack
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

from redis import Redis
from redis.exceptions import ConnectionError, TimeoutError

from run import HERE, digest


def fleet_fixture():
    source = HERE.parent / "cache-acceptance" / "cluster.py"
    spec = importlib.util.spec_from_file_location("cache_acceptance_fixture", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def start_nabedra(fixture, fleet):
    for node in range(1, 4):
        fleet.controller(node)
    fixture.wait("controller quorum", fleet.status)
    for worker in range(1, 4):
        fleet.add_worker(worker)
    fleet.wait_placement(lambda status: len(status["workers"]) == 3 and all(
        uuid.UUID(worker["id"]["incarnation"]).int != 0
        for worker in status["workers"].values()
    ))
    fleet.apply("init", "--groups", "3")
    fleet.wait_placement(fixture.ready)
    return [port for port, _ in fleet.workers.values()]


def start_valkey(args, fixture, fleet):
    fixture.tls(fleet.root, "reference")
    ports = []
    certificate = fleet.root / "ca.crt"
    for index in range(6):
        port = fleet.allocate()
        bus = fleet.allocate()
        directory = fleet.root / f"valkey-{index}"
        directory.mkdir()
        config = directory / "valkey.conf"
        config.write_text(
            f'bind 127.0.0.1\nport 0\ntls-port {port}\n'
            f'tls-cert-file "{fleet.root / "reference.crt"}"\n'
            f'tls-key-file "{fleet.root / "reference.key"}"\ntls-ca-cert-file "{certificate}"\n'
            'tls-auth-clients no\ntls-replication yes\ntls-cluster yes\n'
            'save ""\nappendonly no\ncluster-enabled yes\ncluster-node-timeout 5000\n'
            f'cluster-port {bus}\ncluster-announce-bus-port {bus}\ncluster-announce-ip 127.0.0.1\n'
            f'cluster-announce-tls-port {port}\n'
            f'requirepass {fixture.PASSWORD}\nmasterauth {fixture.PASSWORD}\n'
        )
        with (args.output / f"valkey-{index}.log").open("wb") as output:
            process = subprocess.Popen([args.valkey, config], cwd=directory, stdout=output, stderr=output)
        fleet.scope.callback(fleet.reap, process)
        ports.append(port)
    connections = [fleet.scope.enter_context(Redis(
        host="127.0.0.1", port=port, protocol=2, ssl=True, ssl_check_hostname=True,
        ssl_ca_certs=str(certificate), password=fixture.PASSWORD, socket_connect_timeout=.3,
        socket_timeout=.5,
    )) for port in ports]

    def booted():
        try:
            return all(connection.ping() for connection in connections)
        except (ConnectionError, TimeoutError):
            return False

    fixture.wait("Valkey TLS listeners", booted)
    command = [
        args.valkey_cli, "--tls", "--cacert", certificate, "--cluster", "create",
        *(f"127.0.0.1:{port}" for port in ports), "--cluster-replicas", "1", "--cluster-yes",
    ]
    with (args.output / "valkey-create.log").open("wb") as output:
        subprocess.run(command, timeout=30, check=True, stdout=output, stderr=output,
                       env={**os.environ, "REDISCLI_AUTH": fixture.PASSWORD})

    def assigned():
        for connection in connections:
            rows = connection.execute_command("CLUSTER", "SLOTS")
            if len(rows) != 3 or any(len(row) != 4 for row in rows):
                return False
        return True

    fixture.wait("Valkey slot and replica discovery", assigned)
    return ports


def verify_measurements(output, drivers, workloads, samples):
    rows = {file.stem: json.loads(file.read_text()) for file in sorted(output.glob("3-*.json"))}
    expected = len(drivers) * len(workloads) * 3
    if len(rows) != expected:
        raise RuntimeError(f"expected {expected} calibration and paired runs, found {len(rows)}")
    for name, row in rows.items():
        if row["outcomes"] != {"ok": samples}:
            raise RuntimeError(f"{name} recorded incorrect outcomes {row['outcomes']}")
        if "-set-px-" in name and not row["set_px_verification"]["keys_checked"]:
            raise RuntimeError(f"{name} did not verify published values and TTLs")
    report = json.loads((output / "cluster-summary.json").read_text())
    if any(len(placement["primary_endpoints"]) != 3 or len(placement["replica_endpoints"]) != 3
           for placement in report["profiles"]["3"]["placement"].values()):
        raise RuntimeError("three primary and three replica groups required")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nabedra", required=True, type=Path)
    parser.add_argument("--valkey", required=True, type=Path)
    parser.add_argument("--valkey-cli", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--node-driver", default=HERE / "driver.mjs", type=Path)
    parser.add_argument("--client-cpu", required=True, type=int)
    parser.add_argument("--drivers", default="stock,python,typescript")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        parser.error("choose an empty evidence directory")
    fixture = fleet_fixture()
    samples = 24
    workloads = ("get-hit", "set-px")
    with tempfile.TemporaryDirectory(prefix="nabedra-local-pair-") as temporary, ExitStack() as scope:
        root = Path(temporary)
        fleet = fixture.Fleet(args.nabedra.resolve(), root, args.output, scope)
        placements = {
            "nabedra": start_nabedra(fixture, fleet),
            "valkey": start_valkey(args, fixture, fleet),
        }
        manifest = root / "paired.json"
        manifest.write_text(json.dumps({
            "environment": "docker",
            "clusters": {"3": {
                engine: {
                    "endpoints": [f"rediss://127.0.0.1:{port}" for port in ports],
                    "certificate": str(root / "ca.crt"),
                    "password_env": "NABEDRA_LOCAL_PAIR_PASSWORD",
                    "username": "default",
                }
                for engine, ports in placements.items()
            }},
        }))
        output = args.output / "benchmark"
        command = [
            sys.executable, HERE / "cluster.py", "--manifest", manifest, "--output", output,
            "--groups", "3", "--rates", "24", "--client-cpus", str(args.client_cpu),
            "--drivers", args.drivers, "--workloads", ",".join(workloads), "--sizes", "64",
            "--lanes-per-group", "1", "--keys-per-group", "8", "--repetitions", "1",
            "--samples", str(samples), "--warmup-seconds", ".05", "--node-driver", args.node_driver,
        ]
        with (args.output / "pair.log").open("wb") as log:
            subprocess.run(command, timeout=240, check=True, stdout=log, stderr=log,
                           env={**os.environ, "NABEDRA_LOCAL_PAIR_PASSWORD": fixture.PASSWORD})
        rows = verify_measurements(output, args.drivers.split(","), workloads, samples)
    result = {
        "environment": "docker",
        "qualification": "local benchmark path only",
        "drivers": args.drivers.split(","),
        "workloads": workloads,
        "runs": len(rows),
        "correct_operations": samples * len(rows),
        "nabedra_sha256": digest(args.nabedra),
        "valkey_sha256": digest(args.valkey),
    }
    (args.output / "local-summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

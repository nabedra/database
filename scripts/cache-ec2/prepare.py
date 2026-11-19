#!/usr/bin/env python3
"""Create per-node Cache bootstrap secrets and package an immutable release bundle."""
import argparse
import hashlib
import json
import os
import secrets
import subprocess
import tarfile
import tempfile
import uuid
from pathlib import Path

from settings import dns_name, load_network, require, secret_prefix

SCRIPT = Path(__file__).resolve().parent
MIB = 1 << 20
NIL = "00000000-0000-0000-0000-000000000000"


def openssl(*arguments):
    return subprocess.run(
        ["openssl", *map(str, arguments)], check=True, capture_output=True,
    ).stdout


def issue(directory, name, host, ca):
    key = directory / f"{name}.key"
    csr = directory / f"{name}.csr"
    certificate = directory / f"{name}.pem"
    extensions = directory / f"{name}.ext"
    extensions.write_text(
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature\n"
        "extendedKeyUsage=serverAuth,clientAuth\n"
        f"subjectAltName=DNS:{host}\n",
        encoding="ascii",
    )
    openssl("req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
            "-subj", f"/CN={host}", "-keyout", key, "-out", csr)
    openssl("x509", "-req", "-in", csr, "-CA", ca, "-CAkey", directory / "ca.key",
            "-set_serial", "0x" + secrets.token_hex(16), "-extfile", extensions,
            "-days", "30", "-out", certificate)
    digest = hashlib.sha256(openssl("x509", "-in", certificate, "-outform", "DER")).digest()
    return {
        "host": host,
        "fingerprints": [list(digest)],
        "certificate": certificate.read_text(encoding="ascii"),
        "key": key.read_text(encoding="ascii"),
    }


def endpoint(label, identity, port):
    return (
        f"\n[{label}]\nhost = {json.dumps(identity['host'])}\n"
        f"port = {port}\nfingerprints = {json.dumps(identity['fingerprints'])}\n"
    )


def tls(label="tls"):
    return f'\n[{label}]\nca = "ca.pem"\ncertificate = "node.pem"\nprivate_key = "node.key"\n'


def material(config, identity, ca, extras=None):
    return {"files": {
        "node.toml": config, "node.pem": identity["certificate"], "node.key": identity["key"],
        "ca.pem": ca, **(extras or {}),
    }}


def write_secrets(output, prefix, ca, controllers, workers, operator, network, memory, namespace):
    entries = []

    def save(role, index, files):
        suffix = f"{role}/{index}" if index is not None else role
        path = output / (suffix.replace("/", "-") + ".json")
        encoded = json.dumps(files, separators=(",", ":"))
        if len(encoded.encode()) > 65536:
            raise ValueError(f"secret exceeds the provider payload limit: {suffix}")
        path.write_text(encoded, encoding="utf-8")
        entries.append({"name": f"{prefix}/{suffix}", "file": path.name})

    voter_config = "".join(endpoint(f'voters."{i}"', node, 7401)
                           for i, node in enumerate(controllers, 1))
    controller_config = "".join(endpoint(f'controllers."{i}"', node, 7400)
                                for i, node in enumerate(controllers, 1))
    cluster_id = uuid.uuid4()
    for i, node in enumerate(controllers, 1):
        config = (f'node = {i}\ncluster_id = "{cluster_id}"\nlisten = "0.0.0.0:7400"\nraft_listen = "0.0.0.0:7401"\n'
                  'data_dir = "/var/lib/nabedra-cache"\nclock = "clock_bound"\n'
                  f'operators = {json.dumps(operator["fingerprints"])}\n')
        save("controller", i, material(config + tls() + voter_config, node, ca))

    password = secrets.token_urlsafe(32)
    connections = 40 if memory >= 1024 else 24
    handshakes = min(16, connections)
    pending = min(32, connections)
    members = {}
    for i, node in enumerate(workers, 1):
        zone = network["zones"][(i - 1) % 3]
        identity = (
            f'node = {i}\nzone = "{zone}"\nclient_host = "{node["host"]}"\nclient_port = 6379\n'
        )
        config = (identity + 'peer_listen = "0.0.0.0:7500"\nclock = "clock_bound"\n'
                  'metrics_file = "/run/nabedra-cache/metrics.json"\n')
        config += endpoint("peer", node, 7500) + tls() + controller_config
        config += (
            '\n[client]\nlisten = "0.0.0.0:6379"\n'
            '\n[client.limits]\n'
            f'memory_bytes = {memory * MIB}\ndata_bytes = {(memory // 2) * MIB}\n'
            f'max_connections = {connections}\nmax_handshakes = {handshakes}\nmax_pending = {pending}\n'
            '\n[client.tls]\ncertificate = "node.pem"\nprivate_key = "node.key"\n'
            '\n[[client.users]]\nusername = "cache"\n'
            f'password = "{password}"\nkey_prefixes = ["nb:{namespace}:"]\n'
            'commands = ["get", "set", "del", "exists", "mget", "ttl", "pttl", '
            '"expire", "pexpire", "persist", "incrby"]\n'
        )
        save("worker", i, material(config, node, ca))
        member = identity.split("\n", 1)[1] + f'\n[id]\nnode = {i}\nincarnation = "{NIL}"\n'
        members[f"member-{i}.toml"] = member + endpoint("peer", node, 7500)

    save("operator", None, material(tls() + controller_config, operator, ca, members))
    (output / "secret-index.json").write_text(json.dumps(entries, indent=2) + "\n")
    (output / "ca.pem").write_text(ca, encoding="ascii")
    (output / "client.json").write_text(json.dumps({
        "endpoints": [f"rediss://{node['host']}:6379" for node in workers[:3]],
        "allowed_nodes": [f"rediss://{node['host']}:6379" for node in workers],
        "username": "cache", "password": password, "ca_file": "ca.pem", "namespace": namespace,
    }, indent=2) + "\n")


def credentials(args):
    os.umask(0o077)
    domain = dns_name(args.domain)
    if len(domain) > 240:
        raise ValueError("private DNS name exceeds the hostname limit")
    prefix = secret_prefix(args.bootstrap_prefix)
    namespace = require(r"[a-z0-9_-]{1,64}", args.namespace, "Cache namespace")
    network = load_network(args.network)
    args.output.mkdir(mode=0o700)
    ca = args.output / "ca.pem"
    openssl("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
            "-nodes", "-subj", f"/CN={domain} Cache CA", "-days", "365",
            "-addext", "basicConstraints=critical,CA:TRUE,pathlen:0",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign",
            "-keyout", args.output / "ca.key", "-out", ca)
    with tempfile.TemporaryDirectory(dir=args.output) as temporary:
        directory = Path(temporary)
        # Keep the signing key offline. No instance or artifact receives it.
        (directory / "ca.key").write_bytes((args.output / "ca.key").read_bytes())
        controllers = [issue(directory, f"c{i}", f"c{i}.{domain}", ca) for i in range(1, 4)]
        workers = [issue(directory, f"w{i}", f"w{i}.{domain}", ca) for i in range(1, 13)]
        operator = issue(directory, "operator", f"operator.{domain}", ca)
        write_secrets(args.output, prefix, ca.read_text(encoding="ascii"),
                      controllers, workers, operator, network, args.worker_cache_memory_mib, namespace)


def artifact(args):
    with tarfile.open(args.output, "x:gz") as tar:
        for path, name in (
            (args.binary, "nabedra-db"), (args.clockbound_daemon, "clockbound"),
            (SCRIPT / "bootstrap.sh", "bootstrap.sh"), (SCRIPT / "reconcile.py", "reconcile.py"),
            (SCRIPT / "observations.py", "observations.py"),
        ):
            if not path.is_file() or path.is_symlink():
                raise ValueError(f"expected regular artifact source: {path}")
            tar.add(path, arcname=name, recursive=False)
    with args.output.open("rb") as data:
        print(hashlib.file_digest(data, "sha256").hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    secret_cmd = commands.add_parser("credentials")
    secret_cmd.add_argument("--output", required=True, type=Path)
    secret_cmd.add_argument("--domain", required=True)
    secret_cmd.add_argument("--network", required=True)
    secret_cmd.add_argument("--bootstrap-prefix", required=True)
    secret_cmd.add_argument("--namespace", default="catalog")
    secret_cmd.add_argument("--worker-cache-memory-mib", type=int, default=1024)
    bundle = commands.add_parser("artifact")
    bundle.add_argument("--output", required=True, type=Path)
    bundle.add_argument("--binary", required=True, type=Path)
    bundle.add_argument("--clockbound-daemon", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "credentials":
        if not 768 <= args.worker_cache_memory_mib <= (1 << 20):
            parser.error("worker cache memory must be between 768 MiB and 1 TiB")
        credentials(args)
    else:
        artifact(args)


if __name__ == "__main__":
    main()

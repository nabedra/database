"""Input validation for the private EC2 deployment and its bootstrap bundles."""
import ipaddress
import json
import re
from pathlib import Path


def read_json(path):
    def invalid_constant(value):
        raise ValueError(f"invalid JSON number {value}")
    return json.loads(Path(path).read_text(encoding="utf-8"), parse_constant=invalid_constant)


def deployment_arguments(arguments, *, allow_workers=False):
    allowed = {
        "--region", "--stack", "--domain", "--ami", "--root-device", "--worker-type",
        "--worker-memory-mib", "--controller-type", "--controller-memory-mib", "--network",
        "--artifact", "--bootstrap-prefix", "--timeout",
    }
    if allow_workers:
        allowed.add("--workers")
    if not isinstance(arguments, list) or len(arguments) % 2 or not all(
        isinstance(item, str) and item for item in arguments
    ):
        raise ValueError("deployment arguments must be explicit flag/value pairs")
    parsed = {}
    for key, value in zip(arguments[::2], arguments[1::2]):
        if key not in allowed or key in parsed or value.startswith("--"):
            raise ValueError("unknown, duplicate or malformed deployment option")
        parsed[key] = value
    if not {"--region", "--stack"} <= parsed.keys():
        raise ValueError("deployment arguments require a region and stack")
    return parsed


def require(pattern, value, name):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError(f"invalid {name}")
    return value


def dns_name(name):
    label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    return require(rf"{label}(?:\.{label})+", name, "private DNS name")


def secret_prefix(prefix):
    return require(
        r"[a-zA-Z0-9][a-zA-Z0-9/_+=.@-]{0,383}", prefix, "bootstrap secret prefix"
    ).rstrip("/")


def zones(values):
    if len(values) != 3 or len(set(values)) != 3:
        raise ValueError("exactly three distinct availability zones are required")
    return [require(r"[a-z0-9-]{1,63}", item, "availability zone") for item in values]


def ipv4(value):
    result = ipaddress.IPv4Network(value, strict=True)
    if result.prefixlen == 0:
        raise ValueError("public CIDR is outside the private deployment profile")
    return result


def load_network(path):
    network = read_json(path)
    common = {"zones", "applications"}
    fresh = common | {"cidr", "subnetCidrs"}
    existing = common | {"vpc", "subnets"}
    if not isinstance(network, dict) or set(network) not in (fresh, existing):
        raise ValueError("expected either fresh or existing private network fields")
    network["zones"] = zones(network["zones"])
    apps = network["applications"]
    if not isinstance(apps, list) or not 1 <= len(apps) <= 12 or len(apps) != len(set(apps)):
        raise ValueError("provide one to twelve distinct authorized application sources")
    for app in apps:
        if app.startswith("sg-") and "vpc" in network:
            require(r"sg-[0-9a-f]{8,17}", app, "application security group")
        elif not ipv4(app).is_private:
            raise ValueError("application CIDRs must be private")
    if "vpc" in network:
        require(r"vpc-[0-9a-f]{8,17}", network["vpc"], "VPC ID")
        subnets = network["subnets"]
        if len(subnets) != 3 or len(set(subnets)) != 3:
            raise ValueError("exactly three distinct subnets are required")
        for subnet in subnets:
            require(r"subnet-[0-9a-f]{8,17}", subnet, "subnet ID")
    else:
        vpc = ipv4(network["cidr"])
        subnets = [ipv4(cidr) for cidr in network["subnetCidrs"]]
        if not vpc.is_private or not 16 <= vpc.prefixlen <= 28:
            raise ValueError("require a private VPC CIDR with an EC2 supported prefix")
        if len(subnets) != 3 or any(not s.subnet_of(vpc) or s.prefixlen > 28 for s in subnets):
            raise ValueError("three EC2 subnet CIDRs must fit inside the VPC")
        if any(left.overlaps(right) for index, left in enumerate(subnets) for right in subnets[index + 1:]):
            raise ValueError("subnets must be disjoint")
    return network


def load_artifact(path):
    artifact = read_json(path)
    if not isinstance(artifact, dict) or set(artifact) != {"bucket", "key", "version", "sha256"}:
        raise ValueError("artifact needs bucket, key, object version and SHA256")
    for key, pattern in (
        ("bucket", r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"),
        ("key", r"[a-zA-Z0-9/_.-]{1,768}"),
        ("version", r"[a-zA-Z0-9_+./=-]{1,1024}"),
        ("sha256", r"[0-9a-f]{64}"),
    ):
        require(pattern, artifact[key], f"artifact {key}")
    if artifact["version"] == "null":
        raise ValueError("enable versioning on the artifact bucket before uploading")
    return artifact

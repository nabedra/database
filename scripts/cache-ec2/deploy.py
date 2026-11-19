#!/usr/bin/env python3
"""Deploy or reconcile a private Nabedra Cache stack and print its seed endpoints."""
import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import cloudformation_v1
from settings import dns_name, load_artifact, load_network, require, secret_prefix


class AwsError(RuntimeError):
    pass


class Aws:
    def __init__(self, region):
        self.region = region

    def run(self, *arguments, timeout=60, output=True):
        result = subprocess.run(
            ["aws", "--region", self.region, "--no-cli-pager", "--output", "json", *arguments],
            text=True, capture_output=True, timeout=timeout,
            env={**os.environ, "AWS_CLI_AUTO_PROMPT": "off", "AWS_MAX_ATTEMPTS": "3"},
        )
        if result.returncode:
            raise AwsError(result.stderr.strip() or result.stdout.strip())
        return json.loads(result.stdout or "{}") if output else None

    def stack(self, name):
        deadline = time.monotonic() + 1800
        while True:
            try:
                response = self.run("cloudformation", "describe-stacks", "--stack-name", name)
            except AwsError as error:
                if "does not exist" in str(error) and "ValidationError" in str(error):
                    return None
                raise
            stack, = response["Stacks"]
            status = stack["StackStatus"]
            if status in {"CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"}:
                return {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs", [])}
            if not status.endswith("_IN_PROGRESS") or status.startswith("DELETE_"):
                raise AwsError(f"cache stack cannot be reconciled in {status}")
            if time.monotonic() >= deadline:
                raise TimeoutError("existing cache stack operation did not settle")
            time.sleep(5)

    def reconcile(self, instance, action, workers, prior_workers, timeout):
        remote = shlex.join([
            "python3", "/usr/local/lib/nabedra-cache/reconcile.py", action,
            "--workers", str(workers), "--prior-workers", str(prior_workers),
            "--timeout", str(timeout),
        ])
        wait = (
            "for i in $(seq 1 120); do "
            "if test -x /usr/local/lib/nabedra-cache/reconcile.py && "
            "test -f /etc/nabedra-cache/operator/node.toml; then break; fi; sleep 2; done"
        )
        return self.ssm(instance, [wait, "exec " + remote], timeout + 240)

    def ssm(self, instance, commands, timeout):
        deadline = time.monotonic() + timeout + 120
        while True:
            try:
                response = self.run(
                    "ssm", "send-command", "--instance-ids", instance,
                    "--document-name", "AWS-RunShellScript", "--timeout-seconds", str(timeout + 60),
                    "--parameters", json.dumps({"commands": commands, "executionTimeout": [str(timeout + 30)]}),
                )
                break
            except AwsError as error:
                if "InvalidInstanceId" not in str(error) or time.monotonic() >= deadline:
                    raise
                time.sleep(5)
        identifier = response["Command"]["CommandId"]
        while time.monotonic() < deadline:
            try:
                result = self.run(
                    "ssm", "get-command-invocation", "--command-id", identifier, "--instance-id", instance,
                )
            except AwsError as error:
                if "InvocationDoesNotExist" not in str(error):
                    raise
            else:
                state = result["Status"]
                if state == "Success":
                    return result["StandardOutputContent"]
                if state not in {"Pending", "InProgress", "Delayed", "Cancelling"}:
                    raise AwsError(f"SSM command {identifier} ended with {state}: {result['StandardErrorContent']}")
            time.sleep(5)
        raise TimeoutError(f"SSM command {identifier} did not finish")


def fingerprint(options, network, artifact):
    immutable = {
        "controller_type": options.controller_type, "worker_type": options.worker_type,
        "ami": options.ami, "domain": options.domain, "bootstrap_prefix": options.bootstrap_prefix,
        "root_device": options.root_device, "controller_memory_mib": options.controller_memory_mib,
        "worker_memory_mib": options.worker_memory_mib, "network": network, "artifact": artifact,
    }
    return hashlib.sha256(json.dumps(immutable, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def preflight(aws, options, network):
    zone_rows = aws.run("ec2", "describe-availability-zones", "--zone-names", *network["zones"])["AvailabilityZones"]
    if {item["ZoneName"] for item in zone_rows if item["State"] == "available"} != set(network["zones"]):
        raise ValueError("all three availability zones must be available in the selected region")
    if "vpc" in network:
        subnets = aws.run("ec2", "describe-subnets", "--subnet-ids", *network["subnets"])["Subnets"]
        by_id = {subnet["SubnetId"]: subnet for subnet in subnets}
        for subnet, zone in zip(network["subnets"], network["zones"], strict=True):
            detail = by_id[subnet]
            if (
                detail["VpcId"] != network["vpc"] or detail["AvailabilityZone"] != zone
                or detail["State"] != "available" or detail["MapPublicIpOnLaunch"]
            ):
                raise ValueError("existing subnets must be private and match the VPC and zone order")
    image, = aws.run("ec2", "describe-images", "--image-ids", options.ami)["Images"]
    if image["State"] != "available" or image["RootDeviceName"] != options.root_device:
        raise ValueError("AMI state and root device do not match the deployment")
    types = aws.run("ec2", "describe-instance-types", "--instance-types",
                    *sorted({options.controller_type, options.worker_type}))["InstanceTypes"]
    info = {kind["InstanceType"]: kind for kind in types}
    for kind, memory in (
        (options.controller_type, options.controller_memory_mib), (options.worker_type, options.worker_memory_mib),
    ):
        shape = info[kind]
        if (
            shape["Hypervisor"] != "nitro" or image["Architecture"] not in shape["ProcessorInfo"]["SupportedArchitectures"]
            or shape["MemoryInfo"]["SizeInMiB"] < memory + 256
        ):
            raise ValueError(f"{kind} needs Nitro, compatible architecture, and OS memory headroom")


def deploy(aws, options, network, artifact):
    identity = fingerprint(options, network, artifact)
    previous = aws.stack(options.stack)
    prior_count = int(previous["Workers"]) if previous is not None else options.workers
    if previous is not None and previous["DeploymentFingerprint"] != identity:
        raise ValueError("immutable stack parameters changed; use the rolling replacement or rotation runbook")
    preflight(aws, options, network)
    replacements = {} if previous is None else json.loads(previous.get("WorkerReplacements", "{}"))
    worker = options.replace_failed_worker
    if worker is not None:
        if previous is None or worker > min(prior_count, options.workers) or prior_count != options.workers:
            raise ValueError("failed replacement requires an existing worker at unchanged fleet size")
        instances = aws.run(
            "ec2", "describe-instances", "--instance-ids", previous[f"Worker{worker}"]
        )["Reservations"]
        states = [instance["State"]["Name"] for reservation in instances for instance in reservation["Instances"]]
        if states != ["terminated"]:
            raise ValueError("the worker must already be terminated for failed-instance replacement")
        key = str(worker)
        replacements[key] = replacements.get(key, 0) + 1
    if options.workers < prior_count:
        result = json.loads(aws.reconcile(previous["Controller1"], "drain", options.workers, prior_count, options.timeout))
        missing = set(range(options.workers + 1, prior_count + 1)) - set(result["drained"])
        if missing or not result["ready"]:
            raise ValueError("controller did not attest safe termination for all removed workers")

    template = render(options, network, artifact, replacements)
    with tempfile.TemporaryDirectory(prefix="nabedra-cache-") as temporary:
        path = Path(temporary) / "cache-cloudformation-v1.json"
        path.write_text(json.dumps(template, separators=(",", ":")) + "\n")
        aws.run(
            "cloudformation", "deploy", "--template-file", str(path), "--stack-name", options.stack,
            "--s3-bucket", artifact["bucket"], "--s3-prefix", f"nabedra-cache/templates/{options.stack}",
            "--capabilities", "CAPABILITY_IAM", "--no-fail-on-empty-changeset",
            timeout=options.timeout + 900, output=False,
        )

    outputs = aws.stack(options.stack)
    readiness = json.loads(aws.reconcile(outputs["Controller1"], "apply", options.workers, prior_count, options.timeout))
    if not readiness["ready"]:
        raise ValueError("cache did not pass its private deployment readiness checks")
    controllers = [outputs[f"Controller{i}"] for i in range(1, 4)]
    workers = [outputs[f"Worker{i}"] for i in range(1, options.workers + 1)]
    return {
        "region": options.region, "stack": options.stack, "vpc": outputs["VpcId"],
        "seedEndpoints": outputs["PrivateSeeds"].split(","), "controllerInstances": controllers,
        "workerInstances": workers, "availabilityZones": network["zones"], "readiness": readiness,
    }


def render(options, network, artifact, replacements):
    template = cloudformation_v1.render(options, network, artifact, replacements)
    template["Outputs"]["DeploymentFingerprint"] = {"Value": fingerprint(options, network, artifact)}
    template["Outputs"]["WorkerReplacements"] = {"Value": json.dumps(replacements, sort_keys=True)}
    return template


def parse():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", required=True)
    parser.add_argument("--stack", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--ami", required=True)
    parser.add_argument("--root-device", default="/dev/xvda")
    parser.add_argument("--workers", type=int, choices=(3, 6, 12), default=3)
    parser.add_argument("--worker-type", default="r7g.large")
    parser.add_argument("--worker-memory-mib", type=int, default=1536)
    parser.add_argument("--controller-type", default="m7g.large")
    parser.add_argument("--controller-memory-mib", type=int, default=1024)
    parser.add_argument("--network", required=True, type=Path)
    parser.add_argument("--artifact", required=True, type=Path)
    parser.add_argument("--bootstrap-prefix", required=True)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--replace-failed-worker", type=int, choices=range(1, 13))
    parser.add_argument("--render", action="store_true", help="print the CloudFormation template without calling AWS")
    options = parser.parse_args()
    options.domain = dns_name(options.domain)
    options.bootstrap_prefix = secret_prefix(options.bootstrap_prefix)
    require(r"[a-z]+-[a-z0-9-]+-[0-9]+", options.region, "AWS region")
    require(r"[a-zA-Z][a-zA-Z0-9-]{0,127}", options.stack, "stack name")
    require(r"ami-[0-9a-f]{8,17}", options.ami, "AMI ID")
    require(r"/dev/(?:xvd|sd)[a-z][a-z0-9]*", options.root_device, "root device")
    for value in (options.worker_type, options.controller_type):
        require(r"[a-z][a-z0-9-]+\.[a-z0-9]+", value, "instance type")
    if not 60 <= options.timeout <= 3600:
        parser.error("timeout must be between 60 and 3600 seconds")
    if not (256 <= options.controller_memory_mib <= 1048576 and
            1024 <= options.worker_memory_mib <= 1048576):
        parser.error("invalid systemd memory ceilings")
    return options


def main():
    options = parse()
    network, artifact = load_network(options.network), load_artifact(options.artifact)
    if options.render:
        result = render(options, network, artifact, {})
    else:
        result = deploy(Aws(options.region), options, network, artifact)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (AwsError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as problem:
        print(f"Cache deployment: {problem}", file=sys.stderr)
        sys.exit(1)

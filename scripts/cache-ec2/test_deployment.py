"""Docker-runnable deployment checks without an AWS account."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import deploy
from growth import REQUIRED_EVIDENCE, apply, plan
from provider_acceptance import Acceptance
from reconcile import SLOTS, ready, ranges
from settings import load_network

SCRIPT = Path(__file__).resolve().parent
NETWORK = {
    "cidr": "10.72.0.0/16", "zones": ["eu-west-1a", "eu-west-1b", "eu-west-1c"],
    "subnetCidrs": ["10.72.1.0/24", "10.72.2.0/24", "10.72.3.0/24"],
    "applications": ["10.72.0.0/16"],
}
ARTIFACT = {"bucket": "fixture-cache-artifacts", "key": "cache/v1.tar.gz", "version": "fixed", "sha256": "a" * 64}


def options(workers):
    return SimpleNamespace(
        region="eu-west-1", stack="test-cache", domain="fixture.internal", ami="ami-0123456789abcdef0",
        root_device="/dev/xvda", worker_type="r7g.large", controller_type="m7g.large",
        worker_memory_mib=1536, controller_memory_mib=1024, workers=workers, timeout=60,
        bootstrap_prefix="fixture/cache", replace_failed_worker=None,
    )


def ready_layout(workers):
    nodes = {str(index): {"id": {"node": index, "incarnation": str(index)}, "zone": NETWORK["zones"][(index - 1) % 3]}
             for index in range(1, workers + 1)}
    slots = []
    for low, high, owner in ranges(workers):
        for _ in range(low, high + 1):
            slots.append({
                "primary": nodes[str(owner)]["id"],
                "replica": nodes[str(owner % workers + 1)]["id"],
                "moving": None,
            })
    return {"workers": nodes, "slots": slots, "ready_primaries": SLOTS, "ready_replicas": SLOTS}


class ProviderFixture:
    def __init__(self, count, attested):
        config = options(count)
        self.previous = {"Workers": str(count), "Controller1": "c1",
                         "DeploymentFingerprint": deploy.fingerprint(config, NETWORK, ARTIFACT)}
        self.next = None
        self.events = []
        self.attested = attested

    def stack(self, _):
        return self.next or self.previous

    def reconcile(self, node, action, workers, prior, timeout):
        self.events.append(action)
        return json.dumps({"ready": self.attested if action == "drain" else True,
                           "drained": list(range(workers + 1, prior + 1)) if self.attested else []})

    def run(self, *args, **kwargs):
        if args[:2] != ("cloudformation", "deploy"):
            raise AssertionError(args)
        self.events.append("apply-cloudformation")
        template = json.loads(Path(args[args.index("--template-file") + 1]).read_text())
        outputs = template["Outputs"]
        self.next = {key: value["Value"] for key, value in outputs.items()}
        self.next.update({"PrivateSeeds": "rediss://w1.fixture.internal:6379,rediss://w2.fixture.internal:6379"})


class Deployment(unittest.TestCase):
    def test_three_six_twelve_private_nodes_and_versioned_bootstrap(self):
        for workers in (3, 6, 12):
            template = deploy.render(options(workers), NETWORK, ARTIFACT, {})
            self.assertEqual(template["Metadata"]["NabedraCache"]["TemplateVersion"], 1)
            resources = template["Resources"]
            self.assertEqual(sum(row["Type"] == "AWS::EC2::Instance" for row in resources.values()), workers + 3)
            self.assertEqual(sum(row["Type"] == "AWS::EC2::Volume" for row in resources.values()), 3)
            self.assertEqual(sum(row["Type"] == "AWS::EC2::VPCEndpoint" for row in resources.values()), 4)
            self.assertFalse(any(row["Type"] in {"AWS::EC2::InternetGateway", "AWS::EC2::NatGateway"}
                                 for row in resources.values()))
            for i in range(1, 4):
                volume = resources[f"Controller{i}Metadata"]
                self.assertEqual(volume["DeletionPolicy"], "Retain")
                self.assertIs(volume["Properties"]["Encrypted"], True)
                self.assertEqual(volume["Properties"]["AvailabilityZone"], {"Ref": f"Zone{i}"})
                self.assertEqual(template["Parameters"][f"Zone{i}"]["Default"], NETWORK["zones"][i - 1])
            for i in range(1, workers + 1):
                launch = resources[f"Worker{i}Launch"]["Properties"]["LaunchTemplateData"]
                nic, = launch["NetworkInterfaces"]
                self.assertEqual(nic["SubnetId"], {"Ref": f"Subnet{(i - 1) % 3 + 1}"})
                self.assertIs(nic["AssociatePublicIpAddress"], False)
                self.assertEqual(launch["MetadataOptions"]["HttpTokens"], "required")
                self.assertIn("s3api get-object", launch["UserData"]["Fn::Base64"])
                self.assertIn("sha256sum --check", launch["UserData"]["Fn::Base64"])
                roles = resources[f"Worker{i}Role"]["Properties"]["Policies"]
                self.assertIn(f"/worker/{i}-??????", json.dumps(roles))
                self.assertNotIn("/operator", json.dumps(roles))
                subprocess.run(["bash", "-n"], input=launch["UserData"]["Fn::Base64"], text=True, check=True)

    def test_existing_vpc_template_creates_no_network_endpoints(self):
        network = {"vpc": "vpc-01234567", "subnets": ["subnet-01234567", "subnet-01234568", "subnet-01234569"],
                   "zones": NETWORK["zones"], "applications": ["sg-01234567"]}
        resources = deploy.render(options(3), network, ARTIFACT, {})["Resources"]
        self.assertEqual(resources["Application1"]["Properties"]["SourceSecurityGroupId"], "sg-01234567")
        self.assertEqual(resources["Worker2Launch"]["Properties"]["LaunchTemplateData"]["NetworkInterfaces"][0]["SubnetId"],
                         "subnet-01234568")
        self.assertFalse(any(row["Type"] in {"AWS::EC2::VPC", "AWS::EC2::Subnet", "AWS::EC2::VPCEndpoint"}
                             for row in resources.values()))

    def test_overlapping_subnets_and_public_access_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "network.json"
            for invalid in ({"subnetCidrs": ["10.72.1.0/24"] * 3}, {"applications": ["0.0.0.0/0"]},
                            {"zones": ["eu-west-1a"] * 3}):
                path.write_text(json.dumps({**NETWORK, **invalid}))
                with self.assertRaises(ValueError):
                    load_network(path)

    def test_readiness_requires_live_baselines_and_distinct_zones(self):
        healthy = ready_layout(3)
        self.assertTrue(ready(healthy, 3))
        healthy["ready_replicas"] -= 1
        self.assertFalse(ready(healthy, 3))
        healthy["ready_replicas"] += 1
        healthy["workers"]["2"]["zone"] = healthy["workers"]["1"]["zone"]
        self.assertFalse(ready(healthy, 3))

    def test_scale_down_calls_provider_only_after_attestation(self):
        with patch("deploy.preflight"):
            unsafe = ProviderFixture(6, attested=False)
            with self.assertRaisesRegex(ValueError, "attest safe termination"):
                deploy.deploy(unsafe, options(3), NETWORK, ARTIFACT)
            self.assertEqual(unsafe.events, ["drain"])

            safe = ProviderFixture(6, attested=True)
            self.assertTrue(deploy.deploy(safe, options(3), NETWORK, ARTIFACT)["readiness"]["ready"])
            self.assertEqual(safe.events, ["drain", "apply-cloudformation", "apply"])

    def test_immutable_bootstrap_change_avoids_instance_replacement(self):
        fixture = ProviderFixture(3, attested=True)
        changed = options(3)
        changed.ami = "ami-01234568"
        with self.assertRaisesRegex(ValueError, "immutable"):
            deploy.deploy(fixture, changed, NETWORK, ARTIFACT)
        self.assertEqual(fixture.events, [])

    def test_private_bootstrap_bundles_describe_same_tls_members(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            network = root / "network.json"
            network.write_text(json.dumps(NETWORK))
            out = root / "bootstrap"
            subprocess.run(
                [sys.executable, "-B", SCRIPT / "prepare.py", "credentials", "--output", out,
                 "--network", network, "--domain", "fixture.internal", "--bootstrap-prefix", "fixture/cache"],
                check=True, capture_output=True,
            )
            index = json.loads((out / "secret-index.json").read_text())
            self.assertEqual(len(index), 16)
            self.assertEqual(out.stat().st_mode & 0o777, 0o700)
            self.assertEqual((out / "ca.key").stat().st_mode & 0o777, 0o600)
            control = [tomllib.loads(json.loads((out / f"controller-{i}.json").read_text())["files"]["node.toml"])
                       for i in range(1, 4)]
            self.assertEqual(len({item["cluster_id"] for item in control}), 1)
            self.assertEqual(len({json.dumps(item["voters"], sort_keys=True) for item in control}), 1)
            operator_files = json.loads((out / "operator.json").read_text())["files"]
            self.assertNotIn("ca.key", operator_files)
            for i in range(1, 13):
                worker_files = json.loads((out / f"worker-{i}.json").read_text())["files"]
                self.assertEqual(set(worker_files), {"node.toml", "node.pem", "node.key", "ca.pem"})
                worker = tomllib.loads(worker_files["node.toml"])
                member = tomllib.loads(operator_files[f"member-{i}.toml"])
                self.assertEqual(worker["peer"], member["peer"])
                self.assertEqual(worker["zone"], member["zone"])
                self.assertEqual(worker["client"]["users"][0]["key_prefixes"], ["nb:catalog:"])
                probe = subprocess.run(
                    ["openssl", "x509", "-outform", "DER"], input=worker_files["node.pem"].encode(),
                    check=True, capture_output=True,
                )
                pin = hashlib.sha256(probe.stdout).digest()
                self.assertEqual(worker["peer"]["fingerprints"], [list(pin)])

    def test_artifact_contains_no_bootstrap_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "binary"
            binary.write_text("fixture binary")
            artifact = root / "cache.tar.gz"
            result = subprocess.run(
                [sys.executable, "-B", SCRIPT / "prepare.py", "artifact", "--binary", binary,
                 "--clockbound-daemon", binary, "--output", artifact],
                text=True, check=True, capture_output=True,
            )
            self.assertEqual(result.stdout.strip(), hashlib.sha256(artifact.read_bytes()).hexdigest())
            with tarfile.open(artifact) as archive:
                self.assertEqual(set(archive.getnames()), {
                    "nabedra-db", "clockbound", "bootstrap.sh", "reconcile.py", "observations.py",
                })


class Growth(unittest.TestCase):
    def fixture(self):
        policy = {
            "provider_configured": True, "qualification": {case: "passed" for case in REQUIRED_EVIDENCE},
            "stack": "eu-west-1/test-cache",
            "sustained_seconds": 30, "observation_max_age_seconds": 60, "max_workers": 12,
            "max_attempts": 3, "cooldown_seconds": 10, "hot_slot_share": 0.1, "distributed_slot_ratio": 0.4,
            "memory_ratio": 0.8, "oldest_queued_ms": 20, "utilization_ratio": 0.85,
            "monthly_worker_cost_units": 100, "monthly_spend_ceiling_units": 1000,
        }
        observations = [{
            "observed_at": stamp, "groups": 3, "attempts": 0, "last_attempt": 0, "provisioning": False,
            "moves": 0, "zone_reserve_ok": True, "memory_ratio": 0.9, "oldest_queued_ms": 50,
            "slots_under_pressure_ratio": 0.6, "max_cpu_ratio": 0.7, "max_network_ratio": 0.7,
            "hottest_slot_write_share": 0.01, "monthly_spend_units": 500,
        } for stamp in (60, 75, 90)]
        return policy, observations

    def test_fixed_unsupported_and_qualified_growth_respects_spend(self):
        self.assertEqual(plan({}, [], 90), {"action": "refused", "reason": "unsupported_provider"})
        policy, observations = self.fixture()
        self.assertEqual(plan(policy, observations, 90)["target_workers"], 6)
        policy["monthly_spend_ceiling_units"] = 799
        self.assertEqual(plan(policy, observations, 90)["reason"], "spend_limit")
        policy["qualification"]["K04b-6"] = "unexecuted"
        self.assertEqual(plan(policy, observations, 90)["reason"], "provider_qualification")

    def test_reconcile_lost_provider_reply_without_duplicate_provision(self):
        class Fixture:
            count = 3
            status = "CREATE_COMPLETE"

            def run(self, *args):
                return {"Stacks": [{
                    "StackStatus": self.status,
                    "Outputs": [
                        {"OutputKey": "Workers", "OutputValue": str(self.count)},
                        {"OutputKey": "Controller1", "OutputValue": "fixture-c1"},
                    ],
                }]}

            def reconcile(self, *args):
                return json.dumps({"ready": True, "groups": {str(node): 1 for node in range(1, self.count + 1)}})

        policy, observations = self.fixture()
        fixture = Fixture()
        launches = []
        def lost(arguments, target):
            launches.append(target)
            fixture.status = "UPDATE_IN_PROGRESS"
            raise subprocess.TimeoutExpired(arguments, 1)

        arguments = ["--region", "eu-west-1", "--stack", "test-cache"]
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "state.json"
            def timer(now):
                return apply(policy, observations, now, arguments, path, provider=lambda region: fixture, invoke=lost)
            self.assertEqual(timer(90)["reason"], "provisioning_outcome_unknown")
            self.assertEqual(json.loads(path.read_text())["pending"]["target_workers"], 6)
            self.assertEqual(timer(100)["reason"], "provider_operation_in_progress")
            fixture.status, fixture.count = "UPDATE_COMPLETE", 6
            self.assertEqual(timer(101)["action"], "applied")
            self.assertEqual(launches, [6])
            stored = json.loads(path.read_text())
            self.assertIsNone(stored["pending"])
            self.assertEqual(stored["attempts"], 0)

    def test_growth_rejects_duplicate_targets_and_nonfinite_spend(self):
        policy, observations = self.fixture()
        observations[-1]["monthly_spend_units"] = float("nan")
        with self.assertRaises(ValueError):
            plan(policy, observations, 90)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "state.json"
            values = ["--region", "eu-west-1", "--stack", "test-cache", "--stack", "different"]
            with self.assertRaisesRegex(ValueError, "duplicate"):
                apply(policy, observations, 90, values, path, provider=lambda _: self.fail("provider called"))


class ProviderScope(unittest.TestCase):
    def test_fault_fixture_refuses_an_instance_outside_the_stack(self):
        fixture = Acceptance.__new__(Acceptance)
        fixture.live_stack = lambda: {
            "Workers": "3", **{f"Worker{i}": f"i-w{i}" for i in range(1, 4)},
            **{f"Controller{i}": f"i-c{i}" for i in range(1, 4)},
        }
        fixture.aws = SimpleNamespace(run=lambda *args: self.fail("foreign provider action"))
        with self.assertRaisesRegex(RuntimeError, "outside the live"):
            fixture.mutate_instances("terminate", ["i-foreign"])

    def test_stack_waits_for_provider_update_before_using_outputs(self):
        client = deploy.Aws("eu-west-1")
        client.run = unittest.mock.Mock(side_effect=[
            {"Stacks": [{"StackStatus": "UPDATE_IN_PROGRESS"}]},
            {"Stacks": [{"StackStatus": "UPDATE_COMPLETE", "Outputs": [
                {"OutputKey": "Workers", "OutputValue": "3"},
            ]}]},
        ])
        with patch.object(deploy.time, "sleep"):
            self.assertEqual(client.stack("test-cache"), {"Workers": "3"})


if __name__ == "__main__":
    unittest.main()

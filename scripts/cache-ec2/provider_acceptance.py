#!/usr/bin/env python3
"""Exercise K05 provider histories on a disposable private Cache stack."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import shutil
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlparse

from deploy import Aws, AwsError
from reconcile import SLOTS
from settings import deployment_arguments, read_json

SCRIPT = Path(__file__).resolve().parent
CONTRACT = SCRIPT.parent.parent / "orm/cache-contract"
SCENARIOS = ("deploy", "idempotent", "rolling", "grow-drain", "az-loss",
             "cold", "forced-termination", "provision-failure", "rotation")


def call(*arguments, timeout=60, env=None):
    return subprocess.run(
        [*map(str, arguments)], capture_output=True, text=True,
        timeout=timeout, check=True, env=env,
    ).stdout


def require(condition, explanation):
    if not condition:
        raise RuntimeError(explanation)


class Acceptance:
    def __init__(self, options, work):
        self.options = options
        self.work = work
        self.args = read_json(options.deploy_args)
        self.deploy_options = deployment_arguments(self.args, allow_workers=True)
        self.client = read_json(options.client)
        if self.client["namespace"] != "catalog":
            raise ValueError("the K05 shared ORM vectors use the catalog namespace")
        self.client["caFile"] = str(options.client.resolve().parent / self.client["ca_file"])
        self.aws = Aws(self.argument("--region"))
        self.deploy_timeout = 7200
        self.results = []

    def argument(self, name):
        return self.deploy_options[name]

    def deploy(self, workers=3, extra=(), args=None):
        arguments = list(self.args if args is None else args)
        options = deployment_arguments(arguments, allow_workers=True)
        for field in ("--region", "--stack"):
            require(options[field] == self.deploy_options[field], "fixture names another region or stack")
        if "--workers" in arguments:
            index = arguments.index("--workers")
            arguments[index:index + 2] = []
        stdout = call(
            sys.executable, SCRIPT / "deploy.py", *arguments, "--workers", workers, *extra,
            timeout=self.deploy_timeout,
        )
        result = json.loads(stdout)
        self.verify_state(result)
        return result

    def live_stack(self):
        outputs = self.aws.stack(self.argument("--stack"))
        require(outputs is not None and "Workers" in outputs, "acceptance stack is unavailable")
        return outputs

    def verify_state(self, state):
        outputs = self.live_stack()
        controllers = [outputs[f"Controller{i}"] for i in range(1, 4)]
        workers = [outputs[f"Worker{i}"] for i in range(1, int(outputs["Workers"]) + 1)]
        require(state["controllerInstances"] == controllers and state["workerInstances"] == workers
                and state["seedEndpoints"] == outputs["PrivateSeeds"].split(","),
                "acceptance state does not match the live stack")

    def prepare_packages(self):
        for file in ("python_driver.py", "typescript_driver.mjs", "vectors.json"):
            shutil.copy(CONTRACT / file, self.work)
        for file in ("traffic.py", "traffic.mjs"):
            shutil.copy(SCRIPT / file, self.work)
        wheel, = self.options.packages.glob("nabedra-*.whl")
        package, = self.options.packages.glob("nabedra-*.tgz")
        self.python_lib = self.work / "python"
        call(sys.executable, "-m", "pip", "install", "--no-index", "--no-deps", "--target",
             self.python_lib, wheel, timeout=180)
        call("npm", "install", "--prefix", self.work, "--no-audit", "--no-fund",
             package, timeout=180)

    def smoke(self, state):
        env = {
            **os.environ, "PYTHONNOUSERSITE": "1", "PYTHONPATH": str(self.python_lib),
            "NABEDRA_CACHE_TEST_CONFIG": json.dumps({
                "endpoint": state["seedEndpoints"], "cluster": True,
                "username": self.client["username"], "password": self.client["password"],
                "caFile": self.client["caFile"], "allowedNodes": self.client["allowed_nodes"],
            }),
        }
        return {
            "python": call(sys.executable, self.work / "python_driver.py", env=env, timeout=60).strip(),
            "typescript": call("node", self.work / "typescript_driver.mjs", env=env, timeout=60).strip(),
        }


    @contextmanager
    def replay(self, scenario, state):
        stop = self.work / f"{scenario}-replay.stop"
        evidence = self.options.output.parent / f"{self.options.output.stem}-raw"
        evidence.mkdir(mode=0o700, exist_ok=True)
        environment = {
            **os.environ, "PYTHONPATH": str(self.python_lib), "PYTHONNOUSERSITE": "1",
            "NABEDRA_CACHE_REPLAY": json.dumps({
                **self.client, "seeds": state["seedEndpoints"], "limitSeconds": 14400, "stop": str(stop),
            }),
        }
        processes = []
        result = {}
        try:
            for language, binary, file in (
                ("python", sys.executable, "traffic.py"), ("typescript", "node", "traffic.mjs"),
            ):
                raw = evidence / f"{scenario}-{language}.jsonl"
                with raw.open("x") as output:
                    process = subprocess.Popen([binary, self.work / file], stdout=output, env=environment)
                processes.append((language, process, raw))
            expires = time.monotonic() + 15
            while any(raw.stat().st_size == 0 for _, _, raw in processes):
                if time.monotonic() >= expires:
                    raise TimeoutError("both installed ORM replay processes must emit samples")
                time.sleep(0.05)
            yield result
        finally:
            stop.touch()
            for language, process, raw in processes:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                with raw.open() as samples:
                    outcomes = Counter(json.loads(line)["outcome"] for line in samples)
                result[language] = {"outcomes": dict(outcomes), "raw": raw.name, "exit": process.returncode}
        for language, observations in result.items():
            outcomes = observations["outcomes"]
            require(observations["exit"] == 0 and outcomes.get("ok", 0) > 0,
                    f"{language} replay had no successful operations")
            require(not ({"driver_error", "incorrect_reply"} & outcomes.keys()),
                    f"{language} replay found incorrect data or a driver failure")

    def status(self, state):
        return json.loads(self.aws.reconcile(
            state["controllerInstances"][0], "status", len(state["workerInstances"]), 3, 30,
        ))

    def wait(self, state, predicate, timeout=360):
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            try:
                last = self.status(state)
            except AwsError:
                continue
            if predicate(last):
                return last
            time.sleep(2)
        raise TimeoutError(f"controller did not converge: {last}")

    def coverage(self, state):
        ready = self.wait(
            state, lambda status: status["readyReplicas"] == SLOTS and status["readyPrimaries"] == SLOTS,
        )
        return {"readyPrimaries": ready["readyPrimaries"], "readyReplicas": ready["readyReplicas"]}

    def service(self, instances, action):
        self.verify_instances(instances)
        with ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(lambda node: self.aws.ssm(node, [f"systemctl {action} nabedra-cache.service"], 30), instances))

    def mutate_instances(self, action, instance_ids):
        self.verify_instances(instance_ids)
        self.aws.run("ec2", f"{action}-instances", "--instance-ids", *instance_ids)
        waiter = {"stop": "stopped", "start": "running", "terminate": "terminated"}[action]
        self.aws.run("ec2", "wait", f"instance-{waiter}", "--instance-ids", *instance_ids, timeout=900)

    def verify_instances(self, instances):
        outputs = self.live_stack()
        allowed = {outputs[f"Controller{i}"] for i in range(1, 4)}
        allowed.update(outputs[f"Worker{i}"] for i in range(1, int(outputs["Workers"]) + 1))
        require(bool(instances) and set(instances) <= allowed, "instance is outside the live acceptance stack")

    def cache_command(self, state, command):
        driver = """
import json, os
from nabedra.cache import Cache
request = json.loads(os.environ["CACHE_PROVIDER_TEST"])
with Cache.connect(request["seeds"], cluster=True, namespace=request["namespace"],
                   username=request["username"], password=request["password"],
                   ca_file=request["caFile"], allowed_nodes=request["allowed_nodes"]) as cache:
    if request["command"] == "write":
        assert cache.set("provider-survivor", bytes.fromhex("00ff0100"), ttl_ms=1800000)
        assert cache.set("provider-expires", b"x", ttl_ms=200)
    else:
        survivor = cache.get("provider-survivor")
        print(json.dumps({"survivor": survivor.hex() if survivor is not None else None,
                          "expired": cache.get("provider-expires") is None, "ttl": cache.ttl_ms("provider-survivor")}))
"""
        response = call(sys.executable, "-c", driver, env={
            **os.environ, "PYTHONPATH": str(self.python_lib), "PYTHONNOUSERSITE": "1",
            "CACHE_PROVIDER_TEST": json.dumps({
                **self.client, "seeds": state["seedEndpoints"], "command": command,
            }),
        })
        return json.loads(response) if response.strip() else None

    def run(self, scenario, state):
        if scenario == "deploy":
            require(self.aws.stack(self.argument("--stack")) is None, "clean deployment requires a new stack name")
            state = self.deploy()
            smoke = self.smoke(state)
            return state, {"readiness": state["readiness"], "installedOrm": smoke}
        if state is None:
            raise ValueError("run the clean deploy scenario first or provide an existing acceptance state")
        self.verify_state(state)
        if scenario == "idempotent":
            self.cache_command(state, "write")
            next_state = self.deploy()
            require(next_state["workerInstances"] == state["workerInstances"] and
                    next_state["controllerInstances"] == state["controllerInstances"],
                    "repeat deploy replaced running instances")
            require(next_state["readiness"]["generation"] == state["readiness"]["generation"],
                    "repeat deploy replaced the live generation")
            require(self.cache_command(next_state, "read")["survivor"] == "00ff0100",
                    "repeat deploy discarded a live key")
            return next_state, {"instancesConverged": True}
        if scenario == "rolling":
            original = self.status(state)
            times = []
            for instance in state["controllerInstances"] + state["workerInstances"]:
                start = time.monotonic()
                self.service([instance], "restart")
                self.coverage(state)
                times.append({"instance": instance, "coverageSeconds": time.monotonic() - start})
            status = self.status(state)
            require(status["generation"] == original["generation"], "rolling restart changed generation")
            require(all(status["incarnations"][node] != boot for node, boot in original["incarnations"].items()),
                    "restarted workers did not change incarnation")
            return self.deploy(), {"restarts": times}
        if scenario == "grow-drain":
            sizes = []
            for workers in (6, 12, 6, 3):
                started = time.monotonic()
                state = self.deploy(workers)
                readiness = state["readiness"]
                require(len(readiness["groups"]) == workers, "wrong primary group count")
                sizes.append({"groups": workers, "seconds": time.monotonic() - started, "readiness": readiness})
            return state, {"sizeChanges": sizes}
        if scenario == "az-loss":
            failed = state["controllerInstances"][2:3] + state["workerInstances"][2::3]
            try:
                self.mutate_instances("stop", failed)
                observed = self.wait(state, lambda status: status["readyPrimaries"] == SLOTS)
                self.smoke(state)
            finally:
                self.mutate_instances("start", failed)
            restored = self.coverage(state)
            return self.deploy(), {"stopped": failed, "during": observed, "restored": restored}
        if scenario == "cold":
            self.cache_command(state, "write")
            previous = self.status(state)
            self.service(state["workerInstances"], "stop")
            try:
                self.wait(state, lambda status: status["readyPrimaries"] == 0 and status["readyReplicas"] == 0)
            finally:
                self.service(state["workerInstances"], "start")
            self.wait(state, lambda status: all(status["incarnations"][node] != incarnation
                                                for node, incarnation in previous["incarnations"].items()))
            command = ("/usr/local/bin/nabedra-db cache admin --config /etc/nabedra-cache/operator/node.toml "
                       "cold-reset --confirm-data-loss")
            self.verify_instances(state["controllerInstances"][:1])
            self.aws.ssm(state["controllerInstances"][0], [command], 30)
            self.coverage(state)
            after = self.cache_command(state, "read")
            require(after["survivor"] is None and after["expired"] and after["ttl"] == -2,
                    "cold reset retained old worker data")
            state = self.deploy()
            require(state["readiness"]["generation"] == previous["generation"] + 1,
                    "cold reset did not advance exactly one generation")
            return state, {"oldValuesAbsent": True, "generation": state["readiness"]["generation"]}
        if scenario == "forced-termination":
            victim = state["workerInstances"][-1]
            workers = len(state["workerInstances"])
            self.mutate_instances("terminate", [victim])
            self.wait(state, lambda status: status["readyPrimaries"] == SLOTS)
            next_state = self.deploy(workers, extra=("--replace-failed-worker", str(workers)))
            require(next_state["workerInstances"][-1] != victim, "forced termination did not replace instance")
            return next_state, {"oldInstance": victim, "replacement": next_state["workerInstances"][-1]}
        if scenario == "provision-failure":
            return state, self.provision_failure(state)
        if scenario == "rotation":
            return state, self.rotation(state)
        raise ValueError(f"unrecognized scenario: {scenario}")

    def provision_failure(self, state):
        if self.options.failure_args is None:
            raise ValueError("provide --failure-args for a real rollback fixture that denies worker creation")
        previous = self.aws.run("cloudformation", "describe-stack-events", "--stack-name", state["stack"])
        try:
            self.deploy(6, args=read_json(self.options.failure_args))
        except subprocess.CalledProcessError:
            pass
        else:
            raise RuntimeError("provisioning failure fixture unexpectedly succeeded")
        events = self.aws.run("cloudformation", "describe-stack-events", "--stack-name", state["stack"])
        seen = {event["EventId"] for event in previous["StackEvents"]}
        failures = [event["LogicalResourceId"] for event in events["StackEvents"]
                    if event["EventId"] not in seen and event["ResourceStatus"] in {"CREATE_FAILED", "UPDATE_FAILED"}]
        require(any(resource.startswith(("Worker4", "Worker5", "Worker6")) for resource in failures),
                "the fixture did not reach a provider worker provisioning failure")
        self.coverage(state)
        self.smoke(state)
        return {"failedResources": failures, "originalWorkers": state["workerInstances"]}

    def rotation(self, state):
        if self.options.rotation_fixture is None:
            raise ValueError("provide --rotation-fixture with staged and final operator commands")
        fixture = read_json(self.options.rotation_fixture)
        require(set(fixture) == {"node", "previous", "replacement", "stage", "finish"},
                "rotation fixture needs node, certificate fingerprints and staged commands")
        node = int(fixture["node"])
        endpoint = urlparse(self.client["allowed_nodes"][node - 1])
        old, new = fixture["previous"], fixture["replacement"]
        require(old != new, "rotation requires distinct certificate fingerprints")
        context = ssl.create_default_context(cafile=self.client["caFile"])

        def client_leaf():
            with socket.create_connection((endpoint.hostname, endpoint.port), timeout=3) as connection:
                with context.wrap_socket(connection, server_hostname=endpoint.hostname) as peer:
                    return hashlib.sha256(peer.getpeercert(binary_form=True)).hexdigest()

        require(client_leaf() == old, "fixture does not describe current leaf certificate")
        results = []
        for phase, expected in (("stage", {old, new}), ("finish", {new})):
            commands = fixture[phase]
            require(isinstance(commands, list) and bool(commands), "rotation phase needs nonempty commands")
            self.verify_instances(state["controllerInstances"][:1])
            self.aws.ssm(state["controllerInstances"][0], commands, 120)
            self.coverage(state)
            observed = self.status(state)["fingerprints"][str(node)]
            require(set(observed) == expected, f"{phase} controller trust keys do not match the fixture")
            leaf = client_leaf()
            require(leaf in expected and (phase != "finish" or leaf == new), "unexpected active certificate")
            self.smoke(state)
            results.append({"phase": phase, "activeLeaf": leaf, "trustedPins": observed})
        return {"phases": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deploy-args", required=True, type=Path)
    parser.add_argument("--client", required=True, type=Path)
    parser.add_argument("--packages", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--failure-args", type=Path)
    parser.add_argument("--rotation-fixture", type=Path)
    parser.add_argument("--scenarios", default=",".join(SCENARIOS))
    parser.add_argument("--disposable-stack", action="store_true", required=True)
    options = parser.parse_args()
    scenarios = options.scenarios.split(",")
    if not set(scenarios) <= set(SCENARIOS) or len(scenarios) != len(set(scenarios)):
        parser.error("choose distinct recognized provider scenarios")
    state = read_json(options.state) if options.state else None
    output = {"contract": ["K05", "A01", "A02", "B01", "B02"], "scenarios": {}}
    with tempfile.TemporaryDirectory(prefix="nabedra-cache-provider-") as directory:
        acceptance = Acceptance(options, Path(directory))
        acceptance.prepare_packages()
        for scenario in SCENARIOS:
            if scenario not in scenarios:
                output["scenarios"][scenario] = {"status": "unexecuted"}
                continue
            start = time.monotonic()
            try:
                if state is not None and scenario in {"rolling", "grow-drain", "az-loss", "forced-termination"}:
                    with acceptance.replay(scenario, state) as replay:
                        state, observations = acceptance.run(scenario, state)
                    observations["installedOrmReplay"] = replay
                else:
                    state, observations = acceptance.run(scenario, state)
            except Exception as error:
                output["scenarios"][scenario] = {
                    "status": "failed", "seconds": time.monotonic() - start, "error": str(error),
                }
                break
            else:
                output["scenarios"][scenario] = {
                    "status": "passed", "seconds": time.monotonic() - start, **observations,
                }
        for scenario in SCENARIOS:
            output["scenarios"].setdefault(scenario, {"status": "unexecuted"})
    output["deployment"] = state
    output["all_runner_scenarios_passed"] = all(
        item["status"] == "passed" for item in output["scenarios"].values()
    )
    options.output.write_text(json.dumps(output, indent=2) + "\n")
    if any(item["status"] == "failed" for item in output["scenarios"].values()):
        sys.exit(1)


if __name__ == "__main__":
    main()

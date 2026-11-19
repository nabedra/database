#!/usr/bin/env python3
"""Plan bounded automatic additions against recent Cache capacity observations."""
import argparse
from copy import deepcopy
import fcntl
import json
import math
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from deploy import Aws, AwsError
from settings import deployment_arguments, read_json

SIZES = (3, 6, 12)
REQUIRED_EVIDENCE = ("K04b-3", "K04b-6", "K04b-12", "K05", "A01", "A02")


def refusal(policy):
    if not policy.get("provider_configured", False):
        return {"action": "refused", "reason": "unsupported_provider"}
    if any(policy["qualification"].get(case) != "passed" for case in REQUIRED_EVIDENCE):
        return {"action": "refused", "reason": "provider_qualification"}
    return None


def plan(policy, observations, now):
    """Choose one plan. This function does not publish or remove capacity."""
    if blocked := refusal(policy):
        return blocked
    if (policy["max_workers"] not in SIZES or policy["max_attempts"] < 1
            or not math.isfinite(policy["max_attempts"])
            or any(not math.isfinite(policy[name]) or policy[name] <= 0 for name in (
                "monthly_worker_cost_units", "monthly_spend_ceiling_units", "cooldown_seconds",
                "observation_max_age_seconds", "sustained_seconds",
            ))):
        raise ValueError("invalid provisioning bounds")
    if len(observations) < 3:
        return {"action": "hold", "reason": "insufficient_observations"}
    recent = observations[-3:]
    timestamps = [point["observed_at"] for point in recent]
    window = timestamps[-1] - timestamps[0]
    if (
        sorted(set(timestamps)) != timestamps or timestamps[-1] > now
        or window < policy["sustained_seconds"]
        or now - timestamps[0] > policy["observation_max_age_seconds"]
    ):
        return {"action": "hold", "reason": "stale_or_short_observation_window"}
    latest = recent[-1]
    groups = latest["groups"]
    if groups not in SIZES or any(point["groups"] != groups for point in recent):
        return {"action": "hold", "reason": "fleet_changing"}
    if any(not math.isfinite(latest[name]) or latest[name] < 0
           for name in ("attempts", "monthly_spend_units", "last_attempt")):
        raise ValueError("retry and spend observations must be nonnegative")
    if latest["attempts"] >= policy["max_attempts"]:
        return {"action": "refused", "reason": "retry_limit"}
    if now - latest["last_attempt"] < policy["cooldown_seconds"]:
        return {"action": "hold", "reason": "cooldown"}
    if latest["provisioning"] or latest["moves"] or not latest["zone_reserve_ok"]:
        return {"action": "hold", "reason": "placement_unavailable"}
    if any(
        not math.isfinite(point[field]) or point[field] < 0
        for point in recent for field in ("memory_ratio", "oldest_queued_ms", "slots_under_pressure_ratio",
                                           "hottest_slot_write_share", "max_cpu_ratio", "max_network_ratio")
    ):
        raise ValueError("resource measurements must be finite and nonnegative")
    if any(
        point["hottest_slot_write_share"] >= policy["hot_slot_share"]
        or point["slots_under_pressure_ratio"] < policy["distributed_slot_ratio"]
        for point in recent
    ):
        return {"action": "hold", "reason": "unqualified_load_distribution"}
    if not all(
        (point["memory_ratio"] >= policy["memory_ratio"] or
         point["oldest_queued_ms"] >= policy["oldest_queued_ms"])
        and (point["memory_ratio"] >= policy["memory_ratio"] or
             max(point["max_cpu_ratio"], point["max_network_ratio"]) >= policy["utilization_ratio"])
        for point in recent
    ):
        return {"action": "hold", "reason": "no_sustained_pressure"}
    if groups == SIZES[-1] or 2 * groups > policy["max_workers"]:
        return {"action": "refused", "reason": "worker_limit"}
    target = 2 * groups
    additional = (target - groups) * policy["monthly_worker_cost_units"]
    if additional + latest["monthly_spend_units"] > policy["monthly_spend_ceiling_units"]:
        return {"action": "refused", "reason": "spend_limit"}
    return {
        "action": "add_workers",
        "current_workers": groups,
        "target_workers": target,
        "additional_monthly_cost_units": additional,
        "observed_at": latest["observed_at"],
    }


def save(path, state):
    descriptor, temporary = tempfile.mkstemp(prefix=".cache-growth-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as data:
            json.dump(state, data)
            data.flush()
            os.fsync(data.fileno())
        os.replace(temporary, path)
        parent = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def run_deployment(arguments, target):
    script = Path(__file__).resolve().with_name("deploy.py")
    child = subprocess.run(
        [sys.executable, script, *arguments, "--workers", str(target)],
        check=True, capture_output=True, text=True, timeout=7200,
    )
    return json.loads(child.stdout)


def apply(policy, observations, now, arguments, path, *, provider=Aws, invoke=run_deployment):
    """Reconcile at most one automatic addition for an explicitly qualified stack."""
    if blocked := refusal(policy):
        return blocked
    options = deployment_arguments(arguments)
    region, stack = options["--region"], options["--stack"]
    scope = f"{region}/{stack}"
    if policy.get("stack") != scope:
        raise ValueError("growth policy must authorize the exact region and stack")
    lock_path = path.with_name(path.name + ".lock")
    lock = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"action": "hold", "reason": "concurrent_growth"}
        state = read_json(path) if path.exists() else {
            "scope": scope, "attempts": 0, "last_attempt": 0, "pending": None,
        }
        if state["scope"] != scope:
            raise ValueError("growth state belongs to another stack")
        result = reconcile(policy, observations, now, arguments, path, state, provider(region), stack, invoke)
        return result
    finally:
        os.close(lock)


def reconcile(policy, observations, now, arguments, path, state, aws, stack, invoke):
    pending = state["pending"]
    recent = deepcopy(observations)
    if recent:
        latest = recent[-1]
        latest["attempts"] = max(state["attempts"], latest["attempts"])
        latest["last_attempt"] = max(state["last_attempt"], latest["last_attempt"])
    if pending is None:
        candidate = plan(policy, recent, now)
        if candidate["action"] != "add_workers":
            return candidate
        prior, target = candidate["current_workers"], candidate["target_workers"]
    else:
        prior, target = pending["current_workers"], pending["target_workers"]

    current, = aws.run("cloudformation", "describe-stacks", "--stack-name", stack)["Stacks"]
    stack_id = current.get("StackId")
    if state.get("stack_id") not in (None, stack_id):
        raise ValueError("growth state belongs to a retired stack")
    if current["StackStatus"].endswith("_IN_PROGRESS"):
        return {"action": "hold", "reason": "provider_operation_in_progress"}
    if current["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"}:
        return {"action": "hold", "reason": "provider_stack_unavailable"}
    outputs = {item["OutputKey"]: item["OutputValue"] for item in current["Outputs"]}
    current_count = int(outputs["Workers"])
    if current_count not in ({prior, target} if pending is not None else {prior}):
        return {"action": "hold", "reason": "fleet_changed"}
    status = json.loads(aws.reconcile(outputs["Controller1"], "status", current_count, prior, 45))
    if pending is not None:
        if current_count == target and status["ready"] and len(status["groups"]) == target:
            return complete(path, state, target, status)
        if prior not in SIZES or target != prior * 2 or target not in SIZES or target > policy["max_workers"]:
            raise ValueError("pending growth no longer satisfies worker limits")
        if state["attempts"] >= policy["max_attempts"]:
            return {"action": "refused", "reason": "retry_limit", "pending": pending}
        if now - state["last_attempt"] < policy["cooldown_seconds"]:
            return {"action": "hold", "reason": "cooldown", "pending": pending}
    if current_count == prior:
        if not status["ready"] or len(status["groups"]) != prior:
            return {"action": "hold", "reason": "controller_not_ready"}
        scope = {"stack_id": stack_id, "generation": status.get("generation"), "revision": status.get("revision")}
        if any(scope.keys() & point.keys() for point in recent[-3:]) and any(
            any(value is None or point.get(key) != value for key, value in scope.items())
            for point in recent[-3:]
        ):
            return {"action": "hold", "reason": "observation_scope_changed"}
        # A restart can observe a rolled-back provisioning request. A fresh
        # qualifying observation is required before spending on that retry.
        if pending is not None:
            candidate = plan(policy, recent, now)
            if candidate["action"] != "add_workers" or candidate["target_workers"] != target:
                return {"action": "hold", "reason": "retry_requires_fresh_plan", "pending": pending}

    state["pending"] = {"current_workers": prior, "target_workers": target}
    state["stack_id"] = stack_id
    state["attempts"] += 1
    state["last_attempt"] = now
    save(path, state)
    try:
        response = invoke(arguments, target)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError, ValueError):
        # The next pass observes the actual stack before reissuing this count.
        return {"action": "hold", "reason": "provisioning_outcome_unknown", "pending": state["pending"]}
    readiness = response.get("readiness", {})
    if not readiness.get("ready") or len(response.get("workerInstances", [])) != target:
        return {"action": "hold", "reason": "readiness_unverified", "pending": state["pending"]}
    return complete(path, state, target, readiness)


def complete(path, state, target, readiness):
    state["pending"] = None
    state["attempts"] = 0
    save(path, state)
    return {"action": "applied", "target_workers": target, "readiness": readiness}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", required=True, type=Path)
    parser.add_argument("--observations", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--deploy-args", type=Path)
    parser.add_argument("--state-file", type=Path)
    args = parser.parse_args()
    if args.apply and (args.deploy_args is None or args.state_file is None):
        parser.error("--apply requires --deploy-args and --state-file")
    try:
        policy, observations, now = read_json(args.policy), read_json(args.observations), int(time.time())
        if args.apply:
            output = apply(policy, observations, now, read_json(args.deploy_args), args.state_file)
        else:
            output = plan(policy, observations, now)
    except (ValueError, KeyError, TypeError, OSError, AwsError, subprocess.TimeoutExpired) as error:
        parser.error(f"invalid growth input: {error}")
    print(json.dumps(output, indent=2))
    if output["action"] == "refused":
        sys.exit(2)


if __name__ == "__main__":
    main()

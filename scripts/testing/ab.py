#!/usr/bin/env python3
"""Run a paired A/B benchmark and report a bootstrap confidence interval."""

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import platform
import random
import re
import shlex
from statistics import median, quantiles
import subprocess
import sys
import tempfile
import time
import tomllib


ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SEED = 20261122


@dataclass(frozen=True)
class Spec:
    name: str
    module: str
    command: list[str]
    warmup: int
    rounds: int
    metric: str
    threshold_pct: float = 5
    bootstrap_samples: int = 2000
    metric_file: str | None = None


def load_spec(path):
    with path.open("rb") as source:
        values = tomllib.load(source)
    if isinstance(values.get("command"), str):
        values["command"] = shlex.split(values["command"])
    spec = Spec(**values)
    for key in ("name", "module", "metric"):
        value = getattr(spec, key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{key} must be a non-empty string")
    if (not isinstance(spec.command, list)
            or not all(isinstance(arg, str) for arg in spec.command)
            or not any("{bin}" in arg for arg in spec.command)):
        raise ValueError("command must contain {bin}")
    for key, minimum in (("warmup", 0), ("rounds", 2), ("bootstrap_samples", 2)):
        if type(getattr(spec, key)) is not int or getattr(spec, key) < minimum:
            raise ValueError(f"{key} must be an integer of at least {minimum}")
    if (type(spec.threshold_pct) not in {int, float} or not math.isfinite(spec.threshold_pct)
            or spec.threshold_pct < 0):
        raise ValueError("threshold_pct must be a nonnegative finite number")
    if spec.metric != "wall" and re.compile(spec.metric).groups != 1:
        raise ValueError("metric regex must have exactly one capturing group")
    if spec.metric_file is not None:
        path = Path(spec.metric_file)
        if spec.metric == "wall" or path.is_absolute() or ".." in path.parts:
            raise ValueError("metric_file needs a regex and a path relative to the scratch directory")
    return spec


def invocation(template, binary, work, side):
    command = []
    for arg in template:
        if arg == "{bin}":
            command.extend(binary)
            continue
        arg = arg.replace("{work}", str(work)).replace("{side}", side)
        if "{bin}" in arg:
            if len(binary) != 1:
                raise ValueError("use {bin} as a separate argument to pass a multi-argument command")
            arg = arg.replace("{bin}", binary[0])
        command.append(arg)
    return command


def metric_value(pattern, output):
    matches = list(pattern.finditer(output))
    if not matches:
        raise ValueError("metric regex did not match benchmark output")
    values = [float(match[1]) for match in matches]
    if not all(math.isfinite(value) and value > 0 for value in values):
        raise ValueError("benchmark metrics must be positive and finite")
    return median(values)


def measure(spec, pattern, binary, side):
    with tempfile.TemporaryDirectory(prefix=f"nabedra-ab-{side}-") as directory:
        work = Path(directory)
        command = invocation(spec.command, binary, work, side)
        started = time.perf_counter()
        process = subprocess.run(command, cwd=ROOT, text=True, errors="replace",
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        elapsed = time.perf_counter() - started
        if process.returncode:
            raise ValueError(f"{side} command exited {process.returncode}: {shlex.join(command)}\n"
                             f"{process.stdout.rstrip()}")
        if pattern is None:
            return elapsed
        output = (work / spec.metric_file).read_text(encoding="utf-8") if spec.metric_file else process.stdout
        return metric_value(pattern, output)


def compare(pairs, threshold_pct, bootstrap_samples):
    ratios = [pair["head"] / pair["base"] for pair in pairs]
    ratio = median(ratios)
    rng = random.Random(BOOTSTRAP_SEED)
    # Resample entire pairs so host conditions shared by each pair stay paired.
    estimates = [median(rng.choices(ratios, k=len(ratios))) for _ in range(bootstrap_samples)]
    percentiles = quantiles(estimates, n=40, method="inclusive")
    lower, upper = percentiles[0], percentiles[-1]
    return {
        "median_ratio": ratio,
        "ci95": [lower, upper],
        "regression": ratio >= 1 + threshold_pct / 100 and lower > 1,
    }


def benchmark(spec, base, head):
    if not base or not head:
        raise ValueError("base and head commands must both be non-empty")
    binaries = {"base": base, "head": head}
    pattern = None if spec.metric == "wall" else re.compile(spec.metric)
    pairs = []
    for index in range(spec.warmup + spec.rounds):
        order = ("base", "head") if index % 2 == 0 else ("head", "base")
        pair = {side: measure(spec, pattern, binaries[side], side) for side in order}
        if index >= spec.warmup:
            pairs.append({"order": list(order), **pair})
    return {"pairs": pairs, **compare(pairs, spec.threshold_pct, spec.bootstrap_samples)}


def cpu_model():
    try:
        lines = Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines()
        for key in ("model name", "model", "hardware"):
            for line in lines:
                name, _, value = line.partition(":")
                if name.strip().lower() == key and value.strip():
                    return value.strip()
    except OSError:
        pass
    if platform.system() == "Darwin":
        try:
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], stderr=subprocess.DEVNULL, text=True
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            pass
    return platform.processor() or platform.machine() or "unknown"


def write_result(output, result):
    output.mkdir(parents=True, exist_ok=True)
    (output / "results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    spec = result["spec"]
    lower, upper = result["ci95"]
    status = "Regression" if result["regression"] else "Pass"
    summary = (
        f"# {spec['name']}\n\n"
        f"{status} on {result['cpu_model']}.\n\n"
        f"| Measured pairs | Median head/base | 95% bootstrap CI | Regression threshold |\n"
        f"| ---: | ---: | ---: | ---: |\n"
        f"| {len(result['pairs'])} | {result['median_ratio']:.4f} | {lower:.4f} to {upper:.4f} "
        f"| {spec['threshold_pct']:g}% |\n"
    )
    (output / "summary.md").write_text(summary, encoding="utf-8")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("spec", type=Path)
    parser.add_argument("base", help="base binary path or command")
    parser.add_argument("head", help="head binary path or command")
    parser.add_argument("--commands", action="store_true", help="split base and head into command arguments")
    parser.add_argument("--output", type=Path, required=True, help="directory for results.json and summary.md")
    args = parser.parse_args(argv)
    try:
        spec = load_spec(args.spec)
        base, head = (shlex.split(args.base), shlex.split(args.head)) if args.commands else ([args.base], [args.head])
        result = benchmark(spec, base, head)
        result.update(spec=asdict(spec), base=base, head=head, cpu_model=cpu_model(),
                      bootstrap_seed=BOOTSTRAP_SEED, date=datetime.now(timezone.utc).isoformat())
        write_result(args.output, result)
    except (OSError, ValueError, TypeError, re.error) as error:
        print(f"ab: {error}", file=sys.stderr)
        return 2
    print(f"Median head/base {result['median_ratio']:.4f}. Wrote {args.output / 'results.json'} "
          f"and {args.output / 'summary.md'}.")
    return int(result["regression"])


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Run a paired A/B benchmark and report a bootstrap confidence interval."""

import argparse
from dataclasses import asdict, dataclass, field
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
    metric: str | None = None
    threshold_pct: float = 5
    bootstrap_samples: int = 2000
    metric_file: str | None = None
    # Named metrics measured from the same command: {name = "regex" | "wall"} or
    # {name = {pattern = "regex", file = "relative/path", higher_is_better = true}}.
    # The first one is the primary. A higher-is-better value (a rate) is compared as its
    # reciprocal, so every ratio is a cost. `gate = false` reports a metric without letting it
    # decide the comparison, for one whose A/A noise is above the threshold.
    metrics: dict = field(default_factory=dict)
    timeout_s: float | None = None

    def gated(self, name):
        value = self.metrics.get(name)
        return not isinstance(value, dict) or value.get("gate", True)

    def measurements(self):
        """Return (name, pattern, file, higher_is_better) per metric."""
        if self.metric is not None:
            return [("metric", self.metric, self.metric_file, False)]
        rows = []
        for name, value in self.metrics.items():
            if isinstance(value, str):
                rows.append((name, value, self.metric_file, False))
            else:
                rows.append((name, value.get("pattern"), value.get("file", self.metric_file),
                             value.get("higher_is_better", False)))
        return rows


def check_metric(name, pattern, file, higher_is_better):
    if type(higher_is_better) is not bool or (higher_is_better and pattern == "wall"):
        raise ValueError(f"metric {name}: higher_is_better must be a boolean, and wall time is a cost")
    if not isinstance(pattern, str) or not pattern.strip():
        raise ValueError(f"metric {name} needs a non-empty pattern")
    if pattern != "wall" and re.compile(pattern).groups != 1:
        raise ValueError(f"metric {name} regex must have exactly one capturing group")
    if file is not None:
        path = Path(file)
        if pattern == "wall" or not isinstance(file, str) or path.is_absolute() or ".." in path.parts:
            raise ValueError("metric_file needs a regex and a path relative to the scratch directory")


def load_spec(path):
    with path.open("rb") as source:
        values = tomllib.load(source)
    if isinstance(values.get("command"), str):
        values["command"] = shlex.split(values["command"])
    spec = Spec(**values)
    for key in ("name", "module"):
        value = getattr(spec, key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{key} must be a non-empty string")
    if (spec.metric is None) == (not spec.metrics):
        raise ValueError("give exactly one of metric and metrics")
    if not isinstance(spec.metrics, dict) or not all(
            isinstance(value, str) or (isinstance(value, dict)
                                       and set(value) <= {"pattern", "file", "higher_is_better", "gate"}
                                       and type(value.get("gate", True)) is bool)
            for value in spec.metrics.values()):
        raise ValueError("metrics values must be a pattern or a table of pattern, file, higher_is_better "
                         "and a boolean gate")
    if not any(spec.gated(row[0]) for row in spec.measurements()):
        raise ValueError("at least one metric must gate the comparison")
    if (not isinstance(spec.command, list)
            or not all(isinstance(arg, str) for arg in spec.command)
            or not any("{bin}" in arg for arg in spec.command)):
        raise ValueError("command must contain {bin}")
    for key, minimum in (("warmup", 0), ("rounds", 2), ("bootstrap_samples", 2)):
        if type(getattr(spec, key)) is not int or getattr(spec, key) < minimum:
            raise ValueError(f"{key} must be an integer of at least {minimum}")
    for key in ("threshold_pct", "timeout_s"):
        value = getattr(spec, key)
        if key == "timeout_s" and value is None:
            continue
        if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
            raise ValueError(f"{key} must be a nonnegative finite number")
    for row in spec.measurements():
        check_metric(*row)
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
        raise ValueError(f"metric regex {pattern.pattern!r} did not match benchmark output")
    values = [float(match[1]) for match in matches]
    if not all(math.isfinite(value) and value > 0 for value in values):
        raise ValueError("benchmark metrics must be positive and finite")
    return median(values)


def measure(spec, patterns, binary, side, cwd=ROOT):
    """Run one invocation from cwd and return {metric name: value}."""
    with tempfile.TemporaryDirectory(prefix=f"nabedra-ab-{side}-") as directory:
        work = Path(directory)
        command = invocation(spec.command, binary, work, side)
        started = time.perf_counter()
        try:
            process = subprocess.run(command, cwd=cwd, text=True, errors="replace",
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     timeout=spec.timeout_s)
        except subprocess.TimeoutExpired as error:
            output = error.output or ""
            output = output.decode(errors="replace") if isinstance(output, bytes) else output
            raise ValueError(f"{side} command timed out after {spec.timeout_s} s: {shlex.join(command)}\n"
                             f"{output.rstrip()[-4000:]}") from None
        elapsed = time.perf_counter() - started
        if process.returncode:
            raise ValueError(f"{side} command exited {process.returncode}: {shlex.join(command)}\n"
                             f"{process.stdout.rstrip()}")
        values = {}
        for name, pattern, file, higher_is_better in patterns:
            if pattern is None:
                values[name] = elapsed
            else:
                output = (work / file).read_text(encoding="utf-8") if file else process.stdout
                value = metric_value(pattern, output)
                values[name] = 1 / value if higher_is_better else value
        return values


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


def noise(pairs, ci95):
    """Spread of the paired ratios, and of each side across pairs, for A/A noise floors."""
    ratios = [pair["head"] / pair["base"] for pair in pairs]
    logs = [math.log(value) for value in ratios]
    center = median(logs)
    side = {key: [pair[key] for pair in pairs] for key in ("base", "head")}
    return {
        "ratio_range": [min(ratios), max(ratios)],
        # Median absolute deviation of log ratios, scaled to a normal sigma, in percent.
        "ratio_mad_pct": 100 * 1.4826 * median(abs(value - center) for value in logs),
        # The interval's larger distance from 1 bounds the smallest shift this run could flag.
        "ci_halfwidth_pct": 100 * max(abs(ci95[0] - 1), abs(ci95[1] - 1)),
        "side_spread_pct": {key: 100 * (max(values) - min(values)) / median(values)
                            for key, values in side.items()},
    }


def benchmark(spec, base, head, roots=None):
    """roots maps a side to the tree its command runs from (default: this repository)."""
    if not base or not head:
        raise ValueError("base and head commands must both be non-empty")
    binaries = {"base": base, "head": head}
    patterns = [(name, None if pattern == "wall" else re.compile(pattern), file, higher)
                for name, pattern, file, higher in spec.measurements()]
    runs = []
    started = time.perf_counter()
    for index in range(spec.warmup + spec.rounds):
        order = ("base", "head") if index % 2 == 0 else ("head", "base")
        pair = {side: measure(spec, patterns, binaries[side], side, (roots or {}).get(side, ROOT))
                for side in order}
        if index >= spec.warmup:
            runs.append((list(order), pair))
    metrics = {}
    for name, *_ in patterns:
        pairs = [{"order": order, "base": pair["base"][name], "head": pair["head"][name]}
                 for order, pair in runs]
        result = compare(pairs, spec.threshold_pct, spec.bootstrap_samples)
        metrics[name] = {"pairs": pairs, **result, "gate": spec.gated(name),
                         "noise": noise(pairs, result["ci95"])}
    primary = metrics[patterns[0][0]]
    return {
        "pairs": primary["pairs"],
        "median_ratio": primary["median_ratio"],
        "ci95": primary["ci95"],
        "regression": any(row["regression"] and row["gate"] for row in metrics.values()),
        "metrics": metrics,
        "runtime_seconds": time.perf_counter() - started,
    }


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
    status = "Regression" if result["regression"] else "Pass"
    rows = []
    for name, row in result["metrics"].items():
        lower, upper = row["ci95"]
        rows.append(
            f"| {name} | {len(row['pairs'])} | {row['median_ratio']:.4f} | {lower:.4f} to {upper:.4f} "
            f"| {row['noise']['ratio_mad_pct']:.2f}% | {'yes' if row['regression'] else 'no'}"
            f"{'' if row['gate'] else ' (report only)'} |"
        )
    summary = (
        f"# {spec['name']}\n\n"
        f"{status} on {result['cpu_model']} in {result['runtime_seconds']:.0f} s. "
        f"Regression threshold {spec['threshold_pct']:g}%.\n\n"
        f"| Metric | Measured pairs | Median head/base | 95% bootstrap CI | Ratio MAD | Regression |\n"
        f"| --- | ---: | ---: | ---: | ---: | --- |\n" + "\n".join(rows) + "\n"
    )
    (output / "summary.md").write_text(summary, encoding="utf-8")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("spec", type=Path)
    parser.add_argument("base", help="base binary path or command")
    parser.add_argument("head", help="head binary path or command")
    parser.add_argument("--commands", action="store_true", help="split base and head into command arguments")
    parser.add_argument("--output", type=Path, required=True, help="directory for results.json and summary.md")
    parser.add_argument("--rounds", type=int, help="override the spec's measured pairs (at least 2)")
    parser.add_argument("--warmup", type=int, help="override the spec's warmup pairs")
    for side in ("base", "head"):
        parser.add_argument(f"--{side}-root", type=Path,
                            help=f"run the {side} command from this tree, with its own drivers and config "
                                 "writers (default: this repository)")
    args = parser.parse_args(argv)
    try:
        spec = load_spec(args.spec)
        overrides = {key: value for key, value in (("rounds", args.rounds), ("warmup", args.warmup))
                     if value is not None}
        if overrides.get("rounds", 2) < 2 or overrides.get("warmup", 0) < 0:
            raise ValueError("--rounds must be at least 2 and --warmup nonnegative")
        spec = Spec(**{**asdict(spec), **overrides})
        base, head = (shlex.split(args.base), shlex.split(args.head)) if args.commands else ([args.base], [args.head])
        roots = {side: path.resolve() for side, path in (("base", args.base_root), ("head", args.head_root)) if path}
        for path in roots.values():
            if not path.is_dir():
                raise ValueError(f"{path} is not a directory")
        result = benchmark(spec, base, head, roots)
        result.update(spec=asdict(spec), base=base, head=head, cpu_model=cpu_model(),
                      roots={side: str(roots.get(side, ROOT)) for side in ("base", "head")},
                      bootstrap_seed=BOOTSTRAP_SEED, date=datetime.now(timezone.utc).isoformat())
        write_result(args.output, result)
    except (OSError, ValueError, TypeError, re.error) as error:
        print(f"ab: {error}", file=sys.stderr)
        return 2
    ratios = ", ".join(f"{name} {row['median_ratio']:.4f}" for name, row in result["metrics"].items())
    print(f"Median head/base: {ratios}. Wrote {args.output / 'results.json'} "
          f"and {args.output / 'summary.md'}.")
    return int(result["regression"])


if __name__ == "__main__":
    sys.exit(main())

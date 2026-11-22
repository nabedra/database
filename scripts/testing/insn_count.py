#!/usr/bin/env python3
"""Compare user-space instructions per operation with Callgrind."""

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import json
import math
import shlex
import shutil
from statistics import median
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WORK_DIR = ROOT.with_name(f"{ROOT.name}-work")
DRIVERS = Path(__file__).resolve().with_name("insn_workloads")
EXAMPLE = "phase7_insn_count"
REPETITIONS = 3
DEFAULT_THRESHOLD_PCT = 0.5
# A noise-derived threshold above this cannot certify a pass. The largest
# measured spread (0.27%, OLTP storage) gives 0.8%.
MAX_NOISE_THRESHOLD_PCT = 2.0
MISSING_COUNTER = "not-run: valgrind-missing"


@dataclass(frozen=True)
class Workload:
    name: str
    feature: str
    operations: int


WORKLOADS = (
    Workload("oltp-storage", "oltp", 512),
    Workload("kv-write-flush", "oltp", 1024),
    Workload("cache-set", "cache", 8192),
    Workload("olap-queue", "olap", 512),
)


def run(command, cwd, *, capture=False, timeout=None):
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        timeout=timeout,
    )
    if result.returncode:
        output = f"\n{result.stdout.rstrip()}" if capture else ""
        raise ValueError(f"exit {result.returncode}: {shlex.join(map(str, command))}{output}")
    return result.stdout


def revision(ref):
    return run(
        ["git", "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}"],
        ROOT, capture=True,
    ).strip()


@contextmanager
def checkout(path, sha):
    run(["git", "worktree", "add", "--quiet", "--detach", str(path), sha], ROOT)
    try:
        yield path
    finally:
        run(["git", "worktree", "remove", "--force", str(path)], ROOT)


def install_drivers(tree):
    server = tree / "server"
    shutil.copyfile(DRIVERS / f"{EXAMPLE}.rs", server / "examples" / f"{EXAMPLE}.rs")
    cache_driver = server / "src/cache/standalone/insn_count.rs"
    cache_driver.parent.mkdir(exist_ok=True)
    shutil.copyfile(DRIVERS / "cache.rs", cache_driver)
    additions = {
        "cache/standalone.rs": "\nmod insn_count;\npub use insn_count::run_insn_cache;\n",
        "cache.rs": "\npub use standalone::run_insn_cache;\n",
        "node/mod.rs": "\npub fn insn_queue_now_ms() -> u64 { queue_runtime::now_ms() }\n",
    }
    for path, source in additions.items():
        with (server / "src" / path).open("a", encoding="utf-8") as file:
            file.write(source)


def build(tree, target, cargo, feature):
    command = [
        cargo, "build", "--release", "--locked", "-j", "6",
        "--no-default-features", "--features", feature,
        "--example", EXAMPLE, "--target-dir", str(target),
    ]
    print(f"Build {tree.name}/{feature}: {shlex.join(command)}", flush=True)
    run(command, tree / "server")
    # Cargo replaces the example executable on the next feature build.
    binary = target / f"{EXAMPLE}-{feature}"
    shutil.copyfile(target / "release" / "examples" / EXAMPLE, binary)
    binary.chmod(0o755)
    return binary


def instruction_count(profile):
    fields = {}
    for line in profile.splitlines():
        key, _, value = line.partition(":")
        if key in {"events", "summary"}:
            if key in fields:
                raise ValueError(f"Callgrind output contains multiple {key} lines")
            fields[key] = value.split()
    if fields.get("events") != ["Ir"] or len(fields.get("summary", [])) != 1:
        raise ValueError("Callgrind output must contain one Ir event and one summary")
    count = int(fields["summary"][0])
    if count <= 0:
        raise ValueError("Callgrind reported no instructions")
    return count


def measure(counter, binary, workload, directory):
    # Reuse the same working directory path for both binaries and all runs.
    directory.mkdir()
    try:
        profile = directory / "callgrind.out"
        output = run(
            [
                counter, "--tool=callgrind", "--quiet", "--fair-sched=yes",
                "--cache-sim=no", "--branch-sim=no",
                f"--callgrind-out-file={profile}", str(binary), workload.name,
            ],
            directory, capture=True, timeout=180,
        )
        marker = f"nabedra-insn-count: {workload.name} {workload.operations}"
        markers = [line for line in output.splitlines() if line.startswith("nabedra-insn-count:")]
        if markers != [marker]:
            raise ValueError(f"expected {marker!r} from {binary}, got {output!r}")
        return instruction_count(profile.read_text(encoding="utf-8", errors="replace"))
    finally:
        shutil.rmtree(directory)


def compare(base, head, operations, threshold_pct=DEFAULT_THRESHOLD_PCT):
    base_median, head_median = median(base), median(head)
    # The full base spread also covers worst-case variation between base runs.
    noise_pct = 100 * (max(base) - min(base)) / base_median
    # A noisy base must not widen the threshold until it hides a regression.
    inconclusive = 3 * noise_pct > max(threshold_pct, MAX_NOISE_THRESHOLD_PCT)
    threshold_pct = max(threshold_pct, 3 * noise_pct)
    delta_pct = 100 * (head_median - base_median) / base_median
    return {
        "operations": operations,
        "base_counts": base,
        "head_counts": head,
        "base_per_op": base_median / operations,
        "head_per_op": head_median / operations,
        "noise_pct": noise_pct,
        "threshold_pct": threshold_pct,
        "delta_pct": delta_pct,
        "regression": delta_pct > threshold_pct,
        "inconclusive": inconclusive,
    }


def benchmark(base_sha, head_sha, cargo, counter, workloads, work_dir):
    work_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="nabedra-insn-", dir=work_dir) as scratch:
        work = Path(scratch)
        with checkout(work / "base", base_sha) as base, checkout(work / "head", head_sha) as head:
            trees = {"base": base, "head": head}
            for tree in trees.values():
                install_drivers(tree)
            for feature in dict.fromkeys(workload.feature for workload in workloads):
                binaries = {
                    side: build(tree, work / f"{side}-target", cargo, feature)
                    for side, tree in trees.items()
                }
                for workload in workloads:
                    if workload.feature != feature:
                        continue
                    counts = {"base": [], "head": []}
                    for index in range(REPETITIONS):
                        order = ("base", "head") if index % 2 == 0 else ("head", "base")
                        for side in order:
                            counts[side].append(
                                measure(counter, binaries[side], workload, work / "run")
                            )
                    yield workload.name, counts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="base Git revision")
    parser.add_argument("--head", required=True, help="head Git revision")
    parser.add_argument("--workload", action="append", choices=[row.name for row in WORKLOADS],
                        help="run a named workload only (repeatable, default: all)")
    parser.add_argument("--cargo", default=shutil.which("cargo-slot") or "cargo",
                        help="Cargo executable or wrapper (default: cargo-slot if installed)")
    parser.add_argument("--threshold-pct", type=float, default=DEFAULT_THRESHOLD_PCT,
                        help="minimum regression threshold as a percent (default: 0.5)")
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR,
                        help="parent for worktrees and targets (default: sibling <checkout>-work)")
    parser.add_argument("--output", type=Path, help="write raw instruction counts and result as JSON")
    parser.add_argument("--skip-missing-valgrind", action="store_true",
                        help="emit the registered check's not-run marker if Callgrind is missing")
    args = parser.parse_args(argv)
    if not math.isfinite(args.threshold_pct) or args.threshold_pct < 0:
        parser.error("--threshold-pct must be finite and nonnegative")
    counter = shutil.which("valgrind")
    if not counter:
        print(MISSING_COUNTER if args.skip_missing_valgrind else "insn_count: valgrind is required")
        return 0 if args.skip_missing_valgrind else 2
    try:
        base, head = revision(args.base), revision(args.head)
        version = run([counter, "--version"], ROOT, capture=True).strip()
        print(f"Callgrind ({version}, fair scheduler) base={base} head={head}", flush=True)
        results = {}
        workloads = [row for row in WORKLOADS if not args.workload or row.name in args.workload]
        operations = {workload.name: workload.operations for workload in workloads}
        for name, counts in benchmark(base, head, args.cargo, counter, workloads, args.work_dir.resolve()):
            row = compare(counts["base"], counts["head"], operations[name], args.threshold_pct)
            results[name] = row
            status = "FAIL" if row["regression"] else "NOISY" if row["inconclusive"] else "PASS"
            print(
                f"{status} {name}: ops={row['operations']} base={row['base_per_op']:.2f} "
                f"head={row['head_per_op']:.2f} Ir/op delta={row['delta_pct']:+.4f}% "
                f"noise={row['noise_pct']:.4f}% threshold={row['threshold_pct']:.4f}% "
                f"base_counts={row['base_counts']} head_counts={row['head_counts']}",
                flush=True,
            )
        regression = any(row["regression"] for row in results.values())
        inconclusive = not regression and any(row["inconclusive"] for row in results.values())
        if args.output:
            args.output.write_text(
                json.dumps(
                    {"base": base, "head": head, "counter": version, "profile": "release",
                     "fair_scheduling": True, "regression": regression,
                     "inconclusive": inconclusive, "workloads": results},
                    indent=2,
                ) + "\n",
                encoding="utf-8",
            )
        if inconclusive:
            print("INCONCLUSIVE instruction-count comparison: a NOISY base spread cannot certify a pass")
            return 2
        print(f"{'FAIL' if regression else 'PASS'} instruction-count comparison")
        return int(regression)
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"insn_count: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

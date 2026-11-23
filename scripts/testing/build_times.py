#!/usr/bin/env python3
"""Record server build times and rebuild sets for a base and a head revision (BLD6, BLD9).

Checks out both revisions into worktrees under one work directory and builds them with one shared
target directory, so registry dependencies build once and both sides start from the same warm
dependencies. Workspace packages have different package ids in the two worktrees, so their
artifacts never collide. Each step runs `--rounds` times, alternating the side that goes first.

Steps (phase 8 design, 5.3):
  cold    warm dependencies, cold workspace: `cargo build --lib` per engine and the Events build,
          with per-unit times from Cargo's `--timings` report
  test    cold OLTP test build: `cargo test --lib --tests --no-run` for the server and each OLTP package
  edit    edit loop with incremental on and line-table debug info: append a constant to one file
          per crate, then time `cargo check --lib` and `cargo test --lib --no-run` of its package
  extras  once per side: the duplicate list (`cargo tree -d`) and the test-target counts

Times are recorded, not gated. The rebuild sets of the edit step are deterministic, and the
head must satisfy every rebuild assertion whose packages exist. The exit status is 0 when every
measurement completed and the applicable assertions hold, 1 when an assertion fails and 2 when a
measurement could not be taken. Python standard library only.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
from statistics import median
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WORK_DIR = ROOT.with_name(f"{ROOT.name}-build-times")
SERVER = "nabedra-database"
STEPS = ("cold", "test", "edit", "extras")
DEFAULT_ROUNDS = 3
# The packages of the planned OLTP graph below the server crate (phase 8 design, 3.1).
OLTP_PACKAGES = (
    "nabedra-protocol", "nabedra-resources", "nabedra-oltp-types", "nabedra-transport",
    "nabedra-oltp-storage", "nabedra-oltp-replication",
)
# CI's settings for the cold steps (plans/fast-builds.md), set explicitly so laptop runs match.
COLD_ENV = {"CARGO_INCREMENTAL": "0", "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0"}
# BLD2's local edit loop.
EDIT_ENV = {
    "CARGO_INCREMENTAL": "1",
    "CARGO_PROFILE_DEV_DEBUG": "line-tables-only",
    "CARGO_PROFILE_TEST_DEBUG": "line-tables-only",
}


@dataclass(frozen=True)
class FeatureSet:
    label: str
    manifest: str            # workspace directory, relative to the checkout
    args: tuple[str, ...]


FEATURE_SETS = (
    FeatureSet("oltp", "server", ("--no-default-features", "--features", "oltp")),
    FeatureSet("olap,s3", "server", ("--no-default-features", "--features", "olap,s3")),
    FeatureSet("cache", "server", ("--no-default-features", "--features", "cache")),
    FeatureSet("events", "server/events", ()),
)
SERVER_OLTP = ("--no-default-features", "--features", f"{SERVER}/oltp")


@dataclass(frozen=True)
class Probe:
    """One edited file per crate. The first existing candidate is used on each side.

    A probe without `always` runs only when its first candidate (its own crate) exists on at least
    one side; the other side then measures the same edit at its pre-split path.
    """
    name: str
    candidates: tuple[str, ...]
    always: bool = False


PROBES = (
    Probe("storage", ("server/oltp-storage/src/oltp/pool.rs", "server/src/oltp/pool.rs"), always=True),
    Probe("replication", ("server/oltp-replication/src/oltp_replication/typed_log.rs",
                          "server/src/oltp_replication/typed_log.rs"), always=True),
    Probe("runtime", ("server/src/oltp_runtime/http.rs",), always=True),
    Probe("transport", ("server/transport/src/transport/session.rs", "server/transport/src/session.rs",
                        "server/src/oltp_replication/multiplex/transport/session.rs")),
    Probe("resources", ("server/resources/src/memory.rs", "server/src/oltp/pool/resources.rs")),
)


@dataclass(frozen=True)
class RebuildRule:
    probe: str
    owner: str                     # the package the probe must belong to on the head
    forbidden: tuple[str, ...]     # packages its edit must not rebuild


REBUILD_RULES = (
    RebuildRule("runtime", SERVER, OLTP_PACKAGES),
    RebuildRule("transport", "nabedra-transport", ("nabedra-oltp-storage",)),
    RebuildRule("storage", "nabedra-oltp-storage", ("nabedra-transport",)),
)


class MeasurementError(Exception):
    pass


# Parsing.


def unit_data(html: str) -> list[dict]:
    """Parse the `UNIT_DATA` array of Cargo's `cargo-timing.html`. Fails loudly on any change."""
    match = re.search(r"\bconst UNIT_DATA = ", html)
    if not match:
        raise MeasurementError("cargo-timing.html has no `const UNIT_DATA = ` array")
    try:
        units, _ = json.JSONDecoder().raw_decode(html, match.end())
    except json.JSONDecodeError as error:
        raise MeasurementError(f"cargo-timing.html UNIT_DATA is not JSON: {error}") from error
    if not isinstance(units, list) or not units:
        raise MeasurementError("cargo-timing.html UNIT_DATA is empty")
    rows = []
    for unit in units:
        if not isinstance(unit, dict) or not {"name", "duration"} <= unit.keys():
            raise MeasurementError(f"cargo-timing.html unit has no name or duration: {unit!r}")
        row = {
            "name": unit["name"],
            "version": unit.get("version", ""),
            "target": unit.get("target", "").strip(),
            "mode": unit.get("mode", ""),
            "duration": float(unit["duration"]),
        }
        sections = dict((name, span) for name, span in unit.get("sections") or [])
        for name in ("frontend", "codegen"):
            if name in sections:
                row[name] = round(sections[name]["end"] - sections[name]["start"], 3)
        if "rmeta_time" in unit and unit["rmeta_time"] is not None:
            row["rmeta_time"] = float(unit["rmeta_time"])
        rows.append(row)
    return rows


def package_name(package_id: str) -> str:
    """The package name in a Cargo package id, in the old or the new (`...#name@1.0`) format."""
    if "#" in package_id:
        fragment = package_id.rsplit("#", 1)[1]
        if "@" in fragment:
            return fragment.split("@", 1)[0]
        return package_id.rsplit("#", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    return package_id.split(" ", 1)[0]


def artifacts(stdout: str) -> list[dict]:
    rows = []
    for line in stdout.splitlines():
        if not line.startswith("{"):
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("reason") == "compiler-artifact":
            rows.append(message)
    return rows


def duplicates(tree_output: str) -> list[str]:
    """The top-level `name vX` lines of `cargo tree -d`."""
    found = []
    for line in tree_output.splitlines():
        if line and not line[0].isspace() and line[0] not in "│├└":
            entry = line.replace(" (*)", "").strip()
            if re.match(r"^\S+ v\d", entry) and entry not in found:
                found.append(entry)
    return found


def test_targets(metadata: dict, package: str, features: set[str]) -> int:
    """Integration-test targets of `package` whose required features are all enabled."""
    for row in metadata["packages"]:
        if row["name"] == package:
            enabled = set(features)
            table = row.get("features", {})
            pending = list(enabled)
            while pending:  # expand feature implications
                for item in table.get(pending.pop(), []):
                    if not item.startswith("dep:") and "/" not in item and item not in enabled:
                        enabled.add(item)
                        pending.append(item)
            return sum(
                1 for target in row["targets"]
                if "test" in target["kind"] and set(target.get("required-features", [])) <= enabled
            )
    raise MeasurementError(f"package {package} is not in the workspace")


def summarize(samples: list[dict]) -> list[dict]:
    """Median seconds per (step, label) and side, and the head/base ratio."""
    groups: dict[tuple[str, str], dict[str, list[float]]] = {}
    for sample in samples:
        groups.setdefault((sample["step"], sample["label"]), {"base": [], "head": []})[sample["side"]].append(
            sample["seconds"])
    rows = []
    for (step, label), sides in groups.items():
        base = median(sides["base"]) if sides["base"] else None
        head = median(sides["head"]) if sides["head"] else None
        rows.append({
            "step": step, "label": label, "base_s": base, "head_s": head,
            "ratio": round(head / base, 4) if base and head is not None else None,
            "base_samples": sides["base"], "head_samples": sides["head"],
        })
    return rows


def evaluate_rules(rebuilds: dict[str, dict], head_packages: set[str]) -> list[dict]:
    """Check the head's rebuild sets. A rule whose packages do not exist on the head is n/a."""
    results = []
    for rule in REBUILD_RULES:
        forbidden = [name for name in rule.forbidden if name in head_packages]
        row = {"probe": rule.probe, "owner": rule.owner, "forbidden": forbidden}
        observed = rebuilds.get(rule.probe)
        if rule.owner not in head_packages or not forbidden:
            missing = [rule.owner] if rule.owner not in head_packages else list(rule.forbidden)
            row.update(result="n/a", reason=f"not on the head yet: {', '.join(missing)}")
        elif observed is None:
            row.update(result="fail", reason="the probe did not run")
        elif observed["owner"] != rule.owner:
            row.update(result="fail", reason=f"the probe file belongs to {observed['owner']}, not {rule.owner}")
        else:
            hit = sorted(set(forbidden) & set(observed["rebuilt"]))
            row.update(result="fail" if hit else "pass", rebuilt=observed["rebuilt"],
                       reason=f"rebuilt {', '.join(hit)}" if hit else "")
        results.append(row)
    return results


def round_order(index: int) -> tuple[str, str]:
    """Base first on odd rounds (1, 3, ...), head first on even rounds."""
    return ("base", "head") if index % 2 == 0 else ("head", "base")


# Running Cargo.


@dataclass
class Result:
    seconds: float
    stdout: str
    load: tuple[float, float]


class Runner:
    def __init__(self, cargo: str, target: Path, jobs: int | None, log=print):
        self.cargo = cargo
        self.target = target
        self.jobs = jobs
        self.log = log

    def env(self, extra: dict[str, str]) -> dict[str, str]:
        env = dict(os.environ)
        env.update({"CARGO_TERM_COLOR": "never", "CARGO_TARGET_DIR": str(self.target)})
        # A caching wrapper (sccache) would replay workspace crates instead of compiling them, and
        # it refuses incremental builds. Measure plain rustc.
        env.update({"RUSTC_WRAPPER": "", "CARGO_BUILD_RUSTC_WRAPPER": "", "RUSTC_WORKSPACE_WRAPPER": ""})
        env.update(extra)
        return env

    def run(self, command: list[str], cwd: Path, env: dict[str, str], *, check=True) -> Result:
        load = os.getloadavg()[0] if hasattr(os, "getloadavg") else 0.0
        self.log(f"+ ({cwd.name}) {shlex.join(command)}", flush=True)
        started = time.monotonic()
        process = subprocess.run(
            command, cwd=cwd, env=env, text=True, encoding="utf-8", errors="replace",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        seconds = round(time.monotonic() - started, 3)
        after = os.getloadavg()[0] if hasattr(os, "getloadavg") else 0.0
        if check and process.returncode:
            tail = "\n".join(process.stderr.splitlines()[-40:])
            raise MeasurementError(f"exit {process.returncode} after {seconds}s: {shlex.join(command)}\n{tail}")
        return Result(seconds, process.stdout, (load, after))

    def cargo_command(self, verb: str, args, *, json_output=True) -> list[str]:
        command = [self.cargo, verb, "--locked", *args]
        if json_output:
            command.append("--message-format=json-render-diagnostics")
        if self.jobs:
            command += ["-j", str(self.jobs)]
        return command

    def build(self, verb: str, args, cwd: Path, env: dict[str, str]) -> tuple[Result, list[dict]]:
        result = self.run(self.cargo_command(verb, args), cwd, self.env(env))
        return result, artifacts(result.stdout)

    def clean(self, packages, cwd: Path) -> None:
        if packages:
            args = ["--offline"] + [arg for name in packages for arg in ("-p", name)]
            self.run([self.cargo, "clean", "--locked", *args], cwd, self.env({}))

    def capture(self, args, cwd: Path) -> str:
        return self.run([self.cargo, *args], cwd, self.env({})).stdout


@dataclass
class Tree:
    side: str
    path: Path
    sha: str
    metadata: dict = field(default_factory=dict)

    @property
    def packages(self) -> dict[str, Path]:
        """Workspace package name -> package directory, relative to the checkout."""
        members = set(self.metadata["workspace_members"])
        return {
            row["name"]: Path(row["manifest_path"]).parent.relative_to(self.path)
            for row in self.metadata["packages"] if row["id"] in members
        }

    def owner(self, relative: str) -> str:
        best, length = None, -1
        for name, directory in self.packages.items():
            prefix = directory.as_posix()
            if (relative + "/").startswith(prefix + "/") and len(prefix) > length:
                best, length = name, len(prefix)
        if best is None:
            raise MeasurementError(f"{relative} belongs to no workspace package on the {self.side}")
        return best


def resolve_probes(trees: dict[str, Tree], names: list[str] | None) -> list[tuple[Probe, dict[str, str]]]:
    chosen = []
    for probe in PROBES:
        if names and probe.name not in names:
            continue
        own = probe.candidates[0]
        if not probe.always and not any((tree.path / own).is_file() for tree in trees.values()):
            continue  # the crate does not exist on either side yet
        paths = {}
        for side, tree in trees.items():
            path = next((c for c in probe.candidates if (tree.path / c).is_file()), None)
            if path is None:
                raise MeasurementError(f"probe {probe.name}: no candidate file on the {side}: {probe.candidates}")
            paths[side] = path
        chosen.append((probe, paths))
    return chosen


def package_args(tree: Tree, package: str) -> list[str]:
    return ["-p", package, *(SERVER_OLTP if package == SERVER else ())]


# Steps.


class Session:
    def __init__(self, runner: Runner, trees: dict[str, Tree], rounds: int, log=print):
        self.runner = runner
        self.trees = trees
        self.rounds = rounds
        self.log = log
        self.samples: list[dict] = []
        self.loads: list[dict] = []
        self.rebuilds: dict[str, dict] = {}
        self.extras: dict[str, dict] = {}

    def sample(self, step, label, side, index, result: Result, **extra):
        row = {"step": step, "label": label, "side": side, "round": index + 1, "seconds": result.seconds,
               "load_before": round(result.load[0], 2), "load_after": round(result.load[1], 2), **extra}
        self.samples.append(row)
        self.log(f"nabedra-build-sample: {step} {label} {side} round={index + 1} {result.seconds:.1f}s", flush=True)

    def mark_load(self, step: str, when: str) -> None:
        load = os.getloadavg() if hasattr(os, "getloadavg") else (0.0, 0.0, 0.0)
        self.loads.append({"step": step, "when": when, "load": [round(x, 2) for x in load]})

    def workspace(self, tree: Tree, manifest: str) -> list[str]:
        if manifest == "server":
            return sorted(tree.packages)
        return ["nabedra-events"]

    def cold(self, feature_sets) -> None:
        for feature_set in feature_sets:
            for tree in self.trees.values():  # warm the dependencies, unmeasured
                self.runner.build("build", ["--lib", *feature_set.args], tree.path / feature_set.manifest, COLD_ENV)
            for index in range(self.rounds):
                for side in round_order(index):
                    tree = self.trees[side]
                    cwd = tree.path / feature_set.manifest
                    packages = self.workspace(tree, feature_set.manifest)
                    self.runner.clean(packages, cwd)
                    result, built = self.runner.build("build", ["--lib", "--timings", *feature_set.args], cwd, COLD_ENV)
                    dirty = sorted({package_name(a["package_id"]) for a in built if not a.get("fresh")} & set(packages))
                    if not dirty:
                        raise MeasurementError(f"cold {feature_set.label} on the {side}: no workspace crate rebuilt")
                    timing = self.runner.target / "cargo-timings" / "cargo-timing.html"
                    units = unit_data(timing.read_text(encoding="utf-8", errors="replace"))
                    mine = [u for u in units if u["name"] in packages]
                    top = sorted(units, key=lambda u: -u["duration"])[:15]
                    self.sample("cold", feature_set.label, side, index, result, rebuilt=dirty,
                                workspace_units=mine, top_units=top)

    def test(self) -> None:
        for tree in self.trees.values():
            self.runner.build("test", ["--no-run", "--lib", "--tests", *self.test_args(tree)], tree.path / "server",
                              COLD_ENV)
        for index in range(self.rounds):
            for side in round_order(index):
                tree = self.trees[side]
                cwd = tree.path / "server"
                self.runner.clean(sorted(tree.packages), cwd)
                result, built = self.runner.build(
                    "test", ["--no-run", "--lib", "--tests", "--timings", *self.test_args(tree)], cwd, COLD_ENV)
                executables = sum(1 for a in built if a.get("executable") and a.get("profile", {}).get("test"))
                units = unit_data((self.runner.target / "cargo-timings" / "cargo-timing.html").read_text(
                    encoding="utf-8", errors="replace"))
                mine = [u for u in units if u["name"] in tree.packages]
                self.sample("test", "oltp", side, index, result, test_executables=executables, workspace_units=mine)

    def test_args(self, tree: Tree) -> list[str]:
        args = ["-p", SERVER]
        for name in OLTP_PACKAGES:
            if name in tree.packages:
                args += ["-p", name]
        return [*args, *SERVER_OLTP]

    def edit(self, probes) -> None:
        for tree in self.trees.values():  # the edit loop starts from cold, non-incremental outputs
            self.runner.clean(sorted(tree.packages), tree.path / "server")
        originals = {}
        previous = None
        try:
            for probe, paths in probes:
                owners = {side: self.trees[side].owner(path) for side, path in paths.items()}
                for side, path in paths.items():
                    originals.setdefault((side, path), (self.trees[side].path / path).read_bytes())
                # An unmeasured warm-up round (index -1) brings every build below up to date. After a
                # probe in the server crate, every one of them already is, so the next server-crate
                # probe needs none.
                settled = previous is not None and all(
                    owner == SERVER and previous[side] == SERVER for side, owner in owners.items())
                previous = owners
                for index in range(0 if settled else -1, self.rounds):
                    for side in round_order(max(index, 0)):
                        self.edit_round(probe, side, paths[side], owners[side], index)
        finally:
            for (side, path), data in originals.items():
                (self.trees[side].path / path).write_bytes(data)

    def edit_round(self, probe: Probe, side: str, path: str, owner: str, index: int) -> None:
        tree = self.trees[side]
        cwd = tree.path / "server"
        file = tree.path / path
        original = file.read_bytes().split(b"\n#[allow(dead_code)]\nconst BUILD_TIMES_PROBE")[0]

        def edit(value: int) -> None:
            file.write_bytes(original + f"\n#[allow(dead_code)]\nconst BUILD_TIMES_PROBE: u32 = {value};\n".encode())

        edit(index + 1)
        check, check_built = self.runner.build("check", ["--lib", *package_args(tree, owner)], cwd, EDIT_ENV)
        test, _ = self.runner.build("test", ["--no-run", "--lib", *package_args(tree, owner)], cwd, EDIT_ENV)
        if owner == SERVER:
            graph, graph_built = check, check_built
        else:
            # The OLTP graph above the owner: what an edit here rebuilds before the server links.
            edit(index + 1001)
            graph, graph_built = self.runner.build("check", ["--lib", *package_args(tree, SERVER)], cwd, EDIT_ENV)
        rebuilt = sorted({package_name(a["package_id"]) for a in graph_built if not a.get("fresh")}
                         & set(tree.packages))
        if index < 0:
            return
        if owner not in rebuilt:
            raise MeasurementError(f"edit {probe.name} on the {side}: {owner} was not rebuilt after editing {path}")
        for kind, result in (("check", check), ("test-no-run", test)):
            self.sample("edit", f"{probe.name}-{kind}", side, index, result, path=path, owner=owner)
        if owner != SERVER:
            self.sample("edit", f"{probe.name}-check-oltp-graph", side, index, graph, path=path, owner=owner)
        previous = self.rebuilds.setdefault(side, {}).get(probe.name)
        if previous and previous["rebuilt"] != rebuilt:
            raise MeasurementError(f"edit {probe.name} on the {side}: rebuild set changed between rounds "
                                   f"({previous['rebuilt']} then {rebuilt})")
        self.rebuilds[side][probe.name] = {"path": path, "owner": owner, "rebuilt": rebuilt}

    def extra(self) -> None:
        for side, tree in self.trees.items():
            cwd = tree.path / "server"
            row = {"duplicates": {}, "test_targets": {}}
            for feature_set in FEATURE_SETS:
                if feature_set.manifest != "server":
                    continue
                output = self.runner.capture(
                    ["tree", "--locked", "-d", "-e", "normal", *feature_set.args], cwd)
                row["duplicates"][feature_set.label] = duplicates(output)
                features = set(feature_set.args[-1].split(","))
                row["test_targets"][feature_set.label] = test_targets(tree.metadata, SERVER, features)
            self.extras[side] = row
            olap = row["test_targets"]["olap,s3"]
            self.log(f"nabedra-build-extra: {side} olap-test-binaries={olap} "
                     f"duplicates(oltp)={len(row['duplicates']['oltp'])}", flush=True)


# Checkouts and context.


def git(args, cwd=ROOT) -> str:
    return subprocess.run(["git", *args], cwd=cwd, text=True, capture_output=True, check=True).stdout.strip()


def revision(ref: str) -> str:
    try:
        return git(["rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}"])
    except subprocess.CalledProcessError as error:
        raise MeasurementError(f"unknown revision {ref!r}: {error.stderr.strip()}") from error


@contextmanager
def checkout(path: Path, sha: str):
    git(["worktree", "add", "--quiet", "--detach", str(path), sha])
    try:
        yield path
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(path)], cwd=ROOT)


def context(runner: Runner, tree: Tree, work: Path) -> dict:
    cpu = ""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        cpu = platform.processor()
    rustc = subprocess.run(["rustc", "-vV"], cwd=tree.path / "server", text=True, capture_output=True).stdout
    usage = shutil.disk_usage(work)
    return {
        "cpu": cpu,
        "nproc": os.cpu_count(),
        "rustc": rustc.strip(),
        "cargo_build_jobs": os.environ.get("CARGO_BUILD_JOBS", ""),
        "jobs_flag": runner.jobs,
        "platform": platform.platform(),
        "disk_free_gb": round(usage.free / 2**30, 1),
        "github_actions": os.environ.get("GITHUB_ACTIONS") == "true",
        "load": list(os.getloadavg()) if hasattr(os, "getloadavg") else [],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", required=True, help="base revision (the pre-split baseline)")
    parser.add_argument("--head", required=True, help="head revision")
    parser.add_argument("--rounds", type=int, default=DEFAULT_ROUNDS, help="measured rounds per step (default 3)")
    parser.add_argument("--steps", default=",".join(STEPS), help=f"comma-separated subset of {','.join(STEPS)}")
    parser.add_argument("--feature-set", action="append", choices=[f.label for f in FEATURE_SETS],
                        help="cold step: only these feature sets (repeatable, default: all)")
    parser.add_argument("--probe", action="append", choices=[p.name for p in PROBES],
                        help="edit step: only these probes (repeatable, default: every probe that applies)")
    parser.add_argument("--cargo", default="cargo", help="Cargo executable (a slot wrapper's wait counts as time)")
    parser.add_argument("--jobs", type=int, help="pass -j to Cargo (default: CARGO_BUILD_JOBS or Cargo's)")
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR,
                        help="parent for the worktrees and the shared target (default: sibling <checkout>-build-times)")
    parser.add_argument("--record", type=Path, help="write the measurements as JSON")
    args = parser.parse_args(argv)
    steps = [step.strip() for step in args.steps.split(",") if step.strip()]
    if not steps or any(step not in STEPS for step in steps):
        parser.error(f"--steps must be a subset of {','.join(STEPS)}")
    if args.rounds < 1:
        parser.error("--rounds must be at least 1")

    started = time.monotonic()
    record = {"schema": "nabedra.build-times/1", "steps": steps, "rounds": args.rounds}
    status = 0
    try:
        base, head = revision(args.base), revision(args.head)
        record.update(base=base, head=head)
        print(f"build_times: base={base} head={head} steps={','.join(steps)} rounds={args.rounds}", flush=True)
        args.work_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="nabedra-build-times-", dir=args.work_dir) as scratch:
            work = Path(scratch)
            runner = Runner(args.cargo, work / "target", args.jobs)
            with checkout(work / "base", base) as base_path, checkout(work / "head", head) as head_path:
                trees = {"base": Tree("base", base_path, base), "head": Tree("head", head_path, head)}
                for tree in trees.values():
                    tree.metadata = json.loads(runner.capture(
                        ["metadata", "--format-version", "1", "--no-deps", "--offline"], tree.path / "server"))
                record["context"] = context(runner, trees["head"], work)
                record["packages"] = {side: sorted(tree.packages) for side, tree in trees.items()}
                session = Session(runner, trees, args.rounds)
                for step in steps:
                    session.mark_load(step, "before")
                    if step == "cold":
                        sets = [f for f in FEATURE_SETS if not args.feature_set or f.label in args.feature_set]
                        session.cold(sets)
                    elif step == "test":
                        session.test()
                    elif step == "edit":
                        session.edit(resolve_probes(trees, args.probe))
                    elif step == "extras":
                        session.extra()
                    session.mark_load(step, "after")
                    record["context"]["disk_free_gb_after_" + step] = round(shutil.disk_usage(work).free / 2**30, 1)
                record["samples"] = session.samples
                record["summary"] = summarize(session.samples)
                record["load"] = session.loads
                record["rebuild_sets"] = session.rebuilds
                record["extras"] = session.extras
                if "edit" in steps:
                    rules = evaluate_rules(session.rebuilds.get("head", {}), set(trees["head"].packages))
                    record["rebuild_rules"] = rules
                    for rule in rules:
                        print(f"nabedra-rebuild-rule: {rule['probe']} {rule['result']}"
                              + (f" ({rule['reason']})" if rule.get("reason") else ""), flush=True)
                    if any(rule["result"] == "fail" for rule in rules):
                        status = 1
                for side, rows in session.rebuilds.items():
                    for name, row in rows.items():
                        print(f"nabedra-rebuild-set: {side} {name} {row['path']} -> {' '.join(row['rebuilt'])}")
                for row in record["summary"]:
                    ratio = f" ratio={row['ratio']}" if row["ratio"] is not None else ""
                    print(f"nabedra-build-time: {row['step']}/{row['label']} {row['base_s']} {row['head_s']}{ratio}",
                          flush=True)
    except (MeasurementError, OSError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"build_times: measurement incomplete: {error}", file=sys.stderr, flush=True)
        record["error"] = str(error)
        status = 2
    record["elapsed_s"] = round(time.monotonic() - started, 1)
    record["result"] = {0: "complete", 1: "rebuild-rule-failed", 2: "incomplete"}[status]
    if args.record:
        args.record.parent.mkdir(parents=True, exist_ok=True)
        args.record.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        print(f"build_times: wrote {args.record}")
    print(f"build_times: {record['result']} in {record['elapsed_s']}s")
    return status


if __name__ == "__main__":
    sys.exit(main())

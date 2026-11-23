#!/usr/bin/env python3
"""Validate, verify and search the reverted-fix patches in server/sim-oltp/bugs/.

Each directory under server/sim-oltp/bugs/ holds one reverted fix: `bug.toml` (the manifest) and
`revert.patch` (a diff against the current tree that brings the bug back, with no test hunks).
Product source never contains the buggy code. Every command that applies a patch does it in a
scratch worktree created by this script, never in the shipped tree.

Commands:

  check [ids]    Cheap (T0, T1): validate every manifest against its bug record, resolve the fix
                 SHAs, confirm the patch touches only product files and applies to HEAD
                 (`git apply --check` against HEAD's tree through a private index), confirm that a
                 clean port changes the same lines as the reversed fix in every product file the
                 fix changed, and that a hand port has
                 regression evidence recorded for its current changed lines.
  verify [ids]   Heavy: build HEAD in a scratch worktree, run each bug's original regression test,
                 then apply each patch and run it again. A bug is verified when the test passes
                 without the patch and fails with it (a build failure or a missing test is never a
                 failure of the test).
  run [ids]      The seed search (`oltp-sim-bugs-<set>`). It needs the nabedra-sim-oltp harness.
                 Until server/sim-oltp/Cargo.toml exists it prints `not-run: sim-harness-missing`
                 and exits 0, which the check records as not-run, never as a pass.
  digest <id>    Print the changed-lines digest to record in a manifest's evidence.

Use `--set <name>` instead of ids to select the manifests of one set (direct, queued, topology,
membership, resources). Exit status: 0 success, 1 failure, 2 usage or environment error.
"""

import argparse
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib

REPO = Path(__file__).resolve().parents[2]
BUGS_DIR = Path("server/sim-oltp/bugs")
RECORDS_DIR = Path("docs/testing/bugs")
REGISTERS_DIR = Path("docs/testing/guarantees")
HARNESS_MANIFEST = Path("server/sim-oltp/Cargo.toml")
SCHEMA = "nabedra.sim-bug/1"
SETS = ("direct", "queued", "topology", "membership", "resources")
PORTS = ("clean", "hand")
REGRESSION_KINDS = ("lib-test", "acceptance")
SHA = re.compile(r"[0-9a-f]{40}")
ID = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
TEST_PATH = re.compile(r"[a-z_][a-z0-9_]*(?:::[a-z_][a-z0-9_]*)+")
SCENARIO = re.compile(r"O[0-9]{2}")
GUARANTEE_ROW = re.compile(r"^\|\s*([A-Z][A-Z-]*-G[0-9]{2})\s*\|", re.MULTILINE)
# A patch may only touch product sources: never tests, test data or the harness itself.
TEST_FILE = re.compile(r"(^|/)(tests?|testdata|fixtures)(/|$)|(^|/)tests?\.rs$|_tests?\.rs$|(^|/)test_[^/]*\.py$")
LIBTEST_RESULT = re.compile(r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;", re.MULTILINE)
NOT_RUN_MARKER = "not-run: sim-harness-missing"
# Library tests run with the features the workflow's shared OLTP build uses.
LIB_TEST_FEATURES = ["--no-default-features", "--features", "oltp"]

MANIFEST_FIELDS = {
    "schema": str, "id": str, "bug": str, "set": str, "phase": int, "class": str,
    "guarantees": list, "scenarios": list, "fix_commits": list, "reverts": str, "port": str,
    "port_notes": str, "workload": str, "profile": list, "expect": list, "seeds": list,
    "found_seed": int, "regression": dict,
}
REQUIRED = MANIFEST_FIELDS.keys() - {"port_notes", "found_seed"}
REGRESSION_FIELDS = {"kind": str, "test": str, "scenario": str, "mode": str,
                     "failure_contains": list, "timeout_s": int, "attempts": int, "evidence": dict}
EVIDENCE_FIELDS = {"digest": str, "patched": dict, "clean": dict}
RUN_FIELDS = {"run": str, "sha": str, "result": str, "output": str, "note": str}


class Failure(Exception):
    pass


@dataclass
class Bug:
    path: Path
    manifest: dict
    patch: str
    errors: list = field(default_factory=list)

    @property
    def id(self):
        return self.manifest.get("id", self.path.name)

    @property
    def regression(self):
        return self.manifest.get("regression", {})


# --------------------------------------------------------------------------------------------
# Git and patch helpers.

def git(repo, *args, env=None, input=None, check=True):
    result = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True,
                            env=env, input=input)
    if check and result.returncode:
        raise Failure(f"git {' '.join(args)}: {result.stderr.strip() or result.stdout.strip()}")
    return result


def patch_files(patch):
    """Files a unified diff touches, from its `diff --git a/x b/y` headers."""
    files = []
    for line in patch.splitlines():
        if line.startswith("diff --git "):
            parts = line.split()
            for part in parts[2:4]:
                name = part[2:] if part[:2] in ("a/", "b/") else part
                if name not in files:
                    files.append(name)
    return files


def changed_lines(patch):
    """{file: (removed lines, added lines)}, ignoring context, hunk positions and blob ids."""
    out, current = {}, None
    for line in patch.splitlines():
        if line.startswith("diff --git "):
            current = line.split()[-1][2:]
            out.setdefault(current, ([], []))
        elif current is None or line.startswith(("--- ", "+++ ")):
            continue
        elif line.startswith("-"):
            out[current][0].append(line[1:])
        elif line.startswith("+"):
            out[current][1].append(line[1:])
    return {name: lines for name, lines in out.items() if lines[0] or lines[1]}


def digest(patch):
    """SHA-256 over the changed lines only, so a context refresh keeps recorded evidence valid."""
    h = hashlib.sha256()
    for name, (removed, added) in sorted(changed_lines(patch).items()):
        h.update(f"F {name}\n".encode())
        for line in removed:
            h.update(f"- {line}\n".encode())
        for line in added:
            h.update(f"+ {line}\n".encode())
    return h.hexdigest()


def apply_check_head(repo, patch):
    """`git apply --check` against HEAD's tree through a private index. Touches no worktree."""
    with tempfile.TemporaryDirectory(prefix="sim-bugs-index-") as tmp:
        env = dict(os.environ, GIT_INDEX_FILE=str(Path(tmp) / "index"))
        git(repo, "read-tree", "HEAD", env=env)
        result = git(repo, "apply", "--check", "--cached", "-", env=env, input=patch, check=False)
    return result.returncode == 0, (result.stderr or result.stdout).strip()


# --------------------------------------------------------------------------------------------
# Loading and validation.

def load_bug_records(repo):
    records = {}
    for path in sorted((repo / RECORDS_DIR).glob("*.toml")):
        with path.open("rb") as stream:
            record = tomllib.load(stream)
        if isinstance(record.get("id"), str):
            records[record["id"]] = record
    return records


def guarantee_ids(repo):
    ids = set()
    for path in sorted((repo / REGISTERS_DIR).glob("*.md")):
        ids.update(GUARANTEE_ROW.findall(path.read_text(encoding="utf-8")))
    return ids


def load_bugs(repo):
    bugs, errors = [], []
    root = repo / BUGS_DIR
    if not root.is_dir():
        return bugs, [f"{BUGS_DIR}: no bug directory"]
    for path in sorted(p for p in root.iterdir() if p.is_dir()):
        name = path.relative_to(repo).as_posix()
        try:
            with (path / "bug.toml").open("rb") as stream:
                manifest = tomllib.load(stream)
            patch = (path / "revert.patch").read_text(encoding="utf-8")
        except (OSError, ValueError) as error:
            errors.append(f"{name}: {error}")
            continue
        bugs.append(Bug(path, manifest, patch))
    if not bugs and not errors:
        errors.append(f"{BUGS_DIR}: no bug directories")
    return bugs, errors


def typed(problems, where, data, fields, required=()):
    for key in required:
        if key not in data:
            problems.append(f"{where}: missing field {key}")
    for key, value in data.items():
        if key not in fields:
            problems.append(f"{where}: unknown field {key}")
        elif type(value) is not fields[key]:
            problems.append(f"{where}: {key} must be a {fields[key].__name__}")


def strings(value):
    return isinstance(value, list) and all(isinstance(v, str) and v.strip() for v in value)


def validate_manifest(bug, records, known_guarantees):
    """Field and cross-reference checks that need no Git."""
    m, problems = bug.manifest, []
    typed(problems, "bug.toml", m, MANIFEST_FIELDS, REQUIRED)
    if problems:
        return problems
    if m["schema"] != SCHEMA:
        problems.append(f"schema must be {SCHEMA!r}")
    if m["id"] != bug.path.name or not ID.fullmatch(m["id"]):
        problems.append("id must be kebab-case and equal to the directory name")
    if m["set"] not in SETS:
        problems.append(f"set must be one of {', '.join(SETS)}")
    if m["port"] not in PORTS:
        problems.append("port must be clean or hand")
    if m["port"] == "hand" and not m.get("port_notes", "").strip():
        problems.append("a hand port needs port_notes naming the original hunk")
    for key in ("guarantees", "scenarios", "fix_commits", "profile", "expect"):
        if not m[key] or not strings(m[key]):
            problems.append(f"{key} must be a nonempty array of nonempty strings")
    if problems:
        return problems
    seeds = m["seeds"]
    if not (len(seeds) == 2 and all(type(s) is int for s in seeds) and 1 <= seeds[0] <= seeds[1]):
        problems.append("seeds must be [first, last] with 1 <= first <= last")
    elif m["phase"] == 10 and seeds[1] - seeds[0] >= 10_000:
        problems.append("phase 10 bug search may use at most 10000 seeds")
    elif "found_seed" in m and not seeds[0] <= m["found_seed"] <= seeds[1]:
        problems.append("found_seed must lie inside seeds")
    for sha in m["fix_commits"] + [m["reverts"]]:
        if not SHA.fullmatch(sha):
            problems.append(f"invalid full commit SHA {sha!r}")
    if m["reverts"] not in m["fix_commits"]:
        problems.append("reverts must be one of fix_commits")
    for g in m["guarantees"]:
        if g not in known_guarantees:
            problems.append(f"guarantee {g} is not in the guarantee registers")
    record = records.get(m["bug"])
    if record is None:
        problems.append(f"bug {m['bug']!r} has no record in {RECORDS_DIR}")
    else:
        if record.get("class") != m["class"]:
            problems.append(f"class {m['class']!r} differs from the bug record's {record.get('class')!r}")
        for key in ("guarantees", "scenarios", "fix_commits"):
            extra = sorted(set(m[key]) - set(record.get(key, [])))
            if extra:
                problems.append(f"{key} {extra} are not in the bug record")
    problems.extend(validate_regression(bug))
    problems.extend(validate_patch_paths(bug.patch))
    return problems


def validate_regression(bug):
    r, problems = bug.regression, []
    typed(problems, "regression", r, REGRESSION_FIELDS, ("kind",))
    if problems:
        return problems
    kind = r["kind"]
    if kind not in REGRESSION_KINDS:
        problems.append(f"regression.kind must be one of {', '.join(REGRESSION_KINDS)}")
    elif kind == "lib-test" and not TEST_PATH.fullmatch(r.get("test", "")):
        problems.append("regression.test must be a full library test path")
    elif kind == "acceptance" and not SCENARIO.fullmatch(r.get("scenario", "")):
        problems.append("regression.scenario must be an acceptance scenario such as O15")
    if r.get("mode", "on") not in ("on", "off"):
        problems.append("regression.mode must be on or off")
    if "failure_contains" in r and not strings(r["failure_contains"]):
        problems.append("regression.failure_contains must be an array of nonempty strings")
    if kind == "acceptance" and not r.get("failure_contains"):
        problems.append("an acceptance regression needs failure_contains for its specific failure")
    if not 1 <= r.get("attempts", 1) <= 10:
        problems.append("regression.attempts must be between 1 and 10")
    evidence = r.get("evidence")
    if evidence is not None:
        typed(problems, "regression.evidence", evidence, EVIDENCE_FIELDS, ("digest", "patched"))
        for side, expected in (("patched", "failed"), ("clean", "passed")):
            run = evidence.get(side)
            if isinstance(run, dict):
                typed(problems, f"regression.evidence.{side}", run, RUN_FIELDS, ("run", "sha", "result"))
                if run.get("result") != expected:
                    problems.append(f"regression.evidence.{side}.result must be {expected!r}")
                if not SHA.fullmatch(str(run.get("sha", ""))):
                    problems.append(f"regression.evidence.{side}.sha must be a full commit SHA")
    if bug.manifest.get("port") == "hand" and evidence is None:
        problems.append("a hand port counts only with regression evidence that its test fails")
    return problems


def path_problem(name):
    """Why a patch may not touch `name`, or None for a product source."""
    if not name.startswith("server/") or name.startswith(("server/sim-oltp/", "server/sim/",
                                                           "server/check/", "server/fuzz/")):
        return f"revert.patch touches {name}, outside the product sources"
    if TEST_FILE.search(name):
        return f"revert.patch touches the test file {name}"
    if name.endswith(("Cargo.toml", "Cargo.lock")):
        return f"revert.patch touches the manifest {name}"
    return None


def validate_patch_paths(patch):
    files = patch_files(patch)
    if not files:
        return ["revert.patch is empty"]
    return [problem for problem in map(path_problem, files) if problem]


def validate_git(bug, repo):
    """Checks that need history: SHAs resolve, the patch applies and its changed lines match."""
    m, problems = bug.manifest, []
    missing = [sha for sha in m["fix_commits"]
               if git(repo, "cat-file", "-e", f"{sha}^{{commit}}", check=False).returncode]
    problems.extend(f"fix commit {sha} does not resolve; fetch full history" for sha in missing)
    for sha in set(m["fix_commits"]) - set(missing):
        if git(repo, "merge-base", "--is-ancestor", sha, "HEAD", check=False).returncode:
            problems.append(f"fix commit {sha} is not an ancestor of HEAD")
    ok, detail = apply_check_head(repo, bug.patch)
    if not ok:
        problems.append(f"revert.patch no longer applies to HEAD; refresh it: {detail}")
    if m["port"] == "clean" and m["reverts"] not in missing:
        files = patch_files(bug.patch)
        reverse = git(repo, "diff", m["reverts"], f"{m['reverts']}^", "--", *files).stdout
        if changed_lines(reverse) != changed_lines(bug.patch):
            problems.append(f"port = clean, but the patch does not change exactly the lines of "
                            f"`git diff {m['reverts'][:8]} {m['reverts'][:8]}^` on its files")
        # A clean revert also covers every product file the fix changed, or it is a hand port.
        fixed = git(repo, "diff", "--name-only", f"{m['reverts']}^", m["reverts"]).stdout.split()
        omitted = sorted(name for name in fixed if path_problem(name) is None and name not in files)
        if omitted:
            problems.append(f"port = clean, but the patch leaves out product files the fix changed: "
                            f"{', '.join(omitted)}")
    evidence = bug.regression.get("evidence")
    if evidence and evidence.get("digest") != digest(bug.patch):
        problems.append("the patch's changed lines differ from those its regression evidence "
                        f"used (digest {digest(bug.patch)}); verify it again and record the run")
    if bug.regression.get("kind") == "lib-test":
        name = bug.regression["test"].rsplit("::", 1)[-1]
        found = git(repo, "grep", "-q", "-E", rf"fn {name}\b", "HEAD", "--", "server", check=False)
        if found.returncode:
            problems.append(f"regression test {name} is not defined at HEAD")
    return problems


def select(bugs, ids, set_name):
    if ids and set_name:
        raise Failure("give bug ids or --set, not both")
    if set_name:
        chosen = [b for b in bugs if b.manifest.get("set") == set_name]
        if not chosen:
            raise Failure(f"no bugs in set {set_name!r}")
        return chosen
    if not ids:
        return bugs
    by_id = {b.id: b for b in bugs}
    unknown = [i for i in ids if i not in by_id]
    if unknown:
        raise Failure(f"unknown bug ids: {', '.join(unknown)}")
    return [by_id[i] for i in ids]


def cmd_check(repo, bugs, out):
    records, known = load_bug_records(repo), guarantee_ids(repo)
    failed = 0
    for bug in bugs:
        problems = validate_manifest(bug, records, known)
        if not problems:
            problems = validate_git(bug, repo)
        status = "FAILED" if problems else "ok"
        failed += bool(problems)
        print(f"{bug.id}: {status} ({bug.manifest.get('port', '?')} port of "
              f"{str(bug.manifest.get('reverts', '?'))[:8]}, files: "
              f"{', '.join(patch_files(bug.patch)) or 'none'})", file=out)
        for problem in problems:
            print(f"  {problem}", file=out)
    print(f"sim-bugs check: {len(bugs) - failed} of {len(bugs)} patches valid and applying to HEAD",
          file=out)
    return 1 if failed else 0


# --------------------------------------------------------------------------------------------
# Scratch worktrees and builds (verify, run).

class Scratch:
    """A detached worktree of HEAD under a temporary directory, removed on exit."""

    def __init__(self, repo, keep=False):
        self.repo, self.keep = repo, keep
        base = Path(os.environ.get("RUNNER_TEMP") or tempfile.gettempdir())
        self.path = base / f"sim-bugs-{os.getpid()}-{int(time.time())}"

    def __enter__(self):
        if self.path.resolve() == self.repo.resolve():
            raise Failure("refusing to apply patches in the shipped tree")
        git(self.repo, "worktree", "add", "--detach", "--quiet", str(self.path), "HEAD")
        return self

    def __exit__(self, *exc):
        if self.keep:
            print(f"kept scratch worktree {self.path}", file=sys.stderr)
            return
        git(self.repo, "worktree", "remove", "--force", str(self.path), check=False)
        shutil.rmtree(self.path, ignore_errors=True)

    def apply(self, bug):
        if self.path.resolve() == self.repo.resolve():
            raise Failure("refusing to apply patches in the shipped tree")
        git(self.path, "apply", "-", input=bug.patch)

    def reset(self):
        git(self.path, "checkout", "--quiet", "--", ".")
        git(self.path, "clean", "-fdq")


def run_logged(cmd, cwd, env, log, timeout=None):
    """Run cmd, tee its output to log, and return (status or None on timeout, output)."""
    print(f"$ {' '.join(cmd)}", file=log, flush=True)
    try:
        result = subprocess.run(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as error:
        output = error.stdout or ""
        output = output.decode(errors="replace") if isinstance(output, bytes) else output
        log.write(output)
        return None, output
    log.write(result.stdout)
    log.flush()
    return result.returncode, result.stdout


def build_lib_tests(args, tree, env, log, need_server):
    """Build the OLTP library test executable (and the server for acceptance); return paths."""
    cargo = args.cargo.split()
    common = ["--locked", "-j", str(args.jobs), *LIB_TEST_FEATURES]
    code, output = run_logged([*cargo, "test", *common, "--lib", "--no-run",
                               "--message-format=json-render-diagnostics"], tree / "server", env, log)
    if code != 0:
        raise Failure(f"the library test build failed (exit {code}); see the log")
    exe = None
    for line in output.splitlines():
        if not line.startswith("{"):
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if (msg.get("reason") == "compiler-artifact" and msg.get("profile", {}).get("test")
                and "lib" in msg.get("target", {}).get("kind", []) and msg.get("executable")):
            exe = msg["executable"]
    if exe is None:
        raise Failure("the build produced no library test executable")
    server = None
    if need_server:
        code, _ = run_logged([*cargo, "build", *common, "--bin", "nabedra-db"], tree / "server", env, log)
        if code != 0:
            raise Failure(f"the server build failed (exit {code}); see the log")
        server = str(Path(env["CARGO_TARGET_DIR"]) / "debug" / "nabedra-db")
    return exe, server


def classify_libtest(code, output, name):
    """'passed', 'failed', 'timeout' or 'not-run' for one exact libtest run."""
    if code is None:
        return "timeout"
    results = LIBTEST_RESULT.findall(output)
    if len(results) != 1:
        return "not-run"
    status, passed, failed = results[0]
    if (status, passed, failed, code) == ("ok", "1", "0", 0):
        return "passed"
    if (status, passed, failed, code) == ("FAILED", "0", "1", 101) and re.search(
            rf"^test {re.escape(name)} \.\.\. FAILED$", output, re.MULTILINE):
        return "failed"
    return "not-run"


def run_regression(bug, tree, exe, server, env, log):
    r = bug.regression
    env = dict(env, NABEDRA_TEST_DIRECT_OLTP=r.get("mode", "on"))
    timeout = r.get("timeout_s", 1200)
    if r["kind"] == "lib-test":
        code, output = run_logged([exe, "--exact", r["test"], "--include-ignored", "--test-threads", "1"],
                                  tree / "server", env, log, timeout)
        return classify_libtest(code, output, r["test"]), output
    out_dir = Path(env["CARGO_TARGET_DIR"]) / "sim-bugs-acceptance" / bug.id
    shutil.rmtree(out_dir, ignore_errors=True)
    code, output = run_logged([sys.executable, "scripts/acceptance_oltp.py", "--test-binary", exe,
                               "--server-binary", server, "--scenario", r["scenario"],
                               "--output", str(out_dir)], tree, env, log, timeout)
    scenario_log = out_dir / f"{r['scenario']}.log"
    if not scenario_log.is_file():
        return ("timeout" if code is None else "not-run"), output
    scenario_output = scenario_log.read_text(errors="replace")
    results = LIBTEST_RESULT.findall(scenario_output)
    output += scenario_output
    if code == 0 and results and results[-1] == ("ok", "1", "0"):
        return "passed", output
    if code == 1 and results and results[-1] == ("FAILED", "0", "1"):
        return "failed", output
    return ("timeout" if code is None else "not-run"), output


def matches_failure(bug, output):
    return all(text in output for text in bug.regression.get("failure_contains", []))


def regression_verdict(bug, tree, exe, server, env, log, patched):
    """Run the regression up to `attempts` times (a timing window may need more than one).

    Without the patch every attempt must pass. With it, the first attempt that fails with the
    expected text decides; a failure without that text is `failed-otherwise`, never `failed`.
    """
    seen = []
    for _ in range(bug.regression.get("attempts", 1)):
        status, output = run_regression(bug, tree, exe, server, env, log)
        if patched and status == "failed":
            if matches_failure(bug, output):
                return "failed", output
            status = "failed-otherwise"
        seen.append(status)
        if not patched and status != "passed":
            break
    if not patched:
        return seen[-1], output
    return ("failed-otherwise" if "failed-otherwise" in seen else seen[-1]), output


def print_regression_result(bug, output, out):
    markers = bug.regression.get("failure_contains", [])
    for line in output.splitlines():
        if LIBTEST_RESULT.match(line) or any(marker in line for marker in markers):
            print(line, file=out, flush=True)


def cmd_verify(repo, bugs, args, out):
    target = Path(args.target_dir or repo / "server/target/sim-bugs").resolve()
    log_path = Path(args.log or target / "sim-bugs-verify.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, CARGO_TARGET_DIR=str(target))
    need_server = any(b.regression.get("kind") == "acceptance" for b in bugs)
    verdicts = {}
    with log_path.open("a", encoding="utf-8") as log, Scratch(repo, args.keep) as scratch:
        tree = scratch.path
        sha = git(tree, "rev-parse", "HEAD").stdout.strip()
        print(f"sim-bugs verify on {sha}, log {log_path}", file=out, flush=True)
        exe, server = build_lib_tests(args, tree, env, log, need_server)
        clean = {}
        for bug in bugs:
            clean[bug.id], output = regression_verdict(bug, tree, exe, server, env, log, patched=False)
            print(f"{bug.id}: without the patch {clean[bug.id]}", file=out, flush=True)
            print_regression_result(bug, output, out)
        for bug in bugs:
            scratch.apply(bug)
            try:
                p_exe, p_server = build_lib_tests(args, tree, env, log, need_server)
                patched, output = regression_verdict(bug, tree, p_exe, p_server, env, log, patched=True)
                print_regression_result(bug, output, out)
            except Failure as error:
                patched = "not-run"
                print(f"{bug.id}: {error}", file=out, flush=True)
            finally:
                scratch.reset()
            verdicts[bug.id] = (clean[bug.id], patched)
            ok = clean[bug.id] == "passed" and patched == "failed"
            print(f"{bug.id}: with the patch {patched}; {'verified' if ok else 'NOT verified'} "
                  f"(digest {digest(bug.patch)})", file=out, flush=True)
    good = sum(c == "passed" and p == "failed" for c, p in verdicts.values())
    print(f"sim-bugs verify: {good} of {len(bugs)} regression tests pass without their patch and "
          f"fail with it", file=out)
    return 0 if good == len(bugs) else 1


# --------------------------------------------------------------------------------------------
# Seed search (needs nabedra-sim-oltp).
#
# Contract for the harness (P10-HARNESS implements it): the sweep test reads NABEDRA_SEED (first
# seed), NABEDRA_SEEDS (count), NABEDRA_SIM_WORKLOAD, NABEDRA_SIM_PROFILE (comma list) and
# NABEDRA_SIM_EXPECT (comma list of invariant or anomaly kinds; when set, the sweep stops at the
# first seed that reports one of them). For every seed it prints one line
#   nabedra-sim-seed: {"seed": <int>, "result": "pass" | "violation", "kinds": [<str>...]}
# A run that prints no such line for a seed it was asked to run is an error, never a pass.

SEED_LINE = re.compile(r"^(?:test [^\n]+? \.\.\. )?nabedra-sim-seed: (\{.*\})$", re.MULTILINE)


def sweep_command(args):
    return [*args.cargo.split(), "test", "--locked", "-j", str(args.jobs), "-p", "nabedra-sim-oltp",
            "--release", "--lib", "sweep", "--", "--nocapture", "--test-threads=1"]


def parse_seeds(output):
    seeds = []
    for match in SEED_LINE.finditer(output):
        try:
            entry = json.loads(match.group(1))
        except ValueError:
            continue
        if (type(entry.get("seed")) is int and entry.get("result") in ("pass", "violation")
                and strings(entry.get("kinds"))
                and bool(entry["kinds"]) == (entry["result"] == "violation")):
            seeds.append(entry)
    return seeds


def first_match(seeds, expect):
    for entry in seeds:
        if entry["result"] == "violation" and set(entry.get("kinds", [])) & set(expect):
            return entry
    return None


def sweep(args, tree, env, log, bug, first, count, expect):
    m = bug.manifest
    env = dict(env, NABEDRA_SEED=str(first), NABEDRA_SEEDS=str(count),
               NABEDRA_SIM_WORKLOAD=m["workload"], NABEDRA_SIM_PROFILE=",".join(m["profile"]),
               RUSTFLAGS=os.environ.get("RUSTFLAGS", "--cfg tokio_unstable"))
    if expect:
        env["NABEDRA_SIM_EXPECT"] = ",".join(expect)
    else:
        env.pop("NABEDRA_SIM_EXPECT", None)
    code, output = run_logged(sweep_command(args), tree / "server", env, log)
    return code, parse_seeds(output)


def cmd_run(repo, bugs, args, out):
    if git(repo, "cat-file", "-e", f"HEAD:{HARNESS_MANIFEST.as_posix()}", check=False).returncode:
        print(f"{NOT_RUN_MARKER}: {HARNESS_MANIFEST} does not exist yet, so no seed was searched "
              f"for {', '.join(b.id for b in bugs)}", file=out)
        return 0
    if cmd_check(repo, bugs, out):
        return 1
    print("Fault model: seeded OLTP simulation with one historical fix reverted at a time", file=out)
    target = Path(args.target_dir or repo / "server/target/sim-bugs").resolve()
    log_path = Path(args.log or target / "sim-bugs-run.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, CARGO_TARGET_DIR=str(target))
    found = 0
    with log_path.open("a", encoding="utf-8") as log, Scratch(repo, args.keep) as scratch:
        tree = scratch.path
        for bug in bugs:
            first, last = bug.manifest["seeds"]
            if args.seeds:
                last = min(last, first + args.seeds - 1)
            scratch.apply(bug)
            try:
                code, seeds = sweep(args, tree, env, log, bug, first, last - first + 1,
                                    bug.manifest["expect"])
            finally:
                scratch.reset()
            if code not in (0, 101):
                print(f"{bug.id}: NOT found; patched test exited {code}", file=out)
                continue
            reported = {entry["seed"] for entry in seeds}
            if len(reported) != len(seeds) or any(not first <= seed <= last for seed in reported):
                print(f"{bug.id}: NOT found; duplicate or out-of-budget seed lines in [{first}, {last}]",
                      file=out)
                continue
            hit = first_match(seeds, bug.manifest["expect"])
            if hit is None:
                why = "no seed line" if not seeds else f"{len(seeds)} seeds without {bug.manifest['expect']}"
                print(f"{bug.id}: NOT found in [{first}, {last}] ({why}, exit {code})", file=out)
                continue
            code, replay = sweep(args, tree, env, log, bug, hit["seed"], 1, [])
            if code != 0:
                clean = f"failed (exit {code})"
            elif len(replay) == 1 and replay[0]["seed"] == hit["seed"]:
                clean = replay[0]["result"]
            else:
                clean = "missing or ambiguous"
            ok = clean == "pass"
            found += ok
            print(f"{bug.id}: seed {hit['seed']} reports {sorted(hit['kinds'])} after "
                  f"{len(seeds)} seeds; clean replay {clean}; patch digest {digest(bug.patch)}; "
                  f"{'found' if ok else 'NOT found'}", file=out)
    print(f"sim-bugs run: {found} of {len(bugs)} reverted fixes found within budget", file=out)
    return 0 if found == len(bugs) else 1


# --------------------------------------------------------------------------------------------

def main(argv=None, *, repo=REPO, out=sys.stdout):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("check", "verify", "run", "digest"))
    parser.add_argument("ids", nargs="*")
    parser.add_argument("--set", dest="set_name", choices=SETS)
    parser.add_argument("--cargo", default=os.environ.get("NABEDRA_CARGO", "cargo"),
                        help="cargo command, for example cargo-slot (default: $NABEDRA_CARGO or cargo)")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--target-dir", help="shared target directory (default server/target/sim-bugs)")
    parser.add_argument("--log", help="build and test log (default <target-dir>/sim-bugs-<command>.log)")
    parser.add_argument("--seeds", type=int, help="run: cap the seeds searched per bug")
    parser.add_argument("--keep", action="store_true", help="keep the scratch worktree")
    args = parser.parse_args(argv)
    if args.jobs < 1 or (args.seeds is not None and args.seeds < 1):
        parser.error("--jobs and --seeds must be positive")
    repo = Path(repo)
    try:
        bugs, errors = load_bugs(repo)
        if errors:
            for error in errors:
                print(error, file=out)
            return 1
        chosen = select(bugs, args.ids, args.set_name)
        if args.command == "digest":
            for bug in chosen:
                print(f"{bug.id} {digest(bug.patch)}", file=out)
            return 0
        if args.command == "check":
            return cmd_check(repo, chosen, out)
        if args.command == "verify":
            if cmd_check(repo, chosen, out):
                return 1
            return cmd_verify(repo, chosen, args, out)
        return cmd_run(repo, chosen, args, out)
    except Failure as error:
        print(f"sim-bugs: {error}", file=out)
        return 2


if __name__ == "__main__":
    sys.exit(main())

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
  run [ids]      The seed search (`oltp-sim-bugs-<set>`) through the nabedra-sim-oltp sweep test.
                 It builds the unpatched sweep first. Then, per bug, in a scratch worktree, it
                 applies the patch, builds, searches the manifest's seeds for the first one that
                 reports an expected kind, replays that seed alone on the patched build (it must
                 fail the same way, deterministically, with a history), resets the tree and
                 replays the seed on the unpatched build, which must pass deterministically. The
                 unpatched build must also report no expected kind on any seed tried, so that a
                 failure it already has is never credited to the patch. The hit is then only a
                 candidate, until the manifest's [detection] table records the reviewed mechanism
                 for that exact seed, trace hash and history. It writes sim-bugs-run.json (seeds tried, found seed, kinds, trace hashes, history
                 digests, patch SHA-256) beside the harness artifacts. While the harness or a
                 bug's workload is missing it prints `not-run: sim-harness-missing` and exits 0,
                 which the check records as not-run, never as a pass.
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
# Phase 8 moved these product trees into their own crates. Patches use the current paths; the
# fix commits use the old ones, so a clean port is compared through this map.
MOVED = (("server/src/oltp_replication/", "server/oltp-replication/src/oltp_replication/"),
         ("server/src/oltp/", "server/oltp-storage/src/oltp/"))
# Library tests run with the features the workflow's shared OLTP build uses.
LIB_TEST_FEATURES = ["--no-default-features", "--features", "oltp"]

MANIFEST_FIELDS = {
    "schema": str, "id": str, "bug": str, "set": str, "phase": int, "class": str,
    "guarantees": list, "scenarios": list, "fix_commits": list, "reverts": str, "port": str,
    "port_notes": str, "workload": str, "profile": list, "expect": list, "seeds": list,
    "found_seed": int, "regression": dict, "detection": dict,
}
REQUIRED = MANIFEST_FIELDS.keys() - {"port_notes", "found_seed", "detection"}
# A reviewed diagnosis of one hit: its identity, and how its history and trace show the reverted
# fix's mechanism rather than another defect that reports the same kind.
DETECTION_FIELDS = {"seed": int, "trace_hash": str, "history_sha256": str, "mechanism": str,
                    "detector": str}
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


def current_path(name):
    """`name` after the phase 8 crate moves (a fix commit names the old path)."""
    for old, new in MOVED:
        if name.startswith(old):
            return new + name[len(old):]
    return name


def historical_path(repo, name, commit):
    """`name` as `commit` called it: undo a crate move when the old path exists there."""
    for old, new in MOVED:
        if name.startswith(new):
            candidate = old + name[len(new):]
            if not git(repo, "cat-file", "-e", f"{commit}:{candidate}", check=False).returncode:
                return candidate
    return name


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
    # Phase 10's bugs sit directly in the directory, later phases in phase<N>/.
    groups = [(p, int(p.name[5:])) for p in root.iterdir() if p.is_dir() and re.fullmatch(r"phase[0-9]+", p.name)]
    paths = [(p, 10) for p in root.iterdir() if p.is_dir() and p not in dict(groups)]
    paths += [(p, phase) for group, phase in groups for p in group.iterdir() if p.is_dir()]
    seen = set()
    for path, phase in sorted(paths):
        name = path.relative_to(repo).as_posix()
        try:
            with (path / "bug.toml").open("rb") as stream:
                manifest = tomllib.load(stream)
            patch = (path / "revert.patch").read_text(encoding="utf-8")
        except (OSError, ValueError) as error:
            errors.append(f"{name}: {error}")
            continue
        if manifest.get("phase") != phase:
            errors.append(f"{name}: phase must be {phase} in this directory")
        if path.name in seen:
            errors.append(f"{name}: the id is used twice")
        seen.add(path.name)
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


def validate_manifest(bug, records, known_guarantees, require_evidence=True):
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
        if record.get("status") == "open":
            problems.append(f"bug {m['bug']!r} is open; it has no fix to revert")
        if record.get("class") != m["class"]:
            problems.append(f"class {m['class']!r} differs from the bug record's {record.get('class')!r}")
        for key in ("guarantees", "scenarios", "fix_commits"):
            extra = sorted(set(m[key]) - set(record.get(key, [])))
            if extra:
                problems.append(f"{key} {extra} are not in the bug record")
    if "detection" in m:
        typed(problems, "detection", m["detection"], DETECTION_FIELDS, DETECTION_FIELDS)
        if m["detection"].get("detector") not in m["expect"]:
            problems.append("detection.detector must be one of expect: the check that found the bug")
        if not m["detection"].get("mechanism", "").strip():
            problems.append("detection.mechanism must say how the history shows the reverted fix")
    problems.extend(validate_regression(bug, require_evidence))
    problems.extend(validate_patch_paths(bug.patch))
    return problems


def validate_regression(bug, require_evidence=True):
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
    if require_evidence and bug.manifest.get("port") == "hand" and evidence is None:
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


def validate_git(bug, repo, require_evidence=True):
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
        old = [historical_path(repo, name, m["reverts"]) for name in files]
        reverse = git(repo, "diff", m["reverts"], f"{m['reverts']}^", "--", *old).stdout
        moved = {current_path(name): lines for name, lines in changed_lines(reverse).items()}
        if moved != changed_lines(bug.patch):
            problems.append(f"port = clean, but the patch does not change exactly the lines of "
                            f"`git diff {m['reverts'][:8]} {m['reverts'][:8]}^` on its files")
        # A clean revert also covers every product file the fix changed, or it is a hand port.
        fixed = git(repo, "diff", "--name-only", f"{m['reverts']}^", m["reverts"]).stdout.split()
        omitted = sorted(name for name in fixed
                         if path_problem(name) is None and current_path(name) not in files)
        if omitted:
            problems.append(f"port = clean, but the patch leaves out product files the fix changed: "
                            f"{', '.join(omitted)}")
    evidence = bug.regression.get("evidence")
    if require_evidence and evidence and evidence.get("digest") != digest(bug.patch):
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


def cmd_check(repo, bugs, out, require_evidence=True):
    """`require_evidence=False` is verify's precondition: verify is what records new evidence."""
    records, known = load_bug_records(repo), guarantee_ids(repo)
    failed = 0
    for bug in bugs:
        problems = validate_manifest(bug, records, known, require_evidence)
        if not problems:
            problems = validate_git(bug, repo, require_evidence)
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


def test_executable(output, target=None):
    """The library test executable in cargo's `--message-format=json` output."""
    exe = None
    for line in output.splitlines():
        if not line.startswith("{"):
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        kind = msg.get("target", {})
        if (msg.get("reason") == "compiler-artifact" and msg.get("profile", {}).get("test")
                and "lib" in kind.get("kind", []) and msg.get("executable")
                and target in (None, kind.get("name"))):
            exe = msg["executable"]
    if exe is None:
        raise Failure("the build produced no library test executable")
    return exe


def build_lib_tests(args, tree, env, log, need_server, package=None):
    """Build the OLTP library test executable (and the server for acceptance); return paths."""
    cargo = args.cargo.split()
    features = ["-p", package] if package else LIB_TEST_FEATURES
    common = ["--locked", "-j", str(args.jobs), *features]
    code, output = run_logged([*cargo, "test", *common, "--lib", "--no-run",
                               "--message-format=json-render-diagnostics"], tree / "server", env, log)
    if code != 0:
        raise Failure(f"the library test build failed (exit {code}); see the log")
    exe = test_executable(output)
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
    Returns the verdict, the last attempt's output and every attempt's status.
    """
    seen = []
    for _ in range(bug.regression.get("attempts", 1)):
        status, output = run_regression(bug, tree, exe, server, env, log)
        if patched and status == "failed":
            if matches_failure(bug, output):
                return "failed", output, [*seen, "failed"]
            status = "failed-otherwise"
        seen.append(status)
        if not patched and status != "passed":
            break
    if not patched:
        return seen[-1], output, seen
    return ("failed-otherwise" if "failed-otherwise" in seen else seen[-1]), output, seen


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
        replication = (tree / "server/oltp-replication/src/oltp_replication").is_dir()
        packages = {
            bug.id: "nabedra-oltp-replication"
            if replication and bug.regression.get("test", "").startswith("oltp_replication::")
            else None
            for bug in bugs
        }

        def build(package):
            server_build = need_server and package is None
            if replication and server_build:
                fixture, _ = build_lib_tests(args, tree, env, log, False, "nabedra-oltp-replication")
                env["NABEDRA_OLTP_REPLICATION_TEST_EXE"] = fixture
            return build_lib_tests(args, tree, env, log, server_build, package)

        clean_builds = {package: build(package) for package in dict.fromkeys(packages.values())}
        clean = {}
        for bug in bugs:
            exe, server = clean_builds[packages[bug.id]]
            clean[bug.id], output, seen = regression_verdict(bug, tree, exe, server, env, log, patched=False)
            print(f"{bug.id}: without the patch {clean[bug.id]}", file=out, flush=True)
            print(f"{bug.id}: unpatched attempts {seen}", file=out, flush=True)
            print_regression_result(bug, output, out)
        for bug in bugs:
            scratch.apply(bug)
            try:
                p_exe, p_server = build(packages[bug.id])
                patched, output, seen = regression_verdict(bug, tree, p_exe, p_server, env, log, patched=True)
                print(f"{bug.id}: patched attempts {seen}", file=out, flush=True)
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
# Seed search (`oltp-sim-bugs-<set>`), through the nabedra-sim-oltp sweep test.
#
# Interface of `sweep::tests::sweep` in server/sim-oltp/src/sweep.rs, read from the environment:
# NABEDRA_SIM_WORKLOAD, NABEDRA_SIM_PROFILE (comma list, unset for none), NABEDRA_SEED (first
# seed), NABEDRA_SEEDS (count), NABEDRA_SIM_JOBS, NABEDRA_SIM_CHECK_ID (label) and
# NABEDRA_SIM_ARTIFACTS.
# - With NABEDRA_SIM_EXPECT (comma list of invariant or anomaly kinds) it searches the seeds in
#   order and stops at the first seed that reports one of them. It exits 0 when one is found and
#   fails when the budget runs out.
# - Without it, it runs the seeds as an ordinary sweep, replays them per NABEDRA_SIM_VERIFY_REPLAY
#   (a failing seed always runs twice), and writes <artifacts>/sweep-report.json (trace hash, event
#   count, violation kinds and determinism per seed) and <artifacts>/<label>-seed-<seed>/ with
#   history.jsonl (plus summary.json and trace-tail.txt for a failing seed).
# Either way it prints, for every seed it ran,
#   nabedra-sim-seed: {"seed": <int>, "result": "pass" | "violation", "kinds": [<str>...]}
# A run that prints no such line for a seed it was asked to run is an error, never a pass.

SEED_LINE = re.compile(r"^(?:test [^\n]+? \.\.\. )?nabedra-sim-seed: (\{.*\})$", re.MULTILINE)
SWEEP_PACKAGE = "nabedra-sim-oltp"
SWEEP_TARGET = "nabedra_sim_oltp"
SWEEP_TEST = "sweep::tests::sweep"
# The harness needs this stack for the real worker bootstrap (server/sim-oltp/README.md).
HARNESS_STACK = "16777216"
# The sweep test prints this error through `expect`, so its quotes are escaped.
UNAVAILABLE_WORKLOAD = re.compile(r'OLTP workload [\\]?"([a-z0-9-]+)[\\]?" is unavailable')
# Inherited variables that would change what the harness runs. Each run sets its own.
HARNESS_PREFIXES = ("NABEDRA_SIM_", "NABEDRA_SEED", "NABEDRA_PLANTED_")


@dataclass
class Plan:
    """What one bug's search runs: its manifest, or a rehearsal's overrides."""
    workload: str
    profile: list
    expect: list
    first: int
    last: int
    defect: str | None = None

    @classmethod
    def of(cls, bug, args):
        first, last = bug.manifest["seeds"]
        if args.seeds:
            last = min(last, first + args.seeds - 1)
        profile = bug.manifest["profile"] if args.profile is None else [
            p for p in (part.strip() for part in args.profile.split(",")) if p]
        expect = bug.manifest["expect"] if args.expect is None else [
            e for e in (part.strip() for part in args.expect.split(",")) if e]
        return cls(args.workload or bug.manifest["workload"], profile, expect, first, last,
                   args.rehearse_defect)


def rehearsal(args):
    return any(value is not None for value in (args.workload, args.profile, args.expect,
                                               args.rehearse_defect))


def harness_env(base):
    env = {k: v for k, v in base.items() if not k.startswith(HARNESS_PREFIXES)}
    flags = env.get("RUSTFLAGS", "")
    if "--cfg tokio_unstable" not in flags:
        env["RUSTFLAGS"] = f"{flags} --cfg tokio_unstable".strip()
    env["RUST_MIN_STACK"] = HARNESS_STACK
    return env


def sim_env(env, plan, label, artifacts, first, count, *, expect=False, defect=None, jobs=1):
    env = dict(env, NABEDRA_SIM_WORKLOAD=plan.workload, NABEDRA_SEED=str(first),
               NABEDRA_SEEDS=str(count), NABEDRA_SIM_JOBS=str(jobs), NABEDRA_SIM_CHECK_ID=label,
               NABEDRA_SIM_ARTIFACTS=str(artifacts))
    if plan.profile:
        env["NABEDRA_SIM_PROFILE"] = ",".join(plan.profile)
    if expect:
        env["NABEDRA_SIM_EXPECT"] = ",".join(plan.expect)
    if defect:
        env["NABEDRA_SIM_DEFECT"] = defect
    return env


def build_sweep(args, tree, env, log, dest):
    """Build the release sweep test in `tree` and copy it to `dest` (the next build replaces it)."""
    code, output = run_logged([*args.cargo.split(), "test", "--locked", "-j", str(args.jobs),
                               "-p", SWEEP_PACKAGE, "--release", "--lib", "--no-run",
                               "--message-format=json-render-diagnostics"], tree / "server", env, log)
    if code != 0:
        raise Failure(f"the {SWEEP_PACKAGE} sweep build failed (exit {code}); see the log")
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(test_executable(output, SWEEP_TARGET), dest)
    return dest


def run_sweep(exe, tree, env, log, timeout=None):
    return run_logged([str(exe), SWEEP_TEST, "--exact", "--nocapture", "--test-threads=1"],
                      tree / "server" / "sim-oltp", env, log, timeout)


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


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@dataclass
class Replay:
    """One seed run as an ordinary sweep, with its replay: what the harness reported."""
    code: int | None
    lines: list
    entry: dict | None
    history: Path | None
    summary: dict | None
    artifacts: Path

    @property
    def kinds(self):
        return sorted(self.entry.get("violations") or []) if self.entry else []

    @property
    def deterministic(self):
        return bool(self.entry and self.entry.get("replayed") is True
                    and self.entry.get("deterministic") is True)

    def json(self):
        entry = self.entry or {}
        return {
            "exit": self.code, "kinds": self.kinds, "deterministic": self.deterministic,
            "trace_hash": entry.get("trace_hash"), "trace_events": entry.get("trace_events"),
            "virtual_ms": entry.get("virtual_ms"), "config_hash": entry.get("config_hash"),
            "history": str(self.history) if self.history else None,
            "history_sha256": sha256_file(self.history) if self.history else None,
            "violations": (self.summary or {}).get("violations"),
            "artifacts": str(self.artifacts),
        }


def replay(exe, tree, env, log, plan, label, seed, artifacts, defect=None):
    """Run `seed` alone as an ordinary sweep that replays it, and read the harness's report."""
    shutil.rmtree(artifacts, ignore_errors=True)
    run_env = sim_env(env, plan, label, artifacts, seed, 1, defect=defect)
    run_env.update(NABEDRA_SIM_VERIFY_REPLAY="1", NABEDRA_SIM_RECORD_DIR=str(artifacts / "records"))
    code, output = run_sweep(exe, tree, run_env, log)
    report = read_json(artifacts / "sweep-report.json") or {}
    entry = next((row for row in report.get("seeds", []) if row.get("seed") == seed), None)
    seed_dir = artifacts / f"{label}-seed-{seed}"
    history = seed_dir / "history.jsonl"
    return Replay(code, [row for row in parse_seeds(output) if row["seed"] == seed], entry,
                  history if history.is_file() else None, read_json(seed_dir / "summary.json"),
                  artifacts)


def patched_problem(replay, plan, seed):
    """Why the patched replay does not confirm the search's hit, or None."""
    if replay.entry is None:
        return f"no sweep report row for seed {seed} (exit {replay.code})"
    if not set(replay.kinds) & set(plan.expect):
        return f"the replay reported {replay.kinds}, none of {plan.expect}"
    if not replay.deterministic:
        return "the seed's two runs differed (a determinism bug)"
    if replay.code != 101 or [row["result"] for row in replay.lines] != ["violation"]:
        return f"the sweep did not fail on the seed alone (exit {replay.code})"
    if replay.history is None:
        return "the harness wrote no history for the seed"
    return None


def clean_problem(replay, seed):
    """Why the unpatched replay is not a clean pass, or None."""
    if replay.entry is None:
        return f"no sweep report row for seed {seed} (exit {replay.code})"
    if replay.kinds:
        return f"the unpatched build reported {replay.kinds}"
    if not replay.deterministic:
        return "the seed's two runs differed (a determinism bug)"
    if replay.code != 0 or [row["result"] for row in replay.lines] != ["pass"]:
        return f"the unpatched sweep did not pass (exit {replay.code})"
    return None


def search_bug(args, scratch, env, log, bug, plan, out_dir, clean_exe):
    """Apply, build, search, replay patched, reset, replay clean. Returns the bug's result row."""
    label = f"sim-bug-{bug.id}"
    row = {"id": bug.id, "bug": bug.manifest["bug"], "class": bug.manifest["class"],
           "guarantees": bug.manifest["guarantees"], "scenarios": bug.manifest["scenarios"],
           "patch_sha256": hashlib.sha256(bug.patch.encode()).hexdigest(), "digest": digest(bug.patch),
           "workload": plan.workload, "profile": plan.profile, "expect": plan.expect,
           "seeds": [plan.first, plan.last], "result": "not-found", "rehearse_defect": plan.defect}
    tree = scratch.path
    scratch.apply(bug)
    try:
        row["scratch_worktree"] = str(tree)
        exe = build_sweep(args, tree, env, log, out_dir / "bin" / f"{bug.id}-sweep")
        count = plan.last - plan.first + 1
        search_env = sim_env(env, plan, label, out_dir / bug.id / "search", plan.first, count,
                             expect=True, defect=plan.defect, jobs=args.sim_jobs)
        code, output = run_sweep(exe, tree, search_env, log, args.search_timeout)
        missing = UNAVAILABLE_WORKLOAD.search(output)
        if missing:
            row.update(result="not-run", why=f"the harness has no {missing.group(1)} workload")
            return row
        seeds = parse_seeds(output)
        row["tried"] = len(seeds)
        if code is None:
            row["why"] = f"the search exceeded {args.search_timeout} s after {len(seeds)} seeds"
            return row
        if code not in (0, 101):
            row["why"] = f"the patched search exited {code}"
            return row
        reported = [entry["seed"] for entry in seeds]
        if len(set(reported)) != len(reported) or any(not plan.first <= s <= plan.last for s in reported):
            row["why"] = f"duplicate or out-of-budget seed lines in [{plan.first}, {plan.last}]"
            return row
        hit = first_match(seeds, plan.expect)
        if hit is None:
            why = "no seed line" if not seeds else f"{len(seeds)} seeds without {plan.expect}"
            row["why"] = f"{why}, exit {code}"
            return row
        row.update(found_seed=hit["seed"], search_kinds=sorted(hit["kinds"]),
                   tried=reported.index(hit["seed"]) + 1)
        patched = replay(exe, tree, env, log, plan, label, hit["seed"],
                         out_dir / bug.id / "patched", plan.defect)
        row["patched"] = patched.json()
        problem = patched_problem(patched, plan, hit["seed"])
        if problem:
            row["why"] = f"patched replay of seed {hit['seed']}: {problem}"
            return row
    finally:
        scratch.reset()
    clean = replay(clean_exe, tree, env, log, plan, label, hit["seed"], out_dir / bug.id / "clean")
    row["clean"] = clean.json()
    problem = clean_problem(clean, hit["seed"])
    if problem:
        row["why"] = f"clean replay of seed {hit['seed']}: {problem}"
        return row
    # A patched build has another schedule, so one clean twin can pass by luck while the unpatched
    # build reports the same kind on other seeds. None of the seeds tried may report it there.
    control_env = sim_env(env, plan, label, out_dir / bug.id / "control", plan.first, row["tried"],
                          expect=True, jobs=args.sim_jobs)
    code, output = run_sweep(clean_exe, tree, control_env, log, args.search_timeout)
    control = parse_seeds(output)
    wanted = list(range(plan.first, plan.first + row["tried"]))
    matched = first_match(control, plan.expect)
    row["control"] = {"exit": code, "seeds": len(control), "seed": matched and matched["seed"],
                      "kinds": matched and matched["kinds"]}
    if matched:
        row["why"] = (f"not attributable to the patch: the unpatched build reports "
                      f"{matched['kinds']} at seed {matched['seed']}")
        return row
    if code != 101 or [entry["seed"] for entry in control] != wanted:
        row["why"] = f"the clean control did not run every seed tried (exit {code})"
        return row
    if rehearsal(args):
        row["result"] = "rehearsed"
        return row
    # A matching kind is not a diagnosis: other defects report the same kinds.
    detection = bug.manifest.get("detection")
    identity = {"seed": hit["seed"], "trace_hash": row["patched"]["trace_hash"],
                "history_sha256": row["patched"]["history_sha256"]}
    if detection is None:
        row.update(result="candidate", why="no reviewed mechanism is recorded for this hit")
    elif any(detection[key] != value for key, value in identity.items()):
        row.update(result="candidate", why="the recorded mechanism is for another hit")
    elif detection["detector"] not in patched.kinds:
        row.update(result="candidate", why=f"{detection['detector']} did not report this hit")
    else:
        row.update(result="found", mechanism=detection["mechanism"], detector=detection["detector"])
    return row


def describe(row):
    if row["result"] == "not-run":
        return f"{row['id']}: not-run ({row['why']})"
    if row["result"] not in ("found", "candidate", "rehearsed"):
        where = f"seed {row['found_seed']}" if "found_seed" in row else f"[{row['seeds'][0]}, {row['seeds'][1]}]"
        return f"{row['id']}: NOT found ({where}: {row.get('why', 'unknown')})"
    patched, clean = row["patched"], row["clean"]
    by = f" (defect {row['rehearse_defect']}, not the patch)" if row.get("rehearse_defect") else ""
    if row["result"] == "candidate":
        by = f" (NOT counted: {row['why']})"
    return (f"{row['id']}: {row['result']}{by} seed {row['found_seed']} after {row['tried']} seeds in "
            f"[{row['seeds'][0]}, {row['seeds'][1]}]; patched {patched['kinds']} trace "
            f"{patched['trace_hash']} ({patched['trace_events']} events, replay deterministic), "
            f"history sha256 {patched['history_sha256']}; clean twin pass trace {clean['trace_hash']} "
            f"(replay deterministic); clean control {row['control']['seeds']} seeds without "
            f"{row['expect']}; patch sha256 {row['patch_sha256']}")


def cmd_run(repo, bugs, args, out):
    if git(repo, "cat-file", "-e", f"HEAD:{HARNESS_MANIFEST.as_posix()}", check=False).returncode:
        print(f"{NOT_RUN_MARKER}: {HARNESS_MANIFEST} does not exist yet, so no seed was searched "
              f"for {', '.join(b.id for b in bugs)}", file=out)
        return 0
    if cmd_check(repo, bugs, out):
        return 1
    mode = "rehearsal" if rehearsal(args) else "evidence"
    print("Fault model: seeded OLTP simulation with one historical fix reverted at a time"
          + (" (rehearsal: manifest workload, profile or expectation overridden; not evidence for "
             "these bugs)" if mode == "rehearsal" else ""), file=out, flush=True)
    target = Path(args.target_dir or repo / "server/target/sim-bugs-sim").resolve()
    out_dir = Path(args.out or repo / "target/testing-logs" / f"sim-bugs-run-{os.getpid()}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = Path(args.log or out_dir / "sim-bugs-run.log")
    env = harness_env(dict(os.environ, CARGO_TARGET_DIR=str(target)))
    rows = []
    with log_path.open("a", encoding="utf-8") as log, Scratch(repo, args.keep) as scratch:
        sha = git(scratch.path, "rev-parse", "HEAD").stdout.strip()
        print(f"sim-bugs run on {sha}, artifacts {out_dir}", file=out, flush=True)
        # The unpatched build first: every clean twin replays on it.
        clean_exe = build_sweep(args, scratch.path, env, log, out_dir / "bin" / "clean-sweep")
        for bug in bugs:
            try:
                row = search_bug(args, scratch, env, log, bug, Plan.of(bug, args), out_dir, clean_exe)
            except Failure as error:
                row = {"id": bug.id, "result": "not-found", "why": str(error),
                       "seeds": bug.manifest["seeds"]}
            rows.append(row)
            print(describe(row), file=out, flush=True)
    summary = {"schema": "nabedra.sim-bugs-run/1", "evidence": "fault-model", "mode": mode,
               "sha": sha, "set": args.set_name, "bugs": rows}
    (out_dir / "sim-bugs-run.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    found = sum(row["result"] == ("rehearsed" if mode == "rehearsal" else "found") for row in rows)
    not_run = [row["id"] for row in rows if row["result"] == "not-run"]
    what = ("pipeline rehearsals completed; no reverted fix was searched for" if mode == "rehearsal" else
            "reverted fixes found within budget, each with a passing clean twin, a clean control and a "
            "recorded mechanism")
    print(f"sim-bugs run{' (rehearsal)' if mode == 'rehearsal' else ''}: {found} of {len(rows)} "
          f"{what}; summary {out_dir / 'sim-bugs-run.json'}", file=out)
    if found == len(rows):
        return 0
    if not_run and found + len(not_run) == len(rows):
        print(f"{NOT_RUN_MARKER}: no workload yet for {', '.join(not_run)}", file=out)
        return 0
    return 1


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
    parser.add_argument("--sim-jobs", type=int, default=4, help="run: seeds simulated in parallel")
    parser.add_argument("--search-timeout", type=int, help="run: seconds allowed per bug's search")
    parser.add_argument("--out", help="run: artifact directory (default target/testing-logs/sim-bugs-run-<pid>)")
    rehearse = parser.add_argument_group(
        "rehearsal (run)", "Override a manifest to exercise the pipeline, for example on the smoke "
        "workload. A rehearsal is labelled as such and is never evidence for the bug.")
    rehearse.add_argument("--workload", help="workload instead of the manifest's")
    rehearse.add_argument("--profile", help="comma-separated profiles instead of the manifest's ('' for none)")
    rehearse.add_argument("--expect", help="comma-separated kinds instead of the manifest's")
    rehearse.add_argument("--rehearse-defect", help="NABEDRA_SIM_DEFECT for the patched runs only")
    parser.add_argument("--keep", action="store_true", help="keep the scratch worktree")
    args = parser.parse_args(argv)
    if (args.jobs < 1 or args.sim_jobs < 1 or (args.seeds is not None and args.seeds < 1)
            or (args.search_timeout is not None and args.search_timeout < 1)):
        parser.error("--jobs, --sim-jobs, --seeds and --search-timeout must be positive")
    if args.expect is not None and not args.expect.strip():
        parser.error("--expect needs at least one kind")
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
            if cmd_check(repo, chosen, out, require_evidence=False):
                return 1
            return cmd_verify(repo, chosen, args, out)
        return cmd_run(repo, chosen, args, out)
    except Failure as error:
        print(f"sim-bugs: {error}", file=out)
        return 2


if __name__ == "__main__":
    sys.exit(main())

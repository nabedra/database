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
                 fix changed, that a hand port has regression evidence, that the evidence is for
                 these changed lines, and that no hit or patch is entered twice. It also prints
                 what keeps each target from counting.
  verify [ids]   Heavy: build HEAD in a scratch worktree, run each bug's original regression test,
                 then apply each patch and run it again. A bug is verified when the test passes
                 without the patch and fails with it (a build failure or a missing test is never a
                 failure of the test).
  run [ids]      The seed search (`oltp-sim-bugs-<set>`) through the nabedra-sim-oltp sweep test, by
                 the rule in server/sim-oltp/bugs/README.md. It builds the unpatched sweep first.
                 Then, per bug, it builds the patched sweep in a scratch worktree, resets the tree
                 and tries the seeds in order from 1. A seed that reports an expected kind is
                 replayed alone on the patched build and on the unpatched build. A red twin or a
                 replay that differs masks the seed, and the search goes on. The first seed that
                 nothing masks is the hit. The unpatched build then runs every seed tried, for the
                 base rate. A hit counts as found only if the manifest confirms its mechanism for
                 that exact seed, trace hash and history, and nothing else is missing. Otherwise it
                 is a candidate. `not-detected` needs all 10000 seeds, `incomplete` is a search that
                 stopped earlier, and `error` is a run that left something unsaid.
                 It writes sim-bugs-run.json beside the harness artifacts. While the harness or a
                 bug's workload is missing it prints `not-run: sim-harness-missing` and exits 0,
                 which the check records as not-run, never as a pass.
  record <id>    Append the search in `--from <sim-bugs-run.json>` to the bug's searches.jsonl, which
                 is append-only. `--never-searched <commit> --reason <text>` accounts for a plan that
                 an earlier manifest held and nobody ran.
  evidence [ids] Compare the recorded regression output and searches.jsonl with the logs of attested
                 runs in `--records <dir>`. Signatures are for `gh attestation verify`.
  digest <id>    Print the changed-lines digest to record in a manifest's evidence, and the digest
                 of the whole patch that its [review] must name.

Use `--set <name>` instead of ids to select the manifests of one set (direct, queued, topology,
membership, resources). Exit status: 0 success, 1 failure, 2 usage or environment error.
"""

import argparse
from collections import Counter
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
import unicodedata

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
PRODUCT_SOURCE = re.compile(r"server/(?:[a-z0-9-]+/)?src/(?:[a-z0-9_]+/)*[a-z0-9_]+\.rs")
# Crates and module names of simulators, checkers, test support and their runtime.
NOT_PRODUCT_CRATES = ("sim", "check", "fuzz", "rt")
NOT_PRODUCT_PART = re.compile(r"sim|sim_\w*|\w*_sim|planted\w*|\w*acceptance\w*|testing|test_\w+|"
                              r"\w*fixtures?\w*|faults?")
# Text through which a line can act in the simulator alone, or read the build or its environment.
SIM_ONLY = re.compile(r'\bsim(?:_\w+|ulat\w*)?\b|\w_sim\b|\bWorld\b|\bcfg|\bfeature\s*=|\btokio_unstable\b|'
                      r'\b(?:option_)?env!|\benv::|\bvar_os\b|\bcurrent_exe\b|\binclude(?:_str|_bytes)?!|#\[path')
# A fix may have moved test code, so its revert may change this one attribute.
TEST_CFG = "#[cfg(test)]"
# What a cfg predicate names when its code is compiled for the simulator and not for the product.
SIM_CFG = re.compile(r'feature\s*=\s*"sim"|\btokio_unstable\b')
LIBTEST_RESULT = re.compile(r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;", re.MULTILINE)
NOT_RUN_MARKER = "not-run: sim-harness-missing"
# Phase 8 moved these product trees into their own crates. Patches use the current paths; the
# fix commits use the old ones, so a clean port is compared through this map.
MOVED = (("server/src/oltp_replication/", "server/oltp-replication/src/oltp_replication/"),
         ("server/src/oltp/", "server/oltp-storage/src/oltp/"))
# Library tests run with the features the workflow's shared OLTP build uses.
LIB_TEST_FEATURES = ["--no-default-features", "--features", "oltp"]

# Kinds the harness reports for its own faults: a panic in it, or two runs of a seed that differ.
FAULT_KINDS = ("harness", "determinism")
KIND = re.compile(r"[A-Za-z][A-Za-z0-9_-]*")
TRACE_HASH = re.compile(r"[0-9a-f]{16}")
DIGEST = re.compile(r"[0-9a-f]{64}")
RUN_URL = re.compile(r"https://github\.com/[\w.-]+/[\w.-]+/actions/runs/[0-9]+")
VERIFIED = re.compile(r"^(\S+): with the patch failed; verified \(digest ([0-9a-f]{64})\)$", re.MULTILINE)
# Decision 9 wants text that exists: a plan, a contract or a register.
RULE_SOURCE = re.compile(r"(?:docs|plans)/(?:[a-z0-9_-]+/)*[a-z0-9_-]+\.md")
VERDICTS = ("sensitive", "partial", "not-sensitive")
ASSERTIONS = ("state", "counter", "timeout")
SEED_BUDGET = 10_000
SEARCHES = "searches.jsonl"
TARGET_FILE = re.compile(r"server/sim-oltp/bugs/(?:README\.md|(?:phase[0-9]+/)?(?:simulator\.toml|[a-z0-9-]+/"
                         r"(?:bug\.toml|revert\.patch|probe\.patch|searches\.jsonl)))")
SEARCH_SCHEMA = "nabedra.sim-bug-search/1"
SEARCH_FIELDS = {"schema", "id", "sha", "run", "attempt", "plan", "digest", "patch_sha256", "result", "seeds",
                 "tried", "hit", "masked", "rates", "why"}

MANIFEST_FIELDS = {
    "schema": str, "id": str, "bug": str, "set": str, "phase": int, "class": str,
    "guarantees": list, "scenarios": list, "fix_commits": list, "reverts": (str, list), "port": str,
    "port_notes": str, "workload": str, "profile": list, "expect": list, "seeds": list,
    "found_seed": int, "regression": dict, "detection": dict,
    "expect_reason": str, "hold": str, "review": dict, "rules": list, "masked": list,
}
REQUIRED = MANIFEST_FIELDS.keys() - {"port_notes", "found_seed", "detection", "expect_reason", "hold",
                                     "review", "rules", "masked"}
# A confirmed diagnosis of one hit: its identity, and how its history and trace show the reverted
# fix's mechanism rather than another defect that reports the same kind. The tool records who
# confirmed it and cannot prove that they are independent.
HIT_IDENTITY = ("seed", "trace_hash", "history_sha256")
DETECTION_FIELDS = {"seed": int, "trace_hash": str, "history_sha256": str, "mechanism": str,
                    "detector": str, "lines": list, "search": str, "confirmed_by": str,
                    "confirmation": str, "patch_author": str, "workload_author": str}
# A hit that shows a known finding instead of the reverted fix.
MASKED_FIELDS = {"seed": int, "trace_hash": str, "history_sha256": str, "finding": str, "reason": str,
                 "confirmed_by": str, "confirmation": str}
REVIEW_FIELDS = {"digest": str, "verdict": str, "assertion": str, "reviewer": str, "source": str, "notes": str}
REGRESSION_FIELDS = {"kind": str, "test": str, "scenario": str, "mode": str, "assertion": str,
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
    # `git replace` refs would show another object under a commit's name.
    env = dict(os.environ if env is None else env, GIT_NO_REPLACE_OBJECTS="1")
    result = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True,
                            env=env, input=input)
    if check and result.returncode:
        raise Failure(f"git {' '.join(args)}: {result.stderr.strip() or result.stdout.strip()}")
    return result


@dataclass
class FileDiff:
    """One `diff --git` section: its extended header lines, and its hunks with their markers."""
    name: str
    headers: list = field(default_factory=list)
    hunks: list = field(default_factory=list)


DIFF_HEADER = re.compile(r"diff --git a/(\S+) b/\1")
EXTENDED_HEADER = re.compile(r"(index|old mode|new mode|deleted file mode|new file mode|copy|rename|similarity|"
                             r"dissimilarity|Binary files) ?.*")
# What a patch may do besides changing lines: add or delete a whole product file.
PLAIN_HEADER = re.compile(r"index .*|(?:new|deleted) file mode 100644")
HUNK_HEADER = re.compile(r"@@ -\d+(?:,(\d+))? \+\d+(?:,(\d+))? @@(?: .*)?")


def parse_patch(patch):
    """Read a unified diff line by line, as `git apply` does, and refuse any line that is not part of
    a `diff --git` section. Git also applies a `---`/`+++` pair that no such header announces, so a
    reader of the headers alone would miss the file it edits."""
    files, lines, i = [], patch.split("\n"), 0
    if lines and lines[-1] == "":
        lines.pop()
    while i < len(lines):
        header = DIFF_HEADER.fullmatch(lines[i])
        if header is None:
            raise Failure(f"patch line {i + 1} is not part of a `diff --git` section: {lines[i][:80]!r}")
        current = FileDiff(header.group(1))
        files.append(current)
        i += 1
        while i < len(lines) and EXTENDED_HEADER.fullmatch(lines[i]):
            current.headers.append(lines[i])
            i += 1
        if i + 1 >= len(lines) or not lines[i].startswith("--- "):
            continue
        for line, prefix in ((lines[i], "--- a/"), (lines[i + 1], "+++ b/")):
            if line not in (prefix + current.name, prefix[:4] + "/dev/null"):
                raise Failure(f"a `{prefix[:3]}` line under {current.name} names another file: {line[:80]!r}")
        i += 2
        while i < len(lines) and (counts := HUNK_HEADER.fullmatch(lines[i])):
            old, new = (1 if n is None else int(n) for n in counts.groups())
            hunk, i = [], i + 1
            while (old or new or (i < len(lines) and lines[i].startswith("\\ "))) and i < len(lines):
                line = lines[i] or " "
                if line[0] not in " -+\\":
                    break
                old -= line[0] in " -"
                new -= line[0] in " +"
                hunk.append(line)
                i += 1
            if old or new:
                raise Failure(f"a hunk of {current.name} does not have the lines its header counts")
            current.hunks.append(hunk)
    return files


def patch_files(patch):
    """Files a unified diff touches."""
    return list(dict.fromkeys(diff.name for diff in parse_patch(patch)))


def changed_lines(patch):
    """{file: (removed lines, added lines)}, ignoring context, hunk positions and blob ids."""
    out = {}
    for diff in parse_patch(patch):
        removed, added = out.setdefault(diff.name, ([], []))
        for hunk in diff.hunks:
            removed.extend(line[1:] for line in hunk if line[0] == "-")
            added.extend(line[1:] for line in hunk if line[0] == "+")
    return {name: lines for name, lines in out.items() if lines[0] or lines[1]}


def hunk_texts(patch):
    """{file: its hunks with their context}, ignoring positions and blob ids."""
    return {diff.name: diff.hunks for diff in parse_patch(patch)}


def code_changes(patch):
    """The changed lines that are neither blank nor comments, as a multiset."""
    return Counter((name, sign, line.strip()) for name, sides in changed_lines(patch).items()
                   for sign, lines in zip("-+", sides) for line in lines
                   if line.strip() and not line.strip().startswith("//"))


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
        if any(part.is_symlink() for part in (path.parent, path, *path.rglob("*"))):
            errors.append(f"{name}: symbolic links are not allowed here")
            continue
        try:
            with (path / "bug.toml").open("rb") as stream:
                manifest = tomllib.load(stream)
            patch = (path / "revert.patch").read_bytes().decode("utf-8")
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
            continue
        types = fields[key] if isinstance(fields[key], tuple) else (fields[key],)
        if type(value) not in types:
            problems.append(f"{where}: {key} must be a {' or '.join(t.__name__ for t in types)}")


def strings(value):
    return isinstance(value, list) and all(isinstance(v, str) and v.strip() for v in value)


def reverted(manifest):
    """The SHAs the patch reverts. `reverts` is one SHA or a list."""
    return manifest["reverts"] if isinstance(manifest["reverts"], list) else [manifest["reverts"]]


def plan_of(manifest):
    """What a search of this manifest runs. Changing it restarts the count (lead decision 8)."""
    return {key: manifest.get(key) for key in ("workload", "profile", "expect", "seeds")}


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
    if (m["phase"] == 10) != (m["set"] == "direct"):
        problems.append("phase 10 is the set direct, and the set direct is phase 10")
    if m["set"] not in SETS:
        problems.append(f"set must be one of {', '.join(SETS)}")
    if m["port"] not in PORTS:
        problems.append("port must be clean or hand")
    if m["port"] == "hand" and not m.get("port_notes", "").strip():
        problems.append("a hand port needs port_notes naming the original hunk")
    for key in ("guarantees", "scenarios", "fix_commits", "profile"):
        if not m[key] or not strings(m[key]):
            problems.append(f"{key} must be a nonempty array of nonempty strings")
    if not strings(reverted(m)) or not reverted(m):
        problems.append("reverts must be a commit SHA or a nonempty array of them")
    problems.extend(validate_expect(m))
    if problems:
        return problems
    if not all(ID.fullmatch(name) for name in [m["workload"], *m["profile"]]):
        problems.append("workload and profile names must be kebab-case")
    if m["seeds"] != [1, SEED_BUDGET] or any(type(seed) is not int for seed in m["seeds"]):
        problems.append(f"seeds must be [1, {SEED_BUDGET}]: a search may not choose its seeds")
    if "found_seed" in m and m["found_seed"] != m.get("detection", {}).get("seed"):
        problems.append("found_seed must be the seed of [detection]")
    for sha in m["fix_commits"] + reverted(m):
        if not SHA.fullmatch(sha):
            problems.append(f"invalid full commit SHA {sha!r}")
    if not set(reverted(m)) <= set(m["fix_commits"]):
        problems.append("reverts must be one of fix_commits, or several of them")
    if m["port"] == "clean" and len(reverted(m)) != 1:
        problems.append("a clean port reverts exactly one commit")
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
    problems.extend(validate_regression(bug, require_evidence))
    problems.extend(validate_patch_paths(bug.patch))
    if not problems:
        problems.extend(validate_review(bug))
        problems.extend(validate_hits(bug))
    return problems


def validate_expect(m):
    """The kinds a search looks for. A fault of the harness itself is never one of them."""
    reason = m.get("expect_reason")
    if not m["expect"]:
        if m["phase"] < 11 or not (reason or "").strip():
            return ["expect must be a nonempty array of nonempty strings. Only phase 11 or later may leave it "
                    "empty, with an expect_reason"]
        return []
    if not strings(m["expect"]) or len(set(m["expect"])) != len(m["expect"]):
        return ["expect must be a nonempty array of distinct nonempty strings"]
    if reason is not None:
        return ["expect_reason is only for an empty expect"]
    return [f"expect may not name {kind!r}: it must be one kind that a rule reports, and no fault of the harness"
            for kind in m["expect"] if not KIND.fullmatch(kind) or kind.lower() in FAULT_KINDS]


def whole_digest(bug):
    """SHA-256 of everything a review of the patch reads: the whole patch, with its context and headers, and
    the probe if there is one. Any other byte is another patch to a reviewer."""
    probe = getattr(bug, "probe", None)
    return hashlib.sha256((bug.patch if probe is None else f"{bug.patch}\0{probe}").encode()).hexdigest()


def validate_review(bug):
    """[review], [[rules]] and hold."""
    m, problems = bug.manifest, []
    if "hold" in m and not m["hold"].strip():
        problems.append("hold must say why the target may not count")
    if "review" in m:
        review = m["review"]
        typed(problems, "review", review, REVIEW_FIELDS, REVIEW_FIELDS.keys() - {"notes"})
        if problems:
            return problems
        if review["verdict"] not in VERDICTS:
            problems.append(f"review.verdict must be one of {', '.join(VERDICTS)}")
        if review["assertion"] not in ASSERTIONS:
            problems.append(f"review.assertion must be one of {', '.join(ASSERTIONS)}")
        if review["verdict"] == "sensitive" and review["assertion"] != "state":
            problems.append("review: sensitive needs a state assertion; a counter or a timeout alone shows no defect")
        if review["digest"] != whole_digest(bug):
            problems.append(f"review.digest is of another patch than this one ({whole_digest(bug)}): remove [review] until "
                            "the patch has been reviewed again")
        if bug.regression.get("assertion", review["assertion"]) != review["assertion"]:
            problems.append("regression.assertion contradicts [review]")
        problems.extend(f"review.{key} must not be empty" for key in ("reviewer", "source") if not review[key].strip())
    for rule in m.get("rules", []):
        if (not isinstance(rule, dict) or set(rule) != {"kind", "source", "text"}
                or not strings(list(rule.values()))):
            problems.append("rules: each needs a kind, a source and a text")
        elif rule["kind"] not in m["expect"]:
            problems.append(f"rules: {rule['kind']} is not in expect")
        elif not RULE_SOURCE.fullmatch(rule["source"]):
            problems.append(f"rules: the source {rule['source']!r} is no plan, contract or register of this repository")
        elif len(rule["text"].split()) < 5:
            problems.append(f"rules: the quote for {rule['kind']} is too short to state a rule")
    return problems


def same_person(one, other):
    return " ".join(one.lower().split()) == " ".join(other.lower().split())


def validate_hits(bug):
    """[detection] and [[masked]]: one confirmed hit, and the hits that show a known finding instead."""
    m, problems = bug.manifest, []
    hits = [("masked", hit, MASKED_FIELDS) for hit in m.get("masked", [])]
    if "detection" in m:
        hits.append(("detection", m["detection"], DETECTION_FIELDS))
    for where, hit, fields in hits:
        if not isinstance(hit, dict):
            return [f"{where}: must be a table"]
        typed(problems, where, hit, fields, fields)
        if problems:
            return problems
        for key, value in hit.items():
            if isinstance(value, str) and not value.strip():
                problems.append(f"{where}.{key} must not be empty")
        if not TRACE_HASH.fullmatch(hit["trace_hash"]) or not DIGEST.fullmatch(hit["history_sha256"]):
            problems.append(f"{where} must name the hit's trace hash and history SHA-256")
        if not m["seeds"][0] <= hit["seed"] <= m["seeds"][1]:
            problems.append(f"{where}.seed must lie inside seeds")
    if problems or "detection" not in m:
        return problems
    d = m["detection"]
    if any(all(d[key] == mask[key] for key in HIT_IDENTITY) for mask in m.get("masked", [])):
        problems.append("one hit is in masked and in detection")
    if d["detector"] not in m["expect"]:
        problems.append("detection.detector must be one of expect: the check that found the bug")
    if not d["lines"] or not strings(d["lines"]):
        problems.append("detection.lines must quote history or trace lines that show the reverted path ran")
    if not DIGEST.fullmatch(d["search"]):
        problems.append("detection.search must be the hash of a search in searches.jsonl")
    if any(same_person(d["confirmed_by"], d[key]) for key in ("patch_author", "workload_author")):
        problems.append("detection.confirmed_by wrote the patch or the workload, so it does not confirm")
    if "review" in m and same_person(m["review"]["reviewer"], d["patch_author"]):
        problems.append("review.reviewer wrote the patch, so it does not review it")
    # The tool cannot judge prose. It can refuse prose that does not name this hit and this patch,
    # or that names another hit.
    text, own = d["mechanism"], {*(d[key] for key in ("trace_hash", "history_sha256", "search")), *m["fix_commits"]}
    own.update(entry["search"]["hit"]["twin_trace_hash"] for entry in load_searches(bug)[0]
               if "search" in entry and search_hash(entry["search"]) == d["search"] and entry["search"]["hit"])
    for seed in re.findall(r"\bseed\s+#?(\d+)", text, re.IGNORECASE):
        if int(seed) != d["seed"]:
            problems.append(f"detection.mechanism names seed {seed}, not this hit's seed")
    for token in re.findall(r"\b[0-9a-f]{16,64}\b", text.lower()):
        if len(token) in (16, 40, 64) and token not in own:
            problems.append(f"detection.mechanism names {token}, which is not this hit's")
    if not re.search(rf"\bseed {d['seed']}\b", text) or d["trace_hash"] not in text:
        problems.append(f"detection.mechanism must name seed {d['seed']} and its trace hash")
    if not any(Path(name).name in text for name in patch_files(bug.patch)):
        problems.append("detection.mechanism must name a file the patch changes")
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
    if r.get("assertion", "state") not in ASSERTIONS:
        problems.append(f"regression.assertion must be one of {', '.join(ASSERTIONS)}")
    if "failure_contains" in r and not strings(r["failure_contains"]):
        problems.append("regression.failure_contains must be an array of nonempty strings")
    if kind == "acceptance" and not r.get("failure_contains"):
        problems.append("an acceptance regression needs failure_contains for its specific failure")
    if not 1 <= r.get("attempts", 1) <= 10:
        problems.append("regression.attempts must be between 1 and 10")
    evidence = r.get("evidence")
    if evidence is not None:
        typed(problems, "regression.evidence", evidence, EVIDENCE_FIELDS, EVIDENCE_FIELDS)
        for side, expected in (("patched", "failed"), ("clean", "passed")):
            run = evidence.get(side)
            if isinstance(run, dict):
                typed(problems, f"regression.evidence.{side}", run, RUN_FIELDS, ("run", "sha", "result"))
                if run.get("result") != expected:
                    problems.append(f"regression.evidence.{side}.result must be {expected!r}")
                if not SHA.fullmatch(str(run.get("sha", ""))):
                    problems.append(f"regression.evidence.{side}.sha must be a full commit SHA")
                if not RUN_URL.fullmatch(str(run.get("run", ""))):
                    problems.append(f"regression.evidence.{side}.run must be the URL of an Actions run")
    if require_evidence and bug.manifest.get("port") == "hand" and evidence is None:
        problems.append("a hand port counts only with regression evidence that its test fails")
    return problems


def path_problem(name, strict=True):
    """Why a patch may not touch `name`, or None for a product source. Without `strict`, code that
    ships but that a patch may not touch, such as the runtime crate, is product as well."""
    crate = name.split("/")[1] if name.count("/") > 1 else ""
    if (not name.startswith("server/") or crate.startswith("sim-")
            or crate in NOT_PRODUCT_CRATES[:None if strict else -1]):
        return f"revert.patch touches {name}, outside the product sources"
    if TEST_FILE.search(name):
        return f"revert.patch touches the test file {name}"
    if name.endswith(("Cargo.toml", "Cargo.lock")):
        return f"revert.patch touches the manifest {name}"
    if strict and (not PRODUCT_SOURCE.fullmatch(name) or any(
            NOT_PRODUCT_PART.fullmatch(part) for part in name.removesuffix(".rs").split("/"))):
        return f"revert.patch touches {name}, which is not a product source file"
    return None


def validate_patch_paths(patch):
    try:
        files = parse_patch(patch)
    except Failure as error:
        return [f"revert.patch: {error}"]
    if not files:
        return ["revert.patch is empty"]
    problems = [problem for problem in map(path_problem, dict.fromkeys(diff.name for diff in files)) if problem]
    for diff in files:
        if any(not PLAIN_HEADER.fullmatch(header) for header in diff.headers) or not diff.hunks:
            problems.append(f"revert.patch must only change lines of {diff.name}: "
                            f"{', '.join(diff.headers) or 'no hunk'}")
    if len({diff.name for diff in files}) != len(files):
        problems.append("revert.patch has two sections for one file")
    hidden = sorted({f"U+{ord(c):04X}" for c in patch if c not in "\n\t" and unicodedata.category(c) in ("Cc", "Cf")})
    if hidden:
        problems.append(f"revert.patch holds characters that a reviewer cannot see: {', '.join(hidden)}")
    for name, (removed, added) in changed_lines(patch).items():
        problems.extend(f"revert.patch changes in {name} a line that can act only in the simulator, or that reads the "
                        f"build or its environment: {line.strip()[:80]!r}" for line in [*removed, *added]
                        if SIM_ONLY.search(line) and line.strip() != TEST_CFG)
    return problems


RUST_TEXT = re.compile(r"//[^\n]*|/\*.*?\*/|\br(#*)\".*?\"\1|\"(?:\\.|[^\"\\])*\"|'(?:\\[^\n']+|[^\\'\n])'", re.DOTALL)
CFG = re.compile(r"#(!?)\s*\[\s*cfg(?:_attr)?\s*(\()|\bcfg!\s*(\()")


def closing(code, start):
    """The index after the bracket that closes the one at `start`."""
    depth = 0
    for i in range(start, len(code)):
        depth += (code[i] in "([{") - (code[i] in ")]}")
        if depth == 0:
            return i + 1
    return len(code)


def sim_only_lines(text):
    """Line numbers of Rust code under a cfg predicate that names the simulator's build. Approximate,
    and it errs on the side of calling code simulator-only."""
    code = RUST_TEXT.sub(lambda m: re.sub(r"[^\n]", " ", m.group()), text)
    gated = set()
    for match in CFG.finditer(code):
        opening = match.start(2) if match.group(2) else match.start(3)
        end = closing(code, opening)
        predicate = text[opening:end]
        if not SIM_CFG.search(predicate) or re.fullmatch(
                rf"\(\s*not\s*\(\s*(?:{SIM_CFG.pattern})\s*\)\s*\)", predicate):
            continue
        if match.group(1):
            return set(range(1, text.count("\n") + 2))
        if match.group(2):
            end = code.index("]", end) + 1
            while attribute := re.compile(r"\s*#\s*\[").match(code, end):
                end = closing(code, attribute.end() - 1)
        item = re.compile(r"\s*(?:pub|fn|impl|struct|enum|trait|mod|const|static|async|unsafe|use|type|let|extern)\b")
        stops = "{;" if match.group(3) or item.match(code, end) else "{;,"
        i, depth = end, 0
        while i < len(code):
            if depth <= 0 and code[i] in stops:
                i = closing(code, i) if code[i] == "{" else i + 1
                break
            depth += (code[i] in "([{") - (code[i] in ")]}")
            if depth < 0:
                break
            i += 1
        gated.update(range(code.count("\n", 0, match.start()) + 1, code.count("\n", 0, i) + 2))
    return gated


def applied(repo, patch):
    """Apply `patch` to HEAD's tree through a private index, and say what changed in fact:
    problems with the files and line counts, and changes to code compiled only for the simulator."""
    problems = []
    with tempfile.TemporaryDirectory(prefix="sim-bugs-index-") as tmp:
        env = dict(os.environ, GIT_INDEX_FILE=str(Path(tmp) / "index"))
        git(repo, "read-tree", "HEAD", env=env)
        git(repo, "apply", "--cached", "-", env=env, input=patch)
        diff = git(repo, "diff", "--cached", "--no-renames", "--no-ext-diff", "--no-color", "-U0", "HEAD", env=env).stdout
        numstat = git(repo, "apply", "--numstat", "-z", "-", input=patch).stdout
        counts = {}
        for row in filter(None, numstat.split("\0")):
            added, removed, name = row.split("\t")
            before = counts.get(name, (0, 0))
            counts[name] = (before[0] + int(removed), before[1] + int(added))
        read = {name: (len(removed), len(added)) for name, (removed, added) in changed_lines(patch).items()}
        files = parse_patch(diff)
        if counts != read or sorted(f.name for f in files) != sorted(read):
            problems.append(f"git applies other changes than revert.patch shows: {sorted(set(counts) ^ set(read)) or counts}")
        for changed in files:
            if any(not PLAIN_HEADER.fullmatch(header) for header in changed.headers):
                problems.append(f"git changes more than lines of {changed.name}: {', '.join(changed.headers)}")
        for name in read:
            sides = (git(repo, "show", f"HEAD:{name}", check=False).stdout, git(repo, "show", f":{name}", env=env, check=False).stdout)
            gated = [sim_only_lines(side) for side in sides]
            section = diff.split(f"diff --git a/{name} b/{name}\n", 1)[-1].split("\ndiff --git ", 1)[0]
            for a, n, b, k in re.findall(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", section, re.MULTILINE):
                spans = ((int(a), int(n or 1)), (int(b), int(k or 1)))
                if any(line in lines for (start, count), lines in zip(spans, gated)
                       for line in range(start, start + count)):
                    problems.append(f"revert.patch changes lines of {name} that are compiled only for the simulator")
                    break
    return problems


def regression_evidence(bug, repo):
    """(errors, what keeps it from counting) of the recorded regression run.

    The run is tied to the patch through the commit it tested. A `verify` run applied the committed
    revert.patch of that commit. A run on a scratch commit tested the difference between its clean
    and its patched commit."""
    evidence, errors, uncounted = bug.regression.get("evidence"), [], []
    if evidence is None:
        return [], ["no regression evidence: no test is recorded to fail because of this patch"]
    want, clean, patched = digest(bug.patch), evidence["clean"], evidence["patched"]
    if evidence["digest"] != want:
        errors.append("the patch's changed lines differ from those its regression evidence "
                      f"used (digest {want}); verify it again and record the run")
    output = patched.get("output", "")
    if set(LIBTEST_RESULT.findall(output)) != {("FAILED", "0", "1")}:
        errors.append("the recorded output must hold the run's `test result: FAILED. 0 passed; 1 failed` line, and no "
                      "other result")
    missing = [text for text in bug.regression.get("failure_contains", []) if text not in output]
    if missing:
        errors.append(f"the recorded output lacks the failure_contains text {missing}")
    if "NOT verified" in output:
        errors.append("the recorded output says NOT verified")
    verified = VERIFIED.findall(output)
    for name, found in verified:
        if name != bug.id:
            errors.append(f"the recorded output is {name}'s")
        if found != want:
            errors.append(f"its recorded output names the digest {found}, not this patch's {want}")
    if errors:
        return errors, uncounted
    absent = [sha for sha in dict.fromkeys((clean["sha"], patched["sha"]))
              if git(repo, "cat-file", "-e", f"{sha}^{{commit}}", check=False).returncode]
    if verified and (clean["sha"], clean["run"]) != (patched["sha"], patched["run"]):
        return ["a verify run tests both sides: clean and patched must name the same run and commit"], uncounted
    if absent:
        return [], [f"the commit {sha[:8]} that its regression run tested is not available" for sha in absent]
    if verified:
        if git(repo, "merge-base", "--is-ancestor", patched["sha"], "HEAD", check=False).returncode:
            uncounted.append(f"the commit {patched['sha'][:8]} that its regression run tested is not in HEAD's history")
        name = (bug.path.relative_to(repo) / "revert.patch").as_posix()
        tested = git(repo, "show", f"{patched['sha']}:{name}", check=False)
        if tested.returncode:
            return [], [*uncounted, f"{patched['sha'][:8]} holds no {name} for its regression run to have tested"]
        tested = tested.stdout
    else:
        files = patch_files(bug.patch)
        tested = git(repo, "diff", "--no-renames", "--no-ext-diff", "--no-color", clean["sha"], patched["sha"],
                     "--", *files).stdout
        own = set(files) | set(patch_files(getattr(bug, "probe", None) or ""))
        extras = [set(git(repo, "diff", "--no-renames", "--name-only", base, patched["sha"]).stdout.split()) - own
                  for base in (f"{patched['sha']}^", clean["sha"])]
        if all(extras):
            return [f"{patched['sha'][:8]}, which its regression run tested, also changes {sorted(extras[0])}"], uncounted
    try:
        same_lines, same_hunks = digest(tested) == want, hunk_texts(tested) == hunk_texts(bug.patch)
    except Failure as error:
        return [f"the patch of {patched['sha'][:8]}: {error}"], uncounted
    if not same_lines:
        return [f"{patched['sha'][:8]}, which its regression run tested, "
                + ("holds a patch with other changed lines" if verified else
                   f"does not differ from {clean['sha'][:8]} by this patch")], uncounted
    if not same_hunks:
        uncounted.append("the patch has other context than the patch its regression run tested; verify it again")
    return [], uncounted


def not_shown(bug):
    """Why no hit on this target can count, so that a search would only take time from the others."""
    m = bug.manifest
    if "hold" in m:
        return f"held: {m['hold']}"
    if bug.regression.get("evidence") is None:
        return "no regression evidence: no test is recorded to fail because of this patch"
    if m.get("review", {}).get("verdict", "sensitive") != "sensitive":
        return f"the independent review calls the patch {m['review']['verdict']}"
    return None


def static_count_problems(bug, repo):
    """What keeps a target from counting, whatever a search finds."""
    m, problems = bug.manifest, regression_evidence(bug, repo)[1]
    if "hold" in m:
        problems.append(f"held: {m['hold']}")
    if "assertion" not in bug.regression:
        problems.append("regression.assertion is not recorded")
    review = m.get("review")
    if review is None:
        problems.append("no independent review of the patch is recorded")
    elif review["verdict"] != "sensitive":
        problems.append(f"the independent review calls the patch {review['verdict']}")
    if not m["expect"]:
        problems.append(f"no rule may detect it: {m['expect_reason']}")
    elif m["phase"] >= 11:
        ruled = {rule["kind"] for rule in m.get("rules", [])}
        if not ruled & set(m["expect"]):
            problems.append("no expected kind has the text its rule enforces in [[rules]]")
        elif "detection" in m and m["detection"]["detector"] not in ruled:
            problems.append("the detector has no text in [[rules]]")
    if "detection" in m:
        problems.extend(unrecorded_plans(bug, repo))
    return problems


def manifest_history(bug, repo):
    """(commit, manifest or None when it is unreadable) of every committed version."""
    name = (bug.path.relative_to(repo) / "bug.toml").as_posix()
    for sha in git(repo, "log", "--full-history", "--format=%H", "HEAD", "--", name).stdout.split():
        text = git(repo, "show", f"{sha}:{name}", check=False)
        if text.returncode:
            continue
        try:
            yield sha, tomllib.loads(text.stdout)
        except ValueError:
            yield sha, None


def unrecorded_plans(bug, repo):
    """Plans of earlier committed manifests that searches.jsonl does not account for."""
    if git(repo, "rev-parse", "--is-shallow-repository").stdout.strip() != "false":
        return ["the clone is shallow, so earlier plans and searches are not visible"]
    entries, _ = load_searches(bug)
    known, problems = [plan_of(bug.manifest)], []
    known += [entry["search"]["plan"] if "search" in entry else entry["never_searched"]["plan"] for entry in entries]
    for sha, manifest in manifest_history(bug, repo):
        if manifest is None:
            problems.append(f"the manifest of {sha[:8]} is unreadable, so its plan is unknown")
            continue
        plan = plan_of(manifest)
        if plan not in known:
            known.append(plan)
            problems.append(f"the manifest of {sha[:8]} has an earlier plan ({plan['workload']}, {plan['profile']}, "
                            f"{plan['expect']}, {plan['seeds']}) that searches.jsonl does not account for")
    return problems


# --------------------------------------------------------------------------------------------
# searches.jsonl: every search of a target, also those that restarted. One JSON object per line,
# {"n", "prev", "search"} or {"n", "prev", "never_searched"}. `prev` is the SHA-256 of the line before.

def canonical(data):
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def search_hash(search):
    return hashlib.sha256(canonical(search).encode()).hexdigest()


def load_searches(bug):
    """(entries, problems) of the target's record."""
    path = bug.path / SEARCHES
    if not path.is_file():
        return [], []
    entries, previous, text = [], None, path.read_text(encoding="utf-8")
    for n, line in enumerate(text.splitlines(), 1):
        try:
            entry = strict_json(line)
            if canonical(entry) != line:
                raise ValueError("it is not in the form `record` writes")
            if entry["n"] != n or entry["prev"] != previous:
                raise ValueError("its number or the hash of the line before is wrong")
            kind, = set(entry) - {"n", "prev"}
            body = entry[kind]
            if kind == "never_searched":
                if set(body) != {"plan", "reason"} or not body["reason"].strip():
                    raise ValueError("never_searched needs a plan and a reason")
            elif kind != "search" or set(body) != SEARCH_FIELDS or body["schema"] != SEARCH_SCHEMA:
                raise ValueError("it is not a search")
            elif body["id"] != bug.id or body["result"] not in SEARCHED:
                raise ValueError("it is not a search of this target")
            if set(body["plan"]) != {"workload", "profile", "expect", "seeds"}:
                raise ValueError("its plan is incomplete")
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            return entries, [f"{SEARCHES} line {n}: {error}"]
        entries.append(entry)
        previous = hashlib.sha256(line.encode()).hexdigest()
    if text and not text.endswith("\n"):
        return entries, [f"{SEARCHES}: the last line is cut"]
    return entries, []


def validate_searches(bug, repo):
    """The record is readable, and no committed version of it was ever edited or cut."""
    _, problems = load_searches(bug)
    path = bug.path / SEARCHES
    text = path.read_text(encoding="utf-8") if path.is_file() else ""
    name = path.relative_to(repo).as_posix()
    for sha in git(repo, "log", "--full-history", "--format=%H", "HEAD", "--", name).stdout.split():
        version = git(repo, "show", f"{sha}:{name}", check=False)
        if not version.returncode and not text.startswith(version.stdout):
            problems.append(f"{SEARCHES} is append-only, and it changes or drops lines of its version in {sha[:8]}")
            break
    return problems


def vanished(repo):
    """Targets and records that were committed once and are gone. A replaced target stays listed, or
    a set could pass because the target that was not found had left it."""
    names = git(repo, "log", "--full-history", "--format=", "--name-only", "HEAD", "--",
                *(f":(glob){BUGS_DIR.as_posix()}/**/{name}" for name in (SEARCHES, "bug.toml")), check=False).stdout
    return [f"{name}: a committed target or record of searches cannot leave the tree"
            for name in dict.fromkeys(names.split()) if not (repo / name).is_file()]


def duplicate_problems(bugs):
    """{id: problems}: one hit, one patch or one bug record entered as two targets."""
    problems = {bug.id: [] for bug in bugs}
    for i, one in enumerate(bugs):
        for other in bugs[:i]:
            shared = []
            if one.manifest.get("bug") == other.manifest.get("bug"):
                shared.append(f"the bug record {one.manifest.get('bug')}")
            try:
                mine, theirs = code_changes(one.patch), code_changes(other.patch)
            except Failure:
                mine, theirs = Counter(), Counter()
            if mine and theirs and digest(one.patch) == digest(other.patch):
                shared.append("the same changed lines")
            elif mine and theirs and (mine <= theirs or theirs <= mine):
                shared.append("the changes of one patch within the other")
            here, there = (bug.manifest.get("detection", {}) for bug in (one, other))
            for key in ("trace_hash", "history_sha256", "mechanism", "search"):
                values = [" ".join(str(hit[key]).lower().split()) for hit in (here, there)
                          if isinstance(hit, dict) and key in hit]
                if len(values) == 2 and values[0] == values[1]:
                    shared.append(f"the same detection.{key}")
            for a, b in ((one, other), (other, one)):
                problems[a.id].extend(f"counts once only: {what}, also entered as {b.id}" for what in shared)
    return problems


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
    else:
        problems.extend(applied(repo, bug.patch))
    reverts = reverted(m)[0]
    if m["port"] == "clean" and reverts not in missing:
        files = patch_files(bug.patch)
        old = [historical_path(repo, name, reverts) for name in files]
        reverse = git(repo, "diff", "--no-renames", reverts, f"{reverts}^", "--", *old).stdout
        moved = {current_path(name): lines for name, lines in changed_lines(reverse).items()}
        if moved != changed_lines(bug.patch):
            problems.append(f"port = clean, but the patch does not change exactly the lines of "
                            f"`git diff {reverts[:8]} {reverts[:8]}^` on its files")
        # A clean revert also covers every product file the fix changed, or it is a hand port.
        fixed = git(repo, "diff", "--name-only", f"{reverts}^", reverts).stdout.split()
        omitted = sorted(name for name in fixed
                         if path_problem(name, strict=False) is None and current_path(name) not in files)
        if omitted:
            problems.append(f"port = clean, but the patch leaves out product files the fix changed: "
                            f"{', '.join(omitted)}")
    if require_evidence:
        problems.extend(regression_evidence(bug, repo)[0])
        problems.extend(validate_searches(bug, repo))
        for sha, old in manifest_history(bug, repo):
            if old and any(old.get(key) != m[key] for key in ("bug", "phase", "set")):
                problems.append(f"a target keeps its bug record, phase and set; {sha[:8]} has "
                                f"{old.get('bug')}, {old.get('phase')}, {old.get('set')}")
                break
    for rule in m.get("rules", []):
        source = git(repo, "show", f"HEAD:{rule['source']}", check=False).stdout
        if " ".join(rule["text"].split()) not in " ".join(source.split()):
            problems.append(f"rules: the quote for {rule['kind']} is not in {rule['source']}")
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


def cmd_check(repo, bugs, out, require_evidence=True, every=None):
    """`require_evidence=False` is verify's precondition: verify is what records new evidence.
    `every` holds all targets, of which `bugs` are the selected ones."""
    records, known = load_bug_records(repo), guarantee_ids(repo)
    twice = duplicate_problems(every or bugs) if require_evidence else {}
    failed = 0
    for bug in bugs:
        problems = validate_manifest(bug, records, known, require_evidence)
        if not problems:
            problems = validate_git(bug, repo, require_evidence)
        problems = [*problems, *twice.get(bug.id, [])]
        status = "FAILED" if problems else "ok"
        failed += bool(problems)
        try:
            files = ", ".join(patch_files(bug.patch))
        except Failure:
            files = ""
        reverts = bug.manifest.get("reverts", "?")
        print(f"{bug.id}: {status} ({bug.manifest.get('port', '?')} port of "
              f"{', '.join(str(sha)[:8] for sha in (reverts if isinstance(reverts, list) else [reverts]))}, files: "
              f"{files or 'none'})", file=out)
        for problem in problems:
            print(f"  {problem}", file=out)
        if not problems and require_evidence:
            for problem in static_count_problems(bug, repo):
                print(f"  cannot count: {problem}", file=out)
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
        self.path = base / f"sim-bugs-{os.getpid()}-{time.time_ns()}"

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

    def changed(self):
        """The files that differ from HEAD."""
        status = git(self.path, "status", "--porcelain", "--no-renames", "--untracked-files=all").stdout
        return [line[3:] for line in status.splitlines()]

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
    # The product's own build: no flag of the simulator's, and no inherited one.
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("NABEDRA_SIM_", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_RUSTFLAGS",
                                  "RUSTUP_TOOLCHAIN", "LD_PRELOAD"))}
    env["CARGO_TARGET_DIR"] = str(target)
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
# Seed search (`oltp-sim-bugs-<set>`), through the nabedra-sim-oltp sweep test, by the rule of lead
# decision 8 (server/sim-oltp/bugs/README.md).
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
#   history.jsonl and trace-tail.txt (trace.txt with NABEDRA_SIM_TRACE=full, and summary.json for a
#   failing seed).
# Either way it prints, for every seed it ran,
#   nabedra-sim-seed: {"seed": <int>, "result": "pass" | "violation", "kinds": [<str>...]}
# A run that does not print exactly one readable line for each seed it was asked to run is an error,
# never a pass.

SEED_MARK = "nabedra-sim-seed:"
SEED_LINE = re.compile(r"^(?:test [^\n]+? \.\.\. )?nabedra-sim-seed: (\{.*\})$", re.MULTILINE)
# A trace line: virtual milliseconds, then what happened.
TRACE_LINE = re.compile(r"[0-9]+ (\S.*)")
SWEEP_PACKAGE = "nabedra-sim-oltp"
SWEEP_TARGET = "nabedra_sim_oltp"
SWEEP_TEST = "sweep::tests::sweep"
# The harness needs this stack for the real worker bootstrap (server/sim-oltp/README.md).
HARNESS_STACK = "16777216"
# Both builds use these flags and no inherited ones: a `--cfg` could switch something on.
HARNESS_RUSTFLAGS = "--cfg tokio_unstable"
# The sweep test prints this error through `expect`, so its quotes are escaped.
UNAVAILABLE_WORKLOAD = re.compile(r'OLTP workload [\\]?"([a-z0-9-]+)[\\]?" is unavailable')
# Inherited variables that would change what the harness builds or runs. Each run sets its own.
HARNESS_PREFIXES = ("NABEDRA_", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_", "CARGO_PROFILE_", "RUSTUP_TOOLCHAIN", "RUSTC_BOOTSTRAP",
                    "RUSTC_WORKSPACE_WRAPPER", "LD_PRELOAD")
SEARCHED = ("found", "candidate", "not-detected", "incomplete", "error")


class OutOfTime(Exception):
    pass


class Unavailable(Exception):
    """The harness has no such workload."""


@dataclass
class Plan:
    """What one bug's search runs: its manifest, or a rehearsal's overrides."""
    workload: str
    profile: list
    expect: list
    first: int
    last: int
    budget: int
    defect: str | None = None

    @classmethod
    def of(cls, bug, args):
        first, budget = bug.manifest["seeds"]
        last = min(budget, first + args.seeds - 1) if args.seeds else budget
        profile = bug.manifest["profile"] if args.profile is None else [
            p for p in (part.strip() for part in args.profile.split(",")) if p]
        expect = bug.manifest["expect"] if args.expect is None else [
            e for e in (part.strip() for part in args.expect.split(",")) if e]
        return cls(args.workload or bug.manifest["workload"], profile, expect, first, last, budget,
                   args.rehearse_defect)


def rehearsal(args):
    return any(value is not None for value in (args.workload, args.profile, args.expect,
                                               args.rehearse_defect))


def harness_env(base):
    env = {k: v for k, v in base.items() if not k.startswith(HARNESS_PREFIXES)}
    env["RUSTFLAGS"] = HARNESS_RUSTFLAGS
    env["RUST_MIN_STACK"] = HARNESS_STACK
    return env


def sim_env(env, plan, label, artifacts, first, count, *, expect=False, defect=None, jobs=1):
    env = dict(env, NABEDRA_SIM_WORKLOAD=plan.workload, NABEDRA_SEED=str(first),
               NABEDRA_SEEDS=str(count), NABEDRA_SIM_JOBS=str(jobs), NABEDRA_SIM_CHECK_ID=label,
               NABEDRA_SIM_ARTIFACTS=str(artifacts), NABEDRA_SIM_RECORD_DIR=str(artifacts / "records"))
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


def sweep_binary(env, name):
    """Outside the artifact directory: no build in this job may produce a binary artifact."""
    return Path(env["CARGO_TARGET_DIR"]) / "sweeps" / f"{name}-sweep"


def run_sweep(exe, tree, env, log, timeout=None):
    return run_logged([str(exe), SWEEP_TEST, "--exact", "--nocapture", "--test-threads=1"],
                      tree / "server" / "sim-oltp", env, log, timeout)


def parse_seeds(output):
    """Every seed line of a sweep. A line that cannot be read is a failure, never skipped."""
    seeds = []
    for line in output.splitlines():
        if SEED_MARK not in line:
            continue
        match = SEED_LINE.fullmatch(line)
        try:
            entry = strict_json(match.group(1)) if match else None
        except ValueError:
            entry = None
        if not (isinstance(entry, dict) and type(entry.get("seed")) is int
                and entry.get("result") in ("pass", "violation") and strings(entry.get("kinds"))
                and len(set(entry["kinds"])) == len(entry["kinds"])
                and bool(entry["kinds"]) == (entry["result"] == "violation")):
            raise Failure(f"unreadable seed line {line[:200]!r}")
        seeds.append(entry)
    return seeds


def first_match(seeds, expect):
    for entry in seeds:
        if entry["result"] == "violation" and set(entry["kinds"]) & set(expect):
            return entry
    return None


def strict_json(text):
    """JSON without duplicate keys."""
    def pairs(items):
        data = dict(items)
        if len(data) != len(items):
            raise ValueError("duplicate key")
        return data
    return json.loads(text, object_pairs_hook=pairs)


def read_json(path):
    try:
        return strict_json(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@dataclass
class Replay:
    """One seed run alone as an ordinary sweep, with its replay: what the harness reported."""
    seed: int
    kinds: list
    entry: dict
    history: Path | None
    summary: dict | None
    directory: Path

    @property
    def deterministic(self):
        return self.entry["deterministic"]

    @property
    def identity(self):
        return {"seed": self.seed, "trace_hash": self.entry.get("trace_hash"),
                "history_sha256": sha256_file(self.history) if self.history else None}

    def lines(self):
        """The lines of its history and trace."""
        found = set()
        for name in ("history.jsonl", "trace.txt", "trace-tail.txt"):
            path = self.directory / name
            if path.is_file():
                found.update(path.read_text(encoding="utf-8", errors="replace").splitlines())
        return found

    def events(self):
        """{trace line: its text without the time}, of the lines that are neither the violation nor the
        client history. A line without a time belongs to the text of the line before it."""
        found = {}
        for name in ("trace.txt", "trace-tail.txt"):
            path = self.directory / name
            for line in path.read_text(encoding="utf-8", errors="replace").split("\n") if path.is_file() else ():
                match = TRACE_LINE.fullmatch(line)
                if match and not match.group(1).startswith(("violation ", "history ")):
                    found[line] = match.group(1)
        return found

    def json(self):
        return {"kinds": self.kinds, "deterministic": self.deterministic, **self.identity,
                "trace_events": self.entry.get("trace_events"), "virtual_ms": self.entry.get("virtual_ms"),
                "config_hash": self.entry.get("config_hash"), "violations": (self.summary or {}).get("violations"),
                "history": str(self.history) if self.history else None, "artifacts": str(self.directory)}


@dataclass
class Harness:
    """Runs sweeps of one commit's builds, and raises Failure on anything a run leaves unsaid."""
    args: argparse.Namespace
    tree: Path
    env: dict
    log: object
    out_dir: Path
    clean_exe: Path
    deadline: float | None
    base: dict = field(default_factory=dict)

    def run(self, exe, env, plan):
        """(exit status, seed lines) of one sweep."""
        remaining = None if self.deadline is None else self.deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise OutOfTime
        code, output = run_sweep(exe, self.tree, env, self.log, remaining)
        if code is None:
            raise OutOfTime
        if code < 0:
            raise Failure(f"a sweep from seed {env['NABEDRA_SEED']} died with signal {-code}, so none of its seeds count")
        missing = UNAVAILABLE_WORKLOAD.search(output)
        if missing and SEED_MARK not in output:
            raise Unavailable(missing.group(1))
        entries = parse_seeds(output)
        for entry in entries:
            if entry.get("workload", plan.workload) != plan.workload:
                raise Failure(f"seed {entry['seed']} ran the workload {entry['workload']!r}")
        return code, entries

    def search(self, exe, plan, label, dest, first, last):
        """Seeds first..last of the patched build, up to the first that reports an expected kind."""
        env = sim_env(self.env, plan, label, dest, first, last - first + 1, expect=True,
                      defect=plan.defect, jobs=self.args.sim_jobs)
        code, entries = self.run(exe, env, plan)
        hit = first_match(entries, plan.expect)
        end = hit["seed"] if hit else last
        if [entry["seed"] for entry in entries] != list(range(first, end + 1)):
            raise Failure(f"the search's seed lines are not exactly the seeds {first} to {end}")
        if code != (0 if hit else 101):
            raise Failure(f"the patched search of seeds {first} to {last} exited {code}")
        return entries, hit

    def report(self, side, plan, dest, seeds, defect):
        """The rows of the sweep report, one per seed and in order."""
        report = read_json(dest / "sweep-report.json")
        rows = report.get("seeds") if isinstance(report, dict) else None
        if not isinstance(rows, list) or [isinstance(r, dict) and r.get("seed") for r in rows] != seeds:
            raise Failure(f"the {side} sweep report has no row for exactly seeds {seeds[0]} to {seeds[-1]}")
        if report.get("workload") != plan.workload:
            raise Failure(f"the {side} sweep report is for the workload {report.get('workload')!r}")
        if report.get("defect") != (defect or "none"):
            raise Failure(f"the {side} sweep report names the harness defect {report.get('defect')!r}")
        for row in rows:
            if not strings(row.get("violations")):
                raise Failure(f"the {side} sweep report has no violation kinds for seed {row['seed']}")
        return rows

    def replay(self, side, exe, plan, label, seed, dest, defect=None):
        """Run `seed` alone as an ordinary sweep that replays it, and read the harness's report."""
        shutil.rmtree(dest, ignore_errors=True)
        env = sim_env(self.env, plan, label, dest, seed, 1, defect=defect)
        env.update(NABEDRA_SIM_VERIFY_REPLAY="1", NABEDRA_SIM_TRACE="full")
        code, entries = self.run(exe, env, plan)
        where = f"the {side} replay of seed {seed}"
        if [entry["seed"] for entry in entries] != [seed]:
            raise Failure(f"{where} did not print exactly its seed line")
        entry, = self.report(side, plan, dest, [seed], defect)
        kinds = sorted(entry["violations"])
        if kinds != sorted(entries[0]["kinds"]):
            raise Failure(f"{where}: the seed line and the sweep report disagree")
        if entry.get("replayed") is not True:
            raise Failure(f"{where} was not replayed")
        if entry.get("deterministic") is not ("determinism" not in kinds):
            raise Failure(f"{where} has no deterministic verdict that fits its kinds")
        if code != (101 if kinds else 0):
            raise Failure(f"{where} exited {code} with the kinds {kinds}")
        directory = dest / f"{label}-seed-{seed}"
        history = directory / "history.jsonl"
        history = history if history.is_file() and history.stat().st_size else None
        # A seed that panicked in the harness has no trace and no history to show.
        if "harness" not in kinds:
            if (not TRACE_HASH.fullmatch(str(entry.get("trace_hash"))) or not int(entry["trace_hash"], 16)
                    or type(entry.get("trace_events")) is not int or entry["trace_events"] < 1):
                raise Failure(f"{where} reports no trace")
            if history is None:
                raise Failure(f"{where} left no history")
        return Replay(seed, kinds, entry, history, read_json(directory / "summary.json"), directory)

    def base_kinds(self, plan, label, last):
        """{seed: kinds} of the unpatched build for seeds plan.first..last, cached per workload and profile."""
        key = (plan.workload, tuple(plan.profile), plan.first)
        known = self.base.setdefault(key, {})
        first = plan.first + len(known)
        while first <= last:
            end = min(first + self.args.chunk - 1, last)
            dest = self.out_dir / "base" / f"{label}-{first}"
            env = sim_env(self.env, plan, label, dest, first, end - first + 1, jobs=self.args.sim_jobs)
            code, entries = self.run(self.clean_exe, env, plan)
            seeds = list(range(first, end + 1))
            if [entry["seed"] for entry in entries] != seeds:
                raise Failure(f"the base run's seed lines are not exactly the seeds {first} to {end}")
            kinds = {entry["seed"]: sorted(entry["kinds"]) for entry in entries}
            rows = self.report("base", plan, dest, seeds, None)
            if {row["seed"]: sorted(row["violations"]) for row in rows} != kinds:
                raise Failure("the base run's seed lines and its sweep report disagree")
            if code != (101 if any(kinds.values()) else 0):
                raise Failure(f"the base run of seeds {first} to {end} exited {code}")
            # The report keeps every seed's kinds and trace hash. A seed's files come from a replay.
            for directory in dest.glob(f"{label}-seed-*"):
                shutil.rmtree(directory, ignore_errors=True)
            known.update(kinds)
            first = end + 1
        return {seed: known[seed] for seed in range(plan.first, last + 1)}


def tally(kinds, expect):
    """The seeds that report an expected kind, those that report only other kinds, and those of either
    on which the harness itself failed. The last were tried and are not clean."""
    expected = sorted(seed for seed, reported in kinds.items() if set(reported) & set(expect))
    other = sorted(seed for seed, reported in kinds.items() if reported and seed not in set(expected))
    return {"expected": expected, "other": other,
            "fault": sorted(seed for seed, reported in kinds.items() if set(reported) & set(FAULT_KINDS))}


def judge(harness, bug, plan, label, exe, hit):
    """(mask, patched replay, clean replay) of a seed that the search reports. No mask means a hit
    that only a diagnosis can settle: the mask is why this seed can never count."""
    out = harness.out_dir / bug.id / f"seed-{hit['seed']}"
    seed, kinds = hit["seed"], sorted(hit["kinds"])
    patched = harness.replay("patched", exe, plan, label, seed, out / "patched", plan.defect)

    def mask(reason, **more):
        return {"kinds": kinds, "replay_kinds": patched.kinds, **patched.identity, "reason": reason, **more}

    if set(FAULT_KINDS) & set(kinds) or "harness" in patched.kinds:
        return mask("harness-fault"), patched, None
    if not patched.deterministic:
        return mask("nondeterministic"), patched, None
    if patched.kinds != kinds:
        return mask("search-replay-disagree"), patched, None
    clean = harness.replay("clean", harness.clean_exe, plan, label, seed, out / "clean")
    if clean.kinds:
        return mask("twin-red", twin_kinds=clean.kinds), patched, clean
    if clean.entry["trace_hash"] == patched.entry["trace_hash"]:
        return mask("same-trace"), patched, clean
    known = next((m for m in bug.manifest.get("masked", [])
                  if all(m[key] == value for key, value in patched.identity.items())), None)
    if known:
        return mask("known-finding", finding=known["finding"]), patched, clean
    return None, patched, clean


def mains_build(repo):
    """(what HEAD's build is, why it is not main's or None). Only the files of the targets may differ from
    the merge base: a toolchain file, a Cargo configuration or a rule's source is part of the build."""
    base = git(repo, "merge-base", "HEAD", "refs/remotes/origin/main", check=False)
    if base.returncode:
        return None, "origin/main is unknown, so the unpatched build cannot be shown to be main's"
    base = base.stdout.strip()
    changed = [name for name in git(repo, "diff", "--no-renames", "--name-only", "-z", base, "HEAD").stdout.split("\0")
               if name and not TARGET_FILE.fullmatch(name)]
    if changed:
        return None, f"the unpatched build is not main's: HEAD changes {', '.join(changed[:3])} ({len(changed)} files)"
    behind = git(repo, "rev-list", "--count", f"{base}..refs/remotes/origin/main").stdout.strip()
    return f"on main at {base[:8]}, {behind} commit{'' if behind == '1' else 's'} behind origin/main", None


def count_problems(bug, repo, row, patched, clean):
    """Every reason why this hit is not a detection. Only an empty list counts."""
    problems = static_count_problems(bug, repo)
    if other := mains_build(repo)[1]:
        problems.append(other)
    m, d, seed = bug.manifest, bug.manifest.get("detection"), row["found_seed"]
    if row.get("rates") is None:
        problems.append("the base rate over the seeds tried is incomplete")
    elif seed in row["rates"]["base"]["expected"] + row["rates"]["base"]["other"]:
        problems.append(f"the base run of seed {seed} contradicts its clean twin")
    if d is None:
        return [*problems, "no confirmed mechanism is recorded for this hit"]
    if any(d[key] != value for key, value in patched.identity.items()):
        return [*problems, "the recorded mechanism is for another hit"]
    if d["detector"] not in patched.kinds:
        problems.append(f"{d['detector']} did not report this hit")
    here, events, twin = patched.lines(), patched.events(), clean.events()
    for line in d["lines"]:
        if line not in here:
            problems.append(f"detection.lines: {line!r} is not a line of the hit's history or trace")
    if not any(events.get(line, None) not in (None, *twin.values()) for line in d["lines"]):
        problems.append("no line of detection.lines is an event of the trace that the clean twin lacks, other than the "
                        "violation and the client history")
    entries, _ = load_searches(bug)
    search = next((e["search"] for e in entries if "search" in e and search_hash(e["search"]) == d["search"]), None)
    if search is None:
        problems.append("detection.search names no search in searches.jsonl")
    elif (search["plan"] != plan_of(m) or search["digest"] != digest(bug.patch)):
        problems.append("detection.search is a search of another plan or patch")
    elif search["result"] not in ("candidate", "found") or any(
            (search["hit"] or {}).get(key) != value for key, value in patched.identity.items()):
        problems.append("detection.search did not produce this hit")
    return problems


def search_bug(args, repo, scratch, harness, bug, plan):
    """Apply, build, reset, then search in order until a seed is an unmasked hit. Returns the row."""
    label = f"sim-bug-{bug.id}"
    row = {"id": bug.id, "bug": bug.manifest["bug"], "class": bug.manifest["class"],
           "guarantees": bug.manifest["guarantees"], "scenarios": bug.manifest["scenarios"],
           "patch_sha256": hashlib.sha256(bug.patch.encode()).hexdigest(), "digest": digest(bug.patch),
           "workload": plan.workload, "profile": plan.profile, "expect": plan.expect,
           "seeds": [plan.first, plan.last], "tried": 0, "masked": [], "rates": None,
           "result": "error", "rehearse_defect": plan.defect}
    if not plan.expect:
        return dict(row, result="no-detector", why=bug.manifest["expect_reason"])
    if not_shown(bug) and not rehearsal(args):
        return dict(row, result="not-shown", why=not_shown(bug))
    kinds, hit, patched, clean, stopped = {}, None, None, None, None
    try:
        scratch.apply(bug)
        try:
            row["scratch_worktree"] = str(scratch.path)
            if sorted(scratch.changed()) != sorted(patch_files(bug.patch)):
                raise Failure(f"the patch changed {sorted(scratch.changed())} in the scratch tree")
            exe = build_sweep(args, scratch.path, harness.env, harness.log, sweep_binary(harness.env, bug.id))
        finally:
            scratch.reset()
        row["binaries"] = {"patched": sha256_file(exe), "clean": sha256_file(harness.clean_exe)}
        if row["binaries"]["patched"] == row["binaries"]["clean"]:
            raise Failure("the patched tree builds the same sweep binary as the unpatched one")
        first = plan.first
        try:
            while first <= plan.last and hit is None:
                entries, found = harness.search(exe, plan, label, harness.out_dir / bug.id / "search", first,
                                                min(first + args.chunk - 1, plan.last))
                kinds.update((entry["seed"], entry["kinds"]) for entry in entries)
                row["tried"], first = len(kinds), entries[-1]["seed"] + 1
                if found:
                    masked, patched, clean = judge(harness, bug, plan, label, exe, found)
                    if masked:
                        row["masked"].append(masked)
                    else:
                        hit = found
            if kinds:
                row["rates"] = {"seeds": len(kinds), "patched": tally(kinds, plan.expect),
                                "base": tally(harness.base_kinds(plan, label, max(kinds)), plan.expect)}
        except OutOfTime:
            stopped = f"the time limit passed after {row['tried']} seeds"
    except Unavailable as error:
        return dict(row, result="not-run", why=f"the harness has no {error} workload")
    except Failure as error:
        return dict(row, why=str(error))
    if hit is None:
        if stopped or plan.last < plan.budget:
            why = stopped or f"the run was capped at {plan.last} of {plan.budget} seeds"
            return dict(row, result="incomplete", why=why)
        return dict(row, result="not-detected", why=f"no confirmed hit in {row['tried']} seeds")
    row.update(found_seed=hit["seed"], patched=patched.json(), clean=clean.json())
    if rehearsal(args):
        return dict(row, result="rehearsed") if not stopped else dict(row, result="incomplete", why=stopped)
    problems = count_problems(bug, repo, row, patched, clean)
    if problems:
        return dict(row, result="candidate", why="; ".join(problems), problems=problems)
    detection = bug.manifest["detection"]
    return dict(row, result="found", mechanism=detection["mechanism"], detector=detection["detector"],
                confirmed_by=detection["confirmed_by"], confirmation=detection["confirmation"])


def search_record(bug, row, sha):
    """What searches.jsonl keeps of one search. The log prints it, so the attested log covers it."""
    hit = None
    if "patched" in row:
        hit = {key: row["patched"][key] for key in ("seed", "kinds", "trace_hash", "history_sha256")}
        hit["twin_trace_hash"] = row["clean"]["trace_hash"]
    rates = row["rates"] and {"seeds": row["rates"]["seeds"], **{
        side: {key: len(seeds) for key, seeds in row["rates"][side].items()} for side in ("patched", "base")}}
    url = None
    if os.environ.get("GITHUB_RUN_ID"):
        url = (f"{os.environ.get('GITHUB_SERVER_URL')}/{os.environ.get('GITHUB_REPOSITORY')}"
               f"/actions/runs/{os.environ['GITHUB_RUN_ID']}")
    return {"schema": SEARCH_SCHEMA, "id": bug.id, "sha": sha, "run": url,
            "attempt": os.environ.get("GITHUB_RUN_ATTEMPT"), "plan": plan_of(bug.manifest),
            "digest": row["digest"], "patch_sha256": row["patch_sha256"], "result": row["result"],
            "seeds": row["seeds"], "tried": row["tried"], "hit": hit, "masked": row["masked"],
            "rates": rates, "why": row.get("why")}


def describe(row):
    result, seeds = row["result"], "[{}, {}]".format(*row["seeds"])
    masked = f"{len(row.get('masked', []))} masked"
    rates = ""
    if row.get("rates"):
        base, patched, n = (row["rates"][key] for key in ("base", "patched", "seeds"))
        rates = f"; base rate {len(base['expected'])} of {n}, patched rate {len(patched['expected'])} of {n}"
        if patched["fault"]:
            rates += f"; {len(patched['fault'])} panicked in the harness"
    if result == "not-run":
        return f"{row['id']}: not-run ({row['why']})"
    if result == "not-shown":
        return f"{row['id']}: not shown, not searched ({row['why']})"
    if result == "no-detector":
        return f"{row['id']}: not detected, no rule may detect it ({row['why']})"
    if result == "not-detected":
        return f"{row['id']}: not detected in {seeds}: {row['tried']} seeds tried, {masked}{rates}"
    if result not in ("found", "candidate", "rehearsed"):
        what = "incomplete, neither detected nor not detected" if result == "incomplete" else "error, no conclusion"
        return f"{row['id']}: {what} ({seeds}, {row.get('tried', 0)} seeds tried, {masked}: {row.get('why')}){rates}"
    patched, clean = row["patched"], row["clean"]
    note = f" (defect {row['rehearse_defect']}, not the patch)" if row.get("rehearse_defect") else ""
    if result == "candidate":
        note = f" (NOT counted: {row['why']})"
    confirmed = f"; confirmed by {row['confirmed_by']} ({row['confirmation']})" if result == "found" else ""
    return (f"{row['id']}: {result}{note} seed {row['found_seed']} after {row['tried']} seeds in {seeds}; "
            f"patched {patched['kinds']} trace {patched['trace_hash']} ({patched['trace_events']} events, replay "
            f"deterministic), history sha256 {patched['history_sha256']}; clean twin pass trace "
            f"{clean['trace_hash']} (replay deterministic); {masked}{rates}; patch sha256 "
            f"{row['patch_sha256']}{confirmed}")


def regression_fails_now(repo, bug, args, target, out):
    """Verify the target's regression on this commit and in this job, so that a recorded run is not
    the only thing that shows the patch to be sensitive."""
    options = argparse.Namespace(**{**vars(args), "target_dir": str(target / "regression"), "log": None})
    try:
        return cmd_verify(repo, [bug], options, out) == 0
    except Failure as error:
        print(f"{bug.id}: {error}", file=out, flush=True)
        return False


def differs_from_head(repo, scratch, bugs):
    """Files the run would read that are not the ones HEAD holds. The summary names HEAD."""
    status = git(repo, "status", "--porcelain", "--untracked-files=no").stdout
    names = [line[3:] for line in status.splitlines()]
    for bug in bugs:
        rel = bug.path.relative_to(repo)
        here, there = ({p.relative_to(root).as_posix(): p.read_bytes() for p in (root / rel).rglob("*") if p.is_file()}
                       for root in (repo, scratch.path))
        names += sorted(name for name in here.keys() | there.keys() if here.get(name) != there.get(name))
    if repo.resolve() == REPO and Path(__file__).read_bytes() != (
            scratch.path / Path(__file__).resolve().relative_to(REPO)).read_bytes():
        names.append(Path(__file__).name)
    return names


def cmd_run(repo, bugs, args, out, every=None):
    if git(repo, "cat-file", "-e", f"HEAD:{HARNESS_MANIFEST.as_posix()}", check=False).returncode:
        print(f"{NOT_RUN_MARKER}: {HARNESS_MANIFEST} does not exist yet, so no seed was searched "
              f"for {', '.join(b.id for b in bugs)}", file=out)
        return 0
    if cmd_check(repo, bugs, out, every=every):
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
    limits = [time.monotonic() + args.time_limit] if args.time_limit else []
    rows = []
    with log_path.open("a", encoding="utf-8") as log, Scratch(repo, args.keep) as scratch:
        sha = git(scratch.path, "rev-parse", "HEAD").stdout.strip()
        differing = differs_from_head(repo, scratch, every or bugs) if mode == "evidence" else []
        if differing:
            print(f"the working tree differs from HEAD, which this run would name: {', '.join(differing)}", file=out)
            return 1
        build, other = mains_build(repo)
        print(f"sim-bugs run on {sha}, artifacts {out_dir}\n" + (f"NOT countable: {other}" if other else f"build {build}"),
              file=out, flush=True)
        # The unpatched build first: every clean twin and the base rate run on it.
        clean_exe = build_sweep(args, scratch.path, env, log, sweep_binary(env, "clean"))
        harness = Harness(args, scratch.path, env, log, out_dir, clean_exe, None)
        for bug in bugs:
            own = [time.monotonic() + args.search_timeout] if args.search_timeout else []
            harness.deadline = min(limits + own, default=None)
            row = search_bug(args, repo, scratch, harness, bug, Plan.of(bug, args))
            if row["result"] == "found" and not regression_fails_now(repo, bug, args, target, out):
                why = "its regression did not pass without the patch and fail with it in this run"
                row.update(result="candidate", why=why, problems=[why])
            rows.append(row)
            print(describe(row), file=out, flush=True)
            if mode == "evidence" and row["result"] in SEARCHED:
                row["search"] = search_record(bug, row, sha)
                entries, _ = load_searches(bug)
                row["earlier_searches"] = [e["search"] for e in entries
                                           if "search" in e and e["search"]["plan"] != row["search"]["plan"]]
                print(f"sim-bugs-search: {canonical(row['search'])}", file=out, flush=True)
    summary = {"schema": "nabedra.sim-bugs-run/2", "evidence": "fault-model", "mode": mode,
               "sha": sha, "build": build, "not_mains_build": other, "set": args.set_name, "bugs": rows}
    (out_dir / "sim-bugs-run.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    found = sum(row["result"] == ("rehearsed" if mode == "rehearsal" else "found") for row in rows)
    not_run = [row["id"] for row in rows if row["result"] == "not-run"]
    what = ("pipeline rehearsals completed; no reverted fix was searched for" if mode == "rehearsal" else
            "reverted fixes found within budget, each with a passing clean twin, a base rate over the same "
            "seeds, a confirmed mechanism and its regression failing in this run")
    print(f"sim-bugs run{' (rehearsal)' if mode == 'rehearsal' else ''}: {found} of {len(rows)} "
          f"{what}; summary {out_dir / 'sim-bugs-run.json'}", file=out)
    if found == len(rows):
        return 0
    # Not-run says nothing about the targets that cannot count, and it is never a pass.
    if not_run and all(row["result"] in ("found", "not-run", "not-shown") for row in rows):
        print(f"{NOT_RUN_MARKER}: no workload yet for {', '.join(not_run)}", file=out)
        return 0
    return 1


# --------------------------------------------------------------------------------------------
# The record of searches, and the comparison with attested logs.

def cmd_record(repo, bugs, args, out):
    """Append one search, or the statement that a plan was never searched, to searches.jsonl."""
    if len(bugs) != 1:
        raise Failure("record takes one bug id")
    bug, = bugs
    entries, problems = load_searches(bug)
    if problems:
        raise Failure("; ".join(problems))
    if args.never_searched:
        if not (args.reason or "").strip():
            raise Failure("--never-searched needs --reason")
        rel = (bug.path.relative_to(repo) / "bug.toml").as_posix()
        text = git(repo, "show", f"{args.never_searched}:{rel}").stdout
        entry = {"never_searched": {"plan": plan_of(tomllib.loads(text)), "reason": args.reason}}
    else:
        summary = read_json(args.source or "")
        if not isinstance(summary, dict) or summary.get("schema") != "nabedra.sim-bugs-run/2":
            raise Failure("--from must name the sim-bugs-run.json of a run")
        if summary.get("mode") != "evidence":
            raise Failure("a rehearsal is not a search of the bug")
        search = next((row["search"] for row in summary["bugs"] if row["id"] == bug.id and "search" in row), None)
        if search is None:
            raise Failure(f"that run did not search {bug.id}")
        entry = {"search": search}
    path = bug.path / SEARCHES
    text = path.read_text(encoding="utf-8") if path.is_file() else ""
    previous = text.splitlines()[-1] if text else None
    entry.update(n=len(entries) + 1, prev=previous and hashlib.sha256(previous.encode()).hexdigest())
    path.write_text(text + canonical(entry) + "\n", encoding="utf-8")
    problems = load_searches(bug)[1]
    if problems:
        path.write_text(text, encoding="utf-8")
        raise Failure("; ".join(problems))
    print(f"{bug.id}: recorded as entry {entry['n']}"
          + (f", search {search_hash(entry['search'])}" if "search" in entry else ""), file=out)
    return 0


def cmd_evidence(repo, bugs, args, out):
    """Compare recorded regression lines and searches with the logs of attested runs.

    DIR holds downloaded `evidence-*` artifacts. It does not verify signatures: run
    `gh attestation verify` on each record as well (docs/testing/evidence rules)."""
    root = Path(args.records or "")
    if not args.records or not root.is_dir():
        raise Failure("--records must name a directory of downloaded evidence artifacts")
    logs = {sha256_file(path): path for path in root.rglob("*.log")}
    attested = []
    for path in sorted(root.rglob("*.json")):
        record = read_json(path)
        if not (isinstance(record, dict) and record.get("schema") == "nabedra.evidence/1"):
            continue
        for artifact in record.get("artifacts", []):
            if artifact.get("sha256") in logs:
                text = logs[artifact["sha256"]].read_text(encoding="utf-8", errors="replace")
                attested.append((record, set(text.splitlines())))

    def matching(prefix, sha, url, results):
        return [lines for record, lines in attested
                if str(record.get("check", "")).startswith(prefix) and record.get("sha") == sha
                and (record.get("run") or {}).get("url") == url and record.get("tree_clean") is True
                and record.get("result") in results]

    failed = 0
    for bug in bugs:
        problems, evidence = [], bug.regression.get("evidence")
        if evidence is None:
            problems.append("no regression evidence is recorded")
        else:
            side = evidence["patched"]
            found = matching("oltp-sim-bugs-regression", side["sha"], side["run"], ("pass",))
            wanted = [line for line in side.get("output", "").splitlines() if line.strip()]
            wanted += [f"{bug.id}: without the patch passed",
                       f"{bug.id}: with the patch failed; verified (digest {evidence['digest']})"]
            if not found:
                problems.append(f"no attested oltp-sim-bugs-regression record of {side['sha'][:8]} and its log in DIR")
            elif not any(all(line in lines for line in wanted) for lines in found):
                missing = [line for line in wanted if line not in found[0]]
                problems.append(f"not a line of the attested log: {missing[0]!r}")
        for entry in load_searches(bug)[0]:
            search = entry.get("search")
            if search and search["run"]:
                found = matching("oltp-sim-bugs-", search["sha"], search["run"], ("pass", "fail", "not-run", "error"))
                line = f"sim-bugs-search: {canonical(search)}"
                if not any(line in lines for lines in found):
                    problems.append(f"entry {entry['n']} of {SEARCHES} is not in an attested log in DIR")
            elif search:
                problems.append(f"entry {entry['n']} of {SEARCHES} names no Actions run, so no log covers it")
        failed += bool(problems)
        print(f"{bug.id}: " + ("; ".join(problems) if problems else "the recorded lines are in the log"), file=out)
    print(f"sim-bugs evidence: {len(bugs) - failed} of {len(bugs)} match; signatures are not checked here", file=out)
    return 1 if failed else 0


# --------------------------------------------------------------------------------------------

def main(argv=None, *, repo=REPO, out=sys.stdout):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("check", "verify", "run", "record", "evidence", "digest"))
    parser.add_argument("ids", nargs="*")
    parser.add_argument("--set", dest="set_name", choices=SETS)
    parser.add_argument("--cargo", default=os.environ.get("NABEDRA_CARGO", "cargo"),
                        help="cargo command, for example cargo-slot (default: $NABEDRA_CARGO or cargo)")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--target-dir", help="shared target directory (default server/target/sim-bugs)")
    parser.add_argument("--log", help="build and test log (default <target-dir>/sim-bugs-<command>.log)")
    parser.add_argument("--seeds", type=int, help="run: cap the seeds searched per bug")
    parser.add_argument("--sim-jobs", type=int, default=4, help="run: seeds simulated in parallel")
    parser.add_argument("--chunk", type=int, default=100, help="run: seeds per harness process, which keeps memory bounded")
    parser.add_argument("--search-timeout", type=int, help="run: seconds allowed per bug's search and base rate")
    parser.add_argument("--time-limit", type=int, help="run: seconds after which no further search starts or goes on")
    parser.add_argument("--out", help="run: artifact directory (default target/testing-logs/sim-bugs-run-<pid>)")
    parser.add_argument("--from", dest="source", help="record: the sim-bugs-run.json of a run")
    parser.add_argument("--never-searched", metavar="COMMIT", help="record: a commit whose manifest's plan nobody ran")
    parser.add_argument("--reason", help="record: why that plan was never searched")
    parser.add_argument("--records", help="evidence: directory of downloaded evidence artifacts")
    rehearse = parser.add_argument_group(
        "rehearsal (run)", "Override a manifest to exercise the pipeline, for example on the smoke "
        "workload. A rehearsal is labelled as such and is never evidence for the bug.")
    rehearse.add_argument("--workload", help="workload instead of the manifest's")
    rehearse.add_argument("--profile", help="comma-separated profiles instead of the manifest's ('' for none)")
    rehearse.add_argument("--expect", help="comma-separated kinds instead of the manifest's")
    rehearse.add_argument("--rehearse-defect", help="NABEDRA_SIM_DEFECT for the patched runs only")
    parser.add_argument("--keep", action="store_true", help="keep the scratch worktree")
    args = parser.parse_args(argv)
    if any(value is not None and value < 1 for value in (args.jobs, args.sim_jobs, args.chunk, args.seeds,
                                                         args.search_timeout, args.time_limit)):
        parser.error("--jobs, --sim-jobs, --chunk, --seeds, --search-timeout and --time-limit must be positive")
    if args.expect is not None and not args.expect.strip():
        parser.error("--expect needs at least one kind")
    repo = Path(repo)
    try:
        bugs, errors = load_bugs(repo)
        errors = [*errors, *vanished(repo)]
        if errors:
            for error in errors:
                print(error, file=out)
            return 1
        chosen = select(bugs, args.ids, args.set_name)
        if args.command == "digest":
            for bug in chosen:
                print(f"{bug.id} {digest(bug.patch)} review {whole_digest(bug)}", file=out)
            return 0
        if args.command == "check":
            return cmd_check(repo, chosen, out, every=bugs)
        if args.command == "record":
            return cmd_record(repo, chosen, args, out)
        if args.command == "evidence":
            return cmd_evidence(repo, chosen, args, out)
        if args.command == "verify":
            if cmd_check(repo, chosen, out, require_evidence=False):
                return 1
            return cmd_verify(repo, chosen, args, out)
        return cmd_run(repo, chosen, args, out, every=bugs)
    except Failure as error:
        print(f"sim-bugs: {error}", file=out)
        return 2


if __name__ == "__main__":
    sys.exit(main())

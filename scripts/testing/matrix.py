#!/usr/bin/env python3
"""Coverage matrix: guarantee registers x evidence records.

  matrix.py build  [--allow-unattested] [--verify-attestations] [--head REV] [--out-dir D]
  matrix.py select --base SHA --head SHA [--json]
  matrix.py lint
  matrix.py gate   --sha SHA --scope release|module:<m> [--allow-unattested] [--verify-attestations]

Registers are docs/testing/guarantees/*.md. Each has a `Module:` line, a `Level:` line and one
table with the header `| ID | Guarantee | Failure model | Scenarios | Checks | n/a |`. Optional
`Coverage note:` lines describe check-level scope without changing cell states. Records are
`nabedra.evidence/1` files under docs/testing/evidence/records. Checks are
docs/testing/checks/*.toml, and docs/testing/modules.toml maps paths to modules.

A record counts toward a cell only if it is valid, its sha is HEAD or an ancestor of HEAD, and it
is attested (`gh attestation verify` succeeds under --verify-attestations) or --allow-unattested
is given. Cell states: pass, fail, not-run: <reason>, quarantined p/n, stale (<n> behind),
n/a and missing. The gate verifies attestations by default. Cells whose n/a reason starts with
`cloud-deferred` or `hardware-deferred` are deferred: listed by name and never counted as green.
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evidence  # noqa: E402
import run_check  # noqa: E402

REPO_ROOT = evidence.REPO_ROOT
KINDS = evidence.KINDS
LEVELS = ("L1", "L2", "L3")
DEFERRED_PREFIXES = ("cloud-deferred", "hardware-deferred")
REGISTER_HEADER = ["id", "guarantee", "failure model", "scenarios", "checks", "n/a"]
TEST_FILE_GLOBS = ("server/**/tests/**", "**/test_*.py", "orm/**/test/**", "orm/**/tests/**",
                   "orm/contract/**")
CHECK_FILE = re.compile(r"docs/testing/checks/([a-z0-9-]+)\.toml")
# Design specs (.agents/skills/design-review). specs/<module>/** selects that module's checks, which
# include its kind T checks. The shared tooling selects the testing module and every T check.
SPEC_FILE = re.compile(r"specs/([^/]+)/.+")
SPEC_TOOLING = ("specs/lib/", "specs/tools/")
SPEC_TESTING = (*SPEC_TOOLING, "specs/selftest/", "specs/README.md")


class MatrixError(ValueError):
    pass


@dataclass
class Paths:
    """Every input location, so tests can point the tool at fixtures."""
    repo: Path = REPO_ROOT
    registers: Path = REPO_ROOT / "docs" / "testing" / "guarantees"
    records: Path = REPO_ROOT / "docs" / "testing" / "evidence" / "records"
    checks: Path = REPO_ROOT / "docs" / "testing" / "checks"
    modules: Path = REPO_ROOT / "docs" / "testing" / "modules.toml"
    out: Path = REPO_ROOT / "docs" / "testing"


# --------------------------------------------------------------------------------------------
# Globs and modules.

@functools.lru_cache(maxsize=None)
def glob_regex(pattern: str) -> re.Pattern:
    """`**` spans directories (including none), `*` stays within a segment, `?` is one character."""
    out, i = [], 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("^" + "".join(out) + "$")


def glob_match(path: str, patterns: Iterable[str]) -> bool:
    return any(glob_regex(p).match(path) for p in patterns)


@dataclass
class Module:
    name: str
    level: str
    register: str | None
    paths: list[str]
    exclude: list[str]
    hot_paths: list[str]

    def owns(self, path: str) -> bool:
        return glob_match(path, self.paths) and not glob_match(path, self.exclude)


@dataclass
class Modules:
    modules: list[Module]
    fallback: str
    pools: list[dict]
    core_hours: float

    def names(self) -> set[str]:
        return {m.name for m in self.modules} | {self.fallback}

    def get(self, name: str) -> Module | None:
        return next((m for m in self.modules if m.name == name), None)

    def modules_for(self, path: str) -> list[str]:
        owners = [m.name for m in self.modules if m.owns(path)]
        return owners or [self.fallback]

    def problems(self) -> list[str]:
        out = []
        names = [m.name for m in self.modules]
        if len(set(names)) != len(names):
            out.append("modules.toml: duplicate module names")
        if self.fallback in names:
            out.append(f"modules.toml: fallback {self.fallback!r} must not be a declared module")
        for m in self.modules:
            if m.level not in LEVELS:
                out.append(f"modules.toml: module {m.name} has level {m.level!r}, not one of {LEVELS}")
            if not m.paths:
                out.append(f"modules.toml: module {m.name} has no paths")
        total = sum(p.get("share", 0) for p in self.pools)
        if self.pools and total != 100:
            out.append(f"modules.toml: nightly pool shares add up to {total}, not 100")
        for pool in self.pools:
            for name in pool.get("modules", []):
                if name not in names:
                    out.append(f"modules.toml: nightly pool {pool.get('name')} names unknown module {name!r}")
        return out


def load_modules(path: Path) -> Modules:
    data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    mods = [
        Module(
            name=m["name"], level=m.get("level", ""), register=m.get("register"),
            paths=list(m.get("paths", [])), exclude=list(m.get("exclude", [])),
            hot_paths=list(m.get("hot_paths", [])),
        )
        for m in data.get("module", [])
    ]
    nightly = data.get("nightly", {})
    return Modules(mods, data.get("fallback", "misc"), list(nightly.get("pool", [])),
                   float(nightly.get("core_hours", 0)))


# --------------------------------------------------------------------------------------------
# Registers.

@dataclass
class Guarantee:
    id: str
    statement: str
    failure_model: str
    scenarios: list[str]
    checks: list[str]
    na: dict[str, str]
    module: str = ""
    level: str = ""
    register: str = ""

    def deferred(self, kind: str) -> str | None:
        reason = self.na.get(kind, "")
        return next((p for p in DEFERRED_PREFIXES if reason == p or reason.startswith(f"{p} (")), None)


@dataclass
class Register:
    path: Path
    module: str
    level: str
    guarantees: list[Guarantee]
    coverage_notes: list[str] = field(default_factory=list)


def _split_row(line: str) -> list[str]:
    body = line.strip()
    if body.startswith("|"):
        body = body[1:]
    if body.endswith("|") and not body.endswith("\\|"):
        body = body[:-1]
    cells = re.split(r"(?<!\\)\|", body)
    return [c.strip().replace("\\|", "|") for c in cells]


def _plain(text: str) -> str:
    return text.replace("`", "").replace("*", "").strip()


def parse_register(text: str, path: Path) -> Register:
    module = level = None
    for line in text.splitlines():
        plain = _plain(line)
        m = re.match(r"^Module:\s*(\S+)\s*$", plain)
        if m and module is None:
            module = m.group(1)
        m = re.match(r"^Level:\s*(L[123])\b", plain)
        if m and level is None:
            level = m.group(1)
    if module is None:
        raise MatrixError(f"{path}: no `Module: <module>` line")
    if level is None:
        raise MatrixError(f"{path}: no `Level: L1|L2|L3` line")

    lines = text.splitlines()
    starts = [i for i, l in enumerate(lines)
              if l.strip().startswith("|") and [c.lower() for c in _split_row(l)] == REGISTER_HEADER]
    if len(starts) != 1:
        raise MatrixError(f"{path}: expected exactly one guarantee table with header "
                          f"| ID | Guarantee | Failure model | Scenarios | Checks | n/a |, found {len(starts)}")
    rows = []
    i = starts[0] + 2
    while i < len(lines) and lines[i].strip().startswith("|"):
        cells = _split_row(lines[i])
        if len(cells) != 6:
            raise MatrixError(f"{path}:{i + 1}: expected 6 cells, found {len(cells)}")
        gid = _plain(cells[0])
        if not run_check.GUARANTEE_RE.match(gid):
            raise MatrixError(f"{path}:{i + 1}: {gid!r} is not a guarantee ID")
        checks = [k for k in re.split(r"[\s,]+", _plain(cells[4])) if k]
        bad = [k for k in checks if k not in KINDS or len(k) != 1]
        if bad:
            raise MatrixError(f"{path}:{i + 1}: unknown check kinds {bad}")
        na = {}
        na_text = cells[5].strip()
        if na_text not in ("", "-", "none"):
            for entry in na_text.split(";"):
                entry = entry.strip()
                if not entry:
                    continue
                kind, sep, reason = entry.partition(":")
                kind = _plain(kind)
                if not sep or kind not in KINDS or len(kind) != 1 or not reason.strip():
                    raise MatrixError(f"{path}:{i + 1}: n/a entry {entry!r} is not `<kind>: <reason>`")
                na[kind] = reason.strip()
        scenarios = [s for s in re.split(r"[\s,]+", _plain(cells[3])) if s and s not in ("-", "none")]
        rows.append(Guarantee(gid, cells[1], cells[2], scenarios, checks, na, module, level, str(path)))
        i += 1
    if not rows:
        raise MatrixError(f"{path}: the guarantee table has no rows")
    notes = [line.removeprefix("Coverage note: ").strip()
             for line in lines if line.startswith("Coverage note: ")]
    return Register(Path(path), module, level, rows, notes)


def load_registers(directory: Path) -> list[Register]:
    directory = Path(directory)
    if not directory.exists():
        return []
    return [parse_register(p.read_text(encoding="utf-8"), p) for p in sorted(directory.glob("*.md"))
            if p.name.lower() != "readme.md"]


# --------------------------------------------------------------------------------------------
# Git and attestation.

class Git:
    def __init__(self, repo: Path):
        self.repo = Path(repo)
        self._cache: dict[tuple, object] = {}

    def run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(["git", *args], cwd=self.repo, capture_output=True, text=True, check=check)

    def rev(self, rev: str) -> str:
        out = self.run("rev-parse", "--verify", "--quiet", "--end-of-options", f"{rev}^{{commit}}", check=False)
        if out.returncode != 0:
            raise MatrixError(f"unknown revision {rev!r}")
        return out.stdout.strip()

    def behind(self, sha: str, head: str) -> int | None:
        """Commits from `sha` to `head` if `sha` is `head` or its ancestor, else None."""
        key = ("behind", sha, head)
        if key not in self._cache:
            value = None
            if sha == head:
                value = 0
            else:
                commits = self.run("rev-list", "--left-right", "--count", f"{sha}...{head}", check=False)
                if commits.returncode == 0:
                    ahead, behind = map(int, commits.stdout.split())
                    if ahead == 0:
                        value = behind
            self._cache[key] = value
        return self._cache[key]  # type: ignore[return-value]

    def changed(self, base: str, head: str) -> list[str]:
        out = self.run("diff", "--name-only", "--no-renames", "-z", base, head)
        return out.stdout.rstrip("\0").split("\0") if out.stdout else []

    def files(self) -> list[str]:
        out = self.run("ls-files", "-z", "--cached")
        return sorted(set(out.stdout.rstrip("\0").split("\0"))) if out.stdout else []

    def origin_repo(self) -> str | None:
        out = self.run("remote", "get-url", "origin", check=False)
        m = re.fullmatch(
            r"(?:(?:https|ssh)://)?(?:[^@/:]+@)?(?:github\.com|github-[^:/.]+)"
            r"[:/]([^/]+/[^/]+?)(?:\.git)?", out.stdout.strip()
        )
        return m.group(1) if m else None


def verify_attestation(path: Path, repo_slug: str) -> bool:
    cmd = ["gh", "attestation", "verify", str(path), "--repo", repo_slug]
    try:
        return subprocess.run(cmd, capture_output=True, timeout=120).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


# --------------------------------------------------------------------------------------------
# Cells.

@dataclass
class Counted:
    path: Path
    record: dict
    behind: int


@dataclass
class Cell:
    guarantee: str
    kind: str
    required: bool
    state: str              # pass fail not-run quarantined stale n/a missing (or "" if not required)
    reason: str = ""
    deferred: str | None = None
    behind: int | None = None
    last: str | None = None  # the underlying result of a stale cell
    sha: str | None = None
    date: str | None = None
    checks: list[str] = field(default_factory=list)
    run_url: str | None = None
    executed: int = 0
    failed: int = 0

    def text(self) -> str:
        if not self.required:
            return self.state  # "" or "n/a"; the register reason is in the JSON
        label = self.state
        if self.state == "stale":
            label = f"stale ({self.behind} behind)"
        if self.state == "not-run" and self.reason:
            label = f"{label}: {self.reason}"
        if self.state == "missing" and self.reason:
            label = f"missing ({self.reason})"
        parts = [label]
        if self.sha:
            parts.append(f"`{self.sha[:7]}`")
            if self.date:
                parts.append(self.date)
            parts.append(f"{self.executed - self.failed}/{self.executed}")
            if self.run_url:
                parts.append(f"[run]({self.run_url})")
        return " · ".join(parts).replace("|", "\\|")

    def green(self) -> bool:
        return self.state == "pass" and self.deferred is None


def _combine(group: list[Counted]) -> tuple[str, str, int, int, list[str]]:
    """Combines records on one SHA. Failures and quarantine take precedence over a pass."""
    results = [c.record["result"] for c in group]
    checks = sorted({c.record["check"] for c in group})
    executed = sum(c.record["assertions"]["executed"] for c in group)
    failed = sum(c.record["assertions"]["failed"] for c in group)
    if "fail" in results or "error" in results:
        bad = sorted({c.record["check"] for c in group if c.record["result"] in ("fail", "error")})
        return "fail", "failed: " + ", ".join(bad), executed, failed, checks
    if "quarantined" in results:
        q = [c for c in group if c.record["result"] == "quarantined"]
        runs = sum(c.record["assertions"]["executed"] for c in q)
        ok = runs - sum(c.record["assertions"]["failed"] for c in q)
        return f"quarantined {ok}/{runs}", "", runs, runs - ok, checks
    if "pass" in results:
        return "pass", "", executed, failed, checks
    reasons = sorted({n["reason"] for c in group for n in c.record["assertions"]["not_run"]})
    return "not-run", "; ".join(reasons), executed, failed, checks


def evaluate_cell(g: Guarantee, kind: str, counted: list[Counted], ignored_for: int) -> Cell:
    required = kind in g.checks
    na = g.na.get(kind)
    deferred = g.deferred(kind)
    if not required:
        return Cell(g.id, kind, False, "n/a" if na else "", reason=na or "")
    if na and not deferred:
        return Cell(g.id, kind, True, "n/a", reason=na)
    mine = [c for c in counted if g.id in c.record["guarantees"] and kind in c.record["kind"].split(" ")]
    if not mine:
        if deferred:
            return Cell(g.id, kind, True, "not-run", reason=na or "", deferred=deferred)
        reason = f"{ignored_for} uncounted record(s)" if ignored_for else ""
        return Cell(g.id, kind, True, "missing", reason=reason)
    nearest = min(mine, key=lambda c: (c.behind, c.record["sha"]))
    sha = nearest.record["sha"]
    group = [c for c in mine if c.record["sha"] == sha]
    state, reason, executed, failed, checks = _combine(group)
    run = next((c.record["run"] for c in group if c.record["run"]["url"]), nearest.record["run"])
    cell = Cell(g.id, kind, True, state, reason=reason, deferred=deferred, behind=nearest.behind, sha=sha,
                date=run["date"], checks=checks, run_url=run["url"], executed=executed, failed=failed)
    if deferred:
        cell.last, cell.state, cell.reason = cell.state, "not-run", na
    elif nearest.behind > 0:
        cell.last, cell.state = cell.state, "stale"
    return cell


@dataclass
class Matrix:
    head: str
    registers: list[Register]
    cells: dict[tuple[str, str], Cell]
    kinds: list[str]
    ignored: list[dict]
    allow_unattested: bool
    verified: bool

    def rows(self) -> list[Guarantee]:
        return [g for r in self.registers for g in r.guarantees]


def count_records(records: list[tuple[Path, dict]], head: str, git: Git, *, allow_unattested: bool,
                  verify: bool, repo_slug: str | None) -> tuple[list[Counted], list[dict]]:
    schema = evidence.load_schema()
    counted, ignored = [], []
    for path, record in records:
        errors = evidence.validate(record, schema) if isinstance(record, dict) else ["not an object"]
        if errors:
            ignored.append({"path": str(path), "reason": "invalid: " + errors[0], "record": record})
            continue
        if not record["tree_clean"]:
            ignored.append({"path": str(path), "reason": "record was produced on a dirty tree", "record": record})
            continue
        behind = git.behind(record["sha"], head)
        if behind is None:
            ignored.append({"path": str(path), "reason": "sha is not HEAD or an ancestor of HEAD", "record": record})
            continue
        if not allow_unattested and not (verify and repo_slug and verify_attestation(path, repo_slug)):
            why = "attestation did not verify" if verify else "unattested (pass --verify-attestations)"
            if verify and not repo_slug:
                why = "cannot verify attestation without a GitHub origin"
            ignored.append({"path": str(path), "reason": why, "record": record})
            continue
        counted.append(Counted(path, record, behind))
    return counted, ignored


def build_matrix(paths: Paths, head_rev: str = "HEAD", *, allow_unattested: bool = False,
                 verify: bool = False) -> Matrix:
    git = Git(paths.repo)
    head = git.rev(head_rev)
    registers = load_registers(paths.registers)
    seen: dict[str, str] = {}
    for reg in registers:
        for g in reg.guarantees:
            if g.id in seen:
                raise MatrixError(f"guarantee {g.id} is defined in both {seen[g.id]} and {reg.path}")
            seen[g.id] = str(reg.path)
    records = evidence.load_records(paths.records)
    repo_slug = git.origin_repo() if records and verify and not allow_unattested else None
    counted, ignored = count_records(records, head, git, allow_unattested=allow_unattested,
                                     verify=verify, repo_slug=repo_slug)
    kinds = [k for k in KINDS if any(k in g.checks for r in registers for g in r.guarantees)]
    cells = {}
    for reg in registers:
        for g in reg.guarantees:
            for kind in kinds:
                uncounted = sum(
                    1 for i in ignored
                    if isinstance(i["record"], dict) and isinstance(i["record"].get("guarantees"), list)
                    and g.id in i["record"]["guarantees"]
                    and kind in str(i["record"].get("kind", "")).split(" ")
                )
                cells[(g.id, kind)] = evaluate_cell(g, kind, counted, uncounted)
    return Matrix(head, registers, cells, kinds, ignored, allow_unattested, verify)


def render_markdown(m: Matrix) -> str:
    out = ["# Coverage matrix", "",
           f"Generated by `scripts/testing/matrix.py build` for `{m.head}`. Do not edit by hand.", ""]
    if m.allow_unattested:
        out += ["**Unattested evidence was allowed for this build.** Cells may rest on records whose "
                "signatures were not verified.", ""]
    totals: dict[str, int] = {}
    for cell in m.cells.values():
        if cell.required:
            key = cell.state.split(" ")[0] + (" (deferred)" if cell.deferred else "")
            totals[key] = totals.get(key, 0) + 1
    out.append("Required cells: " + (", ".join(f"{k} {v}" for k, v in sorted(totals.items())) or "none") + ".")
    out.append("")
    for reg in m.registers:
        out += [f"## {reg.module} ({reg.level})", "", f"Register: `{os.path.relpath(reg.path, REPO_ROOT) if reg.path.is_absolute() else reg.path}`", ""]
        if reg.coverage_notes:
            out += [f"- {note}" for note in reg.coverage_notes] + [""]
        out.append("| ID | Guarantee | " + " | ".join(m.kinds) + " |")
        out.append("| --- | --- | " + " | ".join("---" for _ in m.kinds) + " |")
        for g in reg.guarantees:
            cells = [m.cells[(g.id, k)].text() for k in m.kinds]
            out.append(f"| {g.id} | {g.statement} | " + " | ".join(cells) + " |")
        out.append("")
    deferred = [c for c in m.cells.values() if c.deferred]
    if deferred:
        out += ["## Deferred cells", "", "These are never counted as green.", ""]
        out += [f"- {c.guarantee} {c.kind}: {c.reason or c.deferred}" for c in deferred]
        out.append("")
    if m.ignored:
        out += ["## Records not counted", ""]
        out += [f"- `{os.path.basename(i['path'])}`: {i['reason']}" for i in m.ignored]
        out.append("")
    return "\n".join(out)


def render_json(m: Matrix) -> dict:
    rows = []
    for reg in m.registers:
        for g in reg.guarantees:
            cells = {}
            for k in m.kinds:
                c = m.cells[(g.id, k)]
                if not c.required:
                    if c.state:
                        cells[k] = {"required": False, "state": c.state, "reason": c.reason}
                    continue
                cells[k] = {key: getattr(c, key) for key in (
                    "required", "state", "reason", "deferred", "behind", "last", "sha", "date",
                    "checks", "run_url", "executed", "failed") if getattr(c, key) not in (None, "", [])}
                cells[k].setdefault("required", True)
            rows.append({"id": g.id, "module": g.module, "level": g.level, "guarantee": g.statement,
                         "scenarios": g.scenarios, "cells": cells})
    return {"schema": "nabedra.coverage-matrix/1", "head": m.head, "allow_unattested": m.allow_unattested,
            "verified_attestations": m.verified, "kinds": m.kinds, "rows": rows,
            "coverage_notes": {reg.module: reg.coverage_notes for reg in m.registers if reg.coverage_notes},
            "ignored_records": [{"path": os.path.basename(i["path"]), "reason": i["reason"]} for i in m.ignored]}


# --------------------------------------------------------------------------------------------
# Select.

def guarantees_for_module(name: str, modules: Modules, registers: list[Register]) -> list[str]:
    mod = modules.get(name)
    reg_path = mod.register if mod else None
    out = []
    for reg in registers:
        rel = reg.path.as_posix()
        if reg.module == name or (reg_path and rel.endswith(reg_path)):
            out.extend(g.id for g in reg.guarantees)
    return out


def spec_module(path: str, modules: Modules) -> str | None:
    """The module a path under specs/ belongs to by folder convention, or None.

    specs/<module>/** belongs to <module> when it is a module in modules.toml. The shared spec
    tooling (specs/lib, specs/tools, specs/selftest) and specs/README.md belong to `testing`."""
    if path.startswith(SPEC_TESTING):
        return "testing" if modules.get("testing") else None
    m = SPEC_FILE.fullmatch(path)
    if m and modules.get(m.group(1)):
        return m.group(1)
    return None


def select(paths: Paths, base: str, head: str, changed: list[str] | None = None) -> dict:
    git = Git(paths.repo)
    files = changed if changed is not None else git.changed(git.rev(base), git.rev(head))
    modules = load_modules(paths.modules)
    registers = load_registers(paths.registers)
    checks = run_check.load_all(paths.checks, modules.names())
    check_modules = {c.id: c.module for c in checks}
    touched: dict[str, list[str]] = {}
    hot: dict[str, list[str]] = {}
    for f in files:
        owners = modules.modules_for(f)
        spec_owner = spec_module(f, modules)
        if spec_owner:
            owners = list(dict.fromkeys([*(n for n in owners if n != modules.fallback), spec_owner]))
        check_file = CHECK_FILE.fullmatch(f)
        if check_file:
            owner = check_modules.get(check_file[1], modules.fallback)
            owners = list(dict.fromkeys([*(n for n in owners if n != modules.fallback), owner]))
        for name in owners:
            touched.setdefault(name, []).append(f)
            mod = modules.get(name)
            if mod and glob_match(f, mod.hot_paths):
                hot.setdefault(name, []).append(f)
    full = modules.fallback in touched
    every_t = any(f.startswith(SPEC_TOOLING) for f in files)
    t1 = [c for c in checks if "T1" in c.tiers
          and (full or c.module in touched or (every_t and "T" in c.kinds))]
    guarantees: list[str] = []
    for name in sorted(touched):
        for gid in guarantees_for_module(name, modules, registers):
            if gid not in guarantees:
                guarantees.append(gid)
    for c in t1:
        for gid in c.guarantees:
            if gid not in guarantees:
                guarantees.append(gid)
    return {
        "base": base, "head": head, "changed_files": files,
        "modules": {name: {"level": (modules.get(name).level if modules.get(name) else None),
                           "files": fs} for name, fs in sorted(touched.items())},
        "full_t1": full,
        "all_t_checks": every_t,
        "t1_checks": [{"id": c.id, "module": c.module, "kinds": c.kinds, "needs": c.needs} for c in t1],
        "guarantees": guarantees,
        "paired_ab": {name: fs for name, fs in sorted(hot.items())},
    }


def render_select(sel: dict) -> str:
    out = [f"# Selection for `{sel['base'][:12]}..{sel['head'][:12]}`", "",
           f"{len(sel['changed_files'])} changed files.", "", "## Modules", ""]
    for name, info in sel["modules"].items():
        out.append(f"- {name} ({info['level'] or 'unmapped'}): {len(info['files'])} files")
    if sel["full_t1"]:
        out += ["", "Some paths map to no module, so every T1 check is selected."]
    if sel.get("all_t_checks"):
        out += ["", "The shared spec tooling changed, so every kind T check is selected."]
    out += ["", "## T1 checks", ""]
    out += [f"- `{c['id']}` ({c['module']}, {' '.join(c['kinds'])})" for c in sel["t1_checks"]] or ["- none registered"]
    out += ["", "## Guarantees the review must cover", ""]
    out.append(", ".join(sel["guarantees"]) if sel["guarantees"] else "none")
    if sel["paired_ab"]:
        out += ["", "## Paired A/B required (hot paths touched)", ""]
        out += [f"- {name}: {', '.join(fs)}" for name, fs in sel["paired_ab"].items()]
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------------------------
# Lint.

def lint(paths: Paths, files: list[str] | None = None) -> tuple[list[str], list[str]]:
    """Returns (errors, warnings)."""
    errors: list[str] = []
    warnings: list[str] = []
    try:
        modules = load_modules(paths.modules)
    except (OSError, tomllib.TOMLDecodeError, KeyError) as exc:
        return [f"modules.toml: {exc}"], []
    errors.extend(modules.problems())
    registers: list[Register] = []
    for p in sorted(Path(paths.registers).glob("*.md")) if Path(paths.registers).exists() else []:
        if p.name.lower() == "readme.md":
            continue
        try:
            registers.append(parse_register(p.read_text(encoding="utf-8"), p))
        except MatrixError as exc:
            errors.append(str(exc))
    for m in modules.modules:
        if m.register and not any(r.path.as_posix().endswith(m.register) for r in registers):
            errors.append(f"modules.toml: module {m.name} names register {m.register}, "
                          "which matches no register file")
    known: dict[str, Guarantee] = {}
    for reg in registers:
        for g in reg.guarantees:
            if g.id in known:
                errors.append(f"{reg.path}: guarantee {g.id} is also defined in {known[g.id].register}")
            known[g.id] = g
    checks = []
    for p in sorted(Path(paths.checks).glob("*.toml")):
        try:
            checks.append(run_check.load_check(p, modules.names()))
        except run_check.CheckError as exc:
            errors.append(str(exc))
    provided: set[tuple[str, str]] = set()
    for c in checks:
        for gid in c.guarantees:
            if gid not in known:
                errors.append(f"{c.path}: references unknown guarantee {gid}")
            for k in c.kinds:
                provided.add((gid, k))
    for g in known.values():
        for k in g.checks:
            if g.na.get(k):  # n/a, or deferred and already listed by the gate
                continue
            if (g.id, k) not in provided:
                warnings.append(f"{g.id} requires kind {k} but no registered check provides it")
    files = files if files is not None else Git(paths.repo).files()
    for f in files:
        if glob_match(f, TEST_FILE_GLOBS) and modules.modules_for(f) == [modules.fallback]:
            errors.append(f"{f}: test file matches no module glob in modules.toml (dangling test)")
    return errors, warnings


# --------------------------------------------------------------------------------------------
# Gate.

@dataclass
class GateResult:
    ok: bool
    sha: str
    scope: str
    passed: list[Cell]
    failing: list[Cell]
    deferred: list[Cell]

    def report(self) -> str:
        out = [f"gate {self.scope} on {self.sha}: {'PASS' if self.ok else 'FAIL'}",
               f"  green cells: {len(self.passed)}, not green: {len(self.failing)}, deferred: {len(self.deferred)}"]
        for c in self.failing:
            out.append(f"  not green: {c.guarantee} {c.kind}: {c.text() or c.state}")
        for c in self.deferred:
            out.append(f"  deferred (not counted as green): {c.guarantee} {c.kind}: {c.reason or c.deferred}")
        if not self.passed and not self.failing and not self.deferred:
            out.append("  no required cells are in scope")
        return "\n".join(out)


def gate(paths: Paths, sha: str, scope: str, *, allow_unattested: bool = False, verify: bool = False) -> GateResult:
    matrix = build_matrix(paths, sha, allow_unattested=allow_unattested, verify=verify)
    if scope == "release":
        regs = matrix.registers
    elif scope.startswith("module:"):
        name = scope.split(":", 1)[1]
        modules = load_modules(paths.modules)
        wanted = set(guarantees_for_module(name, modules, matrix.registers))
        regs = [Register(r.path, r.module, r.level, [g for g in r.guarantees if g.id in wanted])
                for r in matrix.registers]
    else:
        raise MatrixError(f"unknown scope {scope!r}: use release or module:<name>")
    passed, failing, deferred = [], [], []
    for reg in regs:
        for g in reg.guarantees:
            for k in matrix.kinds:
                cell = matrix.cells[(g.id, k)]
                if not cell.required or (cell.state == "n/a" and not cell.deferred):
                    continue
                if cell.deferred:
                    deferred.append(cell)
                elif cell.green() and cell.behind == 0 and cell.sha == matrix.head:
                    passed.append(cell)
                else:
                    failing.append(cell)
    ok = bool(passed) and not failing and not deferred
    return GateResult(ok, matrix.head, scope, passed, failing, deferred)


# --------------------------------------------------------------------------------------------

def add_path_args(p: argparse.ArgumentParser) -> None:
    d = Paths()
    p.add_argument("--repo", type=Path, default=d.repo)
    p.add_argument("--registers-dir", type=Path, default=d.registers)
    p.add_argument("--evidence-dir", type=Path, default=d.records)
    p.add_argument("--checks-dir", type=Path, default=d.checks)
    p.add_argument("--modules", type=Path, default=d.modules)


def paths_from(args: argparse.Namespace) -> Paths:
    return Paths(args.repo, args.registers_dir, args.evidence_dir, args.checks_dir, args.modules,
                 getattr(args, "out_dir", None) or Paths().out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    add_path_args(b)
    b.add_argument("--head", default="HEAD")
    b.add_argument("--out-dir", type=Path)
    b.add_argument("--allow-unattested", action="store_true")
    b.add_argument("--verify-attestations", action="store_true")
    s = sub.add_parser("select")
    add_path_args(s)
    s.add_argument("--base", required=True)
    s.add_argument("--head", required=True)
    s.add_argument("--json", action="store_true")
    li = sub.add_parser("lint")
    add_path_args(li)
    g = sub.add_parser("gate")
    add_path_args(g)
    g.add_argument("--sha", required=True)
    g.add_argument("--scope", required=True)
    g.add_argument("--allow-unattested", action="store_true")
    g.add_argument("--verify-attestations", action="store_true", default=True)
    args = parser.parse_args(argv)
    paths = paths_from(args)
    try:
        if args.cmd == "build":
            m = build_matrix(paths, args.head, allow_unattested=args.allow_unattested,
                             verify=args.verify_attestations)
            paths.out.mkdir(parents=True, exist_ok=True)
            (paths.out / "coverage-matrix.md").write_text(render_markdown(m), encoding="utf-8")
            (paths.out / "coverage-matrix.json").write_text(
                json.dumps(render_json(m), indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(f"wrote {paths.out / 'coverage-matrix.md'} and .json "
                  f"({len(m.rows())} guarantees, {len(m.ignored)} records not counted)")
            return 0
        if args.cmd == "select":
            sel = select(paths, args.base, args.head)
            print(json.dumps(sel, indent=2) if args.json else render_select(sel), end="" if not args.json else "\n")
            return 0
        if args.cmd == "lint":
            errors, warnings = lint(paths)
            for w in warnings:
                print(f"warning: {w}")
            for e in errors:
                print(f"error: {e}")
            print(f"lint: {len(errors)} errors, {len(warnings)} warnings")
            return 1 if errors else 0
        if args.cmd == "gate":
            res = gate(paths, args.sha, args.scope, allow_unattested=args.allow_unattested,
                       verify=args.verify_attestations)
            print(res.report())
            return 0 if res.ok else 1
    except (MatrixError, run_check.CheckError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    sys.exit(main())

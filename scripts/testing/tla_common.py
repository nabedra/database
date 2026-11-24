#!/usr/bin/env python3
"""Shared parts of the kind T runners (tla_check.py and tla_trace.py).

- Parsing TLC's `-tool` output: verdicts, statistics, coverage and counterexample length.
- Reading `.cfg` files: the properties they check and the `\\* expect:` and `\\* no-mutant:` lines.
- Running TLC through the pinned image (specs/tools/tlc) with a wall-clock budget.
- The time budget from the module's level in docs/testing/modules.toml.
- The `nabedra-tla-summary: {json}` line that run_check.py's `tlc` parser reads.

Stdlib only. See specs/README.md and docs/testing/checks/README.md.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import tomllib
import uuid
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SPECS_DIR = REPO_ROOT / "specs"
TLC_WRAPPER = SPECS_DIR / "tools" / "tlc"
VERSIONS_PATH = SPECS_DIR / "tools" / "versions.toml"
MODULES_PATH = REPO_ROOT / "docs" / "testing" / "modules.toml"
SUMMARY_PREFIX = "nabedra-tla-summary: "

# Wall-clock budget of one TLC run, by assurance level (references/tla.md, "Bounds").
LEVEL_BUDGET_S = {"L1": 900, "L2": 900, "L3": 3600}
# After the budget, TLC stops itself (tlc2.TLC.stopAfter); the container is killed this much later.
KILL_GRACE_S = 300


class TlaError(Exception):
    """A setup problem: a missing file, an unknown module, a malformed cfg."""


# --------------------------------------------------------------------------------------------
# TLC -tool output.

_MSG = re.compile(r"@!@!@STARTMSG (\d+):(\d+) @!@!@\n(.*?)@!@!@ENDMSG \1 @!@!@", re.S)
_COVERAGE = re.compile(
    r"^<(\w+) line (\d+), col (\d+) to line \d+, col \d+ of module (\w+)(?: \((\d+) (\d+) \d+ \d+\))?>: (\d+):(\d+)\s*$"
)
_STATS = re.compile(r"([\d,]+) states generated, ([\d,]+) distinct states found, ([\d,]+) states left on queue")
_DEPTH = re.compile(r"The depth of the complete state graph search is (\d+)")
_SIM_STATES = re.compile(r"The number of states generated: (\d+)")
_TRACE_REJECTED = re.compile(r'<<"nabedra-trace-rejected", (-?\d+), (\d+)>>')

# Message codes (tlc2.output.EC).
EC_SUCCESS = 2193
EC_STATS = 2199
EC_DEPTH = 2194
EC_SIM_DONE = 2210
EC_COVERAGE_START = 2201
EC_COVERAGE_ACTION = 2772
EC_STATE = 2217
EC_STUTTER = 2218
EC_BACK_TO_STATE = 2122
EC_DEADLOCK = 2114
EC_POSTCONDITION = 2404

_VIOLATIONS = (
    ("invariant", re.compile(r"^Invariant (\w+) is violated")),
    ("action-property", re.compile(r"^Action property (\w+) is violated")),
    ("temporal", re.compile(r"^Temporal propert(?:y|ies) (\w+) (?:was|were) violated")),
    ("temporal", re.compile(r"^Temporal properties were violated")),
    ("deadlock", re.compile(r"^Deadlock reached")),
    ("postcondition", re.compile(r"^Postcondition (\w+) .*is false")),
    ("assumption", re.compile(r"^Assumption .* is false")),
)
# Error-class messages that accompany a violation or an error and are not errors themselves.
_CONTEXT_CODES = {2121, 2264, 2103, 2122, 2217, 2218}


@dataclass
class TlcResult:
    """What one TLC run printed."""
    complete: bool = False             # "Model checking completed. No error has been found."
    simulation_done: bool = False      # the simulation finished its behaviors
    violations: list[tuple[str, str]] = field(default_factory=list)  # (kind, property or "")
    errors: list[str] = field(default_factory=list)
    generated: int | None = None
    distinct: int | None = None
    left: int | None = None
    depth: int | None = None
    coverage: dict[str, tuple[int, int]] = field(default_factory=dict)  # action -> (distinct, generated)
    coverage_unparsed: list[str] = field(default_factory=list)  # action coverage lines the parser did not read
    cex_states: int = 0
    lasso: bool = False
    trace_rejected_at: int | None = None  # TraceAccepted's printed index of the first unexplained line
    trace_lines: int | None = None
    exit_code: int | None = None
    timed_out: bool = False

    def violation(self) -> tuple[str, str] | None:
        return self.violations[0] if self.violations else None


def _int(text: str) -> int:
    return int(text.replace(",", ""))


def parse_tlc_output(text: str, exit_code: int | None = None) -> TlcResult:
    """Parses TLC's `-tool` output. Text outside messages (SANY's parse errors) counts as error detail."""
    res = TlcResult(exit_code=exit_code)
    m = _TRACE_REJECTED.search(text)
    if m:
        res.trace_rejected_at, res.trace_lines = int(m.group(1)), int(m.group(2))
    for msg in _MSG.finditer(text):
        code, cls, body = int(msg.group(1)), int(msg.group(2)), msg.group(3).strip()
        if code == EC_SUCCESS:
            res.complete = True
        elif code == EC_STATS:
            s = _STATS.search(body)
            if s:
                res.generated, res.distinct, res.left = (_int(g) for g in s.groups())
        elif code == EC_DEPTH:
            d = _DEPTH.search(body)
            if d:
                res.depth = int(d.group(1))
        elif code == EC_SIM_DONE:
            res.simulation_done = True
            s = _SIM_STATES.search(body)
            if s:
                res.generated = int(s.group(1))
        elif code == EC_COVERAGE_START:
            res.coverage = {}  # a later report replaces an earlier, periodic one
            res.coverage_unparsed = []
        elif code == EC_COVERAGE_ACTION:
            c = _COVERAGE.match(body)
            if c:
                name, mod_line, mod_col = c.group(1), c.group(5), c.group(6)
                # An inline disjunct of Next is reported under the enclosing operator, with its location.
                key = f"{name}@{mod_line}:{mod_col}" if mod_line else name
                distinct, generated = int(c.group(7)), int(c.group(8))
                old = res.coverage.get(key, (0, 0))
                res.coverage[key] = (old[0] + distinct, old[1] + generated)
            else:
                # An action the parser cannot name could hide a zero count, so the coverage check fails on it.
                res.coverage_unparsed.append(body[:120])
        elif code == EC_STATE:
            res.cex_states += 1
        elif code in (EC_STUTTER, EC_BACK_TO_STATE):
            res.lasso = True
        elif cls in (1, 2):  # 1: error, 2: TLC bug
            for kind, pattern in _VIOLATIONS:
                v = pattern.match(body)
                if v:
                    prop = v.group(1) if v.groups() else ""
                    res.violations.append((kind, prop))
                    break
            else:
                if code not in _CONTEXT_CODES:
                    res.errors.append(f"TLC message {code}: {body.splitlines()[0] if body else ''}"[:300])
    # TLC prints "Model checking completed. No error has been found." even when it stopped early, at
    # tlc2.TLC.stopAfter or TLCSet("exit", TRUE). Only a final statistics line with an empty queue
    # means the search was exhaustive; without that line nothing shows that it was.
    if res.complete and res.left != 0:
        res.complete = False
    if not res.complete and not res.simulation_done and not res.violations and not res.errors:
        tail = [l for l in _MSG.sub("", text).splitlines() if l.strip() and not l.startswith(("Parsing", "Semantic", "Linting", "Picked up"))]
        if exit_code not in (0, None) and tail:
            res.errors.append(("TLC: " + " ".join(tail[-3:]))[:300])
    return res


# --------------------------------------------------------------------------------------------
# Configuration files.

_CFG_KEYWORDS = {
    "CONSTANT", "CONSTANTS", "INIT", "NEXT", "SPECIFICATION", "INVARIANT", "INVARIANTS", "PROPERTY",
    "PROPERTIES", "CONSTRAINT", "CONSTRAINTS", "ACTION_CONSTRAINT", "ACTION_CONSTRAINTS", "SYMMETRY",
    "VIEW", "CHECK_DEADLOCK", "POSTCONDITION", "POSTCONDITIONS", "ALIAS",
}
_EXPECT = re.compile(r"^\\\*\s*expect:\s*(\w+)\s*$")
_NO_MUTANT = re.compile(r"^\\\*\s*no-mutant:\s*(\w+)\s*(?:--\s*(.*))?$")


@dataclass
class Cfg:
    path: Path
    invariants: list[str]
    properties: list[str]
    expect: str | None
    no_mutant: dict[str, str]
    check_deadlock: bool


def parse_cfg(path: Path) -> Cfg:
    text = Path(path).read_text(encoding="utf-8")
    lines = text.splitlines()
    first = lines[0].strip() if lines else ""
    expect_match = _EXPECT.match(first)
    no_mutant = {}
    for line in lines:
        nm = _NO_MUTANT.match(line.strip())
        if nm:
            no_mutant[nm.group(1)] = (nm.group(2) or "").strip()
    body = re.sub(r"\(\*.*?\*\)", " ", text, flags=re.S)
    body = re.sub(r"\\\*[^\n]*", " ", body)
    tokens = body.split()
    section = None
    invariants: list[str] = []
    properties: list[str] = []
    deadlock = True
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok in _CFG_KEYWORDS:
            section = tok
            if tok == "CHECK_DEADLOCK" and i + 1 < len(tokens):
                deadlock = tokens[i + 1].upper() != "FALSE"
                i += 1
        elif section in ("INVARIANT", "INVARIANTS") and re.fullmatch(r"\w+", tok):
            invariants.append(tok)
        elif section in ("PROPERTY", "PROPERTIES") and re.fullmatch(r"\w+", tok):
            properties.append(tok)
        i += 1
    return Cfg(Path(path), invariants, properties, expect_match.group(1) if expect_match else None,
               no_mutant, deadlock)


# --------------------------------------------------------------------------------------------
# Budget, tools and running.

def module_level(module: str, path: Path = MODULES_PATH) -> str | None:
    data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    for m in data.get("module", []):
        if m.get("name") == module:
            return m.get("level")
    return None


def budget_for(module: str, override: int | None = None, path: Path = MODULES_PATH) -> int:
    if override is not None:
        if override <= 0:
            raise TlaError("--budget-s must be positive")
        return override
    level = module_level(module, path)
    if level not in LEVEL_BUDGET_S:
        raise TlaError(f"module {module!r} has no level in {path}; pass --module or --budget-s")
    return LEVEL_BUDGET_S[level]


def tool_versions(path: Path = VERSIONS_PATH) -> str:
    try:
        v = tomllib.loads(Path(path).read_text(encoding="utf-8"))
        return f"tlc {v['tlc']['version']}; community-modules {v['community_modules']['version']}"
    except (OSError, KeyError, tomllib.TOMLDecodeError):
        return "unknown"


def default_workers() -> str:
    return "auto" if os.environ.get("GITHUB_ACTIONS") == "true" else "2"


def default_heap() -> str:
    return os.environ.get("NABEDRA_TLC_HEAP") or ("8g" if os.environ.get("GITHUB_ACTIONS") == "true" else "2g")


def run_tlc(spec_dir: Path, args: list[str], *, out_dir: Path, log_path: Path, budget_s: int,
            traces_dir: Path | None = None, trace_path: str | None = None,
            wrapper: Path | None = None) -> tuple[TlcResult, float]:
    """Runs the wrapper in `spec_dir` with `-tool`, logs the output, and parses it.

    TLC stops itself after `budget_s` (states left on queue, so the run reads as incomplete). The
    container is killed `KILL_GRACE_S` later if it is still running."""
    # NABEDRA_TLC_WRAPPER replaces specs/tools/tlc, for the runners' own tests.
    wrapper = wrapper or Path(os.environ.get("NABEDRA_TLC_WRAPPER") or TLC_WRAPPER)
    name = f"nabedra-tla-{uuid.uuid4().hex[:12]}"
    cmd = [str(wrapper), "--tla-out", str(out_dir), "--tla-name", name]
    if traces_dir is not None:
        cmd += ["--tla-traces", str(traces_dir)]
    cmd += ["-tool", *args]
    env = dict(os.environ)
    env["NABEDRA_TLC_HEAP"] = default_heap()
    env["NABEDRA_TLC_JAVA_OPTS"] = f"-Dtlc2.TLC.stopAfter={budget_s}"
    if trace_path is not None:
        env["TRACE_PATH"] = trace_path
    log_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    timed_out = False
    with open(log_path, "w", encoding="utf-8", errors="replace") as log:
        log.write("$ " + " ".join(cmd) + "\n")
        log.flush()
        proc = subprocess.Popen(cmd, cwd=spec_dir, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            code = proc.wait(timeout=budget_s + KILL_GRACE_S)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                subprocess.run(["docker", "kill", name], capture_output=True, timeout=60)
            except (OSError, subprocess.TimeoutExpired):
                pass
            proc.kill()
            code = proc.wait()
    elapsed = time.monotonic() - start
    text = log_path.read_text(encoding="utf-8", errors="replace")
    # A killed run stopped at the budget: its exit status and cut-off output are not a TLC error.
    res = parse_tlc_output(text, None if timed_out else code)
    # A simulation that ran into the budget stopped early, even though TLC reports it as finished.
    res.timed_out = timed_out or (elapsed >= budget_s and not res.complete and not res.violations)
    return res, elapsed


def incomplete_reason(res: TlcResult, budget_s: int) -> str:
    if res.timed_out:
        return f"stopped at the {budget_s} s time budget with {res.left if res.left is not None else '?'} states left on queue"
    if res.left:
        return f"stopped early with {res.left} states left on queue (a state cap or TLCSet(\"exit\"))"
    return "TLC ended without a verdict"


def print_summary(summary: dict) -> None:
    print(SUMMARY_PREFIX + json.dumps(summary, sort_keys=True, separators=(",", ":")), flush=True)


def read_summary(output: str) -> dict | None:
    found = None
    for line in output.splitlines():
        if line.startswith(SUMMARY_PREFIX):
            try:
                found = json.loads(line[len(SUMMARY_PREFIX):])
            except json.JSONDecodeError:
                continue
    return found


def counterexample_text(log_text: str, limit: int = 60) -> list[str]:
    """The counterexample states of a log, as printed by TLC, for the check's own output."""
    out = []
    for msg in _MSG.finditer(log_text):
        if int(msg.group(1)) in (EC_STATE, EC_STUTTER, EC_BACK_TO_STATE):
            out.extend(msg.group(3).rstrip().splitlines())
            if len(out) > limit:
                out = out[:limit] + ["..."]
                break
    return out

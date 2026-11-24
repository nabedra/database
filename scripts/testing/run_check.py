#!/usr/bin/env python3
"""Run one registered check and write its evidence record.

  run_check.py <id> [--evidence-dir D] [--log-dir L] [--request-id R] [--dry-run] [--strict]
  run_check.py --list [--tier T] [--module M] [--json]

A check is `docs/testing/checks/<id>.toml` (format: docs/testing/checks/README.md). The runner
validates it, checks its `needs`, runs `command` in `cwd` with a timeout, tees the output to a log,
parses the assertion counts with `result_parser`, and writes one `nabedra.evidence/1` record.

Results: `pass` needs at least one executed assertion and no failure. Zero executed assertions is
`not-run`, never `pass`. A missing need is `not-run` with its reason. The exit status is 1 for
`fail` or `error`, and 0 otherwise (2 for `not-run` with `--strict`).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evidence  # noqa: E402

REPO_ROOT = evidence.REPO_ROOT
CHECKS_DIR = REPO_ROOT / "docs" / "testing" / "checks"
MODULES_PATH = REPO_ROOT / "docs" / "testing" / "modules.toml"
DEFAULT_LOG_DIR = REPO_ROOT / "target" / "testing-logs"

TIERS = ("T0", "T1", "T2", "T3", "weekly")
NEEDS = ("docker", "kind", "s3", "rust-nightly", "node", "python-orm", "valkey", "tlc")
PARSERS = ("cargo-test", "unittest", "node-test", "exit-code", "tlc")
GUARANTEE_RE = re.compile(r"^[A-Z]+(-[A-Z]+)*-G[0-9]{2}$")
ID_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
FIELDS = {
    "id": str, "module": str, "kinds": list, "guarantees": list, "scenarios": list, "tiers": list,
    "command": list, "cwd": str, "timeout_s": int, "needs": list, "result_parser": str, "env": dict,
    "not_run_marker": str,
}
REQUIRED = ("id", "module", "kinds", "guarantees", "tiers", "command", "cwd", "timeout_s", "result_parser")


class CheckError(ValueError):
    """A check file is malformed."""


@dataclass
class Check:
    id: str
    module: str
    kinds: list[str]
    guarantees: list[str]
    scenarios: list[str]
    tiers: list[str]
    command: list[str]
    cwd: str
    timeout_s: int
    needs: list[str]
    result_parser: str
    env: dict[str, str] = field(default_factory=dict)
    path: Path | None = None
    not_run_marker: str | None = None


# --------------------------------------------------------------------------------------------
# Loading and validation.

def module_names(path: Path = MODULES_PATH) -> set[str] | None:
    if not path.exists():
        return None
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    names = {m["name"] for m in data.get("module", [])}
    names.add(data.get("fallback", "misc"))
    return names


def parse_check(data: dict, path: Path | None = None, modules: set[str] | None = None) -> Check:
    """Validates a decoded check table and returns it. Raises CheckError listing every problem."""
    problems = []
    for key in data:
        if key not in FIELDS:
            problems.append(f"unknown field {key!r}")
    for key in REQUIRED:
        if key not in data:
            problems.append(f"missing field {key!r}")
    for key, typ in FIELDS.items():
        if key in data and not isinstance(data[key], typ) or (typ is int and isinstance(data.get(key), bool)):
            problems.append(f"{key} must be a {typ.__name__}")
    if problems:
        raise CheckError(f"{path or 'check'}: " + "; ".join(problems))

    def str_list(key: str) -> list[str]:
        values = data.get(key, [])
        if not all(isinstance(v, str) for v in values):
            problems.append(f"{key} must contain only strings")
            return []
        return list(values)

    check = Check(
        id=data["id"], module=data["module"], kinds=str_list("kinds"),
        guarantees=str_list("guarantees"), scenarios=str_list("scenarios"), tiers=str_list("tiers"),
        command=str_list("command"), cwd=data["cwd"], timeout_s=data["timeout_s"],
        needs=str_list("needs"), result_parser=data["result_parser"],
        env=dict(data.get("env", {})), path=path, not_run_marker=data.get("not_run_marker"),
    )
    if not ID_RE.fullmatch(check.id):
        problems.append(f"id {check.id!r} is not kebab-case")
    if path is not None and path.stem != check.id:
        problems.append(f"id {check.id!r} does not match the file name {path.name!r}")
    if modules is not None and check.module not in modules:
        problems.append(f"unknown module {check.module!r}")
    if not check.kinds or any(k not in evidence.KINDS or len(k) != 1 for k in check.kinds):
        problems.append(f"kinds must be a non-empty list of {list(evidence.KINDS)}")
    if len(set(check.kinds)) != len(check.kinds):
        problems.append("kinds repeats a letter")
    for g in check.guarantees:
        if not GUARANTEE_RE.match(g):
            problems.append(f"guarantee {g!r} is not a guarantee ID")
    if not check.tiers or any(t not in TIERS for t in check.tiers):
        problems.append(f"tiers must be a non-empty list of {list(TIERS)}")
    if not check.command:
        problems.append("command is empty")
    if Path(check.cwd).is_absolute() or ".." in Path(check.cwd).parts:
        problems.append("cwd must be relative to the repository root, without '..'")
    if check.timeout_s <= 0:
        problems.append("timeout_s must be positive")
    for need in check.needs:
        if need not in NEEDS:
            problems.append(f"unknown need {need!r} (known: {', '.join(NEEDS)})")
    if check.result_parser not in PARSERS:
        problems.append(f"result_parser must be one of {list(PARSERS)}")
    if "T" in check.kinds and (check.result_parser != "tlc" or "tlc" not in check.needs):
        problems.append('a kind T check needs result_parser = "tlc" and needs = ["tlc"]')
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in check.env.items()):
        problems.append("env values must be strings")
    if check.not_run_marker is not None and (
        not check.not_run_marker.strip() or len(check.not_run_marker) > 300
    ):
        problems.append("not_run_marker must be a nonblank string of at most 300 characters")
    if problems:
        raise CheckError(f"{path or check.id}: " + "; ".join(problems))
    return check


def load_check(path: Path, modules: set[str] | None = None) -> Check:
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise CheckError(f"{path}: not TOML: {exc}") from exc
    return parse_check(data, path, modules)


def load_all(checks_dir: Path = CHECKS_DIR, modules: set[str] | None = None) -> list[Check]:
    return [load_check(p, modules) for p in sorted(Path(checks_dir).glob("*.toml"))]


# --------------------------------------------------------------------------------------------
# Needs.

def _succeeds(cmd: list[str], timeout: float = 30.0, env: dict | None = None) -> bool:
    try:
        return subprocess.run(cmd, capture_output=True, timeout=timeout, env=env).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _tlc(env: dict[str, str]) -> bool:
    """The pinned TLC image is loaded, or Docker can build it (specs/tools/tlc --tla-ensure)."""
    wrapper = REPO_ROOT / "specs" / "tools" / "tlc"
    return wrapper.exists() and _succeeds([str(wrapper), "--tla-ensure"], timeout=900, env=env)


def _python_orm(env: dict[str, str]) -> bool:
    src = REPO_ROOT / "orm" / "python" / "src"
    child_env = dict(env, PYTHONPATH=os.pathsep.join(filter(None, [str(src), env.get("PYTHONPATH", "")])))
    return _succeeds([sys.executable, "-c", "import nabedra"], env=child_env)


NEED_PROBES: dict[str, tuple[str, Callable[[dict[str, str]], bool]]] = {
    "s3": ("s3-unconfigured: NABEDRA_TEST_S3_BUCKET is unset", lambda env: bool(env.get("NABEDRA_TEST_S3_BUCKET"))),
    "kind": ("kind-missing: no `kind` binary on PATH", lambda env: shutil.which("kind", path=env.get("PATH")) is not None),
    "docker": ("docker-unavailable: `docker info` failed", lambda env: _succeeds(["docker", "info"], env=env)),
    "rust-nightly": ("rust-nightly-missing: `cargo +nightly --version` failed", lambda env: _succeeds(["cargo", "+nightly", "--version"], env=env)),
    "node": ("node-missing: no `node` binary on PATH", lambda env: shutil.which("node", path=env.get("PATH")) is not None),
    "python-orm": ("python-orm-unavailable: `import nabedra` from orm/python/src failed", _python_orm),
    "valkey": ("valkey-unavailable: no `valkey-server` on PATH and NABEDRA_TEST_VALKEY_URL unset",
               lambda env: bool(env.get("NABEDRA_TEST_VALKEY_URL")) or shutil.which("valkey-server", path=env.get("PATH")) is not None),
    "tlc": ("tlc-unavailable: the pinned TLC image is not loaded and `specs/tools/tlc --tla-ensure` failed "
            "(Docker unavailable or the image build failed)", _tlc),
}


def missing_needs(check: Check, env: dict[str, str]) -> list[dict]:
    """One `{id, reason}` entry per need the environment does not provide."""
    out = []
    for need in check.needs:
        reason, probe = NEED_PROBES[need]
        if not probe(env):
            out.append({"id": f"need:{need}", "reason": reason})
    return out


# --------------------------------------------------------------------------------------------
# Result parsers. Each returns (executed, failed, not_run entries, found).

@dataclass
class Parsed:
    executed: int
    failed: int
    not_run: list[dict]
    found: bool  # False when the output has no summary line at all
    verdict: str | None = None  # set by parsers whose tool reports its own result (tlc)
    extra: dict = field(default_factory=dict)  # tool facts for the record (tlc: `tla`, `artifacts`)


_CARGO = re.compile(
    r"^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out",
    re.M,
)


def parse_cargo_test(output: str, returncode: int) -> Parsed:
    passed = failed = ignored = 0
    found = False
    for m in _CARGO.finditer(output):
        found = True
        passed += int(m.group(1))
        failed += int(m.group(2))
        ignored += int(m.group(3))
    not_run = [{"id": "ignored", "reason": f"{ignored} tests marked #[ignore]"}] if ignored else []
    return Parsed(passed + failed, failed, not_run, found)


_UNITTEST_RAN = re.compile(r"^Ran (\d+) tests? in [0-9.]+s$", re.M)
_UNITTEST_END = re.compile(r"^(OK|FAILED|NO TESTS RAN)(?: \(([^)]*)\))?\s*$", re.M)


def parse_unittest(output: str, returncode: int) -> Parsed:
    ran = [int(n) for n in _UNITTEST_RAN.findall(output)]
    ends = _UNITTEST_END.findall(output)
    if not ran or not ends:
        return Parsed(0, 0, [], False)
    failed = skipped = 0
    for status, detail in ends:
        counts = dict(
            (k.strip(), int(v)) for k, v in (part.split("=") for part in detail.split(",") if "=" in part)
        )
        skipped += counts.get("skipped", 0)
        if status == "FAILED":
            failed += counts.get("failures", 0) + counts.get("errors", 0) + counts.get("unexpected successes", 0)
            failed = max(failed, 1)
    total = sum(ran)
    not_run = [{"id": "skipped", "reason": f"{skipped} tests skipped"}] if skipped else []
    return Parsed(max(total - skipped, 0), failed, not_run, True)


_TAP = re.compile(r"^# (tests|suites|pass|fail|cancelled|skipped|todo) (\d+)\s*$", re.M)


def parse_node_test(output: str, returncode: int) -> Parsed:
    counts: dict[str, int] = {}
    for key, value in _TAP.findall(output):
        counts[key] = counts.get(key, 0) + int(value)
    if "pass" not in counts or "fail" not in counts:
        return Parsed(0, 0, [], False)
    failed = counts["fail"] + counts.get("cancelled", 0)
    not_run = []
    if counts.get("skipped"):
        not_run.append({"id": "skipped", "reason": f"{counts['skipped']} tests skipped"})
    if counts.get("todo"):
        not_run.append({"id": "todo", "reason": f"{counts['todo']} tests marked todo"})
    return Parsed(counts["pass"] + failed, failed, not_run, True)


def parse_exit_code(output: str, returncode: int) -> Parsed:
    return Parsed(1, 0 if returncode == 0 else 1, [], True)


_TLA_SUMMARY = "nabedra-tla-summary: "


def parse_tlc(output: str, returncode: int) -> Parsed:
    """Reads the `nabedra-tla-summary: {json}` line of tla_check.py or tla_trace.py.

    A check runs one runner, so it prints one summary. More than one would let a later pass hide an
    earlier failure, so it is an error."""
    lines = [line for line in output.splitlines() if line.startswith(_TLA_SUMMARY)]
    if len(lines) > 1:
        return Parsed(0, 0, [{"id": "parser", "reason": f"{len(lines)} nabedra-tla-summary lines; a kind T "
                              "check runs one runner"}], True, "error")
    summary = None
    if lines:
        try:
            summary = json.loads(lines[0][len(_TLA_SUMMARY):])
        except json.JSONDecodeError:
            summary = None
    if not isinstance(summary, dict):
        return Parsed(0, 0, [], False)
    try:
        executed, failed = int(summary["executed"]), int(summary["failed"])
        verdict = summary["verdict"]
    except (KeyError, TypeError, ValueError):
        return Parsed(0, 0, [], False)
    not_run = [
        {"id": str(n.get("id", ""))[:200], "reason": str(n.get("reason", ""))[:300]}
        for n in summary.get("not_run", []) if isinstance(n, dict)
    ]
    extra = {k: summary[k] for k in ("tla", "artifacts") if isinstance(summary.get(k), (dict, list))}
    return Parsed(executed, failed, not_run, True, verdict if verdict in evidence.RESULTS else None, extra)


PARSER_FUNCS = {
    "cargo-test": parse_cargo_test,
    "unittest": parse_unittest,
    "node-test": parse_node_test,
    "exit-code": parse_exit_code,
    "tlc": parse_tlc,
}


def classify(parser: str, output: str, returncode: int | None, timed_out: bool = False,
             not_run_marker: str | None = None) -> tuple[str, Parsed]:
    """Maps a command's output and exit status to (result, counts)."""
    if timed_out:
        parsed = Parsed(0, 0, [], False) if parser == "exit-code" else PARSER_FUNCS[parser](output, 1)
        parsed.not_run.append({"id": "timeout", "reason": "the command exceeded timeout_s and was killed"})
        return "error", parsed
    if returncode is None:
        return "error", Parsed(0, 0, [{"id": "spawn", "reason": "the command did not start"}], False)
    parsed = PARSER_FUNCS[parser](output, returncode)
    if returncode == 0 and parsed.failed == 0 and not_run_marker and not_run_marker in output:
        return "not-run", Parsed(0, 0, [{"id": "output-marker", "reason": not_run_marker}], True)
    if not parsed.found:
        parsed.not_run.append({"id": "parser", "reason": f"no {parser} summary line in the output (exit {returncode})"})
        return "fail", parsed
    # A tool that reports its own result: a stop at a budget or cap is not-run, never pass.
    if parsed.verdict in ("not-run", "error") and parsed.failed == 0:
        if parsed.verdict == "not-run" and not parsed.not_run:
            parsed.not_run.append({"id": "tool", "reason": "the tool reported not-run"})
        return parsed.verdict, parsed
    if parsed.verdict == "fail" and parsed.failed == 0:
        parsed.not_run.append({"id": "tool", "reason": "the tool reported fail with no failed assertion"})
        return "error", parsed
    if parsed.failed > 0:
        return "fail", parsed
    if returncode != 0:
        parsed.not_run.append({"id": "exit", "reason": f"exit status {returncode} with no failed assertion parsed"})
        return "error", parsed
    if parsed.executed == 0:
        parsed.not_run.append({"id": "zero-assertions", "reason": "the command executed zero assertions"})
        return "not-run", parsed
    return "pass", parsed


def fit_record(record: dict) -> None:
    """Shrinks a record's tool facts (`tla`) and tool artifacts until it fits the record size limit.

    It drops, in order: the details of passing runs and caught mutants; tool artifacts other than the
    command log and tla/summary.json; passing runs and caught mutants, last first; and it shortens the
    remaining details. Failures are kept. `tla.truncated` says that facts were dropped. The full facts
    stay in the summary.json artifact."""
    tla = record.get("tla")

    def fits() -> bool:
        return len(evidence.dumps(record).encode("utf-8")) <= evidence.MAX_RECORD_BYTES

    if fits() or not isinstance(tla, dict):
        return
    tla["truncated"] = True
    runs, mutants = tla.get("runs", []), tla.get("mutants", [])

    def run_ok(r: dict) -> bool:
        return r.get("verdict") == "pass"

    def mutant_ok(m: dict) -> bool:
        return m.get("ok") is True

    for items, ok in ((runs, run_ok), (mutants, mutant_ok)):
        for item in items:
            if ok(item):
                item.pop("detail", None)
    artifacts = record.get("artifacts", [])
    for i in range(len(artifacts) - 1, 0, -1):
        if fits():
            return
        if artifacts[i].get("name") != "tla/summary.json":
            del artifacts[i]
    for items, ok in ((mutants, mutant_ok), (runs, run_ok)):
        for i in range(len(items) - 1, -1, -1):
            if fits():
                return
            if ok(items[i]):
                del items[i]
    for item in [*runs, *mutants, *tla.get("traces", {}).get("failing", [])]:
        for key in ("detail", "reason"):
            if isinstance(item.get(key), str):
                item[key] = item[key][:60]


# --------------------------------------------------------------------------------------------
# Running.

def resolve_command(command: list[str]) -> list[str]:
    """Only a bare `python3` or `python` in argv[0] uses the runner's interpreter."""
    if command and command[0] in ("python3", "python"):
        return [sys.executable, *command[1:]]
    return list(command)


def run_command(cmd: list[str], cwd: Path, env: dict[str, str], timeout_s: float, log_path: Path,
                echo: bool = True) -> tuple[int | None, str, bool]:
    """Runs `cmd`, teeing combined output to `log_path` (and stdout). Returns (code, output, timed_out)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    chunks: list[str] = []
    with open(log_path, "w", encoding="utf-8", errors="replace") as log:
        try:
            proc = subprocess.Popen(
                cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, errors="replace", start_new_session=True,
            )
        except OSError as exc:
            message = f"cannot start {cmd[0]!r}: {exc}\n"
            log.write(message)
            return None, message, False

        def pump() -> None:
            assert proc.stdout is not None
            for line in proc.stdout:
                chunks.append(line)
                log.write(line)
                if echo:
                    sys.stdout.write(line)
                    sys.stdout.flush()

        reader = threading.Thread(target=pump, daemon=True)
        reader.start()
        timed_out = False
        try:
            proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                proc.kill()
            proc.wait()
        reader.join(timeout=10)
        if proc.stdout is not None:
            proc.stdout.close()
    return proc.returncode, "".join(chunks), timed_out


def execute(check: Check, *, evidence_dir: Path | None, log_dir: Path, request_id: str | None,
            dry_run: bool = False, env: dict[str, str] | None = None, echo: bool = True,
            repo: Path = REPO_ROOT) -> tuple[dict | None, Path | None]:
    """Runs a check and writes its record. Returns (record, record path); both None on dry runs."""
    base_env = dict(os.environ if env is None else env)
    run = evidence.run_info(base_env, request_id)
    stem = evidence.record_stem(run["request_id"], check.id)
    child_env = {**base_env, **check.env, "NABEDRA_REQUEST_ID": run["request_id"]}
    missing = missing_needs(check, child_env)
    cmd = resolve_command(check.command)
    if dry_run:
        print(f"check {check.id} (module {check.module}, kinds {' '.join(check.kinds)}, tiers {' '.join(check.tiers)})")
        print(f"  cwd: {check.cwd}\n  command: {' '.join(cmd)}\n  timeout: {check.timeout_s}s")
        for entry in missing:
            print(f"  would be not-run: {entry['reason']}")
        if not missing:
            print("  needs: satisfied")
        return None, None

    start = time.monotonic()
    artifacts = []
    s3_requests = None
    if missing:
        result, parsed = "not-run", Parsed(0, 0, missing, True)
    else:
        log_path = Path(log_dir) / f"{stem}.log"
        code, output, timed_out = run_command(cmd, repo / check.cwd, child_env, check.timeout_s, log_path, echo)
        result, parsed = classify(check.result_parser, output, code, timed_out, check.not_run_marker)
        s3_requests = evidence.parse_s3_requests(output)
        artifacts.append({"name": log_path.name, "sha256": evidence.sha256_file(log_path)})
        artifacts.extend(a for a in parsed.extra.get("artifacts", [])[:40] if isinstance(a, dict))
    tla = parsed.extra.get("tla") if not missing else None
    record = evidence.new_record(
        check.id, check.kinds, result=result, executed=parsed.executed, failed=parsed.failed,
        not_run=parsed.not_run[:200], guarantees=check.guarantees, scenarios=check.scenarios,
        duration_s=time.monotonic() - start, run=run, artifacts=artifacts, repo=repo,
        s3_requests=s3_requests, tla=tla,
        mode={"tla": tla["mode"]} if isinstance(tla, dict) and isinstance(tla.get("mode"), str) else None,
    )
    fit_record(record)
    path = evidence.write(record, evidence_dir)
    return record, path


def _list(checks: list[Check], tier: str | None, module: str | None, as_json: bool) -> None:
    rows = [c for c in checks if (tier is None or tier in c.tiers) and (module is None or c.module == module)]
    if as_json:
        print(json.dumps([{"id": c.id, "module": c.module, "kinds": c.kinds, "tiers": c.tiers,
                           "needs": c.needs, "guarantees": c.guarantees} for c in rows], indent=2))
        return
    for c in rows:
        needs = f"  needs {','.join(c.needs)}" if c.needs else ""
        print(f"{c.id}\t{c.module}\t{' '.join(c.kinds)}\t{' '.join(c.tiers)}{needs}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("id", nargs="?")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--tier", choices=TIERS)
    parser.add_argument("--module")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--checks-dir", type=Path, default=CHECKS_DIR)
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--request-id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--strict", action="store_true", help="exit 2 when the result is not-run")
    args = parser.parse_args(argv)

    modules = module_names()
    try:
        if args.list:
            _list(load_all(args.checks_dir, modules), args.tier, args.module, args.json)
            return 0
        if not args.id:
            parser.error("give a check id or --list")
        if not ID_RE.fullmatch(args.id):
            parser.error("check id must be kebab-case")
        path = args.checks_dir / f"{args.id}.toml"
        if not path.exists():
            print(f"no such check: {path}", file=sys.stderr)
            return 1
        check = load_check(path, modules)
        record, record_path = execute(check, evidence_dir=args.evidence_dir, log_dir=args.log_dir,
                                      request_id=args.request_id, dry_run=args.dry_run)
    except (CheckError, evidence.EvidenceError, FileExistsError) as exc:
        print(exc, file=sys.stderr)
        return 1
    if record is None:
        return 0
    a = record["assertions"]
    reasons = "; ".join(f"{n['id']}: {n['reason']}" for n in a["not_run"])
    print(f"\n[run_check] {check.id}: {record['result']} (executed {a['executed']}, failed {a['failed']})"
          + (f" [{reasons}]" if reasons else ""))
    print(f"[run_check] record: {record_path}")
    if record["result"] in ("fail", "error"):
        return 1
    if record["result"] == "not-run" and args.strict:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

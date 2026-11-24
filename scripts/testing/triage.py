#!/usr/bin/env python3
"""Triage a check failure against 14 days of main-branch evidence.

A check that is kind T is always blocked, and its command never reruns: model checking is deterministic, so
a retry proves nothing. The kinds in docs/testing/checks/<check>.toml, or in the evidence records of a check
that is not registered, count whatever --kind says.
"""

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
import json
from pathlib import Path
import re
import signal
import subprocess
import sys
import tomllib


CHECKS_DIR = Path(__file__).resolve().parents[2] / "docs" / "testing" / "checks"
MAX_RETRIES = 10
TOLERANCE = Fraction(1, 100)
KINDS = set("MUSHFDRPKT")
CRASH_CODES = {
    -signal.SIGABRT, -signal.SIGKILL, -signal.SIGSEGV,
    128 + signal.SIGABRT, 128 + signal.SIGKILL, 128 + signal.SIGSEGV,
}
CRASH_LOG = re.compile(
    r"\b(?:panicked at|kernel panic|SIGABRT|SIGKILL|SIGSEGV|out of memory|"
    r"OOM[- ]kill(?:ed|er)?|OOMKilled|segmentation fault|"
    r"abort trap|process aborted|exit(?:ed)?(?: with)?(?: (?:status|code))?[:= ]+"
    r"(?:134|137|139))\b|\baborted \(core dumped\)|^[ \t]*(?:panic\s*:|abort(?:ed)?[ \t]*$|"
    r"(?:[^\n]*?:[ \t]+\d+[ \t]+)?Killed(?:\s|$))",
    re.I | re.M,
)


@dataclass(frozen=True)
class Decision:
    blocked: bool
    reason: str
    baseline_passes: int = 0
    baseline_runs: int = 0
    retry_passes: int = 0
    retries: int = 0


def crash_signature(returncode, log):
    if returncode in CRASH_CODES:
        return f"exit code {returncode}"
    match = CRASH_LOG.search(log)
    return match.group().strip() if match else None


DATE_FIELDS = ("date", "finished_at", "started_at")


def parse_date(value):
    try:
        return date.fromisoformat(value)
    except ValueError:
        stamp = datetime.fromisoformat(value)
        return stamp.astimezone(timezone.utc).date() if stamp.tzinfo else stamp.date()


def record_date(record, path):
    """Date a record from run.date, then run or top-level finished_at or started_at, then mtime.

    The evidence schema may not carry run.date yet. The file's mtime is the last
    resort: a fresh checkout resets it, so such records count as recent.
    """
    run = record.get("run")
    if isinstance(run, str):
        return parse_date(run)
    for source in (run if isinstance(run, dict) else {}, record):
        for field in DATE_FIELDS:
            if isinstance(source.get(field), str):
                return parse_date(source[field])
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).date()


def baseline(directory, check, today):
    first_day = today - timedelta(days=13)
    passes = runs = 0
    for path in sorted(directory.rglob("*.json")):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            if record.get("check") != check or record.get("result") not in {"pass", "fail"}:
                continue
            if first_day <= record_date(record, path) <= today:
                runs += 1
                passes += record["result"] == "pass"
        except (ValueError, KeyError, AttributeError, TypeError) as error:
            raise ValueError(f"{path}: {error}") from error
    return passes, runs


def known_kinds(check, evidence):
    """The kinds of the registered check, else of its evidence records, as a set of letters."""
    path = CHECKS_DIR / f"{check}.toml"
    if path.parent == CHECKS_DIR and path.exists():
        declared = tomllib.loads(path.read_text(encoding="utf-8")).get("kinds")
        if not isinstance(declared, list) or not all(isinstance(k, str) for k in declared):
            raise ValueError(f"{path}: kinds is not a list of strings")
        return set(declared)
    kinds = set()
    for record_path in sorted(evidence.rglob("*.json")):
        record = json.loads(record_path.read_text(encoding="utf-8"))
        if isinstance(record, dict) and record.get("check") == check:
            kinds.update(str(record.get("kind", "")).split())
    return kinds


def triage(kind, returncode, log, get_baseline, rerun, known=()):
    # Before the exit status: run_check.py exits 0 on a not-run, which is no pass.
    if "T" in kind.split() or "T" in known:
        return Decision(True, "kind T is never retried or unblocked: read the check's evidence record")
    if crash := crash_signature(returncode, log):
        return Decision(True, f"initial crash ({crash})")
    if returncode == 0:
        return Decision(False, "initial check passed")
    kinds = set(kind.split())
    if "R" not in kinds or kinds.intersection({"S", "M", "H"}):
        return Decision(True, f"kind {kind} is ineligible for retries")

    main_passed, main_runs = get_baseline()
    if not main_runs:
        return Decision(True, "no pass/fail evidence for the check in the 14-day baseline")
    retry_passed = 0
    for attempt in range(1, MAX_RETRIES + 1):
        status, output = rerun()
        if crash := crash_signature(status, output):
            return Decision(True, f"retry {attempt} crashed ({crash})", main_passed, main_runs,
                            retry_passed, attempt)
        retry_passed += status == 0
    # The initial failure selected this run for triage. Count the fresh retries
    # so ten clean retries can match a previously perfect baseline.
    blocked = Fraction(main_passed, main_runs) - Fraction(retry_passed, MAX_RETRIES) > TOLERANCE
    reason = "retry pass rate fell more than one percentage point" if blocked else "retry pass rate within tolerance"
    return Decision(blocked, reason, main_passed, main_runs, retry_passed, MAX_RETRIES)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="Records are dated by run.date, finished_at or started_at, else file mtime. "
               "Only pass/fail records and fresh retries count.",
    )
    parser.add_argument("--kind", required=True, help="check kind or space-separated kinds (only R retries, and never a registered T check)")
    parser.add_argument("--check", required=True, help="check ID in the evidence records")
    parser.add_argument("--evidence", type=Path, required=True, help="directory of main-branch JSON evidence")
    parser.add_argument("--log", type=Path, required=True, help="log of the initial run")
    parser.add_argument("--exit-code", type=int, default=1, help="initial process exit code")
    parser.add_argument("--json", action="store_true", help="emit a JSON decision")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="command to retry after --")
    args = parser.parse_args(argv)
    kinds = set(args.kind.split())
    if not kinds or kinds - KINDS:
        parser.error("--kind must contain check kinds from M U S H F D R P K T")
    command = args.command[1:] if args.command[:1] == ["--"] else args.command

    def rerun():
        if not command:
            raise ValueError("supply a retry command after --")
        process = subprocess.run(command, text=True, errors="replace",
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if process.returncode or crash_signature(process.returncode, process.stdout):
            print(process.stdout.rstrip(), file=sys.stderr)
        return process.returncode, process.stdout

    try:
        log = args.log.read_text(encoding="utf-8", errors="replace")
        today = datetime.now(timezone.utc).date()
        decision = triage(args.kind, args.exit_code, log,
                          lambda: baseline(args.evidence, args.check, today), rerun,
                          known_kinds(args.check, args.evidence))
    except (OSError, ValueError) as error:
        print(f"triage: {error}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(asdict(decision)))
    else:
        label = "blocked" if decision.blocked else "passed"
        print(f"{args.check}: {label} ({decision.reason}). "
              f"Retries {decision.retry_passes}/{decision.retries}. "
              f"Baseline {decision.baseline_passes}/{decision.baseline_runs}.")
    return int(decision.blocked)


if __name__ == "__main__":
    sys.exit(main())

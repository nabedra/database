#!/usr/bin/env python3
"""Build, validate and write `nabedra.evidence/1` records.

A record describes one run of one check. Its JSON Schema is docs/testing/evidence/schema.json, and
the validator here implements the subset of JSON Schema that file uses, so the schema stays the
single definition of the format. Records live at
docs/testing/evidence/records/<yyyy>/<mm>/<request_id>--<check>.json unless a caller passes another
evidence directory.

Usage:
  evidence.py validate <record.json>...
  evidence.py write --check ID --kind "U S" --result pass --executed N --failed N
                    [--guarantee G]... [--scenario S]... [--duration-s X] [--evidence-dir D]
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import math
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

SCHEMA_ID = "nabedra.evidence/1"
MAX_RECORD_BYTES = 20 * 1024
REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "docs" / "testing" / "evidence" / "schema.json"
DEFAULT_RECORDS_DIR = REPO_ROOT / "docs" / "testing" / "evidence" / "records"
KINDS = "MUSHFDRPKT"
RESULTS = ("pass", "fail", "not-run", "quarantined", "error")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class EvidenceError(ValueError):
    """A record does not conform to the schema or to the result rules."""


# --------------------------------------------------------------------------------------------
# Minimal JSON Schema validator (the keywords schema.json uses, nothing more).

_TYPE_CHECKS = {
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
    "string": lambda v: isinstance(v, str),
    "boolean": lambda v: isinstance(v, bool),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: (isinstance(v, int) and not isinstance(v, bool))
                        or (isinstance(v, float) and math.isfinite(v)),
    "null": lambda v: v is None,
}
_SUPPORTED = {
    "$schema", "$id", "title", "description", "type", "const", "enum", "pattern", "maxLength",
    "minimum", "properties", "required", "additionalProperties", "items", "minItems", "maxItems", "format",
}
_DATE_TIME = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])"
)


def _valid_date_time(value: str) -> bool:
    if not _DATE_TIME.fullmatch(value):
        return False
    try:
        _dt.datetime.fromisoformat(value.upper())
    except ValueError:
        return False
    return True


def schema_errors(value: Any, schema: dict, path: str = "$") -> list[str]:
    """Returns every violation of `schema` by `value`, as `<json path>: <problem>` strings."""
    unknown = set(schema) - _SUPPORTED
    if unknown:
        raise EvidenceError(f"schema keyword(s) not supported by this validator: {sorted(unknown)}")
    errors: list[str] = []
    types = schema.get("type")
    if types is not None:
        names = [types] if isinstance(types, str) else list(types)
        if not any(_TYPE_CHECKS[name](value) for name in names):
            return [f"{path}: expected {'|'.join(names)}, got {type(value).__name__}"]
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: must equal {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: {value!r} is not one of {schema['enum']}")
    if isinstance(value, str):
        if "pattern" in schema and re.search(schema["pattern"], value) is None:
            errors.append(f"{path}: {value!r} does not match {schema['pattern']}")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            errors.append(f"{path}: longer than {schema['maxLength']} characters")
        if "format" in schema:
            if schema["format"] != "date-time":
                raise EvidenceError(f"unsupported string format: {schema['format']!r}")
            if not _valid_date_time(value):
                errors.append(f"{path}: expected a date-time with a timezone")
    if _TYPE_CHECKS["number"](value) and "minimum" in schema and value < schema["minimum"]:
        errors.append(f"{path}: {value} is below the minimum {schema['minimum']}")
    if isinstance(value, dict):
        props = schema.get("properties", {})
        for key in schema.get("required", []):
            if key not in value:
                errors.append(f"{path}: missing required field {key!r}")
        extra = schema.get("additionalProperties", True)
        for key, item in value.items():
            if key in props:
                errors.extend(schema_errors(item, props[key], f"{path}.{key}"))
            elif extra is False:
                errors.append(f"{path}: unknown field {key!r}")
            elif isinstance(extra, dict):
                errors.extend(schema_errors(item, extra, f"{path}.{key}"))
    if isinstance(value, list):
        if "minItems" in schema and len(value) < schema["minItems"]:
            errors.append(f"{path}: fewer than {schema['minItems']} items")
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            errors.append(f"{path}: more than {schema['maxItems']} items")
        if "items" in schema:
            for index, item in enumerate(value):
                errors.extend(schema_errors(item, schema["items"], f"{path}[{index}]"))
    return errors


def load_schema(path: Path = SCHEMA_PATH) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def validate(record: dict, schema: dict | None = None) -> list[str]:
    """Returns schema and result-rule violations. An empty list means the record is valid."""
    errors = schema_errors(record, schema if schema is not None else load_schema())
    if errors:
        return errors
    asserts = record["assertions"]
    result = record["result"]
    # Zero executed assertions can never be a pass, and a pass can carry no failure.
    if result == "pass" and asserts["executed"] == 0:
        errors.append("$.result: pass with zero executed assertions (must be not-run)")
    if result == "pass" and asserts["failed"] > 0:
        errors.append("$.result: pass with failed assertions")
    if asserts["failed"] > asserts["executed"]:
        errors.append("$.assertions: failed exceeds executed")
    if result == "not-run" and not asserts["not_run"]:
        errors.append("$.assertions.not_run: a not-run record must give at least one reason")
    if result == "quarantined" and asserts["executed"] == 0:
        errors.append("$.result: quarantined with zero executed runs")
    kinds = record["kind"].split(" ")
    if len(set(kinds)) != len(kinds):
        errors.append("$.kind: repeated kind letter")
    size = len(dumps(record).encode("utf-8"))
    if size > MAX_RECORD_BYTES:
        errors.append(f"$: record is {size} bytes, over the {MAX_RECORD_BYTES} byte limit")
    return errors


def dumps(record: dict) -> str:
    return json.dumps(record, indent=2, sort_keys=True) + "\n"


# --------------------------------------------------------------------------------------------
# Environment: git and GitHub Actions.

def _git(args: list[str], cwd: Path) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def git_state(cwd: Path = REPO_ROOT) -> tuple[str, bool]:
    """Returns (HEAD sha, tree_clean). Untracked files count as dirty: they can change a build."""
    sha = _git(["rev-parse", "HEAD"], cwd)
    status = _git(["status", "--porcelain", "--untracked-files=normal"], cwd)
    return sha, status == ""


def run_info(env: dict[str, str] | None = None, request_id: str | None = None) -> dict:
    """The `run` object from the GitHub Actions environment, or local defaults."""
    env = dict(os.environ if env is None else env)
    server = env.get("GITHUB_SERVER_URL")
    repo = env.get("GITHUB_REPOSITORY")
    run_id = env.get("GITHUB_RUN_ID")
    url = f"{server}/{repo}/actions/runs/{run_id}" if server and repo and run_id else None
    attempt = env.get("GITHUB_RUN_ATTEMPT")
    rid = request_id or env.get("NABEDRA_REQUEST_ID") or default_request_id()
    return {
        "url": url,
        "attempt": int(attempt) if attempt and attempt.isdigit() and int(attempt) >= 1 else None,
        "request_id": rid,
        "runner": env.get("RUNNER_NAME") or None,
        "date": _dt.datetime.now(_dt.timezone.utc).isoformat(),
    }


def default_request_id(now: _dt.datetime | None = None) -> str:
    now = now or _dt.datetime.now()
    try:
        short = _git(["rev-parse", "--short=7", "HEAD"], REPO_ROOT)
    except (OSError, subprocess.CalledProcessError):
        short = "nogit"
    return f"local-{short}-{now:%H%M%S}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 16), b""):
            digest.update(block)
    return digest.hexdigest()


# --------------------------------------------------------------------------------------------
# Building and writing.

def parse_s3_requests(output: str) -> int | None:
    counts = re.findall(r"^nabedra-s3-requests:[ \t]+([0-9]+)(?=[ \t]|$)", output, re.M)
    return sum(map(int, counts)) if counts else None


def new_record(
    check: str,
    kinds: Iterable[str],
    *,
    result: str,
    executed: int,
    failed: int,
    not_run: list[dict] | None = None,
    guarantees: Iterable[str] = (),
    scenarios: Iterable[str] = (),
    duration_s: float = 0.0,
    sha: str | None = None,
    tree_clean: bool | None = None,
    build: dict | None = None,
    mode: dict | None = None,
    run: dict | None = None,
    seeds: dict | None = None,
    sim_core_hours: float | None = None,
    virtual_hours: float | None = None,
    s3_requests: int | None = None,
    artifacts: list[dict] | None = None,
    env: dict[str, str] | None = None,
    tla: dict | None = None,
    repo: Path = REPO_ROOT,
) -> dict:
    """Builds a record, filling `sha` and `tree_clean` from git and `run` from the environment."""
    if sha is None or tree_clean is None:
        head, clean = git_state(repo)
        sha = head if sha is None else sha
        tree_clean = clean if tree_clean is None else tree_clean
    kind_list = list(kinds)
    record = {
        "schema": SCHEMA_ID,
        "check": check,
        "kind": " ".join(kind_list),
        "guarantees": list(guarantees),
        "scenarios": list(scenarios),
        "sha": sha,
        "tree_clean": bool(tree_clean),
        "build": build or {"profile": None, "features": [], "binary_sha256": None},
        "mode": mode,
        "run": run or run_info(env),
        "duration_s": round(float(duration_s), 3),
        "result": result,
        "assertions": {"executed": int(executed), "failed": int(failed), "not_run": not_run or []},
        "seeds": seeds,
        "sim_core_hours": sim_core_hours,
        "virtual_hours": virtual_hours,
        "artifacts": artifacts or [],
    }
    if s3_requests is not None:
        record["s3_requests"] = s3_requests
    if tla is not None:
        record["tla"] = tla
    return record


def record_stem(rid: str, check: str) -> str:
    for name in (rid, check):
        if not _SAFE_NAME.fullmatch(name) or ".." in name:
            raise EvidenceError(f"unsafe name for an evidence path: {name!r}")
    return f"{rid}--{check}"


def record_path(record: dict, evidence_dir: Path | None = None, now: _dt.datetime | None = None) -> Path:
    """`<dir>/<yyyy>/<mm>/<request_id>--<check>.json`; the month is the UTC write time."""
    now = now or _dt.datetime.now(_dt.timezone.utc)
    base = Path(evidence_dir) if evidence_dir is not None else DEFAULT_RECORDS_DIR
    return base / f"{now:%Y}" / f"{now:%m}" / f"{record_stem(record['run']['request_id'], record['check'])}.json"


def write(record: dict, evidence_dir: Path | None = None, now: _dt.datetime | None = None) -> Path:
    """Validates and writes a record. Refuses to overwrite an existing record."""
    errors = validate(record)
    if errors:
        raise EvidenceError("invalid evidence record:\n  " + "\n  ".join(errors))
    path = record_path(record, evidence_dir, now)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "x", encoding="utf-8") as handle:
        handle.write(dumps(record))
    return path


def load_records(directory: Path) -> list[tuple[Path, dict]]:
    """Every `*.json` record under `directory`, sorted by path. Invalid JSON raises."""
    out = []
    directory = Path(directory)
    if not directory.exists():
        return out
    for path in sorted(directory.rglob("*.json")):
        out.append((path, json.loads(path.read_text(encoding="utf-8"))))
    return out


# --------------------------------------------------------------------------------------------

def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    val = sub.add_parser("validate", help="validate record files")
    val.add_argument("paths", nargs="+", type=Path)
    wr = sub.add_parser("write", help="build and write one record")
    wr.add_argument("--check", required=True)
    wr.add_argument("--kind", required=True, help='kind letters, space separated, such as "U S"')
    wr.add_argument("--result", required=True, choices=RESULTS)
    wr.add_argument("--executed", type=int, required=True)
    wr.add_argument("--failed", type=int, default=0)
    wr.add_argument("--not-run", action="append", default=[], metavar="ID=REASON")
    wr.add_argument("--guarantee", action="append", default=[])
    wr.add_argument("--scenario", action="append", default=[])
    wr.add_argument("--duration-s", type=float, default=0.0)
    wr.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args(argv)

    if args.cmd == "validate":
        schema = load_schema()
        bad = 0
        for path in args.paths:
            try:
                errors = validate(json.loads(path.read_text(encoding="utf-8")), schema)
            except json.JSONDecodeError as exc:
                errors = [f"not JSON: {exc}"]
            for error in errors:
                print(f"{path}: {error}")
            bad += bool(errors)
        print(f"{len(args.paths) - bad} valid, {bad} invalid")
        return 1 if bad else 0

    not_run = []
    for item in args.not_run:
        key, _, reason = item.partition("=")
        not_run.append({"id": key, "reason": reason or key})
    record = new_record(
        args.check, args.kind.split(), result=args.result, executed=args.executed,
        failed=args.failed, not_run=not_run, guarantees=args.guarantee, scenarios=args.scenario,
        duration_s=args.duration_s,
    )
    try:
        print(write(record, args.evidence_dir))
    except EvidenceError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))

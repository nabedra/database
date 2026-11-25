#!/usr/bin/env python3
"""Validate bug records, including open bugs without a fix, or print their Markdown counts."""

import argparse
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
import re
import subprocess
import sys
import tomllib

REPO = Path(__file__).resolve().parents[2]
BUG_DIR = REPO / "docs/testing/bugs"
REGISTER_DIR = REPO / "docs/testing/guarantees"
CLASSES = (
    "uncertain-outcome",
    "stale-route-topology-schema",
    "fence-lifecycle-order",
    "membership-replacement-trust",
    "resource-release-order",
    "other",
)
FINDERS = ("acceptance", "review", "campaign", "ci-flake", "user", "unknown")
STRING_FIELDS = (
    "id",
    "title",
    "class",
    "module",
    "found_by",
    "found_date",
    "lowest_regression",
    "why_not_caught",
    "invariant_added",
)
LIST_FIELDS = ("guarantees", "scenarios", "fix_commits")
OPEN_FIELDS = ("owner", "expires")
NOT_AN_OWNER = ("unknown", "unassigned", "none", "tbd")
ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
GUARANTEE_ID = re.compile(r"(?:OLTP|OLAP|OLAP-STR|EVT|CACHE|LSM|ORM|SCHED|VEC)-G[0-9]{2}")
REGISTER_ROW = re.compile(r"^\|\s*([A-Z][A-Z-]*-G[0-9]{2})\s*\|", re.MULTILINE)


def register_ids(directory):
    """Return the guarantee IDs the registers define, or None without registers."""
    ids = set()
    for path in sorted(directory.glob("*.md")):
        ids.update(REGISTER_ROW.findall(path.read_text(encoding="utf-8")))
    return ids or None
SHA = re.compile(r"[0-9a-f]{40}")


def utc_today():
    return datetime.now(timezone.utc).date()


def is_open(record):
    """An open bug has no fix. It never counts as fixed, replayed or detected."""
    return record.get("status") == "open"


def check_open(record, today):
    """Return the errors in the fields that mark a bug as open."""
    if "status" not in record:
        return [
            f'{field} is only valid on a record with status = "open"'
            for field in OPEN_FIELDS
            if field in record
        ]
    if not is_open(record):
        return [f'unknown status {record["status"]!r}; use "open", or omit it for a fixed bug']
    errors = []
    owner = record.get("owner")
    if not isinstance(owner, str) or owner.strip().casefold() in ("", *NOT_AN_OWNER):
        errors.append("an open record must name an owner")
    expires = record.get("expires")
    try:
        if not isinstance(expires, str) or not ISO_DATE.fullmatch(expires):
            raise ValueError
        expiry = date.fromisoformat(expires)
    except ValueError:
        errors.append('an open record must have an expires date, as a "YYYY-MM-DD" string')
    else:
        if expiry < today:
            errors.append(
                f"open record expired on {expires} (owner {owner}): fix the bug and cite the fix, "
                "or renew the expiry"
            )
    if record["fix_commits"]:
        errors.append(
            "an open record must have fix_commits = []; "
            "once the bug is fixed, remove status, owner and expires"
        )
    if record["revert_patch_ok"]:
        errors.append("an open record must have revert_patch_ok = false, as it has no fix to revert")
    return errors


def check_fields(record, known_ids=None, today=None):
    errors = []
    for field in (*STRING_FIELDS, *LIST_FIELDS, "seed", "revert_patch_ok"):
        if field not in record:
            errors.append(f"missing field {field}")
            continue
        value = record[field]
        if field in STRING_FIELDS and (not isinstance(value, str) or not value.strip()):
            errors.append(f"{field} must be a nonempty string")
        elif field in LIST_FIELDS and (
            not isinstance(value, list)
            or any(not isinstance(item, str) or not item.strip() for item in value)
        ):
            errors.append(f"{field} must be an array of nonempty strings")
        elif field == "seed" and not (value == "unknown" or (type(value) is int and value >= 0)):
            errors.append('seed must be a nonnegative integer or "unknown"')
        elif field == "revert_patch_ok" and type(value) is not bool:
            errors.append("revert_patch_ok must be a boolean")

    if errors:
        return errors
    if record["class"] not in CLASSES:
        errors.append(f"unknown class {record['class']!r}")
    if record["found_by"] not in FINDERS:
        errors.append(f"unknown finder {record['found_by']!r}")
    for guarantee in record["guarantees"]:
        if guarantee == "unknown":
            continue
        if not GUARANTEE_ID.fullmatch(guarantee):
            errors.append(f"invalid guarantee ID {guarantee!r}")
        elif known_ids is not None and guarantee not in known_ids:
            errors.append(f"guarantee ID {guarantee!r} is not in the guarantee registers")
    errors.extend(check_open(record, today or utc_today()))
    if not record["fix_commits"] and not is_open(record):
        errors.append("fix_commits must contain at least one Git commit SHA")
    for sha in record["fix_commits"]:
        if not SHA.fullmatch(sha):
            errors.append(f"invalid full Git commit SHA {sha!r}")
    return errors


def is_shallow(repo):
    result = subprocess.run(
        ["git", "rev-parse", "--is-shallow-repository"],
        cwd=repo,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def load_records(directory, repo, registers=REGISTER_DIR, warnings=None, today=None):
    """Return (records, errors). Shallow-clone notes go to `warnings`.

    Open records are included. Filter them out with `is_open` before counting fixed bugs.
    An open record that expired before `today` (default: the UTC date) is an error.
    """
    records = {}
    errors = []
    if warnings is None:
        warnings = []
    known_ids = register_ids(registers)
    paths = sorted(directory.glob("*.toml"))
    if not paths:
        return records, [f"{directory}: no bug records"]
    seen_ids = {}
    for path in paths:
        try:
            with path.open("rb") as stream:
                record = tomllib.load(stream)
        except (OSError, ValueError) as error:
            errors.append(f"{path.name}: {error}")
            continue
        field_errors = check_fields(record, known_ids, today)
        if field_errors:
            errors.extend(f"{path.name}: {error}" for error in field_errors)
            continue
        bug_id = record["id"]
        if bug_id in seen_ids:
            errors.append(f"{path.name}: duplicate id {bug_id!r} in {seen_ids[bug_id]}")
        seen_ids[bug_id] = path.name
        records[path.name] = record

    shas = sorted({sha for record in records.values() for sha in record["fix_commits"]})
    if shas:
        try:
            result = subprocess.run(
                ["git", "cat-file", "--batch-check=%(objectname) %(objecttype)"],
                cwd=repo,
                input="\n".join(shas) + "\n",
                capture_output=True,
                text=True,
                check=True,
            )
        except (OSError, subprocess.CalledProcessError) as error:
            errors.append(f"could not verify commit SHAs: {error}")
        else:
            commits = {
                line.split()[0] for line in result.stdout.splitlines() if line.endswith(" commit")
            }
            missing = [
                (name, sha)
                for name, record in records.items()
                for sha in record["fix_commits"]
                if sha not in commits
            ]
            if missing and is_shallow(repo):
                warnings.append(
                    f"warning: {len(missing)} fix commit SHAs are absent from this shallow clone "
                    "and were not verified; fetch full history (git fetch --unshallow, or "
                    "actions/checkout with fetch-depth: 0) to verify them"
                )
            else:
                errors.extend(f"{name}: unknown Git commit SHA {sha}" for name, sha in missing)
    return records, errors


def markdown_summary(records):
    def cell(value):
        return value.replace("\n", " ").replace("|", "\\|")

    fixed = [record for record in records.values() if not is_open(record)]
    unfixed = [record for record in records.values() if is_open(record)]
    lines = ["# Bug summary", "", f"Fixed records: {len(fixed)}", "", f"Open records: {len(unfixed)}"]
    for label, field in (("Class", "class"), ("Module", "module"), ("Finder", "found_by")):
        counts = Counter(record[field] for record in fixed)
        open_counts = Counter(record[field] for record in unfixed)
        values = sorted(CLASSES if field == "class" else counts.keys() | open_counts.keys())
        lines.extend(
            ["", f"## By {label.lower()}", "", f"| {label} | Fixed | Open |", "| --- | ---: | ---: |"]
        )
        for value in values:
            lines.append(f"| {cell(value)} | {counts[value]} | {open_counts[value]} |")
    if unfixed:
        lines.extend(["", "## Open", "", "| ID | Owner | Expires |", "| --- | --- | --- |"])
        for record in sorted(unfixed, key=lambda record: (record["expires"], record["id"])):
            lines.append(f"| {cell(record['id'])} | {cell(record['owner'])} | {record['expires']} |")
    return "\n".join(lines)


def main(argv=None, *, directory=BUG_DIR, repo=REPO, registers=REGISTER_DIR, today=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "summary"))
    args = parser.parse_args(argv)
    warnings = []
    records, errors = load_records(directory, repo, registers, warnings, today)
    if warnings:
        print("\n".join(warnings), file=sys.stderr)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    if args.command == "validate":
        open_count = sum(map(is_open, records.values()))
        suffix = f" {open_count} open." if open_count else ""
        print(f"Validated {len(records)} bug records.{suffix}")
    else:
        print(markdown_summary(records))
    return 0


if __name__ == "__main__":
    sys.exit(main())

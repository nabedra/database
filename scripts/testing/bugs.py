#!/usr/bin/env python3
"""Validate historical bug records or print their Markdown counts."""

import argparse
from collections import Counter
from pathlib import Path
import re
import subprocess
import sys
import tomllib

REPO = Path(__file__).resolve().parents[2]
BUG_DIR = REPO / "docs/testing/bugs"
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
GUARANTEE_ID = re.compile(r"(?:OLTP|OLAP|OLAP-STR|EVT|KV|ORM|SCHED)-G[0-9]{2}")
SHA = re.compile(r"[0-9a-f]{40}")


def check_fields(record):
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
        if guarantee != "unknown" and not GUARANTEE_ID.fullmatch(guarantee):
            errors.append(f"invalid guarantee ID {guarantee!r}")
    if not record["fix_commits"]:
        errors.append("fix_commits must contain at least one Git commit SHA")
    for sha in record["fix_commits"]:
        if not SHA.fullmatch(sha):
            errors.append(f"invalid full Git commit SHA {sha!r}")
    return errors


def load_records(directory, repo):
    records = {}
    errors = []
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
        field_errors = check_fields(record)
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
            for name, record in records.items():
                for sha in record["fix_commits"]:
                    if sha not in commits:
                        errors.append(f"{name}: unknown Git commit SHA {sha}")
    return records, errors


def markdown_summary(records):
    lines = ["# Bug summary", "", f"Total records: {len(records)}"]
    for label, field in (("Class", "class"), ("Module", "module"), ("Finder", "found_by")):
        counts = Counter(record[field] for record in records.values())
        values = sorted(CLASSES if field == "class" else counts)
        lines.extend(["", f"## By {label.lower()}", "", f"| {label} | Count |", "| --- | ---: |"])
        for value in values:
            escaped = value.replace("\n", " ").replace("|", "\\|")
            lines.append(f"| {escaped} | {counts[value]} |")
    return "\n".join(lines)


def main(argv=None, *, directory=BUG_DIR, repo=REPO):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "summary"))
    args = parser.parse_args(argv)
    records, errors = load_records(directory, repo)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    if args.command == "validate":
        print(f"Validated {len(records)} bug records.")
    else:
        print(markdown_summary(records))
    return 0


if __name__ == "__main__":
    sys.exit(main())

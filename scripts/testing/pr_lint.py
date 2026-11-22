#!/usr/bin/env python3
"""Check the evidence and review lines in a pull request body."""

import argparse
import os
from pathlib import Path
import re
import sys


REQUIRED = ("Guarantees", "Evidence", "Not run", "Reviewers")
FIELD = re.compile(r"^(Guarantees|Evidence|Not run|Reviewers):[ \t]*(.*)$")
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")


def lint(body):
    fields = {}
    fence = ""
    for line in body.splitlines():
        marker = FENCE.match(line)
        if marker:
            run, tail = marker.groups()
            if fence and run.startswith(fence) and not tail.strip():
                fence = ""
            elif not fence:
                fence = run
            continue
        if fence:
            continue
        match = FIELD.fullmatch(line)
        if match:
            name, value = match.groups()
            fields[name] = value.strip()

    errors = []
    for name in REQUIRED:
        if name not in fields:
            errors.append(f"Missing {name}: line.")
        elif not fields[name]:
            errors.append(f"{name}: requires a value.")
        elif name == "Reviewers" and fields[name].casefold() == "none":
            errors.append("Reviewers: must name a reviewer or say review is pending. 'none' is invalid.")
    return errors


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", nargs="?", help="PR body file, or - for stdin")
    parser.add_argument("--docs-only", action="store_true", help="apply the same requirements to a docs-only PR")
    args = parser.parse_args(argv)
    try:
        if args.file == "-":
            body = sys.stdin.read()
        elif args.file:
            body = Path(args.file).read_text(encoding="utf-8")
        elif "PR_BODY" in os.environ:
            body = os.environ["PR_BODY"]
        else:
            body = sys.stdin.read()
    except OSError as error:
        print(f"pr_lint: {error}", file=sys.stderr)
        return 2
    errors = lint(body)
    for error in errors:
        print(error, file=sys.stderr)
    if not errors:
        print("PR body contains all four required lines.")
    return int(bool(errors))


if __name__ == "__main__":
    sys.exit(main())

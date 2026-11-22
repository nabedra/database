#!/usr/bin/env python3
"""Build a GitHub Actions matrix from the checks requested for a pinned revision."""

import json
import math
import os
from pathlib import Path
import re
import tomllib


NEEDS = {"docker", "kind", "node", "python-orm", "rust-nightly", "s3", "valkey"}
# Setup, tool installation and the check's own build come on top of its timeout_s.
SETUP_MINUTES = 30
MAX_JOB_MINUTES = 360


def check_matrix(requested, directory):
    if not requested.strip():
        return []
    checks = []
    for check_id in dict.fromkeys(item.strip() for item in requested.split(",")):
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", check_id):
            raise ValueError(f"invalid check ID {check_id!r}")
        manifest = directory / f"{check_id}.toml"
        if not manifest.is_file():
            raise ValueError(f"unknown check {check_id!r}: {manifest} does not exist")
        with manifest.open("rb") as source:
            check = tomllib.load(source)
        if check.get("id") != check_id:
            raise ValueError(f"{manifest}: id must match the filename")
        needs = check.get("needs", [])
        if not isinstance(needs, list) or any(
            not isinstance(need, str) or need not in NEEDS for need in needs
        ):
            raise ValueError(f"{manifest}: unsupported needs {needs!r}")
        timeout_s = check.get("timeout_s", 3600)
        if not isinstance(timeout_s, int) or isinstance(timeout_s, bool) or timeout_s < 1:
            raise ValueError(f"{manifest}: timeout_s must be a positive integer")
        minutes = min(MAX_JOB_MINUTES, math.ceil(timeout_s / 60) + SETUP_MINUTES)
        checks.append({"id": check_id, "tools": needs, "timeout_minutes": minutes})
    return checks


if __name__ == "__main__":
    try:
        print(json.dumps(check_matrix(os.environ["CHECKS"], Path("docs/testing/checks"))))
    except ValueError as error:
        raise SystemExit(str(error)) from error

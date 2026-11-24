#!/usr/bin/env python3
"""Prove that two revisions compile the same product source.

For each product crate and feature set, both revisions are macro- and cfg-expanded by the
pinned rustc (`-Zunpretty=expanded`), and the expansions must be identical. Code behind
`#[cfg(feature = "sim")]`, comments and formatting are gone from an expansion, so a
sim-only change passes and any statement, block or item that a product build compiles
fails. Source line numbers are the one thing ignored: lines above a site move it.

It compares source, not instructions. `rt-zero-cost` measures those.

    python3 scripts/testing/cfg_unchanged.py --base <rev> [--head <rev>]
"""

import argparse
from contextlib import contextmanager
import difflib
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[2]

# (package, lib crate name, features beyond the package's defaults, default features)
TARGETS = [
    ("nabedra-oltp-types", "nabedra_oltp_types", "", True),
    ("nabedra-oltp-storage", "nabedra_oltp_storage", "", True),
    ("nabedra-oltp-replication", "nabedra_oltp_replication", "", True),
    ("nabedra-oltp-replication", "nabedra_oltp_replication", "s3", True),
    ("nabedra-database", "nabedra_database_server", "", True),
    ("nabedra-database", "nabedra_database_server", "olap", False),
    ("nabedra-database", "nabedra_database_server", "oltp", False),
    ("nabedra-database", "nabedra_database_server", "oltp,s3", False),
    ("nabedra-database", "nabedra_database_server", "cache", False),
]

# Tracing and panic macros expand `line!()` and `file!():line!()`.
LINES = [
    (re.compile(r"Some\(\d+u32\)"), "Some(LINE)"),
    (re.compile(r"(\.rs:)\d+"), r"\1LINE"),
]


def normalise(text):
    for pattern, replacement in LINES:
        text = pattern.sub(replacement, text)
    return text


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()


@contextmanager
def worktree(parent, name, revision):
    path = parent / name
    git("worktree", "add", "--detach", str(path), revision)
    try:
        yield path
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(path)], cwd=ROOT, check=False)


def expand(tree, target_dir, package, crate, features, defaults):
    command = ["cargo", "rustc", "--locked", "-q", "-p", package, "--lib", "--profile", "check"]
    if not defaults:
        command.append("--no-default-features")
    if features:
        command += ["--features", features]
    command += ["--", "-Zunpretty=expanded"]
    # Unstable flags for this crate only, so no dependency sees a nightly compiler.
    env = os.environ | {"CARGO_TARGET_DIR": str(target_dir), "RUSTC_BOOTSTRAP": crate, "CARGO_INCREMENTAL": "0"}
    env.pop("RUSTFLAGS", None)
    result = subprocess.run(command, cwd=tree / "server", env=env, capture_output=True, text=True)
    if result.returncode:
        raise SystemExit(f"{package} [{features}] in {tree.name} failed:\n{result.stderr[-4000:]}")
    return normalise(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--target-dir", type=Path, default=ROOT / "server/target/cfg-unchanged")
    parser.add_argument("--only", help="check one package")
    args = parser.parse_args()
    base, head = git("rev-parse", f"{args.base}^{{commit}}"), git("rev-parse", f"{args.head}^{{commit}}")
    if base == head:
        raise SystemExit("base and head are the same commit: nothing to compare")
    changed = git("diff", "--name-only", base, head, "--", "server").splitlines()
    print(f"base {base}\nhead {head}\n{len(changed)} changed files under server/")
    for entry in (args.target_dir, args.target_dir / "trees"):
        entry.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix="run-", dir=args.target_dir / "trees"))
    failed = 0
    try:
        with worktree(parent, "base", base) as before, worktree(parent, "head", head) as after:
            for package, crate, features, defaults in TARGETS:
                if args.only and package != args.only:
                    continue
                # One build directory per side: both sides share no artifact by accident.
                old = expand(before, args.target_dir / "base", package, crate, features, defaults)
                new = expand(after, args.target_dir / "head", package, crate, features, defaults)
                label = f"{package} [{'default' if defaults else 'no default'}{',' + features if features else ''}]"
                if not old.strip():
                    raise SystemExit(f"{label}: empty expansion, so nothing was compared")
                digest = hashlib.sha256(new.encode()).hexdigest()
                if old == new:
                    print(f"{label}: identical, {new.count(chr(10))} lines, sha256 {digest}", flush=True)
                    continue
                failed += 1
                print(f"{label}: DIFFERENT", flush=True)
                diff = difflib.unified_diff(old.splitlines(), new.splitlines(), "base", "head", lineterm="", n=4)
                for line, _ in zip(diff, range(400)):
                    print(f"    {line}")
    finally:
        shutil.rmtree(parent, ignore_errors=True)
        subprocess.run(["git", "worktree", "prune"], cwd=ROOT, check=False)
    if failed:
        print(f"FAILED: {failed} expansions differ")
        return 1
    print("OK: every product expansion is identical")
    return 0


if __name__ == "__main__":
    sys.exit(main())

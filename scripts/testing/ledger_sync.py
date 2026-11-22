#!/usr/bin/env python3
"""Publish text files from .nabedra/workflows/<name>/ to origin's ledgers branch."""

import argparse
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile


LEDGERS_REF = "refs/heads/ledgers"
TEXT_SUFFIXES = {".md", ".markdown", ".txt"}
MAX_FILE_BYTES = 256 * 1024
MAX_SYNC_BYTES = 2 * 1024 * 1024


def git(cwd, *args):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
    ).stdout.strip()


def collect_text(source):
    files = {}
    total = 0
    for directory, dirs, names in os.walk(source, followlinks=False, onerror=raise_walk_error):
        dirs[:] = sorted(name for name in dirs if name.lower() != ".git")
        for name in sorted(names):
            path = Path(directory) / name
            if path.suffix.lower() not in TEXT_SUFFIXES or not stat.S_ISREG(path.lstat().st_mode):
                continue
            relative = path.relative_to(source)
            with path.open("rb") as stream:
                data = stream.read(MAX_FILE_BYTES + 1)
            if len(data) > MAX_FILE_BYTES:
                print(f"Skipped {str(relative)!r}: exceeds {MAX_FILE_BYTES} bytes")
                continue
            try:
                decoded = data.decode("utf-8")
            except UnicodeDecodeError:
                print(f"Skipped {str(relative)!r}: invalid UTF-8")
                continue
            if re.search(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", decoded):
                print(f"Skipped {str(relative)!r}: contains binary control bytes")
                continue
            total += len(data)
            if total > MAX_SYNC_BYTES:
                raise ValueError(f"ledger sync exceeds {MAX_SYNC_BYTES} bytes")
            files[relative] = data
    return files


def raise_walk_error(exc):
    raise exc


def sync(repo, name):
    source = repo / ".nabedra" / "workflows" / name
    if not source.is_dir() or source.is_symlink():
        raise ValueError(f"missing local workflow directory: {source}")
    files = collect_text(source)
    if not files:
        print("No eligible ledger files")
        return

    git(repo, "fetch", "--no-tags", "origin", f"+{LEDGERS_REF}:refs/remotes/origin/ledgers")
    base = git(repo, "rev-parse", "refs/remotes/origin/ledgers")
    with tempfile.TemporaryDirectory(prefix="nabedra-ledgers-") as scratch:
        worktree = Path(scratch).resolve() / "worktree"
        git(repo, "worktree", "add", "--detach", str(worktree), base)
        try:
            for relative, data in files.items():
                target = worktree / name / relative
                if target.resolve() != target:
                    raise ValueError(f"ledger destination crosses a symlink: {name}/{relative}")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
            git(worktree, "add", "--force", "--", name)
            if not git(worktree, "diff", "--cached", "--name-only"):
                print(f"Ledger {name} is up to date")
                return
            git(worktree, "commit", "-m", f"[skip ci] Sync workflow ledger {name}")
            # A normal push rejects a concurrent update without replacing another workflow's files.
            git(worktree, "push", "origin", f"HEAD:{LEDGERS_REF}")
            print(f"Pushed ledger {name} at {git(worktree, 'rev-parse', 'HEAD')}")
        finally:
            git(repo, "worktree", "remove", "--force", str(worktree))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", help="directory name under .nabedra/workflows")
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="local checkout that contains the ledger")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", args.name):
        parser.error("name must be one workflow directory name")
    try:
        repo = Path(git(args.repo, "rev-parse", "--show-toplevel"))
        sync(repo, args.name)
    except subprocess.CalledProcessError as exc:
        print(f"ledger_sync: {exc.stderr.strip() or exc}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as exc:
        print(f"ledger_sync: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

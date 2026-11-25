#!/usr/bin/env python3
"""Prove that two revisions compile the same product code.

For each product crate and feature set, both revisions are macro- and cfg-expanded by the
pinned rustc (`-Zunpretty=expanded`), and the expansions must be identical, blank lines
aside: the printer copies those from the source, and they are no code. Code behind
`#[cfg(feature = "sim")]`, comments and formatting are gone from an expansion, so a
sim-only change passes and any statement, block or item that a product build compiles
fails. A sim-only site moves the lines below it, and macros expand line numbers, so each
changed source file of the base is first padded with a blank line per line the head
added. A head that changed or removed a line, rather than adding some, cannot be padded
and fails.

With `--binary FEATURES`, it instead builds the release server of both revisions with those
features, at one path, and the two executables must be byte-identical once the symbol
table and the build ID are stripped: every loaded byte, so code, data and unwind tables.
The symbol names differ in the hash that ThinLTO appends to promoted ones. The base then
takes the head's Cargo manifests, whose feature lists cargo passes to rustc: `cargo tree` and
the crate graph check the manifests.

    python3 scripts/testing/cfg_unchanged.py --base <rev> [--head <rev>] [--binary oltp]
"""

import argparse
from contextlib import contextmanager
import difflib
import hashlib
import os
from pathlib import Path
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

# The library source of those packages.
SOURCES = ("server/oltp-types/src/", "server/oltp-storage/src/", "server/oltp-replication/src/", "server/src/")


def git(*args, cwd=ROOT):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def show(revision, path):
    """The file at `revision`, or None."""
    result = subprocess.run(["git", "show", f"{revision}:{path}"], cwd=ROOT, capture_output=True)
    return result.stdout.decode() if result.returncode == 0 else None


def padded(base, head):
    """`base` with a blank line for each line `head` added, or None if it did more than add.

    Where an added block ends like the code before it, as with a closing brace, the first
    match is the old line: the block was added after it."""
    old, new = base.splitlines(), head.splitlines()
    out, at = [], 0
    for line in new:
        same = at < len(old) and line == old[at]
        out.append(line if same else "")
        at += same
    if at < len(old):
        # An added line equals the old line after it, so the first match was not the old line.
        out = []
        for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, old, new, autojunk=False).get_opcodes():
            if tag not in ("equal", "insert"):
                return None
            out += old[i1:i2] if tag == "equal" else [""] * (j2 - j1)
    assert [line for line in out if line] == [line for line in old if line] and len(out) == len(new)
    return "\n".join(out) + "\n"


def pad(tree, base, head):
    """Pads the changed library sources of the base `tree`."""
    for path in git("diff", "--name-only", base, head, "--", *SOURCES).splitlines():
        old, new = show(base, path), show(head, path)
        if old is None:
            print(f"{path}: new in the head")
            continue
        text = padded(old, new) if new is not None else None
        if text is None:
            raise SystemExit(f"FAILED: {path} is not only added to, so its lines cannot be aligned")
        (tree / path).write_text(text, encoding="utf-8")
        print(f"{path}: {len(new.splitlines()) - len(old.splitlines())} lines added")


@contextmanager
def worktree(parent, name, revision):
    path = parent / name
    git("worktree", "add", "--detach", str(path), revision)
    try:
        yield path
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(path)], cwd=ROOT, check=False)


def environment(target_dir):
    # sccache fails on a compilation that writes no output.
    env = os.environ | {"CARGO_TARGET_DIR": str(target_dir), "CARGO_INCREMENTAL": "0", "RUSTC_WRAPPER": ""}
    env.pop("RUSTFLAGS", None)
    return env


def expand(tree, target_dir, package, crate, features, defaults):
    command = ["cargo", "rustc", "--locked", "-q", "-p", package, "--lib", "--profile", "check"]
    if not defaults:
        command.append("--no-default-features")
    if features:
        command += ["--features", features]
    command += ["--", "-Zunpretty=expanded"]
    # Unstable flags for this crate only, so no dependency sees a nightly compiler.
    env = environment(target_dir) | {"RUSTC_BOOTSTRAP": crate}
    result = subprocess.run(command, cwd=tree / "server", env=env, capture_output=True, text=True)
    if result.returncode:
        raise SystemExit(f"{package} [{features}] in {tree.name} failed:\n{result.stderr[-4000:]}")
    return result.stdout


def code(text):
    """The lines of an expansion that are not blank."""
    return "".join(line for line in text.splitlines(keepends=True) if line.strip())


def compare_expansions(parent, target_dir, base, head, only):
    failed = 0
    with worktree(parent, "base", base) as before, worktree(parent, "head", head) as after:
        pad(before, base, head)
        for package, crate, features, defaults in TARGETS:
            if only and package != only:
                continue
            # The two trees have different paths, so they share registry artifacts only.
            old = expand(before, target_dir / "build", package, crate, features, defaults)
            new = expand(after, target_dir / "build", package, crate, features, defaults)
            label = f"{package} [{'default' if defaults else 'no default'}{',' + features if features else ''}]"
            if not old.strip():
                raise SystemExit(f"{label}: empty expansion, so nothing was compared")
            old, new = code(old), code(new)
            digest = hashlib.sha256(new.encode()).hexdigest()
            if old == new:
                print(f"{label}: identical, {new.count(chr(10))} lines, sha256 {digest}", flush=True)
                continue
            failed += 1
            print(f"{label}: DIFFERENT", flush=True)
            diff = difflib.unified_diff(old.splitlines(), new.splitlines(), "base", "head", lineterm="", n=4)
            for line, _ in zip(diff, range(400)):
                print(f"    {line}")
    print(f"FAILED: {failed} expansions differ" if failed else "OK: every product expansion is identical")
    return failed


def executable(tree, target_dir, features):
    command = ["cargo", "build", "--locked", "-q", "--release", "-p", "nabedra-database", "--bin", "nabedra-db",
               "--no-default-features", "--features", features]
    result = subprocess.run(command, cwd=tree / "server", env=environment(target_dir), capture_output=True, text=True)
    if result.returncode:
        raise SystemExit(f"release build [{features}] failed:\n{result.stderr[-4000:]}")
    built, stripped = target_dir / "release/nabedra-db", target_dir / "stripped"
    subprocess.run(["objcopy", "--strip-all", "--remove-section=.note.gnu.build-id", str(built), str(stripped)],
                   check=True)
    return stripped.read_bytes()


def compare_executables(parent, target_dir, base, head, features):
    manifests = [path for path in git("diff", "--name-only", base, head, "--", "server").splitlines()
                 if path.endswith(("/Cargo.toml", "/Cargo.lock"))]
    # One path for both revisions: cargo mixes the workspace path into symbol names.
    with worktree(parent, "tree", base) as tree:
        pad(tree, base, head)
        for path in manifests:
            git("checkout", head, "--", path, cwd=tree)
        print(f"manifests taken from the head: {manifests}", flush=True)
        old = executable(tree, target_dir / "binary", features)
        git("checkout", "--force", "--detach", head, cwd=tree)
        new = executable(tree, target_dir / "binary", features)
    for label, code in (("base", old), ("head", new)):
        print(f"{label} nabedra-db [{features}], stripped: {len(code)} bytes, sha256 {hashlib.sha256(code).hexdigest()}")
    if old != new:
        differing = sum(a != b for a, b in zip(old, new)) + abs(len(old) - len(new))
        print(f"FAILED: the release executables differ in {differing} bytes")
        return 1
    print("OK: the release executables are identical")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--target-dir", type=Path, default=ROOT / "server/target/cfg-unchanged")
    parser.add_argument("--only", help="check one package")
    parser.add_argument("--binary", metavar="FEATURES", help="compare the release executables with these features")
    args = parser.parse_args()
    base, head = git("rev-parse", f"{args.base}^{{commit}}"), git("rev-parse", f"{args.head}^{{commit}}")
    if base == head:
        raise SystemExit("base and head are the same commit: nothing to compare")
    changed = git("diff", "--name-only", base, head, "--", "server").splitlines()
    print(f"base {base}\nhead {head}\n{len(changed)} changed files under server/")
    for entry in (args.target_dir, args.target_dir / "trees"):
        entry.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix="run-", dir=args.target_dir / "trees"))
    try:
        if args.binary:
            failed = compare_executables(parent, args.target_dir, base, head, args.binary)
        else:
            failed = compare_expansions(parent, args.target_dir, base, head, args.only)
    finally:
        shutil.rmtree(parent, ignore_errors=True)
        subprocess.run(["git", "worktree", "prune"], cwd=ROOT, check=False)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

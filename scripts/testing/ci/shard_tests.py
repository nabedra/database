#!/usr/bin/env python3
"""Run one deterministic shard of the libtest suites selected by Cargo."""

import argparse
import os
from pathlib import Path
import subprocess


def shard_tests(listing, shard, shards, excluded):
    names = {line.removesuffix(": test") for line in listing.splitlines()
             if line.endswith(": test")}
    return sorted(names - excluded)[shard - 1::shards]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard", type=int, required=True, help="one-based shard index")
    parser.add_argument("--shards", type=int, required=True)
    parser.add_argument("--test-threads", type=int, default=4)
    parser.add_argument("--exclude-file", type=Path, help="one exact test name per line")
    parser.add_argument("cargo_args", nargs=argparse.REMAINDER, help="Cargo arguments after --")
    args = parser.parse_args()
    if not 1 <= args.shard <= args.shards:
        parser.error("expected 1 <= shard <= shards")
    if args.test_threads < 1:
        parser.error("test-threads must be positive")
    if not args.cargo_args or args.cargo_args[0] != "--":
        parser.error("pass the Cargo test selection after --")
    excluded = set(args.exclude_file.read_text().splitlines()) if args.exclude_file else set()
    cargo = ["cargo", "test", *args.cargo_args[1:]]
    listing = subprocess.check_output([*cargo, "--", "--list"], text=True)
    selected = shard_tests(listing, args.shard, args.shards, excluded)
    if not selected:
        parser.error(f"shard {args.shard}/{args.shards} selected zero tests")
    print(f"Shard {args.shard}/{args.shards}: {len(selected)} distinct test names", flush=True)
    # libtest treats multiple positive filters as alternatives. Exact names keep
    # prefix-related tests on separate shards. Its normal ignored-test handling applies.
    command = [*cargo, "--no-fail-fast", "--", "--test-threads", str(args.test_threads),
               "--exact", *selected]
    os.execvp(command[0], command)


if __name__ == "__main__":
    main()

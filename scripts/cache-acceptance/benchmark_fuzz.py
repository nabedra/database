#!/usr/bin/env python3
"""Run the pinned Valkey benchmark's malformed-command mode when available."""

import argparse
from contextlib import ExitStack
from pathlib import Path
import re
import subprocess
import sys
import uuid

from differential import (
    VALKEY_IMAGE, VALKEY_VERSION, RespError, parse_endpoint, ping, remove_container, spawn_nabedra,
)


def benchmark(network, args, **options):
    name = f"nabedra-kv-benchmark-{uuid.uuid4().hex}"
    try:
        return subprocess.run([
            "docker", "run", "--rm", "--name", name, "--memory=128m", "--network", network,
            VALKEY_IMAGE, "valkey-benchmark", *args,
        ], timeout=180, **options)
    finally:
        remove_container(name)


def supports_fuzz(help_text):
    return (re.search(r"(?<![\w-])--fuzz(?![\w-])", help_text) is not None
            and "--fuzz-mode" in help_text and "malformed-commands" in help_text)


def fuzz(endpoint):
    host, port = endpoint
    ping(endpoint)
    try:
        benchmark("host", [
            "-h", host, "-p", str(port), "-c", "2", "-n", "1000",
            "--fuzz", "--fuzz-mode", "malformed-commands",
        ], check=True)
    finally:
        ping(endpoint)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--target", type=parse_endpoint)
    target.add_argument("--nabedra-bin", type=Path)
    args = parser.parse_args(argv)
    try:
        help_result = benchmark(
            "none", ["--help"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        # Valkey 8.1.11 prints usage with exit status 1. A Docker error or a
        # different executable's help must not be mistaken for a capability skip.
        if help_result.returncode not in (0, 1) or "valkey-benchmark" not in help_result.stdout:
            raise RuntimeError(f"could not read valkey-benchmark help: {help_result.stdout.strip()}")
        if not supports_fuzz(help_result.stdout):
            print(f"not-run: valkey-benchmark {VALKEY_VERSION} lacks "
                  "--fuzz --fuzz-mode malformed-commands")
            return 0
        with ExitStack() as stack:
            endpoint = args.target or stack.enter_context(spawn_nabedra(args.nabedra_bin))
            fuzz(endpoint)
        print("K01 benchmark fuzz completed and Nabedra answered PING")
    except (OSError, RespError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"K01 benchmark fuzz failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Run the pinned Valkey Tcl canary against a disposable standalone cache."""

import argparse
from collections import Counter
from contextlib import ExitStack
import hashlib
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from urllib.request import urlopen
import uuid

from differential import VALKEY_IMAGE, VALKEY_REVISION, RespError, remove_container, spawn_nabedra


ALLOWLIST = (
    "unit/keyspace", "unit/expire", "unit/type/incr", "unit/type/string", "unit/protocol", "unit/auth",
)
TAGS = "-needs:repl -needs:debug -external:skip -large-memory"
ARCHIVE_URL = f"https://codeload.github.com/valkey-io/valkey/tar.gz/{VALKEY_REVISION}"
ARCHIVE_SHA256 = "b5f2527c8792059b51151359269c63905e45de376e800f651b38dc110dacd37d"
SKIPLIST = Path(__file__).resolve().parents[2] / "docs/testing/kv-conformance/skiplist.txt"
MIN_ASSERTIONS = 100
ANSI = re.compile(r"\x1b\[[0-9;]*m")
EVENT = re.compile(r"^\[(ok|err|exception)\]:", re.MULTILINE)
UNIT_EXIT = re.compile(r"^canary unit (\S+) exit=(\d+)$", re.MULTILINE)

# Run units separately because a setup exception ends Valkey's entire Tcl run.
# --durable reports exceptions inside a test body as [err]. [exception]
# records an earlier setup or suite-level error outside an executed test.
TCL_RUNNER = """
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq --no-install-recommends tcl
mkdir /tmp/valkey-tests
cd /tmp/valkey-tests
tar -xzf /work/valkey.tar.gz --strip-components=1
export VALKEY_BIN_DIR="$(dirname "$(command -v valkey-server)")"
test -x "$VALKEY_BIN_DIR/valkey-server"
host="$1"
port="$2"
tags="$3"
shift 3
for unit in "$@"; do
    printf '\ncanary unit %s\n' "$unit"
    status=0
    ./runtest --host "$host" --port "$port" --singledb --tags "$tags" \
        --single "$unit" --clients 1 --durable --timeout 60 \
        --skipfile /work/skiptests.txt || status=$?
    printf 'canary unit %s exit=%s\n' "$unit" "$status"
done
"""


def skip_patterns(path):
    patterns = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        pattern, separator, reason = line.partition("\t")
        if not separator or not pattern.strip() or not reason.strip():
            raise ValueError(f"{path}:{line_number}: expected a test name and a tab-separated reason")
        patterns.append(pattern)
    return patterns


def download_suite(path):
    with urlopen(ARCHIVE_URL, timeout=30) as source, path.open("wb") as archive:
        shutil.copyfileobj(source, archive)
    with path.open("rb") as archive:
        digest = hashlib.file_digest(archive, "sha256").hexdigest()
    if digest != ARCHIVE_SHA256:
        raise ValueError(f"Valkey Tcl archive SHA-256 mismatch: {digest}")


def summarize(output):
    output = ANSI.sub("", output)
    counts = Counter(EVENT.findall(output))
    units = {}
    harness_errors = []
    previous_end = 0
    for match in UNIT_EXIT.finditer(output):
        unit, status = match.groups()
        units[unit] = int(status)
        events = EVENT.findall(output, previous_end, match.start())
        if units[unit] and not ("err" in events or "exception" in events):
            harness_errors.append(unit)
        previous_end = match.end()
    executed = counts["ok"] + counts["err"]
    failed_units = [unit for unit, status in units.items() if status]
    print(f"K01 Tcl executed={executed} [ok]={counts['ok']} [err]={counts['err']} "
          f"[exception]={counts['exception']} failed_units={len(failed_units)}")
    if executed < MIN_ASSERTIONS:
        print(f"K01 Tcl canary is not authoritative ({executed} < {MIN_ASSERTIONS} executed assertions)")
    elif failed_units:
        print("K01 Tcl canary reported failures: " + ", ".join(failed_units))
    if tuple(units) != ALLOWLIST:
        raise ValueError("missing Tcl unit exit markers")
    if harness_errors:
        raise ValueError("Tcl exited without test results for " + ", ".join(harness_errors))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="External mode flushes the target. Docker needs host networking for loopback targets.",
    )
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--nabedra-bin", type=Path)
    parser.add_argument("--log", type=Path, default=Path("artifacts/kv-valkey-tcl.log"))
    args = parser.parse_args(argv)
    endpoint_given = args.host and args.port is not None and 1 <= args.port <= 65535
    if args.nabedra_bin:
        if args.host is not None or args.port is not None:
            parser.error("choose --nabedra-bin or --host and --port")
    elif not endpoint_given:
        parser.error("provide --nabedra-bin or --host and --port on a disposable cache")

    name = f"nabedra-kv-tcl-{uuid.uuid4().hex}"
    try:
        with ExitStack() as stack:
            work = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            download_suite(work / "valkey.tar.gz")
            (work / "skiptests.txt").write_text("\n".join(skip_patterns(SKIPLIST)), encoding="utf-8")
            host, port = (stack.enter_context(spawn_nabedra(args.nabedra_bin))
                          if args.nabedra_bin else (args.host, args.port))
            args.log.parent.mkdir(parents=True, exist_ok=True)
            network = "host" if host in ("127.0.0.1", "::1", "localhost") else "bridge"
            docker = [
                "docker", "run", "--rm", "--name", name, "--memory=256m", "--network", network,
                "--user", "0", "--mount", f"type=bind,source={work},target=/work,readonly",
                "--entrypoint", "sh", VALKEY_IMAGE, "-ec", TCL_RUNNER, "valkey-tcl",
                host, str(port), TAGS, *ALLOWLIST,
            ]
            try:
                with args.log.open("w", encoding="utf-8") as log:
                    result = subprocess.run(docker, stdout=log, stderr=subprocess.STDOUT, timeout=720)
            finally:
                remove_container(name)
            output = args.log.read_text(encoding="utf-8", errors="replace")
            print(output, end="")
            summarize(output)
            result.check_returncode()
    except (OSError, ValueError, RespError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"K01 Tcl canary could not run: {error}. Log {args.log}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

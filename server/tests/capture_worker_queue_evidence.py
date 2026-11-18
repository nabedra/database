#!/usr/bin/env python3
"""Run one immutable queue-process test binary and preserve scoped evidence.

Before compiling, obtain --print-input-sha. After compiling/copying the test
binary, pass that hash as --expected-input-sha. A changed source/fixture or an
existing output directory fails closed. This is a correctness run, not a bench.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = [
    ROOT / "server/tests/oltp_worker_queue.rs",
    ROOT / "server/tests/support/queue_child.rs",
    ROOT / "server/tests/support/tls.rs",
]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_files():
    return [ROOT / "server/Cargo.toml", ROOT / "server/Cargo.lock",
            *sorted((ROOT / "server/src").rglob("*.rs")),
            *sorted((ROOT / "orm/python/src").rglob("*.py"))]


def digest(paths):
    value = hashlib.sha256()
    for path in paths:
        value.update(str(path.relative_to(ROOT)).encode())
        value.update(b"\0")
        value.update(path.read_bytes())
        value.update(b"\0")
    return value.hexdigest()


def run():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print-input-sha", action="store_true")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--expected-input-sha")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    files = source_files()
    input_sha = digest(files + FIXTURES)
    if args.print_input_sha:
        print(input_sha)
        return
    if not all([args.binary, args.expected_input_sha, args.output_dir]):
        parser.error("--binary, --expected-input-sha and --output-dir are required")
    if input_sha != args.expected_input_sha:
        parser.error("compiled input hash differs from current source/fixtures")
    binary = args.binary.resolve(strict=True)
    binary_sha = sha(binary)
    source_sha = digest(files)
    hashes = {str(path.relative_to(ROOT)): sha(path) for path in files + FIXTURES}
    scripts = [Path(__file__).resolve(), ROOT / "server/tests/render_worker_queue_evidence.py"]
    script_hashes = {str(path.relative_to(ROOT)): sha(path) for path in scripts}
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    log = output / "worker-queue-process.log"
    command = [str(binary), "--test-threads=1", "--nocapture"]
    started = datetime.now(timezone.utc).isoformat()
    initial_load = os.getloadavg()
    timer = time.monotonic()
    with log.open("wb") as stream:
        child = subprocess.Popen(command, cwd=ROOT / "server", stdout=stream,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        try:
            status = child.wait(timeout=180)
        except BaseException:
            # The session contains only this fixture and its worker/openssl
            # children. A capture timeout must not leave test workers running.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=10)
            raise
    elapsed = time.monotonic() - timer
    assert status == 0, f"process fixture failed, see {log}"
    raw = log.read_text()
    assert "test result: ok. 5 passed; 0 failed; 1 ignored" in raw
    starts = re.findall(r"queue_child_started pid=(\d+)", raw)
    reaps = re.findall(r"queue_child_reaped pid=(\d+) signal=Some\(9\)", raw)
    assert starts and sorted(starts) == sorted(reaps), "child start/reap evidence differs"
    assert len(starts) == 28, "fixture topology or cleanup evidence changed"
    assert digest(source_files() + FIXTURES) == input_sha, "source/fixtures changed during run"
    assert sha(binary) == binary_sha, "immutable binary changed during run"
    assert all(sha(ROOT / name) == value for name, value in script_hashes.items()), "capture scripts changed"
    # Keep correct process observations separate from benchmarks. RSA fixture
    # setup, scheduling and host activity affect whole-suite elapsed time.
    record = {
        "result": "passed",
        "started_at_utc": started,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "command": command,
        "binary_sha256": binary_sha,
        "build_profile": "dev, unoptimized, debug=0, incremental disabled",
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "compiled_input_sha256": input_sha,
        "source_tree_sha256": source_sha,
        "source_files": hashes,
        "script_files": script_hashes,
        "raw_log_sha256": sha(log),
        "process_tests": {"passed": 5, "failed": 0, "ignored_child": 1, "whole_suite_seconds": elapsed},
        "cleanup": {"started": len(starts), "reaped": len(reaps), "all_reaped_with_SIGKILL": True,
                    "started_pids": starts, "reaped_pids": reaps},
        "scope": {
            "transport": "real OLTP mTLS worker protocol and separate OS processes",
            "authority": "fixture-supplied ready views and explicit fences; Control Raft not integrated",
            "backend": "deterministic pinned table.list fixture; wide insert and deep query are admission-only",
            "API": "separate test-protocol proxy process, not installed production HTTP remote commands",
            "production_remote_commands_enabled": False,
            "performance_claim": False,
        },
        "config": {
            "ready_workers": 2, "joining_workers": 1,
            "shared_query_threads": 1, "shared_control_threads": 1, "native_maintenance_threads": 2,
            "network_threads": 2, "default_task_memory_limit_bytes": 512 << 20,
            "wide_logical_memory_limit_bytes": 768 << 20, "wide_control_memory_reserve_bytes": 256 << 20,
            "queue_bytes_per_worker": 32 << 20, "result_bytes_per_worker": 2 << 20,
            "result_retention_ms": 300_000, "maximum_valid_request_body_bytes": 8 << 20,
            "limit_kind": "logical managed reservations; no OS/RSS cap claim",
        },
        "host": {"system": list(os.uname()), "load_before": initial_load, "load_after": os.getloadavg()},
        "assertions": [
            "Q03/R01/Q06/C07: owner loss at payload/acceptance/terminal barriers, exact surviving result, zero compute during copy waits",
            "Q04: API-only process restart and lost submit reply preserve the worker request sequence and context",
            "R03/Q03: empty/single/multiple/exhausted pages, exact page retries, sealed delta, readiness and last-holder takeover",
            "U02/M02: exact 8 MiB operation copies/joins under the default 512 MiB task ceiling with protected receipt progress",
            "U02: logical predicate depth 32 crosses transport; depth 33 installs no record",
        ],
    }
    artifact = output / "worker-queue-process.json"
    artifact.write_text(json.dumps(record, indent=2) + "\n")
    subprocess.run([sys.executable, str(scripts[1]), str(artifact)], check=True)
    print(json.dumps({"result": "passed", "artifact": str(artifact), "binary_sha256": binary_sha,
                      "compiled_input_sha256": input_sha, "children_reaped": len(reaps)}))


if __name__ == "__main__":
    run()

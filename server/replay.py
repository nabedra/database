#!/usr/bin/env python3
"""Scratch: each seed before and after live.patch. Fails unless the only trace lines that differ are
ExactlyOnceRelease reports and the rule counts, and the patched run has no such report.

    replay.py <seed>:<attested hash>:<red|clean> ...
"""
import json
import os
import subprocess
import sys
from pathlib import Path

SERVER = Path.cwd()
PATCH = SERVER.parent / "live.patch"


def sweep(seed, side):
    out = SERVER / "target" / "replay" / f"{seed}-{side}"
    env = dict(os.environ, NABEDRA_SEED=str(seed), NABEDRA_SEEDS="1", NABEDRA_SIM_WORKLOAD="mixed", NABEDRA_SIM_TRACE="full",
               NABEDRA_SIM_VERIFY_REPLAY="1", NABEDRA_SIM_JOBS="1", NABEDRA_SIM_CHECK_ID="replay", NABEDRA_SIM_ARTIFACTS=str(out))
    with open(SERVER / "target" / f"replay-{seed}-{side}.log", "w") as log:
        code = subprocess.run(["cargo", "test", "--locked", "-j", "2", "--release", "-p", "nabedra-sim-oltp", "--lib", "--", "--exact",
                               "--nocapture", "--test-threads=1", "sweep::tests::sweep"], env=env, stdout=log, stderr=log).returncode
    row = json.loads((out / "sweep-report.json").read_text())["seeds"][0]
    trace = next(out.glob("*/trace.txt")).read_text().splitlines()
    return code, row, trace


def build():
    subprocess.run(["cargo", "test", "--locked", "-j", "2", "--release", "-p", "nabedra-sim-oltp", "--lib", "--no-run"], check=True)


(SERVER / "target").mkdir(exist_ok=True)
cases = [arg.split(":") for arg in sys.argv[1:]]
build()
before = {seed: sweep(seed, "before") for seed, _, _ in cases}
subprocess.run(["git", "apply", "--verbose", str(PATCH)], cwd=SERVER.parent, check=True)
build()
failed = False
for seed, attested, expected in cases:
    code, row, trace = before[seed]
    code2, row2, trace2 = sweep(seed, "after")
    once = [line for line in trace if "violation ExactlyOnceRelease" in line]
    once2 = [line for line in trace2 if "violation ExactlyOnceRelease" in line]

    def rest(lines):
        return [line for line in lines if "violation ExactlyOnceRelease" not in line and "probe rules ran" not in line]

    same = rest(trace) == rest(trace2)
    print(f"seed {seed}: before hash {row['trace_hash']} (attested {attested}) kinds {sorted(row['violations'])} exit {code}, "
          f"{len(once)} ExactlyOnceRelease lines; after hash {row2['trace_hash']} kinds {sorted(row2['violations'])} exit {code2}, "
          f"{len(once2)} lines; deterministic {row['deterministic']} {row2['deterministic']}; other trace lines equal: {same}", flush=True)
    for line in once[:2]:
        at = trace.index(line)
        print("    " + "\n    ".join(text[:170] for text in trace[at - 3:at + 1]))
    ok = (row["trace_hash"] == attested and bool(once) == (expected == "red") and not once2 and same
          and row["deterministic"] and row2["deterministic"])
    failed |= not ok
print("FAILED" if failed else "OK")
sys.exit(1 if failed else 0)

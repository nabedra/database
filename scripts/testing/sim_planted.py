#!/usr/bin/env python3
"""Seeded harness defects of the topology workload, each with its clean twin (`oltp-sim-topology-planted`).

  sim_planted.py --out DIR --first 1 --seeds 8 DEFECT=RULE... -- COMMAND...

COMMAND is the sweep test. It runs once without a defect over every seed, and once per seed with each
defect, from the first seed until the defect's rule reports it. Every verdict comes from a sweep report
and not from the search lines, so a run that panicked, differed from its replay, left the fault model,
gave up or never judged the rule is an error, and neither a detection nor a clean twin.
"""

import argparse
import json
import os
import pathlib
import subprocess
import sys

# Kinds that say the run itself is broken: a panic in the harness or in the product, two runs of a seed
# that differ, and faults beyond the model.
FAULT_KINDS = {"harness", "determinism", "Panic", "BeyondFaultModel"}
RUN_LIMIT_S = 900


class Failure(Exception):
    pass


def usable(row, where):
    """The summary of a seed whose run can say something."""
    kinds = set(row["violations"])
    if broken := sorted(kinds & FAULT_KINDS):
        raise Failure(f"{where}: {', '.join(broken)}")
    if row.get("replayed") is not True or row.get("deterministic") is not True:
        raise Failure(f"{where} has no equal replay")
    if not int(row["trace_hash"], 16) or not row["trace_events"]:
        raise Failure(f"{where} reports no trace")
    summary = row.get("summary")
    if not summary or summary.get("abandoned") is not False:
        raise Failure(f"{where} was abandoned or left no summary")
    return summary


def clean_twins(rows, seeds, rules):
    """{seed: row} of the runs without a defect. None may report a rule that a defect should break."""
    if [row["seed"] for row in rows] != list(seeds):
        raise Failure(f"the clean run has the seeds {[row['seed'] for row in rows]}")
    for row in rows:
        where = f"clean seed {row['seed']}"
        usable(row, where)
        if red := sorted(set(row["violations"]) & set(rules)):
            raise Failure(f"{where} reports {', '.join(red)}")
    return {row["seed"]: row for row in rows}


def detected(row, twin, defect, rule):
    """Whether a run with the defect reports its rule, against its usable clean twin."""
    where = f"{defect} seed {row['seed']}"
    usable(row, where)
    if rule not in row["violations"]:
        return False
    if not twin["summary"]["judged"].get(rule):
        raise Failure(f"{where}: {rule} was not judged in the clean twin")
    if row["trace_hash"] == twin["trace_hash"]:
        raise Failure(f"{where} has the trace of its clean twin")
    return True


def sweep(command, out, label, env, seeds):
    """The rows of one sweep. It exits 101 when a seed is red and 0 when none is."""
    out = out / label
    report = out / "sweep-report.json"
    report.unlink(missing_ok=True)
    env = {**os.environ, **env, "NABEDRA_SEED": str(seeds[0]), "NABEDRA_SEEDS": str(len(seeds)),
           "NABEDRA_SIM_ARTIFACTS": str(out), "NABEDRA_SIM_VERIFY_REPLAY": "1"}
    env.pop("NABEDRA_SIM_EXPECT", None)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "out.log", "wb") as log:
        try:
            code = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=RUN_LIMIT_S).returncode
        except subprocess.TimeoutExpired:
            raise Failure(f"{label}: no verdict in {RUN_LIMIT_S} s") from None
    if not report.is_file():
        raise Failure(f"{label} exited {code} and left no report")
    rows = json.loads(report.read_text())["seeds"]
    if code != (101 if any(row["violations"] for row in rows) else 0):
        raise Failure(f"{label} exited {code}")
    return rows


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--first", type=int, default=1)
    parser.add_argument("--seeds", type=int, required=True)
    parser.add_argument("planted", nargs="+", metavar="DEFECT=RULE")
    if "--" not in argv:
        parser.error("no command")
    split = argv.index("--")
    args, command = parser.parse_args(argv[:split]), argv[split + 1:]
    planted = dict(pair.split("=", 1) for pair in args.planted)
    seeds = range(args.first, args.first + args.seeds)
    out = args.out.resolve()
    try:
        rows = sweep(command, out, "clean", {"NABEDRA_SIM_TOPOLOGY_DEFECT": "none"}, seeds)
        twins = clean_twins(rows, seeds, planted.values())
        print(f"clean twins: seeds {seeds[0]} to {seeds[-1]}, {sorted({k for r in rows for k in r['violations']})}")
    except Failure as error:
        sys.exit(f"clean twins: {error}")
    failed = []
    for defect, rule in planted.items():
        try:
            for seed in seeds:
                label = f"{defect}-{seed}"
                rows = sweep(command, out, label, {"NABEDRA_SIM_TOPOLOGY_DEFECT": defect}, [seed])
                if [row["seed"] for row in rows] != [seed]:
                    raise Failure(f"{label} did not run its seed")
                if detected(rows[0], twins[seed], defect, rule):
                    print(f"{defect}: {rule} at seed {seed}, trace {rows[0]['trace_hash']}, twin {twins[seed]['trace_hash']}")
                    break
            else:
                raise Failure(f"{rule} did not report it in seeds {seeds[0]} to {seeds[-1]}")
        except Failure as error:
            print(f"{defect}: {error}")
            failed.append(defect)
    if failed:
        sys.exit(f"not detected: {', '.join(failed)}")


if __name__ == "__main__":
    main(sys.argv[1:])

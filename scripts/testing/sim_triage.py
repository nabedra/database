#!/usr/bin/env python3
"""Sort the seeds of an OLTP simulation sweep into clean, red with a named cause, and red undiagnosed.

It reads sweep reports and the violations of their red seeds. A cause is named only from what a
diagnostic run of the same seed recorded, and only when that run had the same violation kinds. Every
violation of a seed needs its own evidence. Anything else is undiagnosed: an allocation site that this
file does not list, bytes that the evidence does not add up to, and a holder that is only inferred.

Nothing here is evidence for a guarantee. A named seed is as red as any other.
"""

import argparse
import collections
import csv
import glob
import gzip
import json
import pathlib
import re
import sys

# A leased record of the diagnostic build is this much larger than the product's.
INSTRUMENTATION = 8
SITES = {
    "control-route-cache": ("replica_routes", "ReplicaRouteCache"),
    "read-batch": ("read_batch", "ReadBatch"),
    "raft-log-batch": ("next_stored",),
}
# A Raft batch may belong to the stream to a removed replica, or to any other. The allocation site does
# not say which, so it names nothing.
INFERRED = {"raft-log-batch"}
SAMPLES = ["e0", "e1", "e2", "e3", "e4", "e5", "e6"]
FREEZE = "source refused the exact split freeze"
ACTIVE_MOVE = "Control refuses fences while a replica move is active"


def kind(violation):
    found = re.match(r"t=\d+ms ([\w-]+):", violation)
    return found[1] if found else "unreadable"


def site(stack):
    head = stack.split(" ## ")[0]
    return next((name for name, marks in SITES.items() if any(mark in head for mark in marks)), None)


class Leases:
    """The scratch leases of one seed's diagnostic run, by node and sample."""

    def __init__(self):
        self.rows = collections.defaultdict(dict)

    def add(self, line):
        found = re.match(r"SCR (\w+) n(\d+) #(\d+) (\d+)B t(\d+) ?(.*)", line)
        if found:
            label, node, lease, size, _, stack = found.groups()
            self.rows[int(node), label][int(lease)] = (int(size), site(stack.strip()))
        return bool(found)

    def explain(self, node, growth):
        """The holders of `growth` bytes on a node, or (None, why not)."""
        base, after = self.rows.get((node, "base")), self.rows.get((node, "e0"))
        if base is None or after is None:
            return None, "no lease sample of this node"
        grown = collections.Counter()
        count = collections.Counter()
        for sign, rows in ((-1, base), (1, after)):
            for size, where in rows.values():
                grown[where] += sign * size
                count[where] += sign
        found = sum(grown.values()) - INSTRUMENTATION * count["control-route-cache"]
        if found != growth:
            return None, f"the sampled leases grew by {found} B and the violation says {growth} B"
        holders = sorted((where or "unknown site") for where, size in grown.items() if size)
        if "unknown site" in holders:
            return None, "an unknown allocation site holds part of it"
        inferred = INFERRED.intersection(holders)
        if inferred:
            return None, f"inferred only: {'+'.join(sorted(inferred))}"
        for where in holders:
            if grown[where] > 0 and where == "control-route-cache":
                kept = [{i for i, (_, at) in self.rows.get((node, label), {}).items() if at == where}
                        for label in SAMPLES]
                if not set(after).intersection(*kept):
                    return None, "the route cache is not held in every sample"
        return "+".join(holders), None


def usage(violation):
    """The node and the growth of `working_decode_bytes`, if nothing else grew."""
    found = re.search(r"n(\d+) has (ResourceUsage .*) after quiescence, baseline (ResourceUsage .*)$", violation)
    if not found:
        return None
    node, now, before = int(found[1]), found[2], found[3]
    number = re.compile(r"working_decode_bytes: (\d+)")
    if number.sub("", now) != number.sub("", before):
        return None
    return node, int(number.search(now)[1]) - int(number.search(before)[1])


class Diagnosis:
    """What the diagnostic run of one seed recorded."""

    def __init__(self):
        self.kinds = None
        self.leases = Leases()
        self.busy_active_move = 0

    def add(self, line):
        if self.leases.add(line):
            return
        found = re.search(r"nabedra-sim-seed: (\{.*\})", line)
        if found:
            self.kinds = set(json.loads(found[1])["kinds"])
        elif "CONTROL-FAULT Busy" in line and "machine.rs" in line:
            self.busy_active_move += 1


def cause(violation, diagnosis):
    """(cause, None) from evidence, or (None, why not)."""
    name = kind(violation)
    if name == "ResourceBaseline":
        grown = usage(violation)
        if grown is None:
            return None, "another gauge than the decode bytes"
        holders, why = diagnosis.leases.explain(*grown)
        return (f"holder: {holders}", None) if holders else (None, why)
    if name == "Liveness" and "the split did not publish" in violation:
        start = violation.find('"last":')
        last = json.JSONDecoder().raw_decode(violation[start + 7:])[0] if start >= 0 else []
        if last and all(FREEZE in answer for answer in last):
            return f"the source refused the freeze of every plan ({len(last)})", None
        return None, "not every plan ended with a refused freeze"
    if name == "Liveness" and "survivors never dropped" in violation:
        if diagnosis.busy_active_move > 100:
            return f"{ACTIVE_MOVE} ({diagnosis.busy_active_move} refusals)", None
        return None, "no refused fence was recorded"
    return None, "no rule names a cause"


def classify(row, violations, diagnosis):
    """(verdict, causes) of one seed of a sweep report."""
    if row["verdict"] != "red":
        return row["verdict"], []
    if not violations:
        return "undiagnosed", ["no violation text"]
    if diagnosis is None or diagnosis.kinds is None:
        return "undiagnosed", ["no diagnostic run"]
    if diagnosis.kinds != set(row["violations"]):
        return "undiagnosed", ["the diagnostic run had other violations"]
    causes, named = [], True
    for violation in violations:
        found, why = cause(violation, diagnosis)
        named &= found is not None
        causes.append(f"{kind(violation)}: {found or 'OPEN, ' + why}")
    return ("named" if named else "undiagnosed"), causes


def verdict(row):
    """The sweep's verdict. Reports from before it had one: never clean without a replay, and
    progress is unknown, so `clean` needs a record of both."""
    if "verdict" in row:
        return row["verdict"]
    if row["violations"] or row.get("deterministic") is False:
        return "red"
    return "clean" if row["replayed"] and row.get("chaos_writes") and row.get("chaos_reads") else "unreplayed"


def load(root):
    rows = {}
    for report in glob.glob(f"{root}/**/sweep-report.json", recursive=True):
        base = pathlib.Path(report).parent
        for row in json.loads(pathlib.Path(report).read_text())["seeds"]:
            seed = row["seed"]
            found = glob.glob(f"{base}/*-seed-{seed}/summary.json")
            violations = json.loads(pathlib.Path(found[0]).read_text())["violations"] if found else []
            if seed in rows:
                raise SystemExit(f"seed {seed} is in two reports")
            rows[seed] = (dict(row, verdict=verdict(row)), violations)
    return rows


def diagnoses(paths):
    out, seed = {}, None
    for path in paths:
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt", errors="replace") as lines:
            for line in lines:
                found = re.search(r"SCRSEED (\d+)", line)
                if found:
                    seed = int(found[1])
                    # The last run of a seed counts.
                    out[seed] = Diagnosis()
                elif seed is not None:
                    out[seed].add(line)
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", help="directory of downloaded sweep artifacts")
    parser.add_argument("--diagnostics", nargs="*", default=[], help="logs of diagnostic runs")
    parser.add_argument("--csv")
    args = parser.parse_args(argv)
    known = diagnoses(args.diagnostics)
    table, total, per = [], collections.Counter(), collections.defaultdict(collections.Counter)
    why = collections.Counter()
    for seed, (row, violations) in sorted(load(args.reports).items()):
        result, causes = classify(row, violations, known.get(seed))
        total[result] += 1
        per[row["workload"]][result] += 1
        why.update(re.sub(r"\d+", "#", text) for text in causes if result == "undiagnosed")
        table.append([seed, row["workload"], row["profile"], row["trace_hash"], result, "; ".join(causes)])
    if not table:
        raise SystemExit("no sweep report found")
    if args.csv:
        with open(args.csv, "w", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(["seed", "workload", "profile", "trace_hash", "status", "causes"])
            writer.writerows(table)
    print(len(table), "seeds", dict(total), {name: dict(counts) for name, counts in sorted(per.items())})
    for text, count in why.most_common(30):
        print(f"{count:5} {text}")


if __name__ == "__main__":
    sys.exit(main())
